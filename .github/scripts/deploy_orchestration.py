#!/usr/bin/env python3
"""Deploy committed Fabric orchestration items to a production workspace (VD-4729).

The domain repo is the source of truth: `orchestration/<Name>.DataBuildToolJob/`
and `orchestration/<Name>.DataPipeline/` are committed environment-free, and this
job binds them to one environment at apply time. It is the production counterpart
of the agent's ephemeral apply — same artifacts, same REST surface, different
coordinates.

Contract (proposal §5–§6):
  * package at the deployed commit — the dbt project ships as `Code/dbt/**`
    definition parts plus a `.vd-manifest.json` stamping sourceCommit + contentHash;
  * idempotent — an unchanged contentHash skips the apply entirely;
  * applying never triggers a run;
  * rollback is redeploying an earlier commit, which is just a different hash;
  * schedules reconcile by position (create-or-update), never duplicate — always
    from the invoking pipeline's `.schedules` (2026-08-25 review: a dbt item never
    carries one). A project's one dbt job may be invoked by several pipelines
    (2026-08-26 review), each reconciled independently against its own item;
  * the pipeline's per-environment connection is injected here, never committed.

Auth reuses the bundle's transport (GitHub OIDC -> az CLI token), so no SPN
secret is stored.
"""
import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

try:
    from scripts import fabric_transport
except ImportError:  # invoked as `python3 path/to/deploy_orchestration.py`
    import fabric_transport

ONELAKE_DFS = os.environ.get("ONELAKE_DFS_BASE_URL",
                             "https://onelake.dfs.fabric.microsoft.com")
ZERO_WORKSPACE = "00000000-0000-0000-0000-000000000000"
SENTINEL = "Code/dbt/dbt_project.yml"


def log(msg):
    print(f"deploy-orchestration: {msg}", flush=True)


def fail(msg, code=1):
    print(f"deploy-orchestration: FATAL {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def tracked(repo_root, subdir):
    r = subprocess.run(["git", "ls-files", subdir], cwd=repo_root,
                       capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"git ls-files failed: {r.stderr.strip()}")
    return [line for line in r.stdout.splitlines() if line.strip()]


def head_commit(repo_root):
    r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root,
                       capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"git rev-parse failed: {r.stderr.strip()}")
    return r.stdout.strip()


def package_dbt_project(repo_root, profile, source_commit):
    """Map the tracked `transformation/` tree into `Code/dbt/**` parts.

    Returns the shared project parts and the hash OF THE PROJECT TREE ONLY. That
    hash is NOT the item's identity: the deployed item also carries
    `dbt-content.json` (the rendered environment profile plus the dbt command), so
    the idempotence comparison uses `item_content_hash()` below. Hashing only the
    tree here made a re-bind to a different lakehouse, and an edit to the command
    itself, both report "unchanged — no-op" while production kept running the old
    definition (live-verified against a real workspace).

    Mirrors the plugin's `package-dbtjob.py` deliberately rather than importing it:
    the plugin is agent-runtime and versioned independently, so the two are kept
    as parallel copies with the same rules. Empty files are skipped — the items
    API rejects an empty definition part with a 400, and the seeded scaffold
    tracks a `.gitkeep` in every dbt subdirectory.
    """
    parts, skipped = {}, []
    hasher = hashlib.sha256()
    for rel in tracked(repo_root, "transformation"):
        data = (Path(repo_root) / rel).read_bytes()
        if not data:
            skipped.append(rel)
            continue
        dest = Path(rel).relative_to("transformation").as_posix()
        parts[f"Code/dbt/{dest}"] = data
        hasher.update(f"Code/dbt/{dest}".encode())
        hasher.update(b"\0")
        hasher.update(data)
    if "Code/dbt/dbt_project.yml" not in parts:
        fail("transformation/dbt_project.yml is not tracked — nothing runnable to deploy")
    if skipped:
        log(f"note: skipped {len(skipped)} empty tracked file(s), e.g. {skipped[:3]}")
    return parts, hasher.hexdigest()


def item_content_hash(project_hash, content_bytes):
    """The deployed item's identity: the project tree PLUS this item's own
    dbt-content.json. Anything the item carries must be in here, or a real change
    silently no-ops."""
    h = hashlib.sha256()
    h.update(project_hash.encode())
    h.update(b"\0")
    h.update(content_bytes)
    return h.hexdigest()


def manifest_part(source_commit, content_hash, parts):
    return (json.dumps({"sourceCommit": source_commit,
                        "contentHash": content_hash,
                        "files": sorted(parts)}, indent=1) + "\n").encode()


def render_profile(args):
    """Build the item's environment binding.

    Hand-writing this JSON in a CI variable is a trap: a wrong shape is accepted
    by the workflow and only rejected by the items API, as an opaque
    "Object reference not set to an instance of an object." 400. So the deploy
    renders it from plain coordinates, and `--profile-json` stays available only
    as an escape hatch.
    """
    if args.profile_json:
        try:
            return json.loads(args.profile_json)
        except json.JSONDecodeError as e:
            fail(f"--profile-json is not valid JSON: {e}")
    if not (args.lakehouse_id and args.schema):
        fail("need --lakehouse-id and --schema (or an explicit --profile-json) to bind "
             "the dbt job to this environment")
    return {
        "profileType": "Lakehouse",
        "schema": args.schema,
        "connectionSettings": {
            "name": args.lakehouse_name or "lakehouse",
            "properties": {
                "type": "Lakehouse",
                "typeProperties": {"workspaceId": args.workspace_id,
                                   "artifactId": args.lakehouse_id},
            },
        },
    }


def item_parts_payload(parts):
    return {"parts": [{"path": p,
                       "payload": base64.b64encode(d).decode(),
                       "payloadType": "InlineBase64"}
                      for p, d in sorted(parts.items())]}


def list_items(workspace_id):
    return fabric_transport.request("GET", f"/workspaces/{workspace_id}/items").get("value", [])


def find_item(workspace_id, display_name, item_type):
    for it in list_items(workspace_id):
        if it.get("displayName") == display_name and it.get("type") == item_type:
            return it
    return None


def onelake_get(workspace_id, item_id, rel):
    """GET one file out of an item's OneLake storage.

    `fabric_transport.dfs_request` is write-oriented — it takes a full URL and
    discards the body — so reads are done here with the same storage-audience
    token it would use.
    """
    url = f"{ONELAKE_DFS}/{workspace_id}/{item_id}/{rel}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {fabric_transport.get_token('storage')}")
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def deployed_content_hash(workspace_id, item_id):
    """Read the manifest already in the item's storage, so an unchanged commit is a no-op."""
    try:
        return json.loads(onelake_get(workspace_id, item_id, "Code/dbt/.vd-manifest.json"))[
            "contentHash"]
    except Exception:  # noqa: BLE001 — absent/unreadable manifest means "apply it"
        return None


def apply_item(workspace_id, display_name, item_type, create_type, definition):
    existing = find_item(workspace_id, display_name, item_type)
    if existing:
        fabric_transport.request_long_running(
            "POST", f"/workspaces/{workspace_id}/items/{existing['id']}/updateDefinition",
            {"definition": definition})
        log(f"updated {display_name} ({item_type})")
        return existing["id"]
    created = fabric_transport.request_long_running(
        "POST", f"/workspaces/{workspace_id}/items",
        {"displayName": display_name, "type": create_type, "definition": definition})
    item_id = (created or {}).get("id") or (
        find_item(workspace_id, display_name, item_type) or {}).get("id")
    if not item_id:
        fail(f"created {display_name} but it is not listed")
    log(f"created {display_name} ({item_type})")
    return item_id


def wait_materialized(workspace_id, item_id, timeout=180, interval=5):
    """Definition parts land in item storage asynchronously; a run before that fails
    with a misleading 'Failed to download dbt project from OneLake'."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            onelake_get(workspace_id, item_id, SENTINEL)
            log(f"materialized {SENTINEL} in {item_id}")
            return
        except Exception:  # noqa: BLE001 — keep polling until the deadline
            time.sleep(interval)
    fail(f"{SENTINEL} did not materialize within {timeout}s")


def bind_pipeline(content, logical_to_object, connection_id):
    """Rewrite committed portable references to this environment's ids.

    The committed copy carries the invoked item's `.platform` logicalId, the
    all-zeros workspaceId, and no connection — exactly the shape the ephemeral
    apply consumes. Only the binding differs per environment.
    """
    activities = (content.get("properties") or content).get("activities") or []
    bound = 0
    for act in activities:
        if act.get("type") != "InvokeDataBuildToolJob":
            continue
        tp = act.setdefault("typeProperties", {})
        logical = tp.get("dataBuildToolJobId")
        if logical not in logical_to_object:
            fail(f"activity {act.get('name')!r} references unknown dbt job id {logical!r} — "
                 "the invoked item is not among the committed siblings")
        tp["dataBuildToolJobId"] = logical_to_object[logical]
        if tp.get("workspaceId") in (None, "", ZERO_WORKSPACE):
            tp["workspaceId"] = os.environ.get("VD_PROD_WORKSPACE_ID", "")
        if not connection_id:
            fail(f"activity {act.get('name')!r} needs the shared DataBuildToolJob "
                 "connection; pass --connection-id")
        act["externalReferences"] = {"connection": connection_id}
        bound += 1
    if not bound:
        log("no InvokeDataBuildToolJob activity in this pipeline (nothing to bind)")
    return content, bound


def reconcile_schedules(workspace_id, item_id, schedules, enable):
    """Create-or-update by position, so redeploys never duplicate a schedule."""
    existing = fabric_transport.request(
        "GET", f"/workspaces/{workspace_id}/items/{item_id}/jobs/Execute/schedules"
    ).get("value", [])
    desired = schedules.get("schedules") or []
    for i, want in enumerate(desired):
        body = {"enabled": bool(enable) and want.get("enabled", True),
                "configuration": want["configuration"]}
        if i < len(existing):
            sid = existing[i]["id"]
            fabric_transport.request(
                "PATCH",
                f"/workspaces/{workspace_id}/items/{item_id}/jobs/Execute/schedules/{sid}", body)
            log(f"schedule updated ({sid[:8]}, enabled={body['enabled']})")
        else:
            fabric_transport.request(
                "POST",
                f"/workspaces/{workspace_id}/items/{item_id}/jobs/Execute/schedules", body)
            log(f"schedule created (enabled={body['enabled']})")
    for stale in existing[len(desired):]:
        fabric_transport.request(
            "DELETE",
            f"/workspaces/{workspace_id}/items/{item_id}/jobs/Execute/schedules/{stale['id']}")
        log(f"schedule removed ({stale['id'][:8]}) — absent from the committed desired state")
    return len(desired)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--workspace-id", required=True, help="production workspace")
    ap.add_argument("--connection-id", default=os.environ.get("VD_DBTJOB_CONNECTION_ID", ""))
    ap.add_argument("--lakehouse-id", default=os.environ.get("VD_PROD_LAKEHOUSE_ID", ""))
    ap.add_argument("--lakehouse-name", default=os.environ.get("VD_PROD_LAKEHOUSE_NAME", ""))
    ap.add_argument("--schema", default=os.environ.get("VD_PROD_SCHEMA", ""))
    ap.add_argument("--profile-json", default=os.environ.get("VD_DBT_PROFILE_JSON", ""),
                    help="escape hatch: pass the binding verbatim instead of rendering it")
    ap.add_argument("--enable-schedules", action="store_true",
                    help="activate cadence (production only)")
    ap.add_argument("--force", action="store_true", help="apply even if contentHash is unchanged")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    orchestration = repo_root / "orchestration"
    if not orchestration.is_dir():
        log("no orchestration/ directory — nothing to deploy")
        return
    os.environ.setdefault("VD_PROD_WORKSPACE_ID", args.workspace_id)

    commit = head_commit(repo_root)
    jobs = sorted(orchestration.glob("*.DataBuildToolJob"))
    pipelines = sorted(orchestration.glob("*.DataPipeline"))
    log(f"deploying commit {commit[:12]}: {len(jobs)} dbt job(s), {len(pipelines)} pipeline(s)")
    if jobs:
        # State the binding before anything is applied. The schema is a
        # PRECONDITION this job does not create — Studio issues CREATE SCHEMA at
        # domain setup — and a missing one is not detected until dbt fails inside
        # the run, roughly twenty minutes later behind a Spark cold start
        # ("[SCHEMA_NOT_FOUND] The schema `x` cannot be found"). Printing it up
        # front makes a wrong value obvious while it is still cheap to fix.
        log(f"binding: workspace={args.workspace_id} lakehouse={args.lakehouse_id or '(from --profile-json)'} "
            f"schema={args.schema or '(from --profile-json)'} "
            f"connection={'set' if args.connection_id else 'MISSING'}")

    profile = render_profile(args) if jobs else None
    parts, project_hash = (package_dbt_project(repo_root, profile, commit)
                           if jobs else ({}, None))

    logical_to_object = {}
    for job_dir in jobs:
        name = job_dir.name[: -len(".DataBuildToolJob")]
        content = json.loads((job_dir / "dbt-content.json").read_text())
        if "profile" in content:
            fail(f"{job_dir.name}/dbt-content.json carries a committed profile — the "
                 "environment binding must be rendered at apply time, never committed")
        if profile:
            content["profile"] = profile
        platform = json.loads((job_dir / ".platform").read_text())
        logical = platform["config"]["logicalId"]

        content_bytes = (json.dumps(content, indent=1) + "\n").encode()
        content_hash = item_content_hash(project_hash, content_bytes)

        existing = find_item(args.workspace_id, name, "DataBuildToolJob")
        item_id = None
        if existing and not args.force:
            live = deployed_content_hash(args.workspace_id, existing["id"])
            if live and live == content_hash:
                log(f"{name}: contentHash unchanged ({content_hash[:12]}…) — no-op")
                item_id = existing["id"]

        if item_id is None:
            item_parts = dict(parts)
            item_parts["dbt-content.json"] = content_bytes
            item_parts["Code/dbt/.vd-manifest.json"] = manifest_part(commit, content_hash, parts)
            if args.dry_run:
                log(f"{name}: would apply {len(item_parts)} parts (hash {content_hash[:12]}…)")
                logical_to_object[logical] = existing["id"] if existing else "DRY-RUN"
                continue
            item_id = apply_item(args.workspace_id, name, "DataBuildToolJob", "DbtItem",
                                 item_parts_payload(item_parts))
            wait_materialized(args.workspace_id, item_id)
        logical_to_object[logical] = item_id


    for pipe_dir in pipelines:
        name = pipe_dir.name[: -len(".DataPipeline")]
        content = json.loads((pipe_dir / "pipeline-content.json").read_text())
        bound_content, bound = bind_pipeline(content, logical_to_object, args.connection_id)
        payload = item_parts_payload({
            "pipeline-content.json": (json.dumps(bound_content, indent=1) + "\n").encode()})
        if args.dry_run:
            log(f"{name}: would apply pipeline ({bound} dbt activity bound)")
            continue
        pipe_id = apply_item(args.workspace_id, name, "DataPipeline", "DataPipeline", payload)
        sched_file = pipe_dir / ".schedules"
        if sched_file.is_file():
            n = reconcile_schedules(args.workspace_id, pipe_id,
                                    json.loads(sched_file.read_text()), args.enable_schedules)
            log(f"{name}: reconciled {n} schedule(s)")

    log("done — applying never triggers a run; cadence fires only from a reconciled schedule")


if __name__ == "__main__":
    main()
