#!/usr/bin/env python3
"""Deploy committed Fabric orchestration items to a production workspace (VD-4729).

The domain repo is the source of truth: committed `.Notebook`, `.DataBuildToolJob`
and `.DataPipeline` directories under `orchestration/` are environment-free, and this
job binds them to one environment at apply time. It is the production counterpart
of the agent's ephemeral apply — same artifacts, same REST surface, different
coordinates.

Contract (proposal §5–§6):
  * package at the deployed commit — the dbt project ships as `Code/dbt/**`
    definition parts plus a `.vd-manifest.json` stamping sourceCommit + contentHash;
  * an unchanged dbt contentHash skips its apply; Notebook and pipeline items
    update in place, preserving their physical ids;
  * canonical dlt runners select an immutable tracked-source revision in the named
    Lakehouse; its pointer is committed only after item and schedule application;
  * applying never triggers a run;
  * rollback is redeploying an earlier commit, which is just a different hash;
  * schedules reconcile by position (create-or-update), never duplicate — always
    from the invoking pipeline's `.schedules` (2026-08-25 review: a dbt item never
    carries one). A project's one dbt job may be invoked by several pipelines
    (2026-08-26 review), each reconciled independently against its own item;
  * dbt activities' per-environment connection is injected here, never committed.

Auth reuses the bundle's transport (GitHub OIDC -> az CLI token), so no SPN
secret is stored.
"""
import argparse
import base64
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from concurrent.futures import CancelledError

try:
    from scripts import fabric_transport
    from scripts import domain_repo_pre_hook
    from scripts import ingestion_release
except ImportError:  # invoked as `python3 path/to/deploy_orchestration.py`
    import fabric_transport
    import domain_repo_pre_hook
    import ingestion_release

ONELAKE_DFS = os.environ.get("ONELAKE_DFS_BASE_URL",
                             "https://onelake.dfs.fabric.microsoft.com")
ZERO_WORKSPACE = "00000000-0000-0000-0000-000000000000"
SENTINEL = "Code/dbt/dbt_project.yml"
# Canonical materializer output at vd-data-engineering a413cc02. Reader updates
# require an explicit compatibility review and fixture/digest parity update.
SUPPORTED_INGESTION_RUNNER = "39a5a11a43ea24beaa45aa036f63314eae69d7b47d0f9d95ae917113fbb9182e"


def release_store(workspace_id):
    return ingestion_release.BlobReleaseStore(workspace_id)


def log(msg):
    print(f"deploy-orchestration: {msg}", flush=True)


def fail(msg, code=1):
    print(f"deploy-orchestration: FATAL {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def tracked(repo_root, subdir):
    r = subprocess.run(["git", "ls-files", "-z", "--", subdir], cwd=repo_root,
                       capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"git ls-files failed: {r.stderr.strip()}")
    return [path for path in r.stdout.split("\0") if path]


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
    """Read a complete inventory or fail; absence is meaningful only after the last page."""
    base = f"/workspaces/{workspace_id}/items"
    path, items, seen_tokens = base, [], set()
    for _ in range(100):
        page = fabric_transport.request("GET", path)
        if not isinstance(page, dict) or not isinstance(page.get("value"), list):
            fail("incomplete workspace inventory: malformed item page")
        if any(not isinstance(item, dict) or not all(
            isinstance(item.get(key), str) and item[key] for key in ("id", "displayName", "type")
        ) for item in page["value"]):
            fail("incomplete workspace inventory: malformed item")
        items.extend(page["value"])
        if len(items) > 10000:
            fail("incomplete workspace inventory: exceeded 10000 items")
        token = page.get("continuationToken")
        if token is None:
            if page.get("continuationUri"):
                fail("incomplete workspace inventory: continuation URI without a token")
            return items
        if not isinstance(token, str) or not token or token in seen_tokens:
            fail("incomplete workspace inventory: invalid or repeated continuation token")
        seen_tokens.add(token)
        # Keep requests on the configured transport/workspace, never follow a provider URL.
        path = base + "?" + urllib.parse.urlencode({"continuationToken": token})
    fail("incomplete workspace inventory: exceeded 100 pages")


def find_item(workspace_id, display_name, item_type):
    matches = [it for it in list_items(workspace_id)
               if it.get("displayName") == display_name and it.get("type") == item_type]
    if len(matches) > 1:
        fail(f"ambiguous {item_type} named {display_name!r} in workspace {workspace_id}")
    return matches[0] if matches else None


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
    except CancelledError:
        raise
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
        except CancelledError:
            raise
        except Exception:  # noqa: BLE001 — keep polling until the deadline
            time.sleep(interval)
    fail(f"{SENTINEL} did not materialize within {timeout}s")


def pipeline_activities(content):
    """Visit activities including Fabric's nested control-flow containers."""
    if isinstance(content, dict):
        if content.get("type") in ("TridentNotebook", "InvokeDataBuildToolJob"):
            yield content
        else:
            for value in content.values():
                yield from pipeline_activities(value)
    elif isinstance(content, list):
        for value in content:
            yield from pipeline_activities(value)


def bind_pipeline(content, logical_to_object, connection_id, workspace_id):
    """Rewrite committed portable references to this environment's ids.

    The committed copy carries the invoked item's `.platform` logicalId, the
    all-zeros workspaceId, and no connection — exactly the shape the ephemeral
    apply consumes. Only the binding differs per environment.
    """
    content = copy.deepcopy(content)
    bound = 0
    for act in pipeline_activities(content):
        is_dbt = act["type"] == "InvokeDataBuildToolJob"
        item_type = "DataBuildToolJob" if is_dbt else "Notebook"
        id_key = "dataBuildToolJobId" if is_dbt else "notebookId"
        tp = act.setdefault("typeProperties", {})
        logical = tp.get(id_key)
        target = logical_to_object.get(logical) if isinstance(logical, str) else None
        if not target:
            fail(f"activity {act.get('name')!r} references unknown {item_type} id {logical!r} — "
                 "the invoked item is not among the committed siblings")
        if target["type"] != item_type:
            fail(f"activity {act.get('name')!r} requires {item_type}, but {logical!r} is {target['type']}")
        tp[id_key] = target["id"]
        tp["workspaceId"] = workspace_id
        if is_dbt and not connection_id:
            fail(f"activity {act.get('name')!r} needs the shared DataBuildToolJob "
                 "connection; pass --connection-id")
        if is_dbt:
            act["externalReferences"] = {"connection": connection_id}
        bound += 1
    return content, bound


def ingestion_targets(pipelines, tracked_paths):
    """Validate the canonical runner's parameters and select its code destinations."""
    targets = {}
    for content in pipelines:
        for act in pipeline_activities(content):
            if act["type"] != "TridentNotebook":
                continue
            tp = act["typeProperties"]
            params = tp.get("parameters") or {}
            required = {"DOMAIN_SLUG", "PIPELINE", "SECRET_STORE_LOCATION", "LAKEHOUSE_NAME"}
            if set(params) != required or any(
                not isinstance(p, dict) or p.get("type") != "string"
                or not isinstance(p.get("value"), str) or not p["value"].strip()
                for p in params.values()
            ):
                fail(f"activity {act.get('name')!r} requires the four canonical dlt string parameters")
            slug, script, lakehouse = (params[k]["value"] for k in ("DOMAIN_SLUG", "PIPELINE", "LAKEHOUSE_NAME"))
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", slug):
                fail("DOMAIN_SLUG must be a single safe path segment")
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*_pipeline\.py", script) or f"ingestion/{script}" not in tracked_paths:
                fail(f"PIPELINE {script!r} must name a tracked ingestion/<name>_pipeline.py")
            target = (slug, lakehouse)
            logical = tp["notebookId"]
            if logical in targets and targets[logical] != target:
                fail(f"Notebook {logical!r} has conflicting DOMAIN_SLUG/LAKEHOUSE_NAME parameters")
            targets[logical] = target
    return sorted(set(targets.values()))


def gather_ingestion(repo_root):
    """Snapshot safe tracked bytes once, then apply the complete publish policy."""
    paths = tracked(repo_root, "ingestion")
    if len(paths) > ingestion_release.FILE_COUNT_LIMIT:
        fail("ingestion source file count exceeds bounds")
    files, total = {}, 0
    for rel in paths:
        try:
            ingestion_release.check_source_path(rel)
            if not rel.startswith("ingestion/") or rel in files:
                raise ValueError("invalid ingestion source path")
            data = ingestion_release.read_source(repo_root, rel)
        except ValueError as exc:
            fail(str(exc))
        if data is not None:
            files[rel] = data
            total += len(data)
        if total > ingestion_release.TOTAL_LIMIT:
            fail("ingestion source total bytes exceeds bounds")
    result = domain_repo_pre_hook.evaluate_publish([{"path": p, "content": b} for p, b in files.items()])
    if result["decision"] == "block":
        fail(domain_repo_pre_hook.render_block_message(result["findings"]))
    return files


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
    commit = head_commit(repo_root)
    notebooks = sorted(orchestration.glob("*.Notebook"))
    jobs = sorted(orchestration.glob("*.DataBuildToolJob"))
    pipelines = sorted(orchestration.glob("*.DataPipeline"))
    log(f"deploying commit {commit[:12]}: {len(notebooks)} notebook(s), {len(jobs)} dbt job(s), {len(pipelines)} pipeline(s)")
    logical_to_object, logical_by_dir = {}, {}
    for item_dir in [*notebooks, *jobs, *pipelines]:
        item_type = item_dir.suffix[1:]
        platform = json.loads((item_dir / ".platform").read_text())
        logical = platform.get("config", {}).get("logicalId")
        if not isinstance(logical, str) or not logical.strip():
            fail(f"{item_dir.name} has no logicalId")
        if platform.get("metadata", {}).get("type") != item_type:
            fail(f"{item_dir.name} has mismatched .platform item type")
        if logical in logical_to_object:
            fail(f"duplicate committed logicalId {logical!r}")
        logical_by_dir[item_dir] = logical
        logical_to_object[logical] = {"id": "DRY-RUN", "type": item_type}
    pipeline_contents = {p: json.loads((p / "pipeline-content.json").read_text()) for p in pipelines}
    # Validate every reference before any upload or item publication.
    for content in pipeline_contents.values():
        bind_pipeline(content, logical_to_object, args.connection_id, args.workspace_id)
    has_runner = any(act["type"] == "TridentNotebook"
                     for content in pipeline_contents.values() for act in pipeline_activities(content))
    ingestion_files = gather_ingestion(repo_root) if has_runner else {}
    targets = ingestion_targets(pipeline_contents.values(), ingestion_files)
    if len(targets) > 1:
        fail("one deployment must use one shared ingestion DOMAIN_SLUG/LAKEHOUSE_NAME target")
    release_root = None
    for slug, name in targets:
        lakehouse = find_item(args.workspace_id, name, "Lakehouse")
        if not lakehouse:
            fail(f"no Lakehouse named {name!r} in workspace {args.workspace_id}")
        release_root = f"{lakehouse['id']}/Files/{slug}/.vibedata"
    invoked_notebooks = {act["typeProperties"]["notebookId"] for content in pipeline_contents.values()
                         for act in pipeline_activities(content) if act["type"] == "TridentNotebook"}
    notebook_definitions = {}
    committed_orchestration = set(tracked(repo_root, "orchestration")) if notebooks else set()
    for notebook_dir in notebooks:
        for name in (".platform", "notebook-content.py"):
            path = notebook_dir / name
            if (path.relative_to(repo_root).as_posix() not in committed_orchestration
                    or path.is_symlink() or notebook_dir.is_symlink()):
                fail(f"{notebook_dir.name}/{name} must be a tracked regular file")
        source = (notebook_dir / "notebook-content.py").read_bytes()
        if not source:
            fail(f"{notebook_dir.name}/notebook-content.py is empty")
        if (logical_by_dir[notebook_dir] in invoked_notebooks
                and hashlib.sha256(source).hexdigest() != SUPPORTED_INGESTION_RUNNER):
            fail(f"{notebook_dir.name} is not a supported immutable ingestion runner; refresh with "
                 "the reviewed plugin's materialize-dlt-runner.py <name> --out orchestration --force")
        notebook_definitions[notebook_dir] = {
            **item_parts_payload({"notebook-content.py": source}), "format": "fabricGitSource"}
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

    staged, store = None, None
    if release_root:
        if args.dry_run:
            _, revision = ingestion_release.build_manifest(ingestion_files)
            ingestion_release.preflight_release(release_root, ingestion_files, revision)
            log(f"would stage and publish ingestion revision {revision}")
        else:
            store = release_store(args.workspace_id)
            staged = ingestion_release.stage_release(store, release_root, ingestion_files, commit)

    for notebook_dir, definition in notebook_definitions.items():
        name = notebook_dir.name[:-len(".Notebook")]
        if args.dry_run:
            log(f"{name}: would apply Notebook")
            continue
        item_id = apply_item(args.workspace_id, name, "Notebook", "Notebook", definition)
        logical_to_object[logical_by_dir[notebook_dir]]["id"] = item_id

    for job_dir in jobs:
        name = job_dir.name[: -len(".DataBuildToolJob")]
        content = json.loads((job_dir / "dbt-content.json").read_text())
        if "profile" in content:
            fail(f"{job_dir.name}/dbt-content.json carries a committed profile — the "
                 "environment binding must be rendered at apply time, never committed")
        if profile:
            content["profile"] = profile
        logical = logical_by_dir[job_dir]

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
                logical_to_object[logical]["id"] = existing["id"] if existing else "DRY-RUN"
                continue
            item_id = apply_item(args.workspace_id, name, "DataBuildToolJob", "DbtItem",
                                 item_parts_payload(item_parts))
            wait_materialized(args.workspace_id, item_id)
        logical_to_object[logical]["id"] = item_id


    for pipe_dir in pipelines:
        name = pipe_dir.name[: -len(".DataPipeline")]
        content = pipeline_contents[pipe_dir]
        bound_content, bound = bind_pipeline(content, logical_to_object, args.connection_id, args.workspace_id)
        payload = item_parts_payload({
            "pipeline-content.json": (json.dumps(bound_content, indent=1) + "\n").encode()})
        if args.dry_run:
            log(f"{name}: would apply pipeline ({bound} activities bound)")
            continue
        pipe_id = apply_item(args.workspace_id, name, "DataPipeline", "DataPipeline", payload)
        sched_file = pipe_dir / ".schedules"
        if sched_file.is_file():
            n = reconcile_schedules(args.workspace_id, pipe_id,
                                    json.loads(sched_file.read_text()), args.enable_schedules)
            log(f"{name}: reconciled {n} schedule(s)")

    if staged:
        revision = ingestion_release.commit_release(store, staged)
        log(f"published ingestion revision {revision}")
    log("done — applying never triggers a run; cadence fires only from a reconciled schedule")


if __name__ == "__main__":
    main()
