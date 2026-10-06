#!/usr/bin/env python3
"""Deploy committed Fabric orchestration items to a production workspace (VD-4729).

The domain repo is the source of truth: committed `.Notebook` and `.DataPipeline`
directories under `orchestration/` are environment-free, and this job binds them to
one environment at apply time. It is the production counterpart of the agent's
ephemeral apply — same artifacts, same REST surface, different coordinates.

Contract:
  * a pipeline invokes two kinds of notebook: the dlt runner, pinned to the plugin's
    template bytes, and a dbt job notebook, recognised by its marker, which carries
    its own commands and takes only `DBT_OUTPUT_DIR`;
  * the code each reads replaces its folder in the Domain Lakehouse before any item
    is applied — `Files/<slug>/ingestion` for the runner, `Files/<slug>/transformation`
    for a dbt job, with this commit's `dbt parse --target prod` manifest at
    `prod-target/manifest.json` for an Intent run to defer to — and each is applied
    with that Lakehouse attached (on the applied copy, never the committed file);
  * Notebook and pipeline items update in place, preserving their physical ids;
  * applying never triggers a run;
  * rollback is redeploying an earlier commit;
  * schedules reconcile by position (create-or-update), never duplicate — always
    from the invoking pipeline's `.schedules`.

A Fabric dbt job item (`DataBuildToolJob`) is retired: one committed, or a pipeline
invoking one, fails the deploy before anything is applied.

Auth reuses the bundle's transport (GitHub OIDC -> az CLI token), so no SPN
secret is stored.
"""
import argparse
import base64
import copy
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import tomllib
import urllib.parse
from pathlib import Path

import yaml

try:
    from scripts import fabric_transport
    from scripts import domain_repo_pre_hook
    from scripts import ingestion_release
    from scripts import ci_config
    from scripts import managed_requirements_gate
except ImportError:  # invoked as `python3 path/to/deploy_orchestration.py`
    import fabric_transport
    import domain_repo_pre_hook
    import ingestion_release
    import ci_config
    import managed_requirements_gate

# The plugin's Lakehouse runner template at vd-data-engineering SUPPORTED_RUNNER_REVISION,
# which its materializer copies verbatim. A reader update needs a compatibility review and the
# fixture/digest parity update in the same change. The Warehouse runner is not
# accepted: this bundle deploys `fabric_lakehouse` Domains only.
SUPPORTED_INGESTION_RUNNER = "807d97261940099aa3c8c1398cb50b7ce7ffe27c130dd71f680157b1d46d6c98"
SUPPORTED_RUNNER_REVISION = "f4d2c67f"
# A dbt job notebook is the plugin's template with its own commands written in, so it
# is recognised by the marker its first cell carries rather than by its bytes.
DBT_JOB_MARKER = b"# VibeData dbt job"
DBT_JOB_PARAMETERS = {"DBT_OUTPUT_DIR": {"value": {"value": "@pipeline().RunId", "type": "Expression"},
                                         "type": "string"}}
PROD_MANIFEST = "transformation/prod-target/manifest.json"
RETIRED_DBT_JOB = ("a Fabric dbt job (DataBuildToolJob), which Domains no longer deploy: run its commands "
                   "from a dbt job notebook (vd-data-engineering materialize-dbt-job-notebook.py), point "
                   "the activity at that notebook, then remove the orchestration/*.DataBuildToolJob/ item")
# The first `# META { … }` block of a `notebook-content.py` is the notebook's own
# metadata; later blocks belong to individual cells.
_META_BLOCK_RE = re.compile(r"(?m)^# META \{\n(?:^# META .*\n)*?^# META \}\n")


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


def bind_pipeline(content, logical_to_object, workspace_id):
    """Rewrite committed portable references to this environment's ids.

    The committed copy carries the invoked notebook's `.platform` logicalId and the
    all-zeros workspaceId — exactly the shape the ephemeral apply consumes. Only the
    binding differs per environment.
    """
    content = copy.deepcopy(content)
    bound = 0
    for act in pipeline_activities(content):
        if act["type"] == "InvokeDataBuildToolJob":
            fail(f"activity {act.get('name')!r} invokes {RETIRED_DBT_JOB}")
        tp = act.setdefault("typeProperties", {})
        logical = tp.get("notebookId")
        target = logical_to_object.get(logical) if isinstance(logical, str) else None
        if not target:
            fail(f"activity {act.get('name')!r} references unknown Notebook id {logical!r} — "
                 "the invoked item is not among the committed siblings")
        if target["type"] != "Notebook":
            fail(f"activity {act.get('name')!r} requires Notebook, but {logical!r} is {target['type']}")
        tp["notebookId"] = target["id"]
        tp["workspaceId"] = workspace_id
        bound += 1
    return content, bound


def check_dbt_job_activities(pipelines, dbt_notebooks):
    """A dbt job notebook's activity passes the pipeline run id as DBT_OUTPUT_DIR, and nothing else."""
    for content in pipelines:
        for act in pipeline_activities(content):
            if act["typeProperties"].get("notebookId") in dbt_notebooks and \
                    (act["typeProperties"].get("parameters") or {}) != DBT_JOB_PARAMETERS:
                fail(f"activity {act.get('name')!r} invokes a dbt job notebook, whose only parameter is "
                     f"{json.dumps(DBT_JOB_PARAMETERS)}")


def ingestion_targets(pipelines, tracked_paths, dbt_notebooks):
    """Validate the canonical runner's parameters; return the DOMAIN_SLUGs its code goes under."""
    targets = {}
    for content in pipelines:
        for act in pipeline_activities(content):
            if act["typeProperties"].get("notebookId") in dbt_notebooks:
                continue
            tp = act["typeProperties"]
            params = tp.get("parameters") or {}
            if set(params) != {"DOMAIN_SLUG", "PIPELINE"} or any(
                not isinstance(p, dict) or p.get("type") != "string"
                or not isinstance(p.get("value"), str) or not p["value"].strip()
                for p in params.values()
            ):
                fail(f"activity {act.get('name')!r} requires exactly the two canonical dlt string "
                     "parameters DOMAIN_SLUG and PIPELINE")
            slug, script = (params[k]["value"] for k in ("DOMAIN_SLUG", "PIPELINE"))
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", slug):
                fail("DOMAIN_SLUG must be a single safe path segment")
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*_pipeline\.py", script) or f"ingestion/{script}" not in tracked_paths:
                fail(f"PIPELINE {script!r} must name a tracked ingestion/<name>_pipeline.py")
            logical = tp["notebookId"]
            if logical in targets and targets[logical] != slug:
                fail(f"Notebook {logical!r} has conflicting DOMAIN_SLUG parameters")
            targets[logical] = slug
    return sorted(set(targets.values()))


def _unwrap_meta(block):
    return json.loads("\n".join(line[len("# META "):] for line in block.rstrip("\n").split("\n")))


def _wrap_meta(metadata):
    return "".join(f"# META {line}\n" for line in json.dumps(metadata, indent=2).split("\n"))


def attach_default_lakehouse(text, lakehouse_id, lakehouse_name, workspace_id):
    """Bind the runner's default Lakehouse — its code path — for this workspace.

    Fabric reads the attachment from the notebook's own metadata and honours it on
    updateDefinition as well as create (probed live, VD-5847). Only the lakehouse
    key is set: the same block can carry environment/warehouse dependencies.
    Parallel copy of the plugin's `inject_default_lakehouse.py`.
    """
    match = _META_BLOCK_RE.search(text)
    if match is None:
        raise ValueError("no notebook metadata block found; not a Fabric notebook-content.py")
    metadata = _unwrap_meta(match.group(0))
    metadata.setdefault("dependencies", {})["lakehouse"] = {
        "default_lakehouse": lakehouse_id,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": workspace_id,
    }
    return text[: match.start()] + _wrap_meta(metadata) + text[match.end():]


def code_lakehouse(repo_root, workspace_id):
    """The Lakehouse whose Files/ holds the notebooks' code, and the Domain slug its folders
    sit under: the Domain's own, named in the committed ci-config — the same coordinates
    publish-prod-manifest reads."""
    path = Path(repo_root) / ".workflow" / "ci-config.yml"
    if not path.is_file():
        fail(".workflow/ci-config.yml is missing; it names the Lakehouse the notebooks attach")
    parsed = ci_config.parse_ci_config(path.read_text())
    lakehouse_id = parsed["config"].get("prod_lakehouse_id")
    if not parsed["ok"] or not isinstance(lakehouse_id, str) or not lakehouse_id.strip():
        fail(f"ci-config.yml does not name VD_DOMAIN_FABRIC_LAKEHOUSE_ID: {parsed['error']}")
    matches = [it for it in list_items(workspace_id)
               if it["id"].lower() == lakehouse_id.strip().lower()]
    if len(matches) != 1 or matches[0]["type"] != "Lakehouse":
        fail(f"VD_DOMAIN_FABRIC_LAKEHOUSE_ID {lakehouse_id!r} is not a Lakehouse in workspace {workspace_id}")
    return matches[0], parsed["config"].get("domain")


def preflight_dlt_config(files, workspace_id):
    """Refuse at deploy what the runner would refuse at run time, where it is cheap to see."""
    raw = files.get("ingestion/.dlt/config.toml")
    if raw is None:
        fail("ingestion/.dlt/config.toml is not tracked; the runner reads its destination and vault from it")
    try:
        config = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        fail(f"ingestion/.dlt/config.toml is not valid TOML: {exc}")
    vault = ((config.get("providers") or {}).get("azure_key_vault") or {}).get("vault_url")
    if not isinstance(vault, str) or not vault.strip():
        fail("ingestion/.dlt/config.toml has no [providers.azure_key_vault] vault_url. The runner "
             "resolves the Domain's vault from it and takes no SECRET_STORE_LOCATION parameter; "
             "re-run the source setup to write the provider section, then commit it.")
    destination = str(((config.get("destination") or {}).get("filesystem") or {}).get("workspace_id", ""))
    if destination.lower() != workspace_id.lower():
        fail(f"ingestion/.dlt/config.toml [destination.filesystem] names workspace {destination!r}, "
             f"not the deploy workspace {workspace_id}; the runner refuses that copy")


def check_prod_profile(files):
    """Production runs the committed profile's `prod` target, in a notebook that has none of
    this job's environment — so `prod` must be the default and hold fixed values."""
    raw = files.get("transformation/profiles.yml")
    if raw is None:
        fail("transformation/profiles.yml is not tracked; a dbt job notebook runs its prod target")
    try:
        profiles = yaml.safe_load(raw.decode("utf-8", "replace")) or {}
    except yaml.YAMLError as exc:
        fail(f"transformation/profiles.yml is not valid YAML: {exc}")
    # Searched after parsing, so a comment that names env_var() is not a use of it.
    if "env_var(" in str(profiles):
        fail("transformation/profiles.yml uses env_var(); a Fabric notebook has none of the deploy's "
             "environment, so its prod target must hold the Domain's fixed values")
    for name, profile in profiles.items():
        if not isinstance(profile, dict) or profile.get("target") != "prod" \
                or "prod" not in (profile.get("outputs") or {}):
            fail(f"transformation/profiles.yml profile {name!r} must default to a `prod` target; "
                 "production defers whenever the default target is not prod")


def parse_prod_manifest(repo_root):
    """`dbt parse --target prod` on the deployed commit: the manifest an Intent run defers to.
    Parsing opens no connection, so it needs no Fabric credentials."""
    project = Path(repo_root) / "transformation"
    with tempfile.TemporaryDirectory() as out:
        commands = [["dbt", "deps"]] if any((project / f).is_file() for f in ("packages.yml", "dependencies.yml")) else []
        commands.append(["dbt", "parse", "--target", "prod", "--target-path", out])
        for command in commands:
            r = subprocess.run([*command, "--project-dir", str(project), "--profiles-dir", str(project)],
                               capture_output=True, text=True)
            if r.returncode != 0:
                fail(f"`{' '.join(command)}` failed on the deployed commit:\n{(r.stdout + r.stderr)[-4000:]}")
        return (Path(out) / "manifest.json").read_bytes()


def gather_code(repo_root, folder):
    """Snapshot safe tracked bytes once, then apply the complete publish policy."""
    paths = tracked(repo_root, folder)
    if len(paths) > ingestion_release.FILE_COUNT_LIMIT:
        fail(f"{folder} source file count exceeds bounds")
    files, total = {}, 0
    for rel in paths:
        try:
            ingestion_release.check_source_path(rel)
            if not rel.startswith(f"{folder}/") or rel in files:
                raise ValueError(f"invalid {folder} source path")
            data = ingestion_release.read_source(repo_root, rel)
        except ValueError as exc:
            fail(str(exc))
        if data is not None:
            files[rel] = data
            total += len(data)
        if total > ingestion_release.TOTAL_LIMIT:
            fail(f"{folder} source total bytes exceeds bounds")
    result = domain_repo_pre_hook.evaluate_publish([{"path": p, "content": b} for p, b in files.items()])
    if result["decision"] == "block":
        fail(domain_repo_pre_hook.render_block_message(result["findings"]))
    return files


MANAGED_CONSTRAINTS = ".github/managed-constraints.txt"


def publish_managed_requirements(repo_root, files):
    """Copy of `files` whose ingestion requirements carry no version on a managed package.

    The runner installs these under the managed constraints, where a Domain's own pin on a
    managed package makes pip refuse; the managed version wins here as it does in the sandbox
    (VD-6588). The names come from the delivered constraints file. A pin never blocks the
    deploy: a missing or unreadable constraints file publishes the code as committed with a
    warning. The Domain's repository files are never rewritten.
    """
    try:
        managed = managed_requirements_gate.managed_versions(
            (Path(repo_root) / MANAGED_CONSTRAINTS).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        log(f"WARNING cannot read {MANAGED_CONSTRAINTS} ({exc}); publishing ingestion requirements as committed")
        return files
    published = dict(files)
    for rel, data in files.items():
        if not managed_requirements_gate.is_requirements_file(rel):
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        rewritten, notes = managed_requirements_gate.neutralize_requirements(text, managed)
        for note in notes:
            log(f"{rel}: {note}")
        if notes:
            published[rel] = rewritten.encode("utf-8")
    return published


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
    ap.add_argument("--enable-schedules", action="store_true",
                    help="activate cadence (production only)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    orchestration = repo_root / "orchestration"
    if not orchestration.is_dir():
        log("no orchestration/ directory — nothing to deploy")
        return
    retired = sorted(orchestration.glob("*.DataBuildToolJob"))
    if retired:
        fail(f"{retired[0].name} is {RETIRED_DBT_JOB}")
    commit = head_commit(repo_root)
    notebooks = sorted(orchestration.glob("*.Notebook"))
    pipelines = sorted(orchestration.glob("*.DataPipeline"))
    log(f"deploying commit {commit[:12]}: {len(notebooks)} notebook(s), {len(pipelines)} pipeline(s)")
    logical_to_object, logical_by_dir = {}, {}
    for item_dir in [*notebooks, *pipelines]:
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
        bind_pipeline(content, logical_to_object, args.workspace_id)
    invoked_notebooks = {act["typeProperties"]["notebookId"] for content in pipeline_contents.values()
                         for act in pipeline_activities(content)}

    sources = {}
    committed_orchestration = set(tracked(repo_root, "orchestration")) if notebooks else set()
    for notebook_dir in notebooks:
        for name in (".platform", "notebook-content.py"):
            path = notebook_dir / name
            if (path.relative_to(repo_root).as_posix() not in committed_orchestration
                    or path.is_symlink() or notebook_dir.is_symlink()):
                fail(f"{notebook_dir.name}/{name} must be a tracked regular file")
        sources[notebook_dir] = (notebook_dir / "notebook-content.py").read_bytes()
        if not sources[notebook_dir]:
            fail(f"{notebook_dir.name}/notebook-content.py is empty")
    dbt_notebooks = {logical_by_dir[d] for d, source in sources.items()
                     if logical_by_dir[d] in invoked_notebooks and DBT_JOB_MARKER in source}
    check_dbt_job_activities(pipeline_contents.values(), dbt_notebooks)

    has_runner = bool(invoked_notebooks - dbt_notebooks)
    ingestion_files = (publish_managed_requirements(repo_root, gather_code(repo_root, "ingestion"))
                       if has_runner else {})
    targets = ingestion_targets(pipeline_contents.values(), ingestion_files, dbt_notebooks)
    if len(targets) > 1:
        fail("one deployment must use one shared ingestion DOMAIN_SLUG")
    deliveries = []  # (folder, slug, files)
    if targets:
        preflight_dlt_config(ingestion_files, args.workspace_id)
        deliveries.append(("ingestion", targets[0], ingestion_files))
    lakehouse = None
    if invoked_notebooks:
        lakehouse, domain_slug = code_lakehouse(repo_root, args.workspace_id)
    if dbt_notebooks:
        transformation_files = gather_code(repo_root, "transformation")
        if PROD_MANIFEST in transformation_files:
            fail(f"{PROD_MANIFEST} is written by the deployer, never committed")
        check_prod_profile(transformation_files)
        transformation_files[PROD_MANIFEST] = parse_prod_manifest(repo_root)
        deliveries.append(("transformation", domain_slug, transformation_files))
    planned = []
    for folder, slug, files in deliveries:
        try:
            root = ingestion_release.delivery_root(lakehouse["id"], slug, folder)
            ingestion_release.plan_delivery(root, files, commit, folder)
        except ValueError as exc:
            fail(f"cannot deliver {folder} under Lakehouse {lakehouse['id']!r}: {exc}")
        planned.append((folder, root, files))

    notebook_definitions = {}
    for notebook_dir, source in sources.items():
        logical = logical_by_dir[notebook_dir]
        if logical in invoked_notebooks:
            if logical not in dbt_notebooks and hashlib.sha256(source).hexdigest() != SUPPORTED_INGESTION_RUNNER:
                fail(f"{notebook_dir.name} is neither the supported dlt runner nor a dbt job notebook; "
                     f"refresh the runner with vd-data-engineering {SUPPORTED_RUNNER_REVISION}'s "
                     "materialize-runner.py <name> --engine dlt --platform lakehouse --out orchestration --force")
            source = attach_default_lakehouse(source.decode("utf-8"), lakehouse["id"],
                                              lakehouse["displayName"], args.workspace_id).encode("utf-8")
        notebook_definitions[notebook_dir] = {
            **item_parts_payload({"notebook-content.py": source}), "format": "fabricGitSource"}

    for folder, root, files in planned:
        if args.dry_run:
            log(f"would deliver {len(files)} {folder} file(s) to {root}")
            continue
        try:
            written = ingestion_release.deliver(args.workspace_id, root, files, commit, folder)
        except (RuntimeError, FileExistsError, FileNotFoundError) as exc:
            # The transport's own failure types; anything else is a defect and keeps its traceback.
            fail(f"{folder} delivery to {root} failed before any item was applied ({exc}). "
                 "The folder may be partly written and an already-scheduled run would read it; "
                 "re-run deploy-orchestration to replace it.")
        log(f"delivered {written} {folder} file(s) to {root} at {commit[:12]}")

    for notebook_dir, definition in notebook_definitions.items():
        name = notebook_dir.name[:-len(".Notebook")]
        if args.dry_run:
            log(f"{name}: would apply Notebook")
            continue
        item_id = apply_item(args.workspace_id, name, "Notebook", "Notebook", definition)
        logical_to_object[logical_by_dir[notebook_dir]]["id"] = item_id

    for pipe_dir in pipelines:
        name = pipe_dir.name[: -len(".DataPipeline")]
        bound_content, bound = bind_pipeline(pipeline_contents[pipe_dir], logical_to_object, args.workspace_id)
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

    log("done — applying never triggers a run; cadence fires only from a reconciled schedule")


if __name__ == "__main__":
    main()
