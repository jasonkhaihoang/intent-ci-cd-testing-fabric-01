"""
Fabric REST API wrapper for ephemeral workspace lifecycle management.

Commands:
  provision        --name NAME
                   Find or create workspace + lakehouse. Writes IDs to GITHUB_OUTPUT.

  teardown         --name NAME
                   Find workspace by name and delete it. Exits cleanly if not found.

  cleanup          --repo OWNER/REPO
                   List all vibedata_ephemeral_* workspaces. Delete those whose PR is closed.

  add-contributor  --workspace-id ID --github-login LOGIN --pr-number N --repo OWNER/REPO
                   Add the PR author as Admin via the Power BI REST API.
                   UPN resolution order:
                     1. Trusted PR-metadata UPN — a `<!-- entra-upn: {upn} -->` comment
                        posted by TRUSTED_PR_METADATA_AUTHOR (the Domain's GitHub App
                        installation, per studio VD-3764 — NOT the PR-opening per-user
                        credential, which provides no trust boundary). A marker-matching
                        comment from any other account is not eligible and is treated as
                        absent.
                     2. ENTRA_ID_UPN_OVERRIDE env var — used as-is (dev/test escape hatch for
                        accounts whose GitHub login does not match their AAD UPN prefix).
                     3. {github_login}@{ENTRA_ID_DOMAIN} — correct in production where GitHub
                        SAML SSO enforces that the GitHub login equals the AAD UPN prefix.
                   Warns and continues if the user is not found; never blocks CI.

Authentication: GitHub OIDC via azure/login. No SPN credentials stored.
The workflow runs azure/login before invoking this script, establishing an
Azure CLI session. Token is acquired via: az account get-access-token.

Required env var:
  AZURE_KEYVAULT_URL — used by kv_utils to fetch vibedata-fabric-capacity-id

Optional env vars (add-contributor):
  ENTRA_ID_DOMAIN        — UPN suffix (default: eng.acceleratedata.ai)
  ENTRA_ID_UPN_OVERRIDE  — Use this UPN directly, bypassing {github_login}@{ENTRA_ID_DOMAIN}
                      construction. Useful when GitHub login does not match AAD UPN
                      prefix (e.g. personal GitHub accounts not provisioned via SSO).
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import yaml

try:
    from scripts import fabric_transport
    from scripts import pr_comment
    from scripts import runner_io
    from scripts import shortcut_seeding_report
except ImportError:  # invoked as `python3 path/to/fabric_api.py`
    import fabric_transport
    import pr_comment
    import runner_io
    import shortcut_seeding_report


GITHUB_API = "https://api.github.com"
ONELAKE_DFS = os.environ.get("ONELAKE_DFS_BASE_URL", "https://onelake.dfs.fabric.microsoft.com")

ENTRA_ID_DOMAIN_DEFAULT = "eng.acceleratedata.ai"

ENTRA_UPN_MARKER = "<!-- entra-upn:"
_ENTRA_UPN_RE = re.compile(r"<!--\s*entra-upn:\s*(\S+)\s*-->")

# Placeholder — must equal the login the Domain's GitHub App installation posts
# under (studio VD-3764, not yet implemented). Never the PR author's own login:
# any u2m-minted (per-user) credential provides no trust boundary here — see
# Assumption 1 in the VD-3760 implementation plan.
TRUSTED_PR_METADATA_AUTHOR = "vibedata-pr-bot"


def extract_entra_upn(comment_body: str | None) -> str | None:
    """Parse the Entra UPN out of a trusted `<!-- entra-upn: {upn} -->` comment body.

    Returns None if `comment_body` is None or the marker is absent/malformed.
    """
    if comment_body is None:
        return None
    match = _ENTRA_UPN_RE.search(comment_body)
    return match.group(1) if match else None


# ─── Workspace helpers ─────────────────────────────────────────────────────────

def find_workspace_by_name(name: str) -> dict | None:
    resp = fabric_transport.request("GET", "/workspaces")
    for ws in resp.get("value", []):
        if ws["displayName"] == name:
            return ws
    return None


def find_lakehouse_by_name(workspace_id: str, name: str) -> dict | None:
    resp = fabric_transport.request("GET", f"/workspaces/{workspace_id}/items")
    for item in resp.get("value", []):
        if item["type"] == "Lakehouse" and item["displayName"] == name:
            return item
    return None


# ─── Admin role helper ───────────────────────────────────────────────────────

def add_workspace_user(workspace_id: str, upn: str, role: str = "Admin"):
    """Add or update a user on the workspace via the Power BI REST API.

    Tries PUT first (updates an existing user's role); falls back to POST (adds a
    new user) when PUT returns 404. Both calls are non-blocking: any other error
    logs a warning and allows provisioning to continue.
    """
    payload = {"identifier": upn, "groupUserAccessRight": role, "principalType": "User"}
    for method in ("PUT", "POST"):
        try:
            fabric_transport.request(
                method, f"/groups/{workspace_id}/users", payload, audience="powerbi",
            )
            print(f"Added '{upn}' as {role} on workspace {workspace_id} (via {method}).", flush=True)
            return
        except urllib.error.HTTPError as e:
            body_text = e.read().decode(errors="replace")
            if method == "PUT" and e.code == 404:
                continue  # user not yet in workspace — fall through to POST
            print(
                f"Warning: could not add '{upn}' as {role} via {method} (HTTP {e.code}): {body_text}. "
                "Skipping — provisioning continues.",
                flush=True,
            )
            return


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_provision(args):
    capacity_id = os.environ["FABRIC_CAPACITY_ID"]  # written to GITHUB_ENV by kv_utils fetch-fabric
    name = args.name

    ws = find_workspace_by_name(name)
    if ws:
        workspace_id = ws["id"]
        print(f"Reusing existing workspace: {name} ({workspace_id})", flush=True)
    else:
        print(f"Creating workspace: {name}", flush=True)
        ws = fabric_transport.request("POST", "/workspaces", {
            "displayName": name,
            "capacityId": capacity_id,
        })
        workspace_id = ws["id"]
        print(f"Workspace created: {workspace_id}", flush=True)

    lakehouse_name = os.environ.get("EPHEMERAL_LAKEHOUSE_NAME", "vdephelh")

    lh = find_lakehouse_by_name(workspace_id, lakehouse_name)
    if lh:
        lakehouse_id = lh["id"]
        print(f"Reusing existing lakehouse: {lakehouse_name} ({lakehouse_id})", flush=True)
    else:
        print(f"Creating lakehouse: {lakehouse_name}", flush=True)
        lh = fabric_transport.request("POST", f"/workspaces/{workspace_id}/items", {
            "displayName": lakehouse_name,
            "type": "Lakehouse",
            "creationPayload": {"enableSchemas": True},
        })
        lakehouse_id = lh["id"]
        print(f"Lakehouse created: {lakehouse_id}", flush=True)

    runner_io.set_output("workspace_id", workspace_id)
    runner_io.set_output("lakehouse_id", lakehouse_id)
    runner_io.set_output("lakehouse_name", lakehouse_name)
    print(f"Provision complete: workspace={workspace_id} lakehouse={lakehouse_id} ({lakehouse_name})", flush=True)


def cmd_teardown(args):
    ws = find_workspace_by_name(args.name)
    if not ws:
        print(f"Workspace not found: {args.name} — nothing to teardown.", flush=True)
        return
    workspace_id = ws["id"]
    print(f"Deleting workspace: {args.name} ({workspace_id})", flush=True)
    fabric_transport.request("DELETE", f"/workspaces/{workspace_id}")
    print("Workspace deleted.", flush=True)



def workspace_decision(pr_state, target_pr_number, pr_number):
    """Pure decision function for a single workspace.

    Returns (reason, should_delete) where reason is one of:
      "skip"     - not the targeted PR (targeted mode only)
      "orphaned" - PR not found on GitHub
      "closed"   - PR is closed or merged
      "active"   - open PR (always retained regardless of commit age)
    """
    if target_pr_number is not None and str(pr_number) != str(target_pr_number):
        return "skip", False
    if pr_state == "not_found":
        return "orphaned", True
    if pr_state == "api_error":
        return "active", False  # can't determine state; keep safe
    if pr_state == "closed":
        return "closed", True
    # open PR — retained regardless of commit age
    return "active", False


def _fetch_pr_info(repo, pr_number, gh_token):
    """Return (state, head_sha). state is 'open', 'closed', 'not_found', or 'api_error'."""
    url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {gh_token}")
    req.add_header("Accept", "application/vnd.github+json")
    try:
        with urllib.request.urlopen(req) as r:
            data = json.loads(r.read())
            return data.get("state", "unknown"), data.get("head", {}).get("sha")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "not_found", None
        return "api_error", None


def cmd_cleanup(args):
    gh_token = os.environ.get("GH_TOKEN", "")
    repo = args.repo
    target_pr = getattr(args, "pr_number", None)

    resp = fabric_transport.request("GET", "/workspaces")
    ephemeral = [
        ws for ws in resp.get("value", [])
        if ws["displayName"].startswith("vibedata_ephemeral_")
    ]
    print(f"Found {len(ephemeral)} ephemeral workspace(s).", flush=True)

    audit_log = []
    has_failure = False

    for ws in ephemeral:
        name = ws["displayName"]
        parts = name.split("_")
        if len(parts) < 3 or not parts[-1].isdigit():
            continue
        pr_number = parts[-1]

        pr_state, _head_sha = _fetch_pr_info(repo, pr_number, gh_token)

        reason, should_delete = workspace_decision(pr_state, target_pr, pr_number)

        if reason == "skip":
            continue

        if should_delete:
            try:
                fabric_transport.request("DELETE", f"/workspaces/{ws['id']}")
                outcome = "deleted"
            except Exception as exc:
                print(f"  Failed to delete {name}: {exc}", file=sys.stderr)
                outcome = "failed"
                has_failure = True
        else:
            outcome = "kept"

        print(f"  {name} (PR #{pr_number}): {reason} -> {outcome}", flush=True)
        audit_log.append(
            {"workspace": name, "pr": pr_number, "reason": reason, "outcome": outcome}
        )

    with open("workspace-cleanup-audit.json", "w") as f:
        json.dump(audit_log, f, indent=2)
    print(f"Audit log written ({len(audit_log)} entries).", flush=True)

    if has_failure:
        sys.exit(1)


def resolve_upn(github_login: str, entra_id_domain: str, override: str, metadata_upn: str | None) -> str:
    """Pure UPN precedence: trusted PR-metadata UPN > ENTRA_ID_UPN_OVERRIDE > constructed.

    `entra_id_domain` must already be resolved by the caller (env var or ENTRA_ID_DOMAIN_DEFAULT).
    """
    if metadata_upn:
        return metadata_upn
    if override:
        return override
    return f"{github_login}@{entra_id_domain}"


def cmd_add_contributor(args):
    """Add the PR author as Admin on the ephemeral workspace.

    UPN resolution order:
      1. Trusted PR-metadata UPN — a `<!-- entra-upn: {upn} -->` comment posted by
         TRUSTED_PR_METADATA_AUTHOR (the Domain's GitHub App installation, per
         studio VD-3764 — NOT the PR-opening per-user credential, which provides
         no trust boundary since it authenticates as the PR author themselves).
         A marker-matching comment from any other account is not eligible and is
         treated as absent.
      2. ENTRA_ID_UPN_OVERRIDE env var — used as-is (dev/test escape hatch for accounts
         whose GitHub login does not match their AAD UPN prefix, e.g. personal GitHub
         accounts joined via Google OAuth rather than SAML SSO).
      3. {github_login}@{ENTRA_ID_DOMAIN} — correct in production where GitHub SAML SSO
         enforces that the GitHub login equals the AAD UPN prefix.
    """
    comment_body = pr_comment.find_trusted_comment(
        ENTRA_UPN_MARKER, args.pr_number, args.repo, TRUSTED_PR_METADATA_AUTHOR
    )
    metadata_upn = extract_entra_upn(comment_body)

    override = os.environ.get("ENTRA_ID_UPN_OVERRIDE", "").strip()
    entra_id_domain = os.environ.get("ENTRA_ID_DOMAIN", "").strip() or ENTRA_ID_DOMAIN_DEFAULT

    upn = resolve_upn(args.github_login, entra_id_domain, override, metadata_upn)

    if metadata_upn:
        print(f"Using trusted PR-metadata UPN: {upn}", flush=True)
    elif override:
        print(f"Using ENTRA_ID_UPN_OVERRIDE: {upn}", flush=True)
    else:
        print(f"Constructed UPN: {upn}", flush=True)

    add_workspace_user(args.workspace_id, upn, role=args.role)


# ─── OneLake Files upload ─────────────────────────────────────────────────────

def upload_onelake_file(
    workspace_id: str, lakehouse_id: str, local_path: str, remote_path: str
) -> str:
    """Upload a local file to OneLake via the ADLS Gen2 DFS three-step protocol.

    Steps: CREATE (PUT ?resource=file) → APPEND → FLUSH.
    Returns the ABFSS URI of the uploaded file.
    """
    if ".." in remote_path.split("/"):
        raise ValueError(f"remote_path must not contain '..' components: {remote_path!r}")

    url = f"{ONELAKE_DFS}/{workspace_id}/{lakehouse_id}/{remote_path}"

    with open(local_path, "rb") as f:
        data = f.read()
    size = len(data)

    fabric_transport.dfs_request("PUT", url, params={"resource": "file"})
    print(f"OneLake file path created: {remote_path}", flush=True)

    fabric_transport.dfs_request("PATCH", url, data=data, params={"action": "append", "position": "0"})
    print(f"Data appended ({size} bytes).", flush=True)

    fabric_transport.dfs_request("PATCH", url, params={"action": "flush", "position": str(size)})
    print("File flushed and committed.", flush=True)

    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{remote_path}"


def cmd_upload_file(args):
    abfss = upload_onelake_file(
        args.workspace_id, args.lakehouse_id, args.local_path, args.remote_path
    )
    runner_io.set_output("abfss_path", abfss)
    print(f"ABFSS URI: {abfss}", flush=True)


# ─── Shortcut seeding ─────────────────────────────────────────────────────────


def _build_shortcut_body(entry: dict) -> dict:
    """Pure: derive the Fabric Shortcuts API request body from a manifest entry.

    source_path is e.g. "Tables/raw/opportunity" — the shortcut must land at the
    same schema-qualified path in the ephemeral lakehouse so dbt can resolve
    raw.opportunity with lakehouse_schemas_enabled: true.
    """
    parts = entry["source_path"].rsplit("/", 1)
    path = parts[0] if len(parts) == 2 else "Tables"
    name = parts[-1]
    return {
        "name": name,
        "path": path,
        "target": {
            "oneLake": {
                "workspaceId": entry["source_workspace_id"],
                "itemId": entry["source_lakehouse_id"],
                "path": entry["source_path"],
            }
        },
    }


def seed_shortcuts(
    from_file: str,
    workspace_id: str,
    lakehouse_id: str,
    report_path: str = shortcut_seeding_report.DEFAULT_PATH,
) -> None:
    """Read derived shortcuts list and POST each to the Fabric Shortcuts API.

    Per-entry behaviour:
      * 201 Created → success counted toward `created`.
      * 409 Conflict → idempotent; counted toward `already_existed`.
      * 403 Forbidden → SystemExit with alias + source path + Viewer-on-prod hint.
      * 404 Not Found → SystemExit with alias + source path; halts further entries.
      * Other errors (incl. 500 after fabric_transport retry exhaustion) → SystemExit
        with the alias.

    Updates `report_path` with merged `{created, already_existed}` counts; preserves
    `derived`/`zero_state` keys written by Slice 1 (read-modify-write).
    """
    with open(from_file) as f:
        entries = json.load(f)

    if not entries:
        print("No shortcuts to seed", flush=True)
        return

    created = 0
    already_existed = 0
    api_path = f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"

    for entry in entries:
        body = _build_shortcut_body(entry)
        try:
            fabric_transport.request("POST", api_path, body)
            created += 1
            print(f"Created shortcut: {entry['alias']}", flush=True)
        except urllib.error.HTTPError as e:
            if e.code == 409:
                already_existed += 1
                print(f"Shortcut already exists: {entry['alias']}", flush=True)
                continue
            if e.code == 403:
                raise SystemExit(
                    f"Forbidden creating shortcut '{entry['alias']}' for source "
                    f"'{entry['source_path']}'. The Fabric UAMI must have the "
                    "Viewer role on the production workspace."
                )
            if e.code == 404:
                raise SystemExit(
                    f"Source not found for shortcut '{entry['alias']}': "
                    f"'{entry['source_path']}'."
                )
            raise SystemExit(
                f"Failed creating shortcut '{entry['alias']}' (HTTP {e.code})."
            )

    shortcut_seeding_report.set_seeding(created, already_existed, path=report_path)


def cmd_seed_shortcuts(args):
    seed_shortcuts(args.from_file, args.workspace_id, args.lakehouse_id)


# ─── Cancel in-flight jobs ────────────────────────────────────────────────────

def cancel_running_jobs(workspace_name: str) -> None:
    """Cancel all InProgress/NotStarted Fabric notebook Item Jobs in the workspace.

    No-op when the workspace does not exist (first push — workspace not yet created).
    404 on cancel is treated as success (job already reached a terminal state).
    Any other HTTP error propagates, causing provision to fail.
    """
    ws = find_workspace_by_name(workspace_name)
    if ws is None:
        print(f"Workspace '{workspace_name}' not found — no jobs to cancel.", flush=True)
        return

    workspace_id = ws["id"]
    resp = fabric_transport.request("GET", f"/workspaces/{workspace_id}/items")
    notebooks = [item for item in resp.get("value", []) if item.get("type") == "Notebook"]
    print(f"Found {len(notebooks)} notebook(s) in workspace.", flush=True)

    cancelled = 0
    for notebook in notebooks:
        notebook_id = notebook["id"]
        notebook_name = notebook.get("displayName", notebook_id)
        jobs_resp = fabric_transport.request(
            "GET", f"/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances"
        )
        active_jobs = [
            j for j in jobs_resp.get("value", [])
            if j.get("status") in ("InProgress", "NotStarted")
        ]
        for job in active_jobs:
            job_id = job["id"]
            try:
                fabric_transport.request(
                    "POST",
                    f"/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}/cancel",
                )
                cancelled += 1
                print(f"Cancelled job {job_id} on notebook '{notebook_name}'.", flush=True)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print(
                        f"Job {job_id} already terminal (404) — treating as success.", flush=True
                    )
                else:
                    raise

    print(f"cancel-running-jobs complete: {cancelled} job(s) cancelled.", flush=True)


def cmd_cancel_running_jobs(args):
    cancel_running_jobs(args.name)


def _find_existing_environment(workspace_id: str, env_name: str) -> str | None:
    """Return item ID of an existing environment with the given display name, or None."""
    resp = fabric_transport.request("GET", f"/workspaces/{workspace_id}/environments")
    for item in resp.get("value", []):
        if item.get("displayName") == env_name:
            return item["id"]
    return None


def _translate_keys(source: dict, key_map: dict) -> dict:
    """Rename keys present in `source` per `key_map` (snake_case -> camelCase); others dropped."""
    return {camel: source[snake] for snake, camel in key_map.items() if snake in source}


def _load_spark_compute_overrides(ci_config_path: str) -> dict:
    """Read the optional `spark_compute` stanza from a domain's ci-config.yml (AC-50).

    Translates its snake_case keys to the camelCase fields the Fabric
    `staging/sparkcompute` endpoint expects. Only keys actually present in
    `spark_compute` are included — no defaults are injected here.
    """
    with open(ci_config_path) as f:
        ci_config = yaml.safe_load(f) or {}

    spark_compute = ci_config.get("VD_DOMAIN_CI_SPARK_COMPUTE") or {}
    if not isinstance(spark_compute, dict):
        raise ValueError(
            f"ci-config.yml 'VD_DOMAIN_CI_SPARK_COMPUTE' must be a mapping, got {type(spark_compute).__name__}"
        )

    overrides = _translate_keys(spark_compute, {
        "driver_cores": "driverCores",
        "driver_memory": "driverMemory",
        "executor_cores": "executorCores",
        "executor_memory": "executorMemory",
    })

    if "dynamic_executor_allocation" in spark_compute:
        dea = spark_compute["dynamic_executor_allocation"]
        if not isinstance(dea, dict):
            raise ValueError(
                f"ci-config.yml 'spark_compute.dynamic_executor_allocation' must be a mapping, "
                f"got {type(dea).__name__}"
            )
        overrides["dynamicExecutorAllocation"] = _translate_keys(dea, {
            "enabled": "enabled",
            "min_executors": "minExecutors",
            "max_executors": "maxExecutors",
        })

    return overrides


def create_environment(workspace_id: str, config_path: str, ci_config_path: str | None = None) -> str:
    """Create or reuse a Fabric Environment from a YAML definition and upload pip packages.

    Returns the environment ID. Emits environment_id and environment_name to GITHUB_OUTPUT.
    Idempotent: if an environment with the same name already exists, reuses it.

    Spark compute (AC-50) is domain-controlled: the instance pool is always Fabric's
    built-in Starter Pool (not configurable), and driver/executor sizing is optionally
    overridden from `ci_config_path`'s `spark_compute` stanza.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    env_name = config["name"]
    runtime_version = config.get("runtime_version", "1.3")

    # Create or reuse the environment
    env_id = _find_existing_environment(workspace_id, env_name)
    reused_existing = env_id is not None
    if reused_existing:
        print(f"Reusing existing environment '{env_name}' ({env_id})", flush=True)
    else:
        resp = fabric_transport.request(
            "POST",
            f"/workspaces/{workspace_id}/environments",
            {"displayName": env_name},
        )
        env_id = resp["id"]
        print(f"Created environment '{env_name}' ({env_id})", flush=True)

    # Always configure spark compute against the fixed Starter Pool, merging in
    # any domain-supplied driver/executor overrides (AC-50).
    # Skip only when REUSING an environment that was already published on a prior
    # push to this same PR — re-PATCHing staging on a published environment triggers
    # full pool-size validation and fails on constrained capacities (e.g. F4's Starter
    # Pool cap). A freshly created environment always gets PATCHed unconditionally:
    # Fabric reports publishDetails.state == "Success" on a brand-new environment
    # even though nothing has been staged yet (nothing pending == trivially "in sync"),
    # so checking that state on a fresh environment would wrongly skip its first-ever
    # configuration.
    already_published = False
    if reused_existing:
        env_detail = fabric_transport.request(
            "GET", f"/workspaces/{workspace_id}/environments/{env_id}"
        )
        publish_state = (
            env_detail.get("properties", {})
            .get("publishDetails", {})
            .get("state", "")
        )
        already_published = publish_state == "Success"

    if already_published:
        print(
            "Environment already published (state: Success) — "
            "skipping sparkcompute reconfigure.",
            flush=True,
        )
    else:
        sparkcompute_body = {
            "instancePool": {"name": "Starter Pool", "type": "Workspace"},
            "runtimeVersion": runtime_version,
        }
        if ci_config_path:
            sparkcompute_body.update(_load_spark_compute_overrides(ci_config_path))
        fabric_transport.request(
            "PATCH",
            f"/workspaces/{workspace_id}/environments/{env_id}/staging/sparkcompute",
            sparkcompute_body,
        )
        print(f"Configured spark compute: {sparkcompute_body}", flush=True)

    # Upload pip packages to staging/libraries — API requires multipart/form-data environment.yml
    pip_packages = config.get("pip_packages", [])
    if pip_packages:
        pip_lines = "\n".join(f"    - {pkg}" for pkg in pip_packages)
        env_yml = f"dependencies:\n  - pip:\n{pip_lines}\n".encode()
        fabric_transport.request_multipart(
            "POST",
            f"/workspaces/{workspace_id}/environments/{env_id}/staging/libraries",
            file_content=env_yml,
            filename="environment.yml",
        )
        print(f"Uploaded pip packages: {pip_packages}", flush=True)

    runner_io.set_output("environment_id", env_id)
    runner_io.set_output("environment_name", env_name)
    runner_io.set_output("runtime_version", runtime_version)
    return env_id


def cmd_create_environment(args):
    create_environment(args.workspace_id, args.config, ci_config_path=args.ci_config)


def publish_environment(workspace_id: str, environment_id: str, poll_interval: int = 30) -> None:
    """Trigger Full Mode publish for the Fabric Environment and block until Succeeded.

    Full Mode publish pre-bakes libraries into the runtime (~10–15 min).
    Skips the publish POST when the environment is already in a 'Success' state.
    Raises RuntimeError if publish state is 'Failed'.
    """
    resp = fabric_transport.request(
        "GET",
        f"/workspaces/{workspace_id}/environments/{environment_id}",
    )
    current_state = resp.get("properties", {}).get("publishDetails", {}).get("state", "")
    if current_state == "Success":
        print("Environment already published — skipping publish.", flush=True)
        return

    fabric_transport.request(
        "POST",
        f"/workspaces/{workspace_id}/environments/{environment_id}/staging/publish",
    )
    print(f"Environment publish triggered. Polling every {poll_interval}s...", flush=True)

    while True:
        time.sleep(poll_interval)
        resp = fabric_transport.request(
            "GET",
            f"/workspaces/{workspace_id}/environments/{environment_id}",
        )
        state = resp.get("properties", {}).get("publishDetails", {}).get("state", "")
        print(f"  publish state: {state}", flush=True)
        if state == "Success":
            print("Environment published successfully.", flush=True)
            return
        if state in ("Failed", "Cancelled"):
            raise RuntimeError(
                f"Environment publish {state} for environment {environment_id}"
            )


def cmd_publish_environment(args):
    publish_environment(args.workspace_id, args.environment_id)


def set_workspace_default_environment(
    workspace_id: str, environment_name: str, runtime_version: str = "1.3"
) -> None:
    """Set the named Environment as the workspace's default Spark Environment.

    Uses the Environment name (not GUID) per the Fabric Workspace settings API.
    Requires Admin role on the workspace.
    """
    body = {"environment": {"name": environment_name, "runtimeVersion": runtime_version}}
    fabric_transport.request("PATCH", f"/workspaces/{workspace_id}/spark/settings", body)
    print(f"Workspace default environment set to '{environment_name}'.", flush=True)


def cmd_set_workspace_default_environment(args):
    set_workspace_default_environment(args.workspace_id, args.environment_name, args.runtime_version)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fabric ephemeral workspace CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("provision").add_argument("--name", required=True)
    sub.add_parser("teardown").add_argument("--name", required=True)
    p_cleanup = sub.add_parser("cleanup")
    p_cleanup.add_argument("--repo", required=True)
    p_cleanup.add_argument("--pr-number", default=None)
    p = sub.add_parser("add-contributor")
    p.add_argument("--workspace-id", required=True)
    p.add_argument("--github-login", required=True)
    p.add_argument("--pr-number", required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--role", choices=["Admin", "Member", "Contributor", "Viewer"], default="Admin")
    p2 = sub.add_parser("upload-file")
    p2.add_argument("--workspace-id", required=True)
    p2.add_argument("--lakehouse-id", required=True)
    p2.add_argument("--local-path", required=True)
    p2.add_argument("--remote-path", required=True)
    p3 = sub.add_parser("seed-shortcuts")
    p3.add_argument("--from-file", required=True)
    p3.add_argument("--workspace-id", required=True)
    p3.add_argument("--lakehouse-id", required=True)
    p4 = sub.add_parser("create-environment")
    p4.add_argument("--workspace-id", required=True)
    p4.add_argument("--config", required=True, help="Path to ephemeral-ci-environment.yaml")
    p4.add_argument(
        "--ci-config",
        default=None,
        help="Path to the domain's ci-config.yml; reads its optional spark_compute overrides (AC-50)",
    )
    p5 = sub.add_parser("publish-environment")
    p5.add_argument("--workspace-id", required=True)
    p5.add_argument("--environment-id", required=True)
    p6 = sub.add_parser("set-workspace-default-environment")
    p6.add_argument("--workspace-id", required=True)
    p6.add_argument("--environment-name", required=True)
    p6.add_argument("--runtime-version", default="1.3")
    p7 = sub.add_parser("cancel-running-jobs")
    p7.add_argument("--name", required=True)
    args = parser.parse_args()
    {
        "provision": cmd_provision,
        "teardown": cmd_teardown,
        "cleanup": cmd_cleanup,
        "add-contributor": cmd_add_contributor,
        "upload-file": cmd_upload_file,
        "seed-shortcuts": cmd_seed_shortcuts,
        "create-environment": cmd_create_environment,
        "publish-environment": cmd_publish_environment,
        "set-workspace-default-environment": cmd_set_workspace_default_environment,
        "cancel-running-jobs": cmd_cancel_running_jobs,
    }[args.command](args)


if __name__ == "__main__":
    main()
