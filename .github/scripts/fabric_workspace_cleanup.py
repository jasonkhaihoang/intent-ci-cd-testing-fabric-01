"""Delete the ephemeral Fabric workspaces whose pull request is closed or gone.

Shared by both Fabric lanes: a Lakehouse PR and a Warehouse PR both provision a workspace
named `vibedata_ephemeral_<repo>_<pr>`, and the rule for when one may go is the same — so the
rule lives once. The Lakehouse bundle reaches it through `fabric_api.py cleanup`; the
Warehouse bundle's `workspace-cleanup.yml` invokes this module directly.

CLI:
    fabric_workspace_cleanup.py --repo OWNER/REPO [--pr-number N]

Authentication: the workflow's azure/login session (operator-managed CD identity, AC-31).
Writes `workspace-cleanup-audit.json`; exits 1 when any delete failed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

try:
    from scripts import fabric_transport
except ImportError:  # invoked as `python3 path/to/fabric_workspace_cleanup.py`
    import fabric_transport

GITHUB_API = "https://api.github.com"
EPHEMERAL_PREFIX = "vibedata_ephemeral_"
AUDIT_PATH = "workspace-cleanup-audit.json"


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


def fetch_pr_info(repo, pr_number, gh_token):
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


def repo_workspace_prefix(repo):
    """The name prefix of THIS repository's ephemeral workspaces.

    Matches how both Fabric `ci.yml` workflows name them —
    `vibedata_ephemeral_<repository name, '-' → '_'>_<pr>` — so a sweep never touches another
    repository's workspace that the operator identity can also see, even when the PR numbers
    collide: that PR would be looked up in this repository and read as closed or orphaned.
    """
    return f"{EPHEMERAL_PREFIX}{repo.split('/')[-1].replace('-', '_')}_"


def cleanup(repo, target_pr, gh_token) -> bool:
    """Delete every ephemeral workspace the decision allows; True when no delete failed."""
    prefix = repo_workspace_prefix(repo)
    resp = fabric_transport.request("GET", "/workspaces")
    ephemeral = [
        ws for ws in resp.get("value", [])
        if ws["displayName"].startswith(prefix) and ws["displayName"][len(prefix):].isdigit()
    ]
    print(f"Found {len(ephemeral)} ephemeral workspace(s) for {repo}.", flush=True)

    audit_log = []
    has_failure = False

    for ws in ephemeral:
        name = ws["displayName"]
        pr_number = name[len(prefix):]

        pr_state, _head_sha = fetch_pr_info(repo, pr_number, gh_token)

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

    with open(AUDIT_PATH, "w") as f:
        json.dump(audit_log, f, indent=2)
    print(f"Audit log written ({len(audit_log)} entries).", flush=True)

    return not has_failure


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Delete closed or orphaned ephemeral Fabric workspaces")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr-number", default=None)
    args = parser.parse_args(argv)
    return 0 if cleanup(args.repo, args.pr_number, os.environ.get("GH_TOKEN", "")) else 1


if __name__ == "__main__":
    sys.exit(main())
