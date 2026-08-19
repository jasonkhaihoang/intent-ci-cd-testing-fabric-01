"""Thin shell: load reports, render, post PR comments.

All formatting logic lives in notify_render. This module owns I/O only:
reading JSON reports from reports/ and calling pr_comment.upsert.

Backward-compatible re-exports preserve existing test_notify.py imports.
"""

import json
import os

try:
    from scripts import pr_comment
    from scripts.notify_render import (
        COMMENT_MARKER,
        DETAILS_COMMENT_MARKER,
        ReportBundle,
        _collapsible_build_empty,
        _collapsible_compile,
        _collapsible_gitleaks,
        _collapsible_ruff,
        _collapsible_schema_gate,
        _collapsible_scorecard,
        _collapsible_sqlfluff,
        _detail_build_empty,
        _detail_compile,
        _detail_gitleaks,
        _detail_ruff,
        _detail_schema_gate,
        _detail_scorecard,
        _detail_sqlfluff,
        render_details_comment,
        render_gate_0,
        render_gate_2,
        render_gate_3,
        render_gate_4,
        render_gate_5,
        render_gate_5_comment,
        GATE_5_MARKER,
        render_preflight_comment,
        PREFLIGHT_MARKER,
        render_gitleaks,
        render_provision_failed,
        render_ruff,
        render_scorecard,
        _render_shortcut_seeding as _render_shortcut_seeding_impl,
        render_sqlfluff,
        render_workspace_comment,
    )
except ImportError:
    import pr_comment
    from notify_render import (
        COMMENT_MARKER,
        DETAILS_COMMENT_MARKER,
        ReportBundle,
        _collapsible_build_empty,  # noqa: F401
        _collapsible_compile,  # noqa: F401
        _collapsible_gitleaks,  # noqa: F401
        _collapsible_ruff,  # noqa: F401
        _collapsible_schema_gate,  # noqa: F401
        _collapsible_scorecard,  # noqa: F401
        _collapsible_sqlfluff,  # noqa: F401
        _detail_build_empty,  # noqa: F401
        _detail_compile,  # noqa: F401
        _detail_gitleaks,  # noqa: F401
        _detail_ruff,  # noqa: F401
        _detail_schema_gate,  # noqa: F401
        _detail_scorecard,  # noqa: F401
        _detail_sqlfluff,  # noqa: F401
        render_details_comment,
        render_gate_0,
        render_gate_2,
        render_gate_3,
        render_gate_4,
        render_gate_5,
        render_gate_5_comment,  # noqa: F401
        GATE_5_MARKER,  # noqa: F401
        render_preflight_comment,  # noqa: F401
        PREFLIGHT_MARKER,  # noqa: F401
        render_gitleaks,
        render_provision_failed,
        render_ruff,
        render_scorecard,
        _render_shortcut_seeding as _render_shortcut_seeding_impl,
        render_sqlfluff,
        render_workspace_comment,
    )


# ─── backward-compat aliases (test_notify.py imports these names) ─────────────

format_ruff = render_ruff
format_sqlfluff = render_sqlfluff
format_gitleaks = render_gitleaks
format_scorecard_section = render_scorecard
format_gate_0 = render_gate_0
format_gate_2 = render_gate_2
format_gate_3 = render_gate_3
format_gate_4 = render_gate_4
format_gate_5 = render_gate_5
build_comment = render_workspace_comment
format_shortcut_seeding = _render_shortcut_seeding_impl
format_preflight = render_preflight_comment


def build_details_comment(
    ruff=None,
    sqlfluff=None,
    gitleaks=None,
    scorecard=None,
    compile_result=None,
    schema_gate=None,
    shortcut_seeding=None,
    gate_2=None,
    gate_3=None,
    gate_4=None,
) -> str:
    bundle = ReportBundle(
        ruff=ruff if ruff is not None else [],
        sqlfluff=sqlfluff if sqlfluff is not None else {},
        gitleaks=gitleaks if gitleaks is not None else {},
        scorecard=scorecard if scorecard is not None else {},
        compile_result=compile_result,
        schema_gate=schema_gate,
        shortcut_seeding=shortcut_seeding,
        gate_2=gate_2,
        gate_3=gate_3,
        gate_4=gate_4,
    )
    return render_details_comment(bundle)


# ─── I/O helpers ──────────────────────────────────────────────────────────────

def load_report(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def main():
    # Deprecated: superseded by per-gate comments posted directly from each CI job.
    # The notify CI job has been removed. This function is retained only for
    # backward compatibility with any external callers; do not invoke from CI.
    workspace_id = os.environ.get("EPHEMERAL_WORKSPACE_ID", "")
    workspace_name = os.environ.get("EPHEMERAL_WORKSPACE_NAME", "")
    head_branch = os.environ.get("HEAD_BRANCH", "")
    pr_number = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("REPO", "")
    greenfield_fallback = os.environ.get("GREENFIELD_FALLBACK", "").lower() == "true"

    ruff = load_report("reports/ruff.json")
    sqlfluff_report = load_report("reports/sqlfluff.json")
    gitleaks = load_report("reports/gitleaks.json")
    scorecard = load_report("reports/scorecard.json")
    compile_result = load_report("reports/dbt_compile.json")
    schema_gate = load_report("reports/schema_gate.json")
    shortcut_seeding = load_report("reports/shortcut_seeding.json")
    gate_2 = load_report("reports/gate-2.json") or None
    gate_3 = load_report("reports/gate-3.json") or None
    gate_4 = load_report("reports/gate-4.json") or None

    workspace_comment = build_comment(workspace_id, workspace_name, head_branch, greenfield_fallback)
    details_comment = build_details_comment(
        ruff=ruff,
        sqlfluff=sqlfluff_report,
        gitleaks=gitleaks,
        scorecard=scorecard,
        compile_result=compile_result,
        schema_gate=schema_gate,
        shortcut_seeding=shortcut_seeding,
        gate_2=gate_2,
        gate_3=gate_3,
        gate_4=gate_4,
    )

    pr_comment.upsert(COMMENT_MARKER, workspace_comment, pr_number, repo)
    print("Workspace PR comment posted.", flush=True)

    pr_comment.upsert(DETAILS_COMMENT_MARKER, details_comment, pr_number, repo)
    print("Static analysis details PR comment posted.", flush=True)


def post_workspace_comment_only():
    """Post only the workspace comment — used by the provision job step."""
    workspace_id = os.environ.get("EPHEMERAL_WORKSPACE_ID", "")
    workspace_name = os.environ.get("EPHEMERAL_WORKSPACE_NAME", "")
    head_branch = os.environ.get("HEAD_BRANCH", "")
    pr_number = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("REPO", "")
    greenfield_fallback = os.environ.get("GREENFIELD_FALLBACK", "").lower() == "true"
    provision_outcome = os.environ.get("PROVISION_OUTCOME", "success")
    provision_failed = provision_outcome != "success"
    run_url = os.environ.get("RUN_URL", "")

    interactive_notebook_id = os.environ.get("INTERACTIVE_NOTEBOOK_ID", "")
    notebook_url = ""
    if interactive_notebook_id and workspace_id:
        notebook_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}/synapsenotebooks/{interactive_notebook_id}?experience=fabric-developer"

    if provision_failed:
        workspace_comment = render_provision_failed(
            workspace_name=workspace_name,
            workspace_id=workspace_id,
            head_branch=head_branch,
            run_url=run_url,
        )
    else:
        workspace_comment = build_comment(
            workspace_id, workspace_name, head_branch, greenfield_fallback,
            notebook_url=notebook_url,
        )
        shortcut_seeding = load_report("reports/shortcut_seeding.json")
        shortcut_section = format_shortcut_seeding(shortcut_seeding)
        if shortcut_section:
            workspace_comment = workspace_comment.rstrip("\n") + "\n\n" + shortcut_section

    pr_comment.upsert(COMMENT_MARKER, workspace_comment, pr_number, repo)
    print("Workspace PR comment posted.", flush=True)


if __name__ == "__main__":
    import sys
    if "--workspace-comment-only" in sys.argv:
        post_workspace_comment_only()
    else:
        main()
