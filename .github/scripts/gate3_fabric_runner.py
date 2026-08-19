"""Thin shell for Gate 3 (ci/unit-tests) on Fabric GHA runner (AC-11)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import emit_status
import runner_io
from fabric_runner_utils import select_names as _select_names, setup_defer as _setup_defer, write_gate_result as _write_gate_result
from parse_run_results import summarize

CONTEXT = "ci/unit-tests"
PROFILE = "dbt_fab_spark"
TARGET = "ephemeral_ci"


def _run_url() -> str:
    base = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    return f"{base}/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"


def _post(head_sha: str, state: str, description: str) -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if repo:
        emit_status.emit_status(repo, head_sha, CONTEXT, state, description, _run_url())


def cmd_run_gate(args) -> int:
    head_sha = args.head_sha
    _post(head_sha, "pending", "Gate 3: dbt unit tests running on Fabric runner")

    env = {
        **os.environ,
        "WORKSPACE_ID": args.workspace_id,
        "WORKSPACE_NAME": args.workspace_name,
        "LAKEHOUSE": args.lakehouse_name,
        "LAKEHOUSE_ID": args.lakehouse_id,
        "SCHEMA": args.schema,
        "SESSION_ID_FILE": f"Files/ci-artifacts/{head_sha}/livy-session-id-ci-unit-test.txt",
    }

    names = _select_names(args.deployment_manifest)
    profiles_dir = args.profiles_dir
    defer_args = _setup_defer(args.prod_state_dir)

    subprocess.run(
        ["dbt", "deps", "--project-dir", runner_io.project_dir(),
         "--profiles-dir", profiles_dir, "--profile", PROFILE,
         "--target", TARGET, "--quiet"],
        env=env,
    )

    if not names:
        result: dict = {
            "gate": "3", "head_sha": head_sha, "overall_status": "skipped",
            "total": 0, "counts": {"pass": 0, "fail": 0, "error": 0, "skip": 0},
            "failures": [], "tests": [], "modified_models": [],
        }
    else:
        # Space separates union selectors: each "{n},test_type:unit" is the AND
        # of that model with unit tests — mirrors the notebook's selector pattern.
        select_arg = " ".join(f"{n},test_type:unit" for n in names)
        subprocess.run([
            "dbt", "test", "--project-dir", runner_io.project_dir(),
            "--select", select_arg,
            "--profiles-dir", profiles_dir, "--profile", PROFILE,
            "--target", TARGET, "--target-path", "target/unit",
        ] + defer_args, env=env)

        run_results: dict | None = None
        try:
            with open("target/unit/run_results.json") as f:
                run_results = json.load(f)
        except (OSError, json.JSONDecodeError):
            pass

        if run_results is None:
            result = {
                "gate": "3", "head_sha": head_sha, "overall_status": "fail",
                "total": 0, "counts": {"pass": 0, "fail": 0, "error": 0, "skip": 0},
                "failures": [], "tests": [], "modified_models": names,
            }
        else:
            summary = summarize(run_results)
            if summary["total"] == 0:
                summary["overall_status"] = "skipped"
            summary.update({"gate": "3", "head_sha": head_sha, "modified_models": names})
            result = summary

    _write_gate_result(args.output, result)
    overall = result["overall_status"]
    gh_state = "success" if overall in ("pass", "skipped") else "failure"
    desc = "Gate 3: skipped — no unit tests found" if overall == "skipped" else f"Gate 3: {overall}"
    _post(head_sha, gh_state, desc)
    print(f"Gate 3 result: {overall} → GitHub status: {gh_state}", flush=True)
    return 0 if gh_state == "success" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate 3 (ci/unit-tests) — Fabric GHA runner")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run-gate")
    p.add_argument("--workspace-id", required=True)
    p.add_argument("--workspace-name", required=True)
    p.add_argument("--lakehouse-id", required=True)
    p.add_argument("--lakehouse-name", required=True)
    p.add_argument("--schema", default="dbo")
    p.add_argument("--head-sha", required=True)
    p.add_argument("--deployment-manifest", required=True)
    p.add_argument("--prod-state-dir", default="prod-state")
    p.add_argument("--profiles-dir", default=".github/profiles")
    p.add_argument("--output", default=None)
    args = parser.parse_args(argv)
    return {"run-gate": cmd_run_gate}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
