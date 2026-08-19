"""Thin shell for Gate 4 (ci/data-tests) on Fabric GHA runner (AC-11)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

try:
    import yaml
except ImportError:
    yaml = None

import emit_status
import runner_io
from fabric_runner_utils import select_names as _select_names, setup_defer as _setup_defer, write_gate_result as _write_gate_result
from parse_run_results import check_store_failures_config, enrich_tests_from_manifest, parse_data_test_results

CONTEXT = "ci/data-tests"
PROFILE = "dbt_fab_spark"
TARGET = "ephemeral_ci"


def _run_url() -> str:
    base = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    return f"{base}/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"


def _post(head_sha: str, state: str, description: str) -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if repo:
        emit_status.emit_status(repo, head_sha, CONTEXT, state, description, _run_url())


def _load_manifest_nodes(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f).get("nodes", {})
    except (OSError, json.JSONDecodeError):
        return {}


def _load_dbt_project(path: str) -> dict:
    if yaml is None:
        return {}
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except OSError:
        return {}


def cmd_run_gate(args) -> int:
    head_sha = args.head_sha
    dbt_project = _load_dbt_project(args.dbt_project)
    store_failures_config_ok = check_store_failures_config(dbt_project)
    if not store_failures_config_ok:
        print("Advisory: dbt_project.yml missing store_failures config. Gate unaffected.", flush=True)

    _post(head_sha, "pending", "Gate 4: dbt data tests running on Fabric runner")

    env = {
        **os.environ,
        "WORKSPACE_ID": args.workspace_id,
        "WORKSPACE_NAME": args.workspace_name,
        "LAKEHOUSE": args.lakehouse_name,
        "LAKEHOUSE_ID": args.lakehouse_id,
        "SCHEMA": args.schema,
        "SESSION_ID_FILE": f"Files/ci-artifacts/{head_sha}/livy-session-id-ci-data-test.txt",
    }

    defer_args = _setup_defer(args.prod_state_dir)
    profiles_dir = args.profiles_dir
    names = _select_names(args.deployment_manifest)

    subprocess.run(
        ["dbt", "deps", "--project-dir", runner_io.project_dir(),
         "--profiles-dir", profiles_dir, "--profile", PROFILE,
         "--target", TARGET, "--quiet"],
        env=env,
    )

    cmd = [
        "dbt", "test", "--project-dir", runner_io.project_dir(),
        "--store-failures",
        "--profiles-dir", profiles_dir, "--profile", PROFILE,
        "--target", TARGET, "--target-path", "target/data-test",
        "--exclude", "test_type:unit",
    ] + defer_args
    if names:
        cmd.extend(["--select", " ".join(names)])
    subprocess.run(cmd, env=env)

    run_results: dict | None = None
    try:
        with open("target/data-test/run_results.json") as f:
            run_results = json.load(f)
    except (OSError, json.JSONDecodeError):
        _post(head_sha, "failure", "Gate 4: run_results.json missing or malformed")
        return 1

    summary = parse_data_test_results(run_results)
    manifest_nodes = _load_manifest_nodes("target/data-test/manifest.json")
    summary["tests"] = enrich_tests_from_manifest(summary["tests"], manifest_nodes)
    summary["store_failures_config_ok"] = store_failures_config_ok
    summary.update({"gate": "4", "head_sha": head_sha})

    _write_gate_result(args.output, summary)
    overall = summary["overall_status"]
    gh_state = "success" if overall == "pass" else "failure"
    counts = summary["counts"]
    desc = (f"Gate 4: {overall} — "
            f"{counts['pass']} passed / {counts['fail']} failed / "
            f"{counts['error']} errored / {counts['skip']} skipped")
    _post(head_sha, gh_state, desc)
    print(f"Gate 4 result: {overall} → GitHub status: {gh_state}", flush=True)
    return 0 if gh_state == "success" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate 4 (ci/data-tests) — Fabric GHA runner")
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
    p.add_argument("--dbt-project", default="dbt_project.yml")
    p.add_argument("--profiles-dir", default=".github/profiles")
    p.add_argument("--output", default=None)
    args = parser.parse_args(argv)
    return {"run-gate": cmd_run_gate}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
