"""Shared pure helpers for Fabric GHA gate runner scripts (AC-11).

Extracted here to eliminate duplication across gate2/3/4/5_fabric_runner.py.
Each function is stateless and takes only pre-fetched scalar inputs.
"""
from __future__ import annotations

import json
import pathlib
import re
import shutil

import runner_io


def _load_artifacts(deployment_manifest_path: str) -> list[dict]:
    """Load the artifact list from the deployment manifest, or [] on read error."""
    try:
        with open(deployment_manifest_path) as f:
            dm = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    return dm.get("artifacts") or []


def _leaf_name(artifact: dict) -> str:
    name = artifact.get("name", "")
    return name.split(".")[-1] or name


def mat_map_from_manifest(deployment_manifest_path: str) -> dict[str, str]:
    """Return {model_name: materialized} from the deployment manifest."""
    return {
        _leaf_name(a): a.get("materialized", "")
        for a in _load_artifacts(deployment_manifest_path)
        if a.get("name")
    }


def select_names(deployment_manifest_path: str) -> list[str]:
    """Extract leaf model names from the deployment manifest, excluding ephemeral."""
    return [
        _leaf_name(a)
        for a in _load_artifacts(deployment_manifest_path)
        if a.get("name") and a.get("materialized") != "ephemeral"
    ]


def select_clone_names(deployment_manifest_path: str) -> list[str]:
    """Clone-eligible leaf names: non-ephemeral and non-view.

    Views cannot be shallow-cloned — dbt clone falls back to running the view
    materialization, which the fabricspark macro renders as CREATE VIEW against a
    2-part prod ref that fails in the ephemeral workspace (VD-2336). Views are
    built by the subsequent `dbt run --defer` instead, so they are dropped here.
    """
    return [
        _leaf_name(a)
        for a in _load_artifacts(deployment_manifest_path)
        if a.get("name") and a.get("materialized") not in ("ephemeral", "view")
    ]


class DeferManifestMissingWorkspaceError(RuntimeError):
    """Raised when a project-owned model in the staged defer manifest lacks
    workspace_name (AC-95) — a deferred reference to it would otherwise
    silently resolve against the consuming run's own ephemeral workspace
    instead of failing visibly."""


def find_deferred_nodes_missing_workspace(manifest: dict) -> list[str]:
    """Pure: leaf names of this project's own non-ephemeral models in `manifest`
    whose config lacks a workspace_name (AC-95).

    Scoped to `metadata.project_name` so dependency-package models (e.g. an
    installed dbt package) are never flagged — only this project's own models
    are meant to carry the injected workspace_name (domain-deploy AC-13).
    Ephemeral models are excluded: they compile inline as CTEs and never
    resolve to a real relation, so they never need one.
    """
    project_name = (manifest.get("metadata") or {}).get("project_name", "")
    prefix = f"model.{project_name}."
    missing = []
    for node_id, node in (manifest.get("nodes") or {}).items():
        if not node_id.startswith(prefix):
            continue
        config = node.get("config") or {}
        if config.get("materialized") == "ephemeral":
            continue
        if not config.get("workspace_name"):
            missing.append(node.get("name") or node_id.split(".")[-1])
    return missing


def validate_defer_manifest_workspaces() -> None:
    """Raise DeferManifestMissingWorkspaceError if the manifest setup_defer just
    staged is missing workspace_name on any project-owned model (AC-95).

    No-op if the defer manifest doesn't exist (greenfield — setup_defer already
    returned [] and no --defer args are in play) or can't be parsed.
    """
    defer_manifest_path = pathlib.Path(runner_io.target_path("target/prod-state-defer/manifest.json"))
    if not defer_manifest_path.exists():
        return
    try:
        with open(defer_manifest_path) as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    missing = find_deferred_nodes_missing_workspace(manifest)
    if missing:
        raise DeferManifestMissingWorkspaceError(
            "defer manifest is missing workspace_name for: " + ", ".join(sorted(missing))
        )


def setup_defer(prod_state_dir: str) -> list[str]:
    """Copy manifest_prod.json to target/prod-state-defer/manifest.json.

    Returns --defer --state args if the source file exists, otherwise [].
    Used by Gates 2, 3, and 4 for cross-workspace ref resolution.

    The physical copy is written under $PROJ (VD-4637) since dbt (running with
    --project-dir resolved from $PROJ) looks for --state relative to its own
    project root. The returned --state value itself stays project-dir-relative
    ("target/prod-state-defer", not PROJ-prefixed) because dbt joins
    project_root / state_path internally — prefixing it here would double it up.
    """
    src = pathlib.Path(prod_state_dir) / "manifest_prod.json"
    if not src.exists():
        return []
    state_rel = "target/prod-state-defer"
    dest_dir = pathlib.Path(runner_io.target_path(state_rel))
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dest_dir / "manifest.json")
    return ["--defer", "--state", state_rel]


_REF_RE = re.compile(r"""ref\s*\(\s*['"](\w+)['"]""")
_CLONE_MATERIALIZATIONS = frozenset({"table", "incremental", "materialized_view"})


def _select_unit_test_inputs(
    compiled_manifest_path: str,
    already_built_names: set[str],
    materializations: frozenset[str],
) -> list[str]:
    """Shared core: extract ref() given inputs from unit tests matching materializations."""
    try:
        with open(compiled_manifest_path) as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []

    node_mat: dict[str, str] = {}
    for node_id, node in (manifest.get("nodes") or {}).items():
        if node_id.startswith("model."):
            node_mat[node.get("name", node_id.split(".")[-1])] = (
                (node.get("config") or {}).get("materialized", "")
            )

    seen: set[str] = set()
    result: list[str] = []
    for ut in (manifest.get("unit_tests") or {}).values():
        for given in (ut.get("given") or []):
            m = _REF_RE.search(given.get("input", ""))
            if not m:
                continue
            name = m.group(1)
            if name in seen or name in already_built_names:
                continue
            if node_mat.get(name) in materializations:
                seen.add(name)
                result.append(name)

    return result


def select_unit_test_view_inputs(
    compiled_manifest_path: str,
    already_built_names: set[str] | list[str],
) -> list[str]:
    """Return view-materialized model names needed for unit test fixture introspection.

    Parses target/manifest.json (compiled manifest), extracts ref() inputs from
    unit_tests given clauses, filters to view materializations, and excludes names
    already built by Gate 2's main dbt run.  Gate 2 must build these into the
    ephemeral workspace so dbt 1.11's get_columns_in_relation() can resolve them.
    """
    return _select_unit_test_inputs(
        compiled_manifest_path, set(already_built_names), frozenset({"view"})
    )


def select_unit_test_table_inputs(
    compiled_manifest_path: str,
    already_built_names: set[str] | list[str],
) -> list[str]:
    """Return table/incremental/materialized_view model names needed for unit test fixture introspection.

    Parses target/manifest.json (compiled manifest), extracts ref() inputs from
    unit_tests given clauses, filters to clonable materializations (table, incremental,
    materialized_view), and excludes names already present in the ephemeral workspace.
    Gate 2 must clone these via dbt clone --defer --state so dbt 1.11's
    get_columns_in_relation() can resolve them during Gate 3 fixture introspection.
    """
    return _select_unit_test_inputs(
        compiled_manifest_path, set(already_built_names), _CLONE_MATERIALIZATIONS
    )


def write_gate_result(path: str | None, result: dict) -> None:
    """Write gate result JSON to the output file, creating parent dirs as needed."""
    if not path:
        return
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
