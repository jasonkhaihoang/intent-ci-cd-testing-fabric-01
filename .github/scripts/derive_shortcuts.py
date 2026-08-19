"""
Derive OneLake shortcuts the ephemeral lakehouse needs from dbt manifest analysis.

AWAP v1.4 Phase 3 — Slice 1 (VD-1658). Pure derivation: no Fabric API calls.
The output JSON is consumed by Slice 2's `fabric_api.py seed-shortcuts` subcommand.

Logic:
1. Detect greenfield (`prod-state/source.json.mode == "greenfield"`, or sidecar
   absent) → load `target/manifest.json` and emit shortcuts for every source.*
   node (greenfield = no CI baseline, not no prod data; all sources need shortcuts
   so Gate 2's full build can resolve {{ source() }} refs).
2. Run `dbt ls --select state:modified+ --resource-type model snapshot
   --state ./prod-state --output json` to identify the build closure.
   Empty result → `[]` and zero_state="no-modified-models".
3. Walk `depends_on.nodes` recursively in the current-branch manifest from each
   node in the build closure, collecting upstreams.
4. Filter to source.* only — model.* and snapshot.* resolve to prod via --defer.
5. Resolve schema/alias/path: prod manifest is truth for unmodified models and
   snapshots; current-branch manifest for sources.
6. If all upstreams were modified themselves → `[]` and zero_state="no-upstreams".

Outputs:
  stdout (or `--output PATH`): JSON list of shortcut entries.
  reports/shortcut_seeding.json: {derived, zero_state, ...preserved keys}

Env: PROD_WORKSPACE_ID, PROD_LAKEHOUSE_ID.
"""

import argparse
import json
import os
import sys
from typing import List, Optional, Set, Tuple

try:
    from scripts import shortcut_seeding_report
    from scripts.dbt_ls import run_dbt_ls
except ImportError:  # invoked as `python3 path/to/derive_shortcuts.py`
    import shortcut_seeding_report
    from dbt_ls import run_dbt_ls

from dbt_manifest import Manifest


# ─── Pure core ────────────────────────────────────────────────────────────────

def _is_schema_enabled(upstream_nodes: List[dict]) -> bool:
    """Schema-enabled if every upstream has a non-empty schema; otherwise flat."""
    if not upstream_nodes:
        return True
    return all(n.get("schema") for n in upstream_nodes)


def _resolve_node(unique_id: str, prod_manifest: Manifest, current_manifest: Manifest) -> Optional[dict]:
    """Look up an upstream node. Sources from current-branch; models/snapshots from prod."""
    if unique_id.startswith("source."):
        return current_manifest.source(unique_id)
    return prod_manifest.node(unique_id)


def _node_table_name(node: dict) -> str:
    """Source uses `identifier` (falls back to `name`); model/snapshot uses `alias` (falls back to `name`)."""
    if node.get("resource_type") == "source":
        return node.get("identifier") or node.get("name", "")
    return node.get("alias") or node.get("name", "")


def _is_non_physical_model(node: dict) -> bool:
    """Views and ephemeral models have no physical Delta file in OneLake — skip them."""
    if node.get("resource_type") != "model":
        return False
    materialized = (node.get("config") or {}).get("materialized", "")
    return materialized in ("ephemeral", "view")


def _shortcut_entry(
    node: dict,
    unique_id: str,
    schema_enabled: bool,
    prod_workspace_id: str,
    prod_lakehouse_id: str,
) -> dict:
    schema = node.get("schema") or ""
    table = _node_table_name(node)
    if schema_enabled and schema:
        path = f"Tables/{schema}/{table}"
        alias = f"{schema}__{table}"
    else:
        path = f"Tables/{table}"
        alias = table
    return {
        "alias": alias,
        "source_workspace_id": prod_workspace_id,
        "source_lakehouse_id": prod_lakehouse_id,
        "source_path": path,
        "node_unique_id": unique_id,
    }


def derive_shortcuts(
    current_manifest: dict | Manifest,
    prod_manifest: dict | Manifest,
    modified_unique_ids: List[str],
    prod_workspace_id: str,
    prod_lakehouse_id: str,
) -> Tuple[List[dict], Optional[str]]:
    """Pure derivation. Returns (shortcuts, zero_state).

    zero_state ∈ {None, "no-upstreams"} — greenfield/no-modified-models are
    detected by the shell before this function is called.
    """
    if not modified_unique_ids:
        return [], "no-modified-models"

    cur = current_manifest if isinstance(current_manifest, Manifest) else Manifest.from_dict(current_manifest)
    prod = prod_manifest if isinstance(prod_manifest, Manifest) else Manifest.from_dict(prod_manifest)

    modified_set = set(modified_unique_ids)
    all_upstreams: Set[str] = set()
    for mid in modified_unique_ids:
        all_upstreams |= cur.upstreams_of(mid)

    # Filter: drop modified-set members, seeds, views, and ephemeral models.
    candidates: List[Tuple[str, dict]] = []
    for uid in all_upstreams:
        if uid in modified_set:
            continue
        if uid.startswith("seed."):
            continue
        node = _resolve_node(uid, prod, cur)
        if node is None:
            # Unresolved node — skip; could be a test, exposure, or unknown type.
            continue
        if _is_non_physical_model(node):
            continue
        if not uid.startswith("source."):
            continue
        candidates.append((uid, node))

    if not candidates:
        return [], "no-upstreams"

    schema_enabled = _is_schema_enabled([n for _, n in candidates])
    # Deterministic ordering: by unique_id.
    shortcuts = [
        _shortcut_entry(node, uid, schema_enabled, prod_workspace_id, prod_lakehouse_id)
        for uid, node in sorted(candidates, key=lambda p: p[0])
    ]
    return shortcuts, None


# ─── Mutable shell ────────────────────────────────────────────────────────────

def _read_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _is_greenfield() -> bool:
    """Greenfield = source.json missing or mode == 'greenfield'."""
    src = _read_json("prod-state/source.json")
    if src is None:
        return True
    return src.get("mode") == "greenfield"



def _write_report(derived: List[dict], zero_state: Optional[str]) -> None:
    shortcut_seeding_report.set_derivation(derived, zero_state)


def _emit_shortcuts(shortcuts: List[dict], output_path: Optional[str]) -> None:
    payload = json.dumps(shortcuts, indent=2)
    if output_path:
        with open(output_path, "w") as f:
            f.write(payload)
    else:
        print(payload)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Derive OneLake shortcuts from dbt manifests.")
    parser.add_argument("--output", help="Write JSON to this path instead of stdout.")
    args = parser.parse_args(argv)

    prod_workspace_id = os.environ.get("PROD_WORKSPACE_ID", "")
    prod_lakehouse_id = os.environ.get("PROD_LAKEHOUSE_ID", "")

    if _is_greenfield():
        manifest_data = _read_json("target/manifest.json") or {}
        source_items = sorted((manifest_data.get("sources") or {}).items())
        schema_enabled = _is_schema_enabled([n for _, n in source_items])
        shortcuts = [
            _shortcut_entry(node, uid, schema_enabled, prod_workspace_id, prod_lakehouse_id)
            for uid, node in source_items
        ]
        _emit_shortcuts(shortcuts, args.output)
        _write_report(shortcuts, "greenfield")
        return 0

    modified_ids = run_dbt_ls()
    if not modified_ids:
        _emit_shortcuts([], args.output)
        _write_report([], "no-modified-models")
        return 0

    current_manifest = Manifest.from_path("target/manifest.json")
    prod_manifest = Manifest.from_path("prod-state/manifest.json")

    shortcuts, zero_state = derive_shortcuts(
        current_manifest=current_manifest,
        prod_manifest=prod_manifest,
        modified_unique_ids=modified_ids,
        prod_workspace_id=prod_workspace_id,
        prod_lakehouse_id=prod_lakehouse_id,
    )
    _emit_shortcuts(shortcuts, args.output)
    _write_report(shortcuts, zero_state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
