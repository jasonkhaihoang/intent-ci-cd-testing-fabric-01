"""Pure core for the diff-acknowledged label state machine (VD-1746).

No I/O, no subprocess, no time/random. All side effects live in the callers
(label-handler.yml inline Python, Gate 5 ci.yml inline Python).

Public:
    MARKER, LABEL_NAME, REJECTION_TEXT
    compute_diff_hash(artifacts) -> str | None
    read_marker(body) -> dict
    write_marker(latest_hash, bound_hash=None) -> str
    delete_bound(body) -> str
    decide_gate5_outcome(latest_hash, marker) -> dict
    label_handler_decision(event, label_name, marker) -> dict | None
"""
from __future__ import annotations

import hashlib
import json
import re

MARKER = "<!-- ci-diff-ack-state -->"
LABEL_NAME = "diff-acknowledged"
REJECTION_TEXT = (
    "Cannot acknowledge — Gate 5 has not reported a non-empty diff on the current head."
)

_LATEST_RE = re.compile(r"^latest_hash:\s*(\S+)", re.MULTILINE)
_BOUND_RE = re.compile(r"^bound_hash:\s*(\S+)", re.MULTILINE)


# ── Hash computation ──────────────────────────────────────────────────────────

def _has_nonempty_diff(artifact: dict) -> bool:
    if artifact.get("baseline") is None:
        return False
    schema = artifact.get("schema_delta") or {}
    if any(schema.get(k) for k in ("added", "removed", "renamed", "type_changed", "nullability_flipped")):
        return True
    row = artifact.get("row_count_delta") or {}
    if row.get("delta", 0) != 0:
        return True
    value = artifact.get("value_delta") or {}
    if value and not value.get("skipped_no_unique_key", False) and value.get("rows_with_diffs", 0) > 0:
        return True
    return False


def _canonical_artifact(a: dict) -> dict:
    if a.get("baseline") is None:
        return {"unique_id": a["unique_id"], "schema_delta": None, "row_count_delta": None, "value_delta_count": None}
    schema = a.get("schema_delta") or {}

    def _sorted_by_col(lst):
        return sorted(lst, key=lambda x: x.get("column", "") if isinstance(x, dict) else x)

    canonical_schema = {
        "added": sorted(schema.get("added") or []),
        "nullability_flipped": _sorted_by_col(schema.get("nullability_flipped") or []),
        "removed": sorted(schema.get("removed") or []),
        "renamed": sorted(schema.get("renamed") or []),
        "type_changed": _sorted_by_col(schema.get("type_changed") or []),
    }
    row = a.get("row_count_delta") or {}
    value = a.get("value_delta") or {}
    value_count = 0 if value.get("skipped_no_unique_key") else value.get("rows_with_diffs", 0)
    return {
        "unique_id": a["unique_id"],
        "schema_delta": canonical_schema,
        "row_count_delta": {"pr": row.get("pr", 0), "prod": row.get("prod", 0)},
        "value_delta_count": value_count,
    }


def compute_diff_hash(artifacts: list[dict]) -> str | None:
    """Return sha256 of the canonical diff state, or None when no non-empty diff."""
    if not any(_has_nonempty_diff(a) for a in artifacts):
        return None
    sorted_artifacts = sorted(artifacts, key=lambda a: a.get("unique_id", ""))
    canonical = [_canonical_artifact(a) for a in sorted_artifacts]
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


# ── Marker comment I/O ────────────────────────────────────────────────────────

def read_marker(body: str | None) -> dict:
    """Parse latest_hash and bound_hash from a marker comment body.

    Returns {"latest_hash": str|None, "bound_hash": str|None}.
    Both fields are None when the body is absent or contains no marker fields.
    """
    if not body or MARKER not in body:
        return {"latest_hash": None, "bound_hash": None}
    latest = _LATEST_RE.search(body)
    bound = _BOUND_RE.search(body)
    return {
        "latest_hash": latest.group(1) if latest else None,
        "bound_hash": bound.group(1) if bound else None,
    }


def write_marker(latest_hash: str, bound_hash: str | None = None) -> str:
    """Produce a full marker comment body."""
    lines = [MARKER, f"latest_hash: {latest_hash}"]
    if bound_hash is not None:
        lines.append(f"bound_hash: {bound_hash}")
    return "\n".join(lines) + "\n"


def delete_bound(body: str) -> str:
    """Return marker body with the bound_hash line removed."""
    return re.sub(r"^bound_hash:.*\n?", "", body, flags=re.MULTILINE)


# ── Decision functions ────────────────────────────────────────────────────────

def _decide_labeled(marker: dict) -> dict:
    latest = marker.get("latest_hash")
    if not latest:
        return {"action": "reject"}
    return {"action": "bind", "hash": latest}


def _decide_unlabeled() -> dict:
    return {"action": "revert"}


def _decide_synchronize() -> dict:
    return {"action": "strip_label"}


def decide_gate5_outcome(latest_hash: str, marker: dict) -> dict:
    """Decide Gate 5's final status given its computed hash and the current marker.

    marker: output of read_marker().
    Returns {"action": "auto_pass"|"fail", "strip_bound": bool}.
    """
    bound = marker.get("bound_hash")
    if bound and bound == latest_hash:
        return {"action": "auto_pass", "strip_bound": False}
    if bound and bound != latest_hash:
        return {"action": "fail", "strip_bound": True}
    return {"action": "fail", "strip_bound": False}


def label_handler_decision(event: str, label_name: str, marker: dict) -> dict | None:
    """Route a pull_request event to the correct decision function.

    Returns None for non-target labels (labeled/unlabeled) or unknown events.
    synchronize is always routed regardless of label_name.
    """
    if event == "synchronize":
        return _decide_synchronize()
    if label_name != LABEL_NAME:
        return None
    if event == "labeled":
        return _decide_labeled(marker)
    if event == "unlabeled":
        return _decide_unlabeled()
    return None
