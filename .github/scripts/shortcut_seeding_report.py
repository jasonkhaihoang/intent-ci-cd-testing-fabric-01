"""Owns the read-modify-write cycle of reports/shortcut_seeding.json.

Two CI jobs write to this report:
  * Slice 1 (derive_shortcuts.py) sets `derived` and `zero_state`.
  * Slice 2 (fabric_api seed-shortcuts) sets `created` and `already_existed`.

Either ordering must yield a file containing all four keys; this module is the
single contract that enforces the merge invariant.
"""
from __future__ import annotations

import json
import os
from typing import List, Optional

DEFAULT_PATH = "reports/shortcut_seeding.json"


def read(path: str = DEFAULT_PATH) -> dict:
    """Return the current report; empty dict if absent or unparsable.

    Corrupt JSON is treated as empty so a stale partial write from one slice
    cannot block the other slice's merge. Callers cannot distinguish "absent"
    from "corrupt" from this return value — that is intentional.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f) or {}
    except (json.JSONDecodeError, ValueError):
        return {}


def set_derivation(
    derived: List[dict], zero_state: Optional[str], path: str = DEFAULT_PATH
) -> None:
    """Merge Slice 1 keys (`derived`, `zero_state`) into the report."""
    _merge_and_write(path, {"derived": derived, "zero_state": zero_state})


def set_seeding(created: int, already_existed: int, path: str = DEFAULT_PATH) -> None:
    """Merge Slice 2 keys (`created`, `already_existed`) into the report."""
    _merge_and_write(path, {"created": created, "already_existed": already_existed})


def _merge_and_write(path: str, updates: dict) -> None:
    existing = read(path)
    existing.update(updates)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)
