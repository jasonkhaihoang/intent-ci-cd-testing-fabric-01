"""Preflight's Fabric capacity-grant check (AC-105) — thin shell over check_capacity_access.

Runs inside `ci/preflight` on both Fabric lanes, after azure/login, and folds its row into
the preflight report so the preflight comment shows it beside intent and ci-config.
Fabric-only: delivered by the two Fabric manifests, because MotherDuck has no capacity.

CLI:
    fabric_capacity_check.py --report reports/preflight.json
Env:
    FABRIC_CAPACITY_ID — the capacity the provision step will create the workspace on.
Exits 1 when the CI identity cannot use the capacity.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

try:
    from scripts import fabric_transport
    from scripts.preflight import check_capacity_access
except ImportError:  # invoked as `python3 path/to/fabric_capacity_check.py`
    import fabric_transport
    from preflight import check_capacity_access


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Check the CI identity's Fabric capacity grant")
    parser.add_argument("--report", default="reports/preflight.json")
    args = parser.parse_args(argv)

    capacity_id = os.environ.get("FABRIC_CAPACITY_ID", "")
    try:
        visible = (
            [c.get("id", "") for c in fabric_transport.request("GET", "/capacities").get("value", [])]
            if capacity_id.strip() else []
        )
        row = check_capacity_access(capacity_id, visible)
    except Exception as exc:
        # An unreachable API is not evidence the grant is missing — say which it was, and
        # still land the row so the comment cannot show a passing preflight for a failed job.
        row = {"passed": False, "message": (
            f"Could not list Fabric capacities as the CI identity ({type(exc).__name__}: {exc}), so "
            f"the grant on `{capacity_id.strip()}` was not checked.")}

    with open(args.report) as f:
        report = json.load(f)
    report["capacity"] = row
    if not row["passed"]:
        report["overall_status"] = "fail"
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    print(row["message"], flush=True)
    return 0 if row["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
