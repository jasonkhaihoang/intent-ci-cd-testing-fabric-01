"""Gate 5 diff engine — per-materialization schema/row-count/value delta.

Pure core:
    normalize_unique_key, compute_schema_delta, compute_row_count_delta,
    build_value_delta_count_sql, compute_artifact_diff,
    gate_5_overall_status, build_gate_5_result

I/O shell (executor-injected for testability):
    execute_value_delta

Constants consumed by the notebook cell (VD-1974):
    VALUE_DELTA_MATERIALIZATIONS, SKIP_MATERIALIZATIONS
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Callable

from label_binding import compute_diff_hash

ExecutorFn = Callable[[str], list]

ColumnInfo = dict  # {"name": str, "dtype": str, "nullable": bool}

VALUE_DELTA_MATERIALIZATIONS: frozenset[str] = frozenset({"table", "incremental", "snapshot"})
SKIP_MATERIALIZATIONS: frozenset[str] = frozenset({"ephemeral"})


# ── Pure core ──────────────────────────────────────────────────────────────────

def compute_schema_delta(
    prod_columns: list[ColumnInfo],
    ci_columns: list[ColumnInfo],
) -> dict:
    """Return schema delta: added, removed, renamed, type_changed, nullability_flipped."""
    prod_by_name = {c["name"]: c for c in prod_columns}
    ci_by_name = {c["name"]: c for c in ci_columns}
    prod_names = set(prod_by_name)
    ci_names = set(ci_by_name)

    type_changed = []
    nullability_flipped = []
    for name in sorted(prod_names & ci_names):
        p, c = prod_by_name[name], ci_by_name[name]
        if p.get("dtype") != c.get("dtype"):
            type_changed.append({
                "column": name,
                "prod_dtype": p.get("dtype"),
                "ci_dtype": c.get("dtype"),
            })
        if bool(p.get("nullable")) != bool(c.get("nullable")):
            nullability_flipped.append({
                "column": name,
                "prod_nullable": p.get("nullable"),
                "ci_nullable": c.get("nullable"),
            })

    return {
        "added": sorted(ci_names - prod_names),
        "removed": sorted(prod_names - ci_names),
        "renamed": [],
        "type_changed": type_changed,
        "nullability_flipped": nullability_flipped,
    }


def compute_row_count_delta(prod_count: int, ci_count: int) -> dict:
    """Return {"prod": int, "pr": int, "delta": int}. delta = pr - prod."""
    return {"prod": prod_count, "pr": ci_count, "delta": ci_count - prod_count}


def normalize_unique_key(raw: Any) -> list[str] | None:
    """Normalise manifest unique_key to list[str] or None.

    manifest.json stores unique_key as a string (single-column) or list (composite).
    Returns None when key is absent, empty, or an unexpected type.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        return [raw] if raw else None
    if isinstance(raw, list):
        return raw if raw else None
    return None


def _diff_where(pa: str, ca: str, keys: list[str], common_columns: list[str]) -> str:
    """Generate WHERE clause to detect differing rows via FULL OUTER JOIN.

    Detects:
    - Rows added to CI (missing in prod, detected by prod key IS NULL)
    - Rows deleted from prod (missing in CI, detected by CI key IS NULL)
    - Rows with value changes in non-key columns (detected by IS DISTINCT FROM)
    """
    non_key = [c for c in common_columns if c not in set(keys)]
    null_side = f"({pa}.`{keys[0]}` IS NULL OR {ca}.`{keys[0]}` IS NULL)"
    if not non_key:
        return null_side
    col_diffs = " OR ".join(
        f"({pa}.`{c}` IS DISTINCT FROM {ca}.`{c}`)" for c in non_key
    )
    return f"({null_side} OR ({col_diffs}))"


def build_value_delta_count_sql(
    prod_table: str,
    ci_table: str,
    keys: list[str],
    common_columns: list[str],
) -> str:
    """Return a COUNT query that counts differing rows via key-based FULL OUTER JOIN.

    Joins prod and CI tables on all key columns, then counts rows where:
    - Either side is NULL (row added/deleted), or
    - Non-key column values differ (IS DISTINCT FROM).
    """
    join_on = " AND ".join(f"p.`{k}` = c.`{k}`" for k in keys)
    where = _diff_where("p", "c", keys, common_columns)
    return (
        f"SELECT COUNT(*) AS rows_with_diffs "
        f"FROM {prod_table} p "
        f"FULL OUTER JOIN {ci_table} c ON {join_on} "
        f"WHERE {where}"
    )


# ── I/O shell ─────────────────────────────────────────────────────────────────

def _extract_scalar(result: list) -> int:
    """Extract a single integer from a one-row executor result.

    Handles both dict rows ({"rows_with_diffs": n}) and tuple rows ((n,)).
    """
    if not result:
        return 0
    row = result[0]
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    if isinstance(row, (list, tuple)):
        return int(row[0])
    return int(row)


def execute_value_delta(
    executor: "ExecutorFn",
    prod_table: str,
    ci_table: str,
    keys: list[str],
    common_columns: list[str],
) -> dict:
    """Run the key-based JOIN count query via executor; return value_delta dict.

    executor(sql) must return a list with one row containing the count.
    Use a mocked executor in tests; pass a real Spark SQL executor in the notebook.
    """
    sql = build_value_delta_count_sql(prod_table, ci_table, keys, common_columns)
    rows_with_diffs = _extract_scalar(executor(sql))
    return {
        "sampled_rows": min(rows_with_diffs, 10000),
        "rows_with_diffs": rows_with_diffs,
        "skipped_no_unique_key": False,
    }


def execute_livy_statement(
    session_url: str,
    code: str,
    token_fn: "Callable[[], str]",
    timeout_s: int = 90,
) -> str:
    """Submit PySpark code to a Livy session; return text/plain output.

    Polls every 2s up to timeout_s for state == 'available'. Default 90s suits
    warm sessions; pass a larger value (e.g. 300) for the first statement after
    create_livy_session, where Spark executor cold-start adds 60–180s.
    Raises RuntimeError on output error, TimeoutError on timeout.
    """
    token = token_fn()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = json.dumps({"code": code, "kind": "pyspark"}).encode()
    req = urllib.request.Request(
        f"{session_url}/statements", data=body, method="POST", headers=headers,
    )
    with urllib.request.urlopen(req) as resp:
        stmt = json.loads(resp.read())
    stmt_id = stmt["id"]

    stmt_url = f"{session_url}/statements/{stmt_id}"
    elapsed = 0
    while elapsed < timeout_s:
        time.sleep(2)
        elapsed += 2
        req = urllib.request.Request(stmt_url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req) as resp:
            stmt = json.loads(resp.read())
        if stmt["state"] == "available":
            output = stmt.get("output") or {}
            if output.get("status") == "error":
                raise RuntimeError(output.get("evalue", "Livy statement error"))
            return (output.get("data") or {}).get("text/plain", "")
    raise TimeoutError(f"Livy statement {stmt_id} did not complete within {timeout_s}s")


def execute_spark_schema(
    session_url: str,
    table_ref: str,
    token_fn: "Callable[[], str]",
) -> "list[ColumnInfo]":
    """Fetch table schema via Livy; return list of ColumnInfo dicts."""
    code = f"print(spark.table('{table_ref}').schema.json())"
    output = execute_livy_statement(session_url, code, token_fn)
    schema = json.loads(output)
    return [
        {
            "name": f["name"],
            "dtype": f["type"] if isinstance(f["type"], str) else json.dumps(f["type"]),
            "nullable": f["nullable"],
        }
        for f in schema["fields"]
    ]


def execute_spark_count(
    session_url: str,
    table_ref: str,
    token_fn: "Callable[[], str]",
) -> int:
    """Fetch table row count via Livy."""
    code = f"print(spark.table('{table_ref}').count())"
    return int(execute_livy_statement(session_url, code, token_fn).strip())


def make_livy_sql_executor(
    session_url: str,
    token_fn: "Callable[[], str]",
) -> "ExecutorFn":
    """Return an ExecutorFn that runs SQL via Livy and returns a list of row dicts."""
    def executor(sql: str) -> list:
        code = f"import json\nprint(json.dumps([row.asDict() for row in spark.sql({sql!r}).collect()]))"
        output = execute_livy_statement(session_url, code, token_fn)
        return json.loads(output)

    return executor


def create_livy_session(
    livy_base_url: str,
    session_name: str,
    token_fn: "Callable[[], str]",
) -> str:
    """Create a new Livy session; return session_id as str.

    POSTs to {livy_base_url}/sessions, polls every 2s up to 600s for state == 'idle'.
    Raises TimeoutError on poll timeout; RuntimeError on terminal state (dead/error/killed).
    """
    token = token_fn()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = json.dumps({"name": session_name}).encode()
    req = urllib.request.Request(
        f"{livy_base_url}/sessions", data=body, method="POST", headers=headers,
    )
    with urllib.request.urlopen(req) as resp:
        session = json.loads(resp.read())
    session_id = session["id"]
    poll_url = f"{livy_base_url}/sessions/{session_id}"
    session_url = f"{livy_base_url}/sessions/{session_id}"

    try:
        elapsed = 0
        while elapsed < 600:
            time.sleep(2)
            elapsed += 2
            req = urllib.request.Request(poll_url, headers={"Authorization": f"Bearer {token_fn()}"})
            with urllib.request.urlopen(req) as resp:
                session = json.loads(resp.read())
            state = session.get("state")
            if state == "idle":
                return str(session_id)
            if state in ("dead", "error", "killed"):
                raise RuntimeError(f"Livy session {session_id} entered terminal state: {state}")
        raise TimeoutError(f"Livy session {session_id} did not reach idle within 600s")
    except Exception:
        delete_livy_session(session_url, token_fn)
        raise


def delete_livy_session(
    session_url: str,
    token_fn: "Callable[[], str]",
) -> None:
    """Delete a Livy session. Idempotent: swallows 404."""
    req = urllib.request.Request(
        session_url, method="DELETE",
        headers={"Authorization": f"Bearer {token_fn()}"},
    )
    try:
        with urllib.request.urlopen(req):
            pass
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    except urllib.error.URLError:
        pass


# ── Artifact-level orchestration ──────────────────────────────────────────────

def compute_artifact_diff(
    artifact: dict,
    prod_columns: list[ColumnInfo],
    ci_columns: list[ColumnInfo],
    prod_count: int,
    ci_count: int,
    value_delta: dict | None,
) -> dict:
    """Assemble the per-artifact result dict from pre-fetched values.

    The calling notebook cell (VD-1974) is responsible for:
    - skipping ephemeral materializations (check SKIP_MATERIALIZATIONS before calling)
    - deciding whether to compute value delta (check VALUE_DELTA_MATERIALIZATIONS)
    - fetching prod_columns, ci_columns, prod_count, ci_count from the database
    - passing value_delta=None for view/materialized_view (demo trim)
    - passing {"skipped_no_unique_key": True, ...} when model has no unique_key

    brand-new artifact (pre_existing_in_prod: false) → baseline: null, no deltas.
    """
    unique_id = artifact["unique_id"]
    name = artifact.get("name", "")
    materialized = artifact.get("materialized", "")

    if not artifact.get("pre_existing_in_prod", True):
        return {
            "unique_id": unique_id,
            "name": name,
            "materialized": materialized,
            "baseline": None,
            "schema_delta": None,
            "row_count_delta": None,
            "value_delta": None,
        }

    return {
        "unique_id": unique_id,
        "name": name,
        "materialized": materialized,
        "baseline": name,
        "schema_delta": compute_schema_delta(prod_columns, ci_columns),
        "row_count_delta": compute_row_count_delta(prod_count, ci_count),
        "value_delta": value_delta,
    }


# ── Gate signal + result builder ───────────────────────────────────────────────

def gate_5_overall_status(artifacts: list[dict], *, session_error: bool = False) -> str:
    """Return 'pass', 'fail', or 'error'.

    'error': session_error=True — Livy session dead, no diff computed.
    'fail': any artifact with non-empty schema, row-count, or value diff.
    'pass': all brand-new or no non-empty diff.
    Skipped value delta (skipped_no_unique_key: true or value_delta: null) does NOT fail.
    """
    if session_error:
        return "error"
    for a in artifacts:
        if a.get("baseline") is None:
            continue  # brand-new — auto-pass

        schema = a.get("schema_delta") or {}
        if any(schema.get(k) for k in ("added", "removed", "renamed", "type_changed", "nullability_flipped")):
            return "fail"

        row = a.get("row_count_delta") or {}
        if row.get("delta", 0) != 0:
            return "fail"

        value = a.get("value_delta") or {}
        if value and not value.get("skipped_no_unique_key", False) and value.get("rows_with_diffs", 0) > 0:
            return "fail"

    return "pass"


def build_gate_5_result(
    head_sha: str,
    artifacts: list[dict],
    *,
    session_error: str | None = None,
) -> dict:
    """Assemble the gate-5.json dict (design spec §9.2.2).

    Pass session_error=<msg> for dead-session path; overall_status becomes 'error'
    and a top-level session_error field is included.
    """
    result = {
        "gate": "5",
        "head_sha": head_sha,
        "overall_status": gate_5_overall_status(artifacts, session_error=bool(session_error)),
        "latest_hash": compute_diff_hash(artifacts),
        "artifacts": artifacts,
    }
    if session_error:
        result["session_error"] = session_error
    return result
