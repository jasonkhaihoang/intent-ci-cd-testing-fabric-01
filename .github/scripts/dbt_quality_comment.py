"""
Parse dbt-project-evaluator results from DuckDB and post a PR comment.

Queries the DuckDB file written by `dbt build --select dbt_project_evaluator`
and summarises violations by category. Posts (or updates) a single PR comment.
The workflow always exits 0 — violations are advisory and do not block merge.
"""

import os

try:
    from scripts import pr_comment
except ImportError:
    import pr_comment

DUCKDB_PATH = "/tmp/vibedata_dbt_quality.duckdb"

# Maps dbt-project-evaluator model name prefixes to display categories.
CATEGORIES = {
    "fct_documentation": "Documentation",
    "fct_test": "Testing",
    "fct_structure": "Structure",
    "fct_performance": "Performance",
    "fct_governance": "Governance",
}

EVALUATOR_DOCS_URL = "https://dbt-labs.github.io/dbt-project-evaluator/latest/"
COMMENT_MARKER = "<!-- dbt-quality-evaluator -->"


def query_violations(db_path: str) -> dict[str, list[dict]]:
    """
    Query each evaluator result model from DuckDB.
    Returns {category_name: [row_dict, ...]} for models that have rows.
    """
    try:
        import duckdb
    except ImportError:
        return {}

    if not os.path.exists(db_path):
        return {}

    results: dict[str, list[dict]] = {}
    try:
        con = duckdb.connect(db_path, read_only=True)
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        for prefix, category in CATEGORIES.items():
            matching = [t for t in tables if t.startswith(prefix)]
            rows: list[dict] = []
            for table in matching:
                cols = [desc[0] for desc in con.execute(f"DESCRIBE {table}").fetchall()]
                for row in con.execute(f"SELECT * FROM {table}").fetchall():
                    rows.append(dict(zip(cols, row)))
            if rows:
                results[category] = rows
        con.close()
    except Exception:
        pass
    return results


def _violation_line(row: dict) -> str:
    """Format a single violation row as a readable bullet."""
    # dbt-project-evaluator models include `model_name` or `resource_name` columns.
    name = row.get("model_name") or row.get("resource_name") or row.get("column_name", "")
    reason = row.get("reason") or row.get("violation") or ""
    if name and reason:
        return f"- `{name}` — {reason}"
    if name:
        return f"- `{name}`"
    # Fallback: render as key=value pairs, skipping None values
    parts = [f"{k}={v}" for k, v in row.items() if v is not None and str(v).strip()]
    return "- " + ", ".join(parts) if parts else "- (details unavailable)"


def build_comment(violations: dict[str, list[dict]]) -> str:
    all_categories = list(CATEGORIES.values())

    rows = []
    for category in all_categories:
        count = len(violations.get(category, []))
        icon = "⚠️" if count else "✅"
        rows.append(f"| {category} | {icon} {count} |")

    table = "\n".join(rows)

    sections = []
    for category in all_categories:
        v = violations.get(category, [])
        if v:
            lines = "\n".join(_violation_line(row) for row in v)
            sections.append(f"### {category} violations\n{lines}")

    detail_block = ("\n\n" + "\n\n".join(sections)) if sections else ""

    no_violations_note = (
        "\n\n> No violations found — great work!"
        if not violations
        else ""
    )

    return f"""{COMMENT_MARKER}
## dbt Project Evaluator (advisory) — ci/dbt-project-evaluate

> These are advisory — they do not block merge.
> Reference: [dbt-project-evaluator docs]({EVALUATOR_DOCS_URL})

| Category | Violations |
|----------|-----------|
{table}
{detail_block}{no_violations_note}
"""


def main() -> None:
    pr_number = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("REPO", "")

    violations = query_violations(DUCKDB_PATH)
    comment = build_comment(violations)
    pr_comment.upsert(COMMENT_MARKER, comment, pr_number, repo)
    print("dbt quality PR comment posted.", flush=True)


if __name__ == "__main__":
    main()
