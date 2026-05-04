"""LorePulse MCP Server - dbt pipeline health metrics."""

import json
from pathlib import Path

from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "lorepulse",
    instructions=(
        "LorePulse surfaces dbt pipeline health metrics from local artifacts. "
        "Use get_test_results to check test pass/fail status from the last dbt test run. "
        "Use get_model_freshness to check source freshness from the last dbt source freshness run. "
        "Use get_pipeline_summary for a rolled-up dashboard overview of both. "
        "All tools read from target/ artifacts in the dbt project directory -- run dbt first."
    ),
)


def _resolve_project_path(project_path: str | None) -> Path:
    if project_path:
        return Path(project_path).expanduser().resolve()
    return Path.cwd()


@mcp.tool()
def get_test_results(project_path: str | None = None) -> dict:
    """Get dbt test pass/fail results from the most recent test run.

    Reads target/run_results.json. Returns counts by status plus failure details.
    Run 'dbt test' first to generate fresh results.

    Args:
        project_path: Path to dbt project root. Defaults to current directory.

    Returns:
        dict with keys: total, passed, failed, errored, warned, last_run,
        project_path, failures (list of {test, unique_id, message, failures})
    """
    root = _resolve_project_path(project_path)
    results_file = root / "target" / "run_results.json"

    if not results_file.exists():
        return {
            "error": (
                f"No run_results.json found at {results_file}. "
                "Run 'dbt test' first."
            ),
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errored": 0,
            "warned": 0,
            "last_run": None,
            "project_path": str(root),
            "failures": [],
        }

    data = json.loads(results_file.read_text(encoding="utf-8"))

    # Only process test nodes (resource_type == test, or unique_id starts with "test.")
    results = [
        r for r in data.get("results", [])
        if r.get("unique_id", "").startswith("test.")
    ]

    counts = {"passed": 0, "failed": 0, "errored": 0, "warned": 0}
    failures = []

    for r in results:
        status = r.get("status", "error")
        uid = r.get("unique_id", "")
        parts = uid.split(".")
        # unique_id format: test.<project>.<test_name>.<hash>
        test_name = parts[2] if len(parts) > 2 else uid

        if status == "pass":
            counts["passed"] += 1
        elif status == "fail":
            counts["failed"] += 1
            failures.append({
                "test": test_name,
                "unique_id": uid,
                "message": r.get("message") or "Test failed",
                "failures": r.get("failures", 0),
            })
        elif status == "error":
            counts["errored"] += 1
            failures.append({
                "test": test_name,
                "unique_id": uid,
                "message": r.get("message") or "Test errored",
                "failures": r.get("failures", 0),
            })
        elif status == "warn":
            counts["warned"] += 1

    generated_at = data.get("metadata", {}).get("generated_at")

    return {
        "total": len(results),
        "passed": counts["passed"],
        "failed": counts["failed"],
        "errored": counts["errored"],
        "warned": counts["warned"],
        "last_run": generated_at,
        "project_path": str(root),
        "failures": failures,
    }


@mcp.tool()
def get_model_freshness(project_path: str | None = None) -> dict:
    """Get dbt source freshness status from the most recent freshness run.

    Reads target/sources.json. Returns freshness status per source with age and thresholds.
    Run 'dbt source freshness' first to generate fresh results.

    Args:
        project_path: Path to dbt project root. Defaults to current directory.

    Returns:
        dict with keys: total, fresh, stale, errored, last_run, project_path,
        sources (list of {source, table, status, age_hours, max_loaded_at, unique_id})
    """
    root = _resolve_project_path(project_path)
    sources_file = root / "target" / "sources.json"

    if not sources_file.exists():
        return {
            "error": (
                f"No sources.json found at {sources_file}. "
                "Run 'dbt source freshness' first."
            ),
            "total": 0,
            "fresh": 0,
            "stale": 0,
            "errored": 0,
            "last_run": None,
            "project_path": str(root),
            "sources": [],
        }

    data = json.loads(sources_file.read_text(encoding="utf-8"))
    results = data.get("results", [])

    counts = {"fresh": 0, "stale": 0, "errored": 0}
    sources = []

    for r in results:
        status = r.get("status", "error")
        node = r.get("node", {})
        source_name = node.get("source_name", "unknown")
        table_name = node.get("name", "unknown")
        max_loaded_at = r.get("max_loaded_at")
        age = r.get("age")  # seconds
        age_hours = round(age / 3600, 1) if age is not None else None

        if status == "pass":
            counts["fresh"] += 1
        elif status in ("warn", "error"):
            counts["stale"] += 1
        else:
            counts["errored"] += 1

        sources.append({
            "source": source_name,
            "table": table_name,
            "status": status,
            "age_hours": age_hours,
            "max_loaded_at": max_loaded_at,
            "unique_id": r.get("unique_id", ""),
        })

    generated_at = data.get("metadata", {}).get("generated_at")

    return {
        "total": len(results),
        "fresh": counts["fresh"],
        "stale": counts["stale"],
        "errored": counts["errored"],
        "last_run": generated_at,
        "project_path": str(root),
        "sources": sources,
    }


@mcp.tool()
def get_pipeline_summary(project_path: str | None = None) -> dict:
    """Get a combined dashboard summary of dbt pipeline health.

    Rolls up get_test_results and get_model_freshness into a single overview.
    Returns an overall health status (healthy / unhealthy / unknown) plus
    counts from both test results and freshness checks.

    Args:
        project_path: Path to dbt project root. Defaults to current directory.

    Returns:
        dict with keys: health, status, project_path, tests (summary counts),
        freshness (summary counts), failures (list), stale_sources (list)
    """
    tests = get_test_results(project_path)
    freshness = get_model_freshness(project_path)

    test_failures = tests.get("failed", 0) + tests.get("errored", 0)
    freshness_issues = freshness.get("stale", 0) + freshness.get("errored", 0)

    no_tests = bool(tests.get("error"))
    no_freshness = bool(freshness.get("error"))

    if no_tests and no_freshness:
        health = "unknown"
        status_msg = (
            "No dbt artifacts found. "
            "Run 'dbt test' and 'dbt source freshness' first."
        )
    elif test_failures > 0 or freshness_issues > 0:
        health = "unhealthy"
        parts = []
        if test_failures > 0:
            parts.append(f"{test_failures} test failure(s)")
        if freshness_issues > 0:
            parts.append(f"{freshness_issues} stale source(s)")
        status_msg = ", ".join(parts)
    else:
        health = "healthy"
        status_msg = "All tests passing, all sources fresh."

    stale_sources = [
        s for s in freshness.get("sources", [])
        if s.get("status") in ("warn", "error")
    ]

    return {
        "health": health,
        "status": status_msg,
        "project_path": str(_resolve_project_path(project_path)),
        "tests": {
            "total": tests.get("total", 0),
            "passed": tests.get("passed", 0),
            "failed": tests.get("failed", 0),
            "errored": tests.get("errored", 0),
            "warned": tests.get("warned", 0),
            "last_run": tests.get("last_run"),
        },
        "freshness": {
            "total": freshness.get("total", 0),
            "fresh": freshness.get("fresh", 0),
            "stale": freshness.get("stale", 0),
            "errored": freshness.get("errored", 0),
            "last_run": freshness.get("last_run"),
        },
        "failures": tests.get("failures", []),
        "stale_sources": stale_sources,
    }


def main():
    mcp.run()


if __name__ == "__main__":
    main()
