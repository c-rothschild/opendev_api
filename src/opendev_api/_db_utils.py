"""Internal helpers for DuckDB query results."""

from typing import Any


def fetch_all_dicts(conn, query: str, params: list[Any] | None = None) -> list[dict]:
    """Execute query and return rows as list of dicts (column name -> value)."""
    if params is not None:
        result = conn.execute(query, params)
    else:
        result = conn.execute(query)
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def fetch_one_dict(conn, query: str, params: list[Any] | None = None) -> dict | None:
    """Execute query and return first row as dict, or None if no row."""
    rows = fetch_all_dicts(conn, query, params)
    return rows[0] if rows else None
