"""Ecosystem read API for dashboard: list, get, hierarchy, repos, MADs, search."""

from datetime import date
from typing import Any

from ._db_utils import fetch_all_dicts, fetch_one_dict


def list_ecosystems(
    conn,
    *,
    name_contains: str | None = None,
    is_crypto: bool | None = None,
    is_chain: bool | None = None,
    include_repo_count: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """List ecosystems with optional filters and pagination."""
    where_parts = []
    params: list[Any] = []
    if name_contains is not None:
        where_parts.append("e.name ILIKE ?")
        params.append(f"%{name_contains}%")
    if is_crypto is not None:
        where_parts.append("e.is_crypto = ?")
        params.append(1 if is_crypto else 0)
    if is_chain is not None:
        where_parts.append("e.is_chain = ?")
        params.append(1 if is_chain else 0)
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    if include_repo_count:
        query = f"""
            SELECT e.id, e.name, e.launch_date, e.derived_launch_date,
                   e.is_crypto, e.is_category, e.is_chain, e.is_multichain,
                   count(er.repo_id) AS repo_count
            FROM ecosystems e
            LEFT JOIN ecosystems_repos er ON er.ecosystem_id = e.id
            WHERE {where_sql}
            GROUP BY e.id, e.name, e.launch_date, e.derived_launch_date,
                     e.is_crypto, e.is_category, e.is_chain, e.is_multichain
            ORDER BY e.name
            LIMIT ? OFFSET ?
        """
    else:
        query = f"""
            SELECT id, name, launch_date, derived_launch_date,
                   is_crypto, is_category, is_chain, is_multichain
            FROM ecosystems e
            WHERE {where_sql}
            ORDER BY e.name
            LIMIT ? OFFSET ?
        """
    params.extend([limit, offset])
    return fetch_all_dicts(conn, query, params)


def get_ecosystem(conn, ecosystem_id: int, *, include_latest_mads: bool = False) -> dict | None:
    """Get a single ecosystem by id; optionally include latest eco_mads row."""
    row = fetch_one_dict(
        conn,
        "SELECT id, name, launch_date, derived_launch_date, is_crypto, is_category, is_chain, is_multichain "
        "FROM ecosystems WHERE id = ?",
        [ecosystem_id],
    )
    if row is None:
        return None
    if include_latest_mads:
        mads = fetch_one_dict(
            conn,
            "SELECT day, all_devs, exclusive_devs, num_commits, full_time_devs, part_time_devs, one_time_devs "
            "FROM eco_mads WHERE ecosystem_id = ? ORDER BY day DESC LIMIT 1",
            [ecosystem_id],
        )
        if mads:
            row["latest_mads"] = mads
    return row


def ecosystem_hierarchy(conn, ecosystem_id: int) -> dict:
    """Return parent and child ecosystem ids and names for an ecosystem."""
    parents = fetch_all_dicts(
        conn,
        """
        SELECT p.id AS parent_id, p.name AS parent_name
        FROM ecosystems_child_ecosystems ece
        JOIN ecosystems p ON p.id = ece.parent_id
        WHERE ece.child_id = ?
        """,
        [ecosystem_id],
    )
    children = fetch_all_dicts(
        conn,
        """
        SELECT c.id AS child_id, c.name AS child_name
        FROM ecosystems_child_ecosystems ece
        JOIN ecosystems c ON c.id = ece.child_id
        WHERE ece.parent_id = ?
        """,
        [ecosystem_id],
    )
    return {"parent_id": ecosystem_id, "parents": parents, "children": children}


def repos_in_ecosystem(
    conn,
    ecosystem_id: int,
    *,
    recursive: bool = True,
    sort_by: str = "num_stars",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """List repos in an ecosystem; recursive uses ecosystems_repos_recursive."""
    table = "ecosystems_repos_recursive" if recursive else "ecosystems_repos"
    order = "r.num_stars DESC NULLS LAST" if sort_by == "num_stars" else "r.name"
    query = f"""
        SELECT r.id, r.name, r.link, r.num_stars, r.num_forks, r.num_issues
        FROM {table} er
        JOIN repos r ON r.id = er.repo_id
        WHERE er.ecosystem_id = ?
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """
    return fetch_all_dicts(conn, query, [ecosystem_id, limit, offset])


def ecosystem_mads_time_series(
    conn,
    ecosystem_id: int,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 365,
) -> list[dict]:
    """Return eco_mads rows for an ecosystem over a day range (for charts)."""
    where = "ecosystem_id = ?"
    params: list[Any] = [ecosystem_id]
    if start_date is not None:
        where += " AND day >= ?"
        params.append(start_date)
    if end_date is not None:
        where += " AND day <= ?"
        params.append(end_date)
    params.append(limit)
    query = f"""
        SELECT day, all_devs, exclusive_devs, multichain_devs, num_commits,
               devs_0_1y, devs_1_2y, devs_2y_plus, one_time_devs, part_time_devs, full_time_devs
        FROM eco_mads
        WHERE {where}
        ORDER BY day DESC
        LIMIT ?
    """
    return fetch_all_dicts(conn, query, params)


def search_ecosystems(conn, name_query: str, *, limit: int = 30) -> list[dict]:
    """Search ecosystems by name (ILIKE); for type-ahead."""
    query = """
        SELECT id, name, is_crypto, is_chain
        FROM ecosystems
        WHERE name ILIKE ?
        ORDER BY name
        LIMIT ?
    """
    return fetch_all_dicts(conn, query, [f"%{name_query}%", limit])


def top_repos_in_ecosystem(
    conn,
    ecosystem_id: int,
    *,
    recursive: bool = True,
    limit: int = 20,
) -> list[dict]:
    """Top repos in ecosystem by num_stars (or most active); limit default 20."""
    table = "ecosystems_repos_recursive" if recursive else "ecosystems_repos"
    query = f"""
        SELECT r.id, r.name, r.link, r.num_stars, r.num_forks
        FROM {table} er
        JOIN repos r ON r.id = er.repo_id
        WHERE er.ecosystem_id = ?
        ORDER BY r.num_stars DESC NULLS LAST
        LIMIT ?
    """
    return fetch_all_dicts(conn, query, [ecosystem_id, limit])
