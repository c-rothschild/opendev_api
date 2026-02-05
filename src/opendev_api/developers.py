"""Developer read API for dashboard: list in ecosystem, profile, activity, tenure, search."""

from datetime import date
from typing import Any

from ._db_utils import fetch_all_dicts, fetch_one_dict


def developers_in_ecosystem(
    conn,
    ecosystem_id: int,
    *,
    day: date | None = None,
    contribution_rank: str | None = None,
    include_user_info: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """List developers in an ecosystem from contribution_ranks; optionally join user_info."""
    params: list[Any] = [ecosystem_id]
    where = "ecr.ecosystem_id = ?"
    if day is not None:
        where += " AND ecr.day = ?"
        params.append(day)
    if contribution_rank is not None:
        where += " AND ecr.contribution_rank = ?"
        params.append(contribution_rank)

    # Use latest day per ecosystem if day not specified
    if day is None:
        sub = """
            SELECT ecosystem_id, canonical_developer_id, day, points, points_28d, points_56d, contribution_rank
            FROM eco_developer_contribution_ranks ecr
            WHERE ecr.ecosystem_id = ?
            AND ecr.day = (SELECT max(day) FROM eco_developer_contribution_ranks WHERE ecosystem_id = ?)
        """
        if contribution_rank:
            sub += " AND ecr.contribution_rank = ?"
        sub += " ORDER BY ecr.points DESC NULLS LAST, ecr.canonical_developer_id LIMIT ? OFFSET ?"
        params_sub = [ecosystem_id, ecosystem_id]
        if contribution_rank:
            params_sub.append(contribution_rank)
        params_sub.extend([limit, offset])
        if include_user_info:
            query = f"""
                SELECT ecr.canonical_developer_id, ecr.day, ecr.points, ecr.points_28d, ecr.points_56d, ecr.contribution_rank,
                       u.login, u.name, u.company, u.location, u.url, u.email
                FROM ({sub}) ecr
                LEFT JOIN user_info u ON u.canonical_developer_id = ecr.canonical_developer_id
            """
        else:
            query = sub
        return fetch_all_dicts(conn, query, params_sub)
    else:
        query = f"""
            SELECT ecr.canonical_developer_id, ecr.day, ecr.points, ecr.points_28d, ecr.points_56d, ecr.contribution_rank
            FROM eco_developer_contribution_ranks ecr
            WHERE {where}
            ORDER BY ecr.points DESC NULLS LAST
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        rows = fetch_all_dicts(conn, query, params)
        if include_user_info and rows:
            ids = [r["canonical_developer_id"] for r in rows]
            placeholders = ",".join("?" * len(ids))
            users = fetch_all_dicts(
                conn,
                f"SELECT canonical_developer_id, login, name, company, location, url, email FROM user_info WHERE canonical_developer_id IN ({placeholders})",
                ids,
            )
            by_id = {u["canonical_developer_id"]: u for u in users}
            for r in rows:
                u = by_id.get(r["canonical_developer_id"], {})
                r["login"] = u.get("login")
                r["name"] = u.get("name")
                r["company"] = u.get("company")
                r["location"] = u.get("location")
                r["url"] = u.get("url")
                r["email"] = u.get("email")
        return rows


def get_developer_profile(
    conn,
    canonical_developer_id: int,
    *,
    include_location: bool = False,
) -> dict | None:
    """Get developer profile from user_info; optionally include canonical_developer_locations."""
    row = fetch_one_dict(
        conn,
        "SELECT canonical_developer_id, login, name, company, location, url, email, primary_github_user_id "
        "FROM user_info WHERE canonical_developer_id = ?",
        [canonical_developer_id],
    )
    if row is None:
        return None
    if include_location:
        locs = fetch_all_dicts(
            conn,
            "SELECT country, admin_level_1, locality, lat, lng, formatted_address "
            "FROM canonical_developer_locations WHERE canonical_developer_id = ?",
            [canonical_developer_id],
        )
        row["locations"] = locs
    return row


def developer_activity_in_ecosystem(
    conn,
    ecosystem_id: int,
    canonical_developer_id: int,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 365,
) -> list[dict]:
    """Daily activity (num_commits) for a developer in an ecosystem over a day range."""
    where = "ecosystem_id = ? AND canonical_developer_id = ?"
    params: list[Any] = [ecosystem_id, canonical_developer_id]
    if start_date is not None:
        where += " AND day >= ?"
        params.append(start_date)
    if end_date is not None:
        where += " AND day <= ?"
        params.append(end_date)
    params.append(limit)
    query = f"""
        SELECT day, num_commits
        FROM eco_developer_activities
        WHERE {where}
        ORDER BY day DESC
        LIMIT ?
    """
    return fetch_all_dicts(conn, query, params)


def developer_tenure_in_ecosystem(
    conn,
    ecosystem_id: int,
    canonical_developer_id: int,
) -> list[dict]:
    """Tenure records for a developer in an ecosystem (tenure_days, category, day)."""
    query = """
        SELECT day, tenure_days, category
        FROM eco_developer_tenures
        WHERE ecosystem_id = ? AND canonical_developer_id = ?
        ORDER BY day DESC
        LIMIT 100
    """
    return fetch_all_dicts(conn, query, [ecosystem_id, canonical_developer_id])


def search_developers_in_ecosystem(
    conn,
    ecosystem_id: int,
    query_text: str,
    *,
    day: date | None = None,
    limit: int = 30,
    offset: int = 0,
) -> list[dict]:
    """Search developers by login/name within an ecosystem; paginated."""
    params: list[Any] = [ecosystem_id, f"%{query_text}%", f"%{query_text}%"]
    day_filter = ""
    if day is not None:
        day_filter = "AND ecr.day = ?"
        params.append(day)
    params.extend([limit, offset])
    query = f"""
        SELECT DISTINCT ecr.canonical_developer_id, ecr.day, ecr.points, ecr.contribution_rank,
               u.login, u.name
        FROM eco_developer_contribution_ranks ecr
        JOIN user_info u ON u.canonical_developer_id = ecr.canonical_developer_id
        WHERE ecr.ecosystem_id = ?
          AND (u.login ILIKE ? OR u.name ILIKE ?)
          {day_filter}
        ORDER BY ecr.points DESC NULLS LAST
        LIMIT ? OFFSET ?
    """
    return fetch_all_dicts(conn, query, params)
