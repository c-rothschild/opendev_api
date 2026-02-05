"""Tests for ecosystems API."""

from datetime import date, timedelta

import pytest

from opendev_api import ecosystems


def test_list_ecosystems(conn):
    rows = ecosystems.list_ecosystems(conn, limit=10)
    assert len(rows) == 3
    names = [r["name"] for r in rows]
    assert "Bitcoin" in names
    assert "Ethereum" in names
    assert "Rust" in names


def test_list_ecosystems_with_repo_count(conn):
    rows = ecosystems.list_ecosystems(conn, include_repo_count=True, limit=10)
    assert len(rows) == 3
    for r in rows:
        assert "repo_count" in r
        assert r["repo_count"] >= 0


def test_list_ecosystems_filter_is_crypto(conn):
    rows = ecosystems.list_ecosystems(conn, is_crypto=True, limit=10)
    assert len(rows) == 2
    assert all(r["is_crypto"] == 1 for r in rows)


def test_list_ecosystems_filter_name_contains(conn):
    rows = ecosystems.list_ecosystems(conn, name_contains="Eth", limit=10)
    assert len(rows) == 1
    assert rows[0]["name"] == "Ethereum"


def test_list_ecosystems_pagination(conn):
    page1 = ecosystems.list_ecosystems(conn, limit=2, offset=0)
    page2 = ecosystems.list_ecosystems(conn, limit=2, offset=2)
    assert len(page1) == 2
    assert len(page2) == 1
    assert page1[0]["id"] != page2[0]["id"]


def test_get_ecosystem(conn):
    row = ecosystems.get_ecosystem(conn, 1)
    assert row is not None
    assert row["id"] == 1
    assert row["name"] == "Bitcoin"


def test_get_ecosystem_not_found(conn):
    row = ecosystems.get_ecosystem(conn, 99999)
    assert row is None


def test_get_ecosystem_with_latest_mads(conn):
    row = ecosystems.get_ecosystem(conn, 1, include_latest_mads=True)
    assert row is not None
    assert "latest_mads" in row
    assert "all_devs" in row["latest_mads"]
    assert "num_commits" in row["latest_mads"]


def test_ecosystem_hierarchy(conn):
    result = ecosystems.ecosystem_hierarchy(conn, 1)
    assert "parents" in result
    assert "children" in result
    # We seeded parent_id=1, child_id=2 so ecosystem 1 has child 2
    assert len(result["children"]) >= 1
    child_ids = [c["child_id"] for c in result["children"]]
    assert 2 in child_ids


def test_repos_in_ecosystem(conn):
    rows = ecosystems.repos_in_ecosystem(conn, 1, recursive=True, limit=10)
    assert len(rows) >= 1
    assert any(r["name"] == "bitcoin/bitcoin" for r in rows)


def test_repos_in_ecosystem_sort_by_stars(conn):
    rows = ecosystems.repos_in_ecosystem(conn, 1, sort_by="num_stars", limit=10)
    assert len(rows) >= 1
    if len(rows) >= 2:
        assert rows[0]["num_stars"] >= rows[1]["num_stars"]


def test_ecosystem_mads_time_series(conn):
    rows = ecosystems.ecosystem_mads_time_series(conn, 1, limit=10)
    assert len(rows) >= 1
    assert "day" in rows[0]
    assert "all_devs" in rows[0]
    assert "num_commits" in rows[0]


def test_ecosystem_mads_time_series_date_range(conn):
    end = date.today()
    start = end - timedelta(days=7)
    rows = ecosystems.ecosystem_mads_time_series(
        conn, 1, start_date=start, end_date=end, limit=10
    )
    assert len(rows) >= 1
    for r in rows:
        assert start <= r["day"] <= end


def test_search_ecosystems(conn):
    rows = ecosystems.search_ecosystems(conn, "bit", limit=10)
    assert len(rows) >= 1
    assert any("bit" in r["name"].lower() for r in rows)


def test_search_ecosystems_empty(conn):
    rows = ecosystems.search_ecosystems(conn, "xyznonexistent", limit=10)
    assert len(rows) == 0


def test_top_repos_in_ecosystem(conn):
    rows = ecosystems.top_repos_in_ecosystem(conn, 1, limit=5)
    assert len(rows) >= 1
    assert rows[0]["num_stars"] >= (rows[-1]["num_stars"] or 0)
