"""Tests for developers API."""

from datetime import date

import pytest

from opendev_api import developers


def test_developers_in_ecosystem(conn):
    rows = developers.developers_in_ecosystem(conn, 1, limit=10)
    assert len(rows) >= 1
    assert "canonical_developer_id" in rows[0]
    assert "contribution_rank" in rows[0]
    assert "points" in rows[0]


def test_developers_in_ecosystem_with_user_info(conn):
    rows = developers.developers_in_ecosystem(conn, 1, include_user_info=True, limit=10)
    assert len(rows) >= 1
    assert "login" in rows[0]
    assert "name" in rows[0]


def test_developers_in_ecosystem_filter_by_rank(conn):
    rows = developers.developers_in_ecosystem(
        conn, 1, contribution_rank="full_time", limit=10
    )
    assert all(r["contribution_rank"] == "full_time" for r in rows)


def test_get_developer_profile(conn):
    row = developers.get_developer_profile(conn, 100)
    assert row is not None
    assert row["canonical_developer_id"] == 100
    assert row["login"] == "alice"
    assert row["name"] == "Alice Dev"


def test_get_developer_profile_not_found(conn):
    row = developers.get_developer_profile(conn, 99999)
    assert row is None


def test_get_developer_profile_with_location(conn):
    row = developers.get_developer_profile(conn, 100, include_location=True)
    assert row is not None
    assert "locations" in row
    assert len(row["locations"]) >= 1
    assert row["locations"][0]["country"] == "US"


def test_developer_activity_in_ecosystem(conn):
    rows = developers.developer_activity_in_ecosystem(
        conn, 1, 100, limit=10
    )
    assert len(rows) >= 1
    assert "day" in rows[0]
    assert "num_commits" in rows[0]


def test_developer_tenure_in_ecosystem(conn):
    rows = developers.developer_tenure_in_ecosystem(conn, 1, 100)
    assert len(rows) >= 1
    assert "tenure_days" in rows[0]
    assert "category" in rows[0]


def test_search_developers_in_ecosystem(conn):
    rows = developers.search_developers_in_ecosystem(
        conn, 1, "alice", limit=10
    )
    assert len(rows) >= 1
    assert any(r["login"] == "alice" for r in rows)


def test_search_developers_in_ecosystem_by_name(conn):
    rows = developers.search_developers_in_ecosystem(
        conn, 1, "Alice", limit=10
    )
    assert len(rows) >= 1
    assert any("alice" in (r.get("login") or "").lower() or "alice" in (r.get("name") or "").lower() for r in rows)


def test_search_developers_empty(conn):
    rows = developers.search_developers_in_ecosystem(
        conn, 1, "xyznonexistent123", limit=10
    )
    assert len(rows) == 0
