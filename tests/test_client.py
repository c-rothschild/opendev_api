"""Tests for OpenDevData client (dashboard methods delegate to conn)."""

from datetime import date
import pytest

from opendev_api import OpenDevData


def test_list_ecosystems_via_client(conn):
    client = OpenDevData.__new__(OpenDevData)
    client.conn = conn
    rows = client.list_ecosystems(limit=10)
    assert len(rows) == 3


def test_get_ecosystem_via_client(conn):
    client = OpenDevData.__new__(OpenDevData)
    client.conn = conn
    row = client.get_ecosystem(1)
    assert row is not None
    assert row["name"] == "Bitcoin"


def test_developers_in_ecosystem_via_client(conn):
    client = OpenDevData.__new__(OpenDevData)
    client.conn = conn
    rows = client.developers_in_ecosystem(1, limit=10)
    assert len(rows) >= 1


def test_get_developer_profile_via_client(conn):
    client = OpenDevData.__new__(OpenDevData)
    client.conn = conn
    row = client.get_developer_profile(100)
    assert row is not None
    assert row["login"] == "alice"


def test_close_sets_conn_to_none(conn):
    client = OpenDevData.__new__(OpenDevData)
    client.conn = conn
    client.close()
    assert client.conn is None


def test_raises_when_conn_closed():
    client = OpenDevData.__new__(OpenDevData)
    client.conn = None
    with pytest.raises(RuntimeError, match="Connection is closed"):
        client.list_ecosystems()
