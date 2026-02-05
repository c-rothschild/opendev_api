"""Pytest fixtures: in-memory DuckDB with minimal dashboard schema and seed data."""

from datetime import date, timedelta

import duckdb
import pytest


def _create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE ecosystems (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            launch_date DATE,
            derived_launch_date DATE,
            is_crypto UTINYINT,
            is_category UTINYINT,
            is_chain UTINYINT,
            is_multichain UTINYINT
        )
    """)
    conn.execute("""
        CREATE TABLE ecosystems_repos (
            id INTEGER,
            ecosystem_id INTEGER,
            repo_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE ecosystems_repos_recursive (
            ecosystem_id INTEGER,
            repo_id INTEGER,
            created_at TIMESTAMP,
            connected_at DATE,
            path INTEGER[],
            distance UBIGINT,
            is_explicit BOOLEAN,
            is_direct_exclusive BOOLEAN,
            is_indirect_exclusive BOOLEAN,
            exclusive_at_connection BOOLEAN,
            exclusive_till DATE
        )
    """)
    conn.execute("""
        CREATE TABLE ecosystems_child_ecosystems (
            id INTEGER,
            parent_id INTEGER,
            child_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE repos (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            link VARCHAR,
            num_stars INTEGER,
            num_forks INTEGER,
            num_issues INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE eco_mads (
            ecosystem_id INTEGER,
            day DATE,
            all_devs UBIGINT,
            exclusive_devs UBIGINT,
            multichain_devs UBIGINT,
            num_commits UBIGINT,
            devs_0_1y UBIGINT,
            devs_1_2y UBIGINT,
            devs_2y_plus UBIGINT,
            one_time_devs UBIGINT,
            part_time_devs UBIGINT,
            full_time_devs UBIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE eco_developer_contribution_ranks (
            ecosystem_id INTEGER,
            canonical_developer_id INTEGER,
            day DATE,
            points UTINYINT,
            points_28d UTINYINT,
            points_56d UTINYINT,
            contribution_rank VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE user_info (
            canonical_developer_id INTEGER PRIMARY KEY,
            login VARCHAR,
            name VARCHAR,
            company VARCHAR,
            location VARCHAR,
            url VARCHAR,
            email VARCHAR,
            primary_github_user_id VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE canonical_developer_locations (
            canonical_developer_id INTEGER,
            country VARCHAR,
            admin_level_1 VARCHAR,
            locality VARCHAR,
            lat DOUBLE,
            lng DOUBLE,
            formatted_address VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE eco_developer_activities (
            ecosystem_id INTEGER,
            canonical_developer_id INTEGER,
            day DATE,
            num_commits UBIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE eco_developer_tenures (
            ecosystem_id INTEGER,
            canonical_developer_id INTEGER,
            day DATE,
            tenure_days BIGINT,
            category UTINYINT
        )
    """)


def _seed_data(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        INSERT INTO ecosystems (id, name, launch_date, derived_launch_date, is_crypto, is_category, is_chain, is_multichain)
        VALUES
            (1, 'Bitcoin', date '2009-01-03', date '2009-01-03', 1, 0, 1, 0),
            (2, 'Ethereum', date '2015-07-30', date '2015-07-30', 1, 0, 1, 0),
            (3, 'Rust', NULL, date '2010-01-01', 0, 1, 0, 0)
    """)
    conn.execute("""
        INSERT INTO ecosystems_child_ecosystems (id, parent_id, child_id)
        VALUES (1, 1, 2)
    """)
    conn.execute("""
        INSERT INTO repos (id, name, link, num_stars, num_forks, num_issues)
        VALUES
            (10, 'bitcoin/bitcoin', 'https://github.com/bitcoin/bitcoin', 80000, 40000, 1000),
            (20, 'ethereum/go-ethereum', 'https://github.com/ethereum/go-ethereum', 50000, 20000, 500)
    """)
    conn.execute("""
        INSERT INTO ecosystems_repos (id, ecosystem_id, repo_id) VALUES (1, 1, 10), (2, 2, 20)
    """)
    conn.execute("""
        INSERT INTO ecosystems_repos_recursive (ecosystem_id, repo_id, created_at, connected_at, path, distance, is_explicit, is_direct_exclusive, is_indirect_exclusive, exclusive_at_connection, exclusive_till)
        VALUES (1, 10, current_timestamp, current_date, [1], 1, true, true, false, true, NULL)
    """)
    conn.execute("""
        INSERT INTO ecosystems_repos_recursive (ecosystem_id, repo_id, created_at, connected_at, path, distance, is_explicit, is_direct_exclusive, is_indirect_exclusive, exclusive_at_connection, exclusive_till)
        VALUES (2, 20, current_timestamp, current_date, [2], 1, true, true, false, true, NULL)
    """)
    base = date.today()
    conn.execute("""
        INSERT INTO eco_mads (ecosystem_id, day, all_devs, exclusive_devs, multichain_devs, num_commits, devs_0_1y, devs_1_2y, devs_2y_plus, one_time_devs, part_time_devs, full_time_devs)
        VALUES
            (1, ?, 2500, 1300, 100, 140000, 500, 600, 1400, 200, 800, 500),
            (1, ?, 2480, 1280, 98, 138000, 490, 610, 1380, 210, 790, 490)
    """, [base, base - timedelta(days=1)])
    conn.execute("""
        INSERT INTO eco_developer_contribution_ranks (ecosystem_id, canonical_developer_id, day, points, points_28d, points_56d, contribution_rank)
        VALUES
            (1, 100, ?, 4, 4, 4, 'full_time'),
            (1, 101, ?, 4, 4, 4, 'part_time'),
            (1, 102, ?, 1, 1, 1, 'one_time')
    """, [base, base, base])
    conn.execute("""
        INSERT INTO user_info (canonical_developer_id, login, name, company, location, url, email, primary_github_user_id)
        VALUES
            (100, 'alice', 'Alice Dev', 'Acme', 'NYC', 'https://github.com/alice', 'alice@example.com', 'U_1'),
            (101, 'bob', 'Bob Smith', NULL, 'SF', NULL, NULL, 'U_2'),
            (102, 'carol', 'Carol Lee', 'Co', 'LA', 'https://github.com/carol', NULL, 'U_3')
    """)
    conn.execute("""
        INSERT INTO canonical_developer_locations (canonical_developer_id, country, admin_level_1, locality, lat, lng, formatted_address)
        VALUES (100, 'US', 'New York', 'New York City', 40.7, -74.0, 'NYC, NY, US')
    """)
    conn.execute("""
        INSERT INTO eco_developer_activities (ecosystem_id, canonical_developer_id, day, num_commits)
        VALUES (1, 100, ?, 5), (1, 100, ?, 3)
    """, [base, base - timedelta(days=1)])
    conn.execute("""
        INSERT INTO eco_developer_tenures (ecosystem_id, canonical_developer_id, day, tenure_days, category)
        VALUES (1, 100, ?, 365, 1)
    """, [base])


@pytest.fixture
def conn():
    """In-memory DuckDB connection with minimal dashboard schema and seed data."""
    c = duckdb.connect(":memory:")
    _create_schema(c)
    _seed_data(c)
    yield c
    c.close()
