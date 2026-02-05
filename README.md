# API for data from [opendevdata.org](https://opendevdata.org/)

Python package for working with OpenDev DuckDB data: populating user info from GitHub and querying ecosystems and developers for dashboards.

## Features

### User info

- **Create / populate user_info:** `create_user_info_table(conn, github_token)` (or `OpenDevData.create_user_info_table(github_token)`) — fetches GitHub profile data for canonical developers and writes to the DuckDB `user_info` table. Developers are read from `canonical_developers`; only those not already in `user_info` are processed.

### Dashboard API (ecosystems & developers)

All of these are available as **OpenDevData** methods and as functions in `opendev_api.ecosystems` and `opendev_api.developers` (they take a DuckDB `conn`).

**Ecosystems**

- **List ecosystems** — Paginated list with optional filters (`name_contains`, `is_crypto`, `is_chain`) and optional repo count.
- **Get ecosystem** — By id; optionally include latest `eco_mads` row (all_devs, num_commits, etc.).
- **Ecosystem hierarchy** — Parent and child ecosystems (ids and names).
- **Repos in ecosystem** — Paginated list of repos (direct or recursive); sort by `num_stars` or name.
- **Ecosystem MADs time series** — Daily aggregates (all_devs, exclusive_devs, num_commits, full_time_devs, etc.) over a date range for charts.
- **Search ecosystems** — By name (ILIKE); limit 30 for type-ahead.
- **Top repos in ecosystem** — Top N by stars (default 20).

**Developers**

- **Developers in ecosystem** — From `eco_developer_contribution_ranks`; optional filters by day and contribution_rank (full_time / part_time / one_time); joins `user_info`; paginated.
- **Developer profile** — By `canonical_developer_id` from `user_info`; optionally include `canonical_developer_locations`.
- **Developer activity in ecosystem** — Daily commit counts over a date range.
- **Developer tenure in ecosystem** — Tenure records (tenure_days, category) for a dev in an ecosystem.
- **Search developers in ecosystem** — By login or name within an ecosystem; paginated.

See [docs/dashboard_db_analysis.md](docs/dashboard_db_analysis.md) for DB structure and feature details.

## Installation

```bash
uv sync
# or
pip install -e .
```

Optional test dependencies:

```bash
uv sync --extra test
```

## Usage

```python
from opendev_api import OpenDevData
import os

client = OpenDevData("./data", "odd.duckdb")

# Populate user_info from GitHub (requires GITHUB_TOKEN)
client.create_user_info_table(os.environ.get("GITHUB_TOKEN"))

# Dashboard: list ecosystems, get one, list repos
ecosystems = client.list_ecosystems(limit=20, include_repo_count=True)
eco = client.get_ecosystem(1, include_latest_mads=True)
repos = client.repos_in_ecosystem(1, limit=10)

# Developers in an ecosystem
devs = client.developers_in_ecosystem(1, limit=50)
profile = client.get_developer_profile(100, include_location=True)

# Time series for charts
mads = client.ecosystem_mads_time_series(1, limit=90)

client.close()
```

## Testing

```bash
uv sync --extra test
uv run pytest tests/ -v
```

Tests use an in-memory DuckDB with minimal schema and seed data (see `tests/conftest.py`). No real database file is required.

## TODO

- Interactive contribution network (navigate through repos and users to find who contributes to what)
