import duckdb
from datetime import date
from .get_user_info import create_user_info_table
from . import ecosystems as _ecosystems
from . import developers as _developers


class OpenDevData:
    def __init__(self, folderpath, db_filename):
        self.folderpath = folderpath
        self.db_filename = db_filename
        self.conn = duckdb.connect(f"{folderpath}/{db_filename}")

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _ensure_conn(self) -> None:
        if self.conn is None:
            raise RuntimeError("Connection is closed")

    def create_user_info_table(self, github_token: str) -> None:
        self._ensure_conn()
        try:
            create_user_info_table(self.conn, github_token)
        except Exception as e:
            raise RuntimeError(
                f"Failed to create user_info table: {e}"
            ) from e

    # --- Ecosystems ---
    def list_ecosystems(
        self,
        *,
        name_contains: str | None = None,
        is_crypto: bool | None = None,
        is_chain: bool | None = None,
        include_repo_count: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        self._ensure_conn()
        return _ecosystems.list_ecosystems(
            self.conn,
            name_contains=name_contains,
            is_crypto=is_crypto,
            is_chain=is_chain,
            include_repo_count=include_repo_count,
            limit=limit,
            offset=offset,
        )

    def get_ecosystem(self, ecosystem_id: int, *, include_latest_mads: bool = False) -> dict | None:
        self._ensure_conn()
        return _ecosystems.get_ecosystem(self.conn, ecosystem_id, include_latest_mads=include_latest_mads)

    def ecosystem_hierarchy(self, ecosystem_id: int) -> dict:
        self._ensure_conn()
        return _ecosystems.ecosystem_hierarchy(self.conn, ecosystem_id)

    def repos_in_ecosystem(
        self,
        ecosystem_id: int,
        *,
        recursive: bool = True,
        sort_by: str = "num_stars",
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        self._ensure_conn()
        return _ecosystems.repos_in_ecosystem(
            self.conn,
            ecosystem_id,
            recursive=recursive,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )

    def ecosystem_mads_time_series(
        self,
        ecosystem_id: int,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 365,
    ) -> list[dict]:
        self._ensure_conn()
        return _ecosystems.ecosystem_mads_time_series(
            self.conn,
            ecosystem_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    def search_ecosystems(self, name_query: str, *, limit: int = 30) -> list[dict]:
        self._ensure_conn()
        return _ecosystems.search_ecosystems(self.conn, name_query, limit=limit)

    def top_repos_in_ecosystem(
        self,
        ecosystem_id: int,
        *,
        recursive: bool = True,
        limit: int = 20,
    ) -> list[dict]:
        self._ensure_conn()
        return _ecosystems.top_repos_in_ecosystem(
            self.conn,
            ecosystem_id,
            recursive=recursive,
            limit=limit,
        )

    # --- Developers ---
    def developers_in_ecosystem(
        self,
        ecosystem_id: int,
        *,
        day: date | None = None,
        contribution_rank: str | None = None,
        include_user_info: bool = True,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        self._ensure_conn()
        return _developers.developers_in_ecosystem(
            self.conn,
            ecosystem_id,
            day=day,
            contribution_rank=contribution_rank,
            include_user_info=include_user_info,
            limit=limit,
            offset=offset,
        )

    def get_developer_profile(
        self,
        canonical_developer_id: int,
        *,
        include_location: bool = False,
    ) -> dict | None:
        self._ensure_conn()
        return _developers.get_developer_profile(
            self.conn,
            canonical_developer_id,
            include_location=include_location,
        )

    def developer_activity_in_ecosystem(
        self,
        ecosystem_id: int,
        canonical_developer_id: int,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 365,
    ) -> list[dict]:
        self._ensure_conn()
        return _developers.developer_activity_in_ecosystem(
            self.conn,
            ecosystem_id,
            canonical_developer_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    def developer_tenure_in_ecosystem(
        self,
        ecosystem_id: int,
        canonical_developer_id: int,
    ) -> list[dict]:
        self._ensure_conn()
        return _developers.developer_tenure_in_ecosystem(
            self.conn,
            ecosystem_id,
            canonical_developer_id,
        )

    def search_developers_in_ecosystem(
        self,
        ecosystem_id: int,
        query_text: str,
        *,
        day: date | None = None,
        limit: int = 30,
        offset: int = 0,
    ) -> list[dict]:
        self._ensure_conn()
        return _developers.search_developers_in_ecosystem(
            self.conn,
            ecosystem_id,
            query_text,
            day=day,
            limit=limit,
            offset=offset,
        )
