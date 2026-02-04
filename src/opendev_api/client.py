import duckdb
from .get_user_info import create_user_info_table

class OpenDevData:
    def __init__(self, folderpath, db_filename):
        self.folderpath = folderpath
        self.db_filename = db_filename
        self.conn = duckdb.connect(f"{folderpath}/{db_filename}")

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def create_user_info_table(self, github_token):
        if self.conn is None:
            raise RuntimeError("Connection is closed")
        try:
            create_user_info_table(self.conn, github_token)
        except Exception as e:
            raise RuntimeError(
                f"Failed to create user_info table: {e}"
            ) from e

        
