# # db_utils/sqlite_db.py
#
# import sqlite3
# from config import DB_FILE
#
# class SQLiteDB:
#     def __init__(self, db_path: str = DB_FILE):
#         self.db_path = db_path
#
#     def fetch_all(self, query: str, params: tuple = ()) -> list[tuple]:
#         with sqlite3.connect(self.db_path) as conn:
#             return conn.execute(query, params).fetchall()
#
#     def fetch_one(self, query: str, params: tuple = ()) -> tuple:
#         with sqlite3.connect(self.db_path) as conn:
#             return conn.execute(query, params).fetchone()
#
#     def execute(self, query: str, params: tuple = ()) -> None:
#         with sqlite3.connect(self.db_path) as conn:
#             conn.execute(query, params)
#             conn.commit()

# db_utils/sqlite_db.py
import sqlite3
from typing import Iterable, Optional
import pandas as pd
from config import DB_FILE

class SQLiteDB:
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # để lấy tên cột
        return conn

    def fetch_all(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(query, params).fetchall()

    def fetch_one(self, query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(query, params).fetchone()

    def fetch_df(self, query: str, params: tuple = ()) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    def execute(self, query: str, params: tuple = ()) -> None:
        with self._connect() as conn:
            conn.execute(query, params)
            conn.commit()

    def executemany(self, query: str, seq_of_params: Iterable[tuple]) -> None:
        with self._connect() as conn:
            conn.executemany(query, seq_of_params)
            conn.commit()
