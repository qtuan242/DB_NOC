# db_utils/sqlite_helper.py
import sqlite3
from typing import List, Tuple, Any

class SQLiteHelper:
    def __init__(self, db_file: str):
        self.db_file = db_file

    def _connect(self):
        return sqlite3.connect(self.db_file)

    def get_all_rows(self) -> List[Tuple]:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM config_node_schedule").fetchall()

    def get_row_by_id(self, row_id: int) -> Tuple:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM config_node_schedule WHERE id=?", (row_id,)).fetchone()

    def insert_row(self, data: Tuple[Any, ...]) -> None:
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO config_node_schedule (node, type, ip, user, password, path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, data)

    def update_row(self, data: Tuple[Any, ...]) -> None:
        with self._connect() as conn:
            conn.execute("""
                UPDATE config_node_schedule
                SET node=?, type=?, ip=?, user=?, password=?, path=?
                WHERE id=?
            """, data)

    def delete_row_by_id(self, row_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM config_node_schedule WHERE id=?", (row_id,))
