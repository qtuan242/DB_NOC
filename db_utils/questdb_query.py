# db_utils/questdb_query.py
from __future__ import annotations

from typing import Optional, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from config import QUESTDB_PGWIRE_DSN  # <-- lấy DSN từ config


class QuestDBQuery:
    """
    Client QuestDB dùng PGWire (SQLAlchemy). Phù hợp:
    - Dữ liệu không quá lớn nhưng truy vấn lặp nhiều (pooling).
    - Cần bind params an toàn.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,          # <-- giờ là Optional, mặc định lấy từ config
        *,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_pre_ping: bool = True,
        pool_recycle: int = 300,
        connect_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        effective_dsn = dsn or QUESTDB_PGWIRE_DSN
        self.engine: Engine = create_engine(
            effective_dsn,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=pool_pre_ping,
            pool_recycle=pool_recycle,
            connect_args=connect_args or {},
        )

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with self.engine.begin() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        with self.engine.begin() as conn:
            return conn.execute(text(sql), params or {})

    @staticmethod
    def make_in_params(prefix: str, values: list[str]) -> tuple[str, Dict[str, Any]]:
        binds = {f"{prefix}{i}": v for i, v in enumerate(values)}
        clause = ", ".join(f":{k}" for k in binds)
        return clause, binds
