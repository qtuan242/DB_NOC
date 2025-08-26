import os
import sqlite3
import psycopg2  # dùng psycopg2 để tạo bảng QuestDB
from config import DB_FILE, QUESTDB_PGWIRE_DSN
from urllib.parse import urlparse
def parse_pg_dsn(uri: str):
    u = urlparse(uri)
    return {
        "host": u.hostname,
        "port": u.port,
        "dbname": u.path.lstrip("/"),
        "user": u.username,
        "password": u.password,
    }

def create_questdb_table_pg(sql: str):
    try:
        if QUESTDB_PGWIRE_DSN.startswith("questdb://"):
            conn_params = parse_pg_dsn(QUESTDB_PGWIRE_DSN)
            conn = psycopg2.connect(**conn_params)
        else:
            conn = psycopg2.connect(QUESTDB_PGWIRE_DSN)

        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        print("✅ QuestDB table created or already exists")
    except Exception as e:
        print(f"❌ Failed to create QuestDB table: {e}")

def init_questdb_tables():
    tables = {
        "PGW": """
            CREATE TABLE IF NOT EXISTS PGW (
                timestamp TIMESTAMP,
                Node SYMBOL,
                kpi_name SYMBOL,
                att FLOAT,
                ratio FLOAT
            ) TIMESTAMP(timestamp)
            PARTITION BY HOUR
            TTL 15 DAY
        """,
        "MME": """
            CREATE TABLE IF NOT EXISTS MME (
                timestamp TIMESTAMP,
                Node SYMBOL,
                kpi_name SYMBOL,
                ratio FLOAT
            ) TIMESTAMP(timestamp)
            PARTITION BY HOUR
            TTL 15 DAY
        """,
        "SBG": """
            CREATE TABLE IF NOT EXISTS SBG (
                timestamp TIMESTAMP,
                Node SYMBOL,
                kpi_name SYMBOL,
                ratio FLOAT
            ) TIMESTAMP(timestamp)
            PARTITION BY HOUR
            TTL 15 DAY
        """,
         "KPI": """
            CREATE TABLE IF NOT EXISTS KPI (
                timestamp TIMESTAMP,
				kpi_node SYMBOL,
                KPI FLOAT, 
				Note SYMBOL,
				Nguong SYMBOL
            ) TIMESTAMP(timestamp)
            PARTITION BY HOUR
    """
    }

    for name, sql in tables.items():
        print(f"🔧 Creating QuestDB table: {name}")
        create_questdb_table_pg(sql)


def init_db():
    """Khởi tạo database SQLite + QuestDB"""
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()

        # Bảng config_node_schedule
        cur.execute('''
            CREATE TABLE IF NOT EXISTS config_node_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node TEXT NOT NULL,
                type TEXT NOT NULL,
                ip TEXT NOT NULL,
                user TEXT NOT NULL,
                password TEXT NOT NULL,
                path TEXT NOT NULL,
                status INTEGER DEFAULT 1
            )
        ''')

        # Bảng config_nguong
        cur.execute('''
            CREATE TABLE IF NOT EXISTS config_nguong (
                kpi_name TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                DB INTEGER DEFAULT 0,
                chuky REAL,
                nguong_fix REAL,
                status INTEGER DEFAULT 1
            )
        ''')

        # Dữ liệu mặc định
        defaults = [
            ("attach_lte", "MME", 1, 3, 95.0),
            ("attach_wcdma", "MME", 1, 3, 95.0),
            ("paging_lte", "MME", 1, 3, 95.0),
            ("paging_wcdma", "MME", 1, 3, 95.0),
            ("pdp_activation_wcdma", "MME", 0, 3, 95.0),
            ("inter_mme_tau_lte", "MME", 0, 3, 95.0),
            ("intra_mme_tau_lte", "MME", 0, 3, 95.0),
            ("inter_isc_tau_lte", "MME", 0, 3, 95.0),
            ("intra_rau_wcdma", "MME", 0, 3, 95.0),
            ("israu_wcdma", "MME", 0, 3, 95.0),
            ("bearer_establishment_lte", "MME", 0, 3, 95.0),
            ("service_request_lte", "MME", 0, 3, 95.0),
            ("s1_handover_lte", "MME", 0, 3, 95.0),
            ("sau_wcdma", "MME", 0, 3, 95.0),
            ("sub_lte", "MME", 0, 3, 95.0),
        ]

        for kpi_name, type_, DB, chuky, nguong_fix in defaults:
            cur.execute("""
                INSERT OR IGNORE INTO config_nguong (kpi_name, type, DB, chuky, nguong_fix)
                VALUES (?, ?, ?, ?, ?)
            """, (kpi_name, type_, DB, chuky, nguong_fix))

        conn.commit()

    # ✅ Gọi tạo bảng QuestDB (qua psycopg2)
    init_questdb_tables()
