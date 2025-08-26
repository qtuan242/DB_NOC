# job/worker_PGWE.py

import os
import sqlite3
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler

from db_utils.questdb_client import QuestDBClient
from jobs.module_PGWE import KPI_PGW
from config import LOG_DIR, DB_FILE


class WorkerPGW:
    def __init__(self, DB_FILE, type_filter="PGW"):
        self.db_file = DB_FILE
        self.type_filter = type_filter
        self.client = QuestDBClient()

        # Đảm bảo thư mục log tồn tại
        os.makedirs(LOG_DIR, exist_ok=True)

        # 🔹 Log rotate riêng (vd: pgw_schedule.log)
        schedule_log = os.path.join(LOG_DIR, f"{self.type_filter.lower()}_schedule.log")
        rotating_handler = RotatingFileHandler(
            schedule_log,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,
            encoding="utf-8"
        )
        rotating_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # 🔹 Log last_job (overwrite mỗi lần chạy)
        last_job_log = os.path.join(LOG_DIR, f"last_job_{self.type_filter}.log")
        last_handler = logging.FileHandler(last_job_log, mode="w", encoding="utf-8")
        last_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # Logger riêng cho worker
        self.logger = logging.getLogger(f"Worker_{self.type_filter}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []  # clear tránh bị ghi log trùng
        self.logger.addHandler(rotating_handler)
        self.logger.addHandler(last_handler)

    def run(self):
        start_time = datetime.now()
        self.logger.info(f"=== Worker {self.type_filter} started ===")

        try:
            with sqlite3.connect(self.db_file) as conn:
                rows = conn.execute("""
                    SELECT node, ip, user, password, type
                    FROM config_node_schedule
                    WHERE type = ? AND status = 1
                """, (self.type_filter,)).fetchall()

            if not rows:
                self.logger.info(f"⚠ Không có node nào có type = '{self.type_filter}'.")
                return

            # Folder log cho từng type
            node_log_dir = os.path.join(LOG_DIR, self.type_filter.upper())
            os.makedirs(node_log_dir, exist_ok=True)

            fileName = "/var/log/services/epg/pdc/work/tmp/pm_job_epg-kpi.csv"

            def run_task(row):
                node, ip, user, password, _ = row
                try:
                    self.logger.info(f"▶️ Task: {node} ({ip})")
                    KPI_PGW(node, ip, user, password, fileName, node_log_dir)

                    log_filepath = os.path.join(node_log_dir, f"log_{node}.txt")
                    if os.path.exists(log_filepath):
                        self.client.insert_from_log_PGW_db(
                            log_filepath,
                            node=node,
                            table=self.type_filter
                        )
                        self.logger.info(f"[{self.type_filter}] Node {node} ✅ Inserted log to QuestDB: {log_filepath}")
                    else:
                        self.logger.warning(f"[{self.type_filter}] Node {node} ⚠ Log file not found: {log_filepath}")
                except Exception as e:
                    self.logger.error(f"[{self.type_filter}] Node {node} failed ❌: {e}")

            # Chạy song song
            max_workers = min(len(rows), os.cpu_count() * 2, 32)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(executor.map(run_task, rows))

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"✅ Finish job {self.type_filter} : {end_time.strftime('%H:%M:%S')} ⏱ {duration:.1f}s\n")

        except Exception as e:
            self.logger.error(f" Job [{self.type_filter}] ❌ DB Error: {e}")
