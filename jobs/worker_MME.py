# job/worker_MME.py
import os
import sqlite3
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler

from jobs.ssh_module import Kpi_MME
from db_utils.questdb_client import QuestDBClient
from db_utils.check_signal import SignalChecker
from config import LOG_DIR, DIRPATH, LOG_FILE, DB_FILE


class WorkerMME:
    def __init__(self, DB_FILE, type_filter="MME"):
        self.db_file = DB_FILE
        self.type_filter = type_filter
        self.client = QuestDBClient()
        # self.checker = SignalChecker(self.type_filter)

        # Đảm bảo thư mục log tồn tại
        os.makedirs(LOG_DIR, exist_ok=True)

        # 🔹 Log riêng cho từng loại (rotate)
        schedule_log = os.path.join(LOG_DIR, f"{self.type_filter.lower()}_schedule.log")
        rotating_handler = RotatingFileHandler(
            schedule_log,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5,
            encoding="utf-8"
        )
        rotating_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # 🔹 Log last_job (overwrite mỗi lần)
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
        """Run scheduled KPI tasks from the configuration database for specific type."""
        start_time = datetime.now()
        self.logger.info(f"=== Worker {self.type_filter} started ===")

        try:
            with sqlite3.connect(self.db_file) as conn:
                rows = conn.execute(
                    '''
                    SELECT node, ip, user, password, path, type 
                    FROM config_node_schedule
                    WHERE type = ? AND status = 1
                    ''',
                    (self.type_filter,),
                ).fetchall()

                # 🔹 Lấy danh sách KPI từ config_nguong
                kpi_list = conn.execute(
                    '''
                    SELECT kpi_name 
                    FROM config_nguong
                    WHERE type = ? AND status = 1
                    ''',
                    (self.type_filter,)
                ).fetchall()
                kpi_list = [row[0] for row in kpi_list]

            if not rows:
                self.logger.info(f"⚠ Không có node nào có type = '{self.type_filter}'.")
                return

            def run_task(row):
                node, ip, user, password, path, type_ = row
                try:


                    # 🔻 Tạo folder nếu chưa có
                    log_dir = f"{LOG_DIR}/{self.type_filter}"
                    os.makedirs(log_dir, exist_ok=True)
                    self.logger.info(f"▶️ Task: {node} ({ip})")

                    log_filepath = os.path.join(LOG_DIR, f"{self.type_filter}/log_{node}.txt")

                    print("log_filepath", log_filepath)
                    Kpi_MME(node, ip, user, password, kpi_list, log_filepath)  # ssh2 node và lọc KPI list + lưu log file LOG_DIR
                    self.client.insert_from_log_db(
                        log_filepath,
                        node=node,
                        table=self.type_filter
                    )
                    self.logger.info(f"[{self.type_filter}] Node {node} done ✅: Inserted log to QuestDB {log_filepath}")
                except Exception as e:
                    self.logger.error(f"[{self.type_filter}] Node {node} failed ❌: {e}")

            # Chạy song song các node
            max_workers = min(len(rows), os.cpu_count() * 2, 32)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(executor.map(run_task, rows))
            max_workers = min(len(rows), os.cpu_count() * 2, 32)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                executor.map(run_task, rows)

                # Đợi tất cả task kết thúc
                for future in futures:
                    future.result()
            checker = SignalChecker(self.type_filter)
            checker.run()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"✅ Finish job {self.type_filter} : {end_time.strftime('%H:%M:%S')} ⏱ {duration:.1f}s\n")

        except Exception as e:
            self.logger.error(f" Job [{self.type_filter}] ❌ DB Error: {e}")
