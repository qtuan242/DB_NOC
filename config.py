# config.py

import os
from datetime import datetime

DEBUG = True
SECRET_KEY = os.environ.get("SECRET_KEY", "my_default_secret")

# Database and logging
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_FILE = os.path.join(BASE_DIR, "db", "config_schedule.db")
LOG_SCHEDULE_FILE = os.path.join(BASE_DIR, "logs", "log_schedule.log")

# --- Config folders ---
DOWNLOAD_DIR = "downloads"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "log_schedule.log")

os.makedirs(LOG_DIR, exist_ok=True)  # đảm bảo tồn tại trước khi cấu hình logging






DIRPATH  = "/storage/no-backup/coremw/var/log/saflog/sbgLog/sbgKPIsLog"
SFTP_CMD_NOPASS  = "sudo -n su -c /usr/lib/ssh/sftp-server 2>/dev/null"

SFTP_CMD_WITHPWD = "sudo -S -p '' su -c /usr/lib/ssh/sftp-server 2>/dev/null"


def write_log_schedule(message: str):
    log_schedule_path = os.path.join(LOG_DIR, "log_schedule.log")
    with open(log_schedule_path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")
        
# PGWire DSN cho SQLAlchemy
QUESTDB_PGWIRE_DSN = os.getenv(
    "QUESTDB_PGWIRE_DSN",
    "questdb://admin:quest@localhost:8812/main"
)