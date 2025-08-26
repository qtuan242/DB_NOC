# main.py

import os
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db_utils.init_db import init_db   # 👈 import từ file riêng
from config import DB_FILE, LOG_SCHEDULE_FILE, DEBUG, SECRET_KEY
from routers.config_node import router as config_node_router
from routers.config_nguong import router as config_nguong_router
from routers.kpi_analysis import router as kpi_analysis_router
from routers.last_kpi import router as last_kpi_router
from routers.log_kpi import router as log_kpi_router
from jobs.worker_PGWE import WorkerPGW
from jobs.worker_SBG import WorkerSBG
from jobs.worker_MME import WorkerMME
from datetime import datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# # === Scheduler setup ===
# scheduler = AsyncIOScheduler()
# # Định nghĩa job và đăng ký
#
# def scheduled_task():
#     # from jobs.job_schedule_3m import run_schedule
#     # run_schedule(DB_FILE, type_filter="PGW")
#     from jobs.download_script import run_schedule
#
#     run_schedule(DB_FILE)
# scheduler.add_job(scheduled_task, "interval", minutes=15)
# # === Scheduler setup ===

scheduler = AsyncIOScheduler()

def run_mme_job():
    WorkerMME(DB_FILE, type_filter="MME").run()

def run_pgw_job():
    WorkerPGW(DB_FILE, type_filter="PGW").run()

def run_sbg_job():
    WorkerSBG(DB_FILE, type_filter="SBG").run()


# scheduler.add_job(lambda: run_mme_job(),"cron",minute='2-59/5',id="worker-mme",coalesce=True,max_instances=1,replace_existing=True,next_run_time=datetime.now(),misfire_grace_time=60)
# scheduler.add_job(lambda: run_pgw_job(),"cron",minute='2-59/5',id="worker-pgw",coalesce=True,max_instances=1,replace_existing=True,next_run_time=datetime.now(),misfire_grace_time=60)
# scheduler.add_job(lambda: run_sbg_job(),"cron",minute='1-59/5',id="worker-sbg",coalesce=True,max_instances=1,replace_existing=True,next_run_time=datetime.now(),misfire_grace_time=60)





@app.on_event("startup")
def on_startup():
    init_db()
    scheduler.start()




# === Root redirect (optional) ===
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/last_kpi")

# === Include routers ===
app.include_router(config_node_router)
app.include_router(kpi_analysis_router)
app.include_router(log_kpi_router)
app.include_router(config_nguong_router)

# === Serve schedule log (if you still want standalone) ===
@app.get("/log_schedule")
def view_log_schedule(request: Request):
    log = ""
    if os.path.exists(LOG_SCHEDULE_FILE):
        with open(LOG_SCHEDULE_FILE, encoding="utf-8") as f:
            log = f.read()
    return templates.TemplateResponse("log_schedule.html", {"request": request, "log": log})

# === Run with Uvicorn ===
if __name__ == "__main__":
    # import uvicorn
    # uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=DEBUG)
    import uvicorn

    port = int(os.environ.get("PORT", "8090"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
