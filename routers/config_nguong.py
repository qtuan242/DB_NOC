# routers/config_nguong.py

from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from db_utils.sqlite_db import SQLiteDB

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# khởi tạo SQLiteDB
db = SQLiteDB()

@router.get("/config_nguong")
def view_config_nguong(request: Request):
    rows = db.fetch_all("""
        SELECT rowid, kpi_name, chuky, nguong_fix, type, DB, status
          FROM config_nguong
         ORDER BY kpi_name
    """)
    return templates.TemplateResponse("config_nguong.html", {"request": request, "rows": rows})

@router.post("/config_nguong")
def config_nguong_add_or_update(
    request: Request,
    id: str = Form(None),
    kpi_name: str = Form(...),
    chuky: float = Form(None),
    nguong_fix: float = Form(None),
    type: str = Form(...),
    DB: str = Form(None),
    status: bool = Form(False)
):
    db_val = 1 if DB else 0
    status_val = 1 if status else 0

    if id:
        # update
        db.execute("""
            UPDATE config_nguong
               SET kpi_name = ?, chuky = ?, nguong_fix = ?, type = ?, DB = ?, status = ?
             WHERE rowid = ?
        """, (kpi_name, chuky, nguong_fix, type, db_val, status_val, id))
    else:
        # insert
        db.execute("""
            INSERT INTO config_nguong(kpi_name, chuky, nguong_fix, type, DB, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (kpi_name, chuky, nguong_fix, type, db_val, status_val))

    return RedirectResponse(url="/config_nguong", status_code=303)

@router.get("/config_nguong/delete/{id}")
def config_nguong_delete(id: int):
    db.execute("DELETE FROM config_nguong WHERE rowid = ?", (id,))
    return RedirectResponse(url="/config_nguong", status_code=303)
