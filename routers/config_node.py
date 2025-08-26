# routers/config_node.py

import sqlite3
from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from config import DB_FILE

router = APIRouter(prefix="/config", tags=["ConfigNode"])
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
def list_nodes(request: Request):
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("SELECT * FROM config_node_schedule").fetchall()
    return templates.TemplateResponse(
        "edit_config.html",
        {"request": request, "rows": rows}
    )


@router.post("/add_or_update")
def add_or_update_node(
    request: Request,
    id:       str  = Form(""),       # <- nhận id dưới dạng chuỗi
    node:     str  = Form(...),
    type:     str  = Form(...),
    ip:       str  = Form(...),
    user:     str  = Form(...),
    password: str  = Form(...),
    path:     str  = Form(...),
    status:   bool = Form(False),
):
    # Convert id nếu có giá trị, else None => INSERT
    try:
        id_val = int(id)
    except ValueError:
        id_val = None

    # SQLite boolean: 1/0
    is_status = 1 if status else 0

    values = (node, type, ip, user, password, path, is_status)
    with sqlite3.connect(DB_FILE) as conn:
        if id_val:
            conn.execute("""
                UPDATE config_node_schedule
                SET node=?, type=?, ip=?, user=?, password=?, path=?, status=?
                WHERE id=?
            """, values + (id_val,))
        else:
            conn.execute("""
                INSERT INTO config_node_schedule
                (node, type, ip, user, password, path, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, values)

    return RedirectResponse("/config", status_code=303)


@router.get("/delete/{id}")
def delete_node(id: int):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM config_node_schedule WHERE id=?", (id,))
    return RedirectResponse("/config", status_code=303)
