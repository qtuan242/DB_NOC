# routers/log_kpi.py

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from db_utils.questdb_query import QuestDBQuery
from datetime import datetime

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Khởi tạo client QuestDB
qdb = QuestDBQuery()

@router.get("/LogKPI", response_class=HTMLResponse)
def view_log_kpi(
    request: Request,
    node_type: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    error_msg = None

    # Validate datetime
    start_dt, end_dt = None, None
    try:
        if start:
            start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        if end:
            end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        if start_dt and end_dt and start_dt > end_dt:
            error_msg = "⛔ Thời gian bắt đầu phải nhỏ hơn hoặc bằng thời gian kết thúc."
    except ValueError:
        error_msg = "⛔ Định dạng ngày không hợp lệ (dùng YYYY-MM-DD HH:MM:SS)."

    rows, cols = [], []
    if not error_msg:  # chỉ query khi không có lỗi
        sql = """
            SELECT timestamp, kpi_node, KPI, Note, Nguong
              FROM KPI
             WHERE 1=1
        """
        params = {"limit": limit}

        if node_type:
            sql += " AND kpi_node = :node_type"
            params["node_type"] = node_type

        if start:
            sql += " AND timestamp >= cast(:start as timestamp)"
            params["start"] = start
        if end:
            sql += " AND timestamp <= cast(:end as timestamp)"
            params["end"] = end

        sql += " ORDER BY timestamp DESC LIMIT :limit"

        df = qdb.query(sql, params)
        rows, cols = df.values.tolist(), df.columns.tolist()

    return templates.TemplateResponse(
        "logkpi.html",
        {
            "request": request,
            "cols": cols,
            "rows": rows,
            "node_type": node_type or "",
            "start": start or "",
            "end": end or "",
            "limit": limit,
            "error_msg": error_msg,
        }
    )
