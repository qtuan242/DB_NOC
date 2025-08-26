# routers/kpi_analysis.py
from __future__ import annotations

import io
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from db_utils.questdb_query import QuestDBQuery
from db_utils.sqlite_db import SQLiteDB

router = APIRouter(prefix="/kpi_analysis", tags=["KPI Analysis"])
templates = Jinja2Templates(directory="templates")


def _choose_sample_by(minutes: int) -> str:
    if minutes <= 30:
        return "5s"
    if minutes <= 180:
        return "10s"
    if minutes <= 720:
        return "30s"
    if minutes <= 1440:
        return "1m"
    return "5m"


@router.get("", response_class=HTMLResponse)
def kpi_analysis_page(request: Request):
    return templates.TemplateResponse(
        "kpi_analysis.html", {"request": request, "title": "KPI Analysis"}
    )


def _load_common(node_type_up: str):
    sqlite = SQLiteDB()

    # KPI + ngưỡng
    # thr_df = sqlite.fetch_df(
    #     "SELECT kpi_name, nguong_fix FROM config_nguong WHERE type = ?",
    #     (node_type_up,),
    # )
    thr_df = sqlite.fetch_df(
        """
        SELECT kpi_name, nguong_fix
        FROM config_nguong
        WHERE type = ?
        ORDER BY DB DESC
        """,
        (node_type_up,),
    )
    if thr_df.empty:
        raise HTTPException(
            400, f"Chưa cấu hình KPI cho type {node_type_up} trong config_nguong."
        )
    kpis: List[str] = (
        thr_df["kpi_name"].dropna().astype(str).unique().tolist()
    )
    thr_map = dict(
        zip(thr_df["kpi_name"], pd.to_numeric(thr_df["nguong_fix"], errors="coerce"))
    )

    # Node đang bật
    nodes_df = sqlite.fetch_df(
        "SELECT DISTINCT node FROM config_node_schedule WHERE type = ? AND status = 1",
        (node_type_up,),
    )
    nodes: List[str] = nodes_df["node"].astype(str).tolist()
    if not nodes:
        raise HTTPException(400, f"Không có node nào đang bật cho type {node_type_up}.")

    return sqlite, kpis, thr_map, nodes


@router.get("/api")
def kpi_analysis_api(
    node_type: str = Query("MME"),
    minutes: int = Query(180, ge=5, le=60 * 24 * 14),
):
    """
    JSON cho ECharts: mỗi KPI = 1 chart (gồm series gốc cho từng Node).
    Trục X là epoch seconds (FE chuyển sang ms).
    """
    node_type_up = node_type.upper()
    q = QuestDBQuery()
    _, kpis, thr_map, nodes = _load_common(node_type_up)

    TABLE_MAP = {
        "MME": ("MME", "ratio"),                          # MME(timestamp, Node, kpi_name, ratio DOUBLE)
        "PGW": ("PGW", "ratio"),
        "SBG": ("SBG", "ratio"),# PGW(..., kpi_value TEXT/REAL)
    }
    try:
        table, val_col = TABLE_MAP[node_type_up]
    except KeyError:
        raise HTTPException(400, f"Loại node không hợp lệ: {node_type_up}")

    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    sample_by = _choose_sample_by(minutes)

    charts: List[Dict] = []

    for kpi in kpis:
        prefix = "n"
        node_clause, node_binds = QuestDBQuery.make_in_params(prefix, nodes)

        # Pivot node thành nhiều cột
        cols_sql = ",\n              ".join(
            f'MAX(CASE WHEN Node = :{prefix}{i} THEN v END) AS "{nodes[i]}"'
            for i in range(len(nodes))
        )

        sql = f"""
            WITH base AS (
              SELECT timestamp AS ts, Node, {val_col} AS v
              FROM {table}
              WHERE timestamp > CAST(:cutoff AS TIMESTAMP)
                AND kpi_name = :kpi
                AND Node IN ({node_clause})
            )
            SELECT
              ts,
              {cols_sql}
            FROM base
            TIMESTAMP(ts)
            SAMPLE BY 3m
            ORDER BY ts
        """
        params: Dict[str, str] = {"cutoff": cutoff, "kpi": kpi, **node_binds}
        df = q.query(sql, params=params)
        if df.empty:
            continue

        # Chuẩn hóa thời gian
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts"]).sort_values("ts")
        x_sec = (df["ts"].astype("int64") // 1_000_000_000).tolist()

        series_all: List[Dict] = []

        # ---- series gốc (solid) ----
        for node in nodes:
            col = pd.to_numeric(df.get(node), errors="coerce")
            vals = [None if pd.isna(v) else float(v) for v in col.tolist()]
            # đảm bảo khớp độ dài
            if len(vals) < len(x_sec):
                vals += [None] * (len(x_sec) - len(vals))
            elif len(vals) > len(x_sec):
                vals = vals[: len(x_sec)]
            series_all.append({"label": node, "data": vals})

        chart = {"kpi": kpi, "x": x_sec, "series": series_all}

        thr = thr_map.get(kpi)
        if thr is not None and pd.notna(thr):
            chart["threshold"] = {
                "label": f"nguong_fix={float(thr)}",
                "data": [float(thr)] * len(x_sec),
            }

        charts.append(chart)

    return JSONResponse(
        {"charts": charts, "node_type": node_type_up, "minutes": minutes, "sample_by": sample_by}
    )
