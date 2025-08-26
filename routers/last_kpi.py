# routers/last_kpi.py
from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from db_utils.questdb_query import QuestDBQuery
from db_utils.sqlite_db import SQLiteDB

router = APIRouter(prefix="/last_kpi", tags=["Last KPI"])
templates = Jinja2Templates(directory="templates")


def _build_frames(node_type_up: str) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """
    Trả về (final_df, latest_df, error)
      - final_df: 7 bản ghi gần nhất + EMA + nguong_fix + signal
      - latest_df: bản ghi cuối cùng mỗi KPI|Node, chỉ giữ signal=False
    """
    q = QuestDBQuery()
    sqlite = SQLiteDB()

    # 1) KPI + ngưỡng
    nguong_df = sqlite.fetch_df(
        "SELECT kpi_name, type, nguong_fix FROM config_nguong WHERE type = ?",
        (node_type_up,),
    )
    all_kpis = nguong_df["kpi_name"].dropna().astype(str).unique().tolist()
    if not all_kpis:
        return pd.DataFrame(), pd.DataFrame(), f"⚠ Chưa cấu hình KPI cho type {node_type_up}."

    # 2) Node đang bật
    node_df = sqlite.fetch_df(
        "SELECT DISTINCT node FROM config_node_schedule WHERE type = ? AND status = 1",
        (node_type_up,),
    )
    nodes = node_df["node"].astype(str).tolist()

    # 3) Bảng & cột giá trị
    table = node_type_up
    value_expr = "ratio" if node_type_up == "MME" else "kpi_value"

    # 4) Khung thời gian
    minutes = 180
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)

    # 5) IN (...) an toàn
    kpi_clause, kpi_binds = q.make_in_params("k", all_kpis)
    params: dict = {"cutoff": cutoff.isoformat(), **kpi_binds}

    node_filter_sql = ""
    if nodes:
        node_clause, node_binds = q.make_in_params("n", nodes)
        node_filter_sql = f"AND Node IN ({node_clause})"
        params.update(node_binds)

    # 6) Query QuestDB
    sql = f"""
        SELECT
          timestamp,
          Node,
          kpi_name,
          ratio AS kpi_value
        FROM {table}
        WHERE kpi_name IN ({kpi_clause})
          {node_filter_sql}
          AND timestamp > CAST(:cutoff AS TIMESTAMP)
        ORDER BY kpi_name, Node, timestamp DESC
    """
    df = q.query(sql, params=params)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), "⚠ Không có dữ liệu."

    # 7) 7 bản ghi gần nhất mỗi kpi_node
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["kpi_value"] = pd.to_numeric(df["kpi_value"], errors="coerce")
    df["kpi_node"] = df["Node"].astype(str) + "-" + df["kpi_name"].astype(str)
    df = (
        df.sort_values(["kpi_node", "timestamp"], ascending=[True, False])
          .drop_duplicates(subset=["kpi_node", "timestamp"], keep="first")
          .groupby("kpi_node", as_index=False, group_keys=False)
          .head(7)
          .sort_values(["kpi_node", "timestamp"], ascending=[True, True])
    )

    # 8) EMA
    df["ema_kpi"] = (
        df.groupby("kpi_node")["kpi_value"]
          .transform(lambda s: s.ewm(span=3, adjust=False).mean())
    )

    # 9) Merge ngưỡng
    merged = df.merge(
        nguong_df[["kpi_name", "nguong_fix"]],
        on="kpi_name",
        how="left",
    )
    merged["nguong_fix"] = pd.to_numeric(merged["nguong_fix"], errors="coerce")

    # # 10) Signal rule: False nếu (kpi_value < ema_kpi - 1) OR (kpi_value < nguong_fix)
    # merged["signal"] = ~(
    #     (merged["kpi_value"] < (merged["ema_kpi"] - 1)) |
    #     (merged["kpi_value"] < merged["nguong_fix"])
    # )

    # Final frames
    final_df = merged[["timestamp", "kpi_node", "kpi_value", "ema_kpi", "nguong_fix", "signal"]]
    latest_df = (
        final_df.groupby("kpi_node", as_index=False, group_keys=False)
                .tail(1)
    )
    latest_df = latest_df[latest_df["signal"] == False]  # chỉ cảnh báo

    # Format time for display
    final_df = final_df.copy()
    final_df["timestamp"] = pd.to_datetime(final_df["timestamp"], utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")

    latest_df = latest_df.copy()
    latest_df["timestamp"] = pd.to_datetime(latest_df["timestamp"], utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")

    return final_df, latest_df, None


@router.get("", response_class=HTMLResponse)
def last_kpi_page(request: Request, node_type: str = Query("MME")):
    # Render khung trang; htmx sẽ tự load các fragment
    return templates.TemplateResponse(
        "last_kpi.html",
        {"request": request, "title": "Latest KPI", "node_type": node_type},
    )


@router.get("/frag/latest", response_class=HTMLResponse)
def last_kpi_latest_fragment(request: Request, node_type: str = Query("MME")):
    node_type_up = node_type.upper()
    _, latest_df, error = _build_frames(node_type_up)
    if error:
        return HTMLResponse(f'<div class="alert alert-danger">{error}</div>')

    latest_html = latest_df.to_html(
        classes="table table-sm table-striped table-danger",
        index=False, border=0, justify="center"
    )
    return HTMLResponse(latest_html)


@router.get("/frag/table", response_class=HTMLResponse)
def last_kpi_table_fragment(request: Request, node_type: str = Query("MME")):
    node_type_up = node_type.upper()
    final_df, _, error = _build_frames(node_type_up)
    if error:
        return HTMLResponse(f'<div class="alert alert-danger">{error}</div>')

    table_html = final_df.to_html(
        classes="table table-bordered table-hover",
        index=False, border=0, justify="center"
    )
    return HTMLResponse(table_html)
