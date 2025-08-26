import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from db_utils.sqlite_db import SQLiteDB
from db_utils.questdb_query import QuestDBQuery
from datetime import datetime, timedelta, timezone
import requests  # 👈 thêm thư viện gửi HTTP
class SignalChecker:
    def __init__(self, node_type: str):
        self.node_type = node_type
        self.sqlite_client = SQLiteDB()
        self.qdb = QuestDBQuery()

        self.LOG_DIR = "logs"
        os.makedirs(self.LOG_DIR, exist_ok=True)
        self.ALERT_FILE = os.path.join(self.LOG_DIR, "alert.txt")

        # Load config threshold
        self.nguong_map = {
            row[0]: row[2]  # fix_threshold
            for row in self.sqlite_client.fetch_all(
                """
                SELECT kpi_name, chuky, nguong_fix
                FROM config_nguong
                WHERE type = ?  AND status = 1 AND DB=1 
                """,
                (self.node_type,)
            )
        }

        # Load active nodes
        self.node_list = [
            r[0] for r in self.sqlite_client.fetch_all(
                "SELECT node FROM config_node_schedule WHERE type = ? AND status = 1",
                (self.node_type,)
            )
        ]

    def fetch_last(self, all_kpis: list[str], nodes: list[str], table: str = "MME"):
        try:
            q = self.qdb  # alias cho gọn

            # 1) Khung thời gian (90 phút gần nhất)
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=280)

            # 2) KPI filter
            kpi_clause, kpi_binds = q.make_in_params("k", all_kpis)
            params: dict = {"cutoff": cutoff.isoformat(), **kpi_binds}

            # 3) Node filter
            node_filter_sql = ""
            if nodes:
                node_clause, node_binds = q.make_in_params("n", nodes)
                node_filter_sql = f"AND Node IN ({node_clause})"
                params.update(node_binds)

            # 4) Query QuestDB (gộp Node + KPI thành 1 cột)
            sql = f"""
                SELECT
                  timestamp,
                  Node || '-' || kpi_name AS kpi_node,
                  ratio AS kpi_value, 

                FROM {table}
                WHERE kpi_name IN ({kpi_clause})
                  {node_filter_sql}
                  AND timestamp > CAST(:cutoff AS TIMESTAMP)
                ORDER BY kpi_node, timestamp DESC
            """

            df = q.query(sql, params=params)
            if df.empty:
                return None

            df = (
                df.groupby("kpi_node", group_keys=False)
                .head(7)
                .reset_index(drop=True)
            )

            return df

        except Exception as e:
            print(f"[ERROR fetch_last] {e}")
            return None

    @staticmethod
    def check_signal(df: pd.DataFrame, max_n: int = 7) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # Sắp xếp dữ liệu theo node và thời gian
        df = df.sort_values(["kpi_node", "timestamp"], ascending=[True, True])

        # Mặc định ban đầu
        df["signal"] = False
        df["note"] = ""
        df["nguong"] = df.groupby("kpi_node")["kpi_value"].shift(2)-1  # shift(2) theo nhóm

        # Duyệt qua từng giá trị n
        for n in range(2, max_n):
            nguong_col = f"nguong_{n}"
            df[nguong_col] = df.groupby("kpi_node")["kpi_value"].shift(n)-1

            # Điều kiện 1: giá trị hiện tại < shift(n)
            cond = df["kpi_value"] < df[nguong_col]

            # Điều kiện 2: các điểm trung gian < điểm ở n bước trước
            for k in range(1, n):
                cond &= (
                        df.groupby("kpi_node")["kpi_value"].shift(k)
                        < df.groupby("kpi_node")["kpi_value"].shift(n)-1
                )

            # Gán kết quả nếu thỏa điều kiện
            df.loc[cond, "signal"] = True
            df.loc[cond, "note"] = f"tong so mau giam n={n - 1}"
            df.loc[cond, "nguong"] = df.groupby("kpi_node")["kpi_value"].shift(n)-1
        df.to_csv('tho.csv')
        # Lấy dòng gần nhất cho mỗi node
        result = []
        for node, group in df.groupby("kpi_node"):
            last_time = group["timestamp"].max()
            last_row = group[group["timestamp"] == last_time]
            if not last_row.empty:
                result.append(last_row.sort_values("timestamp").iloc[-1])

        result_df = pd.DataFrame(result)
        return result_df[["timestamp", "kpi_node", "kpi_value", "signal", "note", "nguong"]]


    def run(self):
        if not self.nguong_map or not self.node_list:
            return []

        all_kpis = list(self.nguong_map.keys())
        # # Fetch all KPI-node last
        df = self.fetch_last(all_kpis, self.node_list, table=self.node_type)

        # df.to_csv('step1.csv')
        signals = SignalChecker.check_signal(df, 9)
        signals.to_csv('last.csv')
        kpi_nodes_with_signal = signals[signals["signal"] == False]["kpi_node"].unique()

        if len(kpi_nodes_with_signal) > 0:
            # Giữ lại toàn bộ dòng thuộc các group có signal True
            send_df = signals[signals["kpi_node"].isin(kpi_nodes_with_signal)].copy()

            # Định dạng timestamp
            send_df["timestamp"] = pd.to_datetime(send_df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M")

            # Trích kpi_name từ kpi_node
            send_df["kpi_name"] = send_df["kpi_node"].str.extract(r'-(.*)$')[0]

            json_data = {
                "type": self.node_type,
                "affected_kpis": list(send_df["kpi_name"].unique()),
                "data": send_df.to_dict(orient="records")
            }
            print(json_data)
            # Ghi file
            import json
            file_name = f"{self.node_type}_signals.json"
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)

            print(f"✅ Gửi toàn bộ nhóm có signal=True ({len(kpi_nodes_with_signal)} nhóm)")
            print(json.dumps(json_data, indent=2, ensure_ascii=False))
            # Gửi webhook

            webhook_url = "http://10.149.240.250/webhookmailJsonKPI.php"
            try:
                resp = requests.post(webhook_url, json=json_data, timeout=10)
                print(f"📡 Webhook gửi thành công: {resp.status_code}, {resp.text}")
            except Exception as e:
                print(f"[ERROR webhook] {e}")
        else:
            print("✅ Không có signal nào = True → không gửi")

        return signals

if __name__ == "__main__":
    type_filter = "PGW"
    checker = SignalChecker(type_filter)
    alerts = checker.run()