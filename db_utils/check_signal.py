import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from db_utils.sqlite_db import SQLiteDB
from db_utils.questdb_query import QuestDBQuery

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
                SELECT kpi_name, nguong_ema, nguong_fix
                FROM config_nguong
                WHERE type = ?
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

    def fetch_last_7(self, kpi_name: str, node_name: str):
        try:
            query = f'''
                SELECT timestamp, Node, kpi_name, ratio
                FROM MME
                WHERE kpi_name = '{kpi_name}' AND Node = '{node_name}'
                ORDER BY timestamp DESC
                LIMIT 15
            '''
            df = self.qdb.query(query)
            print(df)
            if df.empty:
                return None
            df = df.drop_duplicates(subset=['timestamp']).head(7)
            df['ratio'] = f"{node_name}-{kpi_name}"
            return df
        except Exception:
            return None

    def run(self):
        if not self.nguong_map or not self.node_list:
            return []

        all_kpis = list(self.nguong_map.keys())

        # Fetch all KPI-node data in parallel
        tasks = [(k, n) for k in all_kpis for n in self.node_list]

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(lambda p: self.fetch_last_7(*p), tasks)

        dfs = [df for df in results if df is not None]
        if not dfs:
            return []

        final_df = pd.concat(dfs, ignore_index=True)
        print(print)
        final_df['timestamp'] = pd.to_datetime(final_df['timestamp'])
        final_df = final_df.sort_values(by=['kpi_node', 'timestamp'])

        # EMA KPI
        final_df['ema_kpi'] = (
            final_df.groupby('kpi_node')['ratio']
            .transform(lambda s: s.ewm(span=3, adjust=False).mean())
        )

        # Latest record per KPI-node
        latest_df = final_df.groupby('kpi_node').tail(1)
        print(latest_df)
        alerts = []
        for _, row in latest_df.iterrows():
            kpi_name = row['kpi_name']
            node_name = row['Node']
            fix_threshold = self.nguong_map[kpi_name]

            if row['ratio'] < row['ema_kpi'] or \
               (fix_threshold and row['ratio'] < fix_threshold):
                alert_data = {
                    "node": node_name,
                    "kpi": kpi_name,
                    "ratio": round(float(row['ratio']), 2),
                    "ema_kpi": round(float(row['ema_kpi']), 2),
                    "fix_threshold": fix_threshold,
                    "timestamp": row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                }
                alerts.append(alert_data)

                print(
                    f"🚨 [{alert_data['timestamp']}] {alert_data['node']} | "
                    f"{alert_data['kpi']} | ratio={alert_data['ratio']} "
                    f"(EMA={alert_data['ema_kpi']}) < fix_threshold={alert_data['fix_threshold']}"
                )

        # Save alerts to file
        if alerts:
            with open(self.ALERT_FILE, "a", encoding="utf-8") as f:
                f.write(f"\n===== {datetime.now().isoformat()} - {self.node_type} Alerts =====\n")
                for a in alerts:
                    f.write(
                        f"[{a['timestamp']}] {a['node']} | {a['kpi']} | "
                        f"ratio={a['ratio']} (EMA={a['ema_kpi']}) "
                        f"< fix_threshold={a['fix_threshold']}\n"
                    )

        return alerts
