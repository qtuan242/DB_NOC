# questdb_client.py

from questdb.ingress import Sender, IngressError
import datetime
import os

class QuestDBClient:
    def __init__(self,
                 host="localhost",
                 port=9000,
                 username="admin",
                 password="quest",
                 flush_rows=1,
                 flush_interval=1000):
        self.conf = (
            f"http::addr={host}:{port};"
            f"username={username};password={password};"
            f"auto_flush_rows={flush_rows};"
            f"auto_flush_interval={flush_interval};"
        )

    def insert_kpi(self, table: str, node: str, kpi_name: str, kpi_value: float, dt: datetime.datetime = None):
        try:
            if dt is None:
                dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

            with Sender.from_conf(self.conf) as sender:
                sender.row(
                    table,
                    symbols={"Node": node, "kpi_name": kpi_name},
                    columns={"kpi_value": kpi_value},
                    at=dt
                )
                sender.flush()
                print(f"✅ Inserted {kpi_name}={kpi_value} for {node} at {dt.isoformat()}")
        except IngressError as e:
            print(f"❌ Ingress error: {e}")



    def parse_datetime_from_day_time(self, day_str: str, time_str: str) -> datetime.datetime:
        try:
            today = datetime.date.today()
            year = today.year
            month = today.month
            day = int(day_str.strip())
            hour_minute = time_str.strip()[:5]  # chỉ lấy HH:MM

            dt_str = f"{year}-{month:02d}-{day:02d} {hour_minute}"
            return datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=datetime.timezone.utc)
        except Exception as e:
            raise ValueError(f"❌ Không thể parse datetime từ: Day={day_str}, Time={time_str} | {e}")

    def insert_from_log_file(self, file_path: str, node: str, table: str = "MME_Short"):
        records = []
        day = None
        time_str = None

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.lower().startswith("day"):
                    day = line.split(":")[1].strip()
                elif line.lower().startswith("time"):
                    time_str = line.split(":", 1)[1].strip()
                elif "%" in line and ":" in line:
                    line = line.replace("%", "")
                    try:
                        kpi_name, kpi_val = map(str.strip, line.split(":", 1))
                        kpi_value = 100 - float(kpi_val)

                        if day and time_str:
                            dt = self.parse_datetime_from_day_time(day, time_str)
                            print(f"{node} | {dt.isoformat()} | {kpi_name} | {kpi_value}")
                            records.append({
                                "node": node,
                                "kpi_name": kpi_name,
                                "kpi_value": kpi_value,
                                "dt": dt
                            })
                    except Exception as e:
                        print(f"❌ Lỗi dòng: {line} | {e}")

        if records:
            self.insert_bulk(table, records)
        else:
            print("⚠️ Không có bản ghi hợp lệ trong file.")


    def insert_log_to_db_PGW(self, file_path: str, table: str = "PGW_Short"):
        records = []
        print('insert_log_to_db_PGW', file_path)
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "insertDB;" in line:
                    print(line)
                    parts = line.split(";")
                    # print(parts)
                    if len(parts) >= 6:
                        print(parts)
                        node = parts[1]
                        time = parts[2]
                        # dt = datetime.datetime.strptime(f"{time}", "%Y-%m-%d %H:%M").replace(
                        #     tzinfo=datetime.timezone.utc)
                        # dt = datetime.datetime.strptime(time[:19], "%Y-%m-%dT%H:%M:%S")
                        time = time[:16].replace("T", " ")
                        dt = datetime.datetime.strptime(f"{time}", "%Y-%m-%d %H:%M").replace(
                            tzinfo=datetime.timezone.utc)
                        print(dt)
                        kpi_name = parts[3]
                        att = float(parts[4])
                        ratio = float(parts[5])
                        records.append({"node": node, "kpi_name": kpi_name, "att": att, "ratio": ratio, "dt": dt})
            print(records)

        if records:
            self.insert_bulk_pgw(table, records)
        else:
            print("⚠️ Không có bản ghi hợp lệ trong file.")

    def insert_bulk_pgw(self, table: str, records: list):
        try:
            with Sender.from_conf(self.conf) as sender:
                for rec in records:
                    dt = rec.get("dt", datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc))
                    sender.row(
                        table,
                        symbols={"Node": rec["node"], "kpi_name": rec["kpi_name"]},
                        columns={"att": rec["att"],"ratio": rec["ratio"]},
                        at=dt
                    )
                sender.flush()
                print(f"✅ Bulk insert {len(records)} records into {table}")
        except IngressError as e:
            print(f"❌ Ingress error: {e}")

    def insert_from_log_db(self, file_path: str, node: str, table: str = "MME_Short"):
        records = []

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "insertDB;" in line:
                    # print(line)
                    parts = line.split(";")
                    # print(parts)
                    if len(parts) == 5:

                        node = parts[1]
                        time = parts[2]
                        dt = datetime.datetime.strptime(f"{time}", "%Y-%m-%d %H:%M").replace(
                            tzinfo=datetime.timezone.utc)
                        kpi_name = parts[3]
                        kpi_value = float(parts[4])
                        records.append({"node": node, "kpi_name": kpi_name, "ratio": kpi_value, "dt": dt})

        if records:
            self.insert_bulk(table, records)
        else:
            print("⚠️ Không có bản ghi hợp lệ trong file.")

    def insert_bulk(self, table: str, records: list):
        try:
            with Sender.from_conf(self.conf) as sender:
                for rec in records:
                    dt = rec.get("dt", datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc))
                    sender.row(
                        table,
                        symbols={"Node": rec["node"], "kpi_name": rec["kpi_name"]},
                        columns={"ratio": rec["ratio"]},
                        at=dt
                    )
                sender.flush()
                print(f"✅ Bulk insert {len(records)} records into {table}")
        except IngressError as e:
            print(f"❌ Ingress error: {e}")

    def insert_from_log_PGW_db(self, file_path: str, node: str, table: str = "PGW"):
        records = []

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "insertDB;" in line:
                    # print(line)
                    parts = line.split(";")
                    # print(parts)
                    if len(parts) == 5:

                        node = parts[1]
                        time = parts[2]
                        # dt = datetime.datetime.strptime(f"{time}", "%Y-%m-%d %H:%M").replace(
                        #     tzinfo=datetime.timezone.utc)
                        dt = datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S").replace(
                            tzinfo=datetime.timezone.utc
                        )
                        kpi_name = parts[3]
                        kpi_value = float(parts[4])
                        records.append({"node": node, "kpi_name": kpi_name, "ratio": kpi_value, "dt": dt})

        if records:
            self.insert_bulk(table, records)
        else:
            print("⚠️ Không có bản ghi hợp lệ trong file.")