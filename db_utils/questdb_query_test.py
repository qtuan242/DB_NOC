
import requests
import pandas as pd
from datetime import datetime

class QuestDBQuery:
    def __init__(self, questdb_url="http://localhost:9000/exec"):
        self.url = questdb_url

    def query(self, sql: str) -> pd.DataFrame:
        print(f"⏳ Start query at: {datetime.now().isoformat()}")

        try:
            response = requests.get(self.url, params={"query": sql})
            # print(f"✅ HTTP {response.status_code} OK")

            if response.status_code != 200:
                raise ValueError(f"❌ HTTP Error: {response.status_code}")

            data = response.json()
            # print(f"[Raw JSON] {str(data)[:1000]}")  # Giới hạn in để tránh quá dài

            # Kiểm tra dữ liệu hợp lệ
            if 'dataset' not in data or 'columns' not in data:
                print(f"[DEBUG JSON] {data}")
                raise ValueError("⚠️ Không có dữ liệu hợp lệ từ QuestDB")

            columns = [col['name'] for col in data['columns']]
            dataset = data['dataset']

            df = pd.DataFrame(dataset, columns=columns)

            # print(f"✅ Retrieved {len(df)} rows")
            print(f"⏱ End query at: {datetime.now().isoformat()} (Elapsed: TODO calc)")

            return df

        except Exception as e:
            raise RuntimeError(f"❌ Lỗi khi truy vấn QuestDB: {e}")

# Test thủ công
if __name__ == "__main__":
    q = QuestDBQuery()
    df = q.query("SELECT * FROM MME_Short WHERE kpi_name = 'attach_lte' ")
    # df = q.query("SELECT * FROM MME_Short  LIMIT 2")
    print(df)
