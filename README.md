📊 DB_NOC

DashBoard KPI / ALARM

🔹 Giới thiệu

DB_NOC là hệ thống Dashboard để thu thập, lưu trữ và hiển thị KPI / ALARM của NOC.
Project sử dụng Python (FastAPI) làm backend/frontend và QuestDB / SQLite để lưu trữ dữ liệu dạng time-series.

🔹 Yêu cầu hệ thống

Python 3.10+ (khuyến nghị 3.12)

QuestDB >= 9.0.2

pip + virtualenv (khuyến nghị)

🔹 Cài đặt
1️⃣ Clone project
git clone https://github.com/qtuan242/DB_NOC.git
cd DB_NOC

2️⃣ Tạo môi trường ảo & kích hoạt

Linux / macOS

python3 -m venv venv
source venv/bin/activate

3️⃣ Cài dependencies
pip install -r requirements.txt