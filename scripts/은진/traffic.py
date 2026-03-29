import requests
import os
import dotenv
import mysql.connector as mc

dotenv.load_dotenv()

# ==============================
# 🔑 환경 변수
# ==============================
SERVICE_KEY = os.getenv('PUBLIC_DATA_API_KEY')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE'),
    'autocommit': False,
}

BASE_URL = "http://apis.data.go.kr/6280000/ICRoadVolStat/NodeLink_Trfc_DD"

# ==============================
# 🗄️ 테이블 생성
# ==============================
def init_db():
    conn = mc.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    print(f"[INFO] DB 확인 완료")

    cursor.close()
    conn.close()

    conn = mc.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_sql = """
    CREATE TABLE IF NOT EXISTS traffic_raw (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,

        statDate DATE NOT NULL,
        roadName VARCHAR(100),
        linkID VARCHAR(20) NOT NULL,
        direction VARCHAR(10) NOT NULL,

        startName VARCHAR(100),
        endName VARCHAR(100),

        hour00 INT, hour01 INT, hour02 INT,
        hour03 INT, hour04 INT, hour05 INT,
        hour06 INT, hour07 INT, hour08 INT,
        hour09 INT, hour10 INT, hour11 INT,
        hour12 INT, hour13 INT, hour14 INT,
        hour15 INT, hour16 INT, hour17 INT,
        hour18 INT, hour19 INT, hour20 INT,
        hour21 INT, hour22 INT, hour23 INT,

        UNIQUE KEY uniq_traffic (statDate, linkID, direction)
    );
    """

    cursor.execute(create_sql)
    conn.commit()

    print("[INFO] traffic_raw 테이블 준비 완료")

    return conn, cursor

# ==============================
# 📥 API 함수
# ==============================
def fetch_data(date):
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 3000,
        "YMD": date,
        "type": "json"
    }
    res = requests.get(BASE_URL, params=params)
    return res.json()

# ==============================
# 🔄 데이터 변환
# ==============================
def extract_items(data):
    if 'response' not in data:
        print("❌ response 없음:", data)
        return []

    items = data['response']['body']['items']

    # items가 dict일 경우 대비
    if isinstance(items, dict):
        items = [items]

    result = []

    for item in items:
        row = [
            item.get("statDate"),
            item.get("roadName"),
            item.get("linkID"),
            item.get("direction"),
            item.get("startName"),
            item.get("endName")
        ]

        for i in range(24):
            key = f"hour{str(i).zfill(2)}"
            row.append(int(item.get(key, 0)))

        result.append(tuple(row))

    return result

# ==============================
# 🚀 실행
# ==============================
conn, cursor = init_db()

insert_sql = """
INSERT IGNORE INTO traffic_raw (
    statDate, roadName, linkID, direction,
    startName, endName,
    hour00, hour01, hour02, hour03, hour04, hour05,
    hour06, hour07, hour08, hour09, hour10, hour11,
    hour12, hour13, hour14, hour15, hour16, hour17,
    hour18, hour19, hour20, hour21, hour22, hour23
)
VALUES (
    %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
"""

from datetime import datetime, timedelta

def generate_dates(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)

    while start <= end:
        yield start.strftime("%Y%m%d")
        start += timedelta(days=1)

total = 0

for date in generate_dates(2023):
    print(f"[INFO] {date}")

    try:
        data = fetch_data(date)
        rows = extract_items(data)

        if not rows:
            print("[WARN] 데이터 없음")
            continue

        cursor.executemany(insert_sql, rows)
        total += len(rows)

        if total % 1000 < len(rows):
            conn.commit()
            print(f"[INFO] commit (누적 {total})")

    except Exception as e:
        print(f"[ERROR] {date}: {e}")
        conn.rollback()

conn.commit()

cursor.close()
conn.close()

print(f"[DONE] 총 입력: {total}")