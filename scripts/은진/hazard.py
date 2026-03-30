import requests
import os
import dotenv
import mysql.connector as mc
from datetime import datetime

dotenv.load_dotenv()

SERVICE_KEY = os.getenv('HARZARD_API_KEY')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE'),
    'autocommit': False,
}

BASE_URL = "http://apis.data.go.kr/6280000/incheon-road-hazard/hazard-list"

# DB 연결
conn = mc.connect(**DB_CONFIG)
cursor = conn.cursor()

# =========================
# API 호출
# =========================
def fetch_data(pageNo=1):
    url = f"{BASE_URL}?serviceKey={SERVICE_KEY}&pageNo={pageNo}&numOfRows=1000"
    return requests.get(url).json()

# =========================
# 데이터 변환
# =========================
def parse_items(data):
    items = data.get("items", [])
    roads = []
    hazards = []

    for item in items:
        link_id = item.get("link_id")

        # 👉 도로 테이블용 (중복 제거)
        roads.append((
            link_id,
            item.get("road_name"),
            item.get("road_type")
        ))

        # 👉 위험 데이터
        hazards.append((
            link_id,
            int(item.get("hazard_grade", 0)),
            int(item.get("hazard_count", 0)),

            float(item.get("car_speed", 0)),
            float(item.get("car_vibrate_x", 0)),
            float(item.get("car_vibrate_y", 0)),
            float(item.get("car_vibrate_z", 0)),

            item.get("hazard_type"),
            item.get("hazard_state"),

            safe_datetime(item.get("created_at"))
        ))

    return roads, hazards

# datetime 처리
def safe_datetime(dt):
    try:
        return datetime.fromisoformat(dt.replace("Z", ""))
    except:
        return None

# =========================
# INSERT 쿼리
# =========================

# 🔥 도로 (중복 제거)
road_query = """
INSERT IGNORE INTO road_info (link_id, road_name, road_type)
VALUES (%s, %s, %s)
"""

# 🔥 위험 데이터 (중복 방지)
hazard_query = """
INSERT IGNORE INTO hazard_data (
    link_id,
    hazard_grade,
    hazard_count,
    car_speed,
    car_vibrate_x,
    car_vibrate_y,
    car_vibrate_z,
    hazard_type,
    hazard_state,
    created_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# =========================
# 실행
# =========================

total = 0

for page in range(1, 50):
    print(f"[INFO] page {page}")

    data = fetch_data(page)
    roads, hazards = parse_items(data)

    if not hazards:
        break

    try:
        cursor.executemany(road_query, roads)
        cursor.executemany(hazard_query, hazards)

        conn.commit()
        total += len(hazards)

        print(f"[INFO] inserted {len(hazards)}")

    except Exception as e:
        print("[ERROR]", e)
        conn.rollback()

cursor.close()
conn.close()

