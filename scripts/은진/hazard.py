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

BASE_URL = "http://apis.data.go.kr/6280000/incheon-road-hazard/hazard-list"

# ==============================
# 🗄️ DB & 테이블 생성
# ==============================
def init_db():
    conn = mc.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    cursor = conn.cursor()

    # DB 없으면 생성
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    print(f"[INFO] DB 생성/확인 완료: {DB_CONFIG['database']}")

    cursor.close()
    conn.close()

    # DB 다시 연결
    conn = mc.connect(**DB_CONFIG)
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hazard_raw (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,

        link_id VARCHAR(20),
        road_name VARCHAR(100),

        hazard_grade INT,
        hazard_count INT,

        latitude FLOAT,
        longitude FLOAT,

        road_type VARCHAR(10),
        road_rank VARCHAR(10),

        created_at DATETIME,

        UNIQUE KEY uniq_hazard (link_id, created_at)
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()

    print("[INFO] hazard_raw 테이블 준비 완료")

    return conn, cursor

# ==============================
# 📥 API 함수
# ==============================
def fetch_data(page):
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page
    }
    res = requests.get(BASE_URL, params=params)
    return res.json()

def extract_items(data):
    if 'items' not in data:
        print("❌ items 없음:", data)
        return []

    result = []

    for item in data['items']:
        result.append((
            item.get("link_id"),
            item.get("road_name"),
            int(item.get("hazard_grade", 0)),
            int(item.get("hazard_count", 0)),
            float(item.get("latitude", 0)),
            float(item.get("longitude", 0)),
            item.get("road_type"),
            item.get("road_rank"),
            item.get("created_at")
        ))

    return result

# ==============================
# 🚀 실행
# ==============================
conn, cursor = init_db()

insert_sql = """
INSERT IGNORE INTO hazard_raw (
    link_id, road_name, hazard_grade, hazard_count,
    latitude, longitude, road_type, road_rank, created_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

total = 0

for page in range(1, 901):
    print(f"[INFO] Page {page}")

    try:
        data = fetch_data(page)
        rows = extract_items(data)

        if not rows:
            print("[INFO] 데이터 없음 → 종료")
            break

        cursor.executemany(insert_sql, rows)
        total += len(rows)

        # 💡 1000건마다 commit
        if total % 1000 < len(rows):
            conn.commit()
            print(f"[INFO] commit 완료 (누적 {total})")

    except Exception as e:
        print(f"[ERROR] Page {page}: {e}")
        conn.rollback()

# 마지막 commit
conn.commit()

cursor.close()
conn.close()

print(f"[DONE] 총 입력: {total}")