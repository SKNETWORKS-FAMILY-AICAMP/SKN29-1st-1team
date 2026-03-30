import mysql.connector as mc 
from mysql.connector import Error
import dotenv
import os
from datetime import datetime, timedelta

import requests
import os
import dotenv

dotenv.load_dotenv(override=True)

APIKEY = os.getenv('PUBLIC_DATA_API_KEY')
print(f"[INFO] decode={APIKEY[:5]}..., encode={0}...")

DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE'),
        'autocommit': False,
}

print_config = {key: DB_CONFIG[key] if key != 'password' else '****' for key in DB_CONFIG}
print(f"[INFO] DB_CONFIG={print_config}")

def create_if_not_exist(cursor):
    query = """
    CREATE TABLE IF NOT EXISTS parking_status (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        floor VARCHAR(100),
        parking INT,
        parkingarea INT,
        datetm DATETIME,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        INDEX idx_datetm (datetm),
        INDEX idx_floor (floor)
    )
    """
    cursor.execute(query)

def insert_all(cursor, conn, res_json, batch_size=10):
    items = res_json.get("response", {}).get("body", {}).get("items", [])
    
    if not items:
        return

    batch = []

    for item in items:
        try:
            floor = item.get("floor")

            parking = int(item["parking"]) if item.get("parking") else None
            parkingarea = int(item["parkingarea"]) if item.get("parkingarea") else None

            datetm_raw = item.get("datetm")
            datetm = None
            if datetm_raw:
                datetm = datetime.strptime(datetm_raw[:14], "%Y%m%d%H%M%S")

            batch.append((
                floor,
                parking,
                parkingarea,
                datetm
            ))

        except Exception as e:
            print(f"[SKIP] item={item} error={e}")
            continue

        # 배치 insert
        print(batch)
        if len(batch) >= batch_size:
            _insert_batch(cursor, conn, batch)
            batch.clear()

    # 마지막 남은 데이터
    if batch:
        _insert_batch(cursor, conn, batch)

def _insert_batch(cursor, conn, batch):
    query = """
    INSERT INTO parking_status (
        floor, parking, parkingarea, datetm
    )
    VALUES (%s, %s, %s, %s)
    """

    try:
        cursor.executemany(query, batch)
        conn.commit()
        print(f"[INSERT] {len(batch)} rows")
    except Exception as e:
        conn.rollback()
        print(f"[BATCH ERROR] {e}")

url = 'http://apis.data.go.kr/B551177/StatusOfParking/getTrackingParking'
params ={'serviceKey' : APIKEY, 'numOfRows' : '100', 'type': 'json' }

conn = mc.connect(**DB_CONFIG)
cursor = conn.cursor()

create_if_not_exist(cursor)

for page_no in range(1, 101):
    params['pageNo'] = page_no
    data = requests.get(url, params).json()
    print(data)
    insert_all(cursor, conn, data)
    print(f'[DEBUG] page_no {page_no} request done')
