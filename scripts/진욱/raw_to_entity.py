import mysql.connector as mc
from mysql.connector import Error
import dotenv
import os
from datetime import datetime, timedelta

dotenv.load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE'),
    'autocommit': False,
}

print_config = {k: v if k != 'password' else '****' for k, v in DB_CONFIG.items()}
print(f"[INFO] DB_CONFIG={print_config}")


# -----------------------------
# DB 연결
# -----------------------------
conn = mc.connect(**DB_CONFIG)
cursor = conn.cursor()


# -----------------------------
# 공통 유틸
# -----------------------------
def build_datetime(stat_date, hour):
    if isinstance(stat_date, str):
        base = datetime.strptime(stat_date, "%Y-%m-%d")
    else:
        base = stat_date
    return base + timedelta(hours=hour)


def get_or_create_road_id(cursor, road_name, cache):
    if not road_name:
        return None

    if road_name in cache:
        return cache[road_name]

    # insert (중복 무시)
    cursor.execute("""
        INSERT INTO pp_road (road_name)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE road_name = VALUES(road_name)
    """, (road_name,))

    # select id
    cursor.execute("SELECT road_id FROM pp_road WHERE road_name = %s", (road_name,))
    road_id = cursor.fetchone()[0]

    cache[road_name] = road_id
    return road_id


# -----------------------------
# traffic insert
# -----------------------------
def insert_traffic(cursor, conn, rows, columns, batch_size=2000):
    col_idx = {col: i for i, col in enumerate(columns)}
    road_cache = {}

    batch = []

    for row in rows:
        try:
            stat_date = row[col_idx['statDate']]
            road_name = row[col_idx['roadName']]
            direction = 0 if row[col_idx['direction']] == '상행' else 1

            road_id = get_or_create_road_id(cursor, road_name, road_cache)

            for h in range(24):
                value = row[col_idx[f'hour{h:02d}']]

                if value is None:
                    continue

                dt = build_datetime(stat_date, h)

                batch.append((road_id, direction, dt, value))

                if len(batch) >= batch_size:
                    _insert_traffic_batch(cursor, conn, batch)
                    batch.clear()

        except Exception as e:
            print(f"[traffic SKIP] {e} row={row}")
            continue

    if batch:
        _insert_traffic_batch(cursor, conn, batch)


def _insert_traffic_batch(cursor, conn, batch):
    query = """
    INSERT INTO PP_traffic (road_id, direction, datetime, volume)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE volume = VALUES(volume)
    """
    try:
        cursor.executemany(query, batch)
        conn.commit()
        print(f"[traffic INSERT] {len(batch)}")
    except Exception as e:
        conn.rollback()
        print(f"[traffic ERROR] {e}")


# -----------------------------
# speed insert
# -----------------------------
def insert_speed(cursor, conn, rows, columns, batch_size=2000):
    col_idx = {col: i for i, col in enumerate(columns)}
    road_cache = {}

    batch = []

    for row in rows:
        try:
            stat_date = row[col_idx['statDate']]
            road_name = row[col_idx['roadName']]
            direction = 0 if row[col_idx['direction']] == '상행' else 1

            road_id = get_or_create_road_id(cursor, road_name, road_cache)

            for h in range(24):
                value = row[col_idx[f'hour{h:02d}']]

                if value is None:
                    continue

                dt = build_datetime(stat_date, h)

                batch.append((road_id, direction, dt, value))

                if len(batch) >= batch_size:
                    _insert_speed_batch(cursor, conn, batch)
                    batch.clear()

        except Exception as e:
            print(f"[speed SKIP] {e} row={row}")
            continue

    if batch:
        _insert_speed_batch(cursor, conn, batch)


def _insert_speed_batch(cursor, conn, batch):
    query = """
    INSERT INTO pp_speed (road_id, direction, datetime, speed)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE speed = VALUES(speed)
    """
    try:
        cursor.executemany(query, batch)
        conn.commit()
        print(f"[speed INSERT] {len(batch)}")
    except Exception as e:
        conn.rollback()
        print(f"[speed ERROR] {e}")


# -----------------------------
# RAW 데이터 조회
# -----------------------------
START, END = '2026-01-01', '2026-01-31'

cursor.execute("""
    SELECT * FROM traffic_raw
    WHERE statDate BETWEEN %s AND %s
""", (START, END))

traffic_columns = [col[0] for col in cursor.description]
traffics = cursor.fetchall()


cursor.execute("""
    SELECT * FROM traffic_speed_raw
    WHERE statDate BETWEEN %s AND %s
""", (START, END))

speed_columns = [col[0] for col in cursor.description]
speeds = cursor.fetchall()


# -----------------------------
# 실행
# -----------------------------
insert_traffic(cursor, conn, traffics, traffic_columns)
insert_speed(cursor, conn, speeds, speed_columns)


# -----------------------------
# 종료
# -----------------------------
cursor.close()
conn.close()

print("[DONE]")