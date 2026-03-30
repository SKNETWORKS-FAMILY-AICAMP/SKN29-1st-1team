import mysql.connector as mc
import os
import dotenv
dotenv.load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE'),
    'autocommit': True,
}

try:
    conn = mc.connect(**DB_CONFIG)
    print("DB 연결 성공!")
    conn.close()
except Exception as e:
    print(f"DB 연결 실패: {e}")speed_pattern_monthly