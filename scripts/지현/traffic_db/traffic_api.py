from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import aiomysql
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_conn():
    return await aiomysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        db=os.getenv("DB_DATABASE", "traffic_db"),
        charset="utf8mb4",
    )


# ── 1. 도로명 목록 ──────────────────────────────────────────
@app.get("/api/roads")
async def get_roads():
    """speed_pattern_monthly 테이블에서 고유 도로명 반환"""
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT DISTINCT roadname
                FROM speed_pattern_monthly
                ORDER BY roadname
            """)
            rows = await cur.fetchall()
            return {"roads": [r["roadname"] for r in rows]}
    finally:
        conn.close()


# ── 2. 평상시 속도 (도로 + 시간대 + 요일) ──────────────────
@app.get("/api/speed/base")
async def get_base_speed(
    roadname: str = Query(...),
    hour: int = Query(..., ge=0, le=23),
    day: str = Query(...),          # mon / tue / wed / thu / fri / sat / sun
):
    """
    speed_pattern_timezone 테이블에서
    선택한 도로·시간·요일의 평균 속도를 반환
    """
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(f"""
                SELECT AVG(speed_{day}) AS avg_speed
                FROM speed_pattern_timezone
                WHERE roadname = %s
                  AND timezone = %s
            """, (roadname, hour))
            row = await cur.fetchone()
            avg = round(float(row["avg_speed"]), 1) if row and row["avg_speed"] else None
            return {"roadname": roadname, "hour": hour, "day": day, "base_speed": avg}
    finally:
        conn.close()


# ── 3. 기상 조건별 예측 속도 ────────────────────────────────
@app.get("/api/speed/weather")
async def get_weather_speed(
    roadname: str = Query(...),
    hour: int = Query(..., ge=0, le=23),
    day: str = Query(...),
    weather: str = Query(...),      # clear / rn_low / rn_mid / rn_high / ws_low / ws_mid / ws_high / snow_low / snow_mid / snow_high
):
    """
    weather_pattern_asos + speed_pattern_monthly 조합으로
    기상 조건별 예측 속도 반환
    """
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:

            # 기상 조건별 필터 매핑
            weather_filter = {
                "clear":    "rn = 0 AND dsnw = 0 AND ws < 5",
                "rn_low":   "rn > 0  AND rn < 5",
                "rn_mid":   "rn >= 5  AND rn < 20",
                "rn_high":  "rn >= 20",
                "ws_low":   "wd IS NOT NULL AND ws >= 3  AND ws < 7",
                "ws_mid":   "ws >= 7  AND ws < 12",
                "ws_high":  "ws >= 12",
                "snow_low": "dsnw > 0 AND dsnw < 1",
                "snow_mid": "dsnw >= 1 AND dsnw < 5",
                "snow_high":"dsnw >= 5",
            }

            cond = weather_filter.get(weather, "rn = 0 AND dsnw = 0")

            # weather_pattern_asos 에서 해당 기상 조건의 날짜·시간 목록 추출 후
            # speed_pattern_monthly 와 조인해 평균 속도 계산
            await cur.execute(f"""
                SELECT AVG(spm.avg_speed) AS predicted_speed
                FROM speed_pattern_monthly spm
                JOIN weather_pattern_asos wpa
                  ON DATE(wpa.tm) = DATE(spm.base_date)
                 AND HOUR(wpa.tm) = %s
                WHERE spm.roadname = %s
                  AND DAYNAME(spm.base_date) = %s
                  AND {cond}
            """, (hour, roadname, _day_to_mysql(day)))

            row = await cur.fetchone()
            predicted = round(float(row["predicted_speed"]), 1) if row and row["predicted_speed"] else None
            return {
                "roadname": roadname,
                "hour": hour,
                "day": day,
                "weather": weather,
                "predicted_speed": predicted
            }
    finally:
        conn.close()


def _day_to_mysql(day: str) -> str:
    mapping = {
        "mon": "Monday", "tue": "Tuesday", "wed": "Wednesday",
        "thu": "Thursday", "fri": "Friday", "sat": "Saturday", "sun": "Sunday"
    }
    return mapping.get(day, "Monday")
