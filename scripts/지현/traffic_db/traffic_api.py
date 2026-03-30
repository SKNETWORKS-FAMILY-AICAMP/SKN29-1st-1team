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
    """pp_road 테이블에서 고유 도로명 반환"""
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT DISTINCT road_name
                FROM pp_road
                ORDER BY road_name
            """)
            rows = await cur.fetchall()
            return {"roads": [r["road_name"] for r in rows]}
    finally:
        conn.close()


# ── 2. 평상시 속도 (도로 + 시간대 + 요일) ──────────────────
@app.get("/api/speed/base")
async def get_base_speed(
    roadname: str = Query(...),
    hour: int = Query(..., ge=0, le=23),
    day: str = Query(...),          # mon / tue / wed / thu / fri / sat / sun
):
    """pp_speed에서 선택한 도로·시간·요일 평균 속도를 반환"""
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT AVG(ps.speed) AS avg_speed
                FROM pp_speed ps
                JOIN pp_road pr
                  ON pr.road_id = ps.road_id
                WHERE pr.road_name = %s
                  AND HOUR(ps.datetime) = %s
                  AND WEEKDAY(ps.datetime) = %s
                """,
                (roadname, hour, _day_to_dow(day)),
            )
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
    """weather_pattern_asos를 '월 평균 강도'로 보고 pp_speed 평균을 조건부로 집계"""
    conn = await get_conn()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            hour_col = f"hour{hour:02d}"
            dow = _day_to_dow(day)

            if weather == "clear":
                await cur.execute(
                    f"""
                    SELECT AVG(ps.speed) AS predicted_speed
                    FROM pp_speed ps
                    JOIN pp_road pr
                      ON pr.road_id = ps.road_id
                    LEFT JOIN weather_pattern_asos w_rn
                      ON w_rn.statDate = DATE_FORMAT(ps.datetime, '%Y%m')
                     AND w_rn.weatherItem = 'rn'
                    LEFT JOIN weather_pattern_asos w_sn
                      ON w_sn.statDate = DATE_FORMAT(ps.datetime, '%Y%m')
                     AND w_sn.weatherItem = 'dsnw'
                    LEFT JOIN weather_pattern_asos w_ws
                      ON w_ws.statDate = DATE_FORMAT(ps.datetime, '%Y%m')
                     AND w_ws.weatherItem = 'ws'
                    WHERE pr.road_name = %s
                      AND HOUR(ps.datetime) = %s
                      AND WEEKDAY(ps.datetime) = %s
                      AND (w_rn.{hour_col} IS NULL OR w_rn.{hour_col} = 0)
                      AND (w_sn.{hour_col} IS NULL OR w_sn.{hour_col} = 0)
                      AND (w_ws.{hour_col} IS NULL OR w_ws.{hour_col} < 5)
                    """,
                    (roadname, hour, dow),
                )
            else:
                # weather_pattern_asos.weatherItem 값 기준으로 조건을 구성
                # (중요) 기존 traffic_api.py의 wd 사용은 현 스키마(weatherItem=ws만 존재)에 맞지 않아 제거했습니다.
                weather_rules: dict[str, tuple[str, str, tuple]] = {
                    "rn_low": ("rn", f"w.{hour_col} > %s AND w.{hour_col} < %s", (0, 5)),
                    "rn_mid": ("rn", f"w.{hour_col} >= %s AND w.{hour_col} < %s", (5, 20)),
                    "rn_high": ("rn", f"w.{hour_col} >= %s", (20,)),
                    "ws_low": ("ws", f"w.{hour_col} >= %s AND w.{hour_col} < %s", (3, 7)),
                    "ws_mid": ("ws", f"w.{hour_col} >= %s AND w.{hour_col} < %s", (7, 12)),
                    "ws_high": ("ws", f"w.{hour_col} >= %s", (12,)),
                    "snow_low": ("dsnw", f"w.{hour_col} > %s AND w.{hour_col} < %s", (0, 1)),
                    "snow_mid": ("dsnw", f"w.{hour_col} >= %s AND w.{hour_col} < %s", (1, 5)),
                    "snow_high": ("dsnw", f"w.{hour_col} >= %s", (5,)),
                }
                if weather not in weather_rules:
                    raise ValueError(f"Unknown weather value: {weather}")

                item, cond_sql, cond_params = weather_rules[weather]

                await cur.execute(
                    f"""
                    SELECT AVG(ps.speed) AS predicted_speed
                    FROM pp_speed ps
                    JOIN pp_road pr
                      ON pr.road_id = ps.road_id
                    JOIN weather_pattern_asos w
                      ON w.statDate = DATE_FORMAT(ps.datetime, '%Y%m')
                     AND w.weatherItem = %s
                    WHERE pr.road_name = %s
                      AND HOUR(ps.datetime) = %s
                      AND WEEKDAY(ps.datetime) = %s
                      AND {cond_sql}
                    """,
                    (item, roadname, hour, dow, *cond_params),
                )

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


def _day_to_dow(day: str) -> int:
    """
    MySQL WEEKDAY() 기준
    0=Monday, 1=Tuesday, ..., 6=Sunday
    """
    mapping = {
        "mon": 0, "tue": 1, "wed": 2,
        "thu": 3, "fri": 4, "sat": 5, "sun": 6,
    }
    return mapping.get(day, 0)
