"""
DB 기반 교통 패턴 조회 서비스.

최종 스키마 기준:
- pp_road(road_id, road_name)
- PP_traffic(road_id, direction, datetime, volume)
- pp_speed(road_id, direction, datetime, speed)
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas as pd

from db.db_client import Client


def _to_dt_range(start: date, end: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start, time.min)
    end_exclusive = datetime.combine(end + timedelta(days=1), time.min)
    return start_dt, end_exclusive


def list_road_names() -> list[str]:
    """사이드바 도로명 필터용 전체 도로명 목록."""
    client = Client()
    query = """
        SELECT road_name
        FROM pp_road
        ORDER BY road_name
    """
    cols, rows = client.select(query)
    if not rows:
        return []
    df = pd.DataFrame(rows, columns=cols)
    return df["road_name"].dropna().astype(str).tolist()


def get_dow_hour_pattern(
    start: date,
    end: date,
    road_name: str = "전체",
) -> pd.DataFrame:
    """
    선택 기간의 요일×시간대 평균 패턴을 반환.
    - avg_volume: 평균 교통량(traffic.volume)
    - avg_speed_kmh: 평균 속도(speed.speed)
    """
    start_dt, end_exclusive = _to_dt_range(start, end)
    client = Client()

    query = """
        SELECT
            DAYOFWEEK(t.datetime) AS dow,
            HOUR(t.datetime) AS hour,
            AVG(t.volume) AS avg_volume,
            AVG(s.speed) AS avg_speed_kmh,
            COUNT(*) AS samples
        FROM PP_traffic t
        JOIN pp_road r
          ON r.road_id = t.road_id
        LEFT JOIN pp_speed s
          ON s.road_id = t.road_id
         AND s.direction = t.direction
         AND s.datetime = t.datetime
        WHERE t.datetime >= %s
          AND t.datetime < %s
          AND (%s = '전체' OR r.road_name = %s)
        GROUP BY dow, hour
        ORDER BY dow, hour
    """
    params = (start_dt, end_exclusive, road_name, road_name)
    cols, rows = client.select(query, params)
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        return df

    # MySQL DAYOFWEEK(): 1=일 ... 7=토
    dow_labels = {1: "일", 2: "월", 3: "화", 4: "수", 5: "목", 6: "금", 7: "토"}
    df["dow"] = pd.to_numeric(df["dow"], errors="coerce").fillna(-1).astype(int)
    df["dow_label"] = df["dow"].map(lambda x: dow_labels.get(x, str(x)))
    df["hour"] = df["hour"].astype(int)
    df["avg_volume"] = pd.to_numeric(df["avg_volume"], errors="coerce")
    df["avg_speed_kmh"] = pd.to_numeric(df["avg_speed_kmh"], errors="coerce")
    df["samples"] = pd.to_numeric(df["samples"], errors="coerce").fillna(0).astype(int)
    return df

