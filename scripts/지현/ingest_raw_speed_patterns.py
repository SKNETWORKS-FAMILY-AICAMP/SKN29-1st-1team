"""
지현(raw) 도로속도/기상 원시 테이블 적재 스크립트.

로컬 파일로부터 INSERT 하되,
- speed_pattern_monthly: `weekday.sql` (INSERT 라인 파싱)
- speed_pattern_timezone: `timezone.sql` (CSV 로드 후 bulk INSERT)
- weather_pattern_asos: ASOS API 호출로 월별 평균 산출 후 bulk INSERT

DB 접속 정보는 환경변수로 주입합니다.
필수: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
weather load 시(기본): ASOS_API_KEY 필요
"""

from __future__ import annotations

import argparse
import calendar
import csv
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

import mysql.connector as mc
import requests
from dotenv import load_dotenv


HOUR_COLS = [f"hour{str(i).zfill(2)}" for i in range(24)]


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @staticmethod
    def from_env() -> "DBConfig":
        load_dotenv()
        host = os.getenv("DB_HOST", "")
        port = int(os.getenv("DB_PORT", "3306"))
        user = os.getenv("DB_USER", "")
        password = os.getenv("DB_PASSWORD", "")
        database = os.getenv("DB_DATABASE", "")

        missing = [k for k, v in {
            "DB_HOST": host,
            "DB_USER": user,
            "DB_PASSWORD": password,
            "DB_DATABASE": database,
        }.items() if not v]
        if missing:
            raise ValueError(f"Missing env vars: {', '.join(missing)}")
        return DBConfig(host=host, port=port, user=user, password=password, database=database)


def connect_db(cfg: DBConfig) -> mc.MySQLConnection:
    return mc.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=False,
    )


def execute_sql_statements(cursor: mc.cursor.MySQLCursor, sql_text: str) -> None:
    """
    세미콜론 단위로 쪼개서 실행.
    (DDL/INSERT 덤프에 공통적으로 유효한 간단한 방식)
    """
    statements: list[str] = []
    buf = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("/*") or stripped.startswith("--"):
            # 주석 라인은 skip (문자열 내 주석이 섞인 덤프가 아니라는 가정)
            continue
        buf.append(line)
        if ";" in line:
            statements.append("\n".join(buf).strip())
            buf = []
    for stmt in statements:
        if stmt:
            cursor.execute(stmt)


def split_sql_values(value_str: str) -> list[str]:
    """
    VALUES(...) 괄호 내부를 '큰따옴표/따옴표'를 존중해 ',' 기준으로 분리.
    입력 포맷은 weekday.sql/weather.sql 덤프에서 그대로 쓰이는 단순 형태(숫자/작은따옴표 문자열)만 가정.
    """
    tokens: list[str] = []
    current = []
    in_quote = False
    i = 0
    while i < len(value_str):
        ch = value_str[i]
        if ch == "'":
            in_quote = not in_quote
            current.append(ch)
            i += 1
            continue
        if ch == "," and not in_quote:
            tokens.append("".join(current).strip())
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    if current:
        tokens.append("".join(current).strip())
    return tokens


def parse_int(token: str) -> int | None:
    token = token.strip()
    if token.upper() == "NULL":
        return None
    return int(token)


def unquote(token: str) -> str | None:
    token = token.strip()
    if token.upper() == "NULL":
        return None
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return token[1:-1]
    return token


def load_speed_pattern_monthly_from_weekday_sql(
    cursor: mc.cursor.MySQLCursor,
    conn: mc.MySQLConnection,
    weekday_sql_path: str,
    batch_size: int = 1000,
) -> int:
    """
    weekday.sql 라인: INSERT INTO `` (`id`,...) VALUES (...);
    => INSERT IGNORE INTO speed_pattern_monthly (...) VALUES ...
    """
    insert_sql = """
        INSERT IGNORE INTO speed_pattern_monthly
        (id, statDate, roadName, direction, sectionName, monthStatValue, mon, tue, wed, thu, fri, sat, sun)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """.strip()

    inserted_rows = 0
    batch: list[tuple[Any, ...]] = []

    with open(weekday_sql_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("INSERT INTO"):
                continue
            if "VALUES" not in line:
                continue

            # VALUES ( ... );
            start = line.find("VALUES")
            open_paren = line.find("(", start)
            close_paren = line.rfind(")")
            if open_paren < 0 or close_paren < 0 or close_paren <= open_paren:
                continue
            values_inside = line[open_paren + 1 : close_paren].strip()

            raw_tokens = split_sql_values(values_inside)
            if len(raw_tokens) != 13:
                # id + statDate + roadName + direction + sectionName + monthStatValue + mon..sun (7)
                # = 1 + 4 + 1 + 7 = 13
                raise ValueError(f"Unexpected token count in weekday.sql: {len(raw_tokens)} line={line[:120]}")

            # token order:
            # id, statDate, roadName, direction, sectionName,
            # monthStatValue, mon, tue, wed, thu, fri, sat, sun
            # => raw_tokens[0..12] (13개)
            row = (
                parse_int(raw_tokens[0]),
                unquote(raw_tokens[1]),
                unquote(raw_tokens[2]),
                unquote(raw_tokens[3]),
                unquote(raw_tokens[4]),
                parse_int(raw_tokens[5]),
                parse_int(raw_tokens[6]),
                parse_int(raw_tokens[7]),
                parse_int(raw_tokens[8]),
                parse_int(raw_tokens[9]),
                parse_int(raw_tokens[10]),
                parse_int(raw_tokens[11]),  # sat
                parse_int(raw_tokens[12]),
            )

            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted_rows += len(batch)
                batch = []

    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted_rows += len(batch)

    return inserted_rows


def load_speed_pattern_timezone_from_csv(
    cursor: mc.cursor.MySQLCursor,
    conn: mc.MySQLConnection,
    timezone_csv_path: str,
    batch_size: int = 1000,
) -> int:
    insert_sql = f"""
        INSERT IGNORE INTO speed_pattern_timezone
        (statDate, roadName, direction, sectionName, {", ".join(HOUR_COLS)})
        VALUES ({", ".join(["%s"] * (4 + len(HOUR_COLS)))})
    """.strip()

    import pandas as pd  # 로컬 CSV 파싱 간결성을 위해 사용

    df = pd.read_csv(timezone_csv_path)

    # id는 덤프에만 있고, DB에는 AUTO_INCREMENT이므로 제외
    cols = ["statDate", "roadName", "direction", "sectionName"] + HOUR_COLS
    df = df[cols]
    for c in HOUR_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64").fillna(0).astype(int)

    rows = []
    inserted_rows = 0
    for tup in df.itertuples(index=False, name=None):
        rows.append(tup)
        if len(rows) >= batch_size:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            inserted_rows += len(rows)
            rows = []
    if rows:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        inserted_rows += len(rows)
    return inserted_rows


# -------------------------
# weather_pattern_asos (ASOS API)
# -------------------------


WEATHER_COLS = ["ta", "rn", "hm", "ws", "wd", "dsnw"]  # ta: 기온, rn: 강수량, ...

INCHEON_STATIONS: dict[str, str] = {
    "112": "인천",
    "201": "강화",
    "232": "백령도",
}

BASE_URL_ASOS = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"


def get_asos_hourly(
    *,
    service_key: str,
    base_url: str,
    start_dt: str,
    end_dt: str,
    stn_id: str,
    max_page_size: int = 999,
) -> dict[str, Any] | None:
    params = {
        "ServiceKey": service_key,
        "pageNo": "1",
        "numOfRows": str(max_page_size),
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR",
        "startDt": start_dt,
        "startHh": "01",
        "endDt": end_dt,
        "endHh": "23",
        "stnIds": stn_id,
    }
    try:
        resp = requests.get(base_url, params=params, timeout=15)
        return resp.json()
    except Exception:
        return None


def extract_items(json_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not json_data:
        return []
    body = json_data.get("response", {}).get("body", {})
    items_entry = body.get("items", [])
    if isinstance(items_entry, dict):
        items = items_entry.get("item", [])
    else:
        items = items_entry
    if not items:
        return []
    if isinstance(items, dict):
        items = [items]
    return [item for item in items if isinstance(item, dict)]


def parse_hourly_by_item(items: Iterable[dict[str, Any]]) -> dict[str, dict[int, float | None]]:
    """
    item 리스트에서 관측 항목별 시간대 값을 dict로 변환.
    결과: { 'ta': {1: -3.2, ..., 23: ...}, 'rn': {...}, ... }
    """
    result: dict[str, dict[int, float | None]] = {col: {} for col in WEATHER_COLS}
    for item in items:
        tm = str(item.get("tm", "") or "")
        try:
            hour = int(tm.strip().split(" ")[1].split(":")[0])
            if hour == 24:
                hour = 0
        except Exception:
            continue

        for col in WEATHER_COLS:
            val = item.get(col, None)
            if val in (None, "", " "):
                result[col][hour] = None
                continue
            try:
                result[col][hour] = float(val)
            except Exception:
                result[col][hour] = None
    return result


def create_table_speed_pattern_timezone(cursor: mc.cursor.MySQLCursor) -> None:
    create_query = """
    CREATE TABLE IF NOT EXISTS speed_pattern_timezone (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        statDate VARCHAR(6) NOT NULL,
        roadName VARCHAR(100),
        direction VARCHAR(20),
        sectionName VARCHAR(200),
        hour00 INT, hour01 INT, hour02 INT, hour03 INT, hour04 INT, hour05 INT,
        hour06 INT, hour07 INT, hour08 INT, hour09 INT, hour10 INT, hour11 INT,
        hour12 INT, hour13 INT, hour14 INT, hour15 INT, hour16 INT, hour17 INT,
        hour18 INT, hour19 INT, hour20 INT, hour21 INT, hour22 INT, hour23 INT,
        UNIQUE KEY uniq_timezone (statDate, roadName, direction, sectionName)
    );
    """.strip()
    cursor.execute(create_query)


def create_table_weather_pattern_asos(cursor: mc.cursor.MySQLCursor) -> None:
    # notebook의 weather_pattern_asos 스키마를 그대로 반영
    hour_defs = ",\n".join([f"        {h} FLOAT" for h in HOUR_COLS])
    create_query = f"""
    CREATE TABLE IF NOT EXISTS weather_pattern_asos (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        statDate VARCHAR(6) NOT NULL,
        stnId VARCHAR(10) NOT NULL,
        stnNm VARCHAR(50),
        weatherItem VARCHAR(10) NOT NULL,
{hour_defs},
        UNIQUE KEY uniq_asos (statDate, stnId, weatherItem)
    );
    """.strip()
    cursor.execute(create_query)


def upsert_weather_pattern_asos_from_api(
    cursor: mc.cursor.MySQLCursor,
    conn: mc.MySQLConnection,
    service_key: str,
    *,
    years: list[int],
    base_url: str = BASE_URL_ASOS,
    max_page_size: int = 999,
    sleep_between_calls_sec: float = 0.2,
) -> int:
    col_str = ", ".join(["statDate", "stnId", "stnNm", "weatherItem"] + HOUR_COLS)
    placeholders = ", ".join(["%s"] * (4 + len(HOUR_COLS)))
    insert_query = f"""
        INSERT IGNORE INTO weather_pattern_asos ({col_str})
        VALUES ({placeholders})
    """.strip()

    total_inserted = 0

    for year in years:
        for month in range(1, 13):
            if year == 2026 and month > 3:
                break

            target_ym = f"{year}{str(month).zfill(2)}"
            last_day = calendar.monthrange(year, month)[1]
            monthly_acc: dict[tuple[str, str, str], dict[int, list[float]]] = {}

            print(f"[weather] collecting {target_ym} ...")

            for day in range(1, last_day + 1):
                ymd = f"{year}{str(month).zfill(2)}{str(day).zfill(2)}"

                for stn_id, stn_nm in INCHEON_STATIONS.items():
                    data = get_asos_hourly(
                        service_key=service_key,
                        base_url=base_url,
                        start_dt=ymd,
                        end_dt=ymd,
                        stn_id=stn_id,
                        max_page_size=max_page_size,
                    )
                    items = extract_items(data)
                    if not items:
                        time.sleep(sleep_between_calls_sec)
                        continue

                    hourly = parse_hourly_by_item(items)
                    for weather_item, hour_vals in hourly.items():
                        key = (stn_id, stn_nm, weather_item)
                        if key not in monthly_acc:
                            monthly_acc[key] = {h: [] for h in range(24)}
                        for h, v in hour_vals.items():
                            if v is not None:
                                monthly_acc[key][h].append(v)

                    time.sleep(sleep_between_calls_sec)

                print(f"[weather] {ymd} done.            ", end="\r")

            final_rows: list[tuple[Any, ...]] = []
            for (stn_id, stn_nm, weather_item), hour_data in monthly_acc.items():
                row: list[Any] = [target_ym, stn_id, stn_nm, weather_item]
                for h in range(24):
                    vals = hour_data.get(h, [])
                    avg = round(sum(vals) / len(vals), 2) if vals else None
                    row.append(avg)
                final_rows.append(tuple(row))

            if final_rows:
                cursor.executemany(insert_query, final_rows)
                conn.commit()
                total_inserted += len(final_rows)
                print(f"[weather] {target_ym}: inserted={len(final_rows)}")

    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", default=None, help="DB_HOST (optional override)")
    parser.add_argument("--db-port", default=None, type=int, help="DB_PORT (optional override)")
    parser.add_argument("--db-user", default=None, help="DB_USER (optional override)")
    parser.add_argument("--db-password", default=None, help="DB_PASSWORD (optional override)")
    parser.add_argument("--db-database", default=None, help="DB_DATABASE (optional override)")

    parser.add_argument("--weekday-sql", default="weekday.sql", help="scripts/지현/weekday.sql path")
    parser.add_argument("--timezone-csv", default="timezone.sql", help="scripts/지현/timezone.sql path")
    parser.add_argument("--speed-monthly-sql", default="speed_pattern_monthly.sql", help="scripts/지현/speed_pattern_monthly.sql path")

    parser.add_argument("--load-weather", action="store_true", help="weather_pattern_asos also load via ASOS API")
    parser.add_argument("--weather-start-year", type=int, default=2026)
    parser.add_argument("--weather-end-year", type=int, default=2027)
    args = parser.parse_args()

    cfg = DBConfig.from_env()
    # optional override
    if args.db_host:
        cfg = DBConfig(host=args.db_host, port=cfg.port, user=cfg.user, password=cfg.password, database=cfg.database)
    if args.db_port:
        cfg = DBConfig(host=cfg.host, port=args.db_port, user=cfg.user, password=cfg.password, database=cfg.database)
    if args.db_user:
        cfg = DBConfig(host=cfg.host, port=cfg.port, user=args.db_user, password=cfg.password, database=cfg.database)
    if args.db_password:
        cfg = DBConfig(host=cfg.host, port=cfg.port, user=cfg.user, password=args.db_password, database=cfg.database)
    if args.db_database:
        cfg = DBConfig(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=args.db_database)

    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    weekday_sql_path = os.path.join(scripts_dir, args.weekday_sql)
    timezone_csv_path = os.path.join(scripts_dir, args.timezone_csv)
    speed_monthly_sql_path = os.path.join(scripts_dir, args.speed_monthly_sql)

    if not os.path.exists(weekday_sql_path):
        raise FileNotFoundError(weekday_sql_path)
    if not os.path.exists(timezone_csv_path):
        raise FileNotFoundError(timezone_csv_path)
    if not os.path.exists(speed_monthly_sql_path):
        raise FileNotFoundError(speed_monthly_sql_path)

    conn = connect_db(cfg)
    cursor = conn.cursor()

    # 1) create tables
    with open(speed_monthly_sql_path, "r", encoding="utf-8") as f:
        speed_monthly_ddl = f.read()
    execute_sql_statements(cursor, speed_monthly_ddl)

    create_table_speed_pattern_timezone(cursor)
    create_table_weather_pattern_asos(cursor)
    conn.commit()

    # 2) insert speed_pattern_monthly
    print("[speed_monthly] inserting from weekday.sql ...")
    inserted_monthly = load_speed_pattern_monthly_from_weekday_sql(cursor, conn, weekday_sql_path)
    print(f"[speed_monthly] done inserted={inserted_monthly}")

    # 3) insert speed_pattern_timezone
    print("[speed_timezone] inserting from timezone.sql ...")
    inserted_timezone = load_speed_pattern_timezone_from_csv(cursor, conn, timezone_csv_path)
    print(f"[speed_timezone] done inserted={inserted_timezone}")

    # 4) insert weather_pattern_asos (ASOS API)
    if args.load_weather:
        service_key = os.getenv("ASOS_API_KEY", "")
        if not service_key:
            raise ValueError("ASOS_API_KEY is required when --load-weather is set")
        years = list(range(args.weather_start_year, args.weather_end_year + 1))
        print(f"[weather] inserting from API years={years} ... (this can take a long time)")
        inserted_weather = upsert_weather_pattern_asos_from_api(
            cursor,
            conn,
            service_key=service_key,
            years=years,
        )
        print(f"[weather] done inserted={inserted_weather}")
    else:
        print("[weather] skipped (pass --load-weather to insert weather_pattern_asos)")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

