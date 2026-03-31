"""
인천 격자 초단기실황(getUltraSrtNcst) 수집 → `pp_incheon_weather` 적재.

- 기간: 2026-01-01 ~ 2026-03-30 (기본값, 옵션으로 변경 가능)
- 수집 항목: PTY(강수형태), RN1(1시간 강수량)
- 격자: 인천 중심 위경도(37.47, 126.61) → nx, ny (기상청 격자 변환)

환경변수
- PUBLIC_DATA_API_KEY
- DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE

실행 예
  python scripts/진욱/ingest_pp_incheon_weather.py
  python scripts/진욱/ingest_pp_incheon_weather.py --start-date 2026-01-01 --end-date 2026-03-30 --sleep 0.2
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

import mysql.connector as mc
import requests
from dotenv import load_dotenv

# test_ultra_srt_ncst.py 와 동일
DEFAULT_LAT = 37.47
DEFAULT_LON = 126.61
BASE_TIME_SLOTS = ("0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300")
API_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"


def latlon_to_grid_nx_ny(lat: float, lon: float) -> tuple[int, int]:
    re = 6371.00877 / 5.0
    deg_rad = math.pi / 180.0
    slat1 = 30.0 * deg_rad
    slat2 = 60.0 * deg_rad
    olon = 126.0 * deg_rad
    olat = 38.0 * deg_rad
    xo = 43
    yo = 136
    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = re * sf / math.pow(math.tan(math.pi * 0.25 + olat * 0.5), sn)
    lat_rad = lat * deg_rad
    ra = re * sf / math.pow(math.tan(math.pi * 0.25 + lat_rad * 0.5), sn)
    theta = lon * deg_rad - olon
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn
    nx = int(math.floor(ra * math.sin(theta) + xo + 0.5))
    ny = int(math.floor(ro - ra * math.cos(theta) + yo + 0.5))
    return nx, ny


def get_db_cfg() -> dict[str, Any]:
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
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "autocommit": False,
    }


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pp_incheon_weather (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    base_datetime DATETIME NOT NULL COMMENT 'KST 기준 base_date+base_time → DATETIME',
    nx INT NOT NULL,
    ny INT NOT NULL,
    pty TINYINT NULL COMMENT '강수형태 0~4',
    rn1 VARCHAR(32) NULL COMMENT '1시간 강수량(mm), API 문자열 그대로',
    UNIQUE KEY uk_incheon_weather (base_datetime, nx, ny),
    INDEX idx_incheon_weather_base_dt (base_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

INSERT_SQL = """
INSERT INTO pp_incheon_weather (base_datetime, nx, ny, pty, rn1)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    pty = VALUES(pty),
    rn1 = VALUES(rn1)
""".strip()


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        items = payload["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return []
    if items is None:
        return []
    if isinstance(items, dict):
        return [items]
    return list(items)


def items_to_pty_rn1(items: list[dict[str, Any]]) -> tuple[int | None, str | None]:
    pty_raw = None
    rn1_raw = None
    for it in items:
        cat = str(it.get("category", "")).upper()
        if cat == "PTY":
            pty_raw = it.get("obsrValue")
        elif cat == "RN1":
            rn1_raw = it.get("obsrValue")
    pty: int | None = None
    if pty_raw is not None and str(pty_raw).strip() != "":
        try:
            pty = int(float(str(pty_raw).strip()))
        except ValueError:
            pty = None
    rn1 = None if rn1_raw is None else str(rn1_raw).strip()
    if rn1 == "":
        rn1 = None
    return pty, rn1


def fetch_ncst(
    service_key: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    num_of_rows: int,
) -> dict[str, Any]:
    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": num_of_rows,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    r = requests.get(API_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def base_datetime_from_response(items: list[dict[str, Any]], fallback_bd: str, fallback_bt: str) -> datetime:
    """API baseDate(YYYYMMDD) + baseTime(HHmm) → naive DATETIME (KST 의미)."""
    if items:
        bd = str(items[0].get("baseDate") or fallback_bd)
        bt = str(items[0].get("baseTime") or fallback_bt).zfill(4)
    else:
        bd, bt = fallback_bd, fallback_bt.zfill(4)
    return datetime.strptime(bd + bt, "%Y%m%d%H%M")


def daterange_inclusive(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2026-01-01", help="YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-03-30", help="YYYY-MM-DD")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT)
    parser.add_argument("--lon", type=float, default=DEFAULT_LON)
    parser.add_argument("--sleep", type=float, default=0.15, help="요청 간 대기(초), API 부하 완화")
    parser.add_argument("--dry-run", action="store_true", help="DB 없이 API만 호출·출력")
    args = parser.parse_args()

    load_dotenv()
    key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    if not key and not args.dry_run:
        print("[ERROR] PUBLIC_DATA_API_KEY 필요", file=sys.stderr)
        return 1
    if not key and args.dry_run:
        print("[WARN] dry-run에서도 키 없으면 스킵됩니다.", file=sys.stderr)
        return 1

    start_d = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_d = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    if start_d > end_d:
        print("[ERROR] start > end", file=sys.stderr)
        return 1

    nx, ny = latlon_to_grid_nx_ny(args.lat, args.lon)
    print(f"[INFO] nx={nx}, ny={ny} (lat={args.lat}, lon={args.lon})")
    print(f"[INFO] 기간 {start_d} ~ {end_d}, 슬롯 {len(BASE_TIME_SLOTS)}회/일")

    conn = None
    if not args.dry_run:
        cfg = get_db_cfg()
        conn = mc.connect(**cfg)
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        cur.close()
        print("[INFO] pp_incheon_weather 테이블 준비 완료")

    total_calls = 0
    ok = 0
    fail = 0

    for d in daterange_inclusive(start_d, end_d):
        base_date = d.strftime("%Y%m%d")
        for base_time in BASE_TIME_SLOTS:
            total_calls += 1
            try:
                payload = fetch_ncst(key, base_date, base_time, nx, ny, num_of_rows=20)
            except Exception as e:
                print(f"[FAIL] HTTP {base_date} {base_time}: {e}")
                fail += 1
                time.sleep(args.sleep)
                continue

            header = payload.get("response", {}).get("header", {})
            rc = str(header.get("resultCode", ""))
            if rc != "00":
                print(f"[FAIL] API {base_date} {base_time} resultCode={rc} msg={header.get('resultMsg')}")
                fail += 1
                time.sleep(args.sleep)
                continue

            items = extract_items(payload)
            pty, rn1 = items_to_pty_rn1(items)
            base_dt = base_datetime_from_response(items, base_date, base_time)

            if args.dry_run:
                print(f"{base_dt} pty={pty} rn1={rn1}")
            else:
                assert conn is not None
                cur = conn.cursor()
                cur.execute(
                    INSERT_SQL,
                    (base_dt, nx, ny, pty, rn1),
                )
                conn.commit()
                cur.close()

            ok += 1
            time.sleep(args.sleep)

    print(f"[DONE] 요청 시도={total_calls}, 성공={ok}, 실패={fail}")
    if conn:
        conn.close()
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
