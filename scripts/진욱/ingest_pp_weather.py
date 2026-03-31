"""
ASOS 시간자료(getWthrDataList)에서 tm(관측시각), rn(강수량)만 수집 → `pp_weather` 적재.

- tm → MySQL DATETIME으로 통일 (KST 의미의 naive datetime)
- rn 결측/빈값/비수치 → 0.0 으로 적재

환경변수
- PUBLIC_DATA_API_KEY
- DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE

실행 예
  python scripts/진욱/ingest_pp_weather.py
  python scripts/진욱/ingest_pp_weather.py --start-dt 20260101 --end-dt 20260330 --start-hh 00 --end-hh 23
  python scripts/진욱/ingest_pp_weather.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any

import mysql.connector as mc
import requests
from dotenv import load_dotenv

API_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
DEFAULT_STN_ID = "112"  # 인천

BATCH_SIZE = 500


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
CREATE TABLE IF NOT EXISTS pp_weather (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    observed_at DATETIME NOT NULL COMMENT 'KST 기준 관측시각 (API tm)',
    stn_id VARCHAR(10) NOT NULL COMMENT 'ASOS 관측소 번호',
    rn_mm DECIMAL(12, 3) NOT NULL DEFAULT 0.000 COMMENT '강수량(mm), 결측은 0',
    UNIQUE KEY uk_pp_weather (observed_at, stn_id),
    INDEX idx_pp_weather_obs (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

INSERT_SQL = """
INSERT INTO pp_weather (observed_at, stn_id, rn_mm)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    rn_mm = VALUES(rn_mm)
""".strip()


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    body = payload.get("response", {}).get("body", {})
    if not isinstance(body, dict):
        return []
    items_entry = body.get("items")
    if items_entry is None:
        return []
    if isinstance(items_entry, dict):
        items = items_entry.get("item", [])
    elif isinstance(items_entry, list):
        items = items_entry
    else:
        return []
    if items is None:
        return []
    if isinstance(items, dict):
        return [items]
    return [x for x in items if isinstance(x, dict)]


def parse_tm_to_datetime(tm_raw: Any) -> datetime | None:
    """API tm 문자열 → naive datetime (통일)."""
    if tm_raw is None:
        return None
    s = str(tm_raw).strip()
    if not s:
        return None
    # 일부 응답: "YYYY-MM-DD 24:00" → 익일 00:00:00
    if " 24:" in s and len(s.split()) >= 2:
        try:
            d_part = s.split()[0]
            base = datetime.strptime(d_part, "%Y-%m-%d") + timedelta(days=1)
            return base.replace(hour=0, minute=0, second=0)
        except ValueError:
            pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y%m%d%H%M",
        "%Y%m%d%H%M%S",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def normalize_rn_mm(val: Any) -> float:
    """강수량: null/빈값/비파싱 → 0."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if s == "" or s.lower() in ("null", "none", "-"):
        return 0.0
    try:
        x = float(s)
        if math.isnan(x):
            return 0.0
        return x
    except (ValueError, TypeError):
        return 0.0


def station_id_from_item(item: dict[str, Any], fallback: str) -> str:
    for k in ("stnId", "STN_ID", "stn_id"):
        v = item.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return fallback


def fetch_page(
    *,
    service_key: str,
    page_no: int,
    num_of_rows: int,
    start_dt: str,
    start_hh: str,
    end_dt: str,
    end_hh: str,
    stn_ids: str,
) -> dict[str, Any]:
    params = {
        "ServiceKey": service_key,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR",
        "startDt": start_dt,
        "startHh": start_hh.zfill(2),
        "endDt": end_dt,
        "endHh": end_hh.zfill(2),
        "stnIds": stn_ids,
    }
    r = requests.get(API_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def fetch_all_items(
    *,
    service_key: str,
    start_dt: str,
    end_dt: str,
    start_hh: str,
    end_hh: str,
    stn_ids: str,
    num_of_rows: int,
    max_pages: int,
    sleep_sec: float,
) -> tuple[list[dict[str, Any]], int]:
    """페이지 순회하여 item 전부 수집. (items, 실패한_페이지수)"""
    all_items: list[dict[str, Any]] = []
    fail_pages = 0
    page = 1
    while page <= max_pages:
        try:
            payload = fetch_page(
                service_key=service_key,
                page_no=page,
                num_of_rows=num_of_rows,
                start_dt=start_dt,
                start_hh=start_hh,
                end_dt=end_dt,
                end_hh=end_hh,
                stn_ids=stn_ids,
            )
        except requests.RequestException as e:
            print(f"[FAIL] HTTP page={page}: {e}")
            fail_pages += 1
            break

        header = payload.get("response", {}).get("header", {})
        rc = str(header.get("resultCode", ""))
        if rc != "00":
            print(f"[FAIL] API page={page} resultCode={rc} msg={header.get('resultMsg')}")
            fail_pages += 1
            break

        body = payload.get("response", {}).get("body", {})
        items = extract_items(payload)
        all_items.extend(items)

        try:
            total = int(body.get("totalCount") or 0)
        except (TypeError, ValueError):
            total = 0

        if total and len(all_items) >= total:
            break
        if not items:
            break
        if len(items) < num_of_rows:
            break
        page += 1
        time.sleep(sleep_sec)

    return all_items, fail_pages


def rows_from_items(items: list[dict[str, Any]], stn_fallback: str) -> list[tuple[datetime, str, float]]:
    rows: list[tuple[datetime, str, float]] = []
    for it in items:
        obs = parse_tm_to_datetime(it.get("tm"))
        if obs is None:
            continue
        sid = station_id_from_item(it, stn_fallback)
        rn = normalize_rn_mm(it.get("rn"))
        rows.append((obs, sid, rn))
    # 동일 키 중복 시 마지막 값 유지
    dedup: dict[tuple[datetime, str], float] = {}
    for obs, sid, rn in rows:
        dedup[(obs, sid)] = rn
    out: list[tuple[datetime, str, float]] = []
    for (obs, sid), rn in sorted(dedup.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        out.append((obs, sid, rn))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-dt", default="20260101", help="YYYYMMDD")
    parser.add_argument("--end-dt", default="20260102", help="YYYYMMDD")
    parser.add_argument("--start-hh", default="00", help="HH")
    parser.add_argument("--end-hh", default="23", help="HH")
    parser.add_argument("--stn-ids", default=DEFAULT_STN_ID, help="ASOS stnIds")
    parser.add_argument("--num-of-rows", type=int, default=1000, help="페이지당 행 수(최대 제한은 API 정책 따름)")
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument("--sleep", type=float, default=0.12, help="페이지 간 대기(초)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    if not key:
        print("[ERROR] PUBLIC_DATA_API_KEY 필요", file=sys.stderr)
        return 1

    print(
        f"[INFO] 기간 {args.start_dt}~{args.end_dt} "
        f"시각 {args.start_hh.zfill(2)}~{args.end_hh.zfill(2)} stnIds={args.stn_ids}"
    )

    items, fail_pages = fetch_all_items(
        service_key=key,
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        start_hh=args.start_hh,
        end_hh=args.end_hh,
        stn_ids=args.stn_ids,
        num_of_rows=args.num_of_rows,
        max_pages=args.max_pages,
        sleep_sec=args.sleep,
    )
    print(f"[INFO] 수집 item 수: {len(items)}, 실패 페이지 플래그: {fail_pages}")

    rows = rows_from_items(items, args.stn_ids)
    print(f"[INFO] DB 적재 후보 행: {len(rows)} (tm 파싱 실패 건은 제외)")

    if args.dry_run:
        for r in rows[:20]:
            print(r)
        if len(rows) > 20:
            print("...")
        return 0

    cfg = get_db_cfg()
    conn = mc.connect(**cfg)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    batch: list[tuple[datetime, str, float]] = []
    inserted = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            inserted += len(batch)
            batch.clear()
    if batch:
        cur.executemany(INSERT_SQL, batch)
        conn.commit()
        inserted += len(batch)

    cur.close()
    conn.close()
    print(f"[DONE] upsert 시도 행 수: {inserted}")
    return 0 if fail_pages == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
