"""
기상청 단기예보 API — 초단기실황(getUltraSrtNcst) 호출 테스트 스크립트.

GET https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst

환경변수
- PUBLIC_DATA_API_KEY : 공공데이터포털 인증키(serviceKey)

사용 예
  python scripts/진욱/test_ultra_srt_ncst.py
  python scripts/진욱/test_ultra_srt_ncst.py --base-date 20260331 --base-time 1400
  python scripts/진욱/test_ultra_srt_ncst.py --nx 55 --ny 124
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

KST = ZoneInfo("Asia/Seoul")

# 인천 중심 좌표 (사용자 지정)
DEFAULT_LAT = 37.47
DEFAULT_LON = 126.61

# 발표 시각 (초단기실황) — 1일 8회
BASE_TIME_SLOTS = ("0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300")

API_URL = (
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
)

# category → 설명 (응답 정리용)
CATEGORY_DESC: dict[str, str] = {
    "T1H": "기온(℃)",
    "RN1": "강수량(mm)",
    "SKY": "하늘상태(1맑음,3구름많음,4흐림)",
    "UUU": "동서풍(m/s)",
    "VVV": "남북풍(m/s)",
    "REH": "습도(%)",
    "PTY": "강수형태(0없음,1비,2비/눈,3눈,4소나기)",
    "LGT": "낙뢰(kA)",
    "VEC": "풍향(deg)",
    "WSD": "풍속(m/s)",
}


def latlon_to_grid_nx_ny(lat: float, lon: float) -> tuple[int, int]:
    """
    위·경도 → 기상청 격자(nx, ny).
    (Lambert 정각원추 투영, 기상청 제공 공식과 동일 계열)
    """
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


def pick_base_date_time(
    now_kst: datetime,
    *,
    use_previous_slot: bool,
) -> tuple[str, str]:
    """
    BASE_TIME_SLOTS 중 '현재 시각 기준'으로 선택 가능한 가장 최근 발표 시각.
    - use_previous_slot=True 이면 한 단계 이전 슬롯(API 반영 지연 대비)
    """
    if now_kst.tzinfo is None:
        now_kst = now_kst.replace(tzinfo=KST)
    else:
        now_kst = now_kst.astimezone(KST)

    slot_hours = [2, 5, 8, 11, 14, 17, 20, 23]
    d = now_kst.date()
    t = now_kst.time()
    minutes = t.hour * 60 + t.minute

    chosen_idx = -1
    for i, h in enumerate(slot_hours):
        if h * 60 <= minutes:
            chosen_idx = i

    if chosen_idx < 0:
        # 오늘 아직 첫 슬롯 전 → 전일 23시
        prev = d - timedelta(days=1)
        return prev.strftime("%Y%m%d"), "2300"

    if use_previous_slot and chosen_idx > 0:
        chosen_idx -= 1
    elif use_previous_slot and chosen_idx == 0:
        prev = d - timedelta(days=1)
        return prev.strftime("%Y%m%d"), "2300"

    h = slot_hours[chosen_idx]
    return d.strftime("%Y%m%d"), f"{h:02d}00"


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """응답 JSON에서 item 리스트 추출."""
    try:
        items = payload["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return []
    if items is None:
        return []
    if isinstance(items, dict):
        return [items]
    return list(items)


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="초단기실황 API 테스트")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT)
    parser.add_argument("--lon", type=float, default=DEFAULT_LON)
    parser.add_argument("--nx", type=int, default=None, help="격자 X (미지정 시 lat/lon으로 계산)")
    parser.add_argument("--ny", type=int, default=None, help="격자 Y (미지정 시 lat/lon으로 계산)")
    parser.add_argument("--base-date", type=str, default=None, help="YYYYMMDD")
    parser.add_argument("--base-time", type=str, default=None, help="0200, 0500, ...")
    parser.add_argument("--page-no", type=int, default=1)
    parser.add_argument("--num-of-rows", type=int, default=20)
    parser.add_argument(
        "--previous-slot",
        action="store_true",
        help="발표 직후 오류 방지용으로 한 단계 이전 base_time 사용",
    )
    args = parser.parse_args()

    key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    if not key:
        print("[ERROR] PUBLIC_DATA_API_KEY 환경변수를 설정하세요.", file=sys.stderr)
        return 1

    if args.nx is not None and args.ny is not None:
        nx, ny = args.nx, args.ny
    else:
        nx, ny = latlon_to_grid_nx_ny(args.lat, args.lon)

    now_kst = datetime.now(KST)
    if args.base_date and args.base_time:
        base_date = args.base_date
        base_time = args.base_time.zfill(4)
    else:
        base_date, base_time = pick_base_date_time(now_kst, use_previous_slot=args.previous_slot)

    params = {
        "serviceKey": key,
        "pageNo": args.page_no,
        "numOfRows": args.num_of_rows,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    print(f"params={params}")

    print("=== 요청 ===")
    print(f"URL: {API_URL}")
    print(f"lat/lon: {args.lat}, {args.lon} → nx, ny: {nx}, {ny}")
    print(f"base_date: {base_date}, base_time: {base_time} (KST 기준 슬롯: {', '.join(BASE_TIME_SLOTS)})")
    print(f"pageNo={args.page_no}, numOfRows={args.num_of_rows}, dataType=json")
    print()

    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] HTTP 요청 실패: {e}", file=sys.stderr)
        return 1

    try:
        payload = resp.json()
    except json.JSONDecodeError:
        print("[ERROR] JSON 파싱 실패. 응답 앞부분:", file=sys.stderr)
        print(resp.text[:800], file=sys.stderr)
        return 1

    header = payload.get("response", {}).get("header", {})
    body = payload.get("response", {}).get("body", {})

    print("=== 응답 헤더(요약) ===")
    print(f"resultCode: {header.get('resultCode')}")
    print(f"resultMsg: {header.get('resultMsg')}")
    print()

    print("=== 응답 바디(메타) ===")
    print(f"numOfRows: {body.get('numOfRows')}")
    print(f"pageNo: {body.get('pageNo')}")
    print(f"totalCount: {body.get('totalCount')}")
    print(f"dataType: {body.get('dataType')}")
    # 일부 필드는 item 안에 있음
    items = extract_items(payload)
    if items:
        sample = items[0]
        print(f"baseDate: {sample.get('baseDate')}")
        print(f"baseTime: {sample.get('baseTime')}")
        print(f"nx: {sample.get('nx')}, ny: {sample.get('ny')}")
    print()

    print(f"=== items 개수: {len(items)} (한 요청당 대략 이 정도 행) ===")
    print()

    print("=== category / obsrValue (정리) ===")
    print(f"{'category':<6} {'obsrValue':<12} 설명")
    print("-" * 60)
    for row in sorted(items, key=lambda r: str(r.get("category", ""))):
        cat = str(row.get("category", "")).upper()
        val = row.get("obsrValue", "")
        desc = CATEGORY_DESC.get(cat, "")
        print(f"{cat:<6} {str(val):<12} {desc}")

    print()
    print("=== 원본 response 일부 (디버그) ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2)[:4000])
    if len(json.dumps(payload, ensure_ascii=False)) > 4000:
        print("... (truncated)")

    rc = str(header.get("resultCode", ""))
    if rc != "00":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
