"""
기상청 ASOS 시간자료 API — getWthrDataList 호출 테스트 스크립트.

GET https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList

파라미터(기본값은 요청 예시와 동일)
- serviceKey: PUBLIC_DATA_API_KEY
- pageNo, numOfRows, dataType=JSON
- dataCd=ASOS, dateCd=HR
- startDt, startHh, endDt, endHh
- stnIds: 관측소 ID (인천 112)

환경변수
- PUBLIC_DATA_API_KEY

사용 예
  python scripts/진욱/test_asos_hourly_wthr.py
  python scripts/진욱/test_asos_hourly_wthr.py --num-of-rows 100 --page-no 1
  python scripts/진욱/test_asos_hourly_wthr.py --fetch-all-pages --max-pages 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv

API_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

# scripts/지현/ingest_raw_speed_patterns.py 와 동일 계열
DEFAULT_STN_ID = "112"  # 인천


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """응답 JSON에서 item 리스트 추출 (items 구조 변형 대응)."""
    body = payload.get("response", {}).get("body", {})
    if not isinstance(body, dict):
        return []
    items_entry = body.get("items")
    if items_entry is None:
        return []
    if isinstance(items_entry, dict):
        items = items_entry.get("item", [])
    elif isinstance(items_entry, list):
        # 드물게 list 직접 오는 경우
        items = items_entry
    else:
        return []
    if items is None:
        return []
    if isinstance(items, dict):
        return [items]
    return [x for x in items if isinstance(x, dict)]


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
        # 공공데이터포털 예시·기존 적재 스크립트와 동일 키명
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
    r = requests.get(API_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def summarize_item_keys(items: list[dict[str, Any]], sample_n: int = 3) -> None:
    if not items:
        print("(items 비어 있음)")
        return
    all_keys: set[str] = set()
    for it in items[: min(50, len(items))]:
        all_keys.update(it.keys())
    print(f"샘플에서 관측된 키(최대 50행 기준, {len(all_keys)}개): {sorted(all_keys)}")
    print()
    print(f"=== 샘플 item {min(sample_n, len(items))}건 ===")
    for i, it in enumerate(items[:sample_n]):
        print(f"--- [{i}] ---")
        print(json.dumps(it, ensure_ascii=False, indent=2))


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="ASOS 시간자료 getWthrDataList 테스트")
    parser.add_argument("--start-dt", default="20260101", help="YYYYMMDD")
    parser.add_argument("--end-dt", default="20260330", help="YYYYMMDD")
    parser.add_argument("--start-hh", default="01", help="시각(HH), 예: 01")
    parser.add_argument("--end-hh", default="01", help="시각(HH), 예: 01")
    parser.add_argument("--stn-ids", default=DEFAULT_STN_ID, help="ASOS stnIds (인천 112)")
    parser.add_argument("--page-no", type=int, default=1)
    parser.add_argument("--num-of-rows", type=int, default=100)
    parser.add_argument(
        "--fetch-all-pages",
        action="store_true",
        help="totalCount 기준으로 페이지를 순회해 items 누적(상한은 --max-pages)",
    )
    parser.add_argument("--max-pages", type=int, default=20, help="fetch-all-pages 시 최대 페이지 수")
    args = parser.parse_args()

    key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    if not key:
        print("[ERROR] PUBLIC_DATA_API_KEY 환경변수를 설정하세요.", file=sys.stderr)
        return 1

    print("=== 요청 ===")
    print(f"URL: {API_URL}")
    print(
        f"startDt={args.start_dt} startHh={args.start_hh.zfill(2)} "
        f"endDt={args.end_dt} endHh={args.end_hh.zfill(2)} stnIds={args.stn_ids}"
    )
    print(f"dataCd=ASOS dateCd=HR dataType=JSON pageNo=… numOfRows=…")
    print("(쿼리 파라미터 이름은 API 스펙상 stnIds 입니다. stnlds 오타 주의)")
    print()

    all_items: list[dict[str, Any]] = []
    page = args.page_no
    total_count = None

    while True:
        try:
            payload = fetch_page(
                service_key=key,
                page_no=page,
                num_of_rows=args.num_of_rows,
                start_dt=args.start_dt,
                start_hh=args.start_hh,
                end_dt=args.end_dt,
                end_hh=args.end_hh,
                stn_ids=args.stn_ids,
            )
        except requests.RequestException as e:
            print(f"[ERROR] HTTP 실패: {e}", file=sys.stderr)
            return 1

        header = payload.get("response", {}).get("header", {})
        body = payload.get("response", {}).get("body", {})
        rc = str(header.get("resultCode", ""))

        print(f"=== 응답 헤더 (page={page}) ===")
        print(f"resultCode: {header.get('resultCode')}")
        print(f"resultMsg: {header.get('resultMsg')}")
        print()

        if rc != "00":
            print("[WARN] resultCode가 00이 아닙니다. 아래 body 일부 확인.")
            print(json.dumps(payload, ensure_ascii=False, indent=2)[:3000])
            return 2

        print("=== 응답 바디 메타 ===")
        print(f"numOfRows: {body.get('numOfRows')}")
        print(f"pageNo: {body.get('pageNo')}")
        print(f"totalCount: {body.get('totalCount')}")
        print()

        try:
            total_count = int(body.get("totalCount") or 0)
        except (TypeError, ValueError):
            total_count = 0

        items = extract_items(payload)
        all_items.extend(items)
        print(f"이번 페이지 items 개수: {len(items)}")
        print(f"누적 items 개수: {len(all_items)}")
        if total_count:
            print(f"totalCount 대비 누적: {len(all_items)}/{total_count}")
        print()

        if not args.fetch_all_pages:
            break
        if total_count and len(all_items) >= total_count:
            break
        if page - args.page_no + 1 >= args.max_pages:
            print(f"[INFO] --max-pages={args.max_pages} 도달, 중단")
            break
        if not items:
            break
        page += 1

    print("=== items 키 / 샘플 ===")
    summarize_item_keys(all_items, sample_n=3)

    # 자주 쓰는 필드만 한 줄 요약 (있을 때)
    if all_items:
        print()
        print("=== tm / ta / rn 등 빠른 엿보기 (누적 처음 10행) ===")
        for it in all_items[:10]:
            tm = it.get("tm", "")
            ta = it.get("ta", "")
            rn = it.get("rn", "")
            stn = it.get("stnId", it.get("STN_ID", ""))
            print(f"tm={tm!s} stn={stn!s} ta={ta!s} rn={rn!s}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
