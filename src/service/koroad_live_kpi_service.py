"""
인천광역시 도로 소통정보 공공 API (일 단위).

- 통행량: ICRoadVolStat / NodeLink_Trfc_DD
- 속도: ICRoadStat_v2 / STAT-Speed_DD_Road

집계가 배치로 반영되므로 KPI는 직전 1시간 구간(KST) 기준이며,
오늘 하루(0~23시) 평균은 같은 날짜 데이터로 계산해 델타 비교에 사용합니다.

환경변수: PUBLIC_DATA_API_KEY
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

KST = ZoneInfo("Asia/Seoul")

URL_VOLUME = "http://apis.data.go.kr/6280000/ICRoadVolStat/NodeLink_Trfc_DD"
URL_SPEED = "http://apis.data.go.kr/6280000/ICRoadStat_v2/STAT-Speed_DD_Road"

PAGE_SIZE = 3000


@dataclass(frozen=True)
class RoadKpiSlice:
    """한 시각 구간 또는 일평균 등 한 덩어리 KPI."""

    speed_kmh: float | None
    volume: float | None
    speed_rows: int
    volume_rows: int


@dataclass(frozen=True)
class LiveRoadKpi:
    """직전 1시간 슬롯 KPI + 오늘 일평균(델타 기준)."""

    ymd: str
    road_name: str
    one_hour_ago: RoadKpiSlice
    one_hour_ago_ymd: str
    one_hour_ago_hour: int
    today_avg: RoadKpiSlice


def _items_from_body(body: dict[str, Any]) -> list[dict[str, Any]]:
    raw = body.get("items")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        if "item" in raw:
            v = raw["item"]
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
            if isinstance(v, dict):
                return [v]
        return [raw] if raw else []
    return []


def _fetch_all_pages(base_url: str, ymd: str, service_key: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    while page <= 50:
        params = {
            "serviceKey": service_key,
            "pageNo": page,
            "numOfRows": PAGE_SIZE,
            "YMD": ymd,
        }
        r = requests.get(base_url, params=params, timeout=45)
        r.raise_for_status()
        data = r.json()
        header = data.get("response", {}).get("header", {})
        if str(header.get("resultCode", "")) != "00":
            raise RuntimeError(f"API 오류: {header.get('resultCode')} {header.get('resultMsg')}")
        body = data.get("response", {}).get("body") or {}
        items = _items_from_body(body)
        out.extend(items)
        try:
            total = int(body.get("totalCount") or 0)
        except (TypeError, ValueError):
            total = 0
        if total and len(out) >= total:
            break
        if not items:
            break
        if len(items) < PAGE_SIZE:
            break
        page += 1
    return out


def _hour_value(row: dict[str, Any], hour: int) -> int | None:
    key = f"hour{hour:02d}"
    v = row.get(key)
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _mean_hour(items: list[dict[str, Any]], hour: int) -> tuple[float | None, int]:
    vals: list[int] = []
    for it in items:
        x = _hour_value(it, hour)
        if x is not None:
            vals.append(x)
    if not vals:
        return None, 0
    return sum(vals) / len(vals), len(vals)


def _mean_hour_filtered(
    items: list[dict[str, Any]], hour: int, road_name: str
) -> tuple[float | None, int]:
    if road_name == "전체":
        return _mean_hour(items, hour)
    target = road_name.strip()
    filtered = [it for it in items if str(it.get("roadName", "")).strip() == target]
    return _mean_hour(filtered, hour)


def _row_mean_hours(row: dict[str, Any], hours: range) -> float | None:
    """한 행에서 hour00~hour23 중 숫자로 읽히는 값만으로 평균."""
    vals: list[int] = []
    for h in hours:
        x = _hour_value(row, h)
        if x is not None:
            vals.append(x)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _mean_day_rowwise_filtered(
    items: list[dict[str, Any]], road_name: str, hours: range | None = None
) -> tuple[float | None, int]:
    """각 행의 일간(기본 24시간) 평균을 구한 뒤, 행 단위로 다시 평균."""
    hr = hours if hours is not None else range(24)
    if road_name == "전체":
        filtered = items
    else:
        target = road_name.strip()
        filtered = [it for it in items if str(it.get("roadName", "")).strip() == target]
    row_means: list[float] = []
    for it in filtered:
        m = _row_mean_hours(it, hr)
        if m is not None:
            row_means.append(m)
    if not row_means:
        return None, 0
    return sum(row_means) / len(row_means), len(row_means)


def get_live_road_kpi(road_name: str = "전체", ymd: str | None = None) -> LiveRoadKpi:
    load_dotenv()
    key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    if not key:
        raise ValueError("공공데이터 API 키가 설정되어 있지 않습니다. 관리자에게 문의하세요.")

    now = datetime.now(KST)
    if ymd is None:
        ymd = now.strftime("%Y%m%d")
    vol_today = _fetch_all_pages(URL_VOLUME, ymd, key)
    spd_today = _fetch_all_pages(URL_SPEED, ymd, key)

    prev_dt = now - timedelta(hours=1)
    prev_ymd = prev_dt.strftime("%Y%m%d")
    prev_hour = prev_dt.hour
    prev_same_day = prev_ymd == ymd

    if prev_same_day:
        vol_prev = vol_today
        spd_prev = spd_today
    else:
        vol_prev = _fetch_all_pages(URL_VOLUME, prev_ymd, key)
        spd_prev = _fetch_all_pages(URL_SPEED, prev_ymd, key)

    vol_prev_h, vol_n_prev = _mean_hour_filtered(vol_prev, prev_hour, road_name)
    spd_prev_h, spd_n_prev = _mean_hour_filtered(spd_prev, prev_hour, road_name)

    vol_day, vol_n_day = _mean_day_rowwise_filtered(vol_today, road_name, range(24))
    spd_day, spd_n_day = _mean_day_rowwise_filtered(spd_today, road_name, range(24))

    return LiveRoadKpi(
        ymd=ymd,
        road_name=road_name,
        one_hour_ago=RoadKpiSlice(
            speed_kmh=spd_prev_h,
            volume=vol_prev_h,
            speed_rows=spd_n_prev,
            volume_rows=vol_n_prev,
        ),
        one_hour_ago_ymd=prev_ymd,
        one_hour_ago_hour=prev_hour,
        today_avg=RoadKpiSlice(
            speed_kmh=spd_day,
            volume=vol_day,
            speed_rows=spd_n_day,
            volume_rows=vol_n_day,
        ),
    )
