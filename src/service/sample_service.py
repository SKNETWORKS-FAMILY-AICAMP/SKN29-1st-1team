"""
인천 도로 분석용 서비스 레이어 (예시/깡통 데이터).
실제 DB 연동 시 이 모듈의 메서드 구현만 교체하면 됩니다.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)


def get_kpi_metrics(
    start: date | None = None,
    end: date | None = None,
    region: str = "전체",
) -> dict[str, float | int]:
    """대시보드 상단 KPI용 요약 지표 (목업)."""
    _ = (start, end, region)
    return {
        "avg_speed_kmh": 41.8,
        "avg_travel_time_min": 19.2,
        "monitored_links": 156,
        "congestion_index": 0.58,
        "incidents_today": 3,
    }


def get_hourly_traffic_df(
    start: date | None = None,
    end: date | None = None,
    region: str = "전체",
) -> pd.DataFrame:
    """시간대별 교통량 추이 (목업)."""
    _ = (start, end, region)
    hours = np.arange(24)
    base = 800 + 600 * np.sin((hours - 8) * np.pi / 12) ** 2
    noise = RNG.normal(0, 120, 24)
    volume = np.clip(base + noise, 0, None).astype(int)
    speed = np.clip(55 - (volume / 200) + RNG.normal(0, 3, 24), 12, 70).round(1)
    return pd.DataFrame({"hour": hours, "volume": volume, "avg_speed_kmh": speed})


def get_zone_ranking_df(
    start: date | None = None,
    end: date | None = None,
    region: str = "전체",
) -> pd.DataFrame:
    """행정구역·권역별 혼잡 지수 순위 (목업)."""
    _ = (start, end, region)
    zones = [
        "서구",
        "남동구",
        "연수구",
        "미추홀구",
        "부평구",
        "계양구",
        "중구",
        "동구",
        "강화군",
        "옹진군",
    ]
    idx = RNG.uniform(0.25, 0.95, len(zones))
    df = pd.DataFrame(
        {
            "zone": zones,
            "congestion_index": np.round(idx, 2),
            "delay_min": np.round(RNG.uniform(2, 25, len(zones)), 1),
        }
    )
    return df.sort_values("congestion_index", ascending=False).reset_index(drop=True)


def get_link_status_sample(
    start: date | None = None,
    end: date | None = None,
    region: str = "전체",
) -> pd.DataFrame:
    """주요 링크(구간) 상태 스냅샷 (목업)."""
    _ = (start, end, region)
    n = 12
    levels = RNG.choice(["원활", "서행", "지체", "정체"], n, p=[0.35, 0.35, 0.22, 0.08])
    return pd.DataFrame(
        {
            "link_id": [f"ICN-L{i:04d}" for i in range(1001, 1001 + n)],
            "road_name": [
                "인천대로",
                "경인로",
                "제2경인연결로",
                "아라로",
                "송도대로",
                "논현로",
                "서해안고속도로 IC",
                "공항로",
                "영종대로",
                "청라대로",
                "석남로",
                "가좌로",
            ],
            "level": levels,
            "avg_speed_kmh": np.round(RNG.uniform(18, 62, n), 1),
            "volume": RNG.integers(120, 4200, n),
        }
    )


def get_incidents_df(
    start: date | None = None,
    end: date | None = None,
    region: str = "전체",
) -> pd.DataFrame:
    """돌발·공사 등 이벤트 목록 (목업)."""
    _ = (start, end, region)
    kinds = ["사고", "공사", "행사통제", "날씨"]
    rows = []
    for i in range(8):
        d = date(2026, 3, 30) - timedelta(days=i % 5)
        rows.append(
            {
                "occurred_at": f"{d} {8 + i % 12}:30",
                "type": kinds[i % len(kinds)],
                "location": f"인천광역시 {'서구' if i % 2 else '연수구'} 구간",
                "severity": ["낮음", "중간", "높음"][i % 3],
                "cleared": i % 4 == 0,
            }
        )
    return pd.DataFrame(rows)


def get_traffic_raw_sample(
    start: date | None = None,
    end: date | None = None,
    limit: int = 40,
) -> pd.DataFrame:
    """원시 교통 테이블 형태의 예시 스냅샷 (목업)."""
    _ = (start, end)
    n = min(max(limit, 5), 200)
    days = [date(2026, 3, 28) + timedelta(days=int(i % 3)) for i in range(n)]
    return pd.DataFrame(
        {
            "statDate": days,
            "linkId": [f"ICN-{RNG.integers(1000, 9999)}" for _ in range(n)],
            "direction": RNG.choice(["상행", "하행"], n),
            "avgSpeed": np.round(RNG.uniform(14, 68, n), 1),
            "volume": RNG.integers(40, 5200, n),
        }
    )
