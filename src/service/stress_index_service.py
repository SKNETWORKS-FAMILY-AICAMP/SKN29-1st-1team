"""
도로별 운전자 스트레스 지수 산출 서비스.

지표(기간/도로명 필터 기반):
- speed_variability: 속도 편차(표준편차)
- peakiness_index: 시간대별 평균 통행량의 피크 집중도 (max/mean)
- commute_concentration: 평일 출퇴근(07-09, 17-19) 통행량 비중
- high_volume_speed_drop: "통행량 상위 구간에서 속도가 뚝 떨어지는 정도"
  (도로별 통행량을 5분위로 나눠 상위(5) vs 하위(1) 평균 속도 차이)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

import pandas as pd

from db.db_client import Client


def _to_dt_range(start: date, end: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start, time.min)
    end_exclusive = datetime.combine(end + timedelta(days=1), time.min)
    return start_dt, end_exclusive


@dataclass(frozen=True)
class RoadStressResult:
    roads: pd.DataFrame  # road-level metrics + stress_score


def compute_road_stress(
    start: date,
    end: date,
    road_name: str = "전체",
) -> RoadStressResult:
    """
    반환 df 컬럼:
    - road_name
    - n_samples
    - avg_volume, avg_speed
    - speed_std
    - peakiness_index
    - commute_concentration
    - speed_drop_top_vs_bottom
    - high_volume_speed_drop_index
    - stress_score
    """
    start_dt, end_exclusive = _to_dt_range(start, end)
    client = Client()

    # 1) PP_traffic·pp_speed 조인 결과 (volume, speed)
    #    - NULL 제거: volume/speed 둘 다 있는 값만 사용
    base = """
        SELECT
            r.road_name AS road_name,
            t.datetime AS dt,
            t.volume AS volume,
            s.speed AS speed
        FROM PP_traffic t
        JOIN pp_road r
          ON r.road_id = t.road_id
        JOIN pp_speed s
          ON s.road_id = t.road_id
         AND s.direction = t.direction
         AND s.datetime = t.datetime
        WHERE t.datetime >= %s
          AND t.datetime < %s
          AND t.volume IS NOT NULL
          AND s.speed IS NOT NULL
          AND (%s = '전체' OR r.road_name = %s)
    """
    params = (start_dt, end_exclusive, road_name, road_name)
    cols, rows = client.select(base, params)
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return RoadStressResult(roads=df)

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df = df.dropna(subset=["volume", "speed"])
    df["dt"] = pd.to_datetime(df["dt"])
    df["hour"] = df["dt"].dt.hour
    df["dow"] = df["dt"].dt.dayofweek  # 0=Mon..6=Sun
    df["is_weekday"] = df["dow"].between(0, 4)
    df["is_commute"] = df["is_weekday"] & df["hour"].isin([7, 8, 17, 18])

    # 2) 도로별 기본 통계
    road_basic = (
        df.groupby("road_name", as_index=False)
        .agg(
            n_samples=("speed", "count"),
            avg_volume=("volume", "mean"),
            avg_speed=("speed", "mean"),
            speed_std=("speed", "std"),
        )
    )

    # 3) 시간대 피크 집중도: (도로, hour) 평균 volume -> max/mean
    hourly = df.groupby(["road_name", "hour"], as_index=False).agg(hourly_avg_volume=("volume", "mean"))
    peak = hourly.groupby("road_name", as_index=False).agg(
        peak_hour_volume=("hourly_avg_volume", "max"),
        mean_hour_volume=("hourly_avg_volume", "mean"),
    )
    peak["peakiness_index"] = peak["peak_hour_volume"] / peak["mean_hour_volume"]
    peak = peak[["road_name", "peakiness_index"]]

    # 4) 출퇴근 집중도: 평일 기준 출퇴근 volume 합 / 평일 전체 volume 합
    commute = (
        df[df["is_weekday"]]
        .groupby("road_name", as_index=False)
        .agg(
            weekday_volume=("volume", "sum"),
            commute_volume=("volume", lambda s: float(s[df.loc[s.index, "is_commute"]].sum())),
        )
    )
    commute["commute_concentration"] = commute["commute_volume"] / commute["weekday_volume"].replace(0, pd.NA)
    commute = commute[["road_name", "commute_concentration"]]

    # 5) 통행량 상위 vs 하위 속도 차이 (volume 5분위)
    #    - MySQL percentile을 SQL로 풀지 않고 pandas로 안정적으로 계산
    def _speed_drop(g: pd.DataFrame) -> pd.Series:
        if len(g) < 50:
            return pd.Series({"speed_drop_top_vs_bottom": pd.NA, "high_volume_speed_drop_index": pd.NA})
        try:
            q = pd.qcut(g["volume"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop")
        except Exception:
            return pd.Series({"speed_drop_top_vs_bottom": pd.NA, "high_volume_speed_drop_index": pd.NA})
        gg = g.assign(vol_bucket=q)
        low = gg[gg["vol_bucket"] == 1]["speed"].mean()
        high = gg[gg["vol_bucket"] == 5]["speed"].mean()
        base_speed = g["speed"].mean()
        drop = (low - high) if pd.notna(low) and pd.notna(high) else pd.NA
        # 상대 지수: 평균속도로 나눠 도로 간 비교 가능하게
        idx = (drop / base_speed) if (pd.notna(drop) and base_speed and base_speed > 0) else pd.NA
        return pd.Series({"speed_drop_top_vs_bottom": drop, "high_volume_speed_drop_index": idx})

    drop_df = df.groupby("road_name", as_index=False).apply(_speed_drop)
    # groupby.apply 결과가 MultiIndex가 될 수 있어 정리
    if isinstance(drop_df.index, pd.MultiIndex):
        drop_df = drop_df.reset_index(level=0)
    drop_df = drop_df.reset_index(drop=True)
    if "road_name" not in drop_df.columns:
        drop_df["road_name"] = df["road_name"].unique()[: len(drop_df)]

    # 6) 머지
    out = road_basic.merge(peak, on="road_name", how="left").merge(commute, on="road_name", how="left").merge(
        drop_df, on="road_name", how="left"
    )

    # 결측 처리
    for c in ["speed_std", "peakiness_index", "commute_concentration", "high_volume_speed_drop_index"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # 7) 최종 stress_score (0~100)
    #    - 각 지표를 robust min-max로 정규화 후 가중합
    def _norm(s: pd.Series) -> pd.Series:
        s = s.astype(float)
        lo = s.quantile(0.05)
        hi = s.quantile(0.95)
        if pd.isna(lo) or pd.isna(hi) or hi <= lo:
            return pd.Series([0.0] * len(s), index=s.index)
        x = (s.clip(lo, hi) - lo) / (hi - lo)
        return x.fillna(0.0)

    n_drop = _norm(out["high_volume_speed_drop_index"].fillna(0))
    n_var = _norm(out["speed_std"].fillna(0))
    n_peak = _norm(out["peakiness_index"].fillna(0))
    n_comm = _norm(out["commute_concentration"].fillna(0))

    out["stress_score"] = (0.40 * n_drop + 0.25 * n_var + 0.20 * n_peak + 0.15 * n_comm) * 100.0
    out["stress_score"] = out["stress_score"].round(1)

    # 보기 좋게
    out["avg_volume"] = out["avg_volume"].round(1)
    out["avg_speed"] = out["avg_speed"].round(1)
    out["speed_std"] = out["speed_std"].round(1)
    out["peakiness_index"] = out["peakiness_index"].round(2)
    out["commute_concentration"] = out["commute_concentration"].round(3)
    out["speed_drop_top_vs_bottom"] = pd.to_numeric(out["speed_drop_top_vs_bottom"], errors="coerce").round(1)
    out["high_volume_speed_drop_index"] = pd.to_numeric(out["high_volume_speed_drop_index"], errors="coerce").round(3)

    out = out.sort_values(["stress_score", "n_samples"], ascending=[False, False]).reset_index(drop=True)
    return RoadStressResult(roads=out)

