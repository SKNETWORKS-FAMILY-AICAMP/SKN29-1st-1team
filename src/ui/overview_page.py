"""개요 — 인천 도로공사 공공데이터 기준 직전 1시간 구간 도로 KPI."""

from __future__ import annotations

import streamlit as st

from service.koroad_live_kpi_service import LiveRoadKpi, get_live_road_kpi


def _fmt_speed(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:.1f} km/h"


def _fmt_vol(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:.0f}"


def _delta_speed_delta(prev: float | None, avg: float | None) -> str | None:
    if prev is None or avg is None:
        return None
    d = prev - avg
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.1f} km/h"


def _delta_vol_delta(prev: float | None, avg: float | None) -> str | None:
    if prev is None or avg is None:
        return None
    d = prev - avg
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.0f}"


@st.cache_data(ttl=120)
def _cached_live_kpi(road_name: str) -> LiveRoadKpi:
    return get_live_road_kpi(road_name=road_name, ymd=None)


def render_overview_page(start, end, road_name: str) -> None:
    _ = (start, end)
    st.subheader("한눈에 보기")
    st.caption(
        "인천 도로의 통행량·속도는 공공데이터 기준으로, 대략 **1시간 단위**로 갱신됩니다. "
        "아래 수치는 **바로 직전 1시간** 구간의 평균이며, "
        "변동 폭은 **오늘 하루 평균**과 비교한 값입니다. "
        "이 화면은 사이드바의 기간 설정을 쓰지 않습니다."
    )

    if st.button("지표 새로고침"):
        _cached_live_kpi.clear()
        st.rerun()

    try:
        kpi = _cached_live_kpi(road_name)
    except ValueError as e:
        st.error(str(e))
        return
    except Exception:
        st.error("도로 정보를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
        return

    prev_label = f"{kpi.one_hour_ago_ymd} {kpi.one_hour_ago_hour:02d}시"
    pa = kpi.one_hour_ago
    ta = kpi.today_avg

    st.caption(
        f"기준일 {kpi.ymd} · 직전 구간 {prev_label} · 도로 {kpi.road_name}"
    )

    help_delta = "직전 1시간 평균과 오늘 하루 평균의 차이입니다."
    c1, c2 = st.columns(2)
    with c1:
        d_spd = _delta_speed_delta(pa.speed_kmh, ta.speed_kmh)
        help_spd = (
            f"오늘 하루 평균 속도(참고): {_fmt_speed(ta.speed_kmh)}. {help_delta}"
        )
        if pa.speed_kmh is not None:
            st.metric(
                "평균 속도 (1시간 전)",
                _fmt_speed(pa.speed_kmh),
                delta=d_spd,
                delta_color="normal",
                help=help_spd,
            )
        else:
            st.metric("평균 속도 (1시간 전)", "—", help=help_spd)
    with c2:
        d_vol = _delta_vol_delta(pa.volume, ta.volume)
        help_vol = (
            f"오늘 하루 평균 통행량(참고): {_fmt_vol(ta.volume)}. {help_delta}"
        )
        if pa.volume is not None:
            st.metric(
                "평균 통행량 (1시간 전)",
                _fmt_vol(pa.volume),
                delta=d_vol,
                delta_color="normal",
                help=help_vol,
            )
        else:
            st.metric("평균 통행량 (1시간 전)", "—", help=help_vol)

    st.info(
        "도로를 고르면 해당 도로(상·하행 포함)만 집계하고, "
        "「전체」는 노선 전체 평균입니다. "
        "수치가 0에 가깝게 나오면 아직 집계가 반영되지 않았을 수 있습니다."
    )
