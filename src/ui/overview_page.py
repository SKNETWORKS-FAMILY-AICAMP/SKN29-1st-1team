"""개요 대시보드 — KPI + 추이 차트 예시."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_overview_page(start, end, region: str) -> None:
    st.subheader("한눈에 보기")
    st.caption("선택한 기간·지역 기준 요약 (예시 데이터)")

    kpi = sample_service.get_kpi_metrics(start, end, region)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("평균 속도", f"{kpi['avg_speed_kmh']:.1f} km/h", delta="-0.8")
    with c2:
        st.metric("평균 통행시간", f"{kpi['avg_travel_time_min']:.1f} 분", delta="1.2")
    with c3:
        st.metric("모니터링 링크", f"{kpi['monitored_links']:,}개")
    with c4:
        st.metric("혼잡 지수", f"{kpi['congestion_index']:.2f}", delta="-0.03")
    with c5:
        st.metric("금일 돌발", f"{kpi['incidents_today']}건")

    st.divider()
    left, right = st.columns((1.1, 1))

    with left:
        st.markdown("##### 시간대별 교통량·속도")
        hourly = sample_service.get_hourly_traffic_df(start, end, region)
        st.line_chart(hourly.set_index("hour")[["volume", "avg_speed_kmh"]])

    with right:
        st.markdown("##### 권역별 혼잡 지수 (상위)")
        zones = sample_service.get_zone_ranking_df(start, end, region).head(8)
        st.bar_chart(zones.set_index("zone")[["congestion_index"]])

    st.info("실제 서비스에서는 API/DB 조회 결과로 위 지표가 채워집니다.")
