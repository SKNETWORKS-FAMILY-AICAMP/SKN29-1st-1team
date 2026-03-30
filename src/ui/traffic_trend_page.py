"""시간대·요일 패턴 인사이트 예시."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_traffic_trend_page(start, end, region: str) -> None:
    st.subheader("시간대 교통 패턴")
    st.caption("피크 구간·속도-교통량 관계 탐색 (예시 데이터)")

    hourly = sample_service.get_hourly_traffic_df(start, end, region)

    tab1, tab2 = st.tabs(["교통량 추이", "속도 추이"])
    with tab1:
        st.area_chart(hourly.set_index("hour")[["volume"]])
    with tab2:
        st.line_chart(hourly.set_index("hour")[["avg_speed_kmh"]])

    st.markdown("##### 원본 표 (샘플)")
    st.dataframe(hourly, use_container_width=True, hide_index=True)
