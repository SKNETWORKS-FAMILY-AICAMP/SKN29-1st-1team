"""인천 도로 분석 — Streamlit 앱 진입점 (UI 초안)."""

from __future__ import annotations

from datetime import date, timedelta

import streamlit as st

from ui.data_page import render_data_page
from ui.incidents_page import render_incidents_page
from ui.overview_page import render_overview_page
from ui.road_status_page import render_road_status_page
from ui.traffic_trend_page import render_traffic_trend_page
from ui.zone_insight_page import render_zone_insight_page

PAGE_LABELS = [
    "개요 대시보드",
    "시간대 교통 패턴",
    "구역·권역 비교",
    "도로 구간 상태",
    "돌발·이벤트",
    "원시 데이터",
]


def _sidebar_filters():
    st.sidebar.header("인사이트 메뉴")
    page = st.sidebar.radio(
        "페이지",
        options=PAGE_LABELS,
        index=0,
        label_visibility="collapsed",
    )

    st.sidebar.divider()
    st.sidebar.subheader("필터 (데모)")
    today = date(2026, 3, 30)
    col_a, col_b = st.sidebar.columns(2)
    with col_a:
        start = st.date_input("시작일", value=today - timedelta(days=7))
    with col_b:
        end = st.date_input("종료일", value=today)

    region = st.sidebar.selectbox(
        "지역",
        options=["전체", "서구", "연수구", "남동구", "미추홀구", "부평구", "계양구", "중구·동구"],
        index=0,
    )

    st.sidebar.caption("필터는 목업 서비스에도 넘겨 두었습니다. 실제 연동 시 쿼리 파라미터로 사용하세요.")

    return page, start, end, region


def main():
    st.set_page_config(
        page_title="인천 도로 분석",
        page_icon="🛣️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    page, start, end, region = _sidebar_filters()

    st.title("인천 도로 교통 인사이트")
    st.caption("프로토타입 · `sample_service`는 예시(깡통) 데이터입니다.")

    if start > end:
        st.error("시작일이 종료일보다 늦을 수 없습니다.")
        return

    routes = {
        "개요 대시보드": render_overview_page,
        "시간대 교통 패턴": render_traffic_trend_page,
        "구역·권역 비교": render_zone_insight_page,
        "도로 구간 상태": render_road_status_page,
        "돌발·이벤트": render_incidents_page,
        "원시 데이터": render_data_page,
    }
    routes[page](start, end, region)


if __name__ == "__main__":
    main()
