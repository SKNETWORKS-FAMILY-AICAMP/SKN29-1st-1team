"""인천 도로 분석 — Streamlit 앱 진입점."""

from __future__ import annotations

from datetime import date, timedelta

import streamlit as st

from ui.incheon_spot_page import render_incheon_spot_page
from ui.overview_page import render_overview_page
from ui.road_stress_page import render_road_stress_page
from ui.traffic_trend_page import render_traffic_trend_page
from service.traffic_service import list_road_names

PAGE_LABELS = [
    "개요 대시보드",
    "시간대 교통 패턴",
    "도로 스트레스 지수",
    "인천 사고·화물 다발",
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
    st.sidebar.subheader("필터")
    today = date(2026, 3, 30)
    col_a, col_b = st.sidebar.columns(2)
    with col_a:
        start = st.date_input("시작일", value=today - timedelta(days=7))
    with col_b:
        end = st.date_input("종료일", value=today)

    road_options = ["전체"]
    try:
        names = list_road_names()
        if names:
            road_options.extend(names)
    except Exception:
        pass

    road_name = st.sidebar.selectbox(
        "도로명",
        options=road_options,
        index=0,
    )

    st.sidebar.caption("선택한 기간과 도로는 대부분의 화면에 적용됩니다.")

    return page, start, end, road_name


def main():
    st.set_page_config(
        page_title="인천 도로 분석",
        page_icon="🛣️",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    page, start, end, road_name = _sidebar_filters()

    st.title("인천 도로 교통 인사이트")
    st.caption("저장된 교통 데이터와 공공 도로 정보를 함께 보여 줍니다.")

    if start > end:
        st.error("시작일이 종료일보다 늦을 수 없습니다.")
        return

    routes = {
        "개요 대시보드": render_overview_page,
        "시간대 교통 패턴": render_traffic_trend_page,
        "도로 스트레스 지수": render_road_stress_page,
        "인천 사고·화물 다발": render_incheon_spot_page,
    }
    routes[page](start, end, road_name)


if __name__ == "__main__":
    main()
