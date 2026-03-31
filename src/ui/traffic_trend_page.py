"""시간대·요일 패턴 인사이트."""

from __future__ import annotations

import streamlit as st

from service.traffic_service import get_dow_hour_pattern


def render_traffic_trend_page(start, end, road_name: str) -> None:
    st.subheader("시간대 교통 패턴")
    st.caption("요일별·시간대별 교통량/속도 패턴")

    pattern = None
    try:
        pattern = get_dow_hour_pattern(start, end, road_name)
    except Exception as e:
        st.error("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
        return

    if pattern is None or pattern.empty:
        st.warning("선택 기간·도로에 맞는 데이터가 없습니다.")
        return

    st.caption(f"선택 기간 {start} ~ {end} · 도로 {road_name}")

    # 요일별 요약
    by_dow = (
        pattern.groupby(["dow", "dow_label"], as_index=False)[["avg_volume", "avg_speed_kmh", "samples"]]
        .mean(numeric_only=True)
        .sort_values("dow")
    )
    import pandas as pd

    dow_order = ["일", "월", "화", "수", "목", "금", "토"]
    by_dow["dow_label"] = by_dow["dow_label"].astype(str)
    by_dow["dow_label"] = by_dow["dow_label"].astype(
        pd.CategoricalDtype(categories=dow_order, ordered=True)
    )
    by_dow = by_dow.sort_values("dow_label").set_index("dow_label")

    # 시간대별 요약
    by_hour = (
        pattern.groupby("hour", as_index=False)[["avg_volume", "avg_speed_kmh", "samples"]]
        .mean(numeric_only=True)
        .sort_values("hour")
        .set_index("hour")
    )

    tab1, tab2, tab3 = st.tabs(["요일별", "시간대별", "요일×시간대(표)"])
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 요일별 평균 교통량")
            st.bar_chart(by_dow[["avg_volume"]])
        with c2:
            st.markdown("##### 요일별 평균 속도(km/h)")
            st.bar_chart(by_dow[["avg_speed_kmh"]])

        st.markdown("##### 요약 표")
        st.dataframe(
            by_dow.reset_index().rename(
                columns={
                    "dow_label": "요일",
                    "avg_volume": "평균 교통량",
                    "avg_speed_kmh": "평균 속도(km/h)",
                    "samples": "집계 샘플 수(평균)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 시간대별 평균 교통량")
            st.area_chart(by_hour[["avg_volume"]])
        with c2:
            st.markdown("##### 시간대별 평균 속도(km/h)")
            st.line_chart(by_hour[["avg_speed_kmh"]])

        st.markdown("##### 요약 표")
        st.dataframe(
            by_hour.reset_index().rename(
                columns={
                    "hour": "시간",
                    "avg_volume": "평균 교통량",
                    "avg_speed_kmh": "평균 속도(km/h)",
                    "samples": "집계 샘플 수(평균)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with tab3:
        st.markdown("##### 요일×시간대 집계 (평균)")
        view = pattern[["dow_label", "hour", "avg_volume", "avg_speed_kmh", "samples"]].copy()
        view = view.rename(
            columns={
                "dow_label": "요일",
                "hour": "시간",
                "avg_volume": "평균 교통량",
                "avg_speed_kmh": "평균 속도(km/h)",
                "samples": "집계 샘플 수",
            }
        )
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.caption(
        "같은 도로·같은 시각에 맞춰 교통량과 속도를 묶은 뒤, 요일과 시간대별로 평균을 냅니다."
    )
