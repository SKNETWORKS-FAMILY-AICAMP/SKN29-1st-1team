"""도로별 운전자 스트레스(혼잡) 지수 인사이트."""

from __future__ import annotations

import streamlit as st

from service.stress_index_service import compute_road_stress


def render_road_stress_page(start, end, road_name: str) -> None:
    st.subheader("도로 스트레스 지수")
    st.caption(
        "통행량이 몰릴 때 속도 하락, 속도 편차, 출퇴근 집중, 피크 반복성(피크 집중도) 등을 종합해 "
        "개선 우선순위(Top N)를 보여줍니다."
    )

    top_n = st.slider("Top N", min_value=5, max_value=50, value=15, step=5)

    try:
        res = compute_road_stress(start, end, road_name)
        df = res.roads
    except Exception as e:
        st.error("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
        return

    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다. 기간이나 도로를 바꿔 보세요.")
        return

    st.caption(f"선택 기간 {start} ~ {end} · 도로 {road_name}")

    # 전체 랭킹
    show_df = df.head(top_n).copy()

    c1, c2 = st.columns((1.3, 1))
    with c1:
        st.markdown(f"##### Top {top_n} 스트레스 도로 (개선 우선)")
        st.dataframe(
            show_df.rename(
                columns={
                    "road_name": "도로명",
                    "stress_score": "스트레스 점수(0~100)",
                    "high_volume_speed_drop_index": "혼잡 민감도(통행량↑/속도↓)",
                    "speed_std": "속도 편차(표준편차)",
                    "peakiness_index": "피크 집중도(max/mean)",
                    "commute_concentration": "출퇴근 집중도(비중)",
                    "avg_speed": "평균 속도",
                    "avg_volume": "평균 통행량",
                    "n_samples": "표본 수",
                    "speed_drop_top_vs_bottom": "속도 하락(km/h)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        st.markdown("##### 스트레스 점수 분포 (상위)")
        st.bar_chart(show_df.set_index("road_name")[["stress_score"]])

    st.divider()

    # 단일 도로 드릴다운 (선택)
    sel = st.selectbox(
        "드릴다운 도로 선택",
        options=show_df["road_name"].tolist(),
        index=0,
    )
    one = df[df["road_name"] == sel].head(1)
    if not one.empty:
        r = one.iloc[0]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("스트레스 점수", f"{r['stress_score']}")
        m2.metric("혼잡 민감도", f"{r['high_volume_speed_drop_index']}")
        m3.metric("속도 편차(표준편차)", f"{r['speed_std']}")
        m4.metric("출퇴근 집중도", f"{r['commute_concentration']}")

        st.caption(
            "혼잡 민감도: 차가 많이 지날 때 속도가 평소보다 얼마나 더 떨어지는지(상대적인 값)입니다."
        )

