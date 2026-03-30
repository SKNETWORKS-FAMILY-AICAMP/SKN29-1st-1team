"""구역·권역 비교 인사이트 예시."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_zone_insight_page(start, end, region: str) -> None:
    st.subheader("구역·권역 인사이트")
    st.caption("행정구역별 혼잡·지체 시간 비교 (예시 데이터)")

    df = sample_service.get_zone_ranking_df(start, end, region)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("##### 혼잡 지수")
        st.bar_chart(df.set_index("zone")[["congestion_index"]])
    with c2:
        st.markdown("##### 평균 지체(분)")
        st.bar_chart(df.set_index("zone")[["delay_min"]])

    st.dataframe(df, use_container_width=True, hide_index=True)
