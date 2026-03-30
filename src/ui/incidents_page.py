"""돌발·이벤트 타임라인 예시."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_incidents_page(start, end, region: str) -> None:
    st.subheader("돌발·이벤트")
    st.caption("사고·공사·통제 등 (예시 데이터)")

    df = sample_service.get_incidents_df(start, end, region)

    only_open = st.toggle("미처리만 보기", value=False)
    view = df[~df["cleared"]] if only_open else df

    for _, row in view.iterrows():
        badge = "종료" if row["cleared"] else "진행중"
        st.markdown(f"**[{row['type']}]** {row['location']} · 심각도 {row['severity']} · `{badge}`")
        st.caption(row["occurred_at"])
        st.divider()

    with st.expander("표로 보기"):
        st.dataframe(view, use_container_width=True, hide_index=True)
