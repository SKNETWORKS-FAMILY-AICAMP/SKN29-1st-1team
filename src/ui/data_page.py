"""원시 데이터 뷰어 (예시 스냅샷)."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_data_page(start, end, region: str) -> None:
    _ = region
    st.subheader("원시 데이터 미리보기")
    st.caption("traffic_raw 형태 예시 — 실제 컬럼에 맞게 매핑하면 됩니다.")

    n = st.slider("행 수", min_value=10, max_value=150, value=40, step=10)
    df = sample_service.get_traffic_raw_sample(start, end, limit=n)

    st.dataframe(df, use_container_width=True, hide_index=True)
