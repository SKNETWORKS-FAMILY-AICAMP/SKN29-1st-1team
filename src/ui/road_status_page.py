"""주요 구간(링크) 상태 모니터링 예시."""

from __future__ import annotations

import streamlit as st

from service import sample_service


def render_road_status_page(start, end, region: str) -> None:
    st.subheader("도로 구간 상태")
    st.caption("주요 링크별 혼잡 등급·속도 (예시 데이터)")

    df = sample_service.get_link_status_sample(start, end, region)

    level = st.multiselect(
        "등급 필터",
        options=["원활", "서행", "지체", "정체"],
        default=["지체", "정체"],
    )
    if level:
        view = df[df["level"].isin(level)]
    else:
        view = df

    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "level": st.column_config.TextColumn("혼잡등급"),
            "avg_speed_kmh": st.column_config.NumberColumn("평균속도(km/h)", format="%.1f"),
            "volume": st.column_config.NumberColumn("교통량", format="%d"),
        },
    )

    agg = view.groupby("level", as_index=False)["link_id"].count()
    if not agg.empty:
        st.markdown("##### 선택 등급 구간 수")
        st.bar_chart(agg.set_index("level"))
