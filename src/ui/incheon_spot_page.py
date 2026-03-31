"""인천 사고다발지·화물차 사고다발지 요약."""

from __future__ import annotations

import streamlit as st

from service.incheon_spot_service import (
    get_by_district,
    get_summary_kpis,
    get_top_spots,
)


def render_incheon_spot_page(start, end, road_name: str) -> None:
    _ = (start, end, road_name)
    st.subheader("인천 사고·화물 다발지")
    st.caption(
        "이 자료는 **발생 시각이 없어** 사이드바의 **기간**으로 나누어 보지는 못합니다. "
        "**도로명** 필터도 이 화면에는 적용되지 않습니다."
    )

    try:
        kpi = get_summary_kpis()
    except Exception:
        st.error("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
        return

    acc = kpi.get("accidents", {})
    trk = kpi.get("truck", {})

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("사고다발지 행 수", f"{int(acc.get('n_rows', 0)):,}")
    with c2:
        st.metric("사고다발 — 사고건수 합", f"{int(acc.get('sum_occ', 0)):,}")
    with c3:
        st.metric("화물다발지 행 수", f"{int(trk.get('n_rows', 0)):,}")
    with c4:
        st.metric("화물다발 — 사고건수 합", f"{int(trk.get('sum_occ', 0)):,}")

    r1, r2 = st.columns(2)
    with r1:
        st.markdown("##### 일반 사고다발 — 사상자·사망 합계")
        st.write(
            f"사상자: **{int(acc.get('sum_caslt', 0)):,}** · "
            f"사망: **{int(acc.get('sum_dth', 0)):,}** · "
            f"중상: **{int(acc.get('sum_se', 0)):,}** · "
            f"경상: **{int(acc.get('sum_sl', 0)):,}**"
        )
    with r2:
        st.markdown("##### 화물 사고다발 — 사상자·사망 합계")
        st.write(
            f"사상자: **{int(trk.get('sum_caslt', 0)):,}** · "
            f"사망: **{int(trk.get('sum_dth', 0)):,}** · "
            f"중상: **{int(trk.get('sum_se', 0)):,}** · "
            f"경상: **{int(trk.get('sum_sl', 0)):,}**"
        )

    tab1, tab2 = st.tabs(["시군구별", "지점 TOP"])
    with tab1:
        c1, c2 = st.columns(2)
        try:
            d_a = get_by_district(kind="accidents")
            d_t = get_by_district(kind="truck")
        except Exception:
            st.error("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
            return
        with c1:
            st.markdown("###### 일반 사고다발 — 시군구별 사고건수")
            if d_a.empty:
                st.info("표시할 데이터가 없습니다.")
            else:
                st.bar_chart(d_a.set_index("district")[["occrrnc_cnt"]])
                st.dataframe(d_a, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("###### 화물 사고다발 — 시군구별 사고건수")
            if d_t.empty:
                st.info("표시할 데이터가 없습니다.")
            else:
                st.bar_chart(d_t.set_index("district")[["occrrnc_cnt"]])
                st.dataframe(d_t, use_container_width=True, hide_index=True)

    with tab2:
        c1, c2 = st.columns(2)
        try:
            s_a = get_top_spots(kind="accidents", limit=20)
            s_t = get_top_spots(kind="truck", limit=20)
        except Exception:
            st.error("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.")
            return
        with c1:
            st.markdown("###### 일반 — 지점별 사고건수 TOP")
            if s_a.empty:
                st.info("표시할 데이터가 없습니다.")
            else:
                st.bar_chart(s_a.head(15).set_index("spot_nm")[["occrrnc_cnt"]])
                st.dataframe(s_a, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("###### 화물 — 지점별 사고건수 TOP")
            if s_t.empty:
                st.info("표시할 데이터가 없습니다.")
            else:
                st.bar_chart(s_t.head(15).set_index("spot_nm")[["occrrnc_cnt"]])
                st.dataframe(s_t, use_container_width=True, hide_index=True)
