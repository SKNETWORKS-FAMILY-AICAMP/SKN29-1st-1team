"""
`pp_incheon_accidents`, `pp_incheon_truck` 집계 (DB_Schema 기준).

날짜 컬럼이 없어 기간(start/end) 필터는 사용하지 않습니다.
도로(`pp_road`) 매핑은 사용하지 않습니다.
"""

from __future__ import annotations

import pandas as pd

from db.db_client import Client


def _df(cols: list[str], rows: list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, columns=cols)


def get_summary_kpis() -> dict[str, dict[str, float | int]]:
    """사고/화물 각각 합계 지표."""
    client = Client()

    out: dict[str, dict[str, float | int]] = {"accidents": {}, "truck": {}}

    for label, table in (("accidents", "pp_incheon_accidents"), ("truck", "pp_incheon_truck")):
        q = f"""
            SELECT
                COUNT(*) AS n_rows,
                COALESCE(SUM(a.occrrnc_cnt), 0) AS sum_occ,
                COALESCE(SUM(a.caslt_cnt), 0) AS sum_caslt,
                COALESCE(SUM(a.dth_dnv_cnt), 0) AS sum_dth,
                COALESCE(SUM(a.se_dnv_cnt), 0) AS sum_se,
                COALESCE(SUM(a.sl_dnv_cnt), 0) AS sum_sl
            FROM {table} a
        """
        cols, rows = client.select(q)
        if not rows:
            out[label] = {c: 0 for c in cols}
            continue
        row = rows[0]
        out[label] = dict(zip(cols, row))
    return out


def get_by_district(*, kind: str) -> pd.DataFrame:
    """시도시군구별 합계 (상위). kind: accidents | truck"""
    table = "pp_incheon_accidents" if kind == "accidents" else "pp_incheon_truck"
    client = Client()
    q = f"""
        SELECT
            COALESCE(NULLIF(TRIM(a.sido_sgg_nm), ''), '(미상)') AS district,
            SUM(a.occrrnc_cnt) AS occrrnc_cnt,
            SUM(a.caslt_cnt) AS caslt_cnt,
            SUM(a.dth_dnv_cnt) AS dth_dnv_cnt
        FROM {table} a
        GROUP BY district
        ORDER BY occrrnc_cnt DESC
        LIMIT 30
    """
    cols, rows = client.select(q)
    return _df(cols, rows)


def get_top_spots(*, kind: str, limit: int = 20) -> pd.DataFrame:
    """지점명별 사고건수 상위."""
    table = "pp_incheon_accidents" if kind == "accidents" else "pp_incheon_truck"
    client = Client()
    q = f"""
        SELECT
            a.spot_nm AS spot_nm,
            SUM(a.occrrnc_cnt) AS occrrnc_cnt,
            SUM(a.caslt_cnt) AS caslt_cnt,
            SUM(a.dth_dnv_cnt) AS dth_dnv_cnt
        FROM {table} a
        GROUP BY a.spot_nm
        ORDER BY occrrnc_cnt DESC
        LIMIT %s
    """
    cols, rows = client.select(q, (limit,))
    return _df(cols, rows)
