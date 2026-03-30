"""
동윤(raw) 사고/트럭 원시 테이블 적재 스크립트.

대상 테이블
- incheon_accidents: 도로공사 오픈API frequentzone/lg
- incheon_truck: 도로공사 오픈API frequentzone/truck

테이블 생성 포함(CREATE TABLE) + 적재(IN INSERT IGNORE)까지 한 번에 수행합니다.

환경변수
- DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
- PUBLIC_DATA_API_KEY (오픈API authKey)

옵션
- API 키가 없으면 사고(accidents)는 로컬 `17_24_lg.csv`로 대체 적재 가능
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable

import mysql.connector as mc
import pandas as pd
import requests
from dotenv import load_dotenv


def get_db_cfg() -> dict[str, object]:
    load_dotenv()
    host = os.getenv("DB_HOST", "")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    database = os.getenv("DB_DATABASE", "")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_DATABASE": database,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "autocommit": False,
    }


CREATE_TABLE_ACCIDENTS = """
    CREATE TABLE IF NOT EXISTS incheon_accidents (
        afos_id VARCHAR(50),
        afos_fid VARCHAR(50) PRIMARY KEY,
        sido_sgg_nm VARCHAR(100),
        spot_nm VARCHAR(255),
        occrrnc_cnt INT,
        caslt_cnt INT,
        dth_dnv_cnt INT,
        se_dnv_cnt INT,
        sl_dnv_cnt INT,
        lo_crd DOUBLE,
        la_crd DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

CREATE_TABLE_TRUCK = """
    CREATE TABLE IF NOT EXISTS incheon_truck (
        afos_id VARCHAR(50),
        afos_fid VARCHAR(50) PRIMARY KEY,
        sido_sgg_nm VARCHAR(100),
        spot_nm VARCHAR(255),
        occrrnc_cnt INT,
        caslt_cnt INT,
        dth_dnv_cnt INT,
        se_dnv_cnt INT,
        sl_dnv_cnt INT,
        lo_crd DOUBLE,
        la_crd DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()

INSERT_ACCIDENTS = """
    INSERT IGNORE INTO incheon_accidents
    (afos_id, afos_fid, sido_sgg_nm, spot_nm, occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt, lo_crd, la_crd)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

INSERT_TRUCK = """
    INSERT IGNORE INTO incheon_truck
    (afos_id, afos_fid, sido_sgg_nm, spot_nm, occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt, lo_crd, la_crd)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()


def normalize_accident_df(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = ["occrrnc_cnt", "caslt_cnt", "dth_dnv_cnt", "se_dnv_cnt", "sl_dnv_cnt"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        else:
            df[col] = 0

    for col in ["lo_crd", "la_crd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    required_cols = [
        "afos_id",
        "afos_fid",
        "sido_sgg_nm",
        "spot_nm",
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "lo_crd",
        "la_crd",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = None

    return df[required_cols]


def bulk_insert_df(
    cursor: mc.cursor.MySQLCursor,
    conn: mc.MySQLConnection,
    insert_sql: str,
    df: pd.DataFrame,
) -> int:
    cols = list(df.columns)
    data = [tuple(row) for row in df[cols].itertuples(index=False, name=None)]
    if not data:
        return 0
    cursor.executemany(insert_sql, data)
    conn.commit()
    return len(data)


def fetch_json(url: str, *, timeout_sec: int = 30) -> dict:
    resp = requests.get(url, timeout=timeout_sec)
    return resp.json()


def load_accidents_from_api(
    *,
    auth_key: str,
    years: list[str],
    gu_codes: list[str],
    sido: str = "28",
    num_of_rows: int = 100,
) -> pd.DataFrame:
    all_data: list[dict] = []
    for year in years:
        print(f"[accidents] year={year} ...")
        for gu in gu_codes:
            url = (
                "https://opendata.koroad.or.kr/data/rest/frequentzone/lg"
                f"?authKey={auth_key}&searchYearCd={year}&sido={sido}&guGun={gu}&type=json&numOfRows={num_of_rows}"
            )
            try:
                data = fetch_json(url)
                items = data.get("items", {}).get("item")
                if not items:
                    continue
                if isinstance(items, dict):
                    items = [items]
                # notebook과 동일하게 year 필드만 임시로 붙였다가 insert에는 쓰지 않음
                for item in items:
                    item["year"] = year
                all_data.extend(items)
                print(f"  gu={gu}: +{len(items)}")
            except Exception as e:
                print(f"  gu={gu} failed: {e}")
    return pd.DataFrame(all_data)


def load_truck_from_api(
    *,
    auth_key: str,
    years: list[str],
    gu_codes: list[str],
    sido: str = "28",
    num_of_rows: int = 100,
) -> pd.DataFrame:
    all_data: list[dict] = []
    for year in years:
        print(f"[truck] year={year} ...")
        for gu in gu_codes:
            url = (
                "https://opendata.koroad.or.kr/data/rest/frequentzone/truck"
                f"?authKey={auth_key}&searchYearCd={year}&sido={sido}&guGun={gu}&type=json&numOfRows={num_of_rows}"
            )
            try:
                data = fetch_json(url)
                items = data.get("items", {}).get("item")
                if not items:
                    continue
                if isinstance(items, dict):
                    items = [items]
                for item in items:
                    item["year"] = year
                all_data.extend(items)
                print(f"  gu={gu}: +{len(items)}")
            except Exception as e:
                print(f"  gu={gu} failed: {e}")
    return pd.DataFrame(all_data)


def load_accidents_from_local_csv(csv_path: str) -> pd.DataFrame:
    """
    scripts/동윤/17_24_lg.csv 컬럼명 기반으로 incheon_accidents 스키마로 매핑.
    """
    df = pd.read_csv(csv_path, encoding="utf-8")

    mapping = {
        "afos_fid": "사고다발지fid",
        "afos_id": "사고다발지id",
        "sido_sgg_nm": "시도시군구명",
        "spot_nm": "지점명",
        "occrrnc_cnt": "사고건수",
        "caslt_cnt": "사상자수",
        "dth_dnv_cnt": "사망자수",
        "se_dnv_cnt": "중상자수",
        "sl_dnv_cnt": "경상자수",
        "lo_crd": "경도",
        "la_crd": "위도",
    }

    # CSV 일부 컬럼이 누락될 수 있으니 존재 여부 체크
    for target, src in mapping.items():
        if src in df.columns:
            df[target] = df[src]
        else:
            df[target] = None

    return normalize_accident_df(df)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--accident-years", nargs="*", default=["2019", "2020", "2021", "2022", "2023", "2024", "2025"])
    parser.add_argument("--truck-years", nargs="*", default=["2020", "2021", "2022", "2023", "2024", "2025"])
    parser.add_argument("--sido", default="28")
    parser.add_argument("--gu-codes", nargs="*", default=["110", "140", "177", "185", "200", "237", "245", "260", "710", "720"])

    parser.add_argument("--use-local-accidents-csv", action="store_true", help="API 대신 accidents는 17_24_lg.csv로 적재")
    parser.add_argument("--accidents-csv-path", default="17_24_lg.csv")
    args = parser.parse_args()

    cfg = get_db_cfg()
    conn = mc.connect(**cfg)
    cursor = conn.cursor()

    cursor.execute(CREATE_TABLE_ACCIDENTS)
    cursor.execute(CREATE_TABLE_TRUCK)
    conn.commit()
    print("[db] tables ready")

    api_key = os.getenv("PUBLIC_DATA_API_KEY", "")

    # 1) accidents
    if args.use_local_accidents_csv or not api_key:
        scripts_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(scripts_dir, args.accidents_csv_path)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(csv_path)
        print(f"[accidents] loading from csv={csv_path}")
        df_acc = load_accidents_from_local_csv(csv_path)
    else:
        print(f"[accidents] loading from API (years={args.accident_years}) ...")
        df_acc = load_accidents_from_api(
            auth_key=api_key,
            years=args.accident_years,
            gu_codes=args.gu_codes,
            sido=args.sido,
        )

    df_acc = normalize_accident_df(df_acc)
    inserted_acc = bulk_insert_df(cursor, conn, INSERT_ACCIDENTS, df_acc)
    print(f"[accidents] inserted rows={inserted_acc}")

    # 2) truck (API 필수)
    if not api_key:
        raise ValueError("PUBLIC_DATA_API_KEY is required to load truck data")

    print(f"[truck] loading from API (years={args.truck_years}) ...")
    df_truck = load_truck_from_api(
        auth_key=api_key,
        years=args.truck_years,
        gu_codes=args.gu_codes,
        sido=args.sido,
    )
    df_truck = normalize_accident_df(df_truck)
    inserted_truck = bulk_insert_df(cursor, conn, INSERT_TRUCK, df_truck)
    print(f"[truck] inserted rows={inserted_truck}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

