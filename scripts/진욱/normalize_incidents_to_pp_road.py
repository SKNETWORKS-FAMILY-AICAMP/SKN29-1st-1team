"""
사고/도로위험 데이터를 `pp_road` 기반으로 정규화하는 적재 스크립트.

목표
- hazard: `road_info` + `hazard_data`의 `road_name`을 `pp_road.road_name`으로 매핑 가능
- accidents/truck: 현재 `incheon_accidents`/`incheon_truck` 스키마에는 roadName이 없을 수 있어,
  매핑 컬럼이 존재할 때만 `pp_road`로 연결하고, 없으면 `road_id`는 NULL로 적재
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

import mysql.connector as mc
from dotenv import load_dotenv


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @staticmethod
    def from_env() -> "DBConfig":
        load_dotenv()
        host = os.getenv("DB_HOST", "")
        port = int(os.getenv("DB_PORT", "3306"))
        user = os.getenv("DB_USER", "")
        password = os.getenv("DB_PASSWORD", "")
        database = os.getenv("DB_DATABASE", "")

        missing = [
            k
            for k, v in {
                "DB_HOST": host,
                "DB_USER": user,
                "DB_PASSWORD": password,
                "DB_DATABASE": database,
            }.items()
            if not v
        ]
        if missing:
            raise ValueError(f"Missing env vars: {', '.join(missing)}")
        return DBConfig(host=host, port=port, user=user, password=password, database=database)


def connect_db(cfg: DBConfig) -> mc.MySQLConnection:
    return mc.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        autocommit=False,
    )


def get_columns(cursor: mc.cursor.MySQLCursor, *, table_name: str, schema: str) -> set[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema, table_name),
    )
    return {row[0] for row in cursor.fetchall()}


CREATE_TABLE_PPHazard = """
CREATE TABLE IF NOT EXISTS pp_hazard (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    road_id BIGINT NOT NULL,
    link_id VARCHAR(20) NOT NULL,

    hazard_grade INT,
    hazard_count INT,
    car_speed DOUBLE,
    car_vibrate_x DOUBLE,
    car_vibrate_y DOUBLE,
    car_vibrate_z DOUBLE,
    hazard_type VARCHAR(50),
    hazard_state VARCHAR(50),
    created_at DATETIME,

    UNIQUE KEY uk_hazard (link_id, created_at, hazard_grade, hazard_count, hazard_type, hazard_state),
    INDEX idx_pp_hazard_road (road_id),
    FOREIGN KEY (road_id) REFERENCES pp_road(road_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()


CREATE_TABLE_PPAccidents = """
CREATE TABLE IF NOT EXISTS pp_incheon_accidents (
    afos_fid VARCHAR(50) PRIMARY KEY,
    afos_id VARCHAR(50),
    road_id BIGINT NULL,

    sido_sgg_nm VARCHAR(100),
    spot_nm VARCHAR(255),

    occrrnc_cnt INT,
    caslt_cnt INT,
    dth_dnv_cnt INT,
    se_dnv_cnt INT,
    sl_dnv_cnt INT,

    lo_crd DOUBLE,
    la_crd DOUBLE,

    INDEX idx_pp_incheon_accidents_road (road_id),
    FOREIGN KEY (road_id) REFERENCES pp_road(road_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()


CREATE_TABLE_PPTruck = """
CREATE TABLE IF NOT EXISTS pp_incheon_truck (
    afos_fid VARCHAR(50) PRIMARY KEY,
    afos_id VARCHAR(50),
    road_id BIGINT NULL,

    sido_sgg_nm VARCHAR(100),
    spot_nm VARCHAR(255),

    occrrnc_cnt INT,
    caslt_cnt INT,
    dth_dnv_cnt INT,
    se_dnv_cnt INT,
    sl_dnv_cnt INT,

    lo_crd DOUBLE,
    la_crd DOUBLE,

    INDEX idx_pp_incheon_truck_road (road_id),
    FOREIGN KEY (road_id) REFERENCES pp_road(road_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-hazard", action="store_true", help="hazard만 적재")
    parser.add_argument("--only-accidents", action="store_true", help="accidents/truck만 적재")
    args = parser.parse_args()

    cfg = DBConfig.from_env()
    conn = connect_db(cfg)
    cursor = conn.cursor()

    # DDL
    cursor.execute(CREATE_TABLE_PPHazard)
    cursor.execute(CREATE_TABLE_PPAccidents)
    cursor.execute(CREATE_TABLE_PPTruck)
    conn.commit()

    # 1) hazard -> pp_hazard
    if not args.only_accidents:
        print("[hazard] inserting pp_hazard ...")
        hazard_sql = """
            INSERT IGNORE INTO pp_hazard (
                road_id,
                link_id,
                hazard_grade,
                hazard_count,
                car_speed,
                car_vibrate_x,
                car_vibrate_y,
                car_vibrate_z,
                hazard_type,
                hazard_state,
                created_at
            )
            SELECT
                pr.road_id,
                h.link_id,
                h.hazard_grade,
                h.hazard_count,
                h.car_speed,
                h.car_vibrate_x,
                h.car_vibrate_y,
                h.car_vibrate_z,
                h.hazard_type,
                h.hazard_state,
                h.created_at
            FROM hazard_data h
            JOIN road_info ri
              ON ri.link_id = h.link_id
            JOIN pp_road pr
              ON pr.road_name = ri.road_name
        """
        cursor.execute(hazard_sql)
        conn.commit()
        print("[hazard] done")

    # 2) accidents/truck -> pp_incheon_accidents / pp_incheon_truck
    if not args.only_hazard:
        accident_cols = get_columns(cursor, table_name="incheon_accidents", schema=cfg.database)
        truck_cols = get_columns(cursor, table_name="incheon_truck", schema=cfg.database)

        # accidents API가 roadName을 제공하면 여기서 연결 가능
        accident_road_col = next((c for c in ["roadName", "road_name"] if c in accident_cols), None)
        truck_road_col = next((c for c in ["roadName", "road_name"] if c in truck_cols), None)

        # accidents
        print("[accidents] inserting pp_incheon_accidents ...")
        if accident_road_col:
            accidents_sql = f"""
                INSERT IGNORE INTO pp_incheon_accidents (
                    afos_id, afos_fid, road_id,
                    sido_sgg_nm, spot_nm,
                    occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt,
                    lo_crd, la_crd
                )
                SELECT
                    a.afos_id,
                    a.afos_fid,
                    pr.road_id,
                    a.sido_sgg_nm,
                    a.spot_nm,
                    a.occrrnc_cnt,
                    a.caslt_cnt,
                    a.dth_dnv_cnt,
                    a.se_dnv_cnt,
                    a.sl_dnv_cnt,
                    a.lo_crd,
                    a.la_crd
                FROM incheon_accidents a
                LEFT JOIN pp_road pr
                  ON pr.road_name = a.{accident_road_col}
            """
        else:
            accidents_sql = """
                INSERT IGNORE INTO pp_incheon_accidents (
                    afos_id, afos_fid, road_id,
                    sido_sgg_nm, spot_nm,
                    occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt,
                    lo_crd, la_crd
                )
                SELECT
                    a.afos_id,
                    a.afos_fid,
                    NULL AS road_id,
                    a.sido_sgg_nm,
                    a.spot_nm,
                    a.occrrnc_cnt,
                    a.caslt_cnt,
                    a.dth_dnv_cnt,
                    a.se_dnv_cnt,
                    a.sl_dnv_cnt,
                    a.lo_crd,
                    a.la_crd
                FROM incheon_accidents a
            """
        cursor.execute(accidents_sql)
        conn.commit()
        print(f"[accidents] done (road_col={accident_road_col or 'None'})")

        # truck
        print("[truck] inserting pp_incheon_truck ...")
        if truck_road_col:
            truck_sql = f"""
                INSERT IGNORE INTO pp_incheon_truck (
                    afos_id, afos_fid, road_id,
                    sido_sgg_nm, spot_nm,
                    occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt,
                    lo_crd, la_crd
                )
                SELECT
                    t.afos_id,
                    t.afos_fid,
                    pr.road_id,
                    t.sido_sgg_nm,
                    t.spot_nm,
                    t.occrrnc_cnt,
                    t.caslt_cnt,
                    t.dth_dnv_cnt,
                    t.se_dnv_cnt,
                    t.sl_dnv_cnt,
                    t.lo_crd,
                    t.la_crd
                FROM incheon_truck t
                LEFT JOIN pp_road pr
                  ON pr.road_name = t.{truck_road_col}
            """
        else:
            truck_sql = """
                INSERT IGNORE INTO pp_incheon_truck (
                    afos_id, afos_fid, road_id,
                    sido_sgg_nm, spot_nm,
                    occrrnc_cnt, caslt_cnt, dth_dnv_cnt, se_dnv_cnt, sl_dnv_cnt,
                    lo_crd, la_crd
                )
                SELECT
                    t.afos_id,
                    t.afos_fid,
                    NULL AS road_id,
                    t.sido_sgg_nm,
                    t.spot_nm,
                    t.occrrnc_cnt,
                    t.caslt_cnt,
                    t.dth_dnv_cnt,
                    t.se_dnv_cnt,
                    t.sl_dnv_cnt,
                    t.lo_crd,
                    t.la_crd
                FROM incheon_truck t
            """
        cursor.execute(truck_sql)
        conn.commit()
        print(f"[truck] done (road_col={truck_road_col or 'None'})")

    cursor.close()
    conn.close()
    print("[done] normalize incidents -> pp_road linked tables complete")


if __name__ == "__main__":
    main()

