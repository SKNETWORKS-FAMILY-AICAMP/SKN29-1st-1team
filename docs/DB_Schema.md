# DB Schema (Final Tables)

아래 문서는 “서비스에서 최종적으로 사용할 테이블”만 요약 정리했습니다.

---

## 1) 정규화(엔티티) 테이블: 교통

### `pp_road`
- `road_id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `road_name` VARCHAR(100) NOT NULL
- `UNIQUE KEY uk_road_name (road_name)`

### `PP_traffic`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `road_id` BIGINT NOT NULL (FK -> `pp_road(road_id)`)
- `direction` INT NOT NULL
- `datetime` DATETIME NOT NULL
- `volume` INT
- `UNIQUE KEY uk_traffic (road_id, direction, datetime)`
- `INDEX idx_datetime (datetime)`

### `pp_speed`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `road_id` BIGINT NOT NULL (FK -> `pp_road(road_id)`)
- `direction` INT NOT NULL
- `datetime` DATETIME NOT NULL
- `speed` INT
- `UNIQUE KEY uk_traffic (road_id, direction, datetime)` *(pp_speed 생성 쿼리 기준)*
- `INDEX idx_datetime (datetime)`

---

## 2) 정규화(엔티티) 테이블: 사고/도로위험(incident & hazard)

> `scripts/진욱/normalize_incidents_to_pp_road.py` 기준으로 생성되는 “pp_ 연결용” 테이블입니다.

### `pp_hazard`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `road_id` BIGINT NOT NULL (FK -> `pp_road(road_id)`)
- `link_id` VARCHAR(20) NOT NULL
- `hazard_grade` INT
- `hazard_count` INT
- `car_speed` DOUBLE
- `car_vibrate_x` DOUBLE
- `car_vibrate_y` DOUBLE
- `car_vibrate_z` DOUBLE
- `hazard_type` VARCHAR(50)
- `hazard_state` VARCHAR(50)
- `created_at` DATETIME
- `UNIQUE KEY uk_hazard (link_id, created_at, hazard_grade, hazard_count, hazard_type, hazard_state)`
- `INDEX idx_pp_hazard_road (road_id)`

### `hazard_type_code`
> 도로 위험(hazard) 결함 유형 코드 조회용 참조 테이블.
- `defect_type_id` INT NOT NULL PRIMARY KEY
- `defect_name` VARCHAR(50) NOT NULL
- 예시 데이터:
  - `0` 포트홀
  - `1` 피로균열
  - `2` 수직균열
  - `3` 수평균열
  - `4` 노면수리불량
  - `5` 쓰레기
  - `6` 현수막
  - `7` 노면표시불량
  - `8` 시선유도봉불량
  - `9` 기타

### `pp_incheon_accidents`
- `afos_fid` VARCHAR(50) PRIMARY KEY
- `afos_id` VARCHAR(50)
- `road_id` BIGINT NULL (있으면 `pp_road`로 연결)
- `sido_sgg_nm` VARCHAR(100)
- `spot_nm` VARCHAR(255)
- `occrrnc_cnt` INT
- `caslt_cnt` INT
- `dth_dnv_cnt` INT
- `se_dnv_cnt` INT
- `sl_dnv_cnt` INT
- `lo_crd` DOUBLE
- `la_crd` DOUBLE
- `INDEX idx_pp_incheon_accidents_road (road_id)`
- `FOREIGN KEY (road_id) REFERENCES pp_road(road_id)`

### `pp_incheon_truck`
- `afos_fid` VARCHAR(50) PRIMARY KEY
- `afos_id` VARCHAR(50)
- `road_id` BIGINT NULL (있으면 `pp_road`로 연결)
- `sido_sgg_nm` VARCHAR(100)
- `spot_nm` VARCHAR(255)
- `occrrnc_cnt` INT
- `caslt_cnt` INT
- `dth_dnv_cnt` INT
- `se_dnv_cnt` INT
- `sl_dnv_cnt` INT
- `lo_crd` DOUBLE
- `la_crd` DOUBLE
- `INDEX idx_pp_incheon_truck_road (road_id)`
- `FOREIGN KEY (road_id) REFERENCES pp_road(road_id)`

---

## 3) 참조(원천) 테이블: 도로위험 & 기상

### `road_info`
> `scripts/은진/hazard.py`에서 `road_info(link_id, road_name, road_type)`로 INSERT 됩니다.
- `link_id` (PK로 사용되는 것으로 전제)
- `road_name`
- `road_type`

### `weather_pattern_asos`
> `scripts/지현/ingest_raw_speed_patterns.py`의 `create_table_weather_pattern_asos()` 스키마.
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `statDate` VARCHAR(6) NOT NULL  # YYYYMM
- `stnId` VARCHAR(10) NOT NULL
- `stnNm` VARCHAR(50)
- `weatherItem` VARCHAR(10) NOT NULL  # 예: rn, dsnw, ws, ...
- `hour00 ~ hour23` FLOAT  # 월별 평균 강도/값
- `UNIQUE KEY uniq_asos (statDate, stnId, weatherItem)`
