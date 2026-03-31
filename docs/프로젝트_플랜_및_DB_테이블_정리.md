# 프로젝트 플랜 및 DB 테이블 정리

## 1) 목적
- `scripts/` 안의 “개인 작업물”로 수행된 데이터 수집/전처리/DB 적재 작업을 요약합니다.
- DB에 존재하는(또는 코드에서 생성되는) 테이블의 스키마를 문서화합니다.
- 앱/서비스에서 실제로 활용할 “주요 전처리 완료 테이블”을 정리하고, 코드 기준 스키마 불일치(있는 경우)를 명시합니다.

## 2) 조사 범위 (scripts 내 개인 작업물)
- `scripts/진욱/`: 통행량/속도 원시 데이터 수집 -> 원시 테이블 생성/적재 -> 정규화 테이블 생성
- `scripts/지현/`: 속도 패턴(요일/시간대) + 기상 패턴(ASOS) 수집/전처리 -> 패턴 테이블 생성
- `scripts/은진/`: 도로 위험(hazard) 데이터 API 수집 -> road/hazard 테이블 적재
- `scripts/동윤/`: 사고/트럭 관련 데이터 테이블 생성 및 적재
- (부가) `scripts/지현/my-traffic-app/`: React 프론트(기상-교통 예측 UI)와 API 호출 흐름 확인

## 3) 개인 작업물 요약: “무슨 작업을 했는지”

### 3.1) 진욱 (`scripts/진욱`)
- **원시 통행량/속도(traffic_raw, traffic_speed_raw) 수집/적재 (레거시)**
  - `raw_to_entity.py`에서 정규화 테이블(`pp_road`, `PP_traffic`, `pp_speed`)을 만드는 데 사용되는 중간 단계입니다.
  - 현재 서비스 로직은 `pp_road/PP_traffic/pp_speed`를 기준으로 동작하며, 원시 raw 테이블은 “미사용/제거 가능”에 가깝습니다.
- **정규화/엔티티화 (raw -> entity)**
  - `raw_to_entity.py`에서 `traffic_raw`/`traffic_speed_raw`를 조회해,
    - `pp_road`(도로명 -> road_id),
    - `PP_traffic`(road_id, direction, datetime -> volume),
    - `pp_speed`(road_id, direction, datetime -> speed)
    로 재적재합니다.
  - 시간은 `statDate + hour(h)`로 `datetime`을 구성합니다.
  - direction은 `상행` -> 0, 그 외 -> 1 로 코딩됩니다.
- (참고) 현재 작업 기준으로 `node`, `link`는 필수로 두지 않습니다.
- (참고) `scripts/진욱/trash/sample.py`에는 주차 정보(`parking_status`) 스키마/적재 로직이 존재합니다. (현재 본 프로젝트의 “핵심 앱 테이블”과는 별개로 보입니다.)

### 3.2) 지현 (`scripts/지현`)
- **속도 패턴(요일/시간대)**
-  “요일/시간대 패턴”은 `pp_speed`를 기준으로 service/FastAPI에서 쿼리 집계하여 생성합니다. (테이블X)
- **기상 패턴(ASOS 관측치)**
  - `speed_pattern_weather.ipynb`에서 `weather_pattern_asos` 테이블을 생성합니다.
    - `statDate(YYYYMM)`, ASOS 관측소(`stnId`, `stnNm`), `weatherItem`(rn/dsnw/ws 등) + `hour00~hour23`(float)를 저장합니다.
    - `UNIQUE KEY uniq_asos (statDate, stnId, weatherItem)`로 중복 방지합니다.
- **서비스/화면 사용 코드 확인**
  - `scripts/지현/app.py`(Streamlit)
    - `pp_speed` + `weather_pattern_asos`로 (요일/시간대) 평균 속도를 DB에서 직접 집계합니다.
  - `scripts/지현/traffic_db/traffic_api.py`(FastAPI)
    - `/api/roads`, `/api/speed/base`, `/api/speed/weather` 엔드포인트를 제공하는 코드를 확인했습니다.
    - 기존 `speed_pattern_*` 테이블 의존을 제거하고 `pp_speed/pp_road` 기반으로 재구성했습니다.
  - `scripts/지현/my-traffic-app/`(React)
    - FastAPI 엔드포인트를 호출해 “평상시 vs 기상 조건 예측 속도”를 비교하는 UI를 구성합니다.
    - 기상 조건 키(`rn_low`, `rn_mid`, `rn_high`, `ws_low` …)가 `weather_pattern_asos.weatherItem`과 매핑된다는 점을 확인했습니다.

### 3.3) 은진 (`scripts/은진`)
- `hazard.py`에서 공공 API(`hazard-list`)를 페이지 단위로 호출해 DB에 적재합니다.
  - 도로 테이블: `road_info (link_id, road_name, road_type)`
  - 위험 테이블: `hazard_data (link_id, hazard_grade, hazard_count, car_speed, car_vibrate_x/y/z, hazard_type, hazard_state, created_at)`
- 주의: `CREATE TABLE` 선언은 이 레포에서 직접 확인되지 않았고, `INSERT IGNORE` 기반으로 존재한다고 가정하는 형태입니다. 따라서 컬럼 타입/인덱스는 DB 실체를 확인해서 문서 보강이 필요합니다.

### 3.4) 동윤 (`scripts/동윤`)
- `sample.ipynb`에서 아래 사고/물류 관련 테이블의 `CREATE TABLE` 문을 확인했습니다.
  - `incheon_accidents`
  - `incheon_truck`
- 두 테이블 모두 동일한 컬럼 세트를 가지며, `afos_fid`가 Primary Key입니다.

## 4) DB 데이터 스키마 문서 (코드/노트북에서 확인된 테이블)
> 아래 스키마는 `scripts/*`의 `CREATE TABLE` 또는 `INSERT` 쿼리에서 근거를 확인한 범위만 정리했습니다.

### 4.1) 교통 원시 테이블 (진욱)
#### `traffic_raw`
- 컬럼
  - `id` (BIGINT, AUTO_INCREMENT, PK)
  - `statDate` (DATE, NOT NULL)
  - `roadName` (VARCHAR(100), nullable)
  - `linkID` (VARCHAR(20), NOT NULL)
  - `direction` (VARCHAR(10), NOT NULL)
  - `startName` (VARCHAR(100))
  - `endName` (VARCHAR(100))
  - `hour00~hour23` (각 INT)
- 제약
  - `UNIQUE KEY uniq_traffic (statDate, linkID, direction)`

#### `traffic_speed_raw`
- 컬럼
  - `id` (BIGINT, AUTO_INCREMENT, PK)
  - `statDate` (DATE, NOT NULL)
  - `roadName` (VARCHAR(100), nullable)
  - `direction` (VARCHAR(10), NOT NULL)
  - `dayStatValue` (int)
  - `hour00~hour23` (각 INT)
- 제약
  - `UNIQUE KEY uniq_traffic (statDate, roadName, direction)`

### 4.2) 정규화 엔티티 테이블 (진욱)
#### `pp_road`
- `road_id` (BIGINT, AUTO_INCREMENT, PK)
- `road_name` (VARCHAR(100), NOT NULL)
- `UNIQUE KEY uk_road_name (road_name)`

#### `PP_traffic`
- `id` (BIGINT, AUTO_INCREMENT, PK)
- `road_id` (BIGINT, NOT NULL, FK -> `pp_road(road_id)`)
- `direction` (INT, NOT NULL)
- `datetime` (DATETIME, NOT NULL)
- `volume` (INT)
- 제약
  - `UNIQUE KEY uk_traffic (road_id, direction, datetime)`
  - `INDEX idx_datetime (datetime)`

#### `pp_speed`
- `id` (BIGINT, AUTO_INCREMENT, PK)
- `road_id` (BIGINT, NOT NULL, FK -> `pp_road(road_id)`)
- `direction` (INT, NOT NULL)
- `datetime` (DATETIME, NOT NULL)
- `speed` (INT)
- 제약
  - `UNIQUE KEY uk_traffic (road_id, direction, datetime)`
  - `INDEX idx_datetime (datetime)`

#### `node`
- `node_id` (BIGINT, AUTO_INCREMENT, PK)
- `node_name` (VARCHAR(100))

#### `link`
- `link_id` (VARCHAR(20), PK) : 공공데이터 link_id 문자열 그대로 사용
- `road_id` (BIGINT, FK -> `road(road_id)`)
- `from_node_id` (BIGINT, FK -> `node(node_id)`)
- `to_node_id` (BIGINT, FK -> `node(node_id)`)
- `direction` (TINYINT, NOT NULL) : 0 정방향, 1 역방향(주석)
- 인덱스
  - `INDEX idx_road (road_id)`
  - `INDEX idx_from_to (from_node_id, to_node_id)`

### 4.3) 속도 패턴 테이블 (지현)
#### `speed_pattern_monthly`
- `id` (BIGINT, AUTO_INCREMENT, PK)
- `statDate` (VARCHAR(6), NOT NULL)  # YYYYMM
- `roadName` (VARCHAR(100))
- `direction` (VARCHAR(20))
- `sectionName` (VARCHAR(200))
- `monthStatValue` (INT)
- `mon/tue/wed/thu/fri/sat/sun` (각 INT)
- 제약
  - `UNIQUE KEY uniq_pattern (statDate, roadName, direction, sectionName)`

#### `speed_pattern_timezone`
- `id` (BIGINT, AUTO_INCREMENT, PK)
- `statDate` (VARCHAR(6), NOT NULL)  # YYYYMM
- `roadName` (VARCHAR(100))
- `direction` (VARCHAR(20))
- `sectionName` (VARCHAR(200))
- `hour00~hour23` (각 INT)
- 제약
  - `UNIQUE KEY uniq_timezone (statDate, roadName, direction, sectionName)`

### 4.4) 기상 패턴 테이블 (지현)
#### `weather_pattern_asos`
- `id` (BIGINT, AUTO_INCREMENT, PK)
- `statDate` (VARCHAR(6), NOT NULL)  # YYYYMM
- `stnId` (VARCHAR(10), NOT NULL)
- `stnNm` (VARCHAR(50))
- `weatherItem` (VARCHAR(10), NOT NULL)  # 예: rn, dsnw, ws 등
- `hour00~hour23` (각 FLOAT)
- 제약
  - `UNIQUE KEY uniq_asos (statDate, stnId, weatherItem)`

### 4.5) 사고/트럭 테이블 (동윤)
#### `incheon_accidents`
- `afos_id` (VARCHAR(50))
- `afos_fid` (VARCHAR(50), PK)
- `sido_sgg_nm` (VARCHAR(100))
- `spot_nm` (VARCHAR(255))
- `occrrnc_cnt` (INT)
- `caslt_cnt` (INT)
- `dth_dnv_cnt` (INT)
- `se_dnv_cnt` (INT)
- `sl_dnv_cnt` (INT)
- `lo_crd` (DOUBLE)
- `la_crd` (DOUBLE)

#### `incheon_truck`
- `incheon_accidents`와 동일 컬럼 구조
- `afos_fid`가 PK

### 4.6) 위험(hazard) 테이블 (은진) - CREATE TABLE 미확인
- `road_info (link_id, road_name, road_type)`  # INSERT 쿼리 근거
- `hazard_data (link_id, hazard_grade, hazard_count, car_speed, car_vibrate_x/y/z, hazard_type, hazard_state, created_at)`  # INSERT 쿼리 근거
- 문서 보강 필요
  - 실제 DB의 컬럼 타입/NOT NULL/인덱스는 별도 확인 필요(CREATE TABLE이 레포에 없었음).

## 5) 앱 서비스용 “주요 전처리 완료 테이블” 정리

### 5.1) Streamlit/로컬 데모 기준(지현 `scripts/지현/app.py`)
- 도로 목록
  - `SELECT DISTINCT road_name FROM pp_road`
- 평상시(base) 속도 조회
  - `pp_speed` + `pp_road`에서 `HOUR(datetime)`와 `WEEKDAY(datetime)` 조건으로 평균 속도 계산
- 기상(weather) 속도 조회
  - `pp_speed` + `weather_pattern_asos`(statDate=YYYYMM, weatherItem, hourXX 강도)로 월 기준 조건 필터링 후 평균 속도 계산

정리:
- 앱 서비스 핵심 테이블: `pp_speed`, `pp_road` (요일/시간대 패턴을 테이블X로 집계)
- (선택) 기상 조건은 `weather_pattern_asos`를 그대로 쓰되, “월 평균 강도” 기반으로 조건 필터링합니다. (중요도 낮음)

### 5.2) FastAPI + React 기준(지현 `traffic_api.py`, `my-traffic-app`)
- 공통 엔드포인트
  - `/api/roads`
  - `/api/speed/base`
  - `/api/speed/weather`
- 코드상 “기대 컬럼”과 “노트북 생성 스키마”의 불일치 가능성이 있어, 아래 2가지 중 하나를 확정해야 합니다.
- (변경) 현재는 `speed_pattern_*` 테이블 의존을 제거하고, `pp_speed/pp_road`에서 “요일/시간대 평균”을 직접 계산하도록 구성했습니다.

정리:
- “정답 스키마”를 하나로 맞춘 뒤, 앱 서비스에서 사용할 SQL/쿼리를 고정하는 것이 다음 단계입니다.

### 5.3) 네트워크/원시 데이터(진욱) 기준
- 원시 분석(데이터 탐색): `traffic_raw`, `traffic_speed_raw`
- 엔티티 기반(도로명->ID, datetime 정규화):
  - `pp_road`, `PP_traffic`, `pp_speed`
- (참고) 현재 작업 기준으로 `node`, `link`는 필수로 두지 않습니다.

### 5.4) 사고/돌발 인사이트 확장(동윤/은진)
- 사고/물류: `incheon_accidents`, `incheon_truck`
- 도로위험: `road_info`, `hazard_data`
- 현재 `src/ui/*`의 앱은 sample/mock 중심이므로, 이 테이블들은 “다음 기능 확장” 후보입니다.

## 6) 차기 작업 플랜(권장)
1. DB 스키마 확정
   - `traffic_api.py`가 기대하는 컬럼과 `scripts/지현` 노트북 생성 스키마를 비교해 하나로 정리합니다.
2. 앱 서비스 쿼리 고정
   - `src/service`에서 쿼리가 참조할 “정확한 테이블/컬럼”을 고정하고, `src/db/queries.py`에 SQL을 모읍니다.
3. 전처리 상태 문서 보강
   - `road_info`, `hazard_data`의 `CREATE TABLE`이 레포에 없으므로, 실제 DB에서 DDL을 확인해 타입/제약을 문서에 보강합니다.
4. UI 페이지와 테이블 매핑
   - 예: (예정) “시간대 패턴”, “기상 조건 예측”, “도로 구간 상태(링크 단위)”, “돌발/사고/위험” 페이지 각각이 어떤 테이블을 사용하는지 명시합니다.

## 7) 원시데이터 전처리 계획
아래는 “원시/적재 상태 테이블(INSERT로 채워진 테이블)”을 기준으로, 정규화/서비스 준비까지 가는 흐름을 단계별로 정리한 계획입니다.

### 7.1) 원시 데이터 수집/적재 (Raw Ingestion)
- `traffic_raw`, `traffic_speed_raw` (레거시/중간 단계)
  - KORoad API(통행량/속도)에서 일 단위로 데이터 수집
  - `INSERT IGNORE` 또는 `UNIQUE KEY` 기반 중복 방지로 재실행 가능 구조로 적재
  - 이후 `raw_to_entity.py`에서 `pp_road/PP_traffic/pp_speed` 생성에만 사용합니다. (현재 서비스는 raw 테이블 미사용)
- `speed_pattern_monthly`, `speed_pattern_timezone`
  - (권장) “요일/시간대 패턴”은 `service/FastAPI`에서 `pp_speed`로 직접 집계하므로, 해당 테이블은 필수 고정이 아닙니다. (테이블X)
- `weather_pattern_asos`
  - ASOS API hourly 데이터를 일별로 수집한 뒤 월별 평균을 계산
  - weatherItem(`ta`, `rn`, `hm`, `ws`, `wd`, `dsnw` 등)별로 hour00~hour23(월 평균 FLOAT)을 만들어 bulk 적재
- `incheon_accidents`, `incheon_truck`
  - KORoad frequentzone API 또는 로컬 CSV(`17_24_lg.csv`)에서 사고/트럭 데이터 적재
  - 숫자형 컬럼(`occrrnc_cnt`, `caslt_cnt` 등)은 INT로 강제 변환, 좌표 컬럼은 DOUBLE로 변환
  - `afos_fid` PK/`INSERT IGNORE`로 중복 방지
- `road_info`, `hazard_data`
  - hazard-list API를 페이지 단위로 호출 후 `INSERT IGNORE` 적재
  - created_at은 ISO datetime 문자열 → datetime 파싱(실제 DDL 타입 확인 후 문서 보강)

### 7.2) 원시 → 정규화/엔티티화 (Raw -> Entity)
- `scripts/진욱/raw_to_entity.py` 실행 흐름 고정
  - `traffic_raw`/`traffic_speed_raw` 조회
  - roadName을 `pp_road.road_name` 기준으로 `pp_road(road_id)`에 매핑(없으면 생성)
  - direction은 INT 코드로 변환(로직은 `raw_to_entity.py` 기준)
  - datetime은 `statDate + hour(h)`로 조합해 `PP_traffic.datetime`, `pp_speed.datetime`에 저장
  - `PP_traffic.volume`, `pp_speed.speed` 컬럼 채우고 `UNIQUE KEY`로 중복 방지

### 7.3) 서비스/화면 조인을 위한 전처리 확인(패턴 테이블)
- `speed_pattern_timezone`와 `weather_pattern_asos`의 조인 키/컬럼명 정합성 확인
  - `weather_pattern_asos.weatherItem` 값이 앱/서비스에서 기대하는 조건 키와 매핑되는지 확인
  - hour 컬럼은 `hour00~hour23` 형식 그대로 유지(서비스 쿼리/코드도 동일 컬럼 사용)
- 누락값/형변환 정책 고정
  - hour 값이 null로 들어오는 경우 평균 계산/조회 시 제외/0 대체 규칙을 문서화
  - (현재 코드 기준) `ingest_raw_speed_patterns.py`에서 CSV numeric 변환 시 coerce 및 int casting을 수행

### 7.4) 품질/재실행 전략
- 모든 단계의 idempotency 확보
  - raw 적재는 UNIQUE KEY + `INSERT IGNORE`로 재실행 안전성 확보
  - 정규화(엔티티 적재)도 `pp_road` 중복 생성 방지 로직과 `UNIQUE KEY`를 동일하게 활용
- 체크포인트/범위 지정
  - API 적재는 year/month 범위를 인자 또는 환경변수로 고정해 불필요한 재수집 최소화


