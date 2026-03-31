<h1> 🚙 Incheon Traffic Insight: 자동차 통계와 교통 흐름이 만나는 패턴 가이드 🚚 </h1>

<br>

## 🚦 데이터로 도로를 읽고, 문제를 찾아낸다
> **인천광역시의 파편화된 교통·사고·기상 데이터를 통합하여 도로 안전 인사이트를 도출하는 데이터 시스템입니다.**
>
> **핵심 목표**: 인천광역시 도로 공공 데이터를 활용하여 교통 흐름과 위험 요인을 분석하고 개선이 필요한 도로 및 정책 인사이트 도출을 목표로 합니다.
<br>

---


### 📎 발표자료

* [발표자료 PDF 보기](SKN29기_발표ppt.pdf)


<br>

## 1. Team Route 👥
### 누가 무엇을 했는지 구조화

<p align="center">
  <img src="https://github.com/user-attachments/assets/a555f545-6001-4a06-9044-4576a5f6f03c" width="100%" alt="Team Photo or Architecture Diagram">
</p>

| 이름 | 역할 | 담당 분석 및 업무 |
| :---: | :---: | :--- |
| **김진욱** | **Project Lead** | 총괄 DB 설계 및 인천공항 방향 교통 흐름 분석 |
| **이지현** | **Data Analysis** | 데이터 수집 및 요일/시간/날씨별 도로 분석 패턴 정립 |
| **이동윤** | **Frontend** | 프론트엔드 개발 및 사고량과의 상관관계 영향도 분석 |
| **김은진** | **Designer** | 크리에이티브 디자인 및 운전자 스트레스 지수 시각화 |

<br>

---

<br>

## 2. Visualization Preview 🖼️


![SKN29_1ST_1TEAM_시연영상](https://github.com/user-attachments/assets/32367bb0-09e1-4716-8c57-72df330117c5)

<br>

---

<br>

## 3. Key Signals 💡
### 분석 결과의 주요 인사이트

* **실시간 KPI**: (KOROAD Live KPI 기반) 구간별 교통량/속도 변화로 혼잡 징후를 빠르게 감지
* **요일별/시간대별 패턴**: 출·퇴근 피크와 주말 이동 패턴 차이가 뚜렷하며, 특정 요일·시간대에 정체가 반복되는 축(도로)이 확인됨
* **운전자 스트레스 지표**: 속도 저하(정체), 변동성(급가감속/흐름 불안정), 교통량 혼잡도를 시간 단위로 결합해 산출한 지표로, “체감 혼잡”이 큰 구간을 우선 식별하는 데 사용

<br>

---

<br>

## 4. 활용 가능성 🎯
### 분석 데이터를 통한 실질적 기대 효과

* **교통 정책 개선**: 데이터 기반의 효율적인 교통 수요 관리 및 정책 수립 지원
* **사고 예방 전략 수립**: 사고 다발 구간의 원인 분석을 통한 선제적 안전 대책 마련
* **도로 설계 및 신호 체계 개선**: 정체 구간의 도로 구조 변경 및 최적화된 신호 주기 도출
* **현장 적용/모니터링**: 실시간 KPI와 결합해 혼잡·위험 구간을 모니터링하고, 우선순위(개선 필요 구간)를 빠르게 정리

<br>

---

<br>

## 5. Project Structure 🚀
### 프로젝트 디렉토리 구조

* `scripts/개인`: 개인별 수집/전처리 스크립트 모음
* `src/`: 애플리케이션 코드
  * `src/app`: 실행 진입점 및 앱 구성
  * `src/db`: DB 연결/쿼리 및 스키마 관련 모듈
  * `src/service`: 데이터 수집/가공 서비스 로직 (예: KOROAD KPI, 인천 스팟 데이터 등)
  * `src/ui`: Streamlit UI 페이지/컴포넌트

<br>

---

<br>

## 6. ERD 구조도 📐
### 데이터 구조 시각화 (테이블 관계)

<p align="center">
  <img src="https://github.com/user-attachments/assets/3443ccbb-e8af-4dd8-802b-489180a0384a" width="90%" alt="Database ERD">
</p>


<br>

---

<br>

## 7. Data Route 🔄
### 데이터 흐름 설명 (출처 → 전처리 → Feature)

##### **Raw 데이터 수집 (Source)**  
* **공공데이터 포털(인천광역시)**, **기상청 ASOS**
* **통행량**, **속도**, **도로 노후/위험물**, **날씨**, **인천지역 트럭-사고 통계**

##### **전처리/모델링 (Preprocessing & Modeling)**  
* 분석 단위를 “시간(시간대)”로 맞추기 위해 시간별 wide 형태 데이터를 **분석용 long 테이블**로 변환
* 도로 모델(구간/링크 등) 추출 후 **도로 기준으로 데이터들을 연결** (예: `link_id`, `road_name` 기반)

##### **최종 분석용 테이블 (Mart)**  
* `pp_traffic`, `pp_speed`, `pp_road`, `pp_incheon_accidents`, `pp_incheon_truck`

##### **분석 → 인사이트 도출 → 시각화**  
* 테이블 기반 패턴 분석 후 핵심 인사이트를 정리하고, **Streamlit**으로 대시보드 형태로 시각화

<br>

---

<br>

## 8. Tech Stack 🛠️
### 사용한 기술 스택

<div align="center">

#### 💻 Language & Data
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

<br>

#### 🗄 Database
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

<br>

#### 📊 Visualization & App
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

<br>

#### ⚙️ Tools
![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

</div>

<br>

---

<br>

## 9. Reference 📚

* [인천광역시 공공데이터 포털](https://www.data.go.kr/)
* [기상청 기상자료개방포털 (ASOS)](https://data.kma.go.kr/)
* [TAAS 교통사고분석시스템](https://taas.koroad.or.kr/web/shp/mik/main.do?menuId=WEB_KMP)
* **SK Networks Family AI Camp**
