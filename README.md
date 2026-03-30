<h1> 🚙 스마트 트래픽 인천: 교통 흐름 데이터 활용 가이드 🚚 </h1>

# 🚦 데이터로 도로를 읽고, 문제를 찾아낸다

## 🛠 Tech Stack### 💻 Language & Data![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)### 🗄 Database![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)### 📊 Visualization / App![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)### ⚙️ Tools![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)





<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white">
  <img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=MySQL&logoColor=white">
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=Pandas&logoColor=white">
  <img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=GitHub&logoColor=white">
</p>

---

## 📝 프로젝트 소개

> [cite_start]**인천광역시 도로 공공 데이터를 활용하여 교통 흐름과 위험 요인을 분석하고, 개선이 필요한 도로 및 정책 인사이트 도출을 목표로 합니다. **

### [cite_start]📈 주요 인사이트 
* **기상 상관성**: 특정 시간대와 날씨 조건이 결합될 때 교통량이 급격히 변동함 
* **사고 분석**: 사고 발생은 단순 교통량보다 특정 변수(위험 요소 등)와 더 높은 상관성을 보임 
* **흐름 집중**: 인천공항으로의 이동 흐름이 특정 주요 도로 축에 집중되는 경향 확인 
* **운전자 환경**: 일부 도로 구조가 운전자에게 상대적으로 높은 스트레스를 유발함을 파악 

---

## 👥 팀원 소개 및 역할

| 이름 | 역할 | 상세 업무 |
| :---: | :---: | :--- |
| **김진욱** | **Project Lead** | 총괄 DB 설계 및 인천공항 방향 교통 흐름 분석  |
| **이지현** | **Data Analysis** | 데이터 수집 및 요일/시간/날씨별 도로 분석 패턴 정립  |
| **이동윤** | **Frontend** | 프론트엔드 개발 및 사고량과의 상관관계 영향도 분석  |
| **김은진** | **Designer** | 크리에이티브 디자인 및 운전자 스트레스 지수 시각화  |

---

## 📐 Database Architecture (ERD)

<p align="center">
  <img src="https://github.com/user-attachments/assets/3443ccbb-e8af-4dd8-802b-489180a0384a" width="90%" alt="Database ERD">
</p>

### [cite_start]**📌 Schema Highlights** 
* **Master Table**: `pp_road` 테이블을 중심으로 모든 교통 지표를 1:N 관계로 매핑하여 통합 관리함 
* **Normalization**: `road_info` 테이블로 도로의 물리적 특성을 분리하여 데이터 중복을 최소화함 
* **Optimization**: `datetime` 및 `road_id` 인덱싱 설계를 통해 대용량 로그 데이터 조회 성능을 최적화함 

---

## [cite_start]🚀 수행 절차 및 구조 

1. **Data Ingestion**: 인천시 공공데이터 포털 및 기상청 API를 통해 원천 데이터를 수집함 
2. **Data Normalization**: `link_id`와 `road_name`을 기준으로 엔티티 간 외래 키(FK) 관계를 설정함 
3. **Database Modeling**: MySQL Workbench를 활용하여 물리 모델링 및 제약 조건을 설정함 
4. **Analysis & Visualization**: 분석된 데이터를 바탕으로 사고 다발 구간 및 위험 지역 시각화 결과를 도출함 

---

## 🔗 프로젝트 결과물

* 📽️ **[발표 자료 바로가기](링크_주소_입력)**
* 🎬 **[시연 영상 바로가기](링크_주소_입력)**

---

## 📚 Reference
* [인천광역시 공공데이터 포털](https://www.data.go.kr/)
* [기상청 기상자료개방포털 (ASOS)](https://data.kma.go.kr/)
* [cite_start]**SK Networks Family AI Camp**
