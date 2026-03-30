<h1> 🚙 스마트 트래픽 인천: 교통 흐름 데이터를 활용한 네트워크 시스템 🚚</h1>
# 🚦 데이터로 도로를 읽고, 문제를 찾아낸다 







<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white">
  <img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=MySQL&logoColor=white">
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=Pandas&logoColor=white">
  <img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=GitHub&logoColor=white">
</p>






---




## 👥 팀원 소개 및 역할


<p align="center">
  <img src="https://github.com/user-attachments/assets/a555f545-6001-4a06-9044-4576a5f6f03c" width="100%" alt="Database ERD">
</p>


| 이름 | 역할 | 상세 업무 |
| :---: | :---: | :--- |
| **김은진** | **Data Engineering** | [cite_start]프로젝트 총괄 DB 설계, 기상 패턴(`weather_pattern_asos`) 및 통계 데이터 모델링 [cite: 5] |
| **김진욱** | **Data Analysis** | [cite_start]인천시 사고(`pp_incheon_accidents`) 및 트럭 사고 데이터 정규화 수집 [cite: 5] |
| **이동윤** | **Backend/UI** | [cite_start]도로 위험(Hazard) 데이터 연동 및 대시보드 시각화 구현 [cite: 5] |
| **이지현** | **Backend/UI** | [cite_start]도로 위험(Hazard) 데이터 연동 및 대시보드 시각화 구현 [cite: 5] |




---




## 📝 프로젝트 소개
[cite_start]**** [cite: 5]
* [cite_start]**주요 인사이트**: 서로 다른 포맷의 데이터를 `road_id` 중심으로 통합 관리하여 데이터 무결성 확보 [cite: 5]
* [cite_start]**핵심 목표**: 인천광역시 도로 공공 데이터를 활용하여 교통 흐름과 위험 요인을 분석하고 개선이 필요한 도로 및 정책 인사이트 도출을 목표로 합니다 [cite: 5]




---



## 🔗 프로젝트 결과물
* 📽️ **[발표 자료 바로가기](링크_주소_입력)**
* 🎬 **[시연 영상 바로가기](링크_주소_입력)**



---



## 📐 Database Architecture (ERD)

<p align="center">
  <img src="https://github.com/user-attachments/assets/3443ccbb-e8af-4dd8-802b-489180a0384a" width="80%" alt="Database ERD">
</p>




### **📌 Schema Highlights**
* [cite_start]**Master Table**: `pp_road` 테이블을 중심으로 모든 교통 지표를 1:N 관계로 매핑 [cite: 5]
* [cite_start]**Normalization**: `road_info`를 통해 도로의 물리적 특성을 분리 관리하여 데이터 중복 최소화 [cite: 5]
* [cite_start]**Optimization**: `datetime` 및 `road_id` 인덱싱을 통해 대용량 로그 데이터 조회 성능 최적화 [cite: 5]




---




## 🚀 수행 절차 및 구조
1. [cite_start]**Data Ingestion**: 인천시 공공데이터 포털 및 기상청 API를 통한 원천 데이터 수집 [cite: 5]
2. [cite_start]**Data Normalization**: `link_id`와 `road_name` 기준의 엔티티 간 외래 키(FK) 관계 설정 [cite: 5]
3. [cite_start]**Database Modeling**: MySQL Workbench를 활용한 물리 모델링 및 제약 조건 설정 [cite: 5]
4. [cite_start]**Analysis & Visualization**: 사고 다발 구간 및 위험 지역 분석 결과 도출 [cite: 5]





---




## 🔗 프로젝트 결과물
* 📽️ **[발표 자료 바로가기](링크_주소_입력)**
* 🎬 **[시연 영상 바로가기](링크_주소_입력)**




---





## 📚 Reference
* [인천광역시 공공데이터 포털](https://www.data.go.kr/)
* [기상청 기상자료개방포털 (ASOS)](https://data.kma.go.kr/)
* [cite_start]**SK Networks Family AI Camp** [cite: 5]
