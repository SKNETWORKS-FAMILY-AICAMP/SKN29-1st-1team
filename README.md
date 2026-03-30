<h1> 🚙 스마트 트래픽 인천: 교통 흐름 데이터 활용 가이드 🚚 </h1>

<br>

## 🚦 데이터로 도로를 읽고, 문제를 찾아낸다
> **인천광역시의 파편화된 교통·사고·기상 데이터를 통합하여 도로 안전 인사이트를 도출하는 데이터 시스템입니다.**

<br>

---

<br>

## 2. Team Route 👥
### 누가 무엇을 했는지 구조화

| 이름 | 역할 | 담당 분석 및 업무 |
| :---: | :---: | :--- |
| **김진욱** | **Project Lead** | [cite_start]총괄 DB 설계 및 인천공항 방향 교통 흐름 분석 [cite: 5] |
| **이지현** | **Data Analysis** | [cite_start]데이터 수집 및 요일/시간/날씨별 도로 분석 패턴 정립 [cite: 5] |
| **이동윤** | **Frontend** | [cite_start]프론트엔드 개발 및 사고량과의 상관관계 영향도 분석 [cite: 5] |
| **김은진** | **Designer** | [cite_start]크리에이티브 디자인 및 운전자 스트레스 지수 시각화 [cite: 5] |

<br>

---

<br>

## 3. Visualization Preview 🖼️
### 결과를 눈으로 바로 보여주는 영역

*(현재 내용을 비워두었습니다. 결과물 스크린샷이나 시연 GIF를 여기에 추가하세요.)*

<br>

---

<br>

## 4. Key Signals 💡
### 분석 결과의 요약된 인사이트

* [cite_start]**기상 상관성**: 특정 시간대와 날씨 조건이 결합될 때 교통량이 급격히 변동함 [cite: 5]
* [cite_start]**사고 분석**: 사고 발생은 단순 교통량보다 특정 위험 요소 변수와 더 높은 상관성을 보임 [cite: 5]
* [cite_start]**흐름 집중**: 인천공항으로의 이동 흐름이 특정 주요 도로 축에 집중되는 경향 확인 [cite: 5]
* [cite_start]**운전자 환경**: 일부 도로 구조가 운전자에게 상대적으로 높은 스트레스를 유발함을 파악 [cite: 5]

<br>

---

<br>

## 5. Project Route 🚀
### 프로젝트 전체 구조

1. **Planning**: 인천시 교통 문제 정의 및 데이터 확보 전략 수립
2. **Modeling**: 논리적/물리적 DB 설계 및 정규화 작업
3. **Processing**: Python 기반 데이터 전처리 및 Feature Engineering
4. **Analysis**: SQL 및 Pandas를 활용한 다각도 교통 패턴 분석
5. **Output**: Streamlit 기반 시각화 대시보드 구현

<br>

---

<br>

## 6. ERD 구조도 📐
### 데이터 구조 시각화 (테이블 관계)

<p align="center">
  <img src="https://github.com/user-attachments/assets/3443ccbb-e8af-4dd8-802b-489180a0384a" width="90%" alt="Database ERD">
</p>

#### **📌 설계 핵심**
* [cite_start]**Master Table**: `pp_road` 테이블을 중심으로 모든 교통 지표를 1:N 관계로 매핑 [cite: 5]
* [cite_start]**Normalization**: `road_info`를 통해 도로의 물리적 특성을 분리 관리하여 데이터 중복 최소화 [cite: 5]
* [cite_start]**Optimization**: `datetime` 및 `road_id` 인덱싱으로 대용량 로그 데이터 조회 성능 최적화 [cite: 5]

<br>

---

<br>

## 7. Data Route 🔄
### 데이터 흐름 설명 (출처 → 전처리 → Feature)

* [cite_start]**Source**: 인천광역시 공공데이터 포털(교통/사고), 기상청 ASOS(기상 패턴) [cite: 5]
* [cite_start]**Preprocessing**: `link_id`와 `road_name` 기준의 엔티티 간 외래 키(FK) 관계 설정 및 정규화 [cite: 5]
* **Feature**: 시간대별 교통량, 도로별 위험 등급, 사고 발생 빈도, 기상 항목별 가중치 등

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

## 9. 레퍼런스 📚

* [인천광역시 공공데이터 포털](https://www.data.go.kr/)
* [기상청 기상자료개방포털 (ASOS)](https://data.kma.go.kr/)
* [cite_start]**SK Networks Family AI Camp** [cite: 5]
