import streamlit as st
import pandas as pd
import mysql.connector as mc
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────────
def get_conn():
    return mc.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_DATABASE", "traffic_db"),
        autocommit=True,
    )

@st.cache_data(ttl=300)
def load_roads():
    """도로명 목록"""
    try:
        conn = get_conn()
        df = pd.read_sql("SELECT DISTINCT roadName FROM speed_pattern_timezone ORDER BY roadName", conn)
        conn.close()
        return df["roadName"].tolist()
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return []

# ─────────────────────────────────────────────
# 데이터 조회 함수 (Pandas 기반 날짜 처리로 전면 개편)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_base_speed(road, hour, day_str):
    """선택한 요일의 평상시 평균 속도"""
    hour_col = f"hour{str(hour).zfill(2)}"
    try:
        conn = get_conn()
        query = f"SELECT statDate, {hour_col} AS speed FROM speed_pattern_timezone WHERE roadName = %s AND {hour_col} > 0"
        df = pd.read_sql(query, conn, params=(road,))
        conn.close()
        
        if df.empty: return None
        
        # 날짜 포맷 강제 변환 (DB 포맷 에러 방지)
        df['statDate'] = pd.to_datetime(df['statDate'].astype(str).str.replace('-', ''), format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['statDate'])
        
        # 선택한 요일 필터링 (Pandas 요일: 0=월 ~ 6=일)
        target_day = {"mon":0, "tue":1, "wed":2, "thu":3, "fri":4, "sat":5, "sun":6}[day_str]
        df = df[df['statDate'].dt.dayofweek == target_day]
        
        if df.empty: return None
        return round(df['speed'].mean(), 1)
    except:
        return None

@st.cache_data(ttl=300)
def load_weather_speed(road, hour, day_str, weather_type, intensity):
    """선택한 요일의 기상 조건 시 평균 속도"""
    hour_col = f"hour{str(hour).zfill(2)}"
    weather_col_map = {"rain": "rn", "wind": "ws", "snow": "dsnw"}
    intensity_ranges = {
        "rain": {"low": (1, 5),   "mid": (5, 20),  "high": (20, 9999)},
        "wind": {"low": (0, 5),   "mid": (5, 10),  "high": (10, 9999)},
        "snow": {"low": (0.1, 1), "mid": (1, 5),   "high": (5, 9999)},
    }
    
    try:
        conn = get_conn()
        if weather_type == "clear":
            query = f"""
                SELECT t.statDate, t.{hour_col} AS speed
                FROM speed_pattern_timezone t
                LEFT JOIN weather_pattern_asos w_rn ON t.statDate = w_rn.statDate AND w_rn.weatherItem = 'rn'
                LEFT JOIN weather_pattern_asos w_sn ON t.statDate = w_sn.statDate AND w_sn.weatherItem = 'dsnw'
                LEFT JOIN weather_pattern_asos w_ws ON t.statDate = w_ws.statDate AND w_ws.weatherItem = 'ws'
                WHERE t.roadName = %s AND t.{hour_col} > 0
                  AND (w_rn.{hour_col} IS NULL OR w_rn.{hour_col} = 0)
                  AND (w_sn.{hour_col} IS NULL OR w_sn.{hour_col} = 0)
                  AND (w_ws.{hour_col} IS NULL OR w_ws.{hour_col} < 5)
            """
            df = pd.read_sql(query, conn, params=(road,))
        else:
            item = weather_col_map[weather_type]
            vmin, vmax = intensity_ranges[weather_type][intensity]
            query = f"""
                SELECT t.statDate, t.{hour_col} AS speed
                FROM speed_pattern_timezone t
                JOIN weather_pattern_asos w ON t.statDate = w.statDate AND w.weatherItem = %s
                WHERE t.roadName = %s AND t.{hour_col} > 0
                  AND w.{hour_col} >= %s AND w.{hour_col} < %s
            """
            df = pd.read_sql(query, conn, params=(item, road, vmin, vmax))
        conn.close()

        if df.empty: return None
        
        df['statDate'] = pd.to_datetime(df['statDate'].astype(str).str.replace('-', ''), format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['statDate'])
        
        target_day = {"mon":0, "tue":1, "wed":2, "thu":3, "fri":4, "sat":5, "sun":6}[day_str]
        df = df[df['statDate'].dt.dayofweek == target_day]
        
        if df.empty: return None
        return round(df['speed'].mean(), 1)
    except:
        return None

@st.cache_data(ttl=300)
def load_weekly_comparison(road, hour, weather_type, intensity):
    """요일별 전체 비교 그래프 데이터"""
    hour_col = f"hour{str(hour).zfill(2)}"
    weather_col_map = {"rain": "rn", "wind": "ws", "snow": "dsnw"}
    intensity_ranges = {
        "rain": {"low": (1, 5),   "mid": (5, 20),  "high": (20, 9999)},
        "wind": {"low": (0, 5),   "mid": (5, 10),  "high": (10, 9999)},
        "snow": {"low": (0.1, 1), "mid": (1, 5),   "high": (5, 9999)},
    }
    day_labels = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    
    try:
        conn = get_conn()
        
        # 평상시 전체 데이터
        query_base = f"SELECT statDate, {hour_col} AS speed FROM speed_pattern_timezone WHERE roadName = %s AND {hour_col} > 0"
        df_base = pd.read_sql(query_base, conn, params=(road,))
        
        # 기상 조건 전체 데이터
        if weather_type == "clear":
            query_weather = f"""
                SELECT t.statDate, t.{hour_col} AS speed
                FROM speed_pattern_timezone t
                LEFT JOIN weather_pattern_asos w_rn ON t.statDate = w_rn.statDate AND w_rn.weatherItem = 'rn'
                LEFT JOIN weather_pattern_asos w_sn ON t.statDate = w_sn.statDate AND w_sn.weatherItem = 'dsnw'
                LEFT JOIN weather_pattern_asos w_ws ON t.statDate = w_ws.statDate AND w_ws.weatherItem = 'ws'
                WHERE t.roadName = %s AND t.{hour_col} > 0
                  AND (w_rn.{hour_col} IS NULL OR w_rn.{hour_col} = 0)
                  AND (w_sn.{hour_col} IS NULL OR w_sn.{hour_col} = 0)
                  AND (w_ws.{hour_col} IS NULL OR w_ws.{hour_col} < 5)
            """
            df_weather = pd.read_sql(query_weather, conn, params=(road,))
        else:
            item = weather_col_map[weather_type]
            vmin, vmax = intensity_ranges[weather_type][intensity]
            query_weather = f"""
                SELECT t.statDate, t.{hour_col} AS speed
                FROM speed_pattern_timezone t
                JOIN weather_pattern_asos w ON t.statDate = w.statDate AND w.weatherItem = %s
                WHERE t.roadName = %s AND t.{hour_col} > 0
                  AND w.{hour_col} >= %s AND w.{hour_col} < %s
            """
            df_weather = pd.read_sql(query_weather, conn, params=(item, road, vmin, vmax))
            
        conn.close()

        # 평상시 요일별 통계
        if not df_base.empty:
            df_base['statDate'] = pd.to_datetime(df_base['statDate'].astype(str).str.replace('-', ''), format='%Y%m%d', errors='coerce')
            df_base = df_base.dropna(subset=['statDate'])
            df_base['dow'] = df_base['statDate'].dt.dayofweek
            base_grouped = df_base.groupby('dow')['speed'].mean().reset_index().rename(columns={'speed': '평상시(Clear)'})
        else:
            base_grouped = pd.DataFrame(columns=['dow', '평상시(Clear)'])

        # 기상 조건 요일별 통계
        if not df_weather.empty:
            df_weather['statDate'] = pd.to_datetime(df_weather['statDate'].astype(str).str.replace('-', ''), format='%Y%m%d', errors='coerce')
            df_weather = df_weather.dropna(subset=['statDate'])
            df_weather['dow'] = df_weather['statDate'].dt.dayofweek
            weather_grouped = df_weather.groupby('dow')['speed'].mean().reset_index().rename(columns={'speed': '기상 조건'})
        else:
            weather_grouped = pd.DataFrame(columns=['dow', '기상 조건'])

        # 월~일(0~6) 틀에 맞추어 데이터 병합
        result = pd.DataFrame({'dow': range(7)})
        result['요일'] = result['dow'].map(day_labels)
        result = result.merge(base_grouped, on='dow', how='left').merge(weather_grouped, on='dow', how='left')
        
        result = result.fillna(0)
        result['평상시(Clear)'] = result['평상시(Clear)'].round(1)
        result['기상 조건'] = result['기상 조건'].round(1)
        
        return result[['요일', '평상시(Clear)', '기상 조건']]
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
# 페이지 설정 및 UI
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="인천 기상-교통 패턴 분석",
    page_icon="❄️",
    layout="wide",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
    .metric-card {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 14px;
        padding: 20px 24px;
        margin: 4px 0;
    }
    .metric-label { font-size: 13px; color: #64748b; margin-bottom: 6px; }
    .metric-value { font-size: 28px; font-weight: 700; font-family: monospace; }
    .metric-sub   { font-size: 12px; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

col_icon, col_title = st.columns([0.06, 0.94])
with col_icon:
    st.markdown("<div style='font-size:40px;margin-top:6px'>❄️</div>", unsafe_allow_html=True)
with col_title:
    st.markdown("## 기상 조건별 출근길 교통 패턴 분석")
    st.markdown("`weather_pattern_asos` 관측치와 `speed_pattern_timezone` 통계를 결합한 정량 분석 리포트 — **인천광역시**", unsafe_allow_html=True)

st.divider()

with st.sidebar:
    st.markdown("### 🔧 조건 설정")
    st.markdown("---")

    roads = load_roads()
    if not roads:
        st.warning("도로 목록을 불러올 수 없습니다.")
        st.stop()

    road = st.selectbox("🛣️ 도로명", roads)
    hour = st.slider("🕐 시간대", 0, 23, 8, format="%d시")

    day_options = {
        "mon": "월요일", "tue": "화요일", "wed": "수요일",
        "thu": "목요일", "fri": "금요일", "sat": "토요일", "sun": "일요일"
    }
    day = st.selectbox("📅 요일", options=list(day_options.keys()), format_func=lambda x: day_options[x])

    st.markdown("---")
    weather_options = {
        "clear": "☀️ 맑음",
        "rain":  "🌧️ 비(강수량)",
        "wind":  "💨 강풍(풍속)",
        "snow":  "❄️ 적설",
    }
    weather_type = st.selectbox("🌦️ 기상 조건", options=list(weather_options.keys()), format_func=lambda x: weather_options[x])

    intensity = "mid"
    if weather_type != "clear":
        intensity_map = {
            "rain": {"low": "약한 비 (1~5mm)",   "mid": "보통 비 (5~20mm)", "high": "강한 비 (20mm+)"},
            "wind": {"low": "약풍 (~5m/s)",       "mid": "보통 바람 (5~10m/s)", "high": "강풍 (10m/s+)"},
            "snow": {"low": "약한 눈 (~1cm)",     "mid": "보통 눈 (1~5cm)",  "high": "폭설 (5cm+)"},
        }
        intensity = st.radio(
            "강도",
            options=["low", "mid", "high"],
            format_func=lambda x: intensity_map[weather_type][x],
            index=1,
            horizontal=True,
        )

    st.markdown("---")
    show_weekly = st.checkbox("📊 요일별 전체 비교 보기", value=True)

with st.spinner("DB에서 데이터를 불러오는 중..."):
    base_speed    = load_base_speed(road, hour, day)
    weather_speed = load_weather_speed(road, hour, day, weather_type, intensity)

weather_label = weather_options[weather_type].split(" ", 1)[1]

if base_speed and weather_speed:
    diff      = round(weather_speed - base_speed, 1)
    diff_pct  = round((diff / base_speed) * 100, 1) if base_speed else 0
    accel_min = abs(int(diff_pct * 1.5))

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">평상시 {day_options[day]} 속도</div>
            <div class="metric-value" style="color:#60a5fa">{base_speed} km/h</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        color = "#f87171" if diff < 0 else "#34d399"
        arrow = "▼" if diff < 0 else "▲"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{weather_label} 시 예상 속도</div>
            <div class="metric-value" style="color:{color}">{weather_speed} km/h</div>
            <div class="metric-sub" style="color:{color}">{arrow} {abs(diff_pct)}%</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        color3 = "#f87171" if diff < 0 else "#34d399"
        sub3   = "⚠ 출발 시간 조정 권장" if diff < 0 else "✓ 원활한 주행 예상"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">정체 시작 가속</div>
            <div class="metric-value" style="color:{color3}">약 {accel_min}분 일찍</div>
            <div class="metric-sub" style="color:{color3}">{sub3}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["평상시(Clear)", weather_label],
        y=[base_speed, weather_speed],
        text=[f"{base_speed}", f"{weather_speed}"],
        textposition="outside",
        marker_color=["#3b82f6", "#ef4444" if diff < 0 else "#22c55e"],
        width=0.4,
    ))
    fig.update_layout(
        title=f"{road} | {hour}시 | {day_options[day]} — 평상시 vs {weather_label}",
        yaxis_title="평균 속도 (km/h)",
        plot_bgcolor="#0a0f1e",
        paper_bgcolor="#0a0f1e",
        font=dict(color="#94a3b8", family="Noto Sans KR"),
        title_font=dict(color="#e2e8f0", size=15),
        yaxis=dict(gridcolor="#1e293b", range=[0, max(base_speed, weather_speed) * 1.3]),
        xaxis=dict(gridcolor="#1e293b"),
        showlegend=False,
        margin=dict(t=50, b=30),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("해당 조건에 맞는 데이터가 없습니다. 기상 강도나 시간대를 변경해보세요.")


if show_weekly:
    st.divider()
    st.markdown("#### 📅 요일별 속도 비교")
    with st.spinner("요일별 데이터 로딩 중..."):
        weekly_df = load_weekly_comparison(road, hour, weather_type, intensity)

    if not weekly_df.empty:
        plot_df = weekly_df.melt(
            id_vars=["요일"], value_vars=["평상시(Clear)", "기상 조건"],
            var_name="기상 상태", value_name="평균 속도"
        )
        plot_df["기상 상태"] = plot_df["기상 상태"].replace({"기상 조건": weather_label})

        fig2 = px.bar(
            plot_df, x="요일", y="평균 속도", color="기상 상태",
            barmode="group", text_auto=".1f",
            color_discrete_map={
                "평상시(Clear)": "#3b82f6",
                weather_label:  "#ef4444",
            },
            category_orders={"요일": ["월", "화", "수", "목", "금", "토", "일"]},
        )
        fig2.update_layout(
            plot_bgcolor="#0a0f1e",
            paper_bgcolor="#0a0f1e",
            font=dict(color="#94a3b8", family="Noto Sans KR"),
            legend_title_text="기상 상태",
            legend=dict(font=dict(color="#cbd5e1")),
            yaxis=dict(gridcolor="#1e293b", title="평균 속도 (km/h)"),
            xaxis=dict(title="요일"),
            margin=dict(t=30, b=30),
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("요일별 비교 데이터를 불러올 수 없습니다.")

st.divider()
st.caption("Data source: speed_pattern_timezone & weather_pattern_asos")