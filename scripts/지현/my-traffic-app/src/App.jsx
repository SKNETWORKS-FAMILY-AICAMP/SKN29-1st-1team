import { useState, useEffect, useRef } from "react";

const API = "http://localhost:8000";

const HOURS = Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`);

const DAYS = [
  { key: "mon", label: "월요일" },
  { key: "tue", label: "화요일" },
  { key: "wed", label: "수요일" },
  { key: "thu", label: "목요일" },
  { key: "fri", label: "금요일" },
  { key: "sat", label: "토요일" },
  { key: "sun", label: "일요일" },
];

const WEATHER_OPTIONS = [
  { key: "clear",   label: "맑음",      icon: "☀️" },
  { key: "rain",    label: "비(강수량)", icon: "🌧️" },
  { key: "wind",    label: "강풍(풍속)", icon: "💨" },
  { key: "snow",    label: "적설",       icon: "❄️" },
];

const WEATHER_DETAIL = {
  rain: [
    { label: "약한 비 (1~5mm)",  value: "rn_low"   },
    { label: "보통 비 (5~20mm)", value: "rn_mid"   },
    { label: "강한 비 (20mm+)",  value: "rn_high"  },
  ],
  wind: [
    { label: "약풍 (~7m/s)",      value: "ws_low"   },
    { label: "보통 바람 (7~12m/s)", value: "ws_mid" },
    { label: "강풍 (12m/s+)",     value: "ws_high"  },
  ],
  snow: [
    { label: "약한 눈 (~1cm)",  value: "snow_low"  },
    { label: "보통 눈 (1~5cm)", value: "snow_mid"  },
    { label: "폭설 (5cm+)",     value: "snow_high" },
  ],
};

// ── 유틸 ────────────────────────────────────────────────────
function weatherDetailKey(weather, detail) {
  if (weather === "clear") return "clear";
  return detail;
}

// ── SelectChip ──────────────────────────────────────────────
function SelectChip({ options, value, onChange, label }) {
  return (
    <div style={{ marginBottom: 20 }}>
      <div style={{ fontSize: 11, color: "#64748B", fontFamily: "'Noto Sans KR', sans-serif", marginBottom: 8, letterSpacing: "0.1em", textTransform: "uppercase" }}>
        {label}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {options.map(opt => {
          const isObj = typeof opt === "object";
          const key   = isObj ? opt.key   : opt;
          const lbl   = isObj ? (opt.icon ? `${opt.icon} ${opt.label}` : opt.label) : opt;
          const selected = value === key;
          return (
            <button key={key} onClick={() => onChange(key)} style={{
              padding: "7px 14px", borderRadius: 8,
              border: selected ? "1px solid #60A5FA" : "1px solid rgba(255,255,255,0.1)",
              background: selected ? "rgba(96,165,250,0.15)" : "rgba(255,255,255,0.03)",
              color: selected ? "#93C5FD" : "#64748B",
              fontSize: 13, cursor: "pointer",
              fontFamily: "'Noto Sans KR', sans-serif",
              transition: "all 0.15s ease", whiteSpace: "nowrap",
              boxShadow: selected ? "0 0 12px rgba(96,165,250,0.2)" : "none",
            }}>
              {lbl}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ── BarChart ────────────────────────────────────────────────
function BarChart({ baseSpeed, weatherSpeed, weatherLabel, loading }) {
  const diff    = weatherSpeed != null && baseSpeed != null ? +(weatherSpeed - baseSpeed).toFixed(1) : null;
  const diffPct = baseSpeed    ? ((diff / baseSpeed) * 100).toFixed(1) : null;
  const maxVal  = Math.max(baseSpeed ?? 0, weatherSpeed ?? 0, 10) * 1.3;

  return (
    <div style={{
      marginTop: 32, background: "rgba(255,255,255,0.03)",
      borderRadius: 20, padding: "28px 32px",
      border: "1px solid rgba(255,255,255,0.08)",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 24 }}>
        <div style={{ width: 4, height: 22, background: "linear-gradient(180deg,#60A5FA,#818CF8)", borderRadius: 2 }} />
        <span style={{ color: "#CBD5E1", fontSize: 14, fontFamily: "'Noto Sans KR', sans-serif", letterSpacing: "0.05em" }}>
          속도 비교 분석
        </span>
      </div>

      {loading ? (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: 180, color: "#64748B" }}>
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: 28, marginBottom: 8, animation: "spin 1s linear infinite" }}>⟳</div>
            <div style={{ fontSize: 13, fontFamily: "'Noto Sans KR', sans-serif" }}>DB에서 데이터 불러오는 중...</div>
          </div>
        </div>
      ) : baseSpeed == null ? (
        <div style={{ textAlign: "center", color: "#475569", padding: 40, fontFamily: "'Noto Sans KR', sans-serif" }}>
          조건을 선택하면 분석 결과가 표시됩니다.
        </div>
      ) : (
        <>
          {/* KPI Cards */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 32 }}>
            {[
              { label: "평상시 속도",           value: `${baseSpeed} km/h`,    color: "#60A5FA", sub: null },
              { label: `${weatherLabel} 예상 속도`, value: weatherSpeed != null ? `${weatherSpeed} km/h` : "데이터 없음",
                color: diff < 0 ? "#F87171" : "#34D399",
                sub: diffPct != null ? `${diff > 0 ? "▲" : "▼"} ${Math.abs(diffPct)}%` : null },
              { label: "속도 변화",
                value: diff != null ? `${diff > 0 ? "+" : ""}${diff} km/h` : "-",
                color: diff < 0 ? "#F87171" : diff > 0 ? "#34D399" : "#94A3B8",
                sub: diff < 0 ? "⚠ 출발 시간 조정 권장" : diff > 0 ? "✓ 원활한 주행 예상" : "변화 없음" },
            ].map((card, i) => (
              <div key={i} style={{
                background: "rgba(255,255,255,0.04)", borderRadius: 14,
                padding: "18px 20px", border: `1px solid ${card.color}25`,
                position: "relative", overflow: "hidden",
              }}>
                <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: card.color, opacity: 0.6 }} />
                <div style={{ fontSize: 11, color: "#64748B", fontFamily: "'Noto Sans KR', sans-serif", marginBottom: 6 }}>{card.label}</div>
                <div style={{ fontSize: 26, fontWeight: 700, color: card.color, fontFamily: "'DM Mono', monospace", lineHeight: 1 }}>{card.value}</div>
                {card.sub && <div style={{ fontSize: 11, color: card.color, marginTop: 6, fontFamily: "'Noto Sans KR', sans-serif", opacity: 0.8 }}>{card.sub}</div>}
              </div>
            ))}
          </div>

          {/* Bar Chart */}
          <div style={{ display: "flex", gap: 40, alignItems: "flex-end", height: 160, paddingBottom: 8 }}>
            {[
              { label: "평상시",   speed: baseSpeed,    color: "#60A5FA" },
              { label: weatherLabel, speed: weatherSpeed, color: diff != null && diff < 0 ? "#F87171" : "#34D399" },
            ].map((bar, i) => {
              const pct = bar.speed != null ? (bar.speed / maxVal) * 100 : 0;
              return (
                <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 8 }}>
                  <div style={{ fontSize: 15, fontWeight: 700, color: bar.color, fontFamily: "'DM Mono', monospace" }}>
                    {bar.speed != null ? bar.speed : "-"} <span style={{ fontSize: 11, fontWeight: 400, color: "#64748B" }}>km/h</span>
                  </div>
                  <div style={{
                    width: "100%", maxWidth: 100, height: `${pct}%`,
                    background: `linear-gradient(180deg,${bar.color}CC,${bar.color}44)`,
                    borderRadius: "10px 10px 4px 4px", minHeight: 8,
                    transition: "height 0.8s cubic-bezier(0.34,1.56,0.64,1)",
                    boxShadow: `0 0 20px ${bar.color}33`,
                    border: `1px solid ${bar.color}44`,
                  }} />
                  <div style={{ fontSize: 12, color: "#94A3B8", fontFamily: "'Noto Sans KR', sans-serif" }}>{bar.label}</div>
                </div>
              );
            })}
          </div>
          <div style={{ height: 1, background: "rgba(255,255,255,0.1)", marginTop: 4 }} />

          <div style={{ marginTop: 16, padding: "12px 16px", background: "rgba(96,165,250,0.05)", borderRadius: 10, border: "1px solid rgba(96,165,250,0.1)" }}>
            <div style={{ fontSize: 12, color: "#94A3B8", fontFamily: "'Noto Sans KR', sans-serif", lineHeight: 1.7 }}>
              📊 <strong style={{ color: "#CBD5E1" }}>분석 기반:</strong> weather_pattern_asos 기상 관측치 × speed_pattern_monthly 통계 결합
              {diff != null && diff < 0 && (
                <span style={{ color: "#F87171" }}> · 해당 기상 조건에서 평균 속도가 <strong>{Math.abs(diffPct)}%</strong> 감소할 것으로 예측됩니다.</span>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// ── Main App ────────────────────────────────────────────────
export default function App() {
  const [roads,         setRoads]         = useState([]);
  const [road,          setRoad]          = useState("");
  const [hour,          setHour]          = useState("08:00");
  const [day,           setDay]           = useState("mon");
  const [weather,       setWeather]       = useState("rain");
  const [weatherDetail, setWeatherDetail] = useState("rn_mid");

  const [baseSpeed,     setBaseSpeed]     = useState(null);
  const [weatherSpeed,  setWeatherSpeed]  = useState(null);
  const [loading,       setLoading]       = useState(false);
  const [roadsLoading,  setRoadsLoading]  = useState(true);
  const [roadsError,    setRoadsError]    = useState(false);

  const [aiInsight, setAiInsight]   = useState("");
  const [aiLoading, setAiLoading]   = useState(false);

  // 도로 목록 로딩
  useEffect(() => {
    fetch(`${API}/api/roads`)
      .then(r => r.json())
      .then(data => {
        setRoads(data.roads ?? []);
        if (data.roads?.length) setRoad(data.roads[0]);
        setRoadsLoading(false);
      })
      .catch(() => { setRoadsError(true); setRoadsLoading(false); });
  }, []);

  // 날씨 변경 시 detail 초기화
  useEffect(() => {
    if (weather !== "clear" && WEATHER_DETAIL[weather]) {
      setWeatherDetail(WEATHER_DETAIL[weather][1].value);
    }
  }, [weather]);

  // 속도 조회
  useEffect(() => {
    if (!road) return;
    const h = parseInt(hour);
    const wKey = weatherDetailKey(weather, weatherDetail);

    setLoading(true);
    setBaseSpeed(null);
    setWeatherSpeed(null);

    Promise.all([
      fetch(`${API}/api/speed/base?roadname=${encodeURIComponent(road)}&hour=${h}&day=${day}`).then(r => r.json()),
      fetch(`${API}/api/speed/weather?roadname=${encodeURIComponent(road)}&hour=${h}&day=${day}&weather=${wKey}`).then(r => r.json()),
    ])
      .then(([base, weather]) => {
        setBaseSpeed(base.base_speed);
        setWeatherSpeed(weather.predicted_speed);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [road, hour, day, weather, weatherDetail]);

  const selectedWeatherLabel =
    weather === "clear" ? "맑음"
    : WEATHER_DETAIL[weather]?.find(w => w.value === weatherDetail)?.label ?? weather;

  async function fetchAiInsight() {
    setAiLoading(true);
    setAiInsight("");
    try {
      const diff = weatherSpeed != null && baseSpeed != null ? (weatherSpeed - baseSpeed).toFixed(1) : "N/A";
      const diffPct = baseSpeed ? (((weatherSpeed - baseSpeed) / baseSpeed) * 100).toFixed(1) : "N/A";
      const dayLabel = DAYS.find(d => d.key === day)?.label ?? day;

      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "claude-sonnet-4-20250514",
          max_tokens: 1000,
          messages: [{ role: "user", content:
            `당신은 도로 교통 및 기상 분석 전문가입니다. 인천광역시 기준으로 아래 조건에 대한 교통 상황 인사이트를 한국어로 3문장 이내로 작성하세요.
- 도로: ${road}
- 시간: ${hour}
- 요일: ${dayLabel}
- 기상: ${selectedWeatherLabel}
- 평상시 속도: ${baseSpeed} km/h
- 예측 속도: ${weatherSpeed} km/h (${diffPct}% 변화)
출발 시간 조정 여부와 주의사항을 포함해 실용적 조언을 제공하세요.`
          }],
        })
      });
      const data = await res.json();
      setAiInsight(data.content?.map(c => c.text || "").join("") ?? "");
    } catch {
      setAiInsight("AI 분석을 불러오는 데 실패했습니다.");
    }
    setAiLoading(false);
  }

  return (
    <div style={{
      minHeight: "100vh", background: "#0A0F1E",
      backgroundImage: `
        radial-gradient(ellipse at 20% 20%, rgba(37,99,235,0.12) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 80%, rgba(124,58,237,0.08) 0%, transparent 50%)
      `,
      fontFamily: "'Noto Sans KR', sans-serif", padding: "40px 20px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+KR:wght@400;500;700&display=swap');
        @keyframes spin    { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        @keyframes fadeIn  { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        button:hover { opacity: 0.85; }
        * { box-sizing: border-box; }
      `}</style>

      <div style={{ maxWidth: 860, margin: "0 auto" }}>
        {/* Header */}
        <div style={{ marginBottom: 36 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
            <div style={{
              width: 40, height: 40, borderRadius: 12,
              background: "linear-gradient(135deg,#2563EB,#7C3AED)",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20, boxShadow: "0 0 24px rgba(37,99,235,0.4)"
            }}>❄️</div>
            <h1 style={{ fontSize: 24, fontWeight: 700, color: "#F1F5F9", margin: 0, letterSpacing: "-0.02em" }}>
              기상 조건별 출근길 교통 패턴 분석
            </h1>
          </div>
          <p style={{ color: "#475569", fontSize: 13, margin: 0, paddingLeft: 52 }}>
            <code style={{ color: "#60A5FA", background: "rgba(96,165,250,0.1)", padding: "2px 6px", borderRadius: 4, fontSize: 11 }}>weather_pattern_asos</code>
            {" "}관측치와{" "}
            <code style={{ color: "#A78BFA", background: "rgba(167,139,250,0.1)", padding: "2px 6px", borderRadius: 4, fontSize: 11 }}>speed_pattern_monthly</code>
            {" "}통계를 결합한 정량 분석 리포트 — 인천광역시
          </p>
        </div>

        {/* Control Panel */}
        <div style={{
          background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.08)",
          borderRadius: 20, padding: "28px 32px", marginBottom: 24, animation: "fadeIn 0.4s ease",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 24 }}>
            <div style={{ width: 4, height: 20, background: "linear-gradient(180deg,#60A5FA,#818CF8)", borderRadius: 2 }} />
            <span style={{ color: "#94A3B8", fontSize: 13, letterSpacing: "0.08em" }}>조건 설정</span>
          </div>

          {/* Road */}
          {roadsLoading ? (
            <div style={{ color: "#475569", fontSize: 13, marginBottom: 20, fontFamily: "'Noto Sans KR', sans-serif" }}>
              🔄 도로 목록 불러오는 중...
            </div>
          ) : roadsError ? (
            <div style={{ color: "#F87171", fontSize: 13, marginBottom: 20 }}>
              ⚠️ DB 연결 실패 — 백엔드 서버가 실행 중인지 확인하세요 (uvicorn traffic_api:app --reload)
            </div>
          ) : (
            <SelectChip label="도로(Road)" options={roads} value={road} onChange={setRoad} />
          )}

          {/* Hour */}
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontSize: 11, color: "#64748B", marginBottom: 8, letterSpacing: "0.1em", textTransform: "uppercase" }}>시간 (Hour)</div>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <input type="range" min={0} max={23} value={parseInt(hour)}
                onChange={e => setHour(`${String(e.target.value).padStart(2,"0")}:00`)}
                style={{ flex: 1, accentColor: "#60A5FA", cursor: "pointer" }} />
              <div style={{
                minWidth: 70, textAlign: "center", padding: "6px 14px",
                background: "rgba(96,165,250,0.12)", border: "1px solid rgba(96,165,250,0.3)",
                borderRadius: 8, color: "#93C5FD", fontSize: 14, fontFamily: "'DM Mono', monospace",
              }}>{hour}</div>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, paddingRight: 84 }}>
              {["00","06","12","18","23"].map(h => <span key={h} style={{ fontSize: 10, color: "#334155" }}>{h}</span>)}
            </div>
          </div>

          <SelectChip label="요일 (Day)"         options={DAYS}            value={day}           onChange={setDay} />
          <SelectChip label="기상 조건 (Weather)" options={WEATHER_OPTIONS} value={weather}       onChange={setWeather} />
          {weather !== "clear" && WEATHER_DETAIL[weather] && (
            <div style={{ animation: "fadeIn 0.3s ease" }}>
              <SelectChip label="강도 선택"
                options={WEATHER_DETAIL[weather].map(w => ({ key: w.value, label: w.label }))}
                value={weatherDetail} onChange={setWeatherDetail} />
            </div>
          )}
        </div>

        {/* Chart */}
        <BarChart baseSpeed={baseSpeed} weatherSpeed={weatherSpeed} weatherLabel={selectedWeatherLabel} loading={loading} />

        {/* AI Insight */}
        <div style={{
          marginTop: 20, background: "rgba(255,255,255,0.02)",
          border: "1px solid rgba(255,255,255,0.07)", borderRadius: 16, padding: "22px 28px",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div style={{ width: 4, height: 18, background: "linear-gradient(180deg,#A78BFA,#F472B6)", borderRadius: 2 }} />
              <span style={{ color: "#94A3B8", fontSize: 13, letterSpacing: "0.08em" }}>AI 교통 인사이트</span>
            </div>
            <button onClick={fetchAiInsight} disabled={aiLoading || !baseSpeed} style={{
              padding: "8px 18px",
              background: aiLoading ? "rgba(124,58,237,0.1)" : "linear-gradient(135deg,rgba(124,58,237,0.3),rgba(37,99,235,0.3))",
              border: "1px solid rgba(124,58,237,0.4)", borderRadius: 8,
              color: "#C4B5FD", fontSize: 12, cursor: aiLoading ? "not-allowed" : "pointer",
              fontFamily: "'Noto Sans KR', sans-serif",
              display: "flex", alignItems: "center", gap: 6,
            }}>
              {aiLoading
                ? <><span style={{ animation: "spin 1s linear infinite", display: "inline-block" }}>⟳</span> 분석중...</>
                : <><span>✦</span> AI 분석 요청</>}
            </button>
          </div>
          {aiInsight
            ? <p style={{ color: "#CBD5E1", fontSize: 14, lineHeight: 1.8, margin: 0, fontFamily: "'Noto Sans KR', sans-serif", animation: "fadeIn 0.5s ease" }}>{aiInsight}</p>
            : <p style={{ color: "#334155", fontSize: 13, margin: 0, fontStyle: "italic" }}>선택한 조건에 맞는 AI 분석 인사이트를 요청하세요.</p>}
        </div>

        <div style={{ marginTop: 28, textAlign: "center", color: "#1E293B", fontSize: 11, letterSpacing: "0.05em" }}>
          speed_pattern_timezone · speed_pattern_monthly · weather_pattern_asos · 인천광역시
        </div>
      </div>
    </div>
  );
}
