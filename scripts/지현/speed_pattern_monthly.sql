CREATE TABLE IF NOT EXISTS speed_pattern_monthly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    statDate VARCHAR(6) NOT NULL, -- 조회연월 (예: 202301)
    roadName VARCHAR(100),
    direction VARCHAR(20),
    sectionName VARCHAR(200),
    monthStatValue INT, -- 월 평균 통행속도
    mon INT, tue INT, wed INT, thu INT, fri INT, sat INT, sun INT, -- 요일별 평균
    UNIQUE KEY uniq_pattern (statDate, roadName, direction, sectionName)
);
