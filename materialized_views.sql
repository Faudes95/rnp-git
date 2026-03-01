CREATE MATERIALIZED VIEW vitals_daily_avg AS
SELECT patient_id, date(timestamp) as day, avg(hr) as hr_avg
FROM vitals
GROUP BY patient_id, date(timestamp);

CREATE INDEX idx_vitals_daily ON vitals_daily_avg(patient_id);

-- Refrescar:
-- REFRESH MATERIALIZED VIEW vitals_daily_avg;
