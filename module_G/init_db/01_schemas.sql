CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS staging.raw_detections (
    detection_id SERIAL PRIMARY KEY,
    camera_id VARCHAR(50),
    vehicle_type VARCHAR(20),
    speed_kmh NUMERIC,
    detected_at TIMESTAMP,
    raw_payload JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.cleaned_detections (
    detection_id SERIAL PRIMARY KEY,
    camera_id VARCHAR(50),
    vehicle_type VARCHAR(20),
    speed_kmh NUMERIC,
    detected_at TIMESTAMP,
    is_valid BOOLEAN,
    quality_issues TEXT[],
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.enriched_detections (
    detection_id SERIAL PRIMARY KEY,
    camera_id VARCHAR(50),
    vehicle_type VARCHAR(20),
    speed_kmh NUMERIC,
    detected_at TIMESTAMP,
    temperature_c NUMERIC,
    weather_condition VARCHAR(50),
    is_weekend BOOLEAN,
    hour_of_day INT,
    enriched_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.hourly_traffic (
    hour TIMESTAMP,
    camera_id VARCHAR(50),
    total_vehicles INT,
    avg_speed_kmh NUMERIC,
    primary_vehicle_type VARCHAR(20),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (hour, camera_id)
);

CREATE TABLE IF NOT EXISTS core.data_quality_audit (
    audit_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    table_name VARCHAR(100),
    detected_at TIMESTAMP,
    issue_count INT,
    details JSONB,
    checked_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_detections_time ON staging.raw_detections(detected_at);
CREATE INDEX IF NOT EXISTS idx_cleaned_time ON core.cleaned_detections(detected_at);
CREATE INDEX IF NOT EXISTS idx_enriched_time ON core.enriched_detections(detected_at);