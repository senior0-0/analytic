-- 1. Слой BRONZE 
CREATE TABLE bronze.raw_detections (
    camera_id UInt32,
    ts DateTime64(3),
    vehicle_type String,
    speed Float32,
    confidence Float32,
    frame_path String,
    weather_temp Nullable(Float32),
    weather_condition Nullable(String)
) ENGINE = S3('http://minio:9000/bronze/detections/*.parquet', 'Parquet')
SETTINGS input_format_parquet_import_nested = 1;

CREATE TABLE bronze.raw_weather (
    station_id UInt32,
    ts DateTime,
    temp Float32,
    condition String
) ENGINE = S3('http://minio:9000/bronze/weather/*.parquet', 'Parquet');

-- 2. Слой SILVER 
CREATE TABLE silver.detections (
    detection_id UInt64,
    camera_id UInt32,
    segment_id UInt32,
    ts DateTime64(3),
    vehicle_type LowCardinality(String),
    speed Float32,
    confidence Float32,
    valid_from DateTime DEFAULT now(),
    valid_to DateTime DEFAULT '2100-01-01'
) ENGINE = ReplacingMergeTree(valid_to)
ORDER BY (detection_id, ts)
PARTITION BY toYYYYMM(ts)
TTL valid_from + INTERVAL 180 DAY;  

CREATE TABLE silver.cameras (
    camera_id UInt32,
    location String,
    road_segment_id UInt32,
    calibration_date Date,
    status String,
    valid_from DateTime,
    valid_to DateTime
) ENGINE = ReplacingMergeTree(valid_to)
ORDER BY camera_id;

CREATE TABLE silver.road_segments (
    segment_id UInt32,
    length_km Float32,
    lanes UInt8,
    speed_limit UInt8,
    valid_from DateTime,
    valid_to DateTime
) ENGINE = ReplacingMergeTree(valid_to)
ORDER BY segment_id;

-- 3. Слой GOLD 
CREATE TABLE gold.mart_current_congestion (
    ts DateTime,
    segment_id UInt32,
    avg_speed Float32,
    intensity UInt32,           
    congestion_level UInt8,     
    update_time DateTime DEFAULT now()
) ENGINE = SummingMergeTree
ORDER BY (segment_id, ts)
PARTITION BY toYYYYMM(ts)
TTL ts + INTERVAL 180 DAY;

-- Прогнозы на 30/60/120 минут
CREATE TABLE gold.mart_forecast (
    segment_id UInt32,
    forecast_ts DateTime,
    avg_speed_forecast Float32,
    congestion_forecast UInt8,
    horizon_min UInt8,          
    model_version String,
    update_time DateTime
) ENGINE = MergeTree
ORDER BY (segment_id, forecast_ts);

-- Исторические 
CREATE TABLE gold.mart_historical_trends (
    segment_id UInt32,
    ts DateTime,
    avg_speed Float32,
    intensity UInt32,
    speed_pct_diff_vs_last_week Float32,
    load_date Date DEFAULT toDate(ts)
) ENGINE = SummingMergeTree
ORDER BY (segment_id, ts);

-- Структура потока 
CREATE TABLE gold.mart_flow_structure (
    segment_id UInt32,
    ts DateTime,
    vehicle_type LowCardinality(String),
    count UInt32,
    share_percent Float32
) ENGINE = SummingMergeTree
ORDER BY (segment_id, ts, vehicle_type);

-- Грузовики в часы пик
CREATE TABLE gold.mart_heavy_truck_hours (
    segment_id UInt32,
    date Date,
    hour UInt8,
    heavy_truck_count UInt32,
    threshold_exceeded Boolean
) ENGINE = SummingMergeTree
ORDER BY (segment_id, date, hour);

-- Опасные сближения 
CREATE TABLE gold.mart_dangerous_proximity (
    segment_id UInt32,
    ts DateTime,
    direction String,
    proximity_events_5min UInt16
) ENGINE = SummingMergeTree
ORDER BY (segment_id, ts);

CREATE TABLE gold.mart_prescriptive_actions (
    segment_id UInt32,
    direction String,
    action_type String,
    expected_effect String,
    valid_until DateTime,
    priority UInt8
) ENGINE = MergeTree
ORDER BY (segment_id, valid_until);