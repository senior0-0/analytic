CREATE TABLE IF NOT EXISTS traffic_facts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    date DATE,
    time TIME,
    avg_speed FLOAT,
    weather_type VARCHAR(50),
    weather_code INTEGER,
    temperature FLOAT,
    precipitation FLOAT,
    intensity_30min INTEGER,
    cars INTEGER,
    trucks INTEGER,
    busses INTEGER,
    load_factor FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Создание индексов
CREATE INDEX IF NOT EXISTS idx_traffic_timestamp ON traffic_facts(timestamp);
CREATE INDEX IF NOT EXISTS idx_traffic_date ON traffic_facts(date);
CREATE INDEX IF NOT EXISTS idx_traffic_weather ON traffic_facts(weather_type);

-- Проверка
DO $$
BEGIN
    RAISE NOTICE 'Database initialized successfully';
    RAISE NOTICE 'User: analytics_user';
    RAISE NOTICE 'Database: transport_dwh';
END $$;
EOF