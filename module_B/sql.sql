CREATE TABLE traffic_data (
    timestamp TIMESTAMP PRIMARY KEY,
    date DATE,
    time TIME,
    avg_speed FLOAT,
    weather_type VARCHAR(20),
    weather_code INTEGER,
    temperature FLOAT,
    precipitation FLOAT,
    intensity_30min INTEGER,
    cars INTEGER,
    trucks INTEGER,
    busses INTEGER
);