CREATE TABLE weather_table (
    id SERIAL PRIMARY KEY,
    location TEXT,
    date_time TIMESTAMP,
    temperature_c REAL,
    humidity_pct REAL,
    precipitation_mm REAL,
    wind_speed_kmh REAL,
    sample_count INTEGER NOT NULL DEFAULT 1,

    CONSTRAINT weather_table_location_date_time_key 
        UNIQUE (location, date_time),

    CONSTRAINT weather_table_humidity_pct_check
        CHECK (humidity_pct >= 0 AND humidity_pct <= 100),

    CONSTRAINT weather_table_precipitation_mm_check
        CHECK (precipitation_mm >= 0),

    CONSTRAINT weather_table_temperature_c_check
        CHECK (temperature_c >= -50 AND temperature_c <= 60),

    CONSTRAINT weather_table_wind_speed_km_check
        CHECK (wind_speed_kmh >= 0)
);