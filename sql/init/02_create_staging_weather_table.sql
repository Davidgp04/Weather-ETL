CREATE TABLE IF NOT EXISTS staging_weather_table (
    location text,
    date_time timestamp,
    temperature_c real,
    humidity_pct real,
    precipitation_mm real,
    wind_speed_kmh real
);