INSERT INTO weather_table (
    location,
    date_time,
    temperature_c,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    sample_count
)
SELECT
    location,
    date_time,
    AVG(temperature_c),
    AVG(humidity_pct),
    AVG(precipitation_mm),
    AVG(wind_speed_kmh),
    COUNT(*)
FROM staging_weather_table
GROUP BY location, date_time
ON CONFLICT (location, date_time)
DO UPDATE SET
    temperature_c =
        (weather_table.temperature_c * weather_table.sample_count
        + EXCLUDED.temperature_c * EXCLUDED.sample_count)
        / (weather_table.sample_count + EXCLUDED.sample_count),

    humidity_pct =
        (weather_table.humidity_pct * weather_table.sample_count
        + EXCLUDED.humidity_pct * EXCLUDED.sample_count)
        / (weather_table.sample_count + EXCLUDED.sample_count),

    precipitation_mm =
        (weather_table.precipitation_mm * weather_table.sample_count
        + EXCLUDED.precipitation_mm * EXCLUDED.sample_count)
        / (weather_table.sample_count + EXCLUDED.sample_count),

    wind_speed_kmh =
        (weather_table.wind_speed_kmh * weather_table.sample_count
        + EXCLUDED.wind_speed_kmh * EXCLUDED.sample_count)
        / (weather_table.sample_count + EXCLUDED.sample_count),

    sample_count =
        weather_table.sample_count + EXCLUDED.sample_count;