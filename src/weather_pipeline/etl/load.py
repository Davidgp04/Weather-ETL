
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s [%(levelname)s] %(message)s")


from src.weather_pipeline.db.database_connection import Database

load_dotenv()
DB_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "dbname": os.getenv('DB_NAME')
}


def load_data(df ):
    database_connection = Database(DB_CONFIG)
    try:
        database_connection.truncate_table('staging_weather_table')
        logging.info("Staging table truncated successfully.")
    except Exception as e:
        logging.error(f"Error truncating staging table: {e}")
    try:
        data_to_insert= [
            (row.Location,
              row.Date_Time,
              row.Temperature_C,
              row.Humidity_pct,
              row.Precipitation_mm,
              row.Wind_Speed_kmh
              ) for row in df.itertuples(index=False)
        ]
        query = '''
        INSERT INTO staging_weather_table 
        (location, date_time, temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''

        # Bulk
        database_connection.execute_many(query, data_to_insert)
        logging.info("Data loaded successfully into staging table")
    except Exception as e:
        logging.error(f"Error loading data into staging table: {e}")

    try:
        with open('sql/merge_weather.sql', 'r') as file:
            merge_query = file.read()
        database_connection.execute_query(merge_query)
        logging.info("Data merged successfully into main table")
    except Exception as e:
        logging.error(f"Error merging data into main table: {e}")
