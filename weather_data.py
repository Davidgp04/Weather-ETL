import pandas as pd
import time
import sqlite3
from datetime import datetime
import pandas as pd
import logging
import argparse
 

## Uncomment these lines when trying to work with Airflow

# from airflow import DAG
# from airflow.sdk import task
# from airflow.sdk import dag
# from airflow.providers.standard.operators.python import PythonOperator

##
import os
from dotenv import load_dotenv
from database_connection import Database
# from vine import transform
import psycopg2



load_dotenv()


logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s [%(levelname)s] %(message)s")

DB_CONFIG = {
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "dbname": os.getenv('DB_NAME')
}

def extract_data(file_path):
    data = pd.read_csv(file_path)
    # data.to_parquet('data/weather_data.parquet', index=False)
    return data


def transform_data(data):
    # data=pd.read_parquet('data/weather_data.parquet')
    data = data.dropna()
    data=data.drop_duplicates()
    data['Date_Time'] = pd.to_datetime(data['Date_Time'], errors='coerce')
    data = data.dropna(subset=['Date_Time'])
    data = (
    data.groupby(['Location','Date_Time'], as_index=False).mean(numeric_only=True)
    )
    data = data[data['Humidity_pct'].between(0, 100)]
    data = data[data['Temperature_C'].between(-50, 60)]
    data = data[data['Wind_Speed_kmh'] >= 0]
    data = data[data['Precipitation_mm'] >= 0]
    # data.to_parquet('data/weather_data_cleaned.parquet', index=False)
    return data

def load_data(df):
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

def run_etl_pipeline(file_path):
    data_path="data"
    data=extract_data(f'{data_path}/{file_path}')
    logging.info(f"Data extracted successfully. {-now+time.time():0.2f} seconds")
    data=transform_data(data)
    logging.info(f"Data transformed successfully. {-now+time.time():0.2f} seconds")
    load_data(data)
    logging.info(f"Data loaded successfully. {now-time.time():0.2f} seconds")
    print(len(data))
    print(data.head())

# @dag(
#         dag_id='weather_etl_pipeline',
#         start_date=datetime(2024, 1, 1),
#         schedule='@once',
#         catchup=False,
# )
# def weather_pipeline():

#     data_path="data"
#     @task
#     def extract():
#         df=extract_data(f'{data_path}/weather_data.csv')
#         raw_path=f'{data_path}/weather_data.parquet'
#         df.to_parquet(raw_path, index=False)
#         return raw_path

#     @task
#     def transform(raw_path:str):
#         df=pd.read_parquet(raw_path)
#         df=transform_data(df)
#         transformed_path=f'{data_path}/weather_data_cleaned.parquet'
#         df.to_parquet(transformed_path, index=False)
#         return transformed_path
#     @task
#     def load(transformed_path:str):
#         load_data(pd.read_parquet(transformed_path),f'{data_path}/weather_data.db')
    
#     raw=extract()
#     transformed=transform(raw)
#     load(transformed)


def test_pipeline():
    database_connection = Database(DB_CONFIG)
    result = database_connection.execute_query("""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';
                                               """)
    print(result)


parser = argparse.ArgumentParser(description='Run the ETL pipeline for weather data.')

parser.add_argument('--file', type=str, required = True)
args = parser.parse_args()
# test_pipeline()
now = time.time()
run_etl_pipeline(args.file)
logging.info(f"Execution time: {time.time() - now:0.2f} seconds")
# dag = weather_pipeline()
# print(f"Execution time: {time.time() - now:0.2f} seconds")