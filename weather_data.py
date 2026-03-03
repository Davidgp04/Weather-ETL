import pandas as pd
import time
import sqlite3
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.sdk import task
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
import os
from dotenv import load_dotenv
from vine import transform

load_dotenv()

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

def load_data(df,db_name):
    # df=pd.read_parquet('data/weather_data_cleaned.parquet')
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT,
                date_time TEXT,
                temperature_c REAL,
                humidity_pct REAL,
                precipitation_mm REAL,
                wind_speed_kmh REAL
            )
""")
        # Coding something
        # Prepare the data as a list of tuples
        # We convert the Timestamp to string here for SQLite compatibility
        data_to_insert = [
            (
                row.Location, 
                str(row.Date_Time), 
                row.Temperature_C, 
                row.Humidity_pct, 
                row.Precipitation_mm, 
                row.Wind_Speed_kmh
            ) 
            for row in df.itertuples(index=False)
        ]

        query = '''
            INSERT INTO weather_data 
            (location, date_time, temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh)
            VALUES (?, ?, ?, ?, ?, ?)
        '''

        # Bulk insert
        cursor.executemany(query, data_to_insert)
        conn.commit()
        print(f"Successfully loaded {len(data_to_insert)} rows.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
now = time.time()

def run_etl_pipeline():
    data_path="data"
    data=extract_data(f'{data_path}/weather_data.csv')
    data=transform_data(data)
    load_data(data,f'{data_path}/weather_data.db')
    print(len(data))
    print(data.head())

@dag(
        dag_id='weather_etl_pipeline',
        start_date=datetime(2024, 1, 1),
        schedule='@once',
        catchup=False,
)
def weather_pipeline():

    data_path="data"
    @task
    def extract():
        df=extract_data(f'{data_path}/weather_data.csv')
        raw_path=f'{data_path}/weather_data.parquet'
        df.to_parquet(raw_path, index=False)
        return raw_path

    @task
    def transform(raw_path:str):
        df=pd.read_parquet(raw_path)
        df=transform_data(df)
        transformed_path=f'{data_path}/weather_data_cleaned.parquet'
        df.to_parquet(transformed_path, index=False)
        return transformed_path
    @task
    def load(transformed_path:str):
        load_data(pd.read_parquet(transformed_path),f'{data_path}/weather_data.db')
    
    raw=extract()
    transformed=transform(raw)
    load(transformed)


# run_etl_pipeline()
dag = weather_pipeline()
print(f"Execution time: {time.time() - now:0.2f} seconds")