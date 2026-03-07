import pandas as pd
import time
import sqlite3
from datetime import datetime
import pandas as pd
import logging
import argparse
from src.weather_pipeline.etl.extract import extract_data
from src.weather_pipeline.etl.transform import transform_data
from src.weather_pipeline.etl.load import load_data
 

## Uncomment these lines when trying to work with Airflow

# from airflow import DAG
# from airflow.sdk import task
# from airflow.sdk import dag
# from airflow.providers.standard.operators.python import PythonOperator

##
import os
from dotenv import load_dotenv
from src.weather_pipeline.db.database_connection import Database
# from vine import transform
import psycopg2



load_dotenv()


logging.basicConfig(level=logging.INFO,
                    format= "%(asctime)s [%(levelname)s] %(message)s")




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


parser = argparse.ArgumentParser(description='Run the ETL pipeline for weather data.')

parser.add_argument('--file', type=str, required = True)
args = parser.parse_args()
# test_pipeline()
now = time.time()
run_etl_pipeline(args.file)
logging.info(f"Execution time: {time.time() - now:0.2f} seconds")
# dag = weather_pipeline()
# print(f"Execution time: {time.time() - now:0.2f} seconds")