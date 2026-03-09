import fastparquet
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.sdk import task
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator

from src.weather_pipeline.etl.extract import extract_data
from src.weather_pipeline.etl.transform import transform_data
from src.weather_pipeline.etl.load import load_data
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
        df=extract_data(f'{data_path}/raw/weather_data.csv')
        raw_path=f'{data_path}/processed/weather_data.parquet'
        df.to_parquet(raw_path, index=False)
        return raw_path

    @task
    def transform(raw_path:str):
        df=pd.read_parquet(raw_path)
        df=transform_data(df)
        transformed_path=f'{data_path}/processed/weather_data_cleaned.parquet'
        df.to_parquet(transformed_path, index=False)
        return transformed_path
    @task
    def load(transformed_path:str):
        load_data(pd.read_parquet(transformed_path))
    
    raw=extract()
    transformed=transform(raw)
    load(transformed)

