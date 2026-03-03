from airflow.decorators import dag, task
from datetime import datetime
import os

@dag(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def weather_pipeline():

    base_path = "/home/ubuntu/weather-etl/data"
    raw_path = os.path.join(base_path, "raw.parquet")
    transformed_path = os.path.join(base_path, "transformed.parquet")
    db_path = os.path.join(base_path, "weather_data.db")

    @task
    def extract():
        df = extract_data(f"{base_path}/weather_data.csv")
        df.to_parquet(raw_path)
        return raw_path

    @task
    def transform(input_path: str):
        df = pd.read_parquet(input_path)
        df = transform_data(df)
        df.to_parquet(transformed_path)
        return transformed_path

    @task
    def load(input_path: str):
        df = pd.read_parquet(input_path)
        load_data(df, db_path)

    raw = extract()
    transformed = transform(raw)
    load(transformed)

dag = weather_pipeline()