import logging
import argparse
import time
from src.weather_pipeline.etl.extract import extract_data
from src.weather_pipeline.etl.transform import transform_data
from src.weather_pipeline.etl.load import load_data

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

parser = argparse.ArgumentParser(description='Run the ETL pipeline for weather data.')
parser.add_argument('--file', type=str, required = True)
args = parser.parse_args()
now = time.time()
run_etl_pipeline(args.file)
logging.info(f"Execution time: {time.time() - now:0.2f} seconds")
