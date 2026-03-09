import pandas as pd
def extract_data(file_path):
    data = pd.read_csv(file_path)
    # data.to_parquet('data/weather_data.parquet', index=False)
    return data
