import pandas as pd
def extract_data(file_path):
    data = pd.read_csv(file_path)
    return data
