import pandas as pd
import os

file_path = "data/weather_data.csv"
data = pd.read_csv(file_path)

chunk_size = len(data) // 4

for i in range(4):
    if i < 3:
        chunk = data.iloc[i*chunk_size:(i+1)*chunk_size]
    else:
        chunk = data.iloc[i*chunk_size:] 

    print(f"Processing chunk {i+1} with {len(chunk)} rows.")
    chunk.to_csv(f"data/weather_data_part_{i+1}.csv", index=False)

print("Dataset split into 4 parts successfully.")