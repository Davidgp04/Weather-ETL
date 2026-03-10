import pandas as pd
def transform_data(data):
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
    return data
