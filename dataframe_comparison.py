import pandas as pd
import dask.dataframe as dd
import time
from pathlib import Path

data_dir = Path("nyc_taxi")
if not any(data_dir.glob("*.parquet")):
    print("Помилка: папка 'nyc_taxi' порожня. Завантажте Parquet-файли з https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    exit(1)

print("Pandas:")
try:
    start = time.time()
    files_pd = list(data_dir.glob("yellow_tripdata_2019-*.parquet"))
    df_pd = pd.concat([pd.read_parquet(f) for f in files_pd], ignore_index=True)
    df_pd['pickup_date'] = pd.to_datetime(df_pd['tpep_pickup_datetime']).dt.date
    result_pd = df_pd.groupby('pickup_date')['fare_amount'].mean()
    print(f"Pandas середня ціна = {result_pd.mean():.2f} USD, час = {time.time()-start:.2f} с")
except Exception as e:
    print(f"Pandas: {str(e)[:100]}...")

print("\nDask DataFrame:")
start = time.time()
df = dd.read_parquet(data_dir / "*.parquet", engine='pyarrow')
# Конвертуємо datetime для групування
df['pickup_date'] = dd.to_datetime(df['tpep_pickup_datetime']).dt.date
daily_mean = df.groupby('pickup_date')['fare_amount'].mean()
daily_dist = df.groupby('pickup_date')['trip_distance'].mean()

result_mean = daily_mean.compute()
result_dist = daily_dist.compute()
print(f"Dask середня ціна поїздки = {result_mean.mean():.2f} USD")
print(f"Dask середня відстань поїздки = {result_dist.mean():.2f} милі")
print(f"Загальний час Dask = {time.time() - start:.2f} секунд")
