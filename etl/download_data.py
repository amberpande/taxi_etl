import requests
import os
from datetime import datetime

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def run():
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    year = '2023'

    for month in months:
        filename = f"yellow_tripdata_{year}-{month}.parquet"
        url = base_url + filename
        local_filename = os.path.join('/tmp', filename)
        download_file(url, local_filename)
        print(f"Downloaded {filename}")

if __name__ == "__main__":
    run()