import requests
import pandas as pd
import time
from requests import RequestException
from config import POLYGON_API_KEY, API_BASE_URL


def extract_polygon_data(date_str):
    url = (API_BASE_URL + f"{date_str}")
    params = {'adjusted': 'true',
              'apiKey': POLYGON_API_KEY}

    data = _make_request_with_retry(url, params=params)

    if data is None or 'results' not in data:
        print(f"Data not downloaded for {date_str}")
        return None

    df = pd.DataFrame(data['results'])
    if df.empty:
        print(f"Data not downloaded for {date_str}")
        return None

    return df


def _make_request_with_retry(url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                print("Rate limited. Waiting...")
                time.sleep(60)
            elif res.status_code >= 500:
                print(f"Server error: {res.status_code}, retrying...")
                time.sleep(5)
            else:
                print(f"Client error: {res.status_code}. Not retrying.")
                break
        except RequestException as e:
            print(f"Request failed: {e}, attempt {attempt + 1}")
            time.sleep(5)
    return None
