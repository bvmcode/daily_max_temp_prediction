import os
import time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv

load_dotenv()

URL_BASE="https://weather.uwyo.edu/cgi-bin/sounding"

def get_session():
    session = requests.Session()
    retries = Retry(
            total=10,
            backoff_factor=0.1,
            status_forcelist=[400, 403, 429, 500, 502, 503, 504],
        )
    session.mount(URL_BASE, HTTPAdapter(max_retries=retries))
    return session

def get_high_temps_at_location(df):
    key = os.getenv("WEATHER_API_KEY")
    station = "KNJATCO14" # my weather station
    url = "https://api.weather.com/v2/pws/history/daily?stationId={station}&format=json&units=m&date={date}&apiKey={key}"
    vals = {'forecast_date': [], 'max_temp_c': []}
    df2 = df.copy()
    df2['forecast_date'] = pd.to_datetime(df2['forecast_date'])
    for date in df2['forecast_date']:
        time.sleep(2)
        dt = date.strftime("%Y%m%d")
        url_date = url.format(station=station, date=dt, key=key)
        try:
            resp = session.get(url_date)
            obs = resp.json()['observations']
        except Exception as e:
            continue
        if len(obs)==0 or obs is None:
            continue
        max_temp_c = int(obs[0]['metric']['tempHigh'])
        vals['forecast_date'].append(date)
        vals['max_temp_c'].append(max_temp_c)
    return pd.DataFrame(vals)

def format_target(df_obs):
    df_obs["max_temp_f"] = df_obs["max_temp_c"].apply(lambda x: round(float(x)*(9/5) + 32,1))
    df_obs = df_obs.drop(columns=['max_temp_c'])
    return df_obs

if __name__ == "__main__":
    session = get_session()
    df = pd.read_csv("features.csv")
    df_labels = get_high_temps_at_location(df)
    df_labels = format_target(df_labels)
    pd.to_csv("labels.csv", index=False)
