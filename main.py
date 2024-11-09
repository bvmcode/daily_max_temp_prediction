from datetime import datetime, timedelta
import pickle
import pytz
import psycopg2 as pg2
import os
import re
import requests
from requests.adapters import HTTPAdapter, Retry

from apscheduler.schedulers.blocking import BlockingScheduler
import boto3
from bs4 import BeautifulSoup
import pandas as pd


STATIONS = {
    "72305": {"city": "Newport, NC", "station_name": "MHX"},
    "72317": {"city": "Greensboro, NC", "station_name": "GSO"},
    "72318": {"city": "Blacksburg, VA", "station_name": "RNK"},
    "72520": {"city": "Pittsburgh, PA", "station_name": "PIT"},
    "72528": {"city": "Buffalo, NY", "station_name": "BUF"},
    "72426": {"city": "Albany, NY", "station_name": "ILN"},
    "72501": {"city": "Upton, NY", "station_name": "OKX"},
    "72403": {"city": "Sterling, VA", "station_name": "IAD"},
    "72402": {"city": "Wallops Island, VA", "station_name": "WAL"}
}
FIELDS = ["pressure","height","temp","dew_point","rel_humidity",
              "mix_ratio","direction", "knots","theta","theta_e","theta_v"]
SOUNDING_HR = "12"
URL_BASE="https://weather.uwyo.edu/cgi-bin/sounding"
MODEL = "randomforest"
FACTOR = 1
PCA = False


def get_session():
    session = requests.Session()
    retries = Retry(
            total=10,
            backoff_factor=0.1,
            status_forcelist=[400, 403, 429, 500, 502, 503, 504],
        )
    session.mount(URL_BASE, HTTPAdapter(max_retries=retries))
    return session


def get_dataframe(data):
    values = {k:[] for k in FIELDS}
    for line in data[5:]:
        tmp_vals = []
        for val in re.finditer("([0-9]+)(\.[0-9]+)?", line):
            tmp_vals.append(val.group())
        if len(tmp_vals)!=len(FIELDS):
            continue
        for val, field in zip(tmp_vals, FIELDS):
            values[field].append(val)
    return pd.DataFrame(values)

def consolidate_pressure_levels(df, station, date, sounding_hr):
    pressure_levels = [1000, 850, 700, 500, 300, 200]
    df['pressure'] = df['pressure'].astype(float)
    indexes = []
    for p in pressure_levels:
        idx = df.iloc[(df['pressure']-p).abs().argsort()].index.values[0]
        indexes.append(idx)
    vals = {}
    for p, idx in zip(pressure_levels, indexes):
        row = df.iloc[idx,:]
        for field in FIELDS:
            val = row[field]
            col = f"{field}_{p}"
            vals[col] = [float(val)]
    df = pd.DataFrame(vals)
    df["forecast_date"] = date
    df["sounding_hr"] = sounding_hr.replace("Z","")
    df["station_name"] = STATIONS[station]["station_name"]
    return df

def get_station_data(date, station):
    params={
    "region":"nacon",
    "TYPE":r"TEXT%3ALIST",
    "YEAR":date.year,
    "MONTH":date.month,
    "FROM":str(date.day).zfill(2)+SOUNDING_HR,
    "TO": str(date.day).zfill(2)+SOUNDING_HR,
    "STNM": station
    }
    url_params="?region={region}&TYPE={TYPE}&YEAR={YEAR}&MONTH={MONTH}&FROM={FROM}&TO={TO}&STNM={STNM}"
    params = url_params.format(**params)
    url = URL_BASE + params
    session = get_session()
    resp=session.get(url, verify=False)
    session.close()
    soup = BeautifulSoup(resp.text)
    tables = soup.findAll(name='pre')
    if len(tables)==0:
        return None
    df = None
    for i, table in enumerate(tables):
        tmp_df = get_dataframe(table.text.split("\n"))
        if tmp_df.shape[0]>0:
            df = tmp_df
    final_df = consolidate_pressure_levels(df, station, date, f"{SOUNDING_HR}Z")
    final_df = final_df.drop(columns=["sounding_hr"])
    return final_df

def consolidate_stations(df):
    ignore = ('forecast_date', 'station_name')
    vals = {}
    for station in STATIONS.keys():
        station_name = STATIONS[station]['station_name']
        for col in df.columns:
            if col not in ignore:
                col_name = f"{col}_{station_name}"
                vals[col_name] = []
    vals['forecast_date'] = []
    for date in df["forecast_date"].unique():
        vals['forecast_date'].append(date)
        for station in STATIONS.keys():
            station_name = STATIONS[station]['station_name']
            tmp_df = df[(df["station_name"]==station_name) & (df["forecast_date"]==date)]
            for col in df.columns:
                if col not in ignore:
                    if tmp_df.shape[0] == 0:
                        val = None
                    else:
                        val = tmp_df[col].values[0]
                    col_name = f"{col}_{station_name}"
                    vals[col_name].append(val)
    df_updated = pd.DataFrame(vals)
    return df_updated

def get_raw_data(date):
    df = None
    for station in STATIONS:
        tmp_df = get_station_data(date, station)
        if tmp_df is None:
            return None
        if df is None:
            df = tmp_df
        else:
            df = pd.concat([df, tmp_df])
    df = consolidate_stations(df)
    return df

def get_observations(date, station="14"):
    station = f"KNJATCO{station}"
    url = "https://api.weather.com/v2/pws/history/hourly?stationId={station}&format=json&units=m&date={date}&apiKey={key}"
    vals = {'forecast_date': [], 'temp_f_12z': [], 'dew_point_f_12z':[],
            'humidity_12z':[], 'pressure_12z':[], 'pressure_trend_12z':[]
           }
    dt = date.strftime("%Y%m%d")
    api_key = os.getenv("API_KEY")
    url_date = url.format(station=station, date=dt, key=api_key)
    session = get_session()
    try:
        resp = session.get(url_date, verify=False)
        obs = resp.json()['observations']
        session.close()
    except Exception as e:
        session.close()
        return None
    if len(obs)==0 or obs is None:
        return None
    
    dt_12 = datetime(date.year, date.month, date.day, 12,0,0)
    min_sec = None
    min_index = None
    for i, o in enumerate(obs):
        dt = datetime.strptime(o["obsTimeUtc"], "%Y-%m-%dT%H:%M:%SZ")
        seconds_diff = abs((dt - dt_12).total_seconds())
        if min_sec is None:
            min_sec = seconds_diff
            min_index = i
        else:
            if seconds_diff < min_sec:
                min_sec = seconds_diff
                min_index = i
    temp_c = int(obs[min_index]['metric']['tempHigh'])
    temp_f = round(float(temp_c)*(9/5) + 32,1)
    dew_point_c = int(obs[min_index]['metric']['dewptHigh'])
    dew_point_f = round(float(dew_point_c)*(9/5) + 32,1)
    if 'windspeedAvg' in obs[min_index]['metric']:
        wind_speed_avg = obs[min_index]['metric']['windspeedAvg']
    else:
        wind_speed_avg = 0
    vals['forecast_date'].append(date)
    vals['temp_f_12z'].append(temp_f)
    vals['dew_point_f_12z'].append(dew_point_f)
    vals['pressure_12z'].append(obs[min_index]["metric"]["pressureMax"])
    vals['humidity_12z'].append(obs[min_index]["humidityAvg"])
    vals['pressure_trend_12z'].append(obs[min_index]["metric"]["pressureTrend"])
    dfx = pd.DataFrame(vals)
    dfx['forecast_date'] = pd.to_datetime(dfx['forecast_date']).dt.date
    return dfx

def prep_prediction_data(date):
    with open('./artificats/scaler.sav', 'rb') as file:
        scaler = pickle.load(file)
    df_date = get_raw_data(date)
    df_obs = get_observations(date)
    df = df_date.merge(df_obs, on='forecast_date', how='inner')
    df["month"] = pd.to_datetime(df['forecast_date']).dt.month
    df = df.drop(columns=['forecast_date'])
    X = scaler.transform(df)
    if PCA is True:
        with open('./artificats/pca.sav', 'rb') as file:
            pca = pickle.load(file)
        X = pca.transform(X)
    return X 

def save_to_s3(date, prediction, filename):
    # save value as text file on s3 using boto3
    with open(filename, 'w') as file:
        file.write(str(prediction))
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret_key)
    bucket = os.getenv("AWS_BUCKET_NAME")
    key = f"{date.year}/{date.month}/{date.day}/{filename}"
    s3.upload_file(filename, bucket, key)
    os.remove(filename)


def get_prev_day_max_tempf(date):
    host = os.getenv("DB_HOST")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT")
    engine = pg2.connect(f"dbname='{db}' user='{username}' host='{host}' port='{port}' password='{password}'")
    date1 = (date + timedelta(days=-1)).strftime("%Y-%m-%d")
    date2 = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    query = f"""
            SELECT api_datetime, temp_f
            FROM public.weather
            where cast(api_datetime as date) >= '{date1}'
                 and cast(api_datetime as date) <= '{date2}'
            order by api_datetime desc
            """
    df = pd.read_sql(query, con=engine)
    df["date"] = df["api_datetime"].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    engine.close()
    df = df[df['date'].dt.date == date]
    return df.temp_f.max()

def predict(data):
    with open(f'./artificats/{MODEL}.pkl', 'rb') as file:
        model = pickle.load(file)
    return model.predict(data)[0]*FACTOR

def main():
    utc_date = datetime.utcnow().replace(tzinfo=pytz.utc)
    date = utc_date.astimezone(pytz.timezone('US/Eastern')).date()
    prev_day = date + timedelta(days=-1)
    try:
        X = prep_prediction_data(date)
        prediction = predict(X)
        save_to_s3(date, prediction, "prediction.txt")
    except Exception:
        pass
    prev_day_tempf = get_prev_day_max_tempf(prev_day)
    save_to_s3(prev_day, prev_day_tempf, "max_temp.txt")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone='US/Eastern')
    scheduler.add_job(main, 'cron', minute='0', hour='9', day='*', year='*', month='*')
    scheduler.start()
