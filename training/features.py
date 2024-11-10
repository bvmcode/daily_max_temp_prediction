import re
import os
import time
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry
import arrow
import pandas as pd
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')

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
MONTH_RANGE = (202001, 202410)

def get_session():
    session = requests.Session()
    retries = Retry(
            total=10,
            backoff_factor=0.1,
            status_forcelist=[400, 403, 429, 500, 502, 503, 504],
        )
    session.mount(URL_BASE, HTTPAdapter(max_retries=retries))
    return session

def get_dates():
    months = []
    cur_mnth = MONTH_RANGE[0]
    while cur_mnth<=MONTH_RANGE[1]:
        year = str(cur_mnth)[:4]
        month = str(cur_mnth)[-2:]
        months.append({"year": year, "month":month,
                     "last_day":str(arrow.get(int(year),int(month),1).ceil('month').date().day).zfill(2)})
        if month=="12":
            cur_mnth = int(str(int(year)+1)+"01")
        else:
            cur_mnth+=1
    return months
    
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
    

def get_station_data(date, station, session):
    params={
    "region":"nacon",
    "TYPE":r"TEXT%3ALIST",
    "YEAR":date["year"],
    "MONTH":date["month"],
    "FROM":"01"+SOUNDING_HR,
    "TO": date["last_day"]+SOUNDING_HR,
    "STNM": station
    }
    url_params="?region={region}&TYPE={TYPE}&YEAR={YEAR}&MONTH={MONTH}&FROM={FROM}&TO={TO}&STNM={STNM}"
    params = url_params.format(**params)
    url = URL_BASE + params
    resp=session.get(url, verify=False)
    soup = BeautifulSoup(resp.text)
    tables = soup.findAll(name='pre')
    h2s = soup.findAll(name='h2')
    dfs = []
    dates = []
    final_df = None
    sounding_hrs = []
    for h2 in h2s:
        txt = h2.text.split(" ")
        sounding_hrs.append(txt[-4])
        day, month, year = txt[-3:]
        dates.append(datetime.strptime(f"{year}-{month}-{day}", "%Y-%b-%d").date())
    for i, table in enumerate(tables):
        tmp_df = get_dataframe(table.text.split("\n"))
        if tmp_df.shape[0]>0:
            dfs.append(tmp_df)
    if len(dfs)==0:
        df_pressure = consolidate_pressure_levels(None, station, dates[i], sounding_hrs[i])
    for i, df in enumerate(dfs):
        df_pressure = consolidate_pressure_levels(df, station, dates[i], sounding_hrs[i])
        if final_df is None:
            final_df = df_pressure
        else:
            final_df = pd.concat([final_df, df_pressure])
    if final_df is not None:
        final_df = final_df[final_df["sounding_hr"]==SOUNDING_HR]
        final_df = final_df.drop(columns=["sounding_hr"])
    return final_df


def get_training_data():
    dates = get_dates()
    df = None
    session = get_session()
    for month in dates:
        df_pressure = None
        print(month)
        for station in STATIONS:
            tmp_df = get_station_data(month, station, session)
            if df_pressure is None:
                df_pressure = tmp_df
            else:
                df_pressure = pd.concat([df_pressure, tmp_df])
        df_station = consolidate_stations(df_pressure)
        if df is None:
            df = df_station
        else:
            df = pd.concat([df_station, df])
    return df.dropna()
    

def get_observation_data(df):
    key = os.getenv("WEATHER_API_KEY")
    station = "KNJATCO2" #"KNJATCO14"
    url = "https://api.weather.com/v2/pws/history/hourly?stationId={station}&format=json&units=m&date={date}&apiKey={key}"
    vals = {'forecast_date': [], 'temp_f_12z': [], 'dew_point_f_12z':[],
            'humidity_12z':[], 'pressure_12z':[], 'pressure_trend_12z':[]
           }
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
        print(obs[min_index]["obsTimeUtc"])
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
    return pd.DataFrame(vals)


def merge_feature_data(df, df_obs12):
    df_obs12['forecast_date'] = pd.to_datetime(df_obs12['forecast_date']).dt.date
    df['forecast_date'] = pd.to_datetime(df['forecast_date']).dt.date
    df = df.merge(df_obs12, on='forecast_date', how='inner')
    return df


if __name__ == "__main__":
    session =  get_session()
    df=get_training_data()
    df_obs12 = get_observation_data(df)
    df = merge_feature_data(df, df_obs12)
    df.to_csv("features.csv", index=False) 