# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Importing Python Libraries
from datetime import datetime, timedelta
import time
import json
import os
from pandas import json_normalize
import pandas as pd
from geopy.geocoders import Nominatim
import csv, sqlite3
import glob
import requests
import numpy as np
# Default Arguments and attibutes
default_args ={
    'start_date': datetime.today() - timedelta(days=1),
    'owner': 'DS_HCMUS'
}

# default_args ={
#     'start_date': datetime.today() - timedelta(1),
#     'owner': 'Matheus'
# }

# Get Current date, subtract 5 days and convert to timestamp
todayLessFiveDays =  datetime.today() - timedelta(days=5)
todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())

# Store last 5 days date into a list
days=[]
i = 1
while i < 6:
  todayLessFiveDays =  datetime.today() - timedelta(days=i)
  todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())
  days.append(todayLessFiveDaysTimestamp)
  i += 1

# Get Connection from airflow db
api_connection = BaseHook.get_connection("openweathermapApi")

# Get Variables
latitude = Variable.get("weather_data_lat")
longitude = Variable.get("weather_data_lon")
units = Variable.get("weather_data_units")
tmp_data_dir = Variable.get("weather_data_tmp_directory")
weather_data_spark_code2 = Variable.get("weather_data_spark_code2")

# Suggested Locations

suggested_locations = (
      ['30.318878','-81.690173'],
      ['28.538336','-81.379234'],
      ['27.950575','-82.457176'],
      ['25.761681','-80.191788'],
      ['34.052235','-118.243683'],
      ['40.712776','-74.005974'],
      ['41.878113','-87.629799'],
      ['32.776665','-96.796989'],
      ['47.950356','-124.385490'],
      ['36.169941','-115.139832']
)

# weather data api query params
api_params = {
    'lat':suggested_locations[0][0],
    'lon':suggested_locations[0][1],
    'units':units,
    'dt':int(todayLessFiveDaysTimestamp),
    'appid':api_connection.password,
}

# Notify, Email
def _notify(ti):
    raise ValueError('Api Not Available')

# Tmp Data Check
def _tmp_data():
    # Checking if directories exist
    if not os.path.exists(tmp_data_dir):
        os.mkdir(tmp_data_dir)
    if not os.path.exists(f'{tmp_data_dir}weatherReatime/'):
        os.mkdir(f'{tmp_data_dir}weatherReatime/')
    if not os.path.exists(f'{tmp_data_dir}processedReatime/'):
        os.mkdir(f'{tmp_data_dir}processedReatime/')
    if not os.path.exists(f'{tmp_data_dir}processedReatime/current_weatherReatime/'):
        os.mkdir(f'{tmp_data_dir}processedReatime/current_weatherReatime/')
    if not os.path.exists(f'{tmp_data_dir}processedReatime/hourly_weatherReatime/'):
        os.mkdir(f'{tmp_data_dir}processedReatime/hourly_weatherReatime/')
    
def get_text(x):
    if isinstance(x, dict) and 'text' in x:
        return x['text']
    else:
        return x    
# Extract Weather
def _extract_weather2():
        
    

    api_key = '3fff49a0fde94b6db4540220231610'
    alerts = 'yes'
    aqi = 'yes'
    lang = 'en'
    name = 'ho chi minh'
    url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={name}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}'

    response = requests.get(url)
    response_data = json.loads(response.text)
    df = pd.DataFrame(response_data)

    data_realtime = pd.DataFrame(columns = df.index.values)
    row = np.concatenate((df['location'].dropna().values, df['current'].dropna().values))
    data_realtime.loc[len(data_realtime)] = row
    data_realtime.drop(columns=['air_quality'], inplace=True)
    data_realtime.rename(columns = {'localtime_epoch':'time_epoch','localtime':'time'}, inplace = True)
    data_realtime.drop(columns=['last_updated_epoch', 'last_updated'], inplace = True)
    data_realtime = data_realtime[['name', 'region', 'country', 'lat', 'lon', 'tz_id', 'time_epoch',
        'time', 'temp_c', 'temp_f', 'is_day', 'condition', 'wind_mph',
        'wind_kph', 'wind_degree', 'wind_dir', 'pressure_mb', 'pressure_in',
        'precip_mm', 'precip_in', 'humidity', 'cloud', 'feelslike_c',
        'feelslike_f', 'vis_km', 'vis_miles', 'gust_mph', 'gust_kph', 'uv']]

    
    ## 
    data_loc = pd.DataFrame(response_data['location'], index=[0])

    data_aqi_tmp = pd.DataFrame(df['current']['air_quality'], index=[0])

    data_aqi = pd.concat([data_loc, data_aqi_tmp], axis=1)

    ### reatime data_astro

    url = f'http://api.weatherapi.com/v1/astronomy.json?key={api_key}&q={name}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}'
    response1 = requests.get(url)
    response_data2 = json.loads(response1.text)

    data_astro = pd.DataFrame(response_data2['astronomy']['astro'], index=[0])
    data_astro_daily = pd.concat([data_loc.drop(['localtime_epoch', 'localtime'], axis=1), data_astro], axis=1)
    data_astro_daily.drop(columns=['is_moon_up', 'is_sun_up'], inplace=True)
    from datetime import datetime
    today = datetime.now().date()
    data_astro_daily.index = [today]

    print(data_realtime.head())        
    time = datetime.today().strftime('%Y%m%d%H%M%S%f')
    # with open(f"{tmp_data_dir}/weather/weather2_output_{time}.json", "w") as outfile:
    #     json.dump(data_json, outfile)
    
   
    # data_astro_daily.to_csv(f'{tmp_data_dir}data_astro_realtime.csv', mode='a', sep=',', index=None, header=None)
    # data_realtime.to_csv(f'{tmp_data_dir}data_realtime_hour.csv', mode='a', sep=',', index=None, header=None)

    # df_day.to_csv(f'{tmp_data_dir}df_day.csv')
    data_astro_daily.to_csv(f'{tmp_data_dir}data_astro_realtime.csv', header=None)

    #data_astro_daily.to_csv(f'{tmp_data_dir}data_astro_realtime.csv', mode='a', sep=',', index=None, header=None)
    # data_realtime.to_csv(f'{tmp_data_dir}data_realtime_hour.csv', header=None)









# Store Location Iterative
def _process_df_astro_csv_iterative():
    
    if((latitude == None or latitude == '') and (longitude == None or longitude == '')):    
        for lat,long in suggested_locations:
            _store_df_astro_csv(lat,long)
            # _store_location_csv(latitude,longitude)
    else:
        _store_df_astro_csv(latitude,longitude)

   
# Processing and Deduplicating Weather API Data
def _store_df_astro_csv(lat,long):
    
    # Invoking geo locator api and getting address from latitude and longitude
    geolocator = Nominatim(user_agent="weather_data")
    location = geolocator.reverse(lat+","+long)
    address = location.raw['address']
    # current = datetime.today().strftime('%Y%m%d%H%M%S%f')
    
    # Process location data
    location_df = json_normalize({
        'latitude':lat,
        'logitude': long,
        'city':address.get('city'),
        'state':address.get('state'),
        'postcode':address.get('postcode'),
        'country':address.get('country')
    })
    
    # Store Location
    location_df.to_csv(f'{tmp_data_dir}location.csv', mode='a', sep=',', index=None, header=False)
    # return 0

# Processed files
def get_current_weather_file():
     for i in glob.glob(f'{tmp_data_dir}processedReatime/current_weatherReatime/part-*.csv'):
         return i

def get_hourly_weather_file():
    for i in glob.glob(f'{tmp_data_dir}processedReatime/hourly_weatherReatime/part-*.csv'):
        return i
    
# DAG Skeleton
with DAG('weather_data_realtime', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # Temp Data 
    tmp_data = PythonOperator(
        task_id='tmp_data',
        python_callable=_tmp_data
    )
    
    # Extract User Records Simple Http Operator
    extracting_weather = PythonOperator(
        task_id='extracting_weather',
        python_callable=_extract_weather2,
        trigger_rule='all_success'
    )
    
    # TaskGroup for Creating Postgres tables
    with TaskGroup('create_postgres_tables') as create_postgres_tables:
        
    # Create table Location
        creating_table_location = PostgresOperator(
            task_id='creating_table_df_astro',
            postgres_conn_id='postgres_default',
            sql='''
                
            

                CREATE TABLE IF NOT EXISTS df_astro_temp_realtime (
                    date VARCHAR(100) NULL,
                    name VARCHAR(100) NULL,
                    region VARCHAR(100) NULL,
                    country	VARCHAR(100) NULL,
                    lat	VARCHAR(100) NULL,
                    lon	VARCHAR(100) NULL,
                    tz_id VARCHAR(100) NULL,
                    sunrise VARCHAR(100) NULL,
                    sunset VARCHAR(100) NULL,
                    moonrise VARCHAR(100) NULL,
                    moonset VARCHAR(100) NULL,
                    moon_phase VARCHAR(100)  NULL,
                    moon_illumination VARCHAR(100)  NULL
                );
                '''
        )
        
    #    Create Table Requested Weather df_hour_temp and df_hour_temp
        creating_table_requested_df_hour_weather = PostgresOperator(
            task_id='creating_table_requested_df_hour_weather_realtime',
            postgres_conn_id='postgres_default',
            sql='''
                
                CREATE TABLE IF NOT EXISTS df_hour_temp_reatime (
                    name VARCHAR(100) NULL,
                    region VARCHAR(100) NULL,
                    country	VARCHAR(100) NULL,
                    lat VARCHAR(100) NULL,
                    lon	VARCHAR(100) NULL,
                    tz_id VARCHAR(100) NULL,
                    time_epoch VARCHAR(100) NULL,
                    time VARCHAR(100) NULL,
                    temp_c VARCHAR(100) NULL,
                    temp_f VARCHAR(100) NULL,
                    is_day VARCHAR(100) NULL,
                    condition VARCHAR(100) NULL,
                    wind_mph VARCHAR(100) NULL,
                    wind_kph VARCHAR(100) NULL,
                    wind_degree VARCHAR(100) NULL,
                    wind_dir VARCHAR(100) NULL,
                    pressure_mb VARCHAR(100) NULL,
                    pressure_in VARCHAR(100) NULL,
                    precip_mm VARCHAR(100) NULL,
                    precip_in VARCHAR(100) NULL,
                    humidity VARCHAR(100) NULL,
                    cloud VARCHAR(100) NULL,
                    feelslike_c VARCHAR(100) NULL,
                    feelslike_f VARCHAR(100) NULL,
                    vis_km VARCHAR(100) NULL,
                    vis_miles VARCHAR(100) NULL,
                    gust_mph VARCHAR(100) NULL,
                    gust_kph VARCHAR(100) NULL,
                    uv VARCHAR(100) NULL
                );
                
                
                '''
            )
        
        
    # Truncate Temp Tables    
    with TaskGroup('truncate_temp_table_postgres') as truncate_temp_table_postgres:  
        
        # Truncate location_temp Postgres
        truncate_location_temp_postgres = PostgresOperator(
            task_id='truncate_location_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_hour_temp_reatime;
                '''
        )
        
        # Truncate current_weather_temp Postgres
        truncate_current_weather_temp_postgres = PostgresOperator(
            task_id='truncate_current_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_astro_temp_realtime;
                '''
        )
        
        

        
    # TaskGroup for Storing processed data into postgres temp tables
    with TaskGroup('store_processed_temp_data_in_postgres') as store_processed_temp_data_in_postgres:

        store_df_astro_tmp_postgres = PostgresOperator(
            task_id='store_df_astro_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY df_astro_temp_realtime
                    FROM '{tmp_data_dir}data_astro_realtime.csv' 
                    DELIMITER ','
                    ;
                '''
        )

        # store_df_hour_tmp_postgres = PostgresOperator(
        #     task_id='store_df_hour_tmp_postgres',
        #     postgres_conn_id='postgres_default',
        #     sql=f'''
        #             COPY df_hour_temp_reatime
        #             FROM '{tmp_data_dir}data_realtime_hour.csv' 
        #             DELIMITER ','
        #             ;
        #         '''
        # )


        
    # TaskGroup for Storing from temp tables to original tables
    with TaskGroup('copy_from_tmp_table_to_original_table') as copy_from_tmp_table_to_original_table:
        
        copy_location_tmp_to_location = PostgresOperator(
            task_id='copy_location_tmp_to_location',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO df_astro 
                    SELECT * 
                    FROM df_astro_temp_realtime
                   
                ''' 
        )
        
        # copy_current_weather_tmp_to_current_weather = PostgresOperator(
        #     task_id='copy_current_weather_tmp_to_current_weather',
        #     postgres_conn_id='postgres_default',
        #     sql='''
        #             INSERT INTO df_hour
        #             SELECT * 
        #             FROM df_hour_temp_reatime
                    
        #         ''' 
        # )
        
        
     # TaskGroup for Creating Postgres Views
    with TaskGroup('create_materialized_views') as create_materialized_views:
        # Create View for DataSet 1
        create_view_dataset_1 = PostgresOperator(
            task_id='create_view_dataset_1',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE OR REPLACE VIEW VW_DATASET_1
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                CAST(hw.datetime AS DATE) AS Date,
                EXTRACT(MONTH FROM CAST(hw.datetime AS DATE)) AS Month,
                MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                GROUP BY City,State,Country,Date,Month
                ORDER BY Date DESC;
                '''
        )
        
        # Create View for DataSet 2
        create_view_dataset_2 = PostgresOperator(
            task_id='create_view_dataset_2',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE OR REPLACE VIEW  VW_DATASET_2
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                CAST(hw.datetime AS DATE) AS Date,
                MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature,
                MIN(CAST(hw.temp AS DECIMAL)) AS Min_Temperature,
                ROUND(AVG(CAST(hw.temp AS DECIMAL)),2) AS Average_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                GROUP BY City,State,Country,Date
                ORDER BY Date DESC;
                '''
        )
        
    # Pre Cleanup task    
    pre_cleanup= BashOperator(
        task_id='pre_cleanup',
        bash_command=f'rm -rf {tmp_data_dir}'
    )    
    
    # Post Cleanup task    
    post_cleanup= BashOperator(
        task_id='post_cleanup',
        bash_command=f'rm -r {tmp_data_dir}'
    )
    
    # DAG Dependencies
    # start >> pre_cleanup >> tmp_data >> check_api >> [extracting_weather,api_not_available]
    # extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> process_location_csv >> spark_process_weather 
    # spark_process_weather >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> create_materialized_views >> post_cleanup


    #extracting_weather >> create_postgres_tables
    # 

    # start >> pre_cleanup >> tmp_data >> extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> process_df_astro_csv

    start >> pre_cleanup >> tmp_data >> extracting_weather >> truncate_temp_table_postgres >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> post_cleanup
   










