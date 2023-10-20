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
    if not os.path.exists(f'{tmp_data_dir}weather/'):
        os.mkdir(f'{tmp_data_dir}weather/')
    if not os.path.exists(f'{tmp_data_dir}processed/'):
        os.mkdir(f'{tmp_data_dir}processed/')
    if not os.path.exists(f'{tmp_data_dir}processed/current_weather/'):
        os.mkdir(f'{tmp_data_dir}processed/current_weather/')
    if not os.path.exists(f'{tmp_data_dir}processed/hourly_weather/'):
        os.mkdir(f'{tmp_data_dir}processed/hourly_weather/')
    
def get_text(x):
    if isinstance(x, dict) and 'text' in x:
        return x['text']
    else:
        return x    
# Extract Weather
def _extract_weather2():
        
    api_key = '3fff49a0fde94b6db4540220231610'
    name= 'ho chi minh'
    start_date = '2022-10-17'
    end_date = '2023-10-18'
    alerts = 'yes'
    aqi = 'yes'
    lang = 'en'

    # Define the interval for each request (e.g., 30 days)
    interval = pd.offsets.MonthBegin()

    # Convert the start and end dates to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    data = []  # List to store the data from each request

    # Iterate through the date range in intervals
    while start_date <= end_date:
        # Calculate the end date for the current interval
        current_end_date = min(start_date + interval, end_date)


        # Create the URL for the API request with the current date range
        url = f'http://api.weatherapi.com/v1/history.json?key={api_key}&q={name}&dt={start_date}&end_dt={current_end_date}&alerts={alerts}&aqi={aqi}&lang={lang}'

        # Make the API request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response and append the data to the list
            response_data = json.loads(response.text)
            data.append(response_data)
        else:
            print(f"Request for {start_date} to {current_end_date} failed with status code {response.status_code}")

        # Move to the next interval
        start_date += interval

    # At this point, the 'data' list contains data for the entire one-year period in intervals

    # You can now process and combine the data as needed





    datanew = data
    data_json = response.json()
   
    # data = response.json()

    data_df = pd.DataFrame(datanew)
    data_df['location'][1]

    df_date=pd.DataFrame(data_df['location'][1], index=[0])


        
    t = data_df['forecast']
    df_day = pd.DataFrame()
    df_astro = pd.DataFrame()
    df_hour = pd.DataFrame()
    for i in range (len(t.index)):

        df = pd.DataFrame(t[i]['forecastday'])
        df.drop(df.tail(1).index, inplace=True)

        # create df_day
        temp1 = pd.DataFrame()
        for j in range(len(df.index)):

            day=pd.DataFrame(df['day'][j], index=[df['date'][j]])
            day['condition'] = df['day'][j]['condition']['text']
            temp1 = pd.concat([temp1, day])
        df_day = pd.concat([df_day, temp1])

        # create df_astro
        temp2 = pd.DataFrame()
        for k in range(len(df.index)):
            astro=pd.DataFrame(df['astro'][k], index=[df['date'][k]])
            temp2 = pd.concat([temp2, astro])
        df_astro = pd.concat([df_astro, temp2])

        # create df_hour
        temp3 = pd.DataFrame()
        for l in range(len(df.index)):
            hour=pd.DataFrame(df['hour'][l])
            temp3 = pd.concat([temp3, hour])
        df_hour = pd.concat([df_hour, temp3])
        df_hour['condition'] = df_hour['condition'].apply(get_text)

    df_hour.drop(['will_it_rain', 'will_it_snow', 'chance_of_rain', 'chance_of_snow', 'windchill_c', 'windchill_f', 'heatindex_c', 'heatindex_f', 'dewpoint_c', 'dewpoint_f'], axis=1, inplace=True)

    # add columns
    df_hour['name'] = df_date['name']
    df_hour['name'].fillna(method='ffill', inplace=True)

    df_hour['region'] = df_date['region']
    df_hour['region'].fillna(method='ffill', inplace=True)

    df_hour['country'] = df_date['country']
    df_hour['country'].fillna(method='ffill', inplace=True)

    df_hour['lat'] = df_date['lat']
    df_hour['lat'].fillna(method='ffill', inplace=True)

    df_hour['lon'] = df_date['lon']
    df_hour['lon'].fillna(method='ffill', inplace=True)

    df_hour['tz_id'] = df_date['tz_id']
    df_hour['tz_id'].fillna(method='ffill', inplace=True)

    df_hour = df_hour[['name', 'region', 'country',
       'lat', 'lon', 'tz_id', 'time_epoch', 'time', 'temp_c', 'temp_f', 'is_day', 'condition',
       'wind_mph', 'wind_kph', 'wind_degree', 'wind_dir', 'pressure_mb',
       'pressure_in', 'precip_mm', 'precip_in', 'humidity', 'cloud',
       'feelslike_c', 'feelslike_f', 'vis_km',
       'vis_miles', 'gust_mph', 'gust_kph', 'uv']]
    
    



    df_day=pd.concat([df_date.drop(columns=['localtime_epoch', 'localtime']), df_day], axis=1)
    df_day['name'].fillna(method='ffill', inplace=True)
    df_day['region'].fillna(method='ffill', inplace=True)
    df_day['country'].fillna(method='ffill', inplace=True)
    df_day['lat'].fillna(method='ffill', inplace=True)
    df_day['lon'].fillna(method='ffill', inplace=True)
    df_day['tz_id'].fillna(method='ffill', inplace=True)
    df_day = df_day.drop(0)


    df_astro=pd.concat([df_date.drop(columns=['localtime_epoch', 'localtime']), df_astro], axis=1)
    df_astro['name'].fillna(method='ffill', inplace=True)
    df_astro['region'].fillna(method='ffill', inplace=True)
    df_astro['country'].fillna(method='ffill', inplace=True)
    df_astro['lat'].fillna(method='ffill', inplace=True)
    df_astro['lon'].fillna(method='ffill', inplace=True)
    df_astro['tz_id'].fillna(method='ffill', inplace=True)
    df_astro = df_astro.drop(0)


    
            
    time = datetime.today().strftime('%Y%m%d%H%M%S%f')
    with open(f"{tmp_data_dir}/weather/weather2_output_{time}.json", "w") as outfile:
        json.dump(data_json, outfile)
    
    # df_day.to_csv(f'{tmp_data_dir}df_day.csv', mode='a', sep=';', index=None, header=False)
    # df_astro.to_csv(f'{tmp_data_dir}df_astro.csv', mode='a', sep=',', index=None, header=False)
    df_hour.to_csv(f'{tmp_data_dir}df_hour.csv', mode='a', sep=',', index=None, header=None)

    df_day.to_csv(f'{tmp_data_dir}df_day.csv', header=None)
    df_astro.to_csv(f'{tmp_data_dir}df_astro.csv', header=None)
    # df_hour.to_csv(f'{tmp_data_dir}df_hour.csv')

    #####
    # df_hour.to_csv(f'{tmp_data_dir}df_hour.csv', mode='a', sep=',', index=None, header=False)

    # df_day.to_csv(f'{tmp_data_dir}df_day.csv')
    # df_astro.to_csv(f'{tmp_data_dir}df_astro.csv')
    #####





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
     for i in glob.glob(f'{tmp_data_dir}processed/current_weather/part-*.csv'):
         return i

def get_hourly_weather_file():
    for i in glob.glob(f'{tmp_data_dir}processed/hourly_weather/part-*.csv'):
        return i
    
# DAG Skeleton
with DAG('weather_data_Job2', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # Temp Data 
    tmp_data = PythonOperator(
        task_id='tmp_data',
        python_callable=_tmp_data
    )
    
    # Create Http Sensor Operator
    # check_api = HttpSensor(
    #     task_id='check_api',
    #     http_conn_id='openweathermapApi111',
    #     endpoint=Variable.get("weather_data_endpoint"),
    #     method='GET',
    #     response_check=lambda response: True if response.status_code == 200 or response.status_code == 204 else False,
    #     poke_interval=5,
    #     timeout=60,
    #     retries=2,
    #     mode="reschedule",
    #     soft_fail=False,
    #     request_params = api_params
    # )
    
    # Api is not available
    # api_not_available = PythonOperator(
    #     task_id='api_not_available',
    #     python_callable=_notify,
    #     trigger_rule='one_failed'
    # )
    
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
                
               
                CREATE TABLE IF NOT EXISTS df_astro (
                    date VARCHAR(100) NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    region VARCHAR(100) NOT NULL,
                    country	VARCHAR(100) NOT NULL,
                    lat	VARCHAR(100) NOT NULL,
                    lon	VARCHAR(100) NOT NULL,
                    tz_id VARCHAR(100) NOT NULL,
                    sunrise VARCHAR(100) NOT NULL,
                    sunset VARCHAR(100) NOT NULL,
                    moonrise VARCHAR(100) NOT NULL,
                    moonset VARCHAR(100) NOT NULL,
                    moon_phase VARCHAR(100) NOT NULL,
                    moon_illumination VARCHAR(100) NOT NULL
                );

                CREATE TABLE IF NOT EXISTS df_astro_temp (
                    date VARCHAR(100) NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    region VARCHAR(100) NOT NULL,
                    country	VARCHAR(100) NOT NULL,
                    lat	VARCHAR(100) NOT NULL,
                    lon	VARCHAR(100) NOT NULL,
                    tz_id VARCHAR(100) NOT NULL,
                    sunrise VARCHAR(100) NOT NULL,
                    sunset VARCHAR(100) NOT NULL,
                    moonrise VARCHAR(100) NOT NULL,
                    moonset VARCHAR(100) NOT NULL,
                    moon_phase VARCHAR(100) NOT NULL,
                    moon_illumination VARCHAR(100) NOT NULL
                );
                '''
        )
        
    #    Create Table Requested Weather df_hour_temp and df_hour_temp
        creating_table_requested_df_hour_weather = PostgresOperator(
            task_id='creating_table_requested_df_hour_weather',
            postgres_conn_id='postgres_default',
            sql='''
                
                CREATE TABLE IF NOT EXISTS df_hour_temp (
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
                
                CREATE TABLE IF NOT EXISTS df_hour (
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
                )
                
                '''
            )
        
        # # Create Table df_day and df_day_temp
        creating_table_df_day_weather = PostgresOperator(
            task_id='creating_table_df_day_weather',
            postgres_conn_id='postgres_default',
            sql='''
                
                CREATE TABLE IF NOT EXISTS df_day_temp (
                    date VARCHAR(100) NULL,
                    name VARCHAR(100) NULL,
                    region VARCHAR(100) NULL,
                    country	VARCHAR(100) NULL,
                    lat	VARCHAR(100) NULL,
                    lon	VARCHAR(100) NULL,
                    tz_id VARCHAR(100) NULL,
                    maxtemp_c VARCHAR(100) NULL,
                    maxtemp_f VARCHAR(100) NULL,
                    mintemp_c VARCHAR(100) NULL,
                    mintemp_f VARCHAR(100) NULL,
                    avgtemp_c VARCHAR(100) NULL,
                    avgtemp_f VARCHAR(100) NULL,
                    maxwind_mph VARCHAR(100) NULL,
                    maxwind_kph VARCHAR(100) NULL,
                    totalprecip_mm VARCHAR(100) NULL,
                    totalprecip_in VARCHAR(100) NULL,
                    avgvis_km VARCHAR(100) NULL,
                    avgvis_miles VARCHAR(100) NULL,
                    avghumidity VARCHAR(100) NULL,
                    condition VARCHAR(100) NULL,
                    uv VARCHAR(100) NULL
                );



                CREATE TABLE IF NOT EXISTS df_day (
                    date VARCHAR(100) NULL,
                    name VARCHAR(100) NULL,
                    region VARCHAR(100) NULL,
                    country	VARCHAR(100) NULL,
                    lat	VARCHAR(100) NULL,
                    lon	VARCHAR(100) NULL,
                    tz_id VARCHAR(100) NULL,
                    maxtemp_c VARCHAR(100) NULL,
                    maxtemp_f VARCHAR(100) NULL,
                    mintemp_c VARCHAR(100) NULL,
                    mintemp_f VARCHAR(100) NULL,
                    avgtemp_c VARCHAR(100) NULL,
                    avgtemp_f VARCHAR(100) NULL,
                    maxwind_mph VARCHAR(100) NULL,
                    maxwind_kph VARCHAR(100) NULL,
                    totalprecip_mm VARCHAR(100) NULL,
                    totalprecip_in VARCHAR(100) NULL,
                    avgvis_km VARCHAR(100) NULL,
                    avgvis_miles VARCHAR(100) NULL,
                    avghumidity VARCHAR(100) NULL,
                    condition VARCHAR(100) NULL,
                    uv VARCHAR(100) NULL
                )
                
                '''
            )
        
    # Truncate Temp Tables    
    with TaskGroup('truncate_temp_table_postgres') as truncate_temp_table_postgres:  
        
        # Truncate location_temp Postgres
        truncate_location_temp_postgres = PostgresOperator(
            task_id='truncate_location_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_astro_temp;
                '''
        )
        
        # Truncate current_weather_temp Postgres
        truncate_current_weather_temp_postgres = PostgresOperator(
            task_id='truncate_current_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_hour_temp;
                '''
        )
        
        # Truncate hourly_weather_temp Postgres
        truncate_hourly_weather_temp_postgres = PostgresOperator(
            task_id='truncate_hourly_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_day_temp;
                '''
        )

    # Process Location Data
    # process_df_astro_csv = PythonOperator(
    #     task_id='process_df_astro_csv',
    #     python_callable=_process_df_astro_csv_iterative
    # )
     
    # Spark Submit
    # spark_process_weather = SparkSubmitOperator(
    #     application=f'{weather_data_spark_code2}', task_id="spark_process_weather"
    # )
        
    # TaskGroup for Storing processed data into postgres temp tables
    with TaskGroup('store_processed_temp_data_in_postgres') as store_processed_temp_data_in_postgres:

        store_df_astro_tmp_postgres = PostgresOperator(
            task_id='store_df_astro_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY df_astro_temp
                    FROM '{tmp_data_dir}df_astro.csv' 
                    DELIMITER ','
                    ;
                '''
        )

        store_df_hour_tmp_postgres = PostgresOperator(
            task_id='store_df_hour_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY df_hour_temp
                    FROM '{tmp_data_dir}df_hour.csv' 
                    DELIMITER ','
                    ;
                '''
        )

        store_df_day_tmp_postgres = PostgresOperator(
            task_id='store_df_day_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY df_day_temp
                    FROM '{tmp_data_dir}df_day.csv' 
                    DELIMITER ','
                    ;
                '''
        )
        










        # store_current_weather_tmp_postgres = PostgresOperator(
        #     task_id='store_current_weather_tmp_postgres',
        #     postgres_conn_id='postgres_default',
        #     sql='''
        #             COPY current_weather_tmp
        #             FROM '%s' 
        #             DELIMITER ','
        #             ;
        #         ''' % get_current_weather_file()
        # )
        
        # store_hourly_weather_tmp_postgres = PostgresOperator(
        #     task_id='store_hourly_weather_tmp_postgres',
        #     postgres_conn_id='postgres_default',
        #     sql='''
        #             COPY hourly_weather_tmp
        #             FROM '%s' 
        #             DELIMITER ','
        #             ;
        #         ''' % get_hourly_weather_file()
        # )
        
    # TaskGroup for Storing from temp tables to original tables
    with TaskGroup('copy_from_tmp_table_to_original_table') as copy_from_tmp_table_to_original_table:
        
        copy_location_tmp_to_location = PostgresOperator(
            task_id='copy_location_tmp_to_location',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO df_astro 
                    SELECT * 
                    FROM df_astro_temp
                   
                ''' 
        )
        
        copy_current_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_current_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO df_hour
                    SELECT * 
                    FROM df_hour_temp
                    
                ''' 
        )
        
        copy_hourly_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_hourly_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO df_day 
                    SELECT * 
                    FROM df_day_temp
                    
                ''' 
        )
        
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

    start >> pre_cleanup >> tmp_data >> extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> post_cleanup
   














    # start >> pre_cleanup >> tmp_data


    # start >> pre_cleanup >> tmp_data >> check_api >> [extracting_weather,api_not_available]
    # extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> spark_process_weather 
    # spark_process_weather >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> create_materialized_views >> post_cleanup


    # tmp_data: Clean, tạo đường dẫn chứa file tạm
    # check_api: Check API còn hoạt động hay không

    # _extract_weather: Lấy tọa độ long lat, nếu chưa có thì lấy trong temp lưu trữ
    # api_not_available : Báo ko lấy được, hoặc gửi mail là ko lấy được
    # create_postgres_tables: Tạo các table templocation, location.
    # truncate_temp_table_postgres: Xóa các bảng tạm
    # process_location_csv: Lưu thông tin bảng location
    # spark_process_weather: Thực 
    # store_processed_temp_data_in_postgres: TaskGroup for Storing processed data into postgres temp tables
    # copy_from_tmp_table_to_original_table: copy dữ liệu temp sang bản gốc

    # create_materialized_views : Tạo dữ liệu view đã được
    # post_cleanup: Sau khi clean


