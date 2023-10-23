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
import pandas as pd
import re

def special_char(x):
    a = str(x)
    regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
    if regex.search(a) is None:
        return 0
    else:
        return 1

def process_column_data2(data, col):
    TotalRecords = len(data)
    NullCount = data.isna().sum()
    NullPct = 100 * (NullCount / TotalRecords)
    Duplicate_Count = data.duplicated().sum()
    Duplicated_Values = list(data[data.duplicated()][col].unique())
    DuplicatePct = 100 * (Duplicate_Count / TotalRecords)
    UniqueValues = len(data.unique())
    UniquePct = 100 * (UniqueValues / TotalRecords)
    NullStringCount = len(data[data.isin(['NULL', 'Null', 'null', '', ' '])])
    NumberInStringCheck = data.str.contains(r'\d').sum()
    NumberCheck = data.str.isnumeric().sum()
    MaxLen = data.str.len().max()
    MinLen = data.str.len().min()
    SpecialCharCount = data.apply(special_char).sum()
    
    


    return TotalRecords, NullCount, NullPct, Duplicate_Count, DuplicatePct, Duplicated_Values, UniqueValues, UniquePct, NullStringCount, NumberInStringCheck, NumberCheck, MaxLen, MinLen, SpecialCharCount

def process_column_data(data, col):
    TotalRecords = len(data)
    NullCount = data.isna().sum()
    NullPct = 100 * (NullCount / TotalRecords)
    Duplicate_Count = data.duplicated().sum()
    Duplicated_Values = 1
    DuplicatePct = 100 * (Duplicate_Count / TotalRecords)
    UniqueValues = len(data.unique())
    UniquePct = 100 * (UniqueValues / TotalRecords)
    NullStringCount = len(data[data.isin(['NULL', 'Null', 'null', '', ' '])])
    NumberInStringCheck = data.str.contains(r'\d').sum()
    NumberCheck = data.str.isnumeric().sum()
    MaxLen = data.str.len().max()
    MinLen = data.str.len().min()
    SpecialCharCount = data.apply(special_char).sum()
    


    return TotalRecords, NullCount, NullPct, Duplicate_Count, DuplicatePct, Duplicated_Values, UniqueValues, UniquePct, NullStringCount, NumberInStringCheck, NumberCheck, MaxLen, MinLen, SpecialCharCount




def part1_ops(data, tablename):
    results = []
    datelist = ['DATE', 'NGAY', 'THOI GIAN']

    for col in data.columns:
        KDE = str(col)
        data1 = data[col]
        
        Default_Dtype = data1.dtype
        Default_Dtype = 'str' if Default_Dtype.name == 'object' else Default_Dtype.name
        
#         if any(x in col.upper() for x in datelist):
#             try:
#                 if data1.dtype == 'datetime64[ns]':
#                     max_date = data1.max().date()
#                     min_date = data1.min().date()
#                 else:
#                     data1 = pd.to_datetime(data1)
#                     max_date = data1.max().date()
#                     min_date = data1.min().date()
#             except:
#                 max_date = 'Error in date format'
#                 min_date = 'Error in date format'
#             Default_Dtype = 'datetime64[ns]'
#         else:
#             max_date = 'NA'
#             min_date = 'NA'

#         if Default_Dtype in ['int64', 'float64']:
#             max_ = data1.max()
#             min_ = data1.min()
#             median_ = data1.median()
#             mean_ = data1.mean()
#         else:
#             max_ = 'NA'
#             min_ = 'NA'
#             median_ = 'NA'
#             mean_ = 'NA'

        TotalRecords, NullCount, NullPct, Duplicate_Count, DuplicatePct, Duplicated_Values, UniqueValues, UniquePct, NullStringCount, NumberInStringCheck, NumberCheck, MaxLen, MinLen, SpecialCharCount = process_column_data(data1, col)

        result = {
            'Column': KDE,
            'TotalRecords': TotalRecords,
            'NullCount': NullCount,
            'NullPct': NullPct,
            'DuplicateCount': Duplicate_Count,
            'DuplicatePct': DuplicatePct,
            'DuplicatedValues': Duplicated_Values,
            'UniqueValues': UniqueValues,
            'UniquePct': UniquePct,
            'DataType': Default_Dtype,
            'NULLStringCount': NullStringCount,
            'NumberinString': NumberInStringCheck,
            'NumberOnly': NumberCheck,
            'MaxValue': '1',
            'MinValue': '1',
            'MedianValue': '1',
            'MeanValue': '1',
            'MaxLen': MaxLen,
            'MinLen': MinLen,
            'SpecialCharinString': SpecialCharCount
            
        }

        results.append(result)

    result_df = pd.DataFrame(results)
    #result_df.to_csv(f"{tablename}-CN_Results.csv")
    
    #result_df.to_csv(f'{tmp_data_dir}df_astro_dataquality.csv', header=None)

    print("Done")
    return result_df

def get_table_info_and_data():
    import pandas as pd
    from sqlalchemy import create_engine
    db_host = "172.31.240.1"
    db_port = "5432"
    db_name = "airflow"
    db_user = "airflow"
    db_password = "airflow"
    table_name = "df_astro"


    # Tạo URL kết nối đến cơ sở dữ liệu PostgreSQL
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Tạo đối tượng kết nối sử dụng SQLAlchemy
    engine = create_engine(db_url)

    # Sử dụng SQLAlchemy để lấy danh sách tên cột và tên bảng
    table_info_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
    table_info_df = pd.read_sql(table_info_query, engine)

    # Sử dụng SQLAlchemy để lấy dữ liệu từ bảng và lưu vào DataFrame
    data_query = f"SELECT * FROM {table_name}"
    data_df = pd.read_sql(data_query, engine)

    # Đóng kết nối
    engine.dispose()

    # Trả về danh sách tên cột, tên bảng và DataFrame
    #return table_info_df['column_name'].tolist(), table_name
    return data_df, table_name
    
def _extract_weather2():
        
   

    data, table_name = get_table_info_and_data()
    results=part1_ops(data,table_name)
    results.to_csv(f'{tmp_data_dir}df_astro_dataquality.csv', header=None)
    # results.to_csv(f'{tmp_data_dir}df_astro_dataquality.csv', mode='a', sep=';', index=None, header=None)

    #ata_astro_daily.to_csv(f'{tmp_data_dir}data_astro_realtime.csv', header=None)

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
with DAG('weather_dataquality', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
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
    extracting_data_database = PythonOperator(
        task_id='extracting_data_database',
        python_callable=_extract_weather2,
        trigger_rule='all_success'
    )
    
    # TaskGroup for Creating Postgres tables
    with TaskGroup('create_postgresDataquality_tables') as create_postgresDataquality_tables:
        
    # Create table Location
        creating_table_location = PostgresOperator(
            task_id='creating_table_df_astro_dataquality_temp',
            postgres_conn_id='postgres_default',
            sql='''
                
                    CREATE TABLE IF NOT EXISTS df_astro_temp_dataquality (
                        index1 VARCHAR(250) NULL,
                        Column1 VARCHAR(250) NULL,
                        TotalRecords VARCHAR(250) NULL,
                        NullCount VARCHAR(250) NULL,
                        NullPct VARCHAR(250) NULL,
                        DuplicateCount VARCHAR(250) NULL,
                        DuplicatePct VARCHAR(250) NULL,
                        DuplicatedValues VARCHAR(250) NULL,
                        UniqueValues VARCHAR(250) NULL,
                        UniquePct VARCHAR(250) NULL,
                        DataType VARCHAR(250) NULL,
                        NULLStringCount VARCHAR(250) NULL,
                        NumberinString VARCHAR(250) NULL,
                        NumberOnly VARCHAR(250) NULL,
                        MaxValue VARCHAR(250) NULL,
                        MinValue VARCHAR(250) NULL,
                        MedianValue VARCHAR(250) NULL,
                        MeanValue VARCHAR(250) NULL,
                        MaxLen VARCHAR(250) NULL,
                        MinLen VARCHAR(250) NULL,
                        SpecialCharinString VARCHAR(250) NULL
                    );

                   


                '''
        )
        
    #    Create Table Requested Weather df_hour_temp and df_hour_temp
        creating_table_requested_df_hour_weather = PostgresOperator(
            task_id='creating_table_requested_df_hour_weather_realtime',
            postgres_conn_id='postgres_default',
            sql='''
                
                 CREATE TABLE IF NOT EXISTS df_astro_dataquality (
                        index1 VARCHAR(250) NULL,
                        Column1 VARCHAR(250) NULL,
                        TotalRecords VARCHAR(250) NULL,
                        NullCount VARCHAR(250) NULL,
                        NullPct VARCHAR(250) NULL,
                        DuplicateCount VARCHAR(250) NULL,
                        DuplicatePct VARCHAR(250) NULL,
                        DuplicatedValues VARCHAR(250) NULL,
                        UniqueValues VARCHAR(250) NULL,
                        UniquePct VARCHAR(250) NULL,
                        DataType VARCHAR(250) NULL,
                        NULLStringCount VARCHAR(250) NULL,
                        NumberinString VARCHAR(250) NULL,
                        NumberOnly VARCHAR(250) NULL,
                        MaxValue VARCHAR(250) NULL,
                        MinValue VARCHAR(250) NULL,
                        MedianValue VARCHAR(250) NULL,
                        MeanValue VARCHAR(250) NULL,
                        MaxLen VARCHAR(250) NULL,
                        MinLen VARCHAR(250) NULL,
                        SpecialCharinString VARCHAR(250) NULL
                );
                
                
                '''
            )
        
        
    # Truncate Temp Tables    
    with TaskGroup('truncate_temp_table_postgres') as truncate_temp_table_postgres:  
        
        # Truncate location_temp Postgres
        truncate_location_temp_postgres = PostgresOperator(
            task_id='truncate_dataquality_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE df_astro_temp_dataquality;
                '''
        )
        
       
        
        

        
    # TaskGroup for Storing processed data into postgres temp tables
    with TaskGroup('store_processed_temp_data_in_postgres') as store_processed_temp_data_in_postgres:

        store_df_astro_tmp_postgres = PostgresOperator(
            task_id='store_df_astro_tmp_dataquality_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY df_astro_temp_dataquality
                    FROM '{tmp_data_dir}df_astro_dataquality.csv' 
                    DELIMITER ','
                    ;
                '''
        )

       


        
    # TaskGroup for Storing from temp tables to original tables
    with TaskGroup('copy_from_tmp_table_to_original_table') as copy_from_tmp_table_to_original_table:
        
        copy_location_tmp_to_location = PostgresOperator(
            task_id='copy_location_tmp_to_location',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO df_astro_dataquality 
                    SELECT * 
                    FROM df_astro_temp_dataquality
                   
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

    start >> pre_cleanup >> tmp_data >> extracting_data_database >> create_postgresDataquality_tables>> truncate_temp_table_postgres >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> post_cleanup
   










