from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from docker.types import Mount
from datetime import datetime, timedelta,date
from sqlalchemy import create_engine
import pandas as pd
import csv
import requests
import json
import boto3

def fetch_api_data(ti):
    dag_var_weather = Variable.get("var_weather",deserialize_json=True)
    city_list = dag_var_weather["path_city"]
    api_url = dag_var_weather["api_url"]
    api_key = dag_var_weather["api_key"]
    path_temp = dag_var_weather["path_temp"]
    df = pd.DataFrame(columns=['City', 'Description', 'Temperature (F)', 'Feels Like (F)', 'Minimun Temp (F)', 'Maximum Temp (F)', 'Pressure', 'Humidty', 'Wind Speed', 'Time of Record', 'Sunrise (Local Time)', 'Sunset (Local Time)'])
    #read bulk list of cities
    with open(city_list, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row if present
        # Loop over each row in the CSV
        for row in reader:
            city = row[0]
            url = f"{api_url}{api_key}&q={city}"
            response = requests.get(url)
            data = response.json()

            city = data["name"]
            weather_description = data["weather"][0]['description']
            temperature = data["main"]["temp"]
            feels_like= data["main"]["feels_like"]
            min_temperature = data["main"]["temp_min"]
            max_temperature = data["main"]["temp_max"]
            pressure = data["main"]["pressure"]
            humidity = data["main"]["humidity"]
            wind_speed = data["wind"]["speed"]
            time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
            sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
            sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

            transformed_data = {"City": city,
                                "Description": weather_description,
                                "Temperature (F)": temperature,
                                "Feels Like (F)": feels_like,
                                "Minimun Temp (F)":min_temperature,
                                "Maximum Temp (F)": max_temperature,
                                "Pressure": pressure,
                                "Humidty": humidity,
                                "Wind Speed": wind_speed,
                                "Time of Record": time_of_record,
                                "Sunrise (Local Time)":sunrise_time,
                                "Sunset (Local Time)": sunset_time                        
                                }

            transformed_data_list = [transformed_data]
            df_data = pd.DataFrame(transformed_data_list)
            df = df.append(df_data, ignore_index=True)
            #ti.xcom_push(key='api_data', value=data)

    csv_filename = path_temp + '/temporary.csv'
    df.to_csv(csv_filename, index=False)


def load_to_s3(ti):
    dag_var_weather = Variable.get("var_weather",deserialize_json=True)
    bucket_name  = dag_var_weather["bucket_name"]
    aws_access_key_id = dag_var_weather["aws_access_key_id"]
    aws_secret_access_key  = dag_var_weather["aws_secret_access_key"]
    path_temp = dag_var_weather["path_temp"]
    csv_file = path_temp + '/temporary.csv'

    df = pd.read_csv(csv_file)
    csv_data = df.to_csv(index=False)

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"weather_api_{current_datetime}.csv"
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=key)


with DAG("DAG_weatherAPI", start_date=datetime(2023, 7, 15),
    schedule_interval=timedelta(days=1), catchup=False) as dag:

    check_api_task = HttpSensor(
        task_id ='check_api_task',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Jakarta&APPID=your_key'
    )
    
    fetch_api_data_task = PythonOperator(
        task_id='fetch_api_data_task',
        python_callable=fetch_api_data,
        provide_context=True
    )

    load_s3_task = PythonOperator(
        task_id='load_s3_task',
        python_callable=load_to_s3,
        provide_context=True
    )

    check_api_task >> fetch_api_data_task >> load_s3_task
