# ETL With Airflow Load from WeatherAPI and Load to AWS S3
![alt text](https://raw.githubusercontent.com/muhk01/airflow_weather_api_etl/main/images/1.PNG)

Workflow for this project is like below, first check availability of the API, then fetch for each city defined in **city.csv** after that load result of daily data into aws S3 Bucket

## How to run?
simply by firing up docker compose
```
docker compose up -d
```

## Configure variables
![alt text](https://raw.githubusercontent.com/muhk01/airflow_weather_api_etl/main/images/3.PNG)

configure variables like above, such for aws key, id and other neccessary variables.

## Configure connection to HTTP API
![alt text](https://raw.githubusercontent.com/muhk01/airflow_weather_api_etl/main/images/2.PNG)

configure API test before running the flow

## Loaded Data Into AWS S3
![alt text](https://raw.githubusercontent.com/muhk01/airflow_weather_api_etl/main/images/4.PNG)

Result after data written into Bucket S3
