from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from pendulum import datetime
import os
import json
import requests


# latitude and longitude for the location you want to get weather data for
LATITUDE = 40.7128  # Example: New York City latitude
LONGITUDE = -74.0060  # Example: New York City longitude
POSTGRES_CONN_ID = 'postgres_default'  
API_CONN_ID = 'openweathermap_api'  # Airflow connection ID for OpenWeatherMap API

YOUR_API_KEY = os.getenv('YOUR_API_KEY')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, tz='UTC')
}

# Define the DAG
with DAG(
    'etl_weather',
    default_args=default_args,
    description='A simple ETL DAG to fetch weather data and store it in Postgres',
    schedule='@daily',
    catchup=False
) as dag:

    @task
    def extract_weather_data():
        # extract weather data from OpenWeatherMap API
        # 1. Initialize the hook
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        
        # 2. Separate the base endpoint path from the query parameters
        endpoint = "data/2.5/weather"
        data = {
            "lat": LATITUDE,
            "lon": LONGITUDE,
            "units": "metric"
            # The 'appid' API key should be stored inside the Connection's "Extra" or "Password" field
        }
        
        # 3. Run the hook (returns a string)
        response_text = http_hook.run(endpoint, data=data).text
        
        # 4. Parse and return the JSON
        return json.loads(response_text)

    @task
    def transform_weather_data(weather_data):
        transformed_data = {
            'temperature': weather_data['main']['temp'],
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'weather_description': weather_data['weather'][0]['description'],
            'wind_speed': weather_data['wind']['speed'],
            'timestamp': weather_data['dt']
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        insert_query = """
            INSERT INTO weather (temperature, humidity, pressure, weather_description, wind_speed, timestamp)
            VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))
        """
        pg_hook.run(insert_query, parameters=(
            transformed_data['temperature'],
            transformed_data['humidity'],
            transformed_data['pressure'],
            transformed_data['weather_description'],
            transformed_data['wind_speed'],
            transformed_data['timestamp']
        ))

    # Define the task dependencies
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)