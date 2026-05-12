
from airflow.sdk import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

LATITUDE = '19.0023'
LONGITUDE = '73.1144'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

@dag(
    schedule='@hourly',
    start_date=datetime(2026, 2, 26),
    max_active_runs=1,
    catchup=False,
    tags=['weather']
)
def etl_weather():
    
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extract_weather(logical_date=None): # Use logical_date from context
    # Format the date for the API (YYYY-MM-DD)
        formatted_date = logical_date.strftime('%Y-%m-%d')
        """Extract weather data from the Open meteo API using Airflow conn"""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        endpoint = f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&start_date={formatted_date}&end_date={formatted_date}&hourly=temperature_2m,wind_speed_10m"
        
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        raise Exception(f'Failed to fetch data: {response.status_code}')

    @task()
    def transform_weather(weather_data, **context):
        """Transform extracted historical hourly data"""
        # Get the hour we are currently processing (0-23)
        execution_hour = context['logical_date'].hour
        
        # Access the 'hourly' key instead of 'current_weather'
        hourly_data = weather_data['hourly']
        
        # Use the execution_hour as the index to pull the specific data point
        transformed_data = {
            'latitude': weather_data['latitude'],
            'longitude': weather_data['longitude'],
            'temperature': hourly_data['temperature_2m'][execution_hour],
            'windspeed': hourly_data['wind_speed_10m'][execution_hour],
            # If your API call doesn't include direction/code for hourly, set defaults or add them to the URL
            'winddirection': 0, 
            'weathercode': 0
        }
        return transformed_data

    @task()
    def load_weather(transformed_data, **context):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Explicitly map the values using dictionary keys
        insert_sql = """
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, timestamp)
        VALUES (%(latitude)s, %(longitude)s, %(temperature)s, %(windspeed)s, %(winddirection)s, %(weathercode)s, %(timestamp)s)
        ON CONFLICT (timestamp) DO NOTHING;
        """
        
        # Add the timestamp to the dictionary
        transformed_data['timestamp'] = context['logical_date']
        
        # Pass the dictionary directly as parameters
        pg_hook.run(insert_sql, parameters=transformed_data)

    # --- ORCHESTRATION: These must be outside the task functions ---
    weather_data = extract_weather()
    transformed_data = transform_weather(weather_data)
    load_weather(transformed_data)

# Call the DAG function
etl_weather()