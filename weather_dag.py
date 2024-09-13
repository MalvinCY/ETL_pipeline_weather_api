# Imports
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd
import pytz

# Function to convert extracted temp data from Kelvin to Celsius
def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius


# Transforming pulled weather data from api
def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
    weather_description = data['weather'][0]['description']
    temp_celsius = kelvin_to_celsius(data['main']['temp'])
    feels_like_celsius = kelvin_to_celsius(data['main']['feels_like'])
    min_temp_celsius = kelvin_to_celsius(data['main']['temp_min'])
    max_temp_celsius = kelvin_to_celsius(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    london_timezone = pytz.timezone('Europe/London')
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']).astimezone(london_timezone)
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']).astimezone(london_timezone)
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']).astimezone(london_timezone)
    
    transformed_data = {'City': city,
                        'Description': weather_description,
                        'Temperature (C)': temp_celsius,
                        'Feels Like (C)': feels_like_celsius,
                        'Minimum Temp (C)': min_temp_celsius,
                        'Maximum Temp (C)': max_temp_celsius,
                        'Pressure': pressure,
                        'Humidity': humidity,
                        'Wind Speed': wind_speed,
                        'Time of Record': time_of_record,
                        'Sunrise (Local Time)': sunrise_time,
                        'Sunset (Local Time)': sunset_time
                        }
    
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    # Access key to access S3 bucket
    aws_credentials = {'key': "xxxxxxxxxxxxxx",
                       'secret': "xxxxxxxxxxxxxx",
                       'token': "xxxxxxxxxxxxxx
                       }
    
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    dt_string = 'current_weather_data_london_' + dt_string
    # Saving csv into S3 bucket
    df_data.to_csv(f's3://malvinweatherapiairflowbucket-yml/{dt_string}.csv', index=False, storage_options=aws_credentials)
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False    
) as dag:
    
    # Checks connection to weather api
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=London&appid=08ea4c6a22eb3d43ffe77bd27b2f101f'
    )
    
    # Gets response for London weather
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=London&appid=08ea4c6a22eb3d43ffe77bd27b2f101f',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    
    # Calls function to transform data
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )
    
    # Dependencies
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data