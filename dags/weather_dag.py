import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import psycopg2
import json


# 1. Define the Python function
def extract_weather_data():
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': 42.5,
        'longitude': 21.2,
        'current_weather': 'true'
    }

    response = requests.get(url, params=params)
    data = response.json()

    with open('/opt/airflow/dags/output/weather.json', 'w') as f:
        json.dump(data, f)

    print("✅ Weather data extracted and saved")


def transform_weather_data():
    input_path = '/opt/airflow/dags/output/weather.json'
    output_path = '/opt/airflow/dags/output/cleaned_weather.csv'

    with open(input_path,'r') as f:
        data = json.load(f)

    current_weather = data.get('current_weather',{})
    
    row = {
        'time': current_weather.get('time', 'N/A'),
        'temperature': current_weather.get('temperature', 'N/A'),
        'windspeed': current_weather.get('windspeed', 'N/A')
    }

    file_exists = False
    try:
        with open(output_path, 'r') as csvfile:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(output_path,'a',newline='') as csvfile:
        fieldnames=['time','temperature','windspeed']
        writer = csv.DictWriter(csvfile,fieldnames = fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
    print("Weather data transformed and saved as CSV successfully!")

def load_weather_data():
    csv_path = '/opt/airflow/dags/output/cleaned_weather.csv'

    with open(csv_path, 'r') as f:
        lines = f.readlines()
        if len(lines) < 2:
            print("No data to load into the database.")
            return
        
        last_line = lines[-1].strip().split(',')
        if last_line[0] == 'time':
            return
        
        time,temperature,windspeed = last_line  

    hook = PostgresHook(postgres_conn_id='postgres_weather')

    insert_query = '''
        CREATE TABLE IF NOT EXISTS weather (
            time TEXT PRIMARY KEY,
            temperature REAL,
            windspeed REAL
        );
        INSERT INTO weather (time, temperature, windspeed)
        VALUES (%s, %s, %s)
        ON CONFLICT (time) DO NOTHING;
    '''
    hook.run(insert_query,parameters=(time,float(temperature), float(windspeed)))
    print("Weather data loaded into the database successfully!")

default_args = {
    'owner': 'edon',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# 3. DAG definition
with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='ETL pipeline to fetch and store weather data',
    start_date=datetime(2024, 6, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    # 4. Define task inside DAG block
    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_data
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable = transform_weather_data
    )
    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather_data
    )

    # 5. Set task order (only one task here)
    extract_task >> transform_task >> load_task
