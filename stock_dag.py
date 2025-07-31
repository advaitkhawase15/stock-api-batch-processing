from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import json
import pandas as pd
import requests
from airflow.operators.bash_operator import BashOperator

# Load JSON config file
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

def extract_stock_news_data(**kwargs):
    url = kwargs['url']
    querystring = kwargs['querystring']
    headers = kwargs['headers']
    dt_string = kwargs['date_string']
    # return headers
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()
    response_data = response_data["data"]["news"]
    response_data_csv = pd.DataFrame(response_data)
    output_file_path = f"/home/ubuntu/stock_response_data_{dt_string}.csv"
    response_data_csv.to_csv(output_file_path, index=False)
    return output_file_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG('stock_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

        extract_stock_news_data = PythonOperator(
        task_id ='Extract_stock_news_data',
        python_callable=extract_stock_news_data,
        op_kwargs={
            'url': "https://real-time-finance-data.p.rapidapi.com/stock-news",
            'querystring': {
                "symbol":"AAPL:NASDAQ",
                "language":"en"
            },
            'headers': api_host_key,
            'date_string':dt_now_string
        })

        load_to_s3 = BashOperator(
            task_id = 'load_to_s3',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("Extract_stock_news_data")}} s3://stocknewsbucket/'
        )
        
        extract_stock_news_data >> load_to_s3