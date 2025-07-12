from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
import os
import boto3

# Конфигурация S3
AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'sprint6'

# Путь для файлов
LOCAL_DATA_DIR = '/data'

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        endpoint_url = Variable.get('endpoint_s3'),
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )     

@dag(
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False,
    tags=['import_s3']
)
def get_data_from_s3_v2():
    bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']
    prev_task = None
    tasks = []
    
    for filename in bucket_files:
        current_task = PythonOperator(
            task_id=f'fetch_local_{filename.replace(".", "_")}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': BUCKET_NAME, 'key': filename},
        )
        tasks.append(current_task)
        
        if prev_task:
            prev_task >> current_task
        prev_task = current_task
    
    return tasks

# DAG
dag = get_data_from_s3_v2()