from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.operators.python import PythonOperator
import pendulum
import os

# Конфигурация
LOCAL_DATA_DIR = '/data'
STAGING_SCHEMA = 'STV202506137__STAGING'
VERTICA_CONN_ID = 'vertica_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 7, 13),
    'retries': 1
}

def get_copy_sql(table_name: str):
    """Генерирует SQL команду COPY"""
    file_path = os.path.join(LOCAL_DATA_DIR, f'{table_name}.csv')
    return f"""
    COPY {STAGING_SCHEMA}.{table_name} 
    FROM LOCAL '{file_path}'
    DELIMITER ','
    ENCLOSED BY '"'
    ESCAPE AS '"'
    DIRECT
    SKIP 1
    ABORT ON ERROR;
    """

with DAG(
    'vertica_load_stg',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['vertica','stage']
) as dag:

    # Загрузка данных
    load_users = VerticaOperator(
        task_id='load_users',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=get_copy_sql('users'),
    )

    load_groups = VerticaOperator(
        task_id='load_groups',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=get_copy_sql('groups'),
    )

    load_dialogs = VerticaOperator(
        task_id='load_dialogs',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=get_copy_sql('dialogs'),
    )
    
    load_group_log = VerticaOperator(
        task_id='load_group_log',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=get_copy_sql('group_log'),
    )    

    # Пайплайн
    load_users >> load_groups >> load_dialogs >> load_group_log