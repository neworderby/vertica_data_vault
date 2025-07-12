from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.operators.python import PythonOperator
import pendulum
import os

# Конфигурация
DWH_SCHEMA = 'STV202506137__DWH'
STAGING_SCHEMA = 'STV202506137__STAGING'
VERTICA_CONN_ID = 'vertica_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 7, 13),
    'retries': 1
}

with DAG(
    'data_vault_vertica_insert_from_stg',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['vertica','data_vault']
) as dag:

    # Загрузка данных HUB
    h_users = VerticaOperator(
        task_id='h_users',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
        select hash(id) as  hk_user_id,
        id as user_id,
        registration_dt,
        now() as load_dt,
        's3' as load_src
        from {STAGING_SCHEMA}.users
        where hash(id) not in (select hk_user_id from {DWH_SCHEMA}.h_users);
        """
    )
    
    h_groups = VerticaOperator(
        task_id='h_groups',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO STV202506137__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
        SELECT
        hash(id) AS hk_group_id,
        id AS group_id,
        registration_dt,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.groups
        WHERE hash(id) NOT IN (SELECT hk_group_id FROM {DWH_SCHEMA}.h_groups);
        """
    )
    
    h_dialogs = VerticaOperator(
        task_id='h_dialogs',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
        SELECT
        hash(message_id) AS hk_message_id,
        message_id AS message_id,
        message_ts AS message_ts,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.dialogs
        WHERE hash(message_id) NOT IN (SELECT hk_message_id FROM {DWH_SCHEMA}.h_dialogs);
        """
    )
    
    # Загрузка данных LINK
    l_admins = VerticaOperator(
        task_id='l_admins',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
        SELECT
        hash(hg.hk_group_id, hu.hk_user_id),
        hg.hk_group_id,
        hu.hk_user_id,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.groups AS g
        LEFT JOIN {DWH_SCHEMA}.h_users AS hu ON g.admin_id = hu.user_id
        LEFT JOIN {DWH_SCHEMA}.h_groups AS hg ON g.id = hg.group_id
        WHERE hash(hg.hk_group_id, hu.hk_user_id) NOT IN (SELECT hk_l_admin_id FROM {DWH_SCHEMA}.l_admins);
        """
    )
 
    l_groups_dialogs = VerticaOperator(
        task_id='l_groups_dialogs',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
        SELECT
        hash(hg.hk_group_id, hd.hk_message_id),
        hd.hk_message_id,
        hg.hk_group_id,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.dialogs AS d
        LEFT JOIN {DWH_SCHEMA}.h_dialogs AS hd ON d.message_id = hd.message_id
        LEFT JOIN {DWH_SCHEMA}.h_groups AS hg ON d.message_group = hg.group_id
        WHERE d.message_group IS NOT NULL
        AND hash(hg.hk_group_id, hd.hk_message_id) NOT IN (SELECT hk_l_groups_dialogs FROM {DWH_SCHEMA}.l_groups_dialogs);
        """
    )
    
    l_user_message = VerticaOperator(
        task_id='l_user_message',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
        SELECT
        hash(hu.hk_user_id, hd.hk_message_id),
        hu.hk_user_id,
        hd.hk_message_id,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.dialogs AS d
        LEFT JOIN {DWH_SCHEMA}.h_dialogs AS hd ON d.message_id = hd.message_id
        LEFT JOIN {DWH_SCHEMA}.h_users AS hu ON d.message_from = hu.user_id
        WHERE hash(hu.hk_user_id, hd.hk_message_id) NOT IN (SELECT hk_l_user_message FROM {DWH_SCHEMA}.l_user_message);
        """
    )
    
    l_user_group_activity = VerticaOperator(
        task_id='l_user_group_activity',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
        SELECT DISTINCT
        hash(hu.hk_user_id, hg.hk_group_id),
        hu.hk_user_id,
        hg.hk_group_id,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.group_log AS gl
        LEFT JOIN {DWH_SCHEMA}.h_users AS hu ON gl.user_id = hu.user_id
        LEFT JOIN {DWH_SCHEMA}.h_groups AS hg ON gl.group_id = hg.group_id
        WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM {DWH_SCHEMA}.l_user_group_activity);
        """
    )
    
    # Загрузка данных SAT
    s_admins = VerticaOperator(
        task_id='s_admins',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
        SELECT 
        la.hk_l_admin_id,
        True AS is_admin,
        hg.registration_dt,
        now() AS load_dt,
        's3' AS load_src
        FROM {DWH_SCHEMA}.l_admins AS la
        LEFT JOIN {DWH_SCHEMA}.h_groups AS hg ON la.hk_group_id = hg.hk_group_id;
        """
    )
    
    s_group_name = VerticaOperator(
        task_id='s_group_name',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_group_name(hk_group_id, group_name, load_dt, load_src)
        SELECT 
        hg.hk_group_id,
        g.group_name,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.groups AS g
        JOIN {DWH_SCHEMA}.h_groups AS hg ON g.id = hg.group_id;
        """
    )
    
    s_group_private_status = VerticaOperator(
        task_id='s_group_private_status',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
        SELECT 
        hg.hk_group_id,
        g.is_private,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.groups AS g
        JOIN {DWH_SCHEMA}.h_groups AS hg ON g.id = hg.group_id;
        """
    )
    
    s_dialog_info = VerticaOperator(
        task_id='s_dialog_info',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
        SELECT 
        hd.hk_message_id,
        d.message,
        d.message_from,
        d.message_to,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.dialogs AS d
        JOIN {DWH_SCHEMA}.h_dialogs AS hd ON d.message_id = hd.message_id;
        """
    )
    
    s_user_socdem = VerticaOperator(
        task_id='s_user_socdem',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
        SELECT 
        hu.hk_user_id,
        u.country,
        u.age,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.users AS u
        JOIN {DWH_SCHEMA}.h_users AS hu ON u.id = hu.user_id;
        """
    )
    
    s_user_chatinfo = VerticaOperator(
        task_id='s_user_chatinfo',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO {DWH_SCHEMA}.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
        SELECT 
        hu.hk_user_id,
        u.chat_name,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.users AS u
        JOIN {DWH_SCHEMA}.h_users AS hu ON u.id = hu.user_id;
        """
    )
    
    s_auth_history = VerticaOperator(
        task_id='s_auth_history',
        vertica_conn_id=VERTICA_CONN_ID,
        sql=f"""
        INSERT INTO STV202506137__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
        SELECT DISTINCT
        lug.hk_l_user_group_activity AS hk_l_user_group_activity,
        gl.user_id_from AS user_id_from,
        gl.event AS event,
        gl.event_datetime AS event_dt,
        now() AS load_dt,
        's3' AS load_src
        FROM {STAGING_SCHEMA}.group_log AS gl
        LEFT JOIN {DWH_SCHEMA}.h_users AS hu ON gl.user_id = hu.user_id
        LEFT JOIN {DWH_SCHEMA}.h_groups AS hg ON gl.group_id = hg.group_id
        LEFT JOIN {DWH_SCHEMA}.l_user_group_activity AS lug ON (hg.hk_group_id = lug.hk_group_id) AND (hu.hk_user_id = lug.hk_user_id)
        WHERE lug.hk_l_user_group_activity NOT IN (SELECT hk_l_user_group_activity FROM {DWH_SCHEMA}.s_auth_history);
        """
    )                                                                  

# Пайплайн
(h_users >> h_groups >> h_dialogs >> #хабы
l_admins >> l_groups_dialogs >> l_user_message >> l_user_group_activity >> #линки
s_admins >> s_group_name >> s_group_private_status >> s_dialog_info >> #сателлиты
s_user_socdem >> s_user_chatinfo >> s_auth_history) #сателлиты