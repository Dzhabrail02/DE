import pandas as pd
import numpy as np
from airflow import DAG
import datetime
import time
import json as js
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.taskinstance import TaskInstance
from airflow.decorators import task  
from psycopg2.extras import execute_values
from etl_init import (logging_table, 
                      get_count_table, 
                      json_logs, 
                      check_pk, 
                      extracting_data,
                      insert_data)

default_args = {
    'owner':'dzhabrail',
    'depends_on_past':False,
    'start_date': datetime.datetime(2026, 3, 17),
    'scheduling_intervals':None
}

dag_id = 1001

logs_dict = dict()

with DAG(
    dag_id='1001_ETL_orders',
    default_args=default_args,
    description='Загрузка данных из excel в postgres',
    schedule='@daily',
    catchup=False,
    tags=['training', 'basics'],  # Теги для фильтрации в UI
) as dag:
    
    table_name, schema_name = 'orders', 'staging'

    file_name, sheet_name = '/opt/airflow/dags/Sample - Superstore.xlsx', 'Orders'
    

    @task
    def check_pk_column(table_name, schema_name, **context):
        
        try:
            wraper_check = json_logs(check_pk)
            json_log, result = wraper_check(table_name, schema_name)
            pk_column = result

            command_name = context['dag'].dag_id

            ti: TaskInstance = context['ti']
            ti.xcom_push(key='pk_column', value=pk_column)
            ti.xcom_push(key='etl_status', value=json_log)
        
        except Exception as e:
            print(str(e))
            command_name = context['dag'].dag_id
            err_message = str(e)
            status = js.dumps(dict(), ensure_ascii=False)
            logging_table(dag_id=dag_id, command_name=command_name, errore=err_message)

            
   
    @task
    def load_data(schema_name, table_name, **context):
        ti: TaskInstance = context["ti"]
        command_name = context['dag'].dag_id

        try:
            pk_column = ti.xcom_pull(task_ids='check_pk_column', key='pk_column')
            pk_check_dict = ti.xcom_pull(task_ids='check_pk_column', key='etl_status')

            wraper_extract = json_logs(extracting_data)
            data_extract_dict, result = wraper_extract(file_name, sheet_name)
            string_column, data_tuple = result[0], result[1]

            wraper_insert = json_logs(insert_data) 
            insertr_dict, _ = wraper_insert(schema_name, table_name, pk_column, string_column, data_tuple)
            rows_cnt = get_count_table(schema_name, table_name)

            status = {**pk_check_dict, **data_extract_dict, **insertr_dict}

            status = js.dumps(status, ensure_ascii=False)
            logging_table(dag_id=dag_id, command_name=command_name, status=status, rows_cnt=rows_cnt)

        except Exception as e:
            print(str(e))
            err_message = str(e)
            status = js.dumps(dict(), ensure_ascii=False)
            logging_table(dag_id=dag_id, command_name=command_name, errore=err_message)


        
    check = check_pk_column(table_name, schema_name)

    load = load_data(schema_name, table_name)

    check >> load