from airflow import DAG
import datetime
import json as js
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task  

from etl_init import(json_logs, 
                     create_customer_dim, 
                     create_calendar_dim, 
                     create_shipping_dim, 
                     create_geo_dim, 
                     create_product_dim, 
                     create_sales_fact,
                     logging_table,
                     get_count_table)




default_args = {
    'owner':'dzhabrail',
    'depends_on_past':False,
    'start_date': datetime.datetime(2026, 3, 17),
    'scheduling_intervals':None
}

dag_id = 1002

with DAG(
    dag_id='1002_ETL_sales_fact',
    default_args=default_args,
    description='Загрузка данных staging>core>data_mart',
    schedule='@daily',
    catchup=False,
    tags=['training', 'basics'],  # Теги для фильтрации в UI
) as dag:
    

    @task
    def task_create_customer_dim(**context):
        customer_dim = json_logs(create_customer_dim)
        log_customer, _ = customer_dim()
        
        ti: TaskInstance = context['ti']
        ti.xcom_push(key='create_customer_dim', value=log_customer)

    @task
    def task_create_shipping_dim(**context):
        shipping_dim = json_logs(create_shipping_dim)
        log_shipping, _ = shipping_dim()
        
        ti: TaskInstance = context['ti']
        ti.xcom_push(key='create_shipping_dim', value=log_shipping)

    @task
    def task_create_calendar_dim(**context):
        calendar_dim = json_logs(create_calendar_dim)
        log_calendar, _ = calendar_dim()

        ti: TaskInstance = context['ti']
        ti.xcom_push(key='create_calendar_dim', value=log_calendar)
        
    @task
    def task_create_geo_dim(**context):
        geo_dim = json_logs(create_geo_dim)
        log_geo, _ = geo_dim()

        ti: TaskInstance = context['ti']
        ti.xcom_push(key='create_geo_dim', value=log_geo)
    
    @task
    def task_create_product_dim(**context):
        product_dim = json_logs(create_product_dim)
        log_product, _ = product_dim()

        ti: TaskInstance = context['ti']
        ti.xcom_push(key='create_product_dim', value=log_product)
        
    @task
    def task_create_sales_fact(**context):

        ti: TaskInstance = context['ti']
        log_customer = ti.xcom_pull(task_ids='task_create_customer_dim', key='create_customer_dim')
        log_ship = ti.xcom_pull(task_ids='task_create_shipping_dim', key='create_shipping_dim')
        log_calendar = ti.xcom_pull(task_ids='task_create_calendar_dim', key='create_calendar_dim')
        log_geo = ti.xcom_pull(task_ids='task_create_geo_dim', key='create_geo_dim')
        log_product = ti.xcom_pull(task_ids='task_create_product_dim', key='create_product_dim')

        sales_fact = json_logs(create_sales_fact)
        log_sales, _ = sales_fact()

        logs_dict = {**log_customer,**log_ship,**log_calendar,**log_geo,**log_product,**log_sales}
        status = js.dumps(logs_dict, ensure_ascii=False)
        command_name = context['dag'].dag_id
        rows_cnt = get_count_table('data_mart_layer', 'sales_fact')
        logging_table(dag_id=dag_id, command_name=command_name, status=status, rows_cnt=rows_cnt)




    calendar_dim = task_create_calendar_dim()
    customer_dim = task_create_customer_dim()
    product_dim = task_create_product_dim()
    geo_dim = task_create_geo_dim()
    shipping_dim = task_create_shipping_dim()
    sales_fact = task_create_sales_fact()
    
    # Устанавливаем зависимости
    [calendar_dim, customer_dim, product_dim, geo_dim, shipping_dim] >> sales_fact