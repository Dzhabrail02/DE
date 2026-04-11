import json as js
import time
import pandas as pd
import numpy as np
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook


def json_logs(func):

    def log_json (*args, **kwargs):
        in_dict = {}
        time_start = time.perf_counter()

        result = func(*args, **kwargs)

        time_end = time.perf_counter()
        elapsed = time.strftime("%H:%M:%S", time.gmtime(time_end - time_start))

        in_dict [func.__name__] = elapsed
        log_js = in_dict

        return log_js, result
    return log_json


def extracting_data(file_name, sheet_name):
    """
    Создает DF из файла file_name, страницы sheet_name
    """
    df = pd.read_excel(file_name, sheet_name)
        
    # transform data
    condition = ((df['City']=='Burlington') & (df['Postal Code'].isna()))
    df['Postal Code'] = np.where(condition, 5401, df['Postal Code'])
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d')

    string_column = ','.join(df.columns).lower().replace(' ', '_').replace('-', '')
    
    df.to_csv(f'/opt/airflow/dags/files/{sheet_name}.csv', index=False)
    path = f'/opt/airflow/dags/files/{sheet_name}.csv'

    return string_column, path


def read_csv(path: str):
    """
    Чтение csv файла - возвращает список кортежей
    """
    df = pd.read_csv(path)
    data_tuple = [tuple(row) for row in df.values]
    return data_tuple

def insert_data(schema_name, table_name, pk_column, string_column, data_tuple):
    pg_hook = PostgresHook(postgres_conn_id='db_datalern')
    querry = f"""
                INSERT INTO {schema_name}.{table_name} ({string_column}) 
                VALUES %s
                ON CONFLICT ({pk_column}) DO NOTHING
            """

    conn = pg_hook.get_conn()
    execute_values(conn.cursor(), querry, data_tuple)
    conn.commit()
    conn.close()

def check_pk(table_name, schema_name):
    """
    Находит ключевое поля для вставки c ON CONFLICT
    """
    pg_hook = PostgresHook(postgres_conn_id='db_datalern')
    querry = """SELECT
                    a.attname AS column_name
                FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_attribute a ON c.oid = a.attrelid
                    LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum  
                WHERE c.relname = %s  	        -- название таблицы
                    AND n.nspname = %s          -- схема 
                    AND a.attnum > 0            -- только пользовательские колонки
                    AND NOT a.attisdropped      -- исключить удаленные колонки
                    AND d.description = 'Ключ'; -- текст комментария
            """

    get_record = pg_hook.get_records(querry, parameters=[table_name, schema_name])
    pk_column = get_record[0][0]
    return pk_column




def logging_table (dag_id, command_name, status, rows_cnt = 0, errore = ' '):
        """
        Записывает в таблицу логов статус, ошибку, этапы дага 
        """
        pg_hook = PostgresHook(postgres_conn_id='db_datalern')

        try:
            querry_check = "select job_id from etl.logs where job_id = %s"
            get_jobid = pg_hook.get_records(querry_check, parameters=[dag_id])
            id_in_logs = get_jobid[0][0]
        except Exception as e:
            print(str(e))
            id_in_logs = []

        if not id_in_logs:
            querry ='''insert into etl.logs (job_id, command, rows_count, errore, etl_status)
                       values (%s, %s, %s, %s, %s)
                    '''
            pg_hook.run(querry, parameters=[dag_id, command_name, rows_cnt, errore, status])

        else:
            querry = '''update etl.logs
                            set errore = %s,
                                etl_status = %s,
                                rows_count = %s,
                                command = %s
                        where job_id = %s
                        '''
            pg_hook.run(querry, parameters=[errore, status, rows_cnt, command_name, dag_id])

def get_count_table(schema_name, table_name):
        
        pg_hook = PostgresHook(postgres_conn_id='db_datalern')

        querry = f'select count(*) from {schema_name}.{table_name}'
        get_rows = pg_hook.get_records(querry)
        cnt_rows = get_rows[0][0]

        return cnt_rows

"""
Заполнение модели данных
"""
def create_customer_dim():
    hook = PostgresHook(postgres_conn_id='db_datalern')

    sql_querry = """CREATE TABLE IF NOT EXISTS  core_layer.customer_dim
                ( cust_id serial not null,
                    customer_id varchar(8) not null,
                    customer_name varchar(22) not null
                    );"""
    hook.run(sql_querry)
    print('Таблица core_layer.customer_dim создана')

    trunc_querry = "truncate table core_layer.customer_dim"
    hook.run(trunc_querry)
    print('Таблица core_layer.customer_dim очищена')

    insert_querry = """
                    insert into core_layer.customer_dim (cust_id, customer_id, customer_name)
                    select 100+row_number() over(), customer_id, customer_name 
                    from (select distinct customer_id, customer_name from staging.orders)
                    """
    hook.run(insert_querry)
    print('В таблицу core_layer.customer_dim добавлены значения')

def create_shipping_dim():
    hook = PostgresHook(postgres_conn_id='db_datalern')
    sql_querry = """
                    CREATE TABLE IF NOT EXISTS core_layer.shipping_dim
                    (
                    ship_id       serial NOT NULL,
                    shipping_mode varchar(14) NOT NULL,
                    CONSTRAINT PK_shipping_dim PRIMARY KEY ( ship_id )
                    );"""
    hook.run(sql_querry)
    print('Таблица core_layer.shipping_dim создана')

    trunc_querry = "truncate table core_layer.shipping_dim"
    hook.run(trunc_querry)
    print('Таблица core_layer.shipping_dim очищена')

    insert_querry = """
                    insert into core_layer.shipping_dim 
                    select 100+row_number() over(), ship_mode from (select distinct ship_mode from staging.orders ) a;
                    """
    hook.run(insert_querry)
    print('В таблицу core_layer.shipping_dim добавлены значения')

def create_calendar_dim():
    hook = PostgresHook(postgres_conn_id='db_datalern')
    sql_querry = """CREATE TABLE IF NOT EXISTS core_layer.calendar_dim
                    (
                    dateid serial  NOT NULL,
                    year        int NOT NULL,
                    quarter     int NOT NULL,
                    month       int NOT NULL,
                    week        int NOT NULL,
                    date        date NOT NULL,
                    week_day    varchar(20) NOT NULL,
                    leap  varchar(20) NOT NULL,
                    CONSTRAINT PK_calendar_dim PRIMARY KEY ( dateid )
                    );"""
    hook.run(sql_querry)
    print('Таблица core_layer.calendar_dim создана')

    trunc_querry = "truncate table core_layer.calendar_dim"
    hook.run(trunc_querry)
    print('Таблица core_layer.calendar_dim очищена')

    insert_querry = """
                    insert into core_layer.calendar_dim 
                    select 
                    to_char(date,'yyyymmdd')::int as date_id,  
                        extract('year' from date)::int as year,
                        extract('quarter' from date)::int as quarter,
                        extract('month' from date)::int as month,
                        extract('week' from date)::int as week,
                        date::date,
                        to_char(date, 'dy') as week_day,
                        extract('day' from
                                (date + interval '2 month - 1 day')
                                ) = 29
                        as leap
                    from generate_series(date '2000-01-01',
                                        date '2030-01-01',
                                        interval '1 day')
                        as t(date);
                    """
    hook.run(insert_querry)
    print('В таблицу core_layer.calendar_dim добавлены значения')


def create_geo_dim():
    hook = PostgresHook(postgres_conn_id='db_datalern')
    sql_querry = """CREATE TABLE IF NOT EXISTS core_layer.geo_dim
                    (
                    geo_id      serial NOT NULL,
                    country     varchar(13) NOT NULL,
                    city        varchar(17) NOT NULL,
                    state       varchar(20) NOT NULL,
                    postal_code varchar(20) NULL,       --can't be integer, we lost first 0
                    CONSTRAINT PK_geo_dim PRIMARY KEY ( geo_id )
                    );"""
    hook.run(sql_querry)
    print('Таблица core_layer.geo_dim создана')

    trunc_querry = "truncate table core_layer.geo_dim"
    hook.run(trunc_querry)
    print('Таблица core_layer.geo_dim очищена')

    insert_querry = """insert into core_layer.geo_dim 
                        select 100+row_number() over(), country, city, state, postal_code 
                        from (select distinct country, city, state, postal_code from staging.orders ) a;
                    """
    hook.run(insert_querry)
    print('В таблицу core_layer.geo_dim добавлены значения')

def create_product_dim():
    hook = PostgresHook(postgres_conn_id='db_datalern')
    sql_querry = """CREATE TABLE IF NOT EXISTS core_layer.product_dim
                    (
                    prod_id   serial NOT NULL, --we created surrogated key
                    product_id   varchar(50) NOT NULL,  --exist in ORDERS table
                    product_name varchar(127) NOT NULL,
                    category     varchar(15) NOT NULL,
                    sub_category varchar(11) NOT NULL,
                    segment      varchar(11) NOT NULL,
                    CONSTRAINT PK_product_dim PRIMARY KEY ( prod_id )
                    );"""
    hook.run(sql_querry)
    print('Таблица core_layer.product_dim создана')

    trunc_querry = "truncate table core_layer.product_dim"
    hook.run(trunc_querry)
    print('Таблица core_layer.product_dim очищена')

    insert_querry = """insert into core_layer.product_dim 
                        select 100+row_number() over () as prod_id ,product_id, product_name, category, subcategory, segment 
                        from (select distinct product_id, product_name, category, subcategory, segment from staging.orders ) a;
                    """
    hook.run(insert_querry)
    print('В таблицу core_layer.product_dim добавлены значения')

def create_sales_fact():
    hook = PostgresHook(postgres_conn_id='db_datalern')
    sql_querry = """CREATE TABLE IF NOT EXISTS data_mart_layer.sales_fact
                    (
                    sales_id      serial NOT NULL,
                    cust_id integer NOT NULL,
                    order_date_id integer NOT NULL,
                    ship_date_id integer NOT NULL,
                    prod_id  integer NOT NULL,
                    ship_id     integer NOT NULL,
                    geo_id      integer NOT NULL,
                    order_id    varchar(25) NOT NULL,
                    sales       numeric(9,4) NOT NULL,
                    profit      numeric(21,16) NOT NULL,
                    quantity    int4 NOT NULL,
                    discount    numeric(4,2) NOT NULL,
                    CONSTRAINT PK_sales_fact PRIMARY KEY ( sales_id ));"""
    hook.run(sql_querry)
    print('Таблица data_mart_layer.sales_fact создана')

    trunc_querry = "truncate table data_mart_layer.sales_fact"
    hook.run(trunc_querry)
    print('Таблица data_mart_layer.sales_fact очищена')

    insert_querry = """insert into data_mart_layer.sales_fact 
                        select
                            100+row_number() over() as sales_id
                            ,cust_id
                            ,to_char(order_date,'yyyymmdd')::int as  order_date_id
                            ,to_char(ship_date,'yyyymmdd')::int as  ship_date_id
                            ,p.prod_id
                            ,s.ship_id
                            ,geo_id
                            ,o.order_id
                            ,sales
                            ,profit
                            ,quantity
                            ,discount
                        from staging.orders o 
                        inner join core_layer.shipping_dim s on o.ship_mode = s.shipping_mode
                        inner join core_layer.geo_dim g on o.postal_code::numeric = g.postal_code::numeric and g.country=o.country and g.city = o.city and o.state = g.state --City Burlington doesn't have postal code
                        inner join core_layer.product_dim p on o.product_name = p.product_name and o.segment=p.segment and o.subcategory=p.sub_category and o.category=p.category and o.product_id=p.product_id 
                        inner join core_layer.customer_dim cd on cd.customer_id=o.customer_id and cd.customer_name=o.customer_name 
                    """
    hook.run(insert_querry)
    print('В таблицу data_mart_layer.sales_fact добавлены значения')