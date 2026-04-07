# Модуль про ETL/ELT

>E - подключениме к источникам (файлы, БД, JDBC/ODBC, API, FTP)
>T - если по середине, то можно сделать понипуляцию над данными, создать новые поля, проверить кач-во данных перед загрузкой, то есть в RAM какого-то orchestrations tool, иначе - после загрузки используя диалект хранилища (Python, SQL)
>L - загрузка данных (insert into или Bulk load) bulk load - копируем целый файл и сразу записываем в базу.
>backfilling - обработка исторических данных.


Airflow - оркестратор задач, где pipline - это [DAG](https://github.com/Dzhabrail02/DE/blob/main/DE-101/module3/etl_orders.py).
Для запуска etl в Airflow создал модуль [etl_init](https://github.com/Dzhabrail02/DE/blob/main/DE-101/module3/etl_init.py): в нем функции для ETL таблиц в staging, core_layer и data_mart, 
а также функции логирования этапов дага - в таблицу etl_logs записываются: 
- кол-во строк, которые было загружено
- логи в json-формате (полученные функцией-декоратором `json_logs`)
- имя и id дага

![etl_logs](https://github.com/Dzhabrail02/DE/blob/main/DE-101/module3/images/etl_logs%20dbeaver.png?raw=true)

реализиованные ETL-подсистемы:

1. Extracting System
Используя connections в UI AirFlow, чтобы подключаться к базе, далее прописываем имя подлкючения внутри функции для подключения к базе
```python
pg_hook = PostgresHook(postgres_conn_id='db_datalern')
```
2. Profilin System
В таблице etl_logs записывается кол-во добавленных строк, время выполения этапов 
с помощью xcom таски передают друг другу словарь, где key - это имя функции, которая использовалась для etl, а value - время выполнения этапа
```python
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
```
3. CDC 
Решил использовать snapshot, так как в источник данных не прибавиться
4. Surrogate Key Creation System 
```sql
insert into core_layer.customer_dim (cust_id, customer_id, customer_name)
select 100+row_number() over(), customer_id, customer_name 
from (select distinct customer_id, customer_name from staging.orders)
```
5. Errore Event
При ошибке в даге ифнормация об ошибке записывается в таблицу etl_logs

