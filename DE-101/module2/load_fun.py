import psycopg2 
import pandas as pd
import config as c

def insert_to_table_from_Excel (table_name, schema_name, sheet):

    conn = psycopg2.connect(database=c.db_name, 
                            user=c.user_name, 
                            password=c.password, 
                            host=c.db_host)
    cursor = conn.cursor()

    #df from excel_file
    df_xl = pd.read_excel(c.excel_file, engine='openpyxl', sheet_name=sheet)

    # создаем из массива колонок строку, если в названии есть пробел, заменяем на '_'
    columns = ','.join(df_xl.columns).replace(' ', '_').replace('-', '').lower()

    # создаем позиционные параметры для вставки на основе кол-ва колонок df
    values = ','.join(['%s']*len(df_xl.columns))

    # df.values выводит значения как список списков
    data_tuples = [tuple(row) for row in df_xl.values]

    # ищем название колонки с коментарием "Ключ"
    cursor.execute("""
    SELECT
        a.attname AS column_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON c.oid = a.attrelid
    LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum  
    WHERE 
        c.relname = %s  	   -- название таблицы
        AND n.nspname = %s  -- схема (public, если не указана)
        AND a.attnum > 0           -- только пользовательские колонки
        AND NOT a.attisdropped     -- исключить удаленные колонки
        AND d.description = 'Ключ';-- текст комментария
    """, (table_name, schema_name))

    pk = cursor.fetchone()[0]

    insert_query = f""" INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values})
                        ON CONFLICT ({pk}) DO NOTHING
                    """

    cursor.executemany(insert_query, data_tuples)

    conn.commit()
    conn.close()
    print('Данные загружены')


