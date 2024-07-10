import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from typing import List
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import vertica_python
import contextlib

# Функция для извлечения данных из Postgres и записи их в CSV файл
def extract_data_to_csv(ds, table_name:str, date_column:str, **kwargs):
    # Создаем подключение к Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # дата старта
    start_date = datetime.strptime(ds, '%Y-%m-%d')
    # Выполняем SQL запрос
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            sql = f"SELECT * FROM public.{table_name} WHERE {date_column}::date = '{start_date.date()}'"
            cursor.execute(sql, (ds,))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=column_names)
    
    try:
        df.to_csv(f'/data/{table_name}/extracted_data_{ds}.csv', index=False)
        print('файл записан')
    except:
        print('файл не был записан') 


def load_data_to_vertica(ds, table_name:str, schema:List[str], **kwargs):

    conn = BaseHook.get_connection('vertica_conn')

    vertica_conn_info = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema,
        'ssl': False
    }

    vertica_connection = vertica_python.connect(**vertica_conn_info)

    dataset_path = f'/data/{table_name}/extracted_data_{ds}.csv'
    df = pd.read_csv(dataset_path)
    num_rows = len(df)
    columns = ', '.join(schema)
    copy_expr = f"""
    COPY STV2024021918__STAGING.{table_name} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_connection.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_connection.commit()
            print("loaded")
            start += chunk_size + 1

        
    vertica_connection.close()

def delete_csv():
    pass

def cdm_load(script_path):
    
    conn = BaseHook.get_connection('vertica_conn')

    vertica_conn_info = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema,
        'ssl': False
    }

    # Считываем содержимое SQL скрипта
    with open(script_path, 'r') as f:
        sql_script = f.read()
    #print(sql_script)

    # Подключаемся к базе данных Vertica
    with vertica_python.connect(**vertica_conn_info) as conn:
        # Создаем курсор для выполнения запросов
        with conn.cursor() as cur:
            try:
                sql_commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
                for sql_command in sql_commands:
                    cur.execute(sql_command)
                conn.commit()
                print("SQL скрипт успешно выполнен.")
            except Exception as e:
                print(f"Ошибка при выполнении SQL скрипта: {e}")
                conn.rollback() 

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'final_project_pipeline',
    default_args=default_args,
    description='full_pipeline_for_transaction_data',
    schedule_interval='@daily',
)


# Определение задачи
extract_from_currencies = PythonOperator(
    task_id='pg_to_csv_currencies',
    python_callable=extract_data_to_csv,
    op_kwargs={'ds': '{{ ds }}',
               'table_name' : 'currencies', 
               'date_column' : 'date_update'},
    dag=dag,
)

# Определение задачи
extract_from_transactions = PythonOperator(
    task_id='pg_to_csv_transactions',
    python_callable=extract_data_to_csv,
    op_kwargs={'ds': '{{ ds }}',
               'table_name' : 'transactions', 
               'date_column' : 'transaction_dt'},
    dag=dag,
)

# Определение задачи
csv_to_vertica_cur = PythonOperator(
    task_id='csv_to_vertica_cur',
    python_callable=load_data_to_vertica,
    op_kwargs={'ds': '{{ ds }}',
               'table_name': 'currencies',
               'schema':['date_update','currency_code','currency_code_with','currency_with_div']
               },
    dag=dag,
)

# Определение задачи
csv_to_vertica_tran = PythonOperator(
    task_id='csv_to_vertica_tran',
    python_callable=load_data_to_vertica,
    op_kwargs={'ds': '{{ ds }}',
               'table_name': 'transactions',
               'schema':['operation_id','account_number_from','account_number_to','currency_code', 
                         'country','status', 'transaction_type','amount','transaction_dt']
               },
    dag=dag,
)

delete_file = PythonOperator(
    task_id='delete_csv',
    python_callable=delete_csv,
    op_kwargs={'ds': '{{ ds }}'},
    dag=dag,
)


load_to_cdm = PythonOperator(
    task_id='load_to_cdm',
    python_callable=cdm_load,
    op_kwargs={'ds': '{{ ds }}',
               'script_path': '/data/sql/cdm_load.sql'},
    dag=dag,
)
# Определение последовательности выполнения задач
(
    [extract_from_currencies, extract_from_transactions] 
    >> csv_to_vertica_cur 
    >> csv_to_vertica_tran
    >> delete_file 
    >> load_to_cdm
)
