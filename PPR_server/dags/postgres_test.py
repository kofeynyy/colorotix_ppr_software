from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def test_pg_hook(**context):
    hook = PostgresHook(postgres_conn_id='client_db_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    print(f"Postgres version: {version}")
    cur.close()
    conn.close()

# DAG определения
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'test_postgres',
    default_args=default_args,
    description='Тестирование успешности коннекта с PostgreSQL',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
)

test_hook = PythonOperator(
    task_id='test_pg_hook',
    python_callable=test_pg_hook,
    provide_context=True,
    dag=dag,
)

test_hook