from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import hashlib
import os
import tempfile
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Integer, Date, Numeric, Boolean, insert)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table, Column, String


# Конфигурация
DATA_DIR = '/opt/airflow/input'
# TMP_TRANSPORT_PATH = '/opt/airflow/input/transport_tmp'
# TMP_EXPENSES_PATH = '/opt/airflow/input/expenses_tmp'
CLIENTS_TABLE = 'clients'
TABLE_PREFIX = 'c_'  # Префикс для клиентских таблиц

# Маппинг колонок
cols_map = {
    'ГосНомер': 'gov_number', 'Гос номер': 'gov_number',
    'Водитель': 'driver', 'ТипТС': 'vehicle_type', 'Тип ТС': 'vehicle_type',
    'ГодВыпуска': 'release_year', 'Год выпуска': 'release_year',
    'Пробег': 'mileage',
    'ДатаПоследнегоРемонта': 'last_repair_date', 'Дата последнего ремонта': 'last_repair_date',
    'Отрасль': 'industry',
    'РасходТоплива': 'fuel_consumption', 'Расход топлива': 'fuel_consumption',
    'Дата': 'date', 'Топливо': 'fuel', 'Ремонт': 'repair', 'Страховка': 'insurance', 'Штрафы': 'fines',
    'Техобслуживание': 'maintenance', 'Шины': 'tire_service', 'Шиномонтаж': 'tire_service',
    'Мойка': 'carwash', 'Автомойка': 'carwash', 'Стоянка': 'parking',
    'НезамерзающиеЖидкости': 'antifreeze_liquid', 'Незамерзающие жидкости': 'antifreeze_liquid',
    'ПлатныеДороги': 'flat_roads', 'Платные дороги': 'flat_roads',
    'ДневноеРасстояние': 'daily_distance', 'Дневное расстояние': 'daily_distance',
    'ЗарплатаРасчетная': 'calculated_salary', 'Зарплата': 'calculated_salary', 'ЕстьПрицеп': 'trailer',
    'Есть прицеп': 'trailer'
}

# Описание ожидаемых колонок для двух таблиц
TRANSPORT_COLS = [
    'gov_number','driver','vehicle_type',
    'release_year','mileage','last_repair_date',
    'industry','fuel_consumption','trailer'
]

EXPENSES_COLS = [
    'date','gov_number','driver','vehicle_type','industry',
    'fuel','repair','insurance','fines','maintenance',
    'tire_service','carwash','parking','antifreeze_liquid',
    'flat_roads','daily_distance','calculated_salary','trailer'
]

def get_engine():
    hook = PostgresHook(postgres_conn_id='postgres_connect')
    return hook.get_sqlalchemy_engine()

def get_raw_conn():
    hook = PostgresHook(postgres_conn_id='postgres_connect')
    return hook.get_conn()

# Предварительное создание public.clients
def init_clients_table():

    engine = get_engine()
    metadata = MetaData(schema='public')
    clients = Table(
        CLIENTS_TABLE, metadata,
        Column('client_id', String(10), primary_key=True),
        Column('client_name', String(255), unique=True)
    )
    metadata.create_all(engine)

def get_client_id_and_schema(client_name, engine):

    # Генерация client_id
    raw = hashlib.sha1(client_name.encode('utf-8')).hexdigest()[:8]
    client_id = f"{TABLE_PREFIX}{raw}"
    schema_name = client_id  # NEW: имя схемы для клиента

    # NEW: создание схемы для клиента, если не существует
    with engine.connect() as conn:
        try:
            conn.execute(CreateSchema(schema_name))
        except ProgrammingError as e:
            if 'already exists' in str(e):
                pass
            else:
                raise

    # Вставляем в public.clients
    metadata = MetaData(schema='public')
    clients = Table(
        CLIENTS_TABLE, metadata,
        Column('client_id', String(10), primary_key=True),
        Column('client_name', String(255), unique=True)
    )
    stmt = pg_insert(clients).values(client_id=client_id, client_name=client_name)
    stmt = stmt.on_conflict_do_nothing(index_elements=['client_name'])
    with engine.begin() as conn:
        conn.execute(stmt)
    return client_id, schema_name


def load_local_file(**context):
    """
        Сканирует входную директорию, определяет для каждого файла его тип (transport или expenses)
        по набору ключевых колонок и пушит пути и client_name через XCom.
    """
    transport_path = None
    expenses_path = None
    client_name = None

    for fname in os.listdir(DATA_DIR):
        if not fname.endswith('.csv'):
            continue
        path = os.path.join(DATA_DIR, fname)
        # читаем только заголовок
        df0 = pd.read_csv(path, nrows=0)
        cols = set(df0.columns)
        # определяем транспортный файл по ключевым столбцам
        if {'Пробег', 'ГосНомер'}.issubset(cols):
            transport_path = path
        # определяем файл расходов
        elif {'Топливо', 'Дата'}.issubset(cols):
            expenses_path = path
        # извлекаем client_name один раз
        if client_name is None and 'Клиент' in cols:
            # читаем первую строку, чтобы получить имя клиента
            with open(path, encoding='utf-8') as f:
                header = f.readline().strip().split(',')
                idx = header.index('Клиент')
                first = f.readline().strip().split(',')
                client_name = first[idx]

    if not transport_path or not expenses_path or not client_name:
        raise ValueError('Не удалось определить пути к файлам transport или expenses или client_name')

    ti = context['ti']
    ti.xcom_push(key='transport_path', value=transport_path)
    ti.xcom_push(key='expenses_path', value=expenses_path)
    ti.xcom_push(key='client_name', value=client_name)


def normalize_and_save_temp_csv(input_path, expected_columns, return_temp_path=False):
    df = pd.read_csv(input_path)

    # Удаляем "Клиент" (если есть)
    if 'Клиент' in df.columns:
        df = df.drop(columns=['Клиент'])

    # Маппинг и приведение типов
    df = df.rename(columns=cols_map)


    for col in df.columns:
        if col in ('date', 'last_repair_date'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif col in ('release_year', 'mileage'):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif col == 'trailer':
            s = df[col].astype(str).str.strip().str.lower()
            df[col] = s.map({'да': True, 'true': True, '1': True, 'нет': False, 'false': False, '0': False}).astype('boolean')
        elif col in ('gov_number', 'driver', 'vehicle_type', 'industry'):
            df[col] = df[col].astype(str).fillna('')
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # сохраняем временный CSV
    # df.to_csv(temp_path, index=False, encoding='utf-8')
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        df.to_csv(tmpfile.name, index=False, encoding='utf-8')
        temp_csv_path = tmpfile.name

    if return_temp_path:
        return temp_csv_path

def bulk_upsert(table_name, temp_csv, pk_cols, update_cols, schema, pg_conn):
    cursor = pg_conn.cursor()

    # df0 = pd.read_csv(csv_path, nrows=0)
    # orig_cols = list(df0.drop(columns=['Клиент']).columns)

    # считываем header
    df0 = pd.read_csv(temp_csv, nrows=0)
    mapped_cols = list(df0.columns)

    cols_ddl = ', '.join(f'{c} TEXT' for c in mapped_cols) # здесь были двойные ковычки

    cursor.execute(
        f"CREATE TEMP TABLE staging_{table_name} ({cols_ddl}) ON COMMIT DROP;"
    )

    cols_list = ','.join(f'{c}' for c in mapped_cols) # здесь были двойные ковычки

    with open(temp_csv, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            f"COPY staging_{table_name} ({cols_list}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')",
            f
        )

    # Формируем SELECT‑выражение для сопоставления колонок
    select_list = []
    for o in mapped_cols:
        # если поле 'date' — приводим к типу date
        if o in ('date', 'last_repair_date'):
            cast = '::date'
        elif o in ('mileage', 'release_year'):
            cast = '::integer'
        elif o == 'trailer':
            cast = '::boolean'
        elif o in ('gov_number', 'driver', 'vehicle_type', 'industry'):
            cast = '::text'
        else:
            cast = '::numeric(10,2)'
        select_list.append(f'"{o}"{cast} AS {o}')
    select_sql = ', '.join(select_list)

    # Условия MERGE
    on_cond = ' AND '.join(f"t.{c} = s.{c}" for c in pk_cols)
    set_list = ', '.join(f"{u} = s.{u}" for u in update_cols)
    all_cols = ', '.join(f'"{c}"' for c in mapped_cols)
    all_src = ', '.join(f's."{c}"' for c in mapped_cols)

    cursor.execute(f"""
        MERGE INTO {schema}.{table_name} AS t
          USING (SELECT {select_sql} FROM staging_{table_name}) AS s
          ON ({on_cond})
        WHEN MATCHED THEN UPDATE SET {set_list}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_src});
    """)
    pg_conn.commit()

def upsert_data(**context):

    engine = get_engine()
    pg_conn = get_raw_conn()

    transport_path = context['ti'].xcom_pull(key='transport_path', task_ids='load_local_file')
    expenses_path = context['ti'].xcom_pull(key='expenses_path', task_ids='load_local_file')
    client_name = context['ti'].xcom_pull(key='client_name', task_ids='load_local_file')

    client_id, schema = get_client_id_and_schema(client_name, engine)

    # Описание таблиц клиента
    client_meta = MetaData(schema=schema)
    transport_table = Table(
        'transport', client_meta,
        Column('gov_number', String(10), primary_key=True),
        Column('driver', String(255), primary_key=True),
        Column('vehicle_type', String(5), primary_key=True),
        Column('release_year', Integer),
        Column('mileage', Integer),
        Column('last_repair_date', Date),
        Column('industry', String(50)),
        Column('fuel_consumption', Numeric(10, 2)),
        Column('trailer', Boolean())
    )
    expenses_table = Table(
        'expenses', client_meta,
        Column('date', Date, primary_key=True),
        Column('gov_number', String(10), primary_key=True),
        Column('driver', String(255), primary_key=True),
        Column('vehicle_type', String(5), primary_key=True),
        Column('industry', String(50)),
        Column('fuel', Numeric(10, 2)),
        Column('repair', Numeric(10, 2)),
        Column('insurance', Numeric(10, 2)),
        Column('fines', Numeric(10, 2)),
        Column('maintenance', Numeric(10, 2)),
        Column('tire_service', Numeric(10, 2)),
        Column('carwash', Numeric(10, 2)),
        Column('parking', Numeric(10, 2)),
        Column('antifreeze_liquid', Numeric(10, 2)),
        Column('flat_roads', Numeric(10, 2)),
        Column('daily_distance', Numeric(10, 2)),
        Column('calculated_salary', Numeric(12, 2)),
        Column('trailer', Boolean())
    )
    client_meta.create_all(engine)

    # normalize_and_save_temp_csv(transport_path, TRANSPORT_COLS, TMP_TRANSPORT_PATH)
    # normalize_and_save_temp_csv(expenses_path, EXPENSES_COLS, TMP_EXPENSES_PATH)

    temp_t_csv_path = normalize_and_save_temp_csv(transport_path, TRANSPORT_COLS, return_temp_path=True)
    temp_e_csv_path = normalize_and_save_temp_csv(expenses_path, EXPENSES_COLS, return_temp_path=True)

    bulk_upsert('transport', temp_t_csv_path, # TMP_TRANSPORT_PATH
                pk_cols=['gov_number', 'driver', 'vehicle_type'],
                update_cols=['release_year', 'mileage', 'last_repair_date', 'industry', 'fuel_consumption',
                             'trailer'],
                schema=schema, pg_conn=pg_conn)

    bulk_upsert('expenses', temp_e_csv_path,  # TMP_EXPENSES_PATH
                pk_cols=['date', 'gov_number', 'driver', 'vehicle_type'],
                update_cols=['industry', 'fuel', 'repair', 'insurance', 'fines', 'maintenance',
                             'tire_service', 'carwash', 'parking', 'antifreeze_liquid',
                             'flat_roads', 'daily_distance', 'calculated_salary', 'trailer'],
                schema=schema, pg_conn=pg_conn)

    os.remove(temp_t_csv_path)
    os.remove(temp_e_csv_path)

# Удаление файлов после обработки
def clean_input():
    for fn in os.listdir(DATA_DIR):
        os.remove(os.path.join(DATA_DIR, fn))
    # for fn in os.listdir(TMP_TRANSPORT_PATH):
    #     os.remove(os.path.join(TMP_TRANSPORT_PATH, fn))
    # for fn in os.listdir(TMP_EXPENSES_PATH):
    #     os.remove(os.path.join(TMP_EXPENSES_PATH, fn))

# DAG определения
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # True
    'email_on_failure': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=25),
}

dag = DAG(
    'etl_crm_erp_pipeline_sqlalchemy_COPY',
    default_args=default_args,
    description='ETL с использованием SQLAlchemy для per-client tables с использованием COPY',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
)

init = PythonOperator(
    task_id='init_clients',
    python_callable=init_clients_table,
    dag=dag
)

# Шаг 1: ожидание файла (если локальный)
wait_for_file = FileSensor(
    task_id='wait_for_file',
    fs_conn_id='fs_default',
    filepath=DATA_DIR,
    poke_interval=20,
    timeout=600,
    dag=dag,
)

# Шаг 2а: чтение файла
load_file = PythonOperator(
    task_id='load_local_file',
    python_callable=load_local_file,
    provide_context=True,
    dag=dag,
)

# Шаг 3: upsert данных
upsert = PythonOperator(
    task_id='upsert_data',
    python_callable=upsert_data,
    provide_context=True,
    dag=dag,
)

cleanup = PythonOperator(
    task_id='clean_input',
    python_callable=clean_input,
    dag=dag
)

# Зависимости
wait_for_file >> init >> load_file >> upsert >> cleanup