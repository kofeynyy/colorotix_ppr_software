from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import hashlib
import os
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Integer, Date, Numeric, insert)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError

# Конфигурация
DATA_DIR = '/opt/airflow/input'
CLIENTS_TABLE = 'clients'
TABLE_PREFIX = 'c_'  # Префикс для клиентских таблиц


def get_engine():
    hook = PostgresHook(postgres_conn_id='postgres_connect')
    return hook.get_sqlalchemy_engine()

# Получение или создание client_id и запись в общую таблицу клиентов
metadata = MetaData()
clients = Table(
    CLIENTS_TABLE, metadata,
    Column('client_id', String(10), primary_key=True),
    Column('client_name', String(255), unique=True)
)

def get_or_create_client_id(client_name):
    engine = get_engine()
    metadata.create_all(engine, tables=[clients])

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

    # Вставка или получение client_id из public.clients
    with engine.begin() as conn:
        stmt = pg_insert(clients).values(client_id=client_id, client_name=client_name)
        stmt = stmt.on_conflict_do_nothing(index_elements=['client_name'])
        conn.execute(stmt)
        sel = clients.select().with_only_columns([clients.c.client_id]) \
            .where(clients.c.client_name == client_name)
        return conn.execute(sel).scalar()

def load_local_file(**context):
    transport_dfs = []
    expenses_dfs = []

    for f in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, f)
        if f.endswith('.csv'):
            df = pd.read_csv(path)
        elif f.endswith('.xlsx'):
            df = pd.read_excel(path)
        else:
            continue

        # Определяем тип файла по наличию колонок
        if 'Пробег' in df.columns:
            transport_dfs.append(df)
        elif 'Дата' in df.columns:
            expenses_dfs.append(df)

    if transport_dfs:
        tdf = pd.concat(transport_dfs, ignore_index=True)
        context['ti'].xcom_push(key='transport_df', value=tdf.to_dict('records'))
    if expenses_dfs:
        edf = pd.concat(expenses_dfs, ignore_index=True)
        context['ti'].xcom_push(key='expenses_df', value=edf.to_dict('records'))


# Функция создания таблиц transport и expenses и UPSERT
def upsert_data(**context):

    transport_data = context['ti'].xcom_pull(key='transport_df', task_ids='load_local_file') or []
    expenses_data = context['ti'].xcom_pull(key='expenses_df', task_ids='load_local_file') or []

    if not transport_data and not expenses_data:
        raise ValueError("Нет данных для загрузки")

    engine = get_engine()
    metadata = MetaData()

    for rec in transport_data:
        client_name = rec.get('Клиент')
        if not client_name:
            raise ValueError("Пустой клиент в транспорте")
        client_id = get_or_create_client_id(client_name)
        schema_name = client_id  # NEW: схема клиента
        # table_name = f"{client_id}_transport"

        # NEW: описание таблицы transport в схеме client_id
        transport_meta = MetaData(schema=schema_name)

        # Описание таблицы транспорта
        transport = Table(
            'transport', transport_meta,
            Column('gov_number', String(9), primary_key=True),
            Column('driver', String(255), primary_key=True),
            Column('vehicle_type', String(2), primary_key=True),
            Column('client', String(255), primary_key=True),
            Column('release_year', Integer),
            Column('mileage', Integer),
            Column('last_repair_date', Date),
            Column('industry', String(255)),
            Column('fuel_consumption', Numeric(6, 2)),
            extend_existing=True
        )
        transport_meta.create_all(engine)

        # UPSERT записи
        ins = pg_insert(transport).values(
            gov_number=rec.get('ГосНомер') or rec.get('Гос номер'),
            driver=rec.get('Водитель'),
            vehicle_type=rec.get('ТипТС') or rec.get('Тип ТС'),
            client=client_name,
            release_year=rec.get('ГодВыпуска') or rec.get('Год выпуска'),
            mileage=rec.get('Пробег'),
            last_repair_date=rec.get('ДатаПоследнегоРемонта') or rec.get('Дата последнего ремонта'),
            industry=rec.get('Отрасль'),
            fuel_consumption=rec.get('РасходТоплива') or rec.get('Расход топлива')
        )
        upsert_stmt = ins.on_conflict_do_update(
            index_elements=['gov_number', 'driver', 'vehicle_type', 'client'],
            set_={
                'mileage': ins.excluded.mileage,
                'last_repair_date': ins.excluded.last_repair_date,
                'fuel_consumption': ins.excluded.fuel_consumption
            }
        )
        with engine.begin() as conn:
            conn.execute(upsert_stmt)

    for rec in expenses_data:
        client_name = rec.get('Клиент')
        if not client_name:
            raise ValueError("Пустой клиент в расходах")
        client_id = get_or_create_client_id(client_name)
        schema_name = client_id  # NEW: схема клиента
        # table_name = f"{client_id}_expenses"
        # tr_table = f"{client_id}_transport"

        # NEW: описание таблицы expenses в схеме client_id
        expense_meta = MetaData(schema=schema_name)

        # Описание таблицы расходов
        expenses = Table(
            'expenses', expense_meta,
            Column('date', Date, primary_key=True),
            Column('gov_number', String(9), primary_key=True),
            Column('driver', String(255), primary_key=True),
            Column('vehicle_type', String(2), primary_key=True),
            Column('client', String(255), primary_key=True),
            Column('industry', String(255)),
            Column('fuel', Numeric(8, 2), default=0),
            Column('repair', Numeric(8, 2), default=0),
            Column('insurance', Numeric(8, 2), default=0),
            Column('fines', Numeric(8, 2), default=0),
            Column('downtime', Numeric(8, 2), default=0),
            Column('maintenance', Numeric(8, 2), default=0),
            Column('tire_service', Numeric(8, 2), default=0),
            Column('carwash', Numeric(8, 2), default=0),
            Column('parking', Numeric(8, 2), default=0),
            Column('antifreeze_liquid', Numeric(8, 2), default=0),
            Column('flat_roads', Numeric(8, 2), default=0),
            Column('daily_distance', Numeric(8, 2), default=0),
            extend_existing=True
        )
        expense_meta.create_all(engine)

        ins = pg_insert(expenses).values(
            date=rec.get('Дата'),
            gov_number=rec.get('ГосНомер') or rec.get('Гос номер'),
            driver=rec.get('Водитель'),
            vehicle_type=rec.get('ТипТС') or rec.get('Тип ТС'),
            client=client_name,
            industry=rec.get('Отрасль'),
            fuel=rec.get('Топливо'),
            repair=rec.get('Ремонт'),
            insurance=rec.get('Страховка'),
            fines=rec.get('Штрафы'),
            downtime=rec.get('Простой'),
            maintenance=rec.get('Техобслуживание'),
            tire_service=rec.get('Шины'),
            carwash=rec.get('Автомойка'),
            parking=rec.get('Стоянка'),
            antifreeze_liquid=rec.get('НезамерзающиеЖидкости') or rec.get('Незамерзающие жидкости'),
            flat_roads=rec.get('ПлатныеДороги') or rec.get('Платные дороги'),
            daily_distance=rec.get('ДневноеРасстояние') or rec.get('Дневное расстояние')
        )
        upsert_stmt = ins.on_conflict_do_update(
            index_elements=['date', 'gov_number', 'driver', 'vehicle_type', 'client'],
            set_={k: ins.excluded[k] for k in ['fuel', 'repair', 'insurance', 'fines',
                                               'downtime', 'maintenance', 'tire_service',
                                               'carwash', 'parking', 'antifreeze_liquid',
                                               'flat_roads', 'daily_distance']}
        )
        with engine.begin() as conn:
            conn.execute(upsert_stmt)


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
    'etl_crm_erp_pipeline_sqlalchemy',
    default_args=default_args,
    description='ETL с использованием SQLAlchemy для per-client tables',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
)

# Шаг 1: ожидание файла (если локальный)
wait_for_file = FileSensor(
    task_id='wait_for_file',
    fs_conn_id='fs_default',
    filepath=DATA_DIR,
    poke_interval=60,
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

# Зависимости
wait_for_file >> load_file >> upsert