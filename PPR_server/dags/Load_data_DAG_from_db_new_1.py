"""
DAG: etl_crm_erp_pipeline_sqlalchemy_COPY_no_limits
Запускается **только вручную** (schedule_interval=None),
например POST-запросом из backend’а:
  POST /api/v1/dags/etl_crm_erp_pipeline_sqlalchemy_COPY_no_limits/dagRuns
  { "conf": { "client_id": "c_1234abcd" } }
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
# from airflow.sdk import Param
from datetime import datetime, timedelta
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
import io
import csv
from sqlalchemy import inspect, text
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
import logging
from airflow.models.param import Param
# --- остальные ваши импорты (pd, sqlalchemy, etc.) ---
# ... весь длинный код, который был выше, БЕЗ изменений ...



DATA_DIR = '/opt/airflow/input'
CLIENTS_TABLE = 'clients'
TABLE_PREFIX = 'c_'  # Префикс для клиентских таблиц
CLIENT_CONN_ID = 'client_db_conn_1'
OUR_CONN_ID = 'postgres_connect'

# Маппинг колонок
# cols_map = {
#     'ГосНомер': 'gov_number', 'Гос номер': 'gov_number', 'drivers': 'driver', 'fuel_per_100km': 'fuel_consumption',
#     'has_trailer': 'trailer', 'mileage_km': 'mileage', 'expense_date': 'date', 'fuel_amount': 'fuel',
#     'Водитель': 'driver', 'ТипТС': 'vehicle_type', 'Тип ТС': 'vehicle_type', 'car_wash': 'carwash',
#     'ГодВыпуска': 'release_year', 'Год выпуска': 'release_year', 'parking_cost': 'parking',
#     'Пробег': 'mileage', 'repair_cost': 'repair', 'maintenance_cost': 'maintenance', 'fines_cost': 'fines',
#     'ДатаПоследнегоРемонта': 'last_repair_date', 'Дата последнего ремонта': 'last_repair_date',
#     'Отрасль': 'industry', 'РасходТоплива': 'fuel_consumption', 'Расход топлива': 'fuel_consumption',
#     'Дата': 'date', 'Топливо': 'fuel', 'Ремонт': 'repair', 'Страховка': 'insurance', 'Штрафы': 'fines',
#     'Техобслуживание': 'maintenance', 'Шины': 'tire_service', 'Шиномонтаж': 'tire_service',
#     'Мойка': 'carwash', 'Автомойка': 'carwash', 'Стоянка': 'parking',
#     'НезамерзающиеЖидкости': 'antifreeze_liquid', 'Незамерзающие жидкости': 'antifreeze_liquid',
#     'ПлатныеДороги': 'flat_roads', 'Платные дороги': 'flat_roads',
#     'ДневноеРасстояние': 'daily_distance', 'Дневное расстояние': 'daily_distance',
#     'ЗарплатаРасчетная': 'calculated_salary', 'Зарплата': 'calculated_salary', 'ЕстьПрицеп': 'trailer',
#     'Есть прицеп': 'trailer', 'salary_cost': 'calculated_salary', 'tolls_cost': 'flat_roads', 'МоторноеМасло': 'engine_oil',
#     'Моторное масло': 'engine_oil', 'ТормознаяЖидкость': 'brake_fluid', 'Тормозная жидкость': 'brake_fluid',
#     'СвечиЗажигания': 'spark_plugs', 'Свечи зажигания': 'spark_plugs', 'ТопливныеФильтры': 'fuel_filters',
#     'Топливные фильтры': 'fuel_filters', 'Топливные_фильтры': 'fuel_filters',
#     'Фильтры': 'filters', 'ГРМ_ремни': 'timing_belts', 'ГРМремни': 'timing_belts', 'грм_ремни': 'timing_belts',
#     'Ремни_ГРМ': 'timing_belts', 'РемниГРМ': 'timing_belts',
#     'ТормозныеКолодки': 'brake_pads', 'Тормозные колодки': 'brake_pads', 'Тормозные_колодки': 'brake_pads',
#     'ДругиеРасходы': 'other_cost', 'Другие_расходы': 'other_cost', 'Другие расходы': 'other_cost',
#     'ПрочиеРасходы': 'other_cost', 'Прочие расходы': 'other_cost', 'Прочие_расходы': 'other_cost',
#     'Другое': 'other_cost', 'Прочие': 'other_cost', 'Другие': 'other_cost'
# }


cols_map = {
    # госномер
    'ГосНомер': 'gov_number', 'госномер': 'gov_number', 'гос_номер': 'gov_number',
    'Гос номер': 'gov_number', 'гос номер': 'gov_number', 'gos_number': 'gov_number',
    'reg_number': 'gov_number', 'registration_number': 'gov_number', 'car_id': 'gov_number',
    'gov_number': 'gov_number',
    # водитель
    'Водитель': 'driver', 'водитель': 'driver', 'drivers': 'driver', 'driver': 'driver',
    'driver_name': 'driver', 'driver_fullname': 'driver', 'водители': 'driver',
    # тип ТС
    'ТипТС': 'vehicle_type', 'Тип ТС': 'vehicle_type', 'типтс': 'vehicle_type',
    'тип тс': 'vehicle_type', 'vehicle type': 'vehicle_type',
    'vehicle_type': 'vehicle_type', 'тип_ТС': 'vehicle_type',
    # год выпуска
    'ГодВыпуска': 'release_year', 'Год выпуска': 'release_year',
    'год выпуска': 'release_year', 'release_year': 'release_year',
    'year_of_release': 'release_year', 'год_выпуска': 'release_year',
    'гв': 'release_year', 'born_year': 'release_year',
    # пробег
    'Пробег': 'mileage', 'пробег': 'mileage', 'mileage': 'mileage',
    'mileage_km': 'mileage', 'пробег_км': 'mileage', 'total_mileage': 'mileage',
    'общий пробег': 'mileage', 'общий_пробег': 'mileage',
    # дата последнего ремонта
    'ДатаПоследнегоРемонта': 'last_repair_date', 'Дата последнего ремонта': 'last_repair_date',
    'last_repair_date': 'last_repair_date', 'дата_последнего_ремонта': 'last_repair_date',
    'дата ремонта': 'last_repair_date', 'repair_date': 'last_repair_date',
    # отрасль
    'Отрасль': 'industry', 'отрасль': 'industry', 'industry': 'industry',
    # расход топлива на 100 км
    'РасходТоплива': 'fuel_consumption', 'Расход топлива': 'fuel_consumption',
    'Расход на 100км': 'fuel_consumption', 'Расход_на_100км': 'fuel_consumption',
    'расход л/100км': 'fuel_consumption', 'fuel_per_100km': 'fuel_consumption',
    'fuel_consumption': 'fuel_consumption', 'avg_fuel_consumption': 'fuel_consumption',
    # прицеп
    'ЕстьПрицеп': 'trailer', 'Есть прицеп': 'trailer', 'trailer': 'trailer',
    'has_trailer': 'trailer', 'прицеп': 'trailer',
    # дата операции
    'Дата': 'date', 'дата': 'date', 'Date': 'date', 'date': 'date',
    'expense_date': 'date', 'transaction_date': 'date',
    'operation_date': 'date', 'date_of_expense': 'date',
    # топливо (литры или сумма)
    'Топливо': 'fuel', 'топливо': 'fuel', 'fuel': 'fuel',
    'fuel_amount': 'fuel', 'fuel_volume': 'fuel',
    'liters': 'fuel', 'л топлива': 'fuel',
    # ремонт (стоимость)
    'Ремонт': 'repair', 'ремонт': 'repair', 'repair': 'repair',
    'repair_cost': 'repair', 'cost_of_repair': 'repair',
    # страховка
    'Страховка': 'insurance', 'страховка': 'insurance',
    'insurance': 'insurance', 'insurance_cost': 'insurance',
    # штрафы
    'Штрафы': 'fines', 'штрафы': 'fines', 'fines': 'fines',
    'fines_cost': 'fines', 'cost_of_fines': 'fines', 'fine': 'fines',
    # техобслуживание
    'Техобслуживание': 'maintenance', 'техобслуживание': 'maintenance',
    'maintenance': 'maintenance', 'maintenance_cost': 'maintenance',
    'cost_of_maintenance': 'maintenance',
    # шиномонтаж / шины
    'Шины': 'tire_service', 'Шиномонтаж': 'tire_service',
    'tire_service': 'tire_service', 'tire_cost': 'tire_service',
    'cost_of_tires': 'tire_service',
    # мойка
    'Мойка': 'carwash', 'Автомойка': 'carwash',
    'carwash': 'carwash', 'car_wash': 'carwash',
    # парковка
    'Стоянка': 'parking', 'parking': 'parking', 'parking_cost': 'parking',
    'cost_of_parking': 'parking',
    # платные дороги (транзит)
    'ПлатныеДороги': 'flat_roads', 'Платные дороги': 'flat_roads',
    'tolls_cost': 'flat_roads', 'flat_roads': 'flat_roads',
    'tolls': 'flat_roads',
    # дневное расстояние
    'ДневноеРасстояние': 'daily_distance', 'Дневное расстояние': 'daily_distance',
    'daily_distance': 'daily_distance', 'distance_per_day': 'daily_distance',
    # зарплата
    'ЗарплатаРасчетная': 'calculated_salary', 'Зарплата': 'calculated_salary',
    'salary_cost': 'calculated_salary', 'calculated_salary': 'calculated_salary',
    'salary': 'calculated_salary',
    # антифриз
    'НезамерзающиеЖидкости': 'antifreeze_liquid', 'Незамерзающие жидкости': 'antifreeze_liquid',
    'antifreeze_liquid': 'antifreeze_liquid', 'antifreeze': 'antifreeze_liquid',
    # моторное масло
    'МоторноеМасло': 'engine_oil', 'Моторное масло': 'engine_oil',
    'engine_oil': 'engine_oil', 'oil_change': 'engine_oil',
    # тормозная жидкость
    'ТормознаяЖидкость': 'brake_fluid', 'Тормозная жидкость': 'brake_fluid',
    'brake_fluid': 'brake_fluid', 'brake_liquid': 'brake_fluid',
    # свечи зажигания
    'СвечиЗажигания': 'spark_plugs', 'Свечи зажигания': 'spark_plugs',
    'spark_plugs': 'spark_plugs', 'plugs': 'spark_plugs',
    # топливные фильтры
    'ТопливныеФильтры': 'fuel_filters', 'Топливные фильтры': 'fuel_filters',
    'fuel_filters': 'fuel_filters', 'filters_fuel': 'fuel_filters',
    # фильтры (общие)
    'Фильтры': 'filters', 'filters': 'filters', 'filter': 'filters',
    # ГРМ ремни
    'ГРМ_ремни': 'timing_belts', 'ГРМремни': 'timing_belts',
    'грм_ремни': 'timing_belts', 'Ремни_ГРМ': 'timing_belts',
    'timing_belts': 'timing_belts', 'belt_timing': 'timing_belts',
    'timing_belt': 'timing_belts',
    # тормозные колодки
    'ТормозныеКолодки': 'brake_pads', 'Тормозные колодки': 'brake_pads',
    'brake_pads': 'brake_pads', 'pads_brake': 'brake_pads',
    # прочие/другие расходы
    'ДругиеРасходы': 'other_cost', 'Другие_расходы': 'other_cost',
    'Другие расходы': 'other_cost', 'ПрочиеРасходы': 'other_cost',
    'Прочие расходы': 'other_cost', 'Прочие_расходы': 'other_cost',
    'other_cost': 'other_cost', 'misc_cost': 'other_cost',
    'miscellaneous_expenses': 'other_cost', 'Другое': 'other_cost',
    'Прочие': 'other_cost', 'Другие': 'other_cost',
    # Клиент
    'Клиент': 'client', 'клиент': 'client', 'client': 'client',
    'client_name': 'client',
    # is_rental
    'is_rental': 'is_rental',
    # rental_cost
    'rental_cost': 'rental_cost',
    # purchase_price
    'purchase_price': 'purchase_price',
    # has_casco
    'has_casco': 'has_casco',
    # start_date
    'start_date': 'start_date',
    # rental_expense
    'rental_expense': 'rental_expense',
    # amortization
    'amortization': 'amortization',
    # transport_tax
    'transport_tax': 'transport_tax',
    # casco
    'casco': 'casco',
    # fire_extinguisher
    'fire_extinguishers': 'fire_extinguishers',
    # first_aid_kit
    'first_aid_kits': 'first_aid_kits',
    # hydraulic_oil
    'hydraulic_oil': 'hydraulic_oil',
    # lubrication
    'special_lubricants': 'special_lubricants',
    # customs_fees
    'customs_fees': 'customs_fees',
    # cargo_insurance
    'cargo_insurance': 'cargo_insurance',
    # transloading_fees
    'transloading_fees': 'transloading_fees',
    # temp_control_maintenance
    'temp_control_maintenance': 'temp_control_maintenance',
    # sterilization_costs
    'sterilization_costs': 'sterilization_costs',
    # pharma_licenses
    'pharma_licenses': 'pharma_licenses',
    # bucket_parts
    'bucket_parts': 'bucket_parts',

}


# Описание ожидаемых колонок для двух таблиц
TRANSPORT_COLS = [
    'gov_number', 'driver', 'vehicle_type',
    'release_year', 'mileage', 'last_repair_date',
    'industry', 'fuel_consumption', 'trailer',
    'is_rental', 'rental_cost', 'purchase_price', 'has_casco', 'start_date'
]

EXPENSES_COLS = [
    'date', 'gov_number', 'driver', 'vehicle_type', 'industry',
    'fuel', 'repair', 'insurance', 'fines', 'maintenance',
    'tire_service', 'carwash', 'parking',
    'flat_roads', 'daily_distance', 'calculated_salary', 'trailer',
    'antifreeze_liquid', 'engine_oil', 'brake_fluid', 'spark_plugs',
    'fuel_filters', 'filters', 'timing_belts', 'brake_pads', 'other_cost',
    'is_rental', 'rental_expense', 'amortization', 'transport_tax', 'casco',
    'fire_extinguishers', 'first_aid_kits', 'hydraulic_oil', 'special_lubricants',
    'customs_fees', 'cargo_insurance', 'transloading_fees', 'temp_control_maintenance',
    'sterilization_costs', 'pharma_licenses', 'bucket_parts',
]

TYPE_MAP = {
    'gov_number': String(20),
    'driver':     String(255),
    'vehicle_type': String(30),
    'release_year': Integer(),
    'mileage':      Numeric(12,2),
    'last_repair_date': Date(),
    'industry':     String(50),
    'fuel_consumption': Numeric(10,2),
    'trailer':        Boolean(),
    'is_rental':      Boolean(),
    'rental_cost':    Numeric(12,2),
    'purchase_price': Numeric(12,2),
    'start_date':     Date(),
    'has_casco':      Boolean(),
    # для expenses:
    'date':        Date(),
    'fuel':        Numeric(12,2),
    'repair':      Numeric(12,2),
    'insurance':   Numeric(12,2),
    'fines':       Numeric(12,2),
    'maintenance': Numeric(12,2),
    'tire_service': Numeric(12,2),
    'carwash':     Numeric(12,2),
    'parking':     Numeric(12,2),
    'flat_roads':  Numeric(12,2),
    'daily_distance':Numeric(12,2),
    'calculated_salary': Numeric(12,2),
    'antifreeze_liquid': Numeric(12,2),
    'engine_oil': Numeric(12,2),
    'brake_fluid': Numeric(12,2),
    'spark_plugs': Numeric(12,2),
    'fuel_filters': Numeric(12,2),
    'filters': Numeric(12,2),
    'timing_belts': Numeric(12,2),
    'brake_pads': Numeric(12,2),
    'other_cost':  Numeric(12,2),
    'rental_expense': Numeric(12,2),
    'amortization': Numeric(12,2),
    'transport_tax': Numeric(12,2),
    'casco': Numeric(12,2),
    'fire_extinguishers': Numeric(12,2),
    'first_aid_kits': Numeric(12,2),
    'hydraulic_oil': Numeric(12,2),
    'special_lubricants': Numeric(12,2),
    'customs_fees': Numeric(12,2),
    'cargo_insurance': Numeric(12,2),
    'transloading_fees': Numeric(12,2),
    'temp_control_maintenance': Numeric(12,2),
    'sterilization_costs': Numeric(12,2),
    'pharma_licenses': Numeric(12,2),
    'bucket_parts': Numeric(12,2),
}

def get_engine():
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    return hook.get_sqlalchemy_engine()

def get_raw_conn():
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    return hook.get_conn()


def init_client_table_dynamic(schema_name: str, table_name: str, cols: list, engine):
    meta = MetaData(schema=schema_name)
    columns = []
    for c in cols:
        col_type = TYPE_MAP.get(c, String(255))  # fallback на String
        # считаем, что первичные ключи — это пересечение с pk_list
        is_pk = c in (['gov_number']
                     if table_name=='transport'
                     else ['date', 'gov_number'])
        columns.append(Column(c, col_type, primary_key=is_pk))
    Table(table_name, meta, *columns)
    meta.create_all(engine)


# В начало файла после импортов
def process_client(**context):
    client_id = context['dag_run'].conf.get('client_id')
    if not client_id:
        raise ValueError("client_id not provided in DAG configuration")
    # Ваша логика обработки для конкретного client_id
    print(f"Processing client: {client_id}")
    ti = context['ti']
    ti.xcom_push(key='client_id', value=client_id)


def get_client_id_and_schema(engine, **context):
    ti = context['ti']
    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')
    if not client_id:
        raise ValueError("client_id not found in XCom")
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

    return client_id, schema_name


def normalize_and_save_temp_csv(df: pd.DataFrame, expected_columns, return_temp_path=False):

    # Маппинг и приведение типов
    df = df.rename(columns=cols_map)

    # Удаляем "Клиент" (если есть)
    if 'client' in df.columns:
        df = df.drop(columns=['client'])

    provided = set(df.columns)
    keep_cols = [c for c in expected_columns if c in provided]

    df = df[keep_cols]

    for col in df.columns:
        if col in ('date', 'last_repair_date', 'start_date'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif col in ('release_year'):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif col in ('trailer', 'has_casco', 'is_rental'):
            s = df[col].astype(str).str.strip().str.lower()
            df[col] = s.map({'да': True, 'true': True, '1': True, 'TRUE': True, 'нет': False, 'false': False, '0': False, 'FALSE': False}).astype('boolean')
        elif col in ('gov_number', 'driver', 'vehicle_type', 'industry'):
            df[col] = df[col].astype(str).fillna('')
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print('df: ', df.head())
    # сохраняем временный CSV

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmpfile:
        df.to_csv(tmpfile.name, index=False, encoding='utf-8')
        temp_csv_path = tmpfile.name

    print('temp_csv: ', pd.read_csv(temp_csv_path).head())
    print('keep_cols: ', keep_cols)

    if return_temp_path:
        return temp_csv_path, keep_cols

def load_local_file(**context):
    """
        Сканирует входную директорию, определяет для каждого файла его тип (transport или expenses)
        по набору ключевых колонок и пушит пути и client_name через XCom.
    """
    transport_path = None
    expenses_path = None
    # client_name = None

    t_cols = []
    e_cols = []

    ti = context['ti']

    client_id = ti.xcom_pull(task_ids='process_client_data', key='client_id')
    print('client_id: ', client_id)
    print('type of client_id: ', type(client_id))
    for fname in os.listdir(DATA_DIR):
        if not fname.endswith('.csv') and not fname.endswith('.xlsx') and not fname.endswith('.json'):
            continue
        path = os.path.join(DATA_DIR, fname)
        try:
            df0 = pd.DataFrame()
            # читаем только заголовок
            print('fname: ', fname)
            print('startswith: ', fname.startswith(client_id))
            if fname.startswith(client_id) and fname.endswith('.csv'):
                df0 = pd.read_csv(path)
            elif fname.startswith(client_id) and fname.endswith('.xlsx'):
                df0 = pd.read_excel(path, engine='openpyxl')
            elif fname.startswith(client_id) and fname.endswith('.json'):
                df0 = pd.read_json(path)

            df0 = df0.rename(columns=cols_map)

            cols = set(df0.columns)


            if {'date'}.issubset(cols) and {'gov_number'}.issubset(cols):
                de_df = df0
                expenses_path, e_cols = normalize_and_save_temp_csv(de_df, EXPENSES_COLS, return_temp_path=True)
            elif {'gov_number'}.issubset(cols) and not {'date'}.issubset(cols):
                tp_df = df0
                transport_path, t_cols = normalize_and_save_temp_csv(tp_df, TRANSPORT_COLS, return_temp_path=True)

        except Exception as e:
            logging.error(f"Ошибка обработки файла {fname}: {e}")
            continue

    if not client_id: # not (transport_path or expenses_path) or
        raise ValueError('Не удалось определить client_id') # пути к файлам transport или expenses или

    ti = context['ti']
    ti.xcom_push(key='transport_path_f', value=transport_path)
    ti.xcom_push(key='expenses_path_f', value=expenses_path)
    ti.xcom_push('t_cols', t_cols)
    ti.xcom_push('e_cols', e_cols)

    ti.xcom_push(key='load_success', value=True)

# --- Из БД клиента ---
def extract_from_db(**context):
    hook = PostgresHook(postgres_conn_id=CLIENT_CONN_ID)
    eng = hook.get_sqlalchemy_engine()
    insp = inspect(eng)
    conn = eng.connect()

    src_tp_cols = [c['name'] for c in insp.get_columns('transport_park')]
    sel_tp_src = []
    for src in src_tp_cols:
        # если по исходному имени есть маппинг в cols_map -> возьмём его
        if src in cols_map:
            sel_tp_src.append(src)
        # либо если оно совпадает с целевым (например, client, или когда вы напрямую храните target-names)
        elif src in EXPENSES_COLS: # or src == 'client'
            sel_tp_src.append(src)
    df_tp = pd.read_sql(f"SELECT {','.join(sel_tp_src)} FROM transport_park", conn)
    df_tp = df_tp.rename(columns=cols_map)

    src_de_cols = [c['name'] for c in insp.get_columns('transport_daily_expenses')]
    sel_de_src = []
    for src in src_de_cols:
        # если по исходному имени есть маппинг в cols_map -> возьмём его
        if src in cols_map:
            sel_de_src.append(src)
        # либо если оно совпадает с целевым (например, client, или когда вы напрямую храните target-names)
        elif src in EXPENSES_COLS: # or src == 'client'
            sel_de_src.append(src)
    df_de = pd.read_sql(f"SELECT {','.join(sel_de_src)} FROM transport_daily_expenses", conn)
    df_de = df_de.rename(columns=cols_map)


    tp_path, t_cols = normalize_and_save_temp_csv(df_tp, TRANSPORT_COLS, return_temp_path=True)
    de_path, e_cols = normalize_and_save_temp_csv(df_de, EXPENSES_COLS, return_temp_path=True)

    ti = context['ti']
    ti.xcom_push('transport_path_db', tp_path)
    ti.xcom_push('expenses_path_db',  de_path)
    ti.xcom_push('t_cols',         t_cols)
    ti.xcom_push('e_cols',         e_cols)


def update_schema(**context):
    ti = context['ti']
    engine = get_engine()
    inspector = inspect(engine)

    # Получаем списки колонок из XCom (БД и файл)
    tcols_db = ti.xcom_pull(task_ids='extract_from_db', key='t_cols') or []
    ecols_db = ti.xcom_pull(task_ids='extract_from_db', key='e_cols') or []
    tcols_f  = ti.xcom_pull(task_ids='load_local_file',   key='t_cols') or []
    ecols_f  = ti.xcom_pull(task_ids='load_local_file',   key='e_cols') or []

    client_id, schema = get_client_id_and_schema(engine, **context)

    # Объединяем все обнаруженные колонки
    new_transport = list(set(tcols_db) | set(tcols_f))
    new_expense   = list(set(ecols_db) | set(ecols_f))

    if not inspector.has_table('transport', schema=schema):
        init_client_table_dynamic(schema, 'transport', new_transport, engine)

    else:
        existing = {c['name'] for c in inspector.get_columns('transport', schema=schema)}
        to_add = [c for c in new_transport if c not in existing and c in TRANSPORT_COLS]
        for col in to_add:
            sql_type = TYPE_MAP[col]
            type_str = str(sql_type.compile(dialect=engine.dialect))
            # col_type = TYPE_MAP[col].compile(dialect=engine.dialect)
            engine.execute(text(
                f'ALTER TABLE {schema}.transport '
                f'ADD COLUMN IF NOT EXISTS "{col}" {type_str}' # col_type
            ))
            logging.info(f"[update_schema] Added column {col} to {schema}.transport")

    if not inspector.has_table('expenses', schema=schema):
        init_client_table_dynamic(schema, 'expenses', new_expense, engine)
    else:
        existing = {c['name'] for c in inspector.get_columns('expenses', schema=schema)}
        to_add = [c for c in new_expense if c not in existing and c in EXPENSES_COLS]
        for col in to_add:
            col_type = TYPE_MAP[col].compile(dialect=engine.dialect)
            engine.execute(text(
                f'ALTER TABLE {schema}.expenses '
                f'ADD COLUMN IF NOT EXISTS "{col}" {col_type}'
            ))
            logging.info(f"[update_schema] Added column {col} to {schema}.expenses")


def bulk_upsert(table_name, temp_csv, pk_cols, schema, pg_conn):
    cursor = pg_conn.cursor()

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
        if o in ('date', 'last_repair_date', 'start_date'):
            cast = '::date'
        elif o == 'release_year':
            cast = '::integer'
        elif o in ('trailer', 'has_casco', 'is_rental'):
            cast = '::boolean'
        elif o in ('gov_number', 'driver', 'vehicle_type', 'industry'):
            cast = '::text'
        else:
            cast = '::numeric(10,2)'
        select_list.append(f'"{o}"{cast} AS {o}')
    select_sql = ', '.join(select_list)

    # Условия MERGE
    on_cond = ' AND '.join(f"t.{c} = s.{c}" for c in pk_cols)
    set_list = ', '.join(f"{u} = s.{u}" for u in [c for c in mapped_cols if c not in pk_cols])
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

    print(f"staging {table_name} columns:", mapped_cols)
    print(pd.read_csv(temp_csv).head())



def upsert_data(**context):

    engine = get_engine()
    pg_conn = get_raw_conn()

    ti = context['ti']

    # забираем из extract_from_db
    transport_db = ti.xcom_pull(key='transport_path_db', task_ids='extract_from_db') or []
    expenses_db = ti.xcom_pull(key='expenses_path_db', task_ids='extract_from_db') or []
    tcols_db = ti.xcom_pull(key='t_cols', task_ids='extract_from_db') or []
    ecols_db = ti.xcom_pull(key='e_cols', task_ids='extract_from_db') or []

    # забираем из load_local_file
    transport_f = ti.xcom_pull(key='transport_path_f', task_ids='load_local_file') or []
    expenses_f = ti.xcom_pull(key='expenses_path_f', task_ids='load_local_file') or []
    tcols_f = ti.xcom_pull(key='t_cols', task_ids='load_local_file') or []
    ecols_f = ti.xcom_pull(key='e_cols', task_ids='load_local_file') or []

    if not any([transport_db, expenses_db, transport_f, expenses_f]):
        raise ValueError("Не удалось получить данные ни из БД, ни из файлов")

    # client_name = name_db or name_f
    client_id, schema = get_client_id_and_schema(engine, **context)

    #    (используем set, чтобы не дублировать)
    final_t_cols = list({*tcols_db, *tcols_f})
    final_e_cols = list({*ecols_db, *ecols_f})

    # 7) Функция-обёртка, чтобы заапдейтить из одного источника, если он есть
    def _maybe_upsert(table_name, csv_path, pk):
        if not csv_path:
            return
        bulk_upsert(table_name, csv_path, pk_cols=pk, schema=schema, pg_conn=pg_conn)

    # 8) Сначала из БД
    _maybe_upsert('transport', transport_db, pk=['gov_number'])
    _maybe_upsert('expenses', expenses_db, pk=['date', 'gov_number'])

    # 9) Потом из файлов
    _maybe_upsert('transport', transport_f, pk=['gov_number'])
    _maybe_upsert('expenses', expenses_f, pk=['date', 'gov_number'])

    for p in [transport_db, expenses_db, transport_f, expenses_f]:
        if p and os.path.exists(p):
            os.remove(p)



# Удаление файлов после обработки
def clean_input(**context):
    ti = context["ti"]
    load_ok = ti.xcom_pull(task_ids='load_local_file', key='load_success')
    if not load_ok:
        # "мягко пропускаем" — просто выходим без ошибок
        print("clean_input: пропускаем удаление — load_local_file не отработал успешно")
        return
    for fn in os.listdir(DATA_DIR):
        os.remove(os.path.join(DATA_DIR, fn))
    print("clean_input: все файлы удалены после успешной загрузки")




# ─────────────────────── DAG definition ───────────────────────
default_args = {
    "owner"          : "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date"     : datetime(2025, 1, 1),
    "retries"        : 1,
    "retry_delay"    : timedelta(seconds=25),
}

with DAG(
    dag_id="etl_crm_erp_pipeline_sqlalchemy_COPY_no_limits",
    description="ETL per-client, старт только по REST",
    schedule_interval=None,          # ←── ОТКЛЮЧИЛИ расписание
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"client_id": Param(default="", type="string")},
) as dag:

    process_task = PythonOperator(
        task_id="process_client_data",
        python_callable=process_client,
        provide_context=True,
    )

    extract_db = PythonOperator(
        task_id="extract_from_db",
        python_callable=extract_from_db,
        provide_context=True,
    )

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=DATA_DIR,
        poke_interval=20,
        timeout=300,
        # provide_context=True,
    )

    load_file = PythonOperator(
        task_id="load_local_file",
        python_callable=load_local_file,
        provide_context=True,
    )

    update_schema_task = PythonOperator(
        task_id="update_schema",
        python_callable=update_schema,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    upsert = PythonOperator(
        task_id="upsert_data",
        python_callable=upsert_data,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    cleanup = PythonOperator(
        task_id="clean_input",
        python_callable=clean_input,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

# ── зависимости ──
process_task >> [extract_db, wait_for_file]
wait_for_file >> load_file >> update_schema_task
extract_db >> update_schema_task
update_schema_task >> upsert
load_file >> cleanup
