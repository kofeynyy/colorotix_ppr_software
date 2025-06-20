from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import tempfile
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Integer, Date, Numeric, Boolean, insert, Text)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import MetaData, Table, Column, String
import io
import csv
from sqlalchemy import inspect, text
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging
import pandas as pd
from prophet import Prophet
import json
from datetime import datetime
import numpy as np
import psutil
import gc

logger = logging.getLogger(__name__)

OUR_CONN_ID = 'postgres_connect'
RESULT_TABLE_NAME = 'fraud_detection_results'


def get_all_clients(**kwargs):
    """Fetch all client_ids from the clients table"""
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    query = 'SELECT companyid, "yearBudget", "companyType" FROM web.users'
    clients_df = hook.get_pandas_df(query)

    if clients_df.empty:
        raise AirflowSkipException("No clients found in the database")

    # Convert to list of dicts for easier processing
    clients = clients_df.to_dict('records')
    kwargs['ti'].xcom_push(key='clients', value=clients)
    return clients


def check_client_tables(client_id):
    """Check if required tables exist in client schema"""
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    # inspector = inspect(hook.get_conn())
    engine = hook.get_sqlalchemy_engine()
    inspector = inspect(engine)
    # Check if schema exists
    try:
        schemas = inspector.get_schema_names()
        if client_id not in schemas:
            raise AirflowSkipException(f"Schema {client_id} doesn't exist")

    # Check if required tables exist
        tables = inspector.get_table_names(schema=client_id)
        required_tables = {'expenses', 'transport'}
        if not required_tables.issubset(tables):
            missing = required_tables - set(tables)
            raise AirflowSkipException(f"Missing required tables in schema {client_id}: {missing}")

        return True
    except Exception as e:
        raise AirflowSkipException(f"Error checking tables for {client_id}: {str(e)}")


def create_result_table(client_id):
    """Create single result table if it doesn't exist"""
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema=client_id)

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=client_id)

    if RESULT_TABLE_NAME not in existing_tables:
        try:
            table = Table(
                RESULT_TABLE_NAME,
                metadata,
                Column('analysis_type', String(50)),  # Название анализа
                Column('result_json', Text),  # Результат в формате JSON
                Column('created_at', Date),  # Дата создания
            )
            table.create(engine)
            logger.info(f"Created table {client_id}.{RESULT_TABLE_NAME}")
        except Exception as e:
            logger.error(f"Failed to create table {client_id}.{RESULT_TABLE_NAME}: {str(e)}")
            raise


def save_results_to_db(client_id, results):
    """Save all analysis results to single table"""
    hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema=client_id)

    try:
        table = Table(RESULT_TABLE_NAME, metadata, autoload_with=engine)
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        records = []
        for analysis_type, result_data in results.items():
            records.append({
                'analysis_type': analysis_type,
                'result_json': json.dumps(result_data, ensure_ascii=False, indent=4),
                'created_at': created_at,
            })

        # Удаляем старые результаты за этот год
        with engine.begin() as conn:
            conn.execute(
                table.delete()
            )
            # Вставляем новые результаты
            conn.execute(table.insert(), records)

        logger.info(f"Saved {len(records)} analysis results to {client_id}.{RESULT_TABLE_NAME}")

    except Exception as e:
        logger.error(f"Failed to save results to {client_id}.{RESULT_TABLE_NAME}: {str(e)}")
        raise


class fraud_finder:
    def __init__(self, expenses, transport, budget, industry, market_averages=None):
        self.budget = budget
        self.market_averages = market_averages
        self.expenses = expenses.copy()
        self.transport = transport.copy()
        self.industry = industry

        self.expenses['date'] = pd.to_datetime(self.expenses['date'], format='%Y-%m-%d')
        self.expenses['month'] = self.expenses['date'].dt.to_period('M')
        self.expenses['year'] = self.expenses['date'].dt.to_period('Y')

        if self.market_averages is None:
            self.market_averages = {
                'Логистика': {
                    'all_expenses': 25605.443195501226,
                    'expenses_structure': {
                        'fuel': 11784.783393025677,
                        'repair': 1990.0962618047165,
                        'insurance': 17.51733211904805,
                        'fines': 243.07818560530916,
                        'maintenance': 4.9202608308837235,
                        'tire_service': 375.4417949781489,
                        'carwash': 21.337011896389004,
                        'parking': 467.32370753758573,
                        'antifreeze_liquid': 3.4442608188610966,
                        'flat_roads': 252.00452736049246,
                        'daily_distance': 113.0785448413915,
                        'rental_expense': 2655.6114084507044,
                        'amortization': 1284.3130700378108,
                        'transport_tax': 5.133060419709894,
                        'casco': 174.41825515620397,
                        'trailer': 0.5211267605633803,
                        'engine_oil': 7.689467577981762,
                        'brake_fluid': 3.803566977132262,
                        'spark_plugs': 6.29696079942811,
                        'filters': 15.879936039626577,
                        'fuel_filters': 15.65431942916569,
                        'timing_belts': 0.12576814822520174,
                        'brake_pads': 3471.401862304858,
                        'other_cost': 523.2015343591063,
                        'fire_extinguishers': 5.6769880915883695,
                        'first_aid_kits': 2.8426478632786907,
                        'calculated_salary': 1866.3418403635644,
                        'temp_control_maintenance': 63.65966114226975,
                        'customs_fees': 91.151455399061,
                        'cargo_insurance': 73.18920638641926,
                        'transloading_fees': 65.50577897603289}
                },
                'Строительство': {
                    'all_expenses': 23853.595043416022,
                    'expenses_structure': {
                        'fuel': 12341.074572485417,
                        'repair': 2103.917988463379,
                        'insurance': 17.970194260143042,
                        'fines': 255.66072194415344,
                        'maintenance': 6.412209159456278,
                        'tire_service': 401.520500523804,
                        'carwash': 21.88702763874339,
                        'parking': 417.78826342813545,
                        'antifreeze_liquid': 3.4310121703591614,
                        'flat_roads': 0.0,
                        'daily_distance': 113.051908069168,
                        'rental_expense': 1854.298787878788,
                        'amortization': 547.7449822165315,
                        'transport_tax': 2.5026190199045515,
                        'casco': 43.45633026811004,
                        'trailer': 0.24242424242424243,
                        'engine_oil': 7.472859193729873,
                        'brake_fluid': 3.9579781692017315,
                        'spark_plugs': 5.952117820599923,
                        'filters': 15.929706799105006,
                        'fuel_filters': 15.775436826653216,
                        'timing_belts': 0.0,
                        'brake_pads': 3778.3532249511763,
                        'other_cost': 527.0162271892686,
                        'fire_extinguishers': 6.009415538224757,
                        'first_aid_kits': 2.8054682548920704,
                        'calculated_salary': 1316.9671357622317,
                        'bucket_parts': 0.0,
                        'special_lubricants': 23.121225054643748}
                },
                'Фармацевтика': {
                    'all_expenses': 22119.44469605468,
                    'expenses_structure': {
                        'fuel': 9507.485147711037,
                        'repair': 2158.9297855777622,
                        'insurance': 13.634614114197703,
                        'fines': 228.98695615060592,
                        'maintenance': 3.02892482695912,
                        'tire_service': 314.10421134183224,
                        'carwash': 19.53046827738499,
                        'parking': 423.39384002301034,
                        'antifreeze_liquid': 3.4534678691384144,
                        'flat_roads': 175.1997618252333,
                        'daily_distance': 112.79994052589582,
                        'rental_expense': 2765.3293478260866,
                        'amortization': 459.94789966412446,
                        'transport_tax': 2.4986546419491917,
                        'casco': 39.61847501345358,
                        'trailer': 0.0,
                        'engine_oil': 7.621091502904117,
                        'brake_fluid': 4.028432422869712,
                        'spark_plugs': 6.511651441498018,
                        'filters': 15.629200764534506,
                        'fuel_filters': 15.558945239288166,
                        'timing_belts': 0.0,
                        'brake_pads': 3461.8961012451523,
                        'other_cost': 532.7829176886469,
                        'fire_extinguishers': 5.762261314925124,
                        'first_aid_kits': 2.8850600308040604,
                        'calculated_salary': 1523.3967461819668,
                        'temp_control_maintenance': 186.74809562248325,
                        'sterilization_costs': 93.84532158696578,
                        'pharma_licenses': 34.83737562396779}
                },
                'Ритейл': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': 0.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': 0.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': 0.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                },
                'Такси': {
                    'all_expenses': 18539.185293844726,
                    'expenses_structure': {
                        'fuel': 8456.119833888177,
                        'repair': 1885.476009048229,
                        'insurance': 17.68253708920188,
                        'fines': 237.57293384549726,
                        'maintenance': 0.0,
                        'tire_service': 263.1655412718737,
                        'carwash': 19.138205377720872,
                        'parking': 350.028563551003,
                        'antifreeze_liquid': 3.4801514297908662,
                        'flat_roads': 0.0,
                        'daily_distance': 113.14272283397354,
                        'rental_expense': 879.2352000000001,
                        'amortization': 1344.4421970123772,
                        'transport_tax': 3.2129748186086213,
                        'casco': 63.93512590695689,
                        'trailer': 0.0,
                        'engine_oil': 7.5459154929577466,
                        'brake_fluid': 3.7984411562497655,
                        'spark_plugs': 5.873636529566063,
                        'filters': 15.795049082373026,
                        'fuel_filters': 15.881912078531796,
                        'timing_belts': 3.3441630148284167,
                        'brake_pads': 3136.341340162185,
                        'other_cost': 539.5613766609409,
                        'fire_extinguishers': 5.84790439607341,
                        'first_aid_kits': 2.8500384122919336,
                        'calculated_salary': 1165.7135207853178}
                },
                'Доставка': {
                    'all_expenses': 19781.530316567627,
                    'expenses_structure': {
                        'fuel': 8740.611579719185,
                        'repair': 1963.3216613589852,
                        'insurance': 15.736805870186151,
                        'fines': 219.67750068397956,
                        'maintenance': 0.0,
                        'tire_service': 266.27693073749407,
                        'carwash': 18.95934928920844,
                        'parking': 370.03803637676873,
                        'antifreeze_liquid': 3.4585846547818377,
                        'flat_roads': 0.0,
                        'daily_distance': 113.70139619379054,
                        'rental_expense': 2559.2366666666667,
                        'amortization': 308.7912212044606,
                        'transport_tax': 0.8306247742867461,
                        'casco': 13.21448504547096,
                        'trailer': 0.0,
                        'engine_oil': 7.648248465149874,
                        'brake_fluid': 3.7709350546831963,
                        'spark_plugs': 6.474838262422125,
                        'filters': 15.74415881458135,
                        'fuel_filters': 16.03527145780667,
                        'timing_belts': 0.0,
                        'brake_pads': 3287.701095461659,
                        'other_cost': 541.1875166464482,
                        'fire_extinguishers': 5.751195596266019,
                        'first_aid_kits': 2.8466025367433816,
                        'calculated_salary': 1300.5156116965975}
                },
                'Автобусы': {
                    'all_expenses': 24268.662149859232,
                    'expenses_structure': {
                        'fuel': 10995.86672548945,
                        'repair': 1837.3279332581142,
                        'insurance': 16.922560439142504,
                        'fines': 241.79151253153486,
                        'maintenance': 5.097021547225146,
                        'tire_service': 309.786130162312,
                        'carwash': 25.534322291384072,
                        'parking': 505.76517720786154,
                        'antifreeze_liquid': 3.4409697730336415,
                        'flat_roads': 272.96753920462464,
                        'daily_distance': 113.2089286393428,
                        'rental_expense': 1760.6943564356432,
                        'amortization': 2722.7670152085634,
                        'transport_tax': 6.8795611955561755,
                        'casco': 211.20421901345065,
                        'trailer': 0.0,
                        'engine_oil': 7.652434257510258,
                        'brake_fluid': 3.8262106697252207,
                        'spark_plugs': 6.3348526213472445,
                        'filters': 15.709144153852005,
                        'fuel_filters': 15.641341598948626,
                        'timing_belts': 0.0,
                        'brake_pads': 3585.966574122201,
                        'other_cost': 532.8068699211465,
                        'fire_extinguishers': 5.691628317761354,
                        'first_aid_kits': 2.828015195885786,
                        'calculated_salary': 1062.9511066036182}
                },
                'Каршеринг': {
                    'all_expenses': 17659.767930180187,
                    'expenses_structure': {
                        'fuel': 8595.443782586428,
                        'repair': 1923.9232755441747,
                        'insurance': 17.0188139991464,
                        'fines': 233.08227827571488,
                        'maintenance': 0.0,
                        'tire_service': 272.4352344003414,
                        'carwash': 18.907465727699535,
                        'parking': 350.02215168587276,
                        'antifreeze_liquid': 3.4619933418693987,
                        'flat_roads': 0.0,
                        'daily_distance': 113.30358924455822,
                        'rental_expense': 246.385,
                        'amortization': 2081.5252408877504,
                        'transport_tax': 4.880921895006402,
                        'casco': 86.64105847204439,
                        'trailer': 0.0,
                        'engine_oil': 7.779026888604354,
                        'brake_fluid': 3.947314067372843,
                        'spark_plugs': 6.179068351577732,
                        'filters': 15.749995731967562,
                        'fuel_filters': 15.249918907383696,
                        'timing_belts': 0.0,
                        'brake_pads': 3127.1941271873666,
                        'other_cost': 527.7651164338777,
                        'fire_extinguishers': 5.933324797268459,
                        'first_aid_kits': 2.9392317541613315,
                        'calculated_salary': 0}
                },
                'Другое': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': 0.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': 0.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': 0.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                },
            }

            self.market_averages = self.market_averages[industry]
            self.cost_cols = [
                "repair", "insurance", "maintenance", "tire_service", "carwash", "parking",
                "antifreeze_liquid", "amortization", "transport_tax", "casco", "engine_oil",
                "brake_fluid", "spark_plugs", "filters", "fuel_filters", "timing_belts",
                "brake_pads", "other_cost", "fire_extinguishers", "first_aid_kits",
                "calculated_salary", "fuel", "fines", "flat_roads", "rental_expense",
                "bucket_parts", "special_lubricants"
            ]
            self.latest_year = self.expenses['year'].max()
            current_year = datetime.now().year
            current_month = datetime.now().month
            if self.latest_year == current_year and current_month < 12:
                self.report_year = self.latest_year - 1
                if self.report_year not in self.expenses['year'].unique():
                    self.report_year = self.latest_year
            else:
                self.report_year = self.latest_year

    def find_high_salary(self):
        result = dict()
        salary_data = self.expenses.groupby(by=['driver', 'month'])['calculated_salary'].sum().reset_index().pivot(
            index='month', columns='driver', values='calculated_salary')
        median = salary_data.median(axis=0).to_dict()

        for i in range(salary_data.shape[0]):
            curr = salary_data.iloc[i]
            threshold = np.quantile(curr, 0.75) + (np.quantile(curr, 0.75) - np.quantile(curr, 0.25)) * 3
            anomaly = curr[curr > threshold]
            for id in anomaly.index:
                anomaly[id] = {'median': median[id], 'value': float(anomaly[id])}
            result[curr.name.to_timestamp().strftime('%Y-%m')] = anomaly.to_dict()

        with open("high_salary_data.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)
        return result

    def budget_deviations(self):
        years = list(map(int, self.expenses['year'].dt.year.unique()))
        result = {}

        for year in years:
            yearly_expenses = self.expenses[self.expenses['year'].dt.year == year]
            expenses_data = yearly_expenses.select_dtypes(include=['int64', 'float64'])

            expenses_data = expenses_data.drop(columns=[
                col for col in expenses_data.columns
                if set(expenses_data[col].dropna().unique()).issubset({0, 1})
            ])
            expenses_data.drop(columns=['daily_distance'], inplace=True, errors='ignore')

            expenses_sum = expenses_data.sum(axis=0).to_dict()
            all_expenses = sum(expenses_sum.values())

            expenses_structure = {key: expenses_sum[key] / all_expenses if all_expenses != 0 else 0 for key in
                                  expenses_sum.keys()}

            result[year] = {
                "all_expenses": all_expenses,
                "expenses_related_plan": all_expenses / self.budget,
                "expenses_structure": expenses_structure
            }

        with open("expenses_data.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def deviation_from_market_indicators(self):
        budget_deviations = self.budget_deviations()
        result = {}

        for year, year_data in budget_deviations.items():
            all_expenses = year_data['all_expenses']
            relations = {
                key: year_data['expenses_structure'][key] / self.market_averages['expenses_structure'].get(key, 1) if
                self.market_averages['expenses_structure'].get(key, 1) != 0 else 0
                for key in year_data['expenses_structure'].keys()
            }

            result[year] = {
                "all_expenses": all_expenses,
                "expenses_related_average_market": all_expenses / self.market_averages['all_expenses'] if
                self.market_averages['all_expenses'] != 0 else 0,
                'expenses_structure': relations
            }

        with open("expenses_with_market_avg.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def fuel_effectivness(self):
        expenses_data = self.expenses.groupby(by=['month', 'gov_number'])[
            ['fuel', 'daily_distance']].sum().reset_index()
        expenses_data['fuel_effectivness'] = (expenses_data['daily_distance'] / expenses_data['fuel']).fillna(0)

        pivot_table = expenses_data.pivot(index='month', columns='gov_number', values='fuel_effectivness')

        res = {}
        for i in range(pivot_table.shape[0]):
            month_str = pivot_table.index[i].to_timestamp().strftime("%Y-%m")
            month_data = pivot_table.iloc[i].replace({np.nan: None})
            res[month_str] = month_data.to_dict()

        with open("fuel_effectivness.json", "w", encoding="utf-8") as file:
            json.dump(res, file, ensure_ascii=False, indent=4)
        return res

    def salary_effectivness(self):
        expenses_data = self.expenses.groupby(by=['month', 'driver'])[
            ['calculated_salary', 'daily_distance']].sum().reset_index()
        expenses_data['salary_effectivness'] = (
                    expenses_data['daily_distance'] / expenses_data['calculated_salary']).fillna(0)

        pivot_table = expenses_data.pivot(index='month', columns='driver', values='salary_effectivness')

        res = {}
        for i in range(pivot_table.shape[0]):
            month_str = pivot_table.index[i].to_timestamp().strftime("%Y-%m")
            month_data = pivot_table.iloc[i].replace({np.nan: None})
            res[month_str] = month_data.to_dict()

        with open("salary_effectivness.json", "w", encoding="utf-8") as file:
            json.dump(res, file, ensure_ascii=False, indent=4)
        return res

    def month_drivers_fine(self):
        grouped = self.expenses.groupby(by=['month', 'driver'])['fines'].sum().reset_index()

        pivot_table = grouped.pivot(index='month', columns='driver', values='fines').fillna(0)

        res = {}
        for i in range(pivot_table.shape[0]):
            month_data = pivot_table.iloc[i]
            month_str = month_data.name.to_timestamp().strftime("%Y-%m")

            month_dict = {driver: fines for driver, fines in month_data.items() if fines != 0}
            sorted_month_dict = dict(sorted(month_dict.items(), key=lambda item: item[1], reverse=True))

            res[month_str] = sorted_month_dict

        with open("month_drivers_fine.json", "w", encoding="utf-8") as file:
            json.dump(res, file, ensure_ascii=False, indent=4)

        return res

    def year_drivers_fine(self):
        grouped = self.expenses.groupby(['year', 'driver'])['fines'].sum().reset_index()

        result = {}
        for year, year_group in grouped.groupby('year'):
            sorted_group = year_group.sort_values(by='fines', ascending=False)
            year_dict = dict(zip(sorted_group['driver'], sorted_group['fines']))
            result[year.year] = year_dict

        with open("year_drivers_fine.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def downtime_finder(self):
        filtered = self.expenses[self.expenses['daily_distance'] == 0].copy()

        numeric_cols = filtered.select_dtypes(include=['int64', 'float64']).columns
        filtered['total_expense'] = filtered[numeric_cols].sum(axis=1)

        grouped = filtered.groupby(['year', 'month', 'gov_number'])['total_expense'].agg(['sum', 'count']).reset_index()

        result = {}
        for index, row in grouped.iterrows():
            year_val = row['year'].year
            month_val = row['month'].strftime("%m")

            if year_val not in result:
                result[year_val] = {}
            if month_val not in result[year_val]:
                result[year_val][month_val] = {}

            result[year_val][month_val][row['gov_number']] = {
                'sum': row['sum'],
                'count': row['count']
            }

        with open("downtime_find.json", "w", encoding="utf-8") as file:
            json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def _round_sig(self, x, sig=3):
        if x <= 0:
            return 0
        s = str(int(x))
        n = len(s)
        digits = n - sig
        if digits <= 0:
            return int(x)
        factor = 10 ** digits
        return int(round(x / factor) * factor)

    def yearly_spending_recommendation(self):
        report_year_data = self.expenses[self.expenses['year'] == self.report_year]
        total = report_year_data[self.cost_cols].sum().sum()
        recommended = total * 1.07
        rec_r = self._round_sig(recommended)
        total_r = int(np.ceil(total))
        next_year = self.report_year + 1
        return (f"По итогам {self.report_year} года общие расходы составили {total_r} рублей, "
                f"рекомендуем закладывать на {next_year} год примерно {rec_r} рублей.")

    def inefficient_drivers_recommendation(self):
        temp_df = self.expenses.copy()
        temp_df['total_cost'] = temp_df[self.cost_cols].sum(axis=1)
        driver_stats = temp_df.groupby('driver').agg(
            total_distance=('daily_distance', 'sum'),
            total_cost=('total_cost', 'sum')
        )
        driver_stats['efficiency'] = driver_stats['total_distance'] / driver_stats['total_cost'].replace(0, np.nan)
        bottom_drivers = driver_stats.nsmallest(5, 'efficiency').index.tolist()
        if not bottom_drivers:
            return None
        return ("Рекомендуем обратить внимание на низкую эффективность работы водителей: "
                + ", ".join(bottom_drivers) + ".")

    def downtime_recommendation(self):
        cost_cols = self.expenses.select_dtypes(include=['number']) # .columns.difference(['trailer'])
        expenses_data = self.expenses[self.expenses['daily_distance'] == 0]
        sums = expenses_data.groupby('gov_number')[cost_cols].sum().sum(axis=1)
        high_downtime = sums.nlargest(5).index.tolist()
        if not high_downtime:
            return None
        return (f"Автомобили с высоким простоем и расходами: "
                + ", ".join(high_downtime)
                + ". Рекомендуем технический аудит.")

    def fuel_efficiency_recommendation(self):
        valid_df = self.expenses[self.expenses['daily_distance'] > 0]
        eff = valid_df.groupby('gov_number').apply(
            lambda x: x['fuel'].sum() / x['daily_distance'].sum()
        )
        low_eff = eff.nlargest(5).index.tolist()
        if not low_eff:
            return None
        return (f"Автомобили с низкой топливной эффективностью: "
                + ", ".join(low_eff)
                + ". Рекомендуем диагностировать двигатель и топливную систему.")

    def high_maintenance_recommendation(self):
        maint = self.expenses.groupby('gov_number')['maintenance'].sum()
        high = maint.nlargest(5).index.tolist()
        if not high:
            return None
        return (f"Автомобили с высокими расходами на обслуживание: "
                + ", ".join(high)
                + ". Рекомендуем пересмотреть условия сервисных договоров.")

    def high_fines_recommendation(self):
        fines = self.expenses.groupby('driver')['fines'].sum()
        drivers = fines.nlargest(5).index.tolist()
        if not drivers:
            return None
        return (f"Водители с избыточными штрафами: "
                + ", ".join(drivers)
                + ". Рекомендуем провести дополнительное обучение по ПДД.")

    def generate_recommendations(self, save_to_drive=False, drive_path=None):
        recs = {
            "yearly_spending": self.yearly_spending_recommendation(),
            "inefficient_drivers": self.inefficient_drivers_recommendation(),
            "downtime": self.downtime_recommendation(),
            "fuel_efficiency": self.fuel_efficiency_recommendation(),
            "high_maintenance": self.high_maintenance_recommendation(),
            "high_fines": self.high_fines_recommendation()
        }
        recs = {k: v for k, v in recs.items() if v is not None}

        # result_json = json.dumps(recs, ensure_ascii=False, indent=2)
        # file_path = "recommendations.json"
        # with open(file_path, "w", encoding="utf-8") as f:
        #     f.write(result_json)
        return recs

    def analyze_and_plot(self):
        # 1. Загрузка и предобработка данных
        df = self.expenses

        # Обработка числовых колонок (сохраняем исходные значения)
        excluded_columns = ['gov_number', 'vehicle_type', 'client', 'industry', 'driver',
                          'is_rental', 'date', 'last_repair_date', 'trailer', 'start_date', 'has_casco']
        numeric_columns = [col for col in df.columns
                          if col not in excluded_columns
                          and pd.api.types.is_numeric_dtype(df[col])]

        # Сохраняем оригинальные значения без преобразований
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Суммарные расходы (сохраняем оригинальные значения)
        df['total_expenses'] = df[numeric_columns].sum(axis=1, skipna=True)

        # 2. Агрегация данных по месяцам
        monthly_data = df.resample('M', on='date')['total_expenses'].sum().reset_index()
        monthly_data = monthly_data.rename(columns={'date': 'ds', 'total_expenses': 'y'})
        monthly_data = monthly_data[(monthly_data['y'] != 0) & (~monthly_data['y'].isna())]

        if len(monthly_data) < 2:
            print("Недостаточно данных для анализа")
            return

        # 3. Построение модели Prophet
        model = Prophet(
            interval_width=0.95,
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode='multiplicative'
        )

        try:
            model.fit(monthly_data)
        except Exception as e:
            print(f"Ошибка при обучении модели: {str(e)}")
            return

        # 4. Прогнозирование
        future = model.make_future_dataframe(periods=3, freq='M')
        forecast = model.predict(future)

        # 5. Анализ выбросов
        merged = pd.merge(monthly_data, forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], on='ds')
        merged['is_outlier'] = (merged['y'] < merged['yhat_lower']) | (merged['y'] > merged['yhat_upper'])

        outliers = merged[merged['is_outlier']]
        normal_points = merged[~merged['is_outlier']]
        forecast_data = forecast[forecast['ds'] > monthly_data['ds'].max()]

        # 6. Формирование JSON с сохранением оригинальной размерности
        result = {
            "data": {
                "x_axis": {
                    "values": monthly_data['ds'].dt.strftime('%Y-%m-%d').tolist(),
                    "type": "date",
                    "unit": "дата (последний день месяца)",
                    "original_format": "YYYY-MM-DD"
                },
                "y_axis": {
                    "values": monthly_data['y'].tolist(),
                    "type": "numeric",
                    "unit": "рубли",
                    "original_format": "число с плавающей точкой",
                    "statistics": {
                        "min": float(monthly_data['y'].min()),
                        "max": float(monthly_data['y'].max()),
                        "mean": float(monthly_data['y'].mean()),
                        "sum": float(monthly_data['y'].sum())
                    }
                },
                "outliers": {
                    "x": outliers['ds'].dt.strftime('%Y-%m-%d').tolist(),
                    "y": outliers['y'].tolist()
                },
                "forecast": {
                    "x": forecast_data['ds'].dt.strftime('%Y-%m-%d').tolist(),
                    "yhat": forecast_data['yhat'].tolist(),
                    "yhat_lower": forecast_data['yhat_lower'].tolist(),
                    "yhat_upper": forecast_data['yhat_upper'].tolist()
                }
            },
            "metadata": {
                "source_file": 'input_csv',
                "analysis_date": datetime.now().isoformat(),
                "model": "Prophet",
                "parameters": {
                    "interval_width": 0.95,
                    "seasonality_mode": "multiplicative"
                }
            }
        }

        return result


class PlotDataMaker:
    def __init__(self, expenses, transport, budget, industry, market_averages=None):
        self.budget = budget
        self.market_averages = market_averages
        self.expenses = expenses.copy()
        self.transport = transport.copy()
        self.industry = industry

        self.expenses['date'] = pd.to_datetime(self.expenses['date'], format='%Y-%m-%d')
        self.expenses['month'] = self.expenses['date'].dt.to_period('M')
        self.expenses['year'] = self.expenses['date'].dt.to_period('Y')

        if self.market_averages is None:
            self.market_averages = {
                'Логистика': {
                    'all_expenses': 25605.443195501226,
                    'expenses_structure': {
                        'fuel': 11784.783393025677,
                        'repair': 1990.0962618047165,
                        'insurance': 17.51733211904805,
                        'fines': 243.07818560530916,
                        'maintenance': 4.9202608308837235,
                        'tire_service': 375.4417949781489,
                        'carwash': 21.337011896389004,
                        'parking': 467.32370753758573,
                        'antifreeze_liquid': 3.4442608188610966,
                        'flat_roads': 252.00452736049246,
                        'daily_distance': 113.0785448413915,
                        'rental_expense': 2655.6114084507044,
                        'amortization': 1284.3130700378108,
                        'transport_tax': 5.133060419709894,
                        'casco': 174.41825515620397,
                        'trailer': 0.5211267605633803,
                        'engine_oil': 7.689467577981762,
                        'brake_fluid': 3.803566977132262,
                        'spark_plugs': 6.29696079942811,
                        'filters': 15.879936039626577,
                        'fuel_filters': 15.65431942916569,
                        'timing_belts': 0.12576814822520174,
                        'brake_pads': 3471.401862304858,
                        'other_cost': 523.2015343591063,
                        'fire_extinguishers': 5.6769880915883695,
                        'first_aid_kits': 2.8426478632786907,
                        'calculated_salary': 1866.3418403635644,
                        'temp_control_maintenance': 63.65966114226975,
                        'customs_fees': 91.151455399061,
                        'cargo_insurance': 73.18920638641926,
                        'transloading_fees': 65.50577897603289}
                    },
                'Строительство': {
                    'all_expenses': 23853.595043416022,
                    'expenses_structure': {
                        'fuel': 12341.074572485417,
                        'repair': 2103.917988463379,
                        'insurance': 17.970194260143042,
                        'fines': 255.66072194415344,
                        'maintenance': 6.412209159456278,
                        'tire_service': 401.520500523804,
                        'carwash': 21.88702763874339,
                        'parking': 417.78826342813545,
                        'antifreeze_liquid': 3.4310121703591614,
                        'flat_roads': 0.0,
                        'daily_distance': 113.051908069168,
                        'rental_expense': 1854.298787878788,
                        'amortization': 547.7449822165315,
                        'transport_tax': 2.5026190199045515,
                        'casco': 43.45633026811004,
                        'trailer': 0.24242424242424243,
                        'engine_oil': 7.472859193729873,
                        'brake_fluid': 3.9579781692017315,
                        'spark_plugs': 5.952117820599923,
                        'filters': 15.929706799105006,
                        'fuel_filters': 15.775436826653216,
                        'timing_belts': 0.0,
                        'brake_pads': 3778.3532249511763,
                        'other_cost': 527.0162271892686,
                        'fire_extinguishers': 6.009415538224757,
                        'first_aid_kits': 2.8054682548920704,
                        'calculated_salary': 1316.9671357622317,
                        'bucket_parts': 0.0,
                        'special_lubricants': 23.121225054643748}
                    },
                'Фармацевтика': {
                    'all_expenses': 22119.44469605468,
                        'expenses_structure': {
                        'fuel': 9507.485147711037,
                        'repair': 2158.9297855777622,
                        'insurance': 13.634614114197703,
                        'fines': 228.98695615060592,
                        'maintenance': 3.02892482695912,
                        'tire_service': 314.10421134183224,
                        'carwash': 19.53046827738499,
                        'parking': 423.39384002301034,
                        'antifreeze_liquid': 3.4534678691384144,
                        'flat_roads': 175.1997618252333,
                        'daily_distance': 112.79994052589582,
                        'rental_expense': 2765.3293478260866,
                        'amortization': 459.94789966412446,
                        'transport_tax': 2.4986546419491917,
                        'casco': 39.61847501345358,
                        'trailer': 0.0,
                        'engine_oil': 7.621091502904117,
                        'brake_fluid': 4.028432422869712,
                        'spark_plugs': 6.511651441498018,
                        'filters': 15.629200764534506,
                        'fuel_filters': 15.558945239288166,
                        'timing_belts': 0.0,
                        'brake_pads': 3461.8961012451523,
                        'other_cost': 532.7829176886469,
                        'fire_extinguishers': 5.762261314925124,
                        'first_aid_kits': 2.8850600308040604,
                        'calculated_salary': 1523.3967461819668,
                        'temp_control_maintenance': 186.74809562248325,
                        'sterilization_costs': 93.84532158696578,
                        'pharma_licenses': 34.83737562396779}
                    },
                'Ритейл': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': 0.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': 0.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': 0.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                    },
                'Такси': {
                    'all_expenses': 18539.185293844726,
                    'expenses_structure': {
                        'fuel': 8456.119833888177,
                        'repair': 1885.476009048229,
                        'insurance': 17.68253708920188,
                        'fines': 237.57293384549726,
                        'maintenance': 0.0,
                        'tire_service': 263.1655412718737,
                        'carwash': 19.138205377720872,
                        'parking': 350.028563551003,
                        'antifreeze_liquid': 3.4801514297908662,
                        'flat_roads': 0.0,
                        'daily_distance': 113.14272283397354,
                        'rental_expense': 879.2352000000001,
                        'amortization': 1344.4421970123772,
                        'transport_tax': 3.2129748186086213,
                        'casco': 63.93512590695689,
                        'trailer': 0.0,
                        'engine_oil': 7.5459154929577466,
                        'brake_fluid': 3.7984411562497655,
                        'spark_plugs': 5.873636529566063,
                        'filters': 15.795049082373026,
                        'fuel_filters': 15.881912078531796,
                        'timing_belts': 3.3441630148284167,
                        'brake_pads': 3136.341340162185,
                        'other_cost': 539.5613766609409,
                        'fire_extinguishers': 5.84790439607341,
                        'first_aid_kits': 2.8500384122919336,
                        'calculated_salary': 1165.7135207853178}
                    },
                'Доставка': {
                    'all_expenses': 19781.530316567627,
                    'expenses_structure': {
                        'fuel': 8740.611579719185,
                        'repair': 1963.3216613589852,
                        'insurance': 15.736805870186151,
                        'fines': 219.67750068397956,
                        'maintenance': 0.0,
                        'tire_service': 266.27693073749407,
                        'carwash': 18.95934928920844,
                        'parking': 370.03803637676873,
                        'antifreeze_liquid': 3.4585846547818377,
                        'flat_roads': 0.0,
                        'daily_distance': 113.70139619379054,
                        'rental_expense': 2559.2366666666667,
                        'amortization': 308.7912212044606,
                        'transport_tax': 0.8306247742867461,
                        'casco': 13.21448504547096,
                        'trailer': 0.0,
                        'engine_oil': 7.648248465149874,
                        'brake_fluid': 3.7709350546831963,
                        'spark_plugs': 6.474838262422125,
                        'filters': 15.74415881458135,
                        'fuel_filters': 16.03527145780667,
                        'timing_belts': 0.0,
                        'brake_pads': 3287.701095461659,
                        'other_cost': 541.1875166464482,
                        'fire_extinguishers': 5.751195596266019,
                        'first_aid_kits': 2.8466025367433816,
                        'calculated_salary': 1300.5156116965975}
                    },
                'Автобусы': {
                    'all_expenses': 24268.662149859232,
                    'expenses_structure': {
                        'fuel': 10995.86672548945,
                        'repair': 1837.3279332581142,
                        'insurance': 16.922560439142504,
                        'fines': 241.79151253153486,
                        'maintenance': 5.097021547225146,
                        'tire_service': 309.786130162312,
                        'carwash': 25.534322291384072,
                        'parking': 505.76517720786154,
                        'antifreeze_liquid': 3.4409697730336415,
                        'flat_roads': 272.96753920462464,
                        'daily_distance': 113.2089286393428,
                        'rental_expense': 1760.6943564356432,
                        'amortization': 2722.7670152085634,
                        'transport_tax': 6.8795611955561755,
                        'casco': 211.20421901345065,
                        'trailer': 0.0,
                        'engine_oil': 7.652434257510258,
                        'brake_fluid': 3.8262106697252207,
                        'spark_plugs': 6.3348526213472445,
                        'filters': 15.709144153852005,
                        'fuel_filters': 15.641341598948626,
                        'timing_belts': 0.0,
                        'brake_pads': 3585.966574122201,
                        'other_cost': 532.8068699211465,
                        'fire_extinguishers': 5.691628317761354,
                        'first_aid_kits': 2.828015195885786,
                        'calculated_salary': 1062.9511066036182}
                    },
                'Каршеринг': {
                    'all_expenses': 17659.767930180187,
                    'expenses_structure': {
                        'fuel': 8595.443782586428,
                        'repair': 1923.9232755441747,
                        'insurance': 17.0188139991464,
                        'fines': 233.08227827571488,
                        'maintenance': -1.0,
                        'tire_service': 272.4352344003414,
                        'carwash': 18.907465727699535,
                        'parking': 350.02215168587276,
                        'antifreeze_liquid': 3.4619933418693987,
                        'flat_roads': -1.0,
                        'daily_distance': 113.30358924455822,
                        'rental_expense': 246.385,
                        'amortization': 2081.5252408877504,
                        'transport_tax': 4.880921895006402,
                        'casco': 86.64105847204439,
                        'trailer': -1.0,
                        'engine_oil': 7.779026888604354,
                        'brake_fluid': 3.947314067372843,
                        'spark_plugs': 6.179068351577732,
                        'filters': 15.749995731967562,
                        'fuel_filters': 15.249918907383696,
                        'timing_belts': -1.0,
                        'brake_pads': 3127.1941271873666,
                        'other_cost': 527.7651164338777,
                        'fire_extinguishers': 5.933324797268459,
                        'first_aid_kits': 2.9392317541613315,
                        'calculated_salary': -1.0}
                    },
                'Другое': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': -1.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': -1.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': -1.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                    }
                }

            self.market_averages = self.market_averages[industry]

    def salary_data(self):
        grouped = self.expenses.groupby(by=['month', 'driver'])['calculated_salary'].sum().reset_index()

        salary_pivot = grouped.pivot(index='month', columns='driver', values='calculated_salary').fillna(0)

        result = {}

        for month_period in salary_pivot.index:
            year = month_period.year
            month_str = f"{month_period.month:02d}"

            month_data = salary_pivot.loc[month_period].to_dict()

            if year not in result:
                result[year] = {}
            result[year][month_str] = month_data

        # with open("salary_data.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result


    def fuel_km_per_month(self):
        expenses_data = self.expenses.groupby(by=['month'])[['fuel', 'daily_distance']].sum().reset_index()
        expenses_data['fuel_effectiveness'] = expenses_data['fuel'] / expenses_data['daily_distance']
        res = dict()
        for i in range(expenses_data.shape[0]):
            res[expenses_data.iloc[i]['month'].strftime("%Y")] = {}
        for i in range(expenses_data.shape[0]):
            res[expenses_data.iloc[i]['month'].strftime("%Y")][expenses_data.iloc[i]['month'].strftime("%m")] = {
                    "fuel_in_month": float(expenses_data.iloc[i]['fuel']),
                    "distance_in_month": float(expenses_data.iloc[i]['daily_distance']),
                    "fuel_effectiveness_in_month": float(expenses_data.iloc[i]['fuel_effectiveness'])
            }

        # with open("fuel_km_per_month.json", "w", encoding="utf-8") as file:
        #     json.dump(res, file, ensure_ascii=False, indent=4)

        return res

    def logistic_expenses_by_month(self):
        grouped = self.expenses.groupby(by=['month'])[['cargo_insurance', 'temp_control_maintenance', 'customs_fees', 'transloading_fees']].sum().reset_index()

        result = {}
        for _, row in grouped.iterrows():
            year = row['month'].year
            month = f"{row['month'].month:02d}"

            if year not in result:
                result[year] = {}

            result[year][month] = {
                "cargo_insurance": float(row['cargo_insurance']),
                "temp_control_maintenance": float(row['temp_control_maintenance']),
                "customs_fees": float(row['customs_fees']),
                "transloading_fees": float(row['transloading_fees']),
                }

        # with open("logistic_expenses_by_month.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def pharma_expenses_by_month(self):
        grouped = self.expenses.groupby(by=['month'])[[
            'sterilization_costs',
            'pharma_licenses',
            'temp_control_maintenance'
        ]].sum().reset_index()

        result = {}
        for _, row in grouped.iterrows():
            year = row['month'].year
            month = f"{row['month'].month:02d}"

            if year not in result:
                result[year] = {}

            result[year][month] = {
                "sterialization_cost": float(row['sterilization_costs']),
                "pharma_licenses_cost": float(row['pharma_licenses']),
                "temperature_control_maintance": float(row['temp_control_maintenance'])
            }

        # with open("pharma_expenses_by_month.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def building_expenses_by_month(self):
        grouped = self.expenses.groupby(['month'])[
            ['bucket_parts', 'special_lubricants']
        ].sum().reset_index()

        result = {}
        for _, row in grouped.iterrows():
            year = row['month'].year
            month = f"{row['month'].month:02d}"

            if year not in result:
                result[year] = {}

            result[year][month] = {
                'bucket_parts': float(row['bucket_parts']),
                'special_lubricants': float(row['special_lubricants'])
            }

        # with open("building_expenses_by_month.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def vehicle_usage_by_month(self):
        used_data = self.expenses[self.expenses['daily_distance'] > 0].copy()

        grouped = used_data.groupby(['month', 'gov_number']).agg(
            days_used=('daily_distance', 'count'),
            total_distance=('daily_distance', 'sum')
        ).reset_index()

        result = {}
        for _, row in grouped.iterrows():
            year_val = row['month'].year
            month_val = f"{row['month'].month:02d}"
            gov_number = row['gov_number']

            if year_val not in result:
                result[year_val] = {}
            if month_val not in result[year_val]:
                result[year_val][month_val] = {}

            result[year_val][month_val][gov_number] = {
                "days_used": int(row['days_used']),
                "total_distance": float(row['total_distance'])
            }

        # with open("vehicle_usage_by_month.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    # def downtime_counter(self):
    #     expenses_data = self.expenses[self.expenses['daily_distance'] == 0]
    #
    #     grouped_data = pd.concat([expenses_data[['month', 'gov_number']], expenses_data.select_dtypes(include=['int64', 'float64'])])
    #     grouped_data = grouped_data.groupby(by=['month', 'gov_number']).sum().agg(['sum'], axis=1)
    #     grouped_data['count'] = expenses_data.groupby(by=['month', 'gov_number']).count().iloc[:, 0]
    #     grouped_data['zipped_data'] = grouped_data[['sum', 'count']].values.tolist()
    #     grouped_data = grouped_data.reset_index().pivot(index='month', columns='gov_number', values='zipped_data')
    #     res = dict()
    #     for id in grouped_data.index:
    #         res[id.strftime("%Y")] = {}
    #     for id in grouped_data.index:
    #         for auto_number in grouped_data.loc[id].index:
    #             if id.strftime("%m") not in res[id.strftime("%Y")].keys():
    #                 res[id.strftime("%Y")][id.strftime("%m")] = {}
    #             res[id.strftime("%Y")][id.strftime("%m")][auto_number] = {"sum": grouped_data.loc[id][auto_number][0] if isinstance(grouped_data.loc[id][auto_number], list) else 0,
    #                                                                       "count": grouped_data.loc[id][auto_number][1] if isinstance(grouped_data.loc[id][auto_number], list) else 0}
    #
    #     # with open("downtime.json", "w", encoding="utf-8") as file:
    #     #     json.dump(res, file, ensure_ascii=False, indent=4)
    #
    #     return res

    def downtime_counter(self):
        expenses_data = self.expenses[self.expenses['daily_distance'] == 0]

        # Исправлено: обернул аргументы в список для pd.concat
        grouped_data = pd.concat([
            expenses_data[['month', 'gov_number']],
            expenses_data.select_dtypes(include=['int64', 'float64'])
        ], axis=1)  # Добавлен axis=1 для объединения по столбцам

        grouped_data = grouped_data.groupby(by=['month', 'gov_number']).sum()
        grouped_data['count'] = expenses_data.groupby(by=['month', 'gov_number']).count().iloc[:, 0]
        grouped_data['zipped_data'] = grouped_data[['sum', 'count']].values.tolist()
        grouped_data = grouped_data.reset_index().pivot(index='month', columns='gov_number', values='zipped_data')

        res = dict()
        for id in grouped_data.index:
            res[id.strftime("%Y")] = {}

        for id in grouped_data.index:
            for auto_number in grouped_data.loc[id].index:
                if id.strftime("%m") not in res[id.strftime("%Y")].keys():
                    res[id.strftime("%Y")][id.strftime("%m")] = {}
                res[id.strftime("%Y")][id.strftime("%m")][auto_number] = {
                    "sum": grouped_data.loc[id][auto_number][0] if isinstance(grouped_data.loc[id][auto_number],
                                                                              list) else 0,
                    "count": grouped_data.loc[id][auto_number][1] if isinstance(grouped_data.loc[id][auto_number],
                                                                                list) else 0
                }

        return res

    def downtime_finder(self):
        filtered = self.expenses[self.expenses['daily_distance'] == 0].copy()

        numeric_cols = filtered.select_dtypes(include=['int64','float64']).columns
        filtered['total_expense'] = filtered[numeric_cols].sum(axis=1)

        grouped = filtered.groupby(['year', 'month', 'gov_number'])['total_expense'].agg(['sum', 'count']).reset_index()

        result = {}
        for index, row in grouped.iterrows():
            year_val = row['year'].year
            month_val = row['month'].strftime("%m")

            if year_val not in result:
                result[year_val] = {}
            if month_val not in result[year_val]:
                result[year_val][month_val] = {}

            result[year_val][month_val][row['gov_number']] = {
                'sum': row['sum'],
                'count': row['count']
            }

        # with open("downtime_find.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def aggregate_expenses_by_month_year(self):
        df = self.expenses.copy()

        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

        non_bool_cols = []
        for col in numeric_cols:
            unique_vals = df[col].dropna().unique()
            if set(unique_vals).issubset({0, 1}):
                continue
            non_bool_cols.append(col)

        if 'daily_distance' in non_bool_cols:
            non_bool_cols.remove('daily_distance')

        non_bool_cols.extend(['year', 'month'])

        grouped = df[non_bool_cols].groupby(['year', 'month']).sum()

        result = {}
        for (year_period, month_period), row in grouped.iterrows():
            year = year_period.year
            month = f"{month_period.month:02d}"

            if year not in result:
                result[year] = {}

            month_expenses = {}
            for col in non_bool_cols:
                if col in ['year', 'month']:
                    continue
                month_expenses[col] = float(row[col])

            result[year][month] = month_expenses

        # with open("aggregated_expenses_by_month_year.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def vehicle_efficiency_per_year(self):
        df = self.expenses.copy()

        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

        expense_cols = []
        for col in numeric_cols:
            unique_vals = df[col].dropna().unique()
            if set(unique_vals).issubset({0, 1}):
                continue
            if col != 'daily_distance':
                expense_cols.append(col)

        df['total_expenses'] = df[expense_cols].sum(axis=1)

        grouped = df.groupby(['year', 'gov_number']).apply(
            lambda x: pd.Series({
                'total_distance': x['daily_distance'].sum(),
                'total_expenses': x['total_expenses'].sum()
            })
        ).reset_index()

        grouped['efficiency'] = grouped['total_distance'] / grouped['total_expenses'] * 100

        result = {}
        for _, row in grouped.iterrows():
            year = row['year'].year
            gov_number = row['gov_number']
            efficiency = row['efficiency']

            if year not in result:
                result[year] = {}

            result[year][gov_number] = float(efficiency)

        # with open("vehicle_efficiency_per_year.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def amortization_by_month_year(self):
        amort_data = self.expenses.groupby(
            ['year', 'month']
        )['amortization'].sum().reset_index()

        result = {}
        for _, row in amort_data.iterrows():
            year = row['year'].year
            month = f"{row['month'].month:02d}"
            amort_value = float(row['amortization'])

            if year not in result:
                result[year] = {}
            result[year][month] = {"amortization": amort_value}

        # with open("amortization_by_month_year.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def repair_expenses_by_month_year(self):
        df = self.expenses.copy()

        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

        non_repair_columns = [
            'daily_distance', 'amortization', 'transport_tax', 'casco',
            'calculated_salary', 'fuel', 'fines', 'rental_expense', 'insurance',
            'parking', 'carwash', 'flat_roads', 'fire_extinguishers','first_aid_kits',
            'temp_control_maintenance', 'customs_fees', 'transloading_fees,',
            'sterilization_costs', 'pharma_licenses',
        ]

        repair_cols = [col for col in numeric_cols if col not in non_repair_columns]

        final_repair_cols = []
        for col in repair_cols:
            unique_vals = df[col].nunique()
            if unique_vals > 2:
                final_repair_cols.append(col)

        final_repair_cols.extend(['year', 'month'])

        grouped = df[final_repair_cols].groupby(['year', 'month']).sum()

        result = {}
        for (year_period, month_period), row in grouped.iterrows():
            year = year_period.year
            month = f"{month_period.month:02d}"

            if year not in result:
                result[year] = {}

            total_repair = row.drop(['year', 'month'], errors='ignore').sum()

            structure = {}
            for col in final_repair_cols:
                if col in ['year', 'month']:
                    continue
                structure[col] = float(row[col])

            result[year][month] = {
                'total': float(total_repair),
                'structure': structure
            }

        # with open("repair_expenses_by_month_year.json", "w", encoding="utf-8") as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)

        return result

    def _normalize_series(self, s: pd.Series):
        return (s - s.min()) / (s.max() - s.min() + 1e-9)

    def top_drivers_fines_by_month(self, top_n=None):
        all_fines = self.expenses.copy()
        all_fines['month'] = all_fines['date'].dt.month.astype(str).str.zfill(2)
        all_fines['year'] = all_fines['date'].dt.year.astype(str)
        grouped = (
            all_fines
            .groupby(['year', 'month', 'driver'])['fines']
            .agg(sum='sum', count_nonzero=lambda x: (x > 0).sum())
            .reset_index()
            .rename(columns={'count_nonzero': 'count'})
        )
        result = {}
        for (year, month), group in grouped.groupby(['year', 'month']):
            sums = group.set_index('driver')['sum'].to_dict()
            counts = group.set_index('driver')['count'].to_dict()
            sorted_drivers = sorted(sums.items(), key=lambda x: x[1], reverse=True)
            if top_n:
                sorted_drivers = sorted_drivers[:top_n]
            month_stats = {
                driver: {"sum": sums[driver], "count": counts[driver]}
                for driver, _ in sorted_drivers
            }
            result.setdefault(year, {})[month] = month_stats
        # with open("top_drivers_fines_by_month.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=4)
        return result

    def top_drivers_salary_efficiency_by_month(self, top_n=None):
        df = self.expenses.copy()
        df['month'] = df['date'].dt.month.astype(str).str.zfill(2)
        df['year'] = df['date'].dt.year.astype(str)
        grouped = (
            df
            .groupby(['year', 'month', 'driver'])
            .agg(
                total_distance=('daily_distance', 'sum'),
                total_salary=('calculated_salary', 'sum')
            )
            .reset_index()
        )
        grouped = grouped[grouped['total_salary'] > 0]
        grouped['raw_salary_eff'] = grouped['total_distance'] / grouped['total_salary']
        grouped['salary_eff_norm'] = self._normalize_series(grouped['raw_salary_eff'])
        result = {}
        for (year, month), g in grouped.groupby(['year', 'month']):
            effs = g.set_index('driver')['salary_eff_norm'].to_dict()
            sorted_effs = dict(sorted(effs.items(), key=lambda x: x[1], reverse=True))
            if top_n:
                sorted_effs = dict(list(sorted_effs.items())[:top_n])
            result.setdefault(year, {})[month] = sorted_effs
        # with open("top_drivers_salary_efficiency.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=4)

        return result

    def seasonal_fines(self):
        df = self.expenses.copy()
        df['month'] = df['date'].dt.month.astype(str).str.zfill(2)
        df['year'] = df['date'].dt.year.astype(str)

        result = {}
        seasons = {
            '1 квартал': ['01', '02', '03'],
            '2 квартал': ['06', '04', '05'],
            '3 квартал': ['09', '07', '08'],
            '4 квартал': ['12', '10', '11']
        }

        for year, group in df.groupby('year'):
            monthly = group.groupby('month')['fines'].sum().to_dict()
            monthly = {month: float(value) for month, value in monthly.items()}
            seasonal_sums = {
                season: sum(monthly.get(m, 0.0) for m in months)
                for season, months in seasons.items()
            }
            max_season = max(seasonal_sums, key=seasonal_sums.get)
            result[year] = {
                "monthly_fines": monthly,
                "seasonal_sums": seasonal_sums,
                "max_season": max_season
            }

        # with open("seasonal_fines.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=4)

        return result


def process_client_data(**kwargs):
    """Process data for a single client"""
    client = kwargs['client']
    client_id = client['companyid']
    budget = client['yearBudget']
    industry = client['companyType']

    print('client_id: ', client_id)
    print('yearBudget: ', budget)
    print('companyType: ', industry)

    print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

    try:
        # 1. Проверяем существование таблиц
        if not check_client_tables(client_id):
            logger.warning(f"Skipping client {client_id} - schema or tables don't exist")
            return False

        # 2. Создаем таблицу для результатов (если нужно)
        create_result_table(client_id)

        # 3. Получаем данные клиента
        hook = PostgresHook(postgres_conn_id=OUR_CONN_ID)
        # expenses_df = hook.get_pandas_df(f"SELECT * FROM {client_id}.expenses", hook.get_sqlalchemy_engine(), chunksize=10000)

        # expenses_chunks = []
        # for chunk in pd.read_sql(f"SELECT * FROM {client_id}.expenses", hook.get_sqlalchemy_engine(), chunksize=100000):
        #     expenses_chunks.append(chunk)
        #     gc.collect()


        # expenses_df = pd.concat(expenses_chunks)
        expenses_df = pd.read_sql(f"SELECT * FROM {client_id}.expenses LIMIT 30000", hook.get_sqlalchemy_engine())
        # expenses_df = pd.read_sql(f"SELECT * FROM {client_id}.expenses", hook.get_sqlalchemy_engine(), chunksize=30000)
        # del expenses_chunks
        gc.collect()

        transport_df = hook.get_pandas_df(f"SELECT * FROM {client_id}.transport")

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        # 4. Выполняем анализ
        ff = fraud_finder(
            expenses=expenses_df,
            transport=transport_df,
            budget=budget,
            industry=industry
        )

        pdm = PlotDataMaker(
            expenses=expenses_df,
            transport=transport_df,
            budget=budget,
            industry=industry
        )

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        results = {}

        if industry == 'Строительство':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'building_expenses_by_month': pdm.building_expenses_by_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),

            }
        elif industry == 'Логистика':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'logistic_expenses_by_month': pdm.logistic_expenses_by_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Фармацевтика':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'pharma_expenses_by_month': pdm.pharma_expenses_by_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Ритейл':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Такси':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Доставка':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Каршеринг':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                # 'salary_effectiveness': ff.salary_effectivness(),
                # 'month_drivers_fine': ff.month_drivers_fine(),
                # 'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                # 'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'vehicle_usage_by_month': pdm.vehicle_usage_by_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                # 'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                # 'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                # 'seasonal_fines': pdm.seasonal_fines(),
            }
        elif industry == 'Другое':
            results = {
                'high_salary_data': ff.find_high_salary(),
                'expenses_data': ff.budget_deviations(),
                'expenses_with_market_avg': ff.deviation_from_market_indicators(),
                'fuel_effectiveness': ff.fuel_effectivness(),
                'salary_effectiveness': ff.salary_effectivness(),
                'month_drivers_fine': ff.month_drivers_fine(),
                'year_drivers_fine': ff.year_drivers_fine(),
                'analyze_and_plot': ff.analyze_and_plot(),
                'recommendations': ff.generate_recommendations(),
                'salary_data': pdm.salary_data(),
                'fuel_km_per_month': pdm.fuel_km_per_month(),
                'downtime': pdm.downtime_counter(),
                'downtime_find': pdm.downtime_finder(),
                'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
                'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
                'amortization_by_month_year': pdm.amortization_by_month_year(),
                'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
                'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
                'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
                'seasonal_fines': pdm.seasonal_fines(),
            }

        # results = {
        #     'high_salary_data': ff.find_high_salary(),
        #     'expenses_data': ff.budget_deviations(),
        #     'expenses_with_market_avg': ff.deviation_from_market_indicators(),
        #     'fuel_effectiveness': ff.fuel_effectivness(),
        #     'salary_effectiveness': ff.salary_effectivness(),
        #     'month_drivers_fine': ff.month_drivers_fine(),
        #     'year_drivers_fine': ff.year_drivers_fine(),
        #     'analyze_and_plot': ff.analyze_and_plot(),
        #     'salary_data': pdm.salary_data(),
        #     'fuel_km_per_month': pdm.fuel_km_per_month(),
        #     'logistic_expenses_by_month': pdm.logistic_expenses_by_month(),
        #     'pharma_expenses_by_month': pdm.pharma_expenses_by_month(),
        #     'building_expenses_by_month': pdm.building_expenses_by_month(),
        #     'vehicle_usage_by_month': pdm.vehicle_usage_by_month(),
        #     'downtime': pdm.downtime_counter(),
        #     'downtime_find': pdm.downtime_finder(),
        #     'aggregated_expenses_by_month_year': pdm.aggregate_expenses_by_month_year(),
        #     'vehicle_efficiency_per_year': pdm.vehicle_efficiency_per_year(),
        #     'amortization_by_month_year': pdm.amortization_by_month_year(),
        #     'repair_expenses_by_month_year': pdm.repair_expenses_by_month_year(),
        #     'top_drivers_fines_by_month': pdm.top_drivers_fines_by_month(),
        #     'top_drivers_salary_efficiency': pdm.top_drivers_salary_efficiency_by_month(),
        #     'seasonal_fines': pdm.seasonal_fines(),
        # }

        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")

        # 5. Сохраняем результаты в БД
        save_results_to_db(client_id, results)
        print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")
        return True

    except AirflowSkipException as e:
        logger.warning(f"Skipping client {client_id}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error processing client {client_id}: {str(e)}")
        return False
    finally:
        # Явное освобождение памяти
        if 'expenses_df' in locals():
            del expenses_df
        if 'transport_df' in locals():
            del transport_df
        gc.collect()

def process_all_clients(**kwargs):
    """Process data for all clients"""
    ti = kwargs['ti']
    clients = ti.xcom_pull(task_ids='get_all_clients', key='clients')

    success_count = 0
    skipped_count = 0
    failed_count = 0

    for client in clients:
        try:
            print(psutil.Process().memory_info().rss / 1024 ** 2, "MB")
            result = process_client_data(client=client)
            if result:
                success_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            logger.error(f"Unexpected error processing client {client['companyid']}: {str(e)}")
            failed_count += 1

    logger.info(f"Processing summary: {len(clients)} total, {success_count} successful, "
                f"{skipped_count} skipped, {failed_count} failed")

    return success_count > 0



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
    dag_id="fraud_find_dag",
    description="находит аномалии, делает рекомендации, складывает в таблицу",
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    get_clients_task = PythonOperator(
        task_id="get_all_clients",
        python_callable=get_all_clients,
        provide_context=True,
    )

    process_clients_task = PythonOperator(
        task_id="process_all_clients",
        python_callable=process_all_clients,
        provide_context=True,
    )


# ── зависимости ──
get_clients_task >> process_clients_task