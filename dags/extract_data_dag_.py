from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from datetime import datetime


def load_csv_to_postgres():
    # Путь к файлу CSV
    file_path = "/opt/airflow/dags/data/med_production_data.csv"  # путь в контейнере Airflow
    table_name = "stg.med_production"

    # Подключение
    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()

    # Загрузка CSV и автоматическое определение структуры
    df = pd.read_csv(file_path, encoding='cp1251')

    # Загружаем в таблицу (перезапись)
    df.to_sql(name="med_production", con=engine, schema="stg", if_exists="replace", index=False)

with DAG(
    dag_id="extract_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["extract"],
):

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_knir",
        sql="""
        CREATE SCHEMA IF NOT EXISTS STG;
        CREATE SCHEMA IF NOT EXISTS ODS;
        CREATE SCHEMA IF NOT EXISTS DM;
        """
    )

    load_csv = PythonOperator(
        task_id="load_csv_to_stg",
        python_callable=load_csv_to_postgres
    )

    # trigger_next_dag = TriggerDagRunOperator(
    #     task_id="trigger_transform_dag",
    #     trigger_dag_id="transform_data",  # имя следующего дага
    #     wait_for_completion=False
    # )

    create_schema >> load_csv #>> trigger_next_dag

