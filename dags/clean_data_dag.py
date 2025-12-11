from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

# Инициализация логгера
logger = logging.getLogger("transform_logger")
logger.setLevel(logging.INFO)

DDS_TABLE = "ods.med_production"
STG_TABLE = "stg.med_production"


def fetch_data_from_stg(**context):
    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql(f"SELECT * FROM {STG_TABLE}", con=engine)
    logger.info("[Загрузка] Данные считаны из STG")
    return df.to_json()


def remove_duplicates(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="fetch_data", key="return_value"))
    before = len(df)
    df.drop_duplicates(inplace=True)
    logger.info(f"[Шаг 1] Удалено дубликатов: {before - len(df)}")
    return df.to_json()


def remove_incomplete_completed(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="remove_duplicates", key="return_value"))
    mask = (df["Completed"] == 1) & (df.isnull().any(axis=1))
    removed = mask.sum()
    df = df[~mask]
    logger.info(f"[Шаг 2] Удалено строк с Completed=1 и неполными данными: {removed}")
    return df.to_json()


def mass_consistency(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="remove_incomplete_completed", key="return_value"))
    mask = df["FinalMass_kg"] > df["InputMass_kg"]
    removed = mask.sum()
    df = df[~mask]
    logger.info(f"[Шаг 3] Удалено строк, где FinalMass > InputMass: {removed}")
    return df.to_json()


def normalize_quality(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="mass_consistency", key="return_value"))
    df["Quality"] = df["Quality"].astype(str).str.strip().str.title()
    logger.info("[Шаг 4] Столбец 'Quality' приведён к единому формату")
    return df.to_json()


def remove_negatives(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="normalize_quality", key="return_value"))
    numeric_cols = df.select_dtypes(include="number").columns
    mask = (df[numeric_cols] < 0).any(axis=1)
    removed = mask.sum()
    df = df[~mask]
    logger.info(f"[Шаг 5] Удалено строк с отрицательными значениями: {removed}")
    return df.to_json()


def remove_outliers(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="remove_negatives", key="return_value"))
    cols_to_check = ["SmeltTemp_C"]
    removed_total = 0

    for col in cols_to_check:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = (df["Completed"] == 1) & ((df[col] < lower) | (df[col] > upper))
        count = mask.sum()
        removed_total += count
        df = df[~mask]
        logger.info(f"[Шаг 6] Удалено выбросов в {col}: {count}")

    logger.info(f"[Шаг 6] Всего удалено выбросов: {removed_total}")
    return df.to_json()


def load_to_dds(**context):
    df = pd.read_json(context['ti'].xcom_pull(task_ids="remove_outliers", key="return_value"))
    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name="med_production",
        con=engine,
        schema="ods",
        if_exists="replace",
        index=False
    )
    logger.info("[Шаг 7] Данные загружены в таблицу ODS")


with DAG(
    dag_id="transform_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transform"]
) as dag:

    fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_data_from_stg)
    t1 = PythonOperator(task_id="remove_duplicates", python_callable=remove_duplicates)
    t2 = PythonOperator(task_id="remove_incomplete_completed", python_callable=remove_incomplete_completed)
    t3 = PythonOperator(task_id="mass_consistency", python_callable=mass_consistency)
    t4 = PythonOperator(task_id="normalize_quality", python_callable=normalize_quality)
    t5 = PythonOperator(task_id="remove_negatives", python_callable=remove_negatives)
    t6 = PythonOperator(task_id="remove_outliers", python_callable=remove_outliers)
    t7 = PythonOperator(task_id="load_to_dds", python_callable=load_to_dds)

    fetch >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7