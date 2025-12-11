from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger("hypothesis_logger")
logger.setLevel(logging.INFO)

ODS_TABLE = "ods.med_production"

def fetch_from_ods():
    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql(f"SELECT * FROM {ODS_TABLE}", con=engine)
    return df.to_json()


def hypothesis_purity_vs_params(**context):
    """
    Гипотеза 1: Чистота меди зависит от параметров производственного процесса.
    Группируем партии по каждому параметру (4 группы) и рассчитываем среднюю чистоту.
    """
    df = pd.read_json(context['ti'].xcom_pull(task_ids="fetch_data", key="return_value"))
    df = df[df['Completed'] == 1]

    # Справочник для русских названий параметров
    param_labels = {
        'SmeltTemp_C':         'Температура плавки',
        'SmeltDuration_h':     'Длительность плавки',
        'RefineDuration_h':    'Длительность рафинирования',
        'RefineEnergy_kWh':    'Энергозатраты рафинирования (кВт·ч)',
        'CoolingTime_h':       'Длительность охлаждения',
        'HeatTreatTemp_C':     'Температура термообработки',
        'HeatTreatDuration_h': 'Длительность термообработки',
    }

    result = []
    features = list(param_labels.keys())

    for col in features:
        try:
            if df[col].nunique() <= 1:
                continue

            q1 = df[col].quantile(0.25)
            q2 = df[col].quantile(0.5)
            q3 = df[col].quantile(0.75)
            bins = [df[col].min() - 1, q1, q2, q3, df[col].max() + 1]
            labels = [f"{round(bins[i] + 1, 1)}–{round(bins[i+1], 1)}" for i in range(4)]

            df["bin_group"] = pd.cut(df[col], bins=bins, labels=labels, include_lowest=True)

            temp = (
                df.groupby("bin_group")
                  .agg(avg_purity=("Purity_pct", "mean"))
                  .reset_index()
            )
            temp = temp.dropna(subset=["avg_purity"])
            temp["avg_purity"] = temp["avg_purity"].round(2)

            temp.rename(columns={"bin_group": "range"}, inplace=True)
            temp["parameter"] = col
            temp["parameter_ru"] = param_labels.get(col, col)

            result.append(temp)
        except Exception as e:
            logger.warning(f"Ошибка при обработке {col}: {e}")
            continue

    if result:
        final = pd.concat(result, ignore_index=True)

        # Ранги внутри каждого параметра по чистоте
        final["rank_by_param"] = (
            final.groupby("parameter")["avg_purity"]
                 .rank(ascending=False, method="dense")
                 .astype(int)
        )

        hook = PostgresHook(postgres_conn_id="postgres_knir")
        engine = hook.get_sqlalchemy_engine()
        final.to_sql("dm_purity_by_param", engine, schema="dm", if_exists="replace", index=False)
        logger.info("[Гипотеза 1] Чистота по параметрам сохранена в dm.dm_purity_by_param")



def hypothesis_yield_vs_params(**context):
    """
    Гипотеза 2: Потери металла (выход) зависят от параметров процесса.
    Группируем по каждому параметру (4 группы) и считаем средний выход.
    """
    df = pd.read_json(context['ti'].xcom_pull(task_ids="fetch_data", key="return_value"))
    df = df[df['Completed'] == 1]
    df["Yield"] = df["FinalMass_kg"] / df["InputMass_kg"]

    param_labels = {
        'SmeltTemp_C':         'Температура плавки',
        'SmeltDuration_h':     'Длительность плавки',
        'RefineDuration_h':    'Длительность рафинирования',
        'RefineEnergy_kWh':    'Энергозатраты рафинирования (кВт·ч)',
        'CoolingTime_h':       'Длительность охлаждения',
        'HeatTreatTemp_C':     'Температура термообработки',
        'HeatTreatDuration_h': 'Длительность термообработки',
    }

    result = []
    features = list(param_labels.keys())

    for col in features:
        try:
            if df[col].nunique() <= 1:
                continue

            q1 = df[col].quantile(0.25)
            q2 = df[col].quantile(0.5)
            q3 = df[col].quantile(0.75)
            bins = [df[col].min() - 1, q1, q2, q3, df[col].max() + 1]
            labels = [f"{round(bins[i] + 1, 1)}–{round(bins[i+1], 1)}" for i in range(4)]

            df["bin_group"] = pd.cut(df[col], bins=bins, labels=labels, include_lowest=True)

            temp = (
                df.groupby("bin_group")
                  .agg(avg_yield=("Yield", "mean"))
                  .reset_index()
            )
            temp = temp.dropna(subset=["avg_yield"])
            temp["avg_yield"] = temp["avg_yield"].round(3)

            temp.rename(columns={"bin_group": "range"}, inplace=True)
            temp["parameter"] = col
            temp["parameter_ru"] = param_labels.get(col, col)

            result.append(temp)
        except Exception as e:
            logger.warning(f"Ошибка при обработке {col}: {e}")
            continue

    if result:
        final = pd.concat(result, ignore_index=True)

        # Ранги внутри каждого параметра по выходу
        final["rank_by_param"] = (
            final.groupby("parameter")["avg_yield"]
                 .rank(ascending=False, method="dense")
                 .astype(int)
        )

        hook = PostgresHook(postgres_conn_id="postgres_knir")
        engine = hook.get_sqlalchemy_engine()
        final.to_sql("dm_yield_by_param", engine, schema="dm", if_exists="replace", index=False)
        logger.info("[Гипотеза 2] Выход по параметрам сохранён в dm.dm_yield_by_param")



def hypothesis_failures_by_stage(**context):
    """
    Гипотеза 3: Сбои чаще всего происходят на ранних этапах.
    Определяем, на каком шаге произошёл обрыв и считаем количество.
    """
    df = pd.read_json(context['ti'].xcom_pull(task_ids="fetch_data", key="return_value"))
    df = df[df['Completed'] == 0]

    def failed_stage(row):
        if pd.isnull(row["SmeltTemp_C"]) or pd.isnull(row["SmeltDuration_h"]):
            return "Плавка"
        elif pd.isnull(row["RefineDuration_h"]) or pd.isnull(row["RefineEnergy_kWh"]):
            return "Рафинирование"
        elif pd.isnull(row["CoolingTime_h"]):
            return "Охлаждение"
        elif pd.isnull(row["HeatTreatTemp_C"]) or pd.isnull(row["HeatTreatDuration_h"]):
            return "Термообработка"
        else:
            return "Неизвестно"

    df["FailedStep"] = df.apply(failed_stage, axis=1)
    agg = df.groupby("FailedStep").agg(failures=("BatchID", "count")).reset_index()

    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()
    agg.to_sql("dm_failures_by_stage", engine, schema="dm", if_exists="replace", index=False)
    logger.info("[Гипотеза 3] Распределение сбоев по этапам сохранено в dm.dm_failures_by_stage")



def summary_current_state(**context):
    """
    Витрина: Текущее состояние производства.
    Показывает общее число партий, долю брака и среднюю чистоту.
    """
    df = pd.read_json(context['ti'].xcom_pull(task_ids="fetch_data", key="return_value"))
    total = len(df)
    completed = df['Completed'].sum()
    failed = total - completed
    defect_rate = failed / total if total > 0 else 0
    avg_purity = df[df['Completed'] == 1]['Purity_pct'].mean()

    summary = pd.DataFrame([{
        'total_batches': total,
        'completed': completed,
        'failed': failed,
        'defect_rate': round(defect_rate, 3),
        'avg_purity': round(avg_purity, 2)
    }])

    hook = PostgresHook(postgres_conn_id="postgres_knir")
    engine = hook.get_sqlalchemy_engine()
    summary.to_sql("dm_summary_state", engine, schema="dm", if_exists="replace", index=False)
    logger.info("[Витрина] Сводная таблица состояния сохранена в dm.dm_summary_state")




with DAG(
    dag_id="dm_build_hypotheses",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dm", "hypotheses"]
) as dag:

    fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_from_ods)

    h1 = PythonOperator(task_id="hypothesis_purity_vs_params", python_callable=hypothesis_purity_vs_params)
    h2 = PythonOperator(task_id="hypothesis_yield_vs_params", python_callable=hypothesis_yield_vs_params)
    h3 = PythonOperator(task_id="hypothesis_failures_by_stage", python_callable=hypothesis_failures_by_stage)
    h4 = PythonOperator(task_id="summary_current_state", python_callable=summary_current_state)

    fetch >> [h1, h2, h3, h4]
