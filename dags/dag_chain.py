from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="run_all_pipelines",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["trigger"],
) as dag:

    extract = TriggerDagRunOperator(
        task_id="run_extract_data",
        trigger_dag_id="extract_data",
    )

    transform = TriggerDagRunOperator(
        task_id="run_transform_data",
        trigger_dag_id="transform_data",
    )

    dm = TriggerDagRunOperator(
        task_id="run_build_dm",
        trigger_dag_id="dm_build_hypotheses",
    )

    extract >> transform >> dm