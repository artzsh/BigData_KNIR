# зафиксируй версию своего Airflow
FROM apache/airflow:3.1.0

USER airflow

RUN  pip install --no-cache-dir \
      apache-airflow-providers-postgres \
      pandas

USER airflow
