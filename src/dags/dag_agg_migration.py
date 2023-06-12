import os
import sys

from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


log = logging.getLogger(__name__)

with DAG(
    dag_id="agg_migration",
    start_date=datetime(2023, 5, 30),
    catchup=False,
    tags=["dag agg migration"],
    schedule_interval='@daily'
) as dag:

    def agg_migration_func():
        from src.pyspark.spark import SparkCustomeSession

        spark = SparkCustomeSession()
        return spark.agg_migration()

    wait_for_migration = ExternalTaskSensor(
        task_id='wait_for_migration',
        external_dag_id='history_dag_migration',
        external_task_id='waiter',
        start_date=datetime(2020, 4, 29),
        execution_delta=timedelta(minutes=2),
        timeout=3600,
    )

    agg_migrate_data = PythonOperator(
        task_id="agg_migration_history", python_callable=agg_migration_func
    )

    wait_for_migration >> agg_migrate_data

