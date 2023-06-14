import os
import sys

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

log = logging.getLogger(__name__)

creds = {
    "user": "etl",
    "password": "etl_pass",
    "host": "192.168.0.29",
    "database": "market",
    "port": 5432
}

with DAG(
        dag_id="history_dag_migration",
        start_date=datetime(2023, 5, 30),
        catchup=False,
        tags=["Dag migration"],
        schedule_interval="@daily",
) as dag:

    def migrate():
        from src.pyspark.spark import SparkCustomeSession

        spark = SparkCustomeSession()
        return spark.do_migration()

    def migrate_etln_func():
        from src.pyspark.spark import SparkCustomeSession

        spark = SparkCustomeSession()
        return spark.migrate_item_etln()

    def clear_daily_data():
        from src.db_module.db_connector import Connector
        TABLE_NAMES = ['csgomarket', 'lisskins', 'pairs', 'skinbaron', 'tradeit']

        connector = Connector(creds)
        return connector.truncate_table(TABLE_NAMES)


    migrate_data = PythonOperator(
        task_id="migrate_data", python_callable=migrate
    )

    migrate_etln = PythonOperator(
        task_id="migrate_etln", python_callable=migrate_etln_func
    )

    dummy_operator = DummyOperator(task_id='waiter')

    '''
    clear_data = PythonOperator(
        task_id="clear_data", python_callable=clear_daily_data
    )
    '''

    [migrate_data, migrate_etln] >> dummy_operator
