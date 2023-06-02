import os
import sys

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
import json

from src.db_module.db_connector import Connector
from src.db_module.db_utils import create_transaction
from src.parsers.tradeit import TradeIt

log = logging.getLogger(__name__)

instance_tradeit = TradeIt()
connector = Connector(json.load(open('../../resources/credentials.json')))

with DAG(
        dag_id="dag_tradeit",
        start_date=datetime(2023, 5, 30),
        catchup=False,
        tags=["example"],
        schedule_interval="*/10 * * * *",
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data", python_callable=instance_tradeit.update_market_status
    )

    write_to_db = PythonOperator(
        task_id="write_data",
        python_callable=create_transaction,
        op_kwargs={
            'data': ti.xcom_pull(task_ids='extract_data'),
            'connector': connector,
            'table_name': 'tradeit'
        }
    )
    extract_data >> write_to_db
