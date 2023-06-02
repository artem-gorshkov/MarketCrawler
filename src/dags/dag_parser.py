import os
import sys

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from src.db_module.db_connector import Connector
from src.db_module.db_utils import create_transaction

from src.parsers.list_skins import LisSkins
from src.parsers.tradeit import TradeIt
from src.parsers.csgo_market import CsGoMarket

log = logging.getLogger(__name__)

connector = Connector({
  "user": "etl",
  "password": "etl_pass",
  "host": "192.168.0.29",
  "database": "market",
  "port": 5432
})

lis_skins_instance = LisSkins()
tradeit_instance = TradeIt()
csgo_market_instance = CsGoMarket()


with DAG(
        dag_id="dag_parses",
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
            'connector': connector,
            'table_name': 'tradeit',
            'task_id': 'extract_data'
        }
    )
    extract_data >> write_to_db
