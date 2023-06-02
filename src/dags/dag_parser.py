import os
import sys

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

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
        schedule_interval="*/30 * * * *",
) as dag:
    extract_data_tradeit = PythonOperator(
        task_id="extract_data_tradeit", python_callable=tradeit_instance.update_market_status
    )

    extract_data_cs_go_market = PythonOperator(
        task_id="extract_data_cs_go_market", python_callable=csgo_market_instance.update_market_status
    )

    extract_data_lis_skins = PythonOperator(
        task_id="extract_data_lis_skins", python_callable=lis_skins_instance.update_market_status
    )

    write_to_db_tradeit = PythonOperator(
        task_id="write_data_tradeit",
        python_callable=create_transaction,
        op_kwargs={
            'connector': connector,
            'table_name': 'tradeit',
            'task_id': 'extract_data_tradeit'
        }
    )

    write_to_db_lis_skins = PythonOperator(
        task_id="write_data_lis_skins",
        python_callable=create_transaction,
        op_kwargs={
            'connector': connector,
            'table_name': 'lisskins',
            'task_id': 'extract_data_lis_skins'
        }
    )

    write_to_db_csgo_market = PythonOperator(
        task_id="write_data_csgo_market",
        python_callable=create_transaction,
        op_kwargs={
            'connector': connector,
            'table_name': 'csgomarket',
            'task_id': 'extract_data_cs_go_market'
        }
    )

    kill_chrome = BashOperator(
        task_id="kill_chrome",
        bash_command="pkill chrome",
        trigger_rule=TriggerRule.ALL_DONE
    )

    extract_data_tradeit >> write_to_db_tradeit
    extract_data_cs_go_market >> write_to_db_csgo_market
    extract_data_lis_skins >> write_to_db_lis_skins

    extract_data_cs_go_market >> kill_chrome
    extract_data_lis_skins >> kill_chrome
