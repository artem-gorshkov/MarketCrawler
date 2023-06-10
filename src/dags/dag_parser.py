import os
import sys

sys.path.insert(0, (os.path.join(os.path.dirname(__file__), '..', '..')))

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from itertools import combinations

creds = {
    "user": "etl",
    "password": "etl_pass",
    "host": "192.168.0.29",
    "database": "market",
    "port": 5432
}

log = logging.getLogger(__name__)

with DAG(
        dag_id="dag_parses",
        start_date=datetime(2023, 5, 30),
        catchup=False,
        tags=["Data Extraction"],
        schedule_interval="*/20 * * * *",
) as dag:
    def extract_data_tradeit():
        from src.parsers.tradeit import TradeIt
        return TradeIt().update_market_status()


    def extract_data_cs_go_market():
        from src.parsers.csgo_market import CsGoMarket
        return CsGoMarket().update_market_status()


    def extract_data_lis_skins():
        from src.parsers.list_skins import LisSkins
        return LisSkins().update_market_status()


    def extract_data_skin_baron():
        from src.parsers.skinbaron import Skinbaron
        return Skinbaron().update_market_status()


    def create_transaction(**kwargs):
        from src.db_module.db_connector import Connector
        from src.db_module.db_utils import create_transaction

        connector = Connector(creds)
        kwargs |= {'connector': connector}
        return create_transaction(**kwargs)


    def find_pairs(**kwargs):
        from src.db_module.db_connector import Connector
        from src.db_module.db_utils import find_pair

        connector = Connector(creds)
        kwargs |= {'connector': connector}
        find_pair(**kwargs)


    extract_data_tradeit = PythonOperator(
        task_id="extract_data_tradeit", python_callable=extract_data_tradeit
    )

    extract_data_cs_go_market = PythonOperator(
        task_id="extract_data_cs_go_market", python_callable=extract_data_cs_go_market
    )

    extract_data_lis_skins = PythonOperator(
        task_id="extract_data_lis_skins", python_callable=extract_data_lis_skins
    )

    extract_data_skin_baron = PythonOperator(
        task_id='extract_data_skin_baron', python_callable=extract_data_skin_baron
    )

    write_to_db_tradeit = PythonOperator(
        task_id="write_data_tradeit",
        python_callable=create_transaction,
        op_kwargs={
            'table_name': 'tradeit',
            'task_id': 'extract_data_tradeit'
        }
    )

    write_to_db_lis_skins = PythonOperator(
        task_id="write_data_lis_skins",
        python_callable=create_transaction,
        op_kwargs={
            'table_name': 'lisskins',
            'task_id': 'extract_data_lis_skins'
        }
    )

    write_to_db_csgo_market = PythonOperator(
        task_id="write_data_csgo_market",
        python_callable=create_transaction,
        op_kwargs={
            'table_name': 'csgomarket',
            'task_id': 'extract_data_cs_go_market'
        }
    )

    write_to_db_skin_baron = PythonOperator(
        task_id="write_data_skin_baron",
        python_callable=create_transaction,
        op_kwargs={
            'table_name': 'skinbaron',
            'task_id': 'extract_data_skin_baron'
        }
    )

    last_point = {
        'tradeit': write_to_db_tradeit,
        'lisskins': write_to_db_lis_skins,
        'csgomarket': write_to_db_csgo_market,
        'skinbaron': write_to_db_skin_baron
    }

    python_operators = []

    for table_name_1, table_name_2 in combinations(sorted(last_point.keys()), 2):
        python_operators.append(
            PythonOperator(
                task_id=f"find_pair_{table_name_1}_{table_name_2}",
                python_callable=find_pairs,
                op_kwargs={
                    'table_name_1': f'market.{table_name_1}',
                    'table_name_2': f'market.{table_name_2}'
                }
            )
        )

    extract_data_tradeit >> write_to_db_tradeit
    extract_data_lis_skins >> write_to_db_lis_skins
    extract_data_cs_go_market >> write_to_db_csgo_market
    extract_data_skin_baron >> write_to_db_skin_baron

    for idx, nodes in enumerate(combinations(sorted(last_point.keys()), 2)):
        (last_point[nodes[0]], last_point[nodes[1]]) >> python_operators[idx]
