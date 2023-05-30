import logging
from datetime import datetime

from airflow import DAG
from parsers.tradeit import TradeIt
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

instance_tradeit = TradeIt()

with DAG(
        dag_id="dag_tradeit",
        start_date=datetime(2023, 5, 30),
        catchup=False,
        tags=["example"],
        schedule_interval='*/10 * * * *'
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=instance_tradeit.update_market_status
    )

    extract_data
