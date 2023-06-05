import csv
from datetime import datetime
from io import StringIO

from src.db_module.db_connector import Connector
from src.parsers.item import Item, ItemWithCup


def create_transaction(**kwargs):
    task_id = kwargs['task_id']
    connector = kwargs['connector']
    table_name = kwargs['table_name']
    data = kwargs['ti'].xcom_pull(task_ids=task_id)

    if task_id in ('extract_data_tradeit', 'extract_data_lis_skins', 'extract_data_skin_baron'):
        data = [ItemWithCup(**item) for item in data]
        update_with_market_cup(data, connector, table_name)
    elif task_id in ('extract_data_cs_go_market'):
        data = [Item(**item) for item in data]
        update_other_db(data, connector, table_name)
    update_etln(data, connector)


def update_with_market_cup(data: list[ItemWithCup], connector: Connector, table_name):
    header = ["price_timestamp", "item_key", "price", "url", "market_cup"]
    time = datetime.now()
    batch = [
        (time, item.item_key, item.price, item.url, item.market_cup) for item in data
    ]

    s_buf = write_to_buffer(batch)

    connector.write_data(s_buf, table_name, header)


def update_other_db(data: list[Item], connector: Connector, table_name: str):
    header = ["price_timestamp", "item_key", "price", "url"]
    time = datetime.now()
    batch = [(time, item.item_key, item.price, item.url) for item in data]

    s_buf = write_to_buffer(batch)

    connector.write_data(s_buf, table_name, header)


def write_to_buffer(batch: list[tuple]) -> StringIO:
    """
    Функция для формирования батча из объектов.
    Запись результата в csv внутри буфера.
    :param batch: list[tuple], список из кортежей атрибутов предметов.
    :return StringIO: буфер с записанным csv файлом
    """
    s_buf = StringIO()
    writer = csv.writer(s_buf)

    writer.writerows(batch)

    s_buf.seek(0)
    return s_buf


def update_etln(batch: list[Item], connector: Connector) -> None:
    """
    Функция для добавления новых объектов в эталонный справочник.
    Запись разницы между батчем и справочником.
    :param connector: Коннектор БД
    :param batch: батч данных заполнения
    """
    header = ["item_key", "name", "quality", "stattrack"]

    etln_data = connector.get_etln()
    curr_item_keys = set([item.item_key for item in batch])

    diff = curr_item_keys - etln_data

    new_items = [
        (item.item_key, item.name, item.quality, item.stattrack)
        for item in batch
        if item.item_key in diff
    ]

    s_buf = write_to_buffer(new_items)

    connector.write_data(s_buf, "item_etln", header)
