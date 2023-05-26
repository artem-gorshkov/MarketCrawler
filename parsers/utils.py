import csv
import hashlib
from io import StringIO

from dbconnector import Connector

from parsers.item import Item


def form_item_key(item) -> str:
    """
    Функция для формирования из атрибутов ключа
    :param item: объект Item
    :return:
    """
    return hashlib.md5(
        f'{item.get("name")}#{item.get("quality")}#{item.get("stattrack")}'.encode()
    ).hexdigest()


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


def update_etln(connector: Connector, batch: dict[Item]) -> None:
    """
    Функция для добавления новых объектов в эталонный справочник.
    Запись разницы между батчем и справочником.
    :param connector: Коннектор БД
    :param batch: батч данных заполнения
    """
    etln_data = connector.get_etln()
    curr_item_keys = set([item.item_key for item in batch.values()])

    diff = curr_item_keys - etln_data

    none_type_func = (
        lambda item: item.quality.name if item.quality is not None else None
    )

    new_items = [
        (item.item_key, item.name, none_type_func(item), item.stattrack)
        for item in batch.values()
        if item.item_key in diff
    ]

    s_buf = write_to_buffer(new_items)

    header = ["item_key", "name", "quality", "stattrack"]
    connector.write_data(s_buf, "item_etln", header)
