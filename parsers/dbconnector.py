from io import StringIO

import psycopg2 as ps


class Connector:
    """
    Коннектор для упрощенного взаимодействия с БД
    """
    def __init__(self, credentials: dict):
        self._connection = ps.connect(**credentials)

    def write_data(self, data: StringIO, table_name: str, columns) -> None:
        columns = ", ".join(f'"{k}"' for k in columns)

        sql = f"COPY market.{table_name} ({columns}) FROM STDIN WITH CSV DELIMITER ',';"

        with self._connection.cursor() as cur:
            cur.copy_expert(sql=sql, file=data)
            self._connection.commit()

    def get_etln(self):
        """
        Получение всех записей эталонного справочника.
        :return: set[str]: список ключей предметов
        """
        sql = """
        select item_key from market.item_etln;
        """

        with self._connection.cursor() as cur:
            cur.execute(sql)
            data = cur.fetchall()
            self._connection.commit()

        return set([item[0].replace("-", "") for item in data])
