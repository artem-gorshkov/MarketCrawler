import hashlib
from datetime import datetime
from io import StringIO

from anticloudflare import AntiCloudflare
from dbconnector import Connector
from item import LisSkinsItem, str_to_enum
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed


class LisSkins:
    URL = "https://lis-skins.ru/market/csgo/?sort_by=popularity&page="
    MAX_PAGES = 5

    credentials = {
        "user": "etl",
        "password": "etl_pass",
        "host": "192.168.0.199",
        "database": "market",
        "port": 5432
    }

    def __init__(self):
        self._session = AntiCloudflare()
        self._connector = Connector(self.credentials)

    def _get_page(self, page=0) -> list[LisSkinsItem]:
        html = self._session.get(self.URL + str(page))

        soup = BeautifulSoup(html, "html.parser")

        parsed_items = self._get_all_items(soup)

        return parsed_items

    def _get_all_items(self, html: BeautifulSoup) -> list[LisSkinsItem]:
        items = html.find_all("div", {"class": "market_item"})
        parsed_items = [self._parse_item(item) for item in items]
        return parsed_items

    def _parse_item(self, item: BeautifulSoup) -> LisSkinsItem:
        parsed = {
            "name": item.find_all("div", {"class": "name-inner"})[0].text,
            "price": item.find_all("div", {"class": "price"})[0].text.replace(' ', ''),
            "url": item.find_all("a", href=True)[0]["href"],
        }
        try:
            parsed.update(
                {"quality": str_to_enum(item.find_all("div", {"class": "name-exterior"})[0].text)}
            )
        except IndexError:
            pass
        try:
            parsed.update(
                {
                    "market_cup": item.find_all("div", {"class": "similar-count"})[
                        0
                    ].text.replace("x", "")
                }
            )
        except IndexError:
            pass
        parsed |= {"item_key": self._form_item_key(parsed)}
        return LisSkinsItem(**parsed)

    def update_market_status(self, n_workers=3):
        result = []
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = []
            for page in range(1, self.MAX_PAGES):
                futures.append(executor.submit(self._get_page, page=page))
            for futures in as_completed(futures):
                result.extend(futures.result())

        del self._session

        # Filter duplicates

        result = {item.name: item for item in result}

        self._form_db_transaction(result)
        self._update_etln(result)

    @staticmethod
    def _write_to_buffer(batch: list[tuple]) -> StringIO:
        s_buf = StringIO()
        writer = csv.writer(s_buf)

        writer.writerows(batch)

        s_buf.seek(0)
        return s_buf

    def _form_db_transaction(self, batch: dict[LisSkinsItem]):
        time = datetime.now()
        batch = [(time, item.item_key, item.price, item.url, item.market_cup) for item in batch.values()]

        header = ["status_timestamp", "item_key", "price", "url", "market_cup"]

        s_buf = self._write_to_buffer(batch)

        self._connector.write_data(s_buf, "lis_skins", header)

    def _update_etln(self, batch: dict[LisSkinsItem]):
        etln_data = set([item[0].replace('-', '') for item in self._connector.get_etln()])
        curr_item_keys = set([item.item_key for item in batch.values()])

        diff = curr_item_keys - etln_data

        none_type_func = lambda item: item.quality.name if item.quality is not None else None

        new_items = [(item.item_key, item.name, none_type_func(item), item.stattrack) for item in batch.values() if
                     item.item_key in diff]

        s_buf = self._write_to_buffer(new_items)

        header = ['item_key', 'name', 'quality', 'stattrack']
        self._connector.write_data(s_buf, 'item_etln', header)

    @staticmethod
    def _form_item_key(item) -> str:
        return hashlib.md5(
            f'{item.get("name")}#{item.get("quality")}#{item.get("stattrack")}'.encode()
        ).hexdigest()


if __name__ == "__main__":
    instance = LisSkins()
    instance.update_market_status()