from datetime import datetime

from anticloudflare import AntiCloudflare
from dbconnector import Connector
from item import LisSkinsItem, str_to_enum
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from parsers.utils import form_item_key, write_to_buffer, update_etln


class LisSkins:
    URL = "https://lis-skins.ru/market/csgo/?sort_by=popularity&page="
    MAX_PAGES = 5

    credentials = json.load(open('credentials.json'))

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
            "price": item.find_all("div", {"class": "price"})[0].text.replace(" ", ""),
            "url": item.find_all("a", href=True)[0]["href"],
        }

        if quality := item.find_all("div", {"class": "name-exterior"}):
            parsed["quality"] = str_to_enum(quality[0].text)

        if market_cup := item.find_all("div", {"class": "similar-count"}):
            parsed["market_cup"] = market_cup[0].text.replace("x", "")

        parsed |= {"item_key": form_item_key(parsed)}
        return LisSkinsItem(**parsed)

    def _form_db_transaction(self, batch: dict[LisSkinsItem]):
        time = datetime.now()
        batch = [
            (time, item.item_key, item.price, item.url, item.market_cup)
            for item in batch.values()
        ]

        header = ["status_timestamp", "item_key", "price", "url", "market_cup"]

        s_buf = write_to_buffer(batch)

        self._connector.write_data(s_buf, "lis_skins", header)

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
        update_etln(self._connector, result)


if __name__ == "__main__":
    instance = LisSkins()
    instance.update_market_status()
