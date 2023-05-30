import json

from bs4 import BeautifulSoup

from src.parsers.anticloudflare import AntiCloudflare
from src.parsers.dbconnector import Connector
import re
from src.parsers.item import str_to_enum_dict, get_quality_from_name


class CsGoMarket:
    credentials = json.load(open("../../resources/credentials.json"))
    URL = "https://market-old.csgo.com/?t=all&sd=desc&p="

    def __init__(self):
        self._session = AntiCloudflare()
        self._connector = Connector(self.credentials)

    def _get_page(self, page: int):
        html = self._session.get(self.URL + str(page))
        self._session.close()
        soup = BeautifulSoup(html, "html.parser")
        parsed_items = self._get_all_items(soup)

        return parsed_items

    def _get_all_items(self, soup: BeautifulSoup):
        market_items = soup.find_all("div", {"class": "market-items"})[0]
        items = market_items.find_all("a", {"class": "item"}, href=True)
        return [self.parse_item(item) for item in items]

    def parse_item(self, item: BeautifulSoup):
        parsed_item = {
            "name": item.find_all("div", {"class": "name"})[0].text.strip(),
            "price": item.find_all("div", {"class": "price"})[0].text.strip(),
            "url": item["href"],
        }
        parsed_item = self.get_stattrack(parsed_item)
        parsed_item = self.get_quality(parsed_item)

        return parsed_item

    @staticmethod
    def get_stattrack(item: dict) -> dict:
        item["stattrack"] = item["name"].find("StatTrak™") != -1
        item["name"].replace("StatTrak™", "").strip()
        return item

    @staticmethod
    def get_quality(item: dict) -> dict:
        item["quality"] = (
            get_quality_from_name(item["name"])
            if any(quality in item["name"] for quality in str_to_enum_dict.keys())
            else False
        )
        pattern = "|".join([f"\({quality}\)" for quality in str_to_enum_dict.keys()])
        item["name"] = re.sub(pattern, "", item["name"]).strip()
        return item


if __name__ == "__main__":
    instance = CsGoMarket()
    print(instance._get_page(2))