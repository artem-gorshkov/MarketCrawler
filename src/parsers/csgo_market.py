from bs4 import BeautifulSoup

from src.parsers.anticloudflare import AntiCloudflare
from src.parsers.utils import form_item_key, get_quality


class CsGoMarket:
    URL = "https://market-old.csgo.com/?t=all&sd=desc&p="
    MAX_PAGES = 5

    def __init__(self):
        self._session = AntiCloudflare(anti=True)

    def _get_page(self, page: int):
        html = self._session.get(self.URL + str(page))
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
            "price": item.find_all("div", {"class": "price"})[0].text.strip().replace(' ', ''),
            "url": item["href"],
        }
        parsed_item = self.get_stattrack(parsed_item)
        parsed_item = get_quality(parsed_item)
        parsed_item |= {"item_key": form_item_key(parsed_item)}
        return parsed_item

    @staticmethod
    def get_stattrack(item: dict) -> dict:
        item["stattrack"] = item["name"].find("StatTrak™") != -1
        item["name"].replace("StatTrak™", "").strip()
        return item

    def update_market_status(self) -> list[dict]:
        result = []
        for page in range(1, self.MAX_PAGES):
            result.extend(self._get_page(page=page))

        del self._session

        # Filter duplicates
        return result


if __name__ == "__main__":
    instance = CsGoMarket()
    print(instance.update_market_status())
