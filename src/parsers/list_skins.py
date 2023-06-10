from bs4 import BeautifulSoup

from src.parsers.selenium_utils.selenium_utils import get_desired_driver
from src.parsers.item import ItemWithCup, str_to_enum
from src.parsers.utils import form_item_key


class LisSkins:
    URL = "https://lis-skins.ru/market/csgo/?sort_by=popularity&page="
    MAX_PAGES = 50

    def __init__(self):
        self._session = get_desired_driver('default')

    def _get_page(self, page=0) -> list[dict]:
        html = self._session.get(self.URL + str(page))

        soup = BeautifulSoup(html, "html.parser")

        parsed_items = self._get_all_items(soup)

        return parsed_items

    def _get_all_items(self, html: BeautifulSoup) -> list[dict]:
        items = html.find_all("div", {"class": "market_item"})
        parsed_items = [self._parse_item(item) for item in items]
        return parsed_items

    def _parse_item(self, item: BeautifulSoup) -> dict:
        parsed = {
            "name": item.find_all("div", {"class": "name-inner"})[0].text,
            "price": item.find_all("div", {"class": "price"})[0]
            .text.replace(" ", "")
            .replace("$", ""),
            "url": item.find_all("a", href=True)[0]["href"],
        }

        if quality := item.find_all("div", {"class": "name-exterior"}):
            parsed["quality"] = str_to_enum(quality[0].text)

        if market_cup := item.find_all("div", {"class": "similar-count"}):
            parsed["market_cup"] = market_cup[0].text.replace("x", "").strip()

        parsed |= {"item_key": form_item_key(parsed)}
        return parsed

    def update_market_status(self) -> list[ItemWithCup]:
        result = []
        for page in range(1, self.MAX_PAGES):
            result.extend(self._get_page(page=page))

        del self._session

        # Filter duplicates
        result = [value for value in {item['item_key']: item for item in result}.values()]
        return result


if __name__ == "__main__":
    instance = LisSkins()
    print(instance.update_market_status())
