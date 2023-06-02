from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from src.parsers.item import ItemWithCup
from src.parsers.utils import form_item_key


class TradeIt:
    URL = (
        "https://tradeit.gg/api/v2/inventory/data?gameId=730&sortType=Popularity&searchValue"
        "=&minPrice=0&maxPrice=100000&minFloat=0&maxFloat=1&showTradeLock=true&colors=&showUserListing=true&fresh"
        "=false&"
    )

    HEADER = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0"
    }

    def __init__(self):
        self._session = requests.Session()

    def _get_page(self, left: int, step: int):
        param = f"offset={left}&limit={step}"
        response = self._session.get(self.URL + param, headers=self.HEADER)
        items = response.json()["items"]

        parsed_items = list(map(self._parse_item, items))

        return parsed_items

    def _parse_item(self, item: dict):
        parsed = {
            "name": item["name"],
            "price": item["price"],
            "stattrack": item.get("hasStattrak"),
            "market_cup": item.get("currentStock"),
        }
        parsed |= {"item_key": form_item_key(parsed)}
        return ItemWithCup(**parsed)


    @staticmethod
    def _create_intervals(n: int) -> tuple[list, int]:
        MAX_ITEMS = 4000
        step = MAX_ITEMS // n

        return [i + step * i for i in range(n)], step

    def update_market_status(self, n_workers=3):
        result = []
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = []
            intervals, step = self._create_intervals(n_workers)
            for left in intervals:
                futures.append(executor.submit(self._get_page, left, step))
            for futures in as_completed(futures):
                result.extend(futures.result())

        return result


if __name__ == "__main__":
    instance = TradeIt()
    instance.update_market_status()
