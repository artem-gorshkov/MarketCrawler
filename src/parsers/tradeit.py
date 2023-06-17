from concurrent.futures import ThreadPoolExecutor, as_completed
from src.parsers.selenium_utils.selenium_utils import get_desired_driver
from src.parsers.utils import form_item_key, get_quality


class TradeIt:
    URL = (
        "https://tradeit.gg/api/v2/inventory/data?gameId=730&sortType=Popularity&searchValue"
        "=&minPrice=0&maxPrice=100000&minFloat=0&maxFloat=1&showTradeLock=true&colors=&showUserListing=true&fresh"
        "=false&"
    )

    exchange_rate_url = 'https://tradeit.gg/api/v2/exchange-rate'

    HEADER = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0"
    }

    def __init__(self):
        self._session = get_desired_driver('special')
        self._get_exchange_rate()

    def _get_exchange_rate(self):
        response = self._session.simple_get(self.exchange_rate_url)
        self._exchange_rate = response.get('rates').get('RUB')

    def _get_page(self, left: int, step: int):
        param = f"offset={left}&limit={step}"
        response = self._session.simple_get(self.URL + param)
        items = response["items"]

        parsed_items = list(map(self._parse_item, items))

        return parsed_items

    def _parse_item(self, item: dict):
        price = str(item["price"])
        parsed = {
            "name": item["name"].replace('StatTrak™', ''),
            "price": round(float(f'{price[:-2]}.{price[-2:]}') * self._exchange_rate, 2) * 0.65,
            "stattrack": item.get("hasStattrak"),
            "market_cup": item.get("currentStock"),
        }

        parsed = get_quality(parsed)
        parsed |= {"item_key": form_item_key(parsed)}
        return parsed

    @staticmethod
    def _create_intervals(n: int) -> tuple[list, int]:
        MAX_ITEMS = 10_000
        step = MAX_ITEMS // n

        return [i + step * i for i in range(n)], step

    def update_market_status(self, n_workers=3):
        result = []
        intervals, step = self._create_intervals(n_workers)
        for left in intervals:
            result.append(self._get_page(left, step))

        return result


if __name__ == "__main__":
    instance = TradeIt()
    print(instance.update_market_status())
