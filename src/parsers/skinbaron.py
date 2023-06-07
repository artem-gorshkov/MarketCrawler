import requests
import time
from random import random

from src.parsers.item import get_quality_from_name
from src.parsers.utils import form_item_key


class Skinbaron:
    URL = 'https://skinbaron.de/api/v2/Browsing/FilterOffers?appId=730&sort=BP&language=en&otherCurrency=RUB&'

    HEADER = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}

    # Нашел перебором, с 1164 страницы сайт возвращает пустой массив
    PAGE_NUMBER = 50

    def __init__(self):
        self._session = requests.Session()

    def _get_page(self, page: int):
        param = f'page={page}'
        response = self._session.get(self.URL + param, headers=self.HEADER)
        while response.status_code == 429:
            print("Слишком много запросов")
            time.sleep(random() * 10)
            response = self._session.get(self.URL + param, headers=self.HEADER)
        print(f'Nice response {page}')
        time.sleep(random() + .5)
        items = response.json()['aggregatedMetaOffers']

        parsed_items = list(map(self._parse_item, items))

        return parsed_items

    def _parse_item(self, item: dict):
        quality = None
        if 'singleOffer' in item:
            price = item['singleOffer']['formattedItemPriceOtherCurrency'][1:]
            if 'localizedExteriorName' in item['singleOffer']:
                quality = get_quality_from_name(item['singleOffer'].get('localizedExteriorName'))
            stattrack = 'statTrakString' in item['singleOffer']
        else:
            if item.get('formattedLowestPriceOtherCurrency'):
                price = item['formattedLowestPriceOtherCurrency'][1:]
            else:
                price = item.get('formattedLowestPriceTradeLockedOtherCurrency')
            stattrack = "StatTrak™" in item['extendedProductInformation']['localizedName']
        parsed_item = {
            'name': item['extendedProductInformation']['localizedName'].replace("StatTrak™", "").strip(),
            'url': 'https://skinbaron.de' + item['offerLink'],
            'price': price.replace('₽', '').strip(),
            'quality': quality,
            'stattrack': stattrack,
            'market_cup': item.get('numberOfOffers', 0)
        }

        parsed_item |= {'item_key': form_item_key(parsed_item)}
        return parsed_item

    def update_market_status(self):
        result = []
        for page in range(1, self.PAGE_NUMBER + 1, 1):
            result.extend(self._get_page(page))
        return result


if __name__ == '__main__':
    instance = Skinbaron()
    print(instance.update_market_status())
