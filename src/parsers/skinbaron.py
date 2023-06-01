
import requests
import time

from src.parsers.item import get_quality_from_name


class Skinbaron:
    URL = 'https://skinbaron.de/api/v2/Browsing/FilterOffers?appId=730&sort=BP&language=en&otherCurrency=USD&'

    HEADER = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}

    # Нашел перебором, с 1164 страницы сайт возвращает пустой массив
    PAGE_NUMBER = 1163
    def __init__(self):
        self._session = requests.Session()

    def _get_page(self, page: int):
        param = f'page={page}'
        response = self._session.get(self.URL + param, headers=self.HEADER)
        if (response.status_code == 429):
            print("Слишком много запросов")
            return
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
            price = item['formattedLowestPriceOtherCurrency'][1:]
            stattrack = "StatTrak™" in item['extendedProductInformation']['localizedName']
        return {
            'name': item['extendedProductInformation']['localizedName'].replace("StatTrak™", "").strip(),
            'url': 'https://skinbaron.de' + item['offerLink'],
            'price': price,
            'quality': quality,
            'stattrack': stattrack,
            'market_cup': item.get('numberOfOffers')
        }

    def update_market_status(self):
        result = []
        for page in range(1, self.PAGE_NUMBER + 1, 1):
            self._get_page(page)
            time.sleep(0.2)
        print(result)


if __name__ == '__main__':
    instance = Skinbaron()
    instance.update_market_status()
