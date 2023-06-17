import json
import time

from pyvirtualdisplay import Display
import undetected_chromedriver as uc
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options


class AntiCloudflare:
    """
    Класс для обхода сервисов с защитой от ботов
    Например использующих CloudFlare.
    Основан на запуске headless браузера и получения html
    """

    def __init__(self):
        display = Display(visible=False, size=(800, 800))
        display.start()

        self._lang_changed = False
        self._currency_changed = False

        self._options = Options()
        # self._options.headless = True
        self._caps = DesiredCapabilities().CHROME
        self._caps["pageLoadStrategy"] = "eager"
        self._options.add_argument("--lang=ru")

        self._driver = uc.Chrome(
            driver_executable_path='/home/ubuntu/airflow/MarketCrawler/resources/chromedriver',
            options=self._options,
            desired_capabilities=self._caps
        )

    def simple_get(self, url: str):
        self._driver.get(url)
        time.sleep(6)
        html = self._driver.find_element(By.TAG_NAME, 'pre').text
        dict_json = json.loads(html)
        return dict_json

    def get(self, url: str) -> str:
        """
        Получение html с страницы указанной в url
        :param url: str, адрес страницы
        :return: str, html страницы
        """
        self._driver.get(url)
        time.sleep(6)

        if not self._lang_changed:
            self._change_lang()
            self._driver.get(url)

        if not self._currency_changed:
            self.change_currency()
            self._driver.get(url)

        html = self._driver.page_source

        return html

    def _change_lang(self):
        try:
            lang = self._driver.find_element(By.CLASS_NAME, 'lang-selector__item_ru')
            ac = ActionChains(self._driver)
            ac.move_to_element(lang).click().perform()
            lang = self._driver.find_element(By.CLASS_NAME, 'lang-selector__item_en')
            ac.move_to_element(lang).click().perform()
        except Exception:
            pass
        self._lang_changed = True

    def change_currency(self):
        try:
            usd = self._driver.find_element(By.XPATH, '/html/body/div[2]/header/div/div[1]')
            ac = ActionChains(self._driver)
            ac.move_to_element(usd)
            rub = self._driver.find_element(By.XPATH, '/html/body/div[2]/header/div/div[1]/div[2]')
            ac.move_to_element(rub).click().perform()
        except Exception:
            pass

    def __del__(self):
        self._driver.close()

    def close(self):
        self._driver.close()
