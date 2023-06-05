import time

import undetected_chromedriver as uc
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options


class AntiCloudflare:
    """
    Класс для обхода сервисов с защитой от ботов
    Например использующих CloudFlare.
    Основан на запуске headless браузера и получения html
    """

    def __init__(self):
        self._options = Options()
        # self._options.headless = True
        self._caps = DesiredCapabilities().CHROME
        self._caps["pageLoadStrategy"] = "eager"
        self._options.add_argument("--lang=en")

        self._driver = uc.Chrome(
            options=self._options,
            desired_capabilities=self._caps
        )

    def get(self, url: str) -> str:
        """
        Получение html с страницы указанной в url
        :param url: str, адрес страницы
        :return: str, html страницы
        """
        self._driver.get(url)
        time.sleep(6)

        html = self._driver.page_source

        return html

    def __del__(self):
        self._driver.close()

    def close(self):
        self._driver.close()