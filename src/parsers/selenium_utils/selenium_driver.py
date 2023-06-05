import time

from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class DefaultSelenium:
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

        self._driver = webdriver.Chrome(
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

        html = self._driver.page_source

        return html

    def __del__(self):
        self._driver.close()

    def close(self):
        self._driver.close()
