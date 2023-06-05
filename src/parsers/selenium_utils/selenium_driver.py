from pyvirtualdisplay import Display
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium import webdriver


class DefaultSelenium:
    """
    Класс для обхода сервисов с защитой от ботов
    Например использующих CloudFlare.
    Основан на запуске headless браузера и получения html
    """

    def __init__(self):
        display = Display(visible=False, size=(800, 800))
        display.start()

        self._options = Options()
        # self._options.headless = True
        self._caps = DesiredCapabilities().CHROME
        self._caps["pageLoadStrategy"] = "eager"
        self._options.add_argument("--lang=en")

        self._driver = webdriver.Chrome(
            '/usr/bin/chromedriver',
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
