import undetected_chromedriver as uc
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class AntiCloudflare:
    """
    Класс для обхода сервисов с защитой от ботов
    Например использующих CloudFlare.
    Основан на запуске headless браузера и получения html
    """

    def __init__(self):
        self._options = uc.ChromeOptions()
        # self._options.headless = True
        caps = DesiredCapabilities().CHROME
        caps["pageLoadStrategy"] = "eager"
        self._options.add_argument("--lang=en")

        self._driver = uc.Chrome(
            driver_executable_path="./chromedriver",
            options=self._options,
            desired_capabilities=caps,
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
