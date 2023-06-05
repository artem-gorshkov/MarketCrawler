from src.parsers.selenium_utils.selenium_driver import DefaultSelenium
from src.parsers.selenium_utils.undetected_driver import AntiCloudflare


def get_desired_driver(type='default'):
    if type == 'default':
        return DefaultSelenium()
    else:
        return AntiCloudflare()
