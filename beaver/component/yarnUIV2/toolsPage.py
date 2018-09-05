import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUIV2.basePage import BasePage
import time

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class ToolsPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {"ToolsPageTitle": (By.XPATH, "//*[contains(@title, 'YARN Tools')]")}

    def isToolsPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('ToolsPageTitle', True),
            locatorMessage='Tools Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )
