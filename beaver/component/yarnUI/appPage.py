import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUI.basePage import BasePage
from selenium.common.exceptions import NoSuchElementException
from beaver.component.hadoop import Hadoop

logger = logging.getLogger(__name__)


class AppPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    if not Hadoop.isSecure() or Hadoop.isUIKerberozied():
        locator_dictionary = {
            "appAttemptTable": (By.XPATH, '//*[@id="attempts"]'),
            "killApplication": (By.XPATH, '/html/body/table/tbody/tr/td[2]/div[1]/button'),
            "user": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[2]/td/a'),
            "appName": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[3]/td'),
            "appType": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[4]/td'),
            "appPriority": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[6]/td'),
            "appState": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[7]/td'),
            "queue": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[2]/table/tbody/tr[8]/td/a'),
            "appAttemptID": (By.XPATH, '//*[@id="attempts"]/tbody/tr/td[1]/a'),
            "amNode": (By.XPATH, '//*[@id="attempts"]/tbody/tr/td[3]/a'),
        }
    else:
        locator_dictionary = {
            "appAttemptTable": (By.XPATH, '//*[@id="attempts"]'),
            "user": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[2]/td/a'),
            "appName": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[3]/td'),
            "appType": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[4]/td'),
            "appPriority": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[6]/td'),
            "appState": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[7]/td'),
            "queue": (By.XPATH, '//*[@id="layout"]/tbody/tr/td[2]/div[1]/table/tbody/tr[8]/td/a'),
            "appAttemptID": (By.XPATH, '//*[@id="attempts"]/tbody/tr/td[1]/a'),
            "amNode": (By.XPATH, '//*[@id="attempts"]/tbody/tr/td[3]/a'),
        }

    def isAppPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False, appID=None):
        return self.checkElementonPage(
            locatorName=self.getUser(returnLocatorName=True),
            locatorMessage='Application %s' % appID,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getUser(self, returnLocatorName=False):
        return self.getElement("user", returnLocatorName)

    def getAppName(self, returnLocatorName=False):
        return self.getElement("appName", returnLocatorName)

    def getAppType(self, returnLocatorName=False):
        return self.getElement("appType", returnLocatorName)

    def getAppPriority(self, returnLocatorName=False):
        return self.getElement("appPriority", returnLocatorName)

    def getAppState(self, returnLocatorName=False):
        return self.getElement("appState", returnLocatorName)

    def getQueue(self, returnLocatorName=False):
        return self.getElement("queue", returnLocatorName)

    def getAppAttempt(self, returnLocatorName=False):
        return self.getElement('appAttemptID', returnLocatorName)

    def getAMNode(self, returnLocatorName=False):
        return self.getElement('amNode', returnLocatorName)

    def clickOnUser(self):
        self.driver.find_element_by_link_text(self.getUser().text).click()

    def clickOnQueue(self):
        self.driver.find_element_by_link_text(self.getQueue().text).click()
