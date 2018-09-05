import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUIV2.basePage import BasePage
import time
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class ServicesPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "ServicePageTitle": (By.XPATH, "//*[contains(@title, 'Services')]"),
        "ServiceComponentPageTitle": (By.XPATH, "//*[contains(@title, 'Components')]"),
        "ServiceConfPageTitle": (By.XPATH, "//*[contains(@title, 'Configurations & Metrics')]"),
    }

    def isServicePage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('ServicePageTitle', True),
            locatorMessage='Services Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isServiceComponentPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('ServiceComponentPageTitle', True),
            locatorMessage='Services Component Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isServiceComponentLink(
            self, appID, component, service, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            locatorName=self.getSpecificServiceComponent(appID, component, service, True),
            locatorMessage='Services Component Link',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isServiceConfPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('ServiceConfPageTitle', True),
            locatorMessage='Services Conf Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isSpecificServiceLink(self, appID, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getServiceComponent(appID, True),
            locatorMessage='Services component appID=%s link' % appID,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def getServiceComponent(self, appID, returnLocatorName=False):
        href = "\'#/yarn-app/%s/components?service=\'" % (appID)
        logger.info("service component HREF =%s" % href)
        self.locator_dictionary[appID + "_serComp"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID + "_serComp", returnLocatorName)

    @TaskReporter.report_test()
    def getServiceConf(self, appID, returnLocatorName=False):
        href = "\'#/yarn-app/%s/configs?service=\'" % (appID)
        logger.info("service config HREF =%s" % href)
        self.locator_dictionary[appID + "_serConf"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID + "_serConf", returnLocatorName)

    @TaskReporter.report_test()
    def getSpecificServiceComponent(self, appID, component, service, returnLocatorName=False):
        href = "\'#/yarn-component-instances/%s/info?service=%s&&appid=%s\'" % (component, service, appID)
        logger.info("service config HREF =%s" % href)
        self.locator_dictionary[appID + "_serSpecificComponent"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID + "_serSpecificComponent", returnLocatorName)

    def clickServiceComponent(self, appID):
        SeleniumDP.click(self.driver, self.getServiceComponent(appID))

    def clickServiceConf(self, appID):
        SeleniumDP.click(self.driver, self.getServiceConf(appID))
