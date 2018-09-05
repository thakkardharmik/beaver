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


class QueuePage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "QueuePageTitle": (By.XPATH, "//*[contains(@title, 'Queues')]"),
    }

    def isQueuePage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('QueuePageTitle', True),
            locatorMessage='Queue Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isSpecificQPage(self, queue, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getQPage(queue, True),
            locatorMessage='%s Queue Page' % queue,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isSpecificQAppPage(self, queue, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getQAppTitle(queue, True),
            locatorMessage='%s Queue App Page' % queue,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getQPage(self, queue, returnLocatorName=False):
        self.locator_dictionary[queue] = (By.XPATH, "//*[@class='node']//*[text()='%s']" % queue)
        return self.getElement(queue, returnLocatorName)

    @TaskReporter.report_test()
    def getQAppPage(self, queue, returnLocatorName=False):
        href = "\'#/yarn-queue/%s/apps\'" % queue
        logger.info("Queue App HREF =%s" % href)
        self.locator_dictionary[queue + "App"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(queue + "App", returnLocatorName)

    @TaskReporter.report_test()
    def getQAppTitle(self, queue, returnLocatorName=False):
        title = "\'Queue [ %s ]\'" % queue
        logger.info("Queue App Title  =%s" % title)
        self.locator_dictionary[queue + "AppTitle"] = (By.XPATH, "//*[contains(@title,%s)]" % title)
        return self.getElement(queue + "AppTitle", returnLocatorName)

    def clickQ(self, queue):
        SeleniumDP.click(self.driver, self.getQPage(queue))

    def clickQApp(self, queue):
        SeleniumDP.click(self.driver, self.getQAppPage(queue))
