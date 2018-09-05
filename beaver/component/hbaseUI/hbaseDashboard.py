import logging
from beaver.seleniumHDP import Selenium
from selenium.webdriver.common.by import By
from beaver.component.hbaseUI.basePage import BasePage
from selenium.common.exceptions import NoSuchElementException
from beaver.component.hadoop import Hadoop

logger = logging.getLogger(__name__)


class HBaseDashboard(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "hbaseDashboardTitle": (By.XPATH, "//h4[text()[contains(.,'Tuning metrics for the HBase cluster')]]"),
        "walThroughputRow": (By.XPATH, "//div[text()[contains(.,'WAL - Throughput')]]"),
        "regionServerRow": (By.XPATH, "//div[text()[contains(.,'Region Server')]]"),
        "rpcQueuesRow": (By.XPATH, "//div[text()[contains(.,'RPC - Queues')]]"),
        "regionsInTransitionRow": (By.XPATH, "//div[text()[contains(.,'Regions In Transition')]]"),
        "fileSystemRow": (By.XPATH, "//div[text()[contains(.,'File System')]]")
    }

    def isHBaseGrafanaDashboardPage(self, retryCount=10, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getHBaseDashboardTitleElement(returnLocatorName=True),
            locatorMessage='HBase Grafana Dashboard Title',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getHBaseDashboardTitleElement(self, returnLocatorName=False):
        return self.getElement('hbaseDashboardTitle', returnLocatorName)

    def getWALThroughputRowElement(self, returnLocatorName=False):
        return self.getElement('walThroughputRow', returnLocatorName)

    def getRegionServerRowElement(self, returnLocatorName=False):
        return self.getElement('regionServerRow', returnLocatorName)

    def getRPCQueuesRowElement(self, returnLocatorName=False):
        return self.getElement('rpcQueuesRow', returnLocatorName)

    def getRegionsInTransitionRowElement(self, returnLocatorName=False):
        return self.getElement('regionsInTransitionRow', returnLocatorName)

    def getFileSystemRowElement(self, returnLocatorName=False):
        return self.getElement('fileSystemRow', returnLocatorName)
