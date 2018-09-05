import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUIV2.basePage import BasePage
from beaver.component.hadoop import MAPRED, Hadoop, YARN, HDFS
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class NodesPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "NodesPageTitle": (By.XPATH, "//*[contains(@title, 'Nodes')]"),
        "NodeStatus": (By.XPATH, "//a[contains(@href,'#/yarn-nodes/status')]"),
        "NodesHeatmap": (By.XPATH, "//a[contains(@href,'#/yarn-nodes/heatmap')]"),
        "NodeInfoPageTitle": (By.TAG_NAME, "h4"),
        "NodeAppPage": (By.XPATH, "//a[contains(@href,'#/yarn-node-apps/')]"),
        "NodeContainerPage": (By.XPATH, "//a[contains(@href,'#/yarn-node-containers/')]"),
        "NodeAppPageTitle": (By.XPATH, "//*[contains(@title, 'Applications')]"),
        "NodeContainerPageTitle": (By.XPATH, "//*[contains(@title, 'Containers')]"),
    }

    for node in YARN.getNodeManagerHosts(True):
        href = "\'#/yarn-node/%s\'" % node
        logger.info("HREF =%s" % href)
        locator_dictionary[node] = (By.XPATH, "//a[contains(@href,%s)]" % href)

    def isNodesPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('NodesPageTitle', True),
            locatorMessage='Nodes Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isNodeInfoPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('NodeInfoPageTitle', True),
            locatorMessage='Nodes Information Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isNodeAppPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('NodeAppPageTitle', True),
            locatorMessage='Nodes Application Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isNodeContainerPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('NodeContainerPageTitle', True),
            locatorMessage='Nodes Container Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isNodeAppIDLink(self, appID, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getIndividualNodeApplication(appID, True),
            locatorMessage='Nodes ApplicationID %s link Page' % appID,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isNodeContainerIDLink(self, appID, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getIndividualNodeContainer(appID, True),
            locatorMessage='Nodes ContainerID container_*_%s_* link Page' % appID,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def getIndividualNodeApplication(self, appID, returnLocatorName=False):
        href = "\'/%s\'" % appID
        logger.info("Node app HREF =%s" % href)
        self.locator_dictionary[appID] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID, returnLocatorName)

    @TaskReporter.report_test()
    def getIndividualNodeContainer(self, appID, returnLocatorName=False):
        href = "\'%s_\'" % appID.replace("application", "")
        logger.info("Node container HREF =%s" % href)
        self.locator_dictionary[appID] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID, returnLocatorName)

    def getNodeStatus(self, returnLocatorName=False):
        return self.getElement('NodeStatus', returnLocatorName)

    def getNodesHeatmap(self, returnLocatorName=False):
        return self.getElement('NodesHeatmap', returnLocatorName)

    def getNodeAddress(self, nodeAddress, returnLocatorName=False):
        return self.getElement(nodeAddress, returnLocatorName)

    def getNodeAppPage(self, returnLocatorName=False):
        return self.getElement('NodeAppPage', returnLocatorName)

    def getNodeContainerPage(self, returnLocatorName=False):
        return self.getElement('NodeContainerPage', returnLocatorName)

    def clickNodeAddress(self, nodeAddress):
        SeleniumDP.click(self.driver, self.getNodeAddress(nodeAddress))

    def clickNodeAppPage(self):
        SeleniumDP.click(self.driver, self.getNodeAppPage())

    def clickNodeContainerPage(self):
        SeleniumDP.click(self.driver, self.getNodeContainerPage())

    def clickNodeStatus(self):
        SeleniumDP.click(self.driver, self.getNodeStatus())

    def clickNodeHeatMap(self):
        SeleniumDP.click(self.driver, self.getNodesHeatmap())
