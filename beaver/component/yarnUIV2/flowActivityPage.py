import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUIV2.basePage import BasePage
from beaver.component.hadoop import Hadoop, MAPRED, YARN
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class FlowActivityPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "FlowPageTitle": (By.XPATH, "//*[contains(@title, 'Flow Activities')]"),
        "FlowInfoTitle": (By.XPATH, "//*[contains(@title, 'Flow Info [yarn-cluster!')]"),
        "RunInfoTitle": (By.XPATH, "//*[contains(@title, 'Run Info [yarn-cluster!')]"),
    }

    def isFlowPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('FlowPageTitle', True),
            locatorMessage='Flow Activity Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isFlowIDPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement("FlowInfoTitle", True),
            locatorMessage='Flow Info Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isRunInfoPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement("RunInfoTitle", True),
            locatorMessage='Run Info Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isSpecificFlowIDLink(
            self, user, flowName, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            locatorName=self.getFlowID(user, flowName, True),
            locatorMessage='Flow ID user=%s flowName=%s link' % (user, flowName),
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def getFlowID(self, user, flowName, returnLocatorName=False):
        href = "\'#/yarn-flow/yarn-cluster!%s!%s/info\'" % (user, flowName)
        logger.info("flowID HREF =%s" % href)
        self.locator_dictionary[flowName] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(flowName, returnLocatorName)

    @TaskReporter.report_test()
    def getFlowRun(self, user, flowName, returnLocatorName=False):
        href = "\'#/yarn-flow/yarn-cluster!%s!%s/runs\'" % (user, flowName)
        logger.info("flowrun HREF =%s" % href)
        self.locator_dictionary[flowName + "_run"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(flowName + "_run", returnLocatorName)

    @TaskReporter.report_test()
    def getFlowRunID(self, user, flowName, runID, returnLocatorName=False):
        href = "\'#/yarn-flowrun/yarn-cluster!%s!%s!%s/info\'" % (user, flowName, runID)
        logger.info("flowrunID HREF =%s" % href)
        self.locator_dictionary[flowName + "_runID"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(flowName + "_runID", returnLocatorName)

    @TaskReporter.report_test()
    def getFlowRunMetrics(self, user, flowName, runID, returnLocatorName=False):
        href = "\'#/yarn-flowrun/yarn-cluster!%s!%s!%s/metrics\'" % (user, flowName, runID)
        logger.info("flowrunID HREF =%s" % href)
        self.locator_dictionary[flowName + "_runMetrics"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(flowName + "_runMetrics", returnLocatorName)

    def clickFlowID(self, user, flowName):
        SeleniumDP.click(self.driver, self.getFlowID(user, flowName))

    def clickFlowRun(self, user, flowName):
        SeleniumDP.click(self.driver, self.getFlowRun(user, flowName))

    def clickflowRunID(self, user, flowName, runID):
        SeleniumDP.click(self.driver, self.getFlowRunID(user, flowName, runID))

    def clickflowRunMetrics(self, user, flowName, runID):
        SeleniumDP.click(self.driver, self.getFlowRunMetrics(user, flowName, runID))
