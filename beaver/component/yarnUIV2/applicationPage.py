import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUIV2.basePage import BasePage
from selenium.webdriver.support.ui import Select
from beaver.component.hadoop import MAPRED, Hadoop, YARN, HDFS

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


## TODO AM NODE LINK AND LOG LINK not working due to BUG-99370; hence no XPATH info
## TODO Cannot click on specific applink in header due to BUG-100892
## TODO Resource Usage Tab doesnot work due to BUG-100896
class ApplicationPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "AppPageTitle": (By.XPATH, "//*[contains(@title, 'Applications')]"),
        "SpecificAppPageTitle": (By.XPATH, "//*[contains(@title, 'Attempts')]"),
        "AppAttempt": (By.XPATH, "//*[contains(@href,'#/yarn-app-attempt/appattempt_')]"),
        "GridViewTab": (By.XPATH, "//*[contains(@href,'#gridViewTab')]"),
        "GraphViewTab": (By.XPATH, "//*[contains(@href,'#graphViewTab')]"),
        "SpecificAppAttemptPageTitle": (By.XPATH, "//*[contains(@title, 'Attempt [appattempt_')]"),
        "DiagnosticTitle": (By.XPATH, "//*[contains(@title,'App [application_')]"),
        "LogsTitle": (By.XPATH, "//*[contains(@title, 'Logs')]"),
        "FetchAttempt": (By.XPATH, "//*[@class='js-fetch-attempt-containers select2-hidden-accessible']"),
        "FetchContainer": (By.XPATH, "//*[@class='js-fetch-logs-containers select2-hidden-accessible']"),
        "FetchLogFile": (By.XPATH, "//*[@class='js-fetch-log-for-container select2-hidden-accessible']"),
        "FindInLog": (By.ID, "logSeachInput98765"),
        "FindButton": (By.XPATH, "//button[@class='btn btn-default']"),
        "sortAppButton": (By.XPATH, "//div[@title='Application ID']//*[contains(@title,'Sort')]")
    }

    def isAppPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('AppPageTitle', True),
            locatorMessage='Application Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def isSpecificAppLink(self, appID, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):

        if not self.checkElementonPage(locatorName=self.getIndividualApplicationPage(
                appID, True), locatorMessage='Application %s Link' % appID, retryCount=retryCount, timeout=timeout,
                                       restartWebDriver=restartWebDriver, quitWebdriver=quitWebdriver):
            logger.info("Clicking App Sort and rechecking if appLink Exists")
            self.clickAppSort()
            return self.checkElementonPage(
                locatorName=self.getIndividualApplicationPage(appID, True),
                locatorMessage='Application %s Link' % appID,
                retryCount=retryCount,
                timeout=timeout,
                restartWebDriver=restartWebDriver,
                quitWebdriver=quitWebdriver
            )
        else:
            return True

    def isAppAttemptPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('SpecificAppPageTitle', True),
            locatorMessage='Specific Application Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isSpecificAppAttemptPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('SpecificAppAttemptPageTitle', True),
            locatorMessage='Specific Application Attempt Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isDiagnosticPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('DiagnosticTitle', True),
            locatorMessage='Diagnostic Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def isLogsPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('LogsTitle', True),
            locatorMessage='Logs Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def getIndividualApplicationPage(self, appID, returnLocatorName=False):
        href = "\'#/yarn-app/%s/attempts\'" % appID
        logger.info("app HREF =%s" % href)
        self.locator_dictionary[appID] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID, returnLocatorName)

    def getAppSort(self, returnLocatorName=False):
        return self.getElement("sortAppButton", returnLocatorName)

    def getAppAttempt(self, returnLocatorName=False):
        return self.getElement("AppAttempt", returnLocatorName)

    def getGridTab(self, returnLocatorName=False):
        return self.getElement("GridViewTab", returnLocatorName)

    def getGraphTab(self, returnLocatorName=False):
        return self.getElement("GraphViewTab", returnLocatorName)

    @TaskReporter.report_test()
    def getDiagnostics(self, appID, returnLocatorName=False):
        href = "\'#/yarn-app/%s/info\'" % appID
        logger.info("app in Diagnostic HREF =%s" % href)
        self.locator_dictionary[appID + "_info"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID + "_info", returnLocatorName)

    @TaskReporter.report_test()
    def getLogs(self, appID, returnLocatorName=False):
        href = "\'#/yarn-app/%s/logs\'" % appID
        logger.info("app in logs HREF =%s" % href)
        self.locator_dictionary[appID + "Logs"] = (By.XPATH, "//a[contains(@href,%s)]" % href)
        return self.getElement(appID + "Logs", returnLocatorName)

    @TaskReporter.report_test()
    def getAttemptFromDropDown(self, appID, returnLocatorName=False):
        select = Select(self.getElement("FetchAttempt", returnLocatorName))
        attempt = appID.replace("application", "appattempt") + "_000001"
        select.select_by_value(attempt)

    @TaskReporter.report_test()
    def getContainerFromDropDown(self, appID, returnLocatorName=False):
        select = Select(self.getElement("FetchContainer", returnLocatorName))
        amContainer = YARN.getAMcontainerId(appID, 1)
        select.select_by_value(amContainer)

    def getLogFileForContainer(self, returnLocatorName=False):
        select = Select(self.getElement("FetchLogFile", returnLocatorName))
        select.select_by_value("syslog")

    def enterFindInLog(self, returnLocatorName=False):
        element = self.getElement("FindInLog", returnLocatorName)
        element.send_keys("hadoop")

    def getFindButton(self, returnLocatorName=False):
        return self.getElement("FindButton", returnLocatorName)

    def clickAppSort(self):
        SeleniumDP.click(self.driver, self.getAppSort())

    def clickIndividualApplication(self, appID):
        SeleniumDP.click(self.driver, self.getIndividualApplicationPage(appID))

    def clickAppAttempt(self):
        SeleniumDP.click(self.driver, self.getAppAttempt())

    def clickGridTab(self):
        SeleniumDP.click(self.driver, self.getGridTab())

    def clickGraphTab(self):
        SeleniumDP.click(self.driver, self.getGraphTab())

    def clickDiagnostics(self, appID):
        SeleniumDP.click(self.driver, self.getDiagnostics(appID))

    def clickLogs(self, appID):
        SeleniumDP.click(self.driver, self.getLogs(appID))

    def clickFindButton(self):
        SeleniumDP.click(self.driver, self.getFindButton())
