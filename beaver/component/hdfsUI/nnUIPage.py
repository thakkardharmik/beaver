#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#

import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.hdfsUI.basePage import BasePage
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class NNUIPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, driver=basePageObj.driver, base_url=basePageObj.base_url)

    locator_dictionary = {
        "overviewTab": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Overview')]"),
        "overviewContainer": (By.ID, "tab-overview"),
        "overviewTabNNHostPortStatus": (By.XPATH, "//*[@id='tab-overview']/div[1]/h1/small"),
        "datanodesTab": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Datanodes')]"),
        "datanodeContainer": (By.ID, "tab-datanode"),
        "firstDatanodeLink": (By.XPATH, "//*[@id='table-datanodes']/tbody/tr[1]/td[2]/a"),
        "secondDatanodeLink": (By.XPATH, "//*[@id='table-datanodes']/tbody/tr[2]/td[2]/a"),
        "datanodesVolumeFailuresTab": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Datanode Volume Failures')]"),
        "datanodeVolumeFailureContainer": (By.ID, "tab-datanode-volume-failures"),
        "snapshotTab": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Snapshot')]"),
        "snapshotContainer": (By.ID, "tab-snapshot"),
        "startupProgressTab": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Startup Progress')]"),
        "startupProgressContainer": (By.ID, "tab-startup-progress"),
        "utilitiesDropdown": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Utilities')]"),
        "utilitiesDropdownBrowseFS": (By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Browse the file system')]"),
        "utilitiesDropdownLogs": (By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Logs')]"),
        "utilitiesDropdownMetrics": (By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Metrics')]"),
        "utilitiesDropdownConfiguration": (By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Configuration')]"),
        "utilitiesDropdownProcessThreadDump": (
            By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Process Thread Dump')]"
        ),
        "browseDirectoryHeader": (By.XPATH, "//div[@class='page-header']//h1[contains(.,'Browse Directory')]"),
        "logsHeader": (By.XPATH, "/html/body/h1[contains(.,'Directory: /logs/')]"),
        "logsHeaderError": (By.XPATH, "/html/body/h2[contains(.,'HTTP ERROR: 403')]"),
        "metricsJmx": (By.XPATH, "/html/body/pre[contains(.,'beans')]"),
        "conf": (By.XPATH, "/configuration"),
        "processThreadDump": (By.XPATH, "/html/body/pre[contains(.,'Process Thread Dump')]"),
    }

    def isNNUIPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getOverviewTab(returnLocatorName=True),
            locatorMessage='NN UI Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getOverviewTab(self, returnLocatorName=False):
        return self.getElement('overviewTab', returnLocatorName)

    def getOverviewContainer(self, returnLocatorName=False):
        return self.getElement('overviewContainer', returnLocatorName)

    def getOverviewTabNNHostPortStatus(self, returnLocatorName=False):
        return self.getElement('overviewTabNNHostPortStatus', returnLocatorName)

    def getOverviewTabNNHostPortStatusText(self):
        return self.getOverviewTabNNHostPortStatus().text.lower()

    def getDatanodesTab(self, returnLocatorName=False):
        return self.getElement('datanodesTab', returnLocatorName)

    def getDatanodeContainer(self, returnLocatorName=False):
        return self.getElement('datanodeContainer', returnLocatorName)

    def getFirstDatanodeLink(self, returnLocatorName=False):
        return self.getElement('firstDatanodeLink', returnLocatorName)

    def getSecondDatanodeLink(self, returnLocatorName=False):
        return self.getElement('secondDatanodeLink', returnLocatorName)

    def getDatanodesVolumeFailuresTab(self, returnLocatorName=False):
        return self.getElement('datanodesVolumeFailuresTab', returnLocatorName)

    def getDatanodeVolumeFailureContainer(self, returnLocatorName=False):
        return self.getElement('datanodeVolumeFailureContainer', returnLocatorName)

    def getSnapshotTab(self, returnLocatorName=False):
        return self.getElement('snapshotTab', returnLocatorName)

    def getSnapshotContainer(self, returnLocatorName=False):
        return self.getElement('snapshotContainer', returnLocatorName)

    def getStartupProgressTab(self, returnLocatorName=False):
        return self.getElement('startupProgressTab', returnLocatorName)

    def getStartupProgressContainer(self, returnLocatorName=False):
        return self.getElement('startupProgressContainer', returnLocatorName)

    def getUtilitiesDropdown(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdown', returnLocatorName)

    def getUtilitiesDropdownBrowseFS(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownBrowseFS', returnLocatorName)

    def getUtilitiesDropdownLogs(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownLogs', returnLocatorName)

    def getUtilitiesDropdownMetrics(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownMetrics', returnLocatorName)

    def getUtilitiesDropdownConfiguration(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownConfiguration', returnLocatorName)

    def getUtilitiesDropdownProcessThreadDump(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownProcessThreadDump', returnLocatorName)

    def getBrowseDirectoryHeader(self, returnLocatorName=False):
        return self.getElement('browseDirectoryHeader', returnLocatorName)

    def getLogsHeader(self, returnLocatorName=False):
        return self.getElement('logsHeader', returnLocatorName)

    def getLogsHeaderError(self, returnLocatorName=False):
        return self.getElement('logsHeaderError', returnLocatorName)

    def getMetricsJmx(self, returnLocatorName=False):
        return self.getElement('metricsJmx', returnLocatorName)

    def getConf(self, returnLocatorName=False):
        return self.getElement('conf', returnLocatorName)

    def getProcessThreadDump(self, returnLocatorName=False):
        return self.getElement('processThreadDump', returnLocatorName)

    def clickOverviewTab(self):
        SeleniumDP.click(self.driver, self.getOverviewTab())

    def clickDatanodesTab(self):
        SeleniumDP.click(self.driver, self.getDatanodesTab())

    def clickFirstDatanodeLink(self):
        SeleniumDP.click(self.driver, self.getFirstDatanodeLink())

    def clickSecondDatanodeLink(self):
        SeleniumDP.click(self.driver, self.getSecondDatanodeLink())

    def clickDatanodesVolumeFailuresTab(self):
        SeleniumDP.click(self.driver, self.getDatanodesVolumeFailuresTab())

    def clickSnapshotTab(self):
        SeleniumDP.click(self.driver, self.getSnapshotTab())

    def clickStartupProgressTab(self):
        SeleniumDP.click(self.driver, self.getStartupProgressTab())

    def clickUtilitiesDropdown(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdown())

    def clickUtilitiesDropdownBrowseFS(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownBrowseFS())

    def clickUtilitiesDropdownLogs(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownLogs())

    def clickUtilitiesDropdownMetrics(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownMetrics())

    def clickUtilitiesDropdownConfiguration(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownConfiguration())

    def clickUtilitiesDropdownProcessThreadDump(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownProcessThreadDump())

    @TaskReporter.report_test()
    def gotoOverviewTab(self):
        self.checkElementonPage(
            locatorName=self.getOverviewTab(returnLocatorName=True),
            locatorMessage='Overview Tab',
            retryCount=4,
            timeout=0.5
        )
        self.clickOverviewTab()
        assert self.checkElementonPage(
            locatorName=self.getOverviewContainer(returnLocatorName=True),
            locatorMessage='Overview Container',
            retryCount=4,
            timeout=0.5
        )

    @TaskReporter.report_test()
    def gotoDatanodesTab(self):
        # Due to intermittent failures added a static wait here.
        import time
        time.sleep(5)
        self.checkElementonPage(
            locatorName=self.getDatanodesTab(returnLocatorName=True),
            locatorMessage='Datanodes Tab',
            retryCount=4,
            timeout=0.5
        )
        self.clickDatanodesTab()
        assert self.checkElementonPage(
            locatorName=self.getDatanodeContainer(returnLocatorName=True),
            locatorMessage='Datanodes Container',
            retryCount=4,
            timeout=0.5
        )

    @TaskReporter.report_test()
    def gotoDatanodeUI(self):
        self.gotoDatanodesTab()
        self.clickFirstDatanodeLink()

    @TaskReporter.report_test()
    def gotoDatanodesVolumeFailuresTab(self):
        self.checkElementonPage(
            locatorName=self.getDatanodesVolumeFailuresTab(returnLocatorName=True),
            locatorMessage='Datanodes Volume Failures Tab',
            retryCount=4,
            timeout=0.5
        )
        self.clickDatanodesVolumeFailuresTab()
        assert self.checkElementonPage(
            locatorName=self.getDatanodeVolumeFailureContainer(returnLocatorName=True),
            locatorMessage='Datanodes Volume Failures Container',
            retryCount=4,
            timeout=0.5
        )

    @TaskReporter.report_test()
    def gotoSnapshotTab(self):
        self.checkElementonPage(
            locatorName=self.getSnapshotTab(returnLocatorName=True),
            locatorMessage='Snapshot Tab',
            retryCount=4,
            timeout=0.5
        )
        self.clickSnapshotTab()
        assert self.checkElementonPage(
            locatorName=self.getSnapshotContainer(returnLocatorName=True),
            locatorMessage='Snapshot Container',
            retryCount=4,
            timeout=0.5
        )

    @TaskReporter.report_test()
    def gotoStartupProgressTab(self):
        self.checkElementonPage(
            locatorName=self.getStartupProgressTab(returnLocatorName=True),
            locatorMessage='Startup Progress Tab',
            retryCount=4,
            timeout=0.5
        )
        self.clickStartupProgressTab()
        assert self.checkElementonPage(
            locatorName=self.getStartupProgressContainer(returnLocatorName=True),
            locatorMessage='Startup Progress Container',
            retryCount=4,
            timeout=0.5
        )

    @TaskReporter.report_test()
    def gotoUtilitiesBrowseFS(self):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownBrowseFS(returnLocatorName=True),
            locatorMessage='Utilities Dropdown BrowseFS',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownBrowseFS()
        assert self.checkElementonPage(
            locatorName=self.getBrowseDirectoryHeader(returnLocatorName=True),
            locatorMessage='Browse Directory Container',
            retryCount=4,
            timeout=5
        )

    @TaskReporter.report_test()
    def gotoUtilitiesLogs(self, errorExcepted=False):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownLogs(returnLocatorName=True),
            locatorMessage='Utilities Dropdown Logs',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownLogs()
        if errorExcepted:
            assert self.checkElementonPage(
                locatorName=self.getLogsHeaderError(returnLocatorName=True),
                locatorMessage='Logs Error',
                retryCount=4,
                timeout=5
            )
        else:
            assert self.checkElementonPage(
                locatorName=self.getLogsHeader(returnLocatorName=True),
                locatorMessage='Logs ',
                retryCount=4,
                timeout=5
            )

    @TaskReporter.report_test()
    def gotoUtilitiesMetrics(self):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownMetrics(returnLocatorName=True),
            locatorMessage='Utilities Dropdown Metrics',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownMetrics()
        assert self.checkElementonPage(
            locatorName=self.getMetricsJmx(returnLocatorName=True),
            locatorMessage='Metrics JMX',
            retryCount=4,
            timeout=5
        )

    @TaskReporter.report_test()
    def gotoUtilitiesConfiguration(self):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownConfiguration(returnLocatorName=True),
            locatorMessage='Utilities Dropdown Configuration',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownConfiguration()
        assert self.checkElementonPage(
            locatorName=self.getConf(returnLocatorName=True), locatorMessage='Configuration', retryCount=4, timeout=5
        )

    @TaskReporter.report_test()
    def gotoUtilitiesProcessThreadDump(self):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownProcessThreadDump(returnLocatorName=True),
            locatorMessage='Utilities Dropdown Process Thread Dump',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownProcessThreadDump()
        assert self.checkElementonPage(
            locatorName=self.getProcessThreadDump(returnLocatorName=True),
            locatorMessage='Process Thread Dump',
            retryCount=4,
            timeout=5
        )
