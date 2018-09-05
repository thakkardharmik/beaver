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


class DNUIPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, driver=basePageObj.driver, base_url=basePageObj.base_url)

    locator_dictionary = {
        "overviewHeader": (By.XPATH, "//*[@id='tab-overview']//h1[contains(.,'DataNode on')]"),
        "utilitiesDropdown": (By.XPATH, "//*[@id='ui-tabs']/li/a[contains(.,'Utilities')]"),
        "utilitiesDropdownLogs": (By.XPATH, "//*[@id='ui-tabs']/li/ul/li/a[contains(.,'Logs')]"),
        "logsHeader": (By.XPATH, "/html/body/h1[contains(.,'Directory: /logs/')]"),
        "logsHeaderError": (By.XPATH, "/html/body/h2[contains(.,'HTTP ERROR: 403')]"),
    }

    def isDNUIPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getOverviewHeader(returnLocatorName=True),
            locatorMessage='DN UI Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getOverviewHeader(self, returnLocatorName=False):
        return self.getElement('overviewHeader', returnLocatorName)

    def getUtilitiesDropdown(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdown', returnLocatorName)

    def getUtilitiesDropdownLogs(self, returnLocatorName=False):
        return self.getElement('utilitiesDropdownLogs', returnLocatorName)

    def getLogsHeader(self, returnLocatorName=False):
        return self.getElement('logsHeader', returnLocatorName)

    def getLogsHeaderError(self, returnLocatorName=False):
        return self.getElement('logsHeaderError', returnLocatorName)

    def clickUtilitiesDropdown(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdown())

    def clickUtilitiesDropdownLogs(self):
        SeleniumDP.click(self.driver, self.getUtilitiesDropdownLogs())

    @TaskReporter.report_test()
    def gotoUtilitiesLogs(self, errorExcepted=False):
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdown(returnLocatorName=True),
            locatorMessage='DN Utilities Dropdown',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdown()
        self.checkElementonPage(
            locatorName=self.getUtilitiesDropdownLogs(returnLocatorName=True),
            locatorMessage='DN Utilities Dropdown Logs',
            retryCount=4,
            timeout=0.5
        )
        self.clickUtilitiesDropdownLogs()
        if errorExcepted:
            assert self.checkElementonPage(
                locatorName=self.getLogsHeaderError(returnLocatorName=True),
                locatorMessage='DN Logs ',
                retryCount=4,
                timeout=5
            )
        else:
            assert self.checkElementonPage(
                locatorName=self.getLogsHeader(returnLocatorName=True),
                locatorMessage='DN Logs ',
                retryCount=4,
                timeout=5
            )
