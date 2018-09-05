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
from beaver.component.yarnUI.basePage import BasePage

logger = logging.getLogger(__name__)


class clusterPage(BasePage):

    appTableKey = ["ID","User","Name","Application Type","Queue","Application Priority","StartTime","FinishTime",\
                     "State","FinalStatus","Running Containers", "Allocated CPU VCores","Allocated Memory MB",\
                       "% of Queue","% of Cluster","Progress","Tracking UI","Blacklisted Nodes"]

    appTableEntries = {}

    locator_dictionary = {
        "appTable": (By.XPATH, '//*[@id="apps"]'),
    }

    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    def isClusterPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('appTable', True),
            locatorMessage='All Applications',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getApplicationInfo(self, searchElement):
        appTableEntries = self.getRowFromTable("appTable", searchElement)
        print appTableEntries
        return appTableEntries

    def getAppFromAppTableEntries(self, appTableEntries):
        for value in appTableEntries["ID"].itervalues:
            print value

    def clickOnApplication(self, appID):
        self.driver.find_element_by_link_text(appID).click()
