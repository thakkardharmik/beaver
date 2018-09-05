#__author__ = 'aleekha'
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
from beaver.component.rangerUI.uiPages.basePage import BasePage

logger = logging.getLogger(__name__)


class AuditPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "accessAuditTabLink": (By.XPATH, "//ul[@class='nav nav-tabs tabs clearfix']/li/a[contains(.,'Access')]"),
    }

    def getAccessAuditTabLink(self, returnLocatorName=False):
        return self.getElement('accessAuditTabLink', returnLocatorName)

    def clickToGotoAccessAuditPage(self, driver):
        SeleniumDP.click(driver, self.getAccessAuditTabLink())

    def checkIsAuditPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName='accessAuditTabLink',
            locatorMessage='Audit Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )
