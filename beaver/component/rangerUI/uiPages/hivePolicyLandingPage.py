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
from beaver.component.rangerUI.uiPages.commonPolicyLandingPage import CommonPolicyLandingPage

logger = logging.getLogger(__name__)


class HivePolicyLandingPage(CommonPolicyLandingPage):
    def __init__(self, basePageObj):
        CommonPolicyLandingPage.__init__(self, basePageObj)

    locator_dictionary = {
        "maskingPolicyTab": (
            By.XPATH,
            "//*[@id='r_content']//div[@data-id='policyTypeTab']/ul[@class='nav nav-tabs tabs clearfix']/li[@data-tab='masking']/a"
        ),
        "activeMaskingPolicyTab": (
            By.XPATH,
            "//*[@id='r_content']//div[@data-id='policyTypeTab']/ul[@class='nav nav-tabs tabs clearfix']/li[@class='active' and @data-tab='masking']/a"
        ),
    }

    def getMaskingPolicyTab(self, returnLocatorName=False):
        return self.getElement('maskingPolicyTab', returnLocatorName)

    def getActiveMaskingPolicyTab(self, returnLocatorName=False):
        return self.getElement('activeMaskingPolicyTab', returnLocatorName)

    def addXpathForPolicyInHivePolicyTableToLocDict(self, policyName):
        self.locator_dictionary["policyInHivePolicyTable"] = (
            By.XPATH,
            "//table[@class='table table-bordered table-condensed backgrid']//td[contains(.,'" + policyName + "')]"
        )

    def getPolicyInHivePolicyTable(self, policyName, returnLocatorName=False):
        self.addXpathForPolicyInHivePolicyTableToLocDict(policyName)
        return self.getElement('policyInHivePolicyTable', returnLocatorName)

    def addXpathForPolicyIdLinkInHivePolicyTableToLocDict(self, policyName):
        self.locator_dictionary["policyIdLinkInHivePolicyTable"] = (
            By.XPATH, "//table[@class='table table-bordered table-condensed backgrid']//td[contains(.,'" + policyName +
            "')]/preceding-sibling::td[1]//a"
        )

    def getPolicyIdLinkFromHivePolicyTable(self, policyName, returnLocatorName=False):
        self.addXpathForPolicyIdLinkInHivePolicyTableToLocDict(policyName)
        return self.getElement('policyIdLinkInHivePolicyTable', returnLocatorName)

    def checkIsMaskingPolicyLandingPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getActiveMaskingPolicyTab(returnLocatorName=True),
            locatorMessage='Hive Masking Policy Landing Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def checkIsHivePolicyAddedSuccessfully(
            self, policyName, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            self,
            locatorName=self.getPolicyInHivePolicyTable(policyName, returnLocatorName=True),
            locatorMessage='Hive Policy: ' + policyName,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def clickOnMaskingTabAndVerify(self):
        SeleniumDP.click(self.driver, self.getMaskingPolicyTab())
        if not self.checkIsMaskingPolicyLandingPage(timeout=10):
            logger.info("Not Masking policy page")
