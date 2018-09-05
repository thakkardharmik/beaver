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


class CommonPolicyLandingPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "addPolicyBtn": (By.XPATH, "//div[contains(@class,'clearfix')]/a[contains(text(),'Add New Policy')]")
    }

    def getAddPolicyBtn(self, returnLocatorName=False):
        return self.getElement('addPolicyBtn', returnLocatorName)

    def addSrvcNamePoliciesBreadcrumbToLocDict(self, serviceName):
        self.locator_dictionary["policiesBreadcrumb"] = (
            By.XPATH, "//section[@id='r_breadcrumbs']//a[contains(.,'" + serviceName + " Policies')]"
        )

    def getSrvcNamePoliciesBreadcrumb(self, serviceName, returnLocatorName=False):
        self.addSrvcNamePoliciesBreadcrumbToLocDict(serviceName)
        return self.getElement('policiesBreadcrumb', returnLocatorName)

    def clickOnAddNewPolicyBtn(self):
        SeleniumDP.click(self.driver, self.getAddPolicyBtn())

    def checkIsPolicyLandingPg(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            pageObj=self,
            locatorName='addPolicyBtn',
            locatorMessage='Policy Landing Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def checkIsPolicyLandingPgOfParticularService(
            self, serviceName, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            pageObj=self,
            locatorName=self.getSrvcNamePoliciesBreadcrumb(serviceName, returnLocatorName=True),
            locatorMessage='Policy Landing Page for service ' + serviceName,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def addXpathForPolicyInPolicyTableToLocDict(self, policyName):
        self.locator_dictionary["policyInPolicyTable"] = (
            By.XPATH,
            "//table[@class='table table-bordered table-condensed backgrid']//td[contains(.,'" + policyName + "')]"
        )

    def getPolicyInPolicyTable(self, policyName, returnLocatorName=False):
        self.addXpathForPolicyInPolicyTableToLocDict(policyName)
        return self.getElement('policyInPolicyTable', returnLocatorName)

    def checkIsPolicyAddedSuccessfully(
            self, policyName=None, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            self,
            locatorName=self.getPolicyInPolicyTable(policyName, returnLocatorName=True),
            locatorMessage='Policy: ' + policyName,
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def addXpathForPolicyIdLinkInPolicyTableToLocDict(self, policyName):
        self.locator_dictionary["policyIdLinkInPolicyTable"] = (
            By.XPATH, "//table[@class='table table-bordered table-condensed backgrid']//td[contains(.,'" + policyName +
            "')]/preceding-sibling::td[1]//a"
        )

    def getPolicyIdLinkFromPolicyTable(self, policyName, returnLocatorName=False):
        self.addXpathForPolicyIdLinkInPolicyTableToLocDict(policyName)
        return self.getElement('policyIdLinkInPolicyTable', returnLocatorName)
