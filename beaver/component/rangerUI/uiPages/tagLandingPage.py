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

from beaver.machine import Machine
import os, sys

logger = logging.getLogger(__name__)


class TagLandingPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "addTagRepoBtn": (By.XPATH, "//span[contains(.,'tag')]/span/a[@class='pull-right text-decoration']"),
    }

    def getAddTagRepoBtn(self, returnLocatorName=False):
        return self.getElement('addTagRepoBtn', returnLocatorName)

    def addLinkForTagServiceToLocDict(self, tagServiceName):
        self.locator_dictionary["tagServiceLink"] = (
            By.XPATH,
            "//table[@class='table table-bordered table-striped']//div/a[contains(.,'" + tagServiceName + "')]"
        )

    def getLinkForTagService(self, tagServiceName, returnLocatorName=False):
        self.addLinkForTagServiceToLocDict(tagServiceName)
        return self.getElement('tagServiceLink', returnLocatorName)

    def checkIsTagLandingPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName='addTagRepoBtn',
            locatorMessage='Tag Landing Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def clickOnTagServiceToGotoTagPolLandingPg(self, tagServiceName):
        SeleniumDP.click(self.driver, self.getLinkForTagService(tagServiceName))
