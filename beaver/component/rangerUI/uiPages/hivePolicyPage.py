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
from beaver.component.rangerUI.uiPages.commonPolicyPage import CommonPolicyPage
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class HivePolicyPage(BasePage):

    global commonPolicyPg

    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)
        self.commonPolicyPg = CommonPolicyPage(basePageObj)

    locator_dictionary = {
        "hiveDbInput": (
            By.XPATH,
            "//div[@class='control-group field-database']//div[@name='database']//ul[@class='select2-choices']/li/input[@type='text']"
        ),
        "hiveDbNameSetInInputBox": (
            By.XPATH, "//div[@name='database']//ul[@class='select2-choices']/li[@class='select2-search-choice']"
        ),
        "hiveRsrcDropdownWithEnteredOption": (
            By.XPATH, "//div[@id='select2-drop']//div[@class='select2-result-label']"
        ),
        "hiveTableInput": (
            By.XPATH,
            "//div[@data-name='field-table']//div[contains(@id,'autogen')]/ul[@class='select2-choices']/li/input[@type='text']"
        ),
        "hiveTableNameSetInInputBox": (
            By.XPATH, "//div[@name='table']//ul[@class='select2-choices']/li[@class='select2-search-choice']"
        ),
        "hiveColumnInput": (By.CSS_SELECTOR, "div[name='column'] li input[type='text'] "),
        "hiveColumnNameSetInInputBox": (
            By.XPATH, "//div[@name='column']//ul[@class='select2-choices']/li[@class='select2-search-choice']"
        ),
    }

    def getHiveDbInput(self, returnLocatorName=False):
        return self.getElement('hiveDbInput', returnLocatorName)

    def getHiveTableInput(self, returnLocatorName=False):
        return self.getElement('hiveTableInput', returnLocatorName)

    def getHiveColumnInput(self, returnLocatorName=False):
        return self.getElement('hiveColumnInput', returnLocatorName)

    def getHiveRsrcDropdownWithEnteredOption(self, returnLocatorName=False):
        return self.getElement('hiveRsrcDropdownWithEnteredOption', returnLocatorName)

    def getHiveDbNameSetInInputBox(self, returnLocatorName=False):
        return self.getElement('hiveDbNameSetInInputBox', returnLocatorName)

    def getHiveTableNameSetInInputBox(self, returnLocatorName=False):
        return self.getElement('hiveTableNameSetInInputBox', returnLocatorName)

    def getHiveColumnNameSetInInputBox(self, returnLocatorName=False):
        return self.getElement('hiveColumnNameSetInInputBox', returnLocatorName)

    def checkIsHivePolicyPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getHiveTableInput(returnLocatorName=True),
            locatorMessage='Hive Policy Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def clickOnHiveRsrcDropdownWithEnteredOption(self, driver):
        #SeleniumDP.click(driver, self.getHiveRsrcDropdownWithEnteredOption()) --> Unable to find element
        driver.find_element_by_xpath("//div[@id='select2-drop']//div[@class='select2-result-label']").click()

    @TaskReporter.report_test()
    def enterValuesInHivePolicyPg(
            self,
            driver,
            policyName,
            dbName,
            tableName,
            columnName,
            accessType,
            isAccessTypeToBeSet=False,
            grpName=None,
            userName=None
    ):
        SeleniumDP.sendKeys(self.commonPolicyPg.getPolicyNameInput(), policyName)
        SeleniumDP.sendKeys(self.getHiveDbInput(), dbName)
        self.waitForElement(self, self.getHiveRsrcDropdownWithEnteredOption(returnLocatorName=True))
        self.clickOnHiveRsrcDropdownWithEnteredOption(driver)
        self.waitForElement(self, self.getHiveDbNameSetInInputBox(returnLocatorName=True))
        SeleniumDP.sendKeys(self.getHiveTableInput(), tableName)
        self.waitForElement(self, self.getHiveRsrcDropdownWithEnteredOption(returnLocatorName=True))
        self.clickOnHiveRsrcDropdownWithEnteredOption(driver)
        self.waitForElement(self, self.getHiveTableNameSetInInputBox(returnLocatorName=True))
        SeleniumDP.sendKeys(self.getHiveColumnInput(), columnName)
        self.waitForElement(self, self.getHiveRsrcDropdownWithEnteredOption(returnLocatorName=True))
        self.clickOnHiveRsrcDropdownWithEnteredOption(driver)
        self.waitForElement(self, self.getHiveColumnNameSetInInputBox(returnLocatorName=True))
        self.commonPolicyPg.enterUserGroupPermissions(driver, accessType, isAccessTypeToBeSet, grpName, userName)
