#__author__ = 'aleekha'
#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#

import logging, time
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.rangerUI.uiPages.basePage import BasePage
from beaver.component.rangerUI.uiPages.commonPolicyPage import CommonPolicyPage
from selenium.webdriver.common.keys import Keys
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class TagPolicyPage(BasePage):

    global commonPolicyPg, basePageObj

    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)
        self.commonPolicyPg = CommonPolicyPage(basePageObj)
        self.basePageObj = basePageObj

    locator_dictionary = {
        "tagInput": (
            By.XPATH, "//div[@class='control-group field-tag']//ul[@class='select2-choices']/li/input[@type='text']"
        ),
        "tagNameSetInInputBox": (
            By.XPATH, "//div[@name='tag']//ul[@class='select2-choices']/li[@class='select2-search-choice']"
        ),
        "addPolicyConditionsBtnOnPage": (
            By.XPATH,
            "//div[@class='wrap position-relative']//div[@data-customfields='groupPerms']//table[@class='table-permission table-condensed']//a[@id='policyConditions']/following-sibling::button[@title='add']"
        ),
        "expiryDateInputBoxInPolicyCondPopup": (
            By.XPATH, "//div[@class='popover-content']//ul[@class='select2-choices']/li/input[@type='text']"
        ),
        "booleanExprTextAreaInPolCondPopup": (
            By.XPATH,
            "//div[@class='popover-content']//div[@class='editable-address margin-bottom-5']//textarea[@class='textAreaContainer']"
        ),
        "submitBtnInPolCondPopup": (
            By.XPATH, "//div[@class='popover-content']//div[@class='editable-buttons']/button[@type='submit']"
        ),
        "filledPolCondExprOnPage": (By.XPATH, "//span[contains(.,'expression : JavaScript Condition')]"),
        "addCompPermsBtnOnPage": (
            By.XPATH,
            "//div[@class='wrap position-relative']//div[@data-customfields='groupPerms']//table[@class='table-permission table-condensed']//a[contains(.,'Add Permissions')]/following-sibling::button[@title='add']"
        ),
        "clickablePartInCompPermDropdownInPopup": (
            By.XPATH,
            "//div[@class='popover-content']//div[@class='select2-container select2-container-multi']//ul[@class='select2-choices']//input[@type='text']"
        ),
        "hiveOptionInCompPermDropdownInPopup": (
            By.XPATH, "//div[@id='select2-drop']//div[@class='select2-result-label']/span[contains(.,'hive')]"
        ),
        "hiveCompPermSetInCompPermInputBoxInPopup": (
            By.XPATH, "//div[@class='editable-checklist']//ul[@class='select2-choices']//li[contains(.,'hive')]"
        ),
        "submitBtnInCompPermPopup": (
            By.XPATH, "//div[@class='editable-buttons editable-buttons-bottom']/button[@type='submit']"
        ),
        "globalHivePermCheckBoxInCompPermPopup": (
            By.XPATH, "//div[@class='editable-checklist']//tr[@data-id='hive']//th//input[@type='checkbox']"
        )
    }

    def getTagInput(self, returnLocatorName=False):
        return self.getElement('tagInput', returnLocatorName)

    def addTagRsrcDropdownWithEnteredOptionToLocDict(self, tagName):
        self.locator_dictionary["tagRsrcDropdownWithEnteredOption"] = (
            By.XPATH, "//div[@id='select2-drop']//ul[@class='select2-results']/li/div[contains(.,'" + tagName + "')]"
        )

    def getTagRsrcDropdownWithEnteredOption(self, tagName, returnLocatorName=False):
        self.addTagRsrcDropdownWithEnteredOptionToLocDict(tagName)
        return self.getElement('tagRsrcDropdownWithEnteredOption', returnLocatorName)

    #def getTagRsrcDropdownWithEnteredOption(self, returnLocatorName=False):
    #    return self.getElement('tagRsrcDropdownWithEnteredOption', returnLocatorName)

    def getTagNameSetInInputBox(self, returnLocatorName=False):
        return self.getElement('tagNameSetInInputBox', returnLocatorName)

    def getAddPolicyConditionsBtnOnPage(self, returnLocatorName=False):
        return self.getElement('addPolicyConditionsBtnOnPage', returnLocatorName)

    def getExpiryDateInputBoxInPolicyCondPopup(self, returnLocatorName=False):
        return self.getElement('expiryDateInputBoxInPolicyCondPopup', returnLocatorName)

    def checkIsTagPolicyPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getTagInput(returnLocatorName=True),
            locatorMessage='Tag Policy Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def clickOnTagRsrcDropdownWithEnteredOption(self, driver):
        #SeleniumDP.click(driver, self.getTagRsrcDropdownWithEnteredOption()) --> Unable to find element
        driver.find_element_by_xpath("//div[@id='select2-drop']//div[@class='select2-result-label']").click()

    def clickOnAddPolicyConditionsBtnOnPage(self, driver):
        SeleniumDP.click(driver, self.getAddPolicyConditionsBtnOnPage())  #--> Unable to find element
        #driver.find_element_by_xpath("//div[@class='wrap position-relative']//div[@data-customfields='groupPerms']//table[@class='table-permission table-condensed']//a[@id='policyConditions']/following-sibling::button[@title='add']").click()

    def getBooleanExprTextAreaInPolCondPopup(self, returnLocatorName=False):
        return self.getElement('booleanExprTextAreaInPolCondPopup', returnLocatorName)

    def getSubmitBtnInPolCondPopup(self, returnLocatorName=False):
        return self.getElement('submitBtnInPolCondPopup', returnLocatorName)

    def clickOnSubmitBtnInPolCondPopup(self, driver):
        SeleniumDP.click(driver, self.getSubmitBtnInPolCondPopup())

    def getFilledPolCondExprOnPage(self, returnLocatorName=False):
        return self.getElement('filledPolCondExprOnPage', returnLocatorName)

    def getAddCompPermsBtnOnPage(self, returnLocatorName=False):
        return self.getElement('addCompPermsBtnOnPage', returnLocatorName)

    def clickOnAddCompPermsBtnOnPage(self, driver):
        SeleniumDP.click(driver, self.getAddCompPermsBtnOnPage())

    def getClickablePartInCompPermDropdownInPopup(self, returnLocatorName=False):
        return self.getElement('clickablePartInCompPermDropdownInPopup', returnLocatorName)

    def clickOnClickablePartInCompPermDropdownInPopup(self, driver):
        driver.find_element_by_xpath(
            "//div[@class='popover-content']//div[@class='select2-container select2-container-multi']//ul[@class='select2-choices']//input[@type='text']"
        ).click()
        #driver.find_element_by_xpath("//div[@class='popover-content']//div[@class='select2-container select2-container-multi']//ul[@class='select2-choices']//input[@type='text']").send_keys(Keys.RETURN)

    def getHiveOptionInCompPermDropdownInPopup(self, returnLocatorName=False):
        return self.getElement('hiveOptionInCompPermDropdownInPopup', returnLocatorName)

    def clickOnHiveOptionInCompPermDropdownInPopup(self, driver):
        driver.find_element_by_xpath(
            "//div[@id='select2-drop']//div[@class='select2-result-label']/span[contains(.,'hive')]"
        ).click()

    def getHiveCompPermSetInCompPermInputBoxInPopup(self, returnLocatorName=False):
        return self.getElement('hiveCompPermSetInCompPermInputBoxInPopup', returnLocatorName)

    def getSubmitBtnInCompPermPopup(self, returnLocatorName=False):
        return self.getElement('submitBtnInCompPermPopup', returnLocatorName)

    def clickOnSubmitBtnInCompPermPopup(self, driver):
        #driver.find_element_by_xpath("//div[@class='editable-buttons editable-buttons-bottom']/button[@type='submit']").click()
        SeleniumDP.click(driver, self.getSubmitBtnInCompPermPopup())

    def getGlobalHivePermCheckBoxInCompPermPopup(self, returnLocatorName=False):
        return self.getElement('globalHivePermCheckBoxInCompPermPopup', returnLocatorName)

    def clickOnGlobalHivePermCheckBoxInCompPermPopup(self, driver):
        SeleniumDP.click(driver, self.getGlobalHivePermCheckBoxInCompPermPopup())

    @TaskReporter.report_test()
    def enterValuesInTagPolicyPg(
            self,
            driver,
            policyName,
            tagName,
            accessType,
            isAccessTypeToBeSet=False,
            grpName=None,
            userName=None,
            policyCondition=None
    ):
        SeleniumDP.sendKeys(self.commonPolicyPg.getPolicyNameInput(), policyName)
        SeleniumDP.sendKeys(self.getTagInput(), tagName + u'\ue007' + u'\ue007')
        #SeleniumDP.sendKeys(self.getTagInput(), tagName+Keys.RETURN+Keys.RETURN)
        #self.waitForElement(self, self.getTagRsrcDropdownWithEnteredOption(returnLocatorName=True))
        #self.waitForElement(self, self.getTagRsrcDropdownWithEnteredOption(tagName,returnLocatorName=True))
        #self.clickOnTagRsrcDropdownWithEnteredOption(driver)
        self.waitForElement(self, self.getTagNameSetInInputBox(returnLocatorName=True))
        self.commonPolicyPg.enterUserGroupPermissions(driver, accessType, isAccessTypeToBeSet, grpName, userName)
        if policyCondition is not None:
            self.clickOnAddPolicyConditionsBtnOnPage(driver)
            self.waitForElement(self, self.getExpiryDateInputBoxInPolicyCondPopup(returnLocatorName=True))
            SeleniumDP.sendKeys(self.getBooleanExprTextAreaInPolCondPopup(), policyCondition)
            time.sleep(5)
            self.clickOnSubmitBtnInPolCondPopup(driver)
            self.waitForElement(self, self.getFilledPolCondExprOnPage(returnLocatorName=True))
        self.clickOnAddCompPermsBtnOnPage(driver)
        time.sleep(3)
        SeleniumDP.sendKeys(self.getClickablePartInCompPermDropdownInPopup(), 'hive' + Keys.RETURN)
        self.waitForElement(self, self.getHiveCompPermSetInCompPermInputBoxInPopup(returnLocatorName=True))
        self.clickOnGlobalHivePermCheckBoxInCompPermPopup(driver)
        time.sleep(3)
        self.clickOnSubmitBtnInCompPermPopup(driver)

    @TaskReporter.report_test()
    def addTagPolicy(
            self,
            driver,
            policyName,
            tagName,
            accessType,
            isAccessTypeToBeSet,
            grpName=None,
            userName=None,
            policyCondition=None
    ):
        self.enterValuesInTagPolicyPg(
            driver, policyName, tagName, accessType, isAccessTypeToBeSet, grpName, userName, policyCondition
        )
        self.commonPolicyPg.clickOnInFormAddPolicyButton(driver)
        #Wait for 35 seconds before issuing Beeline command so that policy cache is refreshed...
        time.sleep(35)
