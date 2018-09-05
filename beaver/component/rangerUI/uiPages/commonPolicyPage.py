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
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class CommonPolicyPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "policyDetailsSubHeading": (By.XPATH, "//div[@data-id='r_form']//p[contains(.,'Policy Details :')]"),
        "policyNameInput": (By.XPATH, "//input[@name='name']"),
        "groupInputBoxInFirstPermRow": (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']/tr[1]/td[1]//li[@class='select2-search-field']/input"
        ),
        "selectChosenGroupInFirstPermRow": (
            By.XPATH, "//div[@id='select2-drop']/ul[@class='select2-results']//div[@class='select2-result-label']"
        ),
        "userInputBoxInFirstPermRow": (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']/tr[1]/td[2]//li[@class='select2-search-field']/input"
        ),
        "selectChosenUserInFirstPermRow": (
            By.XPATH, "//div[@id='select2-drop']/ul[@class='select2-results']//div[@class='select2-result-label']"
        ),
        "addPermissionsBtnInFirstPermRow": (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']/tr[1]//button[contains(@class,'add-permissions')]"
        ),
        "addEditPermsHeadingInPermsPopover": (
            By.XPATH,
            "//div[@class='popover fade in editable-container editable-popup top']//h3[contains(.,'add/edit permissions')]"
        ),
        "tickPermissionsToBeSetInPopover": (
            By.XPATH,
            "//div[@class='popover-content']//form[@class='form-inline editableform']//div[@class='editable-buttons']/button[@type='submit']"
        ),
        "inFormAddPolicyButton": (By.XPATH, "//button[@data-id='save']")
    }

    def getPolicyDetailsSubHeading(self, returnLocatorName=False):
        return self.getElement('policyDetailsSubHeading', returnLocatorName)

    def getPolicyNameInput(self, returnLocatorName=False):
        return self.getElement('policyNameInput', returnLocatorName)

    def getGroupInputBoxInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('groupInputBoxInFirstPermRow', returnLocatorName)

    def getSelectChosenGroupInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('selectChosenGroupInFirstPermRow', returnLocatorName)

    def getUserInputBoxInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('userInputBoxInFirstPermRow', returnLocatorName)

    def getSelectChosenUserInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('selectChosenUserInFirstPermRow', returnLocatorName)

    def getAddPermissionsBtnInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('addPermissionsBtnInFirstPermRow', returnLocatorName)

    def getAddEditPermsHeadingInPermsPopover(self, returnLocatorName=False):
        return self.getElement('addEditPermsHeadingInPermsPopover', returnLocatorName)

    def getTickPermissionsToBeSetInPopover(self, returnLocatorName=False):
        return self.getElement('tickPermissionsToBeSetInPopover', returnLocatorName)

    def getInFormAddPolicyButton(self, returnLocatorName=False):
        return self.getElement('inFormAddPolicyButton', returnLocatorName)

    def addSelectedUserInFirstPermRowToLocDict(self, userName):
        self.locator_dictionary["selectedUserInFirstPermRow"] = (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']//div[contains(@id,'s2id_autogen')]//li[@class='select2-search-choice']/div[contains(.,'"
            + userName + "')]"
        )

    def getSelectedUserInFirstPermRow(self, userName, returnLocatorName=False):
        self.addSelectedUserInFirstPermRowToLocDict(userName)
        return self.getElement('selectedUserInFirstPermRow', returnLocatorName)

    def addSelectedGroupInFirstPermRowToLocDict(self, groupName):
        self.locator_dictionary["selectedGroupInFirstPermRow"] = (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']//div[contains(@id,'s2id_autogen')]//li[@class='select2-search-choice']/div[contains(.,'"
            + groupName + "')]"
        )

    def getSelectedGroupInFirstPermRow(self, groupName, returnLocatorName=False):
        self.addSelectedGroupInFirstPermRowToLocDict(groupName)
        return self.getElement('selectedGroupInFirstPermRow', returnLocatorName)

    def addCheckBoxForPermissionToLocDict(self, permissionName):
        self.locator_dictionary["checkBoxForPermission"] = (
            By.XPATH,
            "//div[@class='popover-content']//form[@class='form-inline editableform']//div[@class='editable-input']//span[contains(.,'"
            + permissionName + "')]/preceding-sibling::input[@type='checkbox']"
        )

    def getCheckBoxForPermissionInPermPopup(self, permissionName, returnLocatorName=False):
        self.addCheckBoxForPermissionToLocDict(permissionName)
        return self.getElement('checkBoxForPermission', returnLocatorName)

    def addSelectedPermissionInAccessTypesToLocDict(self, permName):
        self.locator_dictionary["selectedPermissionInAccessTypes"] = (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']/tr[1]/td[3]//span[contains(.,'" +
            permName + "')]"
        )

    def getSelectedPermissionInAccessTypes(self, permName, returnLocatorName=False):
        self.addSelectedPermissionInAccessTypesToLocDict(permName)
        return self.getElement('selectedPermissionInAccessTypes', returnLocatorName)

    def checkIsPolicyPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getPolicyDetailsSubHeading(returnLocatorName=True),
            locatorMessage='Ranger Policy Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def clickOnInFormAddPolicyButton(self, driver):
        SeleniumDP.click(driver, self.getInFormAddPolicyButton())

    def clickOnChosenGroupInGroupPopUp(self, driver):
        SeleniumDP.click(driver, self.getSelectChosenGroupInFirstPermRow())

    def clickOnChosenUserInUserPopUp1(self, driver):
        SeleniumDP.click(driver, self.getSelectChosenUserInFirstPermRow())

    def clickOnChosenUserInUserPopUp(self, driver):
        driver.find_element_by_xpath(
            "//div[@id='select2-drop']/ul[@class='select2-results']//div[@class='select2-result-label']"
        ).click()

    def clickOnAddPermissionsBtnInFirstPermRow(self, driver):
        SeleniumDP.click(driver, self.getAddPermissionsBtnInFirstPermRow())

    def clickOnPermissionCheckBox(self, driver, permName):
        SeleniumDP.click(driver, self.getCheckBoxForPermissionInPermPopup(permName))

    def clickOnTickToSetPermissionsInPopover(self, driver):
        SeleniumDP.click(driver, self.getTickPermissionsToBeSetInPopover())

    @TaskReporter.report_test()
    def fillGroupInPermissions(self, driver, grpName):
        SeleniumDP.sendKeys(self.getGroupInputBoxInFirstPermRow(), grpName)
        self.waitForElement(self, self.getSelectChosenGroupInFirstPermRow(returnLocatorName=True))
        self.clickOnChosenGroupInGroupPopUp(driver)
        self.waitForElement(self, self.getSelectedGroupInFirstPermRow(grpName, returnLocatorName=True))

    @TaskReporter.report_test()
    def fillUserInPermissions(self, driver, userName):
        SeleniumDP.sendKeys(self.getUserInputBoxInFirstPermRow(), userName)
        self.waitForElement(self, self.getSelectChosenUserInFirstPermRow(returnLocatorName=True))
        self.clickOnChosenUserInUserPopUp(driver)
        self.waitForElement(self, self.getSelectedUserInFirstPermRow(userName, returnLocatorName=True))

    @TaskReporter.report_test()
    def enterUserGroupPermissions(self, driver, accessType, isAccessTypeToBeSet=False, grpName=None, userName=None):
        if grpName:
            self.fillGroupInPermissions(driver, grpName)
        if userName:
            self.fillUserInPermissions(driver, userName)
        if isAccessTypeToBeSet:
            self.clickOnAddPermissionsBtnInFirstPermRow(driver)
            self.waitForElement(self, self.getAddEditPermsHeadingInPermsPopover(returnLocatorName=True))
            self.clickOnPermissionCheckBox(driver, accessType)
            self.clickOnTickToSetPermissionsInPopover(driver)
