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
from beaver.component.rangerUI.uiPages.hivePolicyPage import HivePolicyPage
from beaver.component.rangerUI.uiPages.commonPolicyPage import CommonPolicyPage
import time
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class HiveMaskingPolicyPage(BasePage):

    global hivePolicyPg, commonPolicyPg, basePageObj

    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)
        self.hivePolicyPg = HivePolicyPage(basePageObj)
        self.commonPolicyPg = CommonPolicyPage(basePageObj)
        self.basePageObj = basePageObj

    locator_dictionary = {
        "addMaskingOptionBtnInFirstPermRow": (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']/tr[1]//button[contains(@class,'add-masking-type')]"
        ),
        "maskingOptionHeadingInMaskingPopup": (
            By.XPATH,
            "//div[contains(@class,'popover fade in editable-container editable-popup')]/h3[contains(.,'Select Masking Option')]"
        ),
        "popupForSelectingMaskingOption": (By.XPATH, "//div[@class='popover-content']"),
        "radioBtnForCustomMaskingOption": (
            By.XPATH,
            "//div[@class='popover-content']//div[@class='editable-radiolist']//span[contains(.,'Custom')]/preceding-sibling::input[@type='radio']"
        ),
        "tickBtnToAddMaskOptionsInPopup": (
            By.XPATH,
            "//div[@class='popover-content']//div[@class='control-group']//div[@class='editable-buttons']/button[@type='submit']"
        ),
        "inputBoxForEnteringCustomMaskValue": (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']//a[@data-original-title='Select Masking Option']/following-sibling::input[@data-id='maskTypeCustom']"
        ),
        "popupForMaskValueNotEntered": (By.XPATH, "//div[@class='bootbox modal fade in']/div[@class='modal-body']"),
        "okBtnInPopupForMaskValueNotEntered": (
            By.XPATH, "//div[@class='bootbox modal fade in']/div[@class='modal-footer']/a[contains(.,'OK')]"
        ),
    }

    def getAddMaskingOptionBtnInFirstPermRow(self, returnLocatorName=False):
        return self.getElement('addMaskingOptionBtnInFirstPermRow', returnLocatorName)

    def getMaskingOptionHeadingInMaskingPopup(self, returnLocatorName=False):
        return self.getElement('maskingOptionHeadingInMaskingPopup', returnLocatorName)

    def getPopupForSelectingMaskingOption(self, returnLocatorName=False):
        return self.getElement('popupForSelectingMaskingOption', returnLocatorName)

    def getRadioBtnForCustomMaskingOption(self, returnLocatorName=False):
        return self.getElement('radioBtnForCustomMaskingOption', returnLocatorName)

    def getTickBtnToAddMaskOptionsInPopup(self, returnLocatorName=False):
        return self.getElement('tickBtnToAddMaskOptionsInPopup', returnLocatorName)

    def getInputBoxForEnteringCustomMaskValue(self, returnLocatorName=False):
        return self.getElement('inputBoxForEnteringCustomMaskValue', returnLocatorName)

    def addSelectedMaskingOptionInPermRowToLocDict(self, maskOption):
        self.locator_dictionary["selectedMaskingOptionInPermRow"] = (
            By.XPATH,
            "//tbody[@class='js-formInput permissionItemSortable ui-sortable']//a[@data-original-title='Select Masking Option']/span[contains(.,'"
            + maskOption + "')]"
        )

    def getSelectedMaskingOptionInPermRow(self, maskOption, returnLocatorName=False):
        self.addSelectedMaskingOptionInPermRowToLocDict(maskOption)
        return self.getElement('selectedMaskingOptionInPermRow', returnLocatorName)

    def getHiveRsrcDropdownWithEnteredOption(self, returnLocatorName=False):
        return self.getElement('hiveRsrcDropdownWithEnteredOption', returnLocatorName)

    def getPopupForMaskValueNotEntered(self, returnLocatorName=False):
        return self.getElement('popupForMaskValueNotEntered', returnLocatorName)

    def getOkBtnInPopupForMaskValueNotEntered(self, returnLocatorName=False):
        return self.getElement('okBtnInPopupForMaskValueNotEntered', returnLocatorName)

    def clickOnAddMaskingOptionsBtn(self, driver):
        SeleniumDP.click(driver, self.getAddMaskingOptionBtnInFirstPermRow())

    def clickOnCustomMaskOptionRadioBtnInPopup(self, driver):
        SeleniumDP.click(driver, self.getRadioBtnForCustomMaskingOption())

    def clickOnTickBtnToAddMaskOptionsInPopup(self, driver):
        SeleniumDP.click(driver, self.getTickBtnToAddMaskOptionsInPopup())

    def clickOnOkBtnInPopupForMaskValueNotEntered(self, driver):
        SeleniumDP.click(driver, self.getOkBtnInPopupForMaskValueNotEntered())

    @TaskReporter.report_test()
    def setCustomMaskingOption(self, driver, customMaskValue):
        self.clickOnAddMaskingOptionsBtn(driver)
        self.waitForElement(self, self.getMaskingOptionHeadingInMaskingPopup(returnLocatorName=True))
        self.clickOnCustomMaskOptionRadioBtnInPopup(driver)
        self.clickOnTickBtnToAddMaskOptionsInPopup(driver)
        self.waitForElement(self, self.getInputBoxForEnteringCustomMaskValue(returnLocatorName=True))
        SeleniumDP.sendKeys(self.getInputBoxForEnteringCustomMaskValue(), customMaskValue)

    def isErrorPopUpPresent(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getPopupForMaskValueNotEntered(returnLocatorName=True),
            locatorMessage='Popup for Mask not set',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def addHiveMaskingPolicy(
            self,
            policyName,
            dbName,
            tableName,
            columnName,
            accessType,
            isAccessTypeToBeSet,
            grpName=None,
            userName=None,
            customMaskValue=None
    ):
        self.hivePolicyPg.enterValuesInHivePolicyPg(
            self.basePageObj.driver, policyName, dbName, tableName, columnName, accessType, isAccessTypeToBeSet,
            grpName, userName
        )
        self.setCustomMaskingOption(self.basePageObj.driver, customMaskValue)
        self.commonPolicyPg.clickOnInFormAddPolicyButton(self.basePageObj.driver)
        if self.isErrorPopUpPresent():
            self.clickOnOkBtnInPopupForMaskValueNotEntered(self.basePageObj.driver)
            self.commonPolicyPg.clickOnInFormAddPolicyButton(self.basePageObj.driver)
        #Wait for 35 seconds before issuing Beeline command so that policy cache is refreshed...
        time.sleep(35)
