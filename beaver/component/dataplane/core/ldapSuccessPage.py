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
from beaver.component.dataplane.core.basePage import BasePage

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class LdapSuccessPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "ldapSuccessAlert": (By.XPATH, "//*[contains(.,'LDAP ADDED SUCCESSFULLY!!!')]"),
        "saveAndLoginButton": (By.ID, "save-login-btn"),
        "usersInputField": (By.XPATH, "//tagging-widget[@id='users-tags']//input"),
        "usersInputFieldTaggingWidget": (By.XPATH, "//tagging-widget[@id='users-tags']/div"),
        "groupsInputField": (By.XPATH, "//tagging-widget[@id='groups-tags']//input"),
        "groupsDropdownRow": (
            By.XPATH, "//tagging-widget[@id='groups-tags']//div[@class='dropDown']/div[@class='row']"
        ),
        "groupsDropdownSearchingRow": (
            By.XPATH,
            "//tagging-widget[@id='groups-tags']//div[@class='dropDown']/div[@class='row']/i[@class='fa fa-spin fa-spinner']"
        ),
        "usersDropdown": (By.XPATH, "//tagging-widget[@id='users-tags']//div[@class='dropDown']"),
        "usersDropdownRow": (By.XPATH, "//tagging-widget[@id='users-tags']//div[@class='dropDown']/div[@class='row']"),
        "usersDropdownSearchingRow": (
            By.XPATH,
            "//tagging-widget[@id='users-tags']//div[@class='dropDown']/div[@class='row']/i[@class='fa fa-spin fa-spinner']"
        ),
    }

    def isLdapSuccessPage(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getUsersInputField(returnLocatorName=True),
            locatorMessage='LDAP Success Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getSaveAndLoginButton(self, returnLocatorName=False):
        return self.getElement('saveAndLoginButton', returnLocatorName)

    def clickSaveAndLoginButton(self):
        SeleniumDP.click(self.driver, self.getSaveAndLoginButton())

    def getLdapSuccessAlert(self, returnLocatorName=False):
        return self.getElement('ldapSuccessAlert', returnLocatorName)

    def isLdapSaveAlertDisplayed(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getLdapSuccessAlert(returnLocatorName=True),
            locatorMessage='LDAP Added Successfully Alert',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getUsersInputField(self, returnLocatorName=False):
        return self.getElement('usersInputField', returnLocatorName)

    def getGroupsInputField(self, returnLocatorName=False):
        return self.getElement('groupsInputField', returnLocatorName)

    def getUsersDropdown(self, returnLocatorName=False):
        return self.getElement('usersDropdown', returnLocatorName)

    def getUsersDropdownRow(self, returnLocatorName=False):
        return self.getElement('usersDropdownRow', returnLocatorName)

    def getUsersDropdownSearchingRow(self, returnLocatorName=False):
        return self.getElement('usersDropdownSearchingRow', returnLocatorName)

    def getGroupsDropdownRow(self, returnLocatorName=False):
        return self.getElement('groupsDropdownRow', returnLocatorName)

    def getGroupsDropdownSearchingRow(self, returnLocatorName=False):
        return self.getElement('groupsDropdownSearchingRow', returnLocatorName)

    def enterUserAndGroup(self, userName, group, attempts=2):
        self.enterUserName(userName, attempts)
        self.enterGroup(group, attempts)

    def enterUserName(self, userName, attempts=2):
        SeleniumDP.sendKeys(self.getUsersInputField(), userName)
        if self.checkElementonPage(locatorName=self.getUsersDropdownSearchingRow(returnLocatorName=True),
                                   locatorMessage='User DropDown Searching Row Visible', retryCount=1, timeout=1):
            self.waitForElementInvisibility(self.getUsersDropdownSearchingRow(returnLocatorName=True))
        if self.checkElementonPage(locatorName=self.getUsersDropdownRow(returnLocatorName=True),
                                   locatorMessage='User DropDown Visible', retryCount=1, timeout=1):
            SeleniumDP.click(self.driver, self.getUsersDropdownRow())
        else:
            self.enterUserName(userName, attempts=attempts - 1)

    def enterGroup(self, group, attempts=2):
        SeleniumDP.sendKeys(self.getGroupsInputField(), group)
        if self.checkElementonPage(locatorName=self.getGroupsDropdownSearchingRow(returnLocatorName=True),
                                   locatorMessage='Groups DropDown Searching Row Visible', retryCount=1, timeout=1):
            self.waitForElementInvisibility(self.getGroupsDropdownSearchingRow(returnLocatorName=True))
        if self.checkElementonPage(locatorName=self.getGroupsDropdownRow(returnLocatorName=True),
                                   locatorMessage='Groups DropDown Visible', retryCount=1, timeout=1):
            SeleniumDP.click(self.driver, self.getGroupsDropdownRow())
        else:
            self.enterGroup(group, attempts=attempts - 1)
