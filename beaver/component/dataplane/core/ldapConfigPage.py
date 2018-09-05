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


class LdapConfigPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "ldapConfigLabel": (By.XPATH, "//div[contains(.,'LDAP Configuration')]"),
        "saveButton": (By.ID, "save-btn"),
        "ldapURL": (By.NAME, "ldap-url"),
        "userSearchBase": (By.NAME, "user-search-base"),
        "userSearchAttribute": (By.NAME, "user-search-attr"),
        "groupSearchBase": (By.NAME, "group-search-base"),
        "groupSearchAttribute": (By.NAME, "group-search-attr"),
        "groupObjectClass": (By.NAME, "group-object-class"),
        "groupMemberAttrName": (By.NAME, "group-member-attr-name"),
        "adminBindDn": (By.NAME, "admin-bind-dn"),
        "adminPassword": (By.NAME, "admin-password"),
        "unableToSaveLdapAlert": (By.XPATH, "//div[contains(.,'Unable to save LDAP configuration')]")
    }

    def isLdapConfigPage(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getLdapConfigLabel(returnLocatorName=True),
            locatorMessage='LDAP Config Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getLdapConfigLabel(self, returnLocatorName=False):
        return self.getElement('ldapConfigLabel', returnLocatorName)

    def getSaveButton(self, returnLocatorName=False):
        return self.getElement('saveButton', returnLocatorName)

    def getLdapURL(self, returnLocatorName=False):
        return self.getElement('ldapURL', returnLocatorName)

    def getUserSearchBase(self, returnLocatorName=False):
        return self.getElement('userSearchBase', returnLocatorName)

    def getUserSearchAttribute(self, returnLocatorName=False):
        return self.getElement('userSearchAttribute', returnLocatorName)

    def getGroupSearchBase(self, returnLocatorName=False):
        return self.getElement('groupSearchBase', returnLocatorName)

    def getGroupSearchAttribute(self, returnLocatorName=False):
        return self.getElement('groupSearchAttribute', returnLocatorName)

    def getGroupObjectClass(self, returnLocatorName=False):
        return self.getElement('groupObjectClass', returnLocatorName)

    def getGroupMemberAttrName(self, returnLocatorName=False):
        return self.getElement('groupMemberAttrName', returnLocatorName)

    def getAdminBindDn(self, returnLocatorName=False):
        return self.getElement('adminBindDn', returnLocatorName)

    def getAdminPassword(self, returnLocatorName=False):
        return self.getElement('adminPassword', returnLocatorName)

    def clickSaveButton(self):
        SeleniumDP.click(self.driver, self.getSaveButton())

    def saveLDAPConfig(
            self, ldapURL, userSearchBase, userSearchAttribute, groupSearchBase, groupSearchAttribute,
            groupObjectClass, groupMemberAttrName, adminBindDn, adminPassword
    ):

        SeleniumDP.sendKeys(self.getLdapURL(), ldapURL)
        SeleniumDP.sendKeys(self.getUserSearchBase(), userSearchBase)
        SeleniumDP.sendKeys(self.getUserSearchAttribute(), userSearchAttribute)
        SeleniumDP.sendKeys(self.getGroupSearchBase(), groupSearchBase)
        SeleniumDP.sendKeys(self.getGroupSearchAttribute(), groupSearchAttribute)
        SeleniumDP.sendKeys(self.getGroupObjectClass(), groupObjectClass)
        SeleniumDP.sendKeys(self.getGroupMemberAttrName(), groupMemberAttrName)
        SeleniumDP.sendKeys(self.getAdminBindDn(), adminBindDn)
        SeleniumDP.sendKeys(self.getAdminPassword(), adminPassword)
        self.clickSaveButton()

    def getUnableToSaveLdapAlert(self, returnLocatorName=False):
        return self.getElement('unableToSaveLdapAlert', returnLocatorName)

    def isUnableToSaveLdapAlertDisplayed(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getUnableToSaveLdapAlert(returnLocatorName=True),
            locatorMessage='Unable to Save LDAP Alert',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )
