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


class LoginPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "username": (By.NAME, "username"),
        "password": (By.NAME, "password"),
        "loginButton": (By.XPATH, "//button[contains(.,'Login')]"),
        "errorMessage": (By.XPATH, "//div[contains(.,'Credentials were incorrect')]")
    }

    def isLoginPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getLoginButton(returnLocatorName=True),
            locatorMessage='Login Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getUserNameTextBox(self, returnLocatorName=False):
        return self.getElement('username', returnLocatorName)

    def getPasswordTextBox(self, returnLocatorName=False):
        return self.getElement('password', returnLocatorName)

    def getLoginButton(self, returnLocatorName=False):
        return self.getElement('loginButton', returnLocatorName)

    def getErrorMessage(self, returnLocatorName=False):
        return self.getElement('errorMessage', returnLocatorName)

    def doLogin(self, user_name, password):

        SeleniumDP.sendKeys(self.getUserNameTextBox(), user_name)
        SeleniumDP.sendKeys(self.getPasswordTextBox(), password)

        SeleniumDP.click(self.driver, self.getLoginButton())

    def isIncorrectCredMessageDisplayed(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getErrorMessage(returnLocatorName=True),
            locatorMessage='Incorrect Cred Error Message',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )
