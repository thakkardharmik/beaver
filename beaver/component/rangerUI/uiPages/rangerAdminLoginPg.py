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


class LoginPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "username": (By.ID, "username"),
        "password": (By.ID, "password"),
        "signInButton": (By.XPATH, "//button[@id='signIn']"),
        "errorMessage": (By.ID, "errorBox")
    }

    def isLoginPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName=self.getSignInButton(returnLocatorName=True),
            locatorMessage='Ranger Login Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getUserNameTextBox(self, returnLocatorName=False):
        return self.getElement('username', returnLocatorName)

    def getPasswordTextBox(self, returnLocatorName=False):
        return self.getElement('password', returnLocatorName)

    def getSignInButton(self, returnLocatorName=False):
        return self.getElement('signInButton', returnLocatorName)

    def getErrorMessage(self, returnLocatorName=False):
        return self.getElement('errorMessage', returnLocatorName)

    def doLogin(self, user_name, password):
        SeleniumDP.sendKeys(self.getUserNameTextBox(), user_name)
        SeleniumDP.sendKeys(self.getPasswordTextBox(), password)
        SeleniumDP.click(self.driver, self.getSignInButton())

    def isIncorrectCredMessageDisplayed(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getErrorMessage(returnLocatorName=True),
            locatorMessage='Incorrect Cred Error Message',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )
