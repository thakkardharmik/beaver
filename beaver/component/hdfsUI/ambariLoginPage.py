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
from beaver.component.hdfsUI.basePage import BasePage
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class AmbariLoginPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, driver=basePageObj.driver, base_url=basePageObj.base_url)

    locator_dictionary = {
        "username": (By.XPATH, "//*[@data-qa='username-input']"),
        "password": (By.XPATH, "//*[@data-qa='password-input']"),
        "loginButton": (By.XPATH, "//*[@data-qa='login-button']"),
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

    @TaskReporter.report_test()
    def doLogin(self, user_name, password):

        SeleniumDP.sendKeys(self.getUserNameTextBox(), user_name)
        SeleniumDP.sendKeys(self.getPasswordTextBox(), password)

        SeleniumDP.click(self.driver, self.getLoginButton())
