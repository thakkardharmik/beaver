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
from beaver.component.yarnUIV2.basePage import BasePage
import time

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class CommonPage(BasePage):
    def __init__(self, basePageObj, sso=True):
        BasePage.__init__(self, basePageObj.driver)
        if sso:
            self.loginToknoxSSO()

    locator_dictionary = {
        "ClusterOverview": (By.XPATH, "//a[contains(@href,'#/cluster-overview')]"),
        "Queues": (By.XPATH, "//a[contains(@href,'#/yarn-queues/root')]"),
        "Applications": (By.XPATH, "//a[contains(@href,'#/yarn-apps/apps')]"),
        "Services": (By.XPATH, "//a[contains(@href,'#/yarn-services')]"),
        "FlowActivity": (By.XPATH, "//a[contains(@href,'#/yarn-flow-activity')]"),
        "Nodes": (By.XPATH, "//a[contains(@href,'#/yarn-nodes/table')]"),
        "Tools": (By.XPATH, "//a[contains(@href,'#/yarn-tools/yarn-conf')]"),
        "Refresh": (By.XPATH, "//button[@class='btn btn-sm btn-primary refresh']"),
        "Home": (By.XPATH, "//a[contains(@href,'#/')]"),
        "HomePageTitle": (By.XPATH, "//*[contains(@title, 'Cluster Overview')]")
    }

    def isHomePage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getElement('HomePageTitle', True),
            locatorMessage='RM Home Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getClusterOverview(self, returnLocatorName=False):
        return self.getElement('ClusterOverview', returnLocatorName)

    def getQueues(self, returnLocatorName=False):
        return self.getElement('Queues', returnLocatorName)

    def getApplication(self, returnLocatorName=False):
        return self.getElement('Applications', returnLocatorName)

    def getServices(self, returnLocatorName=False):
        return self.getElement('Services', returnLocatorName)

    def getFlowActivity(self, returnLocatorName=False):
        return self.getElement('FlowActivity', returnLocatorName)

    def getNodes(self, returnLocatorName=False):
        return self.getElement('Nodes', returnLocatorName)

    def getTools(self, returnLocatorName=False):
        return self.getElement('Tools', returnLocatorName)

    def getRefresh(self, returnLocatorName=False):
        return self.getElement('Refresh', returnLocatorName)

    def getHome(self, returnLocatorName=False):
        return self.getElement('Home', returnLocatorName)

    def clickClusterOverview(self):
        SeleniumDP.click(self.driver, self.getClusterOverview())

    def clickQueues(self):
        SeleniumDP.click(self.driver, self.getQueues())

    def clickApplications(self):
        SeleniumDP.click(self.driver, self.getApplication())

    def clickServices(self):
        SeleniumDP.click(self.driver, self.getServices())

    def clickFlowActivity(self):
        SeleniumDP.click(self.driver, self.getFlowActivity())

    def clickNodes(self):
        SeleniumDP.click(self.driver, self.getNodes())

    def clickTools(self):
        SeleniumDP.click(self.driver, self.getTools())

    def clickRefresh(self):
        SeleniumDP.click(self.driver, self.getRefresh())

    def clickHome(self):
        SeleniumDP.click(self.driver, self.getHome())
