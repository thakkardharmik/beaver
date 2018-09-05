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


class PairingsPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "pageTitle": (By.XPATH, "//breadcrumb//*[contains(.,'Pairings')]"),
        "addPairingButton": (By.XPATH, "//button[@qe-attr='add-pairing']"),
        "createPairButton": (By.XPATH, "//button[@qe-attr='create-pair']"),
        "firstCluster": (By.XPATH, "//div[@qe-attr='first-cluster']//dlm-create-pairing-card/dlm-cluster-card"),
        "secondCluster": (By.XPATH, "//div[@qe-attr='second-cluster']//dlm-create-pairing-card/dlm-cluster-card"),
        "inProgress": (By.XPATH, "//div[contains(.,'IN PROGRESS')]"),
        "xOnPair": (By.XPATH, "//button[@qe-attr='unpair']"),
        "unPairButton": (By.XPATH, "//button[contains(.,'Unpair')]"),
        "pairing": (By.XPATH, "//div[@qe-attr='pairing']")
    }

    def isPairingsPage(self, retryCount=2, timeout=3, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getPageTitle(returnLocatorName=True),
            locatorMessage='DLM Pairings Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getPageTitle(self, returnLocatorName=False):
        return self.getElement('pageTitle', returnLocatorName)

    def getAddPairingButton(self, returnLocatorName=False):
        return self.getElement('addPairingButton', returnLocatorName)

    def clickAddPairingButton(self):
        SeleniumDP.click(self.driver, self.getAddPairingButton())

    def getFirstCluster(self, returnLocatorName=False):
        return self.getElement('firstCluster', returnLocatorName)

    def clickFirstCluster(self):
        self.checkElementonPage(
            locatorName=self.getFirstCluster(returnLocatorName=True), locatorMessage='First Cluster Visible'
        )
        SeleniumDP.click(self.driver, self.getFirstCluster())

    def getSecondCluster(self, returnLocatorName=False):
        return self.getElement('secondCluster', returnLocatorName)

    def clickSecondCluster(self):
        self.checkElementonPage(
            locatorName=self.getSecondCluster(returnLocatorName=True), locatorMessage='Second Cluster Visible'
        )
        SeleniumDP.click(self.driver, self.getSecondCluster())

    def getCreatePairButton(self, returnLocatorName=False):
        return self.getElement('createPairButton', returnLocatorName)

    def clickCreatePairButton(self):
        self.checkElementonPage(
            locatorName=self.getCreatePairButton(returnLocatorName=True), locatorMessage='Create Pair Button Visible'
        )
        SeleniumDP.click(self.driver, self.getCreatePairButton())

    def getXOnPair(self, returnLocatorName=False):
        return self.getElement('xOnPair', returnLocatorName)

    def clickXOnPair(self):
        self.checkElementonPage(
            locatorName=self.getXOnPair(returnLocatorName=True), locatorMessage='Unpair Button Visible'
        )
        SeleniumDP.click(self.driver, self.getXOnPair())

    def getUnPairButton(self, returnLocatorName=False):
        return self.getElement('unPairButton', returnLocatorName)

    def clickUnPairButton(self):
        self.checkElementonPage(
            locatorName=self.getUnPairButton(returnLocatorName=True), locatorMessage='Unpair Button Visible'
        )
        SeleniumDP.click(self.driver, self.getUnPairButton())

    def getCurrentPair(self, returnLocatorName=False):
        return self.getElement('pairing', returnLocatorName)

    def pairingExists(self, retryCount=2, timeout=3):
        if self.checkElementonPage(locatorName=self.getCurrentPair(returnLocatorName=True),
                                   locatorMessage='DLM Available Pairing', retryCount=retryCount, timeout=timeout):
            return True
        else:
            return False

    def doPairing(self):
        self.clickAddPairingButton()
        self.clickFirstCluster()
        self.clickSecondCluster()
        self.clickCreatePairButton()

    def doUnPairing(self):
        self.clickXOnPair()
        self.clickUnPairButton()
