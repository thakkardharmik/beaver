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
import time

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class AddClusterPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "ambariURLInputField": (By.NAME, "ambari-url"),
        "goButton": (By.ID, "go-btn"),
        "clearButton": (By.ID, "clear-btn"),
        "clusterDetailsField": (By.CLASS_NAME, "cluster-details"),
        "clusterLocationField": (By.NAME, "location"),
        "clusterLocationAutoComplete": (By.XPATH, "//*[@class='ngui-auto-complete']//li"),
        "dataCenterField": (By.NAME, "datacenter"),
        #TODO: Change below XPATH
        "tagsField": (By.XPATH, "//div[@class='taggingWidget']//input"),
        "descriptionField": (By.NAME, "desc"),
        "addButton": (By.ID, "add-btn"),
        "addAndNewButton": (By.ID, "add-and-new-btn"),
        #"errorMessageClusterInfoNotFound": (By.XPATH, "//div[@id='error-message' and contains(.,'Dataplane could not retreive cluster information')]"),
        "errorMessage": (By.ID, "error-message"),
        "errorMessageMandatoryFields": (
            By.XPATH, "//div[@id='error-message' and contains(.,'Please fill in mandatory fields marked with ')]"
        ),
        "invalidAmbariURL": (
            By.XPATH, "//div[@class='error-section validation-error']//ul/li[contains(.,'Invalid Ambari URL')]"
        ),
        "clusterAlreadyExists": (
            By.XPATH,
            "//div[@class='error-section validation-error']//ul/li[contains(.,'Cluster already exists in DataPlane')]"
        ),
        "cancelButton": (By.ID, "cancel-btn")
    }

    def isAddClusterPage(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getAmbariURLInputField(returnLocatorName=True),
            locatorMessage='Add Cluster Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def isMandatoryFieldsErrorMessageDisplayed(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getMandatoryFieldsErrorMessage(returnLocatorName=True),
            locatorMessage='Please fill Mandatory Fields Error Message Displayed',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getAmbariURLInputField(self, returnLocatorName=False):
        return self.getElement('ambariURLInputField', returnLocatorName)

    def getGoButton(self, returnLocatorName=False):
        return self.getElement('goButton', returnLocatorName)

    def getClearButton(self, returnLocatorName=False):
        return self.getElement('clearButton', returnLocatorName)

    def isClearButtonDisplayed(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getClearButton(returnLocatorName=True),
            locatorMessage='Clear Button Displayed',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def clickGoButton(self):
        SeleniumDP.click(self.driver, self.getGoButton())

    def getClusterLocationField(self, returnLocatorName=False):
        return self.getElement('clusterLocationField', returnLocatorName)

    def getClusterLocationAutoComplete(self, returnLocatorName=False):
        return self.getElement('clusterLocationAutoComplete', returnLocatorName)

    def getDataCenterField(self, returnLocatorName=False):
        return self.getElement('dataCenterField', returnLocatorName)

    def getTagsField(self, returnLocatorName=False):
        return self.getElement('tagsField', returnLocatorName)

    def getDescriptionField(self, returnLocatorName=False):
        return self.getElement('descriptionField', returnLocatorName)

    def getAddButton(self, returnLocatorName=False):
        return self.getElement('addButton', returnLocatorName)

    def clickAddButton(self):
        SeleniumDP.click(self.driver, self.getAddButton())

    def getAddAndNewButton(self, returnLocatorName=False):
        return self.getElement('addAndNewButton', returnLocatorName)

    def getInvalidAmbariURL(self, returnLocatorName=False):
        return self.getElement('invalidAmbariURL', returnLocatorName)

    def getClusterAlreadyExists(self, returnLocatorName=False):
        return self.getElement('clusterAlreadyExists', returnLocatorName)

    def getErrorMessage(self, returnLocatorName=False):
        return self.getElement('errorMessage', returnLocatorName)

    def getMandatoryFieldsErrorMessage(self, returnLocatorName=False):
        return self.getElement('errorMessageMandatoryFields', returnLocatorName)

    def clickAddAndNewButton(self):
        SeleniumDP.click(self.driver, self.getAddAndNewButton())

    def enterAmbariURLAndValidate(
            self,
            ambariURL,
            invalidURL=False,
            incorrectAmbariHost=False,
            clusterAlreadyExists=False,
            mandatoryFieldsTest=False
    ):
        SeleniumDP.sendKeys(self.getAmbariURLInputField(), ambariURL)
        self.clickGoButton()
        if invalidURL:
            # Waiting for 0 seconds for Invalid Ambari URL message to load
            assert self.checkElementonPage(
                locatorName=self.getInvalidAmbariURL(returnLocatorName=True),
                locatorMessage='Invalid Ambari URL Message Displayed',
                retryCount=0
            )
        elif clusterAlreadyExists:
            # Waiting for 0 seconds for Cluster Already Exists message to load
            assert self.checkElementonPage(
                locatorName=self.getClusterAlreadyExists(returnLocatorName=True),
                locatorMessage='Cluster Already Exists Message Displayed',
                retryCount=0
            )
        elif incorrectAmbariHost:
            # Waiting for max 30 seconds for Error Message to load
            assert self.checkElementonPage(
                locatorName=self.getErrorMessage(returnLocatorName=True),
                locatorMessage='Error Message - Dataplane could not retreive cluster information Displayed',
                retryCount=3,
                timeout=10
            )
        else:
            # Waiting for max 60 seconds for Cluster details to load
            assert self.checkElementonPage(
                locatorName=self.getClusterLocationField(returnLocatorName=True),
                locatorMessage='Add Cluster Details Parameters Displayed',
                retryCount=6,
                timeout=10
            )
            if mandatoryFieldsTest:
                SeleniumDP.sendKeys(self.getClusterLocationField(), "San Jose, California, United States of America")
                time.sleep(2)
                SeleniumDP.clickUsingClickable(self.getClusterLocationAutoComplete())

    def addCluster(self, ambariURL, clusterLocation, dataCenter, tags, description, clickAddAndNewButton=False):

        self.enterAmbariURLAndValidate(ambariURL)

        SeleniumDP.sendKeys(self.getClusterLocationField(), clusterLocation)
        time.sleep(2)
        SeleniumDP.clickUsingClickable(self.getClusterLocationAutoComplete())
        SeleniumDP.sendKeys(self.getDataCenterField(), dataCenter)
        SeleniumDP.sendKeys(self.getTagsField(), tags)
        SeleniumDP.sendKeys(self.getDescriptionField(), description)
        if clickAddAndNewButton:
            self.clickAddAndNewButton()
            if self.isClearButtonDisplayed():
                self.waitForElement(self.getGoButton(returnLocatorName=True))
        else:
            self.clickAddButton()
