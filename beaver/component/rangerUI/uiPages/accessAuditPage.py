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
from selenium.webdriver.common.keys import Keys
import time
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class AccessAuditPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "accessTypeHeadingInAccessAuditTable": (
            By.XPATH,
            "//table[@class='table table-bordered table-condensed backgrid']/thead/tr[contains(.,'Policy ID')]/th[contains(.,'Access Type')]"
        ),
        "clickableAreaInSearchSection": (
            By.XPATH, "(//div[@class='search_input ui-menu not_editing not_selected']/input)[2]"
        ),
        "accessEnforcerSearchCriteriaInSearchSection": (
            By.XPATH, "//ul[contains(@class,'VS-interface')]/li[contains(.,'Access Enforcer')]/a"
        ),
        "serviceTypeSearchCriteriaInDropDown": (
            By.XPATH,
            "//ul[contains(@style,'display: block')]/li[contains(.,'Service Type')]/a[contains(.,'Service Type')]"
        ),
        "serviceTypeLabelInSearchSection": (
            By.XPATH,
            "//div[@class='VS-search-inner']//div[contains(@class,'search_facet')]/div[contains(.,'Service Type:')]"
        ),
        "hiveServiceTypeInDropdown": (
            By.XPATH, "//ul[contains(@style,'width:')]/li[contains(.,'HIVE')]/div[contains(.,'HIVE')]"
        ),
        "hiveLabelInSearchSection": (
            By.XPATH,
            "//div[@class='VS-search-inner']//div[@class='search_facet not_editing not_selected']/div[contains(.,'HIVE')]"
        ),
        "userSearchCriteriaInDropDown": (By.XPATH, "//ul[contains(@class,'VS-interface')]/li[contains(.,'User')]/div"),
        "userLabelInSearchSection": (
            By.XPATH, "//div[@class='VS-search-inner']//div[contains(@class,'search_facet')]/div[contains(.,'User:')]"
        ),
        "inputAreaForUserOptionInSearchSection": (
            By.XPATH,
            "//div[contains(@class,'search_facet')]/div[contains(.,'User:')]/following-sibling::div[@class='search_facet_input_container']/input"
        ),
        "dropdownForService": (By.XPATH, "//ul[contains(@style,'display: block')]"),
        "newServiceType": (By.XPATH, "//ul[contains(@style,'display: block')]/li[contains(.,'Service Type')]"),
        "serviceNameSearchCriteriaInDropDown": (
            By.XPATH, "//ul[contains(@style,'width:')]/li[contains(.,'Service Name')]/div[contains(.,'Service Name')]"
        ),
        "hiveQueryPopup": (By.XPATH, "//div[@class='popover fade top in']/div[@class='popover-content']"),
        "hiveQueryInPopup": (
            By.XPATH, "//div[@class='popover fade top in']/div[@class='popover-content']/div[@class='query-content']"
        ),
    }

    def getAccessTypeHeadingInAccessAuditTable(self, returnLocatorName=False):
        return self.getElement('accessTypeHeadingInAccessAuditTable', returnLocatorName)

    def getClickableAreaInSearchSection(self, returnLocatorName=False):
        return self.getElement('clickableAreaInSearchSection', returnLocatorName)

    def clickOnClickableAreaInSearchSection(self, driver):
        SeleniumDP.click(driver, self.getClickableAreaInSearchSection())

    def getAccessEnforcerSearchCriteriaInSearchSection(self, returnLocatorName=False):
        return self.getElement('accessEnforcerSearchCriteriaInSearchSection', returnLocatorName)

    def getServiceTypeSearchCriteriaInDropdown(self, returnLocatorName=False):
        return self.getElement('serviceTypeSearchCriteriaInDropDown', returnLocatorName)

    def getServiceNameSearchCriteriaInDropdown(self, returnLocatorName=False):
        return self.getElement('serviceNameSearchCriteriaInDropDown', returnLocatorName)

    #def clickOnServiceTypeSearchCriteriaInDropDown(self, driver): #-->Clicks on Service Name!!
    #    SeleniumDP.click(driver, self.getServiceTypeSearchCriteriaInDropdown())

    def clickOnServiceTypeSearchCriteriaInDropDown(self, driver):
        driver.find_element_by_xpath(
            "//ul[contains(@style,'width:')]/li[contains(.,'Service Type')]/div[contains(.,'Service Type')]"
        ).click()

    def getServiceTypeLabelInSearchSection(self, returnLocatorName=False):
        return self.getElement('serviceTypeLabelInSearchSection', returnLocatorName)

    def isServiceTypeLabelAddedToSearchSectionBar(
            self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            self,
            locatorName='serviceTypeLabelInSearchSection',
            locatorMessage='Service Type Label In Search Section bar',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getHiveServiceTypeInDropdown(self, returnLocatorName=False):
        return self.getElement('hiveServiceTypeInDropdown', returnLocatorName)

    #def clickOnHiveServiceTypeInDropdown(self, driver): #-->Nothing happens, doesn't click
    #    SeleniumDP.click(driver, self.getHiveServiceTypeInDropdown())

    def clickOnHiveServiceTypeInDropdown(self, driver):
        driver.find_element_by_xpath("//ul[contains(@style,'width:')]/li[contains(.,'HIVE')]/div[contains(.,'HIVE')]"
                                     ).click()

    def getHiveLabelInSearchSection(self, returnLocatorName=False):
        return self.getElement('hiveLabelInSearchSection', returnLocatorName)

    def isHIVELabelAddedToSearchSectionBar(
            self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            self,
            locatorName='hiveLabelInSearchSection',
            locatorMessage='HIVE option for Service Type In Search Section bar',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getUserSearchCriteriaInDropDown(self, returnLocatorName=False):
        return self.getElement('userSearchCriteriaInDropDown', returnLocatorName)

    #def clickOnUserSearchCriteriaInDropDown(self, driver):
    #    SeleniumDP.click(driver, self.getUserSearchCriteriaInDropDown())

    def clickOnUserSearchCriteriaInDropDown(self, driver):
        driver.find_element_by_xpath("//ul[contains(@class,'VS-interface')]/li[contains(.,'User')]/div").click()

    def getUserLabelInSearchSection(self, returnLocatorName=False):
        return self.getElement('userLabelInSearchSection', returnLocatorName)

    def isUserLabelAddedToSearchSectionBar(
            self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        return self.checkElementonPage(
            self,
            locatorName='userLabelInSearchSection',
            locatorMessage='User Label In Search Section bar',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getInputAreaForUserOptionInSearchSection(self, returnLocatorName=False):
        return self.getElement('inputAreaForUserOptionInSearchSection', returnLocatorName)

    def addXpathForUserNameLabelToLocDict(self, userName):
        self.locator_dictionary["enteredUsernameLabelInSearchSection"] = (
            By.XPATH,
            "//div[@class='VS-search-inner']//div[@class='search_facet not_editing not_selected']/div[contains(.,'" +
            userName + "')]"
        )

    def getUsernameLabelInSearchSection(self, userName, returnLocatorName=False):
        self.addXpathForUserNameLabelToLocDict(userName)
        return self.getElement('enteredUsernameLabelInSearchSection', returnLocatorName)

    def addXpathForPolicyIdInAuditTableToLocDict(self, policyId):
        self.locator_dictionary["policyIdInAuditTable"] = (
            By.XPATH, "//table[@class='table table-bordered table-condensed backgrid']/tbody//a[contains(@title,'" +
            policyId + "')]"
        )

    def getPolicyIdInAuditTable(self, policyId, returnLocatorName=False):
        self.addXpathForPolicyIdInAuditTableToLocDict(policyId)
        return self.getElement('policyIdInAuditTable', returnLocatorName)

    def addXpathForQueryInfoBtnToLocDict(self, policyId):
        self.locator_dictionary["queryInfoBtnInAuditTable"] = (
            By.XPATH, "(//tbody//a[contains(@title,'" + policyId +
            "')]/../following-sibling::td[@class='html-cell renderable'])[2]/div[@class='clearfix']//div[@data-name='queryInfo']/i"
        )

    def getQueryInfoBtnInAuditTable(self, policyId, returnLocatorName=False):
        self.addXpathForQueryInfoBtnToLocDict(policyId)
        return self.getElement('queryInfoBtnInAuditTable', returnLocatorName)

    def clickOnQueryInfoBtnInAuditTable(self, driver, policyId):
        SeleniumDP.click(driver, self.getQueryInfoBtnInAuditTable(policyId))

    def getHiveQueryPopup(self, returnLocatorName=False):
        return self.getElement('hiveQueryPopup', returnLocatorName)

    def getHiveQueryInPopup(self, returnLocatorName=False):
        return self.getElement('hiveQueryInPopup', returnLocatorName)

    def checkIsAccessAuditPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName='accessTypeHeadingInAccessAuditTable',
            locatorMessage='Access Audit Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def searchForAccessAuditBasedOnPolicyId(
            self,
            driver,
            policyId,
            accessEnforcer=None,
            accessType=None,
            clusterName=None,
            resourceName=None,
            resourceType=None,
            result=None,
            serviceName=None,
            serviceType=None,
            tags=None,
            user=None
    ):
        if serviceType is not None:
            self.clickOnClickableAreaInSearchSection(driver)  #--> Nothing happens
            SeleniumDP.sendKeys(self.getClickableAreaInSearchSection(),
                                'Service')  #--> if no waitForElement, Service name gets selected
            #SeleniumDP.sendKeys(self.getClickableAreaInSearchSection(), Keys.RETURN) #--> clicks on access enforcer
            self.waitForElement(self, self.getServiceNameSearchCriteriaInDropdown(returnLocatorName=True))
            self.clickOnServiceTypeSearchCriteriaInDropDown(driver)
            #SeleniumDP.sendKeys(self.getServiceTypeSearchCriteriaInDropdown(), Keys.RETURN) #--> throws InvalidElementStateException
            #SeleniumDP.sendKeys(self.getServiceTypeSearchCriteriaInDropdown(), Keys.NULL) #--> throws InvalidElementStateException
            #SeleniumDP.sendKeys(driver.switch_to.active_element, Keys.ARROW_DOWN) #--> clicks on access enforcer
            self.waitForElement(self, self.getServiceTypeLabelInSearchSection(returnLocatorName=True))
            self.clickOnHiveServiceTypeInDropdown(driver)
            self.waitForElement(self, self.getHiveLabelInSearchSection(returnLocatorName=True))
            self.clickOnClickableAreaInSearchSection(driver)
            SeleniumDP.sendKeys(self.getClickableAreaInSearchSection(), 'user')
            self.waitForElement(self, self.getUserSearchCriteriaInDropDown(returnLocatorName=True))
            self.clickOnUserSearchCriteriaInDropDown(driver)
            self.waitForElement(self, self.getUserLabelInSearchSection(returnLocatorName=True))
            SeleniumDP.sendKeys(self.getInputAreaForUserOptionInSearchSection(), user + Keys.RETURN)
            self.waitForElement(self, self.getUsernameLabelInSearchSection(user, returnLocatorName=True))
            time.sleep(2)

    @TaskReporter.report_test()
    def getHiveQueryTextFromQueryInfoPopup(self, driver, policyId):
        self.clickOnQueryInfoBtnInAuditTable(driver, policyId)
        self.waitForElement(self, self.getHiveQueryPopup(returnLocatorName=True))
        hiveQuery = self.getHiveQueryInPopup().text
        logger.info("hive query in popup: " + hiveQuery)
        return hiveQuery
