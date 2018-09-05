#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#

import logging, time
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.dataplane.core.basePage import BasePage
from beaver.component.dataplane.core.commonPageUtils import CommonPageUtils

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class PoliciesPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "pageTitle": (By.XPATH, "//breadcrumb//*[contains(.,'Policies')]"),
        "addPolicyButton": (By.XPATH, "//button[@qe-attr='add-policy']"),
        "policyName": (By.XPATH, "//input[@qe-attr='policy-name']"),
        "policyDescription": (By.XPATH, "//textarea[@qe-attr='policy-description']"),
        "policyTypeHdfs": (By.XPATH, "//dlm-radio-button[@qe-attr='policy-type']/div/div[1]/label"),
        "policyTypeHive": (By.XPATH, "//dlm-radio-button[@qe-attr='policy-type']/div/div[2]/label"),
        "sourceClusterDropdown": (
            By.XPATH, "//dlm-select-field[@qe-attr='policy-source-cluster']//div[@class='select-field-container']/div"
        ),
        "sourceClusterDropdownItem1": (By.XPATH, "//dlm-select-field[@qe-attr='policy-source-cluster']//li[1]"),
        "sourceClusterDropdownItem2": (By.XPATH, "//dlm-select-field[@qe-attr='policy-source-cluster']//li[2]"),
        "destinationClusterDropdown": (
            By.XPATH,
            "//dlm-select-field[@qe-attr='policy-destination-cluster']//div[@class='select-field-container']/div"
        ),
        "destinationClusterDropdownItem1": (
            By.XPATH,
            "//dlm-select-field[@qe-attr='policy-destination-cluster']//div[@class='select-field-container']//li"
        ),
        "folderPathInput": (By.XPATH, "//input[@formcontrolname='directories']"),
        "folderPathInputErrorMessage": (By.XPATH, "//div/dlm-field-error/div[@class='alert alert-danger']"),
        "folderPathInputForm": (
            By.XPATH, "//div[@class='form-section-title actionable' and contains(.,'Select a Folder')]"
        ),
        "dBEntryRadio": (By.XPATH, "//dlm-hive-browser/dlm-hive-database[1]//label[@class='radio']"),
        "dBSearch": (By.XPATH, "//dlm-search-input[@qe-attr='policy-database-search-filter']//input"),
        "policyFrequency": (By.XPATH, "//input[@qe-attr='policy-frequency']"),
        "policyUnitDropdown": (
            By.XPATH, "//dlm-select-field[@qe-attr='policy-unit']//div[@class='select-field-container']/div"
        ),
        "policyUnitDropdownWeek": (By.XPATH, "//dlm-select-field[@qe-attr='policy-unit']//li[1]"),
        "policyUnitDropdownDay": (By.XPATH, "//dlm-select-field[@qe-attr='policy-unit']//li[2]"),
        "policyUnitDropdownHour": (By.XPATH, "//dlm-select-field[@qe-attr='policy-unit']//li[3]"),
        "policyUnitDropdownMinute": (By.XPATH, "//dlm-select-field[@qe-attr='policy-unit']//li[4]"),
        "policyStartFromNow": (
            By.XPATH, "//dlm-radio-button[@qe-attr='policy-start-schedule']//label[contains(.,'From Now')]"
        ),
        "policyStartDate": (By.XPATH, "//my-date-picker[@qe-attr='policy-start-date']//input"),
        "policyEndDate": (By.XPATH, "//my-date-picker[@qe-attr='policy-end-time']//input"),
        "policyStartTime": (By.XPATH, "//timepicker[@qe-attr='policy-start-time']//td[1]/input"),
        "policyEndTime": (By.XPATH, "//timepicker[@qe-attr='policy-start-time']//td[3]/input"),
        "policySubmitButton": (By.XPATH, "//button[@qe-attr='policy-submit']"),
        "policyCancelButton": (By.XPATH, "//button[@qe-attr='policy-cancel']"),
        "policyCreationFailMessage": (By.XPATH, "//dlm-review-policy//span[contains(.,'Create policy failed.')]"),
        "policyCreationErrorDetails": (By.XPATH, "//dlm-review-policy//span[contains(.,'Check error details')]"),
        "policyCreationFailDialog": (
            By.XPATH, "//dlm-modal-dialog-body[contains(.,'Target Hive server already has dataset')]"
        ),
        "dlmModalDialogOKButton": (By.XPATH, "//dlm-review-policy//dlm-modal-dialog//button[contains(.,'OK')]"),
        "filterInput": (By.XPATH, "//input[@qe-attr='table-filter-input']"),
        "policyEntryActionsDropdown": (By.XPATH, "//dlm-policy-actions[@qe-attr='policy-actions-0']//a"),
        "policySuspendAction": (
            By.XPATH, "//dlm-policy-actions[@qe-attr='policy-actions-0']//a[contains(.,'Suspend')]"
        ),
        "policyActivateAction": (
            By.XPATH, "//dlm-policy-actions[@qe-attr='policy-actions-0']//a[contains(.,'Activate')]"
        ),
        "policyDeleteAction": (By.XPATH, "//dlm-policy-actions[@qe-attr='policy-actions-0']//a[contains(.,'Delete')]"),
        "policyViewLogAction": (
            By.XPATH, "//dlm-policy-actions[@qe-attr='policy-actions-0']//a[contains(.,'View Log')]"
        ),
        "policyViewLogDialogHeader": (
            By.XPATH, "//dlm-log-modal-dialog//div[@class='modal-header'][contains(.,'Log')]"
        ),
        "policyDialogOKButton": (
            By.XPATH, "//dlm-modal-dialog[@qe-attr='confirmation-modal']//button[contains(.,'Ok')]"
        ),
        "policyDialogYesButton": (By.XPATH, "//dlm-modal-dialog//button[@qe-attr='modal-confirm'][contains(.,'Yes')]"),
        "policyLogDialogMessage": (By.XPATH, "//dlm-modal-dialog-body//pre[@class='log-message' and text() != '']"),
        "policyStatusFirstEntry": (By.XPATH, "//dlm-policy-table//dlm-table//span[@qe-attr='policy-status-0']"),
        "policyDoesntExistInFilter": (
            By.XPATH, "//dlm-policies//dlm-table-filter//li[contains(.,'No results found')]"
        ),
        "policyAppliedFilter": (By.XPATH, "//dlm-table-filter//span[@qe-attr='applied-filter']")
    }

    def isPoliciesPage(self, retryCount=2, timeout=3, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getPageTitle(returnLocatorName=True),
            locatorMessage='DLM Policies Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getPageTitle(self, returnLocatorName=False):
        return self.getElement('pageTitle', returnLocatorName)

    def getAddPolicyButton(self, returnLocatorName=False):
        return self.getElement('addPolicyButton', returnLocatorName)

    def clickAddPolicyButton(self):
        self.checkElementonPage(
            locatorName=self.getAddPolicyButton(returnLocatorName=True), locatorMessage='Add Policy Visible'
        )
        SeleniumDP.click(self.driver, self.getAddPolicyButton())

    def getPolicyName(self, returnLocatorName=False):
        return self.getElement('policyName', returnLocatorName)

    def getPolicyDescription(self, returnLocatorName=False):
        return self.getElement('policyDescription', returnLocatorName)

    def getPolicyTypeHdfs(self, returnLocatorName=False):
        return self.getElement('policyTypeHdfs', returnLocatorName)

    def clickPolicyTypeHdfs(self):
        self.checkElementonPage(
            locatorName=self.getPolicyTypeHdfs(returnLocatorName=True), locatorMessage='HDFS Radio Button Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyTypeHdfs())

    def getPolicyTypeHive(self, returnLocatorName=False):
        return self.getElement('policyTypeHive', returnLocatorName)

    def clickPolicyTypeHive(self):
        self.checkElementonPage(
            locatorName=self.getPolicyTypeHive(returnLocatorName=True), locatorMessage='Hive Radio Button Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyTypeHive())

    def getSourceClusterDropdown(self, returnLocatorName=False):
        return self.getElement('sourceClusterDropdown', returnLocatorName)

    def clickSourceClusterDropdown(self):
        self.checkElementonPage(
            locatorName=self.getSourceClusterDropdown(returnLocatorName=True),
            locatorMessage='Source Cluster Dropdown Visible'
        )
        SeleniumDP.click(self.driver, self.getSourceClusterDropdown())

    def getSourceClusterDropdownItem1(self, returnLocatorName=False):
        return self.getElement('sourceClusterDropdownItem1', returnLocatorName)

    def clickSourceClusterDropdownItem1(self):
        self.checkElementonPage(
            locatorName=self.getSourceClusterDropdownItem1(returnLocatorName=True),
            locatorMessage='Source Cluster Dropdown Item #1 Visible'
        )
        SeleniumDP.click(self.driver, self.getSourceClusterDropdownItem1())

    def getSourceClusterDropdownItem2(self, returnLocatorName=False):
        return self.getElement('sourceClusterDropdownItem2', returnLocatorName)

    def clickSourceClusterDropdownItem2(self):
        self.checkElementonPage(
            locatorName=self.getSourceClusterDropdownItem2(returnLocatorName=True),
            locatorMessage='Source Cluster Dropdown Item #2 Visible'
        )
        SeleniumDP.click(self.driver, self.getSourceClusterDropdownItem2())

    def getDestinationClusterDropdown(self, returnLocatorName=False):
        return self.getElement('destinationClusterDropdown', returnLocatorName)

    def clickDestinationClusterDropdown(self):
        self.checkElementonPage(
            locatorName=self.getDestinationClusterDropdown(returnLocatorName=True),
            locatorMessage='Destination Cluster Dropdown Visible'
        )
        SeleniumDP.click(self.driver, self.getDestinationClusterDropdown())

    def getDestinationClusterDropdownItem1(self, returnLocatorName=False):
        return self.getElement('destinationClusterDropdownItem1', returnLocatorName)

    def clickDestinationClusterDropdownItem1(self):
        self.checkElementonPage(
            locatorName=self.getDestinationClusterDropdownItem1(returnLocatorName=True),
            locatorMessage='Destination Cluster Dropdown Item #1 Visible'
        )
        SeleniumDP.click(self.driver, self.getDestinationClusterDropdownItem1())

    def getFolderPathInput(self, returnLocatorName=False):
        return self.getElement('folderPathInput', returnLocatorName)

    def getFolderPathInputErrorMessage(self, returnLocatorName=False):
        return self.getElement('folderPathInputErrorMessage', returnLocatorName)

    def getFolderPathInputForm(self, returnLocatorName=False):
        return self.getElement('folderPathInputForm', returnLocatorName)

    def clickFolderPathInputForm(self):
        SeleniumDP.click(self.driver, self.getFolderPathInputForm())

    def getPolicyFrequency(self, returnLocatorFrequency=False):
        return self.getElement('policyFrequency', returnLocatorFrequency)

    def getPolicyUnitDropdown(self, returnLocatorName=False):
        return self.getElement('policyUnitDropdown', returnLocatorName)

    def clickPolicyUnitDropdown(self):
        self.checkElementonPage(
            locatorName=self.getPolicyUnitDropdown(returnLocatorName=True),
            locatorMessage='Policy Unit Dropdown Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyUnitDropdown())

    def getPolicyUnitDropdownWeek(self, returnLocatorName=False):
        return self.getElement('policyUnitDropdownWeek', returnLocatorName)

    def clickPolicyUnitDropdownWeek(self):
        self.checkElementonPage(
            locatorName=self.getPolicyUnitDropdownWeek(returnLocatorName=True),
            locatorMessage='Policy Unit Dropdown Week Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyUnitDropdownWeek())

    def getPolicyUnitDropdownDay(self, returnLocatorName=False):
        return self.getElement('policyUnitDropdownDay', returnLocatorName)

    def clickPolicyUnitDropdownDay(self):
        self.checkElementonPage(
            locatorName=self.getPolicyUnitDropdownDay(returnLocatorName=True),
            locatorMessage='Policy Unit Dropdown Day Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyUnitDropdownDay())

    def getPolicyUnitDropdownHour(self, returnLocatorName=False):
        return self.getElement('policyUnitDropdownHour', returnLocatorName)

    def clickPolicyUnitDropdownHour(self):
        self.checkElementonPage(
            locatorName=self.getPolicyUnitDropdownHour(returnLocatorName=True),
            locatorMessage='Policy Unit Dropdown Hour Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyUnitDropdownHour())

    def getPolicyUnitDropdownMinute(self, returnLocatorName=False):
        return self.getElement('policyUnitDropdownMinute', returnLocatorName)

    def clickPolicyUnitDropdownMinute(self):
        self.checkElementonPage(
            locatorName=self.getPolicyUnitDropdownMinute(returnLocatorName=True),
            locatorMessage='Policy Unit Dropdown Minute Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyUnitDropdownMinute())

    def getPolicyStartDate(self, returnLocatorName=False):
        return self.getElement('policyStartDate', returnLocatorName)

    def getPolicyEndDate(self, returnLocatorName=False):
        return self.getElement('policyEndDate', returnLocatorName)

    def getPolicyStartTime(self, returnLocatorName=False):
        return self.getElement('policyStartTime', returnLocatorName)

    def getPolicyEndTime(self, returnLocatorName=False):
        return self.getElement('policyEndTime', returnLocatorName)

    def getPolicySubmitButton(self, returnLocatorName=False):
        return self.getElement('policySubmitButton', returnLocatorName)

    def clickPolicySubmitButton(self):
        self.checkElementonPage(
            locatorName=self.getPolicySubmitButton(returnLocatorName=True),
            locatorMessage='Policy Submit Button Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicySubmitButton())

    def getPolicyCancelButton(self, returnLocatorName=False):
        return self.getElement('policyCancelButton', returnLocatorName)

    def clickPolicyCancelButton(self):
        self.checkElementonPage(
            locatorName=self.getPolicyCancelButton(returnLocatorName=True),
            locatorMessage='Policy Submit Cancel Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyCancelButton())

    def getDBSearch(self, returnLocatorName=False):
        return self.getElement('dBSearch', returnLocatorName)

    def getDBEntry(self, returnLocatorName=False):
        return self.getElement('dBEntryRadio', returnLocatorName)

    def clickDBEntry(self):
        self.checkElementonPage(
            locatorName=self.getDBEntry(returnLocatorName=True),
            locatorMessage='Default Database Radio Button Visible'
        )
        SeleniumDP.click(self.driver, self.getDBEntry())

    def getPolicyStartFromNow(self, returnLocatorName=False):
        return self.getElement('policyStartFromNow', returnLocatorName)

    def clickPolicyStartFromNow(self):
        self.checkElementonPage(
            locatorName=self.getPolicyStartFromNow(returnLocatorName=True),
            locatorMessage='Policy Start From Now Radio Button Visible'
        )
        SeleniumDP.click(self.driver, self.getPolicyStartFromNow())

    def getPolicyCreationFailMessage(self, returnLocatorName=False):
        return self.getElement('policyCreationFailMessage', returnLocatorName=returnLocatorName)

    def getPolicyCreationErrorDetails(self, returnLocatorName=False):
        return self.getElement('policyCreationErrorDetails', returnLocatorName=returnLocatorName)

    def getPolicyCreationFailDialog(self, returnLocatorName=False):
        return self.getElement('policyCreationFailDialog', returnLocatorName=returnLocatorName)

    def getdlmModalDialogOKButton(self, returnLocatorName=False):
        return self.getElement('dlmModalDialogOKButton', returnLocatorName=returnLocatorName)

    def validatePolicyCreationFailure(self, retryCount=3, timeout=5):
        assert self.checkElementonPage(
            locatorName=self.getPolicyCreationFailMessage(returnLocatorName=True),
            locatorMessage='Policy Creation Fail Message Visible',
            retryCount=retryCount,
            timeout=timeout
        )
        assert self.checkElementonPage(
            locatorName=self.getPolicyCreationErrorDetails(returnLocatorName=True),
            locatorMessage="Policy Creation Error Details Button Visible",
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyCreationErrorDetails())
        assert self.checkElementonPage(
            locatorName=self.getPolicyCreationFailDialog(returnLocatorName=True),
            locatorMessage="Policy Creation Fail Dialog Visible",
            retryCount=retryCount,
            timeout=timeout
        )
        assert self.checkElementonPage(
            locatorName=self.getdlmModalDialogOKButton(returnLocatorName=True),
            locatorMessage="OK Button Visible",
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getdlmModalDialogOKButton())

    # TODO: Need to use better way for policyFrequencyUnit
    def createHdfsReplicationPolicy(
            self, policyName, folderPathInput, policyDescription='', policyFrequency='2', policyFrequencyUnit='minute'
    ):
        self.clickAddPolicyButton()
        self.checkElementonPage(
            locatorName=self.getPolicyName(returnLocatorName=True),
            locatorMessage='Create Replication Policies Page',
            retryCount=1
        )
        SeleniumDP.sendKeys(self.getPolicyName(), policyName)
        SeleniumDP.sendKeys(self.getPolicyDescription(), policyDescription)
        self.clickPolicyTypeHdfs()
        self.clickSourceClusterDropdown()
        self.clickSourceClusterDropdownItem1()
        self.clickDestinationClusterDropdown()
        self.clickDestinationClusterDropdownItem1()
        SeleniumDP.sendKeys(self.getFolderPathInput(), folderPathInput)
        self.clickPolicyStartFromNow()
        SeleniumDP.sendKeys(self.getPolicyFrequency(), policyFrequency)
        self.clickPolicyUnitDropdown()
        if policyFrequencyUnit == 'minute':
            self.clickPolicyUnitDropdownMinute()
        elif policyFrequencyUnit == 'hour':
            self.clickPolicyUnitDropdownHour()
        elif policyFrequencyUnit == 'day':
            self.clickPolicyUnitDropdownDay()
        else:
            self.clickPolicyUnitDropdownWeek()

        self.clickPolicySubmitButton()
        self.clickPolicySubmitButton()

    def createHiveReplicationPolicy(
            self, policyName, dbname, policyDescription='', policyFrequency='2', policyFrequencyUnit='minute'
    ):
        self.clickAddPolicyButton()
        self.checkElementonPage(
            locatorName=self.getPolicyName(returnLocatorName=True),
            locatorMessage='Create Replication Policies Page',
            retryCount=1
        )
        SeleniumDP.sendKeys(self.getPolicyName(), policyName)
        SeleniumDP.sendKeys(self.getPolicyDescription(), policyDescription)
        self.clickPolicyTypeHive()
        self.clickSourceClusterDropdown()
        self.clickSourceClusterDropdownItem1()
        self.clickDestinationClusterDropdown()
        self.clickDestinationClusterDropdownItem1()
        SeleniumDP.sendKeys(self.getDBSearch(), dbname)
        self.clickDBEntry()
        self.clickPolicyStartFromNow()
        SeleniumDP.sendKeys(self.getPolicyFrequency(), policyFrequency)
        self.clickPolicyUnitDropdown()
        if policyFrequencyUnit == 'minute':
            self.clickPolicyUnitDropdownMinute()
        elif policyFrequencyUnit == 'hour':
            self.clickPolicyUnitDropdownHour()
        elif policyFrequencyUnit == 'day':
            self.clickPolicyUnitDropdownDay()
        else:
            self.clickPolicyUnitDropdownWeek()
        self.clickPolicySubmitButton()
        self.clickPolicySubmitButton()

    def getPolicyEntryActionsDropdown(self, returnLocatorName=False):
        return self.getElement('policyEntryActionsDropdown', returnLocatorName=returnLocatorName)

    def clickPolicyEntryActionsDropdown(self, retryCount=3, timeout=10):
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsDropdown(),
            locatorMessage='Policy Entry Drop Down Present',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsDropdown())

    def getPolicyEntryActionsSuspend(self, returnLocatorName=False):
        return self.getElement('policySuspendAction', returnLocatorName=returnLocatorName)

    def clickPolicyEntryActionsSuspend(self, retryCount=3, timeout=10):
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsSuspend(),
            locatorMessage='Suspend Option Visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsSuspend())

    def getPolicyEntryActionsActivate(self, returnLocatorName=False):
        return self.getElement('policyActivateAction', returnLocatorName=returnLocatorName)

    def clickPolicyEntryActionsActivate(self, retryCount=3, timeout=10):
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsActivate(),
            locatorMessage='Activate Option Visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsActivate())

    def getPolicyEntryActionsDelete(self, returnLocatorName=False):
        return self.getElement('policyDeleteAction', returnLocatorName=returnLocatorName)

    def clickPolicyEntryActionsDelete(self, retryCount=3, timeout=10):
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsDelete(),
            locatorMessage='Delete Option Visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsDelete())

    def getPolicyEntryActionsViewLog(self, returnLocatorName=False):
        return self.getElement('policyViewLogAction', returnLocatorName=returnLocatorName)

    def clickPolicyEntryActionsViewLog(self, retryCount=3, timeout=10):
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsViewLog(),
            locatorMessage='Delete Option Visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsViewLog())

    def getFilterInput(self, returnLocatorName=False):
        return self.getElement('filterInput', returnLocatorName=returnLocatorName)

    def getPolicyDialogOKButton(self, returnLocatorName=False):
        return self.getElement('policyDialogOKButton', returnLocatorName=returnLocatorName)

    def clickPolicyDialogOKButton(self, retryCount=3, timeout=5):
        self.checkElementonPage(
            locatorName=self.getPolicyDialogOKButton(),
            locatorMessage='OK button visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyDialogOKButton())

    def getPolicyDialogYesButton(self, returnLocatorName=False):
        return self.getElement('policyDialogYesButton', returnLocatorName=returnLocatorName)

    def clickPolicyDialogYesButton(self, retryCount=3, timeout=5):
        self.checkElementonPage(
            locatorName=self.getPolicyDialogYesButton(),
            locatorMessage='Yes button visible',
            retryCount=retryCount,
            timeout=timeout
        )
        SeleniumDP.click(self.driver, self.getPolicyDialogYesButton())

    def policyAction(self, policyName, action):
        self.checkElementonPage(
            locatorName=self.getFilterInput(returnLocatorName=True),
            locatorMessage='Filter field visible',
            retryCount=3
        )
        SeleniumDP.sendKeys(self.getFilterInput(), policyName + '\n')
        self.clickPolicyEntryActionsDropdown()
        if action == 'Delete':
            self.clickPolicyEntryActionsDelete()
        elif action == 'Suspend':
            self.clickPolicyEntryActionsSuspend()
        elif action == 'Activate':
            self.clickPolicyEntryActionsActivate()
        self.clickPolicyDialogYesButton()

    def viewPolicyLog(self, policyName):
        SeleniumDP.sendKeys(self.getFilterInput(), policyName + '\n')
        self.clickPolicyEntryActionsDropdown()
        self.checkElementonPage(
            locatorName=self.getPolicyEntryActionsViewLog(),
            locatorMessage='Suspend Action Button Visible',
            retryCount=3,
            timeout=5
        )
        SeleniumDP.click(self.driver, self.getPolicyEntryActionsViewLog())

    def verifyLogDialog(self):
        assert self.checkElementonPage(
            locatorName=self.getElement('policyViewLogDialogHeader', returnLocatorName=True),
            locatorMessage='Log Dialog Visible'
        )
        assert self.checkElementonPage(
            locatorName=self.getElement('policyLogDialogMessage', returnLocatorName=True),
            locatorMessage='Log Dialog Message Visible and not empty'
        )
        self.clickPolicyDialogOKButton()

    def validateInvalidPolicyPath(self):
        self.clickAddPolicyButton()
        self.checkElementonPage(
            locatorName=self.getPolicyName(returnLocatorName=True),
            locatorMessage='Create Replication Policies Page',
            retryCount=1
        )
        SeleniumDP.sendKeys(self.getPolicyName(), "Invalid")
        self.clickPolicyTypeHdfs()
        self.clickSourceClusterDropdown()
        self.clickSourceClusterDropdownItem1()
        self.clickDestinationClusterDropdown()
        self.clickDestinationClusterDropdownItem1()
        SeleniumDP.sendKeys(self.getFolderPathInput(), "/AAAAA###BBTTT")
        self.clickFolderPathInputForm()
        time.sleep(2)
        self.clickFolderPathInputForm()
        assert self.checkElementonPage(
            locatorName=self.getFolderPathInputErrorMessage(returnLocatorName=True),
            locatorMessage='Invalid Path Error Message Visible',
            retryCount=2
        )

    def getPolicyStatusOnUI(self, policyName, returnLocatorName=False):
        SeleniumDP.sendKeys(self.getFilterInput(), policyName + '\n')
        return self.getElement('policyStatusFirstEntry', returnLocatorName=returnLocatorName)

    def validatePolicyStatusOnUI(self, policyName, action):
        if action == 'Delete':
            SeleniumDP.sendKeys(self.getFilterInput(), policyName)
            assert self.checkElementonPage(
                locatorName=self.getElement('policyDoesntExistInFilter', returnLocatorName=True),
                locatorMessage='No Data to Show Message',
                retryCount=3,
                timeout=5
            )
        elif action == 'Suspend':
            assert self.getPolicyStatusOnUI(policyName).text == 'SUSPENDED'
        elif action == 'Activate':
            assert self.getPolicyStatusOnUI(policyName).text == 'ACTIVE'

    def validatePolicyActionNotif(self, policyName, action, basePage, retryCount=5, timeout=10):
        commonPage = CommonPageUtils(basePage)
        assert commonPage.isDlmNotificationDisplayed(retryCount=retryCount, timeout=timeout)
        headerText, paragraphText = commonPage.getDlmNotification()
        if action == 'Delete':
            assert headerText.lower() == 'removing policy successful'
            assert paragraphText.lower() == 'policy ' + policyName.lower() + ' removed successfully!'
        elif action == 'Suspend':
            assert headerText.lower() == 'suspending policy successful'
            assert paragraphText.lower() == 'policy ' + policyName.lower() + ' suspended successfully!'
        elif action == 'Activate':
            assert headerText.lower() == 'activating policy successful'
            assert paragraphText.lower() == 'policy ' + policyName.lower() + ' activated successfully!'

    def getPolicyAppliedFilter(self, returnLocatorName=False):
        return self.getElement('policyAppliedFilter', returnLocatorName=returnLocatorName)
