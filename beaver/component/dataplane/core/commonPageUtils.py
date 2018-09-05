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
from beaver.component.dataplane.core.loginPage import LoginPage
from beaver.component.dataplane.core.knoxLoginPage import KnoxLoginPage
import time

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class CommonPageUtils(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "logoutMenu": (By.ID, "header-menu"),
        "loggedInUser": (By.ID, "user-name"),
        "logoutButton": (By.ID, "logout-btn"),
        "userDropdown": (By.XPATH, "//div[@qe-attr='user-dropdown-toggler']"),
        "logoutButtonDLM": (By.XPATH, "//button[contains(.,'Logout')]"),
        "sideNavServices": (By.ID, "persona_side_nav_item_2"),
        "dlmServiceBox": (By.XPATH, "//div[@title='dlm']"),
        "dssServiceBox": (By.XPATH, "//div[@title='dss']"),
        "enableDlmButton": (By.ID, "enable-dlm-btn"),
        "enableDssButton": (By.ID, "enable-dss-btn"),
        "smartsenseId": (By.NAME, "smartsense-id"),
        "smartsenseIdIncorrectFormat": (
            By.XPATH, "//div[@class='error-message' and contains(.,'Incorrect SmartSense ID format')]"
        ),
        "verifyButton": (By.ID, "verify-btn"),
        "nextButton": (By.ID, "next-btn"),
        "cancelButton": (By.ID, "cancel-btn"),
        "successHeading": (By.ID, "success-heading"),
        "serviceNavigationElement": (By.ID, "persona_drop_down"),
        "dataplaneNavigationElement": (By.XPATH, '//*[@id="personaNav"]/li/div[@title="DataPlane Admin"]'),
        "dlmNavigationElement": (By.XPATH, '//*[@id="personaNav"]/li/div[@title="Data Lifecycle Manager"]'),
        "datastewardNavigationElement": (By.XPATH, '//*[@id="personaNav"]/li/div[@title="Data Steward Studio"]'),
        "infraAdminNavigationElement": (By.XPATH, '//*[@id="personaNav"]/li/div[@title="Infra Admin"]'),
        "dlmSideNavOverview": (By.XPATH, "//a[@qe-attr='go-to-overview']"),
        "dlmSideNavClusters": (By.XPATH, "//a[@qe-attr='go-to-clusters']"),
        "dlmSideNavPairings": (By.XPATH, "//a[@qe-attr='go-to-pairings']"),
        "dlmSideNavPolicies": (By.XPATH, "//a[@qe-attr='go-to-policies']"),
        "dlmSideNavHelp": (By.XPATH, "//a[@qe-attr='go-to-help']"),
        "dlmNotificationHeaderElement": (By.XPATH, "//simple-notifications//simple-notification//h4"),
        "dlmNotificationParagraphElement": (By.XPATH, "//simple-notifications//simple-notification//p"),
        "dpSideNavClusters": (By.ID, "persona_side_nav_item_0"),
        "dpSideNavUsers": (By.ID, "persona_side_nav_item_1"),
        "dpSideNavServices": (By.ID, "persona_side_nav_item_2"),
        "notificationsButton": (By.XPATH, "//dlm-notifications//span[@qe-attr='notifications-dropdown-toggler']"),
        "goToNotificationsButton": (By.XPATH, "//dlm-notifications//button[@qe-attr='go-to-notifications']"),
        "notificationsDropdown": (By.XPATH, "//div[@id='notifications-dropdown']"),
        "notificationsDropdownHeader": (
            By.XPATH, "//div[@id='notifications-dropdown']//div[@class='notifications-title']"
        ),
        "notificationsDropdownTable": (By.XPATH, "//div[@id='notifications-dropdown']//table"),
        "notificationsDropdownTableEntries": (By.XPATH, "//div[@id='notifications-dropdown']//table//tr")
    }

    def getLogoutMenu(self, returnLocatorName=False):
        return self.getElement('logoutMenu', returnLocatorName)

    def getLoggedInUser(self, returnLocatorName=False):
        return self.getElement('loggedInUser', returnLocatorName)

    def getLogoutButton(self, returnLocatorName=False):
        locatorName = 'logoutButton'
        return locatorName if returnLocatorName else self.findElement(*self.locator_dictionary[locatorName])

    def getUserDropdown(self, returnLocatorName=False):
        locatorName = 'userDropdown'
        return locatorName if returnLocatorName else self.findElement(*self.locator_dictionary[locatorName])

    def getLogoutButtonDLM(self, returnLocatorName=False):
        locatorName = 'logoutButtonDLM'
        return locatorName if returnLocatorName else self.findElement(*self.locator_dictionary[locatorName])

    def doLogout(self):
        # Element Ids are different for DLM and DP
        if 'dlm' in self.getCurrentURL().lower():
            SeleniumDP.click(self.driver, self.getUserDropdown())
            self.checkElementonPage(
                locatorName=self.getLogoutButtonDLM(returnLocatorName=True),
                locatorMessage='Logout Button DLM',
                retryCount=4,
                timeout=0.5
            )
            SeleniumDP.click(self.driver, self.getLogoutButtonDLM())
        else:
            SeleniumDP.click(self.driver, self.getLogoutMenu())
            self.checkElementonPage(
                locatorName=self.getLogoutButton(returnLocatorName=True),
                locatorMessage='Logout Button',
                retryCount=4,
                timeout=0.5
            )
            #SeleniumDP.clickUsingClickable(self.getLogoutButton())
            SeleniumDP.click(self.driver, self.getLogoutButton())

    def getLoggedInUserName(self):
        SeleniumDP.click(self.driver, self.getLogoutMenu())
        self.checkElementonPage(
            locatorName=self.getLoggedInUser(returnLocatorName=True),
            locatorMessage='Logout Button',
            retryCount=4,
            timeout=0.5
        )
        loggedInUser = self.getLoggedInUser().text
        SeleniumDP.click(self.driver, self.getLoggedInUser())
        return loggedInUser

    def goToLoginPage(self):
        # Try Loading the basePage
        self.goToBasePage()
        if 'knox' in self.getCurrentURL().lower():
            loginPage = KnoxLoginPage(self)
        else:
            loginPage = LoginPage(self)
        # If the user is logged in, need to perform logout
        if not loginPage.isLoginPage():
            self.doLogout()

    def getSideNavServices(self, returnLocatorName=False):
        return self.getElement('sideNavServices', returnLocatorName)

    def clickSideNavServices(self):
        SeleniumDP.click(self.driver, self.getSideNavServices())

    def getDlmServiceBox(self, returnLocatorName=False):
        return self.getElement('dlmServiceBox', returnLocatorName)

    def getDssServiceBox(self, returnLocatorName=False):
        return self.getElement('dssServiceBox', returnLocatorName)

    def clickDlmServiceBox(self):
        SeleniumDP.click(self.driver, self.getDlmServiceBox())

    def clickDssServiceBox(self):
        SeleniumDP.click(self.driver, self.getDssServiceBox())

    def getEnableDlmButton(self, returnLocatorName=False):
        return self.getElement('enableDlmButton', returnLocatorName)

    def clickEnableDlmButton(self):
        SeleniumDP.click(self.driver, self.getEnableDlmButton())

    def getEnableDssButton(self, returnLocatorName=False):
        return self.getElement('enableDssButton', returnLocatorName)

    def clickEnableDssButton(self):
        SeleniumDP.click(self.driver, self.getEnableDssButton())

    def getSmartsenseId(self, returnLocatorName=False):
        return self.getElement('smartsenseId', returnLocatorName)

    def getSmartsenseIdIncorrectFormat(self, returnLocatorName=False):
        return self.getElement('smartsenseIdIncorrectFormat', returnLocatorName)

    def clickSmartsenseId(self):
        SeleniumDP.click(self.driver, self.getSmartsenseId())

    def getVerifyButton(self, returnLocatorName=False):
        return self.getElement('verifyButton', returnLocatorName)

    def clickVerifyButton(self):
        SeleniumDP.click(self.driver, self.getVerifyButton())

    def getNextButton(self, returnLocatorName=False):
        return self.getElement('nextButton', returnLocatorName)

    def clickNextButton(self):
        SeleniumDP.click(self.driver, self.getNextButton())

    def getCancelButton(self, returnLocatorName=False):
        return self.getElement('cancelButton', returnLocatorName)

    def clickCancelButton(self):
        SeleniumDP.click(self.driver, self.getCancelButton())

    def getSuccessHeading(self, returnLocatorName=False):
        return self.getElement('successHeading', returnLocatorName)

    def enableDLM(self, smartsenseId="A-12341234-C-12341234", verifyInvalidSmartSenseId=False):
        # Click SideNav
        self.clickSideNavServices()

        # Verify Dlm Service Box is visible
        self.checkElementonPage(
            locatorName=self.getDlmServiceBox(returnLocatorName=True),
            locatorMessage='DLM Service Box',
            retryCount=4,
            timeout=0.5
        )

        # Click DlmServiceBox to Enable the DLM Button, then click that Button
        self.clickDlmServiceBox()
        self.clickEnableDlmButton()

        # Verify the next screen for Smartsense Id is visible
        self.checkElementonPage(
            locatorName=self.getSmartsenseId(returnLocatorName=True),
            locatorMessage='Smartsense ID Input Field',
            retryCount=4,
            timeout=0.5
        )
        SeleniumDP.sendKeys(self.getSmartsenseId(), smartsenseId)

        # Verify Invalid SmartSenseId
        if verifyInvalidSmartSenseId:
            self.clickVerifyButton()
            assert self.checkElementonPage(
                locatorName=self.getSmartsenseIdIncorrectFormat(returnLocatorName=True),
                locatorMessage='Smartsense ID Incorrect Format Message Displayed',
                retryCount=1
            )
            self.clickCancelButton()
            return

        # Click Next Button
        self.clickNextButton()

        # Verify the message that service is enabled is displayed
        self.checkElementonPage(
            locatorName=self.getSuccessHeading(returnLocatorName=True),
            locatorMessage='Success Heading for DLM Service Enablement Displayed',
            retryCount=4,
            timeout=2
        )

    def enableDSS(self, smartsenseId="A-12341234-C-12341234"):
        # Click SideNav
        self.clickSideNavServices()

        # Verify DSS Service Box is visible
        self.checkElementonPage(
            locatorName=self.getDssServiceBox(returnLocatorName=True), locatorMessage='DSS Service Box', retryCount=4
        )

        # Click DSS service box to Enable the DSS Button, then click that Button
        self.clickDssServiceBox()
        self.clickEnableDssButton()

        # Verify the next screen for Smartsense Id is visible
        self.checkElementonPage(
            locatorName=self.getSmartsenseId(returnLocatorName=True),
            locatorMessage='Smartsense ID Input Field',
            retryCount=4
        )
        SeleniumDP.sendKeys(self.getSmartsenseId(), smartsenseId)

        # Click Next Button
        self.clickNextButton()

        # Verify the message that service is enabled is displayed
        self.checkElementonPage(
            locatorName=self.getSuccessHeading(returnLocatorName=True),
            locatorMessage='Success Heading for DSS Service Enablement Displayed',
            retryCount=4
        )

    def getserviceNavigationElement(self, returnLocatorName=False):
        return self.getElement('serviceNavigationElement', returnLocatorName)

    def clickServiceNavigationElement(self):
        SeleniumDP.click(self.driver, self.getserviceNavigationElement())

    def getDlmNavigationElement(self, returnLocatorName=False):
        return self.getElement('dlmNavigationElement', returnLocatorName)

    def clickDlmNavigationElement(self):
        SeleniumDP.click(self.driver, self.getDlmNavigationElement())

    def getDPNavigationElement(self, returnLocatorName=False):
        return self.getElement('dataplaneNavigationElement', returnLocatorName)

    def clickDPNavigationElement(self):
        SeleniumDP.click(self.driver, self.getDlmNavigationElement())

    def getDSSNavigationElement(self, returnLocatorName=False):
        return self.getElement('datastewardNavigationElement', returnLocatorName)

    def clickDSSNavigationElement(self):
        SeleniumDP.click(self.driver, self.getDlmNavigationElement())

    def getInfraAdminNavigationElement(self, returnLocatorName=False):
        return self.getElement('infraAdminNavigationElement', returnLocatorName)

    def clickInfraAdminNavigationElement(self):
        SeleniumDP.click(self.driver, self.getInfraAdminNavigationElement())

    def goToDlmViaUI(self):
        self.clickServiceNavigationElement()
        logger.info("Checking if the DLM icon is visible..")
        assert self.checkElementonPage(
            locatorName=self.getDlmNavigationElement(returnLocatorName=True), locatorMessage='DLM Icon', retryCount=2
        )
        logger.info("DLM icon is visible !")
        self.clickDlmNavigationElement()

    def navigateToDlm(self):
        dlmUrl = "%s/dlm" % self.getBaseUrl()
        self.navigateToPage(dlmUrl)

    def getDlmSideNavOverview(self, returnLocatorName=False):
        return self.getElement('dlmSideNavOverview', returnLocatorName)

    def clickDlmSideNavOverview(self):
        SeleniumDP.click(self.driver, self.getDlmSideNavOverview())

    def getDlmSideNavClusters(self, returnLocatorName=False):
        return self.getElement('dlmSideNavClusters', returnLocatorName)

    def clickDlmSideNavClusters(self):
        SeleniumDP.click(self.driver, self.getDlmSideNavClusters())

    def getDlmSideNavPairings(self, returnLocatorName=False):
        return self.getElement('dlmSideNavPairings', returnLocatorName)

    def clickDlmSideNavPairings(self):
        SeleniumDP.click(self.driver, self.getDlmSideNavPairings())

    def getDlmSideNavPolicies(self, returnLocatorName=False):
        return self.getElement('dlmSideNavPolicies', returnLocatorName)

    def clickDlmSideNavPolicies(self):
        SeleniumDP.click(self.driver, self.getDlmSideNavPolicies())

    def getDlmSideNavHelp(self, returnLocatorName=False):
        return self.getElement('dlmSideNavHelp', returnLocatorName)

    def clickDlmSideNavHelp(self):
        SeleniumDP.click(self.driver, self.getDlmSideNavHelp())

    # Utility method to get numberOfOpenWindows
    def getNumberOfOpenWindows(self):
        return len(self.driver.window_handles)

    # Utility method to get validate HelpPage Opens In New Window
    def isHelpPageOpensInNewWindow(self):
        helpPageOpensInNewWindow = False
        dlmUIHandle = None
        for handle in self.driver.window_handles:
            self.driver.switch_to.window(handle)
            if 'docs.hortonworks.com' in self.getCurrentURL().lower():
                helpPageOpensInNewWindow = True
            else:
                dlmUIHandle = handle
        # Switch back to dlmUIHandle
        self.driver.switch_to.window(dlmUIHandle)
        return helpPageOpensInNewWindow

    def getDlmNotificationHeaderElement(self, returnLocatorName=False):
        return self.getElement('dlmNotificationHeaderElement', returnLocatorName)

    def getDlmNotificationParagraphElement(self, returnLocatorName=False):
        return self.getElement('dlmNotificationParagraphElement', returnLocatorName)

    def isDlmNotificationDisplayed(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDlmNotificationHeaderElement(returnLocatorName=True),
            locatorMessage='Dlm Notification Displayed',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getDlmNotification(self):
        headerText = self.getDlmNotificationHeaderElement().text
        paragraphText = self.getDlmNotificationParagraphElement().text
        SeleniumDP.click(self.driver, self.getDlmNotificationParagraphElement())
        return headerText, paragraphText

    def validateDlmPairing(self, unpair=False, retryCount=5, timeout=3):
        assert self.isDlmNotificationDisplayed(retryCount=retryCount, timeout=timeout)
        headerText, paragraphText = self.getDlmNotification()
        if unpair:
            assert headerText.lower() == 'unpairing clusters successful'
        else:
            assert headerText.lower() == 'pairing successful'
            assert paragraphText.lower() == 'pairing completed successfully'

    def validatePolicyCreation(self, policyName, retryCount=5, timeout=10):
        assert self.isDlmNotificationDisplayed(retryCount=retryCount, timeout=timeout)
        headerText, paragraphText = self.getDlmNotification()
        assert headerText.lower() == 'policy creation successful'
        assert paragraphText.lower() == 'replication policy ' + policyName.lower() + ' created successfully!'

    def getDPSideNavClusters(self, returnLocatorName=False):
        return self.getElement('dpSideNavClusters', returnLocatorName)

    def clickDPSideNavClusters(self):
        SeleniumDP.click(self.driver, self.getDPSideNavClusters())

    def getDPSideNavUsers(self, returnLocatorName=False):
        return self.getElement('dpSideNavUsers', returnLocatorName)

    def clickDPSideNavUsers(self):
        SeleniumDP.click(self.driver, self.getDPSideNavUsers())

    def getDPSideNavServices(self, returnLocatorName=False):
        return self.getElement('dpSideNavServices', returnLocatorName)

    def clickDPSideNavServices(self):
        SeleniumDP.click(self.driver, self.getDPSideNavServices())

    def verifyIfDPSideNavClustersExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDPSideNavClusters(returnLocatorName=True),
            locatorMessage='Side Navigation to Clusters exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfDPSideNavUsersExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDPSideNavUsers(returnLocatorName=True),
            locatorMessage='Side Navigation to Users exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfDPSideNavServicesExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDPSideNavServices(returnLocatorName=True),
            locatorMessage='Side Navigation to Services exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfDPServiceNavigationExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDPNavigationElement(returnLocatorName=True),
            locatorMessage='Dataplane service navigation exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfDLMServiceNavigationExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDlmNavigationElement(returnLocatorName=True),
            locatorMessage='Data Lifecycle Manager service navigation exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfDSSServiceNavigationExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getDSSNavigationElement(returnLocatorName=True),
            locatorMessage='Data Steward Studio service navigation exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyIfInfraAdminServiceNavigationExists(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getInfraAdminNavigationElement(returnLocatorName=True),
            locatorMessage='Infra Admin service navigation exists',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getNotificationsDropdown(self, returnLocatorName=False):
        return self.getElement('notificationsButton', returnLocatorName)

    def clickNotificationsDropdown(self):
        SeleniumDP.click(self.driver, self.getNotificationsDropdown())

    def getNotificationsDropdownTable(self, returnLocatorName=False):
        return self.getElement('notificationsButton', returnLocatorName)

    def getNotificationsDropdownTableEntriesCount(self, returnLocatorName=False):
        return len(self.getElements('notificationsDropdownTableEntries', returnLocatorName))

    def verifyNotificationDropDown(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getNotificationsDropdown(returnLocatorName=True),
            locatorMessage='Notifications Dropdown Present',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyNotificationsDropdownTable(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getNotificationsDropdownTable(returnLocatorName=True),
            locatorMessage='Table of notifications Present',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def verifyGoToNotificationsButton(self, retryCount=2, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getGoToNotificationsButton(returnLocatorName=True),
            locatorMessage='View All Button Present',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def validateNotificationsDropdown(self, retryCount=2, timeout=None, quitWebdriver=False):
        assert self.verifyNotificationDropDown(retryCount=retryCount, timeout=timeout, quitWebdriver=quitWebdriver)
        assert self.verifyNotificationsDropdownTable(
            retryCount=retryCount, timeout=timeout, quitWebdriver=quitWebdriver
        )
        assert self.getNotificationsDropdownTableEntriesCount() == 5
        assert self.verifyGoToNotificationsButton(retryCount=retryCount, timeout=timeout, quitWebdriver=quitWebdriver)

    def getGoToNotificationsButton(self, returnLocatorName=False):
        return self.getElement('goToNotificationsButton', returnLocatorName)

    def clickGoToNotificationsButton(self):
        SeleniumDP.click(self.driver, self.getGoToNotificationsButton())
