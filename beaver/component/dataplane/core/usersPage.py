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

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class UsersPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    usersTableBodyElementXPath = "//table[@dp-config-table='']/tbody"
    locator_dictionary = {
        "addUserButton": (By.ID, "add-user"),
        "addGroupButton": (By.ID, "add-group"),
        "usersNav": (By.XPATH, "//span[text()[contains(.,'USERS')]]"),
        "groupsNav": (By.XPATH, "//span[text()[contains(.,'GROUPS')]]"),
        "serachUsers": (By.ID, "search"),
        #"usersInfoTable": (By.XPATH, "//*[@class='table-container']/table"),
        "usersInfoTableBody": (By.XPATH, usersTableBodyElementXPath),
        "usernameInputField": (By.XPATH, "//*[@id='user-tags']/div/span/input"),
        "roleInputField": (By.XPATH, "//*[@id='role-tags']/div/span/input"),
        "groupInputField": (By.XPATH, "//*[@id='group-tags']/div/span/input"),
        "usernameDropDownElement": (By.XPATH, "//*[@id='user-tags']/div/span/div"),
        "usernameDropDownValueElement": (By.XPATH, "//*[@id='user-tags']/div/span/div/div"),
        "usernameTaggingElement": (By.XPATH, "//*[@id='user-tags']/div"),
        "groupTaggingElement": (By.XPATH, "//*[@id='group-tags']/div"),
        "rolesTaggingElement": (By.XPATH, "//*[@id='role-tags']/div"),
        "rolesDropDownElement": (By.XPATH, "//*[@id='role-tags']/div/span/div"),
        "rolesDropDownValueElement": (By.XPATH, "//*[@id='role-tags']/div/span/div/div"),
        "groupsDropDownElement": (By.XPATH, "//*[@id='group-tags']/div/span/div"),
        "groupsDropDownValueElement": (By.XPATH, "//*[@id='group-tags']/div/span/div/div"),
        "saveButtonElement": (By.ID, "save-btn"),
        "cancelButtonElement": (By.ID, "cancel-btn"),
        "currentRolesForAddUser": (By.XPATH, "//*[@id='role-tags']/div"),
        "unauthorizedNotificationTitleElement": (By.XPATH, "//div[@class='error-notification-bar']/div[1]"),
        "unauthorizedNotificationTextElement": (By.XPATH, "//div[@class='error-notification-bar']/div[2]"),
        "unauthorizedNotificationSignOutElement": (By.XPATH, "//div[@class='error-notification-bar']/div[3]/a"),
        "statusSwitchToggleElement": (By.XPATH, "//input[@id='status-switch']/../div[@class='mdl-switch__track']"),
        "statusSwitchLabelElement": (By.XPATH, "//input[@id='status-switch']/../span[@class='mdl-switch__label']")
    }

    def isUsersPage(self, retryCount=10, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getAddUserButton(returnLocatorName=True),
            locatorMessage='Add Users Button',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def isGroupsPage(self, retryCount=10, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getAddGroupButton(returnLocatorName=True),
            locatorMessage='Add Groups Button',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getAddUserButton(self, returnLocatorName=False):
        return self.getElement('addUserButton', returnLocatorName)

    def getAddGroupButton(self, returnLocatorName=False):
        return self.getElement('addGroupButton', returnLocatorName)

    def getUsersNavigationLink(self, returnLocatorName=False):
        return self.getElement('usersNav', returnLocatorName)

    def getGroupsNavigationLink(self, returnLocatorName=False):
        return self.getElement('groupsNav', returnLocatorName)

    def getUsersInfoTableBody(self, returnLocatorName=False):
        return self.getElement('usersInfoTableBody', returnLocatorName)

    def getUsernameField(self, returnLocatorName=False):
        return self.getElement('usernameInputField', returnLocatorName)

    def getRoleField(self, returnLocatorName=False):
        return self.getElement('roleInputField', returnLocatorName)

    def getGroupField(self, returnLocatorName=False):
        return self.getElement('groupInputField', returnLocatorName)

    def getUsernameDropDownElement(self, returnLocatorName=False):
        return self.getElement('usernameDropDownElement', returnLocatorName)

    def getUsernameTaggingElement(self, returnLocatorName=False):
        return self.getElement('usernameTaggingElement', returnLocatorName)

    def getGroupTaggingElement(self, returnLocatorName=False):
        return self.getElement('groupTaggingElement', returnLocatorName)

    def getRolesTaggingElement(self, returnLocatorName=False):
        return self.getElement('rolesTaggingElement', returnLocatorName)

    def getUsernameDropDownValueElement(self, returnLocatorName=False):
        return self.getElement('usernameDropDownValueElement', returnLocatorName)

    def getRolesDropDownElement(self, returnLocatorName=False):
        return self.getElement('rolesDropDownElement', returnLocatorName)

    def getRolesDropDownValueElement(self, returnLocatorName=False):
        return self.getElement('rolesDropDownValueElement', returnLocatorName)

    def getGroupsDropDownElement(self, returnLocatorName=False):
        return self.getElement('groupsDropDownElement', returnLocatorName)

    def getGroupsDropDownValueElement(self, returnLocatorName=False):
        return self.getElement('groupsDropDownValueElement', returnLocatorName)

    def getSaveButton(self, returnLocatorName=False):
        return self.getElement('saveButtonElement', returnLocatorName)

    def getCancelButton(self, returnLocatorName=False):
        return self.getElement('cancelButtonElement', returnLocatorName)

    def getCurrentRolesForAddUser(self, returnLocatorName=False):
        return self.getElement('currentRolesForAddUser', returnLocatorName)

    def getUnauthorizedNotificationTitleElement(self, returnLocatorName=False):
        return self.getElement('unauthorizedNotificationTitleElement', returnLocatorName)

    def getUnauthorizedNotificationTextElement(self, returnLocatorName=False):
        return self.getElement('unauthorizedNotificationTextElement', returnLocatorName)

    def getUnauthorizedNotificationSignOutElement(self, returnLocatorName=False):
        return self.getElement('unauthorizedNotificationSignOutElement', returnLocatorName)

    def getStatusSwitchToggleElement(self, returnLocatorName=False):
        return self.getElement('statusSwitchToggleElement', returnLocatorName)

    def getStatusSwitchLabelElement(self, returnLocatorName=False):
        return self.getElement('statusSwitchLabelElement', returnLocatorName)

    def clickUsersLink(self):
        SeleniumDP.click(self.driver, self.getUsersNavigationLink())

    def clickAddUserButton(self):
        SeleniumDP.click(self.driver, self.getAddUserButton())
        assert self.checkElementonPage(
            locatorName=self.getUsernameField(returnLocatorName=True),
            locatorMessage='Username input field',
            retryCount=5
        )

    def clickAddGroupButton(self):
        SeleniumDP.click(self.driver, self.getAddGroupButton())
        assert self.checkElementonPage(
            locatorName=self.getGroupField(returnLocatorName=True), locatorMessage='Group input field', retryCount=5
        )

    def clickSaveButton(self):
        SeleniumDP.click(self.driver, self.getSaveButton())
        self.isUsersPage()

    def clickSaveGroupsButton(self):
        SeleniumDP.click(self.driver, self.getSaveButton())
        self.isGroupsPage()

    def clickCancelButton(self):
        SeleniumDP.click(self.driver, self.getCancelButton())
        self.isUsersPage()

    def clickCancelGroupsButton(self):
        SeleniumDP.click(self.driver, self.getCancelButton())
        self.isGroupsPage()

    def clickRolesInputField(self):
        SeleniumDP.click(self.driver, self.getRoleField())

    def clickUsernameInputField(self):
        SeleniumDP.click(self.driver, self.getUsernameField())

    def clickGroupInputField(self):
        SeleniumDP.click(self.driver, self.getGroupField())

    def clickGroupsLink(self):
        SeleniumDP.click(self.driver, self.getGroupsNavigationLink())
        assert self.checkElementonPage(
            locatorName=self.getAddGroupButton(returnLocatorName=True),
            locatorMessage='Add group button',
            retryCount=5
        )

    def selectUsernameValueFromDropDown(self):
        SeleniumDP.click(self.driver, self.getUsernameDropDownValueElement())

    def selectRolesValueFromDropDown(self):
        SeleniumDP.click(self.driver, self.getRolesDropDownValueElement())

    def selectGroupsValueFromDropDown(self):
        SeleniumDP.click(self.driver, self.getGroupsDropDownValueElement())

    def selectUsername(self, username):
        tries = 5
        while tries > 0:
            try:
                SeleniumDP.sendKeys(self.getUsernameField(), username)
                dropDownElementExists = self.checkElementonPage(
                    locatorName=self.getUsernameDropDownElement(returnLocatorName=True),
                    locatorMessage='Drop Down element',
                    retryCount=15
                )
                if dropDownElementExists:
                    dropDownOptions = self.findElement(
                        *self.locator_dictionary[self.getUsernameDropDownElement(returnLocatorName=True)]
                    )
                    for option in dropDownOptions.find_elements(By.TAG_NAME, 'div'):
                        if username == option.text.lower():
                            SeleniumDP.click(self.driver, option)
                            self.take_screenshot("selectUsername")
                            userTagExists = self.checkElementonPage(
                                locatorName=self.getUsernameTaggingElement(returnLocatorName=True),
                                locatorMessage='User Tags',
                                retryCount=15
                            )
                            if userTagExists:
                                userTags = self.findElement(
                                    *self.locator_dictionary[self.getUsernameTaggingElement(returnLocatorName=True)]
                                )
                                for tag in userTags.find_elements(By.CLASS_NAME, 'tagSticker'):
                                    if username == tag.text.strip().lower():
                                        self.take_screenshot("selectUsername")
                                        return
            except:
                tries = tries - 1

    def selectRoles(self, roles):
        tries = 5
        while tries > 0:
            try:
                SeleniumDP.sendKeys(self.getRoleField(), roles)
                dropDownElementExists = self.checkElementonPage(
                    locatorName=self.getRolesDropDownElement(returnLocatorName=True),
                    locatorMessage='Drop Down element',
                    retryCount=15
                )
                if dropDownElementExists:
                    dropDownOptions = self.findElement(
                        *self.locator_dictionary[self.getRolesDropDownElement(returnLocatorName=True)]
                    )
                    for option in dropDownOptions.find_elements(By.TAG_NAME, 'div'):
                        if roles == option.text.lower():
                            SeleniumDP.click(self.driver, option)
                            self.take_screenshot("selectRoles")
                            rolesTagExists = self.checkElementonPage(
                                locatorName=self.getRolesTaggingElement(returnLocatorName=True),
                                locatorMessage='Role Tags',
                                retryCount=15
                            )
                            if rolesTagExists:
                                rolesTags = self.findElement(
                                    *self.locator_dictionary[self.getRolesTaggingElement(returnLocatorName=True)]
                                )
                                for tag in rolesTags.find_elements(By.CLASS_NAME, 'tagSticker'):
                                    if roles == tag.text.strip().lower():
                                        self.take_screenshot("selectRoles")
                                        return
            except:
                tries = tries - 1

    def selectGroups(self, groups):
        tries = 5
        while tries > 0:
            try:
                SeleniumDP.sendKeys(self.getGroupField(), groups)
                dropDownElementExists = self.checkElementonPage(
                    locatorName=self.getGroupsDropDownElement(returnLocatorName=True),
                    locatorMessage='Drop Down element',
                    retryCount=15
                )
                if dropDownElementExists:
                    dropDownOptions = self.findElement(
                        *self.locator_dictionary[self.getGroupsDropDownElement(returnLocatorName=True)]
                    )
                    for option in dropDownOptions.find_elements(By.TAG_NAME, 'div'):
                        if groups == option.text.lower():
                            SeleniumDP.click(self.driver, option)
                            self.take_screenshot("selectGroups")
                            groupTagExists = self.checkElementonPage(
                                locatorName=self.getGroupTaggingElement(returnLocatorName=True),
                                locatorMessage='Group Tags',
                                retryCount=15
                            )
                            if groupTagExists:
                                groupTags = self.findElement(
                                    *self.locator_dictionary[self.getGroupTaggingElement(returnLocatorName=True)]
                                )
                                for tag in groupTags.find_elements(By.CLASS_NAME, 'tagSticker'):
                                    if groups == tag.text.strip().lower():
                                        self.take_screenshot("selectGroups")
                                        return
            except:
                tries = tries - 1

    def checkIfUsernameExists(self, username):
        self.isUsersPage()
        userInfoTableExists = self.checkElementonPage(
            locatorName=self.getUsersInfoTableBody(returnLocatorName=True),
            locatorMessage='Users info table',
            retryCount=15
        )
        if userInfoTableExists:
            userInfoTable = self.findElement(
                *self.locator_dictionary[self.getUsersInfoTableBody(returnLocatorName=True)]
            )
            for row in userInfoTable.find_elements(By.TAG_NAME, 'tr'):
                columns = row.find_elements(By.TAG_NAME, 'td')
                if username.lower() == columns[0].text.lower():
                    return True
            return False

    def getUserStatus(self, username):
        self.isUsersPage()
        userStatus = None
        tries = 5
        while tries > 0:
            try:
                userInfoTableExists = self.checkElementonPage(
                    locatorName=self.getUsersInfoTableBody(returnLocatorName=True),
                    locatorMessage='Users info table',
                    retryCount=15
                )
                if userInfoTableExists:
                    userInfoTable = self.findElement(
                        *self.locator_dictionary[self.getUsersInfoTableBody(returnLocatorName=True)]
                    )
                    for row in userInfoTable.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if username.lower() == columns[0].text.lower():
                            userStatus = columns[2].text
                            tries = tries - 1
            except:
                tries = tries - 1
                self.take_screenshot("getUserStatus")
        return userStatus

    def getUserRoles(self, username):
        self.isUsersPage()
        userRoles = None
        tries = 5
        while tries > 0:
            try:
                userInfoTableExists = self.checkElementonPage(
                    locatorName=self.getUsersInfoTableBody(returnLocatorName=True),
                    locatorMessage='Users info table',
                    retryCount=15
                )
                if userInfoTableExists:
                    userInfoTable = self.findElement(
                        *self.locator_dictionary[self.getUsersInfoTableBody(returnLocatorName=True)]
                    )
                    for row in userInfoTable.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if username.lower() == columns[0].text.lower():
                            userRoles = columns[1].text.lower()
                            tries = tries - 1
            except:
                tries = tries - 1
                self.take_screenshot("getUserRoles")
        return userRoles

    def getGroupRoles(self, groupName):
        self.isGroupsPage()
        groupRoles = None
        tries = 5
        while tries > 0:
            try:
                groupInfoTableExists = self.checkElementonPage(
                    locatorName=self.getUsersInfoTableBody(returnLocatorName=True),
                    locatorMessage='Group info table',
                    retryCount=15
                )
                if groupInfoTableExists:
                    groupInfoTable = self.findElement(
                        *self.locator_dictionary[self.getUsersInfoTableBody(returnLocatorName=True)]
                    )
                    for row in groupInfoTable.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if groupName.lower() == columns[0].text.lower():
                            groupRoles = columns[1].text.lower()
                            tries = tries - 1
            except:
                tries = tries - 1
                self.take_screenshot("getGroupRoles")
        return groupRoles

    def removeUserRole(self, deleteRole):
        rolesElement = self.checkElementonPage(
            locatorName=self.getCurrentRolesForAddUser(returnLocatorName=True), locatorMessage='Roles', retryCount=15
        )
        if rolesElement:
            currentRoles = self.findElement(
                *self.locator_dictionary[self.getCurrentRolesForAddUser(returnLocatorName=True)]
            )
            for role in currentRoles.find_elements(By.TAG_NAME, 'span'):
                if deleteRole.lower() == role.text.strip().lower():
                    role.find_element(By.TAG_NAME, 'span').click()
                    self.waitForElementInvisibility(locatorName=self.getCurrentRolesForAddUser(returnLocatorName=True))
                    self.take_screenshot("removeUserRole")
                    return True
            return False

    def editUser(self, username):
        self.isUsersPage()
        userInfoTableExists = self.checkElementonPage(
            locatorName=self.getUsersInfoTableBody(returnLocatorName=True),
            locatorMessage='Users info table',
            retryCount=15
        )
        if userInfoTableExists:
            userInfoTable = self.findElement(
                *self.locator_dictionary[self.getUsersInfoTableBody(returnLocatorName=True)]
            )
            for row in userInfoTable.find_elements(By.TAG_NAME, 'tr'):
                columns = row.find_elements(By.TAG_NAME, 'td')
                if username.lower() == columns[0].text.lower():
                    SeleniumDP.click(self.driver, columns[0])
                    self.take_screenshot("editUser")
                    break

    def switchUserStatusToActive(self):
        userStatusLabelExists = self.checkElementonPage(
            locatorName=self.getStatusSwitchLabelElement(returnLocatorName=True),
            locatorMessage='User Status label ',
            retryCount=15
        )
        if userStatusLabelExists:
            currentStatus = self.findElement(
                *self.locator_dictionary[self.getStatusSwitchLabelElement(returnLocatorName=True)]
            ).text
            if currentStatus.lower() == 'inactive':
                SeleniumDP.click(self.driver, self.getStatusSwitchToggleElement())
            else:
                logger.info("--- Current user status is Active")

    def switchUserStatusToInactive(self):
        userStatusLabelExists = self.checkElementonPage(
            locatorName=self.getStatusSwitchLabelElement(returnLocatorName=True),
            locatorMessage='User Status label ',
            retryCount=15
        )
        if userStatusLabelExists:
            currentStatus = self.findElement(
                *self.locator_dictionary[self.getStatusSwitchLabelElement(returnLocatorName=True)]
            ).text
            if currentStatus.lower() == 'active':
                SeleniumDP.click(self.driver, self.getStatusSwitchToggleElement())
            else:
                logger.info("--- Current user status is Inactive")

    def verifyUnauthorizedUserNotification(self):
        unauthorizedUserNotificationTitleExists = self.checkElementonPage(
            locatorName=self.getUnauthorizedNotificationTitleElement(returnLocatorName=True),
            locatorMessage='User Status label ',
            retryCount=15
        )
        if unauthorizedUserNotificationTitleExists:
            unauthorizedUserNotificationTitle = self.findElement(
                *self.locator_dictionary[self.getUnauthorizedNotificationTitleElement(returnLocatorName=True)]
            )
            unauthorizedUserNotificationText = self.findElement(
                *self.locator_dictionary[self.getUnauthorizedNotificationTextElement(returnLocatorName=True)]
            )
            warningTitle = unauthorizedUserNotificationTitle.text
            warningText = unauthorizedUserNotificationText.text
            if warningTitle.lower() == 'unauthorized' and warningText.lower(
            ) == 'you are not an authorized data plane user. please contact your data plane administrator.':
                return True
            else:
                return False

    def signOutUnauthorizedUser(self):
        unauthorizedUserNotificationTitleExists = self.checkElementonPage(
            locatorName=self.getUnauthorizedNotificationTitleElement(returnLocatorName=True),
            locatorMessage='User Status label ',
            retryCount=15
        )
        if unauthorizedUserNotificationTitleExists:
            SeleniumDP.click(self.driver, self.getUnauthorizedNotificationSignOutElement())
