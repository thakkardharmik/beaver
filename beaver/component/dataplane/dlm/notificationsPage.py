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


class NotificationsPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "pageTitle": (By.XPATH, "//breadcrumb//*[contains(.,'Notifications')]"),
        "firstNotifEntity": (By.XPATH, "(//dlm-table//datatable-body-row//dlm-event-entity-link)[1]//span"),
        "firstNotifViewLog": (By.XPATH, "(//dlm-table//datatable-body-row//span[@qe-attr='show-log'])[1]"),
        "notificationsProgressTable": (By.XPATH, "//dlm-notifications-table/dlm-table//datatable-body"),
        "notificationsProgressTableRows": (By.XPATH, "//dlm-notifications-table/dlm-table//datatable-body-row"),
        "notificationViewLogDialogHeader": (
            By.XPATH, "//dlm-log-modal-dialog//div[@class='modal-header'][contains(.,'Log')]"
        ),
        "notificationLogDialogMessage": (
            By.XPATH, "//dlm-modal-dialog-body//pre[@class='log-message' and text() != '']"
        ),
        "notificationsDialogOKButton": (
            By.XPATH, "//dlm-modal-dialog//div[@class='modal-footer']//button[@qe-attr='modal-confirm']"
        )
    }

    def isNotificationsPage(self, retryCount=2, timeout=3, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getPageTitle(returnLocatorName=True),
            locatorMessage='DLM Notifications Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getPageTitle(self, returnLocatorName=False):
        return self.getElement('pageTitle', returnLocatorName)

    def getNotificationsProgressTable(self, returnLocatorName=False):
        return self.getElement('notificationsProgressTable', returnLocatorName)

    def getNotificationsProgressTableRowsCount(self, returnLocatorName=False):
        return len(self.getElements('notificationsProgressTableRows', returnLocatorName))

    def validateNotificationsProgressTable(self):
        assert self.checkElementonPage(
            locatorName=self.getNotificationsProgressTable(returnLocatorName=True),
            locatorMessage='Progress Table Visible'
        )
        assert self.getNotificationsProgressTableRowsCount() == 10

    def getFirstNotifEntity(self, returnLocatorName=False):
        return self.getElement('firstNotifEntity', returnLocatorName)

    def clickFirstNotifEntity(self):
        SeleniumDP.click(self.driver, self.getFirstNotifEntity())

    def getFirstNotifViewLog(self, returnLocatorName=False):
        return self.getElement('firstNotifViewLog', returnLocatorName)

    def clickFirstNotifViewLog(self):
        SeleniumDP.click(self.driver, self.getFirstNotifViewLog())

    def getNotificationsDialogOKButton(self, returnLocatorName=False):
        return self.getElement('notificationsDialogOKButton', returnLocatorName)

    def clickNotificationsDialogOKButton(self):
        SeleniumDP.click(self.driver, self.getNotificationsDialogOKButton())

    def validateLogDialog(self):
        assert self.checkElementonPage(
            locatorName=self.getElement('notificationViewLogDialogHeader', returnLocatorName=True),
            locatorMessage='Log Dialog Visible'
        )
        assert self.checkElementonPage(
            locatorName=self.getElement('notificationLogDialogMessage', returnLocatorName=True),
            locatorMessage='Log Dialog Message Visible and not empty'
        )
        self.clickNotificationsDialogOKButton()
