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

from beaver.machine import Machine
import os, sys

dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(dir, '../../../../tests/xasecure/xa-agents')))
import xa_testenv
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class LandingPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "addHdfsRepoBtn": (By.XPATH, "//span[contains(.,'hdfs')]/span/a[@class='pull-right text-decoration']"),
        "topNavAuditDiv": (
            By.XPATH, "//div[@id='r_topNav']//div[@class='nav-collapse collapse']//a[contains(.,'Audit')]"
        ),
        "topNavAccessManager": (
            By.XPATH, "//div[@id='r_topNav']//div[@class='nav-collapse collapse']//a[contains(.,'Access Manager')]"
        ),
        "accessManagerDropdownTagBasedPols": (
            By.XPATH, "//div[@id='r_topNav']//ul[@class='dropdown-menu']//a[@href='#!/policymanager/tag']"
        )
    }

    def getAddHdfsRepoBtn(self, returnLocatorName=False):
        return self.getElement('addHdfsRepoBtn', returnLocatorName)

    def getTopNavAuditDiv(self, returnLocatorName=False):
        return self.getElement('topNavAuditDiv', returnLocatorName)

    def addServiceLinkToLocDict(self, serviceType, serviceName):
        self.locator_dictionary["serviceLink"] = (
            By.XPATH, "//thead[contains(.,'" + serviceType + "')]/following-sibling::tbody[1]//a[contains(.,'" +
            serviceName + "')]"
        )

    @TaskReporter.report_test()
    def getServiceLink(self, serviceType, serviceName, returnLocatorName=False):
        logger.info("INSIDE getServiceLink --->")
        self.addServiceLinkToLocDict(serviceType, serviceName)
        return self.getElement('serviceLink', returnLocatorName)

    def getTopNavAccessManager(self, returnLocatorName=False):
        return self.getElement('topNavAccessManager', returnLocatorName)

    def getAccessManagerDropdownTagBasedPols(self, returnLocatorName=False):
        return self.getElement('accessManagerDropdownTagBasedPols', returnLocatorName)

    def isLandingPg(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            self,
            locatorName='addHdfsRepoBtn',
            locatorMessage='Landing Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    @TaskReporter.report_test()
    def clickToGoToPolicyLandingPage(self, policyType):

        IS_HUMBOLDT = Machine.isWindows()
        global serviceName, service_name_type_dict, service_name_type_dict_for_Humboldt

        service_name_type_dict_for_Humboldt = {
            'hadoop': 'hadoopdev',
            'hive': 'hivedev',
            'hbase': 'hbasedev',
            'knox': 'knoxdev',
            'storm': 'stormdev'
        }
        service_name_type_dict = {
            'hadoop': xa_testenv.getEnv("XA_HDFS_REPO_NAME", "hadoopdev"),
            'hive': xa_testenv.getEnv("XA_HIVE_REPO_NAME", "hivedev"),
            'hbase': xa_testenv.getEnv("XA_HBASE_REPO_NAME", "hbasedev"),
            'knox': xa_testenv.getEnv("XA_KNOX_REPO_NAME", "knoxdev"),
            'storm': xa_testenv.getEnv("XA_STORM_REPO_NAME", "stormdev")
        }

        if IS_HUMBOLDT:
            logger.info("Its a Humboldt cluster !!")
            serviceName = service_name_type_dict_for_Humboldt[policyType]
        else:
            logger.info("Its NOT a Humboldt cluster !!")
            serviceName = service_name_type_dict[policyType]
        logger.info("serviceName: " + serviceName)
        SeleniumDP.click(self.driver, self.getServiceLink(policyType, serviceName))

    def clickToGotoAuditPage(self):
        SeleniumDP.click(self.driver, self.getTopNavAuditDiv())

    @TaskReporter.report_test()
    def clickToGotoTagLandingPage(self):
        SeleniumDP.click(self.driver, self.getTopNavAccessManager())
        self.waitForElement(self, self.getAccessManagerDropdownTagBasedPols(returnLocatorName=True))
        SeleniumDP.click(self.driver, self.getAccessManagerDropdownTagBasedPols())
