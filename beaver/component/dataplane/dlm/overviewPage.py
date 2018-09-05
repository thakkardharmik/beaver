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


class OverviewPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "totalClusterCountElement": (By.XPATH, "//dlm-clusters-summary/dlm-summary-panel//*[@class='total-counter']"),
        "totalPoliciesCountElement": (By.XPATH, "//dlm-policies-summary/dlm-summary-panel//*[@class='total-counter']"),
        "totalJobsCountElement": (By.XPATH, "//dlm-jobs-summary/dlm-summary-panel//*[@class='total-counter']"),
        "totalActivePoliciesCountElement": (
            By.XPATH, "//dlm-policies-summary/dlm-summary-panel//dlm-summary-panel-cell[1]//p[2]/span"
        ),
        "totalSuspendedPoliciesCountElement": (
            By.XPATH, "//dlm-policies-summary/dlm-summary-panel//dlm-summary-panel-cell[2]//p[2]/span"
        )
    }

    def isOverviewPage(self, retryCount=7, timeout=10, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getTotalClusterCountElement(returnLocatorName=True),
            locatorMessage='DLM Overview Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getTotalClusterCountElement(self, returnLocatorName=False):
        return self.getElement('totalClusterCountElement', returnLocatorName)

    def getTotalClusterCount(self):
        return self.getTotalClusterCountElement().text

    def getTotalPoliciesCountElement(self, returnLocatorName=False):
        return self.getElement('totalPoliciesCountElement', returnLocatorName)

    def getTotalPoliciesCount(self):
        return int(self.getTotalPoliciesCountElement().text)

    def getTotalJobsCountElement(self, returnLocatorName=False):
        return self.getElement('totalJobsCountElement', returnLocatorName)

    def getTotalJobsCount(self):
        return self.getTotalJobsCountElement().text

    def getTotalActivePoliciesCountElement(self, returnLocatorName=False):
        return self.getElement('totalActivePoliciesCountElement', returnLocatorName)

    def getTotalActivePoliciesCount(self):
        count = self.getTotalActivePoliciesCountElement().text
        if count == '-':
            return 0
        else:
            return int(count)

    def getTotalSuspendedPoliciesCountElement(self, returnLocatorName=False):
        return self.getElement('totalSuspendedPoliciesCountElement', returnLocatorName)

    def getTotalSuspendedPoliciesCount(self):
        count = self.getTotalSuspendedPoliciesCountElement().text
        if count == '-':
            return 0
        else:
            return int(count)
