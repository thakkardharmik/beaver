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


class ClustersPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "pageTitle": (By.XPATH, "//breadcrumb//*[contains(.,'Clusters')]"),
        "statusColorClusterOneElement": (By.XPATH, "//dlm-cluster-status-icon/i"),
        "statusColorClusterTwoElement": (By.XPATH, "//datatable-row-wrapper[2]//dlm-cluster-status-icon/i"),
        "statusClusterOneElement": (By.XPATH, "//span[@qe-attr='cluster-status-0']"),
        "statusClusterTwoElement": (By.XPATH, "//span[@qe-attr='cluster-status-1']"),
        "totalNodesCountClusterOneElement": (By.XPATH, "//span[@qe-attr='total-hosts-0']"),
        "totalNodesCountClusterTwoElement": (By.XPATH, "//span[@qe-attr='total-hosts-1']"),
        "totalPairCountClusterOneElement": (By.XPATH, "//span[@qe-attr='total-pairs-0']"),
        "totalPairCountClusterTwoElement": (By.XPATH, "//span[@qe-attr='total-pairs-1']"),
        "totalPoliciesCountClusterOneElement": (By.XPATH, "//span[@qe-attr='total-policies-0']"),
        "totalPoliciesCountClusterTwoElement": (By.XPATH, "//span[@qe-attr='total-policies-1']"),
        "clusterDetailsDropDownClusterOne": (By.XPATH, "//div[@qe-attr='cluster-details-0']"),
        "clusterDetailsDropDownClusterOneHdfsBrowser": (
            By.XPATH, "//dlm-hdfs-browser[@qe-attr='cluster-actions-hdfs-browser0']/dlm-hdfs-browser-breadcrumb//a"
        ),
        "clusterDetailsDropDownClusterOneHdfsBrowserFirstDirElement": (
            By.XPATH,
            "//dlm-hdfs-browser[@qe-attr='cluster-actions-hdfs-browser0']/dlm-hdfs-browser-breadcrumb/div/span[2]/span"
        ),
        "clusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement": (
            By.XPATH,
            "//dlm-hdfs-browser[@qe-attr='cluster-actions-hdfs-browser0']//datatable-scroller//a/span[contains(.,'tmp')]"
        ),
        "clusterDetailsDropDownClusterTwo": (By.XPATH, "//div[@qe-attr='cluster-details-1']"),
        "clusterActionsClusterOneElement": (By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-0']"),
        "clusterActionsClusterTwoElement": (By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-1']"),
        "clusterActionsClusterOneAddPairElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-0']//ul/li[0]/a"
        ),
        "clusterActionsClusterTwoAddPairElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-1']//ul/li[0]/a"
        ),
        "clusterActionsClusterOneAddPolicyElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-0']//ul/li[1]/a"
        ),
        "clusterActionsClusterTwoAddPolicyElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-1']//ul/li[1]/a"
        ),
        "clusterActionsClusterOneLaunchAmbariElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-0']//ul/li[2]/a"
        ),
        "clusterActionsClusterTwoLaunchAmbariElement": (
            By.XPATH, "//dlm-cluster-actions[@qe-attr='cluster-actions-1']//ul/li[2]/a"
        ),
        "addButtonDropDownCluster": (By.XPATH, "//dlm-add-entity-button//button"),
        "addButtonDropDownClusterPolicy": (By.XPATH, "//dlm-add-entity-button//a[@qe-attr='policy']"),
        "addButtonDropDownClusterPairing": (By.XPATH, "//dlm-add-entity-button//a[@qe-attr='pairing']"),
    }

    def isClustersPage(self, retryCount=2, timeout=3, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getPageTitle(returnLocatorName=True),
            locatorMessage='DLM Clusters Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getPageTitle(self, returnLocatorName=False):
        return self.getElement('pageTitle', returnLocatorName)

    def getStatusColorClusterOneElement(self, returnLocatorName=False):
        return self.getElement('statusColorClusterOneElement', returnLocatorName)

    def getStatusColorClusterOne(self):
        # Cluster loading might take time in few cases
        self.checkElementonPage(
            locatorName=self.getStatusColorClusterOneElement(returnLocatorName=True),
            locatorMessage='DLM Clusters Status Color',
            retryCount=2
        )
        return self.getStatusColorClusterOneElement().value_of_css_property('color')

    def getStatusColorClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('statusColorClusterTwoElement', returnLocatorName)

    def getStatusColorClusterTwo(self):
        return self.getStatusColorClusterTwoElement().value_of_css_property('color')

    def getStatusClusterOneElement(self, returnLocatorName=False):
        return self.getElement('statusClusterOneElement', returnLocatorName)

    def getStatusClusterOne(self):
        return self.getStatusClusterOneElement().text

    def getStatusClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('statusClusterTwoElement', returnLocatorName)

    def getStatusClusterTwo(self):
        return self.getStatusClusterTwoElement().text

    def getClusterDetailsDropDownClusterOne(self, returnLocatorName=False):
        return self.getElement('clusterDetailsDropDownClusterOne', returnLocatorName)

    def clickClusterDetailsDropDownClusterOne(self):
        self.checkElementonPage(
            locatorName=self.getClusterDetailsDropDownClusterOne(returnLocatorName=True),
            locatorMessage='Clusters Details DropDown',
            retryCount=2
        )
        SeleniumDP.click(self.driver, self.getClusterDetailsDropDownClusterOne())

    def getClusterDetailsDropDownClusterOneHdfsBrowser(self, returnLocatorName=False):
        return self.getElement('clusterDetailsDropDownClusterOneHdfsBrowser', returnLocatorName)

    def isClusterDetailsDropDownClusterOneHdfsBrowserDisplayed(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getClusterDetailsDropDownClusterOneHdfsBrowser(returnLocatorName=True),
            locatorMessage='HDFS Browser Displayed',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getClusterDetailsDropDownClusterOneHdfsBrowserFirstDirElement(self, returnLocatorName=False):
        return self.getElement('clusterDetailsDropDownClusterOneHdfsBrowserFirstDirElement', returnLocatorName)

    def getClusterDetailsDropDownClusterOneHdfsBrowserFirstDir(self):
        return self.getClusterDetailsDropDownClusterOneHdfsBrowserFirstDirElement().text

    def getClusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement(self, returnLocatorName=False):
        return self.getElement('clusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement', returnLocatorName)

    def clickClusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement(self):
        # First time loading of HDFS data is slow
        self.checkElementonPage(
            locatorName=self.getClusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement(returnLocatorName=True),
            locatorMessage='HDFS Browser tmp Dir Displayed',
            timeout=10,
            retryCount=5
        )
        SeleniumDP.click(self.driver, self.getClusterDetailsDropDownClusterOneHdfsBrowserTmpDirElement())

    def getClusterDetailsDropDownClusterTwo(self, returnLocatorName=False):
        return self.getElement('clusterDetailsDropDownClusterTwo', returnLocatorName)

    def clickClusterDetailsDropDownClusterTwo(self):
        SeleniumDP.click(self.driver, self.getClusterDetailsDropDownClusterTwo())

    def getTotalNodesCountClusterOneElement(self, returnLocatorName=False):
        return self.getElement('totalNodesCountClusterOneElement', returnLocatorName)

    def getTotalNodesCountClusterOne(self):
        return self.getTotalNodesCountClusterOneElement().text

    def getTotalNodesCountClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('totalNodesCountClusterTwoElement', returnLocatorName)

    def getTotalNodesCountClusterTwo(self):
        return self.getTotalNodesCountClusterTwoElement().text

    def getTotalPairCountClusterOneElement(self, returnLocatorName=False):
        return self.getElement('totalPairCountClusterOneElement', returnLocatorName)

    def getTotalPairCountClusterOne(self):
        return self.getTotalPairCountClusterOneElement().text

    def getTotalPairCountClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('totalPairCountClusterTwoElement', returnLocatorName)

    def getTotalPairCountClusterTwo(self):
        return self.getTotalPairCountClusterTwoElement().text

    def getTotalPoliciesCountClusterOneElement(self, returnLocatorName=False):
        return self.getElement('totalPoliciesCountClusterOneElement', returnLocatorName)

    def getTotalPoliciesCountClusterOne(self):
        return self.getTotalPoliciesCountClusterOneElement().text

    def getTotalPoliciesCountClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('totalPoliciesCountClusterTwoElement', returnLocatorName)

    def getTotalPoliciesCountClusterTwo(self):
        return self.getTotalPoliciesCountClusterTwoElement().text

    def getClusterActionsClusterOneElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterOneElement', returnLocatorName)

    def clickClusterActionsClusterOneElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterOneElement())

    def getClusterActionsClusterTwoElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterTwoElement', returnLocatorName)

    def clickClusterActionsClusterTwoElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterTwoElement())

    def getClusterActionsClusterOneAddPairElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterOneAddPairElement', returnLocatorName)

    def clickClusterActionsClusterOneAddPairElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterOneAddPairElement())

    def getClusterActionsClusterTwoAddPairElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterTwoAddPairElement', returnLocatorName)

    def clickClusterActionsClusterTwoAddPairElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterTwoAddPairElement())

    def getClusterActionsClusterOneAddPolicyElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterOneAddPolicyElement', returnLocatorName)

    def clickClusterActionsClusterOneAddPolicyElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterOneAddPolicyElement())

    def getClusterActionsClusterTwoAddPolicyElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterTwoAddPolicyElement', returnLocatorName)

    def clickClusterActionsClusterTwoAddPolicyElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterTwoAddPolicyElement())

    def getClusterActionsClusterOneLaunchAmbariElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterOneLaunchAmbariElement', returnLocatorName)

    def clickClusterActionsClusterOneLaunchAmbariElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterOneLaunchAmbariElement())

    def getClusterActionsClusterTwoLaunchAmbariElement(self, returnLocatorName=False):
        return self.getElement('clusterActionsClusterTwoLaunchAmbariElement', returnLocatorName)

    def clickClusterActionsClusterTwoLaunchAmbariElement(self):
        SeleniumDP.click(self.driver, self.getClusterActionsClusterTwoLaunchAmbariElement())

    def getAddButtonDropDownCluster(self, returnLocatorName=False):
        return self.getElement('addButtonDropDownCluster', returnLocatorName)

    def clickAddButtonDropDownCluster(self):
        self.checkElementonPage(
            locatorName=self.getAddButtonDropDownCluster(returnLocatorName=True),
            locatorMessage='Add Button DropDown Displayed',
            retryCount=2
        )
        SeleniumDP.click(self.driver, self.getAddButtonDropDownCluster())

    def getAddButtonDropDownClusterPolicy(self, returnLocatorName=False):
        return self.getElement('addButtonDropDownClusterPolicy', returnLocatorName)

    def clickAddButtonDropDownClusterPolicy(self):
        SeleniumDP.click(self.driver, self.getAddButtonDropDownClusterPolicy())

    def getAddButtonDropDownClusterPairing(self, returnLocatorName=False):
        return self.getElement('addButtonDropDownClusterPairing', returnLocatorName)

    def clickAddButtonDropDownClusterPairing(self):
        SeleniumDP.click(self.driver, self.getAddButtonDropDownClusterPairing())
