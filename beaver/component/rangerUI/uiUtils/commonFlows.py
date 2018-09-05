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
import beaver.component.rangerUI.ADMIN_PROPERTIES as adminProps
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class CommonFlows(object):

    global basePageObj, loginPage, landingPage, cmnPolicyLandingPage, commonPolicyPg, auditPg, accessAuditPg, tagLandingPg, driver

    def __init__(
            self,
            basePageObj,
            loginPage,
            landingPage,
            cmnPolicyLandingPage=None,
            commonPolicyPg=None,
            auditPg=None,
            accessAuditPg=None,
            tagLandingPg=None
    ):
        self.basePageObj = basePageObj
        self.loginPage = loginPage
        self.landingPage = landingPage
        self.cmnPolicyLandingPage = cmnPolicyLandingPage if cmnPolicyLandingPage is not None else None
        self.commonPolicyPg = commonPolicyPg if commonPolicyPg is not None else None
        self.auditPg = auditPg if auditPg is not None else None
        self.accessAuditPg = accessAuditPg if accessAuditPg is not None else None
        self.tagLandingPg = tagLandingPg if tagLandingPg is not None else None
        self.driver = basePageObj.driver

    @TaskReporter.report_test()
    def goToLoginPage(self):
        # Try Loading the basePage
        self.basePageObj.goToBasePage()
        if not self.loginPage.isLoginPage(timeout=10):
            logger.info("Not login page")

    @TaskReporter.report_test()
    def loginToRangerAdmin(self):
        self.goToLoginPage()
        self.loginPage.doLogin(adminProps.USER_NAME, adminProps.PASSWORD)
        self.basePageObj.waitForElement(self.landingPage, self.landingPage.getAddHdfsRepoBtn(returnLocatorName=True))

    @TaskReporter.report_test()
    def gotoPolicyLandingPageFromLanding(self, policyType):
        self.landingPage.clickToGoToPolicyLandingPage(policyType)
        self.basePageObj.waitForElement(
            self.cmnPolicyLandingPage, self.cmnPolicyLandingPage.getAddPolicyBtn(returnLocatorName=True), timeout=10
        )
        if not self.cmnPolicyLandingPage.checkIsPolicyLandingPg(timeout=10):
            logger.info("Not PolicyLandingPage")

    @TaskReporter.report_test()
    def gotoNewPolicyPageAndVerify(self):
        self.cmnPolicyLandingPage.clickOnAddNewPolicyBtn()
        self.basePageObj.waitForElement(
            self.commonPolicyPg, self.commonPolicyPg.getPolicyDetailsSubHeading(returnLocatorName=True), timeout=10
        )
        if not self.commonPolicyPg.checkIsPolicyPage(timeout=10):
            logger.info("Not New policy page")

    @TaskReporter.report_test()
    def gotoAuditPageFromLandingPageAndVerify(self):
        self.landingPage.clickToGotoAuditPage()
        #self.basePageObj.waitForElement(self.auditPg, self.auditPg.getAccessAuditTabLink(returnLocatorName=True))
        if not self.auditPg.checkIsAuditPage(timeout=10):
            logger.info("Not Audit Page")

    @TaskReporter.report_test()
    def gotoAccessAuditPgFromLandingPgAndVerify(self):
        self.gotoAuditPageFromLandingPageAndVerify()
        self.auditPg.clickToGotoAccessAuditPage(self.driver)
        if not self.accessAuditPg.checkIsAccessAuditPage(timeout=10):
            logger.info("Not Access Audit Page")

    @TaskReporter.report_test()
    def gotoTagLandingPgAndVerify(self):
        self.landingPage.clickToGotoTagLandingPage()
        if not self.tagLandingPg.checkIsTagLandingPage(timeout=10):  #->tagLandingPg, checkIsTagLandingPage
            logger.info("Not Tag Landing Page")
