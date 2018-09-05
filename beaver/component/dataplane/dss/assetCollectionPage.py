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


class AssetCollectionPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "addAssetCollectionButton": (By.XPATH, '//div[@class="addDatasetButtonCont"]/button'),
    }

    def isAssetCollectionPage(self, retryCount=10, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getAddAssetCollectionButton(returnLocatorName=True),
            locatorMessage=' Add Asset Collection Button ',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getAddAssetCollectionButton(self, returnLocatorName=False):
        return self.getElement('addAssetCollectionButton', returnLocatorName)
