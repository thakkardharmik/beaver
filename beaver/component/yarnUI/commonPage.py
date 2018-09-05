import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUI.basePage import BasePage

logger = logging.getLogger(__name__)


class CommonPage(BasePage):
    locator_dictionary = {
        "scheduler": (By.XPATH, '//*[@id="ui-accordion-nav-panel-0"]/li[5]/a'),
        "application": (By.XPATH, '//*[@id="ui-accordion-nav-panel-0"]/li[4]/a'),
    }

    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    def clickOnScheduler(self, returnLocatorName=False):
        return self.getElement('scheduler', returnLocatorName).click()

    def clickOnAllApplication(self, returnLocatorName=False):
        return self.getElement('application', returnLocatorName).click()
