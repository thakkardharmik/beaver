import logging
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.yarnUI.basePage import BasePage
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class SchedulerPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    locator_dictionary = {
        "appQueues": (By.XPATH, '//*[@id="cs-wrapper"]/div[1]'),
        "gridengQueue": (By.XPATH, '//*[@id="pq"]/li[2]/ins'),
        "defaultQueue": (By.XPATH, '//*[@id="pq"]/li[1]/ins'),
        "appTable": (By.XPATH, '//*[@id="apps_wrapper"]')
    }

    def isSchedulerPage(self, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getAppQueueTitle(returnLocatorName=True),
            locatorMessage='Scheduler Page',
            retryCount=retryCount,
            timeout=timeout,
            restartWebDriver=restartWebDriver,
            quitWebdriver=quitWebdriver
        )

    def getAppQueueTitle(self, returnLocatorName=False):
        return self.getElement('appQueues', returnLocatorName)

    @TaskReporter.report_test()
    def clickAlreadyCollapsedQueue(self, queueName, returnLocatorName=False, queues=2):
        for n in range(1, queues + 1):
            self.driver.find_element(By.XPATH, '//*[@id="pq"]/li[%s]/ins' % n).click()
            if self.getQueueName() == queueName:
                return
            else:
                self.driver.find_element(By.XPATH, '//*[@id="pq"]/li[%s]/ins' % n).click()

    def clickAlreadyCollapsedDefaultQueue(self, returnLocatorName=False):
        return self.getElement('defaultQueue', returnLocatorName).click()

    def clickAlreadyCollapsedGridengQueue(self, returnLocatorName=False):
        return self.getElement('gridengQueue', returnLocatorName).click()

    @TaskReporter.report_test()
    def getQueueUserInfo(self, searchElement):
        tableList = self.driver.find_elements_by_xpath('//*[@id="lq"]')
        table = tableList[1]
        colNum = 0
        found = False

        rows = [x for x in table.find_elements_by_tag_name("tr")]
        for row in rows:
            heads = [x for x in row.find_elements_by_tag_name("th")]
            for head in heads:
                colNum = colNum + 1
                if head.text == searchElement:
                    logger.info("table header-%s" % head.text)
                    found = True
                    break
            dataNum = 0
            cols = [x for x in row.find_elements_by_tag_name("td")]
            for col in cols:
                dataNum = dataNum + 1
                if found == True and colNum == dataNum:
                    logger.info("table data-%s" % col.text)
                    return col.text

    @TaskReporter.report_test()
    def getQueueInfo(self, searchElement):
        tableList = self.driver.find_elements_by_xpath('//*[@id="lq"]')

        for table in tableList:
            found = False
            rows = [x for x in table.find_elements_by_tag_name("tr")]
            for row in rows:
                heads = [x for x in row.find_elements_by_tag_name("th")]
                for head in heads:
                    if head.text == searchElement:
                        logger.info("table header-%s" % head.text)
                        found = True

                cols = [x for x in row.find_elements_by_tag_name("td")]
                for col in cols:
                    if found == True:
                        logger.info("table data-%s" % col.text)
                        return col.text

    @TaskReporter.report_test()
    def getQueueName(self):
        tableList = self.driver.find_elements_by_xpath('//*[@id="lq"]')

        for table in tableList:
            found = False
            rows = [x for x in table.find_elements_by_tag_name("tr")]
            for row in rows:
                heads = [x for x in row.find_elements_by_tag_name("th")]
                for head in heads:
                    if head.text == "'grideng' Queue Status":
                        return 'grideng'
                    elif head.text == "'default' Queue Status":
                        return 'default'
