import logging
import os
import time
from beaver.machine import Machine
from taskreporter.taskreporter import TaskReporter
try:
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import *
    from selenium.common.exceptions import NoSuchElementException
    from selenium.webdriver.common.by import By
    from selenium import webdriver
    from beaver.seleniumHDP import Selenium
except Exception:
    logger.info("Selenium should be installed")
logger = logging.getLogger(__name__)


class BasePage(object):

    driver = None
    DEFAULT_TIMEOUT = 3
    __timeout = DEFAULT_TIMEOUT

    def __init__(self, driver=None):
        if driver:
            self.driver = driver
        else:
            self.driver = self.instantiateWebdriver()
            assert self.driver, "Could not initialize selenium webdriver"

    def getBaseUrl(self):
        from beaver.component.hadoop import YARN
        return YARN.getRMUrl()

    def getCurrentURL(self):
        return self.driver.current_url

    def deleteSession(self):
        self.quitWebdriver()

    def goToBasePage(self):
        self.driver.get(self.getBaseUrl())

    def refreshPage(self):
        self.driver.refresh()

    def getElement(self, locatorName, returnLocatorName=False):
        return locatorName if returnLocatorName else self.findElement(*self.locator_dictionary[locatorName])

    @TaskReporter.report_test()
    def getRowFromTable(self, locatorName, searchElement):
        table = self.findElement(*self.locator_dictionary[locatorName])
        rows = [x for x in table.find_elements_by_tag_name("tr")]
        self.appTableEntries.clear()
        for row in rows:
            cols = [x for x in row.find_elements_by_tag_name("td")]
            found = ["found" for x in cols if x.text == searchElement]
            if found == ["found"]:
                n = 0
                for col in cols:
                    self.appTableEntries.setdefault(self.appTableKey[n], []).append(col.text)
                    n = n + 1
        return self.appTableEntries

    @TaskReporter.report_test()
    def findElement(self, *loc):
        try:
            return self.driver.find_element(*loc)
        except (NoSuchElementException):
            logger.error("Element not found")
            logger.error(loc)
            return loc

    @TaskReporter.report_test()
    def quitWebdriver(self):
        try:
            Selenium.quitWebDriver(self.driver)
        except Exception, e:
            self.driver = None
            logger.warn("Ignoring webdriver quit failure")
            pass

    @TaskReporter.report_test()
    def restartWebdriver(self):
        self.quitWebdriver()
        self.driver = self.instantiateWebdriver()
        self.setTimeout(self.DEFAULT_TIMEOUT)

    @TaskReporter.report_test()
    def getTimeout(self):
        if not self.__timeout:
            self.__timeout = self.DEFAULT_TIMEOUT
        return self.__timeout

    @TaskReporter.report_test()
    def setTimeout(self, timeout=None):
        if not timeout:
            self.__timeout = self.DEFAULT_TIMEOUT
        else:
            self.__timeout = timeout

    def __getattr__(self, locatorName):
        try:
            if locatorName in self.locator_dictionary.keys():
                try:
                    element = WebDriverWait(self.driver, self.getTimeout()).until(
                        EC.presence_of_element_located(self.locator_dictionary[locatorName])
                    )
                except (TimeoutException, StaleElementReferenceException):
                    logger.error("Element %s not found on Page" % locatorName)
                    return None

                try:
                    element = WebDriverWait(self.driver, self.getTimeout()).until(
                        EC.visibility_of_element_located(self.locator_dictionary[locatorName])
                    )
                except (TimeoutException, StaleElementReferenceException):
                    logger.error("Element %s not visible on Page" % locatorName)
                    return None
                return self.findElement(*self.locator_dictionary[locatorName])
        except AttributeError:
            super(BasePage, self).__getattribute__("methodMissing")(locatorName)

    @TaskReporter.report_test()
    def waitForElement(self, locatorName, timeout=None):
        self.setTimeout(timeout)
        try:
            WebDriverWait(self.driver, self.getTimeout()).until(
                EC.visibility_of_element_located(self.locator_dictionary[locatorName])
            )
        except (TimeoutException, StaleElementReferenceException):
            logger.error("Element %s not visible on Page" % locatorName)
            return False
        return True

    def methodMissing(self, locatorName):
        print "No %s here!" % locatorName

    @TaskReporter.report_test()
    def instantiateWebdriver(self):
        os.environ['DISPLAY'] = ":99"
        Machine.runas(Machine.getAdminUser(), "dbus-uuidgen --ensure")
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.privatebrowsing.autostart", True)
        profile.set_preference("network.http.phishy-userpass-length", 255)
        profile.set_preference("network.automatic-ntlm-auth.trusted-uris", "x.x.x.x")
        profile.accept_untrusted_certs = True
        driver = None
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", "/tmp")
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip,application/octet-stream")
        profile.set_preference("browser.helperApps.alwaysAsk.force", False)
        # QE-4343
        profile.set_preference("dom.max_script_run_time", 0)
        from beaver.component.hadoop import Hadoop
        if Hadoop.isUIKerberozied() and Machine.isHumboldt():
            profile.set_preference("network.negotiate-auth.delegation-uris", ".hdinsight.net")
            profile.set_preference("network.negotiate-auth.trusted-uris", ".hdinsight.net")

        def restartXvfb():
            selenium_process_list = Machine.getProcessList(filter='selenium-server')
            selenium_pids = [int(p.split()[1]) for p in selenium_process_list]
            selenium_cmds = [' '.join(p.split()[3:]) for p in selenium_process_list]
            selenium_hub_cmd = None
            selenium_wd_cmd = None
            for cmd in selenium_cmds:
                if "role hub" in cmd:
                    selenium_hub_cmd = cmd + "  > /tmp/selenium-hub.log 2>&1 &"
                if "role webdriver" in cmd:
                    selenium_wd_cmd = cmd + " > /tmp/selenium-node.log 2>&1 &"
            assert selenium_hub_cmd and selenium_wd_cmd, "Failed to find selenium-server processes and restart them"
            for pid in selenium_pids:
                Machine.killProcessRemote(pid, host=None, user=Machine.getAdminUser(), passwd=None, logoutput=True)
            xvfb_pid = [int(p.split()[1]) for p in Machine.getProcessList(filter='Xvfb')]
            for pid in xvfb_pid:
                Machine.killProcessRemote(pid, host=None, user=Machine.getAdminUser(), passwd=None, logoutput=True)
            Machine.rm(Machine.getAdminUser(), None, "/tmp/.X99-lock", isdir=False, passwd=None)
            Machine.runas(Machine.getAdminUser(), selenium_hub_cmd, host=None)
            Machine.runas(Machine.getAdminUser(), selenium_wd_cmd, host=None)
            Machine.runinbackgroundAs(Machine.getAdminUser(), cmd="Xvfb :99 -ac -screen 0 1280x1024x24", host=None)
            time.sleep(10)

        num_attempts = 0
        max_attempts = 5
        while num_attempts < max_attempts:
            try:
                driver = Selenium.getWebDriver(browserType='firefox', platformType='LINUX', browser_profile=profile)
                Selenium.setWebDriverWinSize(driver, 1920, 1080)
                from beaver.component.hadoop import Hadoop
                if Hadoop.isUIKerberozied() and Machine.isHumboldt():
                    keytabFile = Machine.getHeadlessUserKeytab("hrt_qa")
                    kinitCmd = '%s -R -kt %s %s' % (
                        Machine.getKinitCmd(), keytabFile, Machine.get_user_principal("hrt_qa")
                    )
                    exitCode, stdout = Machine.runas("hrt_qa", kinitCmd)
                    assert exitCode == 0
                driver.get(self.getBaseUrl())
                break
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    restartXvfb()
                    pass
                else:
                    logger.error("attempt : %s , Failed to get webdriver for Dataplane : %s" % (num_attempts, e))
                num_attempts = num_attempts + 1
        return driver

    @TaskReporter.report_test()
    def checkElementonPage(
            self, locatorName, locatorMessage, retryCount=0, timeout=None, restartWebDriver=False, quitWebdriver=False
    ):
        self.setTimeout(timeout)
        if self.__getattr__(locatorName):
            logger.info("%s Check is Successful !" % locatorMessage)
            return True
        else:
            if retryCount > 0:
                if restartWebDriver:
                    logger.info("Restarting webdriver..")
                    self.restartWebdriver()
                self.checkElementonPage(
                    locatorName, locatorMessage, retryCount - 1, timeout, restartWebDriver, quitWebdriver
                )
            else:
                logger.error("%s Check is NOT Successful !" % locatorMessage)
                if quitWebdriver:
                    logger.error("Exiting Webdriver, as Page Check was not Successful...")
                    self.quitWebdriver()
                    exit(1)
            return False
