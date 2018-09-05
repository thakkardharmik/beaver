from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from beaver.component.hadoop import HDFS, YARN
from taskreporter.taskreporter import TaskReporter

import logging
import os
import time, datetime
from selenium import webdriver

from beaver.machine import Machine
from beaver.config import Config
from beaver.seleniumHDP import Selenium
import beaver.component.hdfsUI.commonUtils as commonUtils
from beaver.component.hdfsUI.knoxLoginPage import KnoxLoginPage

logger = logging.getLogger(__name__)

USER_NAME = Config.get('hadoop', 'HADOOPQA_USER')
PASSWORD = "Horton!#works"


class BasePage(object):
    KNOX_HOST = Config.get('knox', 'KNOX_HOST').split(',')[0]
    KNOX_PORT = '8443'
    PROTOCOL = "https://"
    TOPOLOGY = "ui"
    if HDFS.isFederated():
        TOPOLOGY = "ui_ns1"
    URL_APPEND = "/gateway/%s/yarnuiv2/" % TOPOLOGY
    PROXY_URL = PROTOCOL + KNOX_HOST + ":" + KNOX_PORT + URL_APPEND

    driver = None
    DEFAULT_TIMEOUT = 10
    proxy = 'true'
    __timeout = DEFAULT_TIMEOUT

    def __init__(self, driver=None, proxy='true'):
        self.proxy = proxy
        if driver:
            self.driver = driver
        else:
            self.driver = self.instantiateWebdriver()

            assert self.driver, "Could not initialize selenium webdriver"

    @TaskReporter.report_test()
    def getBaseUrl(self):
        if self.proxy == 'true' and YARN.isKnoxProxySet():
            return self.PROXY_URL
        else:
            return YARN.getRMUrl() + "/ui2/#/cluster-overview"

    def loginToknoxSSO(self):
        loginPage = KnoxLoginPage(self)
        assert loginPage.isLoginPage()

        # Also assert that URL contains SSO Login URL
        currentURL = loginPage.getCurrentURL()
        assert commonUtils.getSSOLoginURL() in currentURL

        # Do Login
        loginPage.doLogin(USER_NAME, PASSWORD)

    def getCurrentURL(self):
        return self.driver.current_url

    @TaskReporter.report_test()
    def checkIfCurrentURLIsProxyURL(self):

        url = self.getCurrentURL()
        if not YARN.isKnoxProxySet() and self.PROXY_URL not in url:
            return True
        if self.PROXY_URL in url:
            return True

        return False

    def deleteSession(self):
        self.quitWebdriver()

    def goToBasePage(self):
        self.driver.get(self.getBaseUrl())

    def navigateToPage(self, url):
        self.driver.get(url)

    def refreshCurrentPage(self):
        self.driver.get(self.driver.current_url)

    def getElement(self, locatorName, returnLocatorName=False):
        return locatorName if returnLocatorName else self.findElement(*self.locator_dictionary[locatorName])

    def getElements(self, locatorName, returnLocatorName=False):
        """
        Returns all the occurences of a matching pattern available on the web page
        """
        return locatorName if returnLocatorName else self.findElements(*self.locator_dictionary[locatorName])

    @TaskReporter.report_test()
    def findElements(self, *loc):
        try:
            return self.driver.find_elements(*loc)
        except Exception as e:
            logger.error("Caught exception: %s" % e)
            return loc

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

    @TaskReporter.report_test()
    def waitForElementInvisibility(self, locatorName, timeout=None):
        self.setTimeout(timeout)
        try:
            WebDriverWait(self.driver, self.getTimeout()).until(
                EC.invisibility_of_element_located(self.locator_dictionary[locatorName])
            )
        except (TimeoutException, StaleElementReferenceException):
            logger.error("Element %s still visible on Page" % locatorName)
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
        if Hadoop.isUIKerberozied() and self.proxy == 'false':
            domainName = ".hwx.site"
            if Machine.isIBMPowerPC():
                domainName = ".openstacklocal"
            profile.set_preference("network.negotiate-auth.delegation-uris", domainName)
            profile.set_preference("network.negotiate-auth.trusted-uris", domainName)

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
                os.environ["http_proxy"] = ''
                #firefox_binary = FirefoxBinary('/base/tools/firefox-45.0/firefox')
                #driver = Selenium.getWebDriver(browserType='firefox', platformType='LINUX', browser_profile=profile, firefox_binary= firefox_binary)
                driver = Selenium.getWebDriver(browserType='firefox', platformType='LINUX', browser_profile=profile)
                # Adding an implicit wait so that the tests wait for some
                # time (30- seconds) before finding elements on web page
                # JIRA: https://hortonworks.jira.com/browse/QE-15773
                driver.implicitly_wait(30)
                Selenium.setWebDriverWinSize(driver, 1920, 1080)
                driver.get(self.getBaseUrl())
                break
            except Exception, e:
                logger.error("Exception is: %s" % e)
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
                return self.checkElementonPage(
                    locatorName, locatorMessage, retryCount - 1, timeout, restartWebDriver, quitWebdriver
                )
            else:
                logger.error("%s Check is NOT Successful !" % locatorMessage)
                if quitWebdriver:
                    logger.error("Exiting Webdriver, as Page Check was not Successful...")
                    self.quitWebdriver()
                    exit(1)
            return False

    @TaskReporter.report_test()
    def take_screenshot(self, test_name):
        try:
            ts = time.time()
            currentTime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H-%M-%S')
            # Adding epoch time to get unique screenshot names as its observed that
            # screenshots taken in the same second are replaced
            epochTime = str(time.time() * 1000)
            filename = "%s-%s-%s.png" % (test_name, currentTime, epochTime)
            logger.info("------ capturing screenshot to file: %s" % (filename))
            self.driver.get_screenshot_as_file(os.path.join(Config.getEnv('ARTIFACTS_DIR'), filename))
        except Exception as e:
            logger.error("%s" % e)

    @TaskReporter.report_test()
    def validateSortingOrder(self, inputList, orderType='ascending'):
        """
        A general function that accepts a list as input parameter and
        validates if the list is sorted properly as per orderType
        """
        logger.info("Input List: %s, Order Type: %s" % (inputList, orderType))
        sortedList = []
        if orderType == 'ascending':
            if isinstance(inputList[0], int):
                sortedList = sorted(inputList)
            else:
                # The current sorting order implemented on the cluster table
                # is case insensitive, hence support for the same
                sortedList = sorted(inputList, key=lambda s: s.lower())
        elif orderType == 'descending':
            if isinstance(inputList[0], int):
                sortedList = sorted(inputList, reverse=True)
            else:
                sortedList = sorted(inputList, reverse=True, key=lambda s: s.lower())
        # Let's validate the input list and sorted list now
        if inputList == sortedList:
            return True
        else:
            logger.info("Mismatch Noticed, Input: %s and sorted: %s" % (inputList, sortedList))
            return False
