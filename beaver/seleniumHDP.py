import logging
import time

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class Selenium(object):
    def __init__(self):
        pass

    _REMOTE_WEB_DRIVER_FIREFOX = None
    FIREFOX = "firefox"
    MAC = "MAC"
    LINUX = "LINUX"

    # If _DEBUG == False, you run selenium in the cluster.
    # If _DEBUG == True, you run selenium driver (browser) from your mac.
    #   Make sure you start selenium in mac and set correct _DEBUG_GATEWAY_HOST below.
    #
    _DEBUG = False
    #_DEBUG = True
    _DEBUG_GATEWAY_HOST = "172.22.68.93"  # External IP of gateway host

    @classmethod
    def getDebug(cls):
        return cls._DEBUG

    @classmethod
    def getDebugGatewayHost(cls):
        return cls._DEBUG_GATEWAY_HOST

    @classmethod
    def getPlatform(cls):
        if Selenium.getDebug():
            PLATFORM_TYPE = "MAC"
        else:
            if Machine.isLinux():
                PLATFORM_TYPE = "LINUX"
            else:
                PLATFORM_TYPE = "Windows"
        return PLATFORM_TYPE

    @classmethod
    def getHubPort(cls, logoutput=False):  # pylint: disable=unused-argument
        return 4444

    @classmethod
    def _getRemoteWebDriver(cls, browserType=FIREFOX, platformType=LINUX, browser_profile=None, logoutput=True):
        d = None
        if browserType == Selenium.FIREFOX:
            # dont cache the driver.
            # if Selenium._REMOTE_WEB_DRIVER_FIREFOX is None:
            c = webdriver.DesiredCapabilities.FIREFOX.copy()
            c['platform'] = platformType
            logger.info("browser profile preferences = %s", browser_profile.default_preferences)
            Selenium._REMOTE_WEB_DRIVER_FIREFOX = webdriver.Remote(
                "http://localhost:%s/wd/hub" % cls.getHubPort(), c, browser_profile=browser_profile
            )
        d = Selenium._REMOTE_WEB_DRIVER_FIREFOX
        if logoutput:
            logger.info("Selenium.getRemoteWebDriver returns %s", d)
        return d

    @classmethod
    def getWebDriver(
            cls, browserType=FIREFOX, platformType=LINUX, browser_profile=None, logoutput=True, firefox_binary=None
    ):
        """
        Get WebDriver for given browser.
        :param browserType:
        :param logoutput:
        :return:
        """
        if cls.getDebug():
            return cls._getRemoteWebDriver(
                browserType, platformType, browser_profile=browser_profile, logoutput=logoutput
            )
        else:
            return cls._getLocalWebDriver(browserType, platformType, browser_profile, logoutput, firefox_binary)

    @classmethod
    def _getLocalWebDriver(  # pylint: disable=unused-argument
            cls, browserType=FIREFOX, platformType=LINUX, browser_profile=None, logoutput=True, firefox_binary=None
    ):
        d = None
        c = webdriver.DesiredCapabilities.FIREFOX.copy()
        c['platform'] = platformType
        c['marionette'] = False
        if firefox_binary:
            d = webdriver.Firefox(firefox_profile=browser_profile, firefox_binary=firefox_binary, capabilities=c)
        else:
            d = webdriver.Firefox(firefox_profile=browser_profile, capabilities=c)
        if logoutput:
            logger.info("Selenium._getLocalWebDriver returns %s", d)
        return d

    @classmethod
    def switchToIFrame(cls, driver, iframeNo=0, logoutput=True):  # pylint: disable=unused-argument
        """
        Switch the driver to given iframe number.
        :param driver:
        :param iframeNo:
        :param logoutput:
        :return: None
        """
        driver.switch_to.frame(driver.find_elements_by_tag_name("iframe")[iframeNo])

    @classmethod
    def switchToWindow(cls, driver, newURLFilter, logoutput=True):
        """
        Switch to new window that its URL contains newURLFilter str.
        If there is no such window url that matches, switch back to initial window.
        Note that iframe will be lost after switching.
        :param driver: does change after the call
        :param windowHandleFilter:
        :param logoutput:
        :return: (str, bool) for (initial window handle, switch succeeds or not)
        """
        switchSucceeds = False
        initialWindowHandle = driver.current_window_handle
        for windowHandle in driver.window_handles:
            driver.switch_to.window(windowHandle)
            if logoutput:
                logger.info("switchToWindow switch and check url %s", driver.current_url)
            if driver.current_url.find(newURLFilter) > -1:
                if logoutput:
                    logger.info("switchToWindow switching to matching url = %s", driver.current_url)
                switchSucceeds = True
                break

        if not switchSucceeds:
            driver.switch_to.window(initialWindowHandle)
            if logoutput:
                logger.info(
                    "switchToWindow can't find any matching url. Switching back to url = %s", driver.current_url
                )

        return initialWindowHandle, switchSucceeds

    @classmethod
    def quitWebDriver(cls, driver):
        try:
            logger.info("Quitting the webdriver and closing all associated windows")
            driver.quit()
        except Exception, e:
            logger.error("Error while quitting webdriver: %s", e)
            raise
        else:
            logger.info("webdriver quit successful")

    @classmethod
    def maximizeWebDriverWin(cls, driver):
        driver.maximize_window()

    @classmethod
    def setWebDriverWinSize(cls, driver, width, height):
        driver.set_window_size(width, height)

    @classmethod
    def waitTillElementBecomesVisible(  # pylint: disable=redefined-builtin
            cls, driver, id=None, xpath=None, name=None, class_name=None, link_text=None, maxWait=10, logoutput=True
    ):
        located = True
        if (xpath and (id or name or class_name or link_text)):
            if logoutput:
                logger.warn(
                    "More than one from [xpath, name, id, class_name, link_text] specified in "
                    "waitTillElementBecomesVisible, xpath will take precedence"
                )
        elif (not xpath and (name and (id or class_name or link_text))):
            if logoutput:
                logger.warn(
                    "More than one from [name, id, class_name, link_text] specified in "
                    "waitTillElementBecomesVisible, name will take precedence"
                )
        elif (not xpath and not name and id and (class_name or link_text)):
            if logoutput:
                logger.warn(
                    "More than one from [id, class_name, link_text] specified in "
                    "waitTillElementBecomesVisible, id will take precedence"
                )
        elif (not xpath and not name and not id and class_name and link_text):
            if logoutput:
                logger.warn(
                    "Both class_name and link_text specified in "
                    "waitTillElementBecomesVisible, class_name will take precedence"
                )
        elif (not xpath and not name and not id and not class_name and not link_text):
            if logoutput:
                logger.error(
                    "None of [xpath, name, id, class_name, link_text] provided in waitTillElementBecomesVisible"
                )
            return False

        try:
            if xpath:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.visibility_of_element_located((By.XPATH, xpath))
                )
            elif name:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.visibility_of_element_located((By.NAME, name))
                )
            elif id:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.visibility_of_element_located((By.ID, id))
                )
            elif class_name:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.visibility_of_element_located((By.CLASS_NAME, class_name))
                )
            else:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.visibility_of_element_located((By.LINK_TEXT, link_text))
                )
        except TimeoutException:
            located = False
            if logoutput:
                if xpath:
                    logger.error("Element located by xpath: %s is not visible within %s seconds", xpath, maxWait)
                elif name:
                    logger.error("Element located by name: %s is not visible within %s seconds", name, maxWait)
                elif id:
                    logger.error("Element located by id: %s is not visible within %s seconds", id, maxWait)
                elif class_name:
                    logger.error(
                        "Element located by class_name: %s is not visible within %s seconds", class_name, maxWait
                    )
                else:
                    logger.error(
                        "Element located by link_text: %s is not visible within %s seconds", link_text, maxWait
                    )
        finally:
            if located:
                if logoutput:
                    if xpath:
                        logger.info("Element located by xpath: %s is present and visible", xpath)
                    elif name:
                        logger.info("Element located by name: %s is present and visible", name)
                    elif id:
                        logger.info("Element located by id: %s is present and visible", id)
                    elif class_name:
                        logger.info("Element located by class_name: %s is present and visible", class_name)
                    else:
                        logger.info("Element located by link_text: %s is present and visible", link_text)
        # TODO: This is a bug and needs to be fixed
        # The return statement was inside the finally which would swallow all the exceptions
        return located

    @classmethod
    def waitTillElementDisappears(  # pylint: disable=redefined-builtin
            cls, driver, id=None, xpath=None, name=None, class_name=None, link_text=None, maxWait=10, logoutput=True
    ):
        invisible = True
        if (xpath and (id or name or class_name or link_text)):
            # pylint: disable=line-too-long
            if logoutput:
                logger.warn(
                    "More than one from [xpath, name, id, class_name, link_text] specified in waitTillElementDisappears, xpath will take precedence"
                )
        elif (not xpath and (name and (id or class_name or link_text))):
            if logoutput:
                logger.warn(
                    "More than one from [name, id, class_name, link_text] specified in waitTillElementDisappears, name will take precedence"
                )
        elif (not xpath and not name and id and (class_name or link_text)):
            if logoutput:
                logger.warn(
                    "More than one from [id, class_name, link_text] specified in waitTillElementDisappears, id will take precedence"
                )
        elif (not xpath and not name and not id and class_name and link_text):
            if logoutput:
                logger.warn(
                    "Both class_name and link_text specified in waitTillElementDisappears, class_name will take precedence"
                )
        elif (not xpath and not name and not id and not class_name and not link_text):
            if logoutput:
                logger.error("None of [xpath, name, id, class_name, link_text] specified in waitTillElementDisappears")
            # pylint: enable=line-too-long
            return False

        try:
            if xpath:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.invisibility_of_element_located((By.XPATH, xpath))
                )
            elif name:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.invisibility_of_element_located((By.NAME, name))
                )
            elif id:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.invisibility_of_element_located((By.ID, id))
                )
            elif class_name:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.invisibility_of_element_located((By.CLASS_NAME, class_name))
                )
            else:
                _element = WebDriverWait(driver, maxWait).until(
                    expected_conditions.invisibility_of_element_located((By.LINK_TEXT, link_text))
                )

        except TimeoutException:
            invisible = False
            if logoutput:
                if xpath:
                    logger.error("Element located by xpath: %s is still visible after %s seconds", xpath, maxWait)
                elif name:
                    logger.error("Element located by name: %s is still visible after %s seconds", name, maxWait)
                elif id:
                    logger.error("Element located by id: %s is still visible after %s seconds", id, maxWait)
                elif class_name:
                    logger.error(
                        "Element located by class_name: %s is still visible after %s seconds", class_name, maxWait
                    )
                else:
                    logger.error(
                        "Element located by link_text: %s is still visible after %s seconds", link_text, maxWait
                    )
        finally:
            if invisible:
                if logoutput:
                    if xpath:
                        logger.info("Element located by xpath: %s disappeared", xpath)
                    elif name:
                        logger.info("Element located by name: %s disappeared", name)
                    elif id:
                        logger.info("Element located by id: %s disappeared", id)
                    elif class_name:
                        logger.info("Element located by class_name: %s disappeared", class_name)
                    else:
                        logger.info("Element located by link_text: %s disappeared", link_text)
        # TODO: This is a bug and needs fixing
        # The return was inside the finally block which essentially swallows exceptions
        return invisible

    @classmethod
    def getElement(  # pylint: disable=redefined-builtin
            cls,
            driver,
            id=None,
            xpath=None,
            name=None,
            class_name=None,
            link_text=None,
            logoutput=True
    ):
        element = None
        # pylint: disable=line-too-long
        if (xpath and (id or name or class_name or link_text)):
            if logoutput:
                logger.warn(
                    "More than one from [xpath, name, id, class_name, link_text] specified in getElement, xpath will take precedence"
                )
        elif (not xpath and (name and (id or class_name or link_text))):
            if logoutput:
                logger.warn(
                    "More than one from [name, id, class_name, link_text] specified in getElement, name will take precedence"
                )
        elif (not xpath and not name and id and (class_name or link_text)):
            if logoutput:
                logger.warn(
                    "More than one from [id, class_name, link_text] specified in getElement, id will take precedence"
                )
        elif (not xpath and not name and not id and class_name and link_text):
            if logoutput:
                logger.warn("Both class_name and link_text specified in getElement, class_name will take precedence")
        elif (not xpath and not name and not id and not class_name and not link_text):
            if logoutput:
                logger.error("None of [xpath, name, id, class_name, link_text] specified in getElement")
            return None
        # pylint: enable=line-too-long

        return_first = False
        try:
            if xpath:
                element = driver.find_elements_by_xpath(xpath)
            elif name:
                element = driver.find_elements_by_name(name)
            elif id:
                element = driver.find_elements_by_id(id)
            elif class_name:
                element = driver.find_elements_by_class_name(class_name)
            else:
                element = driver.find_elements_by_link_text(link_text)
        except NoSuchElementException:
            if logoutput:
                if xpath:
                    logger.error("Element located by xpath: %s is not present in DOM", xpath)
                elif name:
                    logger.error("Element located by name: %s is not present in DOM", name)
                elif id:
                    logger.error("Element located by id: %s is not present in DOM", id)
                elif class_name:
                    logger.error("Element located by class_name: %s is not present in DOM", class_name)
                else:
                    logger.error("Element located by link_text: %s is not present in DOM", link_text)
        finally:
            if element:
                if logoutput:
                    if xpath:
                        logger.info("Element located by xpath: %s is present in DOM", xpath)
                    elif name:
                        logger.info("Element located by name: %s is present in DOM", name)
                    elif id:
                        logger.info("Element located by id: %s is present in DOM", id)
                    elif class_name:
                        logger.info("Element located by class_name: %s is present in DOM", class_name)
                    else:
                        logger.info("Element located by link_text: %s is present in DOM", link_text)
                if len(element) == 1:
                    return_first = True
                    # TODO: check this! This is definitely a bug
                    # A return statement in finally would lead to eating up of all exceptions
                    # return element[0]

        if return_first:
            return element[0]
        return element

    @classmethod
    def isElementCurrentlyDisplayed(  # pylint: disable=redefined-builtin
            cls,
            element=None,
            driver=None,
            id=None,
            xpath=None,
            name=None,
            class_name=None,
            link_text=None,
            logoutput=True
    ):
        elem = None
        if element and not driver:
            elem = element
        elif driver and not element:
            elem = cls.getElement(driver, id=id, xpath=xpath, name=name, class_name=class_name, link_text=link_text)
        elif driver and element and (xpath or id or class_name or name or link_text):
            # pylint: disable=line-too-long
            logger.warn(
                "Both element and at least one of [xpath, name, id, class_name, link_text] specified in isElementCurrentlyDisplayed, element will take precedence"
            )
            # pylint: enable=line-too-long
            elem = element
        else:
            logger.error("Neither element nor [xpath, name, id, class_name, link_text] specified")
            return False

        if elem:
            if elem.is_displayed():
                if logoutput:
                    if xpath:
                        logger.info("Element located by xpath: %s is visible", xpath)
                    elif name:
                        logger.info("Element located by name: %s is visible", name)
                    elif id:
                        logger.info("Element located by id: %s is visible", id)
                    elif class_name:
                        logger.info("Element located by class_name: %s is visible", class_name)
                    elif link_text:
                        logger.info("Element located by link_text: %s is visible", link_text)
                    return True
            else:
                if logoutput:
                    if xpath:
                        logger.info("Element located by xpath: %s is not visible", xpath)
                    elif name:
                        logger.info("Element located by name: %s is not visible", name)
                    elif id:
                        logger.info("Element located by id: %s is not visible", id)
                    elif class_name:
                        logger.info("Element located by class_name: %s is not visible", class_name)
                    elif link_text:
                        logger.info("Element located by link_text: %s is not visible", link_text)
                    return False
        else:
            return False
        return None

    @classmethod
    def isElementVisibleInSometime(  # pylint: disable=redefined-builtin
            cls,
            driver,
            id=None,
            xpath=None,
            name=None,
            class_name=None,
            maxWait=10
    ):
        # This method already assumes that driver has done 'get' for the desired URL
        return Selenium.waitTillElementBecomesVisible(
            driver, id=id, xpath=xpath, name=name, class_name=class_name, maxWait=maxWait
        )

    @classmethod
    def isElementDisappearInSometime(  # pylint: disable=redefined-builtin
            cls,
            driver,
            id=None,
            xpath=None,
            name=None,
            class_name=None,
            maxWait=10
    ):
        # This method already assumes that driver has done 'get' for the desired URL
        return Selenium.waitTillElementDisappears(
            driver, id=id, xpath=xpath, name=name, class_name=class_name, maxWait=maxWait
        )

    @classmethod
    def click(cls, driver, clickable):
        #try:
        #    clickable.click()
        #except:
        try:
            driver.execute_script("return arguments[0].click();", clickable)
        except Exception:
            clickable.click()

    # @classmethod
    # def sendKeys(cls, field, key):
    #     if Machine.isHumboldt():
    #         inter_key_time = 2
    #         wait_time = 10
    #         retry = 3
    #     else:
    #         inter_key_time = 1
    #         wait_time = 5
    #         retry = 3
    #
    #     for i in range(retry):
    #         field.clear()
    #         for k in key:
    #             field.send_keys(k)
    #             time.sleep(inter_key_time)
    #     time.sleep(wait_time)

    @classmethod
    def sendKeys(cls, field, key):
        inter_key_time = 0.1
        for i in range(3):  # pylint: disable=unused-variable
            field.clear()
            for k in key:
                field.send_keys(k)
                time.sleep(inter_key_time)

    @classmethod
    def click_on_element_xpath(cls, driver, xpath):
        Selenium.click(driver=driver, clickable=Selenium.getElement(driver, xpath=xpath))
