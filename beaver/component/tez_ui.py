#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, collections, urllib
import selenium
import traceback
from beaver.machine import Machine
from beaver.config import Config
from selenium import webdriver
from beaver.seleniumHDP import Selenium
from beaver.component.hadoop import Hadoop, MAPRED, YARN, HDFS
from beaver.component.tez import Tez
from beaver.java import Java
from beaver import util
from beaver import configUtils
from beaver.component.dataStructure.yarnContainerLogs import YarnContainerLogs
from selenium.webdriver.common.by import By
from decimal import *
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
logger = logging.getLogger(__name__)


class TezUI:
    """
    A class to work with Tez UI
    """

    # cached driver
    _driver = None

    _IMPLICIT_WAIT_SEC = 10

    _SCREENSHOT_NUM = 0
    _FIREFOX_LOG_NUM = 0

    _ARTIFACTS_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "tezui")

    @classmethod
    def getAmbariVersion(cls, logoutput=False):
        """
        Get Ambari Version
        :param logoutput:
        :return: (str, str)
                 (2.1.0, 1026)
        """
        if Machine.isWindows():
            return (None, None)
        exit_code, ambariVersion = Machine.run('rpm -qa | grep ambari-server')
        if Machine.isDebian() or Machine.isUbuntu():
            exit_code, ambariVersion = Machine.run('dpkg -l | grep ambari-server')
            # ambariVersion = ii  ambari-server                              2.1.1-144                               amd64        Ambari Server
            tmp = ambariVersion.split()[2]
            ambariVer = tmp.split("-")[0]
            ambariBuildNo = tmp.split("-")[-1]
        else:
            # ambariVersion = ambari-server-2.1.0-1026.x86_64 or ambari-server-2.1.0-1026
            ambariVer = ambariVersion.split("-")[2]
            tmp = ambariVersion.split("-")[3]
            if logoutput:
                logger.info("TezUI.getStaticInstanceURL tmp = %s" % tmp)
            if tmp.find(".") > -1:
                ambariBuildNo = tmp[0:tmp.find(".")]
            else:
                ambariBuildNo = tmp
        return (ambariVer, ambariBuildNo)

    @classmethod
    def isAmbari21OrAbove(cls, logoutput=True):
        """
        Return true if Ambari version is 2.1.0+
        Static view is supported from the version.
        :param ambariVer:
        :return: bool
        """
        (ambariVer, _) = cls.getAmbariVersion(logoutput=logoutput)
        if ambariVer == None:
            return None
        result = False
        tokens = ambariVer.split(".")
        if int(tokens[0]) < 2:
            result = False
        else:
            if int(tokens[1]) >= 1:
                result = True
            else:
                result = False
        if logoutput:
            logger.info("isAmbari21OrAbove returns %s" % result)
        return result

    @classmethod
    def isAmbari212orAbove(cls, logoutput=True):
        (ambariVer, _) = cls.getAmbariVersion(logoutput=logoutput)
        if ambariVer == None:
            return None
        result = False
        tokens = ambariVer.split(".")
        if int(tokens[0]) < 2:
            result = False
        else:
            if int(tokens[1]) >= 1:
                if int(tokens[1]) == 1 and int(tokens[2]) >= 2:
                    result = True
                if int(tokens[1]) == 1 and int(tokens[2]) < 2:
                    result = False
                if int(tokens[1]) > 1:
                    result = True
            else:
                result = False
        if logoutput:
            logger.info("isAmbari212orAbove returns %s" % result)
        return result

    @classmethod
    def getAmbariTezViewVersion(cls, user=None):
        '''
        Obtain version for Tez View in Ambari UI
        '''
        if user is None:
            user = "admin"
        logger.info("use user = %s" % user)
        if Machine.isHumboldt():
            password = "HdpCli123!"
        elif Machine.isCloud():
            password = "cloudbreak1"
        else:
            password = user

        url = "%s/api/v1/views/TEZ" % cls.getAmbariUrl()
        curl_cmd = 'curl -k -u %s:%s  -H "X-Requested-By: ambari" %s' % (user, password, url)
        exitCode, stdout = Machine.run(curl_cmd)
        version = re.search("\"version\" : \"(.*)\",", stdout)
        return version.group(1)

    @classmethod
    def getStandaloneUI(cls):
        """
        Get Tez Standalone UI
        """
        addr = YARN.get_ats_web_app_address() + "/tezui"
        return addr

    @classmethod
    def setupStandaloneUI(cls, destdir="/tmp/mytest/"):
        """
        setup Tez Standalone UI
        """
        timelinehost = YARN.getATSHost()
        warfile = Machine.find(
            user=Machine.getAdminUser(),
            host=timelinehost,
            filepath=Config.get('tez', 'TEZ_HOME'),
            searchstr="tez-ui-*.war",
            passwd=Machine.getAdminPasswd(),
            logoutput=False
        )[0]
        logger.info(timelinehost)
        logger.info(warfile)
        Machine.extractWarfile(warfile, destdir=destdir, host=timelinehost)
        configs_env_file = os.path.join(destdir, 'config', 'configs.env')
        local_configs_env_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'configs.env')
        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=timelinehost,
            srcpath=configs_env_file,
            destpath=local_configs_env_file,
            passwd=Machine.getAdminPasswd()
        )
        propChanges = {}
        propChanges['timeline'] = YARN.get_ats_web_app_address()
        propChanges['rm'] = YARN.getResourceManagerWebappAddress()
        cmd = "chmod 777 %s" % local_configs_env_file
        Machine.runas(user=Machine.getAdminUser(), cmd=cmd, passwd=Machine.getAdminPasswd())
        util.writeToFile(
            "ENV = { \n hosts: { \n timeline: \"%s\",\n rm: \"%s\" }\n};" %
            (YARN.get_ats_web_app_address(), YARN.getResourceManagerWebappAddress()), local_configs_env_file
        )
        Machine.copyFromLocal(
            Machine.getAdminUser(),
            timelinehost,
            local_configs_env_file,
            configs_env_file,
            passwd=Machine.getAdminPasswd()
        )
        confProperties = {
            "yarn-site.xml": {
                "yarn.timeline-service.ui-names": "tez",
                "yarn.timeline-service.ui-on-disk-path.tez": destdir,
                "yarn.timeline-service.ui-web-path.tez": "/tezui",
                "yarn.timeline-service.http-cross-origin.enabled": "true",
                'yarn.timeline-service.entity-group-fs-store.scan-interval-seconds': '10',
                'yarn.resourcemanager.webapp.cross-origin.enabled': 'true'
            },
            "core-site.xml": {
                "hadoop.http.cross-origin.enabled": "true",
                "hadoop.http.cross-origin.allowed-origins": "*"
            }
        }
        Hadoop.modifyConfig(
            confProperties, {'services': ['jobtracker', 'timelineserver']}, makeCurrConfBackupInWindows=True
        )
        Tez.modifyConfig({"tez-site.xml": {"tez.tez-ui.history-url.base": cls.getStandaloneUI()}})
        mod_conf_path = Hadoop.getModifiedConfigPath()
        YARN.restartResourceManager(mod_conf_path)
        YARN.restartATSServer(config=mod_conf_path, wait=5, atsHost=timelinehost)

    @classmethod
    def teardownStandaloneUI(cls, destdir="/tmp/mytest/"):
        """
        Teardown the setup for Tez standalone UI
        """
        timelinehost = YARN.getATSHost()
        Machine.rm(Machine.getAdminUser(), timelinehost, destdir, isdir=True, passwd=Machine.getAdminPasswd())
        Hadoop.restoreConfig(["yarn-site.xml"], {'services': ['jobtracker', 'timelineserver']})
        YARN.restartResourceManager()
        YARN.restartATSServer(atsHost=timelinehost)

    @classmethod
    def getStaticInstanceURL(cls, logoutput=False, useAmbariUrl=True):
        """
        Get static instance URL. Make sure this returns correct output or ambari will not load tez ui with about:blank.
        :param logoutput:
        :return: str
        """
        logger.info("getStaticInstance Url, use AmbariUrl = %s" % useAmbariUrl)
        version = cls.getAmbariTezViewVersion()
        if logoutput:
            logger.info("TezUI.getStaticInstanceURL version = %s" % version)
        if useAmbariUrl:
            dags = "?viewPath=%2F%23%2Fdags"
            url = "%s/#/main/view/TEZ/tez_cluster_instance%s" % (cls.getAmbariUrl(), dags)
        else:
            url = cls.getStandaloneUI()
            #url = "http://172.22.65.97:8188/tezui/"
        if logoutput:
            logger.info("TezUI.getStaticInstanceURL returns %s" % url)
        return url

    @classmethod
    def getAmbariServerPrincipalName(cls, logoutput=False):
        return "ambari-server"

    @classmethod
    def getNextSsNum(cls, logoutput=False):
        """
        Get next screenshot number
        :param logoutput:
        :return: int
        """
        cls._SCREENSHOT_NUM += 1
        return cls._SCREENSHOT_NUM

    @classmethod
    def getNextSsFilename(cls, logoutput=False):
        """
        Get next screenshot file path
        :param logoutput:
        :return: str
        """
        ssDir = cls.getArtifactsDir()
        if not os.path.exists(ssDir):
            os.mkdir(ssDir)
        ssFilename = os.path.join(ssDir, "%s.png" % cls.getNextSsNum())
        return ssFilename

    @classmethod
    def getNextFirefoxLogNum(cls, logoutput=False):
        """
        Get next firefox log number
        :param logoutput:
        :return: int
        """
        cls._FIREFOX_LOG_NUM += 1
        return cls._FIREFOX_LOG_NUM

    @classmethod
    def getNextFirefoxLogFilepath(cls, logoutput=False):
        """
        Get next firefox log file path
        :param logoutput:
        :return: str
        """
        tmpDir = cls.getArtifactsDir()
        if not os.path.exists(tmpDir):
            os.mkdir(tmpDir)
        filename = os.path.join(tmpDir, "firefox_log_%s.txt" % cls.getNextFirefoxLogNum(logoutput))
        return filename

    @classmethod
    def getUIPropertiesFile(cls, logoutput=False):
        """
        Get UI Properties file for Tez UI test. For debug-ability, we want to keep the file over test sessions hence using /tmp.
        :param logoutput:
        :return: str
        """
        #return os.path.join(Config.getEnv('ARTIFACTS_DIR'), "tez_ui.properties")
        return os.path.join(Machine.getTempDir(), "tez_ui.properties")

    @classmethod
    def getUIPropertyKeyInstanceUrl(cls, logoutput=False):
        """
        Get instance URL for Ambari View
        :param logoutput:
        :return: str
        """
        return "instanceUrl"

    @classmethod
    def writeInstanceUrl(cls, instanceUrl, logoutput=False):
        """
        Write instance URL to properties file.
        :param instanceUrl:
        :param logoutput:
        :return: None
        """
        logger.info("writing properties file to %s" % cls.getUIPropertiesFile())
        logger.info("instanceUrl = %s" % instanceUrl)
        util.writePropertiesToFile(None, cls.getUIPropertiesFile(), {cls.getUIPropertyKeyInstanceUrl(): instanceUrl})

    @classmethod
    def readInstanceUrl(cls, logoutput=False, useAmbariUrl=True):
        """
        Read instance URL from properties file
        :param logoutput:
        :return: str or None
        """
        if not useAmbariUrl:
            logger.info("returning new readInstanceUrl")
            return cls.getStandaloneUI()
        path = cls.getUIPropertiesFile()
        logger.info("reading properties file from %s" % path)
        if not os.path.exists(path):
            instanceUrl = None
        else:
            instanceUrl = util.getPropertyValueFromFile(path, TezUI.getUIPropertyKeyInstanceUrl())
            logger.info("instanceUrl = %s" % instanceUrl)
            if Selenium.getDebug():
                instanceUrl = instanceUrl.replace(HDFS.getGateway(), cls.getAmbariHost())
                logger.info("instanceUrl = %s" % instanceUrl)
        return instanceUrl

    @classmethod
    def getAmbariHost(cls):
        """
        Get Ambari host
        :return: str
        """
        if Selenium.getDebug():
            host = Selenium.getDebugGatewayHost()
        else:
            if YARN.isHAEnabled():
                for host in [YARN.getRMHostByState('standby'), YARN.getRMHostByState('active'), HDFS.getGateway()]:
                    pid_list = Machine.getProcessListRemote(
                        host, "%U %p %P %a", "java | grep -i org.apache.ambari.server.controller.AmbariServer", None,
                        True
                    )
                    if pid_list:
                        return host
            else:
                host = HDFS.getGateway()
        return host

    @classmethod
    def getAmbariUrl(cls):
        """
        Get Ambari URL
        :return: str
        """
        if HDFS.isEncrypted():
            return "https://%s:8443" % cls.getAmbariHost()
        else:
            return "http://%s:8080" % cls.getAmbariHost()

    @classmethod
    def getTezInstanceName(cls):
        """
        Get Tez instance name
        :return: str
        """
        return "TezDynamicInstance"

    @classmethod
    def getTezDisplayName(cls):
        """
        Get Tez instance display name
        :return: str
        """
        return "TezDynamicDisplayName"

    @classmethod
    def getTezDescription(cls):
        """
        Get Tez instance description
        :return: str
        """
        return "TezDynamicDescription"

    @classmethod
    def getTimelineServerUrl(cls):
        """
        Get YARN ATS URL
        :return: str
        """
        if Selenium.getDebug():
            url = "http://%s:8188" % cls.getAmbariHost()
        else:
            url = YARN.getATSWebappAddress()
        return url

    @classmethod
    def getRMUrl(cls):
        """
        Get YARN RM URL
        :return: str
        """
        if Selenium.getDebug():
            url = "http://%s:8088" % cls.getAmbariHost()
        else:
            url = YARN.getResourceManagerWebappAddress()
        return url

    @classmethod
    def getDriver(cls):
        """
        Get Selenium driver. This API does not set driver. Use setDriver in that case.
        :return: str
        """
        return cls._driver

    @classmethod
    def getArtifactsDir(cls):
        """
        Get Tez UI artifacts dir
        :return: str
        """
        return TezUI._ARTIFACTS_DIR

    @classmethod
    def getDownloadDir(cls):
        if Selenium.getDebug():
            r = "/tmp"
            #r = "/Users/tathiapinya/tmp"
        else:
            r = TezUI._ARTIFACTS_DIR
        return r

    @classmethod
    def setDriver(cls):
        os.environ['DISPLAY'] = ":99"
        Machine.runas(Machine.getAdminUser(), "dbus-uuidgen --ensure")
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.privatebrowsing.autostart", True)
        profile.set_preference("network.http.phishy-userpass-length", 255)
        profile.set_preference("network.automatic-ntlm-auth.trusted-uris", "x.x.x.x")
        profile.accept_untrusted_certs = True
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", cls.getDownloadDir())
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip,application/octet-stream")
        profile.set_preference("browser.helperApps.alwaysAsk.force", False)
        # QE-4343
        profile.set_preference("dom.max_script_run_time", 0)

        def restart_xvfb():
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
                cls._driver = Selenium.getWebDriver(
                    browserType='firefox', platformType='LINUX', browser_profile=profile
                )
                cls._driver.implicitly_wait(cls._IMPLICIT_WAIT_SEC)  # seconds
                break
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    restart_xvfb()
                    pass
                else:
                    logger.error("attempt : %s , Failed to get webdriver: %s" % (num_attempts, e))
                num_attempts = num_attempts + 1

    @classmethod
    def quitDriver(cls):
        """
        Quit Selenium session
        :return: None
        """
        logger.info("Tez UI quitDriver")
        cls._driver.quit()

    @classmethod
    def closeDriver(cls):
        """
        Close a single browser window.
        :return: None
        """
        logger.info("Tez UI closeDriver")
        cls._driver.close()

    @classmethod
    def saveScreenshot(cls, logoutput=True):
        """
        Save screenshot with automatically generated file name
        :param logoutput:
        :return: None
        """
        ssFileName = TezUI.getNextSsFilename()
        if logoutput:
            logger.info("Saving screenshot to %s" % ssFileName)
        if cls._driver is None:
            logger.warn("saveScreenshot fails. cls._driver is None")
        else:
            cls._driver.save_screenshot(ssFileName)

    @classmethod
    def loginToAmbariUI(cls, sleepTime=8, user=None):
        """
        Login to Ambari UI
        :param sleepTime:
        :param user to use to login to Ambari UI. Use admin if None.
        :return: None
        """
        logger.info("Tez UI loginToAmbariUI begin")
        if user is None:
            user = "admin"
        logger.info("loginToAmbariUI use user = %s" % user)
        if Machine.isHumboldt():
            password = "HdpCli123!"
        elif Machine.isCloud():
            password = "cloudbreak1"
        else:
            password = 'admin'
        url = TezUI.getAmbariUrl()
        logger.info("url = %s" % url)
        cls._driver.get(url)
        logger.info("page loaded")
        elem = cls._driver.find_element_by_xpath("//*[@data-qa='username-input']")
        elem.send_keys(user)
        elem = cls._driver.find_element_by_xpath("//*[@data-qa='password-input']")
        elem.send_keys(password)
        elem = cls._driver.find_element_by_xpath("//*[@data-qa='login-button']")
        elem.click()

        logger.info("Tez UI sleep for %s sec after login." % sleepTime)
        time.sleep(sleepTime)
        logger.info("Tez UI loginToAmbariUI end")

    @classmethod
    def getMainUI(cls, instanceUrl, sleepTime=10):
        """
        Get/Refresh Main UI and set selenium iframe to Tez UI
        :param instanceUrl:
        :param sleepTime:
        :return: None
        """
        logger.info("TezUI.getMainUI begin")
        cls._driver.get(instanceUrl)
        time.sleep(sleepTime)
        Selenium.switchToIFrame(cls._driver)
        logger.info("TezUI.getMainUI end")

    @classmethod
    def refresh(cls, sleepTime=10, useAmbariUrl=True):
        """
        Call refresh and set selenium iframe to Tez UI
        :param sleepTime:
        :return: None
        """
        cls._driver.refresh()
        time.sleep(sleepTime)
        if useAmbariUrl:
            cls._driver.switch_to.frame(cls._driver.find_elements_by_tag_name("iframe")[0])

    @classmethod
    def refreshMainUI(cls, instanceUrl, sleepTime=10, useAmbariUrl=True, user=None):
        """
        Go to main UI or refresh if we are at main UI already.
        :param instanceUrl if None, use static view. Otherwise, use the URL.
        :return None
        """
        logger.info("TezUI.refreshMainUI begin instanceUrl = %s" % instanceUrl)
        if instanceUrl is None:
            logger.info("refreshMainUI instanceURL= None, useAmbariUrl=%s" % useAmbariUrl)
            instanceUrl = cls.getStaticInstanceURL(logoutput=True, useAmbariUrl=useAmbariUrl)
            logger.info("instanceUrl is None. Use static view. instanceUrl = ___%s___" % instanceUrl)

        # switch to default frame first
        # otherwise we might see current url which is different from what we see in browser.
        # e.g. I faced these 2 urls and get does not load new page.
        # currentUrl (Tez UI iframe url) = http://192.168.75.54:8080/views/TEZ/0.7.0.2.3.0.0-75/TezInstance/#/dag/dag_1429725697042_0002_2
        # instanceUrl (Ambari main URL) = http://192.168.75.54:8080/#/main/views/TEZ/0.7.0.2.3.0.0-75/TezInstance

        cls.getDriver().switch_to.default_content()

        # this section text is obsolete but keep for fyi. It was when we get current url of iframe.
        # We want to treat these 2 urls to be equal.
        # http://172.22.67.43:8080/views/TEZ/0.7.0.2.3.0.0-75/TezInstance/  (currentUrl of iframe)
        # http://172.22.67.43:8080/#/main/views/TEZ/0.7.0.2.3.0.0-75/TezInstance  (instanceUrl)
        # Issue is there is no easy way to tell that those pages are same or not.
        # Blindly call get then switch iframe can cause an error at locating iframe when get does not move to new page.
        # We can do try-catch when iframe is not found but that will cause implicit wait in every API call. That is not desirable.
        # I choose to do not-pretty URL comparison hack until we have better solution.
        currentUrl = cls.getDriver().current_url
        logger.info("after switch to Ambari outer frame - currentUrl = ___%s___" % currentUrl)
        if "login" in currentUrl:
            cls.loginToAmbariUI(user=user)
        logger.info("instanceUrl = ___%s___" % instanceUrl)

        logger.info("check if we should call selenium refresh or get")
        doRefresh = currentUrl.strip('/') == instanceUrl.strip('/')
        logger.info("doRefresh = %s" % doRefresh)
        if doRefresh:
            logger.info("driver.refresh")
            cls._driver.refresh()
        else:
            logger.info("driver.get")
            cls._driver.get(instanceUrl)
            # We must explicitly refresh in this case too. (get will change url but wont refresh the contents.)
            # currentUrl = http://172.22.70.70:8080/#/main/views/TEZ/0.7.0.2.3.0.0-75/TezInstance?viewPath=%2F%23%2Fdag%2Fdag_1429725697042_0009_2
            # instanceUrl = http://172.22.70.70:8080/#/main/views/TEZ/0.7.0.2.3.0.0-75/TezInstance
            if currentUrl.strip('/').find(instanceUrl.strip('/')) > -1:
                logger.info("force driver.refresh")
                cls._driver.refresh()

        currentUrl = cls.getDriver().current_url
        logger.info("new currentUrl = %s" % currentUrl)
        logger.info("sleep for %s sec" % sleepTime)
        time.sleep(sleepTime)

        # Set all subsequent driver calls to Tez UI iframe
        logger.info("find Tez UI iframe")
        try:
            iframeElems = cls._driver.find_elements_by_tag_name("iframe")
            iframeElem = iframeElems[0]
        except Exception as ex:
            logger.info("exception occurs %s" % traceback.format_exc())
            logger.info("retry")
            iframeElem = None

        # retry if iframe is not found
        try:
            iframeElems = cls._driver.find_elements_by_tag_name("iframe")
            iframeElem = iframeElems[0]
        except Exception as ex:
            logger.info("exception occurs %s" % traceback.format_exc())
            iframeElem = None
        if iframeElem is not None:
            logger.info("switch to Tez UI iframe")
            cls._driver.switch_to.frame(iframeElem)
        else:
            logger.info("do no switch to Tez UI iframe since iFrameElem is None")

        currentUrl = cls.getDriver().current_url
        logger.info("after switch to Tez iframe - currentUrl = %s" % currentUrl)
        logger.info("TezUI.refreshMainUI end")

    @classmethod
    def getMainUITableBodyDivElem(cls, logoutput=True):
        """
        Get table body div element in main UI page
        :param logoutput:
        :return: webelement
        """
        xpath = "//div[@class='table-body']"
        assert Selenium.isElementVisibleInSometime(
            cls._driver, xpath=xpath
        ), "Failed to fetch table body div element in main UI page"
        return cls._driver.find_element_by_xpath(xpath)

    @classmethod
    def getProgressData(cls):
        """
        Get Progress Data  using h4 element
        :return:
        """
        table = cls._driver.find_elements_by_xpath("//table[@class='detail-list']")  # Stats table
        tableBody = table[1].find_element_by_xpath("./tbody")
        trs = tableBody.find_elements(By.TAG_NAME, "tr")  #rows
        vertice = trs[0].find_elements(By.TAG_NAME, "td")[1].text  # 1st row 2nd column
        match = re.search("(\d+)\s*(Succeeded)*", vertice)
        Vertice = Tasks = percent = ""
        if match:
            Vertice = match.group(1)
        tasks = trs[2].find_elements(By.TAG_NAME, "td")[1].text  # 3rd row 2nd column
        match = re.search("(\d+)\s*(Succeeded)*", tasks)
        if match:
            Tasks = match.group(1)
        Percent = cls._driver.find_element_by_xpath("//div[@class='progress']/div/span").text
        match = re.search("(\d+)%*", Percent)
        if match:
            percent = match.group(1)
        return (Vertice, Tasks, int(percent))

    @classmethod
    def getMainProgressBar(cls):
        '''
        Make sure that Progress Bar is present in Dag Page
        If Progress Bar is present , return its progress
        If Progress Bar is not present, return None
        :return:
        '''
        try:
            Percent = cls._driver.find_element_by_xpath("//div[@class='progress']/div/span").text
            match = re.search("(\d+)%*", Percent)
            curr_progress = ""
            if match:
                curr_progress = match.group(1)
            return curr_progress
        except Exception as e:
            return None

    @classmethod
    def validateProgressTable(cls):
        '''
        Make sure that Progress table is present in Dag page
        :return:
        '''
        try:
            vertex_col = cls._driver.find_element_by_xpath('//*[@title="Vertex Name"]')
            progress_col = cls._driver.find_element_by_xpath('//*[@title="Progress"]')
            status_col = cls._driver.find_element_by_xpath('//*[@title="Status"]')
            total_task_col = cls._driver.find_element_by_xpath('//*[@title="Total Tasks"]')
            succeded_col = cls._driver.find_element_by_xpath('//*[@title="Succeeded Tasks"]')
            running_task_col = cls._driver.find_element_by_xpath('//*[@title="Running Tasks"]')
            pending_task_col = cls._driver.find_element_by_xpath('//*[@title="Pending Tasks"]')
            failed_task_col = cls._driver.find_element_by_xpath('//*[@title="Failed Task Attempts"]')
            killed_task_col = cls._driver.find_element_by_xpath('//*[@title="Killed Task Attempts"]')
            return True
        except Exception as e:
            return False

    @classmethod
    def gotoAppDetailPage(cls, tableBodyDivElement, appId):
        applink = tableBodyDivElement.find_element_by_link_text(appId)
        logger.info(applink)
        applink.click()
        time.sleep(5)

    @classmethod
    def validateRunningTezAppDescription(cls, appId):
        verify_appId = cls._driver.find_element_by_link_text(appId)
        appId_val = verify_appId.get_attribute('innerHTML')
        assert appId in appId_val, "AppId : %s missing from App Tracking URL"
        for x in range(1, 10):
            try:
                status = cls._driver.find_elements_by_xpath('.//span[contains(@class,"status")]')
                status_val = status[0].get_attribute('innerHTML')
                logger.info(status_val)
            except:
                continue
        assert "RUNNING" in status_val, "Status : RUNNING missing"

    @classmethod
    def validateFinishedTezAppDescription(cls, appId):
        verify_appId = cls._driver.find_element_by_link_text(appId)
        appId_val = verify_appId.get_attribute('innerHTML')
        assert appId in appId_val, "AppId : %s missing from App Tracking URL"
        status = cls._driver.find_elements_by_xpath('.//span[contains(@class,"status")]')
        status_val = status[1].get_attribute('innerHTML')
        logger.info(status_val)
        assert "SUCCEEDED" in status_val, "Status : SUCCEDED missing"
        state = cls._driver.find_elements_by_xpath('//span[contains(@class,"status")]')
        state_val = state[0].get_attribute('innerHTML')
        logger.info(status_val)
        assert "FINISHED" in state_val, "State : FINISHED missing"

    @classmethod
    def getMainUIRowValuesTuples(cls, tableBodyDivElement, logoutput=True):
        """
        Get a list of row-values tuples in main UI page.
        Row index starts at 0. Each row is a tuple. Tuple index starts at 1. (index 0 is None.)
        (None, DAG name, ID, Submitter, Status, Progress, Start time, End time, Duration, Application ID, Queue)
         0       1        2      3       4        5          6         7           8             9           10

          (DAG name webelement, app ID webelement)
                    11                  12

        :param tableBodyDivElement:
        :param logoutput:
        :return: a list of tuples
        """
        r = []
        rowCount = 0

        # use first column to find row count
        # elems is index starting at 0.
        elems = tableBodyDivElement.find_elements_by_xpath("./div/div[contains(@class,'table-column')]")
        taskColDivElem = elems[0]
        rowElems = taskColDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        rowCount = len(rowElems)
        logger.info("rowCount = %s" % rowCount)

        # build r rows
        for rowNo in range(rowCount):
            r.append((None, ))

        # parse each column
        for colNo in range(1, 11):
            colDivElem = elems[colNo - 1]
            rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
            for rowNo in range(len(rowElems)):
                rowTuple = r[rowNo]
                for x in range(1, 10):
                    try:
                        if colNo in [1, 4, 5, 9]:
                            elem = rowElems[rowNo].find_element_by_xpath("./div")
                        else:
                            elem = rowElems[rowNo]
                        text = elem.text
                    except:
                        continue
                if logoutput:
                    logger.info("(colNo, rowNo) = (%s, %s) text = %s" % (colNo, rowNo, text))
                # parse time object
                if colNo in [6, 7]:
                    if text.strip() != "" and str(text).strip().lower() != "not available!":
                        timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                        rowTuple += (timeObj, )
                    else:
                        rowTuple += ("", )
                else:
                    rowTuple += (str(text).strip(), )
                if logoutput:
                    logger.info("(colNo, rowNo, rowTuple) = (%s, %s, %s)" % (colNo, rowNo, rowTuple))
                r[rowNo] = rowTuple

        # build tuple col 11 dag name webelement
        colDivElem = elems[0]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            rowTuple = r[rowNo]
            if colNo in [1, 4, 5, 9]:
                elem = rowElems[rowNo].find_element_by_xpath("./div")
            else:
                elem = rowElems[rowNo]
            rowTuple += (elem, )
            r[rowNo] = rowTuple

        # build tuple col 12 app ID webelement
        colDivElem = elems[8]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            rowTuple = r[rowNo]
            if colNo in [1, 4, 5, 9]:
                elem = rowElems[rowNo].find_element_by_xpath("./div")
            else:
                elem = rowElems[rowNo]
            rowTuple += (elem, )
            r[rowNo] = rowTuple

        if logoutput:
            logger.info("len(r) = %s" % len(r))
            for rowNo in range(len(r)):
                logger.info("row no. %s = %s" % (rowNo, r[rowNo]))

        return r

    @classmethod
    def getMainUIDAGName(cls, rowValueTuple):
        """
        Get DAG name in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[1]

    @classmethod
    def getMainUIDAGID(cls, rowValueTuple):
        """
        Get DAG ID in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[2]

    @classmethod
    def getMainUISubmitter(cls, rowValueTuple):
        """
        Get submitter in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[3]

    @classmethod
    def getMainUIStatus(cls, rowValueTuple):
        """
        Get DAG status in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[4]

    @classmethod
    def getMainUIDAGStatus(cls, rowValueTuple):
        """
        Get DAG status in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[4]

    @classmethod
    def getApplicationIdTag(cls, rowValueTuple):
        """
        Get Application Id in main UI Page
        :param rowValueTuple:
        :return:
        """
        return rowValueTuple[8]

    @classmethod
    def getMainUIPageNo(cls):
        """
        Get page number in main UI page
        :param rowValueTuple:
        :return: str
        """
        elem = cls._driver.find_element_by_xpath("//div[@class = 'page-count']/span[@class='counter']")
        pageNo = elem.text
        return pageNo.strip()

    @classmethod
    def getMainUITableRows(cls):
        """
        Get table rows in main UI page
        :param rowValueTuple:
        :return: list of webelements
        """
        return cls._driver.find_elements_by_xpath(
            "//div[contains(@class, 'ember-table-body-container')]//div[contains(@class, 'ember-table-table-row') and not(contains(@style, 'display:none'))]"
        )

    @classmethod
    def getMainUIDAGID(cls, rowValueTuple, logoutput=True):
        """
        Get DAG IDs in main UI page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[2]

    @classmethod
    def getMainUIDAGLinkElem(cls, rowValueTuple, rowNo=1):
        """
        Get DAG link in main UI page
        :param rowValueTuple:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//div[contains(@class, 'table-cell')][%s]/div/a" % rowNo)

    @classmethod
    def getMainUINavPageList(cls):
        """
        Get page list ul for pagination in main UI page.
        This element contains first, 1, 2, .. buttons
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//ul[contains(@class, 'page-list')]")

    @classmethod
    def getMainUINavFirst(cls, navPageList):
        """
        Get nav-first element for pagination in main UI page
        :param navPageList
        :return: webelement
        """
        elem = navPageList.find_element_by_xpath("./li[1]")
        return elem

    @classmethod
    def getMainUINavPageNoLink(cls, navPageList, pageNo):
        """
        Get clickable element for given page number for pagination in main UI page
        :param navPageList
        :param pageNo
        :return: webelement
        """
        elem = navPageList.find_element_by_xpath("./li[%s]" % (pageNo + 1))
        return elem

    @classmethod
    def getSingleDAGPageRowValueTuple(cls, rowElement, logoutput=True):
        """
        Get a tuple of each row values in single-dag page for given rowElement.
        :param rowElement:
        :param logoutput:
        :return: a tuple. (None, task index, attempt no, start time, end time, duration, status, container, node, actions, logs, <we-build-attempt>)
                             0    1                2        3            4        5       6           7       8      9       10          11
        """
        r = (None, )
        elems = rowElement.find_elements_by_xpath("./div//div")
        elems = [None] + elems
        for i in range(1, 11):
            elem = elems[i]
            if logoutput:
                logger.info("i=%s elem=%s" % (i, elem))
            text = elem.text
            if logoutput:
                logger.info("i=%s text=%s" % (i, text))
            if i in [3, 4]:
                if text.strip() != "":
                    timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                    r += (timeObj, )
                else:
                    r += ("", )
            else:
                r += (str(text.strip()), )
        attemptElem = rowElement.find_element_by_xpath("./div//div[1]/a")
        href = attemptElem.get_attribute("href")
        if logoutput:
            logger.info("attemptElem = %s" % attemptElem)
            logger.info("href = %s" % href)
        attemptID = re.findall("/(attempt_.*)", href)[0]
        r += (str(attemptID), )
        return r

    @classmethod
    def getSingleDAGPageNavElement(cls):
        """
        Get nav in single-dag page
        :param rowValueTuple:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//ul[contains(@class, 'nav nav-tabs')]")

    @classmethod
    def getSingleDAGPageAllTaskAttemptsLink(cls, navElem):
        """
        Get task attempts link in single-dag page
        :param navElem:
        :return: webelement
        """
        return navElem.find_element_by_xpath(".//a[contains(@href, 'attempts')]")

    @classmethod
    def getSingleDAGCountersLink(cls, navElem):
        """
        Get task attempts link in single-dag page
        :param navElem:
        :return: webelement
        """
        return navElem.find_element_by_xpath(".//a[contains(@href, 'counters')]")

    @classmethod
    def getSingleDAGPageAllTasksLink(cls, navElem):
        """
        Get tasks link in single-dag page
        :param navElem:
        :return: webelement
        """
        return navElem.find_element_by_xpath(".//a[contains(@href, 'tasks')]")

    @classmethod
    def getSingleDAGPageAllVerticesLink(cls, navElem):
        """
        Get tasks link in single-dag page
        :param navElem:
        :return: webelement
        """
        return navElem.find_element_by_xpath(".//a[contains(@href, 'vertices')]")

    @classmethod
    def getSingleVertexSwimlaneLink(cls, navElem):
        """
        Get task attempts link in single-dag page
        :param navElem:
        :return: webelement
        """
        return navElem.find_element_by_xpath(".//a[contains(@href, 'swimlane')]")

    @classmethod
    def getSingleDAGPageAttemptStatus(cls, rowValueTuple):
        """
        Get attempt status in single-dag page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[6]

    @classmethod
    def getAllAttemptsPageTableBodyDivElem(cls, logoutput=True):
        """
        Get table body div element in all-attempts page
        :param logoutput:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//div[@class='table-body']")

    @classmethod
    def getAllTasksPageTableBodyDivElem(cls, logoutput=True):
        """
        Get table body div element in all-Tasks page
        :param logoutput:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//div[@class='table-body']")

    @classmethod
    def getAllVerticesPageTableBodyDivElem(cls, logoutput=True):
        """
        Get table body div element in all-Vertices page
        :param logoutput:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//div[@class='table-body']")

    @classmethod
    def getAllAttemptsPageSortIconsList(cls, tableBodyDivElement, logoutput=True):
        """
        Get a list of sort icon webelments in all-attempts page
        :param tableBodyDivElement:
        :param logoutput:
        :return: a list of webelements
        """

        return tableBodyDivElement.find_elements_by_css_selector(".sort-icon")

    @classmethod
    def getAllTasksPageSortIconsList(cls, tableBodyDivElement, logoutput=True):
        """
        Get a list of sort icon webelments in all-tasks page
        :param tableBodyDivElement:
        :param logoutput:
        :return: a list of webelements
        """
        return tableBodyDivElement.find_elements_by_css_selector(".sort-icon")

    @classmethod
    def getAllVerticesPageSortIconsList(cls, tableBodyDivElement, logoutput=True):
        """
        Get a list of sort icon webelments in all-vertices page
        :param tableBodyDivElement:
        :param logoutput:
        :return: a list of webelements
        """
        return tableBodyDivElement.find_elements_by_css_selector(".sort-icon")

    @classmethod
    def getAllAttemptsPageSearchInput(cls, logoutput=True):
        """
        Get search input in all-attempts page
        :param logoutput:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath(".//div[@class='input-group']/input")

    @classmethod
    def getAllAttemptsPageSearchButton(cls, logoutput=True):
        """
        Get search button in all-attempts page
        :param logoutput:
        :return: webelement
        """
        return cls._driver.find_element_by_css_selector(".btn.btn-default")

    @classmethod
    def getAllAttemptsPageRowValuesTuples(cls, tableBodyDivElement, logoutput=True):
        """
        Get a list of row-values tuples in all-task-attempts page.
        Row index starts at 0. Each row is a tuple. Tuple index starts at 1. (index 0 is None.)
          (None,  Attempt no, task index, vertex index, status, Progress, start time, end time, duration, container, node,  logs,
          (0,      1,            2,          3,            4,      5,        6,          7,        8,        9,      10,     11,

           <we-build-attempt>, <clickable task index webelement>, <clickable attempt no webelement>,
             12                       13                              14

           <clickable counter webelement>, <clickable view webelement>, <clickable download webelement>)
                 15,                                 16,                          17)
        :param tableBodyDivElement:
        :param logoutput:
        :return: a list of tuples
        """
        r = []
        rowCount = 0

        # use first column to find row count
        # elems is index starting at 0.
        elems = tableBodyDivElement.find_elements_by_xpath("./div/div[contains(@class,'table-column')]")
        taskColDivElem = elems[0]
        rowElems = taskColDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        rowCount = len(rowElems)
        logger.info("rowCount = %s" % rowCount)

        # build r rows
        for rowNo in range(rowCount):
            r.append((None, ))

        # parse each column
        for colNo in range(1, 12):
            colDivElem = elems[colNo - 1]
            rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
            for rowNo in range(len(rowElems)):
                rowTuple = r[rowNo]
                if colNo in [8, 9, 10]:
                    elem = rowElems[rowNo]
                else:
                    elem = rowElems[rowNo].find_element_by_xpath("./div")
                text = elem.text
                if logoutput:
                    logger.info("(colNo, rowNo) = (%s, %s) text = %s" % (colNo, rowNo, text))
                # parse time object
                if colNo in [6, 7]:
                    if text.strip() == "" or text.strip() == "Not Available!" or text.strip() == '0':
                        rowTuple += ("", )
                    else:
                        timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                        rowTuple += (timeObj, )
                else:
                    rowTuple += (str(text).strip(), )
                if logoutput:
                    logger.info("(colNo, rowNo, rowTuple) = (%s, %s, %s)" % (colNo, rowNo, rowTuple))
                r[rowNo] = rowTuple

        # build tuple col 12 attempt number
        colDivElem = elems[0]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if logoutput:
                logger.info("build col 12 rowNo = %s" % rowNo)
            rowTuple = r[rowNo]
            attemptElem = rowElems[rowNo].find_element_by_xpath("./div/a")
            href = attemptElem.get_attribute("href")
            if logoutput:
                logger.info("attemptElem = %s" % attemptElem)
                logger.info("href = %s" % href)
            attemptID = re.findall("/(attempt_.*)", href)[0]
            rowTuple += (str(attemptID), )
            r[rowNo] = rowTuple

        # build tuple col 13 clickable task index
        colDivElem = elems[1]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if logoutput:
                logger.info("build col 13 rowNo = %s" % rowNo)
            rowTuple = r[rowNo]
            clickableElem = rowElems[rowNo].find_element_by_xpath("./div/a")
            rowTuple += (clickableElem, )
            r[rowNo] = rowTuple

        # build tuple col 14 clickable attempt no
        colDivElem = elems[0]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if logoutput:
                logger.info("build col 14 rowNo = %s" % rowNo)
            rowTuple = r[rowNo]
            clickableElem = rowElems[rowNo].find_element_by_xpath("./div/a")
            rowTuple += (clickableElem, )
            r[rowNo] = rowTuple

        # build tuple col 15 clickable counter
        #colDivElem = elems[9]
        #rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        #for rowNo in range(len(rowElems)):
        #    if logoutput:
        #        logger.info("build col 15 rowNo = %s" % rowNo)
        #    rowTuple = r[rowNo]
        #    clickableElem = rowElems[rowNo].find_element_by_xpath("./div/a")
        #    rowTuple += (clickableElem, )
        #    r[rowNo] = rowTuple

        # build tuple col 16-17 clickable webelement
        colDivElem = elems[10]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if logoutput:
                logger.info("build col 16 rowNo = %s" % rowNo)
            rowTuple = r[rowNo]
            try:
                # View
                clickableElem1 = rowElems[rowNo].find_element_by_xpath("./div/a[1]")
            except:
                clickableElem1 = None

            # Download (not available if attempt is running)
            if logoutput:
                logger.info("build col 17 rowNo = %s" % rowNo)
            try:
                clickableElem2 = rowElems[rowNo].find_element_by_xpath("./div/a[2]")
            except selenium.common.exceptions.WebDriverException as ex:
                clickableElem2 = None
            logger.info("(clickableElem1, clickableElem2) = (%s,%s)" % (clickableElem1, clickableElem2))
            rowTuple += (
                clickableElem1,
                clickableElem2,
            )
            r[rowNo] = rowTuple

        if logoutput:
            logger.info("len(r) = %s" % len(r))
            for rowNo in range(len(r)):
                logger.info("row no. %s = %s" % (rowNo, r[rowNo]))

        return r

    @classmethod
    def getAllAttemptsPageRowValuesSingleCol(cls, tableBodyDivElement, colNo, logoutput=True):
        """
        Get a list of row values of specific column number in all-task-attempts page.
        Output index starts at 0.
        :param tableBodyDivElement:
        :param colNo: column number starting at 1
        :param logoutput:
        :return: a list of (str or time object)
        """
        r = []
        rowCount = 0

        # use first column to find row count
        # elems is index starting at 0.
        elems = tableBodyDivElement.find_elements_by_xpath("./div/div[contains(@class,'table-column')]")
        taskColDivElem = elems[0]
        rowElems = taskColDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        rowCount = len(rowElems)
        logger.info("rowCount = %s" % rowCount)

        # parse each column

        colDivElem = elems[colNo - 1]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if colNo not in [8, 9, 10]:
                elem = rowElems[rowNo].find_element_by_xpath("./div")
            else:
                elem = rowElems[rowNo]
            text = elem.text
            if logoutput:
                logger.info("(colNo, rowNo) = (%s, %s) text = %s" % (colNo, rowNo, text))
            # parse time object
            if colNo in [6, 7]:
                if text.strip() != "" and str(text).strip().lower() != "not available!":
                    timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                    r.append(timeObj)
                else:
                    r.append("")
            else:
                r.append(str(text).strip())
            if logoutput:
                logger.info("(colNo, rowNo, value) = (%s, %s, %s)" % (colNo, rowNo, r[-1]))

        if logoutput:
            logger.info("len(r) = %s" % len(r))
            for rowNo in range(len(r)):
                logger.info("row no. %s = %s" % (rowNo, r[rowNo]))

        return r

    @classmethod
    def getAllTasksPageRowValuesSingleCol(cls, tableBodyDivElement, colNo, logoutput=True):
        """
        Get a list of row values of specific column number in all-task page.
        Output index starts at 0.
        :param tableBodyDivElement:
        :param colNo: column number starting at 1
        :param logoutput:
        :return: a list of (str or time object)
        """
        r = []
        rowCount = 0

        # use first column to find row count
        # elems is index starting at 0.
        elems = tableBodyDivElement.find_elements_by_xpath("./div/div[contains(@class,'table-column')]")
        taskColDivElem = elems[0]
        rowElems = taskColDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        rowCount = len(rowElems)
        logger.info("rowCount = %s" % rowCount)

        # parse each column

        colDivElem = elems[colNo - 1]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if colNo not in [7]:
                elem = rowElems[rowNo].find_element_by_xpath("./div")
            else:
                elem = rowElems[rowNo]
            text = elem.text
            if logoutput:
                logger.info("(colNo, rowNo) = (%s, %s) text = %s" % (colNo, rowNo, text))
            # parse time object
            if colNo in [5, 6]:
                if text.strip() != "" and str(text).strip().lower() != "not available!":
                    timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                    r.append(timeObj)
                else:
                    r.append("")
            else:
                r.append(str(text).strip())
            if logoutput:
                logger.info("(colNo, rowNo, value) = (%s, %s, %s)" % (colNo, rowNo, r[-1]))

        if logoutput:
            logger.info("len(r) = %s" % len(r))
            for rowNo in range(len(r)):
                logger.info("row no. %s = %s" % (rowNo, r[rowNo]))

        return r

    @classmethod
    def getAllVerticesPageRowValuesSingleCol(cls, tableBodyDivElement, colNo, logoutput=True):
        """
        Get a list of row values of specific column number in all-task page.
        Output index starts at 0.
        :param tableBodyDivElement:
        :param colNo: column number starting at 1
        :param logoutput:
        :return: a list of (str or time object)
        """
        r = []
        rowCount = 0

        # use first column to find row count
        # elems is index starting at 0.
        elems = tableBodyDivElement.find_elements_by_xpath("./div/div[contains(@class,'table-column')]")
        taskColDivElem = elems[0]
        rowElems = taskColDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        rowCount = len(rowElems)
        logger.info("rowCount = %s" % rowCount)

        # parse each column

        colDivElem = elems[colNo - 1]
        rowElems = colDivElem.find_elements_by_xpath("./div[contains(@class, 'table-cell')]")
        for rowNo in range(len(rowElems)):
            if colNo in [1, 3, 4]:
                elem = rowElems[rowNo].find_element_by_xpath("./div")
            else:
                elem = rowElems[rowNo]
            text = elem.text
            if logoutput:
                logger.info("(colNo, rowNo) = (%s, %s) text = %s" % (colNo, rowNo, text))
            # parse time object
            if colNo in [5, 6, 8]:
                if text.strip() != "" and str(text).strip().lower() != "not available!":
                    timeObj = time.strptime(text, "%j %b %Y %H:%M:%S")
                    r.append(timeObj)
                else:
                    r.append("")
            else:
                r.append(str(text).strip())
            if logoutput:
                logger.info("(colNo, rowNo, value) = (%s, %s, %s)" % (colNo, rowNo, r[-1]))

        if logoutput:
            logger.info("len(r) = %s" % len(r))
            for rowNo in range(len(r)):
                logger.info("row no. %s = %s" % (rowNo, r[rowNo]))
        return r

    @classmethod
    def getAllAttemptsPageAttemptStatus(cls, rowValueTuple):
        """
        Get attempt status in all-attempts page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[4]

    @classmethod
    def getAllAttemptsPageAttemptID(cls, rowValueTuple):
        """
        Get attempt ID in all-attempts page
        :param rowValueTuple:
        :return: str
        """
        return rowValueTuple[12]

    @classmethod
    def getAllAttemptsPageAttemptLinkElem(cls, rowValueTuple):
        """
        Get attempt link in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[12]

    @classmethod
    def getAllAttemptsPageViewLink(cls, rowValueTuple):
        """
        Get view link in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[16]

    @classmethod
    def getAllAttemptsPageDownloadLink(cls, rowValueTuple):
        """
        Get download link in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[17]

    @classmethod
    def getAllAttemptId(cls, rowValueTuple):
        """
        Get Attempt number in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[1]

    @classmethod
    def getAllTaskIndex(cls, rowValueTuple):
        """
        Get Task Index in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[2]

    @classmethod
    def getAllvertex(cls, rowValueTuple):
        """
        Get Vertex in all-attempts page
        :param rowValueTuple:
        :return: webelement
        """
        return rowValueTuple[3]

    @classmethod
    def getDurationToCompare(cls, s):
        """
        Get number of milliseconds above 0 ms.
        :return int
        """
        tokens = s.split(' ')
        sum = 0
        for token in tokens:
            num = int(token[0])
            unit = token[1].strip()
            if unit.startswith('ms'):
                sum = sum + num
            elif unit.startswith('s'):
                sum = sum + num * 1000
            elif unit.startswith('min'):
                sum = sum + num * 1000 * 60
            else:
                return -1
        return sum

    @classmethod
    def getDAGDetailsPageDownloadData(cls):
        """
        Get download-data webelement in DAG details page
        :param rowValueTuple:
        :return: webelement
        """
        return cls._driver.find_element_by_xpath("//table[contains(@class,'detail-list')]//button")

    @classmethod
    def checkProgressErrorBar(
            cls, message="Failed to get in-progress status. Manually refresh to get the updated status"
    ):
        """
        Check progress error message
        :return:
        """
        try:
            elem = cls._driver.find_element_by_css_selector("html body.ember-application div.error-bar.visible")
            text = elem.get_attribute('innerHTML')
            logger.info("**********")
            logger.info(text)
            logger.info(message)
            logger.info("**********")
            m = re.search(message, text)
            if m:
                return True
            else:
                return False
        except Exception as e:
            return False

    @classmethod
    def checkErrorMsgDagDetails(cls):
        '''
        Check progress error message
        '''
        try:
            elem = cls._driver.find_element_by_xpath('//div[@class="message"]')
            message = elem.get_attribute('innerHTML')
            return message
        except Exception as e:
            return False

    @classmethod
    def checkErrorMsgAppDetails(cls):
        '''
        Check progress error message
        '''
        try:
            elem = cls._driver.find_element_by_xpath('/html/body/div[1]/div/div[1]')
            message = elem.get_attribute('innerHTML')
            logger.info(message)
            return message
        except Exception as e:
            return False

    @classmethod
    def appendNewProgress(cls, runid, vertex, task, percent, progressbar, progress):
        """
        Manage a global Progress Dictionary
        :param runid:
        :param vertex:
        :param task:
        :param percent:
        :return:
        """
        progress[runid]["vertex"] = vertex
        progress[runid]["task"] = task
        progress[runid]["percent"] = percent
        progress[runid]["main_progressbar"] = progressbar
        return progress

    @classmethod
    def compareProgressBwRunid(cls, progress, runid1, runid2, compare="min"):
        """
        Compare Progress of 2 runids
        :param runid1: Runid like "1" or "2"
        :param runid2: Runid like "1" or "2"
        :param compare: to validate Runid1 > Runid2, use "max"
                    to validate Runid1 < Runid2, use "min"
                    to validate Runid1 = Runid2, use "equal"
        :return:
        """
        for prop in ["vertex", "task", "percent"]:
            v1 = progress[runid1][prop]
            v2 = progress[runid2][prop]
            if prop == "vertex" or prop == "task":
                v1 = Decimal(v1.split("/")[0].strip())
                v2 = Decimal(v2.split("/")[0].strip())

            if compare == "max":
                assert v1 > v2
            elif compare == "min":
                assert v1 <= v2
            else:
                assert v1 == v2

    @classmethod
    def is_application_finished(cls, app_id):
        """
        Checks whether applicattion=${appID} is finished or not
        """
        is_finished = False
        for x in range(0, 10):
            is_finished = YARN.waitForApplicationFinish(app_id)
            if is_finished is True:
                break
        return is_finished

    @classmethod
    def get_all_vertices_swimlanes(cls):
        '''
        Return all vertices and status in swimlane graph
        '''
        r = {}
        vertices = cls._driver.find_elements_by_xpath('.//div[contains(@class,"em-swimlane-vertex-name")]')
        logger.info("No. of vertices : %s " % len(vertices))
        # build vertices
        for vertexNo in range(len(vertices)):
            vertex_status = vertices[vertexNo
                                     ].find_element_by_xpath('.//div[(contains(@class,"em-table-status-cell"))]')
            logger.info(str(vertices[vertexNo].text).split())
            if (len(str(vertices[vertexNo].text).split()) == 2):
                vertex_name = str(vertices[vertexNo].text).split()[1]
                r[vertex_name] = vertex_status.text
            if (len(str(vertices[vertexNo].text).split()) == 3):
                vertex_name = str(vertices[vertexNo].text).split()[2]
                r[vertex_name] = vertex_status.text
        return r

    @classmethod
    def get_vertexTable_toolTip(cls):
        '''
        return the table which contains the vertex details
        '''
        r = []
        vertices = cls._driver.find_elements_by_xpath('.//div[contains(@class,"em-swimlane-vertex-name")]')
        for rowNo in range(len(vertices)):
            r.append((None, ))
        for vertexNo in range(len(vertices)):
            rowTuple = r[vertexNo]
            time.sleep(10)
            if Machine.isIBMPower():
                cls._driver.execute_script(
                    "arguments[0].style.border = arguments[1];", vertices[vertexNo], "2px solid red"
                )
                js_script = "if(document.createEvent){var evObj = document.createEvent('MouseEvents');evObj.initEvent('mouseover', true, false); arguments[0].dispatchEvent(evObj);} else if(document.createEventObject) { arguments[0].fireEvent('onmouseover');}"
                cls._driver.execute_script(js_script, vertices[vertexNo])
            else:
                ActionChains(cls._driver).move_to_element(vertices[vertexNo]).perform()
            cls.saveScreenshot()
            time.sleep(2)
            WebDriverWait(cls._driver, 20).until(
                EC.visibility_of_element_located((By.XPATH, '//div[contains(@class,"tip-props")]/table'))
            )
            vertex_table = cls._driver.find_element_by_xpath('//div[contains(@class,"tip-props")]/table')
            trs = vertex_table.find_elements(By.TAG_NAME, "tr")  # rows
            for tr in range(len(trs)):
                tds = trs[tr].find_elements(By.TAG_NAME, "td")  # columns
                text = tds[1].text
                rowTuple += (str(text).strip(), )
            r[vertexNo] = rowTuple
        return r

    @classmethod
    def get_vertex_bubbles(cls):
        '''
        return the start time the vertex was initialized
        '''
        r = []
        vertex_lines = cls._driver.find_elements_by_xpath(".//div[contains(@class,'em-swimlane-process-visual')]")
        for rowNo in range(len(vertex_lines)):
            r.append((None, ))
        for line in range(len(vertex_lines)):
            logger.info(line)
            logger.info("************")
            rowTuple = r[line]
            xpath = ".//div[contains(@class,'em-swimlane-process-visual')][%s]//div[contains(@class,'em-swimlane-event')]/div[@class='event-line']" % (
                line + 1
            )
            time.sleep(5)
            event_line = cls._driver.find_elements_by_xpath(xpath)
            logger.info(len(event_line))
            logger.info("************")
            for bubble in event_line:
                ActionChains(cls._driver).move_to_element(bubble).perform()
                cls.saveScreenshot()
                try:
                    WebDriverWait(cls._driver, 80).until(
                        EC.visibility_of_element_located((By.XPATH, '//div[@class="bubble"]//table'))
                    )
                except:
                    continue
                if bubble == 2 or 4:
                    bubble_value = cls._driver.find_elements_by_xpath('//div[@class="bubble"]//table')
                    bubble_name = cls._driver.find_elements_by_xpath('//div[@class="bubble"]/div[@class="tip-title"]')
                    for b in range(len(bubble_name)):
                        logger.info(bubble_name[b].text)
                        rowTuple += (str(bubble_name[b].text).strip(), )
                        trs = bubble_value[b].find_elements(By.TAG_NAME, "tr")  # rows
                        for tr in range(len(trs)):
                            tds = trs[tr].find_elements(By.TAG_NAME, "td")  # columns
                            text = tds[1].text
                            logger.info(text)
                        rowTuple += (str(text).strip(), )

                else:
                    bubble_value = cls._driver.find_element_by_xpath('//div[@class="bubble"]//table')
                    bubble_name = cls._driver.find_element_by_xpath('//div[@class="bubble"]/div[@class="tip-title"]')
                    logger.info(bubble_name.text)
                    rowTuple += (str(bubble_name.text).strip(), )
                    trs = bubble_value.find_element(By.TAG_NAME, "tr")  # rows
                    tds = trs.find_elements(By.TAG_NAME, "td")  # columns
                    text = tds[1].text
                    logger.info(text)
                    rowTuple += (str(text).strip(), )
            r[line] = rowTuple
        logger.info(r)

    @classmethod
    def zoomVertexSwimlane(cls, zoomValue='200%'):
        '''
        zoom the vertex swimlane graph to 200%
        '''
        zoom = cls._driver.find_element_by_xpath('//input[@type="range"]')
        cls._driver.execute_script("arguments[0].value = arguments[1];", zoom, zoomValue)
        logger.info(zoom.get_attribute('innerHTML'))
        time.sleep(5)
        cls.saveScreenshot()

    @classmethod
    def verifyDependencyLine(cls, noOfDependencies=1):
        '''
        Return True or False if the dependency line is found.
        '''
        dline = cls._driver.find_elements_by_xpath('//div[@class="event-line" and contains(@style,"height")]')
        if len(dline) == noOfDependencies:
            return True
        else:
            return False

    @classmethod
    def verifyConsolidated(cls):
        '''
        Verify the presence of consolidated bar
        '''
        vertices = cls._driver.find_elements_by_xpath('.//div[contains(@class,"em-swimlane-vertex-name")]')
        consolidatedBars = cls._driver.find_elements_by_xpath("//div[@class='consolidated-view']/div")
        if len(vertices) != len(consolidatedBars):
            return False
        for bar in consolidatedBars:
            style = bar.get_attribute('style')
            if re.search("display\: none", style):
                return False
        return True

    @classmethod
    def clickElementWhenVisible(cls, xpath, errorMsg=None):
        assert Selenium.isElementVisibleInSometime(
            cls._driver, xpath=xpath
        ), errorMsg if errorMsg else "Element with xpath [%s] not found" % xpath
        elem = cls._driver.find_element_by_xpath(xpath)
        elem.click()
