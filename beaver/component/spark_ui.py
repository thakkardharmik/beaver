import logging
import os
import signal
import socket
import time

from beaver.component.ambari import Ambari
from beaver.component.zeppelin import ZeppelinAmbariAPIUtil
from beaver.machine import Machine
from beaver.seleniumHDP import Selenium
from beaver.component.spark import Spark
from beaver.component.hadoop import HDFS
from selenium import webdriver
from tests.spark.data.spark_xpaths import SPARK_XPATH
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

max_wait = 60


class TimeoutException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class SparkUIClientSession:
    def __init__(self, is_proxy=False):
        self.is_proxy = is_proxy
        self.ambari_connector = ZeppelinAmbariAPIUtil()
        self.driver = SparkUIClientSession.__instantiate_webdriver()
        assert self.driver, "Could not initialize selenium webdriver"

        if self.is_proxy:
            self.shs_proxy_url = self.get_shs_proxy_url()
            assert self.shs_proxy_url, "Failed to find SHS knox proxy URL"

        self.shs_direct_url = Spark.getSparkHistoryServerUrl()
        assert self.shs_direct_url, "Failed to find SHS direct URL"

        self.ambari_url = self.get_ambari_url()
        assert self.ambari_url, "Failed to find ambari web URL"

    @classmethod
    @TaskReporter.report_test()
    def __instantiate_webdriver(cls):
        '''
        Method to instantiate the webdriver
        :return:
        '''
        os.environ['DISPLAY'] = ":99"
        Machine.runas(Machine.getAdminUser(), "dbus-uuidgen --ensure")
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.privatebrowsing.autostart", True)
        profile.set_preference("network.http.phishy-userpass-length", 255)
        profile.set_preference("network.automatic-ntlm-auth.trusted-uris", "x.x.x.x")
        profile.set_preference("network.negotiate-auth.trusted-uris", "http://,https://")
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference("browser.download.dir", "/tmp/")
        profile.set_preference("browser.helperApps.alwaysAsk.force", False)
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.manager.showAlertOnComplete", False)
        profile.set_preference("browser.download.manager.closeWhenDone", True)
        profile.set_preference("browser.download.manager.alertOnEXEOpen", False)
        profile.set_preference("browser.download.manager.focusWhenStarting", False)
        profile.set_preference("browser.download.manager.useWindow", False)
        profile.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/x-ustar,application/octet-stream,application/zip,text/csv,text/plain,application/json"
        )

        profile.accept_untrusted_certs = True
        driver = None

        num_attempts = 0
        max_attempts = 5
        while num_attempts < max_attempts:
            try:
                driver = Selenium.getWebDriver(browserType='firefox', platformType='LINUX', browser_profile=profile)
                break
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    pass
                else:
                    logger.error("attempt : %s , Failed to get webdriver for SHS : %s" % (num_attempts, e))
                time.sleep(2)
                num_attempts = num_attempts + 1
        driver.maximize_window()
        return driver

    @TaskReporter.report_test()
    def __quit_webdriver(self):
        '''
        Method to quit the webdriver
        :return:
        '''
        try:
            Selenium.quitWebDriver(self.driver)
        except Exception, e:
            self.driver = None
            logger.warn("Ignoring webdriver quit failure")
            pass

    def restart_session(self):
        '''
        Method to reinstantiate the webdriver
        :return:
        '''
        self.__quit_webdriver()
        self.driver = SparkUIClientSession.__instantiate_webdriver()

    def delete_session(self):
        '''
        Method to delete the session
        :return:
        '''
        self.__quit_webdriver()

    @TaskReporter.report_test()
    def get_screenshot_as_file(self, filename, timeout=10):
        '''
        Method for capturing the screenshot at a given file
        :param filename:
        :param timeout:
        :return:
        '''

        @TaskReporter.report_test()
        def timeout_handler(signum, frame):
            raise TimeoutException("Timeout occurred while capturing screenshot: %s seconds" % timeout)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        try:
            self.driver.get_screenshot_as_file(filename)
        except TimeoutException, e:
            logger.error("%s" % e)
        finally:
            signal.alarm(0)

    @TaskReporter.report_test()
    def get_shs_proxy_url(self):
        '''
        Method to get the proxy URL for history server
        :return:
        '''
        knox_hosts_str = self.ambari_connector.get_component_hosts('KNOX', 'KNOX_GATEWAY')
        assert knox_hosts_str, "Failed to get knox_hosts str for forming SHS proxy URL"
        knox_hosts = sorted(knox_hosts_str.split(','))
        topology = 'ui'
        if HDFS.isFederated():
            topology = 'ui_ns1'
        return "https://{0}:8443/gateway/{1}/sparkhistory".format(knox_hosts[0], topology)

    @TaskReporter.report_test()
    def get_ambari_url(self):
        '''
        Method to get ambari url
        :return:
        '''
        ambari_url = Ambari.getWebUrl()
        host = socket.gethostbyname(socket.gethostname())
        host_name = socket.gethostname()
        if ".hwx.site" not in host_name:
            host_name = host_name + ".hwx.site"
        ambari_url = ambari_url.replace(host, host_name)
        return ambari_url

    @TaskReporter.report_test()
    def login_using_sso(self, user_name, password):
        '''
        Method to use when SSO is enabled. It will login using knox login form

        :param user_name:
        :param password:
        :return:
        '''
        logger.info("Logging with Username = {0}".format(user_name))
        username_xpath = SPARK_XPATH.sso_username
        password_xpath = SPARK_XPATH.sso_password
        login_btn_xpath = SPARK_XPATH.sso_login_btn
        login_error_xpath = SPARK_XPATH.sso_login_error
        assert Selenium.isElementVisibleInSometime(self.driver, xpath=username_xpath), "username field is not visible"
        username_field = Selenium.getElement(self.driver, xpath=username_xpath)
        Selenium.sendKeys(username_field, user_name)

        assert Selenium.isElementVisibleInSometime(self.driver, xpath=password_xpath), "password field is not visible"
        password_field = Selenium.getElement(self.driver, xpath=password_xpath)
        Selenium.sendKeys(password_field, password)

        assert Selenium.isElementVisibleInSometime(self.driver, xpath=login_btn_xpath), "login button is not visible"
        login_button = Selenium.getElement(self.driver, xpath=login_btn_xpath)
        Selenium.click(self.driver, login_button)
        logger.info("Clicked on login button")
        Selenium.waitTillElementDisappears(driver=self.driver, xpath=username_xpath, maxWait=20)
        assert not Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=login_error_xpath), \
            "Login failed due to Invalid Username/password"

    @TaskReporter.report_test()
    def load_shs_home_page(self):
        if not self.is_proxy:
            url = self.shs_direct_url
        else:
            url = self.shs_proxy_url
        self.driver.get(url)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.spark_logo)

    def load_ambari(self):
        self.driver.get(self.ambari_url)

    @TaskReporter.report_test()
    def click_on_app(self, app_id):
        '''
        Method to click on the spark app based on app id
        :param app_id:
        :return:
        '''
        is_app_visible = Selenium.waitTillElementBecomesVisible(
            driver=self.driver, xpath=SPARK_XPATH.app_id.format(app_id), maxWait=max_wait
        )
        if not is_app_visible:
            self.driver.refresh()
        Selenium.waitTillElementBecomesVisible(
            driver=self.driver, xpath=SPARK_XPATH.app_id.format(app_id), maxWait=max_wait
        )
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.app_id.format(app_id))
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.job_header)

    @TaskReporter.report_test()
    def go_to_incomplete_app(self):
        '''
        Method to navigate to incomplete apps page
        :return:
        '''
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.incomplete_app_link)
        Selenium.click_on_element_xpath(driver=self.driver, xpath=SPARK_XPATH.incomplete_app_link)

    @TaskReporter.report_test()
    def go_to_completed_app(self):
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.completed_app_link)
        Selenium.click_on_element_xpath(driver=self.driver, xpath=SPARK_XPATH.completed_app_link)

    @TaskReporter.report_test()
    def is_summary_table_displayed(self):
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.summary_table)
        return Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.summary_table)

    @TaskReporter.report_test()
    def load_jobs_page(self):
        '''
        Method to click on Jobs link and go to Jobs page
        :return:
        '''
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.job_link)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.job_header)

    @TaskReporter.report_test()
    def load_stages_page(self):
        '''
        Method to click on Stages link and go to stages page
        :return:
        '''
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.stage_link)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.stage_header)

    @TaskReporter.report_test()
    def load_storage_page(self):
        '''
        Method to click on storage link and go to storage page
        :return:
        '''
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.storage_link)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.storage_header)

    @TaskReporter.report_test()
    def load_environment_page(self):
        '''
        Method to click on environment link and go to environment page
        :return:
        '''
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.environment_link)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.environment_header)

    @TaskReporter.report_test()
    def load_executor_page(self):
        '''
        Method to click on executor link and go to executor page
        :return:
        '''
        Selenium.click_on_element_xpath(self.driver, SPARK_XPATH.executor_link)
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.executor_header)

    @TaskReporter.report_test()
    def validate_job_page(self, user):
        '''
        Method to validate the contents of jobs page
        :param user:
        :return:
        '''
        assert "Completed Jobs" in Selenium.getElement(
            driver=self.driver, xpath=SPARK_XPATH.complted_job_info
        ).text.strip()
        assert user in Selenium.getElement(driver=self.driver, xpath=SPARK_XPATH.user_info).text.strip()
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.completed_job_table)

    @TaskReporter.report_test()
    def validate_stages_page(self):
        '''
        Method to validate the contents of stages page
        :return:
        '''
        assert "Completed Stages" in Selenium.getElement(
            driver=self.driver, xpath=SPARK_XPATH.complted_stage_info
        ).text.strip()
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.completed_stage_table)

    @TaskReporter.report_test()
    def validate_storage_page(self):
        '''
        Method to validate the contents of storage page
        :return:
        '''
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.storage_header)

    @TaskReporter.report_test()
    def validate_environment_page(self):
        '''
        Method to validate the contents of environment page
        :return:
        '''
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.env_spark_properties)
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.env_system_properties)
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.env_run_time_info)
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.env_class_path_entries)

    @TaskReporter.report_test()
    def validate_executor_page(self):
        '''
        Method to validate the contents of executor page
        :return:
        '''
        Selenium.waitTillElementBecomesVisible(
            driver=self.driver, xpath=SPARK_XPATH.executor_summary_table, maxWait=max_wait
        )
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.executor_summary_table)
        assert Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.executor_active_table)

    @TaskReporter.report_test()
    def download_logs(self, app_id):
        [os.remove(os.path.join("/tmp/", f)) for f in os.listdir("/tmp/") if f.startswith("eventLogs-")]
        Selenium.waitTillElementBecomesVisible(
            driver=self.driver, xpath=SPARK_XPATH.log_download.format(app_id), maxWait=max_wait
        )
        Selenium.click_on_element_xpath(driver=self.driver, xpath=SPARK_XPATH.log_download.format(app_id))
        from glob import glob
        result = glob('/tmp/eventLogs-{0}*.zip'.format(app_id))
        sleep_time = 0
        while len(result) <= 0 and sleep_time <= 10:
            time.sleep(1)
            sleep_time = sleep_time + 1
            result = glob('/tmp/eventLogs-{0}*.zip'.format(app_id))
        return len(result) > 0

    @TaskReporter.report_test()
    def is_403_msg_present(self):
        '''
        Method to get the conetnt when an authorized user tries to access the app
        :return:
        '''
        return Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.un_authorized_page_heading)

    @TaskReporter.report_test()
    def is_incomplete_app_link_displayed(self):
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.incomplete_app_link, maxWait=30)
        return Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.incomplete_app_link)

    @TaskReporter.report_test()
    def is_completed_app_link_displayed(self):
        Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=SPARK_XPATH.completed_app_link, maxWait=30)
        return Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=SPARK_XPATH.completed_app_link)
