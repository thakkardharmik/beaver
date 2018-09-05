import logging

from beaver.component.ambariHelper import Maven2
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.ambari import Ambari
from beaver import util
from upgradelogger import UpgradeLogger
from publishTestCaseListener import nonPyTestReportList
from beaver.component.ambari_commonlib import CommonLib
from beaver.component.ambari_apilib import APICoreLib

import os, collections
import shutil
import pytest
import logging
import json
import re

logger = logging.getLogger(__name__)


# Common Base class for all Upgrades - Rolling Ugrades, Express Upgrades, Host Ordered Upgrades, Patch Upgrades
class Upgrade(object):

    # Constructor for Upgrade class. Initializes common variables to be used across by upgrade tests
    def __init__(self):
        self.COMPONENT = Config.get('ambari', 'COMPONENT')
        self.TESTSUITE_FILE = Config.get('ambari', 'TESTSUITE_FILE')
        self.PYTEST_FILE = os.path.join(
            Config.getEnv('WORKSPACE'), 'beaver', 'component', 'upgrades', 'upgradecommon.py'
        )
        self.SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'uifrm')
        if 'preupgrade' in self.COMPONENT or 'installold' in self.COMPONENT:
            self.SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'uifrm_old', 'uifrm')
        self.LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), self.COMPONENT)
        self.LOCAL_WORK_DIR_UIFRM = os.path.join(Config.getEnv('ARTIFACTS_DIR'), self.COMPONENT)
        self.DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
        self.JAVA_HOME = Config.get('machine', 'JAVA_HOME')
        self.DISPLAY = Config.getEnv('DISPLAY')
        self.RUN_MARKER_VERSION = Config.get('ambari', 'RUN_MARKER_VERSION')
        self.RUN_MARKER_LIST = Config.get('ambari', 'RUN_MARKER_LIST')
        self.STACK_TYPE = Config.get('ambari', 'STACK_TYPE')
        self.TEST_RESULT = {}
        self.TESTCASES = []
        self.env = {}
        self.step_counter = 0
        self.source_stack_version = ''
        self.target_stack_version = Config.get('ambari', 'STACK_UPGRADE_TO')

        self.isambari_upgrade_success = False

        if (len(Config.get('ambari', 'UPGRADE_TO').strip()) > 0):
            self.is_ambari_upgrade = True
        else:
            self.is_ambari_upgrade = False

    #This function fetches the ambari cluster details
    def print_environment_details(self):
        UpgradeLogger.reportProgress("=====Environment Details=====\n ", True)
        is_hdp = self.STACK_TYPE.lower() == "hdp"
        UpgradeLogger.reportProgress("Ambari URL : " + Ambari.getWebUrl(is_hdp=is_hdp), True)
        UpgradeLogger.reportProgress("Ambari OS : " + Ambari.getOS(), True)
        UpgradeLogger.reportProgress("Stack Type : " + Config.get('ambari', 'STACK_TYPE'), True)
        UpgradeLogger.reportProgress("Ambari DB : " + Config.get('ambari', 'AMBARI_DB'), True)
        UpgradeLogger.reportProgress("Kerberos : " + Config.get('machine', 'IS_SECURE'), True)
        UpgradeLogger.reportProgress("HA : " + Config.get('ambari', 'IS_HA_TEST'), True)
        UpgradeLogger.reportProgress("Wire Encryption : " + Config.get('ambari', 'WIRE_ENCRYPTION'), True)

    def get_source_stack_version(self):
        return self.source_stack_version

    def get_target_stack_version(self):
        return self.target_stack_version

    def set_source_stack_version(self, version):
        self.source_stack_version = version

    # This function reads Jenkins job parameters and updates them in ambari.properties file for usage by tests
    def read_and_update_job_parameters(self):
        print "COMPONENT = ", self.COMPONENT
        print "TESTSUITE_FILE = ", self.TESTSUITE_FILE
        print "SRC_DIR = ", self.SRC_DIR
        print "LOCAL_WORK_DIR = ", self.LOCAL_WORK_DIR

        #Set permissions on ambari testcode location
        Machine.runas('root', 'chmod -R 755 ' + self.DEPLOY_CODE_DIR)

        #copy the ambari ui test code to artifacts dir
        shutil.copytree(self.SRC_DIR, self.LOCAL_WORK_DIR)

        #copy the ambari pytest file to artifacts dir
        shutil.copy(self.PYTEST_FILE, self.LOCAL_WORK_DIR)

        #populate the properties file
        propChanges = CommonLib.read_job_properties(self.LOCAL_WORK_DIR)

        self.splitNumStr = str(propChanges['SPLIT_NUM'])

        CommonLib.log_test_properties(propChanges)
        CommonLib.delete_test_time_duration_file()

        self.env['DISPLAY'] = self.DISPLAY
        CommonLib.common_startup_operations(self.LOCAL_WORK_DIR)

    # TODO: This function would return the result of most recent Stack Upgrade
    def get_stack_upgrade_result(self):
        return True  # TODO: Make an API call here to know the status of most recent upgrade operation

    # This function executes 'mvn test' command with appropriate options
    def Maven2runas(self, cmd, cwd=None, env=None, mavenOpts=None, logoutput=True, user=None):
        return CommonLib.Maven2runas(cmd, self.LOCAL_WORK_DIR, cwd, env, mavenOpts, logoutput, user)

    # This function executes pre-upgrade steps, if any
    def perform_pre_upgrade_tests(self):
        raise Exception('This method should be implemented in the child class')

    # This function executes post-upgrade steps, if any
    def perform_post_upgrade_steps(self):
        raise Exception('This method should be implemented in the child class')

    # This function collects test results after all tests have finished execution. Mainly used for Dashboard reporting purpose
    def gatherTestResults(self):
        logger.info("=====Gathering results=========")
        testresult = {}
        global nonPyTestReportList
        Machine.runas('root', 'chmod -R 755 ' + os.path.join(self.LOCAL_WORK_DIR, 'target'))
        # get a list of all the test result files
        testResultFiles = util.findMatchingFiles(
            os.path.join(self.LOCAL_WORK_DIR, 'target', 'surefire-reports'), 'TEST-*.xml'
        )
        #print "Test result files: " % testResultFiles
        for resultFile in testResultFiles:
            testresult.update(util.parseJUnitXMLResult(resultFile))

        for key, value in testresult.items():
            fail_message = value['failure']
            print "key : %s " % key
            print "value : %s " % value
            print "fail_mesg : %s " % fail_message
            m = re.search("([^\.]*)$", key)
            #remove braces and hyphens from tc name
            #key = m.group(1).replace('-','')
            key = 'split-' + self.splitNumStr + '-' + m.group(1)
            #if '[' in key:
            #    key = key[:key.index('[')]
            value = re.match(r'.*result(.*)', str(value)).group(1)
            #remove additional quotes from tc status
            if '\'' in value:
                value = value[3:9].replace('\'', "")
            self.TEST_RESULT[str(key)] = {"status": str(value), "failure": fail_message}
        self.TESTCASES.extend(sorted(self.TEST_RESULT.keys()))
        for testcase in self.TESTCASES:
            print(testcase)

        TEST_SUITE_REPORT = dict()
        jsonFile = os.path.join('/tmp', 'testTimeDurations.json')
        if os.path.exists(jsonFile):
            Machine.runas('root', 'chmod -R 777 /tmp')

            with open(jsonFile, 'r') as data_file:
                data = json.load(data_file)

            testcases = data['testCases']
            for testcase in testcases:
                testcasename = 'split-' + self.splitNumStr + '-' + testcase['testCaseName']
                TEST_SUITE_REPORT[testcasename] = testcase
                nonPyTestReportList.append({testcasename: TEST_SUITE_REPORT[testcasename]})
                print nonPyTestReportList

    # This function returns the counter for a test step
    def get_step_counter(self):
        return str(self.step_counter)

    # This function increments the counters for a test step and returns the udpated counter
    def get_next_step_counter(self):
        self.step_counter = self.step_counter + 1
        return str(self.step_counter)

    # This function logs an error message and then throws an exception
    def throw_exception(self, message, stacktrace):
        UpgradeLogger.reportProgress(message + stacktrace, False)
        raise Exception(message)

    # This function logs an error for a failed test step and returns the result as a failure (False)
    def update_result_as_failed(self, error_message):
        UpgradeLogger.reportProgress(error_message, False)
        return False

    # This function returns current Ambari-server version
    def get_ambari_server_version(self):
        exit_code, stdout = Machine.runas('root', 'ambari-server --version')
        return stdout

    # This function returns hash of ambari-server
    def get_ambari_server_hash(self):
        exit_code, stdout = Machine.runas('root', 'ambari-server --hash')
        return stdout

    # This function returns the status of most recent stack upgrade; True is successful upgrade, False otherwise
    def is_stack_upgrade_success(self):
        from beaver.component.ambari_apilib import APICoreLib
        apicorelib = APICoreLib(self.LOCAL_WORK_DIR_UIFRM)
        status = apicorelib.get_last_upgrade_status()
        if status is not None and status.lower() == "COMPLETED".lower():
            return True
        else:
            return False

    # This function checks return code of the test and logs an error with its stack trace if the test has failed
    def verify_test_result(self, exit_code, test_class_name, current_dir):
        if exit_code != 0:
            UpgradeLogger.reportProgress("=====Error details for failed test(s)=====\n ", False)
            for key, value in UpgradeLogger.get_stack_trace(test_class_name, current_dir).items():
                UpgradeLogger.reportProgress("Test:%s AND Stacktrace: %s" % (str(key), value), False)
