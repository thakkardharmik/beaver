import logging
import os

import json
import shutil

from beaver import util
from upgradelogger import UpgradeLogger
from beaver.config import Config
from beaver.machine import Machine
from upgradecommon import Upgrade
from beaver.component import ambariutils

logger = logging.getLogger(__name__)


# This class contains functions specific to Express Upgrade of cluster. It is a derived class of Upgrade
class ExpressUpgrade(Upgrade):

    # This function executes Express Upgrade and related suites, after cluster is deployed and optionally Ambari has been upgraded
    def perform_express_upgrade(self):
        COMPONENT = str(self.COMPONENT)
        STACK_TYPE = str(self.STACK_TYPE)

        env = {}
        env['DISPLAY'] = self.DISPLAY

        # Update pom.xml of uifrm with Markers - applies for tests that involve a combination of API and UI tests run within the same split
        ambariutils.update_pom_xml_with_markers(self.LOCAL_WORK_DIR)

        SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'apitestframework')
        print "SRC_DIR = ", SRC_DIR

        # Change the current directory for api test code
        LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), COMPONENT, 'apitestframework')

        # Copy the ambari api test code to artifacts dir
        shutil.copytree(SRC_DIR, LOCAL_WORK_DIR)
        # Change the permission of openstack-keypairfile
        os.chmod(os.path.join(LOCAL_WORK_DIR, 'src/resources/openstack-keypair'), 0400)

        # populate the config properties file for api test code
        configPropChanges = ambariutils.updateProperties()

        # Change the current directory for api test code
        LOCAL_WORK_DIR = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), COMPONENT, 'apitestframework', 'src', 'resources'
        )
        util.writePropertiesToFile(
            os.path.join(LOCAL_WORK_DIR, 'config.properties'), os.path.join(LOCAL_WORK_DIR, 'config.properties'),
            configPropChanges
        )

        # Change the current directory for api test code
        LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), COMPONENT, 'apitestframework')

        testSuiteLocation = "src/test/suites"

        # Check what is the STACK_TYPE. Based on the stack type we will decide which test suite to select
        logger.info(
            "STACK TYPE is " + STACK_TYPE + ". So going into " + STACK_TYPE + " if block for test suite "
            "selection"
        )

        if STACK_TYPE == "HDF":
            # Setup for HDF EU. We can now add an else method to handle ru
            if 'ambarieu-hdf-downgrade' in COMPONENT:
                testSuiteFile = "ambarieu-hdf-downgrade.suite"
            elif 'ambarieu-hdf' in COMPONENT:
                testSuiteFile = "ambarieu-hdf.suite"
            logger.info("Setting HDF testsuite file to " + testSuiteFile)
        elif STACK_TYPE == "HDP" and "ambarieu-hdp-hdf" in COMPONENT:
            testSuiteFile = "ambarieu-hdp-hdf.suite"
        else:
            if 'e2e' not in COMPONENT:
                # These are system tests so use suite filename sent from json file
                testSuiteFile = COMPONENT + ".suite"
            else:
                if 'dwngd' in COMPONENT:  # For EU E2E downgrade runs
                    testSuiteFile = "ambari-expressupgrade-downgrade-e2e.suite"
                elif 'mlevel' in COMPONENT:  # For EU E2E multi-level paths runs
                    testSuiteFile = "ambari-expressupgrade-mlevel-e2e.suite"
                elif 'denygpl' in COMPONENT:  # For Deny GPL tests in EU E2E
                    testSuiteFile = "ambarieu-denygpl.suite"
                elif 'wkflow' in COMPONENT:  # For EU E2E workflow suite runs
                    testSuiteFile = "ambari-expressupgrade-wkflow-e2e.suite"
                elif 'iopmigration' in COMPONENT:  # Added for IOP migration E2E tests
                    testSuiteFile = "ambari-iopmigration-e2e.suite"
                elif 'iopintg' in COMPONENT:  # Added for IOP integration tests
                    testSuiteFile = "ambarieu-iopintg-e2e.suite"
                elif 'patchupgradeintg-ru' in COMPONENT:  # Added for RU PU integration tests
                    testSuiteFile = "ambarieu-patchupgradeintg-ru-e2e.suite"
                elif 'patchupgradeintg-thirddigit' in COMPONENT:  # Added for EU PU 3rd digit integration tests
                    testSuiteFile = "ambarieu-patchupgradeintg-thirddigit-e2e.suite"
                elif 'patchupgradeintg-revert' in COMPONENT:  # Added for EU PU integration tests with revert
                    testSuiteFile = "ambarieu-patchupgradeintg-revert-e2e.suite"
                elif 'patchupgradeintg' in COMPONENT:  # Added for EU PU integration tests
                    testSuiteFile = "ambarieu-patchupgradeintg-e2e.suite"
                elif 'experiment' in COMPONENT:  # Added for full EU integration tests
                    testSuiteFile = "ambari-expressupgrade-experiment-e2e.suite"
                else:  # Default ofr EU E2E runs
                    testSuiteFile = "ambari-expressupgrade-upgrade-e2e.suite"

        logger.info("Opening test suite file : " + testSuiteFile + " for test execution")

        file = open(os.path.join(LOCAL_WORK_DIR, testSuiteLocation, testSuiteFile))
        testSuite = json.load(file)
        file.close()

        # magic word to use as key in suite file
        magic = "split" + self.splitNumStr
        print "magic key is : ", magic

        if 'experiment' in COMPONENT or 'patchupgradeintg' in COMPONENT or 'iopintg' in COMPONENT:
            magic = "split1"
            print "magic key for experiment EU/PU run is : ", magic

        # Update pom.xml for API framework with Markers info
        ambariutils.update_pom_xml_with_markers(LOCAL_WORK_DIR)

        upgrade_test_results = {}
        # Iterate over the list of all test classes in the split and execute them
        logger.info("=====Starting Express Upgrade tests=========")

        if testSuite.has_key(magic):
            for testName in testSuite[magic]:
                if not ambariutils.isTestClassPresent(testName, LOCAL_WORK_DIR):
                    LOCAL_WORK_DIR = ambariutils.switchDirectory(LOCAL_WORK_DIR, COMPONENT)

                logger.info('LOCAL_WORK_DIR %s ', LOCAL_WORK_DIR)
                logger.info('================Running %s with maven===============' % (testName))
                self.LOCAL_WORK_DIR = LOCAL_WORK_DIR
                exit_code, stdout = self.Maven2runas(
                    ' -Dtest=%s -DfailIfNoTests=false test' % testName, cwd=self.LOCAL_WORK_DIR, env=env, user='root'
                )
                UpgradeLogger.reportProgress('================Finished %s ========================' % (testName), True)
                logger.info('Exit code of the test: %s ' % (exit_code))

                if exit_code != 0:
                    upgrade_test_results.update(UpgradeLogger.get_stack_trace(testName, LOCAL_WORK_DIR))
                    UpgradeLogger.reportProgress("Test failure encountered: %s" % (testName), False)
                    # Do not run any further tests if Upgrade itself has failed
                    if self.is_upgrade_executed():
                        if not self.is_stack_upgrade_success():
                            UpgradeLogger.reportProgress('Express Upgrade failed, aborting rest of the tests', False)
                            break
                    else:
                        UpgradeLogger.reportProgress(
                            'Error(s) in steps before starting Upgrade, aborting rest of the tests', False
                        )
                        break
        else:
            print "================Not correct test suite format========================"

        if len(upgrade_test_results) > 0:
            UpgradeLogger.reportProgress("=====List of failed test(s)=====\n", False)
            for key, value in upgrade_test_results.items():
                UpgradeLogger.reportProgress(key, False)

            UpgradeLogger.reportProgress("=====Error details for failed test(s)=====", False)
            for key, value in upgrade_test_results.items():
                UpgradeLogger.reportProgress("Test:%s AND Failure details:\n %s" % (key, value), False)
                UpgradeLogger.reportProgress("=======================================================", True)
        else:
            UpgradeLogger.reportProgress("=====Express Upgrade test(s) completed successfully=========", True)

        # Gather reports for tests executed from apitestframework dir
        LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), COMPONENT, 'apitestframework')

        Machine.runas('root', 'chmod -R 777 ' + LOCAL_WORK_DIR)

        uifrmReportDirectory = os.path.join(LOCAL_WORK_DIR, '..', 'target/surefire-reports')
        if not os.path.exists(uifrmReportDirectory):
            Machine.runas('root', 'mkdir -p ' + uifrmReportDirectory)
            Machine.runas('root', 'chmod -R 777 ' + uifrmReportDirectory)
            logger.info('Created path for reporting')

        Machine.runas('root', 'chmod -R 777 ' + os.path.join(LOCAL_WORK_DIR, '..', 'target'))

        apiReportDirectory = os.path.join(LOCAL_WORK_DIR, 'target', 'surefire-reports', 'junitreports')
        if os.path.exists(apiReportDirectory):
            files = os.listdir(os.path.join(LOCAL_WORK_DIR, 'target', 'surefire-reports', 'junitreports'))
            for file in files:
                shutil.copy(
                    os.path.join(LOCAL_WORK_DIR, 'target', 'surefire-reports', 'junitreports', file),
                    os.path.join(LOCAL_WORK_DIR, '..', 'target', 'surefire-reports')
                )

        # Switch back to uifrm dir for reporting purposes
        LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), COMPONENT)
        self.LOCAL_WORK_DIR = LOCAL_WORK_DIR

    # This function executes pre-upgrade steps, if any
    def perform_pre_upgrade_tests(self):
        UpgradeLogger.reportProgress("No pre-upgrade tests defined for EU", True)

    # This function performs Ambari upgrade
    def start_ambari_upgrade(self):
        test_class_name = 'E2E_EU_AmbariSuite'
        exit_code, stdout = self.Maven2runas(
            ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
        )
        self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        return exit_code

    # This function executes post-upgrade steps, if any
    def perform_post_upgrade_steps(self):
        if Config.getEnv("HDP_STACK_INSTALLED").lower() == "true":
            from beaver.component.hadoop import Hadoop, HDFS
            from beaver.component.hive import Hive
            COMPONENT = str(self.COMPONENT)
            HDFS_USER = Config.get('hadoop', 'HDFS_USER')
            if 'experiment' in COMPONENT and Hive.isInstalled():
                HIVE_WAREHOUSE_DIR = Hive.getConfigValue(
                    "hive.metastore.warehouse.dir", defaultValue="/apps/hive/warehouse"
                )
                HDFS.chmod(HDFS_USER, 777, HIVE_WAREHOUSE_DIR, True)
            else:
                UpgradeLogger.reportProgress("No additional post-upgrade steps defined for EU", True)
        else:
            logger.info("No additional post-upgrade steps defined for EU on HDF")

    # This function checks if an upgrade was started on a cluster
    def is_upgrade_executed(self):
        from beaver.component.ambari_apilib import APICoreLib
        apicorelib = APICoreLib(self.LOCAL_WORK_DIR_UIFRM)
        if apicorelib.get_last_upgrade_status() is None:
            return False
        else:
            return True


class UpgradeReleaseInfo(Upgrade):
    def get_ambari_stack_version_after_upgrade(self):
        '''

        :param max_or_min: Should the version fetched be max or min? takes value "max" or "min"
        :return: max stack version
        '''
        from beaver.component.ambari_apilib import APICoreLib
        apicorelib = APICoreLib(self.LOCAL_WORK_DIR_UIFRM)
        ambari_version = apicorelib.get_current_ambari_version()
        stack_version = apicorelib.get_current_stack_version()
        stack_name = apicorelib.get_current_stack_name()
        return "ambari-%s %s-%s" % (ambari_version, stack_name.upper(), stack_version)
