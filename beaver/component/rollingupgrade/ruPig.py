#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, urllib2, collections, datetime, json
import urllib, sys
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruPig:

    _job_user = Config.getEnv("USER")
    _base_hdfs_dir = '/user/%s/ru-pig' % _job_user
    _SmokeInputDir = None
    _queue = 'pig'
    _local_workspace = Config.getEnv("WORKSPACE")
    _pig_script = os.path.join(_local_workspace, 'tests', 'rolling_upgrade', 'pig', 'script.pig')
    _golden_src_file = os.path.join(_local_workspace, 'tests', 'rolling_upgrade', 'pig', 'input.txt')
    _artifacts_dir = Config.getEnv('ARTIFACTS_DIR')
    _hdfs_input_dir = _base_hdfs_dir + '/input'
    _hdfs_input_path = _hdfs_input_dir + '/input.txt'
    _hdfs_smoke_input_path = _hdfs_input_dir + '/smoke_input.txt'
    _hdfs_output_dir = _base_hdfs_dir + '/output'
    _hdfs_smoke_output_dir = _base_hdfs_dir + '/smoke_output'
    _hdfs_success_filepath = _hdfs_output_dir + "/_SUCCESS"
    _process = None
    _long_running_job_timeout_secs = 600

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        Upload Data to HDFS before Upgrade starts
        Creates /user/hrt_qa/ru-pig dir on HDFS
        Creates and Upload large data file to /user/hrt_qa/ru-pig/input/
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Running Pig BackGround Job Setup ####")
        HDFS.deleteDirectory(cls._base_hdfs_dir)
        exit_code, stdout = HDFS.createDirectory(cls._base_hdfs_dir, user=cls._job_user, perm=777, force=True)
        ruAssert("Pig", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')

        HDFS.createDirectory(cls._hdfs_input_dir, force=True)
        srcFile = os.path.join(cls._artifacts_dir, 'pig-ru-input.txt')
        if os.path.exists(srcFile):
            os.remove(srcFile)
        tmpFile = os.path.join(cls._artifacts_dir, 'pig-ru-tmp-input.txt')
        if os.path.exists(tmpFile):
            os.remove(tmpFile)
        util.copyFileToAnotherFile(cls._golden_src_file, srcFile)
        util.copyFileToAnotherFile(srcFile, tmpFile)
        itr = 12
        if Machine.isFlubber():
            itr = 16
        for i in range(itr):
            util.copyFileToAnotherFile(srcFile, tmpFile)
            util.copyFileToAnotherFile(tmpFile, srcFile)
        exit_code, stdout = HDFS.copyFromLocal(srcFile, cls._hdfs_input_path)
        ruAssert("Pig", exit_code == 0, '[BGJobSetup] Data Load failed')

        if runSmokeTestSetup:
            cls.smoke_test_setup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Running Pig Smoke Test Setup ####")
        exit_code, stdout = HDFS.copyFromLocal(cls._golden_src_file, cls._hdfs_smoke_input_path)
        ruAssert("Pig", exit_code == 0, '[SmokeSetup] Smoke Test Data Load failed')

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Pig Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Background application for Pig started ####")
        runCmd = '-p \"INPUT_PATH=%s\" -p \"OUTPUT_PATH=%s\" -p \"QUEUE=%s\" %s' % (
            cls._hdfs_input_path, cls._hdfs_output_dir, cls._queue, cls._pig_script
        )
        #If Tez is enabled, add it in runCmd
        TEZ_ENABLED = Hadoop.isTez(True, False)
        if TEZ_ENABLED:
            runCmd = '-x \"tez\" %s' % runCmd
        cls.runCommand(cls._job_user, runCmd, runInBackground=True)
        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for Pig
        :param smoketestnumber: Used for unique output log location
        '''
        return cls.run_client_smoketest(config=config)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Pig job
        '''
        logger.info("TODO")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Verifying Long running job for Pig ####")
        # Check if the Long Running process is not None
        if cls._process is not None:
            # If process poll() returns None, it means the process has not finished yet.
            if cls._process.poll() is None:
                UpgradePerNode.reportProgress(
                    "### Long running job for Pig has not Finished yet. Waiting for it to complete ####"
                )
                # Wait for _long_running_job_timeout_secs for Job to complete
                starttime = time.time()
                while (starttime - time.time() < cls._long_running_job_timeout_secs) and cls._process.poll() is None:
                    time.sleep(5)

            exit_code = cls._process.poll()
            if exit_code is None:
                logger.info("Killing Pig Long running job process '%d'" % cls._process.pid)
                Machine.killProcess(cls._process.pid)
                UpgradePerNode.reportProgress(
                    "### [FAILED][Pig][BGJob] Long running job for Pig Failed to finish ####"
                )
            elif exit_code != 0:
                UpgradePerNode.reportProgress(
                    "### [FAILED][Pig][BGJob] Long running job for Pig Failed and Exited with '%d' ####" % exit_code
                )
            else:
                UpgradePerNode.reportProgress("### Long running job for Pig Finished ####")

            #Check for _SUCCESS file in HDFS Path
            if HDFS.fileExists(cls._hdfs_success_filepath, cls._job_user):
                UpgradePerNode.reportProgress(
                    "### [PASSED][Pig][BGJob] Found _SUCCESS file in HDFS for Pig Long running job ####"
                )
            else:
                UpgradePerNode.reportProgress(
                    "### [FAILED][Pig][BGJob] Not Found _SUCCESS file in HDFS for Pig Long running job. ####"
                )
        else:
            UpgradePerNode.reportProgress(
                "### [FAILED][Pig][BGJob] Long Running Pig Job Failed. No Process found ####"
            )

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Pig does not have any master, so no master to upgrade ####")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Pig does not have any slaves, so no slaves to upgrade ####")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Pig does not have any master, so no master to downgrade ####")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Pig does not have any slaves, so no slaves to downgrade ####")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Running Pig Smoke test ####")
        runCmd = '-p \"INPUT_PATH=%s\" -p \"OUTPUT_PATH=%s\" -p \"QUEUE=%s\" %s' % (
            cls._hdfs_smoke_input_path, cls._hdfs_smoke_output_dir, cls._queue, cls._pig_script
        )
        #If Tez is enabled, add it in runCmd
        TEZ_ENABLED = Hadoop.isTez(True, False)
        if TEZ_ENABLED:
            runCmd = '-x \"tez\" %s' % runCmd
        cls.runCommand(cls._job_user, runCmd, runInBackground=False)
        UpgradePerNode.reportProgress("### Pig Smoke test Finished ####")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Pig does not have any slaves, so nothing to do here ####")

    @classmethod
    def runCommand(cls, user, cmd, runInBackground=False):
        """
        Function to run pig cmd as a certain user
        :param user:
        :param cmd:
        :return:
        """
        try:
            from beaver.component.pig import Pig
        except ImportError:
            ## Import fails when Pig is not installed on this machine. Nothing to do
            return 0, ""

        if runInBackground:
            cls._process = Pig.runas(user, cmd, runInBackground=True)
        else:
            exit_code, stdout = Pig.runas(user, cmd, runInBackground=False)
            from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
            if exit_code == 0:
                UpgradePerNode.reportProgress("### [PASSED][Pig][Smoke] Pig Smoke test passed ####")
            else:
                UpgradePerNode.reportProgress("### [FAILED][Pig][Smoke] Pig Smoke test Failed ####")
