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
from beaver.component.tez import Tez
from beaver import util
from beaver import configUtils
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')
LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tez')
TEST_NAME = "TezRollingUpgrade"


class ruTez:
    _hdfsInputList = []
    _hdfsOutputList = []
    _background_job_appId = ''
    _queue = "tez"

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Create 5 input datasets for TestOrderedWordCount
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        logger.info("*** Start background job setup for Tez ***")
        Machine.rm(user=HADOOPQA_USER, host=None, filepath=LOCAL_WORK_DIR, isdir=True)
        os.mkdir(LOCAL_WORK_DIR)
        for i in range(0, 4, 1):
            inputDirName = "HDFS_INPUT%d" % i
            inputDirPath = os.path.join(LOCAL_WORK_DIR, inputDirName)
            HadoopJobHelper.runCustomWordWriter(LOCAL_WORK_DIR, inputDirPath, 10, 400, 10000)

            hdfsInputDir = "/user/%s/Input%d" % (HADOOPQA_USER, i)
            hdfsOutputDir = "/user/%s/output%d" % (HADOOPQA_USER, i)

            #In case already present, delete the input directory
            HDFS.deleteDirectory(hdfsInputDir)

            HDFS.createDirectory(hdfsInputDir)
            HDFS.deleteDirectory(hdfsOutputDir)

            HDFS.copyFromLocal(inputDirPath, hdfsInputDir)
            cls._hdfsInputList.append(hdfsInputDir + "/" + inputDirName)
            cls._hdfsOutputList.append(hdfsOutputDir)
            logger.info("Created data for input %d", i)
        logger.info("*** End background job setup for Tez ***")

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        #No setup step required here
        return

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running TestOrderedWordCount tez job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("*** Start background job for Tez ***")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("###  Starting background job for Tez  ####")

        #Sleep for 180 seconds between DAGs
        sleepInterval = 180
        cmdLineArgs = ""
        for i in range(0, 4, 1):
            cmdLineArgs += cls._hdfsInputList[i] + " " + cls._hdfsOutputList[i] + " "
        logger.info(cmdLineArgs)
        Tez.runTezExampleJar(
            "testorderedwordcount \"-DUSE_TEZ_SESSION=true\" \"-Dtez.queue.name=%s\" \"-DINTER_JOB_SLEEP_INTERVAL=%d\" \"-DRETAIN_STAGING_DIR=true\" %s "
            % (cls._queue, sleepInterval, cmdLineArgs),
            runInBackground=True
        )
        interval = 300
        while (cls._background_job_appId == '' or cls._background_job_appId == None and interval > 0):
            logger.info("Trying to get appID..")
            time.sleep(10)
            interval = interval - 10
            cls._background_job_appId = YARN.getAppIDFromAppName("OrderedWordCountSession", state="RUNNING")
        logger.info("*******************appID=%s" % cls._background_job_appId)

        logger.info("*** End background job for Tez ***")
        return 1

    @classmethod
    def validate_correct_tar_used(cls, output):
        '''
        Validates that correct tez tar ball is used
        :param output: Console output from application
        :return: True if corret tar ball is used otherwise returns False
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpRelease
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        currVersion = hdpRelease.getCurrentRelease("hadoop-client")
        pattern = "Using tez.lib.uris value from configuration: /hdp/apps/%s/tez/tez.tar.gz" % currVersion
        m = re.search(pattern, output)
        if m:
            logger.info("Tez application is using correct version")
            UpgradePerNode.reportProgress(
                "### [PASSED][Tez][Smoke] Tez smoke test is using correct version. Passed successfully ####"
            )
            return True
        else:
            logger.info("Tez application is not using correct version")
            UpgradePerNode.reportProgress(
                "### [FAILED][Tez][Smoke] Tez smoke test is not using correct version. Validation Failed ####"
            )
            return False

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run MRRSleep tez job
        :param smoketestnumber: Used for unique output log location
        '''
        logger.info("***** Starting MRRSleep job in Tez ******")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("###  Starting smoke job for tez  ####")
        numNodes = len(MAPRED.getTasktrackers())
        # Post a sleep job
        (exit_code, stdout) = Tez.runTezExampleJar(
            'mrrsleep "-Dmapreduce.framework.name=yarn-tez" "-Dtez.queue.name=%s" -m %d -r %d -ir 3 -irs 5 -mt 10 -rt 10'
            % (cls._queue, numNodes, numNodes)
        )
        # Make sure succeeded
        if exit_code == 0:
            UpgradePerNode.reportProgress("### [PASSED][Tez][Smoke] smoke job for tez passed  ####")
        else:
            UpgradePerNode.reportProgress("### [FAILED][Tez][Smoke] smoke job for tez failed  ####")
        time.sleep(10)
        logger.info("***** Completed MRRSleep job in Tez ******")
        UpgradePerNode.reportProgress("###  smoke test finished for tez  ####")
        cls.validate_correct_tar_used(stdout)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup directories for long running tez job
        '''
        for input in cls._hdfsInputList:
            HDFS.deleteDirectory(input)
        for output in cls._hdfsOutputList:
            HDFS.deleteDirectory(output)
        Machine.rm(user=HADOOPQA_USER, host=None, filepath=LOCAL_WORK_DIR, isdir=True)
        logger.info("**** Completed background job teardown for Tez ****")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Verify long running background job after it finishes
        :return:
        '''
        ruAssert("Tez", YARN.getAppFinalStateFromID(cls._background_job_appId) == 'SUCCEEDED')
        for output in cls._hdfsOutputList:
            ruAssert("Tez", HDFS.fileExists(output + '/part*'))
        logger.info("**** Verified long running job for Tez ****")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upload tez tar ball to HDFS
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("**** Tez has no master component. Thus no upgrade will happen ****")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        #Nothing needs to be done here.
        logger.info("**** Tez has no slave component. Thus no upgrade will happen ****")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("###  Starting  cli smoke job for tez  ####")
        logger.info("***** Starting MRRSleep job in Tez with setting env ******")
        numNodes = len(MAPRED.getTasktrackers())
        # Post a sleep job
        (exit_code, stdout) = Tez.runTezExampleJar(
            'mrrsleep "-Dmapreduce.framework.name=yarn-tez" "-Dtez.queue.name=%s" -m %d -r %d -ir 3 -irs 5 -mt 10 -rt 10'
            % (cls._queue, numNodes, numNodes),
            env=env
        )
        # Make sure succeeded
        if exit_code == 0:
            UpgradePerNode.reportProgress("### [PASSED][Tez][ClientSmoke] cli smoke job for tez passed ####")
        else:
            UpgradePerNode.reportProgress("### [FAILED][Tez][ClientSmoke] cli smoke job for tez failed ####")
        time.sleep(10)
        logger.info("***** Completed MRRSleep job in Tez ******")
        UpgradePerNode.reportProgress("### cli smoke job for tez finished ####")
        cls.validate_correct_tar_used(stdout)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        cls.run_smoke_test("001", config=None)
