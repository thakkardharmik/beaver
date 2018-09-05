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
import httplib
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruYARN:
    _HS_test_appID = None
    _runningMapSleepTime = 300 * 1000
    _runningReduceSleepTime = 200 * 1000
    _hdfs_input = "/user/" + Config.get('hadoop', 'HADOOPQA_USER') + "/rollingUpgradeYarn"
    _hdfs_output = _hdfs_input + "/output"
    _hdfs_output_verify = _hdfs_input + "/outputVerify"
    _multi_version_signal_file_dir = _hdfs_output
    _background_job_appId = None
    _background_job_jobId = None
    _queue = "yarn"
    _jobArgs = {"mapred.job.queue.name": _queue, "mapred.task.timeout": "0"}
    _shortStreamingName = "yarn_verify_classpath_streaming_job"

    @classmethod
    def launchMultipleSleepJobs(cls, numJobs, mapSleepTime=1000, reduceSleepTime=1000, config=None):
        '''
        Function to Launch multiple sleep jobs
        :param numJobs: number of sleep jobs want to run
        :param mapSleepTime: Map sleep time
        :param reduceSleepTime: Reduce sleep time
        :param config: expected Configuration location
        :return: jobIDs
        '''
        jobIds = []
        # Create jobs
        i = 0
        for i in range(0, numJobs):
            jobclientFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "JobClient_output.log")
            HadoopJobHelper.runSleepJob(
                numOfMaps=1,
                numOfReduce=1,
                mapSleepTime=mapSleepTime,
                reduceSleepTime=reduceSleepTime,
                extraJobArg=cls._jobArgs,
                runInBackground=False,
                config=config,
                directoutput=True,
                outputFile=jobclientFile
            )
            f = open(jobclientFile)
            text = f.read()
            f.close()
            currJobId = YARN.getAppAndJobIdsFromConsole(text)[1][0]
            jobIds.append(currJobId)
        # Join jobs

        for job in jobIds:
            ruAssert("YARN", MAPRED.isJobSucceed(job))
        return jobIds

    @classmethod
    def run_JHS_test(cls, config=None):
        '''
        Runs Job History server Test before upgrades starts
        :param config: expected configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][JHSTest] Running JHS test before starting upgrade")
        logger.info("**** Running JHS test before starting upgrade ****")
        jobIds = cls.launchMultipleSleepJobs(numJobs=1, config=config)
        ruAssert("YARN", len(jobIds) == 1, "[JHSTest] More than one history server tests launched!")
        cls._HS_test_appID = jobIds.pop()
        logger.info("**** JHS test Finished ****")
        UpgradePerNode.reportProgress("[INFO][YARN][JHSTest] Running JHS test finished ")

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        cls.run_JHS_test(config=config)
        logger.info("**** Run Yarn long running application setup ****")
        HDFS.createDirectory(cls._hdfs_input, None, "777", False)
        #touch a fake file to trick hadoop streaming
        HDFS.touchz(cls._hdfs_input + "/input.txt")
        HDFS.deleteDirectory(cls._hdfs_output, user=Config.get('hadoop', 'HADOOPQA_USER'))
        if runSmokeTestSetup:
            logger.info("**** Running HDFS Smoke Test Setup ****")
            cls.smoke_test_setup()

    @classmethod
    def checkClasspathVersion(cls, Version_Num, config=None):
        Local_Test_dir = os.path.join(Config.getEnv("WORKSPACE"), "tests", "rolling_upgrade", "yarn")
        Multi_Version_App_Dir = os.path.join(Local_Test_dir, "data")
        Mapper = "data/versionVerifyMapper.py"
        Reducer = "data/versionVerifyReducer.py"
        Verify_File_Name = "test.txt"
        Verify_Test_File = os.path.join(Multi_Version_App_Dir, Verify_File_Name)
        # Set up env
        mapred_app_path = MAPRED.getConfigValue("mapreduce.application.framework.path", None)
        mapred_classpath = MAPRED.getConfigValue("mapreduce.application.classpath", None)
        env = {
            "mapreduce.application.framework.path": mapred_app_path,
            "mapreduce.application.classpath": mapred_classpath
        }
        verifyInput = cls._hdfs_input + "/verify"
        HDFS.createDirectory(verifyInput, None, "777", False)
        # Copy template files for the verifier streaming job
        templateFile = open(Verify_Test_File, 'w')
        templateFile.write(Version_Num)
        templateFile.close()
        HDFS.copyFromLocal(Verify_Test_File, verifyInput, user=Config.get('hadoop', 'HADOOPQA_USER'))
        # Submit the special streaming job
        shortStreamingId = HadoopJobHelper.runStreamJob(
            Mapper,
            Reducer,
            verifyInput,
            cls._hdfs_output_verify,
            files=Multi_Version_App_Dir,
            config=config,
            extraJobArg=cls._jobArgs,
            env=env,
            proposedJobName=cls._shortStreamingName
        )
        MAPRED.waitForJobDoneOrTimeout(shortStreamingId, timeoutInSec=180)
        # Make sure task succeeded
        #assert YARN.getAppFinalStateFromID(appId) == 'SUCCEEDED'

        # Check result content
        retVal, checkContent = HDFS.cat(cls._hdfs_output_verify + '/part-00000')
        logger.info("CHECK CLASSPATH VERSION OUTPUT")
        logger.info(retVal)
        logger.info(checkContent)
        ruAssert("YARN", retVal == 0)
        ruAssert("YARN", 'True' in checkContent, "[VersionVerify] Stream job returns false: " + checkContent)
        #assert retVal == 0
        #assert 'True' in checkContent, "Stream job returns false: " + checkContent
        #assert 'False' not in checkContent, "Stream job returns false: " + checkContent
        HDFS.deleteDirectory(cls._hdfs_output_verify, user=Config.get('hadoop', 'HADOOPQA_USER'))

    @classmethod
    def smoke_test_setup(cls):
        logger.info("No smoke test setup is required")

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][BGJob] Starting background job for Yarn ")
        Local_Test_dir = os.path.join(Config.getEnv("WORKSPACE"), "tests", "rolling_upgrade", "yarn")
        Multi_Version_App_Dir = os.path.join(Local_Test_dir, "data")
        HDFS.deleteDirectory(cls._hdfs_output, user=Config.get('hadoop', 'HADOOPQA_USER'))
        Mapper = "data/mvMapper.py"
        Reducer = "data/mvReducer.py"
        # Launch job
        (jobID, STREAMING_APP_ID) = HadoopJobHelper.checkAndRunStreamJob(
            Mapper,
            Reducer,
            cls._hdfs_input,
            cls._hdfs_output,
            files=Multi_Version_App_Dir,
            config=config,
            forRU=True,
            extraJobArg=cls._jobArgs,
            env=None,
            sleepTimeAfterJobSubmission=60
        )
        cls._background_job_appId = STREAMING_APP_ID
        cls._background_job_jobId = jobID
        logger.info("Background job started, application ID: " + STREAMING_APP_ID + " job ID: " + jobID)
        logger.info("Start second long running job for Yarn")
        num_sec_bkjob = cls.run_second_background_job(config=config)
        return 2

    @classmethod
    def run_second_background_job(cls, runSmokeTestSetup=False, config=None):
        '''
        Runs long running distributed shell Yarn job after RM upgrade 
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][BGJob2] Starting second background job for Yarn ")
        Local_Test_dir = os.path.join(Config.getEnv("WORKSPACE"), "tests", "rolling_upgrade", "yarn")
        Multi_Version_App_Dir = os.path.join(Local_Test_dir, "data")
        Mapper = os.path.join(Multi_Version_App_Dir, "mvMapper.py")
        Reducer = os.path.join(Multi_Version_App_Dir, "mvReducer.py")
        HadoopJobHelper.runSleepJob(
            numOfMaps=1,
            numOfReduce=1,
            mapSleepTime="100000",
            reduceSleepTime="100",
            extraJobArg=cls._jobArgs,
            runInBackground=True,
            config=config,
            directoutput=False
        )
        #(jobID, STREAMING_APP_ID) = HadoopJobHelper.checkAndRunStreamJob(Mapper, Reducer, cls._hdfs_input, cls._hdfs_output, files = Multi_Version_App_Dir, config = config, forRU=True)
        logger.info("Second Long Running job for YARN has started")
        return 1

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        HDFS.deleteDirectory(cls._hdfs_input, user=Config.get('hadoop', 'HADOOPQA_USER'))
        HDFS.deleteDirectory(cls._hdfs_output, user=Config.get('hadoop', 'HADOOPQA_USER'))

    @classmethod
    def stopYarnLongRunningJob(cls):
        '''
        Stop Long running Yarn Dshell Job
        '''
        logger.info("**** Touch the file ****")
        HDFS.createDirectory(cls._multi_version_signal_file_dir, user=None, perm="777", force=False)
        multi_version_signal_file_path = cls._multi_version_signal_file_dir + "/signal"
        HDFS.touchz(multi_version_signal_file_path)
        #YARN.waitForApplicationFinish(cls._background_job_appId)
        time.sleep(2)
        logger.info("**** Done checking status ****")

    @classmethod
    def verifyLongRunningJob(cls, config=None):
        '''
        Verify long running background job after it finishes
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        retVal, checkContent = HDFS.cat(cls._hdfs_output + '/part-00000')
        #assert retVal == 0
        if retVal == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][YARN][BGJobCheck] verifyLongRunning Job for Yarn, retVal = 0. Successful check "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][YARN][BGJobCheck] verifyLongRunning Job for Yarn, retVal != 0. Failed check "
            )
        #assert 'true' in checkContent, "Stream job returns false: " + checkContent
        if 'true' in checkContent:
            UpgradePerNode.reportProgress(
                "[PASSED][YARN][BGJobCheck] verifyLongRunning Job for Yarn, true in checkContent. Successful check "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][YARN][BGJobCheck] verifyLongRunning Job for Yarn, true not in checkContent. Failed check  "
            )

        #verify application's attempt doesn't increase and no failed tasks.
        appID = cls._background_job_appId
        jobID = cls._background_job_jobId
        # temporarily skipping check
        #assert YARN.getNumAttemptsForApp(appID) == 1
        #YARN.verifyMRTasksCount(jobID, appID, 0, skipAssert=True)

        from beaver.component.rollingupgrade.ruCommon import hdpRelease
        Version_Num = hdpRelease.getCurrentRelease("hadoop-client")
        cls.checkClasspathVersion(Version_Num, config)

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][Smoke]  Starting smoke test for Yarn")
        numNodes = len(MAPRED.getTasktrackers())
        # run sleep job
        (exitCode, stdout) = HadoopJobHelper.runSleepJob(
            numOfMaps=numNodes,
            numOfReduce=numNodes,
            mapSleepTime=10,
            reduceSleepTime=10,
            extraJobArg=cls._jobArgs,
            runInBackground=False,
            config=config
        )
        # Make sure succeeded
        ruAssert("YARN", exitCode == 0, "[Smoke] YARN smoke failed")
        UpgradePerNode.reportProgress("[INFO][YARN][Smoke] Smoke test for Yarn finished ")
        cls.run_rest_apis_test()

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run sleep Job passing env variables
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][SmokeClient]  Starting CLI test for Yarn ")
        jobCmd = "jar " + MAPRED.sleepJobJar(
        ) + " sleep -Dmapred.job.queue.name=%s  -m 1 -r 1 -mt 10 -rt 10 " % (cls._queue)
        exit_code, stdout = Hadoop.run(jobCmd, env=env)
        ruAssert("YARN", exit_code == 0, "[SmokeClient] Yarn smoketest failed")
        UpgradePerNode.reportProgress("[INFO][YARN][SmokeClient] CLI test for Yarn finished ")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Yarn Master services: RM and JHS
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Job history server upgrade started ")
        logger.info("**** Begin history server upgrade ****")
        MAPRED.stopHistoryserver()
        historyNode = MAPRED.getHistoryserver()
        # TODO use hdp-select script to upgrade JHS
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hadoop-mapreduce-historyserver", version, historyNode)
        MAPRED.startHistoryserver(config=config)
        time.sleep(5)
        logger.info("**** End history server upgrade, begin timeline server upgrade ****")
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade]Job history server upgrade finished ")
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Timeline server upgrade started  ")

        ytsNode = YARN.getATSHost()
        YARN.stopATSServer(ytsNode)
        hdpSelect.changeVersion("hadoop-yarn-timelineserver", version, ytsNode)
        YARN.startATSServer(config=config)
        time.sleep(5)

        logger.info("**** End timeline server upgrade, begin rm upgrade ****")
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Timeline server upgrade finished  ")
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Resource manager upgrade started  ")
        if YARN.isHAEnabled():
            nodes = []
            nodes.append(YARN.getRMHostByState('standby'))
            nodes.append(YARN.getRMHostByState('active'))
            for node in nodes:
                YARN.resetResourceManager('stop', config=config, host=node)
                time.sleep(2)
                hdpSelect.changeVersion("hadoop-yarn-resourcemanager", version, node)
                YARN.resetResourceManager('start', config=config, host=node)
                time.sleep(5)
        else:
            node = MAPRED.getJobtracker()
            MAPRED.stopJobtracker()
            time.sleep(2)
            # TODO use hdp-select script to upgrade JHs
            hdpSelect.changeVersion("hadoop-yarn-resourcemanager", version, node)
            MAPRED.startJobtracker(config=config)
            time.sleep(5)
        logger.info("*** Ending RM upgrade ****")
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Resource manager Upgrade Finished")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades Yarn slave services : NM
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Nodemanager Upgrade for %s started " % node)
        logger.info("**** Beginning upgrade for nodemanager on %s ****" % node)
        MAPRED.stopTasktrackers(nodes=[node])
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hadoop-yarn-nodemanager", version, node)

        # BUG-27328
        # make sure we set the linux container executor permissions
        if Hadoop.isSecure() and not Machine.isWindows():
            container_exec_cfg = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), 'container-executor.cfg')
            container_exec = os.path.join(
                Config.get('hadoop', 'YARN_HOME').replace("client", "nodemanager"), 'bin', 'container-executor'
            )
            cmd="chown root:hadoop %s %s ; chmod 400 %s ; chmod 6050 %s" % \
                (container_exec_cfg, container_exec, container_exec_cfg, container_exec)
            Machine.runas(
                Machine.getAdminUser(),
                cmd,
                host=node,
                cwd=None,
                env=None,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )

        MAPRED.startTasktrackers(config, nodes=[node])
        logger.info("**** Ending upgrade for nodemanager on %s ****" % node)
        UpgradePerNode.reportProgress("[INFO][YARN][Upgrade] Nodemanager Upgrade on %s Finished " % node)

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Yarn Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade] Job history server downgrade started ")
        logger.info("**** Begin history server downgrade ****")
        MAPRED.stopHistoryserver()
        historyNode = MAPRED.getHistoryserver()
        # TODO use hdp-select script to upgrade JHS
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hadoop-mapreduce-historyserver", version, historyNode)
        MAPRED.startHistoryserver(config=config)
        time.sleep(5)
        logger.info("**** End history server downgrade, begin timeline server downgrade ****")
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade] Job history server downgrade finished ")
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade] Timeline server downgrade started ")
        ytsNode = YARN.getATSHost()
        YARN.stopATSServer(ytsNode)
        hdpSelect.changeVersion("hadoop-yarn-timelineserver", version, ytsNode)
        YARN.startATSServer(config=config)
        time.sleep(5)
        logger.info("**** End timeline server downgrade, begin rm upgrade ****")
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade]Timeline server downgrade finished  ")
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade] Resource manager downgrade started  ")
        if YARN.isHAEnabled():
            nodes = []
            nodes.append(YARN.getRMHostByState('standby'))
            nodes.append(YARN.getRMHostByState('active'))
            for node in nodes:
                YARN.resetResourceManager('stop', config=config, host=node)
                time.sleep(2)
                hdpSelect.changeVersion("hadoop-yarn-resourcemanager", version, node)
                YARN.resetResourceManager('start', config=config, host=node)
                time.sleep(5)
        else:
            node = MAPRED.getJobtracker()
            MAPRED.stopJobtracker()
            time.sleep(2)
            # TODO use hdp-select script to upgrade JHs
            hdpSelect.changeVersion("hadoop-yarn-resourcemanager", version, node)
            MAPRED.startJobtracker(config=config)
            time.sleep(5)
        logger.info("*** Ending RM downgrade ****")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade Yarn slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade]Nodemanager downgrade for %s started " % node)
        cls.upgrade_slave(version, node, config)
        UpgradePerNode.reportProgress("[INFO][YARN][Downgrade] Nodemanager downgrade for %s finished " % node)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all slaves are upgraded for yarn
        :return:
        '''
        logger.info("TODO : need to implement this function")

    @classmethod
    def run_rest_apis_test(self):
        '''
        Run checks to make sure the REST interfaces for the RM, NM, JHS and TimelineServer are up
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "[INFO][YARN][RestTest] Testing REST interfaces for RM, NM, JHS and TimelineServer "
        )
        logger.info("**** Testing REST interfaces for RM, NM, JHS and TimelineServer ****")
        hostUrlMap = {}
        hostUrlExpectedStatusCode = {}
        rmAddress = YARN.getResourceManagerWebappAddress()
        rmPort = None
        nmPort = None
        jhsAddress = None
        atsAddress = YARN.get_ats_web_app_address()
        scheme = "http"

        if YARN.isHttpsEnabled():
            scheme = "https"
            if rmAddress.startswith("https://"):
                rmAddress = rmAddress[len("https://"):]
            nmPort = YARN.getNodeManagerWebappHttpsPort()
            jhsAddress = MAPRED.getHistoryServerWebappHttpsAddress()
            if jhsAddress.startswith("https://"):
                jhsAddress = jhsAddress[len("https://"):]
            if atsAddress.startswith("https://"):
                atsAddress = atsAddress[len("https://"):]
        else:
            if rmAddress.startswith("http://"):
                rmAddress = rmAddress[len("http://"):]
            nmPort = YARN.getNodeManagerWebappPort()
            jhsAddress = MAPRED.getHistoryServerWebappAddress()
            if jhsAddress.startswith("http://"):
                jhsAddress = jhsAddress[len("http://"):]
            if atsAddress.startswith("http://"):
                atsAddress = atsAddress[len("http://"):]

        rmPort = rmAddress.split(":")[1]
        hostUrlMap[rmAddress] = ["/ws/v1/cluster/info"]
        hostUrlExpectedStatusCode[rmAddress] = 200

        for nm in MAPRED.getTasktrackers():
            host = "%s:%s" % (nm, nmPort)
            hostUrlMap[host] = ["/ws/v1/node/info"]
            hostUrlExpectedStatusCode[host] = 200
        hostUrlMap[jhsAddress] = ["/ws/v1/history/info"]
        hostUrlExpectedStatusCode[jhsAddress] = 200
        hostUrlMap[atsAddress] = ["/ws/v1/timeline"]
        hostUrlExpectedStatusCode[atsAddress] = 200

        for host in hostUrlMap.keys():
            urls = hostUrlMap[host]
            for url in urls:
                fetch_url = scheme + "://" + host + url
                (return_code, data, headers) = util.query_yarn_web_service(
                    fetch_url, Config.get('hadoop', 'HADOOPQA_USER'), also_check_modified_config_for_spnego=False
                )
                if int(return_code) == hostUrlExpectedStatusCode[host]:
                    UpgradePerNode.reportProgress(
                        "[PASSED][YARN][RestTest] Got %s status code from url %s. Passed " % (return_code, fetch_url)
                    )
                else:
                    UpgradePerNode.reportProgress(
                        "[FAILED][YARN][RestTest]Got %s status code from url %s. Failed " % (return_code, fetch_url)
                    )
                #assert int(return_code) == hostUrlExpectedStatusCode[host], "Got %s status code from url %s, expected %d" % (return_code, fetch_url, hostUrlExpectedStatusCode[host])
