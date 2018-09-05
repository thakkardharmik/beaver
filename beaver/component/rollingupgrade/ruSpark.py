#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import time
import logging
from beaver.component.hadoop import HDFS, YARN
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruSpark(object):
    def __init__(self):
        pass

    HDFS_CLUSTER_INPUT_DIR = "/tmp/HDFSClusterStreamingInput"
    appId_hdfs_cluster = None
    local_hdfs_cluster = None
    hdfs_thread = None

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        logger.info("runSmokeTestSetup = %s, config = %s", runSmokeTestSetup, config)
        HDFS.createDirectory(cls.HDFS_CLUSTER_INPUT_DIR)

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("No setup needed for spark smoke test.")

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("runSmokeTestSetup = %s, config = %s", runSmokeTestSetup, config)
        from beaver.component.spark_longRunning_event_generator import GenerateHDFSWordcountEvents
        from beaver.component.spark_ha import Spark_Ha
        from tools import stacktracer
        stacktracer.trace_start("/tmp/trace_ru_spark.txt", interval=5, auto=True)
        cls.appId_hdfs_cluster, cls.local_hdfs_cluster = Spark_Ha.start_LongRunning_HDFS_stream_job(
            inputDir=cls.HDFS_CLUSTER_INPUT_DIR, num_executor=4, mode="yarn-cluster", inBackground=True
        )
        cls.hdfs_thread = GenerateHDFSWordcountEvents(
            hdfs_input_dir=cls.HDFS_CLUSTER_INPUT_DIR, interval=900, times=40
        )
        cls.hdfs_thread.start()
        return 1

    @classmethod
    def validate_ApplicationEntry(cls, appId, appName, appUser, mode="yarn-client", url=None):
        '''
        Validate Application entry
        :param entities: Its output from getCorrectApplicationJsonData
        :param appId: Application Id
        :param appName: Application name
        :param appUser: Application user
        :return:
        '''
        from beaver.component.spark import Spark
        if not url:
            entities = Spark.getCorrectApplicationJsonData(appId)
        else:
            entities = Spark.getCorrectApplicationJsonData(appId, url, gatherAppSpecificJson=False)

        logger.info("***** entities *****")
        logger.info(entities)
        logger.info("********************")

        if mode == "yarn-cluster":
            ruAssert(
                "Spark", entities["entity"] == YARN.createAttemptIdFromAppId(appId, "1"),
                "[Smoke] attemptid entity not found in ATS"
            )
        else:
            ruAssert("Spark", entities["entity"] == appId, "[Smoke] appid entity not found in ATS")
        ruAssert("Spark", entities["domain"] == "DEFAULT", "[Smoke] domain is not default")
        ruAssert("Spark", entities["entitytype"] == "spark_event_v01", "[Smoke] entitytype is not spark_event_v01")
        ruAssert(
            "Spark", entities["primaryfilters"]["endApp"] == ['SparkListenerApplicationEnd'],
            "[Smoke] endapp event missing from ats"
        )
        ruAssert(
            "Spark", entities["primaryfilters"]["startApp"] == ['SparkListenerApplicationStart'],
            "[Smoke] startapp event missing from ats"
        )
        if not Machine.isLinux() and appName == "Spark Pi":
            ruAssert(
                "Spark", entities["otherinfo"]["appName"] == "SparkPi",
                "[Smoke] otherinfo -> appname is missing from ats"
            )
        else:
            ruAssert(
                "Spark", entities["otherinfo"]["appName"] == appName,
                "[Smoke] otherinfo -> appname is missing from ats"
            )

        ruAssert(
            "Spark", entities["otherinfo"]["appUser"] == appUser, "[Smoke] otherinfo -> appuser is missing from ats"
        )
        ruAssert(
            "Spark", Spark.matchparamater(entities["otherinfo"]["startTime"], "[0-9]{13}"),
            "[Smoke] otherinfo -> starttime is missing from ats"
        )
        ruAssert(
            "Spark", Spark.matchparamater(entities["otherinfo"]["endTime"], "[0-9]{13}"),
            "[Smoke] otherinfo -> endtime is missing from ats"
        )

    @classmethod
    def run_smoke_test(cls, config=None):
        '''
        Run smoke test for spark
        '''
        logger.info("config = %s", config)
        from beaver.component.spark import Spark
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Spark][Smoke] Smoke test for Spark started ")
        exit_code, _ = Spark.submitSparkApplication("org.apache.spark.examples.SparkPi", "yarn-cluster", "3")
        if exit_code != 0:
            UpgradePerNode.reportProgress("[FAILED][Spark][Smoke] SparkPi Smoke Test Failed in Yarn-cluster mode")
            return

        exit_code, stdout2 = Spark.submitSparkApplication("org.apache.spark.examples.SparkPi", "yarn-client", "3")
        if exit_code != 0:
            UpgradePerNode.reportProgress("[FAILED][Spark][Smoke] SparkPi Smoke Test Failed in Yarn-client mode")
            return

        if Machine.isWindows():
            appName_pi = "SparkPi"
        else:
            appName_pi = "Spark Pi"
        HADOOP_QA = Config.get('hadoop', 'HADOOPQA_USER')
        appId = YARN.getApplicationIDFromStdout(stdout2).strip()
        logger.info("Validate http://<host>:<port>/ws/v1/timeline/spark_event_v01/<appId>")
        Spark.getSparkATSAppUrl(appId)
        time.sleep(30)
        # Spark-ats check. We will enable it once Ambari enables Spark-ATS by default
        #cls.validate_ApplicationEntry(appId, appName_pi, HADOOP_QA, mode="yarn-client", url=url)
        Spark.hitSparkURL()
        time.sleep(50)
        result_HS_completeApp = Spark.validateSparkHSCompletedApps(appId, appName_pi, HADOOP_QA)
        if not result_HS_completeApp:
            UpgradePerNode.reportProgress("[FAILED][Spark][Smoke] SparkPi Spark HS complete App Validation failed")
            return
        result_HS_Jobs = Spark.validateSparkHSJobs(appId, "1/1", "3/3")
        if not result_HS_Jobs:
            UpgradePerNode.reportProgress("[FAILED][Spark][Smoke] SparkPi Spark HS Job page validation failed")
            return

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        HDFS.deleteDirectory(cls.HDFS_CLUSTER_INPUT_DIR)

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.spark_ha import Spark_Ha
        from tools import stacktracer
        stacktracer.trace_stop()
        if cls.hdfs_thread.is_alive():
            cls.hdfs_thread.join()
        YARN.killApplication(cls.appId_hdfs_cluster)
        time.sleep(15)
        patterns = [
            "(Spark,1)", "(,1)", "(world,1)", "(Word,1)", "(hello,1)", "(count,1)", "(HDFS,1)", "(application,1)",
            "(Testing,1)"
        ]
        Spark_Ha.validate_HDFS_stream_job(
            cls.appId_hdfs_cluster,
            "yarn-cluster",
            patterns=patterns,
            expected_count=40,
            clientfile=cls.local_hdfs_cluster
        )

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("Ambari upgradin the services version = %s, config = %s", version, config)

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("No slave for spark. version=%s, node = %s, config=%s", version, node, config)

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("upgrade downgrade is managed by Ambari version = %s, config = %s", version, config)

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info(
            "upgrade downgrade is managed by Ambari version = %s, node = %s, config = %s", version, node, config
        )

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("no need to add extra client side smoke test config=%s, env = %s", config, env)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are
        upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("no need to add smoke test after slave restart")
