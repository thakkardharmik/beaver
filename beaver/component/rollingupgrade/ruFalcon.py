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
from beaver.config import Config
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruFalcon:

    _base_falcon_dir = None
    _local_workspace = Config.getEnv("WORKSPACE")
    _job_user = Config.getEnv("USER")
    _falcon_user = Config.get("falcon", "FALCON_USER")
    _job_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Upload Data to HDFS before Upgrade starts
        Creates /user/hrt_qa/falcon/ dir on HDFS
        Upload demo files to /user/hrt_qa/falcon
        '''
        logger.info("Falcon - starting background job setup")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][BGJobSetup] starting Falcon background job setup")
        from beaver.component.hadoop import HDFS
        if not cls._base_falcon_dir:
            cls._base_falcon_dir = '/user/%s/falcon' % cls._job_user
        exit_code, stdout = HDFS.createDirectory(cls._base_falcon_dir, user=cls._job_user, perm=777, force=True)
        ruAssert("Falcon", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')
        HDFS.copyFromLocal(
            os.path.join(cls._local_workspace, "tests", "rolling_upgrade", "falcon", "demo"),
            cls._base_falcon_dir,
            user=cls._job_user
        )

        ## Create dirs for falcon clusters
        exit_code, stdout = HDFS.createDirectory(
            "/apps/falcon/primaryCluster/staging", user=cls._falcon_user, perm=777, force=True
        )
        ruAssert("Falcon", exit_code == 0, '[BGJobSetup] could not create staging dir on hdfs ')
        exit_code, stdout = HDFS.createDirectory(
            "/apps/falcon/primaryCluster/working", user=cls._falcon_user, perm=755, force=True
        )
        ruAssert("Falcon", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')
        exit_code, stdout = HDFS.createDirectory(
            "/apps/falcon/backupCluster/staging", user=cls._falcon_user, perm=777, force=True
        )
        ruAssert("Falcon", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')
        exit_code, stdout = HDFS.createDirectory(
            "/apps/falcon/backupCluster/working", user=cls._falcon_user, perm=755, force=True
        )
        ruAssert("Falcon", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')

        ## Create cluster entities.
        cls.createClusterEntities("USWestOregon", "oregonHadoopCluster", "primaryCluster")
        cls.createClusterEntities("USEastVirginia", "virginiaHadoopCluster", "backupCluster")

        if runSmokeTestSetup:
            logger.info("**** Running Falcon Smoke Test Setup ****")
            cls.smoke_test_setup()

        logger.info("Falcon - completed background job setup")
        return

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("Falcon smoke test setup is same as background job setup. Nothing to do")
        return

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("Falcon - start running background job")

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[PASSED][Falcon][BGJob] starting Falcon background jobs")

        BACKGROUND_JOB_DIR = os.path.join(
            cls._local_workspace, "tests", "rolling_upgrade", "falcon", "falconChurnDemo"
        )

        # Add clusters
        clusterXml = os.path.join(cls._local_workspace, "primaryCluster.xml")
        logger.info("Content of cluster xml %s" % clusterXml)
        f = open(clusterXml, 'r')
        logger.info(f.read())
        f.close()
        CMD = " entity -type cluster -submit -file " + clusterXml
        cls.runCommand(cls._job_user, CMD)
        clusterXml = os.path.join(cls._local_workspace, "backupCluster.xml")
        CMD = " entity -type cluster -submit -file " + clusterXml
        cls.runCommand(cls._job_user, CMD)

        # Add feed
        feedXml = os.path.join(BACKGROUND_JOB_DIR, "rawEmailFeed.xml")
        logger.info("Content of feed xml %s" % feedXml)
        f = open(feedXml, 'r')
        logger.info(f.read())
        f.close()
        CMD = " entity -type feed -submit -file " + feedXml
        cls.runCommand(cls._job_user, CMD)

        # Add Process
        processXml = os.path.join(BACKGROUND_JOB_DIR, "emailIngestProcess.xml")
        logger.info("Content of process xml %s" % processXml)
        f = open(processXml, 'r')
        logger.info(f.read())
        f.close()
        CMD = " entity -type process -submit -file " + processXml
        cls.runCommand(cls._job_user, CMD)

        # Schedule Feed and Process
        CMD = " entity -type feed -schedule -name rawEmailFeed"
        cls.runCommand(cls._job_user, CMD)
        CMD = " entity -type process -schedule -name rawEmailIngestProcess"
        cls.runCommand(cls._job_user, CMD)

        cls._job_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")
        logger.info("Falcon - completed running background job")

        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        logger.info("Falcon - start running smoke test")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][Smoke] starting Falcon smoke test")

        cls.runCommand(cls._falcon_user, "admin -version")
        # QE-4347: Do not run the list call as smoke is run before long running job is called.
        # cls.runCommand(cls._job_user,
        #                   " instance -type process -name rawEmailIngestProcess -list ")
        logger.info("Falcon - finished running smoke test")
        UpgradePerNode.reportProgress("[INFO][Falcon][Smoke] finishing Falcon smoke test")
        return

    @classmethod
    def stopFalconLongRunningJob(cls):
        logger.info("Falcon - Begin function stopFalconLongRunningJob")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][BGJobStop]Stopped/deleting Falcon background jobs started")

        CMD = " instance -type process -name rawEmailIngestProcess " \
              " -list -start 2016-07-25T00:00Z -end 2018-07-25T00:00Z " \
              " -filterBy STARTEDAFTER:" + cls._job_start_time + " -numResults 1000"
        exitcode, output = cls.runCommand(cls._job_user, CMD)

        success_count = output.count("SUCCEEDED")
        fail_count = output.count("KILLED")

        ruAssert(
            "Falcon", success_count > 0 and fail_count == 0,
            "[BGJobCheck] Long running job failed with " + str(fail_count) + " FAILED instances"
        )
        try:
            from beaver.component.falcon import Falcon
        except ImportError:
            ## Import fails when Falcon is not installed on this machine. Nothing to do
            return

        # Delete entities
        cls.runCommand(cls._job_user, " entity -type process -delete -name rawEmailIngestProcess")
        cls.runCommand(cls._job_user, " entity -type feed -delete -name rawEmailFeed")
        cls.runCommand(cls._job_user, " entity -type cluster -delete -name primaryCluster")
        cls.runCommand(cls._job_user, " entity -type cluster -delete -name backupCluster")

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][BGJobStop]Stopped/deleting Falcon background jobs")
        logger.info("Falcon - End function stopFalconLongRunningJob")

        return

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Falcon job
        '''
        logger.info("Tear down happened when long running job is stopped. Nothing to do")

        return

    @classmethod
    def falcon_teardown(cls):
        '''
        Clean up entities from Falcon
        '''
        # Delete entities
        logger.info("Falcon - Begin function falcon_teardown")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][BGJobTeardown] Falcon background jobs cleanup started")
        cls.runCommand(cls._job_user, " entity -type process -delete -name rawEmailIngestProcess", ignoreError=True)
        cls.runCommand(cls._job_user, " entity -type feed -delete -name rawEmailFeed", ignoreError=True)
        cls.runCommand(cls._job_user, " entity -type cluster -delete -name primaryCluster", ignoreError=True)
        cls.runCommand(cls._job_user, " entity -type cluster -delete -name backupCluster", ignoreError=True)

        logger.info("Falcon - End function falcon_teardown")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        logger.info("Verified when stopping Long Running Job ")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][BGJobCheck] Verify Falcon Background Application")
        cls.runCommand(cls._falcon_user, "admin -version")

    @classmethod
    def background_job_when_master_upgrade(cls):
        '''
        Start a background application which runs while component master service gets upgraded
        :return:
        '''
        logger.info("TODO - short backgroundjob when master upgrade/downgrade")

    @classmethod
    def background_job_teardown_when_master_upgrade(cls):
        '''
        Clean up for background job  which started before upgrading master services
        :return:
        '''
        logger.info("TODO - short backgroundjob teardown when master upgrade/downgrade")

    @classmethod
    def verify_background_job_when_master_upgrade(cls):
        '''
        Validate background job Succeeded when master got upgraded
        :return:
        '''
        logger.info("Falcon - Begin function verify_background_job_when_master_upgrade")
        cls.verifyLongRunningJob()

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("Falcon - Begin function upgrade_master")
        cls.background_job_when_master_upgrade()
        cls.change_master_version(version, config)
        cls.verify_background_job_when_master_upgrade()
        cls.background_job_teardown_when_master_upgrade()

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("Falcon has only a master, No slave to upgrade.")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Exactly same as upgrade master. There is no difference in steps.
        :param version: Version to be downgraded to
        :param config: Config location
        :return:
        '''
        logger.info("Falcon - Begin function downgrade_master ")
        cls.change_master_version(version, config, logText="Downgrade")

    @classmethod
    def change_master_version(cls, version, config=None, logText="Upgrade"):
        '''
        Changes Master services:
        :param version: Version to be changed to
        :param config: Config location
        '''
        logger.info("Falcon - Begin function change_master_version ")

        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from time import sleep
        try:
            from beaver.component.falcon import Falcon
        except ImportError:
            ## Import fails when Falcon is not installed on this machine. Nothing to do
            return

        node = Falcon.get_falcon_server()
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "[INFO][Falcon][%s] Falcon server for node %s to version %s started" % (logText, node, version)
        )

        Falcon.stop(cls._falcon_user, node)
        sleep(30)
        hdpSelect.changeVersion("falcon-server", version, node)
        hdpSelect.changeVersion("falcon-client", version, node)

        Falcon.start(cls._falcon_user, node)
        sleep(60)
        UpgradePerNode.reportProgress(
            "[INFO][Falcon][%s] Falcon server for node %s to version %s finished" % (logText, node, version)
        )
        return

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("Falcon runs in standalone mode. Nothing to do.")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("Falcon - Begin function run_client_smoketest ")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][Smoke] starting Falcon client smoke test")
        cls.run_smoke_test("123", config=None)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("Falcon - Begin function testAfterAllSlavesRestarted ")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Falcon][AFTER SLAVE PASSED] starting Falcon smoke test")
        cls.runCommand(cls._falcon_user, "admin -version")
        cls.runCommand(cls._job_user, " instance -type process -name rawEmailIngestProcess -list -orderBy startTime")

    @classmethod
    def runCommand(cls, user, cmd, ignoreError=True):
        """
        Function to run falcon commands cmd as a certain user
        :param user:
        :param cmd:
        :return:
        """
        try:
            from beaver.component.falcon import Falcon
        except ImportError:
            ## Import fails when Falcon is not installed on this machine. Nothing to do
            return 0, ""

        exit_code, output = Falcon.runas(user, cmd)
        if ignoreError:
            if exit_code != 0:
                #logger.info("Warning (ignoreError=True): Non-zero exit code when running command " + cmd + " as user " + user)
                from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
                UpgradePerNode.reportProgress(
                    "[FAILED][Falcon] Warning (ignoreError=True): Non-zero exit code when running command %s as user %s "
                    % (cmd, user)
                )
        else:
            ruAssert("Falcon", exit_code == 0, "[RunCommand] Could not run command " + cmd + " as user " + user)
        return exit_code, output

    @classmethod
    def createClusterEntities(cls, colo, desc, name):
        try:
            from beaver.component.falcon import Falcon
        except ImportError:
            ## Import fails when Falcon is not installed on this machine. Nothing to do
            return

        from beaver.component.hadoop import Hadoop, HDFS, YARN
        write_endpoint = Hadoop.getFSDefaultValue()
        webhdfs_scheme = 'webhdfs'
        if HDFS.isHttpsEnabled():
            webhdfs_scheme = 'swebhdfs'
        read_endpoint = '%s://%s:%s' % (
            webhdfs_scheme, write_endpoint.split('/')[2].split(':')[0], HDFS.getNNWebPort()
        )
        execute_endpoint = YARN.getResourceManager()
        falconNode = Falcon.get_falcon_server()

        from beaver.component.oozie import Oozie
        oozieUrl = Oozie.getOozieUrl()
        entityText = "<?xml version=\"1.0\"?>" \
                     "<cluster colo=\"" + colo + "\" description=\"" + desc + "\" name=\"" + name + "\" " \
                     "xmlns=\"uri:falcon:cluster:0.1\"> " \
                        "<interfaces> " \
                            "<interface type=\"readonly\" endpoint=\""+read_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"write\" endpoint=\""+write_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"execute\" endpoint=\"" + execute_endpoint + "\" version=\"0.20.2\"/> " \
                            "<interface type=\"workflow\" endpoint=\"" + oozieUrl + "\" version=\"3.1\"/>" \
                            "<interface type=\"messaging\" endpoint=\"" \
                                "tcp://" + falconNode + ":61616?daemon=true\" version=\"5.1.6\"/>" \
                        "</interfaces>" \
                        "<locations>" \
                            "<location name=\"staging\" path=\"/apps/falcon/" + name + "/staging\" />" \
                            "<location name=\"temp\" path=\"/tmp\" />" \
                            "<location name=\"working\" path=\"/apps/falcon/" + name + "/working\" />" \
                        "</locations>" \
                        "<ACL owner=\"" + cls._job_user + "\" group=\"users\" permission=\"0755\"/>"
        if Hadoop.isSecure():
            realm = HDFS.getConfigValue('dfs.namenode.kerberos.principal').split('@')[1]
            entityText += "<properties> <property name=\"dfs.namenode.kerberos.principal\" value=\"nn/_HOST@" + realm + "\"/> </properties>"
        entityText += "</cluster>"
        textFile = open(os.path.join(cls._local_workspace, name + ".xml"), "w")
        textFile.write("%s" % entityText)
        textFile.close()

        return
