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
import urllib, sys, shutil
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils

logger = logging.getLogger(__name__)


class ruFlume:
    _agent1 = None
    _agent2 = None
    _local_work_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'flume')
    _data_file = os.path.join(_local_work_dir, 'data.out')
    _data_stop = os.path.join(_local_work_dir, "data.stop")
    _flume_test_conf = os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'rolling_upgrade', 'flumeconf')
    _flume_datagen_src = os.path.join(_flume_test_conf, 'print_stream.py')
    _flume_test_src = os.path.join(_local_work_dir, 'longrunning.properties')
    _hdfs_test_dir = "/tmp/flumelr"
    _test_user = Config.getEnv('USER')
    _hdfs_user = Config.get('hadoop', 'HDFS_USER')
    _agent1_chkpt_dir = os.path.join(_local_work_dir, 'checkpoint1')
    _agent2_chkpt_dir = os.path.join(_local_work_dir, 'checkpoint2')
    _agent1_data_dir = os.path.join(_local_work_dir, 'datadir1')
    _agent2_data_dir = os.path.join(_local_work_dir, 'datadir2')

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][FLUME][BGJobSetup] Long running job setup for Flume component started")
        from beaver.component.flume import Agent
        global agent1
        global agent2
        if not os.path.exists(cls._local_work_dir):
            os.mkdir(cls._local_work_dir)
        shutil.copy(cls._flume_datagen_src, cls._local_work_dir)
        agent1 = Agent(cls._local_work_dir)
        agent2 = Agent(cls._local_work_dir)
        for outdir in (cls._agent1_chkpt_dir, cls._agent1_data_dir, cls._agent2_chkpt_dir, cls._agent2_data_dir):
            os.mkdir(outdir)
        logger.info("Preparing the Flume configs for long running test")
        propertyMap = {}
        namenode = Hadoop.getFSDefaultValue()
        propertyMap['agent2.sinks.hdfsSink.hdfs.path'] = "%s%s" % (namenode, cls._hdfs_test_dir)
        if Hadoop.isSecure():
            if Config.hasOption('machine', 'USER_REALM'):
                user_realm = Config.get('machine', 'USER_REALM', '')
            else:
                nnKerbPrincipal = HDFS.getNameNodePrincipal(defaultValue='')
                atloc = nnKerbPrincipal.find("@")
                if atloc != -1:
                    user_realm = nnKerbPrincipal[atloc:]
            if user_realm:
                propertyMap['agent2.sinks.hdfsSink.hdfs.kerberosPrincipal'] = cls._test_user + '@' + user_realm
            propertyMap['agent2.sinks.hdfsSink.hdfs.kerberosKeytab'] = Machine.getHeadlessUserKeytab(cls._test_user)
        util.writePropertiesToFile(
            os.path.join(cls._flume_test_conf, 'longrunning.properties'), cls._flume_test_src, propertyMap
        )

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("TODO")

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Flume Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        HDFS.createDirectory(cls._hdfs_test_dir, perm="777", force=True)
        UpgradePerNode.reportProgress("[INFO][FLUME][BGJob] Long running job for Flume component started")
        logger.info("Starting the Flume Agent Topology")
        addlParams = "-Dflume.log.dir=%s -Dflume.log.file=agent2.log" % cls._local_work_dir
        agent2.start("agent2", cls._flume_test_src, addlParams=addlParams, enableDebugLogOnConsole=False)
        logger.info("Sleeping for 10 seconds before starting the other Flume agent")
        time.sleep(10)
        addlParams = "-Dflume.log.dir=%s -Dflume.log.file=agent.log" % cls._local_work_dir
        agent1.start("agent", cls._flume_test_src, addlParams=addlParams, enableDebugLogOnConsole=False)
        time.sleep(5)
        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        logger.info("TODO")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "[INFO][FLUME][BGJobTeardown] teardown for Long running job of Flume component started"
        )
        if agent1.isalive():
            logger.info("Terminating the first Flume agent")
            agent1.stop()
        if agent2.isalive():
            logger.info("Terminating the second Flume agent")
            agent2.stop()
        for outdir in (cls._agent1_chkpt_dir, cls._agent1_data_dir, cls._agent2_chkpt_dir, cls._agent2_data_dir):
            if os.path.exists(outdir):
                shutil.rmtree(outdir)
        if os.path.isfile(cls._data_file):
            os.remove(cls._data_file)
        if os.path.isfile(cls._data_stop):
            os.remove(cls._data_stop)

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        logger.info("Stop the Flume agents before verification")
        open(cls._data_stop, 'a').close()
        time.sleep(60)
        agent1.stop()
        agent2.stop()
        time.sleep(60)
        logger.info("Verifying the sinked data from Flume agent")
        exit_code, stdout, stderr = Hadoop.runas(
            cls._hdfs_user, "dfs -cat %s/*" % cls._hdfs_test_dir, logoutput=False, stderr_as_stdout=False
        )
        if exit_code != 0:
            logger.error("Following error during the HDFS cat while fetching Flume data: %s" % stderr)
        if not util.compareOutputToFileIgnoreDupsAndOrder(stdout, cls._data_file):
            UpgradePerNode.reportProgress(
                "[FAILED][FLUME][BGJob] Long running test for Flume failed while verifying data"
            )
        else:
            UpgradePerNode.reportProgress(
                "### [PASSED][FLUME][BGJob] Long running test validation for Flume passed ####"
            )

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        cls.switch_master_version(version)

    @classmethod
    def switch_master_version(cls, version):
        '''
        Switches the Flume agent service
        :param version: Version to be switched to
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        logger.info("Stop the second Flume Agent before upgrade")
        open(cls._data_stop, 'a').close()
        time.sleep(10)
        agent1.stop()
        time.sleep(3)
        os.remove(cls._data_stop)
        hdpSelect.changeVersion("flume-server", version)
        logger.info("Restart the Flume agents with the new version")
        addlParams = "-Dflume.log.dir=%s -Dflume.log.file=agent.log" % cls._local_work_dir
        agent1.start("agent", cls._flume_test_src, addlParams=addlParams, enableDebugLogOnConsole=False)
        time.sleep(20)
        agent2.stop()
        time.sleep(10)
        addlParams = "-Dflume.log.dir=%s -Dflume.log.file=agent2.log" % cls._local_work_dir
        agent2.start("agent2", cls._flume_test_src, addlParams=addlParams, enableDebugLogOnConsole=False)
        time.sleep(10)

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("*** No slave component in Flume, so nothing to upgrade ***")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        cls.switch_master_version(version)

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("*** No slave component in Flume, so nothing to downgrade ***")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("TODO")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("Nothing to be done here for Flume")
