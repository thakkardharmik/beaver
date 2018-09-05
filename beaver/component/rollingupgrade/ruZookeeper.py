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

logger = logging.getLogger(__name__)


class ruZookeeper:
    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        logger.info("No longrunning job for zookeeper. No setup to do.")

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("No setup needed for zookpeer smoke test.")

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("No longrunning job for zookeeper. Nothing to do.")

    @classmethod
    def run_smoke_test(cls, config=None):
        '''
        Run smoke test for zookeeper
        '''
        from beaver.component.zookeeper import Zookeeper
        zk_nodes = Zookeeper.getZKHosts()
        zk_node1 = zk_nodes[0]

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][Zookeeper][Smoke] Smoke test for Zookeeper started ")

        # Delete Smoke Test Dir
        exit_code, stdout = Zookeeper.runZKCli("delete /zk_smoketest", server=zk_node1)
        if exit_code != 0 or not stdout:
            UpgradePerNode.reportProgress("[FAILED][Zookeeper][Smoke] Zookeeper Smoke Test Failed ")
            return
        Zookeeper.checkExceptions(stdout)

        # Create Smoke Test Dir
        if exit_code != 0 or not stdout:
            UpgradePerNode.reportProgress("[FAILED][Zookeeper][Smoke] Zookeeper Smoke Test Failed ")
            return
        Zookeeper.checkExceptions(stdout)

        # Run tests on host
        for host in zk_nodes:
            logger.info("Running test on host " + host)

            exit_code, stdout = Zookeeper.runZKCli("get /zk_smoketest", server=zk_node1)
            m = re.search('smoke_data', stdout) is not None
            if exit_code != 0 or not stdout or m is None:
                UpgradePerNode.reportProgress("[FAILED][Zookeeper][Smoke]  Zookeeper Smoke Test Failed")
                return
            Zookeeper.checkExceptions(stdout)

            exit_code, stdout = Zookeeper.runZKCli("ls /", server=zk_node1)
            if exit_code != 0 or not stdout:
                UpgradePerNode.reportProgress("[FAILED][Zookeeper][Smoke]  Zookeeper Smoke Test Failed ")
                return
            Zookeeper.checkExceptions(stdout)

        # Delete Smoke Test Dir
        exit_code, stdout = Zookeeper.runZKCli("delete /zk_smoketest", server=zk_node1)
        if exit_code != 0 or not stdout:
            UpgradePerNode.reportProgress(" [FAILED][Zookeeper][Smoke]  Zookeeper Smoke Test Failed ")
            return
        Zookeeper.checkExceptions(stdout)

        UpgradePerNode.reportProgress("[PASSED][Zookeeper][Smoke]  Zookeeper Smoke Test Passed ")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        logger.info("TODO")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        logger.info("TODO")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.zookeeper import Zookeeper
        leaderNode = ""
        zk_nodes = Zookeeper.getZKHosts()
        if zk_nodes != "":
            for node in zk_nodes:
                mode = Zookeeper.getMode(node)
                if (mode != "follower"):
                    leaderNode = node
                else:
                    cls.restartOneNode(node, version, upgrade=True)
                    logger.info("Running smoke test after follower node %s upgraded" % node)
                    cls.run_smoke_test()
        else:
            logger.info("Didn't find any ZK nodes in the zoo.cfg")
            return
        # upgrade the leader last
        logger.info("Running smoke test after leader node upgraded")
        cls.restartOneNode(leaderNode, version, upgrade=True)
        cls.run_smoke_test()

    @classmethod
    def restartOneNode(cls, node, version, upgrade=True):
        '''
        Stop and restart one node with selected version
        :param upgrade: True for Upgrade, False for Downgrade
        :return:
        '''
        from beaver.component.zookeeper import Zookeeper
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        version_change = "Upgrade"
        if not upgrade:
            version_change = "Downgrade"
        UpgradePerNode.reportProgress(
            "[INFO][Zookeeper][Upgrade] Zookeeper %s on %s started " % (version_change, node)
        )
        logger.info("Stopping %s" % node)
        Zookeeper.runZKServer("stop", node)
        logger.info("Changing version on %s" % node)
        hdpSelect.changeVersion("zookeeper-server", version, node)
        logger.info("Starting %s" % node)
        Zookeeper.runZKServer("start", node)

        # Now make sure ZK is usable after restart
        retry_limit = 100
        retry = 0
        while retry < retry_limit:
            exit_code, stdout = Zookeeper.runZKCli("create /zk_smoketest_during_restart", server=node)
            m = re.search('exists', stdout)
            if exit_code == 0:
                break
            elif m is not None:
                Zookeeper.runZKCli("delete /zk_smoketest_during_upgrade", server=node)
            logger.info("Couldn't create a znode after restart. Will retry. Sleeping...")
            time.sleep(1)
            retry += 1

        if retry == retry_limit:
            UpgradePerNode.reportProgress("[FAILED][Zookeeper][Upgrade] Zookeeper Upgrade on %s Failed " % node)
            return

        retry_limit = 100
        retry = 0
        while retry < retry_limit:
            exit_code, stdout = Zookeeper.runZKCli("ls /zk_smoketest_during_restart", server=node)
            if exit_code == 0:
                break
            logger.info("Couldn't ls a znode after restart. Will retry. Sleeping...")
            time.sleep(1)
            retry += 1

        if retry == retry_limit:
            UpgradePerNode.reportProgress("[FAILED][Zookeeper][Upgrade] Zookeeper Upgrade on %s Failed " % node)
            return

        exit_code, stdout = Zookeeper.runZKCli("delete /zk_smoketest_during_restart", server=node)
        if exit_code != 0:
            UpgradePerNode.reportProgress("[FAILED][Zookeeper][Upgrade] Zookeeper Upgrade on %s Failed " % node)
            return

        logger.info("**** Ending upgrade for Zookeeper on %s ****" % node)
        UpgradePerNode.reportProgress("[INFO][Zookeeper][Upgrade] Zookeeper Upgrade on %s Finished " % node)

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("No slave for zookeeper.")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.zookeeper import Zookeeper
        leaderNode = ""
        zk_nodes = Zookeeper.getZKHosts()
        if zk_nodes != "":
            for node in zk_nodes:
                mode = Zookeeper.getMode(node)
                if (mode != "follower"):
                    leaderNode = node
                else:
                    cls.restartOneNode(node, version, upgrade=False)
                    logger.info("Running smoke test after follower node %s downgraded" % node)
                    cls.run_smoke_test()
        else:
            logger.info("Didn't find any ZK nodes in the zoo.cfg")
            return
        # upgrade the leader last
        logger.info("Running smoke test after leader node downgraded")
        cls.restartOneNode(leaderNode, version, upgrade=False)
        cls.run_smoke_test()

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("No slave for zookeeper.")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are
        upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        cls.testAfterAllZKsHaveRestarted()

    @classmethod
    def testAfterAllZKsHaveRestarted(cls):
        cls.run_smoke_test()
