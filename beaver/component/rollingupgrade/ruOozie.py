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
import inspect

logger = logging.getLogger(__name__)


class ruOozie:

    smokeObj = None
    longRunObj = None

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        if runSmokeTestSetup == True:
            cls.smoke_test_setup()
        from tests.oozie.smoke.OozieLongRunningTest import OozieLongRunningTest
        cls.longRunObj = OozieLongRunningTest()
        function_name = inspect.stack()[0][3]
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.longRunObj.setup({'mapred_job_queue_name': 'oozie'})
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        from tests.oozie.smoke.OozieSmokeTest import OozieSmokeTest
        cls.smokeObj = OozieSmokeTest()
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.smokeObj.setup({'mapred_job_queue_name': 'oozie'})

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.longRunObj.run()
        UpgradePerNode.reportProgress("### Oozie %s is now running ####" % function_name)
        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.smokeObj.run()
        cls.smokeObj.verify()
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.longRunObj.teardown()
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.longRunObj.verify()
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)

    @classmethod
    def background_job_when_master_upgrade(cls):
        '''
        Start a background application which runs while component master service gets upgraded
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def background_job_teardown_when_master_upgrade(cls):
        '''
        Clean up for background job  which started before upgrading master services
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def verify_background_job_when_master_upgrade(cls):
        '''
        Validate background job Succeeded when master got upgraded
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.oozie import Oozie
        cls.background_job_when_master_upgrade()
        oozieServers = Oozie.getOozieServers()
        for server in oozieServers:
            UpgradePerNode.reportProgress("### Oozie %s has started %s ####" % (server, function_name))
            Oozie.resetOozie('stop', host=server)
            time.sleep(60)
            hdpSelect.changeVersion("oozie-server", version, server)
            Oozie.resetOozie('start', configDir=config, host=server)
            time.sleep(60)
            UpgradePerNode.reportProgress("### Oozie %s has finished %s ####" % (server, function_name))
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
        # oozie have no slaves
        logger.info("Oozie has no slaves to upgrade")
        return

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.oozie import Oozie
        oozieServers = Oozie.getOozieServers()
        for server in oozieServers:
            UpgradePerNode.reportProgress("### Oozie %s has started %s ####" % (server, function_name))
            Oozie.resetOozie('stop', host=server)
            time.sleep(60)
            hdpSelect.changeVersion("oozie-server", version, server)
            Oozie.resetOozie('start', configDir=config, host=server)
            time.sleep(60)
            UpgradePerNode.reportProgress("### Oozie %s has finished %s ####" % (server, function_name))

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        # oozie have no slaves
        logger.info("Oozie has no slaves to downgrade")
        return

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.smokeObj.run()
        cls.smokeObj.verify()
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        function_name = inspect.stack()[0][3]
        UpgradePerNode.reportProgress("### Oozie %s has started ####" % function_name)
        cls.smokeObj.run()
        cls.smokeObj.verify()
        UpgradePerNode.reportProgress("### Oozie %s has finished ####" % function_name)
