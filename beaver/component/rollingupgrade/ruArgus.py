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
from beaver.component.rollingupgrade.ruArgusAdmin import ruArgusAdmin
from beaver.component.rollingupgrade.ruArgusUserSync import ruArgusUserSync

logger = logging.getLogger(__name__)


class ruArgus:
    @classmethod
    def background_job_setup(cls, components, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        # logger.info("TODO")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### call for background-job setup for argus admin and usersync started ####')
        ruArgusAdmin.background_job_setup(components, runSmokeTestSetup, config)
        ruArgusUserSync.background_job_setup(runSmokeTestSetup)
        UpgradePerNode.reportProgress('#### call background-job setup for argus admin and usersync done ####')

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
        Prepare Argus Admin to save State for Upgrade
        '''
        ruArgusAdmin.get_argus_admin_backup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("TODO")

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None, components_to_test=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("TODO")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### ru-argus run long running job  ####')
        ruArgusAdmin.run_smoke_test(components_to_test)

        return 0

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None, components_to_test=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### ru-argus run smoke test number = %s ####' % str(smoketestnumber))
        ruArgusAdmin.run_smoke_test(components_to_test)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("teardown for Argus Admin and User-Sync ")
        print(
            "**************************************** teardown for Argus Admin and User-Sync **************************************** "
        )
        ruArgusAdmin.background_job_teardown()
        ruArgusUserSync.background_job_teardown()

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        logger.info("TODO")

    @classmethod
    def upgrade_master(cls, latestVersion, config=None, currVersion=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Upgrading Argus Admin and User-sync.")
        ruArgusAdmin.upgrade_master(latestVersion, config, currVersion)
        ruArgusUserSync.upgrade_master(latestVersion, config, currVersion)

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def downgrade_master(cls, version, config=None, currVersion=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Downgrading Argus Admin and User-sync.")
        ruArgusUserSync.downgrade_master(version, config, currVersion)
        ruArgusAdmin.downgrade_master(version, config, currVersion)

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None, components_to_test=[]):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("****************** running client smoke tests for ranger ******************")
        ruArgusAdmin.run_smoke_test(components_to_test)

    @classmethod
    def testAfterAllSlavesRestarted(cls, components_to_test):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("************ running  tests after slaves restarted for Ranger ************")
        ruArgusAdmin.run_smoke_test(components_to_test)
