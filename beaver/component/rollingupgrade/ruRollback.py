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
from beaver.component.rollingupgrade.ruPhoenix import ruPhoenix
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.component.rollingupgrade.ruCommon import Rollingupgrade, hdpRelease, hdpSelect
from beaver.component.rollingupgrade.ruHdfs import ruHDFS
from beaver.component.rollingupgrade.ruYarn import ruYARN
from beaver.component.rollingupgrade.ruZookeeper import ruZookeeper
from beaver.component.rollingupgrade.ruSetup import COMPONENTS_TO_IMPORT

logger = logging.getLogger(__name__)


class Rollback:
    _PROGRESS_STAUS_HDFS_FILE = "/tmp/rollback_progress_hdfs_file"
    _PROGRESS_STATUS_LOCAL_FILE = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'rollback_progress_local_file')
    _HDFS_FLAG_FILE = "/tmp/flagFile"

    #### TODO - was copied from ruUpgrade - need to be moved to common ####
    @classmethod
    def get_progress_local_file(cls):
        '''
        Returns file path for local upgrade progress file
        '''
        return cls._PROGRESS_STATUS_LOCAL_FILE

    #### TODO - was copied from ruUpgrade - need to be moved to common ####
    @classmethod
    def find_existing_core_components(cls, components):
        '''
        Finds Valid Core Components from Component List. Only HDFS, YARN and Hbase is considered as Core component
        :param components: Pass list of component which are chose to be upgraded
        :return: returns [Hdfs, Yarn, Hbase] if these components are included in component list
        '''
        valid_core_component = []
        if "hdfs" in components:
            valid_core_component.append("hdfs")
        if "yarn" in components:
            valid_core_component.append("yarn")
        if "hbase" in components:
            valid_core_component.append("hbase")
        return valid_core_component

    #### TODO - was copied from ruUpgrade - need to be moved to common ####
    @classmethod
    def reportProgress(cls, message):
        '''
        Method to report Upgrade status messages in Local File and HDFS File
        :param message: Message to be appended in Local status file and HDFS status file
        '''
        tmp_append_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp_append')
        if Machine.pathExists(None, None, tmp_append_file, None):
            Machine.rm(None, None, tmp_append_file, isdir=False, passwd=None)
        #if not HDFS.fileExists(cls._PROGRESS_STAUS_HDFS_FILE, user=None):
        #    HDFS.touchz(cls._PROGRESS_STAUS_HDFS_FILE)
        message = "\n" + message + "\n"
        logger.info(message)
        util.writeToFile(message, cls._PROGRESS_STATUS_LOCAL_FILE, isAppend=True)
        util.writeToFile(message, tmp_append_file, isAppend=False)
        #HDFS.appendToHdfs(tmp_append_file, cls._PROGRESS_STAUS_HDFS_FILE, user=None, config=None, logoutput=True)

    @classmethod
    def hdp_upgrade_rollback(cls, components, currVersion, latestVersion, doTeardown=True, finalize=True):
        '''
        Test rollback after a partial upgrade. Note today only HDFS/HBase need state rollback.
        But we do partial upgrage of core without background tests and then rollback

        Steps
        0) Create some state (e.g. file) that we will delete after upgrade
        1) Prepare and save component states.
        4) Partially Upgrade core components
          For each service, does:
          4a) Upgrade Masters, Upgrade 1 slave
          4b) Run smoke tests for all components.
        5) Delete state created in step 0
        6) Create new state
        7) rollback state
        7b) Run smoke tests
        7c) Validate that state create in 0 still exists but state create in step 6 does not

        :param components: list of Components to upgrade (Can only be HDFS, HBASE)
        :param currVersion: Current Version
        :param latestVersion: Version to be upgraded to
        :param doTeardown: Only Cleanup when required
        '''
        cls.reportProgress(
            "###  Starting upgrade from %s to %s for components=%s ####" % (currVersion, latestVersion, components)
        )
        DN = HDFS.getDatanodes()

        # Find core components (HDFS, YARN, HBase) if exist.
        core_components = cls.find_existing_core_components(components)

        #TODO if there are any non-core components then print an error since they are not going to be upgraded.

        #TODO create some state (e.g. files) whose existence will be checked after the rollback
        if "hdfs" in components:
            ruHDFS.createState4Rollback1()
        #if "hdfs" in components:
        #    hbase.createState4Rollback1()
        if "yarn" in components:
            logger.info("Rollback doesn't make sense for YRAN")

        #Prepare and save state before upgrade
        Rollingupgrade.ru_prepare_save_state_for_upgrade(components)

        #upgrade the components in Hierchacy
        cls.reportProgress("###  Starting upgrade of core %s masters  ###" % core_components)
        #### IF XA is enabled, upgrade XA services ####
        from beaver.component.xa import Xa
        cls.reportProgress(
            "******************************* checking for argus to be installed *******************************"
        )
        if "argus" in components and Xa.isArgusInstalled():
            logger.info(
                '**************************************************** XA is Enabled in the cluster, setting up and upgrading the same ****************************************************'
            )
            Rollingupgrade.upgrade_master_and_smoketest(['argus'], latestVersion, config=None, currVersion=currVersion)

        if "zookeeper" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["zookeeper"], latestVersion, config=None)

        # Upgrade Master services -
        # Namenode, Secondarynamenode, Resourcemanager, Application Timelineserver,
        # JobHistoryserver and HbaseMaster with new version

        AfterUpgradeBackGroundJobs = Rollingupgrade.upgrade_master_and_smoketest(
            core_components, latestVersion, config=None
        )
        cls.reportProgress("###  Finished upgrade of core %s masters  ###" % core_components)

        # upgrade 1 slave service - Datanodes, Nodemanagers and Regionservers with new version
        cls.reportProgress("###  Starting upgrade of core %s slaves  ###" % core_components)

        logger.info("**** Upgrading first  slave:" + DN[0] + " ****")
        Rollingupgrade.upgrade_slave_and_smoketest(core_components, latestVersion, DN[0], None, False)

        cls.reportProgress("###  Finished upgrade of 1 core %s slave  ###" % (core_components))
        #### Run all component Smoke tests ####
        Rollingupgrade.run_smokeTests(components, config=None)

        #  Run Tests to verify components accessibility
        Rollingupgrade.testAfterAllMasterSlavesUpgraded(components)

        #### Run all component Smoke tests ####
        Rollingupgrade.run_smokeTests(components, config=None)

        # TODO - delete some state that was created befoire the prepare-save state
        # TODO - create some new state
        if "hdfs" in components:
            ruHDFS.createState4Rollback2()
        if "hbase" in components:
            from beaver.component.rollingupgrade.ruHbase import ruHbase
            ruHbase.createState4Rollback2()
        if "yarn" in components:
            logger.info("Rollback doesn't make sense for YRAN")

        #################### Now do the rollback ########################
        cls.reportProgress(
            "###  Starting  rollback from %s to %s for components=%s ####" % (latestVersion, currVersion, components)
        )

        logger.info("**** Downgrading slave number 0 : " + DN[0] + " ****")
        Rollingupgrade.downgrade_slave_and_smoketest(core_components, currVersion, DN[0], None)

        #### Downgrade Namenode, Resourcemanager, Hbase master ####
        cls.reportProgress("###  Starting downgrade of core %s masters  ###" % core_components)
        Rollingupgrade.downgrade_master_and_smoketest(core_components, currVersion, config=None)

        ## rollback state TODO the rollback function does not exist yet.
        #Rollingupgrade.ru_rollback_state(components)
        if "hdfs" in components:
            ruHDFS.ru_rollback_state()
        #if "hbase" in components:
        #    hbase.ru_rollback_state()
        if "yarn" in components:
            logger.info("Rollback doesn't make sense for YRAN")

        # TODO now check that the deleted state exists and the newly create state does not
        if "hdfs" in components:
            ruHDFS.checkState4Rollback()
        if "hbase" in components:
            from beaver.component.rollingupgrade.ruHbase import ruHbase
            ruHbase.checkState4Rollback()
        if "yarn" in components:
            logger.info("Rollback doesn't make sense for YRAN")

        cls.reportProgress(
            "###  Completed rollback from %s to %s for components=%s ####" % (currVersion, latestVersion, components)
        )
