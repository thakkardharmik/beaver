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
from beaver.component.rollingupgrade.ruOozie import ruOozie
from beaver.component.rollingupgrade.ruSetup import COMPONENTS_TO_IMPORT
if "slider" in COMPONENTS_TO_IMPORT:
    from beaver.component.rollingupgrade.ruSlider import ruSlider

logger = logging.getLogger(__name__)


class UpgradePerNode:
    _PROGRESS_STAUS_HDFS_FILE = "/tmp/upgrade_progress_hdfs_file"
    _PROGRESS_STATUS_LOCAL_FILE = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'upgrade_progress_local_file')
    _HDFS_FLAG_FILE = "/tmp/flagFile"

    @classmethod
    def get_progress_local_file(cls):
        '''
        Returns file path for local upgrade progress file
        '''
        return cls._PROGRESS_STATUS_LOCAL_FILE

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
    def hdp_upgrade(cls, components, currVersion, latestVersion, doTeardown=True, finalize=True):
        '''
        Upgrade HDP Stack With Per Node Method.

        Steps
        1) Prepare and save component states.
        2) Setup prerequisites for background jobs.
        3) Start long-running background jobs for all components.
        4) Upgrade core components from bottom to top.
          For each service, does:
          4a) Upgrade service.
          4b) Run smoke tests for all components.
          4c) Check number of all background jobs.
        5) After all components are upgraded, run another set of tests.
        6) Repeat same process for non-core components.
        7) Upgrade clients of components which were upgraded earlier.
        8) Upgrade client-only components.
        9) After all components are upgraded, run smoke tests.
        10) Stop long running jobs.
        11) Look for failed and kill jobs.
        12) Verify outputs of successful jobs.
        13) Finalize all states.

        :param components: list of Components to upgrade
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

        #Prepare and save state before upgrade
        Rollingupgrade.ru_prepare_save_state_for_upgrade(components)

        # Run setup for background Jobs for all components
        Rollingupgrade.background_job_setup(components, config=None)

        # Starts Long running background Jobs for all components
        numBackgroundJobs = Rollingupgrade.run_longRunning_Application(components, config=None)
        logger.info("Total number of long running background jobs before starting upgrade is %s" % numBackgroundJobs)
        cls.reportProgress("###  Just started %s background jobs  ###" % numBackgroundJobs)

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

        ##### TODO - upgrade ZOOKEEPER ########
        if "zookeeper" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["zookeeper"], latestVersion, config=None)
        # Upgrade Master services - Namenode, Secondarynamenode, Resourcemanager, Application Timelineserver, JobHistoryserver and HbaseMaster with new version
        #### TODO - Application Timelineserver HbaseMaster ####
        AfterUpgradeBackGroundJobs = Rollingupgrade.upgrade_master_and_smoketest(
            core_components, latestVersion, config=None
        )
        cls.reportProgress("###  Finished upgrade of core %s masters  ###" % core_components)
        numBackgroundJobs = numBackgroundJobs + AfterUpgradeBackGroundJobs
        logger.info(
            "Total number of long running background jobs after upgrading master services is %s" % numBackgroundJobs
        )

        # upgrade slave service - Datanodes, Nodemanagers and Regionservers with new version
        cls.reportProgress("###  Starting upgrade of core %s slaves  ###" % core_components)
        i = 0
        #### TODO - upgrade Regionserver  ####
        for node in DN:
            i += 1
            logger.info("**** Upgrading slave number " + str(i) + ": " + node + " ****")
            if i % 4 == 0:
                runSmoke = True
            else:
                runSmoke = False
            Rollingupgrade.upgrade_slave_and_smoketest(core_components, latestVersion, node, None, runSmoke)
            #check if background function running
            runningJobs = YARN.getNumOfRunningJobs()
            logger.info("Long-running job ended too early; running jobs =" + str(runningJobs))
            #assert runningJobs == numBackgroundJobs, 'Long-running job ended too early; running jobs = ' + str(runningJobs)

        cls.reportProgress("###  Finished upgrade of %d core %s slaves  ###" % (i, core_components))
        #### Run all component Smoke tests ####
        Rollingupgrade.run_smokeTests(components, config=None)

        #  Run Tests to verify components accessibility
        Rollingupgrade.testAfterAllMasterSlavesUpgraded(components)

        #### Starting upgrade non core components ####
        cls.reportProgress("###  Starting upgrade of non-core cluster components  ###")
        if "hive" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["hive"], latestVersion, config=None)

        #### TODO- upgrade pig to N+1 version ####

        #### TODO - Run pig smoke test ####
        #     ## Example : ##
        if "pig" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["pig"], latestVersion, config=None)
        # ##    Rollingupgrade.upgrade_slave_and_smoketest(["pig"], latestVersion, node)

        # #### TODO - upgrade oozie server to N+1 version ####

        # #### - Run oozie smoke test ####
        if "oozie" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["oozie"], latestVersion, config=None)

        #### upgrade falcon to N+1 version and run its smoke tests ####

        if "falcon" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["falcon"], latestVersion, config=None)

        #### TODO - upgrade phoenix to N+1 version ####

        #### TODO - Run phoenix smoke test ####
        if "phoenix" in components:
            ruPhoenix.run_smoke_test(ruPhoenix._smokeTestNum)

        #### TODO - upgrade sqoop to N+1 version ####
        #### TODO - Run sqoop smoke test ####

        cls.reportProgress("###  Finished upgrade of non-core cluster components  ###")

        ##For storm-slider we want toverify the topologies and kill the storm-slider app.
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            ruStorm.verify_and_stop_slider_app()

        #### TODO- upgrade clients for Argus, Zk, Hdfs, Yarn, MR, Tez, Hive, Pig, Hbase, Falcon, oozie, sqoop , phoenix, mahout ####
        cls.reportProgress("###  Starting upgrade of clients %s inside the cluster ###" % components)
        Rollingupgrade.upgrade_client_insideCluster_and_smoketest(components, latestVersion, config=None)

        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            ruStorm.start_slider_app_resubmit_topologies()
            time.sleep(120)  # Allow time for storm-slider topologies to run.

        cls.reportProgress("###  Starting upgrade of slider apps ###")
        ### TODO- upgrade slider client and non rolling upgrade of slider-apps ####
        ### TODO- Stop storm-slider app, hbase-slider app, accumulo-slider app
        ### TODO- Upgrade storm-slider client
        ### TODO- resubmit storm-slider app, hbase-slider app, accumulo-slider app
        cls.reportProgress("###  Finished upgrade of slider apps ###")

        #### Knox upgrade
        if "knox" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["knox"], latestVersion, config=None)

        #### upgrade Flume to N+1 version ####
        if "flume" in components:
            Rollingupgrade.upgrade_master_and_smoketest(["flume"], latestVersion, config=None)

        #### TODO - upgrade Kafka to N+1 version ####

        #### TODO - Run Kafka smoke test ####

        ## Example : ##
        ## if "kafka" in components:
        ##    Rollingupgrade.upgrade_master_and_smoketest(["kafka"], latestVersion, config=None)
        ##    Rollingupgrade.upgrade_slave_and_smoketest(["kafka"], latestVersion, node)

        #### TODO - upgrade Storm to N+1 version ####

        #### TODO - Run storm smoke test ####

        ## Example : ##
        ## if "storm" in components:
        ##    Rollingupgrade.upgrade_master_and_smoketest(["storm"], latestVersion, config=None)
        ##    Rollingupgrade.upgrade_slave_and_smoketest(["storm"], latestVersion, node)

        #### TODO - upgrade Hue to N+1 version ####

        #### TODO - Run Hue smoke test ####

        ## Example : ##
        ## if "hue" in components:
        ##    Rollingupgrade.upgrade_master_and_smoketest(["hue"], latestVersion, config=None)
        ##    Rollingupgrade.upgrade_slave_and_smoketest(["hue"], latestVersion, node)
        cls.reportProgress("###  Finished upgrade of non-core components outside the cluster  ###")

        #### TODO - Run all component Smoke tests ####
        Rollingupgrade.run_smokeTests(components, config=None)

        ### Need to stop HDFS Falcon,Yarn long runningJobs ####
        # create flagFile to kill HDFS background job
        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')
        createCmd = "dfs -touchz " + cls._HDFS_FLAG_FILE
        exit_code, output = HDFS.runas(TEST_USER, createCmd)

        if "falcon" in components:
            from beaver.component.rollingupgrade.ruFalcon import ruFalcon
            ruFalcon.stopFalconLongRunningJob()
        if "yarn" in components:
            ruYARN.stopYarnLongRunningJob()
        if "slider" in components:
            ruSlider.stopSliderLongRunningJob()
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            ruStorm.teardown_storm_slider_app()

        ## TODO - wait for long running jobs to finish
        isZero = YARN.waitForZeroRunningApps()
        if isZero:
            cls.reportProgress("#### None apps are running. ####")
        else:
            cls.reportProgress("#### Check Failed. some apps are running. ####")
        #assert isZero, "all long running jobs are not finished"

        ### List down Failed/Killed applications ####
        Failed_Killed_apps = YARN.getFailedKilledAppList()
        cls.reportProgress("### Listing Killed/Failed applications while performing upgrade ####")
        for app in Failed_Killed_apps:
            queue = YARN.getQueueForApp(app)
            logger.info(" %s running on %s queue Failed/Killed." % (app, queue))
            cls.reportProgress("#### %s running on %s queue Failed/Killed. ####" % (app, queue))

        ## TODO - Validate long running jobs
        Rollingupgrade.verifyLongRunningJob(components)

        ## KILL APPLICATIONS ####
        YARN.killAllApplications(useYarnUser=True)

        ## TODO - call Finalize
        if finalize:
            Rollingupgrade.ru_finalize_state(components)

        ## TODO - call Teardown for long running jobs
        if doTeardown:
            Rollingupgrade.background_job_teardown(components, None)
        cls.reportProgress(
            "###  Completed upgrade from %s to %s for components=%s ####" % (currVersion, latestVersion, components)
        )


class DowngradePerNode:
    @classmethod
    def hdp_downgrade(cls, components, currVersion, latestVersion, doTeardown=True):
        '''
        Downgrade HDP Stack With Per Node Method
        :param components: Components to be downgraded
        :param currVersion: Current version (Version V1)
        :param latestVersion: Version to be downgraded to (Version V0)
        '''
        UpgradePerNode.reportProgress(
            "###  Starting downgrade from %s to %s for components=%s ####" % (currVersion, latestVersion, components)
        )
        DN = HDFS.getDatanodes()
        core_components = UpgradePerNode.find_existing_core_components(components)

        # Run setup for background Jobs for all components
        Rollingupgrade.background_job_setup(components, config=None)

        # Starts Long running background Jobs for all components
        numBackgroundJobs = Rollingupgrade.run_longRunning_Application(components, config=None)
        logger.info("Total number of long running background jobs before starting upgrade is %s" % numBackgroundJobs)
        UpgradePerNode.reportProgress("###  Just started %s background jobs  ###" % numBackgroundJobs)

        #### TODO - downgrade Hue and run Hue smoke test ####
        UpgradePerNode.reportProgress("###  Starting downgrade of non-core components outside the cluster  ###")
        ## Example : ##
        ## if "hue" in components:
        ##    Rollingupgrade.downgrade_master_and_smoketest(["hue"], latestVersion, config=None)
        ##    Rollingupgrade.downgrade_slave_and_smoketest(["hue"], latestVersion, node)

        #### TODO - downgrade storm and run smoke test ####

        ## Example : ##
        ## if "storm" in components:
        ##    Rollingupgrade.downgrade_master_and_smoketest(["storm"], latestVersion, config=None)
        ##    Rollingupgrade.downgrade_slave_and_smoketest(["storm"], latestVersion, node)

        #### TODO - downgrade Kafka and run smoke test ####

        ## Example : ##
        ## if "kafka" in components:
        ##    Rollingupgrade.downgrade_master_and_smoketest(["kafka"], latestVersion, config=None)
        ##    Rollingupgrade.downgrade_slave_and_smoketest(["kafka"], latestVersion, node)

        #### downgrade Flume ####
        if "flume" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["flume"], latestVersion, config=None)

        #### downgrade Knox and run smoke test ####
        if "knox" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["knox"], latestVersion, config=None)
        UpgradePerNode.reportProgress("###  Finished downgrade of non-core components outside the cluster  ###")

        UpgradePerNode.reportProgress("###  Starting downgrade of slider apps ###")
        ### TODO- downgrade slider client and non rolling upgrade of slider-apps ####
        ### TODO- Stop storm-slider app, hbase-slider app, accumulo-slider app
        ### TODO- downgrade storm-slider client
        ### TODO- resubmit storm-slider app, hbase-slider app, accumulo-slider app
        UpgradePerNode.reportProgress("###  Finished downgrade of slider apps ###")

        # Downgrade Non core components
        UpgradePerNode.reportProgress("###  Starting downgrade clients %s inside the cluster ###" % components)
        ### TODO - Downgrade CLIENTS ####
        Rollingupgrade.downgrade_client_insideCluster_and_smoketest(components, latestVersion, config=None)
        UpgradePerNode.reportProgress("###  Finished downgrade of clients %s inside the cluster ###" % components)

        #### TODO - Downgrade phoenix and Run phoenix smoke test ####
        UpgradePerNode.reportProgress("###  started downgrade of non-core cluster components  ###")

        ## Example : ##
        ## if "phoenix" in components:
        ##    Rollingupgrade.downgrade_master_and_smoketest(["phoenix"], latestVersion, config=None)
        ##    Rollingupgrade.downgrade_slave_and_smoketest(["phoenix"], latestVersion, node)

        #### downgrade falcon and run smoke test ####

        if "falcon" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["falcon"], latestVersion, config=None)

        # #### - downgrade oozie and run smoke test ####
        if "oozie" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["oozie"], latestVersion, config=None)

        #### Downgrade Pig and run pig smoke test ####
        if "pig" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["pig"], latestVersion, config=None)

        if "hive" in components:
            Rollingupgrade.downgrade_master_and_smoketest(["hive"], latestVersion, config=None)
        UpgradePerNode.reportProgress("###  Finished downgrade of non-core cluster components  ###")

        # Downgrade Slave services of core-components (Hdfs, Yarn, hbase)
        UpgradePerNode.reportProgress("###  Starting downgrade of core %s slaves  ###" % core_components)
        i = 0
        #### TODO - Downgrade Datanode, Nodemanager, Regionserver  ####
        for node in DN:
            i += 1
            logger.info("**** Downgrading slave number " + str(i) + ": " + node + " ****")
            Rollingupgrade.downgrade_slave_and_smoketest(core_components, latestVersion, node, None)
            #check if background function running
            runningJobs = YARN.getNumOfRunningJobs()
            logger.info("Long-running job ended too early; running jobs =" + str(runningJobs))
            #assert runningJobs == numBackgroundJobs, 'Long-running job ended too early; running jobs = ' + str(runningJobs)
        UpgradePerNode.reportProgress("###  Finished downgrade of %d core %s slaves  ###" % (i, core_components))

        # run smoke tests after downgrading
        Rollingupgrade.run_smokeTests(components, config=None)

        #### TODO - Downgrade Namenode, Resourcemanager, Hbase master ####
        UpgradePerNode.reportProgress("###  Starting downgrade of core %s masters  ###" % core_components)
        Rollingupgrade.downgrade_master_and_smoketest(core_components, latestVersion, config=None)

        #### TODO - Run Validation after All Master and slave services are down ####
        Rollingupgrade.testAfterAllMasterSlavesUpgraded(components)

        ### TODO - Downgrade Zookeeper ####
        #Rollingupgrade.downgrade_master_and_smoketest(["zookeeeper"], latestVersion, config=None)
        UpgradePerNode.reportProgress("###  Finished downgrade of core %s masters  ###" % core_components)

        #### IF XA is enabled, downgrade XA services ####
        from beaver.component.xa import Xa
        if "argus" in components and Xa.isArgusInstalled():
            logger.info('XA is Enabled in the cluster, setting up and downgrading the same')
            Rollingupgrade.downgrade_master_and_smoketest(['argus'], latestVersion, config=None, currVersion=None)

#### TODO - Run all component Smoke tests ####
        Rollingupgrade.run_smokeTests(components, config=None)

        #TODO - this is common code with upgrade - move it to a function.   - but the slider part is differnt in downgrade; shouldn't be ---
        ### Need to stop HDFS Falcon,Yarn long runningJobs ####
        # create flagFile to kill HDFS background job
        ### Need to stop HDFS Falcon,Yarn long runningJobs ####
        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')
        createCmd = "dfs -touchz " + UpgradePerNode._HDFS_FLAG_FILE
        exit_code, output = HDFS.runas(TEST_USER, createCmd)

        ruYARN.stopYarnLongRunningJob()
        if "falcon" in components:
            from beaver.component.rollingupgrade.ruFalcon import ruFalcon
            ruFalcon.stopFalconLongRunningJob()
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            ruStorm.teardown_storm_slider_app()

        ## TODO - wait for long running jobs to finish
        isZero = YARN.waitForZeroRunningApps()
        ## Temporarily uncommenting to tune test
        #assert isZero, "all long running jobs are not finished"

        ## TODO - Validate long running jobs
        Rollingupgrade.verifyLongRunningJob(components)

        ## TODO - call Teardown for long running jobs
        Rollingupgrade.background_job_teardown(components, None)

        ## Finalize State
        Rollingupgrade.ru_finalize_state(components)
        UpgradePerNode.reportProgress(
            "###  Completed downgrade from %s to %s for components=%s ####" % (currVersion, latestVersion, components)
        )
