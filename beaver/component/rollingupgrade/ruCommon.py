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
from beaver.component.rollingupgrade.ruPhoenix import ruPhoenix
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.component.rollingupgrade.ruSetup import COMPONENTS_TO_IMPORT
from beaver.component.rollingupgrade.ruHdfs import ruHDFS
from beaver.component.rollingupgrade.ruYarn import ruYARN
from beaver.component.rollingupgrade.RuAssert import ruAssert

# dynamically load RU component libraries
for compName in COMPONENTS_TO_IMPORT:
    if compName in ["hdfs", "yarn"]:
        continue
    if "storm-slider" in compName:
        compName = "storm"
    stmt = "from beaver.component.rollingupgrade.ru%s import ru%s" % (compName.title(), compName.title())
    exec stmt

logger = logging.getLogger(__name__)


class RuLogger:
    @classmethod
    def logEvent(cls, message):
        logger.info("**** " + str(message).lower() + " ****")


class Rollingupgrade:
    """
    Class for rolling upgrade common operations
    """
    # increasing number to indicate number of smoke tests performed
    _smoketestnumber = 0

    @classmethod
    def run_longRunning_Application(cls, components=[], runSmokeTestSetup=True, config=None):
        '''
        Runs long running applications for components specified
        :param components: ['hdfs', 'yarn', etc...]
        :type components: list of str
        :return: Total number of long running application for components
        :rtype: int
        '''
        numBackgroundJobs = 0
        logger.info("**** starting setup for background jobs ****")
        logger.info("**** components = %s ****" % components)
        if "hdfs" in components:
            logger.info("**** Running background job for HDFS ****")
            numBackgroundJobs += ruHDFS.run_background_job(runSmokeTestSetup, config)
        if "yarn" in components:
            logger.info("**** Running background job for Yarn ****")
            numBackgroundJobs += ruYARN.run_background_job(runSmokeTestSetup, config)
        if "zookeeper" in components:
            logger.info("No long running jobs are applicable for zookeeper")
            #numBackgroundJobs += ruZookeeper.run_background_job(runSmokeTestSetup, config)
        if "hbase" in components:
            logger.info("**** Running background job for HBase ****")
            numBackgroundJobs += ruHbase.run_background_job(runSmokeTestSetup, config)
        # if "hive" in components:
        #     logger.info("**** Running background job for Hive ****")
        #     numBackgroundJobs += ruHive.run_background_job(runSmokeTestSetup=False, config=config)
        if "pig" in components:
            logger.info("**** Running backgroup job for Pig ****")
            numBackgroundJobs += ruPig.run_background_job(runSmokeTestSetup, config)
        if "oozie" in components:
            logger.info("**** Running background job for Oozie ****")
            numBackgroundJobs += ruOozie.run_background_job(runSmokeTestSetup, config)
        if "falcon" in components:
            logger.info("**** Running background job for Falcon ****")
            numBackgroundJobs += ruFalcon.run_background_job(runSmokeTestSetup, config)
        if "sqoop" in components:
            logger.info("**** Running background job for Sqoop ****")
            numBackgroundJobs += ruSqoop.run_background_job(runSmokeTestSetup, config)
        if "flume" in components:
            logger.info("**** Running background job for Flume ****")
            numBackgroundJobs += ruFlume.run_background_job(runSmokeTestSetup, config)
        if "knox" in components:
            logger.info("No long running jobs are applicable for knox")
            #numBackgroundJobs += ruKnox.run_background_job(runSmokeTestSetup, config)
        if "phoenix" in components:
            logger.info("No long running jobs are applicable for Phoenix. TODO - Champlain +1")
        if "tez" in components:
            logger.info("**** Running background job for Tez ****")
            numBackgroundJobs += ruTez.run_background_job(runSmokeTestSetup, config)
        if "storm" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Running long running background jobs for storm.")
            numBackgroundJobs += ruStorm.run_background_job(runSmokeTestSetup, config, useStandaloneCmd=True)
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Running long running background jobs for storm-slider.")
            numBackgroundJobs += ruStorm.run_background_job(runSmokeTestSetup, config, useStandaloneCmd=False)
        if "mahout" in components:
            logger.info("TODO")
            #numBackgroundJobs += ruMahout.run_background_job(runSmokeTestSetup, config)
        if "argus" in components:
            logger.info("TODO")
            #numBackgroundJobs += ruArgus.run_background_job(runSmokeTestSetup, config)
        if "slider" in components:
            logger.info("**** Running background job for Slider ****")
            numBackgroundJobs += ruSlider.run_background_job(runSmokeTestSetup, config)
        if "hue" in components:
            logger.info("TODO")
            #numBackgroundJobs += ruHue.run_background_job(runSmokeTestSetup, config)
        if "spark" in components:
            logger.info("skipping due to QE-9319")
            #from beaver.component.rollingupgrade.ruSpark import ruSpark
            #numBackgroundJobs += ruSpark.run_background_job(runSmokeTestSetup=True, config=None)
        if "kafka" in components:
            from beaver.component.rollingupgrade.ruKafka import ruKafka
            numBackgroundJobs += ruKafka.run_background_job(runSmokeTestSetup=True, config=None)
        # #### TODO - add long running applications for all other missing components ####
        #### It must return the number of long running applications ####

        return numBackgroundJobs

    @classmethod
    def background_job_setup(cls, components, config=None):
        '''
        Run setup for background long running job
        :param components: Perform long running job setup for components
        :type components: list of str
        :param config: configuration location
        :type config: str
        :return None
        '''
        logger.info("**** Starting setup for background longrunning applications ****")
        logger.info("**** components = %s *****" % components)
        if "hdfs" in components:
            logger.info("**** Running backgroud job setup for HDFS ****")
            ruHDFS.background_job_setup(runSmokeTestSetup=True, config=config)
        if "yarn" in components:
            logger.info("**** Running backgroud job setup for YARN ****")
            ruYARN.background_job_setup(runSmokeTestSetup=True, config=config)
        if "zookeeper" in components:
            logger.info("No long running jobs for zookeeper hence setup is not applicable")
            #ruZookeeper.background_job_setup(runSmokeTestSetup=True, config=config)
        if "hbase" in components:
            logger.info("**** Running backgroud job setup for HBase ****")
            ruHbase.background_job_setup(runSmokeTestSetup=True, config=config)
        # if "hive" in components:
        #     logger.info("**** Running background job setup for Hive ****")
        #     ruHive.background_job_setup(runSmokeTestSetup=False, config=config)
        if "pig" in components:
            logger.info("**** Running backgroud job setup for Pig ****")
            ruPig.background_job_setup(runSmokeTestSetup=True, config=config)
        if "oozie" in components:
            logger.info("**** Running backgroud job setup for Oozie ****")
            ruOozie.background_job_setup(runSmokeTestSetup=True, config=config)
        if "falcon" in components:
            logger.info("**** Running backgroud job setup for Falcon ****")
            ruFalcon.background_job_setup(runSmokeTestSetup=True, config=config)
        if "sqoop" in components:
            logger.info("**** Running backgroud job setup for Sqoop ****")
            ruSqoop.background_job_setup(runSmokeTestSetup=True, config=config)
        if "flume" in components:
            logger.info("**** Running backgroud job setup for Flume ****")
            ruFlume.background_job_setup(runSmokeTestSetup=True, config=config)
        if "knox" in components:
            logger.info("No long running jobs for knox hence setup is not applicable")
            ruKnox.smoke_test_setup()
            #ruKnox.background_job_setup(runSmokeTestSetup=True, config=config)
        if "phoenix" in components:
            logger.info("Phoenix smoke test setup")
            ruPhoenix.background_job_setup(runSmokeTestSetup=True, config=config)
        if "tez" in components:
            logger.info("Tez background job setup")
            ruTez.background_job_setup(runSmokeTestSetup=True, config=config)
        if "storm" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Running setup for long running storm jobs.")
            ruStorm.background_job_setup(runSmokeTestSetup=True, config=config, useStandaloneCmd=True)
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Running setup for long running storm-slider jobs.")
            ruStorm.background_job_setup(runSmokeTestSetup=True, config=config, useStandaloneCmd=False)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.background_job_setup(runSmokeTestSetup=True, config=config)
        if "argus" in components:
            from beaver.component.xa import Xa
            logger.info("TODO")
            components_to_test = []
            if Xa.isHdfsInstalled():
                components_to_test.append('hdfs')
            if Xa.isHiveInstalled():
                components_to_test.append('hive')
            if Xa.isHBaseInstalled():
                components_to_test.append('hbase')
            if Xa.isKnoxInstalled():
                components_to_test.append('knox')
            if Xa.isStormInstalled():
                components_to_test.append('storm')
            ruArgus.background_job_setup(components_to_test, runSmokeTestSetup=True, config=config)
        if "slider" in components:
            ruSlider.background_job_setup(runSmokeTestSetup=True, config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.background_job_setup(runSmokeTestSetup=True, config=config)
        if "spark" in components:
            logger.info("skipping due to QE-9319")
            #from beaver.component.rollingupgrade.ruSpark import ruSpark
            #ruSpark.background_job_setup(runSmokeTestSetup=True, config=None)
        if "kafka" in components:
            from beaver.component.rollingupgrade.ruKafka import ruKafka
            ruKafka.background_job_setup(runSmokeTestSetup=True, config=None)
        if "atlas" in components:
            from beaver.component.rollingupgrade.ruAtlas import ruAtlas
            ruAtlas.background_job_setup(runSmokeTestSetup=True, config=None)
        #### TODO - add setup function for all component #####

    @classmethod
    def run_smokeTests(cls, components=[], config=None):
        '''
        Runs smoke tests for components
        :param components: ['hdfs', 'yarn', etc...]
        :type components: list of str
        :return None
        '''
        logger.info("**** Starting Smoke tests ****")
        logger.info("**** components = %s *****" % components)
        if "hdfs" in components:
            logger.info("**** Running smoke test for HDFS ****")
            cls._smoketestnumber += 1
            ruHDFS.run_smoke_test(cls._smoketestnumber, config=config)
            logger.info("**** Finishing smoke test for HDFS ****")
        if "yarn" in components:
            logger.info("**** Running smoke test for Yarn ****")
            cls._smoketestnumber += 1
            ruYARN.run_smoke_test(cls._smoketestnumber, config=config)
            logger.info("**** Finishing smoke test for Yarn ****")
        if "zookeeper" in components:
            logger.info("**** Running smoke test for Zookeeper ****")
            ruZookeeper.run_smoke_test()
            logger.info("**** Finishing smoke test for Zookeeper ****")
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.run_smoke_test(cls._smoketestnumber, config=config)
        # if "hive" in components:
        #     logger.info("**** Running smoke test for Hive ****")
        #     cls._smoketestnumber += 3
        #     ruHive.run_smoke_test(cls._smoketestnumber, config = config)
        if "pig" in components:
            logger.info("**** Running smoke test for Pig ****")
            cls._smoketestnumber += 1
            ruPig.run_smoke_test(cls._smoketestnumber, config=config)
            logger.info("**** Finishing smoke test for Pig ****")
        if "oozie" in components:
            logger.info("**** Running smoke test for Oozie ****")
            ruOozie.run_smoke_test(cls._smoketestnumber, config=config)
        if "falcon" in components:
            logger.info("**** Running smoke test for Falcon ****")
            ruFalcon.run_smoke_test(cls._smoketestnumber, config=config)
        if "sqoop" in components:
            logger.info("**** Running smoke test for Sqoop ****")
            ruSqoop.run_smoke_test(cls._smoketestnumber, config=config)
        if "knox" in components:
            logger.info("**** Running smoke test for Knox ****")
            ruKnox.run_smoke_test(cls._smoketestnumber, config=config)
        if "phoenix" in components:
            ruPhoenix.run_smoke_test(cls._smoketestnumber, config=config)
        if "tez" in components:
            logger.info("**** Running smoke test for Tez ****")
            ruTez.run_smoke_test(cls._smoketestnumber, config=config)
            logger.info("**** Finishing smoke test for Yarn ****")
        if "storm" in components:
            logger.info("Storm does not have any smoke tests.")
            #ruStorm.run_smoke_test(cls._smoketestnumber, config = config)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.run_smoke_test(cls._smoketestnumber, config = config)
        if "argus" in components:
            from beaver.component.xa import Xa
            logger.info("TODO")
            cls._smoketestnumber += 1
            components_to_test = []
            if Xa.isHdfsInstalled():
                components_to_test.append('hdfs')
            if Xa.isHiveInstalled():
                components_to_test.append('hive')
            if Xa.isHBaseInstalled():
                components_to_test.append('hbase')
            if Xa.isKnoxInstalled():
                components_to_test.append('knox')
            if Xa.isStormInstalled():
                from xa_admin import XAAdmin
                components_to_test.append('storm')
                XAAdmin.sInstance = None
            ruArgus.run_smoke_test(cls._smoketestnumber, config=config, components_to_test=components_to_test)
        if "slider" in components:
            logger.info("**** Running smoke test for slider ****")
            ruSlider.run_smoke_test(cls._smoketestnumber, config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.run_smoke_test(cls._smoketestnumber, config = config)
        if "spark" in components:
            from beaver.component.rollingupgrade.ruSpark import ruSpark
            logger.info("**** Running smoke test for spark ****")
            ruSpark.run_smoke_test()
        if "kafka" in components:
            from beaver.component.rollingupgrade.ruKafka import ruKafka
            logger.info("**** Running smoke test for kafka ****")
            ruKafka.run_smoke_test()
        if "atlas" in components:
            from beaver.component.rollingupgrade.ruAtlas import ruAtlas
            logger.info("**** Running smoke test for atlas ****")
            ruAtlas.run_smoke_test()
        #### TODO - add smoke test for all missing components ####

    @classmethod
    def upgrade_master_and_smoketest(cls, components, version, config=None, currVersion=None):
        '''
        Upgrades Master services of specific components and runs smoke test after each master upgrade.
        :param components: ["hdfs", "yarn", "hbase"]
        :type components: list of str
        :param version: latestVersion
        :type version: str
        :return: number of background jobs
        :rtype: int
        '''
        numBackgroundJobs = 0
        if "hdfs" in components:
            ruHDFS.upgrade_master(version, config=config)
            cls.run_smokeTests(["hdfs"], config=config)
        if "yarn" in components:
            ruYARN.upgrade_master(version, config=config)
            cls.run_smokeTests(["yarn"], config=config)
            # yarn also starts another long running job
            numBackgroundJobs = ruYARN.run_second_background_job(config=config)
        if "zookeeper" in components:
            logger.info("**** Upgrading Zookeeper servers ****")
            ruZookeeper.upgrade_master(version, config=config)
            #cls.run_smokeTests(["zookeeper"], config=config)
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.upgrade_master(version, config=config)
            cls.run_smokeTests(["hbase"], config=config)
        if "hive" in components:
            logger.info("**** Upgrading Hive component services ****")
            ruHive.upgrade_master(version, config=config)
            cls.run_smokeTests(["hive"], config=config)
        if "pig" in components:
            logger.info("**** Pig has no master to upgrade ****")
        if "oozie" in components:
            ruOozie.upgrade_master(version, config=config)
            cls.run_smokeTests(["oozie"], config=config)
        if "falcon" in components:
            ruFalcon.upgrade_master(version, config=config)
            cls.run_smokeTests(["falcon"], config=config)
        if "sqoop" in components:
            logger.info("**** Sqoop does not have a master ****")
        if "flume" in components:
            ruFlume.upgrade_master(version, config=config)
        if "knox" in components:
            logger.info("TODO")
            ruKnox.upgrade_master(version, config=config)
            cls.run_smokeTests(["knox"], config=config)
        if "phoenix" in components:
            logger.info("**** Phoenix does not have any master  ****")
        if "tez" in components:
            logger.info("**** Tez does not have any master  ****")
        if "storm" in components:
            logger.info("Storm does not support upgrade/downgrade")
            #ruStorm.upgrade_master(version, config=config)
            #cls.run_smokeTests(["storm"], config=config)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.upgrade_master(version, config=config)
            #cls.run_smokeTests(["mahout"], config=config)
        if "argus" in components:
            from xa_admin import XAAdmin
            logger.info("TODO")
            ruArgus.upgrade_master(version, config, currVersion)
            XAAdmin.sInstance = None
            cls.run_smokeTests(["argus"], config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.upgrade_master(version, config=config)
            #cls.run_smokeTests(["argus"], config=config)
        #### TODO - add Hbase master service upgrade and run smoke test####
        return numBackgroundJobs

    @classmethod
    def upgrade_slave_and_smoketest(cls, components, version, node, config=None, runSmoke=True):
        '''
        Upgrades Slave services and runs smoke test after each slave upgrade.
        :param components: ["hdfs", "yarn", "hbase"]
        :type components: list of str
        :param version: latestVersion
        :type version: str
        :param node: Hostname where slave service is running
        :type node str
        :return None
        '''
        if "hdfs" in components:
            logger.info("*** Upgrade Slaves for HDFS ***")
            ruHDFS.upgrade_slave(version, node, config)
            if runSmoke:
                cls.run_smokeTests(["hdfs"], config=config)
        if "yarn" in components:
            logger.info("*** Upgrade Slaves for YARN ***")
            ruYARN.upgrade_slave(version, node, config)
            if runSmoke:
                cls.run_smokeTests(["yarn"], config=config)
        if "zookeeper" in components:
            logger.info("No Slaves for Zookeeper")
            #ruZookeeper.upgrade_slave(version, node, config)
            #if runSmoke:
            #  cls.run_smokeTests(["zookeeper"], config=config)
        if "hbase" in components:
            logger.info("*** Upgrade Slaves for HBase ***")
            ruHbase.upgrade_slave(version, node, config)
            if runSmoke:
                cls.run_smokeTests(["hbase"], config=config)
        if "hive" in components:
            logger.info("Hive has no slaves to upgrade")
        if "pig" in components:
            logger.info("**** Pig has no slaves to upgrade ****")
        if "oozie" in components:
            logger.info("Oozie has no slaves")
        if "falcon" in components:
            logger.info("Falcon has no slaves")
        if "sqoop" in components:
            logger.info("**** No slave components for Sqoop to upgrade ****")
        if "knox" in components:
            logger.info("**** No slave components for Knox to upgrade ****")
        if "phoenix" in components:
            logger.info("**** No slave components for Phoenix to upgrade ****")
        if "tez" in components:
            logger.info("**** Tez has no slave **** ")
        if "storm" in components:
            logger.info("Storm does not support rolling upgrade/downgrade.")
            #ruStorm.upgrade_slave(version, node, config)
            #cls.run_smokeTests(["storm"], config=config)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.upgrade_slave(version, node, config)
            #cls.run_smokeTests(["mahout"], config=config)
        if "argus" in components:
            logger.info("TODO")
            #ruArgus.upgrade_slave(version, node, config)
            #cls.run_smokeTests(["argus"], config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.upgrade_slave(version, node, config)
            #cls.run_smokeTests(["argus"], config=config)
        #### TODO - add Hbase region server upgrade and run smoke test ####

    @classmethod
    def upgrade_client_insideCluster_and_smoketest(cls, components, latestVersion, config=None):
        '''
        Upgrade clients and run smoke test after upgrading clients
        :param components: components needs to be upgraded
        :type components: list of str
        :param latestVersion: Version to move to
        :type latestVersion: str
        :param config: Configuration location
        :type config: str
        :return None
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "### Upgrading clients [%s] to version %s inside the cluster ####" % (components, latestVersion)
        )
        # BUG-25239 is resolved, only one command will alter version for hadoop-client, hadoop-hdfs-client, hadoop-mapreduce-client, pig-client, tez-client and hive-client Togather
        nodes = Cluster.getAllclusterNodes(components)
        if "slider" in components:
            logger.info("Upgrading slider-client on gateway")
            from beaver.component.rollingupgrade.ruSlider import ruSlider
            ruSlider.upgrade_client_gateway(latestVersion, HDFS.getGateway())
        for node in nodes:
            UpgradePerNode.reportProgress("[INFO][CLIENT][Upgrade] Upgrading clients on %s " % node)
            if "hdfs" in components:
                logger.info(
                    "upgrade hadoop-client, hadoop-hdfs-client, hadoop-mapreduce-client, pig-client, tez-client and hive-client"
                )
                hdpSelect.changeVersion("hadoop-client", latestVersion, node)
            if "zookeeper" in components:
                logger.info("upgrade zookeeper-client")
                hdpSelect.changeVersion("zookeeper-client", latestVersion, node)
            if "hbase" in components:
                logger.info("upgrade hbase-client")
                hdpSelect.changeVersion("hbase-client", latestVersion, node)
            if "oozie" in components:
                logger.info("upgrade oozie-client")
                hdpSelect.changeVersion("oozie-client", latestVersion, node)
            if "falcon" in components:
                logger.info("upgrade falcon-client")
                hdpSelect.changeVersion("falcon-client", latestVersion, node)
            if "sqoop" in components:
                logger.info("upgrade sqoop-client")
                hdpSelect.changeVersion("sqoop-client", latestVersion, node)
                # we are upgrading sqoop-server here as there is no sqoop service in Champlain
                # but potentially with Sqoop 2 this could change
                hdpSelect.changeVersion("sqoop-server", latestVersion, node)
            if "phoenix" in components:
                logger.info("upgrade phoenix-client")
                hdpSelect.changeVersion("phoenix-client", latestVersion, node)
            if "slider" in components:
                from beaver.component.rollingupgrade.ruSlider import ruSlider
                logger.info("upgrade slider-client")
                ruSlider.upgrade_client(latestVersion, node)
                #hdpSelect.changeVersion("slider-client", latestVersion, gateway)
                logger.info("TODO- slider app package upgrade if necessary")
                UpgradePerNode.reportProgress("### TDOO - Slider client NOT upgraded ####")
            if "mahout" in components:
                logger.info("upgrade mahout-client")
                #hdpSelect.changeVersion("mahout-client", latestVersion, node)
                UpgradePerNode.reportProgress("### TDOO - mahout client NOT upgraded ####")
        # After upgrading all clients Run Cli Smoke test for each component. Pass mapreduce properties as env
        UpgradePerNode.reportProgress(
            "### Running smoke tests for new version of clients [%s] inside the cluster ####" % components
        )
        Rollingupgrade.run_client_insideCluster_smoketest(components, latestVersion, config=config)
        UpgradePerNode.reportProgress(
            "### Finished Upgrading clients [%s] to version %s inside the cluster ####" % (components, latestVersion)
        )

    @classmethod
    def run_client_insideCluster_smoketest(cls, components, latestVersion, config=None, env=None):
        '''
        Run Smoke test after upgrading All CLIs.
        Pass mapreduce Tar ball and Application classpath as Env variable
        :param components: Component list
        :type component list of str
        :param config: Configuration location
        :type config str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("**** Running HDFS client smoke test ****")
            ruHDFS.run_client_smoketest(config=config, env=env)
        if "yarn" in components:
            logger.info("**** Running YARN client smoke test ****")
            ruYARN.run_client_smoketest(config=config, env=env)
        if "zookeeper" in components:
            logger.info("**** Running ZK client smoke test ****")
            ruZookeeper.run_client_smoketest(config=config, env=env)
        if "hbase" in components:
            logger.info("**** Running HBase client smoke test ****")
            ruHbase.run_client_smoketest(config=config, env=env)
        if "hive" in components:
            logger.info("**** Running Hive client smoke test ****")
            ruHive.run_client_smoketest(config=config, env=env)
        if "pig" in components:
            logger.info("**** Running Pig client smoke test ****")
            ruPig.run_client_smoketest(config=config, env=env)
        if "oozie" in components:
            logger.info("**** Running Oozie client smoke test ****")
            ruOozie.run_client_smoketest(config=config, env=env)
        if "falcon" in components:
            ruFalcon.run_client_smoketest(config=config, env=env)
        if "sqoop" in components:
            logger.info("**** Running Sqoop client smoke test ****")
            ruSqoop.run_client_smoketest(config=config, env=env)
        if "phoenix" in components:
            ruPhoenix.run_client_smoketest(config=config, env=env)
        if "tez" in components:
            ruTez.run_client_smoketest(config=config, env=env)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.run_client_smoketest(config=config, env=env)
        if "slider" in components:
            logger.info("TODO: if slider smoke test is supported")
            ruSlider.run_client_smoketest(config, env)
        if "argus" in components:
            logger.info("TODO")
            #ruArgus.run_client_smoketest(config=config, env=env)

    @classmethod
    def testAfterAllMasterSlavesUpgraded(cls, components):
        '''
        Function to confirm that upgrade is proper after all master/slave is upgraded. Runs smoke test
        :param components: ["hdfs", "yarn", "hbase"]
        :type components: list of str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("****  start hdfs test after restarting all slaves ****")
            ruHDFS.testAfterAllSlavesRestarted()
            logger.info("*****  end hdfs smoke test after restarting all slaves ****")
        if "yarn" in components:
            logger.info("****  start Yarn test after restarting all slaves ****")
            ruYARN.testAfterAllSlavesRestarted()
            logger.info("*****  end Yarn smoke test after restarting all slaves ****")
        if "zookeeper" in components:
            logger.info("TODO")
            ruZookeeper.testAfterAllSlavesRestarted()
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.testAfterAllSlavesRestarted()
        if "hive" in components:
            ruHive.testAfterAllSlavesRestarted()
        if "pig" in components:
            logger.info("*****  Pig has no slaves to test after Restart ****")
        if "oozie" in components:
            logger.info("****  start Oozie test after restarting all slaves ****")
            ruOozie.testAfterAllSlavesRestarted()
            logger.info("*****  end Oozie smoke test after restarting all slaves ****")
        if "falcon" in components:
            ruFalcon.testAfterAllSlavesRestarted()
        if "sqoop" in components:
            ruSqoop.testAfterAllSlavesRestarted()
        if "knox" in components:
            logger.info("****  start knox smoke test after upgrading all masters and slaves ****")
            ruKnox.testAfterAllSlavesRestarted()
            logger.info("****  end knox smoke test after upgrading all masters and slaves ****")
        if "phoenix" in components:
            ruPhoenix.testAfterAllSlavesRestarted()
        if "tez" in components:
            logger.info("****  start Tez test after restarting all slaves ****")
            ruTez.testAfterAllSlavesRestarted()
            logger.info("*****  end Tez smoke test after restarting all slaves ****")
        if "storm" in components:
            logger.info("Storm does not support rolling upgrade/downgrade.")
            #ruStorm.testAfterAllSlavesRestarted()
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.testAfterAllSlavesRestarted()
        if "argus" in components:
            logger.info("TODO")
        if "argus" in components:
            from beaver.component.xa import Xa
            logger.info("TODO")
            components_to_test = []
            if Xa.isHdfsInstalled():
                components_to_test.append('hdfs')
            if Xa.isHiveInstalled():
                components_to_test.append('hive')
            if Xa.isHBaseInstalled():
                components_to_test.append('hbase')
            if Xa.isKnoxInstalled():
                components_to_test.append('knox')
            if Xa.isStormInstalled():
                components_to_test.append('storm')
            ruArgus.testAfterAllSlavesRestarted(components_to_test)
        if "slider" in components:
            logger.info("**** start slider test after all slave restarted ****")
            ruSlider.testAfterAllSlavesRestarted()
            logger.info("**** end slider test after all slave restarted ****")
        if "hue" in components:
            logger.info("TODO")
            #ruHue.testAfterAllSlavesRestarted()

        #### TODO - add Test for Hbase which runs after upgrading All master and slave services for Hdfs/yarn and Hbase ####

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls, components):
        '''
        Prepare components to start Upgrade
        :param components: list of components
        :type components: list of str
        :return: None
        '''
        logger.info("Preparing components for upgrade (prepare states)")
        if "hdfs" in components:
            logger.info("hdfs prepare state")
            ruHDFS.ru_prepare_save_state_for_upgrade()
        if "hbase" in components:
            logger.info("hbase prepare state")
            ruHbase.ru_prepare_save_state_for_upgrade()
        #### TODO - Use this function to prepare component for upgrade. Not mendatory for all components #####

    @classmethod
    def ru_finalize_state(cls, components):
        '''
        Prepare components to run finalize state
        :param components: list of components
        :type components: list of str
        :return: None
        '''
        logger.info("Preparing components for finalizing upgrade")
        if "hdfs" in components:
            logger.info("hdfs finalize state")
            ruHDFS.ru_finalize_state()
        #### TODO - Use this function to prepare component for upgrade. Not mendatory for all components #####

    @classmethod
    def background_job_teardown(cls, components, config=None):
        '''
        Run background long running job cleanup for all component
        :param components: list of components
        :type components: list of str
        :param config: Location of config
        :type config: str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("Run cleanup for HDFS long running job")
            ruHDFS.background_job_teardown()
        if "yarn" in components:
            logger.info("Run cleanup for Yarn long running job")
            ruYARN.background_job_teardown()
        if "zookeeper" in components:
            logger.info("No long running jobs for zookeeper hence verfication of jobs is not applicable")
            #ruZookeeper.background_job_teardown()
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.background_job_teardown()
        if "hive" in components:
            ruHive.background_job_teardown()
        if "pig" in components:
            logger.info("Run cleanup for Pig long running job")
            ruPig.background_job_teardown()
        if "oozie" in components:
            logger.info("Run cleanup for Oozie long running job")
            ruOozie.background_job_teardown()
        if "falcon" in components:
            ruFalcon.background_job_teardown()
        if "sqoop" in components:
            ruSqoop.background_job_teardown()
        if "flume" in components:
            ruFlume.background_job_teardown()
        if "knox" in components:
            logger.info("No long running jobs for knox hence backgrond job teardown is not applicable")
            #ruKnox.background_job_teardown()
        if "phoenix" in components:
            logger.info("No long running jobs for phoenix hence backgrond job teardown is not applicable")
        if "tez" in components:
            logger.info("Run cleanup for Tez long running job")
            ruTez.background_job_teardown()
        if "storm" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("running cleanup for storm")
            ruStorm.background_job_teardown(useStandaloneCmd=True)
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("running cleanup for storm-slider.")
            ruStorm.background_job_teardown(useStandaloneCmd=False)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.background_job_teardown()
        if "argus" in components:
            logger.info("TODO")
            ruArgus.background_job_teardown()
        if "slider" in components:
            logger.info("TODO")
            #ruSlider.background_job_teardown()
        if "hue" in components:
            logger.info("TODO")
            #ruHue.background_job_teardown()
        if "spark" in components:
            logger.info("skipping due to QE-9319")
            #from beaver.component.rollingupgrade.ruSpark import ruSpark
            #ruSpark.background_job_teardown()
        if "kafka" in components:
            from beaver.component.rollingupgrade.ruKafka import ruKafka
            ruKafka.background_job_teardown()
        #### TODO - clean up for long running background jobs. All component must do cleanup ####

    @classmethod
    def verifyLongRunningJob(cls, components, config=None):
        '''
        Function to verify all Long running Apps are finished as expected
        :param components: list of components
        :type components: list of str
        :param config: Location of config
        :type config: str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("Run verify long running app for Hdfs")
            ruHDFS.verifyLongRunningJob(config=config)
        if "yarn" in components:
            logger.info("Run verify long running app for Yarn")
            ruYARN.verifyLongRunningJob(config=config)
        if "zookeeper" in components:
            logger.info("No long running jobs for zookeeper hence verfication of jobs is not applicable")
            #ruZookeeper.verifyLongRunningJob()
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.verifyLongRunningJob()
        # if "hive" in components:
        #     ruHive.verifyLongRunningJob()
        if "pig" in components:
            logger.info("Run verify long running app for Pig")
            ruPig.verifyLongRunningJob()
        if "oozie" in components:
            ruOozie.verifyLongRunningJob()
        if "falcon" in components:
            ruFalcon.verifyLongRunningJob()
        if "sqoop" in components:
            ruSqoop.verifyLongRunningJob()
        if "flume" in components:
            ruFlume.verifyLongRunningJob()
        if "knox" in components:
            logger.info("No long running jobs for knox hence verfication of jobs is not applicable")
            #ruKnox.verifyLongRunningJob()
        if "phoenix" in components:
            logger.info("No long running jobs for Phoenix hence verfication of jobs is not applicable")
        if "tez" in components:
            logger.info("Run verify long running app for Tez")
            ruTez.verifyLongRunningJob()
        if "storm" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Verifying long running jobs for storm")
            ruStorm.verifyLongRunningJob(useStandaloneCmd=True)
        if "storm-slider" in components:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            logger.info("Verifying long running jobs for storm-slider")
            ruStorm.verifyLongRunningJob(useStandaloneCmd=False)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.verifyLongRunningJob()
        if "argus" in components:
            logger.info("TODO")
            #ruArgus.verifyLongRunningJob()
        if "slider" in components:
            logger.info("TODO")
            #ruSlider.verifyLongRunningJob()
        if "hue" in components:
            logger.info("TODO")
            #ruHue.verifyLongRunningJob()
        if "spark" in components:
            logger.info("skipping due to QE-9319")
            #from beaver.component.rollingupgrade.ruSpark import ruSpark
            #ruSpark.verifyLongRunningJob()
        if "kafka" in components:
            from beaver.component.rollingupgrade.ruKafka import ruKafka
            ruKafka.verifyLongRunningJob()
        if "atlas" in components:
            from beaver.component.rollingupgrade.ruAtlas import ruAtlas
            ruAtlas.verifyLongRunningJob()
        #### TODO - Validate all long running application after they finishes. All components must perform validation ####

    @classmethod
    def downgrade_master_and_smoketest(cls, components, version, config=None, currVersion=None):
        '''
        Downgrade master services and run smoke test
        :param components: component lists
        :type components: list of str
        :param version: Version to be downgraded to (version 0)
        :type version: str
        :param config: Configuration to look at
        :type config: str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("TODO")
            ruHDFS.downgrade_master(version, config=config)
            cls.run_smokeTests(["hdfs"], config=config)
        if "yarn" in components:
            logger.info("TODO")
            #ruYARN.downgrade_master(version, config=config)
            #cls.run_smokeTests(["yarn"], config=config)
            # yarn also starts another long running job
            #numBackgroundJobs = ruYARN.run_second_background_job(config=config)
        if "zookeeper" in components:
            logger.info("**** Downgrading Zookeeper servers ****")
            ruZookeeper.downgrade_master(version, config=config)
            #cls.run_smokeTests(["zookeeper"], config=config)
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.downgrade_master(version, config=config)
            cls.run_smokeTests(["hbase"], config=config)
        if "hive" in components:
            ruHive.downgrade_master(version, config=config)
            cls.run_smokeTests(["hive"], config=config)
        if "pig" in components:
            logger.info("**** Pig has no master to downgrade ****")
        if "oozie" in components:
            ruOozie.downgrade_master(version, config=config)
            cls.run_smokeTests(["oozie"], config=config)
        if "falcon" in components:
            ruFalcon.downgrade_master(version, config=config)
            cls.run_smokeTests(["falcon"], config=config)
        if "sqoop" in components:
            logger.info("**** Sqoop does not have a master ****")
        if "flume" in components:
            ruFlume.downgrade_master(version, config=config)
        if "knox" in components:
            logger.info("TODO")
            #ruKnox.downgrade_master(version, config=config)
            #cls.run_smokeTests(["knox"], config=config)
        if "phoenix" in components:
            logger.info("**** Phoniex not have a master ****")
        if "tez" in components:
            logger.info("**** Copying tez tar.gz to HDFS ****")
            #ruTez.downgrade_master(version, config=config)
            #logger.info("**** Running Tez job after upgrading master ****")
            #cls.run_smokeTests(["tez"], config=config)
        if "storm" in components:
            logger.info("Storm does not support rolling upgrade/downgrade.")
            #ruStorm.downgrade_master(version, config=config)
            #cls.run_smokeTests(["storm"], config=config)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.downgrade_master(version, config=config)
            #cls.run_smokeTests(["mahout"], config=config)
        if "argus" in components:
            from xa_admin import XAAdmin
            logger.info("TODO")
            ruArgus.downgrade_master(version, config, currVersion)
            XAAdmin.sInstance = None
            cls.run_smokeTests(["argus"], config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.downgrade_master(version, config=config)
            #cls.run_smokeTests(["argus"], config=config)

    @classmethod
    def downgrade_slave_and_smoketest(cls, components, version, node, config=None):
        '''
        Downgrade Slave services and run smoke test
        :param components: component lists
        :type components: list of str
        :param version: Version to be downgraded to (version 0)
        :type version: str
        :param config: Configuration to look at
        :type config: str
        :return: None
        '''
        if "hdfs" in components:
            logger.info("TODO")
            ruHDFS.downgrade_slave(version, node, config=config)
            cls.run_smokeTests(["hdfs"], config=config)
        if "yarn" in components:
            logger.info("TODO")
            ruYARN.downgrade_slave(version, node, config=config)
            cls.run_smokeTests(["yarn"], config=config)
            # yarn also starts another long running job
            numBackgroundJobs = ruYARN.run_second_background_job(config=config)
        if "zookeeper" in components:
            logger.info("Zookeeper has no slaves to downgrade")
            #ruZookeeper.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["zookeeper"], config=config)
        if "hbase" in components:
            logger.info("TODO")
            ruHbase.downgrade_slave(version, node, config=config)
            cls.run_smokeTests(["hbase"], config=config)
        if "hive" in components:
            logger.info("Hive has no slaves to downgrade")
        if "pig" in components:
            logger.info("**** Pig has no slaves to downgrade ****")
        if "oozie" in components:
            logger.info("Oozie has no slaves to downgrade")
        if "falcon" in components:
            logger.info("Falcon has no slaves to downgrade")
        if "sqoop" in components:
            logger.info("Sqoop has no slaves to downgrade")
        if "knox" in components:
            logger.info("TODO")
            #ruKnox.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["knox"], config=config)
        if "phoenix" in components:
            logger.info("Phoniex has no slaves to downgrade")
        if "tez" in components:
            logger.info("TODO")
            #ruTez.downgrade_slave(version, node, config=config)
            #logger.info("**** Running Tez job after upgrading master ****")
            #cls.run_smokeTests(["tez"], config=config)
        if "storm" in components:
            logger.info("Storm does not support rolling upgrade/downgrade.")
            #ruStorm.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["storm"], config=config)
        if "mahout" in components:
            logger.info("TODO")
            #ruMahout.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["mahout"], config=config)
        if "argus" in components:
            logger.info("TODO")
            #ruArgus.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["argus"], config=config)
        if "hue" in components:
            logger.info("TODO")
            #ruHue.downgrade_slave(version, node, config=config)
            #cls.run_smokeTests(["argus"], config=config)

    @classmethod
    def downgrade_client_insideCluster_and_smoketest(cls, components, latestVersion, config=None):
        '''
        downgrade clients and run smoke test after upgrading clients
        :param components: component lists
        :type components: list of str
        :param version: Version to be downgraded to (version 0)
        :type version: str
        :param config: Configuration to look at
        :type config: str
        :return: None
        '''
        cls.upgrade_client_insideCluster_and_smoketest(components, latestVersion, config=config)


class hdpRelease:
    """
    Class for querying info related to HDP Release
    """

    # HDP root path
    _root = "/usr/hdp"

    # HDP current root path
    _current = os.path.join(_root, "current")

    @classmethod
    def getCurrentRelease(cls, component):
        '''
        get current version of specific component in action
        :param component: Add component like "hadoop", "hive"
        :return: current version in use like "2.2.0.1-177"
        :rtype: str or None
        '''
        # get path of component symlink
        linkname = cls._current + "/" + component
        logger.info(linkname)
        if os.path.isdir(linkname):
            (head, tail) = os.path.split(os.readlink(linkname))
            logger.info(head)
            logger.info(tail)
            return os.path.basename(head)
        else:
            logger.info("No valid symlink for component %s" % component)
            return None

    @classmethod
    def get_correct_release(cls, MaxorMin, releases):
        '''
        get Max or Min HDP release number
        :param MaxorMin: Pass "max" or "min" as string to get higher or lower release number
        :type MaxorMin: str
        :param releases: List of release versions. Only 2 versions are supported ['2.2.0.0-123', '2.2.0.0-124']
        :type releases: a list of str
        :return: Max /Min release , for Max: returns '2.2.0.0-124', for min: returns '2.2.0.0-123'
        :rtype: str
        '''
        if len(releases) == 1:
            RuLogger.logEvent("No side-by-side installation found, flip to the same version instead!")
            releases.append(releases[0])
        m0 = re.search('.*-([0-9]*)', releases[0])
        m1 = re.search('.*-([0-9]*)', releases[1])
        ver0 = int(m0.group(1))
        ver1 = int(m1.group(1))
        if MaxorMin == "max":
            final = max(ver0, ver1)
        else:
            final = min(ver0, ver1)
        if final == ver0:
            return releases[0]
        else:
            return releases[1]

    @classmethod
    def getLatestRelease(cls):
        '''
        :return: latest version of HDP installed on the Machine
        :rtype: str
        '''
        ss = []
        for f in os.listdir(cls._root):
            if f not in [".", "..", "current"]:
                ss.extend([f])
        return cls.get_correct_release("max", ss)

    @classmethod
    def getOldestRelease(cls):
        '''
        :return: oldest version of HDP installed on the Machine
        :rtype: str
        '''
        ss = []
        for f in os.listdir(cls._root):
            if f not in [".", "..", "current"]:
                ss.extend([f])
        return cls.get_correct_release("min", ss)

    @classmethod
    def getLatestTezTargz(cls):
        """
        Get latest version of tez tarball.
        :return: latest version of tez tarball.
        :rtype: str or None
        """
        version = cls.getLatestRelease()
        tezTarFile = ""
        filename = "tez-*%s*.tar.gz" % version
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath="/usr/hdp/" + version,
            searchstr=filename,
            passwd=Machine.getAdminPasswd()
        )
        if lines:
            tezTarFile = lines[0]
        else:
            tezTarFile = None
        return tezTarFile

    @classmethod
    def getLatestMRTargz(cls):
        """
        Get latest version of MR tarball.
        :return: latest version of MR tarball.
        :rtype: str or None.
        """
        version = cls.getLatestRelease()
        filename = "mr-*%s*.tar.gz" % version
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath="/usr/hdp/" + version,
            searchstr=filename,
            passwd=Machine.getAdminPasswd()
        )
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    def getOldestMRTargz(cls):
        """
        Get oldest version of MR tarball.
        :return: oldest version of MR tarball.
        :rtype: str or None.
        """
        version = cls.getOldestRelease()
        filename = "mr-*%s*.tar.gz" % version
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath="/usr/hdp/" + version,
            searchstr=filename,
            passwd=Machine.getAdminPasswd()
        )
        if lines:
            return lines[0]
        else:
            return None


class hdpSelect:
    """
    Class for operations related to HDP Select
    """

    @classmethod
    def gethdpselectLocation(cls):
        '''
        :return: location of hdp-select file
        :rtype: str
        '''
        return "/usr/bin/hdp-select"

    @classmethod
    def changeVersion(cls, package, version, host=None):
        '''
        Changes the version of package on desired host
        Runs "hdp-select set <package> <version>"
        :param package: Installed package name such as "hadoop-client"
        :type package: str
        :param version: Version to move to such as "2.2.0.0-889"
        :type version: str
        :param host: Node name
        :return: exitcode of "hdp-select set <package> <version>"
        :rtype: int
        '''
        if not package or not version:
            return 1
        cmd = " %s set %s %s" % (cls.gethdpselectLocation(), package, version)
        exit_code, stdout = Machine.runPythonAs(
            Machine.getAdminUser(),
            cmd,
            host=host,
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        ruAssert("hdpSelect", exit_code == 0, "[ChangeVersion] upgrade of %s failed to %s " % (package, version))
        return exit_code


class Cluster:
    @classmethod
    def getAllclusterNodes(cls, components):
        '''
        Get list of nodes present in cluster
        :param components: Components needs to be tested
        :return: List of unique nodes for the components
        '''
        nodes = []
        if "hdfs" in components or "yarn" in components:
            hdfs_nodes = list(Hadoop.getAllNodes())
            nodes = list(set(hdfs_nodes + nodes))
        if "zookeeper" in components:
            from beaver.component.zookeeper import Zookeeper
            zoo_nodes = list(Zookeeper.getZKHosts())
            nodes = list(set(zoo_nodes + nodes))
        if "tez" in components:
            tez_nodes = [HDFS.getGateway()]
            nodes = list(set(tez_nodes + nodes))
        if "storm" in components:
            logger.info("TODO")
        if "sqoop" in components:
            sqoop_nodes = [HDFS.getGateway()]
            nodes = list(set(sqoop_nodes + nodes))
        if "slider" in components:
            slider_nodes = [HDFS.getGateway()]
            nodes = list(set(slider_nodes + nodes))
        if "pig" in components:
            pig_nodes = [HDFS.getGateway()]
            nodes = list(set(pig_nodes + nodes))
        if "phoenix" in components:
            phoenix_nodes = [HDFS.getGateway()]
            nodes = list(set(phoenix_nodes + nodes))
        if "oozie" in components:
            from beaver.component.oozie import Oozie
            oozie_nodes = list(Oozie.getOozieServers())
            nodes = list(set(oozie_nodes + nodes))
        if "knox" in components:
            knox_nodes = [HDFS.getGateway()]
            nodes = list(set(knox_nodes + nodes))
        if "mahout" in components:
            logger.info("TODO")
        if "hive" in components:
            from beaver.component.hive import Hive
            hive_nodes = [Hive.getHiveHost()]
            nodes = list(set(hive_nodes + nodes))
        if "hbase" in components:
            from beaver.component.hbase import HBase
            hbase_nodes = list(HBase.getSelectedNodes({'services': ['all']}))
            nodes = list(set(hbase_nodes + nodes))
        if "flume" in components:
            logger.info("No servies. Check with deepesh")
        if "falcon" in components:
            from beaver.component.falcon import Falcon
            falcon_node = [Falcon.get_falcon_server()]
            nodes = list(set(falcon_node + nodes))
        #if "argus" in components:
        #    from beaver.component.xa import Xa
        #    xa_node = list(Xa.getPolicyAdminHost())
        #    nodes = list(set(xa_node + nodes))
        return nodes
