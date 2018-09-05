# Common operations for any stack upgrade method - Express Upgrade, Rolling Upgrade, Patch Upgrade(comming soon)

import logging
import os
import re
from upgradelogger import UpgradeLogger

from beaver import util
from upgradecommon import Upgrade
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


# This class contains functions specific to Rolling Upgrade of cluster. It is a derived class of Upgrade
class RollingUpgrade(Upgrade):

    # Constructor for RollingUpgrade class. Initializes common variables to be used across by RU tests
    def __init__(self):
        super(RollingUpgrade, self).__init__()
        self.AMBARI_PROP_FILE = os.path.join(
            Config.getEnv('WORKSPACE'), '..', 'ambari_deploy', 'uifrm', 'ambari.properties'
        )
        self.AMBARI_OLD_PROP_FILE = os.path.join(
            Config.getEnv('WORKSPACE'), '..', 'ambari_deploy', 'uifrm_old', 'uifrm', 'ambari.properties'
        )
        self.COMPONENTS_TO_TEST = []
        self.DO_DOWNGRADE = False

    # This function updates parameters for Long Running jobs
    def update_lr_job_parameters(self):
        from beaver import beaverConfigs
        beaverConfigs.setConfigs()

        #  get JAVA_HOME from the hadoop configs
        hadoopConfDir = Config.get('hadoop', 'HADOOP_CONF')
        hadoopEnvFile = os.path.join(hadoopConfDir, 'hadoop-env.sh')
        javaHome = util.getPropertyValueFromFile(hadoopEnvFile, 'JAVA_HOME')
        Config.set('machine', 'JAVA_HOME', javaHome, overwrite=True)
        Config.set('machine', 'JAVA_CMD', os.path.join(javaHome, 'bin', 'java'), overwrite=True)
        AMBARI_PROP_FILE = os.path.join(
            Config.getEnv('WORKSPACE'), '..', 'ambari_deploy', 'uifrm', 'ambari.properties'
        )

        self.read_and_update_job_parameters()
        #shutil.copy(AMBARI_PROP_FILE, self.LOCAL_WORK_DIR)

        Config.set('xasecure', 'XA_KMS_HOME', '')

    # This function prepares the environment and starts Long Running (LR) Jobs for various components
    def prepare_and_start_long_running_jobs(self):
        ############################Prepare and start long running jobs
        self.find_components_to_test()

        import beaver.component.rollingupgrade.ruSetup as ruSetup

        ruSetup.COMPONENTS_TO_FLIP = self.COMPONENTS_TO_TEST
        ruSetup.COMPONENTS_AFFECTED = self.COMPONENTS_TO_TEST
        ruSetup.COMPONENTS_TO_TEST = self.COMPONENTS_TO_TEST
        ruSetup.COMPONENTS_TO_IMPORT = self.COMPONENTS_TO_TEST

        from beaver.component.hadoop import HDFS
        if "slider" in self.COMPONENTS_TO_TEST:
            from beaver.component.rollingupgrade.ruSlider import ruSlider
        from beaver.component.rollingupgrade.ruCommon import Rollingupgrade
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        HDFS.createUserDirWithGroup(
            '/user/' + Config.get('hadoop', 'HADOOPQA_USER'),
            'hdfs',
            Config.get('hadoop', 'HADOOPQA_USER'),
            'hdfs',
            diffGroup=True
        )
        logger.info(Config.get('hadoop', 'HADOOPQA_USER'))
        logger.info("1===========================================================================")

        self.setup_ru_cluster()
        logger.info("2===========================================================================")
        logger.info("3===========================================================================")
        DN = HDFS.getDatanodes()

        # Find core components (HDFS, YARN, HBase) if exist.
        #core_components = UpgradePerNode.find_existing_core_components(COMPONENTS_TO_TEST)
        logger.info("4===========================================================================")
        #Prepare and save state before upgrade
        Rollingupgrade.ru_prepare_save_state_for_upgrade(self.COMPONENTS_TO_TEST)
        logger.info("5===========================================================================")
        # Run setup for background Jobs for all components
        Rollingupgrade.background_job_setup(self.COMPONENTS_TO_TEST, config=None)

        logger.info("Running smoke tests before upgrade")
        Rollingupgrade.run_smokeTests(self.COMPONENTS_TO_TEST)

        logger.info("6===========================================================================")
        # Starts Long running background Jobs for all components
        numBackgroundJobs = Rollingupgrade.run_longRunning_Application(self.COMPONENTS_TO_TEST, config=None)
        logger.info("7===========================================================================")
        logger.info("Total number of long running background jobs before starting upgrade is %s" % numBackgroundJobs)
        UpgradePerNode.reportProgress("###  Just started %s background jobs  ###" % numBackgroundJobs)
        UpgradeLogger.reportProgress("###  Just started %s background jobs  ###" % numBackgroundJobs, True)

    # This function updates the list of components for which LR job would be run
    def find_components_to_test(self):
        if self.is_ambari_upgrade:
            ambariprops_file = open(self.AMBARI_OLD_PROP_FILE)
        else:
            ambariprops_file = open(self.AMBARI_PROP_FILE)
        lines = [line.strip() for line in ambariprops_file]
        ambariprops_file.close()
        pattern = re.compile('INSTALL_(\S+)\s*=\s*true', re.IGNORECASE)
        for prop in lines:
            matcher = pattern.match(prop)
            if (matcher != None):
                self.addJobForComponent(matcher.group(1).lower().strip())

        self.fixJobDependecy()
        self.remove_hive_comp()
        logger.info("COMPONENTS_TO_TEST")
        for component in self.COMPONENTS_TO_TEST:
            logger.info(component)

    # This function adds dependent components wherever required for running LR jobs
    def fixJobDependecy(self):
        if 'hbase' not in self.COMPONENTS_TO_TEST:
            if 'slider' in self.COMPONENTS_TO_TEST:  #slider LR job depends on hbase
                self.COMPONENTS_TO_TEST.remove('slider')
            if 'phoenix' in self.COMPONENTS_TO_TEST:  #phoenix LR job depends on hbase
                self.COMPONENTS_TO_TEST.remove('phoenix')
            if 'storm' in self.COMPONENTS_TO_TEST:  #storm LR job depends on hbase
                self.COMPONENTS_TO_TEST.remove('storm')
        pass

    # This function updates COMPONENTS_TO_TEST list for LR jobs
    def addComponentToTest(self, component):
        if component not in self.COMPONENTS_TO_TEST:
            self.COMPONENTS_TO_TEST.append(component)

    # This function adds respective component name to COMPONENTS_TO_TEST list for LR jobs
    def addJobForComponent(self, component):
        if component == "hdfs":
            self.addComponentToTest("hdfs")
            self.addComponentToTest("zookeeper")

        if component == "mr":
            self.addComponentToTest("yarn")

        if component == "hbase":
            self.addComponentToTest("hbase")

        if component == "oozie":
            self.addComponentToTest("oozie")

        if component == "pig":
            self.addComponentToTest("pig")

        if component == "flume":
            self.addComponentToTest("flume")

        if component == "knox":
            self.addComponentToTest("knox")

        if component == "phoenix":
            self.addComponentToTest("phoenix")

        if component == "sqoop":
            self.addComponentToTest("sqoop")

        if component == "falcon":
            self.addComponentToTest("falcon")

        if component == "hive":
            self.addComponentToTest("hive")
            self.addComponentToTest("tez")

        if component == "storm":
            self.addComponentToTest("storm")

        if component == "slider":
            self.addComponentToTest("slider")

        if component == "atlas":
            self.addComponentToTest("atlas")

        if component == "spark":
            self.addComponentToTest("spark")

    # This function removes a component from COMPONENTS_TO_TEST list
    def removeComponentFromTest(self, component):
        if component in self.COMPONENTS_TO_TEST:
            self.COMPONENTS_TO_TEST.remove(component)

    # This function sets up RU cluster for specific components
    def setup_ru_cluster(self):
        if "hbase" in self.COMPONENTS_TO_TEST:
            self.setup_hbase()
        if "phoenix" in self.COMPONENTS_TO_TEST:
            self.setup_phoenix()

    # This function prepares cluster for HBase tests
    def setup_hbase(self):
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.RuAssert import ruAssert
        HBASE_PREUPGRADE_TABLE = "hbase_preUpgrade_table"
        # HBASE_CONF_DIR = HBase.getModifiedConfigPath()
        # propertyMap = {'mapred.job.queue.name':'hbase'}
        # #stopping region servers and master
        # HBase.stopRegionServers(nodes=HBase.getRegionServers(), skip_slider_check=True)
        # HBase.stopHBaseMasterNode(HBase.getMasterNode(), wait=15)
        # HBase.modifyConfig(changes={'hbase-site.xml':propertyMap}, nodeSelection={'services':['all']})
        # host = Machine.getfqdn(HBase.getMasterNode())
        # lhost = Machine.getfqdn()
        # if (host != lhost):
        #     Machine.rm(None, None, HBASE_CONF_DIR, isdir=True)
        #     Machine.copyToLocal(None, host, HBASE_CONF_DIR, HBASE_CONF_DIR)
        # allnodes = HBase.getSelectedNodes({'services':['all']})
        # allnodes.append(Machine.getfqdn())
        # util.copy_back_to_original_config(HBASE_CONF_DIR, Config.get('hbase', 'HBASE_CONF_DIR'), file_list=["hbase-site.xml"], node_list=allnodes)
        # #starting region servers and master
        # HBase.startRegionServers(nodes=HBase.getRegionServers(), skip_slider_check=True)
        # HBase.startHBaseMasterNode(host=HBase.getMasterNode(), masterStartUpWait=True, wait=15)
        # time.sleep(120)
        HBase.dropAndCreateTable(HBASE_PREUPGRADE_TABLE, "cf")
        exit_code, stdout = HBase.runShellCmds(["put '%s','row1', 'cf:f1', 'holmes'" % HBASE_PREUPGRADE_TABLE])
        ruAssert("HBASE", exit_code == 0)
        exit_code, stdout = HBase.runShellCmds(["put '%s','row2', 'cf:f2', 'watson'" % HBASE_PREUPGRADE_TABLE])
        ruAssert("HBASE", exit_code == 0)

    # This function prepares cluster for Phoenix tests
    def setup_phoenix(self):
        from beaver.component.hadoop import Hadoop
        PHOENIX_TEST_TABLE = 'basicTable'
        try:
            logger.info("### Phoenix setup starting ###")
            from beaver.component.hbase import HBase
            from beaver.component.phoenix import Phoenix
            from beaver.component.xa import Xa

            # servers = HBase.getNumOfRegionServers()
            # if servers == -1:
            #     time.sleep(10)
            #     servers = HBase.getNumOfRegionServers()
            #
            # if servers == -1 or servers['running'] <= 0:
            #     #We restart all the regionServers
            #     HBase.startRegionServers(nodes=HBase.getRegionServers())

            if Hadoop.isSecure() and not Xa.isArgus():
                Phoenix.grantPermissionsToSystemTables()
            #We create a table to read through the upgrade process
            primaryKey = {'name': 'ID', 'type': 'BIGINT'}
            columns = [
                {
                    'name': 'FirstName',
                    'type': 'VARCHAR(30)'
                }, {
                    'name': 'SecondName',
                    'type': 'VARCHAR(30)'
                }, {
                    'name': 'City',
                    'type': 'VARCHAR(30)'
                }
            ]
            exit_code, stdout = Phoenix.createTable(PHOENIX_TEST_TABLE, primaryKey, columns)
            env = {}
            env['JAVA_HOME'] = Config.get('machine', 'JAVA_HOME')
            #We insert 10 rows into the table
            for i in range(10):
                Phoenix.runSQLLineCmds(
                    "UPSERT INTO %s VALUES (%s,'Name_%s','Surname_%s','City_%s')" %
                    (PHOENIX_TEST_TABLE, str(i), str(i), str(i), str(i)), env
                )
        except Exception as phoenix_exception:
            logger.info("###   Phoenix setup failed ###")
            logger.info('Caused by: ' + str(phoenix_exception))
            pass

    # This function verifies Long Running jobs after RU is completed
    def validate_lr_job(self):
        #################################################Finsih long running jobs
        ### Need to stop HDFS Falcon,Yarn long runningJobs ####
        # create flagFile to kill HDFS background job
        from beaver.component.hadoop import HDFS, YARN
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        from beaver.component.rollingupgrade.ruCommon import Rollingupgrade

        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')
        createCmd = "dfs -touchz " + UpgradePerNode._HDFS_FLAG_FILE
        exit_code, output = HDFS.runas(TEST_USER, createCmd)
        logger.info("8===========================================================================")
        if self.DO_DOWNGRADE:
            self.removeComponentFromTest("falcon")
        if "falcon" in self.COMPONENTS_TO_TEST:
            from beaver.component.rollingupgrade.ruFalcon import ruFalcon
            ruFalcon.stopFalconLongRunningJob()
        if "yarn" in self.COMPONENTS_TO_TEST:
            from beaver.component.rollingupgrade.ruYarn import ruYARN
            ruYARN.stopYarnLongRunningJob()
        # if "hive" in self.COMPONENTS_TO_TEST:
        #     from beaver.component.rollingupgrade.ruHive import ruHive
        #     ruHive.stopHiveLongRunningJob()
        if "slider" in self.COMPONENTS_TO_TEST:
            from beaver.component.rollingupgrade.ruSlider import ruSlider
            ruSlider.stopSliderLongRunningJob()
        if "storm-slider" in self.COMPONENTS_TO_TEST:
            from beaver.component.rollingupgrade.ruStorm import ruStorm
            ruStorm.teardown_storm_slider_app()

        logger.info("9===========================================================================")
        ## TODO - wait for long running jobs to finish

        isZero = YARN.waitForZeroRunningApps()
        logger.info("10===========================================================================")
        if isZero:
            UpgradePerNode.reportProgress("#### None apps are running. ####")
            UpgradeLogger.reportProgress("#### None apps are running. ####", True)
        else:
            UpgradePerNode.reportProgress("#### Check Failed. some apps are running. ####")
            UpgradeLogger.reportProgress("#### Check Failed. some apps are running. ####", False)
        #assert isZero, "all long running jobs are not finished"

        ### List down Failed/Killed applications ####
        Failed_Killed_apps = YARN.getFailedKilledAppList()
        UpgradePerNode.reportProgress("### Listing Killed/Failed applications while performing upgrade ####")
        UpgradeLogger.reportProgress("### Listing Killed/Failed applications while performing upgrade ####", False)

        for app in Failed_Killed_apps:
            queue = YARN.getQueueForApp(app)
            logger.info(" %s running on %s queue Failed/Killed." % (app, queue))
            UpgradePerNode.reportProgress("#### %s running on %s queue Failed/Killed. ####" % (app, queue))
            UpgradeLogger.reportProgress("#### %s running on %s queue Failed/Killed. ####" % (app, queue), False)

        ## TODO - Validate long running jobs
        Rollingupgrade.verifyLongRunningJob(self.COMPONENTS_TO_TEST)

        ## KILL APPLICATIONS ####
        YARN.killAllApplications(useYarnUser=True)

        #logger.info("Running smoke tests after upgrade")
        #Rollingupgrade.run_smokeTests(COMPONENTS_TO_TEST)

        ## TODO - call Teardown for long running jobs
        Rollingupgrade.background_job_teardown(self.COMPONENTS_TO_TEST, None)
        UpgradePerNode.reportProgress("###  Completed upgrade ")
        UpgradeLogger.reportProgress("###  Completed upgrade ", True)

    # This function performs the actual Rolling Upgrade/Downgrade
    def perform_rolling_upgrade(self):
        ru_upgrade = 'ambariruambupg'
        ru_upgrade_downgrade = 'ambariruambupgdowng'
        pu_upgrade = 'ambarirupuambupg'
        pu_upgrade_downgrade = 'ambarirupuambupgdowng'

        if ru_upgrade_downgrade in Config.get('ambari', 'COMPONENT', ru_upgrade):
            self.DO_DOWNGRADE = True
            test_class_name = 'TestQuickRollingUpgradeApi'
            exit_code, stdout = self.Maven2runas(
                ' -Dtest=%s -Dfinalize=false test' % test_class_name,
                cwd=self.LOCAL_WORK_DIR,
                env=self.env,
                user='root'
            )
            logger.info(
                "8===========================================================================After upgrade, before downgrade"
            )
            self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

            test_class_name = 'TestQuickRollingDowngrade'
            exit_code, stdout = self.Maven2runas(
                ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
            )
            logger.info("9===========================================================================After downgrade")
            self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        elif ru_upgrade in Config.get('ambari', 'COMPONENT', ru_upgrade):
            test_class_name = 'TestQuickRollingUpgradeApi'
            exit_code, stdout = self.Maven2runas(
                ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
            )
            logger.info("8===========================================================================After upgrade")
            self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        elif pu_upgrade_downgrade in Config.get('ambari', 'COMPONENT', ru_upgrade):
            self.DO_DOWNGRADE = True
            test_class_name = 'TestPatchDowngrade'
            exit_code, stdout = self.Maven2runas(
                ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
            )
            logger.info("8==================================================================== After Patch Downgrade")
            self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        elif pu_upgrade in Config.get('ambari', 'COMPONENT', ru_upgrade):
            test_class_name = 'TestPatchUpgrade'
            exit_code, stdout = self.Maven2runas(
                ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
            )
            logger.info("8======================================================================= After Patch Upgrade")
            self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        return exit_code

    # This function executes pre-upgrade steps, in this case starts LR jobs
    def perform_pre_upgrade_tests(self):
        UpgradeLogger.reportProgress("=====Starting LR jobs=========", True)
        self.prepare_and_start_long_running_jobs()

    # This function executes Ambari upgrade
    def start_ambari_upgrade(self):
        test_class_name = 'E2E_RU_AmbariSuite'
        exit_code, stdout = self.Maven2runas(
            ' -Dtest=%s test' % test_class_name, cwd=self.LOCAL_WORK_DIR, env=self.env, user='root'
        )
        self.verify_test_result(exit_code, test_class_name, self.LOCAL_WORK_DIR)

        return exit_code

    # This function executes post-upgrade steps, in this verifies LR jobs
    def perform_post_upgrade_steps(self):
        self.validate_lr_job()

    # This function collects results of Long running job for reporting purposes
    def gather_lr_job_results(self):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        progress_file = UpgradePerNode.get_progress_local_file()
        util.dumpText(progress_file, "HEADER progress_file", "FOOTER progress_file")
        self.TEST_RESULT.update(util.parseUpgradeProgressFile(progress_file))
        logger.info("TEST_RESULT: " + str(self.TEST_RESULT))
        #self.TESTCASES.extend(sorted(self.TEST_RESULT.keys()))

        for key in sorted(self.TEST_RESULT.keys()):
            if key not in self.TESTCASES:
                self.TESTCASES.append(key)

        logger.info("TESTCASES: " + str(self.TESTCASES))
        for testcase in self.TESTCASES:
            logger.info(testcase)

        for key in self.TEST_RESULT.keys():
            logger.info("TC: %s %s" % (key, self.TEST_RESULT.get(key)))

        for key, value in self.TEST_RESULT.items():
            fail_message = value['failure']
            print "key : %s " % key
            print "value : %s " % value
            print "fail_mesg : %s " % fail_message
            if 'result' in value:
                value = re.match(r'.*result(.*)', str(value)).group(1)
                if '\'' in value:
                    value = value[3:11].replace('\'', "")
                self.TEST_RESULT[str(key)] = {"status": str(value), "failure": fail_message}

        print "Final value of TEST_RESULT"
        print self.TEST_RESULT

    def remove_hive_comp(self):
        if 'hive' not in self.COMPONENTS_TO_TEST:
            logger.info("Removing hive from COMPONENTS_TO_TEST as hive will be stopped during RU")
            self.COMPONENTS_TO_TEST.remove('hive')
        pass
