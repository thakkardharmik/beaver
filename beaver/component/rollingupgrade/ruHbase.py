#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, random, time, socket, logging, platform, urllib2, collections, datetime, json
import urllib, sys
from beaver import configUtils, util
from beaver.component import HadoopJobHelper
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.config import Config
from beaver.machine import Machine
from distutils.version import StrictVersion

logger = logging.getLogger(__name__)
DATA_DIRS = util.getPropertyValueFromConfigXMLFile(
    os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hdfs-site.xml"), "dfs.datanode.data.dir"
)
DATA_DIRS = DATA_DIRS.split(",")
UNKNOWN_HEAP_SIZE = 12345
NUM_ROWS = 0
LOAD_FACTOR = 13.9821029083
HBASE_LONGRUN_TABLE = 'loadtest_d1'
HBASE_SMOKE_TABLE = 'hbase_smoke'
CREATION_DATE = time.strftime("%Y%m%d")
HBASE_ROLLBACK_TABLE = 'hbase_rollback'
HBASE_PREUPGRADE_TABLE = "hbase_preUpgrade_table"
PREUPGRADE_TABLE_SNAPSHOT = "preUpgrade_table_snapshot"
HBASE_USER = Config.get('hbase', 'HBASE_USER')
signal_dir_path = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'hbaseLongRunner')
if not os.path.exists(signal_dir_path):
    os.makedirs(signal_dir_path)
SHELL_USER = None
if Hadoop.isSecure():
    SHELL_USER = HBASE_USER


class ruHbase:
    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        # Drop table if exists from before
        from beaver.component.hbase import HBase
        if runSmokeTestSetup:
            cls.smoke_test_setup()
        HBase.dropTable(HBASE_LONGRUN_TABLE, logoutput=True)

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        from beaver.component.hbase import HBase
        exit_code, stdout = HBase.runShellCmds(["disable '%s'" % HBASE_SMOKE_TABLE])
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][SmokeSetup] Hbase smoke test setup: disable %s passed" % HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][SmokeSetup] Hbase smoke test setup: disable %s failed" % HBASE_SMOKE_TABLE
            )

        exit_code, stdout = HBase.runShellCmds(["drop '%s'" % HBASE_SMOKE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][SmokeSetup] Hbase smoke test setup: drop %s passed" % HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][SmokeSetup] Hbase smoke test setup: drop %s failed" % HBASE_SMOKE_TABLE
            )

        exit_code, stdout = HBase.runShellCmds(["disable '%s'" % HBASE_ROLLBACK_TABLE])

        exit_code, stdout = HBase.runShellCmds(["drop '%s'" % HBASE_ROLLBACK_TABLE])

        HBase.dropAndCreateTable(HBASE_PREUPGRADE_TABLE, "cf")
        exit_code, stdout = HBase.runShellCmds(["put '%s','row1', 'cf:f1', 'holmes'" % HBASE_PREUPGRADE_TABLE])
        exit_code, stdout = HBase.runShellCmds(["put '%s','row2', 'cf:f2', 'watson'" % HBASE_PREUPGRADE_TABLE])
        #create snapshot of pre-upgrade table
        exit_code, stdout = HBase.runShellCmds(
            ["snapshot '%s', '%s'" % (HBASE_PREUPGRADE_TABLE, PREUPGRADE_TABLE_SNAPSHOT)], user=HBASE_USER
        )

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running HBase Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.hbase import HBase
        if runSmokeTestSetup:
            cls.smoke_test_setup()

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        scriptPath = os.path.join(os.getcwd(), "tests", "rolling_upgrade", "hbase", "hbaseLongRunner.py")
        UpgradePerNode.reportProgress("[INFO][HBASE][BGJob] Starting Long running job for HBase" + scriptPath)
        env = {}
        env['JAVA_HOME'] = Config.get('machine', 'JAVA_HOME')
        suffix = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
        outDir = os.path.join(signal_dir_path, "hbase" + suffix)
        stdout = open(os.devnull, 'w')
        stderr = open(os.devnull, 'w')
        logger.info("outDir: %s, scriptPath: %s" % (outDir, scriptPath))
        Machine.runinbackground(
            "python " + os.path.join(Config.get('machine', 'PYTEST_DIR'), 'pytest.py') + " --output-dir " + outDir +
            " -s " + scriptPath,
            env=env,
            stdout=stdout,
            stderr=stderr
        )
        time.sleep(5)
        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for HBase
        :param smoketestnumber: Used for unique output log location
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HBASE][Smoke] Starting smoke test for HBase")
        exit_code, stdout = HBase.runShellCmds(["scan '%s'" % HBASE_PREUPGRADE_TABLE])
        if re.search('holmes', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. holmes found in stdout"
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. holmes not found in stdout "
            )
        if re.search('watson', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. watson found in stdout"
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. watson not found in stdout "
            )
        exit_code, stdout = HBase.runShellCmds(["create '%s', 'cf'" % HBASE_SMOKE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. create %s passed " % HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. create %s failed  " % HBASE_SMOKE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["put '%s','row1', 'cf:f1', 'Schweinsteiger'" % HBASE_SMOKE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. put %s row1 cf:f1 passed " %
                HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. put %s row1 cf:f1 failed " %
                HBASE_SMOKE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["put '%s','row2', 'cf:f2', 'Lahm'" % HBASE_SMOKE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. put %s row2 cf:f2 passed" %
                HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. put %s row2 cf:f2 failed " %
                HBASE_SMOKE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["put '%s','row3', 'cf:f3', 'Klose'" % HBASE_SMOKE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. put %s row3 cf:f3 passed  " %
                HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. put %s row3 cf:f3 failed  " %
                HBASE_SMOKE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["scan '%s'" % HBASE_SMOKE_TABLE])
        if re.search('Schweinsteiger', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. Schweinsteiger found in stdout"
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. Schweinsteiger not found in stdout "
            )
        if re.search('Lahm', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. Lahm found in stdout "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. Lahm not found in stdout "
            )
        if re.search('Klose', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. Klose found in stdout "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. Klose not found in stdout "
            )
        exit_code, stdout = HBase.dropTable(HBASE_SMOKE_TABLE, logoutput=True)
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. drop table %s passed " %
                HBASE_SMOKE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. drop table %s failed  " %
                HBASE_SMOKE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["deleteall '%s','row1'" % HBASE_PREUPGRADE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. deletall %s row1 passed " %
                HBASE_PREUPGRADE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. deletall %s row1 failed  " %
                HBASE_PREUPGRADE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["scan '%s'" % HBASE_PREUPGRADE_TABLE])
        if re.search('holmes', stdout) is None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. holmes not found in stdout"
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. holmes found in stdout "
            )
        if re.search('watson', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. watson found in stdout "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. watson not found in stdout "
            )
        exit_code, stdout = HBase.runShellCmds(["disable '%s'" % HBASE_PREUPGRADE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. disable %s passed" %
                HBASE_PREUPGRADE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. disable %s failed" %
                HBASE_PREUPGRADE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["restore_snapshot '%s'" % PREUPGRADE_TABLE_SNAPSHOT], user=HBASE_USER)
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. restore_snapshot %s passed " %
                PREUPGRADE_TABLE_SNAPSHOT
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. restore_snapshot %s failed  " %
                PREUPGRADE_TABLE_SNAPSHOT
            )
        exit_code, stdout = HBase.runShellCmds(["enable '%s'" % HBASE_PREUPGRADE_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. enable %s passed" %
                HBASE_PREUPGRADE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. enable %s failed" %
                HBASE_PREUPGRADE_TABLE
            )
        exit_code, stdout = HBase.runShellCmds(["scan '%s'" % HBASE_PREUPGRADE_TABLE])
        if re.search('holmes', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. holmes found in stdout"
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. holmes not found in stdout "
            )
        if re.search('watson', stdout) is not None:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. watson found in stdout "
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. watson not found in stdout "
            )
        exit_code, stdout = HBase.runHBCKas(HBASE_USER, "hbck")
        if exit_code == 0:
            UpgradePerNode.reportProgress("[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. hbck passed")
        else:
            UpgradePerNode.reportProgress("[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. hbck failed")
        try:
            if StrictVersion('.'.join(str(HBase.getVersion()).split('.')[3:6])) < StrictVersion('2.3.4'):
                exit_code, stdout = HBase.runas(
                    HBASE_USER,
                    "org.apache.hadoop.hbase.mapreduce.RowCounter -Dhbase.rpc.controllerfactory.class=org.apache.hadoop.hbase.ipc.RpcControllerFactory %s"
                    % (HBASE_PREUPGRADE_TABLE)
                )
            else:
                exit_code, stdout = HBase.runas(
                    HBASE_USER, "org.apache.hadoop.hbase.mapreduce.RowCounter %s" % (HBASE_PREUPGRADE_TABLE)
                )
        except:
            exit_code, stdout = HBase.runas(
                HBASE_USER,
                "org.apache.hadoop.hbase.mapreduce.RowCounter -Dhbase.rpc.controllerfactory.class=org.apache.hadoop.hbase.ipc.RpcControllerFactory %s"
                % (HBASE_PREUPGRADE_TABLE)
            )
        if exit_code == 0:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][Smoke] Smoke test validation for hbase passed. RowCounter %s passed" %
                HBASE_PREUPGRADE_TABLE
            )
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][Smoke] Smoke test validation for hbase failed. RowCounter %s failed" %
                HBASE_PREUPGRADE_TABLE
            )
        UpgradePerNode.reportProgress("[INFO][HBASE][Smoke] Smoke test for HBase component finished ")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running HBase job
        '''
        from beaver.component.hbase import HBase
        HBase.dropTable(HBASE_LONGRUN_TABLE, logoutput=True)

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.hbase import HBase
        count1 = 0
        count2 = 0
        countRowPattern = '(\d+ row\(s\) in)'
        exit_code, stdout = HBase.runShellCmds(["count '%s', CACHE => 100000" % HBASE_LONGRUN_TABLE])
        if stdout is not None:
            try:
                count1 = re.search(countRowPattern, stdout).group(0).split(' row(s) in')[0]
            except StandardError:
                logger.error("Could not parse count from stdout")
        time.sleep(65)
        exit_code, stdout = HBase.runShellCmds(["count '%s', CACHE => 100000" % HBASE_LONGRUN_TABLE])
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if stdout is not None:
            try:
                count2 = re.search(countRowPattern, stdout).group(0).split(' row(s) in')[0]
            except StandardError:
                logger.error("Could not parse count from stdout")
        # QE-5559, convert string to int for comparison
        logger.info("count2 = %s, count1 = %s" % (count2, count1))
        count2, count1 = int(count2), int(count1)
        if count2 > count1:
            UpgradePerNode.reportProgress(
                "[PASSED][HBASE][BGJobCheck] Long running job for HBase running as expected "
            )
            UpgradePerNode.reportProgress("[PASSED][HBASE][BGJobCheck] Killing long running job now, since verified ")
            if Machine.isWindows():
                HBase.killProcessWithFilterIfExists(Machine.getfqdn(), "python", "hbaseLongRunner.py")
            else:
                HBase.killProcessWithFilterIfExists(Machine.getfqdn(), "python", "hbaseLongRunner.py")
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][HBASE][BGJobCheck] Long running job for HBase terminated earlier than expected " +
                str(count1) + " " + str(count2)
            )
        cls.run_smoke_test(2)

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.hbase import HBase
        global NUM_ROWS
        if NUM_ROWS == 0:
            return
        HBase.stopHBaseMasterNode()
        node = HBase.getMasterNode()
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #HBase.resetRestService("stop")
        #UpgradePerNode.reportProgress("[INFO][HBASE][Upgrade] Stopping REST service")
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hbase-master", version, node)
        HBase.startHBaseMasterNode()
        UpgradePerNode.reportProgress("[INFO][HBASE][Upgrade] MasterNode Upgrade Finished ")
        #REST_CONF_DIR, REST_SERVER_URL = HBase.setupRestService(master=node)
        #UpgradePerNode.reportProgress("[INFO][HBASE][Upgrade] REST Upgrade Finished ")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Starting region server upgrade on %s " % node)
        global NUM_ROWS
        if NUM_ROWS == 0:
            return
        HBase.stopRegionServers(nodes=[node])
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hbase-regionserver", version, node)
        HBase.startRegionServers(nodes=[node])
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Regionserver Upgrade on %s Finished " % node)

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.hbase import HBase
        global NUM_ROWS
        if NUM_ROWS == 0:
            return
        HBase.stopHBaseMasterNode()
        node = HBase.getMasterNode()
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #HBase.resetRestService("stop")
        #UpgradePerNode.reportProgress("[INFO][HBASE][Downgrade] Stopping REST service")
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hbase-master", version, node)
        HBase.startHBaseMasterNode()
        UpgradePerNode.reportProgress(" MasterNode Downgrade Finished")
        #REST_CONF_DIR, REST_SERVER_URL = HBase.setupRestService(master=node)
        #UpgradePerNode.reportProgress("[INFO][HBASE][Downgrade] REST Downgrade Finished ")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Starting region server downgrade on %s" % node)
        global NUM_ROWS
        if NUM_ROWS == 0:
            return
        HBase.stopRegionServers(nodes=[node])
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hbase-regionserver", version, node)
        HBase.startRegionServers(nodes=[node])
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Regionserver downgrade on %s Finished " % node)

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
        logger.info("TODO : need to implement this function")

    @classmethod
    def createState4Rollback2(cls):
        '''
        Modify state before rollback
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        exit_code, stdout = HBase.runShellCmds(["put '%s','row1', 'cf:c1', 'value2'" % HBASE_ROLLBACK_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress("[PASSED][HBASE][Prepare] Updating HBase for rollback passed ")
        else:
            UpgradePerNode.reportProgress("[FAILED][HBASE][Prepare] Updating HBase for rollback failed")

    @classmethod
    def ru_rollback_state(cls):
        '''
        Saved state is rolled back - upgrade is abandonded
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        exit_code, stdout = HBase.snapshotRestore(CREATION_DATE, user=SHELL_USER)

    @classmethod
    def checkState4Rollback(cls):
        '''
        Saved state has been rolled back - verify
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        exit_code, stdout = HBase.runShellCmds(["get '%s','row1', {COLUMN => 'cf'}" % HBASE_ROLLBACK_TABLE])
        # verify that the data created before the snapshot still exists
        if re.search('value=value1', stdout) is not None:
            UpgradePerNode.reportProgress("[PASSED][HBASE][Prepare] HBase rollback verification passed ")
        else:
            UpgradePerNode.reportProgress("[FAILED][HBASE][Prepare] HBase rollback verification failed")

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
        Prepare HBase to save State for Upgrade
        '''
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("Preparing HBase to save State for Upgrade")
        exit_code, stdout = HBase.runShellCmds(["create '%s', 'cf'" % HBASE_ROLLBACK_TABLE])
        exit_code, stdout = HBase.runShellCmds(["put '%s','row1', 'cf:c1', 'value1'" % HBASE_ROLLBACK_TABLE])
        if exit_code == 0:
            UpgradePerNode.reportProgress("[PASSED][HBASE][Prepare] Preparing HBase for rollback passed ")
        else:
            UpgradePerNode.reportProgress("[FAILED][HBASE][Prepare] Preparing HBase for rollback failed")
        exit_code, output = HBase.snapshotAll(user=SHELL_USER)
        if exit_code == 0:
            UpgradePerNode.reportProgress("[PASSED][HBASE][Prepare] Preparing HBase to save State for Upgrade passed ")
        else:
            UpgradePerNode.reportProgress("[FAILED][HBASE][Prepare] Preparing HBase to save State for Upgrade failed")
