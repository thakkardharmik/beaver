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
import urllib, sys, random
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils

logger = logging.getLogger(__name__)


class ruHive:
    _metastore_backup_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "metastore_backup.sql")
    _yarn_queue = "hive"
    _hdfs_smoketest_dir = "/tmp/hivetest"
    _smoketest_tbl = "smtesttbl"
    _hdfs_bgjtest_dir = "/tmp/hivelrtest"
    _bgjtest_tbl = "lrtesttbl"
    _hdfs_user = Config.get("hadoop", 'HDFS_USER')
    _num_of_webhcat_bgj = 1
    _num_of_rows_lr = 10000
    _num_of_rows_smoke = 100
    _max_bgjtest_duration = 72000000
    _hs2_live_ports = []
    _shortbgj_hive_process = None
    _shortbgj_bline_process = None

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=False, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Long running test setup for Hive started")
        cls.doBackgroundJobSetup(cls._hdfs_bgjtest_dir)
        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Long running test setup for Hive finished")
        if runSmokeTestSetup:
            logger.info("**** Running Hive Smoke Test Setup ****")
            cls.smoke_test_setup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][Smoke] Smoke test setup for Hive started")
        cls.doSetup(cls._hdfs_smoketest_dir, cls._smoketest_tbl, cls._num_of_rows_smoke, "smoke")
        UpgradePerNode.reportProgress("[INFO][Hive][Smoke] Smoke test setup for Hive finished")

    @classmethod
    def doBackgroundJobSetup(cls, hdfs_test_dir):

        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        logger.info("Preparing the test setup for Hive background job")
        udfjar = os.path.join(Config.getEnv('WORKSPACE'), "tests", "hive", "hive-udf", "hive-udfs-0.1.jar")
        HDFS.createDirectory(hdfs_test_dir, user=cls._hdfs_user, perm='777', force=True)
        HDFS.copyFromLocal(udfjar, hdfs_test_dir)
        query = "drop function sleep; create function sleep as 'org.apache.hive.udf.generic.GenericUDFSleep' using jar 'hdfs://%s/hive-udfs-0.1.jar';" % hdfs_test_dir
        exit_code, stdout = Hive.runQuery(query)
        if exit_code != 0:
            UpgradePerNode.reportProgress("[FAILED][Hive][Setup] Long running failed due to exitcode = %d" % exit_code)
        else:
            UpgradePerNode.reportProgress("[PASSED][Hive][Setup] Long running finished successfully")

    @classmethod
    def doSetup(cls, hdfs_test_dir, tbl_name, num_of_rows, type):

        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        logger.info("Generating test table dataset with %d rows" % num_of_rows)
        test_data_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), tbl_name + ".dat")
        f = open(test_data_file, 'w')
        userid = 100000
        for i in xrange(num_of_rows):
            for j in range(random.randint(3, 8)):
                f.write("%d|%d\n" % (userid + i, random.randint(10, 80)))
        f.close()

        hdfs_tbl_dir = hdfs_test_dir + "/" + tbl_name
        logger.info("Copying the test dataset to HDFS directory '%s'" % hdfs_tbl_dir)
        HDFS.createDirectory(hdfs_test_dir, user=cls._hdfs_user, perm='777', force=True)
        HDFS.createDirectory(hdfs_tbl_dir, perm='777')
        HDFS.copyFromLocal(test_data_file, hdfs_tbl_dir)
        HDFS.chmod(cls._hdfs_user, '777', hdfs_tbl_dir)

        logger.info("Creating table '%s' and verification tables" % tbl_name)
        query = "drop table if exists %s;\n" % tbl_name
        query += "create external table %s (userid string, age int) row format delimited fields terminated by '|' stored as textfile location '%s';\n" % (
            tbl_name, hdfs_tbl_dir
        )
        query += "drop table if exists %s_hive_verify;\n" % tbl_name
        query += "create table %s_hive_verify (userid string, age int);\n" % tbl_name
        if type == "Long running":
            for i in range(cls._num_of_webhcat_bgj):
                query += "drop table if exists %s_wh_%d;\n" % (tbl_name, i + 1)
                query += "create table %s_wh_%d (userid string, age int);\n" % (tbl_name, i + 1)
        hivesetupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "hivesetup.sql")
        util.writeToFile(query, hivesetupfile)
        exit_code, stdout = Hive.run("-f " + hivesetupfile, logoutput=False)
        if type:
            msg = "%s job setup for Hive component" % type
            if exit_code != 0:
                UpgradePerNode.reportProgress(
                    "[FAILED][Hive][Setup] %s failed due to exitcode = %d" % (msg, exit_code)
                )
            else:
                UpgradePerNode.reportProgress("[PASSED][Hive][Setup] %s finished successfully" % msg)

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=False, config=None):
        '''
        Runs background long running Hive Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Long running job for Hive component started")

        setqueue = ""
        if Hive.isTezEnabled():
            setqueue = "set tez.queue.name=%s; " % cls._yarn_queue
        else:
            setqueue = "set mapred.job.queue.name=%s; " % cls._yarn_queue

        logger.info("**** Running Hive CLI Test ****")
        query = setqueue + " create table if not exists hive_cli_lr (a string); select sleep(%d, 2000, 'hdfs://%s/hive_cli_lr', 'hdfs://%s/END') from (select count(*) from hive_cli_lr) a;" % (
            cls._max_bgjtest_duration, cls._hdfs_bgjtest_dir, cls._hdfs_bgjtest_dir
        )
        Hive.runQuery(query, background=True)

        logger.info("**** Running Beeline CLI Test ****")
        # Create the sleep function within the same Beeline session
        # Function created outside of HS2 instance are not picked
        query = setqueue + "\n"
        query += "drop function sleep2;\n"
        query += "create function sleep2 as 'org.apache.hive.udf.generic.GenericUDFSleep' using jar 'hdfs://%s/hive-udfs-0.1.jar';\n" % cls._hdfs_bgjtest_dir
        query += "create table if not exists bline_cli_lr (a string);\n"
        query += "select sleep2(%d, 2000, 'hdfs://%s/bline_cli_lr', 'hdfs://%s/END') from (select count(*) from bline_cli_lr) a;\n" % (
            cls._max_bgjtest_duration, cls._hdfs_bgjtest_dir, cls._hdfs_bgjtest_dir
        )
        Hive.runQueryOnBeeline(query, readFromFile=True, background=True)

        logger.info("**** Running WebHCat Test ****")
        webhcatHost = Config.get('templeton', 'TEMPLETON_HOST', default=Machine.getfqdn())
        webhcatPort = Config.get('templeton', 'TEMPLETON_PORT', default="50111")
        url = "http://%s:%s/templeton/v1/hive" % (webhcatHost, webhcatPort)
        query = setqueue + " set mapred.task.timeout=0; create table if not exists whcat_rest_lr (a string); select sleep(%d, 2000, 'hdfs://%s/whcat_rest_lr', 'hdfs://%s/END') from (select count(*) from whcat_rest_lr) a;" % (
            cls._max_bgjtest_duration, cls._hdfs_bgjtest_dir, cls._hdfs_bgjtest_dir
        )
        params = {'execute': query}
        status_code, stdout = util.curl(url, method='POST', params=params)
        retry = 0
        while status_code == 404 and retry < 3:
            time.sleep(15)
            status_code, stdout = util.curl(url, method='POST', params=params)
            retry += 1
        if status_code != 200:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJobSetup] Long running job for WebHCat failed due to status code = %d" % status_code
            )
            logger.error("Webhcat request failed with the following error: %s\n" % stdout)

        if runSmokeTestSetup:
            logger.info("**** Running Hive Smoke Test Setup ****")
            cls.smoke_test_setup()
        return 3

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for hive
        :param smoketestnumber: Used for unique output log location
        '''
        return cls.run_client_smoketest(config=config)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Hive jobs
        '''
        from beaver.component.hive import Hive

        logger.info("Make sure to switch the HiveServer2 to use the default port")
        adminUser = Machine.getAdminUser()
        hiveHost = Hive.getHiveHost()
        for port in cls._hs2_live_ports:
            pid = Machine.getPIDByPort(port, host=hiveHost, user=adminUser)
            if pid:
                Machine.killProcessRemote(pid, host=hiveHost, user=adminUser)
                time.sleep(2)
        if len(cls._hs2_live_ports) > 0:
            Hive.startService(services=["hiveserver2"])

    @classmethod
    def stopHiveLongRunningJob(cls):
        '''
        Stop the long running background queries
        '''
        logger.info("Push the file to HDFS to signal END of queries")
        HDFS.touchz(cls._hdfs_bgjtest_dir + "/END")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        # Assumption is all the MR jobs for Hive are alive and running

        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        logger.info("Verify the query triggered by Hive CLI is alive post upgrade")
        passed = cls.verifyLongRunningQuery("hive_cli_lr")
        if passed:
            UpgradePerNode.reportProgress("[PASSED][Hive][BGJob] Long running test for Hive Metastore passed")
        else:
            UpgradePerNode.reportProgress("[FAILED][Hive][BGJob] Long running test for Hive Metastore failed")

        logger.info("Verify the query triggered by Beeline CLI is alive post upgrade")
        passed = cls.verifyLongRunningQuery("bline_cli_lr")
        if passed:
            UpgradePerNode.reportProgress("[PASSED][Hive][BGJob] Long running test for HiveServer2 passed")
        else:
            UpgradePerNode.reportProgress("[FAILED][Hive][BGJob] Long running test for HiveServer2 failed")

        logger.info("Verify the query triggered by WebHCat is alive post upgrade")
        passed = cls.verifyLongRunningQuery("whcat_rest_lr")
        if passed:
            UpgradePerNode.reportProgress("[PASSED][Hive][BGJob] Long running test for WebHCat passed")
        else:
            UpgradePerNode.reportProgress("[FAILED][Hive][BGJob] Long running test for WebHCat failed")

    @classmethod
    def verifyLongRunningQuery(cls, file_to_verify):
        lfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), file_to_verify)
        exit_code, stdout = HDFS.copyToLocal(cls._hdfs_bgjtest_dir + "/" + file_to_verify, lfile)
        if exit_code != 0:
            logger.info("Error fetching the timestamp file from HDFS")
            return False
        lines = open(lfile, 'r').readlines()
        if len(lines) == 0:
            logger.info("Empty timestamp file")
            return False
        try:
            ts = int(lines[-1])
            # Shutdown gracefully
            if ts == -1:
                return True
            # Timestamp should be less than 5 minutes, which indicates
            # UDF wrote something atleast once in the last 5 minutes
            timegap = time.time() - (ts / 1000)
            if timegap > 300:
                logger.info("Time gap is %d seconds, last line in the timestamp file was '%d'" % (timegap, ts))
                return False
        except ValueError:
            logger.info("Error parsing last line in the timestamp file => '" + lines[-1] + "'")
            return False
        return True

    @classmethod
    def background_job_when_master_upgrade(cls):
        '''
        Start a background application which runs while component master service gets upgraded
        :return:
        '''
        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Background Job test setup when upgrading Hive started")

        logger.info("Creating hive tables for short background jobs")
        query = "drop table if exists shortlr_hive_verify;\n"
        query += "create table shortlr_hive_verify (userid string, age int);\n"
        query += "drop table if exists shortlr_bline_verify;\n"
        query += "create table shortlr_bline_verify (userid string, age int);\n"
        query += "drop table if exists shortlr_bline_verify;\n"
        query += "create table shortlr_bline_verify (userid string, age int);\n"
        short_bgjob_setupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'shortlrsetup.sql')
        util.writeToFile(query, short_bgjob_setupfile)

        exit_code, stdout = Hive.run("-f " + short_bgjob_setupfile)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJob] Background Job test setup when Hive upgrades failed due to exitcode = %d" %
                exit_code
            )

        logger.info("Running the Background Job when upgrading Hive")
        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Long running job for Hive component upgrades started")

        setqueue = ""
        if Hive.isTezEnabled():
            setqueue = "set tez.queue.name=%s; " % cls._yarn_queue
        else:
            setqueue = "set mapred.job.queue.name=%s; " % cls._yarn_queue

        logger.info("**** Running Hive CLI Test ****")
        query = setqueue + " insert overwrite table shortlr_hive_verify select userid, avg(age) from %s group by userid order by userid;" % cls._bgjtest_tbl
        cls._shortbgj_hive_process = Hive.runQuery(query, background=True)

        # Sleeping for 10 seconds to make sure that query initializes before Metastore is restarted
        time.sleep(10)

        logger.info("**** Running Beeline CLI Test ****")
        query = setqueue + "\ninsert overwrite table shortlr_bline_verify select userid, avg(age) from %s group by userid order by userid;" % cls._bgjtest_tbl
        cls._shortbgj_bline_process = Hive.runQueryOnBeeline(query, readFromFile=True, background=True)

        UpgradePerNode.reportProgress("[INFO][Hive][BGJob] Background Job test setup when Hive upgrades finished")

    @classmethod
    def background_job_teardown_when_master_upgrade(cls):
        '''
        Clean up for background job which started before upgrading master services
        :return:
        '''
        cls._shortbgj_hive_process = None
        cls._shortbgj_bline_process = None

    @classmethod
    def verify_background_job_when_master_upgrade(cls):
        '''
        Validate background job Succeeded when master got upgraded
        :return:
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        logger.info("Verify the successful completion of Hive CLI job")
        starttime = time.time()
        while (starttime - time.time() < 300) and cls._shortbgj_hive_process.poll() is None:
            time.sleep(5)
        procpoll = cls._shortbgj_hive_process.poll()
        if procpoll is None:
            logger.info("Killing Hive CLI process '%d'" % cls._shortbgj_hive_process.pid)
            Machine.killProcess(cls._shortbgj_hive_process.pid)
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJob] Background job during master upgrade failed as Hive CLI failed to finish"
            )
        elif procpoll != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJob] Background job during master upgrade failed as Hive CLI exited with '%d'" %
                procpoll
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][Hive][BGJob] Background job for Hive CLI during master upgrade finished successfully"
            )

        logger.info("Verify the successful completion of Beeline CLI job")
        starttime = time.time()
        while (starttime - time.time() < 120) and cls._shortbgj_bline_process.poll() is None:
            time.sleep(5)
        procpoll = cls._shortbgj_bline_process.poll()
        if procpoll is None:
            logger.info("Killing Beeline CLI process '%d'" % cls._shortbgj_bline_process.pid)
            Machine.killProcess(cls._shortbgj_bline_process.pid)
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJob] Background job during master upgrade failed as Beeline CLI failed to finish"
            )
        elif procpoll != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][BGJob] Background job during master upgrade failed as Beeline CLI exited with '%d'" %
                procpoll
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][Hive][BGJob] Background job for Beeline CLI during master upgrade finished successfully"
            )

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][Upgrade] Hive master components upgrade started")

        cls.background_job_when_master_upgrade()
        cls.switch_master_version('upgrade', version, config=config)
        cls.verify_background_job_when_master_upgrade()
        cls.background_job_teardown_when_master_upgrade()

        UpgradePerNode.reportProgress("[INFO][Hive][Upgrade] Hive master components upgrade finished")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("Hive does not have any slaves, so no slaves to upgrade")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Hive Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][Downgrade] Hive master components downgrade started")

        cls.background_job_when_master_upgrade()
        cls.switch_master_version('downgrade', version, config=config)
        cls.verify_background_job_when_master_upgrade()
        cls.background_job_teardown_when_master_upgrade()

        UpgradePerNode.reportProgress("[INFO][Hive][Downgrade] Hive master components downgrade finished")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade Hive slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("Hive does not have any slaves, so no slaves to downgrade")

    @classmethod
    def switch_master_version(cls, action, version, config=None):
        '''
        Switches Hive master services' version
        :param action: Whether to "upgrade" or "downgrade"
        :param version: Version to be switched to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.hive import Hive

        currentHiveVersion = Hive.getVersion()

        if action == 'upgrade':
            # Backup the database used by the Hive Metastore
            logger.info("Performing backup of the Hive Metastore DB before starting the upgrade")
            Hive.backupMetastoreDB(cls._metastore_backup_file)

        node = Hive.getHiveHost()

        # Stop the old Hive Metastore
        logger.info("Stopping the Hive Metastore")
        Hive.stopService(services=["metastore"])

        # Upgrade Hive Metastore servers to new version
        hdpSelect.changeVersion("hive-metastore", version, node)

        if action == 'upgrade':
            logger.info("Upgrading the Hive metastore schema")
            Hive.upgradeSchema()

        # Restart Hive Metastore servers one at a time
        logger.info("Restarting the Hive Metastore")
        Hive.startService(services=["metastore"])

        # Start new Hive Server 2 instance
        confHS2Port = Hive.getHiveserver2ThriftPort()
        hs2port = util.getNextAvailablePort(node, confHS2Port)

        hdpSelect.changeVersion("hive-server2", version, node)

        Hive.modifyConfig(config, services=['hiveserver2'], restartService=False)
        logger.info("Starting a new HiveServer2 at port '%d' for assisting rolling-upgrade" % hs2port)
        if hs2port != confHS2Port:
            changes = {'hive-site.xml': {'hive.server2.thrift.port': hs2port}}
            Hive.modifyConfig(changes, services=["hiveserver2"], restartService=False)
        Hive.startService(services=["hiveserver2"])
        cls._hs2_live_ports = [Hive.getHiveserver2ThriftPort(), hs2port]

        # Deregister the old Hive Server 2 instances
        logger.info("Deregistering the HiveServer2 on version '%s'" % currentHiveVersion)
        Hive.deregisterHiveServer2(version=currentHiveVersion)

        from beaver.component.hcatalog import Hcatalog

        # Stop the old WebHCat server
        logger.info("Stopping the WebHCat server")
        node = Config.get('templeton', 'TEMPLETON_HOST', default=Machine.getfqdn())
        webhcatPort = Config.get('templeton', 'TEMPLETON_PORT', default="50111")
        # Stop the old WebHCat server
        logger.info("Stop the WebHCat server")
        Hcatalog.stop(node)

        # Upgrade WebHCat to the new version
        hdpSelect.changeVersion("hive-webhcat", version, node)

        # Start the WebHCat server
        logger.info("Restarting the WebHCat server")
        newConfDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'localWebhcatConf')
        if os.path.exists(newConfDir):
            Hcatalog.start(node, hcat_confdir=newConfDir)
        else:
            Hcatalog.start(node)

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.hive import Hive
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Hive][Smoke] Smoke test for Hive component started")

        setqueue = ""
        if Hive.isTezEnabled():
            setqueue = "set tez.queue.name=%s; " % cls._yarn_queue
        else:
            setqueue = "set mapred.job.queue.name=%s; " % cls._yarn_queue

        logger.info("**** Running Hive CLI Test ****")
        query = setqueue + " insert overwrite table %s_hive_verify select userid, avg(age) from %s group by userid order by userid; " % (
            cls._smoketest_tbl, cls._smoketest_tbl
        )
        query += "select count(*) from %s_hive_verify;" % cls._smoketest_tbl
        exit_code, stdout, stderr = Hive.runQuery(query, stderr_as_stdout=False)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][Smoke] Smoke test for Hive Metastore failed with exit code '%d'" % exit_code
            )
            logger.error("Smoke test for Hive failed with the following error: " + stderr)
        elif stdout.find("%d" % cls._num_of_rows_smoke) == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][Smoke] Smoke test for Hive Metastore failed to verify number of rows in output"
            )
            logger.error("Smoke test for Hive failed to find [%d] in output [%s]" % (cls._num_of_rows_smoke, stdout))
        else:
            UpgradePerNode.reportProgress("[PASSED][Hive][Smoke] Smoke test for Hive Metastore succeeded")
            logger.info("Smoke test for Hive Metastore succeeded")

        logger.info("**** Running Beeline CLI Test ****")
        query = setqueue + "\ndrop table if exists %s_bline_verify;\n" % cls._smoketest_tbl
        query += "create table %s_bline_verify (userid string, age int);\n" % cls._smoketest_tbl
        query += "insert overwrite table %s_bline_verify select userid, avg(age) from %s group by userid order by userid;\n" % (
            cls._smoketest_tbl, cls._smoketest_tbl
        )
        query += "select count(*) from %s_bline_verify;\n" % cls._smoketest_tbl
        exit_code, stdout, stderr = Hive.runQueryOnBeeline(query, readFromFile=True)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][Smoke] Smoke test for HiveServer2 failed with exit code '%d'" % exit_code
            )
            logger.error("Smoke test for HiveServer2 failed with the following error: " + stderr)
        elif stdout.find("%d" % cls._num_of_rows_smoke) == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][Smoke] Smoke test for HiveServer2 failed to verify number of rows in output"
            )
            logger.error(
                "Smoke test for HiveServer2 failed to find [%d] in output [%s]" % (cls._num_of_rows_smoke, stdout)
            )
        else:
            logger.info("Smoke test for HiveServer2 succeeded")

        logger.info("**** Running WebHCat Smoke Test ****")
        query = "show tables;"
        webhcatHost = Config.get('templeton', 'TEMPLETON_HOST', default=Machine.getfqdn())
        webhcatPort = Config.get('templeton', 'TEMPLETON_PORT', default="50111")
        url = "http://%s:%s/templeton/v1/ddl" % (webhcatHost, webhcatPort)
        params = {'exec': query}
        status_code, stdout = util.curl(url, method='POST', params=params)
        if status_code != 200:
            UpgradePerNode.reportProgress(
                "[FAILED][Hive][Smoke] Smoke test for WebHCat failed due to status code = %d" % status_code
            )
        else:
            logger.info("Smoke test for WebHCat succeeded")

        UpgradePerNode.reportProgress("[INFO][Hive][Smoke] Smoke test for Hive component finished")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("Hive does not have any slaves, so nothing to do here")
