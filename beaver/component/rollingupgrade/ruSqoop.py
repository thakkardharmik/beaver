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
import urllib, sys, random, shutil
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils

logger = logging.getLogger(__name__)


class ruSqoop:
    _yarn_queue = "sqoop"
    _hdfs_lrtest_dir = "/tmp/sqooplrtest"
    _lrtest_tbl = "sqooplr"
    _test_lrrow_count = 10000
    _hdfs_smtest_dir = "/tmp/sqoopsmtest"
    _smtest_tbl = "sqoopsm"
    _test_smrow_count = 1000
    _hdfs_user = Config.get("hadoop", 'HDFS_USER')
    _test_db_host = Machine.getfqdn()
    _test_db_user = "sqoopru"
    _test_db_passwd = "sqoopru"
    _test_db = "sqoopru"
    _test_db_oracle = Config.get('machine', 'ORACLE_DB', default='xe')

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        from beaver.component.sqoop import Sqoop
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][SQOOP][BGSetup] Background job set for sqoop  is called")
        dbflavor = Sqoop.getDatabaseFlavor()
        if dbflavor == "oracle":
            cls._lrtest_tbl = cls._lrtest_tbl.upper()
            cls._smtest_tbl = cls._smtest_tbl.upper()
        logger.info("*** Creating the test data for Sqoop long running test on dbflavorr= %s ***" % dbflavor)
        cls.generate_test_data(cls._hdfs_lrtest_dir, cls._test_lrrow_count)
        create_tbl_query = "create table %s (userid int, age int, primary key (userid));" % cls._lrtest_tbl
        cls.init_test_database(dbflavor)
        cls.run_database_query(create_tbl_query, dbflavor)
        if runSmokeTestSetup:
            logger.info("**** Running Sqoop Smoke Test Setup ****")
            cls.smoke_test_setup()

    @classmethod
    def generate_test_data(cls, hdfs_test_dir, num_of_rows):
        test_data_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "sqooptest.dat")
        f = open(test_data_file, 'w')
        userid = 100000
        for i in xrange(num_of_rows):
            f.write("%d,%d\n" % (userid + i, random.randint(10, 80)))
        f.close()
        HDFS.createDirectory(hdfs_test_dir, user=cls._hdfs_user, perm='777', force=True)
        HDFS.copyFromLocal(test_data_file, hdfs_test_dir)

    @classmethod
    def init_test_database(cls, dbflavor):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if dbflavor == "mysql":
            from beaver.dbutil import MySQL
            MySQL.recreateDatabase(cls._test_db)
            MySQL.createUserWithAllPriveleges(
                cls._test_db_user,
                cls._test_db_passwd,
                hosts=['%', cls._test_db_host, 'localhost'],
                database=cls._test_db
            )
        elif dbflavor == "oracle":
            from beaver.dbutil import Oracle
            Oracle.dropUser(cls._test_db_user, database=cls._test_db_oracle)
            Oracle.createUser(cls._test_db_user, cls._test_db_passwd, database=cls._test_db_oracle)
        elif dbflavor.startswith("postgres"):
            from beaver.dbutil import Postgres
            Postgres.recreateDatabase(cls._test_db)
            Postgres.createUser(cls._test_db_user, cls._test_db_passwd)
            Postgres.grantAllPrivileges(cls._test_db_user, cls._test_db)
        else:
            UpgradePerNode.reportProgress("[FAILED][SQOOP][INIT] Invalid database flavor '%s' " % dbflavor)

    @classmethod
    def run_database_query(cls, query, dbflavor):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if dbflavor == "mysql":
            from beaver.dbutil import MySQL
            return MySQL.runCmd(
                query,
                host=cls._test_db_host,
                database=cls._test_db,
                user=cls._test_db_user,
                passwd=cls._test_db_passwd
            )
        elif dbflavor == "oracle":
            from beaver.dbutil import Oracle
            return Oracle.runCmd(
                query, database=cls._test_db_oracle, user=cls._test_db_user, passwd=cls._test_db_passwd
            )
        elif dbflavor.startswith("postgres"):
            from beaver.dbutil import Postgres
            return Postgres.runCmd(query, database=cls._test_db, user=cls._test_db_user, passwd=cls._test_db_passwd)
        else:
            UpgradePerNode.reportProgress("[FAILED][SQOOP][INIT] Invalid database flavor '%s' " % dbflavor)

    @classmethod
    def getGenericDBFlavor(cls):
        from beaver.component.sqoop import Sqoop
        dbflavor = Sqoop.getDatabaseFlavor()
        if dbflavor.startswith("postgres"): dbflavor = "postgres"
        elif dbflavor.startswith("ora"): dbflavor = "oracle"
        return dbflavor

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        logger.info("*** Creating the test data for Sqoop smoke test ***")
        cls.generate_test_data(cls._hdfs_smtest_dir, cls._test_smrow_count)

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        from beaver.component.sqoop import Sqoop
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][SQOOP][BGJob] Long running job for Sqoop component started")
        dbflavor = cls.getGenericDBFlavor()
        if dbflavor == "oracle":
            db = cls._test_db_oracle
        else:
            db = cls._test_db
        cmd = Sqoop.getJdbcOperationCmd(
            "export",
            dbflavor,
            db,
            cls._test_db_user,
            cls._test_db_passwd,
            cls._test_db_host,
            addlargs="--table %s --export-dir %s" % (cls._lrtest_tbl, cls._hdfs_lrtest_dir),
            options="-Dmapred.job.queue.name=%s" % cls._yarn_queue
        )
        Sqoop.runInBackground(cmd)
        if runSmokeTestSetup:
            logger.info("**** Running Sqoop Smoke Test Setup ****")
            cls.smoke_test_setup()
        return 1

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        return cls.run_client_smoketest(config=config)

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        pass

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.sqoop import Sqoop
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        verify_query = "select count(*) from %s;" % cls._lrtest_tbl
        exit_code, output = cls.run_database_query(verify_query, Sqoop.getDatabaseFlavor())
        if exit_code != 0 or output.find("10000") == -1:
            UpgradePerNode.reportProgress("[FAILED][SQOOP][BGJobCheck] Long running test for Sqoop component failed ")
        else:
            UpgradePerNode.reportProgress("[PASSED][SQOOP][BGJobCheck] Long running test for Sqoop component passed ")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("**** Sqoop has no master services to upgrade ****")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("Sqoop does not have any slaves, so no slaves to upgrade")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        from beaver.component.sqoop import Sqoop
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        dbflavor = cls.getGenericDBFlavor()
        drop_tbl_query = "drop table %s;" % cls._smtest_tbl
        cls.run_database_query(drop_tbl_query, dbflavor)
        create_tbl_query = "create table %s (userid int, age int, primary key (userid));" % cls._smtest_tbl
        cls.run_database_query(create_tbl_query, dbflavor)
        UpgradePerNode.reportProgress("[INFO][SQOOP][Smoke] Smoke test for Sqoop component started ")
        if dbflavor == "oracle":
            db = cls._test_db_oracle
        else:
            db = cls._test_db
        exit_code, stdout = Sqoop.jdbcOperation(
            "export",
            dbflavor,
            db,
            cls._test_db_user,
            cls._test_db_passwd,
            cls._test_db_host,
            addlargs="--table %s --export-dir %s" % (cls._smtest_tbl, cls._hdfs_smtest_dir),
            options="-Dmapred.job.queue.name=%s" % cls._yarn_queue
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][SQOOP][Smoke] Smoke test for Sqoop failed with exit code = %d " % exit_code
            )
            return
        verify_query = "select count(*) from %s;" % cls._smtest_tbl
        exit_code, output = cls.run_database_query(verify_query, dbflavor)
        if exit_code != 0 or output.find("1000") == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][SQOOP][Smoke] Smoke test for Sqoop component failed during verification of exported data "
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][SQOOP][Smoke] Smoke test for Sqoop component passed during verification of exported data "
            )
        UpgradePerNode.reportProgress("### Smoke test for Sqoop component finished ####")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("**** Sqoop has no master services to downgrade ****")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("Sqoop does not have any slaves, so no slaves to downgrade")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("Sqoop does not have any slaves, so nothing to do here")
