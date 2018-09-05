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
import pytest
import random

logger = logging.getLogger(__name__)

HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')

INPUT_CSV_FILE = 'inputCSV.csv'
TEST_FOLDER = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'ru_phoenix_testFolder')
TEST_TABLE = 'testTable'

logger = logging.getLogger(__name__)

HOST = None

PHOENIX_TEST_TABLE = 'basicTable'

EXAMPLE_ROWS = [
    '1,John,Snow,The Wall\n'
    '2,Jaime,Lanninster,Kings Landing\n'
    '3,Ramsay,Bolton,NutHouse\n'
    '4,Kaleesi,Mother of Dragons,Several'
]

EXAMPLE_INSERT_STATEMENT = "UPSERT INTO %s VALUES (5,'Hodor','Hooodor','HODOR!!')" % TEST_TABLE

logger = logging.getLogger(__name__)


class ruPhoenix:

    # set the maven version
    _smokeTestNum = 0

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        if runSmokeTestSetup:
            cls.smoke_test_setup()
        logger.info("Background long running job not applicable to Phoenix")

    @classmethod
    def smoke_test_setup(cls):
        '''
            Setup required to run Smoke test
        '''
        from beaver.component.phoenix import Phoenix
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #We drop the table in case it already existed.
        Phoenix.dropTable(TEST_TABLE)

        #We increase the number of smoke test that have been ran.
        cls._smokeTestNum += 1

        #We create the test folder with permissions for every user
        Machine.makedirs(HADOOPQA_USER, HOST, TEST_FOLDER)

        #We create an empty table
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
        exit_code, stdout = Phoenix.createTable(TEST_TABLE, primaryKey, columns)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] %s creation failed with message: %s" % (TEST_TABLE, str(stdout))
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s creation succeeded" % TEST_TABLE)

        #We create a small file that we will use to insert a few rows (4 rows from EXAMPLE_ROWS)
        Machine.touch(os.path.join(TEST_FOLDER, INPUT_CSV_FILE))
        file = open(os.path.join(TEST_FOLDER, INPUT_CSV_FILE), 'w')
        lines = ''
        for i in range(len(EXAMPLE_ROWS)):
            lines += EXAMPLE_ROWS[i]

        file.writelines(lines)
        file.close()

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
            Runs background long running Phoenix Job
            :param runSmokeTestSetup: Runs smoke test setup if set to true
            :param config: expected configuration location
            :return: Total number of long running jobs started
        '''
        logger.info("Background job not applicable to Phoenix")
        return 0

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None, env=None):
        '''
            Run smoke test for Phoenix
            Steps:
              -Insertion using psql.py.
              -Insertion using sqlline.py.
              -Verify that all three insertions went well
            :param smoketestnumber: Used for unique output log location
        '''
        from beaver.component.phoenix import Phoenix
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][PHOENIX][Smoke] Smoke test for Phoenix component started")

        logger.info('=========================================================================')
        logger.info('Running Phoenix smoke test Num: %s' % str(smoketestnumber))
        logger.info('=========================================================================')

        if config is not None:
            logger.info(
                'config value (smoke test): ' + str(config) +
                ". Config already applied, no config change necessary for Phoenix"
            )

        #We verify that the basic table is still there
        cls.verifyBasicTable(config, env, applyConfigChange=False)

        #We inset some data into testTable using psql.py
        logger.info('LogMessage: Running psql.py')
        exit_code, stdout = Phoenix.insertCSVDataViaPSQL(os.path.join(TEST_FOLDER, INPUT_CSV_FILE), TEST_TABLE, env)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress("[FAILED][PHOENIX][Smoke] Psql.py failed with message: %s " % str(stdout))
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] %s csv insertion with psql.py succeeded" % TEST_TABLE
            )

        #We insert some data into testTable using sqlline.py
        logger.info('LogMessage: Running sqlline.py')
        exit_code, stdout = Phoenix.runSQLLineCmds(EXAMPLE_INSERT_STATEMENT, env)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress("[FAILED][PHOENIX][Smoke] Sqlline.py failed with message: %s " % str(stdout))
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] %s statement insertion with sqlline.py succeeded." % TEST_TABLE
            )

        #we verify that we have 11004 rows on testTable
        exit_code, stdout = Phoenix.runSQLLineCmds(
            'SELECT \'Found \'||count(*)||\' records\' FROM %s;' % TEST_TABLE, env
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] First select query failed with message: %s " % str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s select query succeeded." % TEST_TABLE)
        if stdout.find('Found 5 records') == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Wrong number of records. We expected 5, we got: %s " % str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] The number of rows in %s is correct." % TEST_TABLE)

        #We verify that the rows have been inserted correctly
        exit_code, stdout = Phoenix.runSQLLineCmds(
            'SELECT FirstName,SecondName,City FROM %s WHERE ID = %s;' % (TEST_TABLE, str(5)),
            env,
            outputFormat='xmlattr'
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Second select query failed with message: %s " % str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s select query succeeded." % TEST_TABLE)
        if stdout.find('Hodor') == -1 or stdout.find('Hooodor') == -1 or stdout.find('HODOR!!') == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Wrong values in row. We expected Hodor,Hooodor and HODOR!!, we got: %s " %
                str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s select returned the correct rows." % TEST_TABLE)

        if config is not None:
            logger.info(
                'config value (smoketest): ' + str(config) + ". Config never applied, no config restore necessary"
            )

        UpgradePerNode.reportProgress("[INFO][PHOENIX][Smoke] Smoke test for PHOENIX component finished")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        logger.info("Background job not applicable to Phoenix")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        logger.info("Long running job not applicable to Phoenix")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("Upgrade master not applicable to Phoenix ")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("Upgrade slave not applicable to Phoenix")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.phoenix import Phoenix
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("phoenix-client", version)
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][PHOENIX][Smoke]Phoenix MasterNode Downgrade Finished")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("Downgrade slave not applicable to Phoenix")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("**** Running Phoenix CLI Test ****")
        cls.run_smoke_test(cls._smokeTestNum, config, env)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        cls.verifyBasicTable()

        from beaver.component.phoenix import Phoenix
        if Phoenix.getVersion()[0:3] >= '4.7':
            cls.verifySchemaFunctionality()

    @classmethod
    def verifyBasicTable(cls, config=None, env=None, applyConfigChange=True):
        '''
          We verify that the original table has 10 rows and that rows can be read (random row)
        '''
        from beaver.component.phoenix import Phoenix
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if config is not None and applyConfigChange:
            logger.info(
                'config value (verify basic table): ' + str(config) +
                ". Config already applied, no config change necessary for Phoenix"
            )

        #PHOENIX_TEST_TABLE will have rows from 0 to 9. We test that these exists
        exit_code, stdout = Phoenix.runSQLLineCmds(
            'SELECT \'Found \'||count(*)||\' records\' FROM %s;' % PHOENIX_TEST_TABLE, env
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] BasicTable first select query failed with message: %s " % str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s select query succeeded." % PHOENIX_TEST_TABLE)
        if stdout.find('Found 10 records') == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] BasicTable found a wrong number of queries. Expected 10, found: %s " %
                str(stdout)
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] %s select query returned the correct number of rows." % PHOENIX_TEST_TABLE
            )

        #PHOENIX_TEST_TABLE will have rows from 0 to 9. We test that these exists
        rowNumber = random.randint(0, 9)
        exit_code, stdout = Phoenix.runSQLLineCmds(
            "SELECT * FROM %s WHERE ID=%s;" % (PHOENIX_TEST_TABLE, str(rowNumber)), env, outputFormat='xmlattr'
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] BasicTable second select query failed with message: %s " % str(stdout)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] %s select query succeeded." % PHOENIX_TEST_TABLE)
        if stdout.find('Name_%s' % rowNumber) == -1 or stdout.find('Surname_%s' % rowNumber) == -1 or stdout.find(
                'City_%s' % rowNumber) == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Smoke test for PHOENIX failed due to exitcode = %s " % exit_code
            )
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] BasicTable found incorrect values on the row with ID %s: %s " %
                (rowNumber, str(stdout))
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] %s select query returned a valid row." % PHOENIX_TEST_TABLE
            )

        if config is not None and applyConfigChange:
            logger.info(
                'config value (verify basic table): ' + str(config) +
                ". Config never applied, no config restore necessary"
            )

    @classmethod
    def verifySchemaFunctionality(cls):
        '''
          We verify that the system can operate with SCHEMA functionality.
        '''
        from beaver.component.phoenix import Phoenix
        from beaver.component.hbase import HBase
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        HBASE_HOME = Config.get('hbase', 'HBASE_HOME')
        HBASE_CONF_DIR = os.path.join(HBASE_HOME, "conf")
        #We verify the schema functionality.
        HBASE_CHANGES = {}
        HBASE_CHANGES['hbase-site.xml'] = {}
        HBASE_CHANGES['hbase-site.xml']['phoenix.schema.isNamespaceMappingEnabled'] = 'true'
        HBASE_CHANGES['hbase-site.xml']['phoenix.schema.mapSystemTablesToNamespace'] = 'true'

        TEST_TABLE_A = 'Table_A'

        SCHEMA_1 = 'SCHEMA_1'

        masterNodes = HBase.getAllMasterNodes()
        regionNodes = HBase.getRegionServers()

        hbase_allnodes = masterNodes + regionNodes
        gateway_node = Machine.getfqdn()
        if gateway_node not in hbase_allnodes:
            hbase_allnodes.append(gateway_node)

        HBase.stopHBaseCluster()

        HBase.modifyConfig(changes=HBASE_CHANGES, nodeSelection={'nodes': hbase_allnodes})

        util.copy_back_to_original_config(
            HBase.getModifiedConfigPath(), HBASE_CONF_DIR, file_list=["hbase-site.xml"], node_list=hbase_allnodes
        )

        HBase.startHBaseCluster(HBase.getModifiedConfigPath())

        #We grant permissions to all tables.
        Phoenix.grantPermissionsToSystemTables(schemaFunctionalityEnabled=True)

        #We check that we can still query the original table.
        cls.verifyBasicTable()

        #We check that we can create/query schemas.
        exit_code, stdout = Phoenix.runSQLLineCmds('CREATE SCHEMA IF NOT EXISTS %s;' % SCHEMA_1)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Creation of schema %s failed due to exitcode = %s " % (SCHEMA_1, exit_code)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] Schema creation %s succeeded." % (SCHEMA_1))

        #we create tables inside that schema
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
        exit_code, stdout = Phoenix.createTable(SCHEMA_1 + '.' + TEST_TABLE_A, primaryKey, columns)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Table creation %s on schema %s failed due to exitcode = %s " %
                (TEST_TABLE_A, SCHEMA_1, exit_code)
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] Table creation %s on schema %s succeeded." % (TEST_TABLE_A, SCHEMA_1)
            )

        #We insert some data into the table through upsert.
        for i in range(0, 5):
            exit_code, stdout = Phoenix.runSQLLineCmds(
                'UPSERT INTO %s VALUES (%s, "name_%s","secondName_%s","city_%s");' %
                (SCHEMA_1 + '.' + TEST_TABLE_A, str(i), str(i), str(i), str(i))
            )
            if exit_code != 0:
                UpgradePerNode.reportProgress(
                    "[FAILED][PHOENIX][Smoke] Table UPSERT %s on schema %s failed due to exitcode = %s " %
                    (TEST_TABLE_A, SCHEMA_1, exit_code)
                )
            else:
                UpgradePerNode.reportProgress(
                    "[PASSED][PHOENIX][Smoke] Table UPSERT %s on schema %s succeeded." % (TEST_TABLE_A, SCHEMA_1)
                )

        #We verify that the data has been correctly inserted
        exit_code, stdout = Phoenix.runSQLLineCmds('SELECT * FROM %s WHERE ID=3;' % (SCHEMA_1 + '.' + TEST_TABLE_A))
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Table SELECT %s on schema %s failed due to exitcode = %s " %
                (TEST_TABLE_A, SCHEMA_1, exit_code)
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] Table SELECT %s on schema %s succeeded." % (TEST_TABLE_A, SCHEMA_1)
            )

        if stdout.find('name_3') == -1 or stdout.find('secondName_3') == -1 or stdout.find('city_3') == -1:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Table SELECT %s on schema %s returned the wrong results: %s" %
                (TEST_TABLE_A, SCHEMA_1, stdout)
            )
        else:
            UpgradePerNode.reportProgress(
                "[PASSED][PHOENIX][Smoke] Table SELECT %s on schema %s succeeded." % (TEST_TABLE_A, SCHEMA_1)
            )

        #We verify that we can drop the schemas with tables on it.
        exit_code, stdout = Phoenix.runSQLLineCmds('DROP SCHEMA %s;' % SCHEMA_1)
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Schema drop failed due to exitcode = %s " % (exit_code)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] Schema drop succeeded.")

        #We verify that the schema has been dropped.
        exit_code, stdout = Phoenix.runSQLLineCmds(
            'SELECT TABLE_NAME FROM SYSTEM.CATALOG WHERE SCHEMA = %s' % SCHEMA_1, outputFormat='xmlattr'
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Schema drop failed due to exitcode = %s " % (exit_code)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] Schema drop succeeded.")
        if stdout.find(TEST_TABLE_A) != 0:
            UpgradePerNode.reportProgress(
                "[FAILED][PHOENIX][Smoke] Table %s did not drop on drop schema command " % (TEST_TABLE_A)
            )
        else:
            UpgradePerNode.reportProgress("[PASSED][PHOENIX][Smoke] Table %s successfuly dropped." % TEST_TABLE_A)
