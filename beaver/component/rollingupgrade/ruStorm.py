#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, urllib2, collections, datetime, json, urllib, sys
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component.storm import Storm
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.maven import Maven
from beaver.component.rollingupgrade.RuAssert import ruAssert

#from beaver.component.rollingupgrade.ruCommon import Rollingupgrade, hdpRelease
IS_UPGRADE_SUPPORTED = Hadoop.isDalorBeyond() and Storm.isAfterErie()
logger = logging.getLogger(__name__)

#######HDFS config#####
JAVA_HDFS_SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'storm', 'storm-hdfs', 'java')
LOCAL_HDFS_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'storm-hdfs-tests')
TARGET_HDFS_STORM_JAR = os.path.join(
    Config.getEnv('ARTIFACTS_DIR'), 'storm-hdfs-tests', 'target', 'storm-integration-test-1.0-SNAPSHOT.jar'
)
HDFS_CONFIG_FILE = os.path.join(Machine.getTempDir(), 'hdfs-conf.yaml')
HADOOP_CONF = Config.get('hadoop', 'HADOOP_CONF')
HDFS_FILE = "hdfs-site.xml"
CORE_FILE = "core-site.xml"
MOD_CONF_PATH = Hadoop.getModifiedConfigPath()
HDFS_EXPECTED_LINES = [
    "the dog ate my homework", "my dog has fleas", "i like cold beverages", "don't have a cow man",
    "i don't think i like fleas"
]
HDFS_TRIDENT_EXPECTED_LINES = [
    "the cow jumped over the moon,1", "the man went to the store and bought some candy,2",
    "four score and seven years ago,3", "how many apples can you eat,4", "to be or not to be the person,5"
]
HDFS_TRIDENT_SEQ_EXPECTED_LINES = [
    "the cow jumped over the moon", "the man went to the store and bought some candy",
    "four score and seven years ago", "how many apples can you eat", "to be or not to be the person"
]

HDFS_USER = Config.get('hadoop', 'HDFS_USER')
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')

HADOOP_VERSION = Hadoop.getVersion()

HDFS_TOPOLOGY_NAME = "HDFSTopology"
HDFS_SEQ_TOPOLOGY_NAME = "HDFSSequenceTopology"
HDFS_TRIDENT_TOPOLOGY_NAME = "HDFSTridentTopology"
HDFS_TRIDENT_SEQ_TOPOLOGY_NAME = "HDFSTridentSeqTopology"

######JDBC config######
STORM_JDBC_TEST_USER = "storm_test_user"
STORM_JDBC_TEST_PASSWD = "storm_test_password"
JAVA_JDBC_SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'storm', 'storm-jdbc', 'java')
LOCAL_JDBC_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'storm-jdbc-tests')
TARGET_JDBC_STORM_JAR = os.path.join(
    Config.getEnv('ARTIFACTS_DIR'), 'storm-jdbc-tests', 'target', 'storm-integration-test-1.0-SNAPSHOT.jar'
)
JDBC_TOPOLOGY_NAME = "JdbcTopology"
(isPresent, mysqlnode) = Machine.checkifMySQLisInstalled(True)

#######HBASE config#####
JAVA_HBASE_SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'storm', 'storm-hbase', 'java')
LOCAL_HBASE_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'storm-hbase-tests')
TARGET_HBASE_STORM_JAR = os.path.join(
    Config.getEnv('ARTIFACTS_DIR'), 'storm-hbase-tests', 'target', 'storm-integration-test-1.0-SNAPSHOT.jar'
)
HBASE_TABLE_NAME = "WordCount"
HBASE_CONF = Config.get('hbase', 'HBASE_CONF_DIR')
HBASE_FILE = "hbase-site.xml"
HBASE_EXPECTED_WORDS = ["apple", "orange", "pineapple", "banana", "watermelon"]
HBASE_TOPOLOGY_NAME = "HBaseTopology"

#######HIVE config#####

LOCAL_HIVE_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'storm-hive-tests')
HIVE_HOST = "localhost"
HIVE_PORT = "9083"
TARGET_HIVE_STORM_JAR = os.path.join(
    Config.getEnv('ARTIFACTS_DIR'), 'storm-hive-tests', 'target', 'storm-integration-test-1.0-SNAPSHOT.jar'
)
HIVE_TABLE_NAME = "userdata"
DATABASE_NAME = "stormdb"
HIVE_HOME = Config.get('hive', 'HIVE_HOME')
HCAT_HOME = Config.get('hcatalog', 'HCATALOG_HOME')
HIVE_FILE = "hive-site.xml"
HIVE_CORE_DIR = "/etc/hive/conf"
HIVE_LIB = os.path.join(Config.get('hive', 'HIVE_HOME'), 'lib', '*')
STORM_HIVE_USER = "storm"
HIVE_EXPECTED_ROWS = [
    "1,user1,123456,street1,sunnyvale,ca", "2,user2,123456,street2,sunnyvale,ca", "3,user3,123456,street3,san jose,ca",
    "4,user4,123456,street4,san jose,ca"
]
HIVE_TOPOLOGY_NAME = "HiveTopology"

prefix = "env." if Machine.isWindows() else ""
HADOOP_VERSION_MAVEN_PARAMETER = prefix + "hadoopjarVersion"
STORM_VERSION_MAVEN_PARAMETER = prefix + "stormjarVersion"
HADOOP_CONF_MAVEN_PARAMETER = prefix + "hdfsDir"
HADOOP_CORE_MAVEN_PARAMETER = prefix + "hdfscoreDir"
HBASE_CONF_MAVEN_PARAMETER = prefix + "hbaseDir"
HBASE_FILE_MAVEN_PARAMETER = prefix + "hbaseFile"
HDFS_FILE_MAVEN_PARAMETER = prefix + "hdfsFile"
CORE_FILE_MAVEN_PARAMETER = prefix + "coreFile"
HIVE_VERSION_MAVEN_PARAMETER = prefix + "hivejarVersion"
HIVE_CORE_MAVEN_PARAMETER = prefix + "hivecoreDir"
HIVE_FILE_MAVEN_PARAMETER = prefix + "hivecoreFile"
PUBLIC_REPO_MAVEN_PARAMETER = prefix + "publicRepoUrl"


####################Storm rolling upgrade###########################
class ruStorm:
    _base_hdfs_dir = None
    _SmokeInputDir = None

    #######################Slider##########################
    @classmethod
    ##Setup strom slider app.
    def setup_storm_slider_app(cls):
        sys.path.insert(0, os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'nonnightly', 'storm-slider-ru', 'aaaa'))

        from test_startStorm import setupStorm
        setupStorm()  # This kills any currently running storm app and creates a new one.

    @classmethod
    def teardown_storm_slider_app(cls):
        from beaver.component.slider import Slider

        SLIDER_CLUSTER_NAME = Slider.getStormClusterName(logoutput=True)
        Slider.stop(SLIDER_CLUSTER_NAME, user=HADOOPQA_USER)
        time.sleep(3)
        Slider.destroy(SLIDER_CLUSTER_NAME, user=HADOOPQA_USER)

    #########################HDFS##########################
    @classmethod
    def setup_storm_hdfs_topology(cls, useStandaloneCmd):
        storm_version = Storm.getVersion(useStandaloneCmd=True)
        try:
            file_obj = open(HDFS_CONFIG_FILE, 'w')
            if Hadoop.isSecure():
                file_obj.write('hdfs.keytab.file: ' + Machine.getHeadlessUserKeytab(user=HADOOPQA_USER) + '\n')
                file_obj.write('hdfs.kerberos.principal: ' + Machine.get_user_principal(user=HADOOPQA_USER) + '\n')
        finally:
            file_obj.close()

        HDFS.createDirectory("/tmp/mySeqTopology", HDFS_USER, "777", False)
        HDFS.createDirectory("/tmp/dest", HDFS_USER, "777", False)
        HDFS.createDirectory("/tmp/dest2", HDFS_USER, "777", False)
        HDFS.createDirectory("/tmp/foo", HDFS_USER, "777", False)
        HDFS.createDirectory("/tmp/trident", HDFS_USER, "777", False)
        HDFS.createDirectory("/tmp/trident-seq", HDFS_USER, "777", False)

        Machine.copy(JAVA_HDFS_SRC_DIR, LOCAL_HDFS_WORK_DIR, user=None, passwd=None)
        if not Machine.isWindows():
            (exit_code, _) = Maven.run(
                'package',
                cwd=LOCAL_HDFS_WORK_DIR,
                env={
                    HADOOP_VERSION_MAVEN_PARAMETER: HADOOP_VERSION,
                    STORM_VERSION_MAVEN_PARAMETER: storm_version,
                    HADOOP_CONF_MAVEN_PARAMETER: HADOOP_CONF,
                    HDFS_FILE_MAVEN_PARAMETER: HDFS_FILE,
                    HADOOP_CORE_MAVEN_PARAMETER: HADOOP_CONF,
                    CORE_FILE_MAVEN_PARAMETER: CORE_FILE,
                    PUBLIC_REPO_MAVEN_PARAMETER: Maven.getPublicRepoUrl()
                }
            )
        else:
            filepath = os.path.join(MOD_CONF_PATH, "core-site.xml")
            (exit_code, _) = Maven.run(
                'package -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s' % (
                    HADOOP_VERSION_MAVEN_PARAMETER, HADOOP_VERSION, STORM_VERSION_MAVEN_PARAMETER, storm_version,
                    HADOOP_CONF_MAVEN_PARAMETER, HADOOP_CONF, HDFS_FILE_MAVEN_PARAMETER, HDFS_FILE,
                    HADOOP_CORE_MAVEN_PARAMETER, HADOOP_CONF, CORE_FILE_MAVEN_PARAMETER, CORE_FILE,
                    PUBLIC_REPO_MAVEN_PARAMETER, Maven.getPublicRepoUrl()
                ),
                cwd=LOCAL_HDFS_WORK_DIR
            )
        ruAssert("Storm", exit_code == 0, "[StormHDFSSetup] maven package command failed")

    @classmethod
    def submit_storm_hdfs_topology(cls, tcId, className, args, topologyName, useStandaloneCmd):
        """
        Name:
        Storm-HDFS Topologies

        Description:
        Testing all 4 storm-hdfs topologies in distributed mode
          1. SequenceFileTopology
          2. HdfsFileTopology
          3. TridentFileTopology
          4. TridentSequenceTopology

        Steps to verify:
          1. create necessary input/output dir location if needed
          2. Run storm topology in distributed mode using "storm <jar> <classname> <args>"
          3. Verify expected output from hdfs output dir
          4. kill the topology
        """
        exit_code, stdout = Storm.runStormHdfsTopology(
            TARGET_HDFS_STORM_JAR,
            className,
            args,
            None,
            logoutput=True,
            inBackground=False,
            useStandaloneCmd=useStandaloneCmd
        )
        ruAssert("Storm", exit_code == 0, "[StormHDFSSubmit] %s Failed" % (tcId))

    @classmethod
    def verify_hdfs_topology(cls, topologyName, targetDir, lines, type, useStandaloneCmd):
        """
            Verifies the hdfs topologies produced expected output
        """
        #Slider app is killed before log running job verification so disabling topology activation checks.
        if useStandaloneCmd == True:
            ruAssert(
                "Storm",
                Storm.getTopologyStatus(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd) == 'ACTIVE'
            )

        exit_code, stdout = HDFS.lsr(targetDir, False, True)
        hdfsListOutput = stdout.splitlines()

        #Picking the second last line as the first file might not have enough content and last file gets into transient
        #HDFS issues.
        if len(hdfsListOutput) >= 2:
            fileLine = hdfsListOutput[-2]
            sampleoutfile = fileLine.split(" ")[-1].strip()

            # Hecky solution as the test code for trident and core topologies writes under same directory.
            # if fileLine.endswith(".txt") and type == "cat":
            #     sampleoutfile = fileLine.split(" ")[-1].strip()
            # if fileLine.endswith(".seq") and type == "text":
            #     sampleoutfile = fileLine.split(" ")[-1].strip()

            logger.info("Taking sampleoutput file : %s" % (sampleoutfile))

            if type == "text":
                exit_code, stdout = HDFS.text(sampleoutfile, None)
            else:
                exit_code, stdout = HDFS.cat(sampleoutfile, None)
            for line in lines:
                ruAssert(
                    "Storm",
                    stdout.find(line) >= 0, "[StormHDFSVerify] expected line : %s in %s" % (line, sampleoutfile)
                )
        else:
            ruAssert("Storm", False, "hdfsListOutput must have at least 2 lines")

    @classmethod
    def tear_down_hdfs_topology(cls, topologyName, useStandaloneCmd):
        """
            kills hdfs topologies and deletes the hdfs directories.
        """
        Machine.rm(user=None, host="localhost", filepath=LOCAL_HDFS_WORK_DIR, isdir=True, passwd=None)

        Storm.killTopology(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd)
        HDFS.deleteDirectory("/tmp/mySeqTopology", HDFS_USER)
        HDFS.deleteDirectory("/tmp/dest", HDFS_USER)
        HDFS.deleteDirectory("/tmp/dest2", HDFS_USER)
        HDFS.deleteDirectory("/tmp/foo", HDFS_USER)
        HDFS.deleteDirectory("/tmp/trident", HDFS_USER)
        HDFS.deleteDirectory("/tmp/trident-seq", HDFS_USER)

    ################JDBC##############
    @classmethod
    def setup_storm_jdbc_topology(cls, useStandaloneCmd):
        from beaver.dbutil import MySQL
        MySQL.createUserWithAllPriveleges(STORM_JDBC_TEST_USER, STORM_JDBC_TEST_PASSWD)
        storm_version = Storm.getVersion(useStandaloneCmd=True)
        try:
            file_obj = open(HDFS_CONFIG_FILE, 'w')
            if Hadoop.isSecure():
                file_obj.write('hdfs.keytab.file: ' + Machine.getHeadlessUserKeytab(user=HADOOPQA_USER) + '\n')
                file_obj.write('hdfs.kerberos.principal: ' + Machine.get_user_principal(user=HADOOPQA_USER) + '\n')
        finally:
            file_obj.close()
        Machine.copy(JAVA_JDBC_SRC_DIR, LOCAL_JDBC_WORK_DIR, user=None, passwd=None)
        post_fenton_opt = " -DpostFenton=true" if Storm.isAfterFenton() else ""
        package_cmd = 'package ' + post_fenton_opt
        (exit_code, _) = Maven.run(
            package_cmd,
            cwd=LOCAL_JDBC_WORK_DIR,
            env={
                HADOOP_VERSION_MAVEN_PARAMETER: HADOOP_VERSION,
                STORM_VERSION_MAVEN_PARAMETER: storm_version,
                HADOOP_CONF_MAVEN_PARAMETER: HADOOP_CONF,
                HDFS_FILE_MAVEN_PARAMETER: HDFS_FILE,
                CORE_FILE_MAVEN_PARAMETER: CORE_FILE,
                PUBLIC_REPO_MAVEN_PARAMETER: Maven.getPublicRepoUrl()
            }
        )
        ruAssert("Storm", exit_code == 0, "[StormJDBCSetup] maven package command failed")

    @classmethod
    def submit_storm_jdbc_topology(cls, tcId, className, args, topologyName, useStandaloneCmd):
        """
        Name:
        Storm-JDBC Topology

        Description:
        Testing storm-jdbc topology in distributed mode
          1. UserPersistanceTopology

        """
        from beaver.dbutil import MySQL
        MySQL.runAsRoot("CREATE DATABASE test")
        MySQL.runAsRoot("show databases")

        exit_code, stdout = Storm.runStormJdbcTopology(
            TARGET_JDBC_STORM_JAR,
            className,
            args,
            None,
            logoutput=True,
            inBackground=False,
            useStandaloneCmd=useStandaloneCmd
        )
        ruAssert("Storm", exit_code == 0, "[StormJDBCSubmit] %s Failed" % (tcId))

    @classmethod
    def getRowCountFromMySQL(cls, tablename, database):
        '''
        finds out row count from a table
        :param tablename:
        :param database:
        :return:
        '''
        from beaver.dbutil import MySQL
        exit_code, stdout = MySQL.runCmd(
            "SELECT COUNT(*) FROM %s" % tablename,
            host=mysqlnode,
            database="%s" % database,
            user=Config.get('machine', 'MYSQL_ROOT_USER')
        )
        if exit_code != 0:
            return 0
        else:
            return int(stdout.split("\n")[1])

    @classmethod
    def verify_jdbc_topology(cls, topologyName, useStandaloneCmd):
        """
            Verifies the Jdbc topology produced expected output
            Here, we take row count of the table at two different time stamp
            and make sure that count2 > count1
        """
        logger.info("Get Count from User table")

        first_count = cls.getRowCountFromMySQL("user", "test")
        logger.info("Wait for 20 seconds and again get count from User table")
        time.sleep(20)
        second_count = cls.getRowCountFromMySQL("user", "test")
        ruAssert("Storm", second_count > first_count, "[StormJDBCVerify] JDBC topology was not in progress")

    @classmethod
    def tear_down_jdbc_topology(cls, topologyName, useStandaloneCmd):
        """
            kills Jbbc topology.
        """
        from beaver.dbutil import MySQL

        Storm.killTopology(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd)
        MySQL.runCmd("drop table user", database="test", host=mysqlnode, user=Config.get('machine', 'MYSQL_ROOT_USER'))
        MySQL.runAsRoot("DROP DATABASE IF EXISTS test")

    ######################HBASE########################
    @classmethod
    def setup_storm_hbase_topology(cls, useStandaloneCmd):
        from beaver.component.hbase import HBase

        storm_version = Storm.getVersion(useStandaloneCmd=True)

        Machine.copy(JAVA_HBASE_SRC_DIR, LOCAL_HBASE_WORK_DIR, user=None, passwd=None)

        if Machine.isWindows():
            (_, _) = Maven.run(
                'package -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s -D%s=%s' % (
                    HADOOP_VERSION_MAVEN_PARAMETER, HADOOP_VERSION, STORM_VERSION_MAVEN_PARAMETER, storm_version,
                    HBASE_CONF_MAVEN_PARAMETER, HBASE_CONF, HBASE_FILE_MAVEN_PARAMETER, HBASE_FILE,
                    HADOOP_CONF_MAVEN_PARAMETER, HADOOP_CONF, HDFS_FILE_MAVEN_PARAMETER, HDFS_FILE,
                    CORE_FILE_MAVEN_PARAMETER, CORE_FILE, PUBLIC_REPO_MAVEN_PARAMETER, Maven.getPublicRepoUrl()
                ),
                cwd=LOCAL_HBASE_WORK_DIR
            )
        else:
            (_, _) = Maven.run(
                'package',
                cwd=LOCAL_HBASE_WORK_DIR,
                env={
                    HADOOP_VERSION_MAVEN_PARAMETER: HADOOP_VERSION,
                    STORM_VERSION_MAVEN_PARAMETER: storm_version,
                    HBASE_CONF_MAVEN_PARAMETER: HBASE_CONF,
                    HBASE_FILE_MAVEN_PARAMETER: HBASE_FILE,
                    HADOOP_CONF_MAVEN_PARAMETER: HADOOP_CONF,
                    HDFS_FILE_MAVEN_PARAMETER: HDFS_FILE,
                    CORE_FILE_MAVEN_PARAMETER: CORE_FILE,
                    PUBLIC_REPO_MAVEN_PARAMETER: Maven.getPublicRepoUrl()
                }
            )

        exit_code, stdout = HBase.createTable(HBASE_TABLE_NAME, "cf", True, None)
        ruAssert("Storm", exit_code == 0)
        grant_cmd = "grant '%s', 'RWCXA', '%s', 'cf'" % (Storm.getStormUser(), HBASE_TABLE_NAME)
        exit_code, stdout = HBase.runShellCmds([grant_cmd])

    @classmethod
    def submit_storm_hbase_topology(cls, tcId, className, args, useStandaloneCmd):
        if Hadoop.isSecure():
            keytab_file = Machine.getHeadlessUserKeytab(user=HADOOPQA_USER)
            principal = Machine.get_user_principal(user=HADOOPQA_USER)
            args = args + " %s %s" % (keytab_file, principal)
        exit_code, stdout = Storm.runStormHdfsTopology(
            TARGET_HBASE_STORM_JAR,
            className,
            args,
            None,
            logoutput=True,
            inBackground=False,
            useStandaloneCmd=useStandaloneCmd
        )
        logger.info(exit_code)
        ruAssert("Storm", exit_code == 0, "[StormHBaseSubmit] %s Failed" % (tcId))

    @classmethod
    def verify_hbase_topology(cls, topologyName, lines, useStandaloneCmd):
        from beaver.component.hbase import HBase

        #Slider app is killed before log running job verification so disabling topology activation checks.
        if useStandaloneCmd == True:
            ruAssert(
                "Storm",
                Storm.getTopologyStatus(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd) == 'ACTIVE'
            )
        exit_code, stdout = HBase.runShellCmds(["scan 'WordCount'"])
        logger.info(exit_code)
        logger.info(stdout)
        for word in lines:
            ruAssert("Storm", stdout.find(word) >= 0, "[StormHBaseVerify] %s not found in wordcount table" % word)

    @classmethod
    def tear_down_hbase_topology(cls, topologyName, useStandaloneCmd):
        """
        tear down hbase topology.
        """
        from beaver.component.hbase import HBase

        Machine.rm(user=None, host="localhost", filepath=LOCAL_HBASE_WORK_DIR, isdir=True, passwd=None)

        Storm.killTopology(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd)
        exit_code, stdout = HBase.dropTable(HBASE_TABLE_NAME, True, None)
        ruAssert("Storm", exit_code == 0)

    ######################HIVE########################
    @classmethod
    def get_set_queue_cmd(cls, useStandaloneCmd):
        #For https://hortonworks.jira.com/browse/BUG-27221
        from beaver.component.hive import Hive
        if useStandaloneCmd == True:
            YARN_QUEUE = "storm"
        else:
            YARN_QUEUE = "storm-slider"

        if Hive.isTezEnabled():
            # this wont work because when hive CLI starts hive does not know queues that are not set in hive-site.xml.
            # See Deepesh email on 10/14/2014.
            setqueue = "set tez.queue.name=%s; " % YARN_QUEUE
        else:
            setqueue = "set mapred.job.queue.name=%s; " % YARN_QUEUE
        return setqueue

    @classmethod
    def setup_storm_hive_topology(cls, useStandaloneCmd):
        from beaver.component.hive import Hive

        storm_version = Storm.getVersion(useStandaloneCmd=True)
        hive_version = Hive.getVersion()
        HIVE_METASTORE_URI = Hive.getConfigValue("hive.metastore.uris", defaultValue="thrift://localhost:9083")

        global HIVE_METASTORE_URI
        global HIVE_HOST
        global HIVE_PORT
        global HIVE_WAREHOUSE_DIR
        HIVE_WAREHOUSE_DIR = Hive.getConfigValue("hive.metastore.warehouse.dir", defaultValue="/apps/hive/warehouse")
        HIVE_HOST = Hive.getHiveHost()
        HIVE_PORT = Hive.getMetastoreThriftPort()
        if Storm.isDalorBeyond():
            JAVA_HIVE_SRC_DIR = os.path.join(
                Config.getEnv('WORKSPACE'), 'tests', 'rolling_upgrade', 'Storm', '2_3', 'storm-hive', 'java'
            )
        else:
            JAVA_HIVE_SRC_DIR = os.path.join(
                Config.getEnv('WORKSPACE'), 'tests', 'rolling_upgrade', 'Storm', '2_2', 'storm-hive', 'java'
            )
        # hive.txn.manager and hive.support.concurrency are set through ambari as per bug-40500
        #logger.info("Restart Hive")
        #changes = {'hive-site.xml': {'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
        #                             'hive.support.concurrency': 'true'}}
        #Hive.modifyConfig(changes, services=['metastore'], restartService=True)
        logger.info("Create test database in Hive")

        exit_code, stdout = Hive.runQuery(
            cls.get_set_queue_cmd(useStandaloneCmd) + " drop database if exists stormdb cascade; \
                                               create database stormdb;"
        )
        ruAssert("Storm", exit_code == 0, "[StormHiveSetup] Failed to create test database" + stdout)
        HDFS.chmod(runasUser=HDFS.getHDFSUser(), perm=777, directory=HIVE_WAREHOUSE_DIR + "/" + DATABASE_NAME + ".db")
        #copy tests/storm/storm-hive/java to artifacts/storm-hive-tests
        logger.info("JAVA_SRC_DIR " + JAVA_HIVE_SRC_DIR)
        logger.info("LOCAL_WORK_DIR " + LOCAL_HIVE_WORK_DIR)
        Machine.copy(JAVA_HIVE_SRC_DIR, LOCAL_HIVE_WORK_DIR, user=None, passwd=None)
        #mvn package
        if Machine.isWindows():
            (_, _) = Maven.run(
                'package -D%s=%s -D%s=%s -D%s=%s -D%s=%s' % (
                    HADOOP_VERSION_MAVEN_PARAMETER, HADOOP_VERSION, STORM_VERSION_MAVEN_PARAMETER,
                    storm_version, HIVE_VERSION_MAVEN_PARAMETER, hive_version, PUBLIC_REPO_MAVEN_PARAMETER,
                    Maven.getPublicRepoUrl(), CORE_FILE_MAVEN_PARAMETER, CORE_FILE, HADOOP_CORE_MAVEN_PARAMETER,
                    HADOOP_CONF, HIVE_CORE_MAVEN_PARAMETER, HIVE_CORE_DIR, HIVE_FILE_MAVEN_PARAMETER, HIVE_FILE
                ),
                cwd=LOCAL_HIVE_WORK_DIR
            )
        else:
            (_, _) = Maven.run(
                'package',
                cwd=LOCAL_HIVE_WORK_DIR,
                env={
                    HADOOP_VERSION_MAVEN_PARAMETER: HADOOP_VERSION,
                    STORM_VERSION_MAVEN_PARAMETER: storm_version,
                    HIVE_VERSION_MAVEN_PARAMETER: hive_version,
                    PUBLIC_REPO_MAVEN_PARAMETER: Maven.getPublicRepoUrl(),
                    CORE_FILE_MAVEN_PARAMETER: CORE_FILE,
                    HADOOP_CONF_MAVEN_PARAMETER: HADOOP_CONF,
                    HDFS_FILE_MAVEN_PARAMETER: HDFS_FILE,
                    HADOOP_CORE_MAVEN_PARAMETER: HADOOP_CONF,
                    HIVE_CORE_MAVEN_PARAMETER: HIVE_CORE_DIR,
                    HIVE_FILE_MAVEN_PARAMETER: HIVE_FILE
                }
            )
        create_table_q = "use %s; \
          drop table if exists %s; \
          create table %s (id int, name string, phone string, street string) \
          partitioned by (city string, state string) \
          clustered by (id) into %s buckets \
          stored as orc \
          tblproperties ('transactional'='true');" % (DATABASE_NAME, HIVE_TABLE_NAME, HIVE_TABLE_NAME, "5")

        exit_code, stdout = Hive.runQuery(cls.get_set_queue_cmd(useStandaloneCmd) + create_table_q)
        ruAssert("Storm", exit_code == 0, "[StormHiveSetup] Failed to create test table userdata_partitioned")
        HDFS.chmod(
            runasUser=HDFS.getHDFSUser(),
            perm=777,
            directory=HIVE_WAREHOUSE_DIR + "/" + DATABASE_NAME + ".db/" + HIVE_TABLE_NAME
        )

    @classmethod
    def getHiveQueryOutput(cls, query, willRunMR=True, delim=",", useStandaloneCmd=True):
        from beaver.component.hive import Hive

        hiveconf = {}
        if willRunMR:
            hiveconf = {
                'hive.input.format': 'org.apache.hadoop.hive.ql.io.HiveInputFormat',
                'hive.vectorized.execution.enabled': 'false',
                'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
                'hive.support.concurrency': 'true'
            }

        exit_code, stdout, stderr = Hive.runQuery(
            cls.get_set_queue_cmd(useStandaloneCmd) + query, hiveconf=hiveconf, stderr_as_stdout=False
        )
        ruAssert("Storm", exit_code == 0, "[HiveQueryOutput] Failed to run Hive query [%s]" % query)
        return stdout.replace('\t', delim)

    @classmethod
    def submit_storm_hive_topology(cls, tcId, className, args, useStandaloneCmd):
        if Hadoop.isSecure():
            if Config.hasOption('machine', 'USER_REALM'):
                user_realm = Config.get('machine', 'USER_REALM', '')
            else:
                nnKerbPrincipal = HDFS.getNameNodePrincipal(defaultValue='')
                atloc = nnKerbPrincipal.find("@")
                if (atloc != -1):
                    user_realm = nnKerbPrincipal[atloc:]
            if user_realm != None:
                args += " " + Machine.getHeadlessUserKeytab(Config.getEnv('USER')
                                                            ) + " " + Config.getEnv('USER') + '@' + user_realm

        exit_code, stdout = Storm.runStormHdfsTopology(
            TARGET_HIVE_STORM_JAR,
            className,
            args,
            None,
            logoutput=True,
            inBackground=False,
            useStandaloneCmd=useStandaloneCmd
        )
        logger.info(exit_code)

        ruAssert("Storm", exit_code == 0, "[StormHiveSubmit] %s Failed" % (tcId))

    @classmethod
    def verify_hive_topology(cls, topologyName, rows, useStandaloneCmd):
        #Slider app is killed before log running job verification so disabling topology activation checks.
        if useStandaloneCmd == True:
            ruAssert(
                "Storm",
                Storm.getTopologyStatus(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd) == 'ACTIVE'
            )

        verify_table_q = "select distinct id,name,phone,street,city,state from stormdb.userdata order by id;"
        stdout = cls.getHiveQueryOutput(verify_table_q, willRunMR=True, delim=",", useStandaloneCmd=useStandaloneCmd)
        logger.info(stdout)
        for row in rows:
            ruAssert("Storm", stdout.find(row) >= 0, "[StormHiveVerify] %s not found in userdata table" % row)

    @classmethod
    def tear_down_hive_topology(cls, topologyName, useStandaloneCmd):
        """
        tear down hbase topology.
        """
        from beaver.component.hive import Hive

        Machine.rm(user=None, host="localhost", filepath=LOCAL_HIVE_WORK_DIR, isdir=True, passwd=None)

        Storm.killTopology(topologyName, logoutput=True, useStandaloneCmd=useStandaloneCmd)
        #Hive.restoreConfig(services=['metastore'])
        drop_table_q = "use %s; drop table if exists %s; " % (DATABASE_NAME, HIVE_TABLE_NAME)
        exit_code, stdout = Hive.runQuery(cls.get_set_queue_cmd(useStandaloneCmd) + drop_table_q)
        ruAssert("Storm", exit_code == 0)

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None, useStandaloneCmd=True):
        '''
        Build a fat jar for running HDFS, HBASE and HIVE topologies.
        '''

        if useStandaloneCmd == False:
            cls.setup_storm_slider_app()

        #skip storm hdfs and hive topology due to QE-7577
        #cls.setup_storm_hdfs_topology(useStandaloneCmd)

        #skip storm-hbase topology due to QE-4684
        #cls.setup_storm_hbase_topology(useStandaloneCmd)

        #cls.setup_storm_hive_topology(useStandaloneCmd)

        #Storm-JDBC test should run only when MySQL database is installed
        if isPresent and useStandaloneCmd == True:
            if IS_UPGRADE_SUPPORTED:
                cls.setup_storm_jdbc_topology(useStandaloneCmd)

        if runSmokeTestSetup:
            logger.info("**** Running Storm Smoke Test Setup ****")
            cls.smoke_test_setup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Storm does not need this.
        '''
        return

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None, useStandaloneCmd=True):
        '''
        Submits topologies to storm.
        :return:  number of application started
        '''
        #cls.background_job_setup(runSmokeTestSetup)
        # start long running application which performs I/O operations (BUG-23838)
        from beaver.component.hbase import HBase

        #cls.submit_storm_hdfs_topology(HDFS_SEQ_TOPOLOGY_NAME,"org.apache.storm.hdfs.bolt.SequenceFileTopology", Hadoop.getFSDefaultValue()+"/tmp/mySeqTopology " + HDFS_CONFIG_FILE + " " + HDFS_SEQ_TOPOLOGY_NAME,  HDFS_SEQ_TOPOLOGY_NAME, useStandaloneCmd)
        #cls.submit_storm_hdfs_topology(HDFS_TRIDENT_SEQ_TOPOLOGY_NAME, "org.apache.storm.hdfs.trident.TridentSequenceTopology", Hadoop.getFSDefaultValue()+"/tmp/trident " + HDFS_CONFIG_FILE  + " " + HDFS_TRIDENT_SEQ_TOPOLOGY_NAME, HDFS_TRIDENT_SEQ_TOPOLOGY_NAME, useStandaloneCmd)

        #cls.submit_storm_hdfs_topology(HDFS_TOPOLOGY_NAME, "org.apache.storm.hdfs.bolt.HdfsFileTopology", Hadoop.getFSDefaultValue()+"/tmp " + HDFS_CONFIG_FILE + " " + HDFS_TOPOLOGY_NAME, HDFS_TOPOLOGY_NAME, useStandaloneCmd)
        #cls.submit_storm_hdfs_topology(HDFS_TRIDENT_TOPOLOGY_NAME, "org.apache.storm.hdfs.trident.TridentFileTopology", Hadoop.getFSDefaultValue()+"/trident " + HDFS_CONFIG_FILE  + " " + HDFS_TRIDENT_TOPOLOGY_NAME, HDFS_TRIDENT_TOPOLOGY_NAME, useStandaloneCmd)

        # skip storm-hbase topology due to QE-4684
        #cls.submit_storm_hbase_topology(HBASE_TOPOLOGY_NAME,"org.apache.storm.hbase.topology.PersistentWordCount", HBase.getConfigValue("hbase.rootdir", None) + " " + HBASE_TOPOLOGY_NAME, useStandaloneCmd)

        #cls.submit_storm_hive_topology(HIVE_TOPOLOGY_NAME,"org.apache.storm.hive.bolt.HiveTopologyPartitioned", HIVE_METASTORE_URI+" "+DATABASE_NAME + " " + HIVE_TABLE_NAME + " " + HIVE_TOPOLOGY_NAME, useStandaloneCmd)

        # Storm-JDBC test should run only when MySQL database is installed
        if isPresent and useStandaloneCmd == True:
            if IS_UPGRADE_SUPPORTED:
                args = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource jdbc:mysql://%s/test %s %s %s" % (
                    mysqlnode, STORM_JDBC_TEST_USER, STORM_JDBC_TEST_PASSWD, JDBC_TOPOLOGY_NAME
                )
                #cls.submit_storm_jdbc_topology(JDBC_TOPOLOGY_NAME, "org.apache.storm.jdbc.topology.UserPersistanceTopology", args, JDBC_TOPOLOGY_NAME, useStandaloneCmd)
                #return 7

        return 6

    @classmethod
    def background_job_teardown(cls, useStandaloneCmd=True):
        '''
        Cleanup involves killing all topologies, deleting the hdfs directories , hbase and hive tables.
        '''
        #cls.tear_down_hdfs_topology(HDFS_SEQ_TOPOLOGY_NAME , useStandaloneCmd)
        #cls.tear_down_hdfs_topology(HDFS_TRIDENT_SEQ_TOPOLOGY_NAME, useStandaloneCmd)
        #cls.tear_down_hdfs_topology(HDFS_TOPOLOGY_NAME, useStandaloneCmd)
        #cls.tear_down_hdfs_topology(HDFS_TRIDENT_TOPOLOGY_NAME, useStandaloneCmd)

        #skip storm-hbase topology due to QE-4684
        #cls.tear_down_hbase_topology(HBASE_TOPOLOGY_NAME, useStandaloneCmd)

        #cls.tear_down_hive_topology(HIVE_TOPOLOGY_NAME, useStandaloneCmd)

        if isPresent and useStandaloneCmd == True:
            if IS_UPGRADE_SUPPORTED:
                #cls.tear_down_jdbc_topology(JDBC_TOPOLOGY_NAME, useStandaloneCmd)
                pass

        if useStandaloneCmd == False:
            #cls.teardown_storm_slider_app()
            pass

    @classmethod
    def verifyLongRunningJob(cls, useStandaloneCmd=True):
        '''
        Verify topologies that were submitted as long running job worked as expected.
        :param cls:
        :return:
        '''
        #Adding some sleep to allow topologies to run before the check is called.
        time.sleep(60)
        #cls.verify_hdfs_topology(HDFS_SEQ_TOPOLOGY_NAME, "/tmp/mySeqTopology/tmp/source", HDFS_EXPECTED_LINES, "text", useStandaloneCmd)
        #cls.verify_hdfs_topology(HDFS_TRIDENT_SEQ_TOPOLOGY_NAME, "/tmp/dest2", HDFS_TRIDENT_SEQ_EXPECTED_LINES, "text", useStandaloneCmd)
        #cls.verify_hdfs_topology(HDFS_TOPOLOGY_NAME, "/tmp/dest2", HDFS_EXPECTED_LINES, "cat", useStandaloneCmd)
        #cls.verify_hdfs_topology(HDFS_TRIDENT_TOPOLOGY_NAME, "/tmp/trident", HDFS_TRIDENT_EXPECTED_LINES, "cat", useStandaloneCmd)

        # skip storm-hbase topology due to QE-4684
        #cls.verify_hbase_topology(HBASE_TOPOLOGY_NAME, HBASE_EXPECTED_WORDS, useStandaloneCmd)

        #cls.verify_hive_topology(HIVE_TOPOLOGY_NAME, HIVE_EXPECTED_ROWS, useStandaloneCmd)

        if isPresent and useStandaloneCmd == True:
            if IS_UPGRADE_SUPPORTED:
                #cls.verify_jdbc_topology(JDBC_TOPOLOGY_NAME, useStandaloneCmd)
                pass

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Storm does not need this.
        '''
        return

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Storm does not support rolling upgrades.
        '''
        ## TODO add code to upgrade SNN
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def verify_and_stop_slider_app(cls):
        '''
        Verifies all the topologies and the output, performs the cleanup of all topologies,
        destroys slider app, restarts the storm-slider app and finally resubmits all the topologies.
        :return:
        '''
        cls.verifyLongRunningJob(useStandaloneCmd=False)
        cls.background_job_teardown(useStandaloneCmd=False)

    @classmethod
    def start_slider_app_resubmit_topologies(cls):
        '''
        Restarts storm-slider app and resubmits the topologies
        :return:
        '''
        cls.background_job_setup(runSmokeTestSetup=True, config=None, useStandaloneCmd=False)
        cls.run_background_job(runSmokeTestSetup=True, config=None, useStandaloneCmd=False)

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Storm does not support rolling upgrades.
        '''
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
        Storm does not support rolling upgrades.
        '''
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def ru_rollback_state(cls):
        '''
        Storm does not support rolling upgrades.
        '''
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def ru_downgrade_state(cls):
        '''
        Storm does not support rolling upgrades.
        '''
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def ru_finalize_state(cls):
        '''
        Storm does not support rolling upgrades.
        '''
        logger.info("Storm does not support rolling upgrades.")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("storm does not support rolling upgrade/downgrade.")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("storm does not support downgrade")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("no smoke tests for storm")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        For storm this is not used.
        '''
        logger.info("TODO")
