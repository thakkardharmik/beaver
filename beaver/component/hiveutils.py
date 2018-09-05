#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.hive import Hive
from beaver.machine import Machine
from beaver.config import Config
from beaver.java import Java
from beaver.maven import Maven
from beaver.ant import Ant
from beaver import util
import os, random, re, time, logging, shutil
import tempfile
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
SRC_DIR = os.path.join(Config.getEnv('WORKSPACE'), 'datateamtest', 'hcatalog')
HADOOP_HOME = Config.get('hadoop', 'HADOOP_HOME')
HADOOP_CONF = Config.get('hadoop', 'HADOOP_CONF')
HIVE_HOME = Hive.getHiveHome()
PIG_HOME = HIVE_HOME
HCATALOG_HOME = Config.get('hcatalog', 'HCATALOG_HOME')
HCAT_TEST_DIR = "/user/hrt_qa"
HDFS_TEST_DIR = HCAT_TEST_DIR + "/tests"
HDFS_USER = Config.get('hadoop', 'HDFS_USER')


@TaskReporter.report_test()
def getJobAndAppIds(text):
    '''
    getJobAndAppIds
      text - Text from which to get the application and the job id
  '''
    ids = []
    # pattern to look for is different when tez is enabled.
    if Hive.isTezEnabled():
        # For this method to be backward compatible, we need to check for 2 patterns
        # The following pattern is applicable for pre-champlain releases.
        pattern = 'Status: Running \(application id: (.*)\)'
        for line in re.finditer(pattern, text):
            # with tez we only get the application id
            ids.append({'application': line.group(1)})
        # The following pattern is applicable for champlain and above release.
        if len(ids) == 0:
            pattern = 'Status: Running \(Executing on YARN cluster with App id (.*)\)'
            for line in re.finditer(pattern, text):
                # with tez we only get the application id
                ids.append({'application': line.group(1)})
    else:
        pattern = 'Starting Job = (.*), Tracking URL = h.*://.*:?\d+?/proxy/(.*)/'
        for line in re.finditer(pattern, text):
            ids.append({'job': line.group(1), 'application': line.group(2)})
    return ids


@TaskReporter.report_test()
def getStandaloneHiveJdbcJar():
    hive_lib_dir = os.path.join(Hive.getHiveHome(), 'jdbc')
    jdbc_standalone_jar = util.findMatchingFiles(hive_lib_dir, "hive-jdbc-*-standalone.jar", depth=1)
    return jdbc_standalone_jar[0] if len(jdbc_standalone_jar) > 0 else None


@TaskReporter.report_test()
def getClasspathForJdbcClient():
    jdbc_standalone_jar = getStandaloneHiveJdbcJar()
    assert jdbc_standalone_jar, "No JDBC standalone jar found"
    classpath = [jdbc_standalone_jar]
    hadoop_home = Config.get('hadoop', 'HADOOP_HOME')
    if Hadoop.isHadoop2() and Machine.type() == 'Windows':
        hadoop_home = os.path.join(hadoop_home, "share", "hadoop", "common")
    hadoop_common_jar = util.findMatchingFiles(hadoop_home, "hadoop-common-*[!(tests)].jar", depth=1)
    assert len(hadoop_common_jar) > 0, "No hadoop-common.jar found"
    classpath.append(hadoop_common_jar[0])
    if Hadoop.isSecure():
        hadoop_auth_jar = util.findMatchingFiles(hadoop_home, "hadoop-auth-*[!(tests)].jar", depth=1)
        assert len(hadoop_auth_jar) > 0, "No hadoop-auth.jar found"
        classpath.append(hadoop_auth_jar[0])
    classpath.append(Config.get('hadoop', 'HADOOP_CONF'))
    return (os.pathsep).join(classpath)


@TaskReporter.report_test()
def runJdbcMultiSessionDriver(
        testDir,
        addlClasspath=[],
        connectionUrl=None,
        skippedTests=[],
        addlArgs=[],
        reuseConnections=False,
        testFilter=None,
        logsDir=None,
        queryTimeout=3600
):
    '''
  Run the Hive Jdbc MultiSession Test Driver
  '''
    harnessDir = os.path.join(Config.getEnv('WORKSPACE'), 'datateamtest', 'hive_jdbc_multisession')
    logger.info("Build the TestDriver to run tests")
    exit_code, stdout = Maven.run("clean package", cwd=harnessDir)
    assert exit_code == 0, "Failed to build the test driver"
    classpath = [
        os.path.join(harnessDir, "target", "hive-multisession-test-0.1.jar"),
        Config.get('hadoop', 'HADOOP_CONF')
    ]
    if len(addlClasspath) == 0:
        hiveJdbcDriver = getStandaloneHiveJdbcJar()
        classpath.insert(0, hiveJdbcDriver)
    else:
        classpath = addlClasspath + classpath

    cobert_tool_version = "cobertura-2.1.1"
    COBERTURA_CLASSPTH = os.path.join(
        tempfile.gettempdir(), "coverage-tmp", cobert_tool_version, cobert_tool_version + ".jar"
    )
    if Machine.pathExists(Machine.getAdminUser(), None, COBERTURA_CLASSPTH, Machine.getAdminPasswd()):
        classpath.append(COBERTURA_CLASSPTH)

    args = ["-t " + testDir]
    if connectionUrl is None:
        connectionUrl = Hive.getHiveServer2Url()
    args.append("-c \"%s\"" % connectionUrl)
    if Hadoop.isSecure():
        args.append("-k " + Config.get('machine', 'KEYTAB_FILES_DIR'))
        if Config.hasOption('machine', 'USER_REALM'):
            USER_REALM = Config.get('machine', 'USER_REALM', '')
            args.append("-e USER_REALM=%s" % (USER_REALM))
    args.extend(["--skip %s" % t for t in skippedTests])
    if reuseConnections:
        args.append("--reuseConnections")
    if testFilter:
        args.append("-f " + testFilter)
    from beaver.marker import getMarkerCondition
    markerCondition = getMarkerCondition()
    if markerCondition:
        args.append("-e 'marker=%s'" % markerCondition)
    if not logsDir:
        logsDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "logs_%d" % int(999999 * random.random()))
    args.append("-l " + logsDir)
    if queryTimeout > 0:
        args.append("--queryTimeout %d" % queryTimeout)
    args.extend(addlArgs)
    return Java.runJava(
        Config.getEnv('ARTIFACTS_DIR'),
        "org.apache.hive.jdbc.TestDriver",
        classPath=(os.pathsep).join(classpath),
        cmdArgs=args
    )


@TaskReporter.report_test()
def verifyLogMessageInServiceLog(text, service, timestamp=0, dateTimeFormat=None):
    '''
  Returns true when given log message appears in service log
  '''
    hiveLog = Hive.getServiceLog(service)
    if not hiveLog or not text:
        return None
    hiveHost = Hive.getHiveHost(service)
    destlog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d.log' % int(999999 * random.random()))
    Machine.copyToLocal(None, hiveHost, hiveLog, destlog)
    return util.findMatchingPatternInFileAfterTimestamp(destlog, text, timestamp, dateTimeFormat=dateTimeFormat)


@TaskReporter.report_test()
def setupTestData(stdauth=True):
    data_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "hive-test-data")
    data_tgz = os.path.join(Config.getEnv('WORKSPACE'), "hive-simple-test-data.tgz")
    if not os.path.isfile(data_tgz):
        assert util.downloadUrl(Config.get('hive', 'HIVE_TEST_DATA'), data_tgz)
    Machine.tarExtractAll(data_tgz, data_dir)
    # load data into HDFS
    HDFS.createDirectory("/tmp/hs2data", user=HDFS_USER, perm='777', force=True)
    HDFS.createDirectory("/tmp/hs2data/student", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'studenttab10k'), "/tmp/hs2data/student")
    HDFS.createDirectory("/tmp/hs2data/voter", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'votertab10k'), "/tmp/hs2data/voter")
    query = """drop table if exists student;
create external table student (name string, age int, gpa double) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/student';
drop table if exists voter;
create external table voter (name string, age int, registration string, contributions float) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/voter';"""
    if stdauth:
        query += "\ngrant SELECT, INSERT, UPDATE, DELETE on table student to role public with grant option;"
        query += "\ngrant SELECT, INSERT, UPDATE, DELETE on table voter to role public with grant option;"
        exit_code, stdout, stderr = Hive.runQueryOnBeeline(query, readFromFile=True, logoutput=True)
    assert exit_code == 0, "Test data creation failed"


@TaskReporter.report_test()
def setupHS2ConcurrTestData(stdauth=True):
    # hive.support.concurrency is not in the whitelist, as this is a server setting and not something that user should/can set in a session.
    # In a case of Ranger and SQL std authorization, set hive.support.concurrency to true and restart HS2
    changes = {
        'hive-site.xml': {
            'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
            'hive.support.concurrency': 'true',
            'hive.compactor.initiator.on': 'true',
            'hive.compactor.worker.threads': '3',
            'hive.compactor.check.interval': '10',
            'hive.timedout.txn.reaper.interval': '20s'
        },
        'hiveserver2-site.xml': {
            'hive.compactor.initiator.on': 'false',
            'hive.exec.dynamic.partition.mode': 'nonstrict'
        }
    }
    if not Hive.isHive2():
        changes['hiveserver2-site.xml']['hive.enforce.bucketing'] = 'true'
    else:
        changes['hiveserver2-site.xml']['hive.server2.enable.doAs'] = 'false'
        changes['hiveserver2-site.xml']['hive.txn.manager'] = 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager'
        changes['hiveserver2-site.xml']['hive.support.concurrency'] = 'true'
    Hive.modifyConfig(changes)
    time.sleep(60)
    data_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "hs2concur-test-data")
    data_tgz = os.path.join(Config.getEnv('WORKSPACE'), "hs2concur-test-data.tgz")
    if not os.path.isfile(data_tgz):
        assert util.downloadUrl(Config.get('hive', 'HS2CONCURR_TEST_DATA'), data_tgz)
    Machine.tarExtractAll(data_tgz, data_dir)
    # load data into HDFS
    hdfs_user = Config.get("hadoop", 'HDFS_USER')
    test_user = Config.get("hadoop", 'HADOOPQA_USER')
    HDFS.createDirectory("/tmp/hs2data", user=test_user, perm='777', force=True)
    HDFS.createDirectory("/tmp/hs2data/student", user=test_user, perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'studenttab10k'), "/tmp/hs2data/student")
    HDFS.createDirectory("/tmp/hs2data/voter", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'votertab10k'), "/tmp/hs2data/voter")
    HDFS.createDirectory("/tmp/hs2data/customer_address", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'customer_address10k'), "/tmp/hs2data/customer_address")
    query = """drop table if exists student;
create external table student (name string, age int, gpa double) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/student';
drop table if exists voter;
create external table voter (name string, age int, registration string, contributions float) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/voter';
drop table if exists customer_address;
create external table customer_address (ca_address_sk int, ca_address_id string, ca_street_number string, ca_street_name string, ca_street_type string, ca_suite_number string, ca_city string, ca_county string, ca_state string, ca_zip string, ca_country string, ca_gmt_offset decimal(5,2), ca_location_type string) row format delimited fields terminated by '|' stored as textfile location '/tmp/hs2data/customer_address';
drop table if exists customer_address_partitioned;
create table customer_address_partitioned (ca_address_sk int, ca_address_id string, ca_street_number string, ca_street_name string, ca_street_type string, ca_suite_number string, ca_city string, ca_county string, ca_state string, ca_zip string, ca_country string, ca_gmt_offset decimal(5,2)) partitioned by (ca_location_type string) clustered by (ca_state) into 50 buckets stored as orc tblproperties('transactional'='true');
insert into table customer_address_partitioned partition(ca_location_type) select ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type from customer_address;"""
    if stdauth:
        query += "\ngrant SELECT, INSERT, UPDATE, DELETE on table student to role public with grant option;"
        query += "\ngrant SELECT, INSERT, UPDATE, DELETE on table voter to role public with grant option;"
        query += "\ngrant SELECT, INSERT, UPDATE, DELETE on table customer_address_partitioned to role public with grant option;"
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(query, readFromFile=True, logoutput=True)
    assert exit_code == 0, "Test data creation failed"


@TaskReporter.report_test()
def grantPrivilegesToUsersOnTable(users, tableName, privilege="all"):
    query = ""
    for user in users:
        query += "grant %s on table %s to user %s with grant option;\n" % (privilege, tableName, user)
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(query, readFromFile=True, logoutput=True)
    assert exit_code == 0, "Failed to grant privilege [%s] on table [%s] to users [%s]" % (
        privilege, tableName, ",".join(users)
    )


@TaskReporter.report_test()
def startLLAPWithChaosMonkey(interval='300'):
    hive_changes = {'tez-site.xml': {'tez.am.task.max.failed.attempts': '0'}}
    Hive.modifyConfig(hive_changes, services=['hiveserver2'])

    AMBARI_AGENT_TMP_DIR = '/var/lib/ambari-agent/tmp'
    ARTIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')
    LLAP_START_USER = Config.get('hive', 'HIVE_USER')
    dirs = [
        name for name in os.listdir(AMBARI_AGENT_TMP_DIR) if os.path.isdir(os.path.join(AMBARI_AGENT_TMP_DIR, name))
    ]

    llap_dirs = []
    for dir in dirs:
        if dir.startswith('llap-slider'): llap_dirs.append(dir)

    if len(llap_dirs) < 1:
        logger.info("Could not find llap dir under %s" % AMBARI_AGENT_TMP_DIR)
        Hive.startService(services=['hiveserver2'])
    else:
        llap_dir = llap_dirs[-1]

        resourceConfig = os.path.join(AMBARI_AGENT_TMP_DIR, llap_dir, 'resources.json')
        tmpResourceConfig = os.path.join(ARTIFACTS_DIR, 'resources.json')
        propertyMap = [(["components", "LLAP"], {"yarn.container.failure.threshold": "1000"})]
        util.writePropertiesToConfigJSONFileMulti(resourceConfig, tmpResourceConfig, propertyMap)
        Machine.copy(tmpResourceConfig, resourceConfig, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

        appConfig = os.path.join(AMBARI_AGENT_TMP_DIR, llap_dir, 'appConfig.json')
        tmpAppConfig = os.path.join(ARTIFACTS_DIR, 'appConfig.json')
        propertyMap = [
            (
                ["global"], {
                    "internal.chaos.monkey.probability.containerfailure": "10000",
                    "internal.chaos.monkey.interval.seconds": interval,
                    "internal.chaos.monkey.enabled": "True"
                }
            )
        ]
        util.writePropertiesToConfigJSONFileMulti(appConfig, tmpAppConfig, propertyMap)
        Machine.copy(tmpAppConfig, appConfig, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

        llapShellScript = os.path.join(AMBARI_AGENT_TMP_DIR, llap_dir, 'run.sh')
        exit_code, stdout = Machine.runas(LLAP_START_USER, llapShellScript)
        if exit_code != 0: logger.info("LLAP Shell Script failed to run successfully with %d" % exit_code)

        for i in range(10):
            time.sleep(30)
            logger.info("@%d: Check if LLAP cluster is successfully deployed" % i)
            exit_code, stdout = Machine.runas(LLAP_START_USER, 'slider status llap0')
            if exit_code == 0:
                break
            elif i == 9:
                logger.info("LLAP cluster failed to deploy")


@TaskReporter.report_test()
def getLLAPDaemonPidsHosts():
    hosts = []
    llapdaemon_pids = []
    if not Machine.isHumboldt(): nodes = Hadoop.getAllNodes()
    else: nodes = HDFS.getDatanodes()
    for node in nodes:
        pids = Machine.getProcessListRemote(
            node, format="%U %p %P %a", filter="org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon", logoutput=True
        )
        if pids:
            if Hadoop.isSecure():
                pid = Machine.getPidFromString(pids[0], Config.get('hive', 'HIVE_USER'))
            else:
                pid = Machine.getPidFromString(pids[0], Config.get('hadoop', 'YARN_USER'))
            llapdaemon_pids.append(pid)
            hosts.append(node)
    return llapdaemon_pids, hosts


@TaskReporter.report_test()
def setupMondrianDataset():
    DATABASE_NAME = 'foodmart'
    LOCAL_DATA_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), DATABASE_NAME)
    FOODMART_DDL = os.path.join(LOCAL_DATA_DIR, "foodmart.ddl")
    HADOOPQA_USER = Config.get("hadoop", 'HADOOPQA_USER')

    logger.info("Setup Mondrian dataset")
    if not os.path.exists(LOCAL_DATA_DIR):
        MONDRIAN_DATA_TGZ = LOCAL_DATA_DIR + ".tgz"
        assert util.downloadUrl(Config.get('hive', 'MONDRIAN_DATASET'), MONDRIAN_DATA_TGZ)
        Machine.tarExtractAll(MONDRIAN_DATA_TGZ, Config.getEnv('ARTIFACTS_DIR'))
        assert os.path.isdir(LOCAL_DATA_DIR)

    logger.info("create foodmart database and tables")
    HDFS.createDirectory("/tmp/mondrian", HADOOPQA_USER, perm='777', force=True)
    HDFS.copyFromLocal(LOCAL_DATA_DIR, "/tmp/mondrian", HADOOPQA_USER)
    HDFS.chmod(None, 777, "/tmp/mondrian", recursive=True)
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        FOODMART_DDL,
        hivevar={
            'DB': 'foodmart',
            'LOCATION': '/tmp/mondrian/foodmart'
        },
        logoutput=True,
        queryIsFile=True
    )
    assert exit_code == 0, "Unable to deploy foodmart dataset"


@TaskReporter.report_test()
def ReadFromFile(qfileName):
    qfile = open(qfileName, 'r')
    return qfile.read()


@TaskReporter.report_test()
def setupTableauDataset():
    LOCAL_DATA_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "tableau")
    DATA_DIR = os.path.join(LOCAL_DATA_DIR, 'data')
    SCHEMA_SQL_DIR = os.path.join(LOCAL_DATA_DIR, 'schema_3.0')
    HIVE_TABLES = [
        'Batters', 'Calcs', 'DateBins', 'DateTime', 'Election', 'FischerIris', 'Loan', 'NumericBins', 'REI',
        'SeattleCrime', 'Securities', 'SpecialData', 'Staples', 'Starbucks', 'UTStarcom', 'xy'
    ]
    TABLEAU_TEST_DIR = "/user/hrt_qa/tableau"
    DATABASE_NAME = 'tableau'

    logger.info("Setup Tableau dataset")

    if not os.path.exists(LOCAL_DATA_DIR):
        TABLEAU_DATA_TGZ = LOCAL_DATA_DIR + ".tgz"
        assert util.downloadUrl(Config.get('hive', 'TABLEAU_DATASET'), TABLEAU_DATA_TGZ)
        Machine.tarExtractAll(TABLEAU_DATA_TGZ, Config.getEnv('ARTIFACTS_DIR'))
        assert os.path.isdir(LOCAL_DATA_DIR)

    logger.info("create test directory on hdfs to store tableau data files")
    HDFS.createDirectory(TABLEAU_TEST_DIR, user=HDFS_USER, perm='777', force=True)

    logger.info("create tableau database before creating tables")
    Hive.runQueryOnBeeline("DROP DATABASE IF EXISTS %s" % DATABASE_NAME)
    Hive.runQueryOnBeeline("CREATE DATABASE IF NOT EXISTS %s" % DATABASE_NAME)

    for tbl in HIVE_TABLES:
        hdfsDir = TABLEAU_TEST_DIR + '/%s' % tbl
        hdfsFile = hdfsDir + '/%s' % tbl
        localFile = os.path.join(DATA_DIR, '%s.tbl' % tbl)
        sqlFile = os.path.join(SCHEMA_SQL_DIR, '%s.sql' % tbl)

        logger.info("create directory for %s table" % tbl)
        exit_code, stdout = HDFS.createDirectory(hdfsDir, perm='777', force=True)
        assert exit_code == 0, 'Could not create dir for table %s on hdfs.' % tbl

        logger.info("copy file for table %s to hdfs" % tbl)
        exit_code, stdout = HDFS.copyFromLocal(localFile, hdfsFile)
        assert exit_code == 0, 'Could not copy file for table %s to hdfs.' % tbl

        logger.info("create %s table " % tbl)
        # thing-to-do Modify Hive.runQueryonBeeline to accept query file name
        exit_code, stdout, stderr = Hive.runQueryOnBeeline(
            ReadFromFile(sqlFile), readFromFile=True, hivevar={'HDFS_LOCATION': hdfsDir}, logoutput=True
        )
        assert exit_code == 0, '%s table creation failed' % tbl


@TaskReporter.report_test()
def setupSchemaEvolutionDataset():
    logger.info("Setup Schema Evolution dataset")
    HDFS.createDirectory(HCAT_TEST_DIR, user=HDFS_USER, perm='777', force=True)
    HDFS.createDirectory(HDFS_TEST_DIR, user=HDFS_USER, perm='777', force=True)

    HIVE_TEST_CMD = "-Dhive.use.beeline=true -Dhadoop.home=%s -Dhive.home=%s -Dhcat.home=%s -Dpig.home=%s -Dhbase.home=%s" % (
        HADOOP_HOME, HIVE_HOME, HCATALOG_HOME, PIG_HOME, HIVE_HOME
    )
    if Hadoop.isHadoop2():
        HIVE_TEST_CMD += " -Dmapred.home=%s -Dhadoop.conf.dir=%s" % (Config.get('hadoop', 'MAPRED_HOME'), HADOOP_CONF)
    hiveServer2Url = str(Hive.getHiveServer2Url())
    exit_code, stdout = Ant.run(
        HIVE_TEST_CMD + " deploy-schemaevolution", cwd=SRC_DIR, env={"HIVE_SERVER2_URL": hiveServer2Url}
    )
    assert exit_code == 0


@TaskReporter.report_test()
def downloadDataset(dataDir, dataTgz, downloadUrl, hdfsLocalCopy, textDataDir):
    HDFS.createDirectory(HCAT_TEST_DIR, user=HDFS_USER, perm='777', force=True)
    HDFS.createDirectory(HDFS_TEST_DIR, user=HDFS_USER, perm='777', force=True)

    # change timezone on test machines
    Machine.resetTimeZoneOnCluster()

    # Download the TPCDS dataset if not there
    if not os.path.isfile(dataTgz):
        assert util.downloadUrl(downloadUrl, dataTgz)
        Machine.tarExtractAll(dataTgz, dataDir)

    os.makedirs(hdfsLocalCopy)
    for filename in os.listdir(textDataDir):
        hdfs_localcopy_table_dir = os.path.join(hdfsLocalCopy, filename[:-4])
        os.mkdir(hdfs_localcopy_table_dir)
        shutil.copy(os.path.join(textDataDir, filename), hdfs_localcopy_table_dir)
    HDFS.copyFromLocal(hdfsLocalCopy, HDFS_TEST_DIR)
    HDFS.chmod(None, '777', HDFS_TEST_DIR, recursive=True)


@TaskReporter.report_test()
def setupTPCDSDataset():
    tpcds_data_dir = os.path.join(SRC_DIR, "data", "tpcds")
    TPCDS_DATA_TGZ = os.path.join(tpcds_data_dir, "tpcds_data.tgz")
    hdfs_localcopy_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'data')
    tpcds_text_data_dir = os.path.join(tpcds_data_dir, 'data')

    downloadDataset(
        tpcds_data_dir, TPCDS_DATA_TGZ, Config.get('hive', 'TPCDS_DNLD_URL_HDP3'), hdfs_localcopy_dir,
        tpcds_text_data_dir
    )

    HIVE_TEST_CMD = "-Dhive.use.beeline=true -Dhadoop.home=%s -Dhive.home=%s -Dhcat.home=%s -Dpig.home=%s -Dhbase.home=%s" % (
        HADOOP_HOME, HIVE_HOME, HCATALOG_HOME, PIG_HOME, HIVE_HOME
    )

    if Hadoop.isHadoop2():
        HIVE_TEST_CMD += " -Dmapred.home=%s -Dhadoop.conf.dir=%s" % (Config.get('hadoop', 'MAPRED_HOME'), HADOOP_CONF)

    if Machine.type() == 'Windows':
        HIVE_TEST_CMD += ' -Dharness.conf=conf\windows.conf'

    hiveServer2Url = str(Hive.getHiveServer2Url())

    # generate data
    exit_code, stdout = Ant.run(
        HIVE_TEST_CMD + " deploy-tpcds-orc", cwd=SRC_DIR, env={"HIVE_SERVER2_URL": hiveServer2Url}
    )
    assert exit_code == 0

    exit_code, stdout = Ant.run(HIVE_TEST_CMD + " deploy-tpcds", cwd=SRC_DIR, env={"HIVE_SERVER2_URL": hiveServer2Url})
    assert exit_code == 0

    exit_code, stdout = Ant.run(
        HIVE_TEST_CMD + " deploy-tpcds-parquet", cwd=SRC_DIR, env={"HIVE_SERVER2_URL": hiveServer2Url}
    )
    assert exit_code == 0


@TaskReporter.report_test()
def setupTPCDSOriginalDataset(CURR_DIR):
    tpcds_data_dir = os.path.join(SRC_DIR, "data", "tpcds")
    TPCDS_DATA_TGZ = os.path.join(tpcds_data_dir, "tpcds_original.tgz")
    hdfs_localcopy_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tpcds_original', 'data')
    tpcds_text_data_dir = os.path.join(tpcds_data_dir, 'data')

    downloadDataset(
        tpcds_data_dir, TPCDS_DATA_TGZ, Config.get('hive', 'TPCDS_ORIGINAL_DNLD_URL'), hdfs_localcopy_dir,
        tpcds_text_data_dir
    )

    HIVE_TEST_CMD = "-Dhive.use.beeline=true -Dhadoop.home=%s -Dhive.home=%s -Dhcat.home=%s -Dpig.home=%s -Dhbase.home=%s" % (
        HADOOP_HOME, HIVE_HOME, HCATALOG_HOME, PIG_HOME, HIVE_HOME
    )

    if Hadoop.isHadoop2():
        HIVE_TEST_CMD += " -Dmapred.home=%s -Dhadoop.conf.dir=%s" % (Config.get('hadoop', 'MAPRED_HOME'), HADOOP_CONF)

    if Machine.type() == 'Windows':
        HIVE_TEST_CMD += ' -Dharness.conf=conf\windows.conf'

    query_file_1 = os.path.join(CURR_DIR, 'ddl_queries', 'alltables_text.sql')
    query_file_2 = os.path.join(CURR_DIR, 'ddl_queries', 'alltables_orc.sql')
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        query_file_1,
        hivevar={
            'LOCATION': HDFS_TEST_DIR + '/data',
            'DB': 'tpcds_src'
        },
        cwd=CURR_DIR,
        logoutput=True,
        queryIsFile=True
    )
    logger.info("Check if populating the data in Hive for text tables is successful")
    assert exit_code == 0, "Failed to populate the data in Hive"
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        query_file_2, hivevar={
            'FILE': 'ORC',
            'SOURCE': 'tpcds_src'
        }, cwd=CURR_DIR, logoutput=True, queryIsFile=True
    )
    logger.info("Check if populating the data in Hive for ORC tables is successful")
    assert exit_code == 0, "Failed to populate the data in Hive"


@TaskReporter.report_test()
def setupAcidDataset(testsuite, LOCAL_DIR):
    ddl_location = None
    if testsuite == 'acid':
        ddl_location = os.path.join(LOCAL_DIR, "ddl", "acid-tpch-tablesetup.sql")
    elif testsuite == 'unbucketed':
        ddl_location = os.path.join(LOCAL_DIR, "ddl", "acid-tpch-unbucketed-tablesetup.sql")
    else:
        assert 1 == 0, "The testsuite passed in not correct. Please use value 'acid' or 'unbuckted'"
    # change timezone on test machines
    Machine.resetTimeZoneOnCluster()

    # Download TPCH acids data
    tpch_newdata_dir = os.path.join(LOCAL_DIR, "tpch_newdata_5G")
    TPCH_STAGE_TGZ = os.path.join(LOCAL_DIR, "tpch_newdata_5G.tgz")
    if not os.path.isfile(TPCH_STAGE_TGZ):
        assert util.downloadUrl(Config.get('hive', 'TPCH_NEWDATA_5G_DNLD_URL'), TPCH_STAGE_TGZ)
        Machine.tarExtractAll(TPCH_STAGE_TGZ, LOCAL_DIR)

    # Load the acid tables in Hive
    HADOOPQA_USER = Config.get("hadoop", 'HADOOPQA_USER')
    HDFS.createDirectory("/tmp/lineitem_acid", user=HADOOPQA_USER, perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(tpch_newdata_dir, "lineitem*"), "/tmp/lineitem_acid", HADOOPQA_USER)
    HDFS.chmod(None, 777, "/tmp/lineitem_acid", recursive=True)
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        ddl_location, hivevar={'HDFS_LOCATION': '/tmp'}, logoutput=True, queryIsFile=True
    )
    assert exit_code == 0, "Failed to populate the TPCH acid data in Hive"


@TaskReporter.report_test()
def setupHS2ConcurrencyDataset():
    logger.info("Setup test data")
    data_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "hs2concur-test-data")
    data_tgz = os.path.join(Config.getEnv('WORKSPACE'), "hs2concur-test-data.tgz")
    if not os.path.isfile(data_tgz):
        assert util.downloadUrl(Config.get('hive', 'HS2CONCURR_TEST_DATA'), data_tgz)
    Machine.tarExtractAll(data_tgz, data_dir)
    # load data into HDFS
    hdfs_user = Config.get("hadoop", 'HDFS_USER')
    HDFS.createDirectory("/tmp/hs2data", user=hdfs_user, perm='777', force=True)
    HDFS.createDirectory("/tmp/hs2data/student", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'studenttab10k'), "/tmp/hs2data/student")
    HDFS.createDirectory("/tmp/hs2data/voter", perm='777', force=True)
    HDFS.copyFromLocal(os.path.join(data_dir, 'votertab10k'), "/tmp/hs2data/voter")
    query = """drop table if exists student_txt;
        create external table student_txt (name string, age int, gpa double) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/student';
        drop table if exists voter_txt;
        create external table voter_txt (name string, age int, registration string, contributions float) row format delimited fields terminated by '\\t' stored as textfile location '/tmp/hs2data/voter';
        drop table if exists student;
        create table student (name string, age int, gpa double) CLUSTERED BY (name) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');
        drop table if exists voter;
        create table voter (name string, age int, registration string, contributions float) CLUSTERED BY (name) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');
        Insert into table student select * from student_txt;
        Insert into table voter select * from voter_txt;"""

    exit_code, stdout, stderr = Hive.runQueryOnBeeline(query, readFromFile=True, logoutput=True)
    assert exit_code == 0, "Test data creation failed"


@TaskReporter.report_test()
def setupMergeScaleDataset(LOCAL_DIR):
    # change timezone on test machines
    Machine.resetTimeZoneOnCluster()

    # Download the TPCH dataset if not there
    tpch_data_dir = os.path.join(LOCAL_DIR, "data")
    TPCH_DATA_TGZ = os.path.join(LOCAL_DIR, "tpch_data.tgz")
    if not os.path.isfile(TPCH_DATA_TGZ):
        assert util.downloadUrl(Config.get('hive', 'TPCH_DNLD_URL'), TPCH_DATA_TGZ)
        Machine.tarExtractAll(TPCH_DATA_TGZ, LOCAL_DIR)

    # Load the tables in Hive
    HADOOPQA_USER = Config.get("hadoop", 'HADOOPQA_USER')
    HDFS.createDirectory("/tmp/tpch", user=HADOOPQA_USER, perm='777', force=True)
    HDFS.copyFromLocal(tpch_data_dir, "/tmp/tpch", user=HADOOPQA_USER)
    HDFS.chmod(None, 777, "/tmp/tpch", recursive=True)
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        os.path.join(LOCAL_DIR, "ddl", "merge-tpch-tablesetup.sql"),
        hivevar={'HDFS_LOCATION': '/tmp/tpch/data'},
        logoutput=True,
        queryIsFile=True
    )
    assert exit_code == 0, "Failed to populate the TPCH data in Hive"

    # Download TPCH staging data
    tpch_stage_dir = os.path.join(LOCAL_DIR, "tpch_newdata_5G")
    TPCH_STAGE_TGZ = os.path.join(LOCAL_DIR, "tpch_newdata_5G.tgz")
    if not os.path.isfile(TPCH_STAGE_TGZ):
        assert util.downloadUrl(Config.get('hive', 'TPCH_NEWDATA_5G_DNLD_URL'), TPCH_STAGE_TGZ)
        Machine.tarExtractAll(TPCH_STAGE_TGZ, LOCAL_DIR)

    # Load the staged tables in Hive
    HDFS.createDirectory(
        "/tmp/lineitem_stage /tmp/orders_stage /tmp/delete_stage", user=HADOOPQA_USER, perm='777', force=True
    )
    HDFS.copyFromLocal(os.path.join(tpch_stage_dir, "lineitem*"), "/tmp/lineitem_stage", HADOOPQA_USER)
    HDFS.copyFromLocal(os.path.join(tpch_stage_dir, "order*"), "/tmp/orders_stage", HADOOPQA_USER)
    HDFS.copyFromLocal(os.path.join(tpch_stage_dir, "delete*"), "/tmp/delete_stage", HADOOPQA_USER)
    HDFS.chmod(None, 777, "/tmp/lineitem_stage /tmp/orders_stage /tmp/delete_stage", recursive=True)
    exit_code, stdout, stderr = Hive.runQueryOnBeeline(
        os.path.join(LOCAL_DIR, "ddl", "merge-staged-tpch-tablesetup.sql"),
        hivevar={'HDFS_LOCATION': '/tmp'},
        logoutput=True,
        queryIsFile=True
    )
    assert exit_code == 0, "Failed to populate the TPCH staging data in Hive"
