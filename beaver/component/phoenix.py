#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os
import random
import threading
import time
import logging
import re
import urllib2
from taskreporter.taskreporter import TaskReporter
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.hbase import HBase
from beaver.component.ambari import Ambari
from beaver import util
from beaver import configUtils
from beaver.component.slider import Slider

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')

ARTIFACTS_DIR = Config.getEnv("ARTIFACTS_DIR")
INPUT_FILE = 'inputFile'
PHOENIX_HOME = Config.get('phoenix', 'PHOENIX_HOME')
SQLLINE_SCRIPT = os.path.join('.', 'sqlline.py')
SQLLINE_THIN_SCRIPT = '/usr/bin/phoenix-sqlline-thin'
PSQL_SCRIPT = os.path.join('.', 'psql.py')

HBASE_LIB_DIR = os.path.join(Config.get('hbase', 'HBASE_HOME'), 'lib')

HBASE_CONF_DIR = HBase.getModifiedConfigPath()
if HBASE_CONF_DIR is None or HBASE_CONF_DIR == '' or not os.path.exists(HBASE_CONF_DIR):
    HBASE_CONF_DIR = Config.get('hbase', 'HBASE_CONF_DIR')

HADOOP_CMD = Config.get('hadoop', 'HADOOP_CMD')
HBASE_USER = None
if Hadoop.isSecure():
    HBASE_USER = Config.get('hbase', 'HBASE_USER')

HOST = Machine.getfqdn()

ZK_HOST = util.getPropertyValueFromConfigXMLFile(
    os.path.join(HBASE_CONF_DIR, 'hbase-site.xml'), "hbase.zookeeper.quorum"
).split(',')[0]
ZK_PORT = util.getPropertyValueFromConfigXMLFile(
    os.path.join(HBASE_CONF_DIR, 'hbase-site.xml'), "hbase.zookeeper.property.clientPort"
)
if os.path.exists(os.path.join(HBASE_CONF_DIR, 'hbase-site.xml')):
    ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
        os.path.join(HBASE_CONF_DIR, 'hbase-site.xml'), "zookeeper.znode.parent"
    )
else:
    ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
        os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase-site.xml'), "zookeeper.znode.parent"
    )

ENVIRONMENT = {}

ENVIRONMENT = {
    'JAVA_HOME': Config.get("machine", "JAVA_HOME"),
    'PATH': os.path.join(Config.get("machine", "JAVA_HOME"), "bin") + os.pathsep + os.environ['PATH']
}

RUN_WITH_SQLLINE_THIN = False
QUERYSERVER_URL = None
QUERYSERVER_PORT = 8765

# Check 6 times for PQS to accept requests, waiting 5 seconds inbetween
PQS_STARTUP_CHECK_ATTEMPTS = 6
PQS_STARTUP_CHECK_SECONDS_PERIOD = 5


class Phoenix(object):
    _phoenixqueryservers = []
    _ambariComponentMapping = {'phoenix_query_server': 'PHOENIX_QUERY_SERVER'}

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True, disableAuth=False):
        if not env:
            env = {}
        # if disable auth is requsted set the env
        # var to empty only for a secure cluster
        if disableAuth and Hadoop.isSecure():
            env['KRB5CCNAME'] = ''
            env = env.items() + ENVIRONMENT.items()
        # if disableAuth is false and we are running a secure cluster get
        # the credentials
        elif Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        return Machine.runas(
            user,
            cmd,
            env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT),
            logoutput=logoutput
        )

    @classmethod
    def runSQLLineCmds(
            cls,
            cmd,
            env=None,
            logoutput=True,
            outputFormat=None,
            threadNumber=None,
            runInBackground=False,
            ignoreFailures=False
    ):
        return cls.runSQLLineCmdsAs(
            user=None,
            cmd=cmd,
            env=env,
            logoutput=logoutput,
            outputFormat=outputFormat,
            threadNumber=threadNumber,
            runInBackground=runInBackground,
            ignoreFailures=ignoreFailures
        )

    @classmethod
    @TaskReporter.report_test()
    def runSQLLineCmdsAs(
            cls,
            user,
            cmd,
            env=None,
            logoutput=True,
            outputFormat=None,
            threadNumber=None,
            runInBackground=False,
            ignoreFailures=False
    ):
        global ZK_ZPARENT
        if Slider.isSlider():
            ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
                os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase-site.xml'), "zookeeper.znode.parent"
            )

        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
                kerbTicket = Machine.getKerberosTicket(user)
                env['KRB5CCNAME'] = kerbTicket
                if 'HBASE_CONF_PATH' not in env.keys():
                    env['HBASE_CONF_PATH'] = Config.get('hbase', 'HBASE_CONF_DIR')
                user = None
        if Slider.isSlider():
            env['HBASE_CONF_PATH'] = Config.get('hbase', 'HBASE_CONF_DIR')

        folder = os.path.join(ARTIFACTS_DIR, 'phoenixSQLQuery')

        if threadNumber is None:
            if not runInBackground:
                sqlConsultFile = os.path.join(folder, INPUT_FILE + '.txt')
            else:
                sqlConsultFile = os.path.join(folder, INPUT_FILE + str(random.randint(0, 100000)) + '.txt')
        else:
            sqlConsultFile = os.path.join(folder, INPUT_FILE + "_thread_" + str(threadNumber) + ".txt")

        # We remove the file if exists
        Machine.rm(user, HOST, sqlConsultFile)

        # We create aux file
        Machine.makedirs(user, HOST, folder)

        # We write the sql command to the file. If we are in ASV, we get the output in csv format
        if outputFormat is not None:
            cmd = '!outputformat %s\n%s' % (outputFormat, cmd)

        if HDFS.isASV() and Machine.isWindows():
            cmd = '!outputformat csv\n' + cmd

        # We write the sql command to the file
        fh = open(sqlConsultFile, 'w')
        fh.write(cmd)
        fh.close()

        # We save the current directory
        currentDirectory = os.getcwd()

        # We change the path to execute sqlline.py from /bin folder
        phoenix_bin = os.path.join(PHOENIX_HOME, 'bin')
        os.chdir(phoenix_bin)

        exportCommand = ""
        if "HBASE_CONF_DIR" in env.keys():
            if Machine.isLinux():
                exportCommand = "export HBASE_CONF_DIR=%s;" % env["HBASE_CONF_DIR"]

        # We run the command to execute SQL instruction
        if RUN_WITH_SQLLINE_THIN:
            queryServer = None
            if QUERYSERVER_URL is None:
                # Since phoenix-QueryServer will be running on all regionservers,
                # we re-direct the query to any currently active regionserver
                phoenixqueryservers = cls.getPhoenixQueryServers()
                assert phoenixqueryservers, "Query failed. No active RS found with cls.getPhoenixQueryServers()"
                queryServer = phoenixqueryservers[0]
            else:
                queryServer = QUERYSERVER_URL

            assert queryServer is not None, "Should have a URL to a PQS instance"
            if not ignoreFailures:
                completeCommand = '%s %s %s %s' % (exportCommand, SQLLINE_THIN_SCRIPT, queryServer, sqlConsultFile)
            else:
                completeCommand = '%s %s %s < %s' % (exportCommand, SQLLINE_THIN_SCRIPT, queryServer, sqlConsultFile)
        else:
            phoenix_sqlline = os.path.join(phoenix_bin, SQLLINE_SCRIPT)
            if ZK_ZPARENT is not None and ZK_ZPARENT != '':
                if not ignoreFailures:
                    completeCommand = '%s %s %s:%s:%s %s' % (exportCommand, phoenix_sqlline, ZK_HOST,
                                                             ZK_PORT, ZK_ZPARENT, sqlConsultFile)
                else:
                    completeCommand = '%s %s %s:%s:%s < %s' % (exportCommand, phoenix_sqlline, ZK_HOST,
                                                               ZK_PORT, ZK_ZPARENT, sqlConsultFile)
            else:
                if not ignoreFailures:
                    completeCommand = '%s %s %s:%s %s' % (exportCommand, phoenix_sqlline,
                                                          ZK_HOST, ZK_PORT, sqlConsultFile)
                else:
                    completeCommand = '%s %s %s:%s < %s' % (exportCommand, phoenix_sqlline,
                                                            ZK_HOST, ZK_PORT, sqlConsultFile)
        if runInBackground:
            result = Machine.runinbackgroundAs(
                user, completeCommand, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
            )
        else:
            result = Machine.runas(
                user,
                completeCommand,
                env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT),
                logoutput=logoutput
            )

        # we restore the previous directory
        os.chdir(currentDirectory)

        # We remove the file
        if not runInBackground:
            Machine.rm(user, HOST, sqlConsultFile)
        return result

    @classmethod
    @TaskReporter.report_test()
    def migrateTable(cls, tableName):
        # We save the current directory
        currentDir = os.getcwd()

        # We go to phoenix / bin folder
        os.chdir(os.path.join(PHOENIX_HOME, 'bin'))
        command = PSQL_SCRIPT

        # We run the command
        command = "%s %s -m %s" % (command, cls.getZKConnectString(), tableName)

        result = Machine.run(command, env=ENVIRONMENT)

        os.chdir(currentDir)

        return result

    @classmethod
    @TaskReporter.report_test()
    def runSQLLineCmdsWithoutEnvVars(
            cls, cmd, env=None, logoutput=True, outputFormat=None, threadNumber=None, user=None
    ):
        global ZK_ZPARENT
        if Slider.isSlider():
            ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
                os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase-site.xml'), "zookeeper.znode.parent"
            )

        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
                kerbTicket = Machine.getKerberosTicket(user)
                env['KRB5CCNAME'] = kerbTicket

                user = None

        folder = os.path.join(ARTIFACTS_DIR, 'phoenixSQLQuery')

        if threadNumber is None:
            sqlConsultFile = os.path.join(folder, INPUT_FILE + '.txt')
        else:
            sqlConsultFile = os.path.join(folder, INPUT_FILE + "_thread_" + str(threadNumber) + ".txt")

        # We remove the file if exists
        Machine.rm(user, HOST, sqlConsultFile)

        # We create aux file
        Machine.makedirs(user, HOST, folder)

        # We write the sql command to the file. If we are in ASV, we get the output in csv format
        if outputFormat is not None:
            cmd = '!outputformat %s\n%s' % (outputFormat, cmd)

        if HDFS.isASV() and Machine.isWindows():
            cmd = '!outputformat csv\n%s' % cmd

        # We write the sql command to the file
        fh = open(sqlConsultFile, 'w')
        fh.write(cmd)
        fh.close()
        logger.info("RUNNING Command in input file: %s", cmd)

        # We save the current directory
        currentDirectory = os.getcwd()

        # We change the path to execute sqlline.py from /bin folder
        os.chdir(os.path.join(PHOENIX_HOME, 'bin'))

        exportCommand = ""

        # We run the command to execute SQL instruction
        if RUN_WITH_SQLLINE_THIN:
            queryServer = None
            if QUERYSERVER_URL is None:
                # Since phoenix-QueryServer will be running on all regionservers, we re-direct the query
                # to any currently active regionserver
                phoenixqueryservers = cls.getPhoenixQueryServers()
                assert phoenixqueryservers, "Query failed. No active RS found with cls.getPhoenixQueryServers()"
                queryServer = phoenixqueryservers[0]
            else:
                queryServer = QUERYSERVER_URL

            assert queryServer is not None, "Should have a URL to a PQS instance"
            completeCommand = '%s %s %s %s' % (exportCommand, SQLLINE_THIN_SCRIPT, queryServer, sqlConsultFile)
        else:
            if ZK_ZPARENT is not None and ZK_ZPARENT != '':
                completeCommand = '%s %s %s:%s:%s %s' % (exportCommand, SQLLINE_SCRIPT, ZK_HOST,
                                                         ZK_PORT, ZK_ZPARENT, sqlConsultFile)
            else:
                completeCommand = '%s %s %s:%s %s' % (exportCommand, SQLLINE_SCRIPT,
                                                      ZK_HOST, ZK_PORT, sqlConsultFile)

        result = Machine.runas(
            user,
            completeCommand,
            env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT),
            logoutput=logoutput
        )

        # we restore the previous directory
        os.chdir(currentDirectory)

        # We remove the file
        Machine.rm(user, HOST, sqlConsultFile)
        return result

    @classmethod
    @TaskReporter.report_test()
    def createUDF(cls, udfName, udfArgs, udfReturnValue, functionClass, jarPath, env=None, logoutput=None):
        """
        Function used to insert UDFs into Phoenix.
        """
        cmd = "CREATE FUNCTION %s(%s) RETURNS %s AS '%s' USING JAR '%s'" % (
            udfName, udfArgs, udfReturnValue, functionClass, jarPath
        )
        result = cls.runSQLLineCmds(cmd, env, logoutput)
        return result

    @classmethod
    def createTable(cls, tableName, primaryKey, columns, env=None):
        return cls.createTableAs(tableName, primaryKey, columns, None, env=env)

    @classmethod
    def dropTable(cls, tableName, env=None):
        command = 'DROP TABLE %s;' % tableName
        return cls.runSQLLineCmds(command, env=env)

    @classmethod
    @TaskReporter.report_test()
    def createTableAs(cls, tableName, primaryKey, columns, user, env=None, logoutput=None):
        command = "CREATE TABLE IF NOT EXISTS %s ( %s %s PRIMARY KEY " % (
            tableName, primaryKey['name'], primaryKey['type']
        )
        for column in columns:
            command += ", %s %s " % (column['name'], column['type'])

        command += ');'
        logger.info("RUNNING: %s", command)
        return cls.runSQLLineCmdsAs(user, command, env, logoutput)

    @classmethod
    def createCompoundIndexTable(cls, indexName, tableName, compoundIndexColumns, env=None):
        return cls.createCompoundIndexTableAs(indexName, tableName, compoundIndexColumns, env=env)

    @classmethod
    @TaskReporter.report_test()
    def createCompoundIndexTableAs(
            cls, indexName, tableName, compoundIndexColumns, user=None, env=None, logoutput=None):
        command = "CREATE INDEX %s ON %s (" % (indexName, tableName)
        for column in compoundIndexColumns:
            command += str(column['name']) + ','
        command = command[:-1]
        command += ');'
        logger.info("  ------------------------------  command: %s", command)
        return cls.runSQLLineCmdsAs(user, command, env, logoutput)

    @classmethod
    @TaskReporter.report_test()
    def insertCSVDataViaPSQL(cls, csvFile, tableName, env=None):
        """
          If tableName==None, the table will be created with the name of the file.
        """
        global ZK_ZPARENT
        if Slider.isSlider():
            ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
                os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase-site.xml'), "zookeeper.znode.parent"
            )

        if not env:
            env = {}
        env['HBASE_CONF_PATH'] = Config.get('hbase', 'HBASE_CONF_DIR')
        # We save the current directory
        currentDir = os.getcwd()

        # We go to phoenix / bin folder
        os.chdir(os.path.join(PHOENIX_HOME, 'bin'))

        # We run the command
        if ZK_ZPARENT is not None and ZK_ZPARENT != '':
            command = "%s -t %s %s:%s:%s %s" % (PSQL_SCRIPT, tableName.upper(), ZK_HOST, ZK_PORT, ZK_ZPARENT, csvFile)
        else:
            command = "%s -t %s %s:%s %s" % (PSQL_SCRIPT, tableName.upper(), ZK_HOST, ZK_PORT, csvFile)

        result = Machine.run(command, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT))

        # We restore the execution directory
        os.chdir(currentDir)

        # we return the results
        return result

    @classmethod
    @TaskReporter.report_test()
    def runSQLqueryThroughPSQL(cls, queryFile, env=None):
        """
          If tableName==None, the table will be created with the name of the file.
        """
        if not env:
            env = {}
        # We save the current directory
        currentDir = os.getcwd()

        # We go to phoenix / bin folder
        os.chdir(os.path.join(PHOENIX_HOME, 'bin'))

        # We run the command
        command = "%s %s:%s %s" % (PSQL_SCRIPT, ZK_HOST, ZK_PORT, queryFile)
        result = Machine.run(command, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT))

        # We restore the execution directory
        os.chdir(currentDir)

        # we return the results
        return result

    @classmethod
    @TaskReporter.report_test()
    def indexAsyncPopulation(cls, tableName, indexTableName, runInBackground=False, env=None, partialBuild=False):
        """
          By default, the files will be allocated under /tmp/ folder in HDFS.
        """

        if Machine.isLinux():
            clientjar = Machine.find(
                user=Machine.getAdminUser(),
                host="localhost",
                filepath=PHOENIX_HOME,
                searchstr="phoenix-*[0-9]-client.jar",
                passwd=Machine.getAdminPasswd()
            )
            clientjar = clientjar[0]
            if partialBuild is True:
                finalCommand = "%s jar %s org.apache.phoenix.mapreduce.index.IndexTool --data-table %s " \
                               "--partial-rebuild --output-path=/tmp" % (HADOOP_CMD, clientjar, tableName)
            else:
                finalCommand = "%s jar %s org.apache.phoenix.mapreduce.index.IndexTool --data-table %s " \
                               "--index-table %s --output-path=/tmp" % (HADOOP_CMD, clientjar,
                                                                        tableName, indexTableName)
            # delimiter options
            # if delimiter != "," or arrayDelimiter != None:
            # finalCommand = finalCommand + " --delimiter %s --array-delimiter %s"%(delimiter,arrayDelimiter)
            # ZKHosts options
            # finalCommand = finalCommand + " --zookeeper %s"%cls.getZKConnectString()
            finalCommand = "HADOOP_CLASSPATH=%s %s" % (HBASE_CONF_DIR, finalCommand)
        elif Machine.isWindows():
            raise Exception("Windows not supported")

        if runInBackground:
            exit_code = 0
            stdout = ''
            Machine.runinbackground(
                finalCommand, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
            )
        else:
            exit_code, stdout = Machine.run(
                "HADOOP_CLASSPATH=%s %s" % (HBASE_CONF_DIR, finalCommand),
                env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
            )
        return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def insertCSVDataViaCSVBuildLoad(
            cls,
            csvFile,
            tableName,
            putIntoHDFS=True,
            deleteAfterExec=True,
            runInBackground=False,
            user=None,
            config=None,
            optionAndParameter="",
            env=None,
            delimiter=",",
            arrayDelimiter=None,
            schema=None
    ):
        """
          By default, the files will be allocated under /tmp/ folder in HDFS.
        """
        global ZK_ZPARENT
        if Slider.isSlider():
            ZK_ZPARENT = util.getPropertyValueFromConfigXMLFile(
                os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase-site.xml'), "zookeeper.znode.parent"
            )

        if Machine.isLinux():
            clientjar = Machine.find(
                user=Machine.getAdminUser(),
                host="localhost",
                filepath=PHOENIX_HOME,
                searchstr="phoenix-*[0-9]-client.jar",
                passwd=Machine.getAdminPasswd()
            )
        else:
            clientjar = Machine.find(
                user=Machine.getAdminUser(),
                host="localhost",
                filepath=PHOENIX_HOME,
                searchstr="phoenix-*-client.jar",
                passwd=Machine.getAdminPasswd()
            )

        if Machine.isWindows():
            clientjar = (clientjar[0].strip("\\localhost")).replace("$", ":")
            fileName = csvFile.split('\\')[-1]
        else:
            clientjar = clientjar[0]
            fileName = csvFile.split('/')[-1]

        # If we need to, we insert it into HDFS, since the library will take it from there.
        executingUser = (HADOOPQA_USER) if user is None else user

        if putIntoHDFS:
            if not HDFS.fileExists('/tmp/'):
                HDFS.mkdir('/tmp/')
            HDFS.copyFromLocal(csvFile, '/tmp/', executingUser, config, optionAndParameter)

        hbaseConfDir = HBASE_CONF_DIR
        if Slider.isSlider():
            hbaseConfDir = Config.get('hbase', 'HBASE_CONF_DIR')

        classpath = hbaseConfDir

        finalCommand = "%s jar %s org.apache.phoenix.mapreduce.CsvBulkLoadTool --table %s  --input %s" \
                       % (HADOOP_CMD, clientjar, tableName, '/tmp/%s' % fileName)

        if schema is not None:
            finalCommand = '%s -schema %s' % (finalCommand, schema)

        if Machine.isWindows():
            os.environ['HADOOP_USER_CLASSPATH_FIRST'] = 'true'
            os.environ['HADOOP_CLASSPATH'] = classpath
            if delimiter != "," or arrayDelimiter != None:
                finalCommand = "%s -d `\\\"`%s`\\\" -a `\\\"`%s`\\\"" \
                               % (finalCommand, delimiter, arrayDelimiter.strip("'"))
            finalCommand = "%s --zookeeper %s" % (finalCommand, ZK_HOST)
            if runInBackground:
                exit_code = 0
                stdout = ''
                Machine.runinbackground(
                    finalCommand, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
                )
            else:
                exit_code, stdout = Machine.run(
                    finalCommand, env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
                )
        else:
            # delimiter options
            if delimiter != "," or arrayDelimiter != None:
                finalCommand = "%s --delimiter %s --array-delimiter %s" % (
                    finalCommand, delimiter, arrayDelimiter)
            # ZKHosts options
            finalCommand = "%s --zookeeper %s" % (finalCommand, cls.getZKConnectString())
            if runInBackground:
                exit_code = 0
                stdout = ''
                Machine.runinbackground(
                    "HADOOP_CLASSPATH=%s %s" % (classpath, finalCommand),
                    env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
                )
            else:
                exit_code, stdout = Machine.run(
                    "HADOOP_CLASSPATH=%s %s" % (classpath, finalCommand),
                    env=dict(env.items() + ENVIRONMENT.items() if env is not None else ENVIRONMENT)
                )

        # If selected, after insertion into HBase we will delete the csvFile from HDFS
        if deleteAfterExec and not runInBackground:
            # Does not work for "run in background" option
            HDFS.deleteFile('/tmp/%s' % fileName, executingUser)
        # return 0,""
        return exit_code, stdout

    @classmethod
    def disableIndex(cls, indexName, fromTableName, env=None):
        cmd = 'alter index if exists %s on %s DISABLE' % (indexName, fromTableName)
        return Phoenix.runSQLLineCmds(cmd, env=env)

    @classmethod
    def enableIndex(cls, indexName, fromTableName, env=None):
        cmd = 'alter index if exists %s on %s REBUILD' % (indexName, fromTableName)
        return Phoenix.runSQLLineCmds(cmd, env=env)

    @classmethod
    @TaskReporter.report_test()
    def createLocalCSVDataFile(cls, numMB, filePath, fileName):
        """
         File will be created in test_IndexTable folder
         Max size = 4GB
        """
        cities = ['San Jose', 'San Francisco', 'Santa Clara']
        if numMB > 1024 * 4:
            logger.info("--- Creating CSV data file of 4096 MB (max limit for this function), "
                        "instead of requested %s MB file ---", numMB)
            numMB = 1024 * 4
        # if the file or the path does not exist, we create it
        if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), filePath, Machine.getAdminPasswd()):
            Machine.makedirs(Machine.getAdminUser(), Machine.getfqdn(), filePath, Machine.getAdminPasswd())

        if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                os.getcwd(), filePath, fileName), Machine.getAdminPasswd()):
            Machine.create_file(numMB, os.path.join(os.getcwd(), filePath), fileName)
            Machine.chmod(
                '777',
                os.path.join(filePath, fileName),
                user=Machine.getAdminUser(),
                host=Machine.getfqdn(),
                passwd=Machine.getAdminPasswd()
            )

        # We insert the data.
        insertFile = open(os.path.join(os.getcwd(), filePath, fileName), 'w')
        for j in range(numMB):
            query = ''
            for i in range(1, 11000):
                # 1MB more or less equals 10999 rows of these records.
                randInt = str(i + (j * 10999))
                firstName = 'first_name%s' % randInt
                secondName = 'last_name%s' % randInt
                city = random.choice(cities)
                query += "%s,%s,%s,%s\n" % (str(randInt), firstName, secondName, city)
            insertFile.write(query)
        insertFile.close()

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        jarDir = Config.get("phoenix", "PHOENIX_HOME")
        files = util.findMatchingFiles(jarDir, "phoenix-*-thin-client.jar")
        jarFileName = files[0].split(os.path.sep)[-1]

        p = re.compile(r'phoenix-(\S+)-thin-client.jar')
        m = p.search(jarFileName)
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    @TaskReporter.report_test()
    def grantPermissionsToSystemTables(cls, schemaFunctionalityEnabled=False):
        if schemaFunctionalityEnabled:
            GRANT_CMDS = ["grant '%s','RWXCA'" % HADOOPQA_USER]
        else:
            GRANT_CMDS = [
                "grant '%s','RWXCA', 'SYSTEM.CATALOG'" % HADOOPQA_USER,
                "grant '%s','RWXCA', 'SYSTEM.STATS'" % HADOOPQA_USER,
                "grant '%s','RWXCA', 'SYSTEM.SEQUENCE'" % HADOOPQA_USER,
                "grant '%s','RWXCA', 'SYSTEM.FUNCTION'" % HADOOPQA_USER
            ]
        return HBase.runShellCmds(GRANT_CMDS, user=HBASE_USER)

    @classmethod
    def grantPermissionsToSystemNamespace(cls):
        GRANT_CMDS = ["grant '%s','RWXCA', '@SYSTEM'" % HADOOPQA_USER]
        return HBase.runShellCmds(GRANT_CMDS, user=HBASE_USER)

    @classmethod
    @TaskReporter.report_test()
    def revokePermissionsToSystemTables(cls, schemaFunctionalityEnabled=False):
        if schemaFunctionalityEnabled:
            REVOKE_CMDS = ["revoke '%s', '@SYSTEM'" % HADOOPQA_USER]
        else:
            REVOKE_CMDS = [
                "revoke '%s', 'SYSTEM.CATALOG'" % HADOOPQA_USER,
                "revoke '%s', 'SYSTEM.STATS'" % HADOOPQA_USER,
                "revoke '%s', 'SYSTEM.SEQUENCE'" % HADOOPQA_USER,
                "revoke '%s', 'SYSTEM.FUNCTION'" % HADOOPQA_USER
            ]
        return HBase.runShellCmds(REVOKE_CMDS, user=HBASE_USER)

    @classmethod
    @TaskReporter.report_test()
    def runWithSqllineThin(cls, booleanOption):
        """
          We use this method to modify RUN_WITH_SQLLINE_THIN variable
        """
        global RUN_WITH_SQLLINE_THIN
        RUN_WITH_SQLLINE_THIN = booleanOption

    @classmethod
    @TaskReporter.report_test()
    def getPhoenixQueryServers(cls, host=None, reRunStatus=False):
        if host:
            phoenixqueryservers = Ambari.getHostsForComponent(
                cls._ambariComponentMapping.get('phoenix_query_server'), weburl=Ambari.getWebUrl(hostname=host)
            )
            phoenixqueryservers = [pqs.encode('ascii') for pqs in phoenixqueryservers]
            logger.info("---------- phoenixqueryservers: %s", phoenixqueryservers)
            return phoenixqueryservers
        if (not cls._phoenixqueryservers) or reRunStatus:
            cls._phoenixqueryservers = Ambari.getHostsForComponent(
                cls._ambariComponentMapping.get('phoenix_query_server')
            )
            cls._phoenixqueryservers = [pqs.encode('ascii') for pqs in cls._phoenixqueryservers]
            logger.info("---------- cls._phoenixqueryservers: %s", cls._phoenixqueryservers)
        return cls._phoenixqueryservers

    @classmethod
    @TaskReporter.report_test()
    def setQueryServerUrl(cls, queryServer):
        """
        Provide a URL to the QueryServer endpoint for sqlline-thin.py to use
        """
        assert queryServer is not None, 'Must provide a non-None queryserver URL'
        global QUERYSERVER_URL
        QUERYSERVER_URL = queryServer

    @classmethod
    @TaskReporter.report_test()
    def queryserver(cls, user, host, action, config=None, homePath=None, binFolder='bin'):
        if Machine.type() == 'Windows':
            return Machine.service("queryserver", action, host=host)
        else:
            if homePath is None:
                homePath = Config.get('phoenix', 'PHOENIX_HOME')
            cmd = os.path.join(homePath, binFolder, 'queryserver.py')
            env = None
            if config:
                env = {}
                env['HBASE_CONF_DIR'] = config
            cmd = "%s %s" % (cmd, action)
            exit_code, stdout = Machine.runas(user, cmd, host=host, env=env)
            # The command exited abnormally, don't run any follow-on checks
            if exit_code != 0:
                logger.warn("Failed to execute queryserver command. %d, %s", exit_code, stdout)
                return exit_code, stdout
            # Check if PQS is accepting HTTP request before returning
            if action == 'start':
                logger.info('Verifying that PQS is running')
                if Phoenix.verify_pqs_running(host, QUERYSERVER_PORT):
                    return exit_code, stdout
                # Failed to verify PQS is running, bail out.
                raise Exception('Failed to connect to PQS on %s' % host)
            return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def verify_pqs_running(cls, host, port):
        """
        Verifies that the Phoenix Query Server running on the given host and port
        is accepting HTTP requests.
        Returns True when PQS is verified to be running, False otherwise.
        """
        url = 'http://%s:%d/' % (host, port)
        for _ in xrange(PQS_STARTUP_CHECK_ATTEMPTS):
            time.sleep(PQS_STARTUP_CHECK_SECONDS_PERIOD)
            try:
                urllib2.urlopen(url).getcode()
                # Success, the server is up
                return True
            except urllib2.HTTPError as http_error:
                # PQS doesn't have any health/status endpoint yet, but we know
                # it will return HTTP/404 on / when it's running.
                # For secure clusters, we're providing no credentials, so we
                # would get a not-authenticated HTTP/401. This also tells
                # us that the server is up and running.
                if http_error.code == 404 or http_error.code == 401:
                    return True
                else:
                    logger.warn('Failed to issue request to PQS on %s, will retry. %s', url, http_error)
            except urllib2.URLError as url_error:
                logger.warn('Failed to connect to PQS on %s, will retry. %s', url, url_error)
        return False

    @classmethod
    def isSqlLineThinEnabled(cls):
        """
          We use this method to modify RUN_WITH_SQLLINE_THIN variable
        """
        return RUN_WITH_SQLLINE_THIN

    @classmethod
    @TaskReporter.report_test()
    def runMultipleSQLQueries(cls, queries, ignoreFailures=True):
        if queries is not None and queries:
            sqlQueriesFilePath = os.path.join(ARTIFACTS_DIR, 'sqlQueriesFile_%s.txt' % str(random.randint(0, 100000)))
            sqlQueriesFile = open(sqlQueriesFilePath, 'w')
            try:
                for query in queries:
                    if query[-1] != ';':
                        query += ';'
                    sqlQueriesFile.write(query + '\n')

                sqlQueriesFile.close()
                Phoenix.runSQLLineFile(sqlQueriesFilePath, ignoreFailures=ignoreFailures)
            except Exception:
                pass
            Machine.rm(Machine.getAdminUser(), None, sqlQueriesFilePath, passwd=Machine.getAdminPasswd())

    @classmethod
    def runSQLLineFile(
            cls,
            sqlFile,
            env=None,
            logoutput=True,
            outputFormat=None,
            threadNumber=None,
            runInBackground=False,
            ignoreFailures=True
    ):
        # We read the sql file content and we pass it as an sqlQuery
        query = open(sqlFile, 'r')
        return cls.runSQLLineCmds(
            query.read(),
            env,
            logoutput,
            outputFormat,
            threadNumber=threadNumber,
            runInBackground=runInBackground,
            ignoreFailures=ignoreFailures
        )

    @classmethod
    def getZKConnectString(cls):
        return "%s:%s:%s" % (ZK_HOST, ZK_PORT, ZK_ZPARENT)

    @classmethod
    @TaskReporter.report_test()
    def getTableRegions(cls, table, onlineOnly=False):
        # We locate all the regions of the table
        exit_code, stdout = HBase.runShellCmds(
            [
                'import org.apache.hadoop.hbase.filter.CompareFilter',
                'import org.apache.hadoop.hbase.filter.SingleColumnValueFilter',
                'import org.apache.hadoop.hbase.filter.RegexStringComparator',
                'import org.apache.hadoop.hbase.util.Bytes',
                "scan 'hbase:meta',{COLUMNS => 'info:regioninfo',"
                "FILTER =>SingleColumnValueFilter.new(Bytes.toBytes('info'),"
                "Bytes.toBytes('regioninfo'),"
                "CompareFilter::CompareOp.valueOf('EQUAL'),"
                "RegexStringComparator.new('(?<!_)%s(?!_)'))}"
                % table
            ]
        )
        assert exit_code == 0
        stdout = stdout[stdout.find(table):stdout.rfind('}')]
        lineArray = stdout.splitlines(True)
        regionArray = []
        for line in lineArray:
            if onlineOnly and ("OFFLINE => true" in line):
                continue

            regionArray.append(line.split(', ')[2].split('=>')[1].strip())

        return regionArray

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(cls, changes, nodeSelection, isFirstUpdate=True, host=None, tmpConf=None):
        nodes = HBase.getSelectedNodes(nodeSelection, host)
        phoenix_conf = os.path.join(Config.get('phoenix', 'PHOENIX_HOME'), 'bin')
        if tmpConf is None:
            tmp_conf = os.path.join(Machine.getTempDir(), 'phoenixConf')
        else:
            tmp_conf = tmpConf
        # We change the permissions for phoenix-home/bin
        Machine.chmod('777', tmp_conf, user=Machine.getAdminUser(), host=host, passwd=Machine.getAdminPasswd())
        configUtils.modifyConfig(changes, phoenix_conf, tmp_conf, nodes, isFirstUpdate)

    @classmethod
    def getModifiedConfPath(cls):
        return os.path.join(Machine.getTempDir(), 'phoenixConf')

    @classmethod
    def getConnectionObject(cls):
        pass


class QueryResult(object):
    """A struct around the result of executing a query."""

    def __init__(self, queryKey, correctExecution, exceptionMessage=''):
        self.queryKey = queryKey
        self.correctExecution = correctExecution
        self.exceptionMessage = exceptionMessage


class PhoenixQueryThread(threading.Thread):
    """A thread designed to run a Phoenix query and verify the response."""

    def __init__(self, queries, key, queriesFolder, expectedResults, envVariables):
        super(PhoenixQueryThread, self).__init__()
        # Hash of queries to run
        self.queries = queries
        # Identifier for this thread
        self.threadId = key
        self.queriesFolder = queriesFolder
        # Hash of query file name to expected result
        self.expectedResults = expectedResults
        self.envVariables = envVariables
        self.results = []

    def run(self):
        logging.info("CONCURR_INFO: Start thread with id %s", self.threadId)

        for queryKey in self.queries:
            queryNumber = queryKey[:queryKey.find('_')]
            # query1.sql from 0_query1.sql
            queryFileName = queryKey[queryKey.find('_') + 1:]
            # We create an aux file with the query that replaces {RANDOM_NUMBER}
            # for a number and {RANDOM_STRING} for a the query key
            queryFile = open(os.path.join(self.queriesFolder, queryFileName), 'r')
            finalQuery = queryFile.read().replace('{RANDOM_NUMBER}', str(random.randint(1, 10000000))).replace(
                '{RANDOM_STRING}',
                str(self.threadId) + '_' + (queryNumber).split('.')[0]
            )
            queryFile.close()

            # we run the expected query and verify that we obtained the expected result.
            exit_code, stdout = Phoenix.runSQLLineCmds(
                finalQuery, env=self.envVariables, threadNumber=str(self.threadId) + "_" + queryKey
            )

            if exit_code != 0:
                self.results.append(QueryResult(queryKey, False, str(stdout)))
                logging.info(
                    "CONCURR_ERROR: We found an exception while executing query: %s. Message: %s", queryKey, stdout
                )
            elif self.expectedResults[queryFileName] != []:
                answerFound = False
                for acceptableAnswer in self.expectedResults[queryFileName]:
                    if stdout.find(str(acceptableAnswer)) != -1:
                        answerFound = True
                        self.results.append(QueryResult(queryKey, True))
                        break
                if answerFound != True:
                    exMsg = "CONCURR_ERROR: Thread query %s failed. We did not found any of the acceptable answers" \
                            " for %s: %s on the response : %s" \
                            % (queryKey, queryKey, self.expectedResults[queryFileName], stdout)
                    result = QueryResult(
                        queryKey, False, exMsg
                    )
                    self.results.append(result)
            else:
                # the expected result is an empty array
                self.results.append(QueryResult(queryKey, True))


class PhoenixThread(threading.Thread):
    """A thread designed to run a Phoenix query and verify the response."""

    def __init__(self, query, threadNumber, expectedResult, outputFormat=None):
        super(PhoenixThread, self).__init__()
        self.query = query
        self.expectedResult = expectedResult
        self.threadNumber = threadNumber
        self.correctExecution = True
        self.exceptionMessage = ''
        self.outputFormat = outputFormat

    def run(self):
        logging.info("CONCURR_INFO: Start thread number %s", self.threadNumber)

        # we run the expected query and verify that we obtained the expected result.
        exit_code, stdout = Phoenix.runSQLLineCmds(
            self.query, threadNumber=self.threadNumber, outputFormat=self.outputFormat
        )
        reportFailure = False
        if exit_code != 0:
            self.exceptionMessage = str(stdout)
            self.correctExecution = False
            reportFailure = True
        if stdout.find(self.expectedResult) == -1:
            self.correctExecution = False
            self.exceptionMessage = 'CONCURR_ERROR: Thread query %s failed. We did not found any of ' \
                                    'the acceptable answers for %s: %s on the response : %s' \
                                    % (self.query, self.query, self.expectedResult, stdout)
            reportFailure = True
        else:
            logging.info("CONCURR_PASSED: Thread number %s PASSED", self.threadNumber)

        if reportFailure:
            # We report the failure once per thread:
            logging.info(
                "CONCURR_ERROR: We found an exception while executing query: %s. Exit_code: %s \n Message: %s",
                self.query, exit_code, stdout
            )


class PhoenixInsertThread(threading.Thread):
    """A thread designed to run a Phoenix query and verify the response."""
    csvBulkLoadMethod = 'csvBulkLoad'
    psqlMethod = 'psql'
    sqllineMethod = 'sqlline'

    def __init__(self, insertFile, threadNumber, insertToTable=None, outputFormat=None, insertMethod=None):
        super(PhoenixInsertThread, self).__init__()
        self.insertFile = insertFile
        self.insertToTable = insertToTable
        self.threadNumber = threadNumber
        self.correctExecution = True
        self.exceptionMessage = ''
        self.outputFormat = outputFormat
        self.insertMethod = insertMethod

    def run(self):
        logging.info("CONCURR_INFO: Start insert thread number %s", self.threadNumber)

        if self.insertMethod is None or self.insertMethod == self.csvBulkLoadMethod:
            # Default method is csvBulkLoad
            exit_code, stdout = Phoenix.insertCSVDataViaCSVBuildLoad(self.insertFile, self.insertToTable)
        elif self.insertMethod == self.sqllineMethod:
            exit_code, stdout = Phoenix.runSQLLineFile(self.insertFile, threadNumber=self.threadNumber)
        else:
            # We insert it through psql
            exit_code, stdout = Phoenix.insertCSVDataViaPSQL(self.insertFile, self.insertToTable)

        if exit_code != 0:
            self.exceptionMessage = str(stdout)
            self.correctExecution = False
            logging.info(
                "CONCURR_ERROR: We found an exception while inserting file: %s. Message: %s",
                self.insertFile, stdout
            )
        else:
            logging.info("CONCURR_PASSED: Thread number %s PASSED", self.threadNumber)
