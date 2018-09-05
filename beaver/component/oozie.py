#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

from beaver.component.hadoop import Hadoop, MAPRED, HDFS
from beaver.machine import Machine
from beaver.config import Config
import os, logging, re, sys, time, pytest, fileinput
from beaver import util, configUtils
from beaver.component.hive import Hive
from beaver.component.ambari import Ambari
from beaver.component.spark import Spark
import json as jsonlib
from taskreporter.taskreporter import TaskReporter

OOZIE_CONF_DIR = Config.get('oozie', 'OOZIE_CONF_DIR')
OOZIE_HOME = Config.get('oozie', 'OOZIE_HOME')
OOZIE_CMD = os.path.join(OOZIE_HOME, 'bin', 'oozie')
OOZIE_PORT = Config.get('oozie', 'OOZIE_PORT')

logger = logging.getLogger(__name__)


class Oozie:
    _oozie_url = None
    _is_ha_enabled = None
    _oozie_user = Config.get('oozie', 'OOZIE_USER')
    _pid_file = None

    _oozieHosts = None
    _compSvcMap = {'oozie_server': 'OOZIE_SERVER', 'oozie_client': 'OOZIE_CLIENT'}
    _compSvcTimeouts = {'oozie': 300}
    _ambariConfigMap = {
        'oozie-site.xml': 'oozie-site',
        'oozie-env.sh': 'oozie-env',
    }
    _compLogFileMap = {'oozie': 'oozie.log'}
    _ambariServiceConfigVersion = None
    _ambariServiceName = 'OOZIE'

    @classmethod
    @TaskReporter.report_test()
    def fix_qe_14910(cls):
        # QE-14190: replace ${hdp.version} in mapreduce.application.classpath because it will case bad substitution error
        config = Ambari.getConfig(type='mapred-site')
        for key in config.keys():
            value = config[key]
            value = value.replace('${hdp.version}', '{{version}}')
            config[key] = value
        Ambari.setConfig(type='mapred-site', config=config)
        Ambari.restart_services_with_stale_configs()

    @classmethod
    @TaskReporter.report_test()
    def configureOozieSpark(cls):
        cls.fix_qe_14910()
        # Workaround BUG-63500 oozie spark test cases are failing with org.apache.thrift.transport.TTransportException null
        sparkShareLibPath = cls.getLatestShareLibPath() + "/spark"
        SPARK_HOME = Config.get('spark', 'SPARK2_HOME')
        source = os.path.join(SPARK_HOME, "conf", "hive-site.xml")
        target_hive_site = os.path.join(sparkShareLibPath, "hive-site.xml")
        HDFS.deleteFile(target_hive_site, cls.getOozieUser())
        HDFS.copyFromLocal(source, sparkShareLibPath, cls.getOozieUser())
        isTez = Hadoop.isTez(True, False)
        if Hadoop.isTez:
            target_tez_site = os.path.join(sparkShareLibPath, "tez-site.xml")
            HDFS.deleteFile(target_tez_site, cls.getOozieUser())
            HDFS.copyFromLocal(
                os.path.join(Config.get('tez', 'TEZ_CONF_DIR'), "tez-site.xml"), sparkShareLibPath, cls.getOozieUser()
            )
        exit_code, stdout = Oozie.share_lib_update()
        assert exit_code == 0

    @classmethod
    @TaskReporter.report_test()
    def KillAndStartHAOozie(cls, suspend=False, wait=10):
        if not cls.isHAEnabled():
            raise Exception("This feature requires Oozie HA to be enabled.")
        hosts = cls.getOozieServers()
        for host in hosts:
            cls.killService(host, suspend=suspend)
            time.sleep(wait)
            cls.resetOozie('start', host=host)

    # method to kill a service instead of shutting it down
    @classmethod
    @TaskReporter.report_test()
    def killService(cls, host, user=None, service='oozieservice', pidFile=None, suspend=False):
        '''
          killService
          Method that kills the oozie service on a give host
        '''
        if not user:
            user = cls._oozie_user
        comment = 'service %s on host %s' % (service, host)
        if suspend:
            logger.info('Suspend %s' % comment)
        else:
            logger.info('Kill %s' % comment)
        if Machine.type() == 'Windows':
            # make sure we are using short fqdn on windows
            host = Machine.getfqdn(name=host)
            # use taskkill command on windows to kill the service
            if not suspend:
                killCmd = 'taskkill /t /f /im %s.exe' % service
                return Machine.runas(Machine.getAdminUser(), killCmd, host=host, passwd=Machine.getAdminPasswd())
            else:
                # get the pid
                pid = Machine.getProcessListWithPid(host, "java.exe", service, logoutput=True, ignorecase=True)
                Machine.sendSIGSTOP(Machine.getAdminUser(), host, pid)
        else:
            killOption = '-9'
            if suspend:
                killOption = '-19'
            # allow the user to send a custom pid file
            if not pidFile:
                if not cls._pid_file:
                    cls._pid_file = util.getPropertyValueFromBashFile(
                        os.path.join(OOZIE_CONF_DIR, 'oozie-env.sh'), 'CATALINA_PID'
                    )
                pidFile = cls._pid_file

            killCmd = "cat %s | xargs kill %s" % (pidFile, killOption)
            return Machine.runas(user, killCmd, host=host)

    @classmethod
    def isHAEnabled(cls):
        '''
          isHAEnabled
          Returns true/false based on if HA is enabled for Oozie or not.
        '''
        if cls._is_ha_enabled is None:
            cls._is_ha_enabled = cls.getConfigValue('oozie.zookeeper.connection.string', '') is not ''
        return cls._is_ha_enabled

    @classmethod
    @TaskReporter.report_test()
    def getOozieRestApiEndpointUrls(cls):
        '''
          getOozieRestApiEndpointUrls
          Method to get a list of oozie servers REST API endpoints running on the cluster.
        '''
        cmd = 'admin -oozie %s -servers' % cls.getOozieUrl()
        exit_code, std_out = cls.run(cmd)
        if exit_code != 0:
            return None
        oozie_servers = []
        for line in std_out.splitlines():
            # Ignore lines that starts with SLF4J or log4j.  Need to find a better workaround for this.
            if line.startswith('SLF4J') or line.startswith('log4j'):
                continue
            oozie_servers.append(line.split(':', 1)[1].strip())
        return oozie_servers

    @classmethod
    def runOozieJobsCmd(
            cls, cmd, cwd=None, env=None, logoutput=True, retry=False, num_of_retries=5, wait=30, oozie_server=None
    ):
        logger.info("OOZIE SERVER:%s" % oozie_server)
        return cls.runOozieJobsCmdAs(
            None,
            cmd,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            retry=retry,
            num_of_retries=num_of_retries,
            wait=wait,
            oozie_server=oozie_server
        )

    @classmethod
    @TaskReporter.report_test()
    def runOozieJobsCmdAs(
            cls, user, cmd, cwd=None, env=None, logoutput=True, retry=False, num_of_retries=5, wait=30,
            oozie_server=None
    ):
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        # if oozie server is not sent find it.
        logger.info("OOZIE SERVER:%s" % oozie_server)
        if not oozie_server:
            oozie_server = cls.getOozieUrl()
        logger.info("OOZIE SERVER:%s" % oozie_server)
        env['JAVA_HOME'] = Config.get("machine", "JAVA_HOME")
        if Machine.type() == 'Windows':
            paramsList = cmd.split()
            escapedCmd = ""
            for param in paramsList:
                if param[0] != '"' and param[-1] != '"':
                    escapedCmd = escapedCmd + "\"" + param + "\"" + " "
                else:
                    escapedCmd = escapedCmd + param + " "

            oozie_cmd = OOZIE_CMD + " jobs -oozie " + oozie_server + "  " + escapedCmd
        else:
            oozie_cmd = OOZIE_CMD + " jobs -oozie " + oozie_server + "  " + cmd
        exit_code, output = Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)
        count = 1
        # Handle retires if the user selects
        while retry and exit_code != 0 and count < num_of_retries:
            exit_code, output = Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)
            count += 1
            time.sleep(wait)
        return exit_code, output

    @classmethod
    def runOozieJobCmd(
            cls, cmd, cwd=None, env=None, logoutput=True, retry=False, num_of_retries=5, wait=30, oozie_server=None
    ):
        return cls.runOozieJobCmdAs(
            None,
            cmd,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            retry=retry,
            num_of_retries=num_of_retries,
            wait=wait,
            oozie_server=oozie_server
        )

    @classmethod
    @TaskReporter.report_test()
    def runOozieJobCmdAs(
            cls, user, cmd, cwd=None, env=None, logoutput=True, retry=False, num_of_retries=5, wait=30,
            oozie_server=None
    ):
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        # if oozie server is not sent find it.
        if not oozie_server:
            oozie_server = cls.getOozieUrl()

        env['JAVA_HOME'] = Config.get("machine", "JAVA_HOME")

        if Machine.type() == 'Windows':

            paramsList = cmd.split()
            escapedCmd = ""
            for param in paramsList:
                if param[0] != '"' and param[-1] != '"':
                    escapedCmd = escapedCmd + "\"" + param + "\"" + " "
                else:
                    escapedCmd = escapedCmd + param + " "

            oozie_cmd = OOZIE_CMD + " job -oozie " + oozie_server + "  " + escapedCmd
        else:
            oozie_cmd = OOZIE_CMD + " job -oozie " + oozie_server + "  " + cmd
        exit_code, output = Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)

        count = 1
        # Handle retires if the user selects
        while retry and exit_code != 0 and count < num_of_retries:
            exit_code, output = Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)

            ### Log warning to check for any regression ###
            if "Connection reset" in output:
                logger.warn("OOZIE-264 Connection reset bug reproduced " + output)

            count += 1
            time.sleep(wait)

        return exit_code, output

    @classmethod
    def runOozieAdminCmd(cls, cmd, host=None, cwd=None, env=None, logoutput=True):
        return cls.runOozieAdminCmdAs(None, cmd, host, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def runOozieAdminCmdAs(cls, user, cmd, host=None, cwd=None, env=None, logoutput=True):
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        if host is None:
            oozie_server = cls.getOozieUrl()
        else:
            oozie_server = host

        env['JAVA_HOME'] = Config.get("machine", "JAVA_HOME")

        if Machine.type() == 'Windows':

            paramsList = cmd.split()
            escapedCmd = ""
            for param in paramsList:
                escapedCmd = escapedCmd + "\"" + param + "\"" + " "

            oozie_cmd = OOZIE_CMD + " admin -oozie " + oozie_server + ":" + OOZIE_PORT + "/oozie  " + escapedCmd
        else:
            oozie_cmd = OOZIE_CMD + " admin -oozie " + oozie_server + ":" + OOZIE_PORT + "/oozie  " + cmd
        exit_code, output = Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)

        return exit_code, output

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True, base_cmd=OOZIE_CMD):
        return cls.runas(None, cmd, cwd=cwd, env=env, logoutput=logoutput, base_cmd=base_cmd)

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True, base_cmd=OOZIE_CMD):
        if not env:
            env = {}

        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        env['JAVA_HOME'] = Config.get("machine", "JAVA_HOME")
        oozie_cmd = base_cmd + " " + cmd
        return Machine.runas(user, oozie_cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("version")
        if exit_code == 0:
            return (output.split(":")[-1]).strip()
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getDatabaseFlavor(cls):
        dbdriver = Oozie.getConfigValue("oozie.service.JPAService.jdbc.driver")
        if ("oracle" in dbdriver):
            return "oracle"
        elif ("postgresql" in dbdriver):
            dbUrl = Oozie.getConfigValue("oozie.service.JPAService.jdbc.url")
            m = re.search('jdbc:postgresql://(.*):.*', dbUrl)
            dbHost = Machine.getfqdn()
            if m and m.group(1):
                dbHost = m.group(1)
            dbVersion = Machine.getDBVersion('postgres', host=dbHost)
            if dbVersion:
                return "postgres-%s" % dbVersion
            else:
                return "postgres"
        elif ("derby" in dbdriver):
            return "derby"
        elif ("mysql" in dbdriver):
            return "mysql"
        elif ("sqlserver" in dbdriver):
            return "mssql"
        elif ("sqlanywhere" in dbdriver):
            return "sqlanywhere"
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getEnvironmentVariables(cls, directory):

        # Need to set the below for the oozie-regression test
        OOZIE_HDFS_LOG_DIR = str(
            Hadoop.getFSDefaultValue(True) + '/user/' + Config.get("oozie", "OOZIE_USER") + '/oozie-logs/'
        )
        if Machine.type() == "Windows":
            sep = ";"
            OOZIE_HDFS_LOG_DIR = OOZIE_HDFS_LOG_DIR + os.environ.get("COMPUTERNAME")
        else:
            sep = ":"
            OOZIE_HDFS_LOG_DIR = OOZIE_HDFS_LOG_DIR + Oozie.getOozieServers()[0]

        OOZIE_QA_REG_DIR = cls.getOozieRegressionFolder()
        path = os.path.join(OOZIE_QA_REG_DIR, "lib", "yoozietest-qa-1.0.0-SNAPSHOT.jar") + sep + os.path.join(
            OOZIE_QA_REG_DIR, "lib", "original-yoozietest-qa-1.0.0-SNAPSHOT.jar"
        )
        Config.setEnv("LOCAL_CP", path)

        oozie_server = cls.getOozieUrl()

        if HDFS.isASV() or HDFS.isCabo():
            if HDFS.isCabo():
                # Cabo gets FQDN uri to use the scheme as a differentiator for other FS
                HIT_NN = str(Hadoop.getConfigValue("fs.defaultFS", None))
            else:  # is ASV
                HIT_NN = str(Hadoop.getFSDefaultValue(False))

            return {
                "JAVA_HOME": Config.get('machine', 'JAVA_HOME'),
                "HADOOP_HOME": Config.get('hadoop', 'HADOOP_HOME'),
                "HADOOP_CONF_DIR": Config.get('hadoop', 'HADOOP_CONF'),
                "HIT_NN": HIT_NN,
                "HIT_JT": str(MAPRED.getJobtrackerAddress()[0]),
                "HIT_JT_PORT": str(MAPRED.getJobtrackerAddress()[1]),

                ###TODO Change following 3 for secure setups
                "OOZIE_QA_AUTH": "simple",
                "OOZIE_NN_KRB_PRINCIPAL": "Blah",
                "OOZIE_JT_KRB_PRINCIPAL": "Blah",
                "OOZIE_TEST_SUITE": "testngRegressionSuiteDebug",
                "OOZIE_HOME": Config.get("oozie", "OOZIE_HOME"),
                "OOZIE_PORT": Config.get("oozie", "OOZIE_PORT"),
                "OOZIE_SECURE_HOSTNAME": Machine.getfqdn(),
                "OOZIE_FOLDER": Config.get("oozie", "OOZIE_USER"),
                "OOZIE_USER": Config.get("oozie", "OOZIE_USER"),
                "FIREFOX_PATH": Config.get("firefox", "FIREFOX_PATH"),
                "FIREFOX_DISPLAY": Config.get("firefox", "FIREFOX_DISPLAY"),
                "OOZIE_QA_REG_DIR": OOZIE_QA_REG_DIR,
                "OOZIE_QA_HADOOP_QUEUE": "default",
                "OOZIE_URL": str(oozie_server),
                "HIT_OOZIE": ((str(oozie_server)).split(":")[2]).split("/")[0],
                "LOCAL_CP": path,
                "HIT_HDFS_STORAGE_DIR": directory,
                "OOZIE_HDFS_LOG_DIR": OOZIE_HDFS_LOG_DIR
            }

        else:
            if Hadoop.isSecure():
                oozie_qa_auth = "kerberos"
                nnPrincipal = HDFS.getNameNodePrincipal()
                jtPrincipal = MAPRED.getMasterPrincipal()
                user = Config.getEnv('USER')
                kerbTicket = Machine.getKerberosTicket(user)
            else:
                oozie_qa_auth = "simple"
                nnPrincipal = "blah"
                jtPrincipal = "blah"
                kerbTicket = "blah"

            return {
                "JAVA_HOME": Config.get('machine', 'JAVA_HOME'),
                "HADOOP_HOME": Config.get('hadoop', 'HADOOP_HOME'),
                "HADOOP_CONF_DIR": Config.get('hadoop', 'HADOOP_CONF'),
                "HIT_NN": str(Hadoop.getFSDefaultValue(False)),
                "HIT_NN_PORT": str(Hadoop.getFSDefaultValue(True)).split(":")[2],
                "HIT_JT": str(MAPRED.getJobtrackerAddress()[0]),
                "HIT_JT_PORT": str(MAPRED.getJobtrackerAddress()[1]),
                "OOZIE_QA_AUTH": oozie_qa_auth,
                "OOZIE_NN_KRB_PRINCIPAL": nnPrincipal,
                "OOZIE_JT_KRB_PRINCIPAL": jtPrincipal,
                "OOZIE_TEST_SUITE": "testngRegressionSuiteDebug",
                "OOZIE_HOME": Config.get("oozie", "OOZIE_HOME"),
                "OOZIE_PORT": Config.get("oozie", "OOZIE_PORT"),
                "OOZIE_FOLDER": Config.get("oozie", "OOZIE_USER"),
                "OOZIE_USER": Config.get("oozie", "OOZIE_USER"),
                "FIREFOX_PATH": Config.get("firefox", "FIREFOX_PATH"),
                "FIREFOX_DISPLAY": Config.get("firefox", "FIREFOX_DISPLAY"),
                "OOZIE_QA_REG_DIR": OOZIE_QA_REG_DIR,
                "OOZIE_QA_HADOOP_QUEUE": "default",
                "OOZIE_URL": str(oozie_server),
                "HIT_OOZIE": ((str(oozie_server)).split(":")[2]).split("/")[0],
                "LOCAL_CP": path,
                "HIT_HDFS_STORAGE_DIR": directory,
                "KRB5CCNAME": kerbTicket,
                "OOZIE_HDFS_LOG_DIR": OOZIE_HDFS_LOG_DIR
            }

    @classmethod
    def setupOozieDataDir(cls, directory):
        HDFS.deleteDirectory(directory)
        HDFS.createDirectory(directory)

    # method to read oozie configs
    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(OOZIE_CONF_DIR, "oozie-site.xml"), propertyValue, defaultValue=defaultValue
        )

    # method to get the oozie server
    @classmethod
    @TaskReporter.report_test()
    def getOozieUrl(cls, overwrite=False):
        if not overwrite and cls._oozie_url is not None:
            return cls._oozie_url

        if Machine.type() == 'Windows':
            cls._oozie_url = "http://localhost:11000/oozie"
        else:
            cls._oozie_url = cls.getConfigValue('oozie.base.url')
        return cls._oozie_url

    # get the prtotocol of oozie server
    @classmethod
    def getOozieProtocol(cls):
        oozieUrl = cls.getOozieUrl()
        protocol = oozieUrl.split('//', 1)[0].strip(':')
        return protocol

    # method to update job.properties file
    @classmethod
    @TaskReporter.report_test()
    def updateJobProperties(cls, propFile, properties=None, haEnabled=False, debug=False):
        fileSystemName = Hadoop.getFSDefaultValue()
        jobTrackerIP = MAPRED.getJobtrackerAddress()
        jobTracker = jobTrackerIP[0] + ":" + jobTrackerIP[1]

        if not properties:
            properties = {}
        if not properties.has_key('nameNode'):
            properties['nameNode'] = fileSystemName
        if not properties.has_key('jobTracker'):
            properties['jobTracker'] = jobTracker

        if "hcatalog" in propFile:
            if Hadoop.isSecure():
                kerberosPrincipal = Hive.getConfigValue("hive.metastore.kerberos.principal")
                properties['hive.metastore.kerberos.principal'] = kerberosPrincipal

            logger.info("Updating for hcatalog workflow")
            hcatNode = Hive.getConfigValue("hive.metastore.uris").replace('thrift', 'hcat')
            logger.info("Hcat node is " + hcatNode)
            properties['hcatNode'] = hcatNode

        if Hadoop.isSecure():
            # determine the namenode and the jobtracker principal
            nnPrincipal = None
            if haEnabled:
                nnPrincipal = HDFS.getNameNodePrincipal().replace('_HOST', HDFS.getNamenodeByState('active'))
            else:
                nnPrincipal = HDFS.getNameNodePrincipal().replace('_HOST', HDFS.getNamenodeHttpAddress()[0])
            jtPrincipal = MAPRED.getMasterPrincipal().replace('_HOST', jobTrackerIP[0])
            properties['dfs.namenode.kerberos.principal'] = nnPrincipal
            properties['mapreduce.jobtracker.kerberos.principal'] = jtPrincipal

        wfPath = util.getPropertyValueFromFile(propFile, "oozie.wf.application.path")
        if wfPath != None and wfPath.find("hdfs://localhost:9000") != -1:
            wfPath = wfPath.replace("hdfs://localhost:9000", fileSystemName)
            logger.info("Value of replaced oozie.wf.application.path is " + wfPath)
            properties['oozie.wf.application.path'] = wfPath

        util.writePropertiesToFile(propFile, propFile, properties)

        if debug:
            logger.info('Content of properties file %s' % propFile)
            f = open(propFile, 'r')
            # print the file to the console
            logger.info(f.read())
            f.close()

    # method to get job status
    @classmethod
    @TaskReporter.report_test()
    def getJobStatus(cls, jobId, doAs=None, retry=True, num_of_retries=5, wait=30, logoutput=True, oozie_server=None):
        statusCmd = ' '
        if doAs:
            statusCmd += '-doas %s ' % doAs
        statusCmd += "-info " + jobId
        exit_code, stdout = cls.runOozieJobCmd(
            statusCmd,
            retry=retry,
            num_of_retries=num_of_retries,
            wait=wait,
            logoutput=logoutput,
            oozie_server=oozie_server
        )
        outLines = stdout.split("\n")
        status = ''
        for line in outLines:
            if line.find("Status") == 0:
                status = (line.split(":")[1]).strip()
        return status

    # method to get job info stdout
    @classmethod
    @TaskReporter.report_test()
    def getJobInfo(cls, jobId, doAs=None, verbose=False, retry=True, num_of_retries=5, wait=30, oozie_server=None):
        statusCmd = ' '
        if doAs:
            statusCmd += '-doas %s ' % doAs
        statusCmd += "-info " + jobId
        if verbose:
            statusCmd += " --verbose"
        exit_code, stdout = cls.runOozieJobCmd(
            statusCmd, retry=retry, num_of_retries=num_of_retries, wait=wait, oozie_server=oozie_server
        )
        return stdout

        # get the host where oozie is running

    @classmethod
    def getOozieHost(cls):
        return cls.getOozieServers()[0]

    @classmethod
    def getOozieServers(cls):
        if cls._oozieHosts is None:
            cls._oozieHosts = Ambari.getHostsForComponent(cls._compSvcMap['oozie_server'])
        return cls._oozieHosts

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(cls, changes, env={}, restartService=True):
        logger.info("Current Service Config Version: %s" % cls.getStartingAmbariServiceConfigVersion())
        for key, value in changes.items():
            if cls._ambariConfigMap.has_key(key):
                key = cls._ambariConfigMap[key]
            else:
                logger.warn("Unknown config \"%s\" change requested, ignoring" % key)
                continue
            Ambari.setConfig(key, value)
        if len(env.keys()) > 0:
            key = cls._ambariConfigMap['oozie-env.sh']
            envProps = Ambari.getConfig(key)
            if envProps.has_key("content"):
                content = envProps['content']
                for envKey, envVal in env.items():
                    content = "export %s=%s\n%s" % (envKey, envVal, content)
                Ambari.setConfig(key, {'content': content})
        if restartService:
            Ambari.restart_services_with_stale_configs()

    @classmethod
    @TaskReporter.report_test()
    def getStartingAmbariServiceConfigVersion(cls, update=False):
        if cls._ambariServiceConfigVersion is None or update:
            cls._ambariServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceName)
        return cls._ambariServiceConfigVersion

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls):
        resetVersion = cls.getStartingAmbariServiceConfigVersion()
        logger.info("Restoring Service Config Version to: %s" % resetVersion)
        Ambari.resetConfig(cls._ambariServiceName, resetVersion)
        Ambari.restart_services_with_stale_configs()

    # method to reset oozie service
    @classmethod
    @TaskReporter.report_test()
    def resetOozie(cls, action, configDir=None, host=None):

        env = {}
        if configDir:
            env['OOZIE_CONFIG'] = configDir

        if Machine.type() == 'Windows':
            return Machine.service('oozieservice', action, host=host)

        OOZIE_SERVERHOME = OOZIE_HOME.replace("client", "server")

        binDir = os.path.join(OOZIE_SERVERHOME, 'bin')
        user = Config.get('oozie', 'OOZIE_USER')
        # Only specific to Linux
        if action == 'start':
            resetScript = os.path.join(binDir, 'oozie-start.sh')
        elif action == 'stop':
            resetScript = os.path.join(binDir, 'oozie-stop.sh')
        else:
            logger.error('Invalid action %s sent for oozie service' % (action))

        return Machine.runas(user, resetScript, host=host, env=env)

    # method to restart oozie
    @classmethod
    @TaskReporter.report_test()
    def restartOozie(cls, configDir=None):
        hosts = cls.getOozieServers()
        for host in hosts:
            logger.info("restart host %s" % host)
            cls.resetOozie('stop', host=host)
            time.sleep(60)
            cls.resetOozie('start', configDir=configDir, host=host)
            time.sleep(60)

    # method to get the user of the job
    @classmethod
    @TaskReporter.report_test()
    def getJobUser(cls, jobId, doAs=None):
        statusCmd = ' '
        if doAs:
            statusCmd += '-doas %s ' % doAs
        statusCmd += "-info " + jobId
        exit_code, stdout = cls.runOozieJobCmd(statusCmd)
        outLines = stdout.split("\n")
        user = ''
        for line in outLines:
            if line.find("User") == 0:
                user = (line.split(":")[1]).strip()
        return user

    # method to get workflow ID
    @classmethod
    @TaskReporter.report_test()
    def getWorkflowID(cls, stdout):
        workflowID = ''
        stdout = stdout.split('\n')
        for line in stdout:
            if "job:" in line:
                workflowID = (line.split(":")[1]).strip()
        return workflowID

    @classmethod
    def getOozieRegressionFolder(cls):
        return os.path.join(Config.getEnv('WORKSPACE'), "tests", "oozie", "oozie_1", "oozie-regression", "oozie_40")

    # method to get application and job id
    @classmethod
    @TaskReporter.report_test()
    def getJobAndAppIds(cls, text):
        '''
          getJobAndAppIds
            text - Text from which to get the application and the job id
        '''
        ids = []
        pattern = 'job_(\d+)_(\d+)'
        for line in re.finditer(pattern, text):
            id = '%s_%s' % (line.group(1), line.group(2))
            ids.append({'job': 'job_%s' % id, 'application': 'application_%s' % id})
        return ids

    @classmethod
    def getOozieConfDir(cls, logoutput=False):
        '''
        Returns Oozie conf directory (String).
        '''
        returnValue = OOZIE_CONF_DIR
        if logoutput:
            logger.info("Oozie.getOozieConfDir returns %s" % returnValue)
        return returnValue

    @classmethod
    def getOozieLogDir(cls, logoutput=False):
        '''
        Returns oozie log dir.
        '''
        returnValue = Config.get("oozie", "OOZIE_LOG_DIR")
        if logoutput:
            logger.info("Oozie.getOozieLogDir returns %s" % returnValue)
        return returnValue

    @classmethod
    @TaskReporter.report_test()
    def share_lib_setup(cls, fs=None):
        # only need to do this on windows
        if not Machine.isWindows():
            # return exit code of 0 and empty stdout
            return 0, ''

        oozie_setup_cmd = os.path.join(OOZIE_HOME, 'bin', 'oozie-setup.cmd')
        oozie_sharelib_location = os.path.join(OOZIE_HOME, "share")
        if not fs:
            fs = Hadoop.getFSDefaultValue()
        cmd = 'sharelib create -fs %s -locallib %s' % (fs, oozie_sharelib_location)
        return cls.runas(cls._oozie_user, cmd, base_cmd=oozie_setup_cmd)

    @classmethod
    @TaskReporter.report_test()
    def share_lib_update(cls, fs=None):
        # only need to do this on windows
        oozie_server = cls.getOozieUrl()
        cmd = "  admin -oozie " + oozie_server + "  -sharelibupdate"
        if Hadoop.isSecure():
            cmd = " -Doozie.auth.token.cache=false " + cmd
        return cls.runas(cls._oozie_user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def getOozieJobList(cls, status):
        '''
          Returns an array of jobID
        '''
        jobArray = []
        regex = '[\d]{7}-[\d]{15}-oozie-oozi-[W|B|C]'
        for jobType in ('wf', 'coordinator', 'bundle'):
            cmd = ' jobs -oozie=%s -filter status=%s -jobtype %s' % (cls.getOozieUrl(), status, jobType)
            if Hadoop.isSecure():
                cmd = " -Doozie.auth.token.cache=false " + cmd
            exit_code, stdout = cls.runas(cls._oozie_user, cmd)
            jobArray += re.findall(regex, stdout)

        return jobArray

    @classmethod
    def getOozieJobListFailed(cls):
        return cls.getOozieJobList('FAILED')

    @classmethod
    def getOozieJobListSucceeded(cls):
        return cls.getOozieJobList('SUCCEEDED')

    @classmethod
    def getOozieJobListRunning(cls):
        return cls.getOozieJobList('RUNNING')

    @classmethod
    @TaskReporter.report_test()
    def getOozieJobLog(cls, jobID):
        # only need to do this on windows
        cmd = ' job -oozie=%s -log %s' % (cls.getOozieUrl(), jobID)
        if Hadoop.isSecure():
            cmd = " -Doozie.auth.token.cache=false " + cmd
        return cls.runas(cls._oozie_user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def createTestngXmlWithTestlist(cls, referenceXMLFilePath, testName):
        if pytest.config.option.testlist:
            try:
                NEW_CONF_FILE = referenceXMLFilePath.split(".xml")[0] + "-NEW.xml"
                tempTestList = pytest.config.option.testlist
                tempTestList = tempTestList.split(",")
                newClassList = []
                for test in tempTestList:
                    if testName + "[" in test:
                        className = test.split(testName + "[")[1].split("]")[0].split("-")[1]
                        newClassList.append(className)
                    else:
                        if testName in test:
                            return referenceXMLFilePath
                if util.createTestngXmlWithClasses(referenceXMLFilePath, NEW_CONF_FILE, newClassList):
                    return NEW_CONF_FILE
            except Exception, e:
                logger.exception(e)
                return referenceXMLFilePath
        return referenceXMLFilePath

    @classmethod
    @TaskReporter.report_test()
    def replaceWorkflowLine(cls, findLine, replaceLine, workflowFile, entireLine=False):
        replacedAlready = False
        for line in fileinput.FileInput(workflowFile, inplace=1):
            if findLine in line:
                if not replacedAlready:
                    if entireLine:
                        line = replaceLine
                        replacedAlready = True
                    else:
                        line = line.replace(findLine, replaceLine)
                        replacedAlready = True
            print line,
        fileinput.close()

    # Dynamic lib_<date> so we must grab a dynamic name
    # returns /user/oozie/share/lib for 2.2 and dated lib path for 2.3+
    @classmethod
    @TaskReporter.report_test()
    def getLatestShareLibPath(cls):
        results = HDFS.lsrWithListOutput(
            "/user/" + cls.getOozieUser() + "/share/lib", recursive=False, logoutput=False
        )
        # set to pre-dated share lib path
        shareLibPath = "/user/" + cls.getOozieUser() + "/share/lib"
        for files in results:
            if "lib_" in files[7]:
                # if dated one is found, set to dated one until we get the latest
                shareLibPath = files[7]
        return shareLibPath

    @classmethod
    def getOozieUser(cls):
        return Config.get('oozie', 'OOZIE_USER')

    @classmethod
    @TaskReporter.report_test()
    def getdnjars(cls):
        # Workaround BUG-58287 org.datanucleus.api.jdo.JDOPersistenceManagerFactory
        spark_lib_dir = os.path.join(Spark.getSparkHome(), "lib")
        dn_jars = util.findMatchingFiles(spark_lib_dir, "datanucleus*.jar")
        jars = ''
        for jar in dn_jars:
            jars = jar + "," + jars

        # remove the last "," in the list
        jars = jars[:-1]
        return jars
