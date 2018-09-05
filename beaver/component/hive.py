#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.config import Config
from beaver.machine import Machine
from beaver.component.ambari import Ambari
from beaver.component.hadoop import Hadoop, YARN
from beaver.component.zookeeper import Zookeeper
from beaver import util
import os, re, random, logging, time, ast
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
HDP_Version = re.search(r'\d.\d.\d.(\d.\d.\d).*', Ambari.getHDPVersion()).group(1)


class Hive1:
    _hiveHome = '/usr/hdp/current/hive-client'
    _hiveMetastoreHosts = None
    _hiveServer2Hosts = None
    _hiveMetastorePort = None
    _hiveServer2Port = None
    _hiveLogDir = None
    _compSvcMap = {'metastore': 'HIVE_METASTORE', 'hiveserver2': 'HIVE_SERVER'}
    # hiveserver timeout to be reverted back to 300, once schematool run restricted to install/upgrade
    _compSvcTimeouts = {'metastore': 300, 'hiveserver2': 600}
    _ambariConfigMap = {
        'hive-site.xml': 'hive-site',
        'hiveserver2-site.xml': 'hiveserver2-site',
        'hive-env.sh': 'hive-env',
        'tez-site.xml': 'tez-site',
        'ranger-hive-security.xml': 'ranger-hive-security'
    }
    _compLogFileMap = {'metastore': 'hivemetastore.log', 'hiveserver2': 'hiveserver2.log'}
    if HDP_Version == '2.6.5':
        _configDir = '/etc/hive/conf/conf.server'
    else:
        _configDir = '/etc/hive/conf'
    _localConfDir = "hive_conf"
    _modifiedConfigs = {}
    _ambariServiceName = 'HIVE'
    _ambariServiceConfigVersion = None
    _ambariTezServiceName = 'TEZ'
    _ambariTezServiceConfigVersion = None
    _isHive2 = False
    __name__ = "Hive1"
    _outputSpecMap = {
        'exit_code': 0,
        'appId': 1,
        'queryId': 2,
        'queryCompleted': 3,
        'queryStartTime': 4,
        'queryEndTime': 5,
        'poolName': 6,
        'clusterPercent': 7,
    }

    @classmethod
    def isInstalled(cls, host=None):
        try:
            return Machine.pathExists(Machine.getAdminUser(), host, cls._configDir, Machine.getAdminPasswd())
        except IOError:
            return False

    @classmethod
    def isHive2(cls):
        return cls._isHive2

    @classmethod
    @TaskReporter.report_test()
    def getHiveHome(cls, service="client"):
        hiveHome = cls._hiveHome
        if service == 'metastore':
            hiveHome = hiveHome.replace("-client", "-metastore")
        elif service == 'hiveserver2':
            hiveHome = hiveHome.replace("-client", "-server2")
        return hiveHome

    @classmethod
    @TaskReporter.report_test()
    def getHiveDbHost(cls):
        dbdriver = cls.getConfigValue("javax.jdo.option.ConnectionDriverName")
        dbUrl = cls.getConfigValue("javax.jdo.option.ConnectionURL")
        dbHost = re.findall(r'[0-9]+(?:\.[0-9]+){3}', dbUrl)[0]
        return dbHost

    @classmethod
    @TaskReporter.report_test()
    def runCommandAs(cls, user, cmd, cwd=None, env=None, logoutput=True, stderr_as_stdout=True, background=False):
        # initialize env
        if not env:
            env = {}
        # get kerberos ticket
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        if background:
            return Machine.runinbackgroundAs(user, cmd, cwd=cwd, env=env)

        if stderr_as_stdout:
            return Machine.runas(user, cmd, cwd=cwd, env=env, logoutput=logoutput)
        else:
            return Machine.runexas(user, cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True, stderr_as_stdout=True, background=False):
        hive_cmd = Config.get('hive', 'HIVE_CMD') + " " + cmd
        return cls.runCommandAs(
            user,
            hive_cmd,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            stderr_as_stdout=stderr_as_stdout,
            background=background
        )

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True, stderr_as_stdout=True):
        return cls.runas(None, cmd, cwd, env, logoutput, stderr_as_stdout)

    @classmethod
    def runInBackgroundAs(cls, user, cmd, cwd=None, env=None):
        return cls.runas(user, cmd, cwd=cwd, env=env, background=True)

    @classmethod
    def runinBackground(cls, cmd, cwd=None, env=None):
        return cls.runas(None, cmd, cwd=cwd, env=env, background=True)

    @classmethod
    def runHivequeryThroughWebHCAT(cls):
        # todo Add code as part of QE-12859
        print "Here follows code to run hive query through web hcat :"

    @classmethod
    def getHiveHost(cls, service='metastore'):
        return cls.getServiceHosts(service)[0]

    @classmethod
    @TaskReporter.report_test()
    def getConfigValue(cls, propertyValue, defaultValue=None):
        _localHiveConf = os.path.join(Config.getEnv('ARTIFACTS_DIR'), cls._localConfDir)
        if not os.path.exists(_localHiveConf):
            admin_user = Machine.getAdminUser()
            Machine.copyToLocal(admin_user, cls.getHiveHost(), cls._configDir, _localHiveConf)
            Machine.chmod('777', _localHiveConf, recursive=True, user=admin_user)
        if cls._modifiedConfigs.has_key(propertyValue):
            return cls._modifiedConfigs[propertyValue]
        value = util.getPropertyValueFromConfigXMLFile(
            os.path.join(_localHiveConf, 'hiveserver2-site.xml'), propertyValue
        )
        if value is None or value == '':
            value = util.getPropertyValueFromConfigXMLFile(
                os.path.join(_localHiveConf, 'hive-site.xml'), propertyValue, defaultValue=defaultValue
            )
        return value

    @classmethod
    def getServiceDiscoveryMode(cls):
        return 'zooKeeper'

    @classmethod
    def isDynamicServiceDiscoveryOn(cls):
        return cls.getConfigValue(
            "hive.server2.support.dynamic.service.discovery", defaultValue="false"
        ).lower() == "true"

    @classmethod
    @TaskReporter.report_test()
    def buildcmd(cls, query, hiveconf={}, hivevar={}, option={}, readFromFile=False, queryIsFile=False):
        cmd = ""
        for key, value in hiveconf.items():
            cmd += " --hiveconf %s=%s" % (key, value)
        for key, value in hivevar.items():
            cmd += " --hivevar %s=%s" % (key, value)
        for key, value in option.items():
            cmd += " --%s=%s" % (key, value)
        qfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d' % int(999999 * random.random()))
        if queryIsFile:
            cmd += " -f %s" % query
        elif readFromFile:
            qf = open(qfile, 'w')
            qf.write(query)
            qf.close()
            cmd += " -f %s" % qfile
        else:
            cmd += " -e \"%s\"" % query
        return cmd

    @classmethod
    def runQueryAs(
            cls,
            user,
            query,
            hiveconf={},
            cwd=None,
            env=None,
            logoutput=True,
            stderr_as_stdout=True,
            background=False,
            readFromFile=False,
            queryIsFile=False
    ):
        cmd = cls.buildcmd(query, hiveconf=hiveconf, readFromFile=readFromFile, queryIsFile=queryIsFile)
        return cls.runas(
            user, cmd, cwd=cwd, env=env, logoutput=logoutput, stderr_as_stdout=stderr_as_stdout, background=background
        )

    @classmethod
    def runQuery(
            cls,
            query,
            hiveconf={},
            cwd=None,
            env=None,
            logoutput=True,
            stderr_as_stdout=True,
            background=False,
            readFromFile=False,
            queryIsFile=False
    ):
        return cls.runQueryAs(
            None,
            query,
            hiveconf=hiveconf,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            stderr_as_stdout=stderr_as_stdout,
            background=background,
            readFromFile=readFromFile,
            queryIsFile=queryIsFile
        )

    @classmethod
    def getBeelineCmd(cls):
        return os.path.join(cls.getHiveHome(), 'bin', 'beeline')

    @classmethod
    def getHiveMetastoreHosts(cls):
        if cls._hiveMetastoreHosts is None:
            cls._hiveMetastoreHosts = Ambari.getHostsForComponent(cls._compSvcMap['metastore'])
        return cls._hiveMetastoreHosts

    @classmethod
    @TaskReporter.report_test()
    def getMetastoreThriftPort(cls, host=None):
        if cls._hiveMetastorePort is None:
            cls._hiveMetastorePort = 9083
            hiveMetastoreUris = Hive1.getConfigValue("hive.metastore.uris")
            m = re.findall("thrift://([^,]+):([0-9]+)", hiveMetastoreUris)
            if len(m) > 0:
                if host is None:
                    cls._hiveMetastorePort = int(m[0][1])
                else:
                    for i in len(m):
                        if Machine.isSameHost(m[i][0], host):
                            cls._hiveMetastorePort = int(m[i][1])
                            break
        return cls._hiveMetastorePort

    @classmethod
    @TaskReporter.report_test()
    def getHiveserver2ThriftPort(cls):
        if cls._hiveServer2Port is None or len(cls._modifiedConfigs) > 0:
            if cls.getConfigValue("hive.server2.transport.mode", defaultValue="binary") == "http":
                cls._hiveServer2Port = int(cls.getConfigValue("hive.server2.thrift.http.port", defaultValue="10001"))
            else:
                cls._hiveServer2Port = int(cls.getConfigValue("hive.server2.thrift.port", defaultValue="10000"))
        return cls._hiveServer2Port

    @classmethod
    def getMetastoreWarehouseDir(cls):
        return cls.getConfigValue("hive.metastore.warehouse.dir", defaultValue="/warehouse/tablespace/managed/hive")

    @classmethod
    @TaskReporter.report_test()
    def getHiveserver2Hosts(cls):
        if cls._hiveServer2Hosts is None:
            cls._hiveServer2Hosts = Ambari.getHostsForComponent(cls._compSvcMap['hiveserver2'])
        return cls._hiveServer2Hosts

    @classmethod
    @TaskReporter.report_test()
    def getServiceHosts(cls, service):
        hosts = []
        if service == 'metastore':
            hosts = cls.getHiveMetastoreHosts()
        elif service == 'hiveserver2':
            hosts = cls.getHiveserver2Hosts()
        else:
            logger.error("Unrecognized service: %s" % service)
        return hosts

    @classmethod
    @TaskReporter.report_test()
    def getServicePort(cls, service):
        port = None
        if service == 'metastore':
            port = cls.getMetastoreThriftPort()
        elif service == 'hiveserver2':
            port = cls.getHiveserver2ThriftPort()
        else:
            logger.error("Unrecognized service: %s" % service)
        return port

    @classmethod
    def getZkNamespace(cls):
        return cls.getConfigValue("hive.server2.zookeeper.namespace")

    @classmethod
    @TaskReporter.report_test()
    def _extendedHiveserver2Url(cls, isHttpMode, isLdapMode, isDirectMode, useSSL):
        hs2url = ''
        if not isDirectMode and cls.isDynamicServiceDiscoveryOn():
            zknamespace = cls.getZkNamespace()
            hs2url += (";serviceDiscoveryMode=%s;zooKeeperNamespace=" % cls.getServiceDiscoveryMode()) + zknamespace
        isLdapMode = isLdapMode or cls.getConfigValue("hive.server2.authentication", "NONE").lower() == "ldap"
        if not isLdapMode and Hadoop.isSecure():
            principal = cls.getConfigValue("hive.server2.authentication.kerberos.principal")
            hs2url += ";principal=" + principal
        if isHttpMode or cls.getConfigValue("hive.server2.transport.mode", "binary") == "http":
            hs2url += ";transportMode=http;httpPath=cliservice"
        if useSSL and cls.getConfigValue("hive.server2.use.SSL", defaultValue="false").lower() == "true":
            trustStorePath = "/etc/security/serverKeys/hivetruststore.jks"
            trustStorePwd = "changeit"
            hs2url += ";ssl=true;sslTrustStore=%s;trustStorePassword=%s" % (trustStorePath, trustStorePwd)
        return hs2url

    @classmethod
    @TaskReporter.report_test()
    def _addPortToZkquorum(cls, zkquorum):
        zkquorumwithport = ''
        p = re.compile('\S*:\d+')
        for host in zkquorum.split(','):
            m = p.search(host)
            if not m:
                zkquorumwithport += host + ":2181,"
            else:
                zkquorumwithport += host + ","
        return zkquorumwithport[:-1]

    @classmethod
    @TaskReporter.report_test()
    def getHiveServer2Url(
            cls, httpmode=False, ldap=False, direct=False, hs2port=None, ssl=True, database="", host=None
    ):
        hiveServer2Url = "jdbc:hive2://"
        if cls.isDynamicServiceDiscoveryOn() and not direct:
            zkquorum = cls.getConfigValue("hive.zookeeper.quorum")
            zkquorumwithport = cls._addPortToZkquorum(zkquorum)
            hiveServer2Url += zkquorumwithport + "/"
        else:
            if not hs2port:
                hs2port = cls.getHiveserver2ThriftPort()
            if not host:
                hiveServer2Url += cls.getHiveserver2Hosts()[0] + ":%s/" % str(hs2port)
        hiveServer2Url += database
        return hiveServer2Url + cls._extendedHiveserver2Url(httpmode, ldap, direct, ssl)

    @classmethod
    def getHiveServer2UrlForDynamicServiceDiscovery(cls, zkquorum, zknamespace, httpmode=False, ldap=False, ssl=True):
        extUrl = cls._extendedHiveserver2Url(httpmode, ldap, False, ssl)
        return "jdbc:hive2://" + zkquorum + "/" + extUrl

    @classmethod
    @TaskReporter.report_test()
    def getSimpleHiveServer2UrlForDynamicServiceDiscovery(cls, ssl=False):
        zkquorum = cls.getConfigValue("hive.zookeeper.quorum")
        zkquorumwithport = cls._addPortToZkquorum(zkquorum)
        zknamespace = cls.getZkNamespace()
        hiveServer2Url = "jdbc:hive2://" + zkquorumwithport + (
            "/;serviceDiscoveryMode=%s;zooKeeperNamespace=%s" % (cls.getServiceDiscoveryMode(), zknamespace)
        )
        isSSL = cls.getConfigValue("hive.server2.use.SSL", defaultValue="false")
        if isSSL == "true" and ssl:
            trustStorePath = "/etc/security/serverKeys/hivetruststore.jks"
            trustStorePwd = "changeit"
            hiveServer2Url = "%s;ssl=true;sslTrustStore=%s;trustStorePassword=%s" % (
                hiveServer2Url, trustStorePath, trustStorePwd
            )
        return hiveServer2Url

    @classmethod
    @TaskReporter.report_test()
    def runQueryOnBeelineHelper(
            cls,
            query,
            cmd,
            readFromFile=False,
            hiveconf={},
            hivevar={},
            option={},
            user=None,
            passwd="pwd",
            showHeaders=False,
            cwd=None,
            env=None,
            logoutput=False,
            background=False,
            queryIsFile=False
    ):
        cmd += cls.buildcmd(
            query,
            hiveconf=hiveconf,
            hivevar=hivevar,
            option=option,
            readFromFile=readFromFile,
            queryIsFile=queryIsFile
        )
        retval = cls.runCommandAs(
            user, cmd, cwd=cwd, env=env, logoutput=logoutput, stderr_as_stdout=False, background=background
        )
        if background:
            return retval
        else:
            retcode, stdout, stderr = retval
            stdoutlist = stdout.split("\n")
            if not showHeaders and len(stdoutlist) > 0:
                stdoutlist = stdoutlist[1:]
            pattern1 = re.compile(r"'\t'")
            pattern2 = re.compile(r"(^'|'$)")
            for i in range(len(stdoutlist)):
                stdoutlist[i] = re.sub(pattern1, "\t", stdoutlist[i])
                stdoutlist[i] = re.sub(pattern2, "", stdoutlist[i])
            return retcode, "\n".join(stdoutlist), stderr

    @classmethod
    @TaskReporter.report_test()
    def runQueryOnBeelineViaDynamicServiceDiscovery(
            cls,
            query,
            zkquorum,
            zknamespace,
            readFromFile=False,
            hiveconf={},
            hivevar={},
            option={},
            user=None,
            passwd="pwd",
            showHeaders=False,
            cwd=None,
            env=None,
            logoutput=False,
            background=False,
            queryIsFile=False,
            httpmode=False
    ):
        cmd = cls.getBeelineCmd()
        if user is None: user = Config.getEnv('USER')
        cmd += " -n %s -p %s -u \"%s\" --outputformat=tsv" % (
            user, passwd, cls.getHiveServer2UrlForDynamicServiceDiscovery(zkquorum, zknamespace, httpmode=httpmode)
        )
        return cls.runQueryOnBeelineHelper(
            query,
            cmd,
            readFromFile=readFromFile,
            hiveconf=hiveconf,
            hivevar=hivevar,
            option=option,
            user=user,
            passwd=passwd,
            showHeaders=showHeaders,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            background=background,
            queryIsFile=queryIsFile
        )

    @classmethod
    @TaskReporter.report_test()
    def runQueryOnBeeline(
            cls,
            query,
            readFromFile=False,
            hiveconf={},
            hivevar={},
            option={},
            user=None,
            passwd="pwd",
            showHeaders=False,
            cwd=None,
            env=None,
            logoutput=False,
            direct=False,
            background=False,
            queryIsFile=False,
            httpmode=False,
            jdbcUrl=None
    ):
        if jdbcUrl is None and cls.isDynamicServiceDiscoveryOn() and not direct:
            zkquorum = cls.getConfigValue("hive.zookeeper.quorum")
            zkquorumwithport = cls._addPortToZkquorum(zkquorum)
            zknamespace = cls.getZkNamespace()
            return cls.runQueryOnBeelineViaDynamicServiceDiscovery(
                query,
                zkquorum,
                zknamespace,
                readFromFile=readFromFile,
                hiveconf=hiveconf,
                hivevar=hivevar,
                option=option,
                user=user,
                passwd=passwd,
                showHeaders=showHeaders,
                cwd=cwd,
                env=env,
                logoutput=logoutput,
                background=background,
                queryIsFile=queryIsFile,
                httpmode=httpmode
            )
        cmd = cls.getBeelineCmd()
        if user is None: user = Config.getEnv('USER')
        if jdbcUrl is None: jdbcUrl = cls.getHiveServer2Url(direct=direct, httpmode=httpmode)
        cmd += " -n %s -p %s -u \"%s\" --outputformat=tsv" % (user, passwd, jdbcUrl)
        return cls.runQueryOnBeelineHelper(
            query,
            cmd,
            readFromFile=readFromFile,
            hiveconf=hiveconf,
            hivevar=hivevar,
            option=option,
            user=user,
            passwd=passwd,
            showHeaders=showHeaders,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            background=background,
            queryIsFile=queryIsFile
        )

    @classmethod
    @TaskReporter.report_test()
    def getHiveLogDir(cls, logoutput=False):
        if cls._hiveLogDir is None:
            properties = Ambari.getConfig('hive-env')
            cls._hiveLogDir = properties['hive_log_dir']
        if logoutput:
            logger.info("Hive.getHiveLogDir returns %s" % cls._hiveLogDir)
        return cls._hiveLogDir

    @classmethod
    def getServiceLog(cls, service):
        return os.path.join(cls.getHiveLogDir(), cls._compLogFileMap[service])

    @classmethod
    @TaskReporter.report_test()
    def startService(cls, services=['metastore', 'hiveserver2'], service_hosts=[], waitForPortToOpen=True):
        for service in services:
            if not service_hosts:
                hosts = cls.getServiceHosts(service)
            else:
                hosts = service_hosts
            port = cls.getServicePort(service)
            for host in hosts:
                Ambari.startComponent(
                    host, cls._compSvcMap[service], waitForCompletion=True, timeout=cls._compSvcTimeouts[service]
                )
                if waitForPortToOpen:
                    logger.info("Wait for port %s to open on host \"%s\"" % (port, host))
                    assert util.waitForPortToOpen(
                        host, int(port), timeout=720
                    ), "Service %s failed to start on host \"%s\"" % (
                        service, host
                    )  #increase timeout due to BUG-99441
        time.sleep(30)

    @classmethod
    @TaskReporter.report_test()
    def stopService(cls, services=['metastore', 'hiveserver2']):
        for service in services:
            hosts = cls.getServiceHosts(service)
            for host in hosts:
                Ambari.stopComponent(
                    host, cls._compSvcMap[service], waitForCompletion=True, timeout=cls._compSvcTimeouts[service]
                )
            cls._postStopAction(service)
        # Workaround for BUG-73424
        time.sleep(90)

    @classmethod
    def _postStopAction(cls, service):
        pass

    @classmethod
    def restartServices(cls, services=['metastore', 'hiveserver2'], waitForPortToOpen=True):
        cls.stopService(services=services)
        cls.startService(services=services, waitForPortToOpen=waitForPortToOpen)

    @classmethod
    @TaskReporter.report_test()
    def getStartingAmbariServiceConfigVersion(cls, update=False):
        if cls._ambariServiceConfigVersion is None or update:
            cls._ambariServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceName)
            cls._ambariTezServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariTezServiceName)
        return cls._ambariServiceConfigVersion

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(
            cls, changes, services=['metastore', 'hiveserver2'], env={}, restartService=True, waitForPortToOpen=True
    ):
        logger.info("Current Service Config Version: %s" % cls.getStartingAmbariServiceConfigVersion())
        for key, value in changes.items():
            if cls._ambariConfigMap.has_key(key):
                key = cls._ambariConfigMap[key]
            else:
                logger.warn("Unknown config \"%s\" change requested, ignoring" % key)
                continue
            Ambari.setConfig(key, value)
            for k, v in value.items():
                cls._modifiedConfigs[k] = v
        if len(env.keys()) > 0:
            key = cls._ambariConfigMap['hive-env.sh']
            envProps = Ambari.getConfig(key)
            if envProps.has_key("content"):
                content = envProps['content']
                for envKey, envVal in env.items():
                    content = "export %s=%s\n%s" % (envKey, envVal, content)
                Ambari.setConfig(key, {'content': content})
        if restartService:
            cls.restartServices(services=services, waitForPortToOpen=waitForPortToOpen)

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls, services=['metastore', 'hiveserver2'], restoreTez=False, waitForPortToOpen=True):
        resetVersion = cls.getStartingAmbariServiceConfigVersion()
        logger.info("Restoring Service Config Version to: %s" % resetVersion)
        Ambari.resetConfig(cls._ambariServiceName, resetVersion)
        if restoreTez and not cls.isHive2():
            Ambari.resetConfig(cls._ambariTezServiceName, cls._ambariTezServiceConfigVersion)
        cls._modifiedConfigs.clear()
        cls._hiveServer2Port = None
        if len(services) > 0:
            cls.restartServices(services=services, waitForPortToOpen=waitForPortToOpen)

    @classmethod
    def isConfigModified(cls):
        return len(cls._modifiedConfigs) > 0

    @classmethod
    def isTezEnabled(cls):
        return cls.getConfigValue("hive.execution.engine", 'tez') == 'tez'

    @classmethod
    def isLLAPEnabled(cls):
        return cls.getConfigValue("hive.execution.mode", 'container') == 'llap'

    @classmethod
    def isAcidEnabled(cls):
        return cls.getConfigValue("hive.txn.manager"
                                  ) == 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager' and cls.getConfigValue(
                                      "hive.support.concurrency"
                                  ) == 'true' and cls.getConfigValue("hive.compactor.initiator.on") == 'true'

    @classmethod
    def isHA(cls):
        return (len(cls.getServiceHosts('hiveserver2')) > 1)

    @classmethod
    def usesOracle(cls):
        return cls.getConfigValue("javax.jdo.option.ConnectionURL").find('oracle') != -1

    @classmethod
    @TaskReporter.report_test()
    def getMiscTestLogPaths(cls, logoutput=False):
        HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
        miscTestLogPaths = [
            os.path.join(HADOOPQE_TESTS_DIR, "datateamtest", "hcatalog", "testdist", "out", "out", "log"),
            os.path.join(HADOOPQE_TESTS_DIR, "datateamtest", "hcatalog", "testdist", "out", "out", "pigtest"),
            os.path.join(HADOOPQE_TESTS_DIR, "datateamtest", "hcatalog", "testdist", "benchmarks"),
            os.path.join(HADOOPQE_TESTS_DIR, "datateamtest", "hcatalog", "testdist", "data", "tpcds", "answers")
        ]

        if logoutput:
            logger.info("Hive.getMiscTestLogPaths returns %s" % str(miscTestLogPaths))
        return miscTestLogPaths

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        # determine the hive version from the jar that is deployed
        jarDir = os.path.join(cls.getHiveHome(), 'lib')
        files = util.findMatchingFiles(jarDir, "hive-metastore-*.jar")
        p = re.compile('hive-metastore-(\S+).jar')
        m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    @TaskReporter.report_test()
    def getDatabaseFlavor(cls):
        dbdriver = cls.getConfigValue("javax.jdo.option.ConnectionDriverName")
        if dbdriver.find("oracle") != -1:
            return "oracle"
        elif dbdriver.find("postgresql") != -1:
            dbUrl = cls.getConfigValue("javax.jdo.option.ConnectionURL")
            m = re.search("jdbc:postgresql://(.*):.*", dbUrl)
            dbHost = Machine.getfqdn()
            if m and m.group(1):
                dbHost = m.group(1)
            dbVersion = Machine.getDBVersion('postgres', host=dbHost)
            if dbVersion:
                return "postgres-%s" % dbVersion
            else:
                return "postgres"
        elif dbdriver.find("derby") != -1:
            return "derby"
        elif dbdriver.find("mysql") != -1:
            return "mysql"
        return ""

    #### Methods used for Rolling Upgrade ####
    @classmethod
    @TaskReporter.report_test()
    def backupMetastoreDB(cls, backupfile):
        dbflavor = cls.getDatabaseFlavor()
        jdbcUrl = cls.getConfigValue("javax.jdo.option.ConnectionURL")
        if dbflavor == 'mysql':
            m = re.search("jdbc:mysql://.*/([^?]*)", jdbcUrl)
            if m:
                dbname = m.group(1)
                from beaver.dbutil import MySQL
                logger.info("Backing up the Hive metastore database [%s]" % dbname)
                MySQL.backupDatabase(dbname, backupfile)
            else:
                logger.info("Failed to extract the database name from the JDBC connection url")
        elif dbflavor.startswith('postgres'):
            m = re.search("jdbc:postgresql://.*/([^?]*)", jdbcUrl)
            if m:
                dbname = m.group(1)
                from beaver.dbutil import Postgres
                logger.info("Backing up the Hive metastore database [%s]" % dbname)
                Postgres.backupDatabase(dbname, backupfile)
            else:
                logger.info("Failed to extract the database name from the JDBC connection url")
        elif dbflavor == 'oracle':
            from beaver.dbutil import Oracle
            hiveuser = cls.getConfigValue("javax.jdo.option.ConnectionUserName")
            hivepasswd = cls.getConfigValue("javax.jdo.option.ConnectionPassword", defaultValue="hive")
            Oracle.backupSchema(backupfile, hiveuser, hivepasswd)
        else:
            logger.info("Not implemented for DB flavor: %s" % dbflavor)

    @classmethod
    @TaskReporter.report_test()
    def restoreMetastoreDB(cls, backupfile):
        dbflavor = cls.getDatabaseFlavor()
        jdbcUrl = cls.getConfigValue("javax.jdo.option.ConnectionURL")
        hiveuser = cls.getConfigValue("javax.jdo.option.ConnectionUserName")
        if dbflavor == 'mysql':
            m = re.search("jdbc:mysql://.*/([^?]*)", jdbcUrl)
            if m:
                dbname = m.group(1)
                from beaver.dbutil import MySQL
                logger.info("Recreating the database [%s]" % dbname)
                MySQL.recreateDatabase(dbname)
                logger.info("Grant all privileges to user [%s] on database [%s]" % (hiveuser, dbname))
                MySQL.grantAllPrivileges(hiveuser, host='%', database=dbname)
                MySQL.flushPriveleges()
                logger.info("Restoring the backup from file '%s'" % backupfile)
                MySQL.runAsRoot("source " + backupfile, database=dbname)
            else:
                logger.info("Failed to extract the database name from the JDBC connection url")
        elif dbflavor.startswith('postgres'):
            m = re.search("jdbc:postgresql://.*/([^?]*)", jdbcUrl)
            if m:
                dbname = m.group(1)
                from beaver.dbutil import Postgres
                logger.info("Recreating the database [%s]" % dbname)
                Postgres.recreateDatabase(dbname)
                logger.info("Grant all privileges to user [%s] on database [%s]" % (hiveuser, dbname))
                Postgres.grantAllPrivileges(hiveuser, dbname)
                logger.info("Restoring the backup from file '%s'" % backupfile)
                Postgres.importDmp(backupfile, dbname)
            else:
                logger.info("Failed to extract the database name from the JDBC connection url")
        elif dbflavor == "oracle":
            from beaver.dbutil import Oracle
            hiveuser = cls.getConfigValue("javax.jdo.option.ConnectionUserName")
            hivepasswd = cls.getConfigValue("javax.jdo.option.ConnectionPassword", defaultValue="hive")
            logger.info("Restoring the backup from file '%s'" % backupfile)
            Oracle.importDmp(backupfile, fromuser=hiveuser, touser=hiveuser, touserpasswd=hivepasswd)
        else:
            logger.info("Not implemented for DB flavor: %s" % dbflavor)

    @classmethod
    @TaskReporter.report_test()
    def upgradeSchema(cls):
        hiveHome = cls.getHiveHome(service="metastore")
        hiveUser = Config.get('hive', 'HIVE_USER')
        hiveHost = cls.getHiveHost()
        dbflavor = cls.getDatabaseFlavor()
        if dbflavor.startswith("postgres"): dbflavor = "postgres"
        upCmd = os.path.join(hiveHome, "bin", "hive") + " --service schemaTool -dbType %s -upgradeSchema" % dbflavor
        return Machine.runas(hiveUser, upCmd, host=hiveHost)

    @classmethod
    @TaskReporter.report_test()
    def deregisterHiveServer2(cls, version=None):
        if cls.isDynamicServiceDiscoveryOn():
            hiveHome = cls.getHiveHome(service="hiveserver2")
            hiveUser = Config.get('hive', 'HIVE_USER')
            if not version: version = cls.getVersion()
            deregCmd = os.path.join(hiveHome, "bin", "hive") + " --service hiveserver2 --deregister %s" % version
            return Machine.runas(hiveUser, deregCmd)

    @classmethod
    @TaskReporter.report_test()
    def getConfigValueViaAmbari(cls, type, property_name, cluster=None):
        type_config = Ambari.getConfig(type, service='HIVE')
        param_vale = type_config[property_name]
        return param_vale


class Hive2(Hive1):
    _hiveServerInteractiveHosts = None
    _compSvcMap = {'metastore': 'HIVE_METASTORE', 'hiveserver2': 'HIVE_SERVER_INTERACTIVE'}
    _compSvcTimeouts = {'metastore': 300, 'hiveserver2': 900}
    _ambariConfigMap = {
        'hive-site.xml': 'hive-site',
        'hiveserver2-site.xml': 'hiveserver2-interactive-site',
        'hive-env.sh': 'hive-interactive-env',
        'tez-site.xml': 'tez-interactive-site',
        'ranger-hive-security.xml': 'ranger-hive-security',
        'hive-interactive-site.xml': 'hive-interactive-site'
    }
    _compLogFileMap = {'metastore': 'hivemetastore.log', 'hiveserver2': 'hiveserver2Interactive.log'}
    if HDP_Version == '2.6.5':
        _configDir = '/etc/hive2/conf/conf.server'
    else:
        _configDir = '/etc/hive_llap/conf'
    _localConfDir = "hive2_conf"
    _isHive2 = True
    __name__ = "Hive2"

    @classmethod
    @TaskReporter.report_test()
    def getHiveHome(cls, service="client"):
        if service == 'metastore':
            hiveHome = Hive1.getHiveHome(service='metastore')
        else:
            hiveHome = cls._hiveHome
        return hiveHome

    @classmethod
    @TaskReporter.report_test()
    def getHiveserver2Hosts(cls):
        if cls._hiveServerInteractiveHosts is None:
            cls._hiveServerInteractiveHosts = Ambari.getHostsForComponent(cls._compSvcMap['hiveserver2'])
        return cls._hiveServerInteractiveHosts

    @classmethod
    def getHiveHost(cls, service='hiveserver2'):
        return cls.getServiceHosts(service)[0]

    @classmethod
    @TaskReporter.report_test()
    def getZkNamespace(cls):
        if not cls.isHA():
            zknamespace = cls.getConfigValue("hive.server2.zookeeper.namespace")
        else:
            zknamespace = cls.getConfigValue("hive.server2.active.passive.ha.registry.namespace")
        return zknamespace

    @classmethod
    def isHA(cls):
        return cls.getConfigValue("hive.server2.interactive.ha.enable", defaultValue="false").lower() == "true"

    @classmethod
    @TaskReporter.report_test()
    def getServiceDiscoveryMode(cls):
        if cls.isHA():
            return 'zooKeeperHA'
        else:
            return 'zooKeeper'

    @classmethod
    @TaskReporter.report_test()
    def getHSIInstances(cls):
        """
    Returns the list of active and passive hive server interactive instances.
    """

        hsi_dict = {'active': [], 'passive': []}
        zkhosts = Zookeeper.getZKHosts()
        if Hadoop.isSecure():
            zNode = 'hs2ActivePassiveHA-sasl'
        else:
            zNode = 'hs2ActivePassiveHA-unsecure'
        exit_code, stdout = Zookeeper.runZKCli("ls /%s/instances" % zNode, server=zkhosts[0])
        instances = stdout.split('\n')[-1]
        logger.info(instances)
        if instances == '[]':
            return hsi_dict
        for each_bracket in ['[', ']']:
            instances = instances.replace(each_bracket, '')
        instances_list = instances.split(', ')
        for each_instance in instances_list:
            exit_code, out = Zookeeper.runZKCli("get /%s/instances/%s" % (zNode, each_instance), server=zkhosts[0])
            for line in out.split("\n"):
                match = re.search(".*JSONServiceRecord.*", line)
                if match:
                    json_data = line
                    break
            instance_dict = ast.literal_eval(json_data)
            instance_host = instance_dict['hive.server2.thrift.bind.host']
            if instance_dict['internal'][0]['api'] == 'passiveEndpoint':
                hsi_dict['passive'].append(instance_host)
            else:
                hsi_dict['active'].append(instance_host)
        logger.info(hsi_dict)
        return hsi_dict

    @classmethod
    @TaskReporter.report_test()
    def killHSIProcess(cls, host):
        """
    Kill the hive server interactive instance on the specified host.
    """

        proc_llap_pids = Machine.getProcessListRemote(
            host, format="%U %p %P %a", filter="hiveserver2Interactive", logoutput=True
        )
        logger.info(proc_llap_pids)
        if len(proc_llap_pids) != 0:
            proc_llap_pid = Machine.getPidFromString(proc_llap_pids[0], Config.get('hive', 'HIVE_USER'))
            logger.info(proc_llap_pid)
            if proc_llap_pid:
                logger.info("Found hiveserver2Interactive process with PID %s on %s", proc_llap_pid, host)
                Machine.killProcessRemote(proc_llap_pid, host=host, user=Machine.getAdminUser())
                time.sleep(2)

    @classmethod
    @TaskReporter.report_test()
    def switchOverHSI(cls):
        """
    Kill the active hive server interactive instance so that the passive instance takes the leadership.
    """

        logger.info("Getting the Active and Passive hive server interactive instances.")
        hsi_instances = cls.getHSIInstances()
        logger.info(["Killing the active hive server interactive process at host %s.", hsi_instances['active'][0]])
        cls.killHSIProcess(hsi_instances['active'][0])
        logger.info("The hive server interactive process has been killed. Waiting for switch over to complete.")
        time.sleep(120)
        logger.info("Checking if the Active hive server interactive instance has changed.")
        hsi_updated = cls.getHSIInstances()
        if hsi_updated['active'] != hsi_instances['active']:
            logger.info("The hive server interactive on %s host is Active now.", hsi_updated['active'])
            logger.info("Starting the hive server interactive instance on host %s", hsi_instances['active'])
            cls.startService(services=['hiveserver2'], service_hosts=hsi_instances['active'])
        else:
            assert 1 == 0, "The hive server interactive active instance switch over failed."

    @classmethod
    @TaskReporter.report_test()
    def _postStopAction(cls, service):
        if service == 'hiveserver2':
            logger.info("Hard kill Tez sessions")
            yarn_user = YARN.getYarnUser()
            apps = YARN.getApplicationIDList(state='NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING')
            if len(apps) > 0:
                for app in apps:
                    YARN.killApplicationAs(app, user=yarn_user)
                    time.sleep(5)
            logger.info("Hard kill the HS2 application if still running")
            admin_user = Machine.getAdminUser()
            hosts = cls.getServiceHosts(service)
            port = cls.getHiveserver2ThriftPort()
            for host in hosts:
                pid = Machine.getPIDByPort(port, host=host, user=admin_user)
                if pid:
                    logger.info("Found process for '%s' with PID %d" % (service, pid))
                    Machine.killProcessRemote(pid, host=host, user=admin_user)
                    time.sleep(2)
            logger.info("Hard Kill proc_llap daemon due to BUG-62657")
            allnodes = util.getAllNodes() if Machine.isHumboldt() else Hadoop.getAllNodes()
            for node in allnodes:
                proc_llap_pids = Machine.getProcessListRemote(
                    node, format="%U %p %P %a", filter="proc_llap", logoutput=True
                )
                if len(proc_llap_pids) != 0:
                    proc_llap_pid = Machine.getPidFromString(proc_llap_pids[0], yarn_user)
                    if proc_llap_pid:
                        logger.info("Found proc_llap process with PID %d on %s" % (proc_llap_pid, node))
                        Machine.killProcessRemote(proc_llap_pid, host=node, user=admin_user)
                        time.sleep(2)

    @classmethod
    @TaskReporter.report_test()
    def restartLLAP(cls):
        cluster = Ambari.getClusterName()
        data = '{"RequestInfo":{"context":"Restart LLAP","command":"RESTART_LLAP"},"Requests/resource_filters":[{"service_name":"HIVE","component_name":"HIVE_SERVER_INTERACTIVE","hosts":"%s"}]}' % (
            cls.getHiveHost()
        )
        url = '/api/v1/clusters/' + cluster + '/requests'
        r = Ambari.http_put_post_request(url, data, 'POST')
        assert (r.status_code == 200 or r.status_code == 201 or r.status_code == 202
                ), "Failed to restart LLAP  on host %s, status=%d" % (cls.getHiveHost(), r.status_code)
        time.sleep(240)


if Config.get('hive', 'HIVE2', 'False') == "True":
    Hive = Hive2()
else:
    Hive = Hive1()
