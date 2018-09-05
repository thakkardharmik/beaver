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
import time
import logging
import re
import ntpath
import string
import random

from taskreporter.taskreporter import TaskReporter
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.slider import Slider
from beaver.component.ambari import Ambari
from beaver.component.xa import Xa
from beaver import util
from beaver import configUtils


logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')

if Machine.isIBMPower():
    try:
        logger.info("Updating ruby file for shell wrapping issue")
        hbaseFormatterPath = os.path.join(Config.get('hbase', 'HBASE_HOME'), "lib", "ruby", "shell")
        files = util.findMatchingFiles(hbaseFormatterPath, "formatter.rb")
        logger.info("Files are: %s", "".join(files))
        Machine.runas(
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd(),
            cmd="sed -i -- 's/Java::jline.Terminal.getTerminal().getTerminalWidth()/0/g' " + files[0]
        )
    except Exception as e:
        logger.info("#####Failed to change formatter for POWER tests######")

if Hadoop.isSecure():
    _keytabFile = Machine.getHeadlessUserKeytab(HADOOPQA_USER)
    _kinitloc = Machine.which("kinit", "root")
    _, _ = Machine.run(
        cmd="%s  -k -t %s %s" % (_kinitloc, _keytabFile, Machine.get_user_principal(HADOOPQA_USER)))


class HBase(object):
    _regionServers = []
    _RSListFromStatus = []
    _masterNode = None
    _allMasterNodes = None
    _phoenixQueryServers = []
    _clientNodes = []
    _cluster = Ambari.getClusterName()

    _ambariConfigMap = {'hbase-site.xml': 'hbase-site', 'hbase-env.sh': 'hbase-env', 'log4j.properties': 'hbase-log4j'}
    _ambariServiceName = 'HBASE'
    _ambariServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(_ambariServiceName)
    _ambariComponentMapping = {
        'hbase_master': 'HBASE_MASTER',
        'hbase_regionserver': 'HBASE_REGIONSERVER',
        'hbase_client': 'HBASE_CLIENT',
        'phoenix_query_server': 'PHOENIX_QUERY_SERVER'
    }

    _hbase_user = Config.get('hbase', 'HBASE_USER')
    _conf_dir = Config.get('hbase', 'HBASE_CONF_DIR')
    _pid_dir = None
    _hbaseLogDir = None
    if Machine.type() == 'Linux':
        hbaseEnvFile = os.path.join(_conf_dir, 'hbase-env.sh')
        if os.path.exists(hbaseEnvFile):
            _pid_dir = util.getPropertyValueFromFile(hbaseEnvFile, 'HBASE_PID_DIR')
            _hbaseLogDir = util.getPropertyValueFromFile(hbaseEnvFile, 'HBASE_LOG_DIR')
    elif Machine.type() == 'Windows':
        _hbaseLogDir = os.getenv('HBASE_LOG_DIR')

    @classmethod
    def run(cls, cmd, env=None, logoutput=True):
        return cls.runas(user=None, cmd=cmd, env=env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True, disableAuth=False, configPath=None, host=None):
        configClause = ""
        kinitCmd = ""
        if not configPath is None:
            configClause = " --config " + configPath
        if not env:
            env = {}
        # if disable auth is requsted set the env
        # var to empty only for a secure cluster
        if disableAuth and Hadoop.isSecure():
            env['KRB5CCNAME'] = ''
        # if disableAuth is false and we are running a secure cluster get
        # the credentials
        elif Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            if host:
                kinitCmd = "kinit -k -t " + \
                    Machine.getHeadlessUserKeytab(
                        user) + " " + user + "@" + Machine.get_user_realm() + "; "
            user = None
        if host:
            exit_code, stdout = Machine.runas(
                user,
                kinitCmd + Config.get('hbase', 'HBASE_CMD') + configClause + " " + cmd,
                host=host,
                env=env,
                logoutput=logoutput
            )
        else:
            exit_code, stdout = Machine.runas(
                user, Config.get('hbase', 'HBASE_CMD') + configClause + " " + cmd, env=env, logoutput=logoutput
            )
        return exit_code, re.sub(re.compile(r'\s+$', re.MULTILINE), '', stdout)

    @classmethod
    @TaskReporter.report_test()
    def get_kerberos_ticket(cls, user):
        '''
        Create a cached Kerberos ticket for the service user instead of the test user. Useful
        for MIT+AD deployments.
        '''
        # Copied from machine.py, uses the service keytab and realm instead of user variants
        kinitloc = Machine.getKinitCmd()
        kerbTicketDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'kerberosServiceTickets')
        if not os.path.exists(kerbTicketDir):
            os.mkdir(kerbTicketDir)
        kerbTicket = os.path.join(kerbTicketDir, "%s.mit.kerberos.ticket" % user)

        # If it already exists, return it
        if os.path.isfile(kerbTicket):
            return kerbTicket

        keytabFile = os.path.join(Machine.getServiceKeytabsDir(), "%s.headless.keytab" % user)
        if not os.path.isfile(keytabFile):
            raise ValueError('Could not find expected keytab: %s' % keytabFile)

        # Make sure we can actually read the keytab
        Machine.chmod('444', keytabFile, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

        # Get service username from keytab file
        cmd = "%s -k -t %s " % ("klist", keytabFile)
        exit_code, output = Machine.run(cmd)
        user = str(((output.split(os.linesep)[-1]).split(' ')[-1]))
        logger.info("Username is %s", user)

        # Assumption that service keytabs are always EXAMPLE.COM. Cache the ticket.
        cmd = "%s -c %s -k -t %s %s" % (kinitloc, kerbTicket, keytabFile, '%s' % user)
        exit_code, output = Machine.run(cmd)
        if exit_code == 0:
            return kerbTicket
        return ""

    @classmethod
    @TaskReporter.report_test()
    def runHBCKas(cls, user, cmd, env=None, logoutput=True, disableAuth=False, configPath=None):
        configClause = ""
        if not configPath is None:
            configClause = " --config " + configPath
        if not env:
            env = {}
        # if disable auth is requsted set the env
        # var to empty only for a secure cluster
        if disableAuth and Hadoop.isSecure():
            env['KRB5CCNAME'] = ''
        # if disableAuth is false and we are running a secure cluster get
        # the credentials
        elif Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            # AD+MIT
            if Config.get('machine', 'USER_REALM') != Config.get('machine', 'REALM'):
                kerbTicket = HBase.get_kerberos_ticket(user)
            # AD+AD or MIT
            else:
                kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

            # Handle missing jaas issue
            hbase_rs_jaas = os.path.join(cls._conf_dir, "hbase_regionserver_jaas.conf")
            hbase_service_keytab = os.path.join(Machine.getServiceKeytabsDir(), "hbase.service.keytab")
            rshost = cls.getRegionServers()[-1]
            # 1. if master jaas exists AND we're on Gateway is Master,
            #    we override HBASE_SERVER_JAAS_OPTS to point to it via environmental variable
            # 2. otherwise, we copy over the region server jaas file
            CLIENT_JAAS = os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), 'hbase_client_jaas.conf')
            localHostName = Machine.getfqdn()

            if os.path.exists(CLIENT_JAAS) and localHostName in HBase.getAllMasterNodes():
                env['HBASE_SERVER_JAAS_OPTS'] = "-Djava.security.auth.login.config=" + CLIENT_JAAS
            else:
                if not os.path.exists(hbase_rs_jaas):
                    Machine.copyToLocal(
                        user=Machine.getAdminUser(),
                        host=rshost,
                        srcpath=hbase_rs_jaas,
                        destpath=hbase_rs_jaas,
                        passwd=Machine.getAdminPasswd()
                    )
                    Machine.chmod(
                        perm="777",
                        filepath=hbase_rs_jaas,
                        user=Machine.getAdminUser(),
                        passwd=Machine.getAdminPasswd()
                    )

                if not os.path.exists(hbase_service_keytab):
                    Machine.copyToLocal(
                        user=Machine.getAdminUser(),
                        host=rshost,
                        srcpath=hbase_service_keytab,
                        destpath=hbase_service_keytab,
                        passwd=Machine.getAdminPasswd()
                    )
                Machine.chmod(
                    perm="777",
                    filepath=hbase_service_keytab,
                    user=Machine.getAdminUser(),
                    passwd=Machine.getAdminPasswd()
                )
        exit_code, stdout = Machine.runas(
            user, Config.get('hbase', 'HBASE_CMD') + configClause + " " + cmd, env=env, logoutput=logoutput
        )
        return exit_code, re.sub(re.compile(r'\s+$', re.MULTILINE), '', stdout)

    @classmethod
    def runRubyScript(cls, scriptfile, logoutput=False):
        return cls.run("org.jruby.Main " + scriptfile, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def runShellCmds(
            cls, cmds, logoutput=True, user=None, disableAuth=False, configPath=None, host=None,
            returnValuesOption=False
    ):
        tfpath = cls.getTempFilepath()
        shellOptions = " -n "
        if returnValuesOption:
            shellOptions = shellOptions + " -r"
        logger.info("Writing commands to file %s", tfpath)
        tf = open(tfpath, 'w')
        for cmd in cmds:
            logger.info(" '%s' ", cmd)
            tf.write(cmd + "\n")
        # tf.write("exit\n")
        tf.close()
        logger.info("Done writing commands to file. Will execute them now.")
        if host:
            logger.info("Copying file to remote host: %s", str(host))
            head, tail = ntpath.split(tfpath)
            filename = tail or ntpath.basename(head)
            tfpath_remote = os.path.join(Machine.getTempDir(), filename)
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=host,
                srcpath=tfpath,
                destpath=tfpath_remote,
                passwd=Machine.getAdminPasswd()
            )
            output = cls.runas(
                user,
                "shell %s < %s" % (shellOptions, tfpath_remote),
                logoutput=logoutput,
                disableAuth=disableAuth,
                configPath=configPath,
                host=host
            )
            Machine.rm(filepath=tfpath_remote, user=Machine.getAdminUser(), host=host, passwd=Machine.getAdminPasswd())
        else:
            output = cls.runas(
                user,
                "shell %s < %s" % (shellOptions, tfpath),
                logoutput=logoutput,
                disableAuth=disableAuth,
                configPath=configPath
            )
        if Xa.isArgus() is True:
            for cmd in cmds:
                if 'grant' in cmd or 'revoke' in cmd:
                    cls.waitForXAPluginToLoadPolicies(35)
                    break  # wait only once if grant or revoke command in cmds
        os.remove(tfpath)
        return output

    @classmethod
    def waitForXAPluginToLoadPolicies(cls, sleeptime=35):
        # sleeping for XA plugin to load policies from XA Policy Admin
        time.sleep(sleeptime)

    @classmethod
    def dropTable(cls, tablename, logoutput=True, user=None):
        return cls.runShellCmds(["disable '%s'" % tablename, "drop '%s'" % tablename], logoutput, user=user)

    @classmethod
    @TaskReporter.report_test()
    def disableAndDropAllTables(cls, logoutput=True, user=None):
        disableAndDropAllTablesCmd = "list.each {|t| disable t; drop t};"
        return cls.runShellCmds([disableAndDropAllTablesCmd], logoutput=logoutput, user=user, returnValuesOption=True)

    @classmethod
    def createSnapshot(cls, tablename, snapshotname, logoutput=True, user=None):
        return cls.runShellCmds(["snapshot '%s', '%s'" % (tablename, snapshotname)], logoutput, user=user)

    @classmethod
    def random_table_name_generator(cls, size=6, chars=string.ascii_lowercase + string.digits):
        return 'table_' + ''.join(random.choice(chars) for x in range(size))

    @classmethod
    def random_namespace_name_generator(cls, size=6, chars=string.ascii_lowercase + string.digits):
        return 'namespace_' + ''.join(random.choice(chars) for x in range(size))

    @classmethod
    @TaskReporter.report_test()
    def rowCount(cls, tablename):
        exit_code, stdout = cls.runShellCmds(["count '%s'" % tablename], logoutput=False)
        if exit_code == 0:
            pattern = re.compile(r"(\d+) row")
            m = pattern.search(stdout)
            if m:
                return int(m.group(1))
        return -1

    @classmethod
    @TaskReporter.report_test()
    def createTable(cls, tablename, columnFamily=None, logoutput=True, user=None):
        createcmd = "create '%s'" % tablename
        if columnFamily:
            createcmd += ", '%s'" % columnFamily
        return cls.runShellCmds([createcmd], logoutput, user=user)

    @classmethod
    @TaskReporter.report_test()
    def dropAndCreateTable(cls, tablename, columnFamily=None, logoutput=True, user=None):
        cls.dropTable(tablename, logoutput, user)
        cls.createTable(tablename, columnFamily, logoutput, user)

    @classmethod
    @TaskReporter.report_test()
    def getTableColumnValues(cls, tablename, columnFamily, column):
        tfpath = cls.getTempFilepath()
        if Machine.type() == 'Windows':
            column = "\"" + column + "\""
        exit_code, output = cls.runRubyScript(
            " ".join([os.path.join(CWD, 'read_data.rb'), tablename, columnFamily, column, tfpath])
        )
        if exit_code != 0:
            return []
        output = open(tfpath).readlines()
        os.remove(tfpath)
        return output

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), "hbase-site.xml"),
            propertyValue,
            defaultValue=defaultValue
        )

    @classmethod
    def getHMasterHttpAddress(cls):
        return cls.getMasterNode() + ":" + cls.getConfigValue("hbase.regionserver.info.port", "16010")

    @classmethod
    @TaskReporter.report_test()
    def isHTTPSEnabled(cls):
        return cls.getConfigValue("hbase.ssl.enabled") == "true"

    @classmethod
    @TaskReporter.report_test()
    def getHTTPPrefix(cls):
        if cls.isHTTPSEnabled():
            return "https://"
        else:
            return "http://"

    # method to get jmx metrics from the HBase Mater
    @classmethod
    @TaskReporter.report_test()
    def getJMXPropValueFromHMaster(cls, propertyName):
        hmaster_http_address = cls.getHMasterHttpAddress()

        urlContent = util.getURLContents(cls.getHTTPPrefix() + hmaster_http_address + "/jmx")
        m = re.search(".*" + propertyName + ".*: [0-9]{1,}", urlContent)
        if m is None:
            return 0
        m = re.search("[0-9]{1,}", m.group())
        # logger.info("URL content %s" % urlContent)
        return int(m.group().strip())

    # method to get jmx metrics from the HBase Mater
    @classmethod
    @TaskReporter.report_test()
    def getJMXPropValueFromHRegionServer(cls, name, keys, rshost=None):
        if rshost is None:
            rshost = cls.getRegionServers()[0]
        rs_http_address = rshost + ":" + \
            cls.getConfigValue("hbase.regionserver.info.port", "16030")
        jmx_data = util.getJSONContent(cls.getHTTPPrefix() + rs_http_address + "/jmx", kerberosAuthDisabled=True)
        return util.parseHadoopJMX(jmx_data, name, keys)

    @classmethod
    @TaskReporter.report_test()
    def daemon(
            cls, user, host, service, action, config=None, homePath=None, binFolder='bin', daemonName='hbase-daemon'
    ):
        if homePath is None:
            homePath = Config.get('hbase', 'HBASE_HOME')
        cmd = os.path.join(homePath, binFolder, daemonName)
        if Machine.type() == 'Linux':
            cmd += ".sh"
        if config:
            cmd += " --config " + config
        if service == "rest":
            cmd += " %s %s -p %d --infoport %d" % (action, service, cls.getRestServicePort(), cls.getRestInfoPort())
        else:
            cmd += " %s %s" % (action, service)
        return Machine.runas(user, cmd, host=host)

    @classmethod
    @TaskReporter.report_test()
    def resetService(
            cls, user, host, service, action, config=None, homePath=None, binFolder='bin', daemonName='hbase-daemon'
    ):
        if Machine.type() == 'Windows':
            Machine.service(service, action, host=host)
        else:
            cls.daemon(user, host, service, action, config, homePath, binFolder, daemonName)

    @classmethod
    @TaskReporter.report_test()
    def resetHBaseServiceThroughAmbari(cls, host, service, action, waitForCompletion=True, AmbariHost=None):
        clusterName = cls._cluster
        if AmbariHost:
            clusterName = Ambari.getClusterName(weburl=Ambari.getWebUrl(hostname=AmbariHost))
        if service == "master":
            if action == "stop":
                Ambari.stopComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_master'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
            elif action == "start":
                Ambari.startComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_master'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
        elif service == "regionserver":
            if action == "stop":
                Ambari.stopComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_regionserver'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
            elif action == "start":
                Ambari.startComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_regionserver'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
        elif service == "phoenix_queryserver":
            if action == "stop":
                Ambari.stopComponent(
                    host=host,
                    component=cls._ambariComponentMapping['phoenix_query_server'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
            elif action == "start":
                Ambari.startComponent(
                    host=host,
                    component=cls._ambariComponentMapping['phoenix_query_server'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
        elif service == "hbase_client":
            if action == "stop":
                Ambari.stopComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_client'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )
            elif action == "start":
                Ambari.startComponent(
                    host=host,
                    component=cls._ambariComponentMapping['hbase_client'],
                    cluster=clusterName,
                    waitForCompletion=waitForCompletion,
                    weburl=Ambari.getWebUrl(hostname=AmbariHost)
                )

    @classmethod
    @TaskReporter.report_test()
    def resetRestService(cls, action, user=None, host=None, config=None):
        if not Slider.isSlider():
            if user is None:
                user = Config.get('hbase', 'HBASE_USER')
            if host is None:
                host = cls.getMasterNode()

            if Config.get('machine', 'PLATFORM') == 'ASV' and not Machine.isHumboldt():
                cls.resetService(user, "localhost", "hbrest", action, config=config)
            else:
                cls.resetService(user, host, "rest", action, config=config)

            if action == "start":
                util.waitForPortToOpen(host, cls.getRestServicePort())
        else:
            logger.info("REST service reset skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def setupRestService(cls, master=None, config=None):
        if not master:
            master = HBase.getMasterNode()
        assert master, 'Could not find the hbase master node from the configs'
        rest_url = HBase.getHTTPPrefix() + master + ":%d" % HBase.getRestServicePort()
        if Config.get('machine', 'PLATFORM') == 'ASV' and not Machine.isHumboldt():
            rest_url = "http://localhost:%d" % HBase.getRestServicePort()
        if not config:
            config = Config.get('hbase', 'HBASE_CONF_DIR')
        restServerConfDir = None
        if Hadoop.isSecure():
            principal = HBase.getConfigValue('hbase.master.kerberos.principal')
            keytab = HBase.getConfigValue('hbase.master.keytab.file')
            NEW_CONF_DIR = os.path.join(Machine.getTempDir(), 'hbaseRestServerConf')
            configUtils.modifyConfig(
                {
                    'hbase-site.xml': {
                        'hbase.rest.keytab.file': keytab,
                        'hbase.rest.kerberos.principal': principal
                    },
                    'hbase-env.sh': {
                        'export HBASE_REST_OPTS=$HBASE_MASTER_OPTS': ''
                    }
                }, config, NEW_CONF_DIR, master.split(',')
            )
            restServerConfDir = NEW_CONF_DIR

        logger.info("Starting the rest server...")
        HBase.resetRestService("start", config=restServerConfDir)
        return restServerConfDir, rest_url

    @classmethod
    @TaskReporter.report_test()
    def getRestServicePort(cls):
        if Config.get('machine', 'PLATFORM') == 'ASV':
            return 8090
        if Machine.type() == "Windows" and not Slider.isSlider():
            return 8080
        else:
            hbase_site_file = os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), "hbase-site.xml")
            return int(util.getPropertyValueFromConfigXMLFile(hbase_site_file, 'hbase.rest.port', defaultValue=19000))

    @classmethod
    @TaskReporter.report_test()
    def getRestInfoPort(cls):
        if Config.get('machine', 'PLATFORM') == 'ASV':
            return 8095
        if Machine.type() == "Windows" and not Slider.isSlider():
            return 8085
        else:
            hbase_site_file = os.path.join(Config.get('hbase', 'HBASE_CONF_DIR'), "hbase-site.xml")
            return int(
                util.getPropertyValueFromConfigXMLFile(hbase_site_file, 'hbase.rest.info.port', defaultValue=19050)
            )

    @classmethod
    @TaskReporter.report_test()
    def resetThriftService(cls, action, user=None, host=None, config=None):
        if not Slider.isSlider():
            if user is None:
                user = Config.get('hbase', 'HBASE_USER')
            if host is None:
                host = cls.getMasterNode()
            cls.resetService(user, host, "thrift", action, config=config)
        else:
            logger.info("Thrift service reset skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def resetThrift2Service(cls, action, user=None, host=None):
        if not Slider.isSlider():
            if user is None:
                user = Config.get('hbase', 'HBASE_USER')
            if host is None:
                host = cls.getMasterNode()
            cls.resetService(user, host, "thrift2", action)
        else:
            logger.info("Thrift2 service reset skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def getMasterNode(cls, host=None):
        if not host and cls._masterNode:
            return cls._masterNode
        if host:
            weburl = Ambari.getWebUrl(hostname=host)
        else:
            weburl = None
        url = "/api/v1/clusters/%s/host_components?HostRoles/" \
              "component_name=HBASE_MASTER&metrics/hbase/master/IsActiveMaster=true" % (cls._cluster)
        response = Ambari.http_get_request(url, weburl=weburl)
        if response.status_code != 200:
            return None
        json = response.json()
        items = json['items']
        activeMaster = None
        for item in items:
            if item.has_key("HostRoles") and item["HostRoles"].has_key("host_name"):
                activeMaster = str(item["HostRoles"]["host_name"])
        if activeMaster:
            if not host and not cls._masterNode:
                cls._masterNode = activeMaster
            return activeMaster
        else:
            if host:
                logger.info("----- getMasterNode for host: %s", host)
                return cls.getAllMasterNodes(host=host)[0]
            elif not cls._masterNode:
                all_master_nodes = cls.getAllMasterNodes()
                if all_master_nodes is not None and all_master_nodes:
                    cls._masterNode = all_master_nodes[0]
            return cls._masterNode

    @classmethod
    @TaskReporter.report_test()
    def getAllMasterNodes(cls, host=None):
        if host:
            return Ambari.getHostsForComponent(
                cls._ambariComponentMapping['hbase_master'], weburl=Ambari.getWebUrl(hostname=host)
            )
        elif not cls._allMasterNodes:
            cls._allMasterNodes = Ambari.getHostsForComponent(cls._ambariComponentMapping['hbase_master'])
        return cls._allMasterNodes

    @classmethod
    @TaskReporter.report_test()
    def getPhoenixQueryServers(cls, host=None):
        if host:
            return Ambari.getHostsForComponent(
                cls._ambariComponentMapping['phoenix_query_server'], weburl=Ambari.getWebUrl(hostname=host)
            )
        elif not cls._phoenixQueryServers:
            cls._phoenixQueryServers = Ambari.getHostsForComponent(cls._ambariComponentMapping['phoenix_query_server'])
        return cls._phoenixQueryServers

    @classmethod
    @TaskReporter.report_test()
    def getHBaseClientNodes(cls, host=None):
        if host:
            return Ambari.getHostsForComponent(
                cls._ambariComponentMapping['hbase_client'], weburl=Ambari.getWebUrl(hostname=host)
            )
        elif not cls._clientNodes:
            cls._clientNodes = Ambari.getHostsForComponent(cls._ambariComponentMapping['hbase_client'])
        return cls._clientNodes

    @classmethod
    def snapshotAll(cls, user=None):
        """
        Description:
        Take snapshots for all tables
        """
        return cls.runShellCmds(["snapshot_all"], user=user, logoutput=False)

    @classmethod
    def snapshotRestore(cls, date, user=None):
        """
        Description:
        Restore snapshots taken on the given day
        Date is in 'YYYYmmdd' format
        """
        return cls.runShellCmds(["snapshot_restore '" + date + "'"], user=user, logoutput=False)

    @classmethod
    @TaskReporter.report_test()
    def getVersionFromBuild(cls):
        exit_code, output = cls.run("version", logoutput=False)
        if exit_code == 0:
            pattern = re.compile(r"HBase (\S+)")
            m = pattern.search(output)
            if m:
                logger.info("--- getVersionFromBuild: %s", m.group(1))
                return m.group(1)
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls, configPath=None):  # pylint: disable=inconsistent-return-statements
        exit_code, output = cls.runShellCmds(["status 'detailed'"], logoutput=False, configPath=configPath)
        if exit_code == 0:
            pattern = re.compile(r"version (\S+)")
            m = pattern.search(output)
            if m:
                logger.info("--- getVersion: %s", m.group(1))
                return m.group(1)
            else:
                return cls.getVersionFromBuild()

    @classmethod
    def isInstalled(cls):
        return cls.getVersionFromBuild() != ""

    @classmethod
    def getHbaseRootDataDirectory(cls):
        return cls.getConfigValue('hbase.rootdir')

    # method to get the number of region servers in the hbase cluster
    @classmethod
    @TaskReporter.report_test()
    def getNumOfRegionServers(cls):
        servers = {}
        servers['all'] = len(cls.getRegionServers())
        servers['dead'] = len(
            Ambari.getServiceHostsWithState('HBASE', 'HBASE_REGIONSERVER', 'INSTALLED', cluster=cls._cluster)
        )
        servers['running'] = len(
            Ambari.getServiceHostsWithState('HBASE', 'HBASE_REGIONSERVER', 'STARTED', cluster=cls._cluster)
        )
        logger.info("=============== servers: %s", servers)
        return servers

    @classmethod
    @TaskReporter.report_test()
    def getRSListFromStatus(cls, reRunStatus=False, logoutput=False, host=None):
        """
        Query the list of Region Server hosts from hbase shell. For Ambari calls, better use
        getRegionServers() instead of this one. The returned hosts are IP addresses in unsecure
        environments (be it Azure or not) and so cannot be used in Azure's https API.
        For comparisons with hbase meta table entries, this one will work in secure and
        unsecure as well.

        :param reRunStatus: refresh the cache
        :param logoutput: whether to log hbase shell responses
        :param host: the hbase client host to ask
        :return: the list of RS host addresses
        """
        if host:
            _, stdout = cls.runShellCmds(["status 'simple'"], logoutput=logoutput, host=host)
            if 'backup masters' in stdout:
                return list(set(re.findall(r"(\S+)[:|,]\d{5}[, ]\d+", stdout.split('live servers')[1])))
            else:
                return list(set(re.findall(r"(\S+)[:|,]\d{5}[, ]\d+", stdout)))
        else:
            if (not cls._RSListFromStatus) or reRunStatus:
                _, stdout = cls.runShellCmds(["status 'simple'"], logoutput=logoutput)
                # capture both dead and live region servers. Live region servers have hostname:port and
                # dead region servers have host,port
                if 'backup masters' in stdout:
                    cls._RSListFromStatus.extend(
                        set(re.findall(r"(\S+)[:|,]\d{5}[, ]\d+",
                                       stdout.split('live servers')[1]))
                    )
                else:
                    cls._RSListFromStatus.extend(set(re.findall(r"(\S+)[:|,]\d{5}[, ]\d+", stdout)))
                if reRunStatus:
                    cls._RSListFromStatus = list(set(cls._RSListFromStatus))
            return cls._RSListFromStatus

    @classmethod
    @TaskReporter.report_test()
    def getRegionServers(cls, host=None, reRunStatus=False):
        """
        Query the list of Region Server hosts from Ambari APIs. For comparisons with hbase meta
        table entries, prefer calling getRSListFromStatus to this one. The returned hosts are
        host names in Humboldt environments (even though they are unsecure) and so usable for
        https API calls but won't always match with hbase meta table entries or status outputs
        (especially not in Humboldt.) For Ambari API calls, this one is safe to use both in Azure
        and others.

        :param host: Ambari host or None to autodetect
        :param reRunStatus: refresh cache
        :return: the list of RS host names
        """
        if host:
            regionservers = Ambari.getHostsForComponent(
                cls._ambariComponentMapping.get('hbase_regionserver'), weburl=Ambari.getWebUrl(hostname=host)
            )
            regionservers = [rs.encode('ascii') for rs in regionservers]
            return regionservers
        if (not cls._regionServers) or reRunStatus:
            cls._regionServers = Ambari.getHostsForComponent(cls._ambariComponentMapping.get('hbase_regionserver'))
            cls._regionServers = [rs.encode('ascii') for rs in cls._regionServers]
        return cls._regionServers

    @classmethod
    @TaskReporter.report_test()
    def runInBackgroundAs(cls, user, cmd, env=None, config=None):
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        hbase_cmd = Config.get('hbase', 'HBASE_CMD')
        if config:
            hbase_cmd += " --config " + config
        hbase_cmd += " " + cmd
        return Machine.runinbackgroundAs(user, hbase_cmd, env=env)

    @classmethod
    @TaskReporter.report_test()
    def resetHBaseMasterNode(cls, action, host=None, AmbariHost=None):
        if not host:
            host = cls.getMasterNode(host)
        cls.resetHBaseServiceThroughAmbari(host=host, service='master', action=action, AmbariHost=AmbariHost)

    @classmethod
    @TaskReporter.report_test()
    def startHBaseMasterNode(cls, host=None, masterStartUpWait=True, wait=15, AmbariHost=None):
        cls.resetHBaseMasterNode(action="start", host=host, AmbariHost=AmbariHost)
        if masterStartUpWait:
            # wait for 15 seconds after we start HBase Master
            time.sleep(wait)

    # Takes input of date object in %Y-%m-%d %H:%M:%S format, and host machine
    @classmethod
    @TaskReporter.report_test()
    def verifyMasterInitialized(cls, config=None, host=None):
        tries = 0
        while tries < 100:
            exit_code, output = cls.runShellCmds(["status"], logoutput=True, configPath=config, host=host)
            if exit_code == 0:
                pattern = re.compile(r"Can't get master address")
                m = pattern.search(output)
                if m:
                    time.sleep(1)
                else:
                    pattern = re.compile(r"(\d+) active master")
                    m = pattern.search(output)
                    if m and (int(m.group(1)) > 0):
                        logger.info("-- Master successfully initialized, after tries: %s ", str(tries))
                        return True
            tries = tries + 1
            logger.info("tries: %s ", str(tries))
        logger.info("Tried %s times, but HBase Master not yet initialized.", tries)
        return False

    @classmethod
    @TaskReporter.report_test()
    def stopHBaseMasterNode(cls, host=None, wait=15, AmbariHost=None):
        cls.resetHBaseMasterNode(action="stop", host=host, AmbariHost=AmbariHost)
        time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def restartHBaseMasterNode(cls, host=None, masterStartUpWait=True, wait=15):
        if not Slider.isSlider():
            cls.stopHBaseMasterNode(host=host)
            time.sleep(wait)
            cls.startHBaseMasterNode(host=host, masterStartUpWait=masterStartUpWait, wait=wait)
            cls.verifyMasterInitialized(host=host)
        else:
            logger.info("hbase master restart skipped - Slider present")

    # method specifically written for HA tests, i want to get the host and pass it on to the other methods
    @classmethod
    @TaskReporter.report_test()
    def restartHAHBaseMasterNode(cls, wait=60):
        host = cls.getMasterNode()
        cls.killService('master', host)
        time.sleep(wait)
        cls.startHBaseMasterNode(host=host, masterStartUpWait=False, wait=0)
        cls.verifyMasterInitialized(host=host)

    @classmethod
    @TaskReporter.report_test()
    def resetRegionServers(cls, action, nodes=None, AmbariHost=None):
        if not nodes and not AmbariHost:
            nodes = cls.getRegionServers()
        elif AmbariHost:
            nodes = cls.getRegionServers(host=AmbariHost)
        for node in nodes:
            cls.resetHBaseServiceThroughAmbari(host=node, service='regionserver', action=action, AmbariHost=AmbariHost)

    @classmethod
    @TaskReporter.report_test()
    def startRegionServers(cls, nodes=None, skip_slider_check=False, wait=60, AmbariHost=None):
        if skip_slider_check or not Slider.isSlider():
            cls.resetRegionServers("start", nodes=nodes, AmbariHost=AmbariHost)
            time.sleep(wait)
        else:
            logger.info("region servers start skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def stopRegionServers(cls, nodes=None, wait=5, skip_slider_check=False, AmbariHost=None):
        if skip_slider_check or not Slider.isSlider():
            cls.resetRegionServers("stop", nodes=nodes, AmbariHost=AmbariHost)
            time.sleep(wait)
        else:
            logger.info("region servers stop skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def restartRegionServers(cls, wait=5, nodes=None):
        if not Slider.isSlider():
            cls.stopRegionServers(nodes=nodes)
            time.sleep(wait)
            cls.startRegionServers(nodes=nodes)
            time.sleep(wait)
        else:
            logger.info("region servers restart skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def stopAllPhoenixQueryServers(cls, AmbariHost=None):
        phoenixqueryservers = cls._phoenixQueryServers
        if AmbariHost:
            phoenixqueryservers = cls.getPhoenixQueryServers(host=AmbariHost)
        elif not cls._phoenixQueryServers:
            phoenixqueryservers = cls.getPhoenixQueryServers()
        for pqs in phoenixqueryservers:
            cls.resetHBaseServiceThroughAmbari(
                host=pqs, service='phoenix_queryserver', action='stop', AmbariHost=AmbariHost
            )

    @classmethod
    @TaskReporter.report_test()
    def startAllPhoenixQueryServers(cls, AmbariHost=None):
        phoenixqueryservers = cls._phoenixQueryServers
        if AmbariHost:
            phoenixqueryservers = cls.getPhoenixQueryServers(host=AmbariHost)
        elif not cls._phoenixQueryServers:
            phoenixqueryservers = cls.getPhoenixQueryServers()
        for pqs in phoenixqueryservers:
            cls.resetHBaseServiceThroughAmbari(
                host=pqs, service='phoenix_queryserver', action='start', AmbariHost=AmbariHost
            )

    @classmethod
    @TaskReporter.report_test()
    def stopHBaseClients(cls, AmbariHost=None):
        clientnodes = cls._clientNodes
        if AmbariHost:
            clientnodes = cls.getHBaseClientNodes(host=AmbariHost)
        elif not cls._clientNodes:
            clientnodes = cls.getHBaseClientNodes()
        for node in clientnodes:
            cls.resetHBaseServiceThroughAmbari(host=node, service='hbase_client', action='stop', AmbariHost=AmbariHost)

    @classmethod
    @TaskReporter.report_test()
    def startHBaseClients(cls, AmbariHost=None):
        clientnodes = cls._clientNodes
        if AmbariHost:
            clientnodes = cls.getHBaseClientNodes(host=AmbariHost)
        elif not cls._clientNodes:
            clientnodes = cls.getHBaseClientNodes()
        for node in clientnodes:
            cls.resetHBaseServiceThroughAmbari(
                host=node, service='hbase_client', action='start', AmbariHost=AmbariHost
            )

    @classmethod
    @TaskReporter.report_test()
    def startHBaseCluster(
            cls,
            config=None,  # pylint: disable=unused-argument
            masterHost=None,
            regionservers=None,  # pylint: disable=unused-argument
            verifyMasterInitialization=True,  # pylint: disable=unused-argument
            masterStartUpWait=True,  # pylint: disable=unused-argument
            wait=15,
            AmbariHost=None
    ):
        if not Slider.isSlider():
            cls.startAllHBaseMasters(wait=wait, AmbariHost=AmbariHost)
            cls.startRegionServers(wait=wait, AmbariHost=AmbariHost)
            cls.startAllPhoenixQueryServers(AmbariHost=AmbariHost)
            cls.startHBaseClients(AmbariHost=AmbariHost)
            logger.info("---- Done starting HBase cluster")
            cls.verifyMasterInitialized(host=masterHost)
        else:
            logger.info("hbase cluster start skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def stopHBaseCluster(cls, AmbariHost=None):
        if not Slider.isSlider():
            cls.stopAllPhoenixQueryServers(AmbariHost=AmbariHost)
            cls.stopRegionServers(AmbariHost=AmbariHost)
            cls.stopAllHBaseMasters(AmbariHost=AmbariHost)
            cls.stopHBaseClients(AmbariHost=AmbariHost)
            logger.info("---- Done stopping HBase cluster")
        else:
            logger.info("hbase cluster stop skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def stopAllHBaseMasters(cls, AmbariHost=None):
        masterNodes = HBase.getAllMasterNodes(host=AmbariHost)
        logger.info("------------------------  masterNodes: %s", masterNodes)
        stoppedMasterNodes = []
        for master in masterNodes:
            cls.resetHBaseMasterNode(action="stop", host=master, AmbariHost=AmbariHost)
            logger.info("============  stopped master : %s", master)
            stoppedMasterNodes.append(master)
        logger.info("---- List of masters stopped: %s", stoppedMasterNodes)
        # '''
        # stoppedMasterNodes = []
        # master = cls.getMasterNode()
        # while master and master not in stoppedMasterNodes:
        #    cls.resetHBaseMasterNode(action="stop", host=master)
        #    logger.info("============  stopped master : "+master)
        #    stoppedMasterNodes.append(master)
        #    time.sleep(wait)
        #    master = cls.getMasterNode()
        # '''

    @classmethod
    @TaskReporter.report_test()
    def startAllHBaseMasters(cls, masterStartUpWait=True, wait=15, AmbariHost=None):
        masterNodes = HBase.getAllMasterNodes(host=AmbariHost)
        startedMasterNodes = []
        for master in masterNodes:
            cls.resetHBaseMasterNode(action="start", host=master, AmbariHost=AmbariHost)
            logger.info("============   master started on : %s", master)
            startedMasterNodes.append(master)
            if masterStartUpWait:
                time.sleep(wait)
        logger.info("---- List of masters started: %s", startedMasterNodes)

    @classmethod
    @TaskReporter.report_test()
    def restartHBaseCluster(
            cls,
            config=None,
            masterHost=None,
            regionservers=None,
            masterStartUpWait=True,
            wait=15,
            AmbariHost=None
    ):
        if not Slider.isSlider():
            cls.stopHBaseCluster(AmbariHost=AmbariHost)
            time.sleep(wait)
            cls.startHBaseCluster(
                config=config,
                masterHost=masterHost,
                regionservers=regionservers,
                masterStartUpWait=masterStartUpWait,
                wait=wait,
                AmbariHost=AmbariHost
            )
            logger.info("---- Done restarting HBase cluster")
        else:
            logger.info("hbase cluster restart skipped - Slider present")

    @classmethod
    @TaskReporter.report_test()
    def getModifiedConfigPath(cls):
        return Config.get('hbase', 'HBASE_CONF_DIR')

    @classmethod
    @TaskReporter.report_test()
    def killProcessWithFilterIfExists(cls, host, processName, filterName, collectJStack=False):
        pid = None
        if Machine.isWindows():
            pid = Machine.getProcessListWithPid(host, processName, filterName, logoutput=True, ignorecase=True)
        else:
            processesList = Machine.getProcessListRemote(host, filter=filterName)
            if processesList:
                pid = processesList[0].split()[1]
        if pid:
            if collectJStack:
                JAVA_HOME = Config.get('machine', 'JAVA_HOME')
                JPS_CMD = os.path.join(JAVA_HOME, 'bin', 'jps')
                exit_code, stdout = Machine.runas(HADOOPQA_USER, JPS_CMD)
                assert exit_code == 0
                stdout = stdout.splitlines(True)
                javaPID = None
                for line in stdout:
                    line = line.strip()
                    line_arr = line.split(' ')
                    cmd = line_arr[len(line_arr) - 1].strip()
                    if cmd == filterName:
                        javaPID = line_arr[0].strip()
                        JSTACK_EXE = os.path.join(JAVA_HOME, 'bin', 'jstack')
                        ARTIFACTS_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'))
                        JSTACK_CMD = "%s %s >> %s/jstack-%s-%s-%s.txt" % (
                            JSTACK_EXE, javaPID, ARTIFACTS_DIR, host, filterName, javaPID
                        )
                        JSTACK_CMD_SLES = "%s -J-d64 -m %s >> %s/jstack-%s-%s-%s-sles.txt" % (
                            JSTACK_EXE, javaPID, ARTIFACTS_DIR, host, filterName, javaPID
                        )
                        exit_code, stdout = Machine.runas(HADOOPQA_USER, JSTACK_CMD)
                        exit_code, stdout = Machine.runas(HADOOPQA_USER, JSTACK_CMD_SLES)
                        time.sleep(10)
                        Machine.killProcessRemote(
                            pid=int(javaPID),
                            host=host,
                            user=Machine.getAdminUser(),
                            passwd=Machine.getAdminPasswd(),
                            logoutput=True
                        )
            logger.info("Process with handle %s exists. PID: %s", filterName, pid)
            logger.info("---- killing pid: %s", pid)
            Machine.killProcessRemote(
                pid=int(pid), host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd(), logoutput=True
            )
        else:
            logger.info("No PID for process with handle %s found. Nothing to kill.", filterName)

    @classmethod
    @TaskReporter.report_test()
    def copyFilesToGatewayFromMasterNode(cls, hbase_conf, zk_conf):
        if Hadoop.isAmbari():
            ms_hosts = Ambari.getServiceHosts('HBASE', 'HBASE_MASTER', cluster=cls._cluster)
            if ms_hosts:
                master_host = str(ms_hosts[0])
            if Xa.isArgus():
                if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                        hbase_conf, 'ranger-hbase-audit.xml'), Machine.getAdminPasswd()):
                    Machine.copyToLocal(
                        Machine.getAdminUser(),
                        master_host,
                        os.path.join(hbase_conf, 'ranger-hbase-audit.xml'),
                        hbase_conf,
                        passwd=Machine.getAdminPasswd()
                    )
                if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                        hbase_conf, 'ranger-hbase-security.xml'), Machine.getAdminPasswd()):
                    Machine.copyToLocal(
                        Machine.getAdminUser(),
                        master_host,
                        os.path.join(hbase_conf, 'ranger-hbase-security.xml'),
                        hbase_conf,
                        passwd=Machine.getAdminPasswd()
                    )
                if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                        hbase_conf, 'ranger-policymgr-ssl.xml'), Machine.getAdminPasswd()):
                    Machine.copyToLocal(
                        Machine.getAdminUser(),
                        master_host,
                        os.path.join(hbase_conf, 'ranger-policymgr-ssl.xml'),
                        hbase_conf,
                        passwd=Machine.getAdminPasswd()
                    )
                if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                        hbase_conf, 'ranger-security.xml'), Machine.getAdminPasswd()):
                    Machine.copyToLocal(
                        Machine.getAdminUser(),
                        master_host,
                        os.path.join(hbase_conf, 'ranger-security.xml'),
                        hbase_conf,
                        passwd=Machine.getAdminPasswd()
                    )
            if Hadoop.isSecure():
                if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(
                        hbase_conf, 'hbase_master_jaas.conf'), Machine.getAdminPasswd()):
                    Machine.copyToLocal(
                        Machine.getAdminUser(),
                        master_host,
                        os.path.join(hbase_conf, 'hbase_master_jaas.conf'),
                        hbase_conf,
                        passwd=Machine.getAdminPasswd()
                    )
            if not Machine.pathExists(Machine.getAdminUser(), Machine.getfqdn(), os.path.join(zk_conf, 'zoo.cfg'),
                                      Machine.getAdminPasswd()):
                Machine.copyToLocal(
                    Machine.getAdminUser(),
                    master_host,
                    os.path.join(zk_conf, 'zoo.cfg'),
                    hbase_conf,
                    passwd=Machine.getAdminPasswd()
                )

    @classmethod
    @TaskReporter.report_test()
    def getStartingAmbariServiceConfigVersion(cls, update=False, host=None):
        if host:
            logger.info(" ---- Returning HBase service config version for Host: %s", host)
            return Ambari.getCurrentServiceConfigVersion(cls._ambariServiceName, host=host)
        if cls._ambariServiceConfigVersion is None or update:
            logger.info("-- updating value for cls._ambariServiceConfigVersion")
            cls._ambariServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceName)
        logger.info(" - Returning service config version for HBase: %s", cls._ambariServiceConfigVersion)
        return cls._ambariServiceConfigVersion

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(cls, changes, nodeSelection=None, env=None, restartService=True, AmbariHost=None):  # pylint: disable=unused-argument
        logger.info("- Current service config version for HBase: %s", cls.getStartingAmbariServiceConfigVersion())
        webURL = None
        if AmbariHost:
            webURL = Ambari.getWebUrl(hostname=AmbariHost)
        for key, value in changes.items():
            if cls._ambariConfigMap.has_key(key):
                key = cls._ambariConfigMap[key]
            else:
                logger.warn("Unknown config \"%s\" change requested, ignoring", key)
                continue
            Ambari.setConfig(key, value, webURL=webURL)
        if 'log4j.properties' in changes.keys():
            ambariConfig = cls._ambariConfigMap['log4j.properties']
            envProps = Ambari.getConfig(ambariConfig, webURL=webURL)
            if envProps.has_key("content"):
                content = envProps['content'].splitlines()
                log4jProps = changes['log4j.properties']
                for i, _ in enumerate(content):
                    if '=' in content[i]:
                        lkey, _ = content[i].split("=", 1)
                        lkey = lkey.strip()
                        lkey = re.search("(#)*(.*)", lkey).group(2)
                        if lkey in log4jProps.keys():
                            content[i] = "%s=%s\n" % (lkey, log4jProps[lkey])
                content = '\n'.join(content)
                Ambari.setConfig(ambariConfig, {'content': content}, webURL=webURL)
        if env:
            key = cls._ambariConfigMap['hbase-env.sh']
            envProps = Ambari.getConfig(key, webURL=webURL)
            if envProps.has_key("content"):
                content = envProps['content']
                for envKey, envVal in env.items():
                    content = "export %s=%s\n%s" % (envKey, envVal, content)
                Ambari.setConfig(key, {'content': content}, webURL=webURL)
        if restartService:
            logger.info("-- attempting restart")
            cls.restartHBaseCluster(AmbariHost=AmbariHost)

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls, changes, nodeSelection=None, restartService=True, AmbariHost=None):  # pylint: disable=unused-argument
        resetVersion = cls.getStartingAmbariServiceConfigVersion(host=AmbariHost)
        logger.info("Restoring Service Config Version for HBase to : %s", resetVersion)
        Ambari.resetConfig(cls._ambariServiceName, resetVersion, webURL=Ambari.getWebUrl(hostname=AmbariHost))
        if restartService:
            logger.info("-- attempting restart")
            # Ambari.restart_services_with_stale_configs()
            cls.restartHBaseCluster(AmbariHost=AmbariHost)

    @classmethod
    @TaskReporter.report_test()
    def getSelectedNodes(cls, selection, host=None):
        selnodes = []
        if selection.has_key('nodes'):
            selnodes.extend(selection['nodes'])
        if selection.has_key('services'):
            for service in selection['services']:
                if service == 'all':
                    return cls.getSelectedNodes(
                        {
                            'services': ['masternode', 'masternodes', 'regionservers']
                        }, host=host
                    )
                elif service == 'masternode':
                    if cls.isHAEnabled():
                        selnodes.extend(cls._allMasterNodes)
                    else:
                        selnodes.append(cls.getMasterNode())
                elif service == 'masternodes':
                    masternodes = cls._allMasterNodes
                    if masternodes is None:
                        cls.getAllMasterNodes()
                    if masternodes:
                        selnodes.extend(masternodes)
                elif service == 'regionservers':
                    selnodes.extend(cls.getRegionServers())
        return list(set(selnodes))

    # method to kill a service instead of clean shutdown
    # will only work on linux, gets the pid from the hbase-hbase-master.pid
    # file or another service and kills the process as the hbase user
    @classmethod
    @TaskReporter.report_test()
    def killService(cls, service, host, suspend=False):
        killCmd = None
        logger.info('kill service %s on host %s', service, host)
        if Machine.isWindows():
            if not suspend:
                killCmd = 'taskkill /t /f /im %s.exe' % service
                # make sure we are using short fqdn on windows
                host = Machine.getfqdn(name=host)
                return Machine.runas(Machine.getAdminUser(), killCmd, host=host, passwd=Machine.getAdminPasswd())
            else:
                # get the pid
                pid = Machine.getProcessListWithPid(host, "java.exe", service, logoutput=True, ignorecase=True)
                return Machine.sendSIGSTOP(Machine.getAdminUser(), host, pid)
        else:
            killOption = '-9'
            if suspend:
                killOption = '-19'
            pidFile = os.path.join(cls._pid_dir, 'hbase-%s-%s.pid' % (cls._hbase_user, service))
            killCmd = "cat %s | xargs kill %s" % (pidFile, killOption)
            return Machine.runas(cls._hbase_user, killCmd, host=host)

    @classmethod
    @TaskReporter.report_test()
    def getMasterLogFile(cls, host=None, searchstr=None):
        # Find master node
        if host is None:
            if cls._masterNode is None:
                host = cls.getMasterNode()
            else:
                host = cls._masterNode
        if searchstr is None:
            searchstr = "hbase*-master-*.log"
        return Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=cls._hbaseLogDir,
            searchstr=searchstr,
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    @TaskReporter.report_test()
    def getRegionLogFile(cls, host=None, searchstr=None):
        if searchstr is None:
            searchstr = "hbase*-regionserver-*.log"
        return Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=cls._hbaseLogDir,
            searchstr=searchstr,
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    def getTempFilepath(cls):
        return os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d' % int(999999 * random.random()))

    # Use to Verify Replicas
    @classmethod
    @TaskReporter.report_test()
    def verifyReplicaCount(cls, table_name, replica_num=0, NUM_RS=1, RS_DICT=None, splits=1, original_replica=None):
        """
        Name:
        hbase.py - Verify Replica Count

        Description:
        This is a helper function to scan the hbase meta, and count the number of replicas for a table.

        Some multiline test steps about the testcase:
        - Scan the HBase meta table.
        - Count the replicas for the table in the meta for verification.
        - Verify the replicas are evenly distributed across region servers.
        - Verify the replicas count is greater than the number of region servers.
        """

        if original_replica is None:
            original_replica = NUM_RS

        # Verify Replica count and Replica placement in RSes by Master
        exit_code, stdout = HBase.runShellCmds(["scan 'hbase:meta'"])
        assert exit_code == 0
        count = 0
        stdout = stdout.splitlines(True)
        for line in stdout:
            line_arr = line.split(',')
            if table_name == line_arr[0].strip():
                server = line_arr[2].split(' ')[1].strip()
                if splits > 1:
                    check = count % original_replica
                else:
                    check = count
                if check > 9:
                    if check == 10:
                        check = "A"
                    elif check == 11:
                        check = "B"
                    elif check == 12:
                        check = "C"
                    elif check == 13:
                        check = "D"
                    elif check == 14:
                        check = "E"
                    elif check == 15:
                        check = "F"
                if server == "column=info:server" or server == "column=info:server_000%s" % check:
                    count = count + 1
                    regionServer = line_arr[4].split('=')[1].split(':')[0].strip()
                    RS_DICT[regionServer.lower()] = True
                    if count == replica_num:
                        break
        if (replica_num / splits) >= NUM_RS:
            assert count == replica_num, "Replica count is wrong: expected count: %s actual count was: %s" % (
                replica_num, count
            )
            for key, value in RS_DICT.iteritems():
                assert value, "Not all Region Server have replica. Region Server: %s missing replica." % key
        else:
            assert count == replica_num, "Replica count is wrong: expected count: %s actual count was: %s" % (
                replica_num, count
            )
            count = 0
            for value in RS_DICT.itervalues():
                if value:
                    count = count + 1
            assert count >= (replica_num / splits), "Replicas are not equally distributed and assigned by the Master"

    # Use to Verify Replicas
    @classmethod
    @TaskReporter.report_test()
    def verifyReplicaCountBoolean(
            cls, table_name, replica_num=0, NUM_RS=1, RS_DICT=None, splits=1, original_replica=None
    ):
        """
        Name:
        hbase.py - Verify Replica Count Boolean

        Description:
        This is a helper function to scan the hbase meta, and count the number of replicas for a table.
        This method returns a boolean result as part of the verify.

        Some multiline test steps about the testcase:
        - Scan the HBase meta table.
        - Count the replicas for the table in the meta for verification.
        - Verify the replicas are evenly distributed across region servers.
        - Verify the replicas count is greater than the number of region servers.
        """

        if original_replica is None:
            original_replica = NUM_RS

        # Verify Replica count and Replica placement in RSes by Master
        exit_code, stdout = HBase.runShellCmds(["scan 'hbase:meta'"])
        if exit_code != 0:
            return False
        count = 0
        stdout = stdout.splitlines(True)
        for line in stdout:
            line_arr = line.split(',')
            if table_name == line_arr[0].strip():
                server = line_arr[2].split(' ')[1].strip()
                if splits > 1:
                    check = count % original_replica
                else:
                    check = count
                if check > 9:
                    if check == 10:
                        check = "A"
                    elif check == 11:
                        check = "B"
                    elif check == 12:
                        check = "C"
                    elif check == 13:
                        check = "D"
                    elif check == 14:
                        check = "E"
                    elif check == 15:
                        check = "F"
                if server == "column=info:server" or server == "column=info:server_000%s" % check:
                    count = count + 1
                    regionServer = line_arr[4].split('=')[1].split(':')[0].strip()
                    RS_DICT[regionServer.lower()] = True
                    if count == replica_num:
                        break
        if (replica_num / splits) >= NUM_RS:
            if count != replica_num:
                return False
            count = 0
            for _, value in RS_DICT.iteritems():
                if value:
                    count = count + 1
            if count != NUM_RS:
                return False
        else:
            if count != replica_num:
                return False
            count = 0
            for value in RS_DICT.itervalues():
                if value:
                    count = count + 1
            if count < (replica_num / splits):
                return False
        return True

    # Use to Verify Replicas
    @classmethod
    @TaskReporter.report_test()
    def getReplicaCount(cls, table_name, splits, original_replica):
        """
        Name:
        hbase.py - Verify Minimum Replica Count Boolean

        Description:
        This is a helper function to scan the hbase meta, and count the number of replicas for a table.

        Some multiline test steps about the testcase:
        - Scan the HBase meta table.
        - Count the replicas for the table in the meta
        - Return the replica count
        """

        # Verify Replica count and Replica placement in RSes by Master
        exit_code, stdout = HBase.runShellCmds(["scan 'hbase:meta'"])
        assert exit_code == 0
        count = 0
        stdout = stdout.splitlines(True)
        for line in stdout:
            line_arr = line.split(',')
            if table_name == line_arr[0].strip():
                server = line_arr[2].split(' ')[1].strip()
                if splits > 1:
                    check = count % original_replica
                else:
                    check = count
                if check > 9:
                    if check == 10:
                        check = "A"
                    elif check == 11:
                        check = "B"
                    elif check == 12:
                        check = "C"
                    elif check == 13:
                        check = "D"
                    elif check == 14:
                        check = "E"
                    elif check == 15:
                        check = "F"
                if server == "column=info:server" or server == "column=info:server_000%s" % check:
                    count = count + 1
        return count

    # Find the RS containing the Primary Replica
    @classmethod
    @TaskReporter.report_test()
    def findPrimaryReplica(cls, table_name):
        """
        Name:
        hbase.py - find Primary Replica

        Description:
        This is a helper function to scan the hbase meta and find the RS holding the primary replica for the table.

        Some multiline test steps about the testcase:
        - Scan the HBase meta table.
        - Look for the server id for the primary replica.
        - Parse the output for the primary replica region server.
        """

        # Find RS holding the Primary Replica
        exit_code, stdout = HBase.runShellCmds(["scan 'hbase:meta'"])
        assert exit_code == 0
        regionServer = None
        stdout = stdout.splitlines(True)
        for line in stdout:
            line_arr = line.split(',')
            if table_name == line_arr[0].strip():
                server = line_arr[2].split(' ')[1].strip()
                if server == "column=info:server":
                    regionServer = line_arr[4].split('=')[1].split(':')[0].strip()
                    regionServer = regionServer.lower()
                    break
        return regionServer

    # Find the encoded names of primary regions for a table
    @classmethod
    @TaskReporter.report_test()
    def getEncodedNamesForPrimaryRegions(cls, table_name):
        """
        Name:
        hbase.py - get encoded names of the primary regions

        Description:
        This is a helper function to scan hbase meta and return the encoded names of the primary regions for a table.
        """

        exit_code, stdout = HBase.runShellCmds(["scan 'hbase:meta'"])
        assert exit_code == 0
        stdout = stdout.splitlines(True)
        encoded = []
        for line in stdout:
            line_arr = line.split(',')
            if table_name == line_arr[0].strip():
                encoded_arr = line.split('.')
                server = line_arr[2].split(' ')[1].strip()
                if server == "column=info:server":
                    encoded.append(encoded_arr[1].split(' ')[0].strip())
        return encoded

    @classmethod
    def grantPrivileges(cls, user, privs="RWXC"):
        return cls.runShellCmds(["grant '%s', '%s'" % (user, privs)], user=cls._hbase_user)

    @classmethod
    def revokePrivileges(cls, user):
        return cls.runShellCmds(["revoke '%s'" % user], user=cls._hbase_user)

    @classmethod
    @TaskReporter.report_test()
    def getHBaseLogDir(cls, logoutput=False):
        '''
        Returns HBase log directory (String).
        '''
        returnValue = cls._hbaseLogDir
        if logoutput:
            logger.info("HBase.getHBaseLogDir returns %s", returnValue)
        return returnValue

    @classmethod
    def getHbaseJarPath(cls, jarname):
        '''
        Return absolute path for Jar.
        :param jarname: example: hbase-common.jar
        :return: /usr/hdp/current/hbase-client/lib/hbase-common.jar
        '''
        return Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=os.path.join(Config.get('hbase', 'HBASE_HOME'), "lib"),
            searchstr=jarname,
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    @TaskReporter.report_test()
    def getBackupIdForTable(cls, backup_root_full_path, table_name):
        backup_id = None
        exit_code, stdout = HDFS.runas(cls._hbase_user, 'dfs -ls -R %s' % (backup_root_full_path))
        assert exit_code == 0, stdout
        m = re.search(r"%s/(backup_\d+)/default/%s" % (backup_root_full_path, table_name), stdout)
        if m:
            backup_id = m.group(1)
        else:
            logger.info("---- backup_id not found for test table %s", table_name)
        return backup_id

    @classmethod
    def deleteHBaseBackupWithId(cls, backup_id):
        return cls.runas(user=cls._hbase_user, cmd="backup delete %s"%backup_id)

    @classmethod
    @TaskReporter.report_test()
    def isHAEnabled(cls):
        '''
        Check if cluster has multiple masters
        '''
        if cls._allMasterNodes is None:
            nodes = cls.getAllMasterNodes()
        else:
            nodes = cls._allMasterNodes
        return len(nodes) > 1

    @classmethod
    @TaskReporter.report_test()
    def setAmbariConfigVersion(cls):
        cls._ambariServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceName)
        logger.info("------- _ambariServiceConfigVersion is now set to: %s", cls._ambariServiceConfigVersion)
