import logging
import os
import re
import time
import json
import requests
from taskreporter.taskreporter import TaskReporter
from requests_kerberos import HTTPKerberosAuth, REQUIRED
from beaver.machine import Machine
from beaver.config import Config
from beaver import configUtils
from beaver import util
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.xa_ambari_api_util import AmbariAPIUtil
from beaver.component.ambari import Ambari

logger = logging.getLogger(__name__)

QE_EMAIL = util.get_qe_group_email()


class Livy_Exception(Exception):
    pass


class Livy(object):
    def __init__(self):
        pass

    _livy_ip_addr = None
    _headers = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}

    @classmethod
    @TaskReporter.report_test()
    def isLivy2(cls):
        '''
        Return if Livy2 is present in cluster
        '''
        component = util.get_TESTSUITE_COMPONENT()
        logger.info("component = %s", component)
        return component.lower() == "livy2"

    @classmethod
    @TaskReporter.report_test()
    def getLivyHome(cls):
        if cls.isLivy2():
            if Machine.isHumboldt():
                return "/usr/bin/livy2"
            else:
                return Config.get('livy', 'LIVY2_HOME')
        else:
            return Config.get('livy', 'LIVY_HOME')

    @classmethod
    @TaskReporter.report_test()
    def getLivyLogDir(cls, forceVersion=None):
        if Machine.isCloud():
            ambari_connector = AmbariAPIUtil(isHttps=True)
        else:
            if Hadoop.isEncrypted():
                ambari_connector = AmbariAPIUtil(port=8443, isHttps=True)
            else:
                ambari_connector = AmbariAPIUtil(port=8080, isHttps=False)

        if cls.isLivy2() or forceVersion == "2":
            return ambari_connector.getPropertyValue('livy2-env', 'livy2_log_dir')
        else:
            return ambari_connector.getPropertyValue('livy-env', 'livy_log_dir')

    @classmethod
    def getLivyConfDir(cls, forceVersion=None):
        if cls.isLivy2() or forceVersion == "2":
            return Config.get('livy', 'LIVY2_CONF')
        else:
            return Config.get('livy', 'LIVY_CONF')

    @classmethod
    def getLivyEnvConfFile(cls):
        return os.path.join(cls.getLivyConfDir(), "livy-env.sh")

    @classmethod
    def getLivyConfFile(cls):
        return os.path.join(cls.getLivyConfDir(), "livy.conf")

    @classmethod
    @TaskReporter.report_test()
    def getConfigFromLivyEnv(cls, propertyValue, defaultValue=None):

        livyEnv = cls.getLivyEnvConfFile()
        f = open(livyEnv, "r")
        for line in f:
            if not line.startswith('#'):
                if line.find("export " + propertyValue) == 0:
                    m = re.search('(\\S*)=(\\S*)', line)
                    if m:
                        return m.group(2)
                    else:
                        return None
        return defaultValue

    @classmethod
    @TaskReporter.report_test()
    def getConfigFromLivyConf(cls, propertyKey, defaultValue=None):
        """
        Get Config value from Livy conf
        """
        properties = Ambari.getConfig("livy2-conf", service="SPARK2")
        try:
            val = properties[propertyKey]
        except Exception:
            val = defaultValue
        return val

    @classmethod
    @TaskReporter.report_test()
    def getLivyServerIpAddress(cls, forceVersion=None):
        '''
        returns IP address of the node where livy-server is running
        '''
        if not Livy._livy_ip_addr:
            if Machine.isCloud():
                ambari_connector = AmbariAPIUtil(isHttps=True)
            else:
                if Hadoop.isEncrypted():
                    ambari_connector = AmbariAPIUtil(port=8443, isHttps=True)
                else:
                    ambari_connector = AmbariAPIUtil(port=8080, isHttps=False)

            if cls.isLivy2() or forceVersion == "2":
                fqdn = ambari_connector.getComponentHosts('SPARK2', 'LIVY2_SERVER')
            else:
                fqdn = ambari_connector.getComponentHosts('SPARK', 'LIVY_SERVER')

            if Machine.isCloud():
                all_int_nodes = util.getAllInternalNodes()
                all_nodes = util.getAllNodes()
                for i, node in enumerate(all_int_nodes):
                    if fqdn == node:
                        Livy._livy_ip_addr = all_nodes[i]
                        break
            else:
                # QE-15807: If there are multiple instances of livy/livy2 server,
                # ambari returns comma seperated host list.
                # Thus, adding fix to run test against first livy server instance.
                livy_host = fqdn.split(",")[0]
                Livy._livy_ip_addr = util.getIpAddress(hostname=livy_host)
        logger.info("Livy server IP = %s", Livy._livy_ip_addr)
        return Livy._livy_ip_addr

    @classmethod
    @TaskReporter.report_test()
    def getSelectedNodes(cls, selection):
        selnodes = []
        if selection.has_key('nodes'):
            selnodes.extend(selection['nodes'])
        if selection.has_key('services'):
            for service in selection['services']:
                if service == 'all':
                    return cls.getSelectedNodes({'services': ['livy-server', 'gateway']})
                elif service == 'livy-server':
                    selnodes.append(cls.getLivyServerIpAddress())
                elif service == 'gateway':
                    selnodes.append(HDFS.getGateway())
        return list(set(selnodes))

    @classmethod
    def getLivyServerPort(cls):
        port = cls.getConfigFromLivyConf("livy.server.port")
        logger.info("Livy server port = %s", port)
        return port

    @classmethod
    def getLivyServerLaunchKeytab(cls):
        '''
        Get the value of livy.server.launch.kerberos.keytab
        '''
        return cls.getConfigFromLivyConf("livy.server.launch.kerberos.keytab")

    @classmethod
    def getLivyServerLaunchPrincipal(cls):
        '''
        Get value of livy.server.launch.kerberos.principal
        '''
        return cls.getConfigFromLivyConf("livy.server.launch.kerberos.principal")

    @classmethod
    def getBackupConfigPath(cls):
        return os.path.join(Machine.getTempDir(), 'livyBkupConf')

    @classmethod
    def getModifiedConfigPath(cls):
        return os.path.join(Machine.getTempDir(), 'livyConf')

    @classmethod
    @TaskReporter.report_test()
    def backUpConfig(cls):
        if Machine.isLinux():
            defUser, defPwd = Machine.getPrivilegedUser()
            Machine.rm(defUser, cls.getLivyServerIpAddress(), cls.getBackupConfigPath(), isdir=True, passwd=defPwd)
            Machine.runas(
                defUser,
                "cp -r %s %s" % (cls.getLivyConfDir(), cls.getBackupConfigPath()),
                host=cls.getLivyServerIpAddress(),
                passwd=defPwd
            )

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls):
        if Machine.isLinux():
            defUser, defPwd = Machine.getPrivilegedUser()
            Machine.runas(
                defUser,
                "cp -r %s/* %s" % (cls.getBackupConfigPath(), cls.getLivyConfDir()),
                host=cls.getLivyServerIpAddress(),
                passwd=defPwd
            )
            if not Machine.isSameHost(cls.getLivyServerIpAddress(), None):
                Machine.copyToLocal(
                    defUser,
                    cls.getLivyServerIpAddress(),
                    cls.getLivyConfDir(),
                    cls.getLivyConfDir().replace("conf", ""),
                    passwd=defPwd
                )

    @classmethod
    @TaskReporter.report_test()
    def modifyLivyEnv(cls, changes):
        if Machine.isLinux():
            config = cls.getLivyEnvConfFile()
            logger.info("Modifying file: %s", config)
            propstr = ""

            for line in open(config).readlines():
                sline = line.strip()
                if not sline.startswith("#") and sline.strip() != "":
                    m = re.search('export (\\S+)=', line)
                    if m:
                        if m.group(1) in changes.keys():
                            propstr += "export %s=%s\n" % (m.group(1), changes[m.group(1)])
                        else:
                            propstr += line
                    else:
                        propstr += line
                else:
                    propstr += line

            Machine.rm(
                user=Machine.getAdminUser(),
                host=cls.getLivyServerIpAddress(),
                filepath=cls.getLivyConfDir(),
                isdir=True,
                passwd=Machine.getAdminPasswd()
            )
            Machine.copyFromLocal(None, cls.getLivyServerIpAddress(), cls.getLivyConfDir(), cls.getLivyConfDir())
            Machine.chmod(
                "777",
                cls.getLivyConfDir(),
                recursive=True,
                user=Machine.getAdminUser(),
                host=cls.getLivyServerIpAddress(),
                passwd=Machine.getAdminPasswd()
            )

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(cls, changes, isFirstUpdate=True):
        if Machine.isLinux():
            cls.backUpConfig()
            livy_conf = cls.getLivyConfDir()
            #tmp_conf = cls.getModifiedConfigPath()
            nodes = cls.getSelectedNodes({'services': ['all']})
            for node in nodes:
                Machine.chmod(
                    "777",
                    livy_conf.replace("/conf", ""),
                    recursive=True,
                    user=Machine.getAdminUser(),
                    host=node,
                    passwd=Machine.getAdminPasswd()
                )

            if 'livy-env.sh' in changes.keys():
                env = changes['livy-env.sh']
                del changes['livy-env.sh']
                if changes:
                    configUtils.modifyConfig(changes, livy_conf, livy_conf, nodes, isFirstUpdate=True)
                    #util.copy_back_to_original_config(tmp_conf, livy_conf, file_list=["all"], node_list=[None])
                    for node in nodes:
                        Machine.chmod(
                            "777",
                            livy_conf,
                            recursive=True,
                            user=Machine.getAdminUser(),
                            host=node,
                            passwd=Machine.getAdminPasswd()
                        )
                    cls.modifyLivyEnv(env)
                else:
                    cls.modifyLivyEnv(env)
            else:
                configUtils.modifyConfig(changes, livy_conf, livy_conf, nodes, isFirstUpdate)
                for node in nodes:
                    Machine.chmod(
                        "777",
                        livy_conf,
                        recursive=True,
                        user=Machine.getAdminUser(),
                        host=node,
                        passwd=Machine.getAdminPasswd()
                    )

    @classmethod
    @TaskReporter.report_test()
    def daemon(cls, user, host, action, homePath, daemonName='livy-server', binFolder='bin'):
        '''
        Assumes just for Linux
        '''
        if Machine.isLinux():
            cmd = os.path.join(homePath, binFolder, daemonName)
            cmd += " " + action

        # if cls.isSecurityEnabled():
        #     # workaround for BUG-73540.
        #     principal = cls.getLivyServerLaunchPrincipal().split("@")[1]
        #     if util.isIP(host):
        #         full_hostname = util.getFullHostnameFromIP(host)
        #     else:
        #         full_hostname = host
        #     kinitcmd = "%s -kt %s %s/%s@%s" % (Machine.getKinitCmd(), cls.getLivyServerLaunchKeytab(), user, full_hostname, principal)
        #     cmd = kinitcmd + ";" + cmd

        return Machine.runas(user, cmd, host=host, env={'JAVA_HOME': Config.get('machine', 'JAVA_HOME')})

    @classmethod
    @TaskReporter.report_test()
    def resetLivyServerNode(cls, action, user=Config.get('livy', 'LIVY_USER')):
        '''
        Assumes just for Linux
        '''
        if Machine.isLinux():
            host = cls.getLivyServerIpAddress()
            exit_code, _ = cls.daemon(user, host=host, action=action, homePath=cls.getLivyHome())
            return exit_code == 0
        return False

    @classmethod
    @TaskReporter.report_test()
    def isLivyServerRunning(cls):
        if Machine.isLinux():
            pid = Machine.getProcessListRemote(cls.getLivyServerIpAddress(), filter="livy.server.LivyServer")
            logger.info("livy process: %s", pid)
            return bool(pid)
        return False

    @classmethod
    @TaskReporter.report_test()
    def startLivyServer(cls, user=Config.get('livy', 'LIVY_USER'), livyStartUpWait=True, wait=10):
        if Machine.isLinux():
            if cls.resetLivyServerNode(action="start", user=user):
                if livyStartUpWait:
                    time.sleep(wait)
                logger.info("Livy server started successfully at %s", (cls._get_livy_url()))
                return True
            else:
                logger.error("Failed to start Livy server at %s", (cls._get_livy_url()))
                return False
        return False

    @classmethod
    @TaskReporter.report_test()
    def stopLivyServer(cls, user=Config.get('livy', 'LIVY_USER'), livyStopWait=True, wait=10):
        if Machine.isLinux():
            if cls.resetLivyServerNode(action="stop", user=user):
                if livyStopWait:
                    time.sleep(wait)
                logger.info("Livy server stopped successfully at %s", (cls._get_livy_url()))
                return True
            else:
                logger.error("Failed to stop Livy server at %s", (cls._get_livy_url()))
                return False
        return False

    @classmethod
    @TaskReporter.report_test()
    def restartLivyServer(cls, user=Config.get('livy', 'LIVY_USER'), livyReStartWait=True, wait=15):
        if Machine.isLinux():
            if cls.stopLivyServer(user=user, livyStopWait=False):
                time.sleep(wait)
                return cls.startLivyServer(user=user, livyStartUpWait=livyReStartWait, wait=wait)
            else:
                return False
        return False

    @classmethod
    @TaskReporter.report_test()
    def isSecurityEnabled(cls):
        val = cls.getConfigFromLivyConf('livy.server.auth.kerberos.keytab')
        return val and bool(val)

    @classmethod
    @TaskReporter.report_test()
    def _kerberosAuth(cls):
        if cls.isSecurityEnabled():
            return HTTPKerberosAuth(mutual_authentication=REQUIRED, sanitize_mutual_error_response=False)
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def isSSLEnabled(cls):
        '''
        This function checks if wire encryption is enabled for Livy
        :return:
        '''
        val = cls.getConfigFromLivyConf('livy.keystore')
        return val and bool(val)

    @classmethod
    @TaskReporter.report_test()
    def _get_livy_url(cls):
        if cls.isSSLEnabled():
            protocol = "https://"
        else:
            protocol = "http://"
        return protocol + str(cls.getLivyServerIpAddress()) + ':' + str(cls.getLivyServerPort())

    @classmethod
    @TaskReporter.report_test()
    def getAllSessions(cls):
        r = requests.get(
            cls._get_livy_url() + '/sessions', headers=Livy._headers, auth=cls._kerberosAuth(), verify=False
        )
        if r.status_code == 200:
            return r.json()['sessions']
        else:
            raise Livy_Exception('Cannot get all sessions: ' + str(r.status_code))

    @classmethod
    @TaskReporter.report_test()
    def deleteSession(cls, sessionId):
        r = requests.delete(
            cls._get_livy_url() + '/sessions/' + str(sessionId),
            headers=Livy._headers,
            auth=cls._kerberosAuth(),
            verify=False
        )
        if r.status_code != 200:
            raise Livy_Exception('Cannot delete session ' + str(sessionId))

    @classmethod
    @TaskReporter.report_test()
    def createSession(cls):
        data = {'kind': 'spark', 'numExecutors': 1}
        r = requests.post(
            cls._get_livy_url() + '/sessions',
            data=json.dumps(data),
            headers=Livy._headers,
            auth=cls._kerberosAuth(),
            verify=False
        )

        # Test whether Livy Session is started and idle
        isIdle = False
        curr_time = time.time()
        session_url = cls._get_livy_url() + r.headers['location']
        session_id = 0
        while (not isIdle and time.time() - curr_time < 120):
            time.sleep(1)
            r = requests.get(session_url, headers=Livy._headers, auth=cls._kerberosAuth(), verify=False)
            if r.json()['state'] == 'idle':
                isIdle = True
                session_id = r.json()['id']

        assert isIdle, "Livy session is not started correctly"
        return session_id

    @classmethod
    @TaskReporter.report_test()
    def submitStatement(cls, sessionId, executeCode):
        statement = {'code': executeCode}
        statement_url = cls._get_livy_url() + '/sessions/' + str(sessionId) + '/statements'
        r = requests.post(
            statement_url, data=json.dumps(statement), headers=Livy._headers, auth=cls._kerberosAuth(), verify=False
        )

        curr_time = time.time()
        result = ''
        statement_id = 0
        statement_get_url = cls._get_livy_url() + r.headers['location']
        while (result == '' and time.time() - curr_time < 60):
            time.sleep(1)
            r = requests.get(statement_get_url, headers=Livy._headers, auth=cls._kerberosAuth(), verify=False)
            if r.json()['state'] == 'available':
                result = r.json()['output']['data']['text/plain']
                statement_id = r.json()['id']
        return (statement_id, result)

    @classmethod
    @TaskReporter.report_test()
    def getStatement(cls, sessionId, statementId):
        statement_get_url = cls._get_livy_url() + '/sessions/' + str(sessionId) + '/statements/' + str(statementId)

        curr_time = time.time()
        result = ''
        while (result == '' and time.time() - curr_time < 60):
            time.sleep(1)
            r = requests.get(statement_get_url, headers=Livy._headers, auth=cls._kerberosAuth(), verify=False)
            if r.json()['state'] == 'available':
                result = r.json()['output']['data']['text/plain']
        return result
