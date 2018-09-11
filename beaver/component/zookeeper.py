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
import logging
import sys
import re
import time
from beaver.component.ambari import Ambari
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils

logger = logging.getLogger(__name__)

ZK_HOME = Config.get('zookeeper', 'ZK_HOME')
ZOOKEEPER_CONF_DIR = Config.get('zookeeper', 'ZOOKEEPER_CONF_DIR')
ZOOKEEPER_USER = Config.get('zookeeper', 'ZOOKEEPER_USER', 'zookeeper')
ZOO_CFG_FILE = os.path.join(ZOOKEEPER_CONF_DIR, 'zoo.cfg')
ZOOKEEPER_ENV_FILE = os.path.join(ZOOKEEPER_CONF_DIR, 'zookeeper-env.sh')
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')

if Machine.type() == 'Windows':
    ZK_CLI_CMD = os.path.join(ZK_HOME, 'bin', 'zkCli.cmd')
    NETCAT_CMD = Config.get('zookeeper', 'ZOOKEEPER_NETCAT_HOME')
    ZK_SERVER_CMD = os.path.join(ZK_HOME.replace("client", "server"), 'bin', 'zkServer.cmd')
else:
    ZK_CLI_CMD = os.path.join(ZK_HOME, 'bin', 'zkCli.sh')
    NETCAT_CMD = "nc"
    ZK_SERVER_CMD = os.path.join(ZK_HOME.replace("client", "server"), 'bin', 'zkServer.sh')

if Ambari.is_cluster_secure():
    keytabFile = Machine.getHeadlessUserKeytab(HADOOPQA_USER)
    _kinitloc = Machine.which("kinit", "root")
    _cmd = "%s  -k -t %s %s" % (_kinitloc, keytabFile, Machine.get_user_principal(HADOOPQA_USER))
    exit_code, output = Machine.run(_cmd)


class Zookeeper(object):
    _zkHosts = None
    _version = None
    _zkPort = None

    @classmethod
    def run(cls, cmd, env=None, logoutput=True, runKinit=True):
        return cls.runas(user=None, cmd=cmd, env=env, logoutput=logoutput, runKinit=runKinit)

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True, runKinit=True):
        if Ambari.is_cluster_secure() and runKinit:
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user=user, rmIfExists=True)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        return Machine.runas(user, cmd, env=env, logoutput=logoutput)

    # method to run zkCli cmd
    @classmethod
    def runZKCli(cls, cmd, server='localhost', port=2181, env=None, logoutput=True, runKinit=True):
        runCmd = "%s -server %s:%s -timeout 60000 %s" % (ZK_CLI_CMD, server, port, cmd)
        return cls.run(cmd=runCmd, env=env, logoutput=logoutput, runKinit=runKinit)

    # method to run zkServer cmd
    @classmethod
    def runZKServer(cls, cmd, host="", env=None, logoutput=True):
        if Machine.isLinux():
            runCmd = "%s %s" % (ZK_SERVER_CMD, cmd)
            return Machine.runas(ZOOKEEPER_USER, runCmd, host, env=env, logoutput=logoutput)
        else:
            Machine.service("zkServer", cmd, host=host)
            return None

    # method to run the nc command
    @classmethod
    def runNC(cls, cmd, server='localhost', port=2181):
        runCmd = "echo %s | %s %s %s" % (cmd, NETCAT_CMD, server, port)
        return Machine.run(runCmd)

    @classmethod
    def checkExceptions(cls, stdout):
        stdout = stdout.split(os.linesep)
        for line in stdout:
            if "Exception" in line:
                if "java.lang.SecurityException: Unable to locate a login configuration" in line:
                    logger.info("Ignore this error")
                else:
                    assert True, "Exception exists in the output"

    # method to get all zookeeper hosts from the config
    @classmethod
    def getZKHosts(cls, ignoreError=False):
        if cls._zkHosts and len(cls._zkHosts) >= 3:
            return cls._zkHosts

        cls._zkHosts = Ambari.getHostsForComponent('ZOOKEEPER_SERVER')
        return cls._zkHosts

    @classmethod
    def getZKPort(cls):
        if not cls._zkPort:
            zoo_props = Ambari.getConfig("zoo.cfg")
            if zoo_props:
                cls._zkPort = zoo_props["clientPort"]
        return cls._zkPort

    @classmethod
    def getVersion(cls):
        if cls._version:
            return cls._version

        # get a zookeeper node
        zkNode = cls.getZKHosts()[0]
        _exit_code, _output = cls.runNC(cmd='stat', server=zkNode, port=2181)
        if _exit_code == 0:
            pattern = re.compile(r"Zookeeper version: (\S+),")
            m = pattern.search(_output)
            if m:
                cls._version = m.group(1)
            else:
                cls._version = ""
        else:
            cls._version = ""

        return cls._version

    @classmethod
    def getMode(cls, zkNode):
        _exit_code, _output = cls.runNC(cmd='stat', server=zkNode, port=2181)
        if _exit_code == 0:
            pattern = re.compile(r"Mode: (\S+)")
            m = pattern.search(_output)
            if m:
                return m.group(1)
            else:
                return ""
        return None

    @classmethod
    def getZooLogDir(cls, logoutput=False):
        '''
    Returns Zookeeper log directory (String).
    '''
        matchObjList = None
        if Machine.isHumboldt():
            try:
                from beaver.component.hbase import HBase
                #get some zookeeper node
                hmaster_nodes = HBase.getAllMasterNodes()
                if hmaster_nodes:
                    zkNode = hmaster_nodes[0]
                if zkNode:
                    Machine.copyToLocal(None, zkNode, ZOOKEEPER_ENV_FILE, Machine.getTempDir())
                    REMOTE_ZOOKEEPER_ENV_FILE = os.path.join(Machine.getTempDir(), 'zookeeper-env.sh')
                    matchObjList = util.findMatchingPatternInFile(
                        REMOTE_ZOOKEEPER_ENV_FILE, "export ZOO_LOG_DIR=(.*)", return0Or1=False
                    )
            except Exception:
                pass
        if not matchObjList:
            #gateway should have the config file.
            matchObjList = util.findMatchingPatternInFile(
                ZOOKEEPER_ENV_FILE, "export ZOO_LOG_DIR=(.*)", return0Or1=False
            )
        returnValue = None
        if matchObjList:
            returnValue = matchObjList[0].group(1)
        if logoutput:
            logger.info("Zookeeper.getZooLogDir returns %s", returnValue)
        return returnValue

    @classmethod
    def modifyConfig(cls, changes, nodes, isFirstUpdate=True):
        zk_conf = Config.get('zookeeper', 'ZOOKEEPER_CONF_DIR')
        tmp_conf = os.path.join(Machine.getTempDir(), 'zkConf')
        configUtils.modifyConfig(changes, zk_conf, tmp_conf, nodes, isFirstUpdate)

    @classmethod
    def getModifiedConfigPath(cls):
        if Machine.type() == 'Windows':
            return Config.get('zookeeper', 'ZOOKEEPER_CONF_DIR')
        else:
            return os.path.join(Machine.getTempDir(), 'zkConf')

    @classmethod
    def stopZKServers(cls, env=None, wait=3, zkServers=None):
        if not zkServers:
            zkServers = []

        if zkServers == []:
            zkServers = cls.getZKHosts()
        for server in zkServers:
            cls.runZKServer("stop", host=server, env=env)
        time.sleep(wait)

    @classmethod
    def startZKServers(cls, env=None, wait=3, zkServers=None):
        if not zkServers:
            zkServers = []

        if zkServers == []:
            zkServers = cls.getZKHosts()
        for server in zkServers:
            cls.runZKServer("start", host=server, env=env)
        time.sleep(wait)

    @classmethod
    def getZKpid(cls, host=None):
        '''
      Find zookeeper pid from machine
      :param host: machine hostname
      :return: pid of zookeeper service
      '''
        service = "zookeeper"
        if Machine.isWindows():
            pid = Machine.getProcessListWithPid(host, "java.exe", service, logoutput=True, ignorecase=True)
            return int(pid)
        else:
            pids = Machine.getProcessListRemote(host, format="%U %p %P %a", filter=service, user=None, logoutput=True)
            for p in pids:
                pid = Machine.getPidFromString(p, user=None, isPidStringWithoutuser=True)
                if not pid:
                    # TODO: Need to convert into raw string to fix pylint issue. Disabled it temporarily.
                    pid = Machine.getPidFromString(p, user="zookeep\+", isPidStringWithoutuser=False)  # pylint: disable=anomalous-backslash-in-string
                if pid:
                    return int(pid)
        return None

    @classmethod
    def killZkService(cls, host=None, user=None, passwd=None, logoutput=True):
        '''
      Kill Zk service
      :return:
      '''
        pid = cls.getZKpid(host)
        if Machine.isLinux():
            _exit_code, _ = Machine.killProcessRemote(pid, host, user, passwd, logoutput)
        else:
            _exit_code = Machine.killProcessRemote(
                pid, host, Machine.getAdminUser(), Machine.getAdminPasswd(), logoutput
            )
        return _exit_code

    @classmethod
    def suspendZkService(cls, host=None, user=None, passwd=None, logoutput=True):
        '''
      Suspend Zk service
      :return:
      '''
        pid = cls.getZKpid(host)
        _exit_code, _ = Machine.shutdownProcessRemote(pid, host, user, passwd, logoutput)
        return _exit_code

    @classmethod
    def killAllZkService(cls, allZkhosts=None):
        '''
      Kill all Zk services
      :return:
      '''
        if not allZkhosts:
            allZkhosts = []

        if allZkhosts == []:
            allZkhosts = cls.getZKHosts()
        for zk in allZkhosts:
            cls.killZkService(zk)

    @classmethod
    def suspendAllZkService(cls, allZkhosts=None):
        '''
      Suspend all Zk services
      :return:
      '''
        if not allZkhosts:
            allZkhosts = []

        if allZkhosts == []:
            allZkhosts = cls.getZKHosts()
        for zk in allZkhosts:
            cls.suspendZkService(zk)
