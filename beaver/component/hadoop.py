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
import string
import time
import socket
import logging
import collections
import datetime
import json
import threading
import Queue
import re
import xml.etree.ElementTree as ET

from ConfigParser import ConfigParser, NoOptionError
from urllib import quote, urlencode
from urlparse import urlparse

from taskreporter.taskreporter import TaskReporter
from beaver.component.dataStructure.yarnContainerLogs import YarnContainerLogs
from beaver.config import Config
from beaver import util, configUtils
from beaver.machine import Machine
from beaver.util import getHTTPResponse, getValue, getElement, getAttribute, \
    query_yarn_web_service, ats_v2_url_encode
from beaver.component.ambari import Ambari

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
ADMIN_USER = Machine.getAdminUser()
ADMIN_PWD = Machine.getAdminPasswd()
HRT_QA = Config.get('hadoop', 'HADOOPQA_USER')
QE_EMAIL = util.get_qe_group_email()


class Hadoop(object):
    _isSecure = None
    _version = None
    # default the hadoop version to 1
    _isHadoop2 = False
    _pid_dir = None
    _xmlConfigMap = {
        'core-site.xml': 'core-site',
        'hdfs-site.xml': 'hdfs-site',
        'hadoop-env.sh': 'hadoop-env',
        'log4j.properties': 'hdfs-log4j',
        'hadoop-metrics2.properties': 'hadoop-metrics2',
        'yarn-site.xml': 'yarn-site',
        'capacity-scheduler.xml': 'capacity-scheduler',
        'yarn-env.sh': 'yarn-env',
        'yarn-log4j.properties': 'yarn-log4j',
        'mapred-site.xml': 'mapred-site',
        'mapred-log4j.properties': 'mapred-log4j'
    }
    _hdfsPropertyFileList = [
        'core-site.xml', 'hdfs-site.xml', 'hadoop-env.sh', 'log4j.properties', 'hadoop-metrics2.properties'
    ]
    # TODO: Update PropertyFileLists for Yarn & Mapreduce
    _yarnPropertyFileList = ['yarn-site.xml', 'capacity-scheduler.xml', 'yarn-env.sh', 'yarn-log4j.properties']
    _mapredPropertyFileList = ['mapred-site.xml', 'mapred-log4j.properties']
    _ambariServiceHDFSName = 'HDFS'
    _ambariServiceYARNName = 'YARN'
    _ambariServiceMAPRED2Name = 'MAPREDUCE2'
    _ambariServiceHDFSConfigVersion = None
    _ambariServiceYARNConfigVersion = None
    _ambariServiceMAPRED2ConfigVersion = None
    _compSvcMap = {
        'namenode': 'NAMENODE',
        'secondarynamenode': 'SECONDARY_NAMENODE',
        'datanodes': 'DATANODE',
        'nfs3': 'NFS_GATEWAY',
        'journalnode': 'JOURNALNODE',
        'zkfc': 'ZKFC',
        'hdfsclient': 'HDFS_CLIENT',
        'jobtracker': 'RESOURCEMANAGER',
        'tasktrackers': 'NODEMANAGER',
        'timelineserver': 'APP_TIMELINE_SERVER',
        'yarnclient': 'YARN_CLIENT',
        'historyserver': 'HISTORYSERVER',
        'mapreduceclient': 'MAPREDUCE2_CLIENT',
        'yarnregistry': 'YARN_REGISTRY_DNS',
        'timelinereader': 'TIMELINE_READER'
    }
    _compSvcTimeouts = {
        'namenode': 900,
        'secondarynamenode': 300,
        'datanodes': 300,
        'nfs3': 300,
        'journalnode': 300,
        'zkfc': 300,
        'hdfsclient': 300,
        'jobtracker': 300,
        'tasktrackers': 300,
        'timelineserver': 300,
        'yarnclient': 300,
        'historyserver': 300,
        'mapreduceclient': 300,
        'HDFS': 1800,
        'YARN': 300,
        'MAPREDUCE2': 300,
        'yarnregistry': 300,
        'timelinereader': 300
    }
    _namenodeHosts = None
    _secondaryNamenodeHost = None
    _datanodeHosts = None
    _nfsGatewayHosts = None
    _journalnodeHosts = None
    _zkfcHosts = None
    _jobtrackerHosts = None
    _tasktrackersHosts = None
    _timelineserverHosts = None
    _historyserverHost = None
    _yarnclientHost = None
    _yarnTimelineReader = None

    HDFS_NUMBER_OF_BYTES_READ = 'HDFS: Number of bytes read'
    HDFS_NUMBER_OF_BYTES_WRITTEN = 'HDFS: Number of bytes written'

    if Machine.type() == 'Linux':
        _pid_dir = Config.get('hadoop', 'HADOOP_PID_DIR')

    # method to kill a service instead of shutting it down
    @classmethod
    @TaskReporter.report_test()
    def killService(cls, service, user, host, pidFile=None, suspend=False):
        comment = 'service %s on host %s' % (service, host)
        if suspend:
            logger.info('Suspend %s', comment)
        else:
            logger.info('Kill %s', comment)
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
                return Machine.sendSIGSTOP(Machine.getAdminUser(), host, pid)
        else:
            killOption = '-9'
            if suspend:
                killOption = '-19'
            # allow the user to send a custom pid file
            if not pidFile:
                pidFile = os.path.join(cls._pid_dir, user, 'hadoop-%s-%s.pid' % (user, service))
            killCmd = "cat %s | xargs kill %s" % (pidFile, killOption)
            return Machine.runas(user, killCmd, host=host)

    # TODO: sending host in secure mode wont work as we need to refactor how
    # we get kerberos credentials on a specific host
    @classmethod
    def runas(
            cls,
            user,
            cmd,
            env=None,
            logoutput=True,
            config=None,
            host=None,
            skipAuth=False,
            cwd=None,
            stderr_as_stdout=True,
            forceNonVersionedRPMCmd=False,
            overrideSecurityType=None,
            doEscapeQuote=True
    ):
        """
        Hadoop.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :param config:
        :param host:
        :param skipAuth:
        :param cwd:
        :param stderr_as_stdout:
        :param forceNonVersionedRPMCmd: if True, use non-versioned rpm command regardless of cluster setup.
        :param overrideSecurityType: if None, figure out kinit based on cluster setup.
                                  if secure, always do kinit.
                                  if unsecure, always not do kinit.
        :return: (exit_code, stdout)
        """
        if (overrideSecurityType is None and cls.isSecure()
                and not skipAuth) or (overrideSecurityType is not None and overrideSecurityType.lower() == "secure"):
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None

        hadoop_cmd = Config.get('hadoop', 'HADOOP_CMD')
        if forceNonVersionedRPMCmd:
            hadoop_cmd = "/usr/lib/hadoop/bin/hadoop"
        if config:
            hadoop_cmd += " --config " + config
        elif Config.get('hadoop', 'HADOOP_CONF_EXCLUDE') == 'False':
            hadoop_cmd += " --config " + Config.get('hadoop', 'HADOOP_CONF')
        hadoop_cmd += " " + cmd
        if stderr_as_stdout:
            return Machine.runas(
                user, hadoop_cmd, env=env, logoutput=logoutput, host=host, cwd=cwd, doEscapeQuote=doEscapeQuote
            )
        else:
            return Machine.runexas(user, hadoop_cmd, env=env, logoutput=logoutput, host=host, cwd=cwd)

    @classmethod
    def runInBackgroundAs(cls, user, cmd, env=None, config=None, stdout=None, stderr=None):
        if cls.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        hadoop_cmd = Config.get('hadoop', 'HADOOP_CMD')
        if config:
            hadoop_cmd += " --config " + config
        elif Config.get('hadoop', 'HADOOP_CONF_EXCLUDE') == 'False':
            hadoop_cmd += " --config " + Config.get('hadoop', 'HADOOP_CONF')
        hadoop_cmd += " " + cmd
        return Machine.runinbackgroundAs(user, hadoop_cmd, env=env, stdout=stdout, stderr=stderr)

    @classmethod
    def run(cls, cmd, env=None, logoutput=True, config=None, cwd=None):
        return cls.runas(None, cmd, env=env, logoutput=logoutput, config=config, cwd=cwd)

    @classmethod
    def runInBackground(cls, cmd, env=None, config=None, stdout=None, stderr=None):
        return cls.runInBackgroundAs(None, cmd, env=env, config=config, stdout=stdout, stderr=stderr)

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "core-site.xml"),
            propertyValue,
            defaultValue=defaultValue
        )

    @classmethod
    @TaskReporter.report_test()
    def isUIKerberozied(cls):
        '''
        Returns true if UI is spenego kerberozied
        '''
        httpAuthType = cls.getConfigValue("hadoop.http.authentication.type", "simple")
        return httpAuthType == "kerberos"


    @classmethod
    @TaskReporter.report_test()
    def getConfigFromHadoopEnv(cls, propertyValue, defaultValue=None):
        hadoopEnv = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hadoop-env.sh")
        rfile = open(hadoopEnv, "r")
        for line in rfile:
            if line.find(propertyValue) >= 0:
                print line
                m = re.search(r'(\S*)=(\S*)', line)
                if m:
                    return m.group(2)
                else:
                    return None
        return defaultValue

    @classmethod
    @TaskReporter.report_test()
    def getFSDefaultValue(cls, withPort=True):
        if cls.isHadoop2():
            configKey = 'fs.defaultFS'
        else:
            configKey = 'fs.default.name'

        namenodeWithPort = cls.getConfigValue(configKey)

        if withPort:
            return namenodeWithPort
        else:
            return urlparse(namenodeWithPort).hostname

    @classmethod
    @TaskReporter.report_test()
    def getSlaves(cls):
        slaveFile = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "slaves")
        f = open(slaveFile, "r")
        tasktrackers = f.readlines()
        return tasktrackers

    _isAmbari = None

    @classmethod
    @TaskReporter.report_test()
    def isAmbari(cls, force=False):
        # if we have computed this information earlier, let us use that.
        if not force and cls._isAmbari is not None:
            return cls._isAmbari
        if (Machine.isDebian()) or (Machine.isUbuntu()):
            command = "dpkg --get-selections | grep ambari"
        elif Machine.isWindows():
            command = "wmic /node:localhost product get name | findstr Ambari"
        else:
            command = "rpm -qa | grep ambari"
        _, stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
        )
        cls._isAmbari = False
        if "ambari-server" in stdout or "ambari-agent" in stdout:
            cls._isAmbari = True
        return cls._isAmbari

    @classmethod
    @TaskReporter.report_test()
    def isTez(cls, checkMRConfig=False, checkHiveConfig=True):
        if Machine.type() != "Windows":
            if not Hadoop.isAmbari() and Machine.pathExists(user=None, host=None, filepath=Config.get(
                    'tez', 'TEZ_HOME'), passwd=None):
                if checkHiveConfig:
                    try:
                        return util.getPropertyValueFromConfigXMLFile(
                            os.path.join(Config.get('hive', 'HIVE_CONF_DIR'), "hive-site.xml"),
                            "hive.execution.engine",
                            defaultValue="mr"
                        ) == "tez"
                    except IOError:
                        return False
                elif not checkMRConfig:
                    return True
                else:
                    return util.getPropertyValueFromConfigXMLFile(
                        os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"),
                        "mapreduce.framework.name", defaultValue="yarn") == "yarn-tez"
            elif Hadoop.isAmbari():
                if checkHiveConfig:
                    try:
                        from beaver.component.hive import Hive
                        if Hive.isHive2():
                            return Hive.getConfigValue('hive.execution.engine') == "tez"
                        return util.getPropertyValueFromConfigXMLFile(
                            os.path.join(Config.get('hive', 'HIVE_CONF_DIR'), "hive-site.xml"),
                            "hive.execution.engine", defaultValue="mr") == "tez"
                    except (IOError, NoOptionError):
                        return False
                else:
                    return util.getPropertyValueFromConfigXMLFile(
                        os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"),
                        "mapreduce.framework.name", defaultValue="yarn") == "yarn-tez"
            else:
                return False

        else:
            #Windows
            if checkMRConfig:
                return util.getPropertyValueFromConfigXMLFile(
                    os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"),
                    "mapreduce.framework.name", defaultValue="yarn") == "yarn-tez"
            else:
                return util.getPropertyValueFromConfigXMLFile(
                    os.path.join(Config.get('hive', 'HIVE_CONF_DIR'), "hive-site.xml"),
                    "hive.execution.engine", defaultValue="mr") == "tez"

    @classmethod
    def isEncrypted(cls):
        return HDFS.isEncrypted()

    @classmethod
    @TaskReporter.report_test()
    def daemon(
            cls,
            user,
            host,
            service,
            action,
            config=None,
            homePath=None,
            binFolder='bin',
            daemonName='hadoop-daemon',
            libexecPath=None,
            option=None,
            env=None
    ):
        # Pylint changes for unused-argument
        logger.info(libexecPath)
        cmd = ""
        #env = {}
        if homePath is None:
            if service in ['jobtracker', 'tasktracker', 'historyserver', 'resourcemanager', 'nodemanager',
                           'timelineserver']:
                homePath = Config.get('hadoop', 'YARN_HOME').replace("client", service)
            elif service in ['namenode', 'datanode', 'secondarynamenode', 'nfs3', 'portmap', 'journalnode']:
                logger.info(Config.get('hadoop', 'HDFS_HOME'))
                homePath = Config.get('hadoop', 'HDFS_HOME').replace("client", service) + "/../hadoop"
            # BUG-25534: Use Namenode link for zkfc until the bug is fixed
            elif service == 'zkfc':
                logger.info(Config.get('hadoop', 'HDFS_HOME'))
                homePath = Config.get('hadoop', 'HDFS_HOME').replace("client", 'namenode') + "/../hadoop"
            else:
                homePath = Config.get('hadoop', 'HADOOP_HOME')
        #if libexecPath: env['HADOOP_LIBEXEC_DIR'] = libexecPath
        cmd += os.path.join(homePath, binFolder, daemonName)
        if Machine.type() == 'Linux':
            cmd += ".sh"
        if config:
            cmd += " --config " + config
        cmd += " %s %s" % (action, service)
        if option:
            cmd = cmd + " %s" % option
        return Machine.runas(user, cmd, host=host, env=env)

    @classmethod
    @TaskReporter.report_test()
    def resetService(
            cls,
            user,
            host,
            service,
            action,
            config=None,
            homePath=None,
            binFolder='bin',
            daemonName='hadoop-daemon',
            libexecPath=None,
            option=None,
            env=None
    ):
        if Machine.type() == 'Windows':
            Machine.service(service, action, host=host)
        else:
            Hadoop.daemon(
                user, host, service, action, config, homePath, binFolder, daemonName, libexecPath, option, env=env
            )

    @classmethod
    @TaskReporter.report_test()
    def isSecure(cls):
        if cls._isSecure is None:
            cls._isSecure = cls.getConfigValue("hadoop.security.authentication", "simple") == "kerberos"
        return cls._isSecure

    @classmethod
    @TaskReporter.report_test()
    def startCluster(cls, config=None, secondaryNN=False, historyServer=False, logoutput=False):
        HDFS.startServices(config=config, secondaryNN=secondaryNN, logoutput=logoutput)
        MAPRED.startServices(config=config, historyServer=historyServer)

    @classmethod
    @TaskReporter.report_test()
    def stopCluster(cls, secondaryNN=False, historyServer=False):
        MAPRED.stopServices(historyServer=historyServer)
        HDFS.stopServices(secondaryNN=secondaryNN)

    # Pass service name such as for Hadoop2, pass resourcemanager and for Hadoop1, pass jobtracker along
    #  with the hostname where the process is running
    @classmethod
    @TaskReporter.report_test()
    def getServicePid(cls, node, service):
        if cls.isHadoop2():
            if service == "nodemanager" or service == "resourcemanager":
                user = "yarn"
            elif service == "historyserver":
                user = "mapred"
            else:
                user = "hdfs"
        else:
            if service == "tasktracker" or service == "jobtracker" or service == "historyserver":
                user = "mapred"
            else:
                user = "hdfs"
        if Machine.type() != "Windows":
            if Machine.isSameHost(node, HDFS.getGateway()):
                _, stdout = Machine.run("ps aux | grep \"%s\"" % (service), logoutput=True)
            else:
                _, stdout = Machine.run(
                    "ssh -o StrictHostKeyChecking=no %s \"ps aux | grep \"%s\"\"" % (node, service), logoutput=True
                )
            if Hadoop.isSecure() and service == "datanode":
                for x in stdout.split("\n"):
                    if x.find("jsvc.exec -Dproc_%s" % (service), 0) > -1:
                        m = re.match(r'(hdfs[\s]+)([0-9]+)', x)
                        if m:
                            return m.group(2)
                return "notFound"
            else:
                for x in stdout.split("\n"):
                    if x.find("java -Dproc_%s" % (service), 0) > -1:
                        m = re.match(r'(%s[\s]+)([0-9]+)' % (user), x)
                        if m:
                            return m.group(2)
                return "notFound"
        else:
            logger.info("getService Pid is not supported on windows")
            return "notFound"

    @classmethod
    @TaskReporter.report_test()
    def getAllNodes(cls):
        dn_hosts = []
        for item in HDFS.getDatanodes():
            if item != ' ':
                dn_hosts.append(socket.gethostbyaddr(item)[0])
        if HDFS.isHAEnabled():
            nn_HA = HDFS.getHANameNodes()
        else:
            nn_HA = [HDFS.getNamenode()]
        if YARN.isHAEnabled():
            rm_HA = [YARN.getRMHostByState('standby'), YARN.getRMHostByState('active')]
        else:
            rm_HA = [MAPRED.getJobtracker()]
        nodeset = set(
            [socket.gethostbyaddr(HDFS.getGateway())[0]] + dn_hosts + MAPRED.getTasktrackers() + nn_HA +
            [HDFS.getSecondaryNamenode()] + rm_HA + [MAPRED.getHistoryserver()]
        )
        return nodeset

    @classmethod
    @TaskReporter.report_test()
    def getModifiedConfigPath(cls):
        if Machine.type() == 'Windows':
            return Config.get('hadoop', 'HADOOP_CONF')
        else:
            return os.path.join(Machine.getTempDir(), 'hadoopConf')

    @classmethod
    def getHadoopHome(cls):
        '''
        Returns HADOOP_HOME. Use cautiously in Linux.
        '''
        return Config.get('hadoop', 'HADOOP_HOME')

    @classmethod
    def getHadoopGroup(cls):
        '''
        Returns HADOOP_GROUP ("hadoop").
        '''
        return Config.get('hadoop', 'HADOOP_GROUP')

    @classmethod
    def getHadoopBinHome(cls):
        return os.path.join(cls.getHadoopHome(), 'bin')

    @classmethod
    @TaskReporter.report_test()
    def getHadoopEnvSh(cls):
        if Machine.isLinux():
            return 'hadoop-env.sh'
        else:
            return 'hadoop-env.cmd'

    @classmethod
    @TaskReporter.report_test()
    def getStartingAmbariServiceConfigVersion(cls, serviceName, update=False):
        if serviceName == cls._ambariServiceHDFSName:
            if cls._ambariServiceHDFSConfigVersion is None or update:
                cls._ambariServiceHDFSConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceHDFSName)
            return cls._ambariServiceHDFSConfigVersion
        elif serviceName == cls._ambariServiceYARNName:
            if cls._ambariServiceYARNConfigVersion is None or update:
                cls._ambariServiceYARNConfigVersion = Ambari.getCurrentServiceConfigVersion(cls._ambariServiceYARNName)
            return cls._ambariServiceYARNConfigVersion
        elif serviceName == cls._ambariServiceMAPRED2Name:
            if cls._ambariServiceMAPRED2ConfigVersion is None or update:
                cls._ambariServiceMAPRED2ConfigVersion = Ambari.getCurrentServiceConfigVersion(
                    cls._ambariServiceMAPRED2Name
                )
            return cls._ambariServiceMAPRED2ConfigVersion
        else:
            logger.info("Not valid service name")
            return None

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(
            cls,
            changes,
            nodeSelection,
            isFirstUpdate=True,
            makeCurrConfBackupInWindows=True,
            regenServiceXmlsInWindows=False,
            env=None,
            restartService=False,
            useAmbari=False,
            deleteConfig=False
    ):
        if not env:
            env = {}
        if useAmbari:
            cls.modifyConfigWithAmbari(changes, nodeSelection, env, restartService, deleteConfig)
        else:
            cls.modifyConfigWithoutAmbari(
                changes, nodeSelection, isFirstUpdate, makeCurrConfBackupInWindows, regenServiceXmlsInWindows
            )

    @classmethod
    @TaskReporter.report_test()
    def modifyConfigWithoutAmbari(
            cls,
            changes,
            nodeSelection,
            isFirstUpdate=True,
            makeCurrConfBackupInWindows=True,
            regenServiceXmlsInWindows=False
    ):
        '''
        Modify config.
        Returns None.
        makeCurrConfBackupInWindows argument is very important in Windows.
        Taking good copy as backup and not replacing it with bad copy is needed.
        Current config means the config before modifications.
        changes is a map.

        Modify config takes source config (local /etc/hadoop/conf) as source and add entries from there.
        '''
        nodes = cls.getSelectedNodes(nodeSelection)
        hadoop_conf = Config.get('hadoop', 'HADOOP_CONF')
        tmp_conf = os.path.join(Machine.getTempDir(), 'hadoopConf')
        configUtils.modifyConfig(
            changes,
            hadoop_conf,
            tmp_conf,
            nodes,
            isFirstUpdate,
            makeCurrConfBackupInWindows=makeCurrConfBackupInWindows
        )
        if Machine.isWindows() and cls.getHadoopEnvSh() in changes.keys():
            if regenServiceXmlsInWindows:
                cls.regenServiceXmlsInWindows(nodeSelection)
            else:
                logger.info("WARNING: modifyConfig in Windows without propery xml generation.")

    @classmethod
    @TaskReporter.report_test()
    def modifyConfigWithAmbari(cls, changes, nodeSelection, env=None, restartService=True, deleteConfig=False):
        '''
        Modify config using Ambari
        '''
        # Get Current Config version
        if not env:
            env = {}
        propertyFilesToBeUpdated = changes.keys()
        if [i for i in cls._hdfsPropertyFileList if i in propertyFilesToBeUpdated]:
            logger.info(
                "Current Service Config Version for HDFS : %s",
                cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceHDFSName)
            )
        if [i for i in cls._yarnPropertyFileList if i in propertyFilesToBeUpdated]:
            logger.info(
                "Current Service Config Version for YARN : %s",
                cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceYARNName)
            )
        if [i for i in cls._mapredPropertyFileList if i in propertyFilesToBeUpdated]:
            logger.info(
                "Current Service Config Version for MAPRED2 : %s",
                cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceMAPRED2Name)
            )

        for key, value in changes.items():
            if cls._xmlConfigMap.has_key(key):
                key = cls._xmlConfigMap[key]
                # For log4j.properties need to overwrite the existing content with new values.
                if key == 'hdfs-log4j':
                    log4jProps = Ambari.getConfig(key)
                    if log4jProps.has_key("content"):
                        content = log4jProps['content']
                        for log4jKey, log4jVal in value.items():
                            content = "%s\n%s=%s\n" % (content, log4jKey, log4jVal)
                        value = {'content': content}
            else:
                logger.warn("Unknown config %s change requested, ignoring", key)
                continue
            if deleteConfig:
                Ambari.deleteConfig(key, value)
            else:
                Ambari.setConfig(key, value)
        if env.keys():
            for key in env.keys():
                ambariKey = cls._xmlConfigMap[key]
                envProps = Ambari.getConfig(ambariKey)
                if envProps.has_key("content"):
                    content = envProps['content']
                    for envKey, envVal in env[key].items():
                        content = "%s\nexport %s=\"%s ${%s}\"\n" % (content, envKey, envVal, envKey)
                    Ambari.setConfig(ambariKey, {'content': content})

        if restartService:
            services = cls.getServiceNamesFromDict(nodeSelection)
            cls.restartServices(services)

    @classmethod
    @TaskReporter.report_test()
    def startServices(cls, services=None):
        for service in services:
            hosts = cls.getServiceHosts(service)
            for host in hosts:
                Ambari.startComponent(
                    host, cls._compSvcMap[service], waitForCompletion=True, timeout=cls._compSvcTimeouts[service]
                )
        # The previous code - if  'jobtracker' or 'timelineserver' or 'historyserver' or 'tasktrackers' in services
        # Did not seem to work. Added below tested code
        for s in ['jobtracker', 'timelineserver', 'historyserver', 'tasktrackers']:
            if s in services:
                time.sleep(10)
                return
        # This will only be executed if service is not one of 'jobtracker', 'timelineserver',
        #  'historyserver', 'tasktrackers'
        time.sleep(30)

    @classmethod
    @TaskReporter.report_test()
    def stopServices(cls, services=None):
        for service in services:
            hosts = cls.getServiceHosts(service)
            for host in hosts:
                Ambari.stopComponent(
                    host, cls._compSvcMap[service], waitForCompletion=True, timeout=cls._compSvcTimeouts[service]
                )
        # The previous code - if  'jobtracker' or 'timelineserver' or 'historyserver' or 'tasktrackers' in services
        # Did not seem to work. Added below tested code
        for s in ['jobtracker', 'timelineserver', 'historyserver', 'tasktrackers']:
            if s in services:
                return
        # This will only be executed if service is not one of 'jobtracker', 'timelineserver',
        #  'historyserver', 'tasktrackers'
        time.sleep(15)

    @classmethod
    @TaskReporter.report_test()
    def restartServices(cls, services=None):
        if 'all' in services:
            Ambari.start_stop_service(
                'HDFS', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['HDFS']
            )
            Ambari.start_stop_service(
                'YARN', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['YARN']
            )
            Ambari.start_stop_service(
                'MAPREDUCE2', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['MAPREDUCE2']
            )
            # workaround for BUG-73424 / BUG-75943
            time.sleep(90)
            Ambari.start_stop_service('HDFS', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['HDFS'])
            Ambari.start_stop_service('YARN', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['YARN'])
            Ambari.start_stop_service(
                'MAPREDUCE2', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['MAPREDUCE2']
            )
        else:
            stopOrderedServices = []
            startOrderedServices = []
            # This is when a test just wants to restart HDFS and dont want to start YARN and MAPREDUCE
            if 'HDFS' in services:
                Ambari.start_stop_service(
                    'HDFS', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['HDFS']
                )
                # workaround for BUG-73424 / BUG-75943
                time.sleep(90)
                Ambari.start_stop_service(
                    'HDFS', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['HDFS']
                )
                services.remove('HDFS')
            elif 'YARN' in services:
                Ambari.start_stop_service(
                    'YARN', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['YARN']
                )
                # workaround for BUG-73424 / BUG-75943
                time.sleep(10)
                Ambari.start_stop_service(
                    'YARN', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['YARN']
                )
                services.remove('YARN')
            elif 'MAPREDUCE2' in services:
                Ambari.start_stop_service(
                    'MAPREDUCE2', 'INSTALLED', waitForCompletion=True, timeout=cls._compSvcTimeouts['MAPREDUCE2']
                )
                # workaround for BUG-73424 / BUG-75943
                time.sleep(90)
                Ambari.start_stop_service(
                    'MAPREDUCE2', 'STARTED', waitForCompletion=True, timeout=cls._compSvcTimeouts['MAPREDUCE2']
                )
                services.remove('MAPREDUCE2')
            else:
                # Need to start the services in this order if started individually
                for service in ('datanodes', 'journalnode', 'zkfc', 'namenode', 'nfs3', 'tasktrackers',
                                'timelineserver', 'historyserver', 'jobtracker'):
                    if service in services:
                        stopOrderedServices.append(service)
                for service in ('datanodes', 'journalnode', 'zkfc', 'namenode', 'nfs3', 'jobtracker', 'timelineserver',
                                'historyserver', 'tasktrackers'):
                    if service in services:
                        startOrderedServices.append(service)
            for service in services:
                if service not in stopOrderedServices:
                    stopOrderedServices.append(service)
                    startOrderedServices.append(service)
            cls.stopServices(services=stopOrderedServices)
            if 'jobtracker' or 'timelineserver' or 'historyserver' or 'tasktrackers' in stopOrderedServices:
                pass
            else:
                # workaround for BUG-73424 / BUG-75943
                time.sleep(90)
            cls.startServices(services=startOrderedServices)
            if "jobtracker" in services or "YARN" in services or "all" in services:
                MAPRED.waitForNMToRegister()

    @classmethod
    @TaskReporter.report_test()
    def getServiceNamesFromDict(cls, selection):
        services = []
        if selection.has_key('services'):
            for service in selection['services']:
                if service == 'all':
                    services = ['all']
                    break
                elif service == 'secondarynamenode':
                    if not HDFS2.isHAEnabled():
                        services.append(service)
                else:
                    services.append(service)
        return services

    @classmethod
    @TaskReporter.report_test()
    def getServiceHosts(cls, service):
        hosts = []
        if service == 'namenode':
            hosts = cls.getNamenodeHosts()
        elif service == 'secondarynamenode':
            hosts = cls.getSecondaryNamenodeHost()
        elif service == 'datanodes':
            hosts = cls.getDatanodeHosts()
        elif service == 'nfs3':
            hosts = cls.getNfsGatewayHosts()
        elif service == 'journalnode':
            hosts = cls.getJournalnodeHosts()
        elif service == 'zkfc':
            hosts = cls.getZkfcHosts()
        elif service == 'tasktrackers':
            hosts = cls.getResourceManagerHosts()
        elif service == 'jobtracker':
            hosts = cls.getNodeManagerHosts()
        elif service == 'timelineserver':
            hosts = cls.getTimelineServerHosts()
        elif service == 'historyserver':
            hosts = cls.getHistoryServerHost()
        elif service == 'yarnclient':
            hosts = cls.get_yarn_client_host()
        elif service == "yarnregistry":
            hosts = Ambari.getHostsForComponent('YARN_REGISTRY_DNS')
        elif service == 'timelinereader':
            hosts = cls.get_yarn_timeline_reader()
        else:
            logger.error("Unrecognized service: %s", service)
        return hosts

    @classmethod
    @TaskReporter.report_test()
    def getNamenodeHosts(cls):
        if cls._namenodeHosts is None:
            cls._namenodeHosts = Ambari.getHostsForComponent(cls._compSvcMap['namenode'])
        return cls._namenodeHosts

    @classmethod
    @TaskReporter.report_test()
    def getSecondaryNamenodeHost(cls):
        if cls._secondaryNamenodeHost is None:
            cls._secondaryNamenodeHost = Ambari.getHostsForComponent(cls._compSvcMap['secondarynamenode'])
        return cls._secondaryNamenodeHost

    @classmethod
    @TaskReporter.report_test()
    def getDatanodeHosts(cls):
        if cls._datanodeHosts is None:
            cls._datanodeHosts = Ambari.getHostsForComponent(cls._compSvcMap['datanodes'])
        return cls._datanodeHosts

    @classmethod
    @TaskReporter.report_test()
    def getNfsGatewayHosts(cls):
        if cls._nfsGatewayHosts is None:
            cls._nfsGatewayHosts = Ambari.getHostsForComponent(cls._compSvcMap['nfs3'])
        return cls._nfsGatewayHosts

    @classmethod
    @TaskReporter.report_test()
    def getJournalnodeHosts(cls):
        if cls._journalnodeHosts is None:
            cls._journalnodeHosts = Ambari.getHostsForComponent(cls._compSvcMap['journalnode'])
        return cls._journalnodeHosts

    @classmethod
    @TaskReporter.report_test()
    def getZkfcHosts(cls):
        if cls._zkfcHosts is None:
            cls._zkfcHosts = Ambari.getHostsForComponent(cls._compSvcMap['zkfc'])
        return cls._zkfcHosts

    @classmethod
    @TaskReporter.report_test()
    def getNodeManagerHosts(cls):
        if cls._jobtrackerHosts is None:
            cls._jobtrackerHosts = Ambari.getHostsForComponent(cls._compSvcMap['jobtracker'])
        return cls._jobtrackerHosts

    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerHosts(cls):
        if cls._tasktrackersHosts is None:
            cls._tasktrackersHosts = Ambari.getHostsForComponent(cls._compSvcMap['tasktrackers'])
        return cls._tasktrackersHosts

    @classmethod
    @TaskReporter.report_test()
    def getTimelineServerHosts(cls):
        if cls._timelineserverHosts is None:
            cls._timelineserverHosts = Ambari.getHostsForComponent(cls._compSvcMap['timelineserver'])
        return cls._timelineserverHosts

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerHost(cls):
        if cls._historyserverHost is None:
            cls._historyserverHost = Ambari.getHostsForComponent(cls._compSvcMap['historyserver'])
        return cls._historyserverHost

    @classmethod
    @TaskReporter.report_test()
    def get_yarn_client_host(cls):
        if cls._yarnclientHost is None:
            cls._yarnclientHost = Ambari.getHostsForComponent(cls._compSvcMap['yarnclient'])
        return cls._yarnclientHost

    @classmethod
    @TaskReporter.report_test()
    def get_yarn_timeline_reader(cls):
        if cls._yarnTimelineReader is None:
            cls._yarnTimelineReader = Ambari.getHostsForComponent(cls._compSvcMap['timelinereader'])
        return cls._yarnTimelineReader

    @classmethod
    @TaskReporter.report_test()
    def regenServiceXmlsInWindows(cls, nodeSelection):
        '''
        Regenerate service xmls for Windows Hadoop2 services.
        Service keys have to be present.
        '''
        if Hadoop.isHadoop1() or Machine.isLinux():
            return
        if not 'services' in nodeSelection.keys():
            logger.info(
                "WARNING: regenServiceXmlsInWindows: config regeneration should be used with services argument."
            )
            return
        #if any node has both DN and NN, correctness is untested.
        doneHDFSNodes = []
        if 'services' in nodeSelection.keys():
            for service in nodeSelection['services']:
                if service in ['namenode']:
                    node = HDFS.getNamenode()
                    if node not in doneHDFSNodes:
                        #hdfs.cmd --service namenode > namenode.xml
                        HDFS.runas(
                            user=Machine.getAdminUser(),
                            cmd=" --service namenode > %s%snamenode.xml" % (cls.getHadoopBinHome(), os.sep),
                            env=None,
                            logoutput=True,
                            config=None,
                            host=node,
                            skipAuth=False
                        )
                        doneHDFSNodes.append(node)
                if service in ['datanodes']:
                    #hdfs.cmd --service datanode > datanode.xml
                    for node in HDFS.getDatanodes():
                        if node not in doneHDFSNodes:
                            HDFS.runas(
                                user=Machine.getAdminUser(),
                                cmd=" --service datanode > %s%sdatanode.xml" % (cls.getHadoopBinHome(), os.sep),
                                env=None,
                                logoutput=True,
                                config=None,
                                host=node,
                                skipAuth=False
                            )
                            doneHDFSNodes.append(node)
                if service == 'jobtracker':
                    #yarn.cmd --service $service  > $service.xml
                    YARN.runas(
                        user=Machine.getAdminUser(),
                        cmd=" --service resourcemanager > %s%sresourcemanager.xml" % (cls.getHadoopBinHome(), os.sep),
                        env=None,
                        logoutput=True,
                        host=YARN.getResourceManagerHost()
                    )

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls, changes, nodeSelection, restartService=False, useAmbari=False):
        if useAmbari:
            cls.restoreConfigWithAmbari(changes, nodeSelection, restartService)
        else:
            cls.restoreConfigWithoutAmbari(changes, nodeSelection)

    @classmethod
    @TaskReporter.report_test()
    def restoreConfigWithAmbari(cls, changes, nodeSelection, restartService=True):
        if [i for i in cls._hdfsPropertyFileList if i in changes]:
            resetVersion = cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceHDFSName)
            logger.info("Restoring Service Config Version for HDFS to : %s", resetVersion)
            Ambari.resetConfig(cls._ambariServiceHDFSName, resetVersion)
        if [i for i in cls._yarnPropertyFileList if i in changes]:
            resetVersion = cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceYARNName)
            logger.info("Restoring Service Config Version for YARN to : %s", resetVersion)
            Ambari.resetConfig(cls._ambariServiceYARNName, resetVersion)
        if [i for i in cls._mapredPropertyFileList if i in changes]:
            resetVersion = cls.getStartingAmbariServiceConfigVersion(cls._ambariServiceMAPRED2Name)
            logger.info("Restoring Service Config Version for MAPRED2 to : %s", resetVersion)
            Ambari.resetConfig(cls._ambariServiceMAPRED2Name, resetVersion)

        if restartService:
            services = cls.getServiceNamesFromDict(nodeSelection)
            cls.restartServices(services)

    @classmethod
    @TaskReporter.report_test()
    def restoreConfigWithoutAmbari(cls, changes, nodeSelection):
        '''
        Restore config.
        changes is a list.
        '''
        hadoop_conf = Config.get('hadoop', 'HADOOP_CONF')
        nodes = cls.getSelectedNodes(nodeSelection)
        if Machine.isLinux():
            configUtils.restoreConfig(changes, hadoop_conf, nodes)
        else:
            if Machine.pathExists(user=None, host=None, filepath=configUtils.getBackupConfigLocation(), passwd=None):
                configUtils.restoreConfig(changes, hadoop_conf, nodes)
                #If Windows and hadoop-env.cmd is changed and services is in changes, regen service xmls.
                if changes is not None and \
                   isinstance(changes, list) and cls.getHadoopEnvSh() in changes:
                    if 'services' in nodeSelection.keys():
                        cls.regenServiceXmlsInWindows(nodeSelection)

    @classmethod
    def getmodifiedConfigValue(cls, filename, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(cls.getModifiedConfigPath(), filename), propertyValue, defaultValue=defaultValue
        )

    @classmethod
    @TaskReporter.report_test()
    def deleteConfig(cls, changes, nodeSelection, isFirstUpdate=True):
        nodes = cls.getSelectedNodes(nodeSelection)
        hadoop_conf = Config.get('hadoop', 'HADOOP_CONF')
        tmp_conf = os.path.join(Machine.getTempDir(), 'hadoopConf')
        configUtils.deleteConfig(changes, hadoop_conf, tmp_conf, nodes, isFirstUpdate)

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        # only get the version if its not already determined
        if not cls._version:
            exit_code, output = cls.run("version")
            if exit_code == 0:
                # perform a multi line check
                pattern = re.compile(r"^Hadoop (\S+)", re.M)
                m = pattern.search(output)
                if m:
                    cls._version = m.group(1)
                else:
                    cls._version = ""
            else:
                cls._version = ""

        return cls._version

    @classmethod
    @TaskReporter.report_test()
    def getShortVersion(cls):
        '''
        Return short version.
        If version = 2.7.1.2.3.0.0-1675,
        :return: 2.3.0.0-1675
        '''
        version = cls.getVersion()
        return version.split(".", 3)[-1]

    # method to determine if we are running hadoop 2 or 1
    @classmethod
    @TaskReporter.report_test()
    def isHadoop2(cls):
        if not cls._isHadoop2:
            output = cls.getVersion()
            # if command fails assume version 1
            if not output:
                cls._isHadoop2 = False

            version = output.split('.')
            # if first version number is 2 or greater we are
            # on hadoop 2 else we are on hadoop 1
            return int(version[0]) >= 2
        else:
            return None


    # Returns True if the run is Hadoop1
    @classmethod
    def isHadoop1(cls):
        return not cls.isHadoop2()

    @classmethod
    @TaskReporter.report_test()
    def isHadoop1_2(cls):
        Valid_Version = False
        version = cls.getVersion()
        logger.info(version)
        output = version.split('.')

        if int(output[0]) == 1:
            temp = int(output[1])
            if temp >= 2:
                Valid_Version = True
        return Valid_Version

    #Checks if Hadoop version is (hadoop2) or (hadoop1 Comanche or beyond)
    @classmethod
    def isComancheOrBeyond(cls):
        #Comanche is "1.2.0.1.3.1.0-38"
        "Condor is 1.2.0.1.3.0.0-<build>"
        return cls.getVersion() >= "1.2.0.1.3.1"

    @classmethod
    @TaskReporter.report_test()
    def isDalorBeyond(cls):
        v = cls.getShortVersion()
        expected_v = "2.3.0.0-0001"
        return v > expected_v


    #Added by: Logigear
    #Added date: 24-Aug-2012
    #This function use to check a message exists in Hadoop logfile.
    #Parameters:
    #logFile - File name to check
    #msg - Message to check
    #startTime : Time to distinguish that message is written to log file
    @classmethod
    @TaskReporter.report_test()
    def checkHadoopLogForMessage(cls, logFile, msg, startTime):
        if not os.path.isfile(logFile):
            logger.info("File not found: '%s'", logFile)
            return False
        f = open(logFile)
        lines = f.readlines()
        f.close()
        count = 0
        for line in lines:
            if re.search(msg.lower(), line.lower()):
                lineDateCounter = 0
                dateList = []
                #if do not see date on line, loop until see it
                while not dateList and count - lineDateCounter >= 0:
                    line = lines[count - lineDateCounter]
                    dateList = re.findall(r"\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}", line)
                    lineDateCounter += 1
                dformat = "%Y-%m-%d %H:%M:%S"
                for date in dateList:
                    if startTime <= time.mktime(time.strptime(date, dformat)):
                        logger.info(
                            "checkHadoopLogForMessage returns True."
                            " startTime <= time.mktime(time.strptime(date, dformat))"
                        )
                        logger.info("startTime=%s", startTime)
                        logger.info("strptime=%s", time.strptime(date, dformat))
                        return True
            count += 1
        return False

    @classmethod
    @TaskReporter.report_test()
    def verifyJTRestartMessage(cls, message):
        # get the job tracker logs
        localJTLogFile = MAPRED.copyJTLogToLocalMachine()
        index = 0
        logger.info("opening log file")
        f = open(localJTLogFile)
        for i, line in enumerate(f):
            if line.find("STARTUP_MSG: Starting JobTracker") >= 0:
                index = i
        f.close()
        f = open(localJTLogFile)
        for i, line in enumerate(f):
            if i > index:
                if line.find(message) >= 0:
                    logger.info("Message:%s found", message)
                    f.close()
                    return True
        f.close()
        return False

    @classmethod
    @TaskReporter.report_test()
    def getSelectedNodes(cls, selection):
        selnodes = []
        if selection.has_key('nodes'):
            selnodes.extend(selection['nodes'])
        if selection.has_key('services'):
            if 'all' in selection['services']:
                if HDFS2.isHAEnabled():
                    return cls.getSelectedNodes(
                        {
                            'services': [
                                'ha-namenode', 'datanodes', 'ha-rm', 'tasktrackers', 'historyserver', 'gateway',
                                'timelineserver'
                            ]
                        }
                    )
                else:
                    return cls.getSelectedNodes(
                        {
                            'services': [
                                'namenode', 'datanodes', 'secondarynamenode', 'jobtracker', 'tasktrackers',
                                'historyserver', 'gateway', 'timelineserver'
                            ]
                        }
                    )
            elif HDFS2.isHAEnabled():
                if 'namenode' in selection['services']:
                    selection['services'].remove('namenode')
                    selection['services'].append('ha-namenode')
                if 'secondarynamenode' in selection['services']:
                    selection['services'].remove('secondarynamenode')
            for service in selection['services']:
                if service == 'namenode':
                    selnodes.append(HDFS.getNamenode())
                elif service == 'datanodes':
                    selnodes.extend(HDFS.getDatanodes())
                elif service == 'secondarynamenode':
                    selnodes.append(HDFS.getSecondaryNamenode())
                elif service == 'jobtracker':
                    if YARN.isHAEnabled():
                        nodes = YARN.getRMHANodes()
                        for rmNode in nodes:
                            selnodes.append(rmNode)
                    else:
                        selnodes.append(MAPRED.getJobtracker())
                elif service == 'tasktrackers':
                    selnodes.extend(MAPRED.getTasktrackers())
                elif service == 'historyserver':
                    selnodes.append(MAPRED.getHistoryserver())
                elif service == 'gateway':
                    selnodes.append(HDFS.getGateway())
                elif service == 'timelineserver':
                    selnodes.append(YARN.getATSHost())
                elif service == 'ha-namenode':
                    # add active namenode
                    selnodes.append(HDFS.getNamenodeByState('active'))
                    # add the standby namenode
                    selnodes.append(HDFS.getNamenodeByState('standby'))
                elif service == 'ha-rm':
                    nodes = YARN.getRMHANodes()
                    for rmNode in nodes:
                        selnodes.append(rmNode)

        return list(set(selnodes))

    @classmethod
    @TaskReporter.report_test()
    def get_counters(cls, stdout):
        """
        Given a stdout of an MR run, all the vital counters are returned in a dictionary
        :rtype : dict
        :param stdout: output of the MR run
        :return: dictionary of the MR counters
        """
        run_counters = dict()
        counter_pattern = re.compile(r'\s*.*=\d+\s*')
        all_counters = counter_pattern.findall(stdout)
        for counter in all_counters:
            counter = counter.strip()
            name, val = counter.split('=')
            run_counters[name] = int(val)
        return run_counters

    @classmethod
    @TaskReporter.report_test()
    def getMinUserId(cls):
        """
        min.user.id is returned in int
        :rtype : int
        :return: min.user.id of container-executor.cfg
        """
        container_exe_cfg = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), 'container-executor.cfg')
        min_user_id = -1
        if os.path.isfile(container_exe_cfg):
            min_user_id = int(re.findall(r'\d+', util.getPropertyValueFromFile(container_exe_cfg, 'min.user.id'))[0])
        return min_user_id

    @classmethod
    @TaskReporter.report_test()
    def getLzoJar(cls):
        """
        Gets the location of Hadoop LZO jar.
        """
        lzoJar = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=cls.getHadoopHome(),
            searchstr='hadoop-lzo-*.jar',
            passwd=Machine.getAdminPasswd()
        )
        if lzoJar:
            return lzoJar[0]
        else:
            return None


class BaseHDFS(object):
    _nnHost = None
    _nnPort = "50070"
    _dnHosts = []
    _snnHost = None
    _snnPort = None
    _dnIpcPort = None
    _gwHost = "localhost"
    _jmxPortNamenode = 8002
    _jmxPortDatanode = 8004
    _dfs_http_address_prop = 'dfs.http.address'
    _dfs_https_address_prop = 'dfs.https.address'

    @classmethod
    def getHDFSUser(cls):
        '''
        Returns HDFS user (hdfs).
        '''
        return Config.get('hadoop', 'HDFS_USER', 'hdfs')

    # this method in hadoop 1 just returns the key
    @classmethod
    def getConfigKey(cls, key):
        return key

    # method to kill the namenode
    @classmethod
    def killNameNode(cls):
        Hadoop.killService('namenode', Config.get('hadoop', 'HDFS_USER'), cls.getNamenode())

    # TODO: sending host in secure mode wont work as we need to refactor how
    # we get kerberos credentials on a specific host
    @classmethod
    def runas(
            cls,
            user,
            cmd,
            env=None,
            logoutput=True,
            config=None,
            host=None,
            skipAuth=False,
            stderr_as_stdout=True,
            doEscapeQuote=True
    ):
        """
        BaseHDFS.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :param config:
        :param host:
        :param skipAuth:
        :param stderr_as_stdout:
        :param doEscapeQuote:
        :return:
        """
        return Hadoop.runas(
            user,
            cmd,
            env=env,
            logoutput=logoutput,
            config=config,
            host=host,
            skipAuth=skipAuth,
            stderr_as_stdout=stderr_as_stdout,
            doEscapeQuote=doEscapeQuote
        )

    # Runs HDFS CLI given command in background as specified user.
    # Returns subprocess.Popen object.
    @classmethod
    def runInBackgroundAs(cls, user, cmd, env=None, stdout=None, stderr=None):
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        hdfs_cmd = Config.get('hadoop', 'HDFS_CMD') + " " + cmd
        return Machine.runinbackgroundAs(user, hdfs_cmd, env=env, stdout=stdout, stderr=stderr)

    # Runs HDFS CLI with given command in background as current user.
    # Returns subprocess.Popen object.
    @classmethod
    def runInBackground(cls, cmd, env=None, stdout=None, stderr=None):
        return cls.runInBackgroundAs(None, cmd, env=env, stdout=stdout, stderr=stderr)

    @classmethod
    def run(cls, cmd, env=None, logoutput=True, config=None, skipAuth=False):
        return cls.runas(None, cmd, env=env, logoutput=logoutput, config=config, skipAuth=skipAuth)

    @classmethod
    def runasAdmin(cls, cmd, env=None, logoutput=True, config=None, host=None, stderr_as_stdout=True):
        return cls.runas(
            Config.get('hadoop', 'HDFS_USER'),
            cmd,
            env=env,
            logoutput=logoutput,
            config=config,
            host=host,
            stderr_as_stdout=stderr_as_stdout
        )

    @classmethod
    @TaskReporter.report_test()
    def fileExists(cls, filepath, user=None, config=None, logoutput=False, options=""):
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        exit_code, stdout = cls.runas(user, "%s dfs %s -ls %s" % (configStr, options, filepath), logoutput=logoutput)
        # check both the exit code and stdout to determine if the path exists on HDFS or
        # not
        if exit_code != 0:
            logger.error(stdout)
        return exit_code == 0 and "No such file or directory" not in stdout

    @classmethod
    @TaskReporter.report_test()
    def getSumFileSize(cls, filepath, user=None, recursive=False, logoutput=True):
        if not cls.fileExists(filepath, user):
            return None
        if recursive:
            lsCmd = "-lsr"
        else:
            lsCmd = "-ls"
        _, stdout = cls.runas(user, "dfs %s " % lsCmd + filepath, logoutput=logoutput)
        sumvalue = 0
        found = False
        for line in stdout.split('\n'):
            if re.search("Found.*items", line) is None and re.search(
                    "azurefs2 has a full queue and can.*t consume the given metrics", line) is None:
                if (line.split()[4]).isdigit():
                    sumvalue += int(line.split()[4])
                    found = True
        if not found:
            return None
        else:
            return sumvalue

    @classmethod
    @TaskReporter.report_test()
    def lsrCmd(cls):
        return "lsr"

    @classmethod
    @TaskReporter.report_test()
    def lsr(cls, path, recursive=True, logoutput=True, config=None, options='', user=None, stderr_as_stdout=True):
        '''
        Runs HDFS ls or lsr.
        Returns (exit_code, stdout)
        '''
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        if recursive:
            return HDFS.runas(user, "%s dfs %s -lsr %s" % (configStr, options, path), logoutput=logoutput,
                              stderr_as_stdout=stderr_as_stdout)
        else:
            return HDFS.runas(user, "%s dfs %s -ls %s" % (configStr, options, path), logoutput=logoutput,
                              stderr_as_stdout=stderr_as_stdout)

    @classmethod
    @TaskReporter.report_test()
    def lsrWithListOutput(cls, path, recursive=True, logoutput=True, config=None, user=None):
        '''
        Runs HDFS ls or lsr.
        Returns a list of 8-entry list. Each entry is a line in output.
        Returns None if exit code is non-zero.
        Each element is str.
        #e.g. 8-entry list is ['-rw-r--r--', '3', 'hrt_qa', 'hrt_qa', '220444061', '2014-02-26',
        '00:46', '/user/hrt_qa/testCacheExistingFilesTrue/part-m-00001']
        '''
        (exit_code, stdout) = cls.lsr(path, recursive, logoutput, config=config, user=user)
        if exit_code != 0:
            return None
        result = []
        for line in stdout.split('\n'):
            line = line.rstrip('\r')
            if len(line.split()) == 8 and line.lower().find("metric") == -1:
                result.append(line.split())
        if logoutput:
            logger.info("lsrWithListOutput returns")
            for entry in result:
                logger.info(entry)
        return result

    @classmethod
    def rmrCmd(cls):
        return "rmr"

    @classmethod
    @TaskReporter.report_test()
    def createDirectory(cls, directory, user=None, perm=None, force=False):
        out = [0, '']
        bFileExists = False
        if cls.fileExists(directory, user):
            bFileExists = True
            if force:
                exitCode, _ = cls.deleteDirectory(directory, user=user)
                if exitCode == 0:
                    bFileExists = False
        if not bFileExists:
            exitCode, _ = cls.runas(user, "dfs -mkdir " + directory)
            if exitCode == 0:
                out[1] = "Created directory \"%s\"" % directory
        if perm:
            cls.runas(user, "dfs -chmod %s %s" % (perm, directory))
        return out

    @classmethod
    @TaskReporter.report_test()
    def chmod(cls, runasUser, perm, directory, recursive=False, config=None):
        '''
        Runs chmod <perm> <directory> as <runasUser>.
        Returns (exit_code, stdout).
        '''
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        if recursive:
            return cls.runas(runasUser, "%s dfs -chmod -R %s %s" % (configStr, perm, directory))
        return cls.runas(runasUser, "%s dfs -chmod %s %s" % (configStr, perm, directory))

    @classmethod
    @TaskReporter.report_test()
    def chgrp(cls, runasUser, group, directory, recursive=False, config=None):
        '''
        Runs chgrp [-R] <group> <directory>
        Returns (exit_code, stdout).
        '''
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        if recursive:
            return cls.runas(runasUser, "%s dfs -chgrp -R %s %s" % (configStr, group, directory))
        return cls.runas(runasUser, "%s dfs -chgrp %s %s" % (configStr, group, directory))

    @classmethod
    @TaskReporter.report_test()
    def chown(cls, runasUser, new_owner, directory, recursive=False, config=None):
        '''
        Runs -chown [-R] [OWNER][:[GROUP]] <directory>
        Returns (exit_code, stdout).
        '''
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        if recursive:
            return cls.runas(runasUser, "%s dfs -chown -R %s %s" % (configStr, new_owner, directory))
        return cls.runas(runasUser, "%s dfs -chown %s %s" % (configStr, new_owner, directory))

    @classmethod
    def mv(cls, runasUser, src, destination, config=None):
        '''
        Runs dfs -mv <src> <destination>
        '''
        cmd = "dfs -mv %s %s" % (src, destination)
        return cls.runas(runasUser, cmd, None, True, config, None, False)

    @classmethod
    @TaskReporter.report_test()
    def put(cls, runasUser, localscr, destination, options=None):
        '''
        Runs dfs -put <localsrc> <destination>
        '''
        if not options:
            return cls.runas(runasUser, "dfs -put %s %s" % (localscr, destination), None, True, None, None, False)
        else:
            return cls.runas(
                runasUser, "dfs %s -put %s %s" % (options, localscr, destination), None, True, None, None, False
            )

    @classmethod
    @TaskReporter.report_test()
    def deleteDirectory(cls, directory, user=None, skipTrash=True, trashProp='', config=None, options=""):
        if config is None:
            configStr = ""
        else:
            configStr = "--config %s " % config
        out = [0, '']
        delcmd = "%s dfs %s %s -%s " % (configStr, options, trashProp, cls.rmrCmd())
        if skipTrash:
            delcmd += "-skipTrash "
        delcmd += directory
        if cls.fileExists(directory, user, config=config, options=options):
            out = cls.runas(user, delcmd)
        return out

    @classmethod
    @TaskReporter.report_test()
    def createFile(cls, filename, user=None, force=False):
        out = [0, '']
        bFileExists = False
        if cls.fileExists(filename, user):
            bFileExists = True
            if force:
                exitCode, _ = cls.deleteFile(filename, user=user)
                if exitCode == 0:
                    bFileExists = False
        if not bFileExists:
            exitCode, _ = cls.runas(user, "dfs -touchz " + filename)
            if exitCode == 0:
                out[1] = "Created file \"%s\"" % filename
        return out

    @classmethod
    @TaskReporter.report_test()
    def deleteFile(cls, filePath, user=None, skipTrash=True, trashProp=''):
        '''
        Deletes HDFS file.
        Returns (exit_code, stdout) if file does exist before deleting.
        '''
        out = [0, '']
        delcmd = "dfs %s -rm " % trashProp
        if skipTrash:
            delcmd += "-skipTrash "
        delcmd += filePath
        if cls.fileExists(filePath, user):
            out = cls.runas(user, delcmd)
        return out

    @classmethod
    @TaskReporter.report_test()
    def createDirectoryAsUser(cls, directory, adminUser=None, user=None, perm="711", force=False):
        out = cls.createDirectory(directory, user=adminUser, perm=perm, force=force)
        if out[1] != "" and user:
            #  PROD-1410 - For Secure ADLS run 'chown' with full user names
            if HDFS.isCabo() and Hadoop.isSecure():
                AAD_DOMAIN = Config.get('machine', 'AAD_DOMAIN')
                out = cls.runas(
                    adminUser, "dfs -chown -R %s@%s:%s@%s %s" % (user, AAD_DOMAIN, user, AAD_DOMAIN, directory)
                )
            else:
                out = cls.runas(adminUser, "dfs -chown -R %s:%s %s" % (user, user, directory))

        return out

    @classmethod
    @TaskReporter.report_test()
    def createDirectoryWithOutput(cls, directory, user=None, perm=None, force=False):
        """
        Creates a directory and if it fails, the returned stdout will have the message.
        :param directory: beaver/component
        :param user: Hadoop user to be run as. Default/logged-in user is default
        :param perm: Permission
        :param force: Delete and recreate if the directory already exists
        :return: exitCode and stdoutput both on success or failure
        """
        out = [0, '']
        bFileExists = False
        if cls.fileExists(directory, user):
            bFileExists = True
            if force:
                exitCode, stdout = cls.deleteDirectory(directory, user=user)
                if exitCode == 0:
                    bFileExists = False
        if not bFileExists:
            exitCode, stdout = cls.runas(user, "dfs -mkdir " + directory)
            if exitCode == 0:
                out[1] = "Created directory \"%s\"" % directory
                if perm:
                    cls.runas(user, "dfs -chmod %s %s" % (perm, directory))
            else:
                out[0] = exitCode
                out[1] = stdout
        return out

    @classmethod
    @TaskReporter.report_test()
    def createUserDirWithGroup(
            cls, directory, adminUser=None, user=None, group=None, perm="711", force=False, diffGroup=False
    ):
        out = cls.createDirectory(directory, user=adminUser, perm=perm, force=force)
        if out[1] != "" and diffGroup:
            if group:
                out = cls.runas(adminUser, "dfs -chown -R %s:%s %s" % (user, group, directory))
        return out

    @classmethod
    def mkdir(cls, path, user=None, config=None, logoutput=True, options=""):
        '''
        Make a directory in HDFS.
        Returns (exit_code, stdout).
        '''
        return cls.runas(user, "dfs %s -mkdir %s" % (options, path), config=config, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def copyFromLocal(
            cls,
            localpath,
            hdfspath,
            user=None,
            config=None,
            optionAndParameter="",
            enableDebug=False,
            debugClientfile=None
    ):
        '''
        Copies files/directories from local path to HDFS.
        Returns (exit_code, stdout)
        Target directory must exist.
        '''
        if enableDebug:
            if not debugClientfile:
                debugClientfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "debug_copyfromlocal.txt")
            return cls.runas(
                user, " --loglevel DEBUG dfs %s -copyFromLocal %s %s 2>&1 | tee %s " %
                (optionAndParameter, localpath, hdfspath, debugClientfile), None, True, config, None
            )
        else:
            return cls.runas(
                user, "dfs %s -copyFromLocal %s %s" % (optionAndParameter, localpath, hdfspath), None, True, config,
                None
            )

    @classmethod
    def copyToLocal(cls, hdfspath, localpath, user=None):
        '''
        Copies files/directories from HDFS to local path.
        Returns (exit_code, stdout)
        '''
        return cls.runas(user, "dfs -copyToLocal %s %s" % (hdfspath, localpath))

    @classmethod
    @TaskReporter.report_test()
    def cat(cls, hdfspath, user=None, logoutput=False, config=None, stderr_as_stdout=True):
        return cls.runas(
            user, "dfs -cat %s" % hdfspath, logoutput=logoutput, config=config, stderr_as_stdout=stderr_as_stdout
        )

    @classmethod
    def appendToHdfs(cls, localappendfile, hdfsfile, user=None, config=None, logoutput=False):
        cmd = "dfs -appendToFile %s %s" % (localappendfile, hdfsfile)
        return cls.runas(user, cmd, None, logoutput, config, None, False)

    @classmethod
    def text(cls, hdfspath, user=None):
        return cls.runas(user, "dfs -text %s" % hdfspath, logoutput=False)

    @classmethod
    @TaskReporter.report_test()
    def getDatanodeCount(cls):
        '''
        Get total datanode count including Livenodes and Deadnnodes
        '''
        platform = Config.get('machine', 'PLATFORM')
        if platform == 'ASV':
            livenodes = cls.getDatanodesFromJmx()
            deadnodes = cls.getDatanodesFromJmx(Type="DeadNodes")
            allnodes = list(set(livenodes + deadnodes))
            return len(allnodes)
        else:
            return cls.getLiveDeadNodeCount()[0]

    @classmethod
    @TaskReporter.report_test()
    def getLiveDeadNodeCount(cls):
        exit_code, output = cls.runAdminReport()
        live = 0
        dead = 0
        if exit_code == 0:
            liveNodes = re.search(r'Live datanodes \((\d+)\):', output)
            if liveNodes:
                live = int(liveNodes.group(1))
            deadNodes = re.search(r'Dead datanodes \((\d+)\):', output)
            if deadNodes:
                dead = int(deadNodes.group(1))
            return live, dead
        else:
            return live, dead

    @classmethod
    @TaskReporter.report_test()
    def getLiveNodes(cls):
        '''
        Get live nodes.
        Returns a list of hosts.
        In Linux, hostnames are returned.
        '''
        hostlist = []
        exit_code, output = cls.runAdminReport()
        if exit_code == 0:
            hostname = ""
            Getname = 1
            for line in output.split("\n"):
                line = line.strip()
                # There are two sections in the Admin report
                # parse section must be in alive (available state)

                if line.find("Name: ") == 0:
                    hostname = Machine.getfqdn(line[6:].split(":")[0])
                    Getname = 0

                if line.find("Configured Capacity") >= 0 and Getname == 0:
                    if line.find("(0 KB)") >= 0:
                        logger.info("Deadnode")
                    else:
                        logger.info("Livenode")
                        hostlist.append(hostname)
                    Getname = 1
                    hostname = ""
        return hostlist

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hdfs-site.xml"),
            propertyValue,
            defaultValue=defaultValue
        )

    @classmethod
    @TaskReporter.report_test()
    def isFederated(cls):
        if HDFS2.isHAEnabled():
            param_value = cls.getConfigValue("dfs.nameservices")
            if len(param_value.split(",")) >= 2:
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getNameServices(cls):
        if HDFS2.isHAEnabled():
            param_value = cls.getConfigValue("dfs.nameservices")
            namespaces = param_value.split(",")
            return namespaces
        else:
            logger.error("Not a HA enabled cluster. Returning None")
            return None

    @classmethod
    @TaskReporter.report_test()
    def getFirstNameService(cls):
        if HDFS2.isHAEnabled():
            nameservices = cls.getConfigValue("dfs.nameservices")
            nameservicesList = nameservices.split(",")
            return nameservicesList[0]
        else:
            logger.error("Not a HA enabled cluster. Returning None")
            return None

    @classmethod
    @TaskReporter.report_test()
    def getNamenodeHttpAddress(cls, nameservice2=False):
        if HDFS.isHAEnabled():
            nnHttpAddress = None
            if nameservice2:
                if not cls.isFederated():
                    logger.error(
                        "Cluster is not Federated. Incorrect parameter sent to get NamenodeHttpAddress"
                        " for nameservice2"
                    )
                    return None
                nameservice = cls.getNameServices()[1]
            else:
                nameservice = cls.getFirstNameService()
            serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameservice)
            serviceIds = serviceIds.split(',')
            for serviceId in serviceIds:
                serviceId = serviceId.strip()
                # get the service state
                _, output = cls.runasAdmin('haadmin -ns %s -getServiceState %s' % (nameservice, serviceId))
                if output.lower() == 'active':
                    nnHttpAddress = cls.getConfigValue('dfs.namenode.http-address.%s.%s' % (nameservice, serviceId))
                    cls._nnHost, cls._nnPort = nnHttpAddress.split(":")
                    return nnHttpAddress
        if cls._nnHost is None:
            # check both hadoop 1 and hadoop2 config values
            nnHttpAddress = cls.getConfigValue("dfs.http.address", cls.getConfigValue('dfs.namenode.http-address'))
            if nnHttpAddress:
                cls._nnHost, cls._nnPort = nnHttpAddress.split(":")
                return nnHttpAddress
            else:
                cls._nnHost = Hadoop.getFSDefaultValue(withPort=False)
        return cls._nnHost + ":" + cls._nnPort

    @classmethod
    @TaskReporter.report_test()
    def getNamenodeHttpsAddress(cls, nameservice2=False):
        nnHttpAddress = None
        if HDFS.isHAEnabled():
            if nameservice2:
                if not cls.isFederated():
                    logger.error(
                        "Cluster is not Federated. Incorrect parameter sent to get NamenodeHttpAddress"
                        " for nameservice2"
                    )
                    nameservice = None
                else:
                    nameservice = cls.getNameServices()[1]
            else:
                nameservice = cls.getFirstNameService()
            # if federation is not enabled and nameservice2=True, return None
            if not nameservice:
                return nameservice


            serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameservice)
            serviceIds = serviceIds.split(',')
            for serviceId in serviceIds:
                serviceId = serviceId.strip()
                # get the service state
                _, output = cls.runasAdmin('haadmin -ns %s -getServiceState %s' % (nameservice, serviceId))
                if output.lower() == 'active':
                    nnHttpAddress = cls.getConfigValue('dfs.namenode.https-address.%s.%s' % (nameservice, serviceId))
                    return nnHttpAddress
            logger.error("no Active NN was found")
            return None
        else:
            nnHttpsAddress = cls.getConfigValue("dfs.namenode.https-address", "0.0.0.0:50470")
            return nnHttpsAddress

    @classmethod
    @TaskReporter.report_test()
    def getDataNodeIPCPort(cls):
        if cls._dnIpcPort is None:
            ipcAddress = cls.getConfigValue("dfs.datanode.ipc.address", "0.0.0.0:8010")
            cls._dnIpcPort = ipcAddress.split(":")[1]

        return cls._dnIpcPort

    @classmethod
    @TaskReporter.report_test()
    def getDatanodeHttpAddress(cls):
        dnHttpAddress = cls.getConfigValue("dfs.datanode.address")
        if dnHttpAddress:
            dnHttp = dnHttpAddress.split(":")[1]
            return dnHttp
        else:
            logger.error("dfs.datanode.address property not found")
            return None

    @classmethod
    @TaskReporter.report_test()
    def getNamenode(cls):
        nnHttpAddress = cls.getNamenodeHttpAddress()
        cls._nnHost, cls._nnPort = nnHttpAddress.split(":")
        return cls._nnHost

    @classmethod
    def getNamenodeHttpPort(cls):
        if cls._nnPort is None:
            cls.getNamenodeHttpAddress()
        return cls._nnPort

    @classmethod
    def getNamenodeHttpsPort(cls):
        return cls.getNamenodeHttpsAddress().split(":")[1]

    @classmethod
    @TaskReporter.report_test()
    def getGateway(cls):
        if cls._gwHost == "localhost":
            if Machine.isWindows():
                #always use short hostname in Windows
                return Machine.getfqdn(name="")
            else:
                cls._gwHost = socket.gethostbyname(socket.gethostname())
        return cls._gwHost

    # Get the status of a node
    # (e.g.) Decommission Status : Decommissioned
    # param:  targetHost - name of Host to look for
    # return:  empty string or status if found
    @classmethod
    @TaskReporter.report_test()
    def getNodeStatus(cls, targetHost):
        targetHostSection = 0
        exit_code, output = cls.runAdminReport()
        if exit_code != 0:
            logger.info("Failed to get node status")
            return None
        for line in output.split("\n"):
            line = line.strip()

            if line.find("Name: ") == 0:
                hostname = Machine.getfqdn(line[6:].split(":")[0])
                if hostname == targetHost:
                    targetHostSection = 1
            if line.find("Decommission Status :") == 0 and targetHostSection == 1:
                nodeStatus = line.split(" : ")[1]
                return nodeStatus
        return None

    @classmethod
    @TaskReporter.report_test()
    def getDatanodes(cls, reRunAdminReport=False, config=None, logoutput=False):
        '''
        Gets list of datanode hosts.
        Returns a list of Strings.
        In Linux, returns a list of IPs.
        '''
        if not cls.isHDFSDefaultFS():
            livenodes = cls.getDatanodesFromJmx(logoutput=logoutput)
            deadnodes = cls.getDatanodesFromJmx(Type="DeadNodes", logoutput=logoutput)
            cls._dnHosts = list(set(livenodes + deadnodes))
            return cls._dnHosts
        else:
            if not cls._dnHosts or reRunAdminReport:
                if HDFS2.isHAEnabled():
                    # Only need to run on the first Namespace as Datanodes are shared across namespaces
                    name_service = cls.getFirstNameService()
                    _, admin_report = cls.runAdminReport(config=config, options='-fs hdfs://' + name_service)
                    stdout = admin_report
                else:
                    _, stdout = cls.runAdminReport(config=config)

                if Machine.isLinux():
                    cls._dnHosts.extend(re.findall(r"Name: (\S+):\d+", stdout))
                else:
                    #Windows dfs admin report output sample:
                    #Name: 10.215.20.34:50010 (10.215.20.34)
                    #Hostname: qeempress23.qeempress2.s3.internal.cloudapp.net
                    tmpDnHosts = []
                    if Hadoop.isHadoop2():
                        tmpRegexResult = re.findall(r"Hostname: (\S+)", stdout)
                    else:
                        tmpRegexResult = re.findall(r"Name: (\S+)", stdout)
                    #use short host name
                    for entry in tmpRegexResult:
                        tmpDnHosts.append(Machine.getfqdn(entry))
                    cls._dnHosts.extend(tmpDnHosts)
                if reRunAdminReport:
                    cls._dnHosts = list(set(cls._dnHosts))
            return cls._dnHosts

    @classmethod
    @TaskReporter.report_test()
    def getDatanodeInfo(cls, datanode_host, datanode_ipc_port, option=""):
        """
        Runs dfsadmin -getDatanodeInfo <datanode_host:ipc_port>
        """
        if option == "":
            cmd = "dfsadmin -getDatanodeInfo %s:%s" % (datanode_host, datanode_ipc_port)
        else:
            cmd = "dfsadmin -D%s -getDatanodeInfo %s:%s" % (option, datanode_host, datanode_ipc_port)
        return cls.runasAdmin(cmd)

    @classmethod
    @TaskReporter.report_test()
    def waitForDNDown(cls, datanode_host, datanode_ipc_port, option="", retry=10):
        """
        check if datanode is down after running -shutdownDatanode command
        If datanode is shutdown, return True , otherwise False
        """
        i = 0
        while i < retry:
            exit_code, _ = cls.getDatanodeInfo(datanode_host, datanode_ipc_port, option)
            if exit_code == 0:
                i = i + 1
            else:
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getDatanodesFromJmx(cls, outputfile="", Type="LiveNodes", logoutput=False):
        """
        Gets list of datanodes from http://<NN>:<NN_PORT>/jmx
        Return list of Live Datanodes
        """
        if outputfile == "":
            outputfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "tmpjmxsource")
        if cls.isEncrypted():
            url = "https://" + cls.getNamenodeHttpsAddress() + "/jmx"
        else:
            url = "http://" + cls.getNamenodeHttpAddress() + "/jmx"
        util.getURLContents(url, outputfile)
        text = open(outputfile).read()
        if text == "":
            logger.info("getDatanodesFromJmx text is null. Returning []")
            return []
        if logoutput:
            logger.info("getDatanodesFromJmx text=%s", text)
        d = json.loads(text)
        l = len(d["beans"])
        foundNum = False
        for i in range(0, l):
            txt = d["beans"][i]
            if str(txt).find(Type) >= 0:
                num = i
                foundNum = True
                break
        if not foundNum:
            if logoutput:
                logger.info("getDatanodesFromJmx returns []")
            return []
        allnodes = d["beans"][num][Type]
        x = json.loads(allnodes)
        # Livenodes returns <DN>:<port>.  Removing :<port> from keys
        tmp_datanodes = x.keys()
        for index, dn in enumerate(tmp_datanodes):
            tmp_datanodes[index] = dn.split(":")[0]

        cls._dnHosts = tmp_datanodes

        if logoutput:
            logger.info("getDatanodesFromJmx returns %s", tmp_datanodes)
        return tmp_datanodes

    @classmethod
    @TaskReporter.report_test()
    def getDecommissionedDatanodes(cls, config=None, logoutput=True):
        '''
        Gets list of decommmissioned datanode hosts.
        Returns a list of Strings.
        In Linux, returns a list of IPs.
        '''
        hosts = []
        exit_code, stdout = cls.runAdminReport(config=config, logoutput=logoutput)
        if exit_code != 0:
            if logoutput:
                logger.info("getDecommissionedDatanodes returns None")
            return None
        if Machine.isLinux():
            nodeStatusList = re.findall(
                "Name: (.*?):.*?Decommission Status : (.*?)Configured Capacity", stdout, re.DOTALL
            )
        else:
            nodeStatusList = re.findall(
                "Hostname: (.*?):.*?Decommission Status : (.*?)Configured Capacity", stdout, re.DOTALL
            )
        if logoutput:
            logger.info("getDecommissionedDatanodes nodeStatusList = %s", nodeStatusList)
        for nodeStatus in nodeStatusList:
            if nodeStatus[1].strip().strip('\n') == "Decommissioned":
                hosts.append(nodeStatus[0].strip().strip('\n'))
        if logoutput:
            logger.info("getDecommissionedDatanodes returns %s", hosts)
        return hosts

    @classmethod
    @TaskReporter.report_test()
    def getSecondaryNamenode(cls):
        if cls._snnHost is None:
            HDFS2.getSecondaryNamenodeHttpAddress()
        return cls._snnHost

    @classmethod
    @TaskReporter.report_test()
    def getNNLogFile(cls, host=None):
        if not host:
            host = cls.getNamenode()
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr="hadoop*-namenode-*.log",
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getNNLogDir(cls, host=None):
        '''
        Gets NN log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/hdfs"
        '''
        tmp = cls.getNNLogFile(host)
        if tmp is None:
            return None
        return util.getDir(tmp)

    @classmethod
    @TaskReporter.report_test()
    def getSNNLogFile(cls, host=None):
        if not host:
            host = cls.getSecondaryNamenode()
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr="hadoop-*secondarynamenode-*.log",
            passwd=Machine.getAdminPasswd()
        )
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getSNNLogDir(cls, host=None):
        '''
        Gets SNN log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/hdfs"
        '''
        tmp = cls.getSNNLogFile(host)
        if tmp is None:
            return None
        return util.getDir(tmp)

    @classmethod
    @TaskReporter.report_test()
    def getDNLogFile(cls, host=None):
        '''
        Returns path of Datanode service log.
        '''
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr="hadoop-*datanode-*.log",
            passwd=Machine.getAdminPasswd(),
            modifiedWitninMin=5
        )
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getDNLogDir(cls, host=None):
        '''
        Gets DN log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/hdfs"
        '''
        if host is None:
            host = cls.getDatanodes()
            if host is None or not host:
                return None
            host = host[0]
        tmp = cls.getDNLogFile(host)
        if tmp is None:
            return None
        return util.getDir(tmp)

    @classmethod
    @TaskReporter.report_test()
    def getNFSLogFile(cls, Node, user=None):
        filepath = ""
        searchstr = "hadoop-*nfs3-*.log"
        if not user:
            filepath = os.path.join(Config.get('hadoop', 'HADOOP_LOG_DIR'), Machine.getAdminUser())
        else:
            filepath = os.path.join(Config.get('hadoop', 'HADOOP_LOG_DIR'), user)

        return Machine.find(
            user=Machine.getAdminUser(),
            host=Node,
            filepath=filepath,
            searchstr=searchstr,
            passwd=Machine.getAdminPasswd(),
            modifiedWitninMin=5
        )[0]

    @classmethod
    @TaskReporter.report_test()
    def getHDFSAuditLogFile(cls, host=None):
        if not host:
            host = cls.getNamenode()
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr="hdfs-audit.log",
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    def getJMXPortNamenode(cls):
        return cls._jmxPortNamenode

    @classmethod
    def getJMXPortDatanode(cls):
        return cls._jmxPortDatanode

    # This method returns 1 if Attribute value is found for Namenode Service in JMX
    @classmethod
    @TaskReporter.report_test()
    def checkNodeAttrVal(cls, service, className, attribute, expectedval, contains):
        node = ''
        port = ''
        if service == 'NameNode':
            node = cls.getNamenode()
            port = cls.getJMXPortNamenode()
        att, value = HDFS.jmxGet(node, port, service, className, attribute)
        result = 0
        if contains:
            if (att == attribute) and (re.search(expectedval.lower(), value.lower())):
                result = 1
        else:
            if (att == attribute) and (value == expectedval):
                result = 1
        if result == 0:
            logger.info("checkNodeAttrVal result == 0")
            logger.info("checkNodeAttrVal (expectedval, value) = (%s, %s)", expectedval, value)
        return result

    @classmethod
    @TaskReporter.report_test()
    def copyNFSLogToLocalMachine(cls, node, user=None, passwd=None):
        nfsLogFile = cls.getNFSLogFile(node, user)
        localNFSLogFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'local-nfs3-' + str(int(time.time())) + '.log')
        user = Machine.getAdminUser()
        passwd = Machine.getAdminPasswd()
        # copy the file to local machine
        Machine.copyToLocal(user=user, host=node, srcpath=nfsLogFile, destpath=localNFSLogFile, passwd=passwd)
        Machine.chmod("777", localNFSLogFile, user=user, passwd=passwd)
        return localNFSLogFile

    @classmethod
    @TaskReporter.report_test()
    def copyDNLogToLocalMachine(cls, dnHost, logoutput=True):
        '''
        Copies DN logs from specified host to local machine.
        Local file is in artifacts directory.
        Returns a String of full path of local datanode file regardless of copy result.
        '''
        dnLogFile = HDFS.getDNLogFile(dnHost)
        logger.info("dnLogFile: %s", dnLogFile)
        localDNLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-datanode-%s-%s.log' % (dnHost, str(int(time.time())))
        )
        if dnLogFile:
            # copy the file to local machine
            if not Hadoop.isSecure():
                #use hrt_qa
                Machine.copyToLocal(user=None, host=dnHost, srcpath=dnLogFile, destpath=localDNLogFile, passwd=None)
            else:
                if Machine.isLinux():
                    remoteTmpFile = Machine.getTempDir() + os.sep + 'datanode-%s.log' % str(int(time.time()))
                    cmd = "cp %s %s" % (dnLogFile, remoteTmpFile)
                    Machine.runas(
                        user=Machine.getAdminUser(),
                        cmd=cmd,
                        host=dnHost,
                        cwd=None,
                        env=None,
                        logoutput=True,
                        passwd=Machine.getAdminPasswd()
                    )
                    Machine.chmod(
                        perm="777",
                        filepath=remoteTmpFile,
                        recursive=False,
                        user=Machine.getAdminUser(),
                        host=dnHost,
                        passwd=Machine.getAdminPasswd(),
                        logoutput=logoutput
                    )
                    Machine.copyToLocal(
                        user=None, host=dnHost, srcpath=remoteTmpFile, destpath=localDNLogFile, passwd=None
                    )
                    Machine.rm(
                        user=Machine.getAdminUser(), host=dnHost, filepath=remoteTmpFile, isdir=False, passwd=None
                    )
                else:
                    #unimplemented
                    pass
        return localDNLogFile

    @classmethod
    @TaskReporter.report_test()
    def verifyNFSMessage(cls, node, message, user=None):
        # get the nfs logs
        localNFSLogFile = cls.copyNFSLogToLocalMachine(node, user)
        index = 0
        f = open(localNFSLogFile)
        for i, line in enumerate(f):
            if line.find("STARTUP_MSG: Starting Nfs3") >= 0:
                index = i
        f.close()
        f = open(localNFSLogFile)
        for i, line in enumerate(f):
            if i > index:
                if re.search(message, line):
                    logger.info("Message:%s found", message)
                    f.close()
                    return True
        f.close()
        return False

    @classmethod
    @TaskReporter.report_test()
    def getNfsJsvcErrfile(cls, Node):
        searchstr = "nfs3_jsvc.err"
        return Machine.find(
            user=Machine.getAdminUser(),
            host=Node,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr=searchstr,
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    @TaskReporter.report_test()
    def copyToLocalNfsJsvcErr(cls, node, passwd=None):
        nfsLogFile = cls.getNfsJsvcErrfile(node)
        localNFSLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-nfs3-jsvc' + str(int(time.time())) + '.err'
        )
        user = Machine.getAdminUser()
        passwd = Machine.getAdminPasswd()
        # copy the file to local machine
        Machine.copyToLocal(user=user, host=node, srcpath=nfsLogFile, destpath=localNFSLogFile, passwd=passwd)
        Machine.chmod("777", localNFSLogFile, user=user, passwd=passwd)
        return localNFSLogFile

    @classmethod
    @TaskReporter.report_test()
    def verifyJsvcErr(cls, node, message, passwd=None):
        #get NFS jsvc error file
        local_nfs_jsvc_err = cls.copyToLocalNfsJsvcErr(node, passwd)
        f = open(local_nfs_jsvc_err)
        text = f.read()
        return re.search(message, text)


    @classmethod
    @TaskReporter.report_test()
    def setup_NFS_manually(cls, NFS_SERVER, skipConfSet=False):
        '''
        This function sets up NFS by Test script
        '''
        mod_conf_path = Hadoop.getModifiedConfigPath()
        if Machine.isDebian() or Machine.isUbuntu():
            Machine.setup_nfs_debian([None], NFS_SERVER)
        cls.setup_portmap(NFS_SERVER)
        if not skipConfSet:
            changes = collections.defaultdict(dict)
            changes['hdfs-site.xml']['dfs.access.time.precision'] = '90000'
            changes['hdfs-site.xml']['dfs.nfs3.stream.timeout'] = '90000'
            changes['hdfs-site.xml']['test.SymlinkEnabledForTesting'] = 'true'
            changes['log4j.properties']['log4j.logger.org.apache.hadoop.hdfs.nfs'] = 'DEBUG'
            changes['core-site.xml']['hadoop.proxyuser.hdfs.hosts'] = '*'
            changes['core-site.xml']['hadoop.proxyuser.hdfs.groups'] = '*'
            # workaround for Nano clusters to set keytab. It will be removed when gsInstaller is fixed
            if not Machine.isFlubber():
                hdfskeytab = Machine.getHeadlessUserKeytab(cls.getHDFSUser())
                changes['hdfs-site.xml']['dfs.nfs.keytab.file'] = hdfskeytab
            if Hadoop.isAmbari() and Hadoop.isSecure():
                changes['hdfs-site.xml']['dfs.nfs.keytab.file'] = '/home/hrt_qa/hadoopqa/keytabs/hdfs.headless.keytab'
                changes['hdfs-site.xml']['dfs.nfs.kerberos.principal'] = 'hdfs@EXAMPLE.COM'
            #restart the cluster with the new config
            Hadoop.modifyConfig(changes, {'nodes': [NFS_SERVER, cls.getNamenode(), cls.getGateway()]})
            cls.restartNamenode(config=mod_conf_path)
            cls.restartNFS(NFS_SERVER, config=mod_conf_path, wait=5)
        if not Machine.isSameHost(NFS_SERVER):
            cls.setup_portmap(None)

    @classmethod
    @TaskReporter.report_test()
    def teardown_NFS_manually(cls, NFS_SERVER):
        '''
        This function cleans up the NFS set by test script
        '''
        Hadoop.restoreConfig(
            ['hdfs-site.xml', 'log4j.properties', 'core-site.xml'],
            {'nodes': [NFS_SERVER, cls.getNamenode(), cls.getGateway()]}
        )
        cls.stopNFS(NFS_SERVER)
        cls.stopPortmap(NFS_SERVER)
        cls.stopPortmap(None)
        cls.restartNamenode()
        if Machine.isDebian():
            if Machine.isDebian7():
                cmd_portmap = "service rpcbind start"
            else:
                cmd_portmap = "service portmap start"
        else:
            cmd_portmap = "service rpcbind start"
        Machine.runas(Machine.getAdminUser(), cmd_portmap, NFS_SERVER, None, None, "True", Machine.getAdminUser())
        Machine.runas(Machine.getAdminUser(), cmd_portmap, None, None, None, "True", Machine.getAdminUser())

    @classmethod
    @TaskReporter.report_test()
    def isNFSAlive(cls, host):
        '''
        This function returns True if NFS is running on host
        otherwise False
        '''
        if Machine.isSameHost(host, cls.getGateway()):
            _, nfs_pid_str = Machine.run("ps aux | grep \"nfs3\"", logoutput=True)
        else:
            _, nfs_pid_str = Machine.run(
                "ssh -o StrictHostKeyChecking=no %s \"ps aux | grep \"nfs3\"\"" % (host), logoutput=True
            )
        check_root_pid = r"root\s+(\d+).*-Dproc_nfs3"
        m1 = re.search(check_root_pid, nfs_pid_str)
        logger.info(m1)
        if not m1:
            logger.info("NFS root user PID is missing")
            return False
        check_hdfs_pid = r"hdfs\s+(\d+).*-Dproc_nfs3"
        m2 = re.search(check_hdfs_pid, nfs_pid_str)
        if not m2:
            logger.info("NFS Hdfs user PID is missing")
            return False
        return True

    @classmethod
    @TaskReporter.report_test()
    def mount_soft_dir(cls, servernode=None, clientnode=None, targetdir=None):
        assert targetdir != None, "Target mount dir is not specified"
        command = ''
        if Machine.isDebian() or Machine.isUbuntu():
            # install nfs-common
            command = 'apt-get -y --force-yes install nfs-common'
        elif Machine.isSuse():
            command = "zypper install -y  nfs-utils"
        else:
            command = "yum -y install nfs-utils"
        exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), command, clientnode, None, None, "True", Machine.getAdminPasswd()
        )
        assert exit_code == 0, "Unable to install nfs utils"
        if not Machine.pathExists(Machine.getAdminUser(), clientnode, targetdir, Machine.getAdminPasswd()):
            command = "mkdir " + targetdir
            Machine.runas(Machine.getAdminUser(), command, clientnode, None, None, "True", Machine.getAdminPasswd())
        command = "mount"
        verify_str = servernode + ":/ on " + targetdir + " type nfs"
        stdout = Machine.runas(
            Machine.getAdminUser(), command, clientnode, None, None, "True", Machine.getAdminPasswd()
        )
        mount_out = str(stdout)
        if mount_out.find(verify_str) < 0:
            command = "mount -o soft,proto=tcp,vers=3,rsize=1048576,wsize=1048576,nolock,sync "\
                      + servernode + ":/ " + targetdir
            return Machine.runas(
                Machine.getAdminUser(), command, clientnode, None, None, "True", Machine.getAdminPasswd()
            )
            #assert exit_code == 0, "Unsucessful mount"
            #assert str(stdout).find(verify_str) <= -1, "Does not mouted properly"
        else:
            logger.info("Already mounted")
            return (1, "Already mounted")

    @classmethod
    @TaskReporter.report_test()
    def umount_dir(cls, node, targetdir):
        command = "umount -l " + targetdir
        _, _ = Machine.runas(
            Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd()
        )

    @classmethod
    def getReplication(cls):
        return cls.getConfigValue("dfs.replication", defaultValue="3")

    @classmethod
    def isWebhdfsEnabled(cls):
        return cls.getConfigValue("dfs.webhdfs.enabled", defaultValue="true") == "true"

    @classmethod
    def getAccessTimePrecision(cls):
        return cls.getConfigValue("dfs.access.time.precision", defaultValue="3600000")

    @classmethod
    @TaskReporter.report_test()
    def getNNFsOption(cls):
        if Hadoop.isEncrypted():
            NAME_NODE = "hdfs://%s" % (cls.getNamenodeHttpsAddress().split(':')[0])
        else:
            NAME_NODE = "hdfs://%s" % (cls.getNamenodeHttpAddress().split(':')[0])
        return '-fs %s' % NAME_NODE

    @classmethod
    @TaskReporter.report_test()
    def enterSafemode(cls, options=''):
        if HDFS.isASV() and Machine.type() == "Windows":
            options = '%s -D "fs.default.name=hdfs://%s"' % (options, cls.getNamenodeRPCAddress())
        if Machine.isHumboldt() and '-fs ' not in options:
            options = '%s %s' % (cls.getNNFsOption(), options)
        _, output = cls.runasAdmin("dfsadmin %s -safemode enter" % options)
        return re.search(".*Safe mode is ON", output) is not None


    @classmethod
    @TaskReporter.report_test()
    def exitSafemode(cls, options='', config=None):
        if HDFS.isASV() and Machine.type() == "Windows":
            options = '%s -D "fs.default.name=hdfs://%s"' % (options, cls.getNamenodeRPCAddress())
        if Machine.isHumboldt() and '-fs ' not in options:
            options = '%s %s' % (cls.getNNFsOption(), options)
        _, output = cls.runasAdmin("dfsadmin %s -safemode leave" % options, None, True, config)
        return re.search(".*Safe mode is OFF", output) is not None


    @classmethod
    def getSafemode(cls, options=''):
        return cls.runasAdmin("dfsadmin %s -safemode get" % options)

    @classmethod
    def waitSafemode(cls):
        '''
        Waits while safemode is ON
        '''
        return cls.runasAdmin("dfsadmin -safemode wait")

    @classmethod
    @TaskReporter.report_test()
    def getNamenodeRPCAddress(cls):
        """
        Returns host:rpc-port of Namenode
        """
        defaultValue = None
        if HDFS.isASV() and Machine.type() == "Windows":
            defaultValue = "namenodehost:9000"
        else:
            if HDFS2.isHAEnabled():
                nameservice = cls.getFirstNameService()
                serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameservice)
                serviceIds = serviceIds.split(',')
                for serviceId in serviceIds:
                    serviceId = serviceId.strip()
                    # get the service state
                    _, output = cls.runasAdmin('haadmin -ns %s -getServiceState %s' % (nameservice, serviceId))
                    if output.lower() == 'active':
                        defaultValue = cls.getConfigValue('dfs.namenode.rpc-address.%s.%s' % (nameservice, serviceId))
            else:
                #fail fast
                defaultValue = None
        return cls.getConfigValue("dfs.namenode.rpc-address", defaultValue=defaultValue)

    # add a default timeout for wait for NN out of safemode for 5 mins = 300s
    # and force the namenode out of safemode if its get stuck by default.
    # If the namenode gets stuck we will return fasle as the resuls of this
    # method
    @classmethod
    @TaskReporter.report_test()
    def waitForNNOutOfSafemode(cls, timeout=1200, forceOutSafemode=True, options='', config=None):
        # sleep 30 seconds between checks
        sleepTime = 30
        # how many checks to do
        numOfChecks = timeout / sleepTime
        i = 0
        # custom config can change fs.default.name
        # Lets append fs.default.name only when config is None, I hope there is no regression in ASV.
        # config is None: ## Does not work at times.Hence appending all the time
        if (HDFS.isASV() and Machine.type() == "Windows") or HDFS.isS3():
            options = '%s -D "fs.default.name=hdfs://%s"' % (options, cls.getNamenodeRPCAddress())
        if Machine.isHumboldt() and '-fs ' not in options:
            options = '%s %s' % (cls.getNNFsOption(), options)
        _, output = cls.runasAdmin("dfsadmin %s -safemode get" % options, None, True, config)
        while (re.search(".*Safe mode is ON", output) != None or re.search(
                ".*failed on connection exception: java.net.ConnectException: Connection refused", output) != None
               or re.search(".*NameNode still not started", output) != None
               or re.search(".* is not an HDFS file system", output) != None):
            # if we have crossed the number of checks return false
            if i >= numOfChecks:
                # if the user selected to force the namenode out of safemode
                # then leave it.
                if forceOutSafemode:
                    cls.exitSafemode(options=options, config=config)

                return False

            _, output = cls.runasAdmin("dfsadmin %s -safemode get" % options, None, True, config)
            i += 1
            time.sleep(sleepTime)

        return True

    @classmethod
    def runAdminReport(cls, logoutput=False, config=None, options=''):
        return cls.runasAdmin("dfsadmin %s -report" % options, logoutput=logoutput, config=config)

    @classmethod
    @TaskReporter.report_test()
    def refreshDatanodes(cls, logoutput=False, config=None, withFS=False):
        if withFS:
            host = cls.getConfigValue("dfs.namenode.rpc-address", defaultValue=None)
            cmd = "dfsadmin -fs hdfs://%s -refreshNodes" % host
            return cls.runasAdmin(cmd, logoutput=logoutput, config=config)

        return cls.runasAdmin("dfsadmin -refreshNodes", logoutput=logoutput, config=config)

    @classmethod
    @TaskReporter.report_test()
    def decompressedText(cls, inpath, outfile, hideKey=False, user=None):
        cmd = "jar %s com.hw.util.DecodeFile -i %s -o %s" % (os.path.join(CWD, 'hdfsutil-0.1.jar'), inpath, outfile)
        if hideKey:
            cmd += " --hideKey"
        return Hadoop.runas(user, cmd, logoutput=False)

    @classmethod
    @TaskReporter.report_test()
    def getBlockInfoDeprecated(cls, dirpath):
        '''
        Deprecated method: use getBlockInfo(..) instead.
        Gets block info for a file that spans only one block.
        Returns (blocksLocationList, blockID)
        Returns (a list of String, String).
        0. BP-1331954746-206.190.52.34-1393265833071:blk_1073741966_1142 len=134217728 repl=3
        [206.190.52.48:50010, 206.190.52.50:50010, 206.190.52.183:50010]
        '''
        _, stdout = cls.runas("fsck " + dirpath + " -files -blocks -locations")
        relevantLine = re.search("(.*0. blk_-*.*)", stdout).group(1)
        blockID = re.search(r"(blk_-?\d+)", relevantLine).group(1)
        blockLocationsList = re.findall(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", relevantLine)
        return blockLocationsList, blockID

    @classmethod
    @TaskReporter.report_test()
    def getBlockInfo(cls, dirpath, logoutput=True):
        '''
        Gets block info for files/directories that might span on more than only one block.
        Returns a list of (blockID, blockPool, blockLength, repl, locations)
        Return type is a list of (str, str, int, int, a list of str)
        e.g. [(1073742191,BP-1331954746-206.190.52.34-1393265833071,27901618,3,
        ['206.190.52.183', ' 206.190.52.48', ' 206.190.52.50'])]
        If exit code is non-zero, returns None.
        '''
        exit_code, stdout = cls.run("fsck " + dirpath + " -files -blocks -locations", logoutput=logoutput)
        if exit_code != 0:
            return None
        result = []
        #logs: 0. BP-1331954746-206.190.52.34-1393265833071:blk_1073741966_1142 len=134217728
        #  repl=3 [206.190.52.48:50010, 206.190.52.50:50010, 206.190.52.183:50010]
        blocksStdout = re.findall(r'\d+[.] BP-.*blk_.*]', stdout)
        for blockStdout in blocksStdout:
            # QE-6879- Erasure coded blocks are -ve numbers. Adding optional - in regex
            blockID = re.search(r"blk_([-]?\d+)", blockStdout).group(1)
            blockPool = re.search(r"(BP-.*?):", blockStdout).group(1)
            blockLength = re.search(r"len=(\d+)", blockStdout).group(1)
            blockLength = int(blockLength)
            repl = re.search(r"repl=(\d+)", blockStdout).group(1)
            repl = int(repl)
            locationStdout = re.search(r"repl=\d+\s+\[(.*)\]", blockStdout).group(1)
            locations = re.findall(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", locationStdout)
            entry = (blockID, blockPool, blockLength, repl, locations)
            result.append(entry)
        return result

    @classmethod
    def getBlockSize(cls, logoutput=True):
        '''
        Gets HDFS block size.
        Returns int.
        '''
        # Pylint changes for unused-argument
        logger.info(logoutput)
        return int(cls.getConfigValue("dfs.blocksize", defaultValue=134217728))

    @classmethod
    @TaskReporter.report_test()
    def getBlockpoolIds(cls):
        # This method has only been used / tested for federated env
        blockpoolIds = []
        dataDirs = cls.getConfigValue('dfs.datanode.data.dir')
        firstDataDir = dataDirs.split(",")[0]
        firstDataDirCurr = os.path.join(firstDataDir, 'current/')
        # pylint: disable=protected-access
        cmd = Machine._getRunnablePythonCmd('glob', "print glob.glob('%sBP-*')" % firstDataDirCurr)
        _, stdout = Machine.runas(Machine.getAdminUser(), cmd)
        # pylint: enable=protected-access
        import ast
        blockpoolDataDirList = ast.literal_eval(stdout)
        for blockpoolDataDir in blockpoolDataDirList:
            blockpoolIds.append(blockpoolDataDir.split(firstDataDirCurr)[1])
        return blockpoolIds

    @classmethod
    def resetNamenode(cls, action, config=None, host=None, option=None):
        if not host:
            host = cls.getNamenode()
        Hadoop.resetService(
            Config.get('hadoop', 'HDFS_USER'), host, "namenode", action, config=config, option=option
        )

    @classmethod
    @TaskReporter.report_test()
    def startNamenode(cls, config=None, safemodeWait=True, wait=5, option=None):
        '''
        Starts Namenode with modified config in namenode.
        Then waits for Namenode with modified config in gateway.
        Required modified config are in sync between gateway and namenode.
        '''
        logger.info(option)
        if HDFS2.isHAEnabled():
            for node in HDFS2.getHANameNodes():
                Hadoop.resetService(
                    Config.get('hadoop', 'HDFS_USER'),
                    node,
                    "namenode",
                    'start',
                    config=config,
                    binFolder="sbin",
                    option=option
                )
        else:
            cls.resetNamenode("start", config=config, option=option)
        if safemodeWait:
            # wait for 5 seconds before we check for safemode
            time.sleep(wait)
            if HDFS2.isHAEnabled():
                HDFS2.waitForActiveAndStandbyNNOutOfSafemode()
            else:
                cls.waitForNNOutOfSafemode(config=config)

    @classmethod
    @TaskReporter.report_test()
    def stopNamenode(cls):
        if HDFS2.isHAEnabled():
            for node in HDFS2.getHANameNodes():
                Hadoop.resetService(Config.get('hadoop', 'HDFS_USER'), node, "namenode", 'stop', binFolder="sbin")
        else:
            cls.resetNamenode("stop")

    @classmethod
    @TaskReporter.report_test()
    def restartNamenode(cls, config=None, wait=5, safemodeWait=True, option=None):
        cls.stopNamenode()
        time.sleep(wait)
        cls.startNamenode(config=config, safemodeWait=safemodeWait, option=option)
        if not safemodeWait:
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def resetDatanodes(cls, action, config=None, nodes=None, logoutput=False):
        if not nodes:
            nodes = []
        if not nodes:
            nodes = cls.getDatanodes(logoutput=logoutput)
        for node in nodes:
            # in secure mode datanode user is root
            if Hadoop.isSecure():
                user = "root"
            else:
                user = Config.get('hadoop', 'HDFS_USER')
            Hadoop.resetService(user, node, "datanode", action, config=config)

    @classmethod
    def startDatanodes(cls, config=None, nodes=None, logoutput=False):
        if not nodes:
            nodes = []
        cls.resetDatanodes("start", config=config, nodes=nodes, logoutput=logoutput)

    @classmethod
    def stopDatanodes(cls, nodes=None):
        if not nodes:
            nodes = []
        cls.resetDatanodes("stop", nodes=nodes)

    @classmethod
    @TaskReporter.report_test()
    def restartDatanodes(cls, config=None, wait=5, nodes=None):
        if not nodes:
            nodes = []
        cls.stopDatanodes(nodes=nodes)
        time.sleep(wait)
        cls.startDatanodes(config=config, nodes=nodes)
        time.sleep(wait)

    @classmethod
    def resetSecondaryNamenode(cls, action, config=None):
        Hadoop.resetService(
            Config.get('hadoop', 'HDFS_USER'), cls.getSecondaryNamenode(), "secondarynamenode", action, config=config
        )

    @classmethod
    def startSecondaryNamenode(cls, config=None):
        cls.resetSecondaryNamenode("start", config=config)

    @classmethod
    def stopSecondaryNamenode(cls):
        cls.resetSecondaryNamenode("stop")

    @classmethod
    @TaskReporter.report_test()
    def restartSecondaryNamenode(cls, config=None, wait=5):
        cls.stopSecondaryNamenode()
        time.sleep(wait)
        cls.startSecondaryNamenode(config=config)
        time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def resetNFS(cls, action, node, config=None, user=None):
        if node == "":
            logger.info("Node not defined")
        else:
            if not user:
                user = Config.get('hadoop', 'HDFS_USER')
            Hadoop.resetService(user, node, "nfs3", action, config=config)

    @classmethod
    @TaskReporter.report_test()
    def startNFS(cls, node=None, config=None, user=None):
        if not node:
            node = cls.getGateway()
        if not user:
            user = Machine.getAdminUser()
        cls.resetNFS("start", node, config=config, user=user)

    @classmethod
    @TaskReporter.report_test()
    def stopNFS(cls, node=None, user=None):
        if not node:
            node = cls.getGateway()
        if not user:
            user = Machine.getAdminUser()
        cls.resetNFS("stop", node, user=user)

    @classmethod
    @TaskReporter.report_test()
    def restartNFS(cls, node, config=None, wait=5, user=None):
        if not node:
            node = cls.getGateway()
        if not user:
            user = Machine.getAdminUser()
        cls.stopNFS(node, user=user)
        time.sleep(wait)
        cls.startNFS(node, config=config, user=user)
        time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def resetPortmap(cls, action, node, config=None):
        if node == "":
            logger.info("Node not defined")
        else:
            Hadoop.resetService(Machine.getAdminUser(), node, "portmap", action, config=config)

    @classmethod
    @TaskReporter.report_test()
    def startPortmap(cls, node, config=None):
        if node == "":
            logger.info("Node not defined")
        else:
            cls.resetPortmap("start", node, config=config)

    @classmethod
    @TaskReporter.report_test()
    def stopPortmap(cls, node):
        if node == "":
            logger.info("Node not defined")
        else:
            cls.resetPortmap("stop", node)

    @classmethod
    @TaskReporter.report_test()
    def restartPortmap(cls, node, config=None, wait=5):
        if node == "":
            logger.info("Node not defined")
        else:
            cls.stopPortmap(node)
            time.sleep(wait)
            cls.startPortmap(node, config=config)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def setup_portmap(cls, host):
        """
        setup nfs portmap on host
        """
        if Machine.isDebian():
            if Machine.isDebian7():
                cmd = "service rpcbind stop"
            else:
                cmd = "service portmap stop"
        else:
            cmd = "service rpcbind stop"
        _ = Machine.runas(Machine.getAdminUser(), cmd, host, None, None, "True", Machine.getAdminUser())
        cls.startPortmap(host)

    @classmethod
    @TaskReporter.report_test()
    def stopServices(cls, secondaryNN=False):
        cls.stopDatanodes()
        if not HDFS2.isHAEnabled():
            if secondaryNN:
                cls.stopSecondaryNamenode()
        cls.stopNamenode()

    @classmethod
    @TaskReporter.report_test()
    def startServices(cls, config=None, secondaryNN=False, safemodeWait=True, logoutput=False):
        cls.startNamenode(config=config, safemodeWait=False)
        if not HDFS2.isHAEnabled():
            if secondaryNN:
                cls.startSecondaryNamenode(config=config)
        cls.startDatanodes(config=config, logoutput=logoutput)
        if safemodeWait:
            if HDFS2.isHAEnabled():
                HDFS2.waitForActiveAndStandbyNNOutOfSafemode()
            else:
                cls.waitForNNOutOfSafemode(config=config)

    @classmethod
    @TaskReporter.report_test()
    def getNFSPrincipal(cls, defaultValue=None, fromModified=False):
        if fromModified:
            return Hadoop.getmodifiedConfigValue("hdfs-site.xml", "nfs.kerberos.principal", defaultValue=defaultValue)
        else:
            return cls.getConfigValue("nfs.kerberos.principal", defaultValue=defaultValue)

    @classmethod
    @TaskReporter.report_test()
    def getNameNodePrincipal(cls, defaultValue=None):
        return cls.getConfigValue("dfs.namenode.kerberos.principal", defaultValue=defaultValue)

    @classmethod
    @TaskReporter.report_test()
    def getPartFileContents(cls, hdfspath, user=None):
        return cls.cat(hdfspath, user)

    # method to get the datanode which is serving a file on hdfs
    @classmethod
    @TaskReporter.report_test()
    def getDatanodeForAnHdfsFile(cls, path, user=None):
        p = re.compile(r"^0. blk_.*\[(.*?):.*\]")
        cmd = 'fsck %s -files -blocks -locations' % path
        _, output = cls.runas(user, cmd)
        for line in output.split("\n"):
            line = line.strip()
            m = p.search(line)
            if m:
                # found a datanode returning it
                return m.group(1)
        # did not find a datanode returning None
        return None

    # method to get jmx metrics from namenode
    @classmethod
    @TaskReporter.report_test()
    def getJMXPropValueFromNN(cls, Property):
        nn_http_address = cls.getNamenodeHttpAddress()
        urlContent = util.getURLContents("http://" + nn_http_address + "/jmx")
        logger.info("**** urlcontent from getJMXPropValueFromNN ****")
        logger.info(urlContent)
        logger.info("****************")
        m = re.search(".*" + Property + ".*: [0-9]{1,}", urlContent)
        if not m:
            return 0
        m = re.search("[0-9]{1,}", m.group())
        #logger.info("URL content %s" % urlContent)
        return int(m.group().strip())

    # method to create snapshot
    @classmethod
    @TaskReporter.report_test()
    def createSnapshot(cls, snapshotDir, snapshotName="", user=None):
        return cls.runas(user, "dfs -createSnapshot " + snapshotDir + " " + snapshotName)

    # method to delete snapshot
    #@classmethod
    #@TaskReporter.report_test()
    #def deleteSnapshot(cls, snapshotDir, snapshotName, user=None):
    #  return cls.runas(user, "dfs -deleteSnapshot " + snapshotDir + " " + snapshotName)

    # method to rename snapshot
    @classmethod
    def renameSnapshot(cls, snapshotDir, oldSnapshotName, newSnapshotName, user=None):
        return cls.runas(user, "dfs -renameSnapshot " + snapshotDir + " " + oldSnapshotName + " " + newSnapshotName)

    # method to list snapshots
    @classmethod
    def listSnapshot(cls, user=None):
        return cls.runas(user, "lsSnapshottableDir")

    # method to allow snapshot
    @classmethod
    def allowSnapshot(cls, snapshotDir, user=None):
        return cls.runas(user, "dfsadmin -allowSnapshot " + snapshotDir)

    # method to disallow snapshot
    @classmethod
    def disallowSnapshot(cls, snapshotDir, user=None):
        return cls.runas(user, "dfsadmin -disallowSnapshot " + snapshotDir)

    # method to find diff of snapshots
    @classmethod
    def diffSnapshot(cls, snapshotDir, snapshot1, snapshot2, user=None):
        return cls.runas(user, "snapshotDiff " + snapshotDir + " " + snapshot1 + " " + snapshot2)

    # method to delete a single snapshot
    @classmethod
    @TaskReporter.report_test()
    def deleteSnapshot(cls, snapshotDir, snapshotName, user=None):
        '''
        Deletes snapshot in a given directory
        Returns (exit_code, stdout)
        '''
        return cls.runas(user, "dfs -deleteSnapshot %s %s" % (snapshotDir, snapshotName))

    # method to delete all snapshots
    @classmethod
    @TaskReporter.report_test()
    def deleteAllSnapshots(cls, user=None):
        output = cls.runas(user, "lsSnapshottableDir")
        for Dir in re.findall("(/.*)", output[1]):
            lsOutput = cls.runas(user, "dfs -ls " + Dir + "/" + ".snapshot")
            for path in re.findall("(/.*)", lsOutput[1]):
                cls.runas(user, "dfs -deleteSnapshot " + Dir + " " + path.split("/")[-1])

    @classmethod
    def isASV(cls):
        return str(Hadoop.getConfigValue('fs.defaultFS'))[:4] == 'wasb'

    @classmethod
    def isCabo(cls):
        '''
        Determines if the defaultFS is ADLS. Cabo is now deprecated.
        '''
        return str(Hadoop.getConfigValue('fs.defaultFS'))[:3] == 'adl'

    @classmethod
    def isS3(cls):
        return str(Hadoop.getConfigValue('fs.defaultFS'))[:3] == 's3a'

    @classmethod
    @TaskReporter.report_test()
    def getFSType(cls):
        if cls.isASV():
            return "wasb"
        elif cls.isCabo():
            return "adl"
        elif cls.isS3():
            return "s3a"
        else:
            return "hdfs"

    @classmethod
    @TaskReporter.report_test()
    def isHDFSDefaultFS(cls):
        return cls.getFSType() == "hdfs"


    @classmethod
    @TaskReporter.report_test()
    def isEncrypted(cls):
        return cls.getConfigValue('dfs.encrypt.data.transfer', defaultValue="false").lower() == "true"

    @classmethod
    @TaskReporter.report_test()
    def isS3Encrypted(cls):
        s3Encryption = Hadoop.getConfigValue('fs.s3a.server-side-encryption-algorithm', None)
        return s3Encryption


    @classmethod
    @TaskReporter.report_test()
    def getDataConnectorTypes(cls):
        if cls.isS3Encrypted():
            return ["s3a"]
        else:
            return ["s3a", "wasb", "adl"]

    @classmethod
    def isHttpsEnabled(cls):
        '''
        Returns true/false if HTTPS is enabled or not.
      '''
        # if HTTPS_ONLY or HTTP_AND_HTTPS are set for this property return true
        return 'https' in cls.getConfigValue('dfs.http.policy', 'HTTP_ONLY').lower()


    @classmethod
    def fsck(cls, path, args=None, logoutput=False):
        '''
        Runs hdfs fsck.
        Returns (exit_code, stdout).
        '''
        pass

    @classmethod
    @TaskReporter.report_test()
    def copyNNLogToLocalMachine(cls):
        '''
        Copies NN logs to local machine.
        Returns a string for local NN log file.
        '''
        nnHost = cls.getNamenode()
        nnLogFile = cls.getNNLogFile(nnHost)
        localNNLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-namenode-' + str(int(time.time())) + '.log'
        )
        # copy the file to local machine
        Machine.copyToLocal(user=None, host=nnHost, srcpath=nnLogFile, destpath=localNNLogFile, passwd=None)
        return localNNLogFile

    @classmethod
    @TaskReporter.report_test()
    def copyAllDNLogsToLocalMachine(cls, logoutput=True):
        '''
        Copies all DN logs to local machine.
        Returns a list of (String, String).
        Returns a list of (DN host, local DN log path)
        '''
        datanodes = cls.getDatanodes()
        result = []
        for dn in datanodes:
            localDNLogPath = cls.copyDNLogToLocalMachine(dn, logoutput=logoutput)
            result.append((dn, localDNLogPath))
        return result

    @classmethod
    @TaskReporter.report_test()
    def copySNNLogToLocalMachine(cls):
        '''
        Copies secondary namenode logs to local machine.
        Returns a String for local SNN log path.
        '''
        snnHost = cls.getSecondaryNamenode()
        snnLogFile = cls.getSNNLogFile(snnHost)
        localSNNLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-secondarynamenode-' + str(int(time.time())) + '.log'
        )
        # copy the file to local machine
        Machine.copyToLocal(user=None, host=snnHost, srcpath=snnLogFile, destpath=localSNNLogFile, passwd=None)
        return localSNNLogFile

    @classmethod
    def restoreFailedStorage(cls, flag):
        '''
        Possible values of flag are true, false and check.
        Enables, disables or checks restoring of
        failed storage according to the flag
        '''
        return cls.runasAdmin("dfsadmin -restoreFailedStorage " + flag)

    @classmethod
    def metasave(cls, fileName):
        '''
        Save Namenode's primary data structures to <filename> in the
        directory specified by hadoop.log.dir property.
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -metasave " + fileName)

    @classmethod
    def printTopology(cls):
        '''
        Prints a tree of the racks and their nodes as reported by the Namenode
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -printTopology")

    @classmethod
    def setQuota(cls, quota, dirName):
        '''
        Set the quota <quota> for each directory <dirName>.
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -setQuota %s %s" % (quota, dirName))

    @classmethod
    def clrQuota(cls, dirName):
        '''
        Clear the quota for each directory <dirName>.
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -clrQuota %s" % (dirName))

    @classmethod
    def setSpaceQuota(cls, quota, dirName):
        '''
        Set the space quota <quota> for each directory <dirName>.
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -setSpaceQuota %s %s" % (quota, dirName))

    @classmethod
    def clrSpaceQuota(cls, dirName):
        '''
        Clear the space quota for each directory <dirName>.
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -clrSpaceQuota %s" % (dirName))

    @classmethod
    @TaskReporter.report_test()
    def formatNN(cls, force=False, logoutput=False):
        """
        Format namenode

        This API uses su hdfs to run format. No console log will be printed out.
        :param logoutput:
        :return: (exit_code, stdout)
        """
        cmd = "hdfs namenode -format"
        if force:
            cmd += " -force"
        #runasAdmin API eventually calls Hadoop.runas which will not really switch to HDFS user
        #  but to kinit as HDFS and run as current user. This is not what we want.
        exit_code, stdout = Machine.runas(user=cls.getHDFSUser(), cmd=cmd, host=cls.getNamenode(), logoutput=logoutput)
        return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def runDiskbalancer(cls, options, logoutput=True, runInBackground=False):
        """
        Run diskbalancer
        This API uses su hdfs to run format.
        :return: (exit_code, stdout)
        """
        cmd = "diskbalancer -%s" % options
        if runInBackground:
            return cls.runInBackgroundAs(user=Config.get('hadoop', 'HDFS_USER'), cmd=cmd)
        else:
            (exit_code, stdout) = cls.runasAdmin(cmd=cmd, host=None, logoutput=logoutput)
            return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def getDiskBalancerNodesWithDataDensity(cls, stdout):
        '''
        Report return output like below:
        [hrt_qa@ctr-e137-1514896590304-0633-01-000002 ~]$ hdfs diskbalancer -report
        18/01/05 01:47:50 INFO command.Command: Processing report command
        18/01/05 01:47:52 INFO balancer.KeyManager: Block token params received from NN:
         update interval=10hrs, 0sec, token lifetime=10hrs, 0sec
        18/01/05 01:47:52 INFO block.BlockTokenSecretManager: Setting block keys
        18/01/05 01:47:52 INFO balancer.KeyManager: Update block keys every 2hrs, 30mins, 0sec
        18/01/05 01:47:52 INFO command.Command: No top limit specified, using default top value 100.
        18/01/05 01:47:52 INFO command.Command: Reporting top 6 DataNode(s) benefiting from running DiskBalancer.
        Processing report command
        No top limit specified, using default top value 100.
        Reporting top 6 DataNode(s) benefiting from running DiskBalancer.
        1/6 ctr-e137-1514896590304-0633-01-000007.hwx.site[172.27.10.214:8010]
         - <2fdebb8e-a544-4156-b15c-383f6760746d>: 1 volumes with node data density 0.00.
        2/6 ctr-e137-1514896590304-0633-01-000005.hwx.site[172.27.17.32:8010]
         - <fbf15a9c-83f1-4fa2-a773-fc880cd72cf8>: 1 volumes with node data density 0.00.
        3/6 ctr-e137-1514896590304-0633-01-000006.hwx.site[172.27.23.18:8010]
         - <5619b6de-72fa-4a2a-af86-b51a0103d4bb>: 1 volumes with node data density 0.00.
        4/6 ctr-e137-1514896590304-0633-01-000004.hwx.site[172.27.38.201:8010]
         - <425b61e8-8261-44b9-b4cb-630d6c805c57>: 1 volumes with node data density 0.00.
        5/6 ctr-e137-1514896590304-0633-01-000003.hwx.site[172.27.56.146:8010]
         - <48c27ce5-1fbf-4751-91a1-c0c4f11e65a2>: 1 volumes with node data density 0.00.
        6/6 ctr-e137-1514896590304-0633-01-000002.hwx.site[172.27.68.73:8010]
         - <3a5f04aa-022c-4ce7-80c4-c1cf52d15f02>: 1 volumes with node data density 0.00.


        '''
        allNodesString = stdout.split('benefiting from running DiskBalancer.')[2]
        dataNodeDataDensityDict = {}
        dataDensityList = re.findall(r"volumes with node data density (\d+\.\d+)", allNodesString)
        dataNodesList = re.findall(r"\d/\d (.*)\[\S+:\d+", allNodesString)
        for dn, dd in zip(dataNodesList, dataDensityList):
            dataNodeDataDensityDict[dn] = dd

        dnWithMaxDataDensity = max(dataNodeDataDensityDict, key=dataNodeDataDensityDict.get)
        return dataNodeDataDensityDict, dnWithMaxDataDensity

    @classmethod
    @TaskReporter.report_test()
    def getDiskBalancerVolumesWithMaxUsedVolume(cls, stdout):
        '''
        Report on a particular DN return output like below:
        18/01/11 02:18:23 INFO command.Command: Processing report command
        18/01/11 02:18:25 INFO balancer.KeyManager: Block token params received from NN: update interval=10hrs, 0sec,
         token lifetime=10hrs, 0sec
        18/01/11 02:18:25 INFO block.BlockTokenSecretManager: Setting block keys
        18/01/11 02:18:25 INFO balancer.KeyManager: Update block keys every 2hrs, 30mins, 0sec
        18/01/11 02:18:25 INFO command.Command: Reporting volume information for DataNode(s).
         These DataNode(s) are parsed from 'ctr-e137-1514896590304-0633-01-000007.hwx.site'.
        ^FProcessing report command
        Reporting volume information for DataNode(s).
         These DataNode(s) are parsed from 'ctr-e137-1514896590304-0633-01-000007.hwx.site'.
        ctr-e137-1514896590304-0633-01-000007.hwx.site[172.27.10.214:8010]
         - <2fdebb8e-a544-4156-b15c-383f6760746d>: 3 volumes with node data density 0.02.
        [DISK: volume-/grid/0/hadoop/hdfs/data/] - 0.01 used: 3486670848/236626177536,
         0.99 free: 233139506688/236626177536, isFailed: False, isReadOnly: False, isSkip: False, isTransient: False.
        [DISK: volume-/grid/0/hadoop/hdfs/newDisk1/] - 0.00 used: 11481088/236626177536,
         1.00 free: 236614696448/236626177536, isFailed: False, isReadOnly: False, isSkip: False, isTransient: False.
        [DISK: volume-/grid/0/hadoop/hdfs/newDisk2/] - 0.00 used: 69632/236626177536,
         1.00 free: 236626107904/236626177536, isFailed: False, isReadOnly: False, isSkip: False, isTransient: False.



        '''
        allDisksString = re.split(r'volumes with node data density \d+\.\d+\.', stdout)[1]
        volumeUsedDict = {}
        diskVolumeList = re.findall(r"DISK: volume-(\S+)/\]", allDisksString)
        diskUsedList = re.findall(r"DISK: volume-\S+\] - (\d+\.\d+)", allDisksString)
        for dv, du in zip(diskVolumeList, diskUsedList):
            volumeUsedDict[dv] = du

        maxUsedVolume = max(volumeUsedDict, key=volumeUsedDict.get)
        return volumeUsedDict, maxUsedVolume

    @classmethod
    def triggerBlockReport(cls, dn):
        '''
        Fetch image to <path>
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -triggerBlockReport %s:%s" % (dn, cls.getDataNodeIPCPort()))

    @classmethod
    @TaskReporter.report_test()
    def runBalancer(cls, config=None, output="", param="", logoutput=False):
        """
        Run balancer

        This API uses su hdfs to run format. No console log will be printed out.
        :param logoutput:
        :return: (exit_code, stdout)
        """
        cmd = "balancer %s" % param
        if output != "":
            cmd += " > %s" % output
        (exit_code, stdout) = cls.runasAdmin(cmd=cmd, host=None, config=config, logoutput=logoutput)
        #(exit_code, stdout) = Machine.runas(user=cls.getHDFSUser(), cmd=cmd, host=cls.getNamenode())
        return exit_code, stdout

    @classmethod
    def fetchImage(cls, path):
        '''
        Fetch image to <path>
        Returns (exit_code, stdout)
        '''
        return cls.runasAdmin("dfsadmin -fetchImage %s" % path)

    @classmethod
    def rollEdits(cls, options=''):
        '''
        Runs dfsadmin -rollEdits
        '''
        return cls.runasAdmin("dfsadmin %s -rollEdits" % options)

    @classmethod
    def saveNamespace(cls):
        '''
        Runs dfsadmin -saveNamespace
        '''
        return cls.runasAdmin("dfsadmin -saveNamespace")

    @classmethod
    def refreshUserToGroupsMappings(cls):
        '''
        Runs dfsadmin -refreshUserToGroupsMappings
        '''
        return cls.runasAdmin("dfsadmin -refreshUserToGroupsMappings")

    @classmethod
    @TaskReporter.report_test()
    def runOIV(cls, fsimage, output_file, user=None, options=None):
        '''
        run OIV on fsimage copy
        Returns pOpenObj and pid in case of default or Web, else
        Returns (exit_code, stdout)
        '''
        if not options:
            cmd = "oiv -i %s -o %s" % (fsimage, output_file)
            pOpenObj = cls.runInBackground(cmd)
            time.sleep(9)
            if Machine.isLinux():
                _, pid = Machine.run(
                    "ps -ef| grep hdfs.tools.offlineImageViewer.OfflineImageViewerPB| grep -v grep | awk '{print $2}'"
                )
                return (pOpenObj, int(pid))
            else:
                pid = Machine.getProcessListWithPid(None, "java.exe", "OfflineImageViewerPB", True)
                return ('', int(pid))
        if options.find('Web') != -1:
            cmd = "oiv %s -i %s -o %s" % (options, fsimage, output_file)
            pOpenObj = cls.runInBackground(cmd)
            time.sleep(9)
            if Machine.isLinux():
                _, pid = Machine.run(
                    "ps -ef| grep hdfs.tools.offlineImageViewer.OfflineImageViewerPB| grep -v grep | awk '{print $2}'"
                )
                return (pOpenObj, int(pid))
            else:
                pid = Machine.getProcessListWithPid(None, "java.exe", "OfflineImageViewerPB", True)
                return ('', int(pid))
        if options != None or options.find('Web') == -1:
            return cls.runas(user, "oiv %s -i %s -o %s" % (options, fsimage, output_file))
        else:
            return None


class HDFS2(BaseHDFS):

    _hdfs_user = Config.get('hadoop', 'HDFS_USER')
    _dfs_http_address_prop = 'dfs.namenode.http-address'
    _dfs_https_address_prop = 'dfs.namenode.https-address'

    # define config key mapping from hadoop 1 to hadoop 2
    _configKeyMap = {}
    _configKeyMap['dfs.block.size'] = 'dfs.blocksize'
    _ha_namenodes = []
    _journal_nodes = []

    # if the key is found in the map return the value for hadoop 2
    # else just return the key back
    @classmethod
    @TaskReporter.report_test()
    def getConfigKey(cls, key):
        if cls._configKeyMap[key]:
            return cls._configKeyMap[key]
        else:
            return key

    # TODO: sending host in secure mode wont work as we need to refactor how
    # we get kerberos credentials on a specific host
    @classmethod
    def runas(
            cls,
            user,
            cmd,
            env=None,
            logoutput=True,
            config=None,
            host=None,
            skipAuth=False,
            stderr_as_stdout=True,
            doEscapeQuote=True
    ):
        """
        HDFS2.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :param config:
        :param host:
        :param skipAuth:
        :param stderr_as_stdout:
        :param doEscapeQuote:
        :return:
        """
        if Hadoop.isSecure() and not skipAuth:
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        hdfs_cmd = Config.get('hadoop', 'HDFS_CMD')
        if config:
            hdfs_cmd += " --config " + config
        hdfs_cmd += " " + cmd
        if stderr_as_stdout:
            return Machine.runas(user, hdfs_cmd, env=env, logoutput=logoutput, host=host, doEscapeQuote=doEscapeQuote)
        else:
            return Machine.runexas(user, hdfs_cmd, env=env, logoutput=logoutput, host=host)

    @classmethod
    @TaskReporter.report_test()
    def createDirectory(cls, directory, user=None, perm=None, force=False):
        out = [0, '']
        bFileExists = False
        if cls.fileExists(directory, user):
            bFileExists = True
            if force:
                exitCode, _ = cls.deleteDirectory(directory, user=user)
                if exitCode == 0:
                    bFileExists = False
        if not bFileExists:
            exitCode, _ = cls.runas(user, "dfs -mkdir -p " + directory)
            if exitCode == 0:
                out[1] = "Created directory \"%s\"" % directory
                if perm:
                    cls.runas(user, "dfs -chmod %s %s" % (perm, directory))
        return out

    @classmethod
    def lsrCmd(cls):
        return "ls -R"

    @classmethod
    def rmrCmd(cls):
        return "rm -r"

    @classmethod
    @TaskReporter.report_test()
    def resetNamenode(cls, action, config=None, host=None, option=None):
        if not host:
            host = cls.getNamenode()
        Hadoop.resetService(
            Config.get('hadoop', 'HDFS_USER'), host, "namenode", action, config, binFolder="sbin", option=option
        )

    @classmethod
    @TaskReporter.report_test()
    def resetZkfc(cls, action, hosts=None):
        if not cls.isHAEnabled():
            raise Exception("This feature requires HDFS HA to be enabled.")

        if not hosts:
            hosts = cls.getHANameNodes()
        for nn in hosts:
            Hadoop.resetService(cls._hdfs_user, nn, "zkfc", action, binFolder="sbin")

    @classmethod
    @TaskReporter.report_test()
    def resetJournalNodes(cls, action):
        if not cls.isHAEnabled():
            raise Exception("This feature requires HDFS HA to be enabled.")
        for jn in cls.getJournalNodes():
            Hadoop.resetService(cls._hdfs_user, jn, "journalnode", action, binFolder="sbin")

    @classmethod
    @TaskReporter.report_test()
    def getJournalNodes(cls):
        if not cls.isHAEnabled():
            raise Exception("This feature requires HDFS HA to be enabled.")
        if cls._journal_nodes is None or not cls._journal_nodes:
            if cls.isFederated():
                nameservice = cls.getFirstNameService()
                shared_edits_dirs = cls.getConfigValue('dfs.namenode.shared.edits.dir.%s' % nameservice).split(';')
            else:
                # sample value
                # qjournal://h2-ha-suse-sec-1395721849-2-8.cs1cloud.internal:8485;
                # h2-ha-suse-sec-1395721849-2-13.cs1cloud.internal:8485;
                # h2-ha-suse-sec-1395721849-2-11.cs1cloud.internal:8485/h2-ha-suse-sec-1395721849-2
                shared_edits_dirs = cls.getConfigValue('dfs.namenode.shared.edits.dir').split(';')
            my_size = len(shared_edits_dirs)
            i = 0
            while i < my_size:
                jn = None
                val = shared_edits_dirs[i]
                # the first JN is of the format qjournal://h2-ha-suse-sec-1395721849-2-8.cs1cloud.internal:8485
                if i == 0:
                    jn = val.split('/')[2].split(':')[0]
                # if its the last elemment it would be like
                #  h2-ha-suse-sec-1395721849-2-11.cs1cloud.internal:8485/h2-ha-suse-sec-1395721849-2
                elif i == my_size - 1:
                    jn = val.split('/')[0].split(':')[0]
                # otherwise we are in the middle and it will look like
                #  h2-ha-suse-sec-1395721849-2-13.cs1cloud.internal:8485
                else:
                    jn = val.split(':')[0]
                cls._journal_nodes.append(jn)
                i += 1
        return cls._journal_nodes

    @classmethod
    def resetSecondaryNamenode(cls, action, config=None):
        Hadoop.resetService(
            Config.get('hadoop', 'HDFS_USER'),
            cls.getSecondaryNamenode(),
            "secondarynamenode",
            action,
            config=config,
            binFolder="sbin"
        )

    @classmethod
    @TaskReporter.report_test()
    def resetDatanodes(cls, action, config=None, nodes=None, logoutput=False):
        if not nodes:
            nodes = []
        if not nodes:
            nodes = cls.getDatanodes(logoutput=logoutput)
        for node in nodes:
            # in secure mode datanode user is root
            if Hadoop.isSecure():
                user = "root"
            else:
                user = Config.get('hadoop', 'HDFS_USER')
            Hadoop.resetService(user, node, "datanode", action, config, binFolder="sbin")

    @classmethod
    @TaskReporter.report_test()
    def resetNFS(cls, action, node, config=None, user=None):
        env = {}
        default_pid_dir = Hadoop.getConfigFromHadoopEnv("HADOOP_PID_DIR",
                                                        "/var/run/hadoop/$USER").replace("$USER", user)
        default_log_dir = Hadoop.getConfigFromHadoopEnv("HADOOP_LOG_DIR",
                                                        "/grid/0/log/hadoop/$USER").replace("$USER", user)
        env = {
            "HADOOP_PRIVILEGED_NFS_USER": Config.get('hadoop', 'HDFS_USER'),
            "HADOOP_PRIVILEGED_NFS_PID_DIR": default_pid_dir,
            "HADOOP_PRIVILEGED_NFS_LOG_DIR": default_log_dir
        }
        Hadoop.resetService(user, node, "nfs3", action, config=config, binFolder="sbin", env=env)

    @classmethod
    @TaskReporter.report_test()
    def resetPortmap(cls, action, node, config=None):
        if node == "":
            logger.info("Node not defined")
        else:
            Hadoop.resetService(Machine.getAdminUser(), node, "portmap", action, config=config, binFolder="sbin")

    @classmethod
    @TaskReporter.report_test()
    def jmxGet(cls, node, port, service, className, attribute, logoutput=True):
        cmd = "jmxget -server %s -port %s -service %s,name=%s %s" % (node, port, service, className, attribute)
        exit_code, output = cls.run(cmd, logoutput=logoutput)
        if exit_code == 0:
            pattern = "(%s)=(.*)" % attribute
            m = re.search(pattern, output)
            return m.group(1), m.group(2)
        else:
            return "", ""

    @classmethod
    @TaskReporter.report_test()
    def getNNHttpAddress(cls):
        nnhttp = cls.getConfigValue("dfs.namenode.http-address")
        if nnhttp:
            _, nodeport = nnhttp.split(":")
            return nodeport
        return False

    @classmethod
    @TaskReporter.report_test()
    def getSNHttpAddress(cls):
        snnHttpAddress = cls.getConfigValue("dfs.namenode.secondary.http-address")
        nodeport = None
        if snnHttpAddress:
            _, nodeport = snnHttpAddress.split(":")
            return nodeport
        return nodeport

    @classmethod
    @TaskReporter.report_test()
    def getDNHttpAddress(cls):
        dnHttpAddress = cls.getConfigValue("dfs.datanode.http.address")
        nodeport = None
        if dnHttpAddress:
            _, nodeport = dnHttpAddress.split(":")
            return nodeport
        return nodeport

    @classmethod
    @TaskReporter.report_test()
    def getDNHttpsPort(cls):
        dnHttpsAddress = cls.getConfigValue("dfs.datanode.https.address", "0.0.0.0:50475")
        return dnHttpsAddress.split(":")[1]

    @classmethod
    @TaskReporter.report_test()
    def getSecondaryNamenodeHttpAddress(cls):
        snnHttpAddress = cls.getConfigValue("dfs.namenode.secondary.http-address")
        if snnHttpAddress:
            cls._snnHost, cls._snnPort = snnHttpAddress.split(":")
            # work around for ASV 2.1 HDFS-26 (from monarch Jira)
            if cls._snnHost == "0.0.0.0":
                hostsnn = Hadoop.getSecondaryNamenodeHost()
                if hostsnn:
                    cls._snnHost = hostsnn[0]
        return snnHttpAddress

    @classmethod
    @TaskReporter.report_test()
    def getSecondaryNamenodeHttpsAddress(cls):
        default = cls.getSecondaryNamenode() + ":50091"
        snnHttpAddress = cls.getConfigValue("dfs.namenode.secondary.https-address", default)
        return snnHttpAddress

    @classmethod
    def getSecondaryNamenodeHttpsPort(cls):
        return cls.getSecondaryNamenodeHttpsAddress().split(":")[1]

    @classmethod
    def getSecondaryNamenodeHttpPort(cls):
        return cls.getSecondaryNamenodeHttpAddress().split(":")[1]

    @classmethod
    def getPartFileContents(cls, hdfspath, user=None):
        return cls.text(hdfspath, user)

    # method to get the datanode which is serving a file on hdfs
    @classmethod
    @TaskReporter.report_test()
    def getDatanodeForAnHdfsFile(cls, path, user=None):
        p = re.compile(r"^0. BP-.*\[(.*?):.*\]")
        cmd = 'fsck %s -files -blocks -locations' % path
        _, output = cls.runas(user, cmd)
        for line in output.split("\n"):
            line = line.strip()
            m = p.search(line)
            if m:
                # found a datanode returning it
                return m.group(1)
        # did not find a datanode returning None
        return None

    # method to get jmx metrics from namenode
    @classmethod
    @TaskReporter.report_test()
    def getJMXPropValueFromNN(cls, Property):
        if Hadoop.isEncrypted():
            nn_http_address = cls.getNamenodeHttpsAddress()
            protocol = "https://"
            urlContent = util.getURLContents(protocol + nn_http_address + "/jmx")
        else:
            nn_http_address = cls.getNamenodeHttpAddress()
            protocol = "http://"
            urlContent = util.getURLContents(protocol + nn_http_address + "/jmx")
        m = re.search(".*" + Property + ".*: [0-9]{1,}", urlContent)
        if not m:
            return 0
        m = re.search("[0-9]{1,}", m.group())
        #logger.info("URL content %s" % urlContent)
        return int(m.group().strip())

    # method to get jmx metrics from namenode
    @classmethod
    @TaskReporter.report_test()
    def getStrJMXPropValueFromNN(cls, Property, logoutput=False):
        if Hadoop.isEncrypted():
            nn_http_address = cls.getNamenodeHttpsAddress()
            protocol = "https://"
            urlContent = util.getURLContents(protocol + nn_http_address + "/jmx")
        else:
            nn_http_address = cls.getNamenodeHttpAddress()
            protocol = "http://"
            urlContent = util.getURLContents(protocol + nn_http_address + "/jmx")
        if logoutput:
            logger.info(
                "getStrJMXPropValueFromNN (nn_http_address, protocol, urlContent) = "
                "(%s,%s,%s)", nn_http_address, protocol, urlContent)
        m = re.search('.*' + Property + '.*: "(.*)"[,]?', urlContent)
        if logoutput:
            logger.info("getStrJMXPropValueFromNN m = %s", m)
        if not m:
            return None
        return m.group(1).strip()

    @classmethod
    @TaskReporter.report_test()
    def getJSONJMXPropValueFromNN(cls, Property, logoutput=False):
        propertyValue = cls.getStrJMXPropValueFromNN(Property, logoutput)
        if propertyValue:
            propertyValue = eval(propertyValue.replace('\\', ''))  # pylint: disable=eval-used
        return propertyValue

    @classmethod
    @TaskReporter.report_test()
    def getLiveNodes(cls):
        hostlist = []
        exit_code, output = cls.runAdminReport()
        logger.info(output)
        if exit_code == 0:
            hostname = ""
            Live = 0
            for line in output.split("\n"):
                line = line.strip()
                # There are two sections in the Admin report
                # parse section must be in alive (available state)
                if line.find("Live datanodes") >= 0:
                    Live = 1
                if Machine.isLinux():
                    if line.find("Name: ") == 0 and Live == 1:
                        hostname = Machine.getfqdn(line[6:].split(":")[0])
                        hostlist.append(hostname)
                else:
                    if line.find("Hostname: ") == 0 and Live == 1:
                        hostname = Machine.getfqdn(re.findall(r"Hostname: (\S+)", line)[0])
                        hostlist.append(hostname)
                if line.find("Dead datanodes") >= 0:
                    Live = 0
        return hostlist

    # method to get the active or standby namenode. This only applies for HA
    # clusters. If HA is not setup it will return None
    @classmethod
    @TaskReporter.report_test()
    def getNamenodeByState(cls, expectedState, retry=True, tries=10):
        # first determine the nameservice
        # for now assume we only have 1
        nameService = cls.getFirstNameService()
        # if name service is not found return
        # none
        if not nameService:
            return None

        # get the service ids
        serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameService)
        serviceIds = serviceIds.split(',')
        for serviceId in serviceIds:
            # trim the text
            serviceId = serviceId.strip()
            # get the hostname
            host = cls.getConfigValue('dfs.namenode.rpc-address.%s.%s' % (nameService, serviceId))
            stepTime = 20
            count = 0
            # get the service state
            exit_code, output, _ = cls.runasAdmin(
                'haadmin -ns %s -getServiceState %s' % (nameService, serviceId), stderr_as_stdout=False
            )
            logger.info('exit code = %s', exit_code)
            logger.info('stdout = %s ', output)
            # retry code because of BUG-9798
            while exit_code != 0 and count < tries and retry:
                logger.info('Retry ' + str(count + 1) + ' of ' + tries)
                count += 1
                time.sleep(stepTime)
                exit_code, output, _ = cls.runasAdmin(
                    'haadmin -ns %s -getServiceState %s' % (nameService, serviceId), stderr_as_stdout=False
                )

            # if the service state is as expected return the hostname
            if output.lower() == expectedState.lower():
                return host.split(':')[0]

        return None

    @classmethod
    @TaskReporter.report_test()
    def getServiceIdByState(cls, expectedState, retry=True):
        '''
        method to get the active or standby service id of namenode.
        This only applies for HA clusters. If HA is not setup it will return None
        '''
        # first determine the nameservice
        # for now assume we only have 1
        nameService = cls.getFirstNameService()
        # if name service is not found return
        # none
        if not nameService:
            return None

        # get the service ids
        serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameService)
        serviceIds = serviceIds.split(',')
        for serviceId in serviceIds:
            # trim the text
            serviceId = serviceId.strip()

            tries = 10
            stepTime = 20
            count = 0
            # get the service state
            exit_code, output, _ = cls.runasAdmin(
                'haadmin -ns %s -getServiceState %s' % (nameService, serviceId), stderr_as_stdout=False
            )
            logger.info('exit code = %s', exit_code)
            # retry code because of BUG-9798
            while exit_code != 0 and count < tries and retry:
                logger.info('Retry ' + str(count + 1) + ' of ' + tries)
                count += 1
                time.sleep(stepTime)
                exit_code, output, _ = cls.runasAdmin(
                    'haadmin -ns %s -getServiceState %s' % (nameService, serviceId), stderr_as_stdout=False
                )

            # if the service state is as expected return the hostname
            if output.lower() == expectedState.lower():
                return serviceId

        return None

    @classmethod
    @TaskReporter.report_test()
    def getNamenodePortByState(cls, state, protocol):
        '''
        Find Http/Https port for active/standy Namenode
        state = active or standby
        protocol = http or https
        '''
        Hostname = cls.getNamenodeByState(state)
        nameService = cls.getFirstNameService()
        if not nameService:
            return None

        # get the service ids
        serviceIds = cls.getConfigValue('dfs.ha.namenodes.%s' % nameService)
        serviceIds = serviceIds.split(',')
        for serviceId in serviceIds:
            # trim the text
            serviceId = serviceId.strip()
            if protocol == "https":
                hostaddress = cls.getConfigValue('dfs.namenode.https-address.%s.%s' % (nameService, serviceId))
            else:
                hostaddress = cls.getConfigValue('dfs.namenode.http-address.%s.%s' % (nameService, serviceId))
            if hostaddress.find(Hostname) >= 0:
                port = hostaddress.split(":")[1]
                return port
        return None

    @classmethod
    def restartActiveNamenode(cls, wait=5, kill=False, waitForSafemodeExit=False, config=None):
        '''
        Restart the Active Namenode
        wait - How long to wait in seconds before we start the namenode
        kill - Do we want to shutdown the namenode or issue kill -9
      '''
        return cls.restartHANamenode(
            'active', wait=wait, kill=kill, waitForSafemodeExit=waitForSafemodeExit, config=config
        )

    @classmethod
    @TaskReporter.report_test()
    def restartHANamenode(cls, state, wait=5, kill=False, waitForSafemodeExit=False, config=None):
        '''
        Restart the namenode based on the state
        wait - How long to wait in seconds before we start the namenode
        kill - Do we want to shutdown the namenode or issue kill -9
      '''
        # get the namenode by state
        namenode = cls.getNamenodeByState(state)
        if not kill:
            cls.resetNamenode('stop', host=namenode)
        else:
            if not namenode:
                logger.error("Not able to get NN by state: %s. Exiting without restarting", state)
                return
            Hadoop.killService('namenode', cls._hdfs_user, namenode)

        # wait and start the namenode
        time.sleep(wait)
        cls.resetNamenode('start', host=namenode, config=config)
        if waitForSafemodeExit:
            cls.waitForActiveAndStandbyNNOutOfSafemode()

    @classmethod
    @TaskReporter.report_test()
    def getHANameNodes(cls):
        if not cls.isHAEnabled():
            raise Exception("This feature requires HDFS HA to be enabled.")

        if cls._ha_namenodes is None or not cls._ha_namenodes:
            # get the name service
            name_services = cls.getConfigValue('dfs.nameservices')
            for name_service in name_services.split(','):
                # get the nn ids
                nn_ids = cls.getConfigValue('dfs.ha.namenodes.%s' % name_service).split(',')
                for nn_id in nn_ids:
                    cls._ha_namenodes.append(
                        cls.getConfigValue('dfs.namenode.rpc-address.%s.%s' % (name_service, nn_id)).split(':')[0]
                    )

        return cls._ha_namenodes

    @classmethod
    @TaskReporter.report_test()
    def resetHANamenodes(cls, action, wait=5, waitForSafemodeExit=False):
        '''
        Restart active and standby namenodes
        wait - How long to wait in seconds before we start the namenode
        waitForSafemodeExit - If you want to wait for safemode exit. Defaults to false
        config - If you want to start the namenodes with a non default config location
      '''
        if not cls.isHAEnabled():
            raise Exception("This feature requires HDFS HA to be enabled.")

        nns = cls.getHANameNodes()
        for nn in nns:
            cls.resetNamenode(action, host=nn)
            time.sleep(wait)

        if waitForSafemodeExit:
            cls.waitForActiveAndStandbyNNOutOfSafemode()

    @classmethod
    @TaskReporter.report_test()
    def waitForActiveAndStandbyNNOutOfSafemode(cls):
        nns = cls.getHANameNodes()
        for node in nns:
            cls.waitForNNOutOfSafemode(options='-fs hdfs://%s:8020' % node)

    @classmethod
    @TaskReporter.report_test()
    def getDataNodeCountAsPerUser(cls, node, user):
        '''
        This function is used to get if the Datanode is started or not.
        It returns 0 if the datanode is not started, else it returns
        number of datanode process running
        '''
        count = 0
        if Machine.type() != "Windows":
            _, stdout = Machine.run("ssh -o StrictHostKeyChecking=no %s \"ps aux " \
                               "| grep \"datanode\"\"" % (node), logoutput=True)
            if Hadoop.isSecure() and user == 'root':
                for x in stdout.split("\n"):
                    if x.find("jsvc.exec -Dproc_datanode", 0) > -1:
                        count = count + 1
            else:
                result = stdout.find("java -Dproc_datanode", 0)
                if result > -1:
                    count = count + 1
        else:
            count = len(Machine.getServiceProcessesList("datanode", node, False))

        return count

    @classmethod
    @TaskReporter.report_test()
    def cacheAddPool(cls, poolName, owner=None, group=None, mode=None, limit=None, maxTtl=None, logoutput=True):
        '''
        Adds cache pool
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -addPool %s" % poolName
        if owner is not None:
            cmd += " -owner %s" % owner
        if group is not None:
            cmd += " -group %s" % group
        if mode is not None:
            cmd += " -mode %s" % mode
        if limit is not None:
            cmd += " -limit %s" % limit
        if maxTtl is not None:
            cmd += " -maxTtl %s" % maxTtl
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def cacheModifyPool(cls, poolName, owner=None, group=None, mode=None, limit=None, maxTtl=None, logoutput=True):
        '''
        Modifies cache pool
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -modifyPool %s" % poolName
        if owner is not None:
            cmd += " -owner %s" % owner
        if group is not None:
            cmd += " -group %s" % group
        if mode is not None:
            cmd += " -mode %s" % mode
        if limit is not None:
            cmd += " -limit %s" % limit
        if maxTtl is not None:
            cmd += " -maxTtl %s" % maxTtl
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def cacheRemovePool(cls, poolName, logoutput=True):
        '''
        Removes cache pool
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -removePool %s" % poolName
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def isErasureCoded(cls, path='/user/hrt_qa/'):
        '''
        Check if Erasure coding is enabled
        return boolean: True or False
        '''
        try:
            cmd = "ec -getPolicy -path %s" % path
            exit_code, stdout = cls.runasAdmin(cmd)
            if exit_code == 0:
                if re.search("(RS|XOR)-", stdout):
                    return True
                strg = 'The erasure coding policy of %s is unspecified' % path
                if re.search(strg, stdout) != None:
                    return False
                else:
                    return True
            else:
                return False
        except Exception:
            return False

    class CachePool(collections.namedtuple('CachePool',
                                           ['name', 'owner', 'group', 'mode', 'limit', 'maxttl', 'bytesNeeded',
                                            'bytesCached', 'bytesOverLimit', 'filesNeeded', 'filesCached'])):
        '''
        A class to store cacheadmin list pool result
        limit is a string.
        '''

        def __str__(self):
            s = "%s,%s,%s,%s,%s,%s" % (self.name, self.owner, self.group, self.mode, self.limit, self.maxttl)
            if self.bytesNeeded is not None:
                s += ",%s,%s,%s,%s,%s" % (
                    self.bytesNeeded, self.bytesCached, self.bytesOverLimit, self.filesNeeded, self.filesCached
                )
            return s

    @classmethod
    @TaskReporter.report_test()
    def cacheListPools(cls, poolName=None, stats=False, logoutput=True):
        '''
        Lists cache pool
        Returns a list of CachePool objects each representing a cache pool.
        Returns None if exit code is non-zero.
        Returns [] if no cache pool is found.
        if name is None, show all pools
        if stats is True, use -stats flag too.
        '''
        (exit_code, stdout) = cls.cacheListPoolsInternal(poolName=poolName, stats=stats, logoutput=logoutput)
        if exit_code != 0:
            return None
        result = []
        passedHeader = False
        for line in stdout.split('\n'):
            if passedHeader:
                #NAME   OWNER  GROUP   MODE            LIMIT  MAXTTL
                #pool1  hdfs   hadoop  rwxr-xr-x   unlimited   never
                entry = line.split()
                if stats:
                    appendItem = HDFS2.CachePool(
                        name=entry[0],
                        owner=entry[1],
                        group=entry[2],
                        mode=entry[3],
                        limit=entry[4],
                        maxttl=entry[5],
                        bytesNeeded=int(entry[7]),
                        bytesCached=int(entry[8]),
                        bytesOverLimit=int(entry[9]),
                        filesNeeded=int(entry[10]),
                        filesCached=int(entry[11])
                    )
                else:
                    appendItem = HDFS2.CachePool(
                        name=entry[0],
                        owner=entry[1],
                        group=entry[2],
                        mode=entry[3],
                        limit=entry[4],
                        maxttl=entry[5],
                        bytesNeeded=None,
                        bytesCached=None,
                        bytesOverLimit=None,
                        filesNeeded=None,
                        filesCached=None
                    )
                result.append(appendItem)
            else:
                if re.search("NAME.*OWNER", line):
                    passedHeader = True
        if logoutput:
            logger.info("cacheListPools returns:")
            for item in result:
                logger.info(item)
        return result

    @classmethod
    @TaskReporter.report_test()
    def cacheListPoolsInternal(cls, poolName=None, stats=False, logoutput=True):
        '''
        Internal method to list cache pools.
        Returns (exit_code, stdout).
        '''
        cmd = "cacheadmin -listPools"
        if stats:
            cmd += " -stats"
        if poolName is not None:
            cmd += " %s" % poolName
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def cacheAddDirective(cls, path, poolName, force=False, replication=None, ttl=None, logoutput=True):
        '''
        Adds cache directive
        Returns (exit_code, stdout, directiveID). DirectiveID is int.
        The 3rd return element is <directiveID> if successful or None if unsucessful)
        '''
        cmd = "cacheadmin -addDirective -path %s -pool %s" % (path, poolName)
        if force is True:
            cmd += " -force"
        if replication is not None:
            cmd += " -replication %s" % replication
        if ttl is not None:
            cmd += " -ttl %s" % ttl
        (exit_code, stdout) = HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)
        if exit_code != 0:
            return (exit_code, stdout, None)
        else:
            matchObj = re.search(r"Added cache directive (\d+)", stdout)
            directiveId = matchObj.group(1)
            return (exit_code, stdout, int(directiveId))

    @classmethod
    @TaskReporter.report_test()
    def cacheModifyDirective(cls, directiveId, path=None, force=False, replication=None, ttl=None, logoutput=True):
        '''
        Modifies cache directive
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -modifyDirective -id %s" % directiveId
        if path is not None:
            cmd += " -path %s" % path
        if force:
            cmd += " -force"
        if replication is not None:
            cmd += " -replication %s" % replication
        if ttl is not None:
            cmd += " -ttl %s" % ttl
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    class CacheDirective(collections.namedtuple('CacheDirective', ['id', 'pool', 'repl', 'expiry',
                                                                   'path', 'bytesNeeded', 'bytesCached',
                                                                   'filesNeeded', 'filesCached'])):
        '''
        A class to store cacheadmin list directives result
        expiry can be "never" or datetime object.
        '''

        def __str__(self):
            s = "%s,%s,%s,%s,%s" % (self.id, self.pool, self.repl, self.expiry, self.path)
            if self.bytesNeeded is not None:
                s += ",%s,%s,%s,%s" % (self.bytesNeeded, self.bytesCached, self.filesNeeded, self.filesCached)
            return s

    @classmethod
    @TaskReporter.report_test()
    def cacheListDirectives(cls, stats=False, path=None, pool=None, logoutput=True):
        '''
        Lists cache directives
        Returns a list of CacheDirectives objects each representing a cache directive.
        Returns None if exit code is non-zero.
        Returns [] if no cache directive is found.
        if name is None, show all directives
        if stats is True, use -stats flag too.
        Expiry field is either datetime type or "never" to indicate no-expiration
        '''
        (exit_code, stdout) = cls.cacheListDirectivesInternal(stats=stats, path=path, pool=pool, logoutput=logoutput)
        if exit_code != 0:
            return None
        result = []
        passedHeader = False
        for line in stdout.split('\n'):
            if passedHeader:
                #ID   POOL                   REPL EXPIRY                    PATH
                #6    testAddPoolMinimal      1   2014-01-31T21:01:17+0000  /tmp
                entry = line.split()
                #I don't know how to convert timezone. %z got complaint in python. Cutting it off.
                if entry[3].strip() == "never":
                    expiryDate = "never"
                else:
                    expiryStr = entry[3]
                    if expiryStr.find("+") > -1:
                        expiryStr = expiryStr[0:expiryStr.find("+")]
                    else:
                        expiryStr = expiryStr[0:expiryStr.rfind("-")]
                    dateTimeFormat = "%Y-%m-%dT%H:%M:%S"
                    expiryDate = datetime.datetime.strptime(expiryStr, dateTimeFormat)
                if stats:
                    appendItem = HDFS2.CacheDirective(
                        id=int(entry[0]),
                        pool=entry[1],
                        repl=int(entry[2]),
                        expiry=expiryDate,
                        path=entry[4],
                        bytesNeeded=int(entry[5]),
                        bytesCached=int(entry[6]),
                        filesNeeded=int(entry[7]),
                        filesCached=int(entry[8])
                    )
                else:
                    appendItem = HDFS2.CacheDirective(
                        id=int(entry[0]),
                        pool=entry[1],
                        repl=int(entry[2]),
                        expiry=expiryDate,
                        path=entry[4],
                        bytesNeeded=None,
                        bytesCached=None,
                        filesNeeded=None,
                        filesCached=None
                    )
                result.append(appendItem)
            else:
                if re.search("ID.*POOL", line):
                    passedHeader = True
        if logoutput:
            logger.info("cacheListDirectives returns:")
            for item in result:
                logger.info(item)
        return result

    @classmethod
    @TaskReporter.report_test()
    def cacheListDirectivesInternal(cls, stats=False, path=None, pool=None, logoutput=True):
        '''
        Internal method to list cache directives.
        Returns (exit_code, stdout).
        '''
        cmd = "cacheadmin -listDirectives"
        if stats:
            cmd += " -stats"
        if path is not None:
            cmd += " -path %s" % path
        if pool is not None:
            cmd += " -pool %s" % pool
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def cacheRemoveDirective(cls, directiveId, logoutput=True):
        '''
        Removes single cache directive
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -removeDirective %s" % directiveId
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def cacheRemoveDirectives(cls, path, logoutput=True):
        '''
        Removes cache directives
        Returns (exit_code, stdout)
        '''
        cmd = "cacheadmin -removeDirectives -path %s" % path
        return HDFS.runasAdmin(cmd=cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def touchz(cls, path, user=None, logoutput=True):
        '''
        Touches a file.
        Returns (exit_code, stdout).
        '''
        cmd = "dfs -touchz %s" % path
        return HDFS.runas(user=user, cmd=cmd, env=None, logoutput=logoutput, config=None, host=None, skipAuth=False)

    @classmethod
    @TaskReporter.report_test()
    def removeAcls(cls, path, user=None, recursive=False):
        '''
        This function deletes all Access acls from file/directory
        '''
        if recursive:
            cmd = "dfs -setfacl -R -b %s" % path
        else:
            cmd = "dfs -setfacl -b %s" % path
        return cls.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def removeDefaultAcls(cls, path, user=None, recursive=False):
        '''
        This function deletes all the default acls from Directory
        '''
        if recursive:
            cmd = "dfs -setfacl -R -k %s" % path
        else:
            cmd = "dfs -setfacl -k %s" % path
        return cls.runas(user, cmd)

    @classmethod
    def removeSpecificAcls(cls, path, parameter, user=None):
        '''
        This function deletes specific Access acls from file/directory
        '''
        cmd = "dfs -setfacl -x %s %s" % (parameter, path)
        return cls.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def setAcls(cls, parameter, path, user=None, recursive=False):
        '''
        This function sets Access/Default acls using setfacl -m option
        '''
        if recursive:
            cmd = "dfs -setfacl -R -m %s %s" % (parameter, path)
        else:
            cmd = "dfs -setfacl -m %s %s" % (parameter, path)
        return cls.runas(user, cmd)

    @classmethod
    def ReplaceAcls(cls, parameter, path, user=None):
        '''
        This function resets Access/default acls.
        if Acls are not present on path, it creates new Acls
        if Acls are present on path, it resets to specified Acls
        '''
        cmd = "dfs -setfacl --set %s %s" % (parameter, path)
        return cls.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def getAcls(cls, path, user=None, recursive=False):
        '''
        This function returns Acls of file/dir.
        Set recursive = true for using -R option
        '''
        if recursive:
            cmd = "dfs -getfacl -R %s" % (path)
        else:
            cmd = "dfs -getfacl %s" % (path)
        return cls.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def checkReadPermissionOnFile(
            cls, filename, expected_file, user=None, allow=True, ishdfs=True, node=None, passwd=None, checkErrormsg=""
    ):
        '''
        This function confirms read permission of file using cat command
        Returns True, if allow = True and filename is readable as user
        Returns True, if allow = False and filename can not be read by user
        if ishdfs=True, 'dfs -cat' command is used
        if ishdfs=False, 'cat' command is run locally
        '''
        if expected_file == "" or filename == "":
            return None
        if ishdfs:
            exit_code, stdout = cls.cat(filename, user, False)
        else:
            exit_code, stdout = Machine.cat(host=node, path=filename, logoutput=True, user=user, passwd=passwd)
        logger.info(stdout)
        if not allow:
            if exit_code != 0:
                if checkErrormsg != "":
                    if stdout.find(checkErrormsg) >= 0:
                        return True
                    else:
                        logger.info(checkErrormsg + " is not found in " + stdout)
                        return False
                else:
                    return True
            else:
                return False
        else:
            if exit_code != 0:
                return False
            result = util.compareOutputToFile(Machine.skipWarnMsg(stdout), expected_file, regex=False, inbetween=False)
            return result

    @classmethod
    @TaskReporter.report_test()
    def checkWritePermissionOnFile(
            cls,
            hdfsfile,
            localappendfile,
            expected_file,
            user=None,
            allow=True,
            config=None,
            ishdfs=True,
            node=None,
            passwd=None,
            mount_point=None,
            checkErrormsg=""
    ):
        '''
        This function confirms write permission of file from HDFS cli (ishdfs = True)
         and NFS mount point (ishdfs = False)
        Parameters:
          hdfsfile = file on HDFS
                     [if ishdfs=True, it is the source file on hdfs where append operation will be performed.
                      if ishdfs=False, it is a new file location in hdfs where file will be uploaded]
          localappendfile = source file for append operation on local machine
                     [if ishdfs=True, it is the local source to use in appendTofile command.
                      if ishdfs=False, localappendfile=""]
          expected_file = expected file after append on local machine
                     [if ishdfs=True, it is the local expected file,
                      if ishdfs=False, expected_file=""]
          user = user to run this test, only used when ishdfs=False
          allow = if write operation is expected to pass, set allow=True , otherwise allow=False
          config = configuration with which this validation works
          ishdfs = to select whether to test with hdfs or nfs
                   [if ishdfs= True, test with hdfs and if ishdfs=False, test with nfs]
          node = hostname only used when ishdfs=False
          passwd = password for user , only used when ishdfs=False
          mount_point = mount point for the nfs client
                     [if ishdfs=True, mount_point = None.
                      if ishdfs=False, mount_point = pass mount point for node]
          checkErromsg = if allow is set to false and if function is looking for perticular error message,
                         checkErrormsg needs to be set like Permission denied
        '''
        if hdfsfile == "":
            return None
        if ishdfs:
            cmd = "dfs -appendToFile %s %s" % (localappendfile, hdfsfile)
            exit_code, stdout = cls.runas(user, cmd, None, True, config, None, False)
        else:
            tmp_input_file = os.path.join(Machine.getTempDir(), "tmp_10MB.txt")
            if not Machine.pathExists(None, None, tmp_input_file, None):
                assert Machine.create_file("1M", Machine.getTempDir(), "tmp_10MB.txt", "10"), "10MB file not created"
            if not Machine.isSameHost(node):
                Machine.copyFromLocal(None, node, tmp_input_file, tmp_input_file, None)
            if not HDFS.fileExists(hdfsfile.split(mount_point)[1], user=Config.get('hadoop', 'HDFS_USER')):
                cmd = "cp %s %s" % (tmp_input_file, hdfsfile)
                exit_code, stdout = Machine.runas(user, cmd, node, None, None, True, passwd)
        if not allow:
            if exit_code != 0:
                if checkErrormsg == "":
                    return True
                else:
                    if stdout.find(checkErrormsg) >= 0:
                        return True
                    else:
                        logger.info(checkErrormsg + " is not found in " + stdout)
                        return False
            else:
                return False
        else:
            if exit_code != 0:
                return False
            time.sleep(5)
            if ishdfs:
                exit_code, stdout = cls.cat(hdfsfile, Config.get('hadoop', 'HDFS_USER'), False, config)
                result = util.compareOutputToFile(
                    Machine.skipWarnMsg(stdout), expected_file, regex=False, inbetween=False
                )
                return result
            else:
                time.sleep(20)
                exit_code, stdout = cls.runas(None, "dfs -ls " + hdfsfile.split(mount_point)[1])
                return bool(stdout.find("10485760") >= 0)

    @classmethod
    @TaskReporter.report_test()
    def Check_Acl(cls, filename, user, expected_acls):
        '''
       This function finds the acls of a file/directory and compares the expected acls to actual acls
       getfacl -R is not handled here
       The expected acl is a dictonary object like expected_acls =
        {'# file': '/tmp/HDFSAcls/Dir-0/file-02',
         'mask:': 'rw-', '# group': 'hdfs', 'user:hrt_1': 'rw-',
          'user:': 'rw-', '# owner': 'hrt_qa', 'other:': 'r--', 'group:': 'r--'}
       '''
        cmd = "dfs -getfacl %s" % (filename)
        exit_code, output = cls.runas(user, cmd)
        assert exit_code == 0, "getfacl failed"
        actual_acls = {}
        lines = output.split("\n")
        for line in lines:
            if line == "":
                print "skipped empty line"
            elif line.find("WARN") >= 0:
                print "skipped warn line"
            else:
                tmp = line.rsplit(":", 1)
                actual_acls[tmp[0].strip().replace(" ", "")] = tmp[1].strip()
        logger.info("actual_acls = %s", actual_acls)
        logger.info("expected_acls = %s", expected_acls)
        return cmp(actual_acls, expected_acls)

    @classmethod
    @TaskReporter.report_test()
    def Check_Acl_Recursive(cls, dirname, user, expected_acls):
        '''
        This function finds the acls of a directory recursively (using -R option)
         and compares the expected acls to actual acls
        getfacl -R command returns Access/defaultAcls of each file/dir inside input directory.
        expected_acl is a default dictionary object like {<file> : {"# file": filename, "# owner": user}}
        '''
        _, stdout = HDFS.getAcls(dirname, user, True)
        actual_acls = collections.defaultdict(dict)
        lines = stdout.split("\n")
        filename = ""
        for line in lines:
            line = line.replace("\r", "").replace("\n", "")
            if line == "":
                print "skip"
            elif line.find("# file") >= 0:
                tmp = line.rsplit(":", 1)
                filename = tmp[1].strip()
                actual_acls[filename][tmp[0].strip()] = filename
            else:
                tmp = line.rsplit(":", 1)
                actual_acls[filename][tmp[0].strip()] = tmp[1].strip()
        logger.info("expected = %s", expected_acls)
        logger.info("actual_acls = %s", actual_acls)
        return cmp(actual_acls, expected_acls)

    @classmethod
    @TaskReporter.report_test()
    def getNNWebAppAddress(cls, haNN=None):
        '''
          Returns the namenode web app address based on if HTTPS is enabled or not.
          Keyword arguments:
            haNN -- HA Namenode for which to get the webapp http address. Only applies
                    if HA is enabled.
        '''
        propName = cls._dfs_http_address_prop
        scheme = 'http'
        if cls.isHttpsEnabled():
            propName = cls._dfs_https_address_prop
            scheme = 'https'
        # If HA is enabled then get all http/https addresses and determine which is ithe active one.
        logger.info("HDFS2.getNNWebAppAddress (propName, scheme)=(%s,%s)", propName, scheme)
        if cls.isHAEnabled():
            # if no HA Namenode is provided use the active namenode
            if not haNN:
                haNN = str(cls.getNamenodeByState('active'))
            nameService = cls.getFirstNameService()
            # get the service ids
            dfs_ha_namenode_prop = "dfs.ha.namenodes." + nameService
            serviceIds = cls.getConfigValue(dfs_ha_namenode_prop).split(',')
            logger.info("HDFS2.getNNWebAppAddress serviceIds=%s", serviceIds)
            for serviceId in serviceIds:
                logger.info("HDFS2.getNNWebAppAddress serviceId=%s", serviceId)
                conf_param = propName + "." + nameService + "." + serviceId
                webAppAddress = '%s://%s' % (
                    scheme, cls.getConfigValue(conf_param)
                )
                webAppAddress = str(webAppAddress)
                logger.info("HDFS2.getNNWebAppAddress webAppAddress=%s", webAppAddress)
                if haNN in webAppAddress:
                    return webAppAddress
            logger.error("in HA env, active NN webaddress not found")
            return None
        else:
            return '%s://%s' % (scheme, cls.getConfigValue(propName))

    @classmethod
    @TaskReporter.report_test()
    def getNNWebPort(cls):
        '''
        Return the port where Namenode serve's http/https requests
        '''
        return cls.getNNWebAppAddress().split(':')[2]

    @classmethod
    @TaskReporter.report_test()
    def isHAEnabled(cls):
        '''
        Returns true/false if HA is enabled or not.
      '''
        return cls.getConfigValue('dfs.nameservices', '') != ''

    @classmethod
    @TaskReporter.report_test()
    def fsck(cls, path, args=None, logoutput=False):
        '''
        Runs hdfs fsck.
        Returns (exit_code, stdout).
        Overridden method.
        '''
        cmd = "fsck %s" % path
        if args is not None:
            cmd += " %s" % args
        logger.info("DEBUG0 %s", cmd)
        return cls.runasAdmin(cmd, env=None, logoutput=logoutput, config=None)

    @classmethod
    @TaskReporter.report_test()
    def isKMSEnabled(cls):
        '''
        Checks if KMS server is started or not by looking into core-site.xml
        and hdfs-site.xml
        '''

        keyProviderPath = Hadoop.getConfigValue("hadoop.security.key.provider.path")
        if keyProviderPath is not None:
            if re.search("kms://.*/kms", keyProviderPath) is not None:
                keyProviderURI = HDFS.getConfigValue("dfs.encryption.key.provider.uri")
                if re.search("kms://.*/kms", keyProviderURI) is not None:
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getKMSHost(cls):
        '''
        Obtains Kms host. in case of HA, returns first host in core-site.xml

        :return:
        '''
        kmsHost = None
        if not cls.isKMSEnabled():
            return kmsHost
        keyProviderPath = Hadoop.getConfigValue("hadoop.security.key.provider.path")
        if keyProviderPath is not None:
            kmsHost = str(keyProviderPath).replace("kms://", "").replace("@", "://")
            if len(kmsHost.split(";")) == 2:
                return kmsHost.split(";")[0] + ":" + kmsHost.split(";")[1].split(":")[1]
            else:
                return kmsHost
        else:
            return kmsHost

    @classmethod
    @TaskReporter.report_test()
    def validateEncryptionZone(cls, path, isEZ=True, user=None):
        '''
        Validates if the path passed is encrypted if isEZ=True; or return True/False based on path being encrypted
        or not if is EZ = None
        '''
        nn_addr = HDFS.getNamenode()
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        if Hadoop.isEncrypted():
            nn_port = HDFS.getConfigValue('dfs.https.port', '50701')
            http_protocol = 'https://'
        else:
            nn_port = HDFS.getNamenodeHttpPort()
            http_protocol = 'http://'
        protocol = 'webhdfs'
        env = {}

        if Hadoop.isSecure():
            kerbTicket = Machine.getKerberosTicket(user, rmIfExists=True)
            env['KRB5CCNAME'] = kerbTicket
            url = "%s%s:%s/%s/v1%s?op=GETFILESTATUS" % (http_protocol, nn_addr, nn_port, protocol, path)
            if Hadoop.isEncrypted():
                cmd = "curl -X GET --negotiate -u : -k \"" + url + "\""
            else:
                cmd = "curl -i --negotiate -u : \"" + url + "\""
        else:
            url = "%s%s:%s/%s/v1%s?user.name=%s&op=GETFILESTATUS" % (
                http_protocol, nn_addr, nn_port, protocol, path, user
            )
            cmd = "curl -i \"" + url + "\""

        logger.info("Curl command - %s ", cmd)

        # curl command fails when user is specified. Dont need to run the with Machine.runas, if kinit is done.
        if Hadoop.isSecure():
            _, stdout = Machine.run(cmd, env=env, logoutput=True)
        else:
            _, stdout = Machine.runas(user, cmd, env=env, logoutput=True, host=None)

        # isEZ is None; func is used to get if path is encrypted or not
        if not isEZ:
            return re.search("\"encBit\":true", stdout) is not None
        elif isEZ:
            assert re.search("\"encBit\":true", stdout), "%s is not encrypted" % path
            return True
        else:
            assert not re.search("\"encBit\":true", stdout), "%s is encrypted" % path
            return True

    @classmethod
    @TaskReporter.report_test()
    def createEncryptionZone(cls, path, keyName, keySize=None, user=None):
        '''
        creates encryption zone by creating key
        '''
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        if not HDFS.fileExists(path, user=user):
            HDFS.createDirectory(path, user=user)

        cmd = "key create %s" % keyName
        if keySize is not None:
            cmd += " -size %s" % keySize
        exitCode, stdout = Hadoop.run(cmd=cmd)
        assert exitCode == 0

        cmd = "key list -metadata"
        exitCode, stdout = Hadoop.run(cmd=cmd)
        assert exitCode == 0 and keyName in stdout

        cmd = "crypto -listZones"
        exitCode, stdout = HDFS.runas(user=Config.get('hadoop', 'HDFS_USER'), cmd=cmd)
        assert exitCode == 0
        # get parent folder of the path
        parent = os.path.dirname(path)
        found = False
        for line in stdout.split("\n"):
            # zonekeypair [0] is the encryption zone folder zonekeypair[1] is the key value
            zonekeypair = line.split()
            if len(zonekeypair) < 2:
                logger.info("less than 2 elements found in this line")
                continue
            if (zonekeypair[0] == parent) or (zonekeypair[0] == path):
                logger.info("Encryption zone found. No need to add the path into the zone")
                found = True
                break
        if not found:
            logger.info("create encryption zone")
            cmd = "crypto -createZone -keyName %s -path %s" % (keyName, path)
            exitCode, stdout = HDFS.runas(user=Config.get('hadoop', 'HDFS_USER'), cmd=cmd)
            assert exitCode == 0
            cmd = "crypto -listZones"
            exitCode, stdout = HDFS.runas(user=Config.get('hadoop', 'HDFS_USER'), cmd=cmd)
            assert exitCode == 0 and path in stdout

    @classmethod
    @TaskReporter.report_test()
    def getActiveNN(cls, host, component, cluster=None):
        weburl = Ambari.getWebUrl(hostname=host)
        if not cluster:
            cluster = Ambari.getClusterName(weburl=weburl)
        uri = "/api/v1/clusters/%s/host_components?HostRoles/component_name=" \
              "%s&metrics/dfs/FSNamesystem/HAState=active" % (cluster, component)
        logger.info("Requesting WebURL %s", weburl)
        logger.info("Requesting uri %s", uri)
        response = Ambari.http_get_request(uri, weburl)

        json_data = json.loads(response._content) # pylint: disable=protected-access

        items = json_data['items']
        last = items[-1]
        active_nn = last['HostRoles']['host_name']
        return active_nn

    @classmethod
    @TaskReporter.report_test()
    def getActiveNNFed(cls, host, component, cluster=None):

        weburl = Ambari.getWebUrl(hostname=host)
        if not cluster:
            cluster = Ambari.getClusterName(weburl=weburl)

        HOST_CONFIG1 = Ambari.getConfig(type='hdfs-site', service='HDFS', webURL=weburl)
        host_nameservices = HOST_CONFIG1['dfs.nameservices']
        nameservice = host_nameservices.split(",")[0]
        logger.info("nameservice selected %s", nameservice)
        serviceIds = HOST_CONFIG1['dfs.ha.namenodes.%s' % nameservice]
        logger.info("serviceIds for %s : %s", nameservice, serviceIds)
        serviceIds = serviceIds.split(',')

        uri = "/api/v1/clusters/%s/host_components?HostRoles/component_name=" \
              "%s&metrics/dfs/FSNamesystem/HAState=active" % (cluster, component)

        for _ in range(2):
            logger.info("Requesting WebURL %s", weburl)
            logger.info("Requesting uri %s", uri)
            response = Ambari.http_get_request(uri, weburl)
            json_data = json.loads(response._content) # pylint: disable=protected-access
            items = json_data['items']
            time.sleep(5)
            if items:
                break
        if items:
            for serviceId in serviceIds:
                nnHttpAddress = HOST_CONFIG1['dfs.namenode.http-address.%s.%s' % (nameservice, serviceId)]
                nnHost1 = nnHttpAddress.split(":")[0]
                logger.info("namenode host : %s", nnHost1)

                for item in items:
                    active_nn = item['HostRoles']['host_name']
                    logger.info("ambari active namenode : %s", active_nn)
                    if nnHost1 == active_nn:
                        return active_nn

        logger.info("Could not find any Active NameNode!!")
        return None

    @classmethod
    @TaskReporter.report_test()
    def createErasureCodedZone(cls, path, logoutput=True):
        '''
        creates encryption zone by creating key
        '''
        if not HDFS.fileExists(path):
            HDFS.createDirectory(path)

        cmd = "ec -setPolicy -path %s" % (path)
        return HDFS.runasAdmin(cmd=cmd, logoutput=logoutput)

    @classmethod
    def getErasureCodedZone(cls, path, logoutput=True):
        '''
        returns the exitcode and stdout
        hdfs ec -getPolicy -path <path>
        '''
        cmd = "ec -getPolicy -path %s" % (path)
        return HDFS.runasAdmin(cmd=cmd, logoutput=logoutput)

    @classmethod
    def listErasureCodingPolicies(cls, logoutput=True):
        '''
        returns the exitcode and stdout
        hdfs ec -listPolicies
        '''
        cmd = "ec -listPolicies"
        return HDFS.runasAdmin(cmd=cmd, logoutput=logoutput)


    @classmethod
    @TaskReporter.report_test()
    def isPatchUpgrade(cls):
        '''
        returns True if HDFS Major Version, Minor Version and Maintenance Release are same for
        both from_repository_version and to_repository_version after patch upgrade
        '''
        url = '/api/v1/clusters/cl1/upgrades?fields=Upgrade'
        response = Ambari.http_get_request(url)
        json_data = json.loads(response.content)
        if json_data.get("items") != []:
            from_repository_version = str(json_data["items"][0]["Upgrade"]["versions"]["HDFS"]
                                          ["from_repository_version"])
            to_repository_version = str(json_data["items"][0]["Upgrade"]["versions"]["HDFS"]["to_repository_version"])
            logger.info("from_repository_version : %s", from_repository_version)
            logger.info("to_repository_version : %s", to_repository_version)
            match1 = re.search(r'^(\d+\.\d+\.\d+)', from_repository_version)
            match2 = re.search(r'^(\d+\.\d+\.\d+)', to_repository_version)

            if match1 and match2 and match1.group(1) == match2.group(1):
                return True
        return False


class BaseMAPRED(object):
    _jtHost = None
    _jtPort = "50030"
    _ttHosts = []
    _jhsHost = None
    _jhsPort = None
    _history_server_http_address_prop = 'mapreduce.history.server.http.address'

    # this method in hadoop 1 just returns the key
    @classmethod
    def getConfigKey(cls, key):
        return key

    # method to kill the namenode
    @classmethod
    def killJobtracker(cls):
        Hadoop.killService('jobtracker', Config.get('hadoop', 'MAPRED_USER'), cls.getJobtracker())

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True):
        """
        BaseMAPRED.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :return:
        """
        return Hadoop.runas(user, cmd, env=env, logoutput=logoutput)

    @classmethod
    def run(cls, cmd, env=None, logoutput=True):
        return cls.runas(None, cmd, env=env, logoutput=logoutput)

    @classmethod
    def runasAdmin(cls, cmd, env=None, logoutput=True):
        return cls.runas(Config.get('hadoop', 'MAPRED_USER'), cmd, env=env, logoutput=logoutput)

    @classmethod
    def sleepJobJar(cls):
        return Config.get('hadoop', 'HADOOP_EXAMPLES_JAR')

    @classmethod
    @TaskReporter.report_test()
    def triggerSleepJob(
            cls,
            numOfMaps,
            numOfReduce,
            mapsleeptime,
            reducesleeptime,
            numOfJobs,
            queue='',
            background=False,
            options=None,
            config=None
    ):
        jobCounter = 0
        while jobCounter < numOfJobs:
            if options != None:
                sleepCmd = "jar " + cls.sleepJobJar(
                ) + " sleep " + options + " " + queue + " -m " + numOfMaps + " -r " + numOfReduce + " -mt " +\
                           mapsleeptime + " -rt " + reducesleeptime
            else:
                sleepCmd = "jar " + cls.sleepJobJar(
                ) + " sleep " + queue + " -m " + numOfMaps + " -r " + numOfReduce + " -mt " + mapsleeptime +\
                           " -rt " + reducesleeptime
            if background:
                Hadoop.runInBackground(cmd=sleepCmd, env=None, config=config)
            else:
                Hadoop.run(cmd=sleepCmd, env=None, logoutput=True, config=config)
            jobCounter = jobCounter + 1

    # Gets last entry of job -list as job ID.
    # If targetState is None, any state is fine. If it is not None, only returns job ID if
    # state of last entry equals to target state.
    # Returns None if no job is found.
    @classmethod
    @TaskReporter.report_test()
    def getJobID(cls, targetState=None, user=None):
        output = cls.runas(user, "job -list")
        actLines = output[1].split("\n")
        jobID = ((actLines)[-1]).split()[0]
        # In 3.0 there is new param for JobName which causes the State to move to next index - JobId JobName State.
        #  Updating the index below:
        state = ((actLines)[-1]).split()[2].strip()
        if re.search(r"job_\d+", jobID) is None:
            return None
        if targetState is None or state == targetState:
            return jobID
        else:
            #running can have state shown as "1" in hadoop1 job -list
            if targetState.upper() == "RUNNING" and state == "1":
                return jobID
            return None

    # Gets job ID of first entry with target state.
    # Returns None if no job is found.
    @classmethod
    @TaskReporter.report_test()
    def getJobIDV2(cls, targetState=None):
        output = cls.run("job -list")
        actLines = output[1].split("\n")
        for line in actLines:
            jobID = line.split()[0]
            state = line.split()[1].strip()
            if re.search(r"job_\d+", jobID) is not None:
                if targetState is None or state == targetState:
                    return jobID
                else:
                    #running can have state shown as "1" in hadoop1 job -list
                    if targetState.upper() == "RUNNING" and state == "1":
                        return jobID
        return None

    # Gets a job ID (last entry) with target state within timeout limit.
    # Returns None if not found.
    @classmethod
    @TaskReporter.report_test()
    def waitForJobID(cls, targetState=None, timeout=660, user=None):
        if Machine.isWindows():
            timeout = timeout * 4
        startTime = time.time()
        while time.time() - startTime < timeout:
            jobID = MAPRED.getJobID(targetState=targetState, user=user)
            logger.info("JOB ID is %s", str(jobID))
            if jobID is not None:
                return jobID
            if time.time() - startTime + 5 > timeout:
                break
            logger.info("Sleeping for 5 seconds before getting job ID..")
            time.sleep(5)
        return None

    @classmethod
    def killAJob(cls, jobID):
        return cls.runas(YARN.getYarnUser(), "job -kill %s" % jobID)

    @classmethod
    @TaskReporter.report_test()
    def checkForJobCompletion(cls, jobID, user=None):
        output = cls.runas(user, "job -status %s" % jobID)
        if string.find(
                output[1],
                "reduce() completion: 1.0",
        ) == -1:
            return False
        else:
            return True

    @classmethod
    @TaskReporter.report_test()
    def isJobComplete(cls, jobID):
        _, stdout = cls.run("job -status %s" % jobID)
        return stdout.find("reduce() completion: 1.0") != -1

    @classmethod
    @TaskReporter.report_test()
    def failAttempts(cls, attemptIDs):
        for attemptID in attemptIDs:
            cls.run("job -fail-task " + attemptID)

    @classmethod
    def failTask(cls, taskId):
        cls.run("job -fail-task " + taskId)

    @classmethod
    @TaskReporter.report_test()
    def failAllJobTasks(cls, jobId):
        attemptIds = cls.getAttemptIdsForJobId(jobId)
        #race condition could happen here. What if at this stage some known attempt IDs are done?
        #use the method cautiously.
        while attemptIds:
            for Id in attemptIds:
                cls.failTask(Id)
            attemptIds = cls.getAttemptIdsForJobId(jobId)
            if Machine.isHumboldt():
                # retry to get attempt ids for humboldt as per YARN-95
                time.sleep(10)
                attemptIds = cls.getAttemptIdsForJobId(jobId)

    @classmethod
    @TaskReporter.report_test()
    def killAttempts(cls, attemptIDs):
        for attemptID in attemptIDs:
            cls.run("job -kill-task " + attemptID)

    @classmethod
    @TaskReporter.report_test()
    def isJobFailed(cls, jobID):
        output = cls.run("job -status %s " % jobID)
        if string.find(output[1], "Failed") == -1 and output[1].find("FAILED") == -1:
            return False
        else:
            return True

    @classmethod
    @TaskReporter.report_test()
    def isJobSucceed(cls, jobID):
        output = cls.run("job -status %s " % jobID)
        if string.find(
                output[1],
                "SUCCEEDED",
        ) == -1:
            return False
        else:
            return True

    @classmethod
    @TaskReporter.report_test()
    def isJobPREP(cls, jobID):
        output = cls.run("job -status %s " % jobID)
        if string.find(
                output[1],
                "PREP",
        ) == -1:
            return False
        else:
            return True

    @classmethod
    @TaskReporter.report_test()
    def waitforPREPTofinish(cls, jobID, timeout=5):
        i = 0
        if not cls.isJobPREP(jobID):
            return True
        while (cls.isJobPREP(jobID) and i < timeout):
            time.sleep(2)
            if not cls.isJobPREP(jobID):
                return True
            i = i + 1
        return False

    @classmethod
    @TaskReporter.report_test()
    def isJobRunning(cls, jobID, user=HRT_QA):
        output = cls.runas(cmd="job -status %s " % jobID, user=user)
        if string.find(
                output[1],
                "RUNNING",
        ) == -1:
            return False
        else:
            return True

    @classmethod
    @TaskReporter.report_test()
    def isJobKilled(cls, jobID):
        output = cls.run("job -status %s " % jobID)
        if string.find(
                output[1],
                "KILLED",
        ) == -1:
            return False
        else:
            return True

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"),
            propertyValue,
            defaultValue=defaultValue
        )

    @classmethod
    @TaskReporter.report_test()
    def isEncrypted(cls):
        return cls.getConfigValue("mapreduce.shuffle.ssl.enabled") == "true"

    @classmethod
    @TaskReporter.report_test()
    def getLocalDirs(cls):
        localdirs = cls.getConfigValue("mapred.local.dir")
        if localdirs:
            localdirs = localdirs.split(",")
        return localdirs

    @classmethod
    @TaskReporter.report_test()
    def getJobtrackerHttpAddress(cls):
        if YARN.isHAEnabled():
            jtHttpAddress = YARN.getRMHostByState("active")
            cls._jtHost, cls._jtPort = jtHttpAddress.split(":")
            return cls._jtHost + ":" + cls._jtPort
        if cls._jtHost is None:
            jtHttpAddress = cls.getConfigValue("mapred.job.tracker.http.address")
            if jtHttpAddress:
                cls._jtHost, cls._jtPort = jtHttpAddress.split(":")
            else:
                jtAddress = cls.getJobtrackerAddress()
                if jtAddress:
                    cls._jtHost = jtAddress[0]
        return cls._jtHost + ":" + cls._jtPort

    @classmethod
    @TaskReporter.report_test()
    def getJobtrackerAddress(cls):
        if YARN.isHAEnabled():
            jtAddress = YARN.getRMHostByState("active")
        else:
            jtAddress = cls.getConfigValue("mapred.job.tracker")
        if jtAddress:
            return jtAddress.split(":")
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getJobtracker(cls):
        if YARN.isHAEnabled():
            cls.getJobtrackerHttpAddress()
        else:
            if cls._jtHost is None:
                cls.getJobtrackerHttpAddress()
        return cls._jtHost

    @classmethod
    @TaskReporter.report_test()
    def getTasktrackers(cls, state=None):
        if not state:
            if not cls._ttHosts:
                for st in ('active', 'blacklisted'):
                    _, stdout = cls.run("job -list-%s-trackers" % st)
                    if Machine.isLinux() or HDFS.isASV():
                        cls._ttHosts.extend(re.findall(r"tracker_(\S+?):", stdout))
                    else:
                        tmpTtHosts = re.findall(r"tracker_(\S+?):", stdout)
                        #get short host name
                        for index, host in enumerate(tmpTtHosts):
                            tmpTtHosts[index] = host.split('.')[0]
                        cls._ttHosts.extend(tmpTtHosts)
            return cls._ttHosts
        elif state in ('active', 'blacklisted'):
            _, stdout = cls.run("job -list-%s-trackers" % state)
            if Machine.isLinux() or HDFS.isASV():
                return re.findall(r"tracker_(\S+?):", stdout)
            else:
                tmpTtHosts = re.findall(r"tracker_(\S+?):", stdout)
                #get short host name
                for index, host in enumerate(tmpTtHosts):
                    tmpTtHosts[index] = host.split('.')[0]
                return tmpTtHosts
        else:
            return None

    @classmethod
    def getActiveTasktrackers(cls):
        return cls.getTasktrackers('active')

    @classmethod
    @TaskReporter.report_test()
    def checkTasktrackerState(cls, node, state, find=True, timeout=90, interval=10):
        exptime = time.time() + timeout
        nodes = cls.getTasktrackers(state=state)
        while time.time() < exptime and ((node not in nodes and find) or (node in nodes and not find)):
            logger.info("Sleeping for %d seconds", interval)
            time.sleep(interval)
            logger.info(state.upper() + " nodes: " + ",".join(nodes))
            nodes = cls.getTasktrackers(state=state)
        return nodes.count(node)

    @classmethod
    @TaskReporter.report_test()
    def checkTasktrackerStateInJobtrackerUI(cls, node, state, msg="", find=True, timeout=90, interval=10):
        exptime = time.time() + timeout
        jtHttpAddress = cls.getJobtrackerHttpAddress()
        urlout = util.getURLContents("http://%s/machines.jsp?type=%s" % (jtHttpAddress, state))
        found = urlout.find(node) != -1 and (msg == "" or (msg != "" and urlout.find(msg) != -1))
        while time.time() < exptime and ((find and not found) or (not find and found)):
            logger.info("Sleeping for %d seconds", interval)
            time.sleep(interval)
            urlout = util.getURLContents("http://%s/machines.jsp?type=%s" % (jtHttpAddress, state))
            found = urlout.find(node) != -1 and (msg == "" or (msg != "" and urlout.find(msg) != -1))
        return (find and found) or (not find and not found)

    @classmethod
    @TaskReporter.report_test()
    def getHistoryserverHttpAddress(cls):
        if cls._jhsHost is None:
            jhsHttpAddress = cls.getConfigValue(cls._history_server_http_address_prop)
            if jhsHttpAddress:
                cls._jhsHost, cls._jhsPort = jhsHttpAddress.split(":")
            return jhsHttpAddress
        else:
            return cls._jhsHost + ":" + cls._jhsPort

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerWebappHttpsAddress(cls):
        jhsWebapphttpsAddress = cls.getConfigValue(
            "mapreduce.jobhistory.webapp.https.address",
            cls.getHistoryserverHttpAddress().split(':')[0] + ":19889"
        )
        return jhsWebapphttpsAddress

    @classmethod
    def getHistoryServerWebappPort(cls):
        return cls.getHistoryServerWebappAddress().split(":")[2]

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerWebappHttpsPort(cls):
        jhsWebapphttpsAddress = cls.getConfigValue(
            "mapreduce.jobhistory.webapp.https.address",
            cls.getHistoryserverHttpAddress().split(':')[0] + ":19889"
        )
        return jhsWebapphttpsAddress.split(":")[1]

    @classmethod
    def getHistoryserver(cls):
        if cls._jhsHost is None:
            cls.getHistoryserverHttpAddress()
        return cls._jhsHost

    @classmethod
    def getCompletedJobLocation(cls):
        return cls.getConfigValue("mapred.job.tracker.history.completed.location")

    # Get job list. Returns [] if not found.
    @classmethod
    @TaskReporter.report_test()
    def getJobList(cls, logoutput=True):
        output = cls.run("job -list", logoutput=logoutput)
        return re.findall("(job_[0-9_]+)", output[1])

    @classmethod
    def isJobExists(cls, jobID):
        return jobID in cls.getJobList()

    @classmethod
    @TaskReporter.report_test()
    def killAllJobs(cls):
        jobidlist = cls.getJobList()
        for jobid in jobidlist:
            cls.killAJob(jobid)

    @classmethod
    @TaskReporter.report_test()
    def getJobOwner(cls):
        output = cls.run("job -list")
        actLines = output[1].split("\n")
        jobOwner = ((actLines)[-1]).split()[5]
        return jobOwner

    @classmethod
    @TaskReporter.report_test()
    def getTTHostForAttemptId(cls, attemptID):
        tthost = None
        # copy the file to the local machine
        localJTLogFile = cls.copyJTLogToLocalMachine()

        f = open(localJTLogFile, "r")
        for line in f:
            searchFor = re.search(attemptID + r".*tracker_(.*):.*/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", line)
            if searchFor:
                tthost = searchFor.group(1)
                break
        f.close()
        return tthost

    @classmethod
    @TaskReporter.report_test()
    def getMRTasks(cls, jobId, max_retries=20):
        '''
        Return the MR tasks for a given id
        :param jobId: jobId to be retrieved.
        :param max_retries: Max # of retries to be tried with a sleep of 10 secs before we fail.
      '''
        for _ in range(max_retries):
            host = cls.getHistoryServerWebappAddress()
            jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s/tasks' % (host, jobId))
            if jmxData:
                if jmxData.has_key('tasks'):
                    return jmxData['tasks']
            # If we cannot find the data, sleep.
            time.sleep(15)

        return None

    @classmethod
    @TaskReporter.report_test()
    def getMRTaskInfo(cls, jobId, taskId):
        '''
        Return the MR task info for a given job id, task id
      '''
        host = cls.getHistoryServerWebappAddress()
        jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s' % (host, jobId, taskId))
        if jmxData and jmxData['task']:
            return jmxData['task']
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getMRTaskAttemptsInfo(cls, jobId, taskId):
        '''
        Return the MR task info for a given job id, task id
      '''
        host = cls.getHistoryServerWebappAddress()
        jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s/attempts' % (host, jobId, taskId))
        if jmxData and jmxData['taskAttempts']:
            return jmxData['taskAttempts']
        else:
            return None

    # method to copy the jobtracker log file to the local machine
    @classmethod
    @TaskReporter.report_test()
    def copyJTLogToLocalMachine(cls):
        jtHost = cls.getJobtrackerHttpAddress().split(":")[0]
        jtLogFile = cls.getJobTrackerLogFile()
        localJTLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-jobtracker-' + str(int(time.time())) + '.log'
        )

        # copy the file to local machine
        Machine.copyToLocal(user=None, host=jtHost, srcpath=jtLogFile, destpath=localJTLogFile, passwd=None)
        return localJTLogFile

    # method to copy the tasktracker log file to the local machine
    @classmethod
    @TaskReporter.report_test()
    def copyTTLogToLocalMachineGivenAttemptID(cls, attemptID):
        ttHost = cls.getTTHostForAttemptId(attemptID)
        ttLogFile = cls.getTaskTrackerLogFile(ttHost)
        localTTLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-tasktracker-' + str(int(time.time())) + '.log'
        )

        # copy the file to local machine
        Machine.copyToLocal(user=None, host=ttHost, srcpath=ttLogFile, destpath=localTTLogFile, passwd=None)
        return localTTLogFile

    # method to copy the tasktracker log file to the local machine
    @classmethod
    @TaskReporter.report_test()
    def copyTTLogToLocalMachineGivenTTHost(cls, ttHost):
        ttLogFile = cls.getTaskTrackerLogFile(ttHost)
        localTTLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-tasktracker-' + str(int(time.time())) + '.log'
        )

        # copy the file to local machine
        Machine.copyToLocal(user=None, host=ttHost, srcpath=ttLogFile, destpath=localTTLogFile, passwd=None)
        return localTTLogFile

    @classmethod
    @TaskReporter.report_test()
    def getTaskDirOnTasktracker(cls, node, user, jobId, attemptId, addlpath=None):
        localdirs = cls.getLocalDirs()
        for ldir in localdirs:
            fpath = os.path.join(ldir, 'taskTracker', user, 'jobcache', jobId, attemptId, 'work')
            logger.info("FPATH: %s", fpath)
            if addlpath:
                fpath = os.path.join(fpath, addlpath)
            if Machine.type() != 'Windows':
                # Use mapred user to search for the file in the cluster nodes
                mapredUser = Config.get('hadoop', 'MAPRED_USER')
                if Machine.pathExists(mapredUser, node, fpath):
                    return fpath
            else:
                # Use admin user to search for the file in the cluster nodes
                adminuser = Machine.getAdminUser()
                adminpasswd = Machine.getAdminPasswd()
                if Machine.pathExists(adminuser, node, fpath, passwd=adminpasswd):
                    return fpath
        return None

    @classmethod
    def refreshTasktrackers(cls):
        _, output = cls.runasAdmin("mradmin -refreshNodes")
        return output

    # Returns a list of attempt IDs.
    @classmethod
    @TaskReporter.report_test()
    def getAttemptIdsForJobId(cls, jobId, task='map', state="running", logoutput=True):
        #Use "map" in hadoop1. Use "MAP" in hadoop2. Overridden method will take care of doing uppercase.
        output = cls.runas(
            YARN.getYarnUser(), "job -list-attempt-ids " + jobId + " " + task.lower() + " " + state.lower(),
            logoutput=logoutput
        )
        return re.findall("(attempt_[mr0-9_]+)", output[1])

    @classmethod
    @TaskReporter.report_test()
    def resetJobtracker(cls, action, config=None):
        if YARN.isHAEnabled():
            YARN.resetHARMNodes(action, config)
        else:
            Hadoop.resetService(
                Config.get('hadoop', 'MAPRED_USER'), cls.getJobtracker(), "jobtracker", action, config=config
            )

    @classmethod
    @TaskReporter.report_test()
    def startJobtracker(cls, config=None):
        if YARN.isHAEnabled():
            YARN.resetHARMNodes("start", config)
        else:
            cls.resetJobtracker("start", config=config)

    @classmethod
    @TaskReporter.report_test()
    def stopJobtracker(cls):
        if YARN.isHAEnabled():
            YARN.resetHARMNodes("stop")
        else:
            cls.resetJobtracker("stop")

    @classmethod
    @TaskReporter.report_test()
    def restartJobtracker(cls, config=None, wait=5, waitForNMToRegister=True, getRMHostwithAmbari=False):
        if YARN.isHAEnabled():
            YARN.restartHARMNodes(
                config=config, waitForNMToRegister=waitForNMToRegister, getRMHostwithAmbari=getRMHostwithAmbari
            )
        else:
            cls.stopJobtracker()
            time.sleep(wait)
            cls.startJobtracker(config=config)
            time.sleep(wait)
            if waitForNMToRegister:
                MAPRED.waitForNMToRegister(getRMHostwithAmbari=getRMHostwithAmbari)

    @classmethod
    @TaskReporter.report_test()
    def resetTasktrackers(cls, action, config=None, nodes=None, doInSerial=False):
        # Pylint changes for unused-argument
        logger.info(doInSerial)
        if not nodes:
            nodes = []
        if not nodes:
            nodes = cls.getTasktrackers()
        for node in nodes:
            Hadoop.resetService(Config.get('hadoop', 'MAPRED_USER'), node, "tasktracker", action, config=config)

    @classmethod
    def startTasktrackers(cls, config=None, nodes=None, doInSerial=False):
        if not nodes:
            nodes = []
        cls.resetTasktrackers("start", config=config, nodes=nodes, doInSerial=doInSerial)

    @classmethod
    def stopTasktrackers(cls, config=None, nodes=None, doInSerial=False):
        if not nodes:
            nodes = []
        cls.resetTasktrackers("stop", config=config, nodes=nodes, doInSerial=doInSerial)

    @classmethod
    @TaskReporter.report_test()
    def restartTasktrackers(cls, config=None, wait=30, nodes=None, doInSerial=False):
        # Pylint changes for unused-argument
        logger.info(wait)
        if not nodes:
            nodes = []
        cls.stopTasktrackers(doInSerial=doInSerial, nodes=nodes)
        #time.sleep(wait)
        cls.startTasktrackers(doInSerial=doInSerial, config=config, nodes=nodes)
        #time.sleep(wait)

    @classmethod
    def resetHistoryserver(cls, action, config=None):
        '''
        BaseMAPRED.resetHistoryserver
        '''
        Hadoop.resetService(
            Config.get('hadoop', 'MAPRED_USER'), cls.getHistoryserver(), "historyserver", action, config=config
        )

    @classmethod
    def startHistoryserver(cls, config=None):
        cls.resetHistoryserver("start", config=config)

    @classmethod
    def stopHistoryserver(cls):
        cls.resetHistoryserver("stop")

    @classmethod
    @TaskReporter.report_test()
    def restartHistoryserver(cls, config=None, wait=5):
        cls.stopHistoryserver()
        time.sleep(wait)
        cls.startHistoryserver(config=config)
        time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def stopServices(cls, historyServer=False):
        cls.stopTasktrackers()
        if historyServer:
            cls.stopHistoryserver()
        cls.stopJobtracker()

    @classmethod
    @TaskReporter.report_test()
    def startServices(cls, config=None, historyServer=False):
        cls.startJobtracker(config=config)
        if historyServer:
            cls.startHistoryserver(config=config)
        cls.startTasktrackers(config=config)

    #Added by: Logigear
    #Added date: 05-Sep-2012
    #Function to get active tracker list
    @classmethod
    def getActiveTrackerList(cls, user):
        return cls.runas(user, "job -list-active-trackers")

    #Added by: Logigear
    #Added date: 05-Sep-2012
    #Function to check a name node is active or not
    @classmethod
    @TaskReporter.report_test()
    def isActiveNode(cls, nodeName):
        output = cls.getActiveTrackerList(Config.get('hadoop', 'HDFS_USER'))
        activeTrackerList = output[1]
        startPosition = activeTrackerList.find("_")
        endPosition = activeTrackerList.find(":")
        activeNodeName = activeTrackerList[(startPosition + 1):endPosition]
        return nodeName in activeNodeName

    @classmethod
    @TaskReporter.report_test()
    def enterSafemode(cls):
        _, output = cls.runasAdmin("mradmin -safemode enter")
        return re.search(".*Safe mode is ON", output) is not None

    @classmethod
    @TaskReporter.report_test()
    def exitSafemode(cls):
        _, output = cls.runasAdmin("mradmin -safemode leave")
        return re.search(".*Safe mode is OFF", output) is not  None


    @classmethod
    @TaskReporter.report_test()
    def isSafemodeON(cls):
        _, output = cls.runasAdmin("mradmin -safemode get")
        return re.search(".*Safe mode is ON", output) is not None


    # method to get the location of the job summary log
    @classmethod
    @TaskReporter.report_test()
    def getJobSummaryLogFile(cls, jtHost=None):
        if not jtHost:
            jtHost = cls.getJobtracker()
        file_path = Config.get('hadoop', 'HADOOP_LOG_DIR')
        if Hadoop.isHadoop2():
            file_path = Config.get('hadoop', 'YARN_LOG_DIR')
        file_name = 'hadoop-mapreduce.jobsummary.log'
        if Machine.isWindows():
            file_name = 'rm-appsummary.log'

        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=jtHost,
            filepath=file_path,
            searchstr=file_name,
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)

        # if the job summary log is found return that
        if lines and lines[0]:
            return lines[0]
        # otherwise return the RM/jobtracker log
        else:
            return cls.getJobTrackerLogFile()

    # method to get the location of the job tracker log
    @classmethod
    @TaskReporter.report_test()
    def getJobTrackerLogFile(cls):
        jtHost = cls.getJobtrackerHttpAddress().split(":")[0]
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=jtHost,
            filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
            searchstr="hadoop-*jobtracker-*.log",
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        return lines[0]

    @classmethod
    @TaskReporter.report_test()
    def getJTLogDir(cls):
        '''
        Gets JT/RM log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/yarn"
        '''
        tmp = cls.getJobTrackerLogFile()
        logger.info("tmp location for RM log file is %s", tmp)
        if tmp is None:
            return None
        return util.getDir(tmp)

    # method to get the location of the tasktracker log
    @classmethod
    @TaskReporter.report_test()
    def getTaskTrackerLogFile(cls, ttHost):
        if Machine.isLinux():
            lines = Machine.find(
                user=Machine.getAdminUser(),
                host=ttHost,
                filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
                searchstr="yarn-yarn-nodemanager-*.log",
                passwd=Machine.getAdminPasswd()
            )
        else:
            lines = Machine.find(
                user=Machine.getAdminUser(),
                host=ttHost,
                filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
                searchstr="yarn-nodemanager-*.log",
                passwd=Machine.getAdminPasswd()
            )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if not lines:
            return None
        else:
            return lines[0]

    @classmethod
    @TaskReporter.report_test()
    def getTaskTrackerLogDir(cls):
        '''
        Gets TT/NM log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/yarn"
        '''
        tt = cls.getTasktrackers()
        if tt is None or not tt:
            return None
        tmp = cls.getTaskTrackerLogFile(tt[0])
        if tmp is None:
            return None
        return util.getDir(tmp)

    @classmethod
    @TaskReporter.report_test()
    def getTaskLogContents(cls, ttHost, attemptId):
        url = "http://%s:50060/tasklog?attemptid=%s&all=true" % (ttHost, attemptId)
        filename = 'tasklog-%s-%s-%s.log' % (ttHost, attemptId, str(int(time.time())))
        localTaskLogFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), filename)
        response = util.getURLContents(url, localTaskLogFile)
        logger.info(
            "getTaskLogContents (ttHost,attemptId,localTaskLogFile)=(%s,%s,%s)",
            ttHost, attemptId, localTaskLogFile
        )
        return (response, localTaskLogFile)

    @classmethod
    @TaskReporter.report_test()
    def getTaskLogSyslogFile(cls, ttHost, jobID, attemptID):
        filepath = Config.get('hadoop', 'HADOOP_LOG_DIR') + os.sep + Config.get('hadoop', 'MAPRED_USER') + \
                   "/userlogs/%s/%s/syslog" % (jobID, attemptID)
        searchstr = "syslog"
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=ttHost,
            filepath=filepath,
            searchstr=searchstr,
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        return lines[0]

    # method to get yarn nodemanager principal
    # the reason its called master is so that the method
    # name can remain the same between YARN and MR
    @classmethod
    def getMasterPrincipal(cls, defaultValue=None):
        return cls.getConfigValue("mapreduce.jobtracker.kerberos.principal", defaultValue=defaultValue)

    # Get a job ID (if found) and corresponding attempt IDs after given maximum retry count.
    # Returns (job ID, [attempt ID])
    # If job ID is found at any point, job ID element has job ID. Otherwise, it is None.
    # If attempt IDs are not found, returns [].
    @classmethod
    @TaskReporter.report_test()
    def getJobIDAttemptIDsWithRetries(cls, maxRetryCount, taskType='map', taskState='running'):
        retryCount = 0
        attemptIdsFound = False
        jobId = None
        attemptIds = []
        #poll every 5 seconds
        pollInterval = 5
        while not attemptIdsFound and retryCount < maxRetryCount:
            time.sleep(pollInterval)

            #get job id
            jobId = cls.getJobID()
            if jobId is not None:
                attemptIds = cls.getAttemptIdsForJobId(jobId, taskType, taskState)

            #check if attempt IDs are found
            if attemptIds:
                attemptIdsFound = True
                break
            retryCount += 1

            logger.info(
                "attempt ID list is not found. Retrying within %s secs. Current retry count is %s.",
                pollInterval, retryCount
            )
        return (jobId, attemptIds)

    @classmethod
    @TaskReporter.report_test()
    def getJobIDWithOneMapAttemptIDOneReduceAttemptID(cls, maxRetryCount):
        jobId = None
        mapAttemptID = None
        mapTTHost = None
        reduceAttemptID = None
        reduceTTHost = None

        retryCount = 0
        #sleep every 10 seconds
        sleepInterval = 10
        while (jobId is None or mapAttemptID is None or reduceAttemptID is None) and \
              (retryCount < maxRetryCount):
            if mapAttemptID is None:
                (jobId, attemptIds) = cls.getJobIDAttemptIDsWithRetries(1, 'map')
                if attemptIds:
                    mapAttemptID = attemptIds[0]
                    attemptIdContainerIdHostList = MAPRED2.getAttemptIdContainerIdHostList(jobId)
                    for (attemptID, _, host) in attemptIdContainerIdHostList:
                        if attemptID == mapAttemptID:
                            mapTTHost = host

            if reduceAttemptID is None:
                (jobId, attemptIds) = cls.getJobIDAttemptIDsWithRetries(1, 'reduce')
                if attemptIds:
                    reduceAttemptID = attemptIds[0]
                    attemptIdContainerIdHostList = MAPRED2.getAttemptIdContainerIdHostList(jobId)
                    for (attemptID, _, host) in attemptIdContainerIdHostList:
                        if attemptID == reduceAttemptID:
                            reduceTTHost = host

            retryCount += 1
            time.sleep(sleepInterval)

        return (jobId, mapAttemptID, mapTTHost, reduceAttemptID, reduceTTHost)

    #If treatFailedAsDone==False, given Job ID, wait for either job completes or timeout exceeds.
    #Returns True or False whether job does succeed or not.
    #If treatFailedAsDone==True, returns True if job succeeds or failed.
    @classmethod
    @TaskReporter.report_test()
    def waitForJobDoneOrTimeout(cls, jobID, timeoutInSec=10, sleepIntervalInSec=5, treatFailedAsDone=False):
        if Machine.isWindows():
            timeoutInSec = timeoutInSec * 3
        endTime = time.time() + timeoutInSec
        isJobCompleted = False
        isJobFailed = False
        loopCount = 1
        while not isJobCompleted and time.time() <= endTime:
            isJobCompleted = isJobCompleted or cls.isJobComplete(jobID)
            if cls.isJobFailed(jobID):
                isJobFailed = True
                break
            if Hadoop.isTez():
                if cls.isJobSucceed(jobID):
                    isJobCompleted = True
            logger.info(
                "Sleeping for %d seconds for job to complete with iteration no. %d", sleepIntervalInSec, loopCount
            )
            if not isJobCompleted:
                time.sleep(sleepIntervalInSec)
            loopCount += 1
        if isJobCompleted:
            logger.info("Job ID %s is completed.", jobID)
            return True
        else:
            logger.info("Job ID %s is incomplete.", jobID)
            if time.time() > endTime:
                logger.info("Timeout exceeds.")
            return treatFailedAsDone and isJobFailed


    @classmethod
    @TaskReporter.report_test()
    def waitForJobRunningOrTimeout(cls, jobID, user=HRT_QA, timeoutInSec=20, sleepIntervalInSec=1):
        '''
        Given Job ID, wait for either job is in running state or timeout exceeds.
        Returns True or False whether job is in running state.
        '''
        if Machine.isWindows():
            timeoutInSec = timeoutInSec * 2
        endTime = time.time() + timeoutInSec
        isJobRunning = False
        loopCount = 1

        while not isJobRunning and time.time() <= endTime:
            isJobRunning = isJobRunning or cls.isJobRunning(jobID, user)
            logger.info(
                "Sleeping for %d seconds for job to be in running state with iteration no. %d",
                sleepIntervalInSec, loopCount
            )
            if not isJobRunning:
                time.sleep(sleepIntervalInSec)
            loopCount += 1
        if isJobRunning:
            logger.info("Job ID %s is running.", jobID)
        else:
            logger.info("Job ID %s does not run. Timeout exceeds.", jobID)
        return isJobRunning

    #Given Job ID, wait for either job is in killed state or timeout exceeds.
    #Returns True if job is in KILLED state.
    #Otherwise False.
    @classmethod
    @TaskReporter.report_test()
    def waitForJobKilledOrTimeout(cls, jobID, timeoutInSec=20, sleepIntervalInSec=1):
        if Machine.isWindows():
            timeoutInSec = timeoutInSec * 2
        endTime = time.time() + timeoutInSec
        isJobKilled = False
        loopCount = 1
        while not isJobKilled and time.time() <= endTime:
            isJobKilled = isJobKilled or cls.isJobKilled(jobID)
            logger.info(
                "Sleeping for %d seconds for job to be in KILLED state with iteration no. %d",
                sleepIntervalInSec, loopCount
            )
            if not isJobKilled:
                time.sleep(sleepIntervalInSec)
            loopCount += 1
        if isJobKilled:
            logger.info("Job ID %s is KILLED.", jobID)
        else:
            logger.info("Job ID %s is not in KILLED. Timeout exceeds.", jobID)
        return isJobKilled

    #Get JMX Attribute of RM with HDFS jmxGet. This works on singlenode.
    #For multinode, there might be a jira. Investigating.
    #With minor change, this might work with JT. Currently known to work only with RM.
    @classmethod
    @TaskReporter.report_test()
    def getRMJMXAttribute(cls, jmxPort, className, attribute, logoutput=False):
        (_, value) = HDFS.jmxGet(
            cls.getJobtracker(), jmxPort, 'ResourceManager', className, attribute, logoutput=logoutput
        )
        logger.info("Value of JMX (%s, %s) = (%s)", className, attribute, value)
        return value

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerWebappAddress(cls):
        '''
        Returns Historyserver WebApp Address based on if Https is enabled or not.
        If https is available, returns "https://<JHS-host>:<https-port>".
        Otherwise, returns "http://<JHS-host>:<http-port>".
        '''
        propName = 'mapreduce.jobhistory.webapp.address'
        scheme = 'http'
        defPort = '19888'
        if cls.isHttpsEnabled():
            propName = 'mapreduce.jobhistory.webapp.https.address'
            scheme = 'https'
            defPort = '19890'
        return '%s://%s' % (scheme, cls.getConfigValue(propName, '%s:%s' % (cls.getHistoryserver(), defPort)))

    @classmethod
    def isHttpsEnabled(cls):
        '''
        Returns true/false if HTTPS is enabled or not.
      '''
        # if HTTPS_ONLY or HTTP_AND_HTTPS are set for this property return true
        return 'https' in cls.getConfigValue('mapreduce.jobhistory.http.policy', 'HTTP_ONLY').lower()

    @classmethod
    @TaskReporter.report_test()
    def verifyJobclientOutput(cls, output_file):
        '''
        Function to validate Job client Output
        returns True, if progress of the job is sequential
        returns False, if map/reduce task progress is decreased or job stays at one progress for longer time.
        '''
        f = open(output_file)
        text = f.read()
        pattern = r'mapreduce.Job:  map (\d+)% reduce (\d+)%'
        lines = re.findall(pattern, text)
        pre_map = pre_reduce = 0
        similar_count = 0
        for line in lines:
            curr_map = int(line[0])
            curr_reduce = int(line[1])
            if curr_map == pre_map and curr_reduce == pre_reduce:
                similar_count += 1
            if curr_map < pre_map - 10 or curr_reduce < pre_reduce - 10:
                logger.info("Map or reduce progress has decreased at map:%s reduce:%s", pre_map, pre_reduce)
                return False
            if curr_map > pre_map or curr_reduce > pre_reduce:
                similar_count = 0
                pre_map = curr_map
                pre_reduce = curr_reduce
            if similar_count >= 3:
                logger.info(
                    "The job is steady at one state. map: %s reduce: %s . The job client might be hanged",
                    curr_map, curr_reduce
                )
                return False
        return True


class MAPRED2(BaseMAPRED):
    # define config key mapping from hadoop 1 to hadoop 2
    _configKeyMap = {}
    _configKeyMap['test.randomtextwrite.total_bytes'] = 'mapreduce.randomtextwriter.totalbytes'
    _history_server_http_address_prop = 'mapreduce.jobhistory.webapp.address'

    # if the key is found in the map return the value for hadoop 2
    # else just return the key back
    @classmethod
    @TaskReporter.report_test()
    def getConfigKey(cls, key):
        if cls._configKeyMap[key]:
            return cls._configKeyMap[key]
        else:
            return key

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True):
        """
        MAPRED2.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :return:
        """
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env = {'KRB5CCNAME': kerbTicket}
            user = None
        mapred_cmd = Config.get('hadoop', 'MAPRED_CMD') + " " + cmd
        return Machine.runas(user, mapred_cmd, env=env, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def sleepJobJar(cls):
        ## Get latest MR test jar file. Required by RU
        Dir = Config.get('hadoop', 'HADOOP_HOME').replace("hadoop-client", "hadoop-mapreduce-client")
        testJar = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=Dir,
            searchstr='hadoop-mapreduce-client-jobclient-*-tests.jar',
            passwd=Machine.getAdminPasswd()
        )[0]
        return testJar

    @classmethod
    def getCompletedJobLocation(cls):
        return cls.getConfigValue("mapreduce.jobhistory.done-dir")

    @classmethod
    @TaskReporter.report_test()
    def isJobComplete(cls, jobID):
        _, stdout = cls.run("job -status %s" % jobID)
        return stdout.find("Job state: SUCCEEDED") != -1

    @classmethod
    @TaskReporter.report_test()
    def isJobFailed(cls, jobID):
        _, stdout = cls.run("job -status %s " % jobID)
        return stdout.find("FinalApplicationStatus=FAILED") != -1 or \
               stdout.find("Job state: FAILED") != -1

    @classmethod
    @TaskReporter.report_test()
    def isJobFailedafterRM(cls, jobID):
        _, stdout = cls.run("job -status %s " % jobID)
        return stdout.find("Job state: FAILED") != -1

    # Given job ID, find corresponding attempt IDs
    # Returns [] if no attempt ID is found.
    # Use hadoop job -list-attempt-ids to find out info.
    @classmethod
    @TaskReporter.report_test()
    def getAttemptIdsForJobId(cls, jobId, task='MAP', state="running", logoutput=True):
        if jobId is None:
            return []
        #Use "map" in hadoop1. Use "MAP" in hadoop2. Overridden method will take care of doing uppercase.

        (_, output) = cls.runas(
            YARN.getYarnUser(),
            "job -list-attempt-ids " + jobId + " " + task.upper() + " " + state.lower(),
            logoutput=logoutput
        )
        return re.findall("(attempt_[mr0-9_]+)", output)

    # get RM host
    @classmethod
    @TaskReporter.report_test()
    def getJobtracker(cls):
        if YARN.isHAEnabled():
            cls._jtHost, cls._jtPort = YARN.getResourceManager().split(":")
        if cls._jtHost is None:
            cls._jtHost, cls._jtPort = YARN.getResourceManager().split(":")
        return cls._jtHost

    @classmethod
    @TaskReporter.report_test()
    def resetJobtracker(cls, action, config=None):
        if YARN.isHAEnabled():
            YARN.resetHARMNodes(action, config)
            return
        yarnHome = Config.get('hadoop', 'YARN_HOME').replace("client", "resourcemanager")
        libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
        Hadoop.resetService(
            Config.get('hadoop', 'YARN_USER'), cls.getJobtracker(), "resourcemanager", action, config, yarnHome,
            "sbin", "yarn-daemon", libexecPath
        )

    @classmethod
    @TaskReporter.report_test()
    def checkIfNMIsUp(cls, node, action):
        time.sleep(10)  # Work around to avoid hitting curl returning exit code 7
        nmWebUrl = YARN.getNMWSURL(node)
        ret_code, _, _ = util.query_yarn_web_service(nmWebUrl, HRT_QA, use_user_auth_in_un_secure_mode=True)
        count = 10
        if action == "stop":
            while ret_code != -1 and count > 0:
                logger.info("NM is not yet stopped")
                time.sleep(3)
                count = count - 1
                ret_code, _, _ = util.query_yarn_web_service(
                    nmWebUrl, HRT_QA, use_user_auth_in_un_secure_mode=True
                )
        else:
            while ret_code != 200 and count > 0:
                logger.info("NM is not yet started")
                time.sleep(3)
                count = count - 1
                ret_code, _, _ = util.query_yarn_web_service(
                    nmWebUrl, HRT_QA, use_user_auth_in_un_secure_mode=True
                )

    @classmethod
    @TaskReporter.report_test()
    def threadFuncResetTasktrackers(cls, action, node, config=None):
        yarnHome = Config.get('hadoop', 'YARN_HOME').replace("client", "nodemanager")
        libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
        Hadoop.resetService(
            Config.get('hadoop', 'YARN_USER'), node, "nodemanager", action, config, yarnHome, "sbin", "yarn-daemon",
            libexecPath
        )
        if action == "start":
            logger.info("Checking if NM is %s or not", action)
            cls.checkIfNMIsUp(node, action)
            logger.info("NM action %s - successfull", action)

    @classmethod
    @TaskReporter.report_test()
    def resetTasktrackers(cls, action, config=None, nodes=None, doInSerial=False):
        if not nodes:
            nodes = []
        if not nodes:
            nodes = cls.getTasktrackers()
        threads = []
        if not doInSerial:
            for node in nodes:
                processThread = threading.Thread(
                    target=cls.threadFuncResetTasktrackers, args=(
                        action,
                        node,
                        config,
                    )
                )
                processThread.start()
                threads.append(processThread)
            for thread in threads:
                thread.join()
        else:
            for node in nodes:
                yarnHome = Config.get('hadoop', 'YARN_HOME').replace("client", "nodemanager")
                libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
                Hadoop.resetService(
                    Config.get('hadoop', 'YARN_USER'), node, "nodemanager", action, config, yarnHome, "sbin",
                    "yarn-daemon", libexecPath
                )
                logger.info("Checking if NM is %s or not", action)
                cls.checkIfNMIsUp(node, action)
                logger.info("NM action %s - successfull", action)

    @classmethod
    @TaskReporter.report_test()
    def resetHistoryserver(cls, action, config=None):
        '''
        MAPRED2.resetHistoryserver
        '''
        host = cls.getHistoryserver()
        libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
        if not config:
            hconfig = Config.get('hadoop', 'HADOOP_CONF')
        else:
            hconfig = config

        #if Machine.isLinux():
        serviceName = "historyserver"
        #else:
        #  serviceName = "jobhistoryserver"
        MapredHome = Config.get('hadoop', 'MAPRED_HOME').replace("client", serviceName)
        Hadoop.resetService(
            Config.get('hadoop', 'MAPRED_USER'), host, serviceName, action, hconfig, MapredHome, "sbin",
            "mr-jobhistory-daemon", libexecPath
        )

    @classmethod
    @TaskReporter.report_test()
    def getContainerResources(cls, containerId, NM):
        '''
        Obtain Container Resources
        :param containerId: container Id
        :param NM: host on which container is launched
        :return: memory n vcores used by container
        '''
        nmWebUrl = YARN.getNMWSURL(NM)
        url = nmWebUrl + "/containers/" + containerId
        logger.info(url)
        resType = "JSON"
        res = getHTTPResponse(resType, url, kerberosAuthDisabled=True)
        containerData = getElement(resType, res, "container")
        mem = containerData['totalMemoryNeededMB']
        vcores = containerData['totalVCoresNeeded']
        logger.info("container %s, mem = %d, vcores = %d", containerId, mem, vcores)
        return mem, vcores

    @classmethod
    @TaskReporter.report_test()
    def getClusterMetrics(cls, retry=5):
        '''
        Obtain cluster Metrics
        :param retry:
        :return: cluster Metrics
        '''
        rmServerUrl = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster"
        url = rmServerUrl + "/metrics"
        if not Hadoop.isSecure():
            url = rmServerUrl + "/metrics?user.name=%s" % (Config.get('hadoop', 'YARN_USER'))
        logger.info(url)
        resType = "JSON"
        res = getHTTPResponse(resType, url, kerberosAuthDisabled=True)
        clusterMetrics = getElement(resType, res, "clusterMetrics")
        while getValue(resType, getElement(resType, clusterMetrics, "totalMB")) == 0 and retry > 0:
            time.sleep(10)
            retry = retry - 1
            res = getHTTPResponse(resType, url)
            clusterMetrics = getElement(resType, res, "clusterMetrics")
        return clusterMetrics

    @classmethod
    @TaskReporter.report_test()
    def waitForNMToRegister(cls, prevActiveNM=None, sleep=5, count=15, getRMHostwithAmbari=True):
        '''

        :param prevActiveNM: Previously Known Running NM
        :param sleep: duration to sleep till the current NM count = Previously known NM count
        :return:
        '''
        if not prevActiveNM:
            prevActiveNM = len(Ambari.getHostsForComponent('NODEMANAGER'))
        logger.info("Previously Active NM %s", prevActiveNM)
        if not YARN.checkIfResourceManagerAlive():
            logger.info("Resource manager is not up to fetch metrics")
            return
        rmServerUrl = YARN.getResourceManagerWebappAddress(getRMHostwithAmbari=getRMHostwithAmbari) + "/ws/v1/cluster"
        url = rmServerUrl + "/metrics"
        if not Hadoop.isSecure():
            url = rmServerUrl + "/metrics?user.name=%s" % (Config.get('hadoop', 'YARN_USER'))
        logger.info(url)

        if not YARN.isHAEnabled():
            count = 30
        resType = "JSON"
        for _ in range(count):
            res = getHTTPResponse(resType, url, kerberosAuthDisabled=True)
            if res:
                break
            time.sleep(10)

        clusterMetrics = getElement(resType, res, "clusterMetrics")
        while prevActiveNM != int(getValue(resType, getElement(resType, clusterMetrics, "totalNodes"))) and count > 0:
            time.sleep(sleep)
            count = count - 1
            res = getHTTPResponse(resType, url)
            clusterMetrics = getElement(resType, res, "clusterMetrics")

    @classmethod
    def getLocalDirs(cls):
        return YARN.getLocalDirs()

    @classmethod
    @TaskReporter.report_test()
    def getTaskDirOnTasktracker(cls, node, user, jobId, attemptId, addlpath=None):
        localdirs = cls.getLocalDirs()
        justId = jobId[4:]
        applicationId = "application_" + justId
        # Use admin user to search for the file in the cluster nodes
        adminuser = Machine.getAdminUser()
        adminpasswd = Machine.getAdminPasswd()
        for ldir in localdirs:
            for i in range(1, 4):
                containerId_tmp = "container_" + justId + "_01_00000%s" % i
                containerId = YARN.confirmContainerId(containerId_tmp)
                fpath = os.path.join(ldir, 'usercache', user, 'appcache', applicationId, containerId)
                if addlpath:
                    fpath = os.path.join(fpath, addlpath)
                if HDFS.isCabo():
                    fpath = util.convertFileURIToLocalPath(fpath, isWindows=True)
                if Machine.pathExists(adminuser, node, fpath, passwd=adminpasswd):
                    return fpath
        return None

    @classmethod
    @TaskReporter.report_test()
    def getTTHostForAttemptId(cls, attemptID):
        AMContainerID = cls.getAMContainerID(attemptID)
        amhost = cls.getAMHostForContainerID(AMContainerID)
        localAMLogFile = cls.copyAMLogToLocalhost(amhost, AMContainerID)
        f = open(localAMLogFile, "r")
        for line in f:
            searchFor = re.search("Assigned container (.*) to " + attemptID, line)
            if searchFor:
                containerForAttemptID = searchFor.group(1)
                break
        f.close()
        tthost = cls.getAMHostForContainerID(containerForAttemptID)
        return tthost

    # Copy AM Log to localhost
    # Returns string path of local AM log
    @classmethod
    @TaskReporter.report_test()
    def copyAMLogToLocalhost(cls, amhost, amContainerID, logoutput=True):
        (amLogFile, amLogDir) = cls.getAMLogFile(amhost, amContainerID, logoutput=logoutput)

        localAMLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-application-master-' + str(int(time.time())) + '.log'
        )
        if Machine.type() == "Windows":
            if HDFS.isCabo():
                amLogFile = util.convertFileURIToLocalPath(amLogFile, isWindows=True)

            Machine.copyToLocal(
                user=Machine.getAdminUser(),
                host=amhost,
                srcpath=amLogFile,
                destpath=localAMLogFile,
                passwd=Machine.getAdminPasswd()
            )
        else:
            if Hadoop.isSecure():
                Machine.chmod(
                    "755",
                    amLogDir + "/" + cls.getApplicationID(amContainerID),
                    True,
                    Machine.getAdminUser(),
                    amhost,
                    Machine.getAdminPasswd(),
                    logoutput=False
                )
            else:
                Machine.chmod(
                    "755",
                    amLogDir + "/" + cls.getApplicationID(amContainerID),
                    True,
                    Config.get('hadoop', 'YARN_USER'),
                    amhost,
                    None,
                    logoutput=False
                )
            # copy the file to local machine
            Machine.copyToLocal(user=None, host=amhost, srcpath=amLogFile, destpath=localAMLogFile, passwd=None)
        return localAMLogFile

    # Gets AM Host for given AM container ID.
    # Returns None if not found.
    # Can be used to get TT host of MR container ID too.
    @classmethod
    @TaskReporter.report_test()
    def getAMHostForContainerID(cls, amContainerID):
        if amContainerID is None:
            return None
        #2013-06-29 23:35:31,956 INFO  fica.FiCaSchedulerNode (FiCaSchedulerNode.java:allocateContainer(106)) -
        #Assigned container container_1372548911573_0001_01_000001 of capacity <memory:1536, vCores:1> on host
        #ip-10-152-137-205.ec2.internal:45454, which has 1 containers, <memory:1536, vCores:1> used
        #and <memory:6656, vCores:7> available
        localJTLogFile = cls.copyJTLogToLocalMachine()
        f = open(localJTLogFile, "r")
        host = None
        logger.info("getAMHostForContainerID %s", str(amContainerID))
        for line in f:
            # Added an optional -? check in the regex for AllocationRequestId to be negative like below:
            #container_1511225810785_0027_01_000001, AllocationRequestId: -1, Version: 0,
            #  NodeId: ctr-e134-1499953498516-324884-01-000008.hwx.site:25454
            #searchFor = re.search(amContainerID +", AllocationRequestId: -?\d+, Version: \d+,
            #  NodeId: (.*):(\d*), NodeHttpAddress:.*",line)
            searchFor = re.search(
                amContainerID + r" of capacity .* on host (.*):(\d*), which has .* used and .* available", line
            )
            if searchFor:
                host = searchFor.group(1)
                break
        f.close()
        if host is None:
            logger.info("Warning: AM host is None.")
        return host

    # This method actually works only with AM at this time.
    @classmethod
    def getContainerHost(cls, containerId):
        return cls.getAMHostForContainerID(containerId)

    # override method to copy resource manager log file to the local machine
    @classmethod
    @TaskReporter.report_test()
    def copyJTLogToLocalMachine(cls):
        jtHost = cls.getJobtracker()
        if YARN.isHAEnabled():
            jtHost = YARN.getRMHostByState('active', retry=False)
        jtLogFile = cls.getJobTrackerLogFile()
        localJTLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-resourcemanager-' + str(int(time.time())) + '.log'
        )
        # copy the file to local machine
        Machine.copyToLocal(user=None, host=jtHost, srcpath=jtLogFile, destpath=localJTLogFile, passwd=None)
        return localJTLogFile

    @classmethod
    @TaskReporter.report_test()
    def copyTTLogToLocalMachineGivenAttemptID(cls, attemptID):
        '''
        Override
        '''
        assert False, "invalid api. Use YARN copy NMs."

    @classmethod
    @TaskReporter.report_test()
    def copyTTLogToLocalMachineGivenTTHost(cls, ttHost):
        '''
        Override
        '''
        assert False, "invalid api. Use YARN copy NMs."

    # Get list of (attempt ID, container ID, host) of running MR tasks.
    # AM container is not included in the list. The returned list is sorted by attempt ID.
    # Returns (None, None, None) if no attempt ID is found.
    # Overrides parent's method
    @classmethod
    @TaskReporter.report_test()
    def getAttemptIdContainerIdHostList(cls, jobId, maxRetries=3, sleepInterval=3):
        if jobId is None:
            return (None, None, None)
        amContainerId = cls.getAMContainerID(jobId)
        if amContainerId is None:
            return (None, None, None)
        amHost = cls.getAMHostForContainerID(amContainerId)
        if amHost is None:
            return (None, None, None)
        retryCount = 0
        result = []
        while not result and retryCount < maxRetries:
            result = cls._getAttemptIdContainerIdHostListInternal(jobId, amHost, amContainerId)
            retryCount += 1
            time.sleep(sleepInterval)
        return result

    # Internal method to get list of (attempt ID, container ID, host) of running MR tasks sorted by attempt ID.
    # It uses -list-attempt-ids to find running attempts and goes through AM logs to get container IDs and hosts.
    @classmethod
    @TaskReporter.report_test()
    def _getAttemptIdContainerIdHostListInternal(cls, jobID, amhost, amContainerID):
        runningAttemptIdsMap = cls.getAttemptIdsForJobId(jobID, task='MAP', state="running")
        runningAttemptIdsReduce = cls.getAttemptIdsForJobId(jobID, task='REDUCE', state="running")

        #2013-06-17 17:39:21,937 INFO [AsyncDispatcher event handler]
        #  org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl:
        # TaskAttempt: [attempt_1371368685287_0003_m_000002_0] using containerId:
        #[container_1371368685287_0003_01_000004 on NM: [ip-10-98-62-249:45454]
        localAMLogFile = cls.copyAMLogToLocalhost(amhost, amContainerID)
        f = open(localAMLogFile, "r")
        result = []
        for line in f:
            searchFor = re.search(r"TaskAttempt: \[(.*)\] using containerId: \[(.*) on NM: \[(.*):(.*)\]", line)
            if searchFor:
                attemptId = searchFor.group(1)
                if attemptId in runningAttemptIdsMap or attemptId in runningAttemptIdsReduce:
                    result.append((searchFor.group(1), searchFor.group(2), searchFor.group(3)))
        f.close()

        #result will be list of (attempt ID, container ID, host)
        #sample list element is
        #("attempt_1371368685287_0003_m_000002_0", "container_1371368685287_0003_01_000004", "ip-10-98-62-249")
        result = sorted(result, key=lambda row: row[0])
        return result

    @classmethod
    @TaskReporter.report_test()
    def getReducerTasks(cls, jobId, maxRetries=3, sleepInterval=3, logoutput=True):
        # Pylint changes for unused-argument
        logger.info(logoutput)
        logger.info("MAPRED2 getReducerTasks")
        if jobId is None:
            return [(None, None, None)]
        amContainerId = cls.getAMContainerID(jobId)
        if amContainerId is None:
            return [(None, None, None)]
        amHost = cls.getAMHostForContainerID(amContainerId)
        if amHost is None:
            return [(None, None, None)]
        retryCount = 0
        result = []
        while not result and retryCount < maxRetries:
            result = cls._getAttemptIdContainerIdReducer(jobId, amHost, amContainerId)
            retryCount += 1
            time.sleep(sleepInterval)
        return result

    @classmethod
    @TaskReporter.report_test()
    def _getAttemptIdContainerIdReducer(cls, jobID, amhost, amContainerID):
        #runningAttemptIdsMap = cls.getAttemptIdsForJobId(jobID, task='MAP', state = "running")
        runningAttemptIdsReduce = cls.getAttemptIdsForJobId(jobID, task='REDUCE', state="running")

        #2013-06-17 17:39:21,937 INFO [AsyncDispatcher event handler]
        #  org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl:
        # TaskAttempt: [attempt_1371368685287_0003_m_000002_0] using containerId:
        #[container_1371368685287_0003_01_000004 on NM: [ip-10-98-62-249:45454]
        localAMLogFile = cls.copyAMLogToLocalhost(amhost, amContainerID)
        f = open(localAMLogFile, "r")
        result = []
        for line in f:
            searchFor = re.search(r"TaskAttempt: \[(.*)\] using containerId: \[(.*) on NM: \[(.*):(.*)\]", line)
            if searchFor:
                attemptId = searchFor.group(1)
                if attemptId in runningAttemptIdsReduce:
                    result.append((searchFor.group(1), searchFor.group(2), searchFor.group(3)))
        f.close()

        #result will be list of (attempt ID, container ID, host)
        #sample list element is
        #("attempt_1371368685287_0003_m_000002_0", "container_1371368685287_0003_01_000004", "ip-10-98-62-249")
        result = sorted(result, key=lambda row: row[0])
        return result

    # Returns AM container ID for given job ID or attemptID.
    # Returns None if jobID/attemptID is malformed.
    @classmethod
    @TaskReporter.report_test()
    def getAMContainerID(cls, jobIDOrAttemptID, amContainerNum="000001", attemptNum="01"):
        if jobIDOrAttemptID is None:
            return None
        #jobID = job_1379351803379_0009
        matchObj = re.search("job_(.*)", jobIDOrAttemptID)
        if matchObj is None:
            #attempt ID
            #attempt_1379351803379_0009_m_000001_0
            matchObj = re.search("attempt_(.*)_[mr]_.*", jobIDOrAttemptID)
            if matchObj is None:
                return None
        #Param is in good form.
        #AM container ID = container_1379351803379_0009_01_000001
        AMContainerId = "container_" + matchObj.group(1) + "_" + attemptNum + "_" + amContainerNum
        confirmed_AM = YARN.confirmContainerId(AMContainerId)
        logger.info("*** confirm AM Container ID ***")
        logger.info(confirmed_AM)
        return confirmed_AM

    # Get Application ID. The parameter is either an attempt ID or container ID.
    # Returns None is the pattern is malformed.
    @classmethod
    @TaskReporter.report_test()
    def getApplicationID(cls, attemptIDOrContainerID):
        justId = ""
        #attempt_1379351803379_0009_m_000001_0
        searchForJustId = re.search(r"attempt_(.*)_[mr]_(.*)", attemptIDOrContainerID)
        if searchForJustId is None:
            #container_1379351803379_0009_01_000001 or container_e10_1379351803379_0009_01_000001 (WPR enabled)
            searchForJustId = re.search(r"container_(\d{13}_\d{4})_\d{2}_\d{6}", attemptIDOrContainerID) or \
                              re.search(r"container_e[0-9]+_(\d{13}_\d{4})_\d{2}_\d{6}", attemptIDOrContainerID)
        if searchForJustId is None:
            return None
        if searchForJustId:
            justId = searchForJustId.group(1)
        #application_1379351803379_0009
        applicationID = "application_" + justId
        return applicationID

    # Returns string paths of (AM applogs syslog, AM applogs path)
    # Returns (None, None) if syslog file is not found
    @classmethod
    @TaskReporter.report_test()
    def getAMLogFile(cls, amhost, amContainerID, logoutput=True):
        #For instance, AM log is at /grid/0/mapred/application_1372551911973_0001/container
        # _1372551911973_0001_01_000001/syslog
        #or /grid/4/hdp/yarn/local/application_1379351803379_0026/container_1379351803379_0026_01_000001/syslog
        applicationID = cls.getApplicationID(amContainerID)
        #this is "mapreduce.cluster.local.dir" for hadoop1.
        appLogsPaths = YARN.getConfigValue("yarn.nodemanager.log-dirs").split(',')

        syslogPath = None
        amPath = None
        for path in appLogsPaths:
            amAppLogs = os.path.join(path, applicationID, amContainerID)
            findResult = Machine.find(
                Config.get('hadoop', 'YARN_USER'), amhost, amAppLogs, "syslog", None, logoutput=logoutput
            )
            findResult = util.prune_output(findResult, Machine.STRINGS_TO_IGNORE)
            if not (findResult == [''] or not findResult):
                syslogPath = os.path.join(path, applicationID, amContainerID, "syslog")
                amPath = path
                break
        if logoutput:
            logger.info("AM log file is at %s on %s", syslogPath, amhost)
        return (syslogPath, amPath)

    @classmethod
    @TaskReporter.report_test()
    def refreshTasktrackers(cls):
        _, output = YARN.runas(Config.get('hadoop', 'YARN_USER'), "rmadmin -refreshNodes")
        return output

    @classmethod
    @TaskReporter.report_test()
    def checkTasktrackerStateInJobtrackerUI(cls, node, state, msg="", find=True, timeout=90, interval=10):
        exptime = time.time() + timeout
        jtHttpAddress = cls.getJobtracker()
        states = {'active': '', 'excluded': 'lost'}
        urlout = util.getURLContents("http://%s:8088/cluster/nodes/%s" % (jtHttpAddress, states[state]))
        found = urlout.find(node) != -1 and (msg == "" or (msg != "" and urlout.find(msg) != -1))
        while time.time() < exptime and ((find and not found) or (not find and found)):
            logger.info("Sleeping for %d seconds", interval)
            time.sleep(interval)
            urlout = util.getURLContents("http://%s:8088/cluster/nodes/%s" % (jtHttpAddress, states[state]))
            found = urlout.find(node) != -1 and (msg == "" or (msg != "" and urlout.find(msg) != -1))
        return (find and found) or (not find and not found)

    @classmethod
    @TaskReporter.report_test()
    def isJobRunning(cls, jobID, user=HRT_QA):
        _, stdout = cls.runas(cmd="job -status %s" % jobID, user=user)
        return stdout.find("Job state: RUNNING") != -1

    @classmethod
    @TaskReporter.report_test()
    def isJobKilled(cls, jobID):
        _, stdout = cls.run("job -status %s" % jobID)
        return stdout.find("Job state: KILLED") != -1

    # this has been added for backwards compatablity. So the tests dont
    # have to write an if block to choose hadoop1 and hadoop2
    @classmethod
    def getJobtrackerAddress(cls):
        return YARN.getResourceManager().split(':')

    # Get path of RM log file.
    @classmethod
    def getJobTrackerLogFile(cls):
        return YARN.getRMLogFile()

    @classmethod
    @TaskReporter.report_test()
    def getTaskTrackerLogFile(cls, ttHost):
        '''
        Override
        Gets location of NM logs.
        Returns a string for full path of log.
        '''
        if Machine.isLinux():
            lines = Machine.find(
                user=Machine.getAdminUser(),
                host=ttHost,
                filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
                searchstr="hadoop-yarn-nodemanager-*.log",
                passwd=Machine.getAdminPasswd()
            )
        else:
            lines = Machine.find(
                user=Machine.getAdminUser(),
                host=ttHost,
                filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
                searchstr="yarn-nodemanager-*.log",
                passwd=Machine.getAdminPasswd()
            )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if lines:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerLogFile(cls, logoutput=False):
        '''
        Gets location of JHS logs.
        Returns a string for full path of log.
        '''
        HS_Log_Dir = Hadoop.getConfigFromHadoopEnv("HADOOP_MAPRED_LOG_DIR", defaultValue=None)
        if HS_Log_Dir:
            HS_Log_Dir = HS_Log_Dir.replace("$USER", "mapred")
        else:
            HS_Log_Dir = Config.get('hadoop', 'HADOOP_LOG_DIR')
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=cls.getHistoryserver(),
            filepath=HS_Log_Dir,
            searchstr="mapred-mapred-historyserver-*.log",
            passwd=Machine.getAdminPasswd(),
            logoutput=logoutput
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if not lines:
            return None
        else:
            return lines[0]

    @classmethod
    @TaskReporter.report_test()
    def getHistoryServerLogDir(cls):
        '''
        Gets MR JHS log dir with no ending slash.
        Returns a string.
        e.g. "/grid/0/var/log/hadoop/mapred"
        '''
        tmp = cls.getHistoryServerLogFile()
        if tmp is None:
            return None
        return util.getDir(tmp)

    # Get RM host and process ID of RM
    # Returns (RM Host, process ID)
    @classmethod
    @TaskReporter.report_test()
    def getRMHostProcessID(cls):
        rmHost = cls.getJobtracker()
        if Machine.isWindows():
            logger.info("Windows is likely to fail in this method. More test is needed.")
        procList = Machine.getProcessListRemote(rmHost, filter='ResourceManager')
        pID = procList[0].split()[1]
        return (rmHost, pID)

    # method to get yarn nodemanager principal
    # the reason its called master is so that the method
    # name can remain the same between YARN and MR
    @classmethod
    def getMasterPrincipal(cls, defaultValue=None):
        return YARN.getMasterPrincipal(defaultValue=defaultValue)

    @classmethod
    @TaskReporter.report_test()
    def copyJHSLogToLocalMachine(cls, logoutput=False):
        jhsLogFile = cls.getHistoryServerLogFile(logoutput)
        localJHSLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-jobhistoryserver-%s.log' % str(int(time.time()))
        )
        # copy the file to local machine
        Machine.copyToLocal(
            user=None, host=cls.getHistoryserver(), srcpath=jhsLogFile, destpath=localJHSLogFile, passwd=None
        )
        return localJHSLogFile


class YARN(object):
    _jmxPortResourceManager = 8006
    _activeRMwebAddress = None
    YARN_SITE_XML = "yarn-site.xml"
    CAPACITY_SCHEDULER_XML = "capacity-scheduler.xml"

    CLUSTER_MAX_APPLICATION_PROPERTY = "yarn.cluster.max-application-priority"
    DEFAULT_APPLICATION_PRIORITY = "default-application-priority"
    CAPACITY_SCHEDULER_ROOT = "yarn.scheduler.capacity.root"
    ACL_APPLICATION_MAX_PRIORITY = "acl_application_max_priority"
    ACL_ENABLE = "yarn.acl.enable"
    CAPACITY_SCHEDULER_ROOT_QUEUES = CAPACITY_SCHEDULER_ROOT + ".queues"

    CONFIG_NM_LOG_DIRS = "yarn.nodemanager.log-dirs"
    CONFIG_NM_LOCAL_DIRS = "yarn.nodemanager.local-dirs"
    HTTP_XML_HEADER = {
        "Accept": "application/xml",
    }
    DIAGNOSTICS = 'diagnostics'
    TIMEOUT = 'timeout'
    TIMEOUTS = TIMEOUT + 's'
    LIFETIME = 'LIFETIME'
    APPLICATION_KILLED_MESSAGE = \
        'Application is killed by ResourceManager as it has exceeded the lifetime period.'
    _rm_ha_nodes = []

    # ATS v2 urls
    ATS_V2_BASE_URL = '/ws/v2/timeline'
    MULTI_CLUSTER_URL = '/clusters/%s'
    FLOWS = '/flows'
    FLOW = FLOWS + '/%s'
    USERS = '/users'
    USER = USERS + '/%s'
    APPS = '/apps'
    APP = APPS + '/%s'
    RUNS = '/runs'
    RUN = RUNS + '/%s'
    ENTITIES = '/entities'
    ENTITY = ENTITIES + '/%s'
    ENTITY_TYPES = '/entity-types'

    FLOW_RUNS = USER + FLOW + RUNS
    FLOW_RUN = USER + FLOW + RUN
    APPS_FOR_A_FLOW = USER + FLOW + APPS
    APPS_FOR_A_FLOW_RUN = USER + FLOW + RUN + APPS
    GENERIC_ENTITIES_APP = APP + ENTITY
    GENERIC_ENTITIES = USER + ENTITY
    GENERIC_ENTITY_APP = APP + ENTITY + '/%s'
    GENERIC_ENTITY = USER + ENTITY + '/%s'
    GENERIC_ENTITY_TYPES = APP + ENTITY_TYPES

    # ATS v2 constants
    # Entity Types
    YARN_APPLICATION_ATTEMPT = 'YARN_APPLICATION_ATTEMPT'
    YARN_CONTAINER = 'YARN_CONTAINER'
    YARN_COMPONENT_INSTANCE = 'COMPONENT_INSTANCE'

    # MR Entity Types
    MAPREDUCE_JOB = 'MAPREDUCE_JOB'
    MAPREDUCE_TASK = 'MAPREDUCE_TASK'
    MAPREDUCE_TASK_ATTEMPT = 'MAPREDUCE_TASK_ATTEMPT'
    MAPREDUCE_ENTITY_TYPES = [
        MAPREDUCE_JOB, MAPREDUCE_TASK, MAPREDUCE_TASK_ATTEMPT, YARN_APPLICATION_ATTEMPT, YARN_CONTAINER
    ]

    # DS Entity Types
    DS_APP_ATTEMPT = 'DS_APP_ATTEMPT'
    DS_CONTAINER = 'DS_CONTAINER'
    DS_ENTITY_TYPES = [DS_APP_ATTEMPT, DS_CONTAINER, YARN_APPLICATION_ATTEMPT, YARN_CONTAINER]

    # YARN Metric types
    YARN_APPLICATION_CPU_PREEMPT_METRIC = 'YARN_APPLICATION_CPU_PREEMPT_METRIC'
    MEMORY = 'MEMORY'
    YARN_APPLICATION_CPU = 'YARN_APPLICATION_CPU'
    YARN_APPLICATION_RESOURCE_PREEMPTED_MEMORY = 'YARN_APPLICATION_RESOURCE_PREEMPTED_MEMORY'
    YARN_APPLICATION_MEM_PREEMPT_METRIC = 'YARN_APPLICATION_MEM_PREEMPT_METRIC'
    CPU = 'CPU'
    YARN_APPLICATION_MEMORY = 'YARN_APPLICATION_MEMORY'
    YARN_APPLICATION_NON_AM_CONTAINER_PREEMPTED = 'YARN_APPLICATION_NON_AM_CONTAINER_PREEMPTED'
    YARN_APPLICATION_AM_CONTAINER_PREEMPTED = 'YARN_APPLICATION_AM_CONTAINER_PREEMPTED'
    YARN_APPLICATION_RESOURCE_PREEMPTED_CPU = 'YARN_APPLICATION_RESOURCE_PREEMPTED_CPU'
    METRIC_TYPES = [
        YARN_APPLICATION_CPU_PREEMPT_METRIC, MEMORY, YARN_APPLICATION_CPU, YARN_APPLICATION_RESOURCE_PREEMPTED_MEMORY,
        YARN_APPLICATION_MEM_PREEMPT_METRIC, CPU, YARN_APPLICATION_MEMORY, YARN_APPLICATION_NON_AM_CONTAINER_PREEMPTED,
        YARN_APPLICATION_AM_CONTAINER_PREEMPTED, YARN_APPLICATION_RESOURCE_PREEMPTED_CPU
    ]

    YARN_APPLICATION = 'YARN_APPLICATION'
    YARN_APPLICATION_NAME = 'YARN_APPLICATION_NAME'
    YARN_APPLICATION_TYPE = 'YARN_APPLICATION_TYPE'
    YARN_APPLICATION_STATE = 'YARN_APPLICATION_STATE'
    YARN_APPLICATION_FINAL_STATUS = 'YARN_APPLICATION_FINAL_STATUS'
    YARN_APPLICATION_STATE_UPDATED = 'YARN_APPLICATION_STATE_UPDATED'
    YARN_APPLICATION_ATTEMPT_STATE = 'YARN_APPLICATION_ATTEMPT_STATE'
    YARN_APPLICATION_ATTEMPT_FINAL_STATUS = 'YARN_APPLICATION_ATTEMPT_FINAL_STATUS'
    YARN_APPLICATION_ATTEMPT_REGISTERED = 'YARN_APPLICATION_ATTEMPT_REGISTERED'
    YARN_APPLICATION_ATTEMPT_FINISHED = 'YARN_APPLICATION_ATTEMPT_FINISHED'
    YARN_APPLICATION_DIAGNOSTICS_INFO = 'YARN_APPLICATION_DIAGNOSTICS_INFO'
    YARN_APPLICATION_FINISHED = 'YARN_APPLICATION_FINISHED'
    YARN_CONTAINER_STATE = 'YARN_CONTAINER_STATE'
    MAPREDUCE = 'MAPREDUCE'
    DISTRIBUTED_SHELL = 'DistributedShell'
    DEFAULT_MR_FLOW_NAME = 'Sleep job'

    # Placement constants
    NODE = 'NODE'
    NODE_ANTI_AFFINITY = 'NOTIN'
    ALLOCATION_TAG_INFO = 'allocationTagInfo'
    ALLOCATION_TAGS = 'allocationTags'
    ALLOCATION_TAG = 'allocationTag'
    ALLOCATIONS_COUNT = 'allocationsCount'

    AUX_SERVICES = 'yarn.nodemanager.aux-services'
    TIMELINE_SERVICE_READ = 'yarn.timeline-service.read'
    YARN_ATS_USER = 'yarn-ats'
    YARN_HBASE_KEYTAB_FILE = 'yarn-ats.hbase-client.headless.keytab'

    # method to reset Resource Manager
    @classmethod
    @TaskReporter.report_test()
    def resetResourceManager(cls, action, config=None, host=None):
        if not host:
            host = cls.getResourceManagerHost()
        yarnHome = Config.get('hadoop', 'YARN_HOME').replace("client", "resourcemanager")
        libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
        Hadoop.resetService(
            Config.get('hadoop', 'YARN_USER'), host, "resourcemanager", action, config, yarnHome, "sbin",
            "yarn-daemon", libexecPath
        )

    @classmethod
    @TaskReporter.report_test()
    def restartResourceManager(cls, config=None, wait=5, getRMHostwithAmbari=True):
        if YARN.isHAEnabled():
            YARN.restartHARMNodes(config=config, getRMHostwithAmbari=getRMHostwithAmbari)
        else:
            cls.resetResourceManager('stop')
            time.sleep(wait)
            cls.resetResourceManager('start', config=config)
            time.sleep(wait)
            MAPRED.waitForNMToRegister(getRMHostwithAmbari=getRMHostwithAmbari)

    # method to kill the resource manager
    @classmethod
    @TaskReporter.report_test()
    def killResourceManager(cls, rmhost=None, suspend=False):
        if not rmhost:
            rmhost = cls.getResourceManagerHost()
        yarn_user = Config.get('hadoop', 'YARN_USER')
        service = 'resourcemanager'
        pidFile = None
        if not Machine.isWindows():
            pidFile = os.path.join(
                Config.get('hadoop', 'YARN_PID_DIR'), yarn_user, 'hadoop-%s-%s.pid' % (yarn_user, service)
            )

        Hadoop.killService(service, yarn_user, rmhost, pidFile=pidFile, suspend=suspend)

    # check if RM pid is present
    @classmethod
    @TaskReporter.report_test()
    def checkIfResourceManagerAlive(cls, rmhost=None):
        yarn_user = Config.get('hadoop', 'YARN_USER')
        service = 'resourcemanager'
        pidFile = os.path.join(
            Config.get('hadoop', 'YARN_PID_DIR'), yarn_user, 'hadoop-%s-%s.pid' % (yarn_user, service)
        )
        pid_cmd = "cat %s" % pidFile
        rms = []

        if not rmhost:
            if not YARN.isHAEnabled():
                rms.append(YARN.getResourceManagerHost())
            else:
                for rm in cls.getRMHANodes():
                    rms.append(rm)
        else:
            rms.append(rmhost)

        foundPID = False
        for rm in rms:
            _, pid = Machine.runas(yarn_user, pid_cmd, host=rm)
            existing_pids = Machine.getProcessListRemote(
                rm, format="%U %p %P %a", filter=service, user=None, logoutput=True
            )
            if not existing_pids:
                return False
            for pids in existing_pids:
                existing_pid = Machine.getPidFromString(pids, yarn_user)
                if pid == existing_pid:
                    foundPID = True
                    break
            if not foundPID:
                return False
        return True

    @classmethod
    @TaskReporter.report_test()
    def rmfailover_and_checkswitchedRM(cls, active_rm, standby_rm, config=None, sleeptime=30, hardCheck=True):
        # Force the RM Failover
        cls.restartActiveRM(wait=30, config=config)
        time.sleep(sleeptime)

        # validate RMs are switched
        new_active_rm = cls.getRMHostByState('active', retry=False)
        new_standby_rm = cls.getRMHostByState('standby', retry=False)
        if not hardCheck:
            # Altering validation condition as per BUG-43308
            # validating that both RM are up. and make sure active rm and standby rm are identical.
            assert new_active_rm != new_standby_rm
        elif hardCheck:
            assert new_active_rm == standby_rm
            assert new_standby_rm == active_rm

        # Validate both RM is running
        cls.checkIfResourceManagerAlive(rmhost=new_active_rm)
        cls.checkIfResourceManagerAlive(new_standby_rm)

        return new_active_rm, new_standby_rm

    # Gets property value from yarn-site.xml
    @classmethod
    @TaskReporter.report_test()
    def getConfigValue(cls, propertyValue, defaultValue=None, useAmbari=False, Type="yarn-site"):
        if useAmbari:
            yarnsite = Ambari.getConfig(type=Type)
            if yarnsite[propertyValue]:
                return yarnsite[propertyValue]
            else:
                return defaultValue
        else:
            value = util.getPropertyValueFromConfigXMLFile(
                os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "yarn-site.xml"),
                propertyValue,
                defaultValue=defaultValue
            )
            if Machine.isWindows() and Hadoop.isHadoop2() and HDFS.isASV():
                if re.match("file://.*", value):
                    value = util.convertFileURIToLocalPath(value, isWindows=True)
            return value

    # get Zk hosts from yarn-site.xml
    @classmethod
    @TaskReporter.report_test()
    def getZKhostsfromyarnsite(cls):
        val = cls.getConfigValue("hadoop.registry.zk.quorum")
        if val != None:
            zk_hosts = val.split(",")
            zk_host_list = []
            for zk in zk_hosts:
                zk_host_list.append(zk.split(":")[0])

            return zk_host_list
        return None

    @classmethod
    @TaskReporter.report_test()
    def getZKPortfromyarnsite(cls):
        val = cls.getConfigValue("hadoop.registry.zk.quorum")
        if val != None:
            zk_hosts = val.split(",")
            return zk_hosts[0].split(":")[1]

        return None

    # return True if Ats v1.5 is deployed
    @classmethod
    @TaskReporter.report_test()
    def isATSv_1_5(cls):
        val = cls.getConfigValue("yarn.timeline-service.version", "1.0")
        return val == "1.5"


    # Gets property value from yarn-env.sh
    @classmethod
    def getYarnEnvConfigValue(cls, propertyValue):
        return util.getPropertyValueFromBashFile(
            os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "yarn-env.sh"), propertyValue
        )

    # Gets property value from capacity-scheduler.xml
    @classmethod
    def getCapacityConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "capacity-scheduler.xml"),
            propertyValue,
            defaultValue=defaultValue
        )

    # Gets YARN_HOME (/usr/hdp/current/hadoop-yarn-client)
    @classmethod
    @TaskReporter.report_test()
    def getYarnHome(cls):
        if Machine.isLinux():
            return cls.getYarnEnvConfigValue("HADOOP_YARN_HOME")
        else:
            return Config.get('hadoop', 'HADOOP_YARN_HOME')

    @classmethod
    def getYarnUser(cls):
        '''
        Returns a string for yarn user.
        '''
        return Config.get('hadoop', 'YARN_USER')

    @classmethod
    def get_cluster_id(cls):
        """
        Return id of the YARN cluster
        """
        return cls.getConfigValue('yarn.resourcemanager.cluster-id')

    #Returns host:rpc-port of RM
    @classmethod
    @TaskReporter.report_test()
    def getResourceManager(cls):
        if cls.isHAEnabled():
            resourceManagerHostname = cls.getResourceManagerHost()
            resourceManagerPort = cls.getConfigValue("yarn.resourcemanager.address").split(":")[1]
            return "%s:%s" % (resourceManagerHostname, resourceManagerPort)
        resourceManagerHostname = cls.getConfigValue("yarn.resourcemanager.hostname")
        # check both yarn.resourcemanager.address and yarn.resourcemanager.hostname if the former is not found
        # default the port to 8032 as thats what it defaults to in yarn-default.xml
        if resourceManagerHostname != None:
            return cls.getConfigValue(
                "yarn.resourcemanager.address",
                cls.getConfigValue("yarn.resourcemanager.hostname") + ':8032'
            )
        else:
            return cls.getConfigValue("yarn.resourcemanager.address")

    #Returns host of RM
    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerHost(cls):
        # we should always return the Active RM if HA is enabled.
        if cls.isHAEnabled():
            return cls.getRMHostByState('active')
        else:
            resourcemngr = cls.getResourceManager()
            if resourcemngr.find(":") != -1:
                resourcemngr = resourcemngr.split(":")[0]
            return resourcemngr

    @classmethod
    @TaskReporter.report_test()
    def getRMSchedulerAddress(cls):
        rmHost = cls.getResourceManagerHost()
        return cls.getConfigValue('yarn.resourcemanager.scheduler.address', rmHost + ":8030")

    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerWebappAddress(cls, getRMHostwithAmbari=True):
        '''
          Returns YARN RM WebApp address based on if HTTPS is enabled or not.
          If HA is enabled return Active RM's webapp address.
          If https is available, returns "https://<RM-host>:<https-port>".
          Otherwise, returns "http://<RM-host>:<http-port>".
        '''
        propName = 'yarn.resourcemanager.webapp.address'
        defaultPort = '8088'
        scheme = 'http'
        if cls.isHttpsEnabled():
            propName = 'yarn.resourcemanager.webapp.https.address'
            defaultPort = '8090'
            scheme = 'https'
        if cls.isHAEnabled():
            ## check if previous RM is still active, if yes, return active rm url
            if cls._activeRMwebAddress:
                prev_active_rm = urlparse(cls._activeRMwebAddress).hostname
                curr_active_rm = cls.getRMHostByState('active', getRMHostwithAmbari=getRMHostwithAmbari)
                if Machine.isSameHost(prev_active_rm, curr_active_rm):
                    return cls._activeRMwebAddress

            activeRM = cls.getRMHostByState('active', getRMHostwithAmbari=getRMHostwithAmbari)
            # workaround for BUG-15419
            if activeRM is None or activeRM == '':
                time.sleep(10)
                activeRM = cls.getRMHostByState('active', tries=20, getRMHostwithAmbari=getRMHostwithAmbari)
            logger.info("activeRM: %s", activeRM)

            rmIds = cls.getConfigValue("yarn.resourcemanager.ha.rm-ids").split(",")
            for rmId in rmIds:
                webAppAddress = cls.getConfigValue('%s.%s' % (propName, rmId), '')
                # dont use config defaults as that will cause another call to be made to determine the active RM.
                if webAppAddress == '':
                    webAppAddress = '%s:%s' % (cls.getResourceManagerHost(), defaultPort)
                webAppAddress = '%s://%s' % (scheme, webAppAddress)
                logger.info("RMID : %s, webaddress: %s", rmId, webAppAddress)
                if activeRM is None or activeRM == '':
                    activeRM = cls.getRMHostByState('active', tries=20)
                if activeRM and activeRM in webAppAddress:
                    cls._activeRMwebAddress = webAppAddress
                    return webAppAddress
            return None
        else:
            return '%s://%s' % (
                scheme, cls.getConfigValue(propName, '%s:%s' % (cls.getResourceManagerHost(), defaultPort))
            )

    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerHttpAdd(cls):
        propName = 'yarn.resourcemanager.webapp.address'
        defaultPort = '8088'
        webAppAddress = cls.getConfigValue(propName, cls.getResourceManagerHost() + ":" + defaultPort)
        return webAppAddress

    #Returns web-port of RM webapp
    @classmethod
    def getResourceManagerWebappPort(cls):
        return cls.getResourceManagerHttpAdd().split(":")[1]

    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerPortByState(cls, state, protocol):
        '''
        Find Http/Https port for active/standy Resource Manager
        state = active or standby
        protocol = http or https
        '''
        Hostname = cls.getRMHostByState(state)
        rm_ids = cls.getConfigValue("yarn.resourcemanager.ha.rm-ids").split(",")
        for rm_id in rm_ids:
            rm_id = rm_id.strip()
            if protocol == "https":
                hostaddress = cls.getConfigValue(
                    'yarn.resourcemanager.webapp.https.address.%s' % (rm_id),
                    cls.getConfigValue("yarn.resourcemanager.hostname." + rm_id) + ':8090'
                )
            else:
                hostaddress = cls.getConfigValue(
                    'yarn.resourcemanager.webapp.address.%s' % (rm_id),
                    cls.getConfigValue("yarn.resourcemanager.hostname." + rm_id) + ':8088'
                )
            if hostaddress.find(Hostname) >= 0:
                port = hostaddress.split(":")[1]
                return port
        return None

    #Returns host:port of RM https webapp
    @classmethod
    @TaskReporter.report_test()
    def getResourceManagerWebappHttpsAddress(cls):
        if YARN.isHAEnabled():
            resourceManagerHostname = cls.getRMHostByState("active")
        else:
            resourceManagerHostname = cls.getConfigValue("yarn.resourcemanager.hostname")
        if resourceManagerHostname != None and not YARN.isHAEnabled():
            return cls.getConfigValue("yarn.resourcemanager.webapp.https.address", '%s:8090' % resourceManagerHostname)
        elif resourceManagerHostname != None and YARN.isHAEnabled():
            return "%s:%s" % (resourceManagerHostname, "8090")
        else:
            return cls.getConfigValue("yarn.resourcemanager.webapp.https.address")

    #Returns web-port of RM https webapp
    @classmethod
    def getResourceManagerWebappHttpsPort(cls):
        return cls.getResourceManagerWebappHttpsAddress().split(":")[1]

    #Returns host:rpc-port of NM
    @classmethod
    def getNodeManagerAddress(cls):
        return cls.getConfigValue("yarn.nodemanager.address")

    #Returns host:web-port of NM webapp
    @classmethod
    @TaskReporter.report_test()
    def getNodeManagerWebappAddress(cls):
        nodeManagerHostname = cls.getNodeManagerAddress().split(":")[0]
        if nodeManagerHostname != None:
            return cls.getConfigValue("yarn.nodemanager.webapp.address", nodeManagerHostname + ':8042')
        else:
            return cls.getConfigValue("yarn.nodemanager.webapp.address")

    #Returns web-port of NM webapp
    @classmethod
    def getNodeManagerWebappPort(cls):
        return cls.getNodeManagerWebappAddress().split(":")[1]

    @classmethod
    @TaskReporter.report_test()
    def getNodeManagerWebappHttpsPort(cls):
        nodeManagerHostname = cls.getNodeManagerAddress().split(":")[0]
        return cls.getConfigValue("yarn.nodemanager.webapp.https.address", nodeManagerHostname + ':8044').split(":")[1]

    @classmethod
    def getJMXPortResourceManager(cls):
        return cls._jmxPortResourceManager

    @classmethod
    @TaskReporter.report_test()
    def getLocalDirs(cls):
        localdirs = cls.getConfigValue("yarn.nodemanager.local-dirs")
        if localdirs:
            localdirs = localdirs.split(",")
        return localdirs

    @classmethod
    @TaskReporter.report_test()
    def getNMMemory(cls, config=None):
        propertyValue = "yarn.nodemanager.resource.memory-mb"
        defaultValue = 8192
        if not config:
            myconfig = Config.get('hadoop', 'HADOOP_CONF')
        else:
            myconfig = config
        return int(
            util.getPropertyValueFromConfigXMLFile(
                os.path.join(myconfig, "yarn-site.xml"), propertyValue, defaultValue=defaultValue
            )
        )

    # Run YARN CLI with given command with current user
    # Returns (exit_code, stdout).
    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True, host=None, stderr_as_stdout=True):
        """
        YARN.runas

        :param user:
        :param cmd:
        :param env:
        :param logoutput:
        :param host:
        :param stderr_as_stdout:
        :return:
        """
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        yarn_cmd = Config.get('hadoop', 'YARN_CMD') + " " + cmd
        if stderr_as_stdout:
            return Machine.runas(user, yarn_cmd, host=host, env=env, logoutput=logoutput)
        else:
            return Machine.runexas(user, yarn_cmd, host=host, env=env, logoutput=logoutput)

    #run command with current user
    # Returns (exit_code, stdout)
    @classmethod
    def run(cls, cmd, env=None, logoutput=True):
        return cls.runas(None, cmd, env=env, logoutput=logoutput)

    # Runs YARN CLI with given command in background as current user.
    # Returns subprocess.Popen object.
    @classmethod
    def runInBackground(cls, cmd, env=None, stdout=None, stderr=None):
        return cls.runInBackgroundAs(None, cmd, env=env, stdout=stdout, stderr=stderr)

    # Runs YARN CLI given command in background as specified user.
    # Returns subprocess.Popen object.
    @classmethod
    @TaskReporter.report_test()
    def runInBackgroundAs(cls, user, cmd, env=None, stdout=None, stderr=None):
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        yarn_cmd = Config.get('hadoop', 'YARN_CMD') + " " + cmd
        return Machine.runinbackgroundAs(user, yarn_cmd, env=env, stdout=stdout, stderr=stderr)

    # method to get yarn nodemanager principal
    # the reason its called master is so that the method
    # name can remain the same between YARN and MR
    @classmethod
    def getMasterPrincipal(cls, defaultValue=None):
        return cls.getConfigValue("yarn.resourcemanager.principal", defaultValue=defaultValue)

    # returns AM log of any application
    @classmethod
    @TaskReporter.report_test()
    def getAMlogFromAppID(cls, appID):
        appID = appID.replace("\r", "").replace("\n", "")
        containerId_tmp = "container_" + appID.split("application_")[1] + "_01_000001"
        containerId = cls.confirmContainerId(containerId_tmp)
        Am_host = MAPRED.getAMHostForContainerID(containerId)
        NM_port = cls.getConfigValue("yarn.nodemanager.address", "0.0.0.0:45454")
        NM_address = Am_host + ":" + NM_port.split(":")[1]
        Container_Log = cls.getLogsApplicationID(appID, None, NM_address, containerId, False, None)
        return Container_Log

    @classmethod
    @TaskReporter.report_test()
    def getAMHost(cls, appID, logoutput=True, containerId=None, attemptID=1):
        '''
        Use getApplicationMasterHost instead as that is simpler to use.
        '''
        if containerId:
            containerId = containerId
        else:
            attemptIdStr = YARN.createAttemptIdFromAppId(appID, attemptID)
            containerId = YARN.createContainerIdFromAttemptId(attemptIdStr, 1)
        logger.info("getAMHost for containerId %s", str(containerId))
        amHost = MAPRED.getAMHostForContainerID(containerId)
        if logoutput:
            logger.info("getAMHost returns %s", amHost)
        return amHost

    @classmethod
    @TaskReporter.report_test()
    def getApplicationMasterHost(cls, appID, user=None):
        '''
        Get the host on which ApplicationMaster is running.
        '''
        status = cls.getApplicationStatus(appID, user=user)
        amHost = ""
        for line in status:
            res = re.search(".*AM Host : (.*)", str(line))
            if res:
                amHost = res.group(1).strip()
                break
        logger.info("getAMHost returns %s", amHost)
        return amHost

    @classmethod
    @TaskReporter.report_test()
    def getAMPID(cls, appID, logoutput=True, containerId=None, yarnAppType='tez'):
        '''
        Returns AM PID.
        Now working only with Tez AM and distributed shell.
        Returns None if AMHost is not found.
        If containerId == None, presume first container. Otherwise, use container ID given as AM container ID.
        '''
        filterProcessName = "DAGAppMaster"
        if yarnAppType.lower() == 'tez':
            filterProcessName = "DAGAppMaster"
        elif yarnAppType == "MRAppMaster":
            filterProcessName = "MRAppMaster"
        elif yarnAppType == "DistributedShell":
            filterProcessName = "ApplicationMaster"
        elif yarnAppType == "spark":
            filterProcessName = "ApplicationMaster"
        elif yarnAppType == "yarnservice":
            filterProcessName = containerId + "/launch_container.sh"

        logger.info("Looking for AM with process name: %s", filterProcessName)
        if Machine.isWindows():
            amHost = YARN.getAMHost(appID, logoutput, containerId)
            amHost = util.getShortHostname(amHost)
            if amHost is None:
                return None
            amPID = Machine.getProcessListWithPid(
                amHost, "java.exe", filterProcessName, logoutput=True, ignorecase=True
            )
            if logoutput:
                logger.info("getAMPID PID = %s", str(amPID))
            return amPID
        else:
            amHost = YARN.getAMHost(appID, logoutput, containerId)
            if amHost is None:
                return None
            procListLines = Machine.getProcessListRemote(amHost, filter=filterProcessName, logoutput=True)
            if not procListLines:
                return None
            for line in procListLines:
                if line.find("grep") == -1 and line.find("/bin/bash") == -1:
                    if logoutput:
                        logger.info("getAMPID matching line = %s", line)
                        logger.info("getAMPID returns %s", line.split()[1])
                    return line.split()[1]
            if logoutput:
                logger.info("getAMPID returns None")
            return None

    @classmethod
    @TaskReporter.report_test()
    def getContainerPID(cls, containerID, filterProcessName):
        '''
        Returns Non-AM container PID for the given containerID
        filterProcessName: the process name keyword to filter with.
        Verified working with distributed shell to find the task container process.
         e.g. to find the 'sleep' process ID, pass in "/bin/bash -c sleep"
        Returns None if the container process is not found.
        '''
        host = MAPRED2.getContainerHost(containerID)
        if host is None:
            logger.info("No host found")
            return None
        logger.info("getContainerPID host = %s", host)
        if Machine.isWindows():
            containerPID = Machine.getProcessListWithPid(
                host, filterProcessName[0], filterProcessName[1], logoutput=True, ignorecase=True
            )
            logger.info("getContainerPID PID = %s", str(containerPID))
            assert containerPID != None
            return containerPID
        else:
            procListLines = Machine.getProcessListRemote(host, filter=filterProcessName, logoutput=True)
            if not procListLines:
                return None
            for line in procListLines:
                # do not check "/bin/bash"
                if line.find("grep") == -1:
                    logger.info("getContainerPID matching line = %s", line)
                    logger.info("getContainerPID returns %s", line.split()[1])
                    return line.split()[1]
            return None

    @classmethod
    @TaskReporter.report_test()
    def killProcess(cls, PID, host):
        if PID is None or re.match(r"^\d+$", PID) is None:
            logger.warn("Got Empty/Invalid PID %s ", str(PID))
            return None
        if Machine.isWindows():
            return Machine.killProcessRemotePsexec(int(PID), host, True)
        else:
            return Machine.killProcessRemote(int(PID), host, ADMIN_USER, ADMIN_PWD, True)

    @classmethod
    @TaskReporter.report_test()
    def getAMcontainerId(cls, appID, attemptNum):
        '''
        Get container Id for AM as per attempt number
        QE-5878 : AM can be started in any containers. Thus, Rest api should be used to find out AM container.
        '''
        url = "%s/ws/v1/cluster/apps/%s/appattempts" % (cls.getResourceManagerWebappAddress(), appID)
        appData = util.getJSONContent(url)
        return appData["appAttempts"]["appAttempt"][int(attemptNum) - 1]["containerId"]

    @classmethod
    @TaskReporter.report_test()
    def killAMProcess(cls, appID, attemptNum, filterAMName):
        '''
        Kill the AM process.
        appID: applicationID of the AM
        attemptNum: the attempt number of the attempt.
        filterAMName: the AM process name to filter with.
        '''
        containerId = YARN.getAMcontainerId(appID, attemptNum)
        amPID = cls.getAMPID(appID, True, containerId, filterAMName)
        amHost = cls.getAMHost(appID, True, containerId, attemptNum)
        if amPID:
            logger.info("Kill AM Process " + amPID + " on host " + amHost)
            return cls.killProcess(amPID, amHost)
        else:
            logger.info("cant find amPID")
            return 1, ''

    @classmethod
    @TaskReporter.report_test()
    def waitForAMProcessToKill(cls, appID, attemptNum, filterAMName):
        '''
        Kill the AM process.
        appID: applicationID of the AM
        attemptNum: the attempt number of the attempt.
        filterAMName: the AM process name to filter with.
        '''
        containerId = YARN.getAMcontainerId(appID, attemptNum)
        amPID = cls.getAMPID(appID, True, containerId, filterAMName)
        amHost = cls.getAMHost(appID, True, containerId, attemptNum)
        logger.info("amPID: %s , amHost: %s", amPID, amHost)
        startTime = time.time()
        while (not amPID and time.time() - startTime <= 30):
            amPID = cls.getAMPID(appID, True, containerId, filterAMName)
            attemptID2 = YARN.createAttemptIdFromAppId(appID, attemptNum)
            state = YARN.getAppAttemptState(attemptID2)
            logger.info(
                "Wait For AMPID for to be killed appID=%s, attemptNum=%s, state=%s, amPid=%s",
                appID, attemptNum, state, amPID
            )
            time.sleep(2)
        logger.info("Kill AM Process " + amPID + " on host " + amHost)
        return cls.killProcess(amPID, amHost)

    @classmethod
    @TaskReporter.report_test()
    def killContainerProcess(cls, containerID, filterProcessName):
        '''
        Kill the contaienr process.
        filterProcessName: the process name keyword to filter with.
        '''
        containerPID = cls.getContainerPID(containerID, filterProcessName)
        containerHost = MAPRED2.getContainerHost(containerID)
        logger.info("Kill Task Process " + str(containerPID) + " on host " + str(containerHost))
        return cls.killProcess(containerPID, containerHost)

    @classmethod
    @TaskReporter.report_test()
    def getFailedKilledAppList(cls):
        '''
        Return list of Failed/Killed Apps in RM
        '''
        list_killed = cls.getApplicationIDList(state="ALL", appID=None, user=None, finalStatus="KILLED")
        list_failed = cls.getApplicationIDList(state="ALL", appID=None, user=None, finalStatus="FAILED")
        final_list = list(set(list_killed + list_failed))
        return final_list

    @classmethod
    @TaskReporter.report_test()
    def getQueueForApp(cls, appId):
        '''
        get Queue for AppId
        returns queue name such as default, joblauncher
        '''
        status_out = cls.getApplicationStatus(appId)
        p = "Queue : (.*)"
        m = re.search(p, status_out[1])
        if m:
            return m.group(1)
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getApplicationIDList(
            cls, state="ALL", appID=None, user=None, finalStatus=None, apptype=None, searchString=None
    ):
        '''
        Returns list of application ID which matches given state and application ID and finalStatus.
        Each returned element is an applicationID.
        Returns [] if nothing fulfills given conditions.
        Filtering is done in AND manner. Can query many states at once with comma-separated states
         according to YARN CLI spec.
        By default, this method asks for ALL applications.
        e.g. ['application_1374638600275_0002', 'application_1374638600275_0001']
        '''
        cmd = "application -list -appStates %s" % state
        if apptype != None:
            cmd = cmd + " -appTypes %s" % apptype
        if searchString != None:
            cmd = cmd + " | grep " + searchString
        if user is None:
            _, stdout = cls.run(cmd, None, True)
        else:
            _, stdout = cls.runas(user, cmd, env=None, logoutput=True, host=None)
        stdout = cls._filterApplicationListOutput(stdout, targetAppID=appID, targetFinalStatus=finalStatus)
        return re.findall("(^application_[0-9_]+)", stdout, re.MULTILINE)

    # Returns complete list of YARN applications which matches given state and application ID.
    # Each returned element is a line from standard output.
    # Returns [] if nothing fulfills given conditions.
    # Filtering is done in AND manner. Can query many states at once with comma-separated states
    #  according to YARN CLI spec.
    # By default, this method asks for ALL applications.
    # e.g. ['application_1374638600275_0002               Sleep job               MAPREDUCE        hrt_qa
    #        default              FINISHED                KILLED               100%
    #     hor11n17.gq1.ygridcore.net:19888/jobhistory/job/job_1374638600275_0002']
    # Header is "Application-Id        Application-Name        Application-Type          User         Queue
    #                  State           Final-State           Progress                           Tracking-URL"
    @classmethod
    @TaskReporter.report_test()
    def getApplicationList(cls, state="ALL", appID=None, config=None, user=None):
        '''
        Gets List of Applications for specified state.
        :param state: Get application list for specified state. Default=ALL
        :param appID: Look specified application Id only. Default=None
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: list application ids found
        '''
        if config is None:
            cmd = "application -list -appStates %s" % state
        else:
            cmd = "--config " + config + " application -list -appStates %s" % state
        if user is None:
            _, stdout = cls.run(cmd, None, True)
        else:
            _, stdout = cls.runas(user, cmd, None, True)
        stdout = cls._filterApplicationListOutput(stdout, targetAppID=appID)
        result = re.findall("(^application_[0-9_]+.*)", stdout, re.MULTILINE)
        logger.info("YARN getApplicationList returns:")
        for line in result:
            logger.info(line)
        return result

    @classmethod
    @TaskReporter.report_test()
    def getAppIDFromAppName(cls, appName, state="ALL", config=None, user=None):
        '''
        Find applicationId by using application Name
        :param appName: Application name such as Sleep/myStream
        :param state: The app State to look for such as Running
        :param config: Configuration
        :param user: User who started job
        :returns: ApplicationId (application_1234567891_0001). If appId is not found, returns None
        '''
        listapp = YARN.getApplicationList(state=state, appID=None, config=config, user=user)
        for app in listapp:
            if re.search(appName, app):
                return re.search("application_[0-9]*_[0-9]*", app).group(0)
        return None

    @classmethod
    def getAppUserFromAppID(cls, appID):
        '''
        Find appUser from AppId
        '''
        appInfo = cls.getApplicationInfo(appID, user=cls.getYarnUser())
        return appInfo['user']

    @classmethod
    @TaskReporter.report_test()
    def getNumOfRunningJobs(cls):
        '''
        Get count of Running Jobs
        '''
        if Hadoop.isHadoop2():
            apps = []
            # get all the apps that could be in a non finished state
            apps = YARN.getApplicationIDList(state='NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING')
            return len(apps)
        else:
            cmd = 'job -list'
            exit_code, stdout = Hadoop.run(cmd)
            assert exit_code == 0, 'job -list command failed'
            jobs = []
            for line in re.finditer("job_([0-9]*_[0-9]*)", stdout):
                if line.group(1):
                    jobs.append('job_%s' % (line.group(1)))
            return len(jobs)

    @classmethod
    @TaskReporter.report_test()
    def waitForZeroRunningApps(cls, wait_time=25, timeout=1200):
        running_jobs = cls.getNumOfRunningJobs()
        t = 0
        while t < timeout:
            running_jobs = cls.getNumOfRunningJobs()
            if running_jobs > 0:
                logger.info("Waiting for %s seconds to finish all background jobs", wait_time)
                time.sleep(wait_time)
                t = t + wait_time
            else:
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getAppAttemptList(cls, appID, config=None, user=None):
        '''
        Gets Application Attempt IDs List for specifed Applciation Id.
        :param appID: Application Id for which we want find Application Attempts
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: exit code and list Application Attempts IDs ran under specified Application ID
        '''
        if config is None:
            cmd = "applicationattempt -list %s" % appID
        else:
            cmd = "--config " + config + " applicationattempt -list %s" % appID
        if user is None:
            (exit_code, stdout) = cls.run(cmd, None, True)
        else:
            (exit_code, stdout) = cls.runas(user, cmd, None, True)
        result = re.findall("(^appattempt_[0-9_]+)", stdout, re.MULTILINE)
        logger.info("YARN getAppAttemptList returns:")
        for line in result:
            logger.info(line)
        return exit_code, result

    @classmethod
    @TaskReporter.report_test()
    def getAMContainerIdFromattempt(cls, appID, attemptId, user=None):
        """
        Find AM container Id using application attempt cli
        :param appID:
        :param attemptId:
        :param user:
        :return:
        """
        cmd = "applicationattempt -list %s" % appID
        (exit_code, stdout) = cls.runas(user, cmd, None, True)
        assert exit_code == 0, "applicationattempt list cli failed for %s" % appID
        for line in stdout.split("\n"):
            if line.find(attemptId) >= 0:
                m = re.search(r'.*(container_\d+_\d+_\d+_\d+).*', line) or \
                    re.search(r'.*(container_e[0-9]+_\d+_\d+_\d+_\d+).*', line)
                containerid = None
                if m and m.group(1):
                    containerid = m.group(1)
                return containerid
        return None

    @classmethod
    @TaskReporter.report_test()
    def getContainerList(cls, appAttemptID, config=None, user=None):
        '''
        Gets Container List for specified Application Attempt ID.
        :param appAttemptID: Application Attempt ID for which we want get container list
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: exit code and list of Container IDs ran under specified Application Attempt ID
        '''
        if config is None:
            cmd = "container -list %s" % appAttemptID
        else:
            cmd = "--config " + config + " container -list %s" % appAttemptID
        if user is None:
            (exit_code, stdout) = cls.run(cmd, None, True)
        else:
            (exit_code, stdout) = cls.runas(user, cmd, None, True)

        result = re.findall("(^container_[0-9_]+)", stdout, re.MULTILINE) or \
                 re.findall("(^container_e[0-9]+_[0-9_]+)", stdout, re.MULTILINE)
        logger.info("YARN getContainerList returns:")
        for line in result:
            logger.info(line)
        return exit_code, result

    @classmethod
    @TaskReporter.report_test()
    def getNumRunningContainersForAttempt(cls, attemptID, user=None):
        '''
        Return the number of currently running containers for the given attempt.
        '''
        cmd = "container -list %s" % attemptID
        if user is None:
            _, stdout = cls.run(cmd, None, True)
        else:
            _, stdout = cls.runas(user, cmd, None, True)
        count = 0
        for line in stdout.split("\n"):
            if re.search("^container_.*RUNNING.*", line) is not None:
                count = count + 1
        return count

    @classmethod
    @TaskReporter.report_test()
    def createAttemptIdFromAppId(cls, appId, attemptNum):
        '''
        Create the attemptID based on the appID and the attemptNum integer
        '''
        attemptId = appId.replace("application", "appattempt") + "_" + str(attemptNum).zfill(6)
        logger.info("New attemptId:  %s", attemptId)
        return attemptId

    @classmethod
    @TaskReporter.report_test()
    def createAppIdFromAttemptId(cls, attemptId):
        '''
        Create AppId based on attemptId
        :param attemptId: appattempt_1428685307383_0007_000001
        :return: application_1428685307383_0007
        '''
        tempVar = attemptId.replace("appattempt", "application")
        appId = tempVar.rsplit("_", 1)[0]
        return appId

    @classmethod
    @TaskReporter.report_test()
    def createRegExForContainerWPR(cls, containerId):
        '''
        Create regex for the container Id when WPR is enabled
        :param containerId: container_1428619004728_0003_01_000001
        :return: container_e[0-9]+_1428619004728_0003_01_000001
        '''
        tempVar = containerId.split("_")
        tempVar.insert(1, 'e[0-9]+')
        contId = "_".join(tempVar)
        return contId

    @classmethod
    @TaskReporter.report_test()
    def getAppIdFromContainerId(cls, containerId):
        '''
        Get AppId from container ID like container_e05_1428619004728_0003_01_000001 or
        container_1428619004728_0003_01_000001
        :param containerId: containerId
        :return: Application ID such as application_1428619004728_0003
        '''
        p = "container_.*([0-9]{13}_[0-9]{4})_*"
        m = re.search(p, containerId)
        if m:
            app_no = str(m.group(1))
            return "application_" + app_no
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def confirmContainerId(cls, containerId):
        '''
        Confirm container Id (The actual containerId can be different due to WPR enabled.)
        :param containerId: The expected containerId
        :return: Actual container Id
        '''
        appId = cls.getAppIdFromContainerId(containerId)
        logger.info("*** appId in confirmContainer ***")
        logger.info(appId)
        amContainer = YARN.getAMContainerIdFromAppAttempt(appId)
        lenContainerIdParts = len(amContainer.split("_"))
        if lenContainerIdParts == 5:
            logger.info("Confirmed containerId: %s", containerId)
            return containerId
        else:
            newContainerId = "_".join(amContainer.split("_")[0:2] + containerId.split("_")[1:])
            logger.info("Confirmed containerId: %s", newContainerId)
            return newContainerId

    @classmethod
    @TaskReporter.report_test()
    def createContainerIdFromAttemptId(cls, attemptID, containerNum):
        '''
        Create the containerID based on the attemptID and the containerNum integer
        '''
        lists = attemptID.split('_')
        formattedAttemptId = lists[3].lstrip('0').zfill(2)
        containerId = "container_" + lists[1] + '_' + lists[2] + '_' + formattedAttemptId + '_' + str(containerNum).\
            zfill(6)
        logger.info("containerId without WPR: %s", containerId)
        return cls.confirmContainerId(containerId)

    @classmethod
    @TaskReporter.report_test()
    def waitForNumRunningContainers(cls, attemptID, numContainers, timeout=30, interval=5, user=None):
        '''
        Wait until the attempt reaches the specified number of running containers.
        '''
        starttime = time.time()
        count = YARN.getNumRunningContainersForAttempt(attemptID, user)
        while (time.time() - starttime) < timeout and count < numContainers:
            logger.info("Wait for %s RUNNING containers, currently has %s RUNNING containers.", numContainers, count)
            time.sleep(interval)
            count = YARN.getNumRunningContainersForAttempt(attemptID, user)
        assert count >= numContainers
        return count

    @classmethod
    @TaskReporter.report_test()
    def getNumAttemptsForApp(cls, appID):
        '''
        Return the number of attempts for the given application.
        '''
        cmd = "applicationattempt -list %s" % appID
        _, stdout = cls.run(cmd, None, True)
        count = 0
        for line in stdout.split("\n"):
            if re.search("^appattempt_", line) is not None:
                count = count + 1
        return count

    @classmethod
    @TaskReporter.report_test()
    def getApplicationIDFromStdout(cls, stdout, logoutput=False):
        '''
        Gets YARN application ID from client stdout.
        Returns a string of application ID.
        Returns None if pattern is not found.
        '''
        # Pylint changes for unused-argument
        logger.info(logoutput)
        matchObj = re.search("YarnClientImpl: Submitted application (.*)", stdout)
        if matchObj is None:
            return None
        else:
            return matchObj.group(1)

    @classmethod
    @TaskReporter.report_test()
    def getApplicationIDFromFile(cls, filename, logoutput=False):
        '''
        Gets YARN application ID from client stdout file.
        Returns a string of application ID.
        Returns None if pattern is not found.
        '''
        f = open(filename)
        text = f.read()
        f.close()
        return cls.getApplicationIDFromStdout(text, logoutput)

    # public method to call cls._filterApplicationListOutput function.
    # This is introduced because of unplanned need.
    @classmethod
    @TaskReporter.report_test()
    def getFilterApplicationListOutput(cls, stdout, state=None, appID=None):
        return cls._filterApplicationListOutput(stdout, targetState=state, targetAppID=appID)

    @classmethod
    @TaskReporter.report_test()
    def _filterApplicationListOutput(cls, sourceStdout, targetState=None, targetAppID=None, targetFinalStatus=None):
        '''
        Given standard output from yarn application list command, do filter and return only lines
        with target state and target app ID (if a target is not None) and target final-status.
        If target state is None, it means target state is not taken into account. This is vice-versa for targetAppID.
        If no such lines are found, returns "".
        '''
        tempResult = []
        for line in sourceStdout.split("\n"):
            if re.search("^application_", line) is not None:
                rowFilterOK = True
                if targetState is not None and cls.getAppState(line) != targetState:
                    rowFilterOK = False
                if targetAppID is not None and cls.getAppID(line) != targetAppID:
                    rowFilterOK = False
                #final-status can implement better if final-status filter is added. (BUG-18793/YARN-2150)
                if targetFinalStatus is not None and cls.getAppFinalState(line) != targetFinalStatus:
                    rowFilterOK = False
                if rowFilterOK:
                    tempResult.append(line)
            else:
                tempResult.append(line)
        return '\n'.join(tempResult)

    # Given a line from yarn application list, get application ID.
    @classmethod
    def getAppID(cls, appListLine):
        return appListLine.split("\t")[0].strip()

    # Given a line from yarn application attempt list, get application attempt ID.
    @classmethod
    def getAppAttemptID(cls, appAttemptListLine):
        '''
        Returns parsed Application Attempt ID from Application Attempt list status line
        :param appAttemptListLine: Line containing application attempt report on Yarn ClI yarn applicationattempt
        -list <Application Id>
        :return: Parsed Application Attempt ID
        '''
        return appAttemptListLine.split("\t")[0].strip()

    @classmethod
    @TaskReporter.report_test()
    def getAttemptIdList(cls, appID, user=None):
        '''
        Returns parsed application attempt id list from application id.
        :param appID: Application Id
        :return: Application attempt id list
        '''
        attemptIdList = []
        _, stdout = cls.getAppAttemptList(appID, user=user)
        for attemptLine in stdout:
            attemptIdList.append(cls.getAppAttemptID(attemptLine))

        return attemptIdList

    # Given a line from yarn container list, get container ID.
    @classmethod
    def getContainerID(cls, containerListLine):
        '''
        Returns parsed Container ID from Container list status list
        :param containerListLine: Line containing report on Yarn ClI yarn container -list <Application Attempt Id>
        :return: Parsed Container ID
        '''
        return containerListLine.split("\t")[0].strip()

    # Given a line from yarn application list, get application state.
    @classmethod
    def getAppState(cls, appListLine):
        return appListLine.split("\t")[5].strip()

    # Get application state from application ID
    # Returns None if not found
    @classmethod
    @TaskReporter.report_test()
    def getAppStateFromID(cls, appID, user=None):
        (_, status) = cls.getApplicationStatus(appID, user=user)
        matchObj = re.search("State : (.*)$", status, re.MULTILINE)
        #Final-State is also matched at next element.
        if matchObj is None:
            return None
        else:
            return matchObj.group(1).rstrip()

    @classmethod
    @TaskReporter.report_test()
    def getAppAttemptState(cls, attemptID):
        '''
        Get application attempt state for given  attempt ID
        Returns None if not found
        '''
        (_, status) = cls.getAppAttemptStatus(attemptID)
        matchObj = re.search("State : (.*)$", status, re.MULTILINE)
        #Final-State is also matched at next element.
        if matchObj is None:
            return None
        else:
            return matchObj.group(1).rstrip()

    # Given a line from yarn application list, get application final state.
    @classmethod
    def getAppFinalState(cls, appListLine):
        return appListLine.split("\t")[6].strip()

    # Get application final state from application ID
    # Returns None if not found
    @classmethod
    @TaskReporter.report_test()
    def getAppFinalStateFromID(cls, appID, user=None):
        (_, status) = cls.getApplicationStatus(appID, user=user)
        matchObj = re.search("Final-State : (.*)$", status, re.MULTILINE)
        if matchObj is None:
            return None
        else:
            return matchObj.group(1).rstrip()

    @classmethod
    @TaskReporter.report_test()
    def killApplication(cls, appID):
        '''
        Kills an application with YARN CLI with current user.
        Returns (exit_code, stdout).
        '''
        cmd = "application -kill %s" % appID
        return cls.run(cmd, None, True)

    @classmethod
    @TaskReporter.report_test()
    def killApplicationAs(cls, appID, user=None):
        '''
        Kills an application with YARN CLI as selected user.
        Returns (exit_code, stdout).
        '''
        cmd = "application -kill %s" % appID
        return cls.runas(user=user, cmd=cmd, env=None, logoutput=True, host=None)

    @classmethod
    @TaskReporter.report_test()
    def killAllApplications(cls, useYarnUser=True, state=None):
        '''
        Kills all applications with YARN CLI with current user or yarn user.
        Returns True if no running application is found after killing.
        '''
        if useYarnUser:
            yarnCLIUser = cls.getYarnUser()
        else:
            yarnCLIUser = None
        if state:
            appIDList = cls.getApplicationIDList(state=state, user=yarnCLIUser)
        else:
            appIDList = cls.getApplicationIDList(state="NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING", user=yarnCLIUser)
        if appIDList is not None:
            for appID in appIDList:
                if useYarnUser:
                    cls.killApplicationAs(appID, user=yarnCLIUser)
                else:
                    cls.killApplication(appID)
        retryCount = 0
        maxRetryCount = 10
        while retryCount < maxRetryCount:
            if cls.getApplicationIDList(state="NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING", user=yarnCLIUser):
                logger.info("There are still some applications. Will sleep another 5 seconds")
                logger.info("Sleeping for 5 seconds for applications to be killed")
                time.sleep(5)
            else:
                logger.info("There are no more application")
                return True
            retryCount += 1
        return False

    @classmethod
    @TaskReporter.report_test()
    def killApplicationExceptOwner(cls, protect_owner_list=None):
        '''
        This function will keep the apps alive from protect_owner_list and kill rest of the running/accepted apps
        :param appuser:
        :return:
        '''
        if not protect_owner_list:
            protect_owner_list = []
        appIDList = cls.getApplicationIDList(state="NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING", user=cls.getYarnUser())
        if appIDList is not None:
            for appId in appIDList:
                app_owner = cls.getAppUserFromAppID(appId)
                if not app_owner in protect_owner_list:
                    cls.killApplication(appId)

    # Wait until an app ID is available.
    # Returns app ID of first-found app with target state or None otherwise.
    @classmethod
    @TaskReporter.report_test()
    def waitForAppId(cls, timeout=30, interval=5, targetState="RUNNING", user=None):
        if Machine.isWindows():
            timeout = timeout * 2
        starttime = time.time()
        appList = cls.getApplicationList(targetState, user=user)
        while (time.time() - starttime) < timeout and not appList:
            logger.info("Sleeping for %d seconds for app ID to become available", interval)
            time.sleep(interval)
            appList = cls.getApplicationList(targetState, user=user)
        if appList:
            return cls.getAppID(appList[0])
        return None

    @classmethod
    @TaskReporter.report_test()
    def waitForAppIdFromQueue(cls, queue, timeout=30, interval=5):
        appID = None
        starttime = time.time()
        while ((time.time() - starttime) < timeout) and not appID:
            try:
                logger.info("Waiting for AppID")
                appID = queue.get(block=False)
            except Queue.Empty:
                time.sleep(interval)
        assert appID is not None, "AppID returned None"
        return appID

    @classmethod
    @TaskReporter.report_test()
    def waitForAppState(cls, appID, timeout=30, interval=5, targetState="FINISHED", logoutput=False, user=None):
        '''
        Wait until the application reaches target state (FINISHED/RUNNING).
        Returns True if the app finishes within timeout.
        '''
        if Machine.isWindows():
            timeout = timeout * 2
        starttime = time.time()
        state = cls.getAppStateFromID(appID, user=user)
        while (time.time() - starttime) < timeout and (state is None or state != targetState):
            logger.info("Sleeping for %d seconds for app ID to reach %s state", interval, targetState)
            time.sleep(interval)
            state = cls.getAppStateFromID(appID, user=user)
            if logoutput:
                logger.info("YARN.waitForAppState state = %s", state)
            if state in ["KILLED", "FAILED"]:
                if logoutput:
                    logger.info("YARN.waitForAppState returns False")
                return False
        if state == targetState:
            if logoutput:
                logger.info("YARN.waitForAppState returns True")
            return True
        if logoutput:
            logger.info("YARN.waitForAppState returns False. Target state is not reachable within time limit.")
        return False

    @classmethod
    @TaskReporter.report_test()
    def waitForAttemptState(cls, attempID, targetState, timeout=30, interval=5):
        '''
        Wait until the application attempt reaches target state (FINISHED/RUNNING).
        Returns True if the app finishes within timeout.
        '''
        if Machine.isWindows():
            timeout = timeout * 2
        starttime = time.time()
        state = cls.getAppAttemptState(attempID)
        while (time.time() - starttime) < timeout and (state is None or state != targetState):
            logger.info("Sleeping for %d seconds for attempt %s to reach %s state", interval, attempID, targetState)
            time.sleep(interval)
            state = cls.getAppAttemptState(attempID)
            if state == targetState:
                return True
        if state == targetState:
            return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def waitForApplicationFinish(cls, appid, timeout=60, user=None):
        '''
        Wait for AppId to finish
        '''
        i = 0
        while i < timeout:
            finish_app_list = YARN.getApplicationList("FINISHED", appid, user=user)
            logger.info(finish_app_list)
            if finish_app_list:
                return True
            else:
                time.sleep(5)
            i = i + 1
        return False

    @classmethod
    @TaskReporter.report_test()
    def waitForAppProgress(cls, appID, progress, timeout):
        '''
        Wait for application progress
        '''
        startTime = time.time()
        while time.time() - startTime < timeout:
            status = cls.getApplicationStatus(appID, False)
            res = re.search(".*Progress : (.*)%.*", str(status))
            if res:
                percentage = float(res.group(1))
                if percentage >= progress:
                    return True
            logger.info("Waiting for %s to reach %s, currently %s", appID, progress, float(res.group(1)))
            time.sleep(3)
        return False

    @classmethod
    @TaskReporter.report_test()
    def getApplicationStatus(cls, appID, logOutput=True, config=None, user=None):
        '''
        Get application status with YARN CLI
        Return (exit_code, stdout)
        :param appID: Application ID to get status of Application
        :param logOutput: Whether to print output of YARN CLIN  using logger
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: (exit_code, stdout) from YARN CLI of yarn application -status appID

        '''
        if config is None:
            cmd = "application -status %s" % appID
        else:
            cmd = "--config " + config + " application -status %s" % appID
        if user is None:
            return cls.run(cmd, None, logOutput)
        else:
            return cls.runas(user, cmd, None, logOutput)

    @classmethod
    @TaskReporter.report_test()
    def getLogAggregationStatus(cls, appId):
        '''
        Get application Log Aggregation Status
        '''
        _, stdout = cls.getApplicationStatus(appId)
        return re.search("Log Aggregation Status : SUCCEEDED", stdout)


    @classmethod
    @TaskReporter.report_test()
    def isLogAggregationTimedOut(cls, appId):
        '''
        Check if log aggregation is Timed out
        :param appId:
        :return:
        '''
        msg = "Log Aggregation Status : TIME_OUT"
        _, stdout = cls.getApplicationStatus(appId)
        return re.search(msg, stdout)


    @classmethod
    @TaskReporter.report_test()
    def waitForLogAggregationToFinish(cls, appId, timeout=120):
        '''
        wait till Log Aggregation status is SUCCEEDED
        '''
        if Machine.isWindows():
            timeout = timeout * 4
        startTime = time.time()
        while time.time() - startTime < timeout:
            r = cls.getLogAggregationStatus(appId)
            if r:
                return r
            if time.time() - startTime + 5 > timeout:
                break
            time.sleep(5)
        return None

    @classmethod
    @TaskReporter.report_test()
    def getAppAttemptStatus(cls, attemptID, config=None, user=None):
        '''
        Get application attempt status with YARN CLI
        Return (exit_code, stdout)
        :param attemptID: Application Attempt ID to get status of Application Attempt
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: (exit_code, stdout) from YARN CLI of yarn applicationattempt -status attemptID
        '''
        if config is None:
            cmd = "applicationattempt -status %s" % attemptID
        else:
            cmd = "--config " + config + " applicationattempt -status %s" % attemptID
        if user is None:
            return cls.run(cmd, None, True)
        else:
            return cls.runas(user, cmd, None, True)

    @classmethod
    @TaskReporter.report_test()
    def getContainerStatus(cls, containerID, config=None, user=None):
        '''
        Get container status with YARN CLI
        Return (exit_code, stdout)
        :param containerID: Container ID to get status of Container
        :param config: If specified try use provided config directory.
        Default=None (means use default config directory)
        :param user: Run application list command as specified user.
        Default=None (means run command as logged-in user)
        :return: (exit_code, stdout) from YARN CLI of yarn container -status containerID

        '''
        if config is None:
            cmd = "container -status %s" % containerID
        else:
            cmd = "--config " + config + " container -status %s" % containerID
        if user is None:
            return cls.run(cmd, None, True)
        else:
            return cls.runas(user, cmd, None, True)

    @classmethod
    @TaskReporter.report_test()
    def getContainerState(cls, containerID, config=None, user=None):
        """
        Get container state
        :param containerID: containerId
        :param config:
        :param user:
        :return: state of container such as RUNNING, COMPLETED
        """
        _, stdout = cls.getContainerStatus(containerID, config=config, user=user)
        matchObj = re.search("State : (.*)$", stdout, re.MULTILINE)
        #Final-State is also matched at next element.
        if matchObj is None:
            return None
        else:
            return matchObj.group(1).rstrip()

    # Get version with YARN CLI
    @classmethod
    def getVersion(cls):
        cmd = "version"
        return cls.run(cmd, None, True)

    @classmethod
    def get_version(cls):
        exit_code, output = cls.run("version")
        version = None
        if exit_code == 0:
            pattern = re.compile(r"^Hadoop (\S+)", re.M)
            m = pattern.search(output)
            if m:
                version = m.group(1)
        return version

    @classmethod
    @TaskReporter.report_test()
    def getRawNodeList(cls, states=None, logoutput=True):
        '''
        Gets node managers of specified states.
        Returns (exit_code, stdout) of $ yarn node -list
        states is a string.
        If states is None, returns nodes of RUNNING states.
        If states is "ALL", returns nodes of any states.
        Example of NM-ID (first entry) is "hor17n11.gq1.ygridcore.net:45454".
        '''
        cmd = "node -list"
        if states is not None and states.upper() != "RUNNING":
            if states.upper() == "ALL":
                cmd += " -all"
            else:
                cmd += " -states %s" % states
        return cls.run(cmd, None, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def getNodeIDNumContainerList(cls, states=None, logoutput=True):
        '''
        Gets a list of (Node Manager ID, number of containers).
        If you want to get container IDs, use MAPRED.getAttemptIdContainerIdHostList
        '''
        (_, stdout) = cls.getRawNodeList(states, logoutput=logoutput)
        headerFound = False
        tempList = []
        for line in stdout.split('\n'):
            if re.search("Node-Id.*Node-State", line) is not None:
                headerFound = True
            elif headerFound:
                tempList.append((line.split()[0].strip(), line.split()[3].strip()))
        return tempList

    @classmethod
    @TaskReporter.report_test()
    def getNodeList(cls, states=None, logoutput=True):
        '''
        Gets list of Node Manager IDs.
        Returns a list of strings (node IDs).
        Returns [] if none found.
        To get raw result of node list, use getRawNodeList method.
        '''
        nodeIDContainerIDList = cls.getNodeIDNumContainerList(states, logoutput=logoutput)
        result = []
        for entry in nodeIDContainerIDList:
            result.append(entry[0])
        rex = re.compile(".*azurefs2 has a full queue and can't consume the given metrics.")
        newResult = [x for x in result if not rex.match(x)]
        return newResult

    @classmethod
    @TaskReporter.report_test()
    def getNodeManagerHosts(cls, logoutput=True, states=None):
        '''
        Gets hosts of Node Manager.
        Returns a list of String.
        '''
        return [entry.split(":")[0] for entry in cls.getNodeList(states=states, logoutput=logoutput)]

    # Get node status
    @classmethod
    @TaskReporter.report_test()
    def getNodeStatus(cls, nodeId):
        cmd = "node -status %s" % nodeId
        return cls.run(cmd, None, True)

    @classmethod
    def getGenericHistoryWebServiceURL(cls):
        '''
        Get Generic History Web service url
        :return:
        '''
        return YARN.get_ats_web_app_address() + '/ws/v1/applicationhistory'

    @classmethod
    @TaskReporter.report_test()
    def getAllContainerHostMapForFinishedApp(cls, appId, app_attempt_id, query_user, use_xml=True):
        '''
        Query ATS and Get a list of containers for a finished app
        :param appId: Application Id
        :param app_attempt Id: App Attempt for which containers are required
        :param query_user: User whoc is querying this request
        :param use_xml: Use XML format to get results
        :return:
        '''
        url = cls.getGenericHistoryWebServiceURL() + '/apps/' + appId + '/appattempts/' + \
              app_attempt_id + '/containers'
        _, containers, _ = util.query_yarn_web_service(
            url, query_user, use_xml=use_xml, use_user_auth_in_un_secure_mode=True, use_xm2list=True
        )
        if not use_xml:
            containers = containers['container']

        container_host_map = {}
        for container in containers:
            container_host_map[str(container['containerId'])] = str(container['nodeId']).split(":")[0]
        return container_host_map

    @classmethod
    @TaskReporter.report_test()
    def getLogsApplicationID(
            cls,
            appId,
            appOwner=None,
            nodeAddress=None,
            containerId=None,
            logoutput=True,
            grepFilter=None,
            pipeToFileOutput=None,
            config=None,
            am=None,
            logFiles=None,
            log_files_pattern=None,
            list_nodes=False,
            out=None,
            size=None,
            retry=1,
            check_for_log_aggregation_status=False,
    ):
        '''
        Get logs of given application ID
        Returns (exit_code, stdout)
        Before you call this method, you should wait about 3 seconds after application finishes.
        '''
        cmd = ""
        if config:
            cmd += ' --config %s' % config
        cmd += " logs -applicationId " + appId
        if appOwner is not None:
            cmd += " -appOwner " + appOwner
        if containerId is not None:
            cmd += " -containerId " + containerId
        if nodeAddress is not None:
            cmd += " -nodeAddress " + nodeAddress
        if am is not None:
            cmd += " -am " + am
        if logFiles is not None:
            cmd += " -log_files " + logFiles
        if log_files_pattern is not None:
            cmd += " -log_files_pattern " + log_files_pattern
        if list_nodes is not False:
            cmd += " -list_nodes "
        if out is not None:
            cmd += " -out " + out
        if size is not None:
            cmd += " -size %s" % size
        if grepFilter is not None:
            cmd += " | grep " + grepFilter
        if pipeToFileOutput is not None:
            cmd += " > " + pipeToFileOutput
        i = 0
        if check_for_log_aggregation_status:
            status = cls.waitForLogAggregationToFinish(appId)
            if not status:
                return -1, ""
        while i < retry:
            exit_code, stdout = cls.runas(appOwner, cmd, logoutput=logoutput)
            if exit_code == 0:
                return exit_code, stdout
            else:
                i += 1
        return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def getApplicationLogWithTimedOut(cls, appId, appOwner=None, retry=30, check_for_log_aggregation_status=True):
        '''
        Gather application log even if log aggregation status is Timed_out
        :param appId:
        :param appOwner:
        :param retry:
        :param check_for_log_aggregation_status:
        :return:
        '''
        exit_code, stdout = cls.getLogsApplicationID(
            appId, appOwner=appOwner, retry=retry, check_for_log_aggregation_status=check_for_log_aggregation_status
        )
        if exit_code != 0:
            if cls.isLogAggregationTimedOut(appId) or cls.getLogAggregationStatus(appId):
                return cls.getLogsApplicationID(appId, appOwner=appOwner, retry=retry)
            else:
                reason = "Log aggregation status is neither SUCCEDDED nor TIMED_OUT"
                logger.info(reason)
                return cls.getLogsApplicationID(appId, appOwner=appOwner, retry=retry)
        else:
            return exit_code, stdout

    # Get logs of given application ID.
    # Returns YarnContainerLogs object if yarn logs succeeds or None otherwise.
    @classmethod
    @TaskReporter.report_test()
    def getLogsApplicationIDAsObject(
            cls,
            appId,
            appOwner=None,
            nodeAddress=None,
            containerId=None,
            logoutput=True,
            logParsing=False,
            rstripLineInWindows=True
    ):
        logObj = YarnContainerLogs()
        (exit_code, stdout) = cls.getLogsApplicationID(
            appId, appOwner=appOwner, nodeAddress=nodeAddress, containerId=containerId, logoutput=logoutput
        )
        if exit_code != 0 or not stdout:
            return None
        else:
            rstripLine = False
            if Machine.isWindows() and rstripLineInWindows:
                rstripLine = True
            logObj.parse(stdout, logParsing=logParsing, rstripLine=rstripLine)
            return logObj

    @classmethod
    @TaskReporter.report_test()
    def countContinersinApp(cls, appId):
        '''
        Count the containers started for application
        :param appId: application id
        :return: count of containers launched for appid
        '''
        text = cls.getLogsApplicationID(appId, logoutput=False)[1]
        containerId = appId.split("application_")[1]
        p1 = r"Container: container_.*_" + containerId + r"_\d{2}_\d{6}"
        p2 = "Container: container_" + containerId + r"_\d{2}_\d{6}"
        list_of_occurence = re.findall(p1, text) or re.findall(p2, text)
        if list_of_occurence:
            unique_containers = set(list_of_occurence)
            return len(unique_containers)
        else:
            return 0

    @classmethod
    @TaskReporter.report_test()
    def countAttempts(cls, appId):
        '''
        Find number of attempts used for appId
        :param cls:
        :param appId:
        :return:
        '''
        _, stdout = cls.getAppAttemptList(appId)
        app_attempt_ids = []
        for appAttemptLine in stdout:
            app_attempt_ids.append(cls.getAppAttemptID(appAttemptLine))
        return len(app_attempt_ids)

    # $ yarn rmadmin -getGroups
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminGetGroups(cls, username=None):
        cmd = "rmadmin -getGroups"
        if username is not None:
            cmd += " " + username
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -refreshUserToGroupsMappings
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminRefreshUserToGroupsMappings(cls):
        cmd = "rmadmin -refreshUserToGroupsMappings"
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -refreshSuperUserGroupsConfiguration
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminRefreshSuperUserGroupsConfig(cls, config=None):
        if config is None:
            cmd = "rmadmin -refreshSuperUserGroupsConfiguration"
        else:
            cmd = "--config " + config + " rmadmin -refreshSuperUserGroupsConfiguration"
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -addToClusterNodeLabels
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminAddLabels(cls, labels):
        '''
      Add labels to RM
      '''
        if not labels:
            return (-1, "")
        cmd = "rmadmin -addToClusterNodeLabels " + "\"" + string.join(labels, ",") + "\""
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -removeFromClusterNodeLabels
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminRemoveLabels(cls, labels):
        '''
      Remove labels to RM
      '''
        if not labels:
            return (-1, "")

        if Hadoop.isSecure() or Machine.isWindows():
            cmd = "rmadmin -removeFromClusterNodeLabels \"" + string.join(labels, ",") + "\""
        else:
            cmd = "rmadmin -removeFromClusterNodeLabels \\\"" + string.join(labels, ",") + "\\\""
        #if runas is None:
        #  return cls.run(cmd, None, True)
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -replaceLabelsOnNode
    @classmethod
    @TaskReporter.report_test()
    def runRmAdminSetLabelsOnNodes(cls, nodeToLabels):
        '''
      Set labels to nodes
      '''
        if not nodeToLabels:
            return (-1, "")
        strg = ""
        for node, labels in nodeToLabels.iteritems():
            strg = strg + node + "," + string.join(labels, ",") + " "
        if Hadoop.isSecure() or Machine.isWindows():
            cmd = "rmadmin -replaceLabelsOnNode " + "\"" + strg + "\""
        else:
            cmd = "rmadmin -replaceLabelsOnNode " + "\\\"" + strg+ "\\\""
        if not Hadoop.isSecure() and Machine.isLinux():
            cmd = "rmadmin -replaceLabelsOnNode " + "\"" + str + "\""
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    # $ yarn rmadmin -getNodeToLabels
    @classmethod
    @TaskReporter.report_test()
    def runYarnGetLabelsOnNodes(cls, nodeId):
        '''
      Get labels to nodes
      Return node to labels dict
      '''
        cmd = "node -status " + nodeId
        (exitCode, stdout) = cls.run(cmd, None, True)
        if exitCode != 0:
            return None
        # initialize dict
        labels = {}
        for line in stdout.split('\n'):
            prefix = "Node-Labels : "
            if line.__contains__(prefix):
                strg = line[line.find(prefix) + len(prefix):]
                if strg:
                    labels = set()
                    for s in strg.split(","):
                        s = s.strip()
                        if s:
                            labels.add(s)
                    return labels
                else:
                    return set()
        return set()

    @classmethod
    @TaskReporter.report_test()
    def runRmAdminClearAllLabels(cls):
        labels = cls.runYarnClusterGetLabelNames()
        cls.runRmAdminRemoveLabels(labels)

    # $ yarn cluster -listNodeLabels
    @classmethod
    @TaskReporter.report_test()
    def runYarnClusterGetLabelNames(cls):
        '''
      Get all labels in the cluster
      Return list of label names
      '''
        infos = cls.runYarnClusterGetLabelInfos()
        return [i[0] for i in infos]

    @classmethod
    @TaskReporter.report_test()
    def runYarnClusterGetLabelInfos(cls):
        '''
      Get all labels in the cluster
      Return list of label infos
      '''
        cmd = "cluster -list-node-labels"
        (exitCode, stdout) = cls.run(cmd, None, True)
        if exitCode != 0:
            return None
        prefix = "Node Labels: "

        ret = []
        for label in stdout[stdout.find(prefix) + len(prefix):].split(","):
            labelName = label[1:label.find(":")]
            exclusivity = label[label.find("=") + 1:len(label) - 1] == "true"
            if labelName:
                ret.append((labelName, exclusivity))

        return ret

    # yarn rmadmin -updateNodeResource
    @classmethod
    @TaskReporter.report_test()
    def runYarnUpdateNodeResource(cls, node, memSize='', vCores='', timeout=''):
        '''
      yarn rmadmin [-updateNodeResource [NodeID] [MemSize] [vCores] ([OvercommitTimeout])]
      run rmadmin commnand to updateNodeResource
      '''
        if vCores == '':
            if Machine.isWindows():
                vCores = '1'
            else:
                vCores = cls.getConfigValue('yarn.nodemanager.resource.cpu-vcores')
        if memSize == '':
            if Machine.isWindows():
                memSize = '8192'
            else:
                memSize = cls.getConfigValue('yarn.nodemanager.resource.memory-mb')
        cmd = "rmadmin -updateNodeResource %s %s %s %s" % (node, memSize, vCores, timeout)
        return cls.runas(Config.get('hadoop', 'YARN_USER'), cmd, None, True)

    @classmethod
    @TaskReporter.report_test()
    def killAllRunningAMs(cls):
        '''
        Find all running AM's using YARN WS and then kill them
      '''
        # add retries just in case the RM was down when
        # this call is being made especially in HA tests
        tries = 1
        apps = cls.getApplicationByState('RUNNING')
        while not apps and tries < 11:
            time.sleep(5)
            apps = cls.getApplicationByState('RUNNING')
            tries += 1

        for app in apps:
            # get the container
            m = re.search(r'.*(container_\d+_\d+_\d+_\d+).*', app['amContainerLogs']) or \
                re.search(r'.*(container_e[0-9]+_\d+_\d+_\d+_\d+).*', app['amContainerLogs'])
            containerId = None
            if m and m.group(1):
                containerId = m.group(1)
            else:
                # if container id is not found do nothing
                logger.error(
                    "Could not retrieve container id for application %s from text '%s' ",
                    app['id'], app['amContainerLogs']
                )
                continue

            # get the host its running on
            host = None
            m = re.search(r'(.*):\d*', app['amHostHttpAddress'])
            if m and m.group(1):
                host = m.group(1)
            else:
                # if host is not found do nothing
                logger.error(
                    "Could not retrieve host for application %s from text '%s' ", app['id'], app['amHostHttpAddress']
                )
                continue

            logger.info('Kill container %s on host %s', containerId, host)
            if Machine.isWindows():
                procList = []
                host = Machine.getfqdn(name=host)
                # log onto the host and get all process for the container
                all_processes = Machine.getProcessListPowershell(
                    host, filterProcessName='java.exe', filterCmdLine=containerId, logoutput=True
                )
                for p in all_processes:
                    procList.append(p[2])
                if not procList:
                    logger.info("No AM process found.")
                    return
                killCmd = "taskkill /f /t"
                for p in procList:
                    killCmd += " /pid %s" % p

            else:
                # log onto the host and get all process for the container
                procList = Machine.getProcessListRemote(host, filter=containerId)
                if not procList:
                    logger.info("No AM process found.")
                    return

                pIds = ''
                for proc in procList:
                    pIds += ' %s' % proc.split()[1]
                killCmd = "kill -9 %s" % pIds

            _, _ = Machine.runas(
                Machine.getAdminUser(), killCmd, host, None, None, "True", Machine.getAdminPasswd()
            )

    # Find all applciation by state. Will return a list of dictionary objects about the app
    @classmethod
    @TaskReporter.report_test()
    def getApplicationByState(cls, state):
        jmxData = cls.getApplicationsInfo(state=state)
        if jmxData:
            return jmxData
        else:
            return []

    # Find all applciation by state. Will return a list of dictionary objects about the app
    @classmethod
    @TaskReporter.report_test()
    def getApplicationsInfo(cls, state=None, startedTimeBegin=None, finishedTimeEnd=None):
        '''
        getApplicationsInfo
          params
            state - which state the applications should be in. Defaut None
            startedTimeBegin - Time after which the applications started. Default None
            finishedTimeEnd - Time before which the applications Ended. Default None
      '''
        params = {}
        if state:
            params['state'] = state
        if startedTimeBegin:
            params['startedTimeBegin'] = startedTimeBegin
        if finishedTimeEnd:
            params['finishedTimeEnd'] = finishedTimeEnd
        queryParams = urlencode(params)
        rm = cls.getResourceManagerWebappAddress()
        url = '%s/ws/v1/cluster/apps' % rm
        if queryParams:
            url += '?%s' % url
        jmxData = util.getJSONContent(url)
        if jmxData and jmxData['apps'] and jmxData['apps']['app']:
            return jmxData['apps']['app']
        else:
            return []

    # Return the application info for a given id
    @classmethod
    @TaskReporter.report_test()
    def getApplicationInfo(cls, appId, user=HRT_QA):
        rm = cls.getResourceManagerWebappAddress()
        logger.info("url - %s/ws/v1/cluster/apps/%s?user.name=%s", rm, appId, user)
        jmxData = util.getJSONContent('%s/ws/v1/cluster/apps/%s?user.name=%s' % (rm, appId, user), user=user)
        if jmxData['app']:
            return jmxData['app']
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getApplicationInfoFromName(cls, appName, user=HRT_QA):
        rm = cls.getResourceManagerWebappAddress()
        logger.info("url - %s/ws/v1/cluster/apps?user.name=%s", rm, user)
        apps = util.getJSONContent('%s/ws/v1/cluster/apps?user.name=%s' % (rm, user), user=user)['apps']['app']
        for app in apps:
            if app['name'] == appName:
                return YARN.getApplicationInfo(app['id'], user=user)
        return None

    @classmethod
    @TaskReporter.report_test()
    def getApplicationAttempts(cls, appId, max_retries=20):
        '''
      Returns the appAttempts for an given appId.
      :param appId: AppId to be retrieved.
      :param max_retries: Max # of retries to be tried with a sleep of 15 secs before we fail.
      :return: appAttempts or None.
      '''
        for _ in range(max_retries):
            rm = cls.getResourceManagerWebappAddress()
            jmxData = util.getJSONContent('%s/ws/v1/cluster/apps/%s/appattempts' % (rm, appId))
            if jmxData and jmxData.has_key('appAttempts'):
                return jmxData['appAttempts']
            # If we cannot find the data, sleep.
            time.sleep(15)

        return None

    @classmethod
    @TaskReporter.report_test()
    def getMRJobInfo(cls, jobId, user=None):
        '''
        Return the MR job info for a given id
      '''
        host = MAPRED.getHistoryServerWebappAddress()
        if user:
            jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s' % (host, jobId), user=user)
        else:
            jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s' % (host, jobId))
        if jmxData and 'job' in jmxData:
            return jmxData['job']
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getMRJobAttempts(cls, jobId):
        '''
        Return the MR job attempts info for a given id
      '''
        host = MAPRED.getHistoryServerWebappAddress()
        jmxData = util.getJSONContent('%s/ws/v1/history/mapreduce/jobs/%s/jobattempts' % (host, jobId))
        if jmxData and jmxData['jobAttempts']:
            return jmxData['jobAttempts']
        else:
            return None

    # Gets path of disributed shell jar in YARN HOME
    # Returns None if not found
    @classmethod
    @TaskReporter.report_test()
    def getDistributedShellJar(cls):
        if Machine.isLinux():
            jarLoc = Machine.find(None, None, cls.getYarnHome(), "\\*distributedshell\\*.jar")
        else:
            jarLoc = Machine.find(None, None, cls.getYarnHome(), "*distributedshell*.jar")
        if not jarLoc:
            return None
        else:
            return jarLoc[0]

    @classmethod
    @TaskReporter.report_test()
    def getRMUrl(cls, getRMHostwithAmbari=True):
        '''
        Returns URL of ResourceManager web UI.
        If https is available, returns "https://<RM-host>:<https-port>".
        Otherwise, returns "http://<RM-host>:<http-port>".
        Deprecated. Use cls.getResourceManagerWebappAddress() instead.
        '''
        return cls.getResourceManagerWebappAddress(getRMHostwithAmbari=getRMHostwithAmbari)

    # Returns HTTP response of RM web UI main page
    # Returns "" if the url is invalid.
    @classmethod
    @TaskReporter.report_test()
    def getRMMainPageUrlContents(cls):
        url = cls.getRMUrl()
        logger.info("getRMMainPageUrlContents asking for contents from %s", url)
        #return "" if url is invalid
        rsp = util.getURLContents(url, outfile="")
        return rsp

    @classmethod
    @TaskReporter.report_test()
    def refreshRMQueues(cls, propDict, makeCurrConfBackupInWindows=False, host=None):
        '''
        Refresh RM Queues.
        Returns (exit_code, stdout).
        RM must be restarted earlier with this modified config path.
        '''
        Hadoop.modifyConfig(
            {
                "capacity-scheduler.xml": propDict
            }, {'services': ['jobtracker']},
            isFirstUpdate=True,
            makeCurrConfBackupInWindows=makeCurrConfBackupInWindows
        )
        if host is None:
            #I don't feel it is correct. This doesn't take custom config and runs on gateway. -Tassapol
            return cls.runas(Config.get('hadoop', 'YARN_USER'), "rmadmin -refreshQueues", None, True)
        else:
            #works only when RM is started with this modified config path before.
            mod_conf_path = Hadoop.getModifiedConfigPath()
            return cls.runas(
                Config.get('hadoop', 'YARN_USER'),
                "rmadmin -refreshQueues -conf %s" % mod_conf_path,
                None,
                logoutput=False,
                host=host
            )

    @classmethod
    def restoreRMQueues(cls):
        '''
        Restore RM Queues with refresh queue CLI. Removing queue is not supported.
        Returns (exit_code, stdout)
        '''
        Hadoop.restoreConfig(["capacity-scheduler.xml"], {'services': ['jobtracker']})
        return cls.run("rmadmin -refreshQueues", None, True)

    @classmethod
    def restoreRMQueuesRMRestart(cls):
        '''
        Restore RM Queues and do RM restart. Removing queue is supported.
        Returns None
        '''
        Hadoop.restoreConfig(["capacity-scheduler.xml"], {'services': ['jobtracker']})
        cls.restartResourceManager(None)

    @classmethod
    def getContainerMinimumMB(cls):
        return int(cls.getConfigValue('yarn.scheduler.minimum-allocation-mb', '1024'))

    # function to check all jobs succeeeded
    @classmethod
    @TaskReporter.report_test()
    def checkJobsSucceeded(cls, ids, user=None):
        '''
        checkJobsSucceeded
          ids - List of ids you want to check
        Method takes a list of ids and will use YARN RM WS to check the
        status of the job. If all jobs succeeded it will return true and false
        other wise. Will return the boolean and a dictionary object of all the jobs
        checked and their returned status
        '''
        d = {}
        status = True
        ids.sort()
        for Id in ids:
            jobInfo = cls.getMRJobInfo(Id, user)
            logger.info("JobInfo: %s", jobInfo)
            if jobInfo:
                d['%s' % Id] = jobInfo['state']
                if jobInfo['state'] != 'SUCCEEDED':
                    status = False
            else:
                status = False
                d[str(Id)] = 'Job not found!'

        return status, d

    # function to check all apps succeeeded
    @classmethod
    @TaskReporter.report_test()
    def checkAppsSucceeded(cls, ids, logPrefix=None, useWS=True, localDir=None):
        '''
        checkAppsSucceeded
          ids - List of ids you want to check
          logPrefix - Default None - If you want to add a prefix the rm log copied locally
          useWS - Default False - If you want to use the WS to check the status or not. If using WS you should
                  be careful as any completed apps wont be available to the RM after RM restart. In that case
                  set this to False so we can instead check the RM log
        Method takes a list of application ids and then looks for a specific pattern in the RM
        log to determine if the app succeeded or not. The reason we check the log rather than Yarn WS or RPC
        is because after RM restart finished apps will no longer be known to the RM.
      '''
        d = {}
        status = True
        ids.sort()
        if useWS:
            for Id in ids:
                logger.info('Getting Application Info ')
                appInfo = cls.getApplicationInfo(Id)
                logger.info(appInfo)
                if appInfo:
                    d['%s' % Id] = appInfo['finalStatus']
                    if appInfo['finalStatus'] != 'SUCCEEDED':
                        status = False
                else:
                    d[str(Id)] = 'Could not get app info!'
                    status = False
        else:
            # sleep 20s just to make sure log is populated
            time.sleep(20)
            # get the rm job summary log file
            rmLogFile = MAPRED.getJobSummaryLogFile()
            logFile = 'local-rm-job-summary-%s.log' % int(time.time())
            if logPrefix:
                logFile = logPrefix + '-' + logFile
            if not localDir:
                localDir = Config.getEnv('ARTIFACTS_DIR')
            localRMLog = os.path.join(localDir, logFile)
            if os.path.exists(localRMLog):
                # delete the file
                os.remove(localRMLog)

            # copy the rm log file to local host
            Machine.copyToLocal(
                user=None, host=cls.getResourceManagerHost(), srcpath=rmLogFile, destpath=localRMLog, passwd=None
            )
            logoutput = open(localRMLog.rstrip('\r\n')).read()
            # check the log file pattern for each id
            for Id in ids:
                patternFound = False
                pattern = r'ApplicationSummary: appId=%s,name=.*?,.*?finalStatus=(\S+)' % Id
                m = re.search(pattern, logoutput, re.DOTALL)
                if m and m.group(1):
                    patternFound = True
                    d[str(Id)] = m.group(1)
                    if m.group(1) != 'SUCCEEDED':
                        status = False

                # if pattern is not found set it to false
                if not patternFound:
                    d[str(Id)] = "Did not find pattern '%s' in log file %s" % (pattern, localRMLog)
                    status = False

            # clean up the log if status is True
            if status:
                # delete the file
                os.remove(localRMLog)

        return status, d

    @classmethod
    @TaskReporter.report_test()
    def getAppAndJobIdsFromConsole(
            cls, text, jobPattern='.* Running job: (.*)', appPattern='.* Submitted application (.*)'
    ):
        '''
        getAppAndJobIdsFromConsole
          text - text to search for app and job ids
          jobPattern - If you want something different than the default
          appPattern - If you want something different than the default
        Given the console output for a Yarn Job get the job id and the application id
        submitted. Returns two lists applications and jobs
        '''
        apps = []
        jobs = []

        # get all the apps
        for line in re.finditer(appPattern, text):
            apps.append(line.group(1))

        # get all the jobs
        for line in re.finditer(jobPattern, text):
            jobs.append(line.group(1))

        return list(set(apps)), list(set(jobs))

    @classmethod
    @TaskReporter.report_test()
    def verifyMRTasksCount(cls, jobID, appID, amResyncCount, skipAssert=True):
        '''
      Function to verify if a MR Job does not repeat any tasks.
      '''
        _, appLogs = cls.getLogsApplicationID(appID, logoutput=False)
        # this message is periodically printed in AM log to show the current task statistics:
        # After Scheduling: PendingReds:0 ScheduledMaps:31 ScheduledReds:0
        # AssignedMaps:5 AssignedReds:1 CompletedMaps:16 CompletedReds:0 ContAlloc:20
        # ContRel:0 HostLocal:19 RackLocal:0"
        completedMaps = completedReds = count = 0
        for line in appLogs.split('\n'):
            res = re.search("After Scheduling:.* CompletedMaps:(.*) CompletedReds:(.*) ContAlloc:.*", line)
            if res:
                assert int(res.group(1)) >= completedMaps
                assert int(res.group(2)) >= completedReds
                completedMaps = int(res.group(1))
                completedReds = int(res.group(2))
            # each time RM restart, AM will print this when it resyncs with RM.
            res = re.search("ApplicationMaster is out of sync with ResourceManager", line)
            if res:
                count = count + 1
        if not skipAssert:
            assert (count == amResyncCount or count > 0)
        cls.verifySingleAttemptForMRTask(jobID, 'MAP', "completed")
        cls.verifySingleAttemptForMRTask(jobID, 'REDUCE', "completed")

    @classmethod
    @TaskReporter.report_test()
    def verifySingleAttemptForMRTask(cls, jobID, taskType, state):
        '''
      Verify each task type has only one attempt
      '''
        taskList = MAPRED.getAttemptIdsForJobId(jobID, taskType, state)
        assert taskList != None and taskList
        for task in taskList:
            count = task.split('_')[-1]
            # only one attempt for each task.
            assert int(count) == 0

    @classmethod
    @TaskReporter.report_test()
    def verifyMRTaskAttempts(cls, jobId, expectedNumAttempts, logoutput=False):
        '''
      Function to verify if map and reduce tasks are attempted expectedNumAttempts.
      :param jobId: JobId being verified.
      :param expectedNumAttempts: # of map and reduce attempts that are expected for the job id.
      :param logoutput: Whether to logoutput.
      :return: success status and a dict with the key and the relevant info.
      '''
        d = {}
        numAttemptsAsExpected = True

        if not jobId:
            d[str(jobId)] = "Job not found!"
            numAttemptsAsExpected = False
        else:
            tasks = MAPRED.getMRTasks(jobId)
            if not tasks or not tasks["task"]:
                d["%s" % jobId] = "Tasks not found for the given job!"
                numAttemptsAsExpected = False
            else:
                for task in tasks["task"]:
                    if task:
                        taskId = task["id"]
                        taskAttempts = MAPRED.getMRTaskAttemptsInfo(jobId, taskId)

                        if taskAttempts and len(taskAttempts["taskAttempt"]) != expectedNumAttempts:
                            d["Check Failed %s - %s TaskAttempts : " % (jobId, taskId)] = \
                                "# TaskAttempts: %s Expected: %s TaskAttemptsInfo: %s" \
                                % (len(taskAttempts["taskAttempt"]), expectedNumAttempts, taskAttempts)
                            numAttemptsAsExpected = False

                    else:
                        d["%s" % jobId] = "Task not found!"
                        numAttemptsAsExpected = False

        # Prepend the method names to all the keys in the dict.
        tempd = {}
        for k, v in d.items():
            tempd["%s: %s" % ("verifyMRTaskAttempts", k)] = v

        if logoutput:
            for k, v in tempd.items():
                logger.info("%s -> %s", k, v)

        return numAttemptsAsExpected, tempd

    @classmethod
    @TaskReporter.report_test()
    def verifyJobSuccessWithWorkPreservingRMRestart(cls, jobIDs, logoutput=False):
        '''
      Function to verify if all Jobs succeeded without task repetition.
      :param jobIDs: List of jobIDs that need to be validated.
      :param logoutput: Whether to logoutput.
      :return: success status and a dict with the relevant info.
      '''
        # If WorkPreservingRM Restart feature is enabled, we will be performing the following validations for each
        # of the jobs:
        # 1) All the maps and reduces completed successfully
        # 2 (deprecated) The # of task attempts == 1 for both map and reduce tasks
        # 3) (deprecated) The AM attempts == 1

        d = {}
        status = True

        if jobIDs is None or len(jobIDs) <= 0:
            d[str(jobIDs)] = "Job IDs not found!"
            status = False

        elif YARN.isWorkPreservingRMRestartEnabled() is False:
            for jobID in jobIDs:
                d[jobID] = "Work Preserving RM Restart feature is not enabled on this cluster."
                status = False

        else:
            for jobID in jobIDs:
                jobInfo = cls.getMRJobInfo(jobID)

                # Verifying #1 from above
                if jobInfo and (jobInfo["state"] != "SUCCEEDED" or
                                jobInfo["mapsTotal"] != jobInfo["mapsCompleted"] or
                                jobInfo["reducesTotal"] != jobInfo["reducesCompleted"]):
                    d["Check Failed %s : " % jobID] = \
                        "state==SUCCEEDED, mapsTotal==mapsCompleted, reducesTotal==reducesCompleted, JobInfo: %s" %\
                        (jobInfo)
                    status = False

                # No longer checking this due to BUG-41081
                #
                # Verifying #2 from above
                # mr_task_attempt_status, td = cls.verifyMRTaskAttempts(jobID, 1)
                # d.update(td)
                ## set the status to false if it failed
                # if not mr_task_attempt_status:
                #   status = False

                # Verifying #3 from above
                #appID = jobID.replace("job", "application")
                #appAttempts = cls.getApplicationAttempts(appID)

                #if appAttempts and len(appAttempts["appAttempt"]) != 1:
                # d["Check Failed %s : " % appID] = "# App Attempts: %s Expected: 1 AppAttemptsInfo: %s" % \
                #                                   (len(appAttempts["appAttempt"]), appAttempts)
                # status = False

        # Prepend the method names to all the keys in the dict.
        tempd = {}
        for k, v in d.items():
            tempd["%s: %s" % ("verifyJobSuccessWithWorkPreservingRMRestart", k)] = v

        if logoutput:
            for k, v in tempd.items():
                logger.info("%s -> %s", k, v)

        return status, tempd

    @classmethod
    @TaskReporter.report_test()
    def getRMLogFile(cls, host=None):
        '''
        getRMLogFile
          gets the filname with the full path for the yarn resourcemanager log file.
      '''
        file_prefix = 'hadoop-resourcemanager-*.log'
        # on linux the username is part of the file name
        if not Machine.isWindows():
            yarn_user = Config.get('hadoop', 'YARN_USER', 'yarn')
            file_prefix = 'hadoop-%s-resourcemanager-*.log' % yarn_user

        if not host:
            host = cls.getResourceManagerHost()
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=host,
            filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
            searchstr=file_prefix,
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        logger.info("getRMLogFile Lines")
        logger.info(lines)
        if lines and lines[0]:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getNMWebappUrl(cls, host):
        '''
        Returns NM webapp url.
        If https is available, returns "https://<host>:https-port".
        Otherwise, returns "http://<host>:http-port".
        '''
        if cls.isHttpsEnabled():
            return "https://%s:%s" % (host, cls.getNodeManagerWebappHttpsPort())
        else:
            return "http://%s:%s" % (host, cls.getNodeManagerWebappPort())

    @classmethod
    @TaskReporter.report_test()
    def getNMWSURL(cls, host):
        url = cls.getNMWebappUrl(host) + "/ws/v1/node"
        return url

    @classmethod
    @TaskReporter.report_test()
    def getNMWSInfoXMLContents(cls, host, key, logoutput=False):
        url = cls.getNMWSURL(host) + "/info"
        result = util.getXMLValue(url, key, logoutput=logoutput)
        if logoutput:
            logger.info("getNMWSInfoXMLContents host=%s key=%s result=%s", host, key, result)
        return result

    @classmethod
    @TaskReporter.report_test()
    def getRMHostByState(cls, expectedState, retry=True, tries=10, getRMHostwithAmbari=True): #pylint: disable=inconsistent-return-statements
        """
        Method that returns the hostname for RM based on State
        :param expectedState:
        :param retry:
        :param tries:
        :param useAmbari:
        :return:
        """
        if not cls.isHAEnabled():
            raise Exception("This feature requires YARN HA to be enabled.")

        if getRMHostwithAmbari:
            return Ambari.getHostPerHAState('RESOURCEMANAGER', expectedState.upper())
        else:
            rm_ids = cls.getConfigValue("yarn.resourcemanager.ha.rm-ids").split(",")
            step_time = 10
            for rm_id in rm_ids:
                count = 0
                cmd = 'rmadmin -getServiceState %s' % rm_id
                exit_code, output, _ = cls.runasAdmin(cmd, stderr_as_stdout=False)
                logger.info('exit code = %s', exit_code)
                while exit_code != 0 and count < tries and retry:
                    logger.info('Retry %s of %s', (count + 1), tries)
                    count += 1
                    time.sleep(step_time)
                    exit_code, output, _ = cls.runasAdmin(cmd, stderr_as_stdout=False)
                if expectedState.lower() in output.lower():
                    return cls.getConfigValue(
                        'yarn.resourcemanager.hostname.%s' % rm_id,
                        cls.getConfigValue('yarn.resourcemanager.address.%s' % rm_id)
                    ).split(':')[0]
            return None

    @classmethod
    @TaskReporter.report_test()
    def restartActiveRM(cls, wait=10, config=None):
        rmhost = cls.getRMHostByState('active')
        cls.killResourceManager(rmhost=rmhost)
        time.sleep(wait)
        cls.resetResourceManager('start', config=config, host=rmhost)

    @classmethod
    @TaskReporter.report_test()
    def isHAEnabled(cls):
        '''
        Returns true/false if HA is enabled or not.
      '''
        return cls.getConfigValue('yarn.resourcemanager.ha.enabled', 'false').lower() == 'true'

    @classmethod
    @TaskReporter.report_test()
    def runasAdmin(cls, cmd, env=None, logoutput=True, stderr_as_stdout=True):
        return cls.runas(
            Config.get('hadoop', 'YARN_USER'), cmd, env=env, logoutput=logoutput, stderr_as_stdout=stderr_as_stdout
        )

    @classmethod
    @TaskReporter.report_test()
    def isHttpsEnabled(cls):
        '''
        Returns true/false if HTTPS is enabled or not.
      '''
        # if HTTPS_ONLY or HTTP_AND_HTTPS are set for this property return true
        return 'https' in cls.getConfigValue('yarn.http.policy', 'HTTP_ONLY').lower()

    @classmethod
    @TaskReporter.report_test()
    def resetHARMNodes(cls, action, config=None):
        '''
        Resart HA RM nodes.
        '''
        if not YARN.isHAEnabled():
            raise Exception("This feature requires YARN HA to be enabled.")

        nodes = cls.getRMHANodes()
        for node in nodes:
            cls.resetResourceManager(action, config=config, host=node)
            time.sleep(20)

    @classmethod
    @TaskReporter.report_test()
    def restartHARMNodes(cls, config=None, waitForNMToRegister=True, getRMHostwithAmbari=True):
        '''
        Resart HA RM nodes.
      '''
        if not cls.isHAEnabled():
            raise Exception("This feature requires YARN HA to be enabled.")

        nodes = cls.getRMHANodes()
        for node in nodes:
            cls.resetResourceManager('stop', config=config, host=node)
            cls.resetResourceManager('start', config=config, host=node)
            time.sleep(20)
        if waitForNMToRegister:
            MAPRED.waitForNMToRegister(getRMHostwithAmbari=getRMHostwithAmbari)

    @classmethod
    @TaskReporter.report_test()
    def getRMHANodes(cls):
        if not cls.isHAEnabled():
            raise Exception("This feature requires YARN HA to be enabled.")

        if cls._rm_ha_nodes is None or not cls._rm_ha_nodes:
            rm_ids = cls.getConfigValue('yarn.resourcemanager.ha.rm-ids').split(',')
            for rm_id in rm_ids:
                cls._rm_ha_nodes.append(cls.getConfigValue('yarn.resourcemanager.hostname.%s' % rm_id))
        return cls._rm_ha_nodes

    @classmethod
    @TaskReporter.report_test()
    def getRmStatebyRmid(cls, rm_id):
        '''
        Get Rm state (active or stanby) by rmId (rm1, rm2)
        :param rmid:
        :return:
        '''
        if not cls.isHAEnabled():
            raise Exception("This feature requires YARN HA to be enabled.")
        cmd = 'rmadmin -getServiceState %s' % rm_id
        exit_code, output, _ = cls.runasAdmin(cmd, stderr_as_stdout=False)
        logger.info('exit code = %s', exit_code)
        return output.lower()

    @classmethod
    @TaskReporter.report_test()
    def validate_zk_rm_state_in_ui(cls, state="CONNECTED", rm=None, outputfile="ZK_RM_UI", getRMHostwithAmbari=False):
        '''
        Validate RMUI about page for "ResourceManager HA zookeeper connection state" field
        :return:
        '''
        if not rm:
            url = cls.getRMUrl(getRMHostwithAmbari=getRMHostwithAmbari) + "/cluster/cluster"
        else:
            rm_ids = cls.getConfigValue("yarn.resourcemanager.ha.rm-ids").split(",")
            proto = "http://"
            port = cls.getConfigValue(
                'yarn.resourcemanager.webapp.address.%s' % (rm_ids[0]),
                cls.getConfigValue("yarn.resourcemanager.hostname." + rm_ids[0]) + ':8088'
            ).split(":")[1]
            if cls.isHttpsEnabled():
                proto = "https://"
                port = cls.getConfigValue(
                    'yarn.resourcemanager.webapp.https.address.%s' % (rm_ids[0]),
                    cls.getConfigValue("yarn.resourcemanager.hostname." + rm_ids[0]) + ':8090'
                ).split(":")[1]
            url = proto + rm + ":" + port + "/cluster/cluster"
        outputfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), outputfile)
        logger.info("OUTPUT FILE : %s", outputfile)
        if Machine.pathExists(None, None, outputfile):
            Machine.rm(None, None, outputfile, isdir=False, passwd=None)
        util.getURLContents(url, outputfile)
        pattern = r"ResourceManager HA zookeeper connection state:\s+<\/th>\s+<td>\s+" + state + r"\s+<\/td>"
        f = open(outputfile)
        text = f.read()
        f.close()
        m = re.search(pattern, text)
        return m


    @classmethod
    @TaskReporter.report_test()
    def copyNMLogToLocalMachine(cls, nmHost):
        nmLogFile = MAPRED.getTaskTrackerLogFile(nmHost)
        localNMLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-nodemanager-%s-%s.log' % (nmHost, str(int(time.time())))
        )
        # copy the file to local machine
        Machine.copyToLocal(user=None, host=nmHost, srcpath=nmLogFile, destpath=localNMLogFile, passwd=None)
        return localNMLogFile

    @classmethod
    @TaskReporter.report_test()
    def copyAllNMLogsToLocalMachine(cls, logoutput=True):
        '''
        Copies all NM logs to local machine.
        Returns a list of (String, String).
        Returns a list of (NM host, local NM log path).
        '''
        nodeManagers = cls.getNodeManagerHosts(logoutput)
        result = []
        for nm in nodeManagers:
            localNMLogPath = cls.copyNMLogToLocalMachine(nm)
            result.append((nm, localNMLogPath))
        return result

    @classmethod
    @TaskReporter.report_test()
    def enableATSService(cls, host):
        '''
        Enable timeserver service on windows
        '''
        if Machine.isWindows():
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="sc config timelineserver start= demand",
                host=host,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )

    @classmethod
    @TaskReporter.report_test()
    def resetATSServer(cls, action, config=None, host=None):
        '''
        Performs actions such as starting and stopping of ATS (Timeline Server) on specified host and config
        If host is not specified by default ResourceManager host is considered.
        '''
        if not host:
            host = cls.getATSHost()
        if re.match("start", action, re.I) is not None:
            cls.enableATSService(host)
        yarnHome = Config.get('hadoop', 'YARN_HOME').replace("client", "timelineserver")
        libexecPath = Config.get('hadoop', 'HADOOP_LIBEXEC')
        Hadoop.resetService(
            Config.get('hadoop', 'YARN_USER'), host, "timelineserver", action, config, yarnHome, "sbin", "yarn-daemon",
            libexecPath
        )

    @classmethod
    @TaskReporter.report_test()
    def restartATSServer(cls, config=None, wait=5, atsHost=None):
        '''
        Restarts ATS Server
        '''
        cls.resetATSServer("stop", host=atsHost, config=None)
        time.sleep(wait)
        return cls.resetATSServer("start", config, atsHost)

    @classmethod
    def stopATSServer(cls, atsHost=None):
        '''
        Stops ATS (Timeline server).
        '''
        cls.resetATSServer("stop", host=atsHost, config=None)

    @classmethod
    def startATSServer(cls, config=None, atsHost=None):
        '''
        Starts ATS (Timeline server).
        '''
        return cls.resetATSServer("start", config, atsHost)

    @classmethod
    @TaskReporter.report_test()
    def isATSEnabled(cls):
        '''
        Checks whether YARN ATS is enabled or not
        '''
        if Hadoop.isHadoop2():
            if cls.getConfigValue("yarn.timeline-service.enabled", defaultValue="false") == "true":
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getATSHost(cls):
        '''
        Returns YARN ATS server host name, default is same as of ResourceManager Host
        '''
        ats_host = cls.getConfigValue('yarn.timeline-service.hostname', '')
        ats_host = ats_host.strip()
        if ats_host is None or not ats_host:
            ats_host = cls.getConfigValue('yarn.timeline-service.address', '')
            ats_host = ats_host.strip().split(':')[0]
        #if ats_host is None or len(ats_host) == 0:
        #    ats_host = cls.getConfigValue('yarn.timeline-service.bind-host', '')
        #    ats_host = ats_host.strip()
        if ats_host is None or not ats_host or ats_host == '0.0.0.0':
            ats_host = cls.getResourceManagerHost()
        return ats_host

    @classmethod
    @TaskReporter.report_test()
    def get_ats_web_app_address(cls):
        '''
        Returns YARN ATS WebApp address based on if HTTPS is enabled or not.
        '''
        default_ats_web_app_address = cls.getATSHost() + ":8188"
        url_scheme = "http"
        value = YARN.getConfigValue('yarn.timeline-service.webapp.address', defaultValue=default_ats_web_app_address)
        if YARN.isHttpsEnabled():
            default_ats_web_app_address = cls.getATSHost() + ":8190"
            url_scheme = "https"
            value = YARN.getConfigValue(
                "yarn.timeline-service.webapp.https.address", defaultValue=default_ats_web_app_address
            )
        return "%s://%s" % (url_scheme, value)

    @classmethod
    @TaskReporter.report_test()
    def get_ats_v2_reader_web_app_address(cls):
        """
        Return ATSv2 reader address
        """
        scheme = 'http'
        ats_v2_host_address = cls.getConfigValue('yarn.timeline-service.reader.webapp.address', '')
        if YARN.isHttpsEnabled():
            scheme = 'https'
            ats_v2_host_address = cls.getConfigValue('yarn.timeline-service.reader.webapp.https.address', '')
        return '%s://%s' % (scheme, ats_v2_host_address)

    @classmethod
    @TaskReporter.report_test()
    def is_ats_got_started_stopped(cls, started_or_stopped, wait=20, interval=1):
        '''
        When ATS is started or stopped, checks whether ATS is came upor stopped
        if startOrStopped is True, it will wait ATS server to start listening to http webapp port after starting it
        else startOrStopped is Falsue, it will wait ATS server to stop listening to http webapp port
        after being stopped
        '''
        ats_web_app_addr = cls.get_ats_web_app_address()
        ats_web_app_addr = ats_web_app_addr.replace("http://", '').replace("https://", '')
        return util.is_service_got_started_or_stopped(ats_web_app_addr, started_or_stopped, wait, interval)

    @classmethod
    @TaskReporter.report_test()
    def is_ats_running(cls):
        '''
        Returns True/False according to  whether YARN ATS is enabled and listening on its WebAppPort
        '''
        ats_web_app_addr = cls.get_ats_web_app_address()
        ats_web_app_addr = ats_web_app_addr.replace("http://", '').replace("https://", '')
        return util.is_service_running(ats_web_app_addr)

    @classmethod
    @TaskReporter.report_test()
    def get_ats_json_code_data_headers(
            cls,
            url_comp,
            user,
            return_exception_json=False,
            do_not_use_curl_in_secure_mode=False,
            use_user_auth_in_un_secure_mode=False,
            user_delegation_token=None,
            renew_cancel_delegation_token_use_curl=False,
            cookie=None,
            http_method='GET'
    ):
        '''
        Access ATS Web Service with http://rmhost:8188/ws/v1/timeline+urlpath/queryComp=${urlcomp}
        Returns HTTP(s) reponse code, response data and respons headers
        '''
        # Pylint changes for unused-argument
        logger.info(return_exception_json)
        ws_url = cls.get_ats_web_app_address() + "/ws/v1/timeline"
        if url_comp is not None and url_comp.strip():
            ws_url += '/' + url_comp
        logger.info("get_ats_json_code_data_headers ws_url = %s", ws_url)
        return util.query_yarn_web_service(
            ws_url,
            user,
            None,
            None,
            False,
            http_method,
            False,
            do_not_use_curl_in_secure_mode,
            use_user_auth_in_un_secure_mode,
            user_delegation_token,
            renew_cancel_delegation_token_use_curl,
            cookie_string=cookie
        )

    @classmethod
    @TaskReporter.report_test()
    def get_ats_json_data(
            cls,
            url_comp,
            user,
            return_exception_json=False,
            do_not_use_curl_in_secure_mode=False,
            use_user_auth_in_un_secure_mode=False,
            user_delegation_token=None,
            renew_cancel_delegation_token_use_curl=False,
            cookie=None,
            http_method='GET'
    ):
        '''
        Access ATS Web Service with http://rmhost:8188/ws/v1/timeline+urlpath/queryComp=${urlcomp}
        Returns HTTP(s) repsonse data if HTTP(s) response code is 2XX series,
        otherwise return None if return_exception_json is not True, else returns exception JSON
        '''
        logger.info("get_ats_json_data url_comp = %s", url_comp)
        ret_code, ret_data, _ = cls.get_ats_json_code_data_headers(
            url_comp, user, return_exception_json, do_not_use_curl_in_secure_mode, use_user_auth_in_un_secure_mode,
            user_delegation_token, renew_cancel_delegation_token_use_curl, cookie, http_method
        )
        if ret_code < 100 or ret_code > 299:
            if ret_data is not None and 'exception' in ret_data:
                logger.error(
                    "Got Exception in repsonse JSON %s by user %s for urlcomp %s", str(ret_data), user, url_comp
                )
                if not return_exception_json:
                    ret_data = None
        return ret_data

    @classmethod
    @TaskReporter.report_test()
    def access_ats_ws_path(
            cls,
            url_comp,
            ids,
            app_id,
            user,
            expected_values=1,
            use_user_auth_in_un_secure_mode=False,
            delegation_token=None,
            cookie=None
    ):
        '''
        Access ATS WS API for give URL path=${url_comp} amd validate that returned josn contains 'entities
        And logs elements contained in 'entities' array in returned json

        '''
        p = cls.get_ats_json_data(
            url_comp,
            user,
            use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
            user_delegation_token=delegation_token,
            cookie=cookie
        )
        assert p is not None and isinstance(p, dict),\
            "Got None/Null  in JSON reponse for user %s and url query comp %s " % (user, url_comp)
        assert 'exception' not in p, 'Got exception %s in reponse for user %s and url query comp %s ' % (
            str(p), user, url_comp
        )
        assert 'entities' in p, "entities' is not found in JSON response %s for user %s and url query comp %s" % (
            str(p), user, url_comp
        )
        logger.info("list of entites %s", str(len(p['entities'])))
        if len(p['entities']) < expected_values:
            return
        for e in p['entities']:
            if e['entity'] in ids:
                logger.debug(
                    "ATS WS " + url_comp + " contains " + e['entity'] + " which is also ran by our application " +
                    app_id
                )

    @classmethod
    @TaskReporter.report_test()
    def isWorkPreservingRMRestartEnabled(cls):
        '''
        Checks if Work preserving RM restart feature is enabled on the cluster.
        Returns: Bool.
        '''
        if Hadoop.isHadoop2():
            recovery_enabled = cls.getConfigValue("yarn.resourcemanager.recovery.enabled", "false")
            wpr_enabled = cls.getConfigValue("yarn.resourcemanager.work-preserving-recovery.enabled", "false")
            logger.info("recovery_enabled  : %s", recovery_enabled)
            logger.info("wpr_enabled       : %s", wpr_enabled)
            if recovery_enabled == "true" and wpr_enabled == "true":
                logger.info("Work Preserving RM-restart (WPR) is enabled.")
                return True
        logger.info("Work Preserving RM-restart (WPR) is not enabled.")
        return False

    @classmethod
    @TaskReporter.report_test()
    def getATSWebappAddress(cls):
        '''
        Returns YARN ATS WebApp address based on if HTTPS is enabled or not.
        If https is available, returns "https://<ATS-host>:<https-port>".
        Otherwise, returns "http://<ATS-host>:<http-port>".
        '''
        propName = 'yarn.timeline-service.webapp.address'
        defaultPort = '8188'
        scheme = 'http'
        if cls.isHttpsEnabled():
            propName = 'yarn.timeline-service.webapp.https.address'
            defaultPort = '8190'
            scheme = 'https'
        return '%s://%s' % (scheme, cls.getConfigValue(propName, '%s:%s' % (cls.getATSHost(), defaultPort)))

    @classmethod
    @TaskReporter.report_test()
    def getAMContainerIdFromAppAttempt(cls, applicationId, config=None):
        '''
        This function is used to fetch amContainerId for FINISHED applications
        :param applicationId:
        :param user:
        :param config:
        :return:
        '''
        if config != None:
            addConfig = "--config %s" % config
        else:
            addConfig = ""

        amContainer = None

        cmd = "%s applicationattempt -list %s" % (addConfig, applicationId)
        (rc, stdout) = cls.runas(YARN.getYarnUser(), cmd, None, True)
        if rc != 0:
            return None

        for line in stdout.splitlines():
            if line.startswith("appattempt_"):
                amContainer = line.split()[2]

        return amContainer

    @classmethod
    @TaskReporter.report_test()
    def getContainerIdAndHosts(
            cls, applicationId, user=Config.getEnv('USER'), filter_attemptId=None, config=None, status=None
    ):
        '''
        Get all container Id by given applicationId
        :param appplicationId:
        :return (list of container id, list of hosts of these container)
        '''
        if not Hadoop.isHadoop2():
            return []

        if config != None:
            addConfig = "--config %s" % config
        else:
            addConfig = ""

        cmd = "%s applicationattempt -list %s" % (addConfig, applicationId)
        (rc, stdout) = cls.runas(user, cmd, None, True)
        if rc != 0:
            return None

        containerIds = []
        containerHosts = []

        # loop all attempts
        for line in stdout.splitlines():
            if line.startswith("appattempt_"):
                attemptId = line.split()[0]
                if filter_attemptId is None or filter_attemptId == attemptId:
                    cmd = "%s container -list %s" % (addConfig, attemptId)
                    (containerRc, containerStdout) = cls.runas(user, cmd, None, True)
                    if containerRc != 0:
                        return None
                    for cline in containerStdout.splitlines():
                        if cline.startswith("container_"):
                            # State of container will be null for containers which are in RESERVED-KILL state
                            if "null" in cline:
                                continue
                            tmpList = []
                            if status != None:
                                if status in cline:
                                    for s in cline.split():
                                        if s:
                                            tmpList.append(s)
                                    containerIds.append(tmpList[0])
                                    containerHosts.append(
                                        tmpList[len(tmpList) - 3][:tmpList[len(tmpList) - 3].find(':')]
                                    )
                            else:
                                for s in cline.split():
                                    if s:
                                        tmpList.append(s)
                                containerIds.append(tmpList[0])
                                containerHosts.append(tmpList[len(tmpList) - 3][:tmpList[len(tmpList) - 3].find(':')])

        return (containerIds, containerHosts)

    @classmethod
    @TaskReporter.report_test()
    def setupContainerExecutor(cls, hosts, cgroupsMountPath="/cgroup", banned_users=None, allowed_system_users=None):
        '''
        Configures container-executor used by LCE, on the specified hosts
        :param list of hosts to configure container-executor on
        '''
        if not (Hadoop.isHadoop2() and Machine.isLinux()):
            assert False, "You shouldn't be configuring container-executor here."

        isSecureHadoop = Hadoop.isSecure()
        hadoopConfDir = Config.get('hadoop', 'HADOOP_CONF')
        hadoopYarnHome = Config.get('hadoop', 'YARN_HOME')
        containerExecutorPath = os.path.join(hadoopYarnHome, 'bin', 'container-executor')

        for nm in hosts:
            assert Machine.makedirs("root", nm, cgroupsMountPath), "Directory creation failed"
            assert Machine.makedirs("root", nm, os.path.join(cgroupsMountPath, "cpu")), "Directory creation failed"
            assert Machine.makedirs("root", nm, os.path.join(cgroupsMountPath, "net_cls")), "Directory creation failed"
            assert Machine.makedirs("root", nm, os.path.join(cgroupsMountPath, "blkio")), "Directory creation failed"
            if not isSecureHadoop:
                Machine.runas(
                    user=Machine.getAdminUser(),
                    cmd='chgrp hadoop ' + containerExecutorPath,
                    host=nm,
                    passwd=Machine.getAdminPasswd()
                )
                Machine.runas(
                    user=Machine.getAdminUser(),
                    cmd='chmod 6050 ' + containerExecutorPath,
                    host=nm,
                    passwd=Machine.getAdminPasswd()
                )

        #Now write out a container-executor.cfg (only if not in secure mode)
        #We want to be able to run as user 'nobody' and the uid for this user could be 'low'.
        #hence, the 'min user id' is set to a low value
        containerExecutorCfgPath = os.path.join(hadoopConfDir, 'container-executor.cfg')
        yarnNMLocalDirs = "yarn.nodemanager.local-dirs=" + str(cls.getConfigValue('yarn.nodemanager.local-dirs')) \
                              + "\n"
        yarnNMlogDirs = "yarn.nodemanager.log-dirs=" + str(cls.getConfigValue('yarn.nodemanager.log-dirs')) + "\n"
        min_user_settings = 'min.user.id=50\n'

        banned_user_settings = 'banned.users=yarn\n'
        if banned_users:
            banned_user_settings = 'banned.users=%s\n' % banned_users

        allowed_system_user_settings = 'allowed.system.users=nobody\n'
        if allowed_system_users:
            allowed_system_user_settings = 'allowed.system.users=%s\n' % allowed_system_users

        userSettings = min_user_settings + banned_user_settings + allowed_system_user_settings

        featureTCEnabled = "feature.tc.enabled=1\n"
        wtext = yarnNMLocalDirs + yarnNMlogDirs + "yarn.nodemanager.linux-container-executor.group=hadoop\n" + \
                userSettings + featureTCEnabled
        localDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "container-executor-setup")

        Machine.makedirs(user=None, host=None, filepath=localDir)
        localContainerCfgFile = os.path.join(localDir, 'local-container-executor.cfg')

        with open(localContainerCfgFile, "w") as f:
            f.write(wtext)
            f.close()

        for nm in hosts:
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=nm,
                srcpath=localContainerCfgFile,
                destpath=containerExecutorCfgPath,
                passwd=Machine.getAdminPasswd()
            )
        return

    @classmethod
    @TaskReporter.report_test()
    def get_timeline_server_v1_5_configs(cls, create_ats_dirs_on_hdfs=True):
        '''
        Provides configs for Timeline Sever 1.5 (ATS V1.5) with option creating required HDFS directories for it
        :param create_ats_dirs_on_hdfs: If True create Active and Done directory paths on HDFS
        with groud ownwership=hadoop (yarn user group)
        :return: dict contains ATS V1.5 configs
        '''
        import copy
        entities = 'TEZ_DAG_ID,TEZ_APPLICATION,TEZ_APPLICATION_ATTEMPT,TEZ_VERTEX_ID,TEZ_TASK_ID,' \
                   'TEZ_TASK_ATTEMPT_ID,TEZ_CONTAINER_ID,'
        entities += 'HIVE_QUERY_ID,YARN_APPLICATION_ATTEMPT,YARN_APPLICATION,YARN_CONTAINER,' \
                    'DS_APP_ATTEMPT,DS_CONTAINER'
        active_dir = '/tmp/ats/active'
        done_dir = '/ats/entity-file-history/done'
        if create_ats_dirs_on_hdfs:
            HDFS.createDirectoryAsUser(active_dir, HDFS.getHDFSUser(), YARN.getYarnUser(), '1777')
            HDFS.createDirectoryAsUser(done_dir, HDFS.getHDFSUser(), YARN.getYarnUser(), '0700')
            HDFS.chgrp(HDFS.getHDFSUser(), "hadoop", active_dir + " " + done_dir)
        timeline_store_path = os.path.join(Machine.getTempDir(), 'yarn', 'timeline-store')
        yarn_ats_config = {
            'yarn-site.xml': {
                'yarn.timeline-service.store-class':
                    'org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore',
                'yarn.timeline-service.version': '1.5',
                'yarn.timeline-service.entity-group-fs-store.active-dir': active_dir,
                'yarn.timeline-service.entity-group-fs-store.done-dir': done_dir,
                'yarn.timeline-service.entity-group-fs-store.threads': '16',
                'yarn.timeline-service.entity-group-fs-store.scan-interval-seconds': '10',
                'yarn.timeline-service.entity-group-fs-store.retain-seconds': '655200',
                'yarn.timeline-service.entity-group-fs-store.unknown-active-seconds': '86400',
                'yarn.timeline-service.entity-group-fs-store.cleaner-interval-seconds': '120',
                'yarn.timeline-service.leveldb-timeline-store.path': timeline_store_path,
                'yarn.timeline-service.plugin.enabled': True,
                'yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes':
                    'org.apache.tez.dag.history.logging.ats.TimelineCachePluginImpl',
                'yarn.timeline-service.address': cls.getATSHost() + ':10200'
            }
        }

        return copy.deepcopy(yarn_ats_config)

    @classmethod
    @TaskReporter.report_test()
    def check_if_component_is_tez_v15(cls):
        '''
        Checks whether NAT/tests are running with COMPONENT set to Tez_v15 in conf/report.conf
        :return: True if component is Tez_v15 else False
        '''
        report_conf = os.path.join(Config.getEnv('WORKSPACE'), 'conf', 'report.conf')
        if os.path.isfile(report_conf):
            logging.info("Going to Parse file %s", str(report_conf))
            config = ConfigParser()
            config.optionxform = str
            config.read(report_conf)
            section = "HW-QE-PUBLISH-REPORT"
            if config.has_section(section):
                component = config.get(section, "COMPONENT")
                logging.info("File %s contains %s with value %s", report_conf, section, component)
                if component is not None:
                    component = component.lower()
                if "tez_v15" in str(component):
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def check_if_component_is_tez_v2(cls):
        '''
        Checks whether NAT/tests are running with COMPONENT set to Tez_v15 in conf/report.conf
        :return: True if component is Tez_v15 else False
        '''
        report_conf = os.path.join(Config.getEnv('WORKSPACE'), 'conf', 'report.conf')
        if os.path.isfile(report_conf):
            logging.info("Going to Parse file %s", str(report_conf))
            config = ConfigParser()
            config.optionxform = str
            config.read(report_conf)
            section = "HW-QE-PUBLISH-REPORT"
            if config.has_section(section):
                component = config.get(section, "COMPONENT")
                logging.info("File %s contains %s with value %s", report_conf, section, component)
                if component is not None:
                    component = component.lower()
                if "tez_v2" in str(component):
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def deploy_yarn_configs_for_ats_v1_5(cls, restart_resource_manager=True, wait=15, sleep_time_after_restart=15):
        '''
        Deploys ATS V1.5 configs on Cluster (at default config location), and restart Timeline server,
        optinally restart ResourceManager
        CAUTION:  This method deploys ATS V1.5 configs on cluster by over-writting default location,
        without taking any backup
        :param restart_resource_manager: yes RM if True
        :param wait: sleep time between stopping and starting of TImeline server/ATS and/or RM server
        :param sleep_time_after_restart:  sleep after starting of Timeline serve /ATS and/or RM server
        :return: None
        '''
        host_set = {'services': ['gateway']}
        if Machine.isWindows():
            host_set = {'services': ['all']}
        yarn_confs = cls.get_timeline_server_v1_5_configs()
        Hadoop.modifyConfig(yarn_confs, host_set, True, False)
        if Machine.isLinux():
            mod_conf_path = Hadoop.getModifiedConfigPath()
            admin_user = Machine.getAdminUser()
            admin_passwd = Machine.getAdminPasswd()
            conf_path = Config.get('hadoop', 'HADOOP_CONF')
            src_yarn_conf_path = os.path.join(mod_conf_path, 'yarn-site.xml')
            dest_path = os.path.join(conf_path, 'yarn-site.xml')
            all_nodes = Hadoop.getSelectedNodes({'services': ['all']})
            logger.info("All nodes: %s  ", str(all_nodes))
            for host in all_nodes:
                Machine.copyFromLocal(admin_user, host, src_yarn_conf_path, dest_path, admin_passwd)
                Machine.chmod("755", dest_path, user=admin_user, host=host, passwd=admin_passwd)
        YARN.restartATSServer(wait=wait)
        time.sleep(sleep_time_after_restart)
        if restart_resource_manager:
            YARN.restartResourceManager(wait=wait)
            time.sleep(sleep_time_after_restart)

    @classmethod
    @TaskReporter.report_test()
    def fetch_timeline_delegation_token(cls, user=None):
        if not Hadoop.isSecure():
            return None
        if user is None:
            return None
        if not cls.is_ats_running():
            return None
        ws_url = cls.get_ats_web_app_address() + "/ws/v1/timeline" + "?op=GETDELEGATIONTOKEN&renewer=yarn"
        ret_code, data, _ = util.query_yarn_web_service(ws_url, user)
        logger.debug("Timeline: " + str(ret_code) + ", " + str(data))
        logger.debug("Fetched Delegation for timeline service returned %s %s", str(ret_code), str(data))
        if ret_code == 200 and data is not None and isinstance(data, dict):
            if 'Token' in data and data['Token'] is not None:
                if isinstance(data['Token'], dict) and 'urlString' in data['Token']:
                    if data['Token']['urlString'] is not None and data['Token']['urlString']:
                        return data['Token']['urlString']
        return None

    @classmethod
    @TaskReporter.report_test()
    def getTimelineLogFile(cls):
        '''
        getTimelineLogFile
      '''
        file_prefix = 'yarn-timelineserver-*.log'
        # on linux the username is part of the file name
        if not Machine.isWindows():
            yarn_user = Config.get('hadoop', 'YARN_USER', 'yarn')
            file_prefix = 'yarn-%s-timelineserver-*.log' % yarn_user

        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=cls.getATSHost(),
            filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
            searchstr=file_prefix,
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        if lines and lines[0]:
            return lines[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def copyTimelineserverLogToLocalMachine(cls):
        '''
        Copy Timelineserver log to local machine in artifacts dir
        '''
        TimelineLogFile = cls.getTimelineLogFile()
        localTimelineLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-timeline-' + str(int(time.time())) + '.log'
        )
        user = Machine.getAdminUser()
        passwd = Machine.getAdminPasswd()
        # copy the file to local machine
        Machine.copyToLocal(
            user=user, host=cls.getATSHost(), srcpath=TimelineLogFile, destpath=localTimelineLogFile, passwd=passwd
        )
        Machine.chmod("777", localTimelineLogFile, user=user, passwd=passwd)
        return localTimelineLogFile

    @classmethod
    @TaskReporter.report_test()
    def checkActivedirmovedtoDonedir(cls, appID, user=HRT_QA):
        '''
        confirm if application data has moved from ats active dir or not
        '''
        localfile = cls.copyTimelineserverLogToLocalMachine()
        active_dir = cls.getConfigValue("yarn.timeline-service.entity-group-fs-store.active-dir", "/tmp/ats/active")
        done_dir = cls.getConfigValue(
            "yarn.timeline-service.entity-group-fs-store.done-dir", "/ats/entity-file-history/done"
        )
        done_dir_extra_path = "0000/000"
        done_dir_app_folder = re.match("application_([0-9]+)_[0-9]+", appID)
        done_path = done_dir + '/?%s/%s/%s' % (done_dir_app_folder.group(1), done_dir_extra_path, appID)
        if Machine.isHumboldt():
            active_path = Hadoop.getFSDefaultValue() + active_dir + '/' + user + '/?' + appID
        else:
            active_path = Hadoop.getFSDefaultValue() + active_dir + '/?' + appID

        msg = "Moved %s to %s" % (active_path, done_path)
        logger.info("** debug msg = %s", msg)
        with open(localfile, 'r') as f:
            for line in f:
                if re.search(msg, line):
                    logger.info(line)
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def killLocalApplications(cls, filter_message, user=None, host=None):
        '''
        kill applications started in Local mode.
        :param filter_message: message which helps to find the pid. such as applicationname "PageViewStream"
        :param user: user who started application
        :param host: Host machine
        '''
        if not host:
            host = HDFS.getGateway()
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        if Machine.isLinux():
            pids = Machine.getProcessListRemote(
                host, format="%U %p %P %a", filter="'%s'" % filter_message, user=user, logoutput=True
            )
            while len(pids) >= 1:
                pid = Machine.getPidFromString(pids[0], user)
                Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
                pids = Machine.getProcessListRemote(
                    host, format="%U %p %P %a", filter="'%s'" % filter_message, user=user, logoutput=True
                )
        else:
            pid = Machine.getProcessListWithPid(host, "java.exe", filter_message, logoutput=True, ignorecase=True)
            if pid:
                Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)

    @classmethod
    @TaskReporter.report_test()
    def set_cluster_max_application_priority(cls, priority):
        Hadoop.modifyConfig(
            {
                YARN.YARN_SITE_XML: {
                    YARN.CLUSTER_MAX_APPLICATION_PROPERTY: priority
                }
            }, {'services': ['jobtracker', 'tasktrackers', 'yarnclient']},
            useAmbari=True,
            restartService=True
        )

    @classmethod
    @TaskReporter.report_test()
    def get_cluster_max_application_priority(cls):
        cluster_max_application_priority = cls.getConfigValue(YARN.CLUSTER_MAX_APPLICATION_PROPERTY, useAmbari=True)
        if cluster_max_application_priority is None:
            assert False, YARN.CLUSTER_MAX_APPLICATION_PROPERTY + " not set"
        return int(cluster_max_application_priority)

    @classmethod
    @TaskReporter.report_test()
    def update_application_priority_cli(cls, app_id, priority, user=None):
        if user is None:
            user = YARN.getYarnUser()
        YARN.runas(user, "application  -appId " + app_id + " -updatePriority " + str(priority))

    @classmethod
    @TaskReporter.report_test()
    def get_application_priority_via_rest(cls, app_id):
        priority_url = YARN.getResourceManagerWebappAddress() + \
                       "/ws/v1/cluster/apps/%s/priority" % app_id
        result = getHTTPResponse("JSON", priority_url)
        priority = getAttribute("JSON", result, "priority")
        return int(priority)

    @classmethod
    @TaskReporter.report_test()
    def get_queue_max_application_priority(cls, queue):
        queue_priority_property = queue + "." + YARN.DEFAULT_APPLICATION_PRIORITY
        queue_priority = cls.getConfigValue(queue_priority_property)
        if queue_priority is None:
            assert False, queue_priority_property + " not set"
        return int(queue_priority)

    @classmethod
    @TaskReporter.report_test()
    def set_queue_default_application_priority(cls, queue, priority):
        capacity_properties = {queue + "." + YARN.DEFAULT_APPLICATION_PRIORITY: priority}
        Hadoop.modifyConfig(
            {
                YARN.CAPACITY_SCHEDULER_XML: capacity_properties
            }, {'services': ['jobtracker']},
            useAmbari=True,
            restartService=True
        )

    @classmethod
    @TaskReporter.report_test()
    def set_application_priority_acls(cls, queue, acls):
        capacity_properties = {queue + "." + YARN.ACL_APPLICATION_MAX_PRIORITY: acls}
        Hadoop.modifyConfig(
            {
                YARN.CAPACITY_SCHEDULER_XML: capacity_properties
            }, {'services': ['jobtracker']},
            useAmbari=True,
            restartService=True
        )

    @classmethod
    @TaskReporter.report_test()
    def verify_app_killed_with_timeout(cls, app_id, user=HRT_QA):
        YARN.waitForAppState(app_id, timeout=5, targetState="KILLED", user=user)
        assert YARN.getAppStateFromID(app_id, user=user) == "KILLED"
        assert YARN.getAppFinalStateFromID(app_id, user=user) == "KILLED"
        app_info = YARN.getApplicationInfo(app_id, user=user)
        assert app_info[YARN.DIAGNOSTICS] == YARN.APPLICATION_KILLED_MESSAGE

    @classmethod
    @TaskReporter.report_test()
    def update_application_lifetime_cli(cls, app_id, timeout_value, user=None):
        if user is None:
            user = YARN.getYarnUser()
        if isinstance(timeout_value, int):
            return YARN.runas(user, "application -appId " + app_id + " -updateLifetime " + str(timeout_value))
        else:
            return YARN.runas(user, "application -appId " + app_id + " -updateLifetime " + timeout_value)

    @classmethod
    @TaskReporter.report_test()
    def get_application_timeouts(cls, app_id, user=None):
        if user is None:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        timeouts_url = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster/apps/" + app_id + \
                      "/" + YARN.TIMEOUTS + "?user.name=" + user
        response = getHTTPResponse("JSON", timeouts_url)
        timeouts = getAttribute("JSON", response, YARN.TIMEOUTS)
        return timeouts

    @classmethod
    @TaskReporter.report_test()
    def get_application_lifetime_timeout(cls, app_id, timeout_type="LIFETIME", user=None):
        if user is None:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        timeout_url = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster/apps/" + app_id + \
                      "/" + YARN.TIMEOUTS + "/" + timeout_type + "?user.name=" + user
        response = getHTTPResponse("JSON", timeout_url)
        timeout = getAttribute("JSON", response, YARN.TIMEOUT)
        expiry_time = timeout['expiryTime']
        remaining_time = timeout['remainingTimeInSeconds']
        return expiry_time, remaining_time

    @classmethod
    @TaskReporter.report_test()
    def get_app_expiry_time(cls, app_id):
        app_info = YARN.getApplicationInfo(app_id)
        timeouts = app_info[YARN.TIMEOUTS]
        timeout_array = timeouts[YARN.TIMEOUT]
        for timeout in timeout_array:
            if timeout['type'] == YARN.LIFETIME:
                return timeout['expiryTime']
        return None

    @classmethod
    @TaskReporter.report_test()
    def update_application_lifetime_rest(cls, app_id, expiry_time, user=None):
        if user is None:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        timeout_url = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster/apps/" + app_id + \
                      "/" + YARN.TIMEOUT
        timeout = {"type": YARN.LIFETIME, "expiryTime": expiry_time}
        json_data = {"timeout": timeout}
        status_code, response, _ = query_yarn_web_service(
            ws_url=timeout_url, data=json_data, user=user, http_method='PUT'
        )
        assert status_code == 200
        updated_timeout = getAttribute('JSON', response, YARN.TIMEOUT)
        return updated_timeout

    @classmethod
    @TaskReporter.report_test()
    def get_expiry_time_for_app_from_now(cls, delta_in_seconds=60):
        now = datetime.datetime.now()
        now_plus_one_min = now + datetime.timedelta(seconds=delta_in_seconds)
        year = now_plus_one_min.strftime("%Y")
        month = now_plus_one_min.strftime("%m")
        day = now_plus_one_min.strftime("%d")
        hour = now_plus_one_min.strftime("%H")
        minute = now_plus_one_min.strftime("%M")
        sec = now_plus_one_min.strftime("%S")
        expiry_time = str(year) + '-' + str(month) + '-' + str(day) + 'T' + str(hour) + ':' + \
                      str(minute) + ':' + str(sec) + '.000+0000'
        return expiry_time

    @classmethod
    @TaskReporter.report_test()
    def refreshRMQueueswithAmbari(cls, properties, expected_to_fail=False):
        '''
        Do refreshQueue operation from ambari api
        :param properties: CS property
        :return:
        '''
        Hadoop.modifyConfigWithAmbari(
            {
                'capacity-scheduler.xml': properties
            }, {'services': ['jobtracker']}, restartService=False
        )
        Ambari.refreshQueuesFromAmbari(expected_to_fail=expected_to_fail)

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_base_url(cls, cluster_name=None):
        """
        Return either
        /ws/v2/timeline/clusters/{cluster name} OR
        /ws/v2/timeline/ based on the cluster name specified
        """
        base_url = YARN.get_ats_v2_reader_web_app_address() + YARN.ATS_V2_BASE_URL
        if cluster_name is not None:
            base_url += (YARN.MULTI_CLUSTER_URL % cluster_name)
        return base_url

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_about_page(cls, run_as_user=None):
        """
        GET /ws/v2/timeline/
        """
        if not run_as_user:
            run_as_user = Config.get('hadoop', 'HADOOPQA_USER')
        status_code, response, _ = query_yarn_web_service(user=run_as_user, ws_url=YARN.ats_v2_get_base_url())
        return status_code, response

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_flows(cls, cluster_name=None, limit=None, date_range=None, from_id=None, user=None):
        """
        GET /ws/v2/timeline/flows
        GET /ws/v2/timeline/clusters/{cluster_name}/flows
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name)
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        url_params = ats_v2_url_encode(limit=limit, daterange=date_range, fromid=from_id)
        status_code, flows, _ = query_yarn_web_service(ws_url + YARN.FLOWS + url_params, user, max_time=120)
        return status_code, flows

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_flow_runs(
            cls,
            user,
            flow_name,
            cluster_name=None,
            limit=None,
            created_time_start=None,
            created_time_end=None,
            metrics_to_retrieve=None,
            fields=None,
            from_id=None
    ):
        """
        GET /ws/v2/timeline/users/{user}/flows/{flow_name}/runs
        GET /ws/v2/timeline/clusters/{cluster_name}/users/{user}/flows/{flow_name}/runs
        """
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        ws_url = YARN.ats_v2_get_base_url(cluster_name) + YARN.FLOW_RUNS % (user, quote(flow_name))
        url_params = ats_v2_url_encode(
            limit=limit,
            createdtimestart=created_time_start,
            createdtimeend=created_time_end,
            metricstoretrieve=metrics_to_retrieve,
            fields=fields,
            fromid=from_id
        )
        status_code, flow_runs, _ = query_yarn_web_service(ws_url + url_params, user, max_time=120)
        return status_code, flow_runs

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_flow_run(cls, user, flow_name, run_id, cluster_name=None, metrics_to_retrieve=None):
        """
        GET /ws/v2/timeline/users/{user}/flows/{flow_name}/runs/{run_id}
        GET /ws/v2/timeline/clusters/{cluster_name}/users/{user}/flows/{flow_name}/runs/{run_id}
        """
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        ws_url = YARN.ats_v2_get_base_url(cluster_name) + YARN.FLOW_RUN % (user, quote(flow_name), run_id)
        url_params = ats_v2_url_encode(metricstoretrieve=metrics_to_retrieve)
        status_code, flow_run, _ = query_yarn_web_service(ws_url + url_params, user, max_time=120)
        return status_code, flow_run

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_apps_for_a_flow(
            cls,
            user,
            flow_name,
            cluster_name=None,
            limit=None,
            created_time_start=None,
            created_time_end=None,
            relates_to=None,
            is_related_to=None,
            info_filters=None,
            conf_filters=None,
            metric_filters=None,
            event_filters=None,
            metrics_to_retrieve=None,
            conf_store_retrieve=None,
            fields=None,
            metrics_limit=None,
            metrics_Time_Start=None,
            metrics_Time_End=None,
            from_id=None
    ):
        """
        GET /ws/v2/timeline/users/{user}/flows/{flow_name}/apps
        GET /ws/v2/timeline/clusters/{cluster_name}/users/{user}/flows/{flow_name}/apps
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) +\
                 YARN.APPS_FOR_A_FLOW % (user, quote(flow_name))
        url_params = ats_v2_url_encode(
            limit=limit,
            createdtimestart=created_time_start,
            createdtimeend=created_time_end,
            relatesto=relates_to,
            isrelatedto=is_related_to,
            infofilters=info_filters,
            conffilters=conf_filters,
            metricfilters=metric_filters,
            eventfilters=event_filters,
            metricstoretrieve=metrics_to_retrieve,
            confstoretrieve=conf_store_retrieve,
            fields=fields,
            metricslimit=metrics_limit,
            metrics_Time_Start=metrics_Time_Start,
            metrics_Time_End=metrics_Time_End,
            fromid=from_id
        )
        status_code, apps, _ = query_yarn_web_service(ws_url + url_params, user, max_time=120)
        return status_code, apps

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_apps_for_a_flow_run(
            cls,
            user,
            flow_name,
            run_id,
            cluster_name=None,
            limit=None,
            created_time_start=None,
            created_time_end=None,
            relates_to=None,
            is_related_to=None,
            info_filters=None,
            conf_filters=None,
            metric_filters=None,
            event_filters=None,
            metrics_to_retrieve=None,
            conf_store_retrieve=None,
            fields=None,
            metrics_limit=None,
            metrics_Time_Start=None,
            metrics_Time_End=None,
            from_id=None
    ):
        """
        GET /ws/v2/timeline/users/{user}/flows/{flow_name}/runs/{run_id}/apps
        GET /ws/v2/timeline/clusters/{cluster_name}/users/{user}/flows/{flow_name}/runs/{run_id}/
            apps
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) + \
                 YARN.APPS_FOR_A_FLOW_RUN % (user, quote(flow_name), run_id)
        url_params = ats_v2_url_encode(
            limit=limit,
            createdtimestart=created_time_start,
            createdtimeend=created_time_end,
            relatesto=relates_to,
            isrelatedto=is_related_to,
            infofilters=info_filters,
            conffilters=conf_filters,
            metricfilters=metric_filters,
            eventfilters=event_filters,
            metricstoretrieve=metrics_to_retrieve,
            confstoretrieve=conf_store_retrieve,
            fields=fields,
            metricslimit=metrics_limit,
            metrics_Time_Start=metrics_Time_Start,
            metrics_Time_End=metrics_Time_End,
            fromid=from_id
        )
        status_code, apps, _ = query_yarn_web_service(ws_url + url_params, user, max_time=120)
        return status_code, apps

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_app(
            cls,
            app_id,
            cluster_name=None,
            user_id=None,
            flow_name=None,
            flow_run_id=None,
            metrics_to_retrieve=None,
            conf_store_retrieve=None,
            fields=None,
            metrics_limit=None,
            metrics_Time_Start=None,
            metrics_Time_End=None,
            run_as_user=None
    ):
        """
        GET /ws/v2/timeline/apps/{app_id}
        GET /ws/v2/timeline/clusters/{cluster_name}/apps/{app_id}
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) + YARN.APP % app_id
        url_params = ats_v2_url_encode(
            userid=user_id,
            flowname=flow_name,
            flowrunid=flow_run_id,
            metricstoretrieve=metrics_to_retrieve,
            confstoretrieve=conf_store_retrieve,
            fields=fields,
            metricslimit=metrics_limit,
            metrics_Time_Start=metrics_Time_Start,
            metrics_Time_End=metrics_Time_End
        )
        if not run_as_user:
            run_as_user = Config.get('hadoop', 'HADOOPQA_USER')
        status_code, app, _ = query_yarn_web_service(ws_url + url_params, run_as_user, max_time=120)
        return status_code, app

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_generic_entities_within_scope_of_app(
            cls,
            app_id,
            entity_type,
            cluster_name=None,
            user_id=None,
            flow_name=None,
            flow_run_id=None,
            limit=None,
            created_time_start=None,
            created_time_end=None,
            relates_to=None,
            is_related_to=None,
            info_filters=None,
            conf_filters=None,
            metric_filters=None,
            event_filters=None,
            metrics_to_retrieve=None,
            conf_store_retrieve=None,
            fields=None,
            metrics_limit=None,
            metrics_Time_Start=None,
            metrics_Time_End=None,
            from_id=None
    ):
        """
        GET /ws/v2/timeline/apps/{app_id}/entities/{entity_type}
        GET /ws/v2/timeline/clusters/{cluster_name}/apps/{app id}/entities/{entity_type}
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) +\
                 YARN.GENERIC_ENTITIES_APP % (app_id, entity_type)
        url_params = ats_v2_url_encode(
            userid=user_id,
            flowname=flow_name,
            flowrunid=flow_run_id,
            limit=limit,
            createdtimestart=created_time_start,
            createdtimeend=created_time_end,
            relatesto=relates_to,
            isrelatedto=is_related_to,
            infofilters=info_filters,
            conffilters=conf_filters,
            metricfilters=metric_filters,
            eventfilters=event_filters,
            metricstoretrieve=metrics_to_retrieve,
            confstoretrieve=conf_store_retrieve,
            fields=fields,
            metricslimit=metrics_limit,
            metrics_Time_Start=metrics_Time_Start,
            metrics_Time_End=metrics_Time_End,
            fromid=from_id
        )
        if not user_id:
            user_id = Config.get('hadoop', 'HADOOPQA_USER')
        status_code, entities, _ = query_yarn_web_service(ws_url + url_params, user_id, max_time=120)
        return status_code, entities

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_generic_entity_within_scope_of_app(
            cls,
            app_id,
            entity_type,
            entity_id,
            cluster_name=None,
            user_id=None,
            flow_name=None,
            flow_run_id=None,
            limit=None,
            created_time_start=None,
            created_time_end=None,
            relates_to=None,
            is_related_to=None,
            info_filters=None,
            conf_filters=None,
            metric_filters=None,
            event_filters=None,
            metrics_to_retrieve=None,
            conf_store_retrieve=None,
            fields=None,
            metrics_limit=None,
            metrics_Time_Start=None,
            metrics_Time_End=None,
            from_id=None
    ):
        """
        GET /ws/v2/timeline/apps/{app_id}/entities/{entity_type}/{entity_id}
        GET /ws/v2/timeline/clusters/{cluster_name}/apps/{app_id}/entities/{entity_type}/{entity_id}
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) +\
                 YARN.GENERIC_ENTITY_APP % (app_id, entity_type, entity_id)
        url_params = ats_v2_url_encode(
            userid=user_id,
            flowname=flow_name,
            flowrunid=flow_run_id,
            limit=limit,
            createdtimestart=created_time_start,
            createdtimeend=created_time_end,
            relatesto=relates_to,
            isrelatedto=is_related_to,
            infofilters=info_filters,
            conffilters=conf_filters,
            metricfilters=metric_filters,
            eventfilters=event_filters,
            metricstoretrieve=metrics_to_retrieve,
            confstoretrieve=conf_store_retrieve,
            fields=fields,
            metricslimit=metrics_limit,
            metrics_Time_Start=metrics_Time_Start,
            metrics_Time_End=metrics_Time_End,
            fromid=from_id
        )
        if not user_id:
            user_id = Config.get('hadoop', 'HADOOPQA_USER')
        status_code, entity, _ = query_yarn_web_service(ws_url + url_params, user_id, max_time=120)
        return status_code, entity

    @classmethod
    @TaskReporter.report_test()
    def ats_v2_get_generic_entity_types(
            cls, app_id, cluster_name=None, user_id=None, flow_name=None, flow_run_id=None
    ):
        """
        GET /ws/v2/timeline/apps/{app_id}/entity-types
        GET /ws/v2/timeline/clusters/{cluster_name}/apps/{app_id}/entity-types
        """
        ws_url = YARN.ats_v2_get_base_url(cluster_name) + YARN.GENERIC_ENTITY_TYPES % app_id
        url_params = ats_v2_url_encode(userid=user_id, flowname=flow_name, flowrunid=flow_run_id)
        user = Config.get('hadoop', 'HADOOPQA_USER')
        status_code, entity_types, _ = query_yarn_web_service(ws_url + url_params, user, max_time=120)
        return status_code, entity_types

    @classmethod
    @TaskReporter.report_test()
    def validate_flow_fields(cls, flows, cluster_name):
        """
        Verify basic flow information in the specified flows
        """
        for i in xrange(len(flows)):
            flow = flows[i]
            assert flow['type'] == 'YARN_FLOW_ACTIVITY'
            info = flow['info']
            assert info['SYSTEM_INFO_CLUSTER'] == cluster_name
            assert info['SYSTEM_INFO_FLOW_NAME'] is not None
            assert info['SYSTEM_INFO_USER'] == HRT_QA

    @classmethod
    @TaskReporter.report_test()
    def verify_apps_for_a_flow(cls, app_id, start_time):
        """
        Verify basic information for app created as part of the flow 'Sleep job'
        """
        status_code, apps = YARN.ats_v2_get_apps_for_a_flow(flow_name=YARN.DEFAULT_MR_FLOW_NAME, limit=1, user=HRT_QA)
        assert status_code == 200
        assert len(apps) == 1
        app = apps[0]
        assert app['type'] == YARN.YARN_APPLICATION
        assert app['id'] == app_id
        assert app['createdtime'] == start_time

    @classmethod
    @TaskReporter.report_test()
    def verify_app_info(cls, app_id, start_time, diagnostics=None):
        """
        Verify FINISHED/SUCCEEDED app's info from ATSv2
        """
        status_code, app = YARN.ats_v2_get_app(app_id=app_id, fields='INFO')
        assert status_code == 200
        assert app['createdtime'] == start_time
        assert app['id'] == app_id
        attempt_id = YARN.createAttemptIdFromAppId(appId=app_id, attemptNum=1)
        info = app['info']
        assert info['YARN_APPLICATION_NAME'] == YARN.DEFAULT_MR_FLOW_NAME
        assert info['YARN_APPLICATION_TYPE'] == YARN.MAPREDUCE
        assert info['YARN_APPLICATION_LATEST_APP_ATTEMPT'] == attempt_id
        if diagnostics:
            assert info['YARN_APPLICATION_DIAGNOSTICS_INFO'] == diagnostics
        assert info['YARN_APPLICATION_STATE'] == 'FINISHED'
        assert info['YARN_APPLICATION_FINAL_STATUS'] == 'SUCCEEDED'

    @classmethod
    @TaskReporter.report_test()
    def verify_app_events(cls, app_id, is_app_recovered=False):
        """
        Verify FINISHED/SUCCEEDED app's events from ATSv2
        """
        status_code, app = YARN.ats_v2_get_app(app_id=app_id, fields='EVENTS')
        assert status_code == 200
        events = app['events']
        app_state_updated_count = 0
        is_app_finished = False
        for event in events:
            if event['id'] == YARN.YARN_APPLICATION_STATE_UPDATED:
                app_state_updated_count += 1
                event_info = event['info']
                assert event_info[YARN.YARN_APPLICATION_STATE] == 'RUNNING'
            elif event['id'] == YARN.YARN_APPLICATION_FINISHED:
                is_app_finished = True
        if is_app_recovered:
            assert app_state_updated_count == 2
        else:
            assert app_state_updated_count == 1
        assert is_app_finished

    @classmethod
    @TaskReporter.report_test()
    def get_node_allocation_tag_info(cls, node_id):
        node_url = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster/nodes/" + node_id
        response = getHTTPResponse('JSON', node_url)
        node = response['node']
        allocation_tags = node[YARN.ALLOCATION_TAGS]
        if not allocation_tags or YARN.ALLOCATION_TAG_INFO not in allocation_tags:
            return None
        return allocation_tags[YARN.ALLOCATION_TAG_INFO]

    @classmethod
    @TaskReporter.report_test()
    def get_containers(cls, appId, user=None, logoutput=True):
        """
        Get container list using ATS v2 api
        """
        status_code, entities = cls.ats_v2_get_generic_entities_within_scope_of_app(
            app_id=appId, entity_type=YARN.YARN_CONTAINER, user_id=user
        )
        if logoutput:
            logger.info("Entities=%s", entities)
            logger.info("status_code=%s", status_code)
        assert status_code == 200, "Atsv2 call to get containers failed"
        container_list = []
        for container in entities:
            container_list.append(container['id'])
        return container_list

    @classmethod
    @TaskReporter.report_test()
    def wait_for_running_containers(cls, attempt_id, num_running_containers, user=None):
        """
        Wait for attempt id to have exactly the specified number of containers
        """
        i = 0
        while i < 20:
            num = YARN.getNumRunningContainersForAttempt(attempt_id, user)
            if num_running_containers == num:
                break
            time.sleep(6)
            i += 1
        assert i < 20, 'Expected number of containers: %d not created' % num_running_containers

    @classmethod
    @TaskReporter.report_test()
    def verify_allocation_info(cls, node_ids, container_tag, allocation_on_all_nodes=True):
        """
        Validate anti-affinity allocation info the node managers
        """
        for node_id in node_ids:
            allocation_tag_info = YARN.get_node_allocation_tag_info(node_id)
            if not allocation_on_all_nodes:
                if not allocation_tag_info:
                    continue
            else:
                assert allocation_tag_info, 'Allocation tag info not found for node %s' % node_id
            for allocation_tag in allocation_tag_info:
                allocation_tag_name = allocation_tag[YARN.ALLOCATION_TAG]
                assert allocation_tag_name == container_tag
                allocations_count = allocation_tag[YARN.ALLOCATIONS_COUNT]
                assert allocations_count == 1

    @classmethod
    @TaskReporter.report_test()
    def verify_container_anti_affinity(cls, app_id, num_running_containers, attempt_id, user=None):
        """
        Verify containers for the specified attempt id are created on different nodes
        """
        am_container_id = YARN.getAMContainerIdFromAppAttempt(app_id)
        container_ids, node_ids = YARN.getContainerIdAndHosts(app_id, filter_attemptId=attempt_id, user=user)
        nodes_found = []
        for i in xrange(len(container_ids)):
            container_id = container_ids[i]
            if container_id == am_container_id:
                continue
            _, stdout = YARN.getContainerStatus(container_id, user=user)
            if re.search('State : COMPLETE', stdout):
                continue
            node_id = node_ids[i]
            if node_id not in nodes_found:
                nodes_found.append(node_id)
            else:
                assert False, 'Container not expected on node %s' % node_id
        assert len(nodes_found) == num_running_containers
        return container_ids, node_ids

    @classmethod
    @TaskReporter.report_test()
    def wait_for_timeline_reader(cls, user):
        """
        Wait till the timeline reader about page can be loaded by the specified user
        """
        count = 0
        while count < 15:
            status_code, _ = YARN.ats_v2_get_about_page(run_as_user=user)
            if status_code == 200:
                return
            count += 1
            logger.info('Waiting for TimelineReader to be active..')
            time.sleep(12)
        assert count < 15, 'TimelineReader not up after 3 mins.'

    @classmethod
    @TaskReporter.report_test()
    def parseContainerLogForGPUInfo(cls, outputfile):
        '''
        Parse the container log which contains xml format of GPU info
        :input outputfile - xml format of GPU info fetched from task container log
        :return
        noOfGPU - no of GPUs used by that container
        gpus - GPU id used by that container
        '''
        tree = ET.parse(outputfile)
        root = tree.getroot()
        gpus = []
        for child in root:
            if child.tag == "attached_gpus":
                logger.info("No of GPUs -%s %s", child.tag, child.text)
                noOfGPU = child.text
            if child.tag == "gpu":
                logger.info("List of GPUs -%s %s", child.tag, child.attrib)
                gpus.append(child.attrib)
        return (noOfGPU, gpus)

    @classmethod
    @TaskReporter.report_test()
    def fetchGPUInfo(cls, appID, container):
        '''
        Fetch GPU information from the task container stdout logs
        :input
        appID - application ID
        container - task container which contains GPU info in xml format
        '''
        exit_code, stdout = YARN.getLogsApplicationID(appID, containerId=container, logFiles="stdout", logoutput=False)
        if exit_code != 0:
            return (0, {'id': '0'})
        ARTIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')
        nvidiaOutput = ARTIFACTS_DIR + os.sep + container
        for lines in stdout.split("\n"):
            if lines.strip().startswith("<"):
                util.writeToFile(lines + "\n", nvidiaOutput, isAppend=True)
        return cls.parseContainerLogForGPUInfo(nvidiaOutput)

    @classmethod
    @TaskReporter.report_test()
    def gpuInfoFromNMRestAPI(cls, node=None):
        '''
        Fetch which GPU is being assigned to the task container via NM Web Services
        :return
        gpuInfo[container] ={host:,gpuId:,noOfGPUs:}
        '''
        gpuInfo = {}
        tasktrackers = MAPRED.getTasktrackers()
        if node != None:
            tasktrackers = node
        for nm in tasktrackers:
            nmWebUrl = YARN.getNMWebappUrl(nm)
            url = nmWebUrl + "/ws/v1/node/resources/yarn.io%2Fgpu"
            logger.info("URL -- %s", url)
            resType = "JSON"
            res = getHTTPResponse(resType, url)
            gpuDevices = getElement(resType, res, "assignedGpuDevices")
            container = None
            noOFGpu = -1
            for GPU in gpuDevices:
                logger.info((resType, GPU))
                gpuIndex = (str(getValue(resType, getElement(resType, GPU, "index"))))
                gpuMinor = (str(getValue(resType, getElement(resType, GPU, "minorNumber"))))
                if container != (str(getValue(resType, getElement(resType, GPU, "containerId")))):
                    if not container:
                        noOFGpu = 1
                        container = (str(getValue(resType, getElement(resType, GPU, "containerId"))))
                        gpuInfo[container] = {'host': nm, 'gpu': [(gpuIndex, gpuMinor)], 'noOfGPU': noOFGpu}
                    else:
                        noOFGpu = 1
                        container = (str(getValue(resType, getElement(resType, GPU, "containerId"))))
                        gpuInfo[container] = {'host': nm, 'gpu': [(gpuIndex, gpuMinor)], 'noOfGPU': noOFGpu}
                else:
                    noOFGpu = 2
                    gpuInfo[container]['noOfGPU'] = noOFGpu
                    gpuInfo[container]['gpu'].append((gpuIndex, gpuMinor))

        return gpuInfo

    @classmethod
    @TaskReporter.report_test()
    def change_rm_log_level(cls, level='INFO'):
        log_level = '/logLevel?log=org.apache.hadoop.yarn.server.resourcemanager&level=%s' % level
        log_url = YARN.getResourceManagerWebappAddress() + log_level
        status_code, _, _ = query_yarn_web_service(log_url, user=YARN.getYarnUser())
        assert status_code == 200

    @classmethod
    @TaskReporter.report_test()
    def change_nm_log_level(cls, nm, level='INFO'):
        log_level = '/logLevel?log=org.apache.hadoop.yarn.server.nodemanager&level=%s' % level
        scheme = 'http'
        port = YARN.getNodeManagerWebappPort()
        if YARN.isHttpsEnabled():
            scheme = 'https'
            port = YARN.getNodeManagerWebappHttpsPort()
        log_url = '%s://%s:%s' % (scheme, nm, port) + log_level
        status_code, _, _ = query_yarn_web_service(log_url, user=YARN.getYarnUser())
        assert status_code == 200

    @classmethod
    @TaskReporter.report_test()
    def change_ats_log_level(cls, user=None, level='INFO'):
        log_level = '/logLevel?log=org.apache.hadoop.yarn.server.timeline&level=%s' % level
        log_url = YARN.get_ats_web_app_address() + log_level
        if not user:
            user = YARN.getYarnUser()
        status_code, _, _ = query_yarn_web_service(log_url, user=user)
        logger.info("status_code=%s", status_code)

    @classmethod
    def get_yarn_ats_keytab_file(cls):
        key_tab_file = os.path.join(Machine.getServiceKeytabsDir(), YARN.YARN_HBASE_KEYTAB_FILE)
        return key_tab_file

    @classmethod
    def get_yarn_ats_user_principal(cls, logoutput=True):
        if not Machine.isADMIT():
            return YARN.YARN_ATS_USER
        # Get user when is KDC type is not MIT
        klist_cmd = 'klist -kt ' + YARN.get_yarn_ats_keytab_file() + ' | head -4 | tail -1 | awk \'{print \\$4}\' ' \
                    '| cut -d \'@\' -f 1'
        _, ats_user_principal = Machine.runas(YARN.YARN_ATS_USER, klist_cmd, logoutput=logoutput)
        return ats_user_principal

    @classmethod
    @TaskReporter.report_test()
    def get_conf_from_RM(cls, Property, user=None):
        requiredProperty = '/conf?name=%s ' % Property
        conf_url = YARN.getResourceManagerWebappAddress() + requiredProperty
        if not user:
            user = YARN.getYarnUser()
        status_code, data, _ = query_yarn_web_service(conf_url, user=user)
        logger.info("status_code=%s", status_code)
        logger.info("data = %s", data)
        if status_code == 200:
            propValue = data["property"]["value"]
        else:
            propValue = None
        return (status_code, propValue)

    @classmethod
    @TaskReporter.report_test()
    def isKnoxProxySet(cls):
        isKnoxProxySet = Config.get("knox", "ENABLE_KNOX_PROXY")
        if isKnoxProxySet == "UI":
            return True
        return False


if Hadoop.isHadoop2():
    HDFS = HDFS2()
    MAPRED = MAPRED2()
else:
    HDFS = BaseHDFS()
    MAPRED = BaseMAPRED()
