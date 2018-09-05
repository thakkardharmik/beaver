#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import base64
import collections
import datetime
import logging
import os
import re
import time

from beaver import configUtils
from beaver import util
from beaver.component.dataStructure.storm_rest.storm_client import StormRestClient
from beaver.component.hdf_component import HdfComponent
from beaver.config import Config
from beaver.machine import Machine
from ambari import Ambari

ADMIN_USER = Machine.getAdminUser()
'''
Description:
Test API to work with Storm

Prerequisite:
Storm deployed and running
'''

logger = logging.getLogger(__name__)
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')
STORM_SERVICE_COMMON_CMD = "export PATH='/usr/sbin:/sbin:/usr/lib/ambari-server/*:/usr/sbin:/sbin:/usr/lib/ambari-server/*:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin:/var/lib/ambari-agent:/var/lib/ambari-agent:/usr/hdp/current/storm-client/bin' ; export PATH=$JAVA_HOME/bin:$PATH"

NimbusSummary = collections.namedtuple("NimbusSummary", "status logLink host version port upTime")
SupervisorSummary = collections.namedtuple("SupervisorSummary", "host id slotsTotal slotsUsed uptime version")
ExecutorAssignment = collections.namedtuple('ExecutorAssignment', "executorID host port nodeID startTime")
TopologyRun = collections.namedtuple('TopologyRun', "nimbuses leader nonLeaders executorAssignment startTime")
ProcessInfo = collections.namedtuple("ProcessInfo", "user pid ppid args")
ProcessOnMachine = collections.namedtuple("ProcessOnMachine", "host user pid ppid args")


class Storm:
    AMBARI_AUTH = "admin:HdpCli123!" if Machine.isHumboldt() else "admin:admin"
    AMBARI_AUTH_HEADER = {"Authorization": ("Basic %s" % base64.b64encode(AMBARI_AUTH))}
    exclFilters = ["grep", "bash", " -su ", "org.apache.storm.LogWriter"]

    @classmethod
    def getVersion(cls, useStandaloneCmd=True):
        '''
        Returns storm version.
        '''
        cmd = "cat " + os.path.join(cls.getStormHome(), "RELEASE")
        (_, stdout) = Machine.run(cmd)
        return stdout

    @classmethod
    def getModifiedConfigPath(cls):
        return os.path.join(Machine.getTempDir(), 'stormConf')

    @classmethod
    def isDalorBeyond(cls):
        v = cls.getShortVersion()
        expected_v = "2.3.0.0-0001"
        if v > expected_v:
            return True
        else:
            return False

    @classmethod
    def isAfterErie(cls):
        v = cls.getShortVersion()
        erie_start_version = "2.5.0.0-0000"
        return v > erie_start_version

    @classmethod
    def isAfterFenton(cls):
        v = cls.getShortVersion()
        fenton_start_version = "2.6.0.0-0000"
        return v > fenton_start_version

    @classmethod
    def getShortVersion(cls):
        '''
        Return short version.
        If version = 2.7.1.2.3.0.0-1675,
        :return: 2.3.0.0-1675
        '''
        version = cls.getVersion()
        return version.split(".", 3)[-1]

    @classmethod
    def isHA(cls):
        return cls.getNimbusSeeds()

    @classmethod
    def isSecured(cls):
        '''
        Returns True if storm is in secured cluster.
        '''
        return util.checkTextInFile("java.security.auth.login.config", cls.getStormYaml())

    @classmethod
    def run(cls, cmd, env=None, logoutput=True, useStandaloneCmd=False):
        '''
        Runs storm command with current user.
        Returns (exit_code, stdout)
        '''
        return cls.runas(None, cmd, env=env, logoutput=logoutput, useStandaloneCmd=useStandaloneCmd)

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True, host=None, useStandaloneCmd=False, skipAuth=False):
        '''
        Runs storm command with target user.
        Returns (exit_code, stdout).
        In standalone storm, always run storm bin.
        In slider storm, run "storm jar" or slider-storm-client depending on useStandaloneCmd param.
        '''
        if cls.isSecured() and not skipAuth:
            # other user is not tested yet. I dont know it will be same as hadoop where job owner depends solely on kerberos ticket used.
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user=user, rmIfExists=True)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        cmd = cls.getStormCmd(useStandaloneCmd=useStandaloneCmd) + " " + cmd
        return Machine.runas(user, cmd, host=host, env=env, logoutput=logoutput)

    @classmethod
    def runinbackground(cls, cmd, cwd=None, env=None, stdout=None, stderr=None, useStandaloneCmd=False):
        '''
        Runs storm commnad in background
        '''
        cmd = cls.getStormCmd(useStandaloneCmd) + " " + cmd
        return Machine.runinbackground(cmd, cwd=None, env=None, stdout=None, stderr=None)

    @classmethod
    def getStormHome(cls):
        '''
        Returns STORM_HOME ("/usr/hdp/current/storm-client").
        '''
        return Config.get('storm', 'STORM_HOME')

    @classmethod
    def getStormCmd(cls, useStandaloneCmd=False, userFlagIfSlider=None):
        '''
        Returns "/usr/lib/storm/bin/storm-slider.py" (slider and useStandaloneCmd=False) 
        or "/usr/bin/storm" (otherwise).
        '''
        # useStandaloneCmd parameter is useless & can be deleted
        cmd = Config.get('storm', 'STORM_CMD')
        if cls.isSecured():
            flag = " -c java.security.auth.login.config=%s" % cls.getClientJaasConf()
            short_version = cls.getShortVersion()
            #  pre erie namespace was backtype.storm
            if short_version < "2.5.0.0-000":
                namespace = "backtype.storm"
            else:
                namespace = "org.apache.storm"
            logger.info("short_version = %s namespace = %s " % (short_version, namespace))
            thrift_trasport = "%s.security.auth.kerberos.KerberosSaslTransportPlugin" % namespace
            flag += " -c storm.thrift.transport=%s" % thrift_trasport
            cmd += flag
        cmd += " -c client.jartransformer.class=nil"
        return cmd

    @classmethod
    def getStormConf(cls, file_name=None):
        '''
        returns path to storm conf dir (typically /etc/storm/conf) or the requested file under it
        '''
        conf_dir = Config.get('storm', 'STORM_CONF')
        if file_name:
            return os.path.join(conf_dir, file_name)
        return conf_dir

    @classmethod
    def getStormYaml(cls):
        '''
        returns a string for storm yaml. 
        '''
        return cls.getStormConf("storm.yaml")

    @classmethod
    def getClientJaasConf(cls):
        '''
        Returns full path of client_jaas.conf (client)
        '''
        return cls.getStormConf("client_jaas.conf")

    @classmethod
    def getStormJaasConf(cls):
        '''
        Returns full path of storm_jaas.conf (services)
        '''
        return cls.getStormConf("storm_jaas.conf")

    @classmethod
    def getStormKafkaExampleJar(cls):
        '''
        Returns a stirng of full path of storm-kafka-example jar
        '''
        return Config.get('storm', 'STORM_KAFKA_EXAMPLE_JAR')

    @classmethod
    def getStormStarterJar(cls):
        '''
        Returns a string of full path of storm-starter jar.
        '''
        return Config.get('storm', 'STORM_STARTER_JAR')

    @classmethod
    def runStormStarterTopology(cls, shortClassName, arg='', env=None, logoutput=True):
        '''
        Runs storm starter jar with current user.
        Returns (exit_code, stdout)
        '''
        cmd = 'jar %s org.apache.storm.starter.%s %s' % (cls.getStormStarterJar(), shortClassName, arg)
        return cls.run(cmd, env, logoutput)

    @classmethod
    def runStarterTopologySuccessfully(cls, shortClassName, arg='', env=None, logoutput=True):
        '''
        Runs storm starter jar with current user. Asserts that exit code is zero.
        Returns stdout
        '''
        exit_code, stdout = cls.runStormStarterTopology(
            shortClassName=shortClassName, arg=arg, env=env, logoutput=logoutput
        )
        assert exit_code == 0
        return stdout

    @classmethod
    def __submit_topology(cls, ClassName, args, env, inBackground, jarfile, logoutput, useStandaloneCmd):
        cmd = "jar %s %s %s" % (jarfile, ClassName, args)
        if inBackground:
            return cls.runinbackground(cmd, None, env, None, None, useStandaloneCmd)
        else:
            return cls.run(cmd, env, logoutput, useStandaloneCmd)

    @classmethod
    def runStormHdfsTopology(
            cls, jarfile, ClassName, args='', env=None, logoutput=True, inBackground=False, useStandaloneCmd=False
    ):
        '''
        Runs storm hdfs jar as current user
        Returns (exit_code, stdout)
        '''
        return cls.__submit_topology(ClassName, args, env, inBackground, jarfile, logoutput, useStandaloneCmd)

    @classmethod
    def runStormJdbcTopology(
            cls, jarfile, ClassName, args='', env=None, logoutput=True, inBackground=False, useStandaloneCmd=False
    ):
        '''
        Runs storm jdbc jar as current user
        Returns (exit_code, stdout)
        '''
        return cls.__submit_topology(ClassName, args, env, inBackground, jarfile, logoutput, useStandaloneCmd)

    @classmethod
    def runKafkaExampleTopology(
            cls, zknodes, topologyName='', env=None, logoutput=True, inBackground=False, useStandaloneCmd=True
    ):
        '''
        Runs storm kafka example, in local mode if topologyName is empty , in distributed mode if topologyName is not empty
        Returns (exit_code, stdout)
        '''
        kafka_example_jar = cls.getStormKafkaExampleJar()
        ClassName = 'org.apache.storm.kafka.TestKafkaTopology'
        isSecure = cls.isSecured()
        cmd = "jar %s %s %s %s %s" % (kafka_example_jar, ClassName, zknodes, isSecure, topologyName)
        print "cmd = ", cmd
        if inBackground:
            return cls.runinbackground(cmd, None, env, None, None, useStandaloneCmd)
        else:
            return cls.run(cmd, env, logoutput, useStandaloneCmd)

    @classmethod
    def getNimbus(cls, logoutput=True):
        '''
        Returns nimbus server host.

        This API is not safe to call anymore with nimbus HA can result in seed being a list.
        '''
        nimbus_seeds = cls.get_config_value("^nimbus[.]seeds", None, logoutput)
        if nimbus_seeds is None:
            nimbus_seeds = cls.get_config_value("^nimbus[.]host", None, logoutput)
        result = nimbus_seeds.strip('[').strip(']').strip("'").strip('"')
        if logoutput:
            logger.info("getNimbus returns %s" % result)
        return result

    @classmethod
    def getStormProcessesForHost(cls, host, grep_filter, logoutput=True):
        if logoutput:
            logger.info("getStormProcessesForHost: host=%s grep_filter=%s" % (host, grep_filter))
        lines = Machine.getProcessListRemote(
            host, filter=grep_filter, logoutput=False, useEGrep=True, exclFilters=cls.exclFilters
        )
        result = []  # type: list[ProcessInfo]
        if len(lines) == 0 or (len(lines) == 1 and lines[0] == ''):
            return result
        for line in lines:
            s = line.split()
            result.append(ProcessInfo(s[0], s[1], s[2], s[3]))
        if logoutput:
            logger.info("getStormProcessesForHost return %s" % result)
        return result

    @classmethod
    def getNimbusProcessesForHost(cls, host, logoutput=True):
        """ Get nimbus processes for a particular host """
        return cls.getStormProcessesForHost(host, "daemon.name=nimbus", logoutput=logoutput)

    @classmethod
    def getSupervisorProcessesForHost(cls, host, logoutput=True):
        """ Get supervisor processes for a particular host """
        return cls.getStormProcessesForHost(host, "daemon.name=supervisor", logoutput=logoutput)

    @classmethod
    def getUIProcessForHost(cls, host, logoutput=True):
        """ Get ui process for a particular host """
        result = cls.getStormProcessesForHost(host, "daemon.name=ui", logoutput)
        if result:
            return result[0]
        return None

    @classmethod
    def waitForNimbusProcess(cls, host, logoutput=True):
        """ Wait till nimbus is up. Raise error if nimbus is not up. """
        nimbus_processes = Storm.getNimbusProcessesForHost(host)
        for count in range(12):
            if nimbus_processes:
                break
            util.sleep(10)
            nimbus_processes = Storm.getNimbusProcessesForHost(host)
        assert nimbus_processes, "Nimbus did not come up on host: %s" % host

    @classmethod
    def checkSupervisorProcessIsUp(cls, host, logoutput=True):
        """ Check if supervisor process is up and retry. """
        supervisors = Storm.getSupervisorProcessesForHost(host)
        for i in range(5):
            if supervisors:
                break
            supervisors = Storm.getSupervisorProcessesForHost(host)
            time.sleep(10)
        return supervisors

    @classmethod
    def waitForLeaderElection(cls, logoutput=True):
        """
        Retry till leader nimbus is selected. Else raise error.
        :param logoutput:
        :return:
        :rtype: None or list of str
        """
        leader = cls.getRESTLeaderNimbus(logoutput=logoutput)
        for i in range(15):
            if leader:
                break
            leader = cls.getRESTLeaderNimbus(logoutput=logoutput)
            time.sleep(3)
        assert leader, "Leader nimbus is not selected yet."

    @classmethod
    def checkRetryUntilNoOfflineNimbuses(cls, logoutput=True):
        """
        Check if all nimbuses are not offline and retry.
        :param logoutput:
        :return: True if eventually no offline nimbuses are found
        :rtype: boolean
        """
        noOffline = False
        for retryCount in range(6):
            if noOffline:
                break
            try:
                nimbusTuples = cls.getRESTNimbusSummary(logoutput=logoutput)
                noOffline = True
                for nimbusTuple in nimbusTuples:
                    if nimbusTuple[0].upper() == "OFFLINE":
                        noOffline = False
            except Exception as ex:
                logger.info("Caught error: %s" % str(ex))
            finally:
                logger.info("noOffline = %s. retry count = %s" % (noOffline, retryCount))
                util.sleep(5)
        return noOffline

    @classmethod
    def getSupervisorsForLogUtil(cls, logoutput=True):
        """
        Return list of hosts of supervisor nodes.
        :param logoutput: String: True will log output to console.
        :return: list of str
        """
        result = util.getAllNodes()
        if logoutput:
            logger.info("getSupervisorsForLogUtil returns %s" % result)
        return result

    @classmethod
    def getSupervisors(cls, logoutput=True):
        '''
        Returns a list of supervisor hosts.
        Returns None if none is found.
        '''
        return cls._getSupervisorsByWebUI(logoutput, debug=True)

    @classmethod
    def _getSupervisorsByWebUI(cls, logoutput=True, debug=True):
        '''
        Returns a list of supervisor hosts with web ui contents.
        Returns None if none is found.
        '''
        # buggy in storm security as of Champlain 10/25/2014.
        url = cls.getWebUIUrl(logoutput) + "/api/v1/supervisor/summary"
        if debug:
            logger.info("web ui url = %s" % url)
        httpResponse = cls.getJsonWithRetry(url, True)
        #js_obj = json.loads(httpResponse)
        if httpResponse is not None and httpResponse.has_key("supervisors"):
            return [node["host"] for node in httpResponse["supervisors"]]
        return []

    @classmethod
    def get_json_from_url(cls, url, headers={}, debug=True, logoutput=True):
        import json
        if debug:
            logger.info("url = %s" % url)
        headers["Content-Type"] = "application/json"
        httpResponse = util.getURLContents(url, headers=headers, logoutput=logoutput)
        js_obj = json.loads(httpResponse)
        return js_obj

    @classmethod
    def getWebUIUrl(cls, logoutput=True):
        '''
        Returns "http://<storm ui host>:<port>"
        '''
        return "http://%s:%s" % (cls.getWebUIHost(logoutput), cls.getWebUIPort(logoutput))

    @classmethod
    def getAmbariClusterUrl(cls):
        if hasattr(cls, "_getAmbariClusterUrl"):
            return cls._getAmbariClusterUrl
        cls._getAmbariClusterUrl = Ambari.getWebUrl(is_hdp=False, is_enc=HdfComponent.get_ambari_server_ssl()) +\
                                   "/api/v1/clusters/%s/services/STORM" % HdfComponent.get_ambari_cluster_name()
        logger.info("ambari cluster endpoint = %s" % cls._getAmbariClusterUrl)
        assert cls._getAmbariClusterUrl, "Ambari cluster url could not be determined."
        return cls._getAmbariClusterUrl

    _web_ui_host = None

    @classmethod
    def getWebUIHost(cls, logoutput=True):
        '''
        Returns web ui host.
        '''
        if cls._web_ui_host:
            return cls._web_ui_host
        try:
            hosts = util.getAllInternalNodes(logoutput=logoutput)
        except:
            hosts = util.getAllNodes(logoutput=logoutput)
        logger.info("Storm.getWebUIHost searches for UI processes in %s" % hosts)
        result = None
        for host in hosts:
            if cls.getUIProcessForHost(host, logoutput=logoutput):
                result = host
                break
        if logoutput:
            logger.info("Storm.getWebUIHost returns %s" % result)
        cls._web_ui_host = result
        return result

    @classmethod
    def getWebUIHostForLogUtil(cls, logoutput=True):
        '''
        Returns web ui host for log util use.
        '''
        # It is risky to use process grep at last week of Dal.
        hosts = util.getAllNodes()
        if logoutput:
            logger.info("Storm.getWebUIHostForLogUtil returns %s" % hosts)
        return hosts

    @classmethod
    def getWebUIPort(cls, logoutput=True):
        '''
        Returns web ui port as an int.
        '''
        result = int(cls.get_config_value("^ui[.]port", 8080, logoutput))
        if logoutput:
            logger.info("getNimbus returns %s" % result)
        return result

    @classmethod
    def getNimbusSeeds(cls, logoutput=True):
        """
        Get nimbus seeds.
        :param logoutput:
        :return:
        :rtype: None or list of str
        """
        match_obj = util.findMatchingPatternInFileMultiLine(cls.getStormYaml(), 'nimbus[.]seeds\s*:\s*[\[](.*?)[\]]')
        result = None
        match_str = None
        if match_obj is None:
            result = None
        else:
            match_str = match_obj.group(1)
        if match_str is not None:
            result = [x.strip().strip("'") for x in match_str.split(',')]
        if logoutput:
            logger.info("getNimbusSeeds returns %s" % result)
        return result

    @classmethod
    def getRESTNimbusSummary(cls, logoutput=True):
        """
        Get nimbus summary json from http://<ui>:<port>/api/v1/nimbus/summary
        :param logoutput:
        :return: list of tuple
        """
        if logoutput:
            logger.info("Storm.getRESTNimbusSummary start")
        host = cls.getWebUIHost(logoutput=logoutput)
        port = cls.getWebUIPort(logoutput=logoutput)
        url = "http://%s:%s/api/v1/nimbus/summary" % (host, port)
        j = cls.getJsonWithRetry(url, logoutput=logoutput)
        tmp = j['nimbuses']
        r = []  # type: list[NimbusSummary]
        for nimbus in tmp:
            r.append(
                NimbusSummary(
                    nimbus['status'], nimbus['nimbusLogLink'], nimbus['host'], nimbus['version'], nimbus['port'],
                    nimbus['nimbusUpTime']
                )
            )
        if logoutput:
            logger.info("Storm.getRESTNimbusSummary returns:")
            for entry in r:
                logger.info("%s" % (entry, ))
        return r

    @classmethod
    def getJsonWithRetry(cls, url, logoutput):
        is_secured = cls.isSecured()
        jdict = util.getJSONWithOptionalSPNego(url, is_secured, logoutput=logoutput)
        for i in range(1, 7):
            if jdict and "errorMessage" not in jdict:
                break
            util.sleep(10)
            logger.info("Retrying curl command: %d" % i)
            jdict = util.getJSONWithOptionalSPNego(url, is_secured, logoutput=logoutput)
        return jdict

    @classmethod
    def getRESTSupervisorSummary(cls, logoutput=True):
        """
        Get supervisor summary json from http://<ui>:<port>/api/v1/supervisor/summary
        :param logoutput:
        :return: list of tuple
        """
        if logoutput:
            logger.info("Storm.getRESTSupervisorSummary start")
        host = cls.getWebUIHost(logoutput=logoutput)
        port = cls.getWebUIPort(logoutput=logoutput)
        url = "http://%s:%s/api/v1/supervisor/summary" % (host, port)
        j = cls.getJsonWithRetry(url, logoutput)
        tmp = j['supervisors']
        r = []  # type: list[SupervisorSummary]
        for supervisor in tmp:
            r.append(
                SupervisorSummary(
                    supervisor['host'], supervisor['id'], supervisor['slotsTotal'], supervisor['slotsUsed'],
                    supervisor['uptime'], supervisor['version']
                )
            )
        if logoutput:
            logger.info("Storm.getRESTSupervisorSummary returns:")
            for entry in r:
                logger.info("%s" % (entry, ))
        return r

    @classmethod
    def getRESTLeaderNimbus(cls, logoutput=True):
        """
        Get nimbus leader
        :param logoutput:
        :return:
        :return: None or a tuple

        """
        nimbus_summary = cls.getRESTNimbusSummary(logoutput=logoutput)
        leader_list = [i for i in nimbus_summary if str(i.status).upper() == "LEADER"]
        assert len(leader_list) < 2, "At most one leader can be present at a time."
        r = leader_list[0] if len(leader_list) == 1 else None
        if logoutput:
            logger.info("Storm.getRESTLeaderNimbus returns %s" % str(r))
        return r

    @classmethod
    def getRESTNonLeaderNimbuses(cls, logoutput=True):
        """
        Get non-leader nimbuses
        :param logoutput:
        :return:
        :return: [] or a list of tuples
        """
        nimbus_summary = cls.getRESTNimbusSummary(logoutput=logoutput)
        r = [i for i in nimbus_summary if str(i.status).upper() != "LEADER"]
        if logoutput:
            logger.info("Storm.getRESTNonLeaderNimbuses returns %s" % str(r))
        return r

    @classmethod
    def killTopology(cls, name, logoutput=True, useStandaloneCmd=False):
        '''
        Kills a topology.
        Returns True if exit code is 0.
        '''
        cmd = 'kill %s' % name
        (exit_code, stdout) = cls.run(cmd, env=None, logoutput=logoutput, useStandaloneCmd=useStandaloneCmd)
        return exit_code == 0 and 'Killed topology: %s' % name in stdout

    @classmethod
    def get_config_value(cls, key_regex, default_value=None, logoutput=False):
        result = util.getYamlValue(cls.getStormYaml(), key_regex, default_value, True, logoutput)
        if logoutput:
            logger.info("get_config_value for key_regex=%s returns %s" % (key_regex, result))
        return result

    @classmethod
    def getLogDir(cls, logoutput=False):
        '''
        Returns a string of full path of log directory.
        '''
        return cls.get_config_value("^storm[.]log[.]dir", "/var/log/storm", logoutput)

    @classmethod
    def getNimbusLogPath(cls):
        '''
        Returns a string of full path of nimbus log
        '''
        return os.path.join(cls.getLogDir(), "nimbus.log")

    @classmethod
    def getSupervisorLogPath(cls):
        '''
        Returns a string of full path of supervisor log
        '''
        return os.path.join(cls.getLogDir(), "supervisor.log")

    @classmethod
    def getUILogPath(cls):
        '''
        Returns a string of full path of UI log
        '''
        return os.path.join(cls.getLogDir(), "ui.log")

    @classmethod
    def getAllWorkerLogPaths(
            cls, host=None, latestLogOnly=True, acceptedPortList=[], logoutput=True, logDirIfIsSlider=None, topoName=""
    ):
        '''
        Return a list of paths of worker logs for given host.
        If host is None, it is gateway host.
        If acceptedPortList is not [], filter to get only listed ports.
        '''
        #e.g. mytopology-1-1410547189-worker-6703.log
        if latestLogOnly:
            path_pattern = "*%s*worker*.log" % topoName
        else:
            #incl. worker-6700.log.1 and such
            path_pattern = "*%s*worker*.log*" % topoName
        tmpList = Machine.find_path(
            user=Machine.getAdminUser(),
            host=host,
            search_dir=cls.getLogDir() + os.sep,
            path_pattern=path_pattern,
            passwd=Machine.getAdminPasswd(),
            logoutput=logoutput
        )
        if len(acceptedPortList) != 0:
            tmpList2 = []
            for port in acceptedPortList:
                for entry in tmpList:
                    if entry.find(port) > -1:
                        tmpList2.append(entry)
            tmpList = tmpList2
        if logoutput:
            logger.info("getAllWorkerLogPaths(%s) = %s" % (host, tmpList))
        return tmpList

    @classmethod
    def copyWorkerLogsToLocalMachine(
            cls,
            topoName=None,
            latestLogOnly=True,
            logoutput=True,
            logdebug=True,
            nimbusHost=None,
            executorAssignment=None
    ):
        '''
        Copy worker logs from supervisors to artifacts directory in local machine.
        Returns a list of path of copied files in artifacts directory.
        Returns [] if no file is found.
        If latestLogOnly is True, only files ending with .log are copied.

        If executorAssignment is not None, use the executor assignment. In the case, nimbus host can be ignored.
        '''
        logger.info("Storm.copyWorkerLogsToLocalMachine start")
        artifactsDir = Config.getEnv('ARTIFACTS_DIR')
        localPathList = []
        logLocations = {}
        if topoName is None:
            for workerHost in cls.getSupervisors():
                logLocations[workerHost] = []  #no filtering. accept all ports found.
            #executorAssignment is not implemented here yet.
        else:
            if executorAssignment is None:
                executorAssignment = cls.getExecutorAssignment(topoName, nimbusHost=nimbusHost)
            for entry in executorAssignment:
                if entry.host not in logLocations:
                    logLocations[entry.host] = []
                if entry.port not in logLocations[entry.host]:
                    logLocations[entry.host].append(entry.port)
        if logoutput:
            logger.info("copyWorkerLogsToLocalMachine: logLocations=%s" % logLocations)
        for workerHost in logLocations.keys():
            workerLogPaths = cls.getAllWorkerLogPaths(
                workerHost,
                latestLogOnly=latestLogOnly,
                acceptedPortList=logLocations[workerHost],
                logoutput=logoutput,
                topoName=topoName
            )
            util.dumpTextString(
                workerLogPaths, "==== Storm.copyWorkerLogsToLocalMachine workerLogPaths ====", "============="
            )
            for path in workerLogPaths:
                # /grid/0/log/storm/workers-artifacts/KillLeaderWhenTopologyIsRunning-7-1466810973/6700/worker.log
                fileName = "_".join(path.split(os.sep)[-3:])
                localFile = os.path.join(
                    artifactsDir, 'worker-%s-%s-%s.log' %
                    (util.getShortHostname(workerHost), fileName.split(".")[0], str(int(time.time())))
                )
                util.dumpTextString(path, "==== Storm.copyWorkerLogsToLocalMachine path to copy ====", "=============")
                Machine.copyToLocal(user=ADMIN_USER, host=workerHost, srcpath=path, destpath=localFile, passwd=None)
                Machine.chmod("a+r", localFile, recursive=True, user=ADMIN_USER)
                localPathList.append(localFile)
        if logoutput:
            logger.info("copyWorkerLogsToLocalMachine returns %s" % localPathList)
        return localPathList

    @classmethod
    def copyNimbusLogToLocalMachine(cls, logoutput=True, nimbusHost=None):
        '''
        Copy Nimbus logs to artifacts directory in local machine.
        Returns a string path of local nimbus log.

        :param: nimbusHost: if None, use default Nimbus.
        '''
        localFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'nimbus-%s.log' % (str(int(time.time()))))
        if os.path.exists("/etc/storm/conf/config.yaml"):
            # regular storm
            if nimbusHost is None:
                nimbus = cls.getNimbus()
            else:
                nimbus = nimbusHost
            Machine.copyToLocal(
                user=ADMIN_USER, host=nimbus, srcpath=cls.getNimbusLogPath(), destpath=localFile, passwd=None
            )
            Machine.chmod("a+r", localFile, recursive=True, user=ADMIN_USER)
        return localFile

    @classmethod
    def copySupervisorLogsToLocalMachine(cls, logoutput=True):
        '''
        Copy supervisors logs to artifacts directory in local machine.
        Returns a list of path of copied files in artifacts directory.
        Returns [] if no file is found.
        '''
        artifactsDir = Config.getEnv('ARTIFACTS_DIR')
        localPathList = []
        if os.path.exists("/etc/storm/conf/config.yaml"):
            # regular storm
            for workerHost in cls.getSupervisors():
                remotePath = cls.getSupervisorLogPath()
                localFile = os.path.join(
                    artifactsDir, 'supervisor-%s-%s.log' % (util.getShortHostname(workerHost), str(int(time.time())))
                )
                Machine.copyToLocal(
                    user=ADMIN_USER, host=workerHost, srcpath=remotePath, destpath=localFile, passwd=None
                )
                Machine.chmod("a+r", localFile, recursive=True, user=ADMIN_USER)
                localPathList.append(localFile)
        if logoutput:
            logger.info("copySupervisorLogsToLocalMachine returns %s" % localPathList)
        return localPathList

    @classmethod
    def getSupervisorSlotsPorts(cls):
        '''
        Returns a list of int referring to supervisor slots ports.
        Also means number of workers per machine.
        Ref: https://github.com/nathanmarz/storm/wiki/Setting-up-a-Storm-cluster
        '''
        #default to "6700, 6701, 6702, and 6703"
        return [6700, 6701, 6702, 6703]

    @classmethod
    def getExecutorAssignment(cls, topoName, logoutput=True, nimbusHost=None):
        '''
        Parses nimbus log to get topology's executor-host assignment.
        Returns a list of ExecutorAssignment objects representing
                (executor ID, executor host, port, node ID, startTime).
        Returns [] if contents are not found.
        e.g.
        ['2 2', 'hor17n29', 6700, "f6786fa3-6b18-465d-a541-4d9f45bd64e3", 1390669436]
        Note: start time is not implemented yet.
        
        One or more executors may run within a single worker process (port).
        API caller should be aware that 1 pair of host/port can appear more than once with different executor IDs.

        :param: nimbusHost: if None, use default Nimbus.
        '''
        if logoutput:
            logger.info("Storm.getExecutorAssignment nimbusHost=%s" % nimbusHost)
        assignmentLine = cls.getNimbusLogAssignmentLine(topoName, logoutput=logoutput, nimbusHost=nimbusHost)
        nodeIDHostMap = cls.getNodeIDHostMapping(assignmentLine)
        matchObj = re.search(':executor->node[+]port {(.*?)}', assignmentLine)
        executorNodePortMapString = matchObj.group(1)
        result = []  # type: list[ExecutorAssignment]
        for entry in executorNodePortMapString.split(","):
            entry = entry.strip()
            executorID = entry.split("] [")[0].strip('[').strip(']')
            nodePortMap = entry.split("] [")[1].strip('[').strip(']')
            nodeID = nodePortMap.split(" ")[0].strip('"')
            port = nodePortMap.split(" ")[1].strip('"')
            if Machine.isHumboldt():
                result.append(ExecutorAssignment(executorID, nodeIDHostMap[nodeID], port, nodeID, 'unimplemented'))
                continue
            result.append(
                ExecutorAssignment(
                    executorID, util.getShortHostname(nodeIDHostMap[nodeID]), port, nodeID, 'unimplemented'
                )
            )
        if logoutput:
            logger.info('getExecutorAssignment returns:')
            for entry in result:
                logger.info(entry)
        return result

    @classmethod
    def getExecutorAssignmentStrAfterTimestamp(cls, topoName, timestamp, logoutput=True, nimbusHost=None):
        """
        Get executor assignment line as str after timestamp
        :param topoName:
        :param timestamp: str in this format '2015-05-28 02:14:54'
        :param logoutput:
        :param nimbusHost:
        :return: None or str
        """
        if logoutput:
            logger.info("Storm.getExecutorAssignmentStrAfterTimestamp start")
        pattern = "Setting new assignment for topology id %s" % topoName
        assignmentLine = cls.getNimbusLogAssignmentLine(topoName, logoutput=logoutput, nimbusHost=nimbusHost)
        f = None
        nimbusLogPath = cls.copyNimbusLogToLocalMachine(logoutput=logoutput, nimbusHost=nimbusHost)
        #dateTimeFormat = "%Y-%m-%dT%H:%M:%S"
        #if Machine.isDebian():
        dateTimeFormat = "%Y-%m-%d %H:%M:%S"
        result = util.findMatchingPatternInFileAfterTimestamp(
            filename=nimbusLogPath, regex=pattern, timestamp=timestamp, returnLine=True, dateTimeFormat=dateTimeFormat
        )
        if logoutput:
            logger.info("Storm.getExecutorAssignmentStrAfterTimestamp returns %s" % result)
        return result

    @classmethod
    def getNodeIDHostMapping(cls, assignmentLine):
        '''
        Parses NodeID-executor host mapping.
        Returns a map object.
        Each entry is nodeID -> hostname.
        Returns {} if not found.
        '''
        matchObj = re.search(':node->host {(.*?)}', assignmentLine)
        nodeIDHostMap = {}
        nodeIDHostMapString = matchObj.group(1)
        for entry in nodeIDHostMapString.split(","):
            entry = entry.strip()
            nodeIDHostMap[entry.split(" ")[0].strip('"')] = entry.split(" ")[1].strip('"')
        return nodeIDHostMap

    @classmethod
    def getNimbusLogAssignmentLine(cls, topoName, logoutput=True, nimbusHost=None):
        '''
        Find last entry of assignment log line for given topology name.
        Returns a string for the line.
        Returns None if not Found.

        :param: nimbusHost: if None, use default Nimbus.
        '''
        pattern = "Setting new assignment for topology id %s" % topoName
        return cls.getLastGoodLineFromNimbusLog(pattern, logoutput=logoutput, nimbusHost=nimbusHost)

    @classmethod
    def canFindEmitPattern(
            cls,
            topoName,
            key,
            emitType="default",
            valueStr="",
            acceptedBeginTimestamp=None,
            latestLogOnly=True,
            logoutput=True,
            emitMarker="Emitting",
            nimbusHost=None,
            executorAssignment=None
    ):
        '''
        Returns True if the method can find given pattern in emit.
        valueStr can be regex.
        
        Spout/Bolt logs emit similarly.
        #2014-01-26 17:32:02 b.s.d.task [INFO] Emitting: count default [3:-6982270738643048586, dog, 7, 4]

        If executorAssignment is not None, use the executor assignment. In the case, nimbus host can be ignored.
        '''
        localFiles = cls.copyWorkerLogsToLocalMachine(
            topoName,
            latestLogOnly=latestLogOnly,
            logoutput=logoutput,
            nimbusHost=nimbusHost,
            executorAssignment=executorAssignment
        )
        regex = "%s.*%s.*%s.*\[%s\]" % (emitMarker, key, emitType, valueStr)
        if logoutput:
            logger.info("canFindEmitPattern: regex = %s" % regex)
        found = False
        if acceptedBeginTimestamp is None:
            for localFile in localFiles:
                if logoutput:
                    logger.info("canFindEmitPattern: reading %s" % localFile)
                if util.findMatchingPatternInFile(localFile, regex) == 0:
                    found = True
                    break
        else:
            for localFile in localFiles:
                if util.findMatchingPatternInFileAfterTimestamp(localFile, regex, acceptedBeginTimestamp):
                    found = True
                    break
        if logoutput:
            logger.info("canFindEmitPattern returns %s" % found)
        return found

    @classmethod
    def getTopologyList(
            cls, targetStatus=None, logoutput=True, isSlider=False, refreshUserKeyTab=False, useStandaloneCmd=False
    ):
        '''
        Gets a list of topologies in target status.
        Returns a list of (topologyName, status, int(numTasks), int(numWorkers), int(uptimeSecs))
        Returns [] if no entry is found.
        If target status is None, show all.
        '''
        (exit_code, stdout) = cls.run('list', env=None, logoutput=logoutput, useStandaloneCmd=useStandaloneCmd)
        result = []
        if exit_code != 0:
            return result
        passedHeader = False
        for line in stdout.split('\n'):
            line = line.rstrip('\r')
            if re.search("--------", line) is not None:
                passedHeader = True
            else:
                if passedHeader and not line.startswith("Running") and line.find("org.apache.storm") == -1:
                    items = line.split()
                    logger.info("items=%s" % (items))
                    (topologyName, status, numTasks, numWorkers, uptimeSecs) = items
                    if targetStatus is None or status.upper() == targetStatus.upper():
                        result.append((topologyName, status, int(numTasks), int(numWorkers), int(uptimeSecs)))
        if logoutput:
            logger.info('getTopologyList(%s) returns:' % targetStatus)
            for entry in result:
                logger.info(entry)
        return result

    @classmethod
    def kinit_qa(cls):
        if not Ambari.is_cluster_secure():
            logger.warning("Attempting kinit in unsecure environment.")
        logger.info("kinit as " + HADOOPQA_USER)
        kinitCmd = '%s -kt %s %s' % (
            Machine.getKinitCmd(), Machine.getHeadlessUserKeytab(user=HADOOPQA_USER),
            Machine.get_user_principal(user=HADOOPQA_USER)
        )
        return Machine.run(kinitCmd)

    @classmethod
    def getTopologyStatus(cls, topologyName, logoutput=True, useStandaloneCmd=False):
        '''
        Gets status of given topology.
        Returns a string indicating the status.
        Returns None if not found.
        '''

        topologies = cls.getTopologyList(
            targetStatus=None,
            logoutput=logoutput,
            isSlider=False,
            refreshUserKeyTab=False,
            useStandaloneCmd=useStandaloneCmd
        )
        for topology in topologies:
            if topology[0].strip() == topologyName:
                return topology[1]
        return None

    @classmethod
    def getNimbusProcessID(cls, logoutput=True):
        '''
        Gets Nimbus Process ID
        Returns an int for the PID.
        Returns None if not found.

        Doesnt work with nimbus HA
        '''
        entries = Machine.getProcessListRemote(
            hostIP=cls.getNimbus(), filter="org.apache.storm.daemon.nimbus", logoutput=logoutput
        )
        pid = int(ProcessInfo(entries[0]).pid)
        if logoutput:
            logger.info('getNimbusProcessID returns %s' % pid)
        return pid

    @classmethod
    def killNimbusProcess(cls, logoutput=True):
        '''
        Kills Nimbus process.
        Returns (exit_code, stdout)

        This API assume single nimbus.
        '''
        (exit_code, stdout) = Machine.killProcessRemote(
            pid=cls.getNimbusProcessID(logoutput),
            host=cls.getNimbus(),
            user=cls.getStormUser(),
            passwd=None,
            logoutput=logoutput
        )
        if logoutput:
            logger.info("killNimbusProcess gets (%s,%s)" % (exit_code, stdout))
        return (exit_code, stdout)

    @classmethod
    def killNimbusProcessAtHost(cls, host, logoutput=True):
        '''
        Kills first Nimbus process at particular host.
        Returns (exit_code, stdout)
        '''
        if logoutput:
            logger.info('killNimbusProcessAtHost start')
        nimbusProcesses = cls.getNimbusProcessesForHost(host, logoutput)
        if logoutput:
            logger.info("nimbusProcesses = %s" % nimbusProcesses)
        pid = nimbusProcesses[0][1]
        (exit_code, stdout) = Machine.killProcessRemote(
            pid=pid, host=host, user=cls.getStormUser(), passwd=cls.getStormPassword(), logoutput=logoutput
        )
        if logoutput:
            logger.info("killNimbusProcessAtHost gets (exit_code, stdout) = (%s,%s)" % (exit_code, stdout))
        return (exit_code, stdout)

    @classmethod
    def killProcess(cls, host, pid, logoutput=True):
        """
        Kill process owned by storm user
        :param host:
        :param pid:
        :param logoutput:
        :return:
        """
        if logoutput:
            logger.info('killProcess start host=%s pid=%s' % (host, pid))
        (exit_code, stdout) = Machine.killProcessRemote(
            pid=pid, host=host, user=cls.getStormUser(), passwd=cls.getStormPassword(), logoutput=logoutput
        )
        if logoutput:
            logger.info("killProcess gets (exit_code, stdout) = (%s,%s)" % (exit_code, stdout))
        return (exit_code, stdout)

    @classmethod
    def killSupervisorProcessAtHost(cls, host, logoutput=True):
        '''
        Kills first supervisor process at particular host.
        Returns (exit_code, stdout)
        '''
        if logoutput:
            logger.info('killSupervisorProcessAtHost start')
        processes = cls.getSupervisorProcessesForHost(host, logoutput)
        if logoutput:
            logger.info("processes = %s" % processes)
        for process in processes:
            pid = process[1]
            (exit_code, stdout) = Machine.killProcessRemote(
                pid=pid, host=host, user=cls.getStormUser(), passwd=cls.getStormPassword(), logoutput=logoutput
            )
            if logoutput:
                logger.info("killSupervisorProcessAtHost gets (exit_code, stdout) = (%s,%s)" % (exit_code, stdout))

    @classmethod
    def getWorkerProcesses(cls, supervisorHosts, topologyName, logoutput=True):
        """

        :param supervisorHosts list of str
        :param topologyName:
        :param logoutput:
        :return: [] or list of tuples
        """
        if logoutput:
            logger.info("Storm.getWorkerProcesses supervisorHosts=%s" % supervisorHosts)
        filter = "[0-9]+.*java.*%s*" % topologyName
        #if Machine.isDebian():
        #filter = "java.*%s" % topologyName
        result = []  # type: list[ProcessOnMachine]
        for host in supervisorHosts:
            lines = Machine.getProcessListRemote(
                host, filter=filter, logoutput=True, useEGrep=True, exclFilters=cls.exclFilters
            )
            if len(lines) == 0 or (len(lines) == 1 and lines[0] == ''):
                pass
            else:
                for line in lines:
                    s = line.split()
                    result.append(ProcessOnMachine(host, s[0], s[1], s[2], s[3]))
        if logoutput:
            logger.info("Storm.getWorkerProcesses return %s" % result)
        return result

    @classmethod
    def getStormUser(cls):
        '''
        Returns a user we use to run storm processes.
        '''
        return Config.get('storm', 'STORM_USER')

    @classmethod
    def getStormPassword(cls):
        '''
        Returns mock password for sudo.
        '''
        return "password"

    @classmethod
    def getLastGoodLineFromNimbusLog(cls, pattern, logoutput=True, nimbusHost=None):
        '''
        Gets last good(relevant) line from nimbus log.
        The line has to match with pattern for line.find(pattern).
        Returns a string of the line.
        Returns None if not found.

        :param: nimbusHost: if None, use default Nimbus.
        '''
        f = None
        result = None
        nimbusLogPath = cls.copyNimbusLogToLocalMachine(logoutput=logoutput, nimbusHost=nimbusHost)
        try:
            f = open(nimbusLogPath, "r")
            lastGoodLine = None
            for line in f:
                if line.find(pattern) > -1:
                    lastGoodLine = line
            if lastGoodLine is None:
                result = None
            result = lastGoodLine
            if logoutput:
                logger.info("getLastGoodLineFromNimbusLog returns:\n%s" % result)
            return result
        except:
            return None
        finally:
            if f is not None:
                f.close()

    @classmethod
    def getStormID(cls, topologyName, logoutput=True):
        '''
        When a user submits "mytopo", storm will build unique storm ID "mytopo-2-1390669435" for instance.
        '''
        regexPattern = "Activating %s:" % topologyName
        relevantLine = cls.getLastGoodLineFromNimbusLog(regexPattern, logoutput=logoutput)
        stormID = None
        matchObj = re.search("Activating %s: (.*)$" % topologyName, relevantLine, re.MULTILINE)
        if logoutput:
            logger.info("getStormID: matchObj = %s" % matchObj)
        if matchObj:
            stormID = matchObj.group(1).strip()
        if logoutput:
            logger.info("getStormID returns:\n%s" % stormID)
        return stormID

    @classmethod
    def waitUntilNoTopologyRunning(cls, timeout=120, interval=5, logoutput=True):
        '''
        Waits until no topology is running.
        Returns True if wait succeeds.
        Returns False if timed out.
        '''
        starttime = time.time()
        topologies = cls.getTopologyList(targetStatus=None, logoutput=logoutput)
        while (time.time() - starttime) < timeout and len(topologies) > 0:
            logger.info("Sleeping for %d seconds to wait for no topology is running" % interval)
            time.sleep(interval)
            topologies = cls.getTopologyList(targetStatus=None, logoutput=logoutput)
        if len(topologies) != 0:
            logger.info("WARNING: some topologies are stil running.")
        return len(topologies) == 0

    @classmethod
    def getStormServerPrincipal(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(cls.getStormJaasConf(), 'StormServer.*?principal="(.*?)"')
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getStormServerPrincipal returns %s" % result)
        return result

    @classmethod
    def getStormServerKeytab(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(cls.getStormJaasConf(), 'StormServer.*?keyTab="(.*?)"')
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getStormServerKeytab returns %s" % result)
        return result

    @classmethod
    def getStormClientPrincipal(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(cls.getStormJaasConf(), 'StormClient.*?principal="(.*?)"')
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getStormClientPrincipal returns %s" % result)
        return result

    @classmethod
    def getStormClientKeytab(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(cls.getStormJaasConf(), 'StormClient.*?keyTab="(.*?)"')
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getStormClientKeytab returns %s" % result)
        return result

    @classmethod
    def getUIPrincipal(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(
            cls.getStormYaml(), 'ui.filter.params.*?"kerberos.principal":.*?"(.*?)"'
        )
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getUIPrincipal returns %s" % result)
        return result

    @classmethod
    def getUIKeytab(cls, logoutput=True):
        o = util.findMatchingPatternInFileMultiLine(
            cls.getStormYaml(), 'ui.filter.params.*?"kerberos.keytab":.*?"(.*?)"'
        )
        if o is None:
            result = None
        else:
            result = o.group(1)
        if logoutput:
            logger.info("getUIKeytab returns %s" % result)
        return result

    @classmethod
    def getKerberosDomain(cls, logoutput=True):
        tmp = cls.getStormServerPrincipal()
        if tmp is None:
            return None
        else:
            result = re.findall("@(.*)", tmp)[0]
        if logoutput:
            logger.info("getKerberosDomain returns %s" % result)
        return result

    @classmethod
    def startNimbus(cls, host):
        Ambari.startComponent(host=host, component="NIMBUS")

    @classmethod
    def restart_supervisors(cls):
        cls.stop_supervisors()
        cls.start_supervisors()

    @classmethod
    def stop_supervisors(cls):
        Ambari.start_stop_service("STORM", state="INSTALLED", waitForCompletion=True)

    @classmethod
    def start_supervisors(cls):
        Ambari.start_stop_service("STORM", state="STARTED", waitForCompletion=True)

    @classmethod
    def startSupervisor(cls, host="", config=None, logoutput=True):
        # in future we should dynamically build the command.
        config_opt = "--config %s" % config if config else ""
        supervisor_log = os.path.join(cls.getLogDir(), "supervisor.log")
        cmd = "%s; source /usr/hdp/current/storm-supervisor/conf/storm-env.sh;  storm %s supervisor >> %s 2>&1 &" % (
            STORM_SERVICE_COMMON_CMD, config_opt, supervisor_log
        )
        # run as storm user
        Machine.runas(user=cls.getStormUser(), cmd=cmd, host=host, logoutput=logoutput, passwd=cls.getStormPassword())
        Machine.runas(user=cls.getStormUser(), cmd=cmd, host=host, logoutput=logoutput, passwd=cls.getStormPassword())

    @classmethod
    def runAndAssertExclamationTopology(cls, topologyName, extra_opt='', logoutput=True):
        """
        Run exclamation topology and make sure the output is found in worker log.
        :param topologyName:
        :param logoutput:
        :return: TopologyRun Object
        """
        if logoutput:
            logger.info("Storm.runAndAssertExclamationTopology start")
        className = "ExclamationTopology"
        startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cls.runStarterTopologySuccessfully(className, arg=topologyName + " " + extra_opt)
        util.sleep(15)
        assert Storm.getTopologyStatus(topologyName) == 'ACTIVE'

        nimbuses = cls.getRESTNimbusSummary(True)
        leader = None  # type: NimbusSummary
        nonLeaders = []  # type: list[NimbusSummary]
        leaderCount = 0
        for nimbus in nimbuses:
            if nimbus[0].upper() == "LEADER":
                leader = nimbus
                leaderCount += 1
            elif nimbus[0].upper() == "NOT A LEADER":
                nonLeaders.append(nimbus)
        assert leaderCount == 1
        logger.info("leader = %s" % (leader, ))

        executorAssignment = cls.getExecutorAssignment(topologyName, nimbusHost=leader.host)
        logger.info("%s" % executorAssignment)

        word = "nathan!!!!!!"
        rest_client = cls.get_rest_client()
        all_worker_logs = rest_client.fetch_combined_worker_log(topologyName)
        for i in range(12):
            if word in all_worker_logs:
                break
            util.sleep(10)
            all_worker_logs = rest_client.fetch_combined_worker_log(topologyName)
        assert word in all_worker_logs
        r = TopologyRun(nimbuses, leader, nonLeaders, executorAssignment, startTime)
        return r

    @classmethod
    def get_supervisors_ambari(cls):
        # set Ambari._weburl field just in case
        Ambari.getWebUrl(is_hdp=False, is_enc=HdfComponent.get_ambari_server_ssl())
        hosts = Ambari.getHostsForComponent("SUPERVISOR", cluster=HdfComponent.get_ambari_cluster_name())
        return hosts

    @classmethod
    def restore_config_supervisors(cls, changes=None):
        '''
        Restore config.
        changes is a list.
        '''
        storm_conf = Config.get('storm', 'STORM_CONF')
        nodes = cls.getSupervisors()
        configUtils.restoreConfig(changes, storm_conf, nodes)

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
        java_tests = os.path.join(HADOOPQE_TESTS_DIR, "tests", "storm", "java_tests")
        miscTestLogPaths = [java_tests, cls.getLogDir()]
        if logoutput:
            logger.info("Storm.getMiscTestLogPaths returns %s" % str(miscTestLogPaths))
        return miscTestLogPaths

    @staticmethod
    def get_rest_client():
        web_ui_host = Storm.getWebUIHost()
        web_ui_port = Storm.getWebUIPort()
        client = StormRestClient(web_ui_host, web_ui_port)
        return client
