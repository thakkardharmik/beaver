import logging
import os
import re
import traceback

from beaver import util
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class ComponentLogUtil(object):
    _allNodes = None

    def __init__(self):
        pass

    @classmethod
    def getAllNodes(cls, logoutput=True):
        try:
            if cls._allNodes:
                return cls._allNodes
            if Machine.isLinux():
                # retrieve nodes from /tmp/all_nodes for linux machine
                cls._allNodes = util.getAllNodes()
                return cls._allNodes
            else:
                # retrieve nodes from c:\testtools\all_hosts for onprem & ASV machines
                filePath = "c:\\testtools\\all_hosts"
                nodeList = []
                fd = None
                try:
                    fd = open(filePath)
                    for line in fd:
                        nodeList.append(line.strip(os.linesep).strip())
                finally:
                    if fd is not None:
                        fd.close()

                try:
                    nodeList.remove('')
                except Exception:
                    pass

                if logoutput:
                    logger.info("getAllNodes return %s", str(nodeList))
                cls._allNodes = nodeList
                return cls._allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during getAllNodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_isHAEnabled(cls, logoutput=True):
        try:
            from beaver.component.hadoop import HDFS
            return HDFS.isHAEnabled()
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_isHAEnabled() call")
                logger.error(traceback.format_exc())
            return False

    @classmethod
    def HDFS_getConfigValue(cls, propertyValue, defaultValue=None):
        try:
            hdfs_conf_path = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hdfs-site.xml")
            logger.info("hdfs_conf_path is %s", hdfs_conf_path)
            return util.getPropertyValueFromConfigXMLFile(hdfs_conf_path, propertyValue, defaultValue=defaultValue)
        except Exception:
            logger.error("Exception occured during HDFS_getConfigValue() call")
            logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getNamenodes(cls, logoutput=True):
        try:
            from beaver.component.hadoop import HDFS
            nnHostList = []
            nnHttpAddressList = []
            if HDFS.isHAEnabled():
                nameservice = cls.HDFS_getConfigValue('dfs.nameservices')
                serviceIds = cls.HDFS_getConfigValue('dfs.ha.namenodes.%s' % nameservice)
                serviceIds = serviceIds.split(',')
                for serviceId in serviceIds:
                    nnHttpAddress = cls.HDFS_getConfigValue(
                        'dfs.namenode.rpc-address.%s.%s' % (nameservice, serviceId)
                    )
                    nnHttpAddressList.append(nnHttpAddress)
            else:
                nnHttpAddress = cls.HDFS_getConfigValue(
                    "dfs.namenode.rpc-address", cls.HDFS_getConfigValue('dfs.namenode.http-address')
                )
                nnHttpAddressList.append(nnHttpAddress)

            if nnHttpAddressList:
                for nnHttpAddress in nnHttpAddressList:
                    if nnHttpAddress:
                        nnHost, _nnPort = nnHttpAddress.split(":")
                        nnHostList.append(nnHost)
            else:
                configKey = 'fs.defaultFS'
                namenodeWithPort = cls.HDFS_getConfigValue(configKey)
                if not namenodeWithPort:
                    configKey = 'fs.default.name'
                    namenodeWithPort = cls.HDFS_getConfigValue(configKey)
                if namenodeWithPort.find("hdfs") != -1:
                    nnHost = re.search("hdfs://(.*):([0-9]+)", namenodeWithPort).group(1)
                elif namenodeWithPort.find("adl") != -1:
                    nnHost = re.search("adl://(.*):([0-9]+)", namenodeWithPort).group(1)
                else:
                    nnHost = re.search("wasb://(.*)", namenodeWithPort).group(1)
                nnHostList.append(nnHost)
            return nnHostList
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getNamenodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getDatanodes(cls, logoutput=True):
        try:
            dnHostList = cls.getAllNodes()
            return dnHostList
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getDatanodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getSecondaryNamenode(cls, logoutput=True):
        try:
            snnHttpAddress = cls.HDFS_getConfigValue("dfs.secondary.http.address")
            if snnHttpAddress:
                snnHost, _snnPort = snnHttpAddress.split(":")
                return snnHost
            else:
                return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getSecondaryNamenode() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getGateway(cls, logoutput=True):
        try:
            from beaver.component.hadoop import HDFS
            return HDFS.getGateway()
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getGateway() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getNNLogDir(cls, logoutput=True):
        try:
            nnHostList = cls.HDFS_getNamenodes()
            if nnHostList:
                for nnHost in nnHostList:
                    lines = Machine.find(
                        user=Machine.getAdminUser(),
                        host=nnHost,
                        filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
                        searchstr="hadoop*-namenode-*.log",
                        passwd=Machine.getAdminPasswd()
                    )
                    lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
                    if lines:
                        nnLogFilePath = lines[0]
                        if nnLogFilePath is None:
                            continue
                        else:
                            return util.getDir(nnLogFilePath)
            return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getNNLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getDNLogDir(cls, logoutput=True):
        try:
            dnHostList = cls.HDFS_getDatanodes()
            if dnHostList:
                for dnHost in dnHostList:
                    lines = Machine.find(
                        user=Machine.getAdminUser(),
                        host=dnHost,
                        filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
                        searchstr="hadoop-*datanode-*.log",
                        passwd=Machine.getAdminPasswd()
                    )
                    if lines:
                        dnLogFilePath = lines[0]
                        if dnLogFilePath is None:
                            continue
                        else:
                            return util.getDir(dnLogFilePath)
            return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getDNLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HDFS_getSNNLogDir(cls, logoutput=True):
        try:
            snnHost = cls.HDFS_getSecondaryNamenode()
            if snnHost:
                lines = Machine.find(
                    user=Machine.getAdminUser(),
                    host=snnHost,
                    filepath=Config.get('hadoop', 'HADOOP_LOG_DIR'),
                    searchstr="hadoop-*secondarynamenode-*.log",
                    passwd=Machine.getAdminPasswd()
                )
                if lines:
                    snnLogFilePath = lines[0]
                    if snnLogFilePath:
                        return util.getDir(snnLogFilePath)
            return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during HDFS_getSNNLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getJobtracker(cls, logoutput=True):
        try:
            from beaver.component.hadoop import MAPRED
            return MAPRED.getJobtracker()
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED_getJobtracker() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getTasktrackers(cls, logoutput=True):
        try:
            ttHostList = cls.getAllNodes()
            return ttHostList
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED_getTasktrackers() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getHistoryserver(cls, logoutput=True):
        try:
            from beaver.component.hadoop import MAPRED
            return MAPRED.getHistoryserver()
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED.getHistoryserver() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getJTLogDir(cls, logoutput=True):
        try:
            from beaver.component.hadoop import MAPRED
            return MAPRED.getJTLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED_getJTLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getTaskTrackerLogDir(cls, logoutput=True):
        try:
            ttHostList = cls.MAPRED_getTasktrackers()
            if ttHostList:
                for ttHost in ttHostList:
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
                        continue
                    else:
                        if lines[0] is None:
                            return None
                        else:
                            return util.getDir(lines[0])
            return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED_getTaskTrackerLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getHistoryServerLogDir(cls, logoutput=True):
        try:
            from beaver.component.hadoop import MAPRED
            return MAPRED.getHistoryServerLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED.getHistoryServerLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def MAPRED_getJobSummaryLogFile(cls, jtHost=None, logoutput=True):
        if not jtHost:
            jtHost = cls.MAPRED_getJobtracker()
        try:
            file_paths = [Config.get('hadoop', 'YARN_LOG_DIR'), Config.get('hadoop', 'HADOOP_LOG_DIR')]
            file_name = 'hadoop-mapreduce.jobsummary.log'
            if Machine.isWindows():
                file_name = 'rm-appsummary.log'
            for file_path in file_paths:
                lines = Machine.find(
                    user=Machine.getAdminUser(),
                    host=jtHost,
                    filepath=file_path,
                    searchstr=file_name,
                    passwd=Machine.getAdminPasswd()
                )
                lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
                if lines and lines[0]:
                    return lines[0]
                else:
                    continue
            # otherwise return the RM/jobtracker log
            from beaver.component.hadoop import MAPRED
            return MAPRED.getJobTrackerLogFile()
        except Exception:
            if logoutput:
                logger.error("Exception occured during MAPRED_getJobSummaryLogFile() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def YARN_isHAEnabled(cls, logoutput=True):
        try:
            from beaver.component.hadoop import YARN
            return YARN.isHAEnabled()
        except Exception:
            if logoutput:
                logger.error("Exception occured during YARN_isHAEnabled() call")
                logger.error(traceback.format_exc())
            return False

    @classmethod
    def YARN_getRMHANodes(cls, logoutput=True):
        try:
            from beaver.component.hadoop import YARN
            return YARN.getRMHANodes()
        except Exception:
            if logoutput:
                logger.error("Exception occured during YARN_getRMHANodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Zookeeper_getZooLogDir(cls, logoutput=True):
        try:
            from beaver.component.zookeeper import ZOOKEEPER_ENV_FILE
            matchObjList = None
            if Machine.isHumboldt():
                for node in cls.Zookeeper_getZKHosts():
                    try:
                        Machine.copyToLocal(None, node, ZOOKEEPER_ENV_FILE, Machine.getTempDir())
                        REMOTE_ZOOKEEPER_ENV_FILE = os.path.join(Machine.getTempDir(), 'zookeeper-env.sh')
                        matchObjList = util.findMatchingPatternInFile(
                            REMOTE_ZOOKEEPER_ENV_FILE, "export ZOO_LOG_DIR=(.*)", return0Or1=False
                        )
                        if matchObjList:
                            returnValue = matchObjList[0].group(1)
                            if returnValue == "${LOG_DIR}":
                                continue
                            else:
                                break
                    except Exception:
                        pass

            if not matchObjList:
                try:
                    matchObjList = util.findMatchingPatternInFile(
                        ZOOKEEPER_ENV_FILE, "export ZOO_LOG_DIR=(.*)", return0Or1=False
                    )
                except Exception as e:
                    logger.info("Exception occured during Zookeeper_getZooLogDir() call: %s", str(e))
                    if Machine.isWindows():
                        logger.info("Using the default zookeer log dir")
                        winZKLogDir = "D:\\hadoop\\logs\\zookeeper"
                        return winZKLogDir

            returnValue = None
            if matchObjList:
                returnValue = matchObjList[0].group(1)
                if returnValue == "${LOG_DIR}":
                    return None
            return returnValue
        except Exception:
            if logoutput:
                logger.error("Exception occured during Zookeeper_getZooLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Zookeeper_getZKHosts(cls, logoutput=True):
        try:
            if Machine.isHumboldt():
                return cls.getAllNodes()
            from beaver.component.zookeeper import ZOOKEEPER_CONF_DIR
            ZOO_CFG_FILE = os.path.join(ZOOKEEPER_CONF_DIR, 'zoo.cfg')
            file_obj = open(ZOO_CFG_FILE, 'r')
            zkHosts = []
            for line in file_obj:
                if re.search('server.[0-9]*=', line) is not None:
                    zkHosts.append(re.split(':', re.split('=', line)[1])[0])
            if not zkHosts:
                zkHosts.append('localhost')
            file_obj.close()
            return zkHosts
        except Exception:
            if logoutput:
                logger.error("Exception occured during Zookeeper_getZKHosts() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HBase_getMasterNodes(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during HBase_getAllMasterNodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HBase_getRegionServers(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during HBase_getRegionServers() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def HBase_getHBaseLogDir(cls, logoutput=True):
        try:
            from beaver.component.hbase import HBase
            return HBase.getHBaseLogDir(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during HBase_getHBaseLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.druid import Druid
            return Druid.getDruidLogDir(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_get_logs_dir call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_getDruidBroker(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_getDruidBroker() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_getDruidCoordinator(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_getDruidCoordinator() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_getDruidOverlord(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_getDruidOverlord() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_getDruidMiddleManagers(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_getDruidMiddleManagers() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Druid_getDruidHistoricals(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Druid_getDruidHistoricals() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Oozie_isHAEnabled(cls, logoutput=True):
        try:
            from beaver.component.oozie import Oozie
            return Oozie.isHAEnabled()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Oozie_isHAEnabled() call")
                logger.error(traceback.format_exc())
            return False

    @classmethod
    def Oozie_getOozieServers(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Oozie_getOozieServers() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Oozie_getOozieServer(cls, logoutput=True):
        try:
            from beaver.component.oozie import Oozie
            # TODO: This was previously Oozie.getOozieServer(). Clean this!
            return Oozie.getOozieServers()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Oozie_getOozieServer() call: %s", str(e))
            return None

    @classmethod
    def Oozie_getOozieLogDir(cls, logoutput=True):
        try:
            from beaver.component.oozie import Oozie
            return Oozie.getOozieLogDir(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Oozie_getOozieLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Falcon_get_all_nodes(cls, logoutput=True):
        try:
            from beaver.component.falcon import Falcon
            return Falcon.get_all_nodes()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Falcon_get_all_nodes() call: %s", str(e))
            return None

    @classmethod
    def Falcon_getLogDir(cls, logoutput=True):
        try:
            from beaver.component.falcon import Falcon
            return Falcon.getLogDir(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Falcon_getLogDir() call: %s", str(e))
            return None

    @classmethod
    def Hive_getHiveHosts(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Hive_getHiveHosts() call: %s", str(e))
            return None

    @classmethod
    def Hive_getTempletonHost(cls, logoutput=True):
        try:
            return Config.get('templeton', 'TEMPLETON_HOST', 'localhost')
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hive_getTempletonHost() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Hive_getHiveLogDir(cls, logoutput=True):
        try:
            from beaver.component.hive import Hive
            return Hive.getHiveLogDir(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hive_getHiveLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Hcatalog_getTempletonLogDir(cls, logoutput=True):
        try:
            from beaver.component.hcatalog import Hcatalog
            return Hcatalog.getTempletonLogDir(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hcatalog_getTempletonLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Accumulo_get_masters(cls, logoutput=True):
        try:
            from beaver.component.accumulo import Accumulo
            return Accumulo.get_masters(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Accumulo_get_masters() call: %s", str(e))
            return None

    @classmethod
    def Accumulo_get_monitors(cls, logoutput=True):
        try:
            from beaver.component.accumulo import Accumulo
            return Accumulo.get_monitors(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Accumulo_get_monitors() call: %s", str(e))
            return None

    @classmethod
    def Accumulo_get_tracers(cls, logoutput=True):
        try:
            from beaver.component.accumulo import Accumulo
            return Accumulo.get_tracers(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Accumulo_get_tracers() call: %s", str(e))
            return None

    @classmethod
    def Accumulo_get_tservers(cls, logoutput=True):
        try:
            from beaver.component.accumulo import Accumulo
            return Accumulo.get_tservers(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Accumulo_get_tservers() call: %s", str(e))
            return None

    @classmethod
    def Accumulo_getAccumuloLogDir(cls, logoutput=True):
        try:
            from beaver.component.accumulo import Accumulo
            return Accumulo.getAccumuloLogDir(logoutput)
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Accumulo_getAccumuloLogDir() call: %s", str(e))
            return None

    @classmethod
    def Xa_getRangerNodes(cls, logoutput=True):
        try:
            from beaver.component.xa import Xa
            return Xa.getRangerNodes(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Xa_getRangerNodes() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Xa_getPolicyMgrLogs(cls, logoutput=True):
        try:
            from beaver.component.xa import Xa
            return Xa.getPolicyMgrLogs(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Xa_getPolicyMgrLogs() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Xa_getUserSyncLogs(cls, logoutput=True):
        try:
            from beaver.component.xa import Xa
            return Xa.getUserSyncLogs(logoutput)
        except Exception:
            if logoutput:
                logger.error("Exception occured during Xa_getUserSyncLogs() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Spark_getSparkHistoryServerHostname(cls, logoutput=True):
        try:
            from beaver.component.spark import Spark
            return Spark.getSparkHistoryServerHostname()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Spark_getSparkHistoryServerHostname() call: %s", str(e))
            return None

    @classmethod
    def Spark_getSparkLogDir(cls, logoutput=True):
        try:
            from beaver.component.spark import Spark
            return Spark.getSparkLogDir()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Spark_getSparkLogDir() call: %s", str(e))
            return None

    @classmethod
    def Zeppelin_getZeppelinServerHostname(cls, logoutput=True):
        try:
            logger.info("in Zeppelin_getZeppelinServerHostname")
            from beaver.component.zeppelin import ZeppelinLogCollectionUtils
            logger.info("calling ZeppelinLogCollectionUtils.get_zeppelin_server_ip_addr")
            ip = ZeppelinLogCollectionUtils.get_zeppelin_server_ip_addr()
            logger.info("in Zeppelin_getZeppelinServerHostname ip: %s", ip)
            return ip
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Zeppelin_getZeppelinServerHostname() call: %s", str(e))
            return None

    @classmethod
    def Zeppelin_getZeppelinLogDir(cls, logoutput=True):
        try:
            logger.info("in Zeppelin_getZeppelinLogDir")
            from beaver.component.zeppelin import ZeppelinLogCollectionUtils
            logger.info("calling ZeppelinLogCollectionUtils.get_zeppelin_log_dir()")
            log_dir = ZeppelinLogCollectionUtils.get_zeppelin_log_dir()
            logger.info("in Zeppelin_getZeppelinLogDir log_dir: %s", log_dir)
            return log_dir
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Zeppelin_getZeppelinLogDir() call: %s", str(e))
            return None

    @classmethod
    def Livy_getLivyServerHostname(cls, logoutput=True):
        try:
            from beaver.component.livy import Livy
            return Livy.getLivyServerIpAddress()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Livy_getLivyServerHostname() call: %s", str(e))
            return None

    @classmethod
    def Livy_getLivy2ServerHostname(cls, logoutput=True):
        try:
            from beaver.component.livy import Livy
            return Livy.getLivyServerIpAddress(forceVersion="2")
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Livy_getLivy2ServerHostname() call: %s", str(e))
            return None

    @classmethod
    def Livy_getLivyLogDir(cls, logoutput=True):
        try:
            from beaver.component.livy import Livy
            return Livy.getLivyLogDir()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Livy_getLivyLogDir() call: %s", str(e))
            return None

    @classmethod
    def Livy_getLivy2LogDir(cls, logoutput=True):
        try:
            from beaver.component.livy import Livy
            return Livy.getLivyLogDir(forceVersion="2")
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Livy_getLivy2LogDir() call: %s", str(e))
            return None

    @classmethod
    def Atlas_get_host(cls, logoutput=True):
        try:
            # TODO: change it to get the atlas host from beaver atlas
            from beaver.component.ambari import Ambari
            return Ambari.getServiceHosts("ATLAS", "ATLAS_SERVER")
        except Exception:
            if logoutput:
                logger.error("Exception occured during Atlas_get_host() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Atlas_get_log_dir(cls, logoutput=True):
        try:
            # TODO: change it to read from atlas-env.sh
            from beaver.component.ambari import Ambari
            return Ambari.getConfig("atlas-env")['metadata_log_dir']
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Atlas_get_log_dir() call: %s", str(e))
            return None

    @classmethod
    def Hue_get_hue_hosts(cls, logoutput=True):
        try:
            if Machine.isLinux():
                return "localhost"
            else:
                return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hue_get_hue_hosts() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Hue_get_hue_log_dir(cls, logoutput=True):
        try:
            if Machine.isLinux():
                return "/var/log/hue"
            else:
                return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hue_get_hue_log_dir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Hue_get_hue_tests_dir(cls, logoutput=True):
        try:
            if Machine.isLinux():
                return "/tmp/tests"
            else:
                return None
        except Exception:
            if logoutput:
                logger.error("Exception occured during Hue_get_hue_tests_dir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Storm_isHA(cls, logoutput=True):
        try:
            from beaver.component.storm import Storm
            return Storm.isHA()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Storm_isHA() call")
                logger.error(traceback.format_exc())
            return False

    @classmethod
    def Storm_getNimbus(cls, logoutput=True):
        try:
            from beaver.component.storm import Storm
            return Storm.getNimbus()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Storm_getNimbus() call: %s", str(e))
            return None

    @classmethod
    def Storm_getNimbusSeeds(cls, logoutput=True):
        try:
            from beaver.component.storm import Storm
            return Storm.getNimbusSeeds()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Storm_getNimbusSeeds() call")
                logger.error(traceback.format_exc())
            return cls.getAllNodes()

    @classmethod
    def Storm_getSupervisors(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Storm_getSupervisors() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Storm_getWebUIHost(cls, logoutput=True):
        try:
            allNodes = cls.getAllNodes()
            return allNodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Storm_getWebUIHost() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Storm_getLogDir(cls, logoutput=True):
        try:
            from beaver.component.storm import Storm
            return Storm.getLogDir()
        except Exception as e:
            if logoutput:
                logger.error("Exception occured during Storm_getLogDir() call: %s", str(e))
            storm_log = "/var/log/storm"
            return storm_log

    @classmethod
    def Kafka_getServiceLogDir(cls, logoutput=True):
        try:
            from beaver.component.kafka import Kafka
            return Kafka.getServiceLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Kafka_getServiceLogDir() call")
                logger.error(traceback.format_exc())
            kafka_log_default_dir = "/var/log/kafka"
            return kafka_log_default_dir

    @classmethod
    def Ambari_getAgentLogDir(cls, logoutput=True):  # pylint: disable=unused-argument
        return "/var/log/ambari-agent"

    @classmethod
    def Knox_getLogDir(cls, logoutput=True):
        try:
            return Config.get('knox', 'KNOX_LOG', 'knoxlogdirnotfound')
        except Exception:
            if logoutput:
                logger.error("Exception occured during Knox_getLogDir() call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def Nifi_get_hosts(cls, logoutput=True):
        try:
            all_nodes = cls.getAllNodes()
            return all_nodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during get_nifi_hosts() call")
                logger.error(traceback.format_exc())
                return None

    @classmethod
    def Streamline_get_hosts(cls, logoutput=True):
        try:
            all_nodes = cls.getAllNodes()
            return all_nodes
        except Exception:
            if logoutput:
                logger.error("Exception occured during Streamline_get_hosts() call")
                logger.error(traceback.format_exc())
                return None

    @classmethod
    def Nifi_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.nifi import Nifi
            return Nifi.get_log_dir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Nifi_get_master_logs() call")
                logger.error(traceback.format_exc())
                return Nifi.default_log_dir

    @classmethod
    def Streamline_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.streamline import Streamline
            return Streamline.get_log_dir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Nifi_get_master_logs() call")
                logger.error(traceback.format_exc())
                return Streamline.default_log_dir

    @classmethod
    def SMM_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.smm import SMM
            return SMM.get_log_dir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during SMM.get_log_dir() call")
                logger.error(traceback.format_exc())
                return SMM.default_log_dir

    @classmethod
    def SmartSense_getSmartSenseServiceNodes(cls, logoutput=True):
        try:
            from beaver.component.smartsense import SmartSense
            return SmartSense.getSmartSenseServiceNodes()
        except Exception:
            if logoutput:
                logger.error("Exception occured during getSmartSenseServiceNodes call")
                logger.error(traceback.format_exc())
            return "localhost"

    @classmethod
    def SmartSense_getActivityComponentsNodes(cls, logoutput=True):
        try:
            from beaver.component.smartsense import SmartSense
            return SmartSense.getActivityComponentsNodes()
        except Exception:
            if logoutput:
                logger.error("Exception occured during getActivityComponentsNodes call")
                logger.error(traceback.format_exc())
            return "localhost"

    @classmethod
    def SmartSense_getSmartSenseServiceLogDir(cls, logoutput=True):
        try:
            from beaver.component.smartsense import SmartSense
            return SmartSense.getSmartSenseServiceLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during getSmartSenseServiceLogDir call")
                logger.error(traceback.format_exc())
            return "/var/log/hst"

    @classmethod
    def SmartSense_getActivityComponentsLogDir(cls, logoutput=True):
        try:
            from beaver.component.smartsense import SmartSense
            return SmartSense.getActivityComponentsLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during getActivityComponentsLogDir call")
                logger.error(traceback.format_exc())
            return "/var/log/smartsense-activity"

    @classmethod
    def Ambari_infra_solr_get_hosts(cls, logoutput=True):
        try:
            from beaver.component.ambari import Ambari
            return Ambari.getServiceHosts("AMBARI_INFRA", "INFRA_SOLR")
        except Exception:
            if logoutput:
                logger.error("Exception occured during get Ambari_infra_solr_get_hosts call")
                logger.error(traceback.format_exc())
                return None

    @classmethod
    def Ambari_infra_solr_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.ambari_infra import AmbariInfra
            return AmbariInfra.getAmbariInfraLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Ambari_infra_solr_get_logs_dir() call")
                logger.error(traceback.format_exc())
                return AmbariInfra.default_log_dir

    @classmethod
    def Ambari_infra_solrclient_get_hosts(cls, logoutput=True):
        try:
            from beaver.component.ambari import Ambari
            return Ambari.getServiceHosts("AMBARI_INFRA", "INFRA_SOLR_CLIENT")
        except Exception:
            if logoutput:
                logger.error("Exception occured during get Ambari_infra_solrclient_get_hosts call")
                logger.error(traceback.format_exc())
                return None

    @classmethod
    def Ambari_infra_solrclient_get_logs_dir(cls, logoutput=True):
        try:
            from beaver.component.ambari_infra import AmbariInfra
            return AmbariInfra.getAmbariInfraClientLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occured during Ambari_infra_solrclient_get_logs_dir() call")
                logger.error(traceback.format_exc())
                return AmbariInfra.default_log_dir

    @classmethod
    def BigSql_getLogsDir(cls, logoutput=True):
        try:
            # TODO: Is this code unused? No module named bigsql
            from component.bigsql import BigSQL  # pylint: disable=relative-import,import-error,no-name-in-module
            return BigSQL.getBigSqlLogDir()
        except Exception:
            if logoutput:
                logger.error("Exception occurred during BigSql_getLogsDir call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def BigSql_getHeadNodes(cls, logoutput=True):
        try:
            # TODO: Is this code unused? No module named bigsql
            from component.bigsql import BigSQL  # pylint: disable=relative-import,import-error,no-name-in-module
            return BigSQL.getBigSqlHeadHosts()
        except Exception:
            if logoutput:
                logger.error("Exception occurred during BigSql_getHeadNodes call")
                logger.error(traceback.format_exc())
            return None

    @classmethod
    def BigSql_getWorkerNodes(cls, logoutput=True):
        try:
            # TODO: Is this code unused? No module named bigsql
            from component.bigsql import BigSQL  # pylint: disable=relative-import,import-error,no-name-in-module
            return BigSQL.getBigSqlWorkerHosts()
        except Exception:
            if logoutput:
                logger.error("Exception occurred during BigSql_getWorkerNodes call")
                logger.error(traceback.format_exc())
            return None
