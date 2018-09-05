#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
'''
QE HDP Stack Log Aggregation Framework
'''
import collections
import logging
import os
import re
import time
import traceback
from ConfigParser import ConfigParser
from threading import Thread

from beaver import util
from beaver.componentLogUtil import ComponentLogUtil
from beaver.config import Config
from beaver.machine import Machine
from beaver.logUtilMachine import LogUtilMachine
from beaver.rsync import RSync

logger = logging.getLogger(__name__)

# Service misc test log map
SERVICE_MISC_TEST_LOG_MAP = {}
try:
    from beaver.component.pig import Pig

    SERVICE_MISC_TEST_LOG_MAP[Pig] = []
except Exception:
    pass
try:
    from beaver.component.hive import Hive

    SERVICE_MISC_TEST_LOG_MAP[Hive] = []
except Exception:
    pass
try:
    from beaver.component.hcatalog import Hcatalog

    SERVICE_MISC_TEST_LOG_MAP[Hcatalog] = []
except Exception:
    pass
try:
    from beaver.component.falcon import Falcon

    SERVICE_MISC_TEST_LOG_MAP[Falcon] = []
except Exception:
    pass
try:
    from beaver.component.accumulo import Accumulo

    SERVICE_MISC_TEST_LOG_MAP[Accumulo] = []
except Exception:
    pass
try:
    from beaver.component.atlas_resources.atlas import Atlas

    SERVICE_MISC_TEST_LOG_MAP[Atlas] = []
except Exception as e:
    logger.error("Exception while setting up atlas misc log map. %s", e)
try:
    from beaver.component.storm import Storm

    SERVICE_MISC_TEST_LOG_MAP[Storm] = []
except Exception:
    pass
try:
    from beaver.component.nifi import Nifi

    SERVICE_MISC_TEST_LOG_MAP[Nifi] = []
except Exception as e:
    logger.error("Exception while setting up nifi misc log map. %s", e)

try:
    from beaver.component.streamline import Streamline
    SERVICE_MISC_TEST_LOG_MAP[Streamline] = []
except Exception as e:
    logger.error("Exception while setting up streamline misc log map. %s", e)

try:
    from beaver.component.smm import SMM
    SERVICE_MISC_TEST_LOG_MAP[SMM] = []
except Exception as e:
    logger.error("Exception while setting up SMM misc log map. %s", e)

class LogUtil(object):
    '''
    Log Util Class
    '''
    ALL = "all"
    YARN_STR = "yarn"
    MAPRED_STR = "mapreduce"
    HDFS_STR = "hdfs"
    STORM_STR = "storm"
    OOZIE_STR = "oozie"
    TEZ_STR = "tez"
    TEZ_V15_STR = "tez_v15"
    ZOOKEEPER_STR = "zookeeper"
    HBASE_STR = "hbase"
    SLIDER_STR = "slider"
    HBASELONGRUNNING_STR = "hbaselongrunning"
    PHOENIX_STR = "phoenix"
    PHOENIX_QUERY_STR = "phoenix-queryserver"
    PHOENIX_QS_CONCUR_STR = "phoenix-qs-concurr"
    MAHOUT_STR = "mahout"
    HIVE_STR = "hive"
    HIVESERVER2_STR = "hiveserver2"
    HIVESERVER2CONCUR_STR = "hiveserver2concurr"
    HIVESERVER2CONCURHTTP_STR = "hiveserver2concurrhttp"
    HIVESERVER2CONCURRLDAP_STR = "hiveserver2concurrldap"
    HIVESERVER2CONCURRLDAPHTTP_STR = "hiveserver2concurrldaphttp"
    HIVESERVER2CONCURRSSL_STR = "hiveserver2concurrssl"
    HIVESERVER2CONCURSSLHTTP_STR = "hiveserver2concurrsslhttp"
    HIVESERVER2CONCURRTPUSER_STR = "hiveserver2concurrtpuser"
    HIVESERVER2_INTERACTIVE_STR = "hsihiveserver2"
    WEBHCAT_STR = "webhcat"
    HCATALOG_STR = "hcatalog"
    SQOOP_STR = "sqoop"
    FLUME_STR = "flumeng"
    PIG_STR = "pig"
    FALCON_STR = "falcon"
    KNOX_STR = "knox"
    HUE_STR = "hue"
    HUE_TEST_STR = "huetest"
    NIFI_STR = "nifi"
    STREAMLINE_STR = "streamline"
    SMM_STR = "smm"
    ZEPPELIN_STR = "zeppelin"
    LIVY_STR = "livy"
    LIVY2_STR = "livy2"
    DRUID_STR = "druid"
    SMARTSENSE_STR = "smartsense"
    ROLLINGUPGRADE_STR = "RollingUpgrade"
    AMBARI_ROLLINGUPGRADE_STR1 = "ambariructodal-uns"
    AMBARI_ROLLINGUPGRADE_STR2 = "ambariructodal-uns-ha"
    AMBARI_ROLLINGUPGRADE_STR3 = "ambariructodal-sec"
    AMBARI_ROLLINGUPGRADE_STR4 = "ambariructodal-sec-ha"
    AMBARI_ROLLINGUPGRADE_STR5 = "ambariructodal-all-sec"
    AMBARI_ROLLINGUPGRADE_STR6 = "ambariruc10todal-sec"
    AMBARI_ROLLINGUPGRADE_STR7 = "ambariruc10todal-sec-noranger"
    AMBARI_ROLLINGUPGRADE_STR8 = "ambarirudtodal-sec"
    AMBARI_ROLLINGUPGRADE_STR9 = "ambarirudtodal-sec-ha"
    AMBARI_ROLLINGUPGRADE_STR10 = "ambariruc10todal-sec-noranger-ha"
    AMBARI_ROLLINGUPGRADE_STR11 = "ambariructocm10-uns-ha"
    AMBARI_ROLLINGUPGRADEDOWNGRADE_STR1 = "ambariruupdownc10todaltoc10-uns"
    AMBARI_ROLLINGUPGRADEDOWNGRADE_STR2 = "ambariruupdownc10todaltoc10-sec"

    XAAGENTS_STR = "xaagents"
    ADMIN_STR = "admin"
    ROLLBACK_STR = "rollback"
    ACCUMULO_STR = "accumulo"
    ACCUMULO_MASTER_STR = "accumulo-masters"
    ACCUMULO_MONITORS_STR = "accumulo-monitors"
    ACCUMULO_TRACERS_STR = "accumulo-tracers"
    ACCUMULO_TSERVERS_STR = "accumulo-tservers"
    SPARK_STR = "spark"
    SPARK2_STR = "spark2"
    SPARKHIVE_STR = "sparkhive"
    HA_STR = "-ha"
    RANGER_STR = "ranger"
    RANGER_MOCK_SERVICE_1_STR = "ranger-mock-service-1"
    RANGER_MOCK_SERVICE_2_STR = "ranger-mock-service-2"
    ATLAS_STR = "atlas"
    ATLAS_HSI_STR = "atlas_hs_interactive"
    ATLAS_UI = "atlas_ui"
    KAFKA_STR = "kafka"
    AMBARI_STR = "ambari-agent"
    AMBARI_INFRA_STR = "ambari-infra"
    CLOUDBREAK_STR = "cloudbreak"
    ARTIFACTS_OUTPUT = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "output.log")
    DATATEAMTEST_OUTPUT = os.path.join(
        Config.getEnv("WORKSPACE"), "datateamtest", "hcatalog", "testdist", "out", "out", "log"
    )
    FALCON_OUTPUT = os.path.join(
        Config.getEnv("WORKSPACE"), "tests", "falcon", "falcon-regression", "falcon-regression", "merlin", "target",
        "surefire-reports", "TestSuite-output.txt"
    )
    if not Machine.pathExists(None, None, FALCON_OUTPUT, None):
        FALCON_OUTPUT = os.path.join(
            Config.getEnv("WORKSPACE"), "tests", "falcon", "falcon-regression", "merlin", "target", "surefire-reports",
            "TestSuite-output.txt"
        )
    WEBHCAT_OUTPUT = os.path.join(
        Config.getEnv("WORKSPACE"), "templeton", "src", "test", "e2e", "templeton", "testdist"
    )
    ATLAS_OUTPUT = os.path.join(
        Config.getEnv("WORKSPACE"), "tests", "atlas", "regression", "target", "surefire-reports",
        "TestSuite-output.txt"
    )
    TYPE1 = "pytest_artifacts_default"
    TYPE2 = "pytest_artifacts_java"
    TYPE3 = "falcon_specific"
    TYPE4 = "hiveserver2_concur_specific"
    TYPE5 = "pytest_artifiact_object_oriented"
    TYPE6 = "oozie_miscellaneous"

    NAMENODE = "namenode"
    DATANODES = "datanodes"
    SECONDARY_NAMENODE = "secondarynamenode"
    RESOURCE_MANAGER = "resourcemanager"
    NODE_MANAGERS = "nodemanagers"
    MR_JOB_HISTORY_SERVER = "jobhistoryserver"
    # timeline server is not implemented yet. Get host API is not in YARN class yet.
    YARN_TIMELINE_SERVER = "timelineserver"
    NIMBUS = "nimbus"
    SUPERVISOR = "supervisor"
    STORM_UI = "stormui"
    HBASE_MASTER = "hbasemaster"
    HBASE_REGION_SERVERS = "hbaseregionservers"
    OOZIE_SERVER = "oozieserver"
    HIVE_METASTORE = "hivemetastore"
    HIVE_SERVER2 = "hiveserver2"
    HIVE_SERVER2_INTERACTIVE = "hsihiveserver2"
    TEMPLETON = "templeton"
    DRUID_BROKER = "druidbroker"
    DRUID_COORDINATOR = "druidcoordinator"
    DRUID_OVERLORD = "druidoverlord"
    DRUID_MIDDLEMANAGERS = "druidmiddlemanagers"
    DRUID_HISTORICALS = "druidhistoricals"
    FALCON_SERVERS = "falconserver"
    KNOX_SERVER = "knoxserver"
    SPARK_HISTORY_SERVER = "sparkhistoryserver"
    LIVY_SERVER = "livyserver"
    LIVY2_SERVER = "livy2server"
    ZEPPELIN_SERVER = "zeppelinserver"
    ATLAS_SERVER = "atlasserver"
    AMBARI_INFRA_SOLR = "ambari_infra_solr"
    AMBARI_INFRA_SOLR_CLIENT = "ambari_infra_solr_client"
    KAFKA_LOGS = "kafka_logs"
    AMBARI_AGENT_LOGS = "ambari_agent_logs"
    NIFI_MASTER = "NIFI_MASTER"
    STREAMLINE_SERVER = "streamline_server"
    SMM_SERVER = "smm_server"
    SMARTSENSE_SERVICE_STR = "smartsenseservice"
    ACTIVITY_COMPONENTS_STR = "activitycomponents"
    BIGSQL_STR = "bigsql"
    BIGSQL_HEAD = "bigsqlhead"
    BIGSQL_WORKER = "bigsqlworker"

    MACHINE_CONFS_TO_COLLECT = ['/etc/passwd', '/etc/group']
    NUM_OF_THREADS = 4

    # A dict for log output
    OUTPUT_LOG_MAP = {
        HDFS_STR: [ARTIFACTS_OUTPUT, TYPE1],
        YARN_STR: [ARTIFACTS_OUTPUT, TYPE1],
        STORM_STR: [ARTIFACTS_OUTPUT, TYPE1],
        MAPRED_STR: [ARTIFACTS_OUTPUT, TYPE1],
        TEZ_STR: [ARTIFACTS_OUTPUT, TYPE1],
        TEZ_V15_STR: [ARTIFACTS_OUTPUT, TYPE1],
        ZOOKEEPER_STR: [ARTIFACTS_OUTPUT, TYPE1],
        HBASE_STR: [ARTIFACTS_OUTPUT, TYPE1],
        OOZIE_STR: [ARTIFACTS_OUTPUT, TYPE1 + "and" + TYPE2 + "and" + TYPE5 + "and" + TYPE6],
        PHOENIX_STR: [ARTIFACTS_OUTPUT, TYPE1],
        PHOENIX_QUERY_STR: [ARTIFACTS_OUTPUT, TYPE1],
        PHOENIX_QS_CONCUR_STR: [ARTIFACTS_OUTPUT, TYPE1],
        MAHOUT_STR: [ARTIFACTS_OUTPUT, TYPE1],
        HIVE_STR: [DATATEAMTEST_OUTPUT, TYPE1 + "or" + TYPE2],
        HIVESERVER2_STR: [DATATEAMTEST_OUTPUT, TYPE1 + "or" + TYPE2],
        HIVESERVER2_INTERACTIVE_STR: [DATATEAMTEST_OUTPUT, TYPE1 + "or" + TYPE2],
        HIVESERVER2CONCUR_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HCATALOG_STR: [DATATEAMTEST_OUTPUT, TYPE1 + "or" + TYPE2],
        HBASELONGRUNNING_STR: [ARTIFACTS_OUTPUT, TYPE1],
        HIVESERVER2CONCURHTTP_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HIVESERVER2CONCURSSLHTTP_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HIVESERVER2CONCURRLDAP_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HIVESERVER2CONCURRTPUSER_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HIVESERVER2CONCURRLDAPHTTP_STR: [ARTIFACTS_OUTPUT, TYPE4],
        HIVESERVER2CONCURRSSL_STR: [ARTIFACTS_OUTPUT, TYPE4],
        WEBHCAT_STR: [WEBHCAT_OUTPUT, TYPE2],
        SQOOP_STR: [ARTIFACTS_OUTPUT, TYPE1],
        PIG_STR: [ARTIFACTS_OUTPUT, TYPE1 + "or" + TYPE4],
        FLUME_STR: [ARTIFACTS_OUTPUT, TYPE1],
        KNOX_STR: [ARTIFACTS_OUTPUT, TYPE2],
        FALCON_STR: [FALCON_OUTPUT, TYPE3],
        ACCUMULO_STR: [ARTIFACTS_OUTPUT, TYPE1],
        HA_STR: [ARTIFACTS_OUTPUT, TYPE1],
        SLIDER_STR: [ARTIFACTS_OUTPUT, TYPE1],
        ROLLINGUPGRADE_STR: [ARTIFACTS_OUTPUT, TYPE1],
        ROLLBACK_STR: [ARTIFACTS_OUTPUT, TYPE1],
        SPARK_STR: [ARTIFACTS_OUTPUT, TYPE1],
        SPARK2_STR: [ARTIFACTS_OUTPUT, TYPE1],
        SPARKHIVE_STR: [ARTIFACTS_OUTPUT, TYPE1],
        XAAGENTS_STR: [ARTIFACTS_OUTPUT, TYPE1],
        ADMIN_STR: [ARTIFACTS_OUTPUT, TYPE1],
        ATLAS_STR: [ATLAS_OUTPUT, TYPE3],
        ATLAS_HSI_STR: [ATLAS_OUTPUT, TYPE3],
        ATLAS_UI: [ATLAS_OUTPUT, TYPE3],
        KAFKA_STR: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR1: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR2: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR3: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR4: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR5: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR6: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR7: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR8: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR9: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR10: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADEDOWNGRADE_STR1: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADEDOWNGRADE_STR2: [ARTIFACTS_OUTPUT, TYPE1],
        AMBARI_ROLLINGUPGRADE_STR11: [ARTIFACTS_OUTPUT, TYPE1],
        HUE_STR: [ARTIFACTS_OUTPUT, TYPE1],
        CLOUDBREAK_STR: [ARTIFACTS_OUTPUT, TYPE1],
        NIFI_STR: [ARTIFACTS_OUTPUT, TYPE2],
        ZEPPELIN_STR: [ARTIFACTS_OUTPUT, TYPE1],
        LIVY_STR: [ARTIFACTS_OUTPUT, TYPE1],
        LIVY2_STR: [ARTIFACTS_OUTPUT, TYPE1],
        DRUID_STR: [ARTIFACTS_OUTPUT, TYPE1],
        SMARTSENSE_STR: [ARTIFACTS_OUTPUT, TYPE1],
        STREAMLINE_STR: [ARTIFACTS_OUTPUT, TYPE2],
        SMM_STR: [ARTIFACTS_OUTPUT, TYPE2],
        AMBARI_INFRA_STR: [ARTIFACTS_OUTPUT, TYPE1],
        BIGSQL_STR: [ARTIFACTS_OUTPUT, TYPE1]
    }

    try:
        SERVICE_LOG_MAP = {
            MAPRED_STR: {
                MR_JOB_HISTORY_SERVER: ComponentLogUtil.MAPRED_getHistoryServerLogDir()
            },
            KNOX_STR: {
                KNOX_SERVER: Config.get('knox', 'KNOX_LOG', 'knoxlogdirnotfound')
            }
        }
        SERVICE_HOSTS_MAP = {
            MAPRED_STR: {
                MR_JOB_HISTORY_SERVER: ComponentLogUtil.MAPRED_getHistoryserver()
            },
            KNOX_STR: {
                KNOX_SERVER: Config.get('knox', 'KNOX_HOST', '')
            }
        }

        # HDFS
        if not ComponentLogUtil.HDFS_isHAEnabled():
            # Non-HA
            SERVICE_LOG_MAP[HDFS_STR] = {
                NAMENODE: ComponentLogUtil.HDFS_getNNLogDir(),
                DATANODES: ComponentLogUtil.HDFS_getDNLogDir(),
                SECONDARY_NAMENODE: ComponentLogUtil.HDFS_getSNNLogDir()
            }
            SERVICE_HOSTS_MAP[HDFS_STR] = {
                NAMENODE: ComponentLogUtil.HDFS_getNamenodes(),
                DATANODES: ComponentLogUtil.HDFS_getDatanodes(),
                SECONDARY_NAMENODE: ComponentLogUtil.HDFS_getSecondaryNamenode()
            }
        else:
            # HA
            SERVICE_LOG_MAP[HDFS_STR] = {
                NAMENODE: ComponentLogUtil.HDFS_getNNLogDir(),
                DATANODES: ComponentLogUtil.HDFS_getDNLogDir()
            }
            SERVICE_HOSTS_MAP[HDFS_STR] = {
                NAMENODE: ComponentLogUtil.HDFS_getNamenodes(),
                DATANODES: ComponentLogUtil.HDFS_getDatanodes()
            }

        # YARN
        if not ComponentLogUtil.YARN_isHAEnabled():
            # Non-HA
            SERVICE_LOG_MAP[YARN_STR] = {
                RESOURCE_MANAGER: ComponentLogUtil.MAPRED_getJTLogDir(),
                NODE_MANAGERS: ComponentLogUtil.MAPRED_getTaskTrackerLogDir()
            }
            SERVICE_HOSTS_MAP[YARN_STR] = {
                RESOURCE_MANAGER: ComponentLogUtil.MAPRED_getJobtracker(),
                NODE_MANAGERS: ComponentLogUtil.MAPRED_getTasktrackers()
            }
        else:
            # HA
            SERVICE_LOG_MAP[YARN_STR] = {
                RESOURCE_MANAGER: ComponentLogUtil.MAPRED_getJTLogDir(),
                NODE_MANAGERS: ComponentLogUtil.MAPRED_getTaskTrackerLogDir()
            }
            SERVICE_HOSTS_MAP[YARN_STR] = {
                RESOURCE_MANAGER: ComponentLogUtil.YARN_getRMHANodes(),
                NODE_MANAGERS: ComponentLogUtil.MAPRED_getTasktrackers()
            }

        # Storm
        if not ComponentLogUtil.Storm_isHA():
            # Non-HA
            SERVICE_LOG_MAP[STORM_STR] = {
                NIMBUS: ComponentLogUtil.Storm_getLogDir(),
                SUPERVISOR: ComponentLogUtil.Storm_getLogDir(),
                STORM_UI: ComponentLogUtil.Storm_getLogDir()
            }
            SERVICE_HOSTS_MAP[STORM_STR] = {
                NIMBUS: ComponentLogUtil.Storm_getNimbus(),
                SUPERVISOR: ComponentLogUtil.Storm_getSupervisors(),
                STORM_UI: ComponentLogUtil.Storm_getWebUIHost()
            }
        else:
            # HA
            SERVICE_LOG_MAP[STORM_STR] = {
                NIMBUS: ComponentLogUtil.Storm_getLogDir(),
                SUPERVISOR: ComponentLogUtil.Storm_getLogDir(),
                STORM_UI: ComponentLogUtil.Storm_getLogDir()
            }
            SERVICE_HOSTS_MAP[STORM_STR] = {
                NIMBUS: ComponentLogUtil.Storm_getNimbusSeeds(),
                SUPERVISOR: ComponentLogUtil.Storm_getSupervisors(),
                STORM_UI: ComponentLogUtil.Storm_getWebUIHost()
            }

        # Kafka
        SERVICE_LOG_MAP[KAFKA_STR] = {KAFKA_LOGS: ComponentLogUtil.Kafka_getServiceLogDir()}
        SERVICE_HOSTS_MAP[KAFKA_STR] = {KAFKA_LOGS: ComponentLogUtil.getAllNodes()}

        # Ambari
        # SERVICE_LOG_MAP[AMBARI_STR] = {AMBARI_AGENT_LOGS: ComponentLogUtil.Ambari_getAgentLogDir()}
        # SERVICE_HOSTS_MAP[AMBARI_STR] = {AMBARI_AGENT_LOGS: ComponentLogUtil.getAllNodes()}

        # Zookeeper
        SERVICE_LOG_MAP[ZOOKEEPER_STR] = {ZOOKEEPER_STR: ComponentLogUtil.Zookeeper_getZooLogDir()}
        SERVICE_HOSTS_MAP[ZOOKEEPER_STR] = {ZOOKEEPER_STR: ComponentLogUtil.Zookeeper_getZKHosts()}

        # HBase
        SERVICE_LOG_MAP[HBASE_STR] = {
            HBASE_MASTER: ComponentLogUtil.HBase_getHBaseLogDir(),
            HBASE_REGION_SERVERS: ComponentLogUtil.HBase_getHBaseLogDir()
        }
        SERVICE_HOSTS_MAP[HBASE_STR] = {
            HBASE_MASTER: ComponentLogUtil.HBase_getMasterNodes(),
            HBASE_REGION_SERVERS: ComponentLogUtil.HBase_getRegionServers()
        }

        # Oozie
        if not ComponentLogUtil.Oozie_isHAEnabled:
            # Non-HA
            SERVICE_LOG_MAP[OOZIE_STR] = {OOZIE_SERVER: ComponentLogUtil.Oozie_getOozieLogDir()}
            SERVICE_HOSTS_MAP[OOZIE_STR] = {OOZIE_SERVER: ComponentLogUtil.Oozie_getOozieServer()}
        else:
            #HA
            SERVICE_LOG_MAP[OOZIE_STR] = {OOZIE_SERVER: ComponentLogUtil.Oozie_getOozieLogDir()}
            SERVICE_HOSTS_MAP[OOZIE_STR] = {OOZIE_SERVER: ComponentLogUtil.Oozie_getOozieServers()}

        #Falcon
        SERVICE_LOG_MAP[FALCON_STR] = {FALCON_SERVERS: ComponentLogUtil.Falcon_getLogDir()}
        SERVICE_HOSTS_MAP[FALCON_STR] = {FALCON_SERVERS: ComponentLogUtil.Falcon_get_all_nodes()}

        #Hive
        SERVICE_LOG_MAP[HIVE_STR] = {
            HIVE_METASTORE: ComponentLogUtil.Hive_getHiveLogDir(),
            HIVE_SERVER2: ComponentLogUtil.Hive_getHiveLogDir(),
            HIVE_SERVER2_INTERACTIVE: ComponentLogUtil.Hive_getHiveLogDir()
        }
        SERVICE_HOSTS_MAP[HIVE_STR] = {
            HIVE_METASTORE: ComponentLogUtil.Hive_getHiveHosts(),
            HIVE_SERVER2: ComponentLogUtil.Hive_getHiveHosts(),
            HIVE_SERVER2_INTERACTIVE: ComponentLogUtil.Hive_getHiveHosts()
        }

        #WebHCat
        SERVICE_LOG_MAP[WEBHCAT_STR] = {TEMPLETON: ComponentLogUtil.Hcatalog_getTempletonLogDir()}
        SERVICE_HOSTS_MAP[WEBHCAT_STR] = {TEMPLETON: ComponentLogUtil.Hive_getTempletonHost()}

        #Accumulo
        SERVICE_LOG_MAP[ACCUMULO_STR] = {
            ACCUMULO_MASTER_STR: ComponentLogUtil.Accumulo_getAccumuloLogDir(),
            ACCUMULO_MONITORS_STR: ComponentLogUtil.Accumulo_getAccumuloLogDir(),
            ACCUMULO_TRACERS_STR: ComponentLogUtil.Accumulo_getAccumuloLogDir(),
            ACCUMULO_TSERVERS_STR: ComponentLogUtil.Accumulo_getAccumuloLogDir()
        }
        SERVICE_HOSTS_MAP[ACCUMULO_STR] = {
            ACCUMULO_MASTER_STR: ComponentLogUtil.Accumulo_get_masters(),
            ACCUMULO_MONITORS_STR: ComponentLogUtil.Accumulo_get_monitors(),
            ACCUMULO_TRACERS_STR: ComponentLogUtil.Accumulo_get_tracers(),
            ACCUMULO_TSERVERS_STR: ComponentLogUtil.Accumulo_get_tservers()
        }

        #Ranger
        SERVICE_LOG_MAP[RANGER_STR] = {
            RANGER_MOCK_SERVICE_1_STR: ComponentLogUtil.Xa_getPolicyMgrLogs(),
            RANGER_MOCK_SERVICE_2_STR: ComponentLogUtil.Xa_getUserSyncLogs()
        }
        SERVICE_HOSTS_MAP[RANGER_STR] = {
            RANGER_MOCK_SERVICE_1_STR: ComponentLogUtil.Xa_getRangerNodes(),
            RANGER_MOCK_SERVICE_2_STR: ComponentLogUtil.Xa_getRangerNodes()
        }

        #Spark
        SERVICE_LOG_MAP[SPARK_STR] = {SPARK_HISTORY_SERVER: ComponentLogUtil.Spark_getSparkLogDir()}
        SERVICE_HOSTS_MAP[SPARK_STR] = {SPARK_HISTORY_SERVER: ComponentLogUtil.Spark_getSparkHistoryServerHostname()}

        #Atlas
        SERVICE_LOG_MAP[ATLAS_STR] = {ATLAS_SERVER: ComponentLogUtil.Atlas_get_log_dir()}
        SERVICE_HOSTS_MAP[ATLAS_STR] = {ATLAS_SERVER: ComponentLogUtil.Atlas_get_host()}

        #Hue
        SERVICE_LOG_MAP[HUE_STR] = {
            HUE_STR: ComponentLogUtil.Hue_get_hue_log_dir(),
            HUE_TEST_STR: ComponentLogUtil.Hue_get_hue_tests_dir()
        }
        SERVICE_HOSTS_MAP[HUE_STR] = {
            HUE_STR: ComponentLogUtil.Hue_get_hue_hosts(),
            HUE_TEST_STR: ComponentLogUtil.Hue_get_hue_hosts()
        }

        #Nifi
        SERVICE_LOG_MAP[NIFI_STR] = {NIFI_MASTER: ComponentLogUtil.Nifi_get_logs_dir()}
        SERVICE_HOSTS_MAP[NIFI_STR] = {NIFI_MASTER: ComponentLogUtil.Nifi_get_hosts()}

        # Streamline
        SERVICE_LOG_MAP[STREAMLINE_STR] = {STREAMLINE_SERVER: ComponentLogUtil.Streamline_get_logs_dir()}
        SERVICE_HOSTS_MAP[STREAMLINE_STR] = {STREAMLINE_SERVER: ComponentLogUtil.Streamline_get_hosts()}

        # SMM
        SERVICE_LOG_MAP[SMM_STR] = {SMM_SERVER: ComponentLogUtil.SMM_get_logs_dir()}
        SERVICE_HOSTS_MAP[SMM_STR] = {SMM_SERVER: ComponentLogUtil.SMM_get_hosts()} #pylint: disable=no-member

        #zeppelin
        SERVICE_LOG_MAP[ZEPPELIN_STR] = {ZEPPELIN_SERVER: ComponentLogUtil.Zeppelin_getZeppelinLogDir()}
        SERVICE_HOSTS_MAP[ZEPPELIN_STR] = {ZEPPELIN_SERVER: ComponentLogUtil.Zeppelin_getZeppelinServerHostname()}

        #livy
        SERVICE_LOG_MAP[LIVY_STR] = {LIVY_SERVER: ComponentLogUtil.Livy_getLivyLogDir()}
        SERVICE_HOSTS_MAP[LIVY_STR] = {LIVY_SERVER: ComponentLogUtil.Livy_getLivyServerHostname()}

        #livy2
        SERVICE_LOG_MAP[LIVY2_STR] = {LIVY2_SERVER: ComponentLogUtil.Livy_getLivy2LogDir()}
        SERVICE_HOSTS_MAP[LIVY2_STR] = {LIVY2_SERVER: ComponentLogUtil.Livy_getLivy2ServerHostname()}

        #Druid
        SERVICE_LOG_MAP[DRUID_STR] = {
            DRUID_BROKER: ComponentLogUtil.Druid_get_logs_dir(),
            DRUID_COORDINATOR: ComponentLogUtil.Druid_get_logs_dir(),
            DRUID_OVERLORD: ComponentLogUtil.Druid_get_logs_dir(),
            DRUID_MIDDLEMANAGERS: ComponentLogUtil.Druid_get_logs_dir(),
            DRUID_HISTORICALS: ComponentLogUtil.Druid_get_logs_dir()
        }
        SERVICE_HOSTS_MAP[DRUID_STR] = {
            DRUID_BROKER: ComponentLogUtil.Druid_getDruidBroker(),
            DRUID_COORDINATOR: ComponentLogUtil.Druid_getDruidCoordinator(),
            DRUID_OVERLORD: ComponentLogUtil.Druid_getDruidOverlord(),
            DRUID_MIDDLEMANAGERS: ComponentLogUtil.Druid_getDruidMiddleManagers(),
            DRUID_HISTORICALS: ComponentLogUtil.Druid_getDruidHistoricals()
        }

        #SmartSense
        SERVICE_LOG_MAP[SMARTSENSE_STR] = {
            SMARTSENSE_SERVICE_STR: ComponentLogUtil.SmartSense_getSmartSenseServiceLogDir(),
            ACTIVITY_COMPONENTS_STR: ComponentLogUtil.SmartSense_getActivityComponentsLogDir()
        }
        SERVICE_HOSTS_MAP[SMARTSENSE_STR] = {
            SMARTSENSE_SERVICE_STR: ComponentLogUtil.SmartSense_getSmartSenseServiceNodes(),
            ACTIVITY_COMPONENTS_STR: ComponentLogUtil.SmartSense_getActivityComponentsNodes()
        }
        # Ambari_Infra
        SERVICE_LOG_MAP[AMBARI_INFRA_STR] = {
            AMBARI_INFRA_SOLR: ComponentLogUtil.Ambari_infra_solr_get_logs_dir(),
            AMBARI_INFRA_SOLR_CLIENT: ComponentLogUtil.Ambari_infra_solrclient_get_logs_dir()
        }
        SERVICE_HOSTS_MAP[AMBARI_INFRA_STR] = {
            AMBARI_INFRA_SOLR: ComponentLogUtil.Ambari_infra_solr_get_hosts(),
            AMBARI_INFRA_SOLR_CLIENT: ComponentLogUtil.Ambari_infra_solrclient_get_hosts()
        }

        # BigSql
        SERVICE_LOG_MAP[BIGSQL_STR] = {
            BIGSQL_HEAD: ComponentLogUtil.BigSql_getLogsDir(),
            BIGSQL_WORKER: ComponentLogUtil.BigSql_getLogsDir()
        }
        SERVICE_HOSTS_MAP[BIGSQL_STR] = {
            BIGSQL_HEAD: ComponentLogUtil.BigSql_getHeadNodes(),
            BIGSQL_WORKER: ComponentLogUtil.BigSql_getWorkerNodes()
        }

    except Exception:
        logger.error("Exception occured during logUtil SERVICE LOG/HOST map construction")
        logger.error(traceback.format_exc())

    def __init__(self):
        pass

    @classmethod
    def get_log_aggregation_Dir_Locations(cls):
        '''
        Gets base dir to for log aggregation.
        Returns a tuple of (str, str, str, str).
        Returns a tuple of
            (cluster name, temporary cluster dir for logs, temporary dir for applications, test component)
        '''
        config = ConfigParser()
        reportconf = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'test_report.conf')
        SECTION = "HW-QE-PUBLISH-REPORT"
        config.optionxform = str
        config.read(reportconf)
        CLUSTER_NAME = config.get(SECTION, "CLUSTER_NAME")
        cls.LOCAL_TMP_CLUSTER_DIR = Config.getEnv('ARTIFACTS_DIR')
        cls.LOCAL_TMP_APP_STORAGE = os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, "Application-logs")
        cls.LOCAL_TMP_LLAP_APP_STORAGE = os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, "Llap-Application-logs")
        cls.COMPONENT = ''
        if config.has_option(SECTION, 'TESTSUITE_COMPONENT'):
            cls.COMPONENT = config.get(SECTION, 'TESTSUITE_COMPONENT')
            logger.info("Set LogUtil.COMPONENT to %s", cls.COMPONENT)
        return (
            CLUSTER_NAME, cls.LOCAL_TMP_CLUSTER_DIR, cls.LOCAL_TMP_APP_STORAGE, cls.LOCAL_TMP_LLAP_APP_STORAGE,
            cls.COMPONENT
        )

    @classmethod
    def get_log_aggregation_Dir_Locations_Mawo(cls):
        '''
        Gets base dir to for log aggregation.
        Returns a tuple of (str, str, str, str).
        Returns a tuple of
            (cluster name, temporary cluster dir for logs, temporary dir for applications, test component)
        '''
        config = ConfigParser()
        reportconf = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'test_report.conf')
        SECTION = "HW-QE-PUBLISH-REPORT"
        config.optionxform = str
        config.read(reportconf)
        CLUSTER_NAME = config.get(SECTION, "CLUSTER_NAME")
        SPLIT_NUMBER = config.get(SECTION, "split_number")
        cls.LOCAL_TMP_CLUSTER_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), CLUSTER_NAME)
        cls.LOCAL_TMP_APP_STORAGE = os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, "Application-logs")
        cls.COMPONENT = ''
        if config.has_option(SECTION, 'TESTSUITE_COMPONENT'):
            cls.COMPONENT = config.get(SECTION, 'TESTSUITE_COMPONENT')
            logger.info("Set LogUtil.COMPONENT to %s", cls.COMPONENT)
        CLUSTER_NAME_MAWO = '%s/%s_%s' % (CLUSTER_NAME, CLUSTER_NAME, SPLIT_NUMBER)
        return (CLUSTER_NAME_MAWO, cls.LOCAL_TMP_CLUSTER_DIR, cls.LOCAL_TMP_APP_STORAGE, cls.COMPONENT)

    @classmethod
    def createDirInternal(cls, host, user, filepath, passwd, perm="777", logoutput=False):
        '''
        Create log dir and grant permission on log server only.
        Returns None
        '''
        # permission of 644 won't succeed to rsync.
        LogUtilMachine.makedirs(user=user, host=host, filepath=filepath, passwd=passwd)
        LogUtilMachine.chmod(
            perm=perm, filepath=filepath, recursive=False, user=user, host=host, passwd=passwd, logoutput=logoutput
        )

    @classmethod
    def create_Logs_directories(  # pylint: disable=redefined-builtin
            cls, logHost, logHostUser, logHostBasePath, passwd=None, logoutput=False, type="all"
    ):
        '''
        Create log directories for log collection.
        type : Choose the type to create directory
               if type = "service-logs" , <logHostBasepath>/service-logs dir will be created on logHost
               if type = "app-logs", <logHostBasepath>/app-logs dir will be created on logHost
               if type = "llap-app-logs", <logHostBasepath>/llap-app-logs dir will be created on logHost
               if type == "test-logs", <logHostBasepath>/test-logs dir will be created on logHost
               if type == "artifacts", <logHostBasepath>/artifacts dir will be created on logHost
               if type == "test-logs", <logHostBasepath>/test-logs dir will be created on logHost
               if type == "jenkins-logs", <logHostBasepath>/jenkins-logs dir will be created on logHost
               if type == all, all of above directories will be created.
        Returns None.
        '''
        cls.get_log_aggregation_Dir_Locations()
        # create temp dir in gateway for app logs
        if not Machine.pathExists(None, logHost, cls.LOCAL_TMP_CLUSTER_DIR, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_CLUSTER_DIR, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_CLUSTER_DIR, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )
        if not Machine.pathExists(None, None, cls.LOCAL_TMP_APP_STORAGE, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_APP_STORAGE, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_APP_STORAGE, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )
        if not Machine.pathExists(None, None, cls.LOCAL_TMP_LLAP_APP_STORAGE, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_LLAP_APP_STORAGE, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_LLAP_APP_STORAGE, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(),
                True
            )
        cls.createDirInternal(logHost, logHostUser, logHostBasePath, passwd, logoutput=logoutput)

        # create base dirs in log server
        if type == "all" or type == "service-logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "service-logs", passwd, logoutput=logoutput
            )
        if type == "all" or type == "app-logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "app-logs", passwd, logoutput=logoutput
            )

        if type == "all" or type == "llap-app-logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "llap-app-logs", passwd, logoutput=logoutput
            )

        if type == "all" or type == "test-logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "test-logs", passwd, logoutput=logoutput
            )
        if type == "all" or type == "deploy_logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "deploy_logs", passwd, logoutput=logoutput
            )
        if type == "all" or type == "artifacts":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "artifacts", passwd, logoutput=logoutput
            )
        if cls.COMPONENT != '':
            if type == "all" or type == "test-logs":
                cls.createDirInternal(
                    logHost,
                    logHostUser,
                    logHostBasePath + "/" + "test-logs" + "/" + cls.COMPONENT,
                    passwd,
                    logoutput=logoutput
                )
        if type == "all" or type == "conf":
            cls.createDirInternal(logHost, logHostUser, logHostBasePath + "/" + "conf", passwd, logoutput=logoutput)
        if type == "all" or type == "machineConf":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "machineConf", passwd, logoutput=logoutput
            )
        if type == "all" or type == "jenkins-logs":
            cls.createDirInternal(
                logHost, logHostUser, logHostBasePath + "/" + "jenkins-logs", passwd, logoutput=logoutput
            )

    @classmethod
    def collect_application_log_locally(cls, appId, user):
        '''
        Collects application log and save it in Local Dir with <appId>.log filename
        '''
        try:
            from beaver.component.hadoop import YARN
            filename = os.path.join(cls.LOCAL_TMP_APP_STORAGE, appId + ".log")
            if not Machine.pathExists(None, None, filename, None):
                logger.info("Storing syslog of %s in %s", appId, filename)
                YARN.getLogsApplicationID(appId, user, None, None, False, None, filename)
            else:
                logger.info("%s already present at %s", appId, filename)
        except Exception:
            logger.error("Exception occured during collect_application_log_locally() call")
            logger.error(traceback.format_exc())

    @classmethod
    def collect_llap_application_logs_locally(cls):
        '''
        Collects application log and save it in Local Dir with <appId>.log filename
        '''
        try:
            from beaver.component.hadoop import YARN
            apps = YARN.getApplicationIDList(state='ALL', searchString='llap0')
            logger.info(apps)
            for appId in apps:
                filename = os.path.join(cls.LOCAL_TMP_LLAP_APP_STORAGE, appId + ".log")
                if not Machine.pathExists(None, None, filename, None):
                    logger.info("Storing llap-app-log of %s in %s", appId, filename)
                    YARN.getLogsApplicationID(
                        appId, Config.get('hive', 'HIVE_USER'), None, None, False, None, filename
                    )
                else:
                    logger.info("%s already present at %s", appId, filename)
        except Exception:
            logger.error("Exception occured during collect_application_log_locally() call")
            logger.error(traceback.format_exc())

    @classmethod
    def collect_application_log_for_Falcon_locally(cls, JobSummaryLogfile, appId, user):
        '''
        Collects application logs for Falcon component and save it in Local Dir with <appId>.log filename
        '''
        host = re.search("jobsummary_(.*).log", JobSummaryLogfile).group(1)
        if not Machine.pathExists(None, None, os.path.join(cls.LOCAL_TMP_APP_STORAGE, host), None):
            Machine.makedirs(None, None, os.path.join(cls.LOCAL_TMP_APP_STORAGE, host), None)
            Machine.chmod(
                "777",
                os.path.join(cls.LOCAL_TMP_APP_STORAGE, host),
                recursive=True,
                user=None,
                host=None,
                passwd=None,
                logoutput=True
            )
        filename = os.path.join(cls.LOCAL_TMP_APP_STORAGE, host, appId + ".log")
        try:
            from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
            Falcon.get_application_log(
                host,
                appId,
                appOwner=user,
                nodeAddress=None,
                containerId=None,
                logoutput=False,
                grepFilter=None,
                pipeToFileOutput=filename
            )
        except Exception:
            logger.error("Exception occured during collect_application_log_for_Falcon_locally() call")
            logger.error(traceback.format_exc())
            logger.info("Get application log for Falcon is broken")

    @classmethod
    def gather_applicationId_user_mapping(  # pylint: disable=unused-argument
            cls, JobSummaryLogfiles, startTime, endTime, isFalcon=False, ignoreStartEndTime=False
    ):
        '''
        Function to Find application-user-jobsummaryLog mapping to gather app logs
        :param JobSummaryLogfiles: List of JobSummary Files
        :param startTime: Test Start Time
        :param endTime: Test end time
        :param isFalcon: if falcon is component
        :param ignoreStartEndTime:  Ignore startend time (for --gatherAllAppLogs/--gatherAllTestAppLogs)
        :return:A dictionary object in below format for which app logs needs to be collected
        {'app1':['user1', 'jobsummaryfile'], 'app2':['user2', 'jobsummaryfile']}
        '''
        appId_user_map = collections.defaultdict(dict)
        for JobSummaryLogfile in JobSummaryLogfiles:
            logger.info("Reading from %s", JobSummaryLogfile)
            f = open(JobSummaryLogfile)
            # pylint: disable=line-too-long
            msg = r"(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*ApplicationSummary: appId=(.*),name=.*,user=(.*),queue=.*,state=.*,trackingUrl=.*,appMasterHost=.*,startTime=[0-9]{13},finishTime=[0-9]{13}"
            # pylint: enable=line-too-long
            # Find out applicationIds between start/end time stamp
            for line in f:
                m = re.search(msg, line)
                if m:
                    appId = m.group(2)
                    user = m.group(3)
                if not ignoreStartEndTime:
                    currTime = time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
                    if currTime >= startTime and currTime <= endTime:
                        appId_user_map[appId] = [user, JobSummaryLogfile]
                else:
                    appId_user_map[appId] = [user, JobSummaryLogfile]
            f.close()
        return appId_user_map

    @classmethod
    def create_parallel_thread_pool(cls, appId_user_map, numofthreads):
        '''
        Create Parallel Thread pool from collection of applications.
        Example:
        :param appId_user_map: {
            app1: [user1, jobsummaryfile],
            app2:[user2,jobsummaryfile],
            app3:[user3,jobsummaryfile],
            app4:[user4,jobsummaryfile],
            app5:[user5,jobsummaryfile]
        }
        :param numofthreads: 2
        This function will create sets of 2 applicationId objects
        1 => app1 , app2
        2 => app3, app4
        3 => app5
        output = {
            "1": {"app1": ["user1", "jobsummaryfile"], "app2": ["user2", "jobsummaryfile"]},
            "2": {"app3": ["user3", "jobsummaryfile"], "app4":["user4", "jobsummaryfile"]},
            "3": {"app5": ["user5", "jobsummaryfile"]}
        }
        '''
        count = 0
        key = 0
        parallel_appid_user_map = collections.defaultdict(dict)
        for appId, user in appId_user_map.items():
            if count % numofthreads == 0:
                key = key + 1
            parallel_appid_user_map[str(key)][appId] = user
            count = count + 1
        return parallel_appid_user_map

    @classmethod
    def gather_application_log_from_JobSummary(
            cls,
            JobSummaryLogfiles,
            startTime,
            endTime,
            isFalcon=False,
            ignoreStartEndTime=False,
            numofthreads=NUM_OF_THREADS
    ):
        '''
        Function to Find applications from Jobsummary log and gather application logs which started
                between startTime and endTime
        startTime and endTime will be in epoch format
        Returns None
        '''
        appId_user_map = cls.gather_applicationId_user_mapping(
            JobSummaryLogfiles, startTime, endTime, isFalcon=isFalcon, ignoreStartEndTime=ignoreStartEndTime
        )
        pool = cls.create_parallel_thread_pool(appId_user_map, numofthreads)
        for iter_, appsuserjs in pool.iteritems():
            logger.info("*** Start Pool number : %s ***", iter_)
            threads = []
            print threads
            for app in appsuserjs:
                appId = app
                user = appsuserjs[app][0]
                js = appsuserjs[app][1]
                logger.info("Gather app log for %s", appId)
                if isFalcon:
                    thread = Thread(target=cls.collect_application_log_for_Falcon_locally, args=(js, appId, user))
                else:
                    thread = Thread(target=cls.collect_application_log_locally, args=(appId, user))
                threads.append(thread)
                thread.start()
            logger.info("*** Wait for threads from pool %s to Finish", iter_)
            for thread in threads:
                thread.join()

    @classmethod
    def find_start_end_time_failed_test(cls, output_log, test_name, type=TYPE1):  # pylint: disable=redefined-builtin
        '''
        Function to find out start time and end time for the failed test.
        output = artifacts/output.log (Python output file)
        returns (epoch starttime, epoch endtime)
        '''
        try:
            # modify test name as per artifacts/output.log
            test_name = test_name.replace("pytest-", "")
            test_name = test_name.replace('[', r'\[', 1)
            test_name_remain = test_name.rsplit(']', 1)
            test_name = r'\]'.join(test_name_remain)

            startTime = endTime = None
            dformat = "%Y-%m-%d %H:%M:%S"

            # pylint: disable=line-too-long
            if type == cls.TYPE1:
                start_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*RUNNING TEST "%s" at location.*at line number "\d+"' % test_name
                end_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*TEST "%s" FAILED in \d+.\d+ seconds' % test_name
            if type == cls.TYPE5:
                start_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*RUNNING TEST "\w+\.%s" at location.*at line number "\d+"' % test_name
                end_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*TEST "\w+\.%s" FAILED in \d+.\d+ seconds' % test_name
            # pylint: enable=line-too-long
            # set flag forAllTests to False.
            # If we can not find start time of the test_case, we will set flag forAllTests to True
            # If forAllTests flag is set True, we will get start/end time of test function instead test case.
            Flag_forAllTests = False

            # open output file
            f = open(output_log)
            text = f.read()

            # find start_time
            start_lines = re.findall(start_test_pattern, text)
            if len(start_lines) == 1:
                startTime = time.mktime(time.strptime(start_lines[0], dformat))
            else:
                logger.info("can not find start_time for testcase %s.", test_name)
                Flag_forAllTests = True
                index = test_name.find(r"\[")
                test_name = test_name[:index]
                logger.info("Finding start time of test function : %s", test_name)
                # pylint: disable=line-too-long
                start_full_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*RUNNING TEST "%s.*.*' % test_name
                # pylint: enable=line-too-long
                start_lines = re.findall(start_full_test_pattern, text)
                startTime = util.get_min_or_max_time(start_lines, dformat, "min")

            # find end_time
            if not Flag_forAllTests:
                end_lines = re.findall(end_test_pattern, text)
                endTime = time.mktime(time.strptime(end_lines[0], dformat)) + 20.0
            else:
                logger.info("Finding end_Time for the complete test suit %s", test_name)
                # pylint: disable=line-too-long
                end_full_test_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*TEST "%s.*.*' % test_name
                # pylint: enable=line-too-long
                end_lines = re.findall(end_full_test_pattern, text)
                # adding 20 sec buffer period. I have seen the jobSummary take few more secs to put entry in log.
                endTime = util.get_min_or_max_time(end_lines, dformat, "max") + 20.0
            f.close()
            return startTime, endTime, Flag_forAllTests
        except Exception:
            return None, None, False

    @classmethod
    def get_start_end_pattern_for_other_types(cls, type, failTest):  # pylint: disable=redefined-builtin
        '''
        Find start/end message pattern for Type2 , Type3 and Type4
        returns Testcase_start_pattern , Testcase_end_pattern
        '''
        start_log_pattern = end_log_pattern = None
        i1 = failTest.find("[")
        i2 = failTest.rfind("]")
        if type == cls.TYPE2:
            test_name = failTest[i1 + 1:i2].split(".")[-1]
            start_log_pattern = r"Beginning test %s at (\d{1,10})" % test_name
            end_log_pattern = r"Ending test %s at (\d{1,10})" % test_name
        if type == cls.TYPE3:
            test_name = failTest[i1 + 1:i2]
            name = test_name.replace("(", r"\(")
            name = name.replace(")", r"\)")
            name = name.replace("[", r"\[")
            name = name.replace("]", r"\]")

            # pylint: disable=line-too-long
            start_log_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*Testing going to start for: %s.*' % name
            end_log_pattern = r'(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*Testing going to end for: %s.*' % name
            # pylint: enable=line-too-long
        if type == cls.TYPE4:
            test_name = failTest[i1 + 1:i2]
            start_log_pattern = r"Beginning test %s.* at (\d{1,10})" % test_name
            end_log_pattern = r"Ending test %s.* at (\d{1,10})" % test_name
        if type == cls.TYPE6:
            test_name = failTest.replace("pytest-", "")
            test_name = test_name.replace("[", r"\[")
            test_name = test_name.replace("]", r"\]")
            start_log_pattern = r"Beginning test %s at (\d{1,10})" % test_name
            end_log_pattern = r"Ending test %s at (\d{1,10})" % test_name

        return start_log_pattern, end_log_pattern

    @classmethod
    def find_start_end_time_failed_test_other(cls, output_log, failTest, type):  # pylint: disable=redefined-builtin
        '''
        Find start/end time in epoch format for failed test
        output_log = Output file which follows, testcase_start_pattern = Beginning test <Testcase> <epoch Time>
        failTest =  Failed Testcase name
        type = Type2 or Type3 or Type4
        Returns (start_test_time, end_test_time) in epoch format
        if start_test_time or end_test_time is not found, it returns (None, None)
        '''
        try:
            startTime = endTime = None
            start_log_pattern, end_log_pattern = cls.get_start_end_pattern_for_other_types(type, failTest)

            f = open(output_log)
            text = f.read()
            # find beginning and end of the test
            tmp_start = re.findall(start_log_pattern, text)
            if len(tmp_start) == 1:
                if type == cls.TYPE2 or type == cls.TYPE4 or type == cls.TYPE6:
                    startTime = float(tmp_start[0])
                else:
                    startTime = time.mktime(time.strptime(tmp_start[0], "%Y-%m-%d %H:%M:%S"))
            tmp_end = re.findall(end_log_pattern, text)
            if len(tmp_end) == 1:
                if type == cls.TYPE2 or type == cls.TYPE4 or type == cls.TYPE6:
                    endTime = float(tmp_end[0]) + 20.0
                else:
                    endTime = time.mktime(time.strptime(tmp_end[0], "%Y-%m-%d %H:%M:%S")) + 20.0
            f.close()
            return startTime, endTime
        except Exception:
            return None, None

    @classmethod
    def get_correct_output_file(cls, output_dir):
        '''
        Function to find correct test_harness_<time> output file for Hive component
        Returns test_harness output file
        '''
        try:
            lines = Machine.find(
                user=Machine.getAdminUser(),
                host=None,
                filepath=output_dir,
                searchstr="test_harnesss_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]",
                passwd=Machine.getAdminPasswd()
            )
            logger.info(lines)
            files = []
            # grep the latest file
            for file_ in lines:
                files.append(file_.split("/")[-1])
            out_file = os.path.join(output_dir, max(files))
            logger.info("Hive Output File")
            logger.info(out_file)
            if Machine.pathExists(Machine.getAdminUser(), None, out_file, Machine.getAdminPasswd()):
                return out_file
            else:
                return None
        except Exception:
            return None

    @classmethod
    def find_pig_other_output_file(cls, output_log):
        '''
        Function to find output file of test_pig.
        returns output file
        '''
        try:
            p = "pig output log file :(.*)"
            f = open(output_log)
            m = re.findall(p, f.read())
            f.close()
            out_file = m[0].strip()
            if Machine.pathExists(Machine.getAdminUser(), None, out_file, Machine.getAdminPasswd()):
                return out_file
            else:
                return None
        except Exception:
            return None

    @classmethod
    def get_webhcat_output_file(cls, output_dir):
        '''
        Find out test_harness output files for webhcat component
        '''
        files = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath=output_dir,
            searchstr="test_harnesss_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]",
            passwd=Machine.getAdminPasswd()
        )
        return files

    @classmethod
    def find_start_end_time_for_complete_test_run(cls, config, section):
        '''
        Function to find out start and end time of the complete test execution
        '''
        startTime = float(config.get(section, "START_TIME").strip())
        endTime = float(config.get(section, "END_TIME").strip()) + 20.0
        return startTime, endTime

    @classmethod
    def get_local_job_summary_logs(cls, component):
        """
        Copy Job_summary Logs to local dirs [artifacts/job_summary_local.log]
        parameter: component : Component name for which log collection is taking place
        return: List of Local copies of Job summary log
        Note: Some components need special handling where there are multiple Job Summary Log files
              such as HA and Falcon
        """
        LocalJobSummaryLogs = []
        try:
            if component == cls.HA_STR:
                for host in ComponentLogUtil.YARN_getRMHANodes():
                    JobSummaryLog = ComponentLogUtil.MAPRED_getJobSummaryLogFile(host)
                    LocalJobSummaryLog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "jobsummary_" + host + ".log")
                    Machine.copyToLocal(None, host, JobSummaryLog, LocalJobSummaryLog, None)
                    if Machine.pathExists(None, None, LocalJobSummaryLog, passwd=None):
                        LocalJobSummaryLogs.append(LocalJobSummaryLog)
            elif component == cls.FALCON_STR:
                from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
                host1 = Falcon.get_cluster_1_masters()['rm']
                host2 = Falcon.get_cluster_2_masters()['rm']
                host3 = Falcon.get_cluster_3_masters()['rm']
                for host in [host1, host2, host3]:
                    JobSummaryLog = ComponentLogUtil.MAPRED_getJobSummaryLogFile(host)
                    LocalJobSummaryLog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "jobsummary_" + host + ".log")
                    Machine.copyToLocal(None, host, JobSummaryLog, LocalJobSummaryLog, None)
                    if Machine.pathExists(None, None, LocalJobSummaryLog, passwd=None):
                        LocalJobSummaryLogs.append(LocalJobSummaryLog)
            else:
                JobSummaryLog = ComponentLogUtil.MAPRED_getJobSummaryLogFile()
                LocalJobSummaryLog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "jobsummary.log")
                Machine.copyToLocal(
                    None, ComponentLogUtil.MAPRED_getJobtracker(), JobSummaryLog, LocalJobSummaryLog, None
                )
                if Machine.pathExists(None, None, LocalJobSummaryLog, passwd=None):
                    LocalJobSummaryLogs.append(LocalJobSummaryLog)
            return LocalJobSummaryLogs
        except Exception as e:
            logger.info("Exception occurs at job_summary_log collection %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
            return LocalJobSummaryLogs

    @classmethod
    def get_current_component(cls, config, section):
        """
        Findout the current component.
        Due to split of tests, component name can be "mapreduce_1", "mapreduce_2".
                This function handles such splits and returns component = Mapreduce
        """
        curr_component = config.get(section, "TESTSUITE_COMPONENT").lower()
        logger.info("get_current_component before pattern matching: curr_component = %s", curr_component)
        try:
            curr_ex_framework = config.get(section, "execution_framework").lower()
        except Exception:
            curr_ex_framework = curr_component
        if "tez_v15" in curr_component:
            return "tez_v15"
        pattern = "([a-z]+[0-9]{0,1})_[0-9]+"
        m = re.search(pattern, curr_component)
        if m:
            curr_component = m.group(1)
        logger.info("get_current_component after pattern matching: curr_component = %s", curr_component)
        if curr_component.strip().endswith("ha"):
            return cls.HA_STR
        elif curr_component.strip().endswith("slider") or curr_ex_framework.strip().endswith("slider"):
            return cls.SLIDER_STR
        else:
            return curr_component

    @classmethod
    def parse_failed_test_and_find_app_logs(cls, component_for_test_app_logs="default", ignoreStartEndTime=False):
        """
        - Finds out proper output file as per component
        - Prepares list of Failed tests
        - For each failed test case,
            Finds start/end time of test case
            Finds applicationIds between start/end time
            Downloads application log at LOCAL_TMP_APP_STORAGE
        """
        # parse outputlog file
        config = ConfigParser()
        SECTION = "HW-QE-PUBLISH-REPORT"
        config.optionxform = str
        config.read(os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'test_report.conf'))

        # presteps: Find out Component
        curr_component = cls.get_current_component(config, SECTION)

        # presteps: Gather jobSummary log
        LocalJobSummaryLogs = cls.get_local_job_summary_logs(curr_component)

        # List of components which will collect application log for Entire NAT run regardless of Pass/Failure
        # if component_for_test_app_logs=true, full application log for that component can be captured.
        Default_component_for_complete_log_collection = [
            cls.HA_STR, cls.SLIDER_STR, cls.FALCON_STR, cls.ROLLINGUPGRADE_STR, cls.ROLLBACK_STR,
            cls.AMBARI_ROLLINGUPGRADE_STR1, cls.AMBARI_ROLLINGUPGRADE_STR2, cls.AMBARI_ROLLINGUPGRADE_STR3,
            cls.AMBARI_ROLLINGUPGRADE_STR4, cls.AMBARI_ROLLINGUPGRADE_STR5, cls.AMBARI_ROLLINGUPGRADE_STR6,
            cls.AMBARI_ROLLINGUPGRADE_STR7, cls.AMBARI_ROLLINGUPGRADE_STR8, cls.AMBARI_ROLLINGUPGRADE_STR9,
            cls.AMBARI_ROLLINGUPGRADE_STR10, cls.AMBARI_ROLLINGUPGRADEDOWNGRADE_STR1,
            cls.AMBARI_ROLLINGUPGRADEDOWNGRADE_STR2, cls.AMBARI_ROLLINGUPGRADE_STR11
        ]
        if component_for_test_app_logs == "default":
            component_for_complete_log_collection = list(Default_component_for_complete_log_collection)
        else:
            component_for_complete_log_collection = list(Default_component_for_complete_log_collection)
            if curr_component not in Default_component_for_complete_log_collection:
                component_for_complete_log_collection.append(curr_component)

        logger.info("Complete list for collecting App logs for Full NAT run")
        logger.info(component_for_complete_log_collection)
        flagTocollectFullLogs = False

        # validate timezone of RM and Gateway
        isTzSame = Machine.validateTimeZoneSame(
            ComponentLogUtil.MAPRED_getJobtracker(), ComponentLogUtil.HDFS_getGateway()
        )
        # if TimeZone is not same ,Enable component to collect App log for complete run
        if not isTzSame:
            component_for_complete_log_collection.append(curr_component)

        # find out failed tests list
        failed_tests = []
        result = util.parseJUnitXMLResult(os.path.join(Config.getEnv('ARTIFACTS_DIR'), "junitresults.xml"))
        for k, v in result.items():
            if (v['result']) == "fail":
                failed_tests.append(k)

        skipTests = []

        logger.info("parse_failed_test_and_find_app_logs curr_component = %s", curr_component)
        # find out start/end time for all components
        if curr_component in [cls.HIVESERVER2_STR, cls.HIVESERVER2_INTERACTIVE_STR, cls.HCATALOG_STR, cls.HIVE_STR]:
            output_log = cls.get_correct_output_file(cls.OUTPUT_LOG_MAP[curr_component][0])
            other_output = cls.ARTIFACTS_OUTPUT
        elif curr_component == cls.WEBHCAT_STR:
            output_Files = cls.get_webhcat_output_file(cls.OUTPUT_LOG_MAP[curr_component][0])
        else:
            output_log = cls.OUTPUT_LOG_MAP[curr_component][0]
        if curr_component == cls.PIG_STR:
            other_output = cls.find_pig_other_output_file(output_log)
        for failTest in failed_tests:
            if flagTocollectFullLogs:
                break
            start_time = end_time = None
            if util.get_test_name(failTest) in skipTests:
                continue
            if cls.OUTPUT_LOG_MAP[curr_component][1] in [cls.TYPE2, cls.TYPE3, cls.TYPE4]:
                if curr_component == cls.WEBHCAT_STR:
                    for outfile in output_Files:
                        start_time, end_time = cls.find_start_end_time_failed_test_other(
                            outfile, failTest, cls.OUTPUT_LOG_MAP[curr_component][1]
                        )
                        if start_time or end_time:
                            break
                else:
                    start_time, end_time = cls.find_start_end_time_failed_test_other(
                        output_log, failTest, cls.OUTPUT_LOG_MAP[curr_component][1]
                    )
            if cls.OUTPUT_LOG_MAP[curr_component][1] == cls.TYPE1:
                start_time, end_time, flag = cls.find_start_end_time_failed_test(output_log, failTest)
                if flag is True:
                    test_name = util.get_test_name(failTest)
                    skipTests.append(test_name)
            if cls.OUTPUT_LOG_MAP[curr_component
                                  ][1] == cls.TYPE1 + "and" + cls.TYPE2 + "and" + cls.TYPE5 + "and" + cls.TYPE6:
                start_time, end_time = cls.find_start_end_time_failed_test_other(output_log, failTest, cls.TYPE2)
                if not start_time or not end_time:
                    start_time, end_time = cls.find_start_end_time_failed_test_other(output_log, failTest, cls.TYPE6)
                if not start_time or not end_time:
                    start_time, end_time, flag = cls.find_start_end_time_failed_test(output_log, failTest)
                if not start_time or not end_time:
                    start_time, end_time, flag = cls.find_start_end_time_failed_test(
                        output_log, failTest, type=cls.TYPE5
                    )
            if cls.OUTPUT_LOG_MAP[curr_component][1] == cls.TYPE1 + "or" + cls.TYPE2:
                start_time, end_time = cls.find_start_end_time_failed_test_other(output_log, failTest, cls.TYPE2)
                if not start_time or not end_time:
                    start_time, end_time, flag = cls.find_start_end_time_failed_test(other_output, failTest)
            if cls.OUTPUT_LOG_MAP[curr_component][1] == cls.TYPE1 + "or" + cls.TYPE4:
                if other_output:
                    start_time, end_time = cls.find_start_end_time_failed_test_other(other_output, failTest, cls.TYPE4)
                if not start_time or not end_time:
                    start_time, end_time, flag = cls.find_start_end_time_failed_test(output_log, failTest)

            if curr_component in component_for_complete_log_collection:
                logger.info("Gathering application log for complete test run")
                start_time, end_time = cls.find_start_end_time_for_complete_test_run(config, SECTION)
                flagTocollectFullLogs = True

            # find out Application logs from start/end time
            logger.info("**** Handling  %s ****", failTest)
            logger.info("Start_time = %s  , End_time = %s", start_time, end_time)
            if not start_time or not end_time:
                logger.info("start_time or end_time not found from output.log for %s", failTest)
            else:
                if curr_component == cls.FALCON_STR:
                    cls.gather_application_log_from_JobSummary(
                        LocalJobSummaryLogs, start_time, end_time, True, ignoreStartEndTime=ignoreStartEndTime
                    )
                elif curr_component == cls.SLIDER_STR:
                    cls.gather_application_log_from_JobSummary(LocalJobSummaryLogs, start_time, end_time, False, True)
                else:
                    cls.gather_application_log_from_JobSummary(
                        LocalJobSummaryLogs, start_time, end_time, ignoreStartEndTime=ignoreStartEndTime
                    )

        if failed_tests == [] and ignoreStartEndTime is True:
            if curr_component == cls.FALCON_STR:
                cls.gather_application_log_from_JobSummary(
                    LocalJobSummaryLogs, None, None, True, ignoreStartEndTime=ignoreStartEndTime
                )
            else:
                cls.gather_application_log_from_JobSummary(
                    LocalJobSummaryLogs, None, None, ignoreStartEndTime=ignoreStartEndTime
                )

        if (failed_tests == [] and curr_component in component_for_complete_log_collection
                and ignoreStartEndTime != True):
            logger.info("Gathering application log for complete test run")
            start_time, end_time = cls.find_start_end_time_for_complete_test_run(config, SECTION)
            if not start_time or not end_time:
                logger.info("start_time or end_time not found from output.log")
            else:
                if curr_component == cls.FALCON_STR:
                    cls.gather_application_log_from_JobSummary(
                        LocalJobSummaryLogs, start_time, end_time, True, ignoreStartEndTime=ignoreStartEndTime
                    )
                else:
                    cls.gather_application_log_from_JobSummary(
                        LocalJobSummaryLogs, start_time, end_time, ignoreStartEndTime=ignoreStartEndTime
                    )

    @classmethod
    def gather_application_log(cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False):
        '''
        Gather app logs to destination host with rsync.
        '''
        try:
            if cleanupDirFirst:
                LogUtilMachine.rm(destUser, destHost, destPath, isdir=True, passwd=passwd)
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath=os.path.join(cls.LOCAL_TMP_APP_STORAGE, '*'),
                destPath=destPath,
                flag="-rhp --chmod=u=rwx,g=rwx,o=rwx",
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gather_application_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
        return None

    @classmethod
    def gather_llap_application_log(cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False):
        '''
        Gather app logs to destination host with rsync.
        '''
        try:
            if cleanupDirFirst:
                LogUtilMachine.rm(destUser, destHost, destPath, isdir=True, passwd=passwd)
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath=os.path.join(cls.LOCAL_TMP_LLAP_APP_STORAGE, '*'),
                destPath=destPath,
                flag="-rhp --chmod=u=rwx,g=rwx,o=rwx",
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gather_application_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
        return None

    @classmethod
    def gather_service_log(  # pylint: disable=unused-argument
            cls,
            destHost,
            destUser,
            destPath,
            passwd,
            cleanupDirFirst=False,
            logoutput=False,
            component_list=None,
            numofthreads=NUM_OF_THREADS
    ):
        '''
        Gather service logs to destination host with rsync.
        Returns None.
        destHost is a String for log server host.
        destUser is a String for user at log server host.
        destPath is a String for destination path.
        Do ssh to each remote host and call rsync straight to rsync server
        destPath must exist beforehand (for now).
        '''
        try:
            if cleanupDirFirst:
                LogUtilMachine.rm(destUser, destHost, destPath, isdir=True, passwd=passwd)
            # Can't create automatically with makedirs because of cross-platform.
            # Machine.makedirs(destUser, destHost, destPath, passwd)
            if not component_list:
                servicelog_component_list = cls.SERVICE_LOG_MAP.keys()
            else:
                servicelog_component_list = component_list.split(",")
            component_threads = []
            for component in servicelog_component_list:
                logger.info("***** Start log collection for component = %s ******", component)
                comp_thread = Thread(
                    target=cls.gather_log_for_component,
                    args=(component, destHost, destUser, destPath, passwd, logoutput)
                )
                component_threads.append(comp_thread)
                comp_thread.start()
            logger.info("******** waiting for component =  %s to finish *********", servicelog_component_list)
            for thread in component_threads:
                thread.join()
            logger.info("******* Log collection finished for component = %s ********", servicelog_component_list)

        except Exception as e:
            logger.info("Exception occurs at gather_service_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
            logger.info("continue")
        return None

    @classmethod
    def gather_log_for_component(cls, component, destHost, destUser, destPath, passwd, logoutput=True):
        try:
            threads = []
            for service in cls.SERVICE_LOG_MAP[component]:
                logger.info("***** Start log collection for service = %s ******", service)
                thread = Thread(
                    target=cls.gather_log_from_service,
                    args=(component, service, destHost, destUser, destPath, passwd, logoutput)
                )
                threads.append(thread)
                thread.start()
            logger.info("******** waiting for service = %s to finish  *********", (cls.SERVICE_LOG_MAP[component]))
            for thread in threads:
                thread.join()
            logger.info("******* log collection finished for service = %s ********", (cls.SERVICE_LOG_MAP[component]))
        except Exception as e:
            logger.info("Exception occurs for component=%s %s", component, e)
            tb = traceback.format_exc()
            logger.info(tb)
            logger.info("continue")

    @classmethod
    def gather_log_from_service(cls, component, service, destHost, destUser, destPath, passwd, logoutput=True):
        hosts = cls.SERVICE_HOSTS_MAP[component][service]
        logger.info("****** Hosts for service %s = %s ********", service, hosts)
        if hosts is None:
            return
        if isinstance(hosts, (str, unicode)):
            hosts = [hosts]
        hosts = list(set(hosts))

        while hosts:
            # process each host only once per each service
            host = hosts.pop()

            # change the hostname FQDN to IP-address
            if Machine.isLinux():
                host = util.getIpAddress(host)
            else:
                # Windows
                if util.isIP(host):
                    host = util.getShortHostnameFromIP(host)
            if host:
                logDir = cls.SERVICE_LOG_MAP[component][service]
                logger.info("(host,logDir) = (%s,%s)", host, logDir)
                # JHS in Windows can be None
                if logDir is not None:
                    srcPath = os.path.join(logDir, "*")

                    # hardcode argue-usersync path. Importing Xa class can cause an issue in future.
                    if component == cls.RANGER_STR and logDir == "/var/log/ranger/usersync":
                        # workaround for QE-2486. Must grant read permission to hrt_qa.
                        # Log collection is changing permission of Ranger logs at end of test.
                        Machine.chmod(
                            perm="777",
                            filepath=logDir,
                            recursive=True,
                            user=Machine.getAdminUser(),
                            host=host,
                            passwd=Machine.getAdminPasswd(),
                            logoutput=True
                        )
                    tmpDestPath1 = destPath + "/" + component
                    tmpDestPath2 = destPath + "/" + component + "/" + host
                    LogUtilMachine.makedirs(destUser, destHost, tmpDestPath1, passwd)
                    LogUtilMachine.makedirs(destUser, destHost, tmpDestPath2, passwd)
                    logger.info("Rsync-ing to remote dir %s", tmpDestPath2)
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=host,
                        passwd=None,
                        srcPath=srcPath,
                        destPath=tmpDestPath2,
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput
                    )

    @classmethod
    def get_kdc_log_files(cls, kdcfile):
        '''
        Function to find kdc log files
        parameter: krb5.conf file
        returns [krb5libs.log , krb5kdc.log, kadmind.log]
        '''
        f = open(kdcfile)
        text = f.read()
        p_default = "default = (.*)"
        p_kdc = "kdc = (.*)"
        p_adminserver = "admin_server = (.*)"
        m_default = re.search(p_default, text)
        m_kdc = re.search(p_kdc, text)
        m_adminserver = re.search(p_adminserver, text)
        f.close()
        return [
            m_default.group(1).replace("FILE:", "").strip(),
            m_kdc.group(1).replace("FILE:", "").strip(),
            m_adminserver.group(1).replace("FILE:", "").strip()
        ]

    @classmethod
    def get_kdc_hosts(cls, kdcfile):
        '''
        Function to find KDC hosts
        parameter: krb5.conf file
        returns [kdc_server host, kdc_adminserver host]
        '''
        kdc_log_host = []
        f = open(kdcfile)
        text = f.read()
        p_kdc = r"^[\s]*kdc[\s]*=[\s]*(.*)"
        p_adminserver = r"^[\s]*admin_server[\s]*=[\s]*(.*)"
        m_kdc = re.findall(p_kdc, text, re.MULTILINE)
        for kdc_host in m_kdc:
            if kdc_host.find("ad-nano") >= 0 or kdc_host.find("ad-ec2") >= 0:
                logger.info("hosts running AD.Do not need to collect kdc logs")
            else:
                kdc_log_host.append(kdc_host)
        m_adminserver = re.findall(p_adminserver, text, re.MULTILINE)
        for kdc_adminserver in m_adminserver:
            if kdc_adminserver.find("ad-nano") >= 0 or kdc_adminserver.find("ad-ec2") >= 0:
                logger.info("hosts running AD.Do not need to collect kdc logs")
            else:
                kdc_log_host.append(kdc_adminserver)
        f.close()

        for kdc_host in kdc_log_host:
            if "FILE:" in kdc_host or ".log" in kdc_host:
                kdc_log_host.remove(kdc_host)

        kdc_hosts = []
        for kdc_host in kdc_log_host:
            kdc_hosts.append(kdc_host.split(":")[0])
        return list(set(kdc_hosts))

    @classmethod
    def gather_KDC_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather KDC logs to destination host with rsync.
        returns None
        '''
        try:
            logger.info("Gathering KDC Logs")
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            _krb_conf_file = "/etc/krb5.conf"
            kdc_log_files = cls.get_kdc_log_files(_krb_conf_file)
            logger.info(kdc_log_files)
            kdc_hosts = cls.get_kdc_hosts(_krb_conf_file)
            logger.info(kdc_hosts)
            for host in kdc_hosts:
                host_dir = os.path.join(destPath, host)
                cls.createDirInternal(destHost, destUser, host_dir, passwd, logoutput=logoutput)
                tmp_host_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), host)
                Machine.makedirs(None, None, tmp_host_dir, None)
                for file_ in kdc_log_files:
                    if Machine.pathExists(Machine.getAdminUser(), host, file_, Machine.getAdminPasswd()):
                        Machine.copyToLocal(
                            Machine.getAdminUser(), host, file_, os.path.join(tmp_host_dir,
                                                                              file_.split("/")[-1]),
                            Machine.getAdminPasswd()
                        )
                    else:
                        logger.info("%s missing on %s", file_, host)
                RSync.rsyncToRemoteHost(
                    user=None,
                    host=None,
                    passwd=None,
                    srcPath=os.path.join(tmp_host_dir, '*'),
                    destPath=host_dir,
                    destHost=destHost,
                    destUser=destUser,
                    logoutput=logoutput
                )
        except Exception as e:
            logger.info("Exception occurs at gather_KDC_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_gsInstaller_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather gsInstaller logs to destination host with rsync.
        returns None
        '''
        try:
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath="/tmp/gsinstaller-*",
                destPath=destPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath="/tmp/*-setup.log",
                destPath=destPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gather_gsInstaller_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def collect_jenkins_log_locally(cls):
        '''
        Collects jenkins job log and save it in local folder
        '''
        logger.info("Collecting jenkins log to local folder")
        jobs = []
        if Machine.isLinux():
            jobs = ['Run-HDP-Tests', 'Run-Ambari-Setup']
        tmp_dir = Machine.getTempDir()
        build_urls = os.path.join(tmp_dir, 'hwqe-build-urls.log')
        if not os.path.exists(build_urls):
            logger.info("%s not found")
            # QE-4756 windows does not have hwqe-build-urls.log yet
            return
        jenkins_local_dir = os.path.join(tmp_dir, 'jenkins_logs')
        logger.info("The jenkins_local_dir is %s", jenkins_local_dir)
        if not Machine.pathExists(None, None, jenkins_local_dir, None):
            logger.info("Creating %s", jenkins_local_dir)
            Machine.makedirs(Machine.getAdminUser(), None, jenkins_local_dir, Machine.getAdminPasswd())
            Machine.chmod(
                "777", jenkins_local_dir, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )
        for j in jobs:
            try:
                build_urls_file = open(build_urls, 'r')
                # lines in reversed order
                lines = build_urls_file.readlines()[::-1]
                for l in lines:
                    if j in l:
                        # strip newline character
                        l = l.strip()
                        console_file = os.path.join(jenkins_local_dir, j + '-jenkins-console.log')
                        console_url = l + 'consoleText'
                        logger.info("The console_url is %s", console_url)
                        try:
                            util.getURLContents(console_url, outfile=console_file)
                        except Exception:
                            logger.info("Exception occurs at downloading %s from %s", console_file, console_url)
                        break
            except Exception:
                logger.info("Exception occurs at collecting %s", console_file)
            finally:
                build_urls_file.close()

    @classmethod
    def gather_jenkins_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather jenkins logs to destination host with rsync.
        returns None
        '''
        cls.collect_jenkins_log_locally()
        tmp_dir = Machine.getTempDir()
        jenkins_local_dir = os.path.join(tmp_dir, 'jenkins_logs')
        if not Machine.pathExists(None, None, jenkins_local_dir, None):
            logger.info("%s not found", jenkins_local_dir)
            return
        srcPath = os.path.join(jenkins_local_dir, '*')
        try:
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath=srcPath,
                destPath=destPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gather_jenkins_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_artifacts_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False, srcPath=None
    ):
        '''
        Gather artifacts to destination host with rsync.
        returns None
        '''
        try:
            if srcPath is None:
                srcPath = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "*")
            logger.info("Gathering artifacts logs from: %s to %s", srcPath, destPath)
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath=srcPath,
                destPath=destPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput,
                flag="-rhp --chmod=u=rwx,g=rwx,o=r"
            )
        except Exception as e:
            logger.info("Exception occurs at gather_artifacts_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_deployng_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False, srcPath=None
    ):
        '''
        Gather artifacts to destination host with rsync.
        returns None
        '''
        try:
            src_paths = ["/installer/logs/*"]
            for path in src_paths:
                folder_path = srcPath + path
                logger.info("Gathering artifacts logs from: %s to %s", folder_path, destPath)
                RSync.rsyncToRemoteHost(
                    user=None,
                    host=None,
                    passwd=None,
                    srcPath=folder_path,
                    destPath=destPath,
                    destHost=destHost,
                    destUser=destUser,
                    logoutput=logoutput,
                    flag="-rhp --chmod=u=rwx,g=rwx,o=r"
                )
        except Exception as e:
            logger.info("Exception occurs at gather_artifacts_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_varlog_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather /var/log/* to destination host with rsync.
        returns None
        '''
        try:
            # this works only in Linux for now.
            srcPaths = [
                os.path.join("/", "var", "log", "dmesg*"),
                os.path.join("/", "var", "log", "kern.log"),
                os.path.join("/", "var", "log", "syslog*"),
                os.path.join("/", "var", "log", "messages*")
            ]
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            for host in ComponentLogUtil.getAllNodes(logoutput):
                destPath2 = destPath + "/" + host
                cls.createDirInternal(destHost, destUser, destPath2, passwd, logoutput=logoutput)
                for srcPath in srcPaths:
                    # we are changing permission of /var/log/* at end of tests.
                    Machine.chmod(
                        perm="777",
                        filepath=srcPath,
                        recursive=True,
                        user=Machine.getAdminUser(),
                        host=host,
                        passwd=Machine.getAdminPasswd(),
                        logoutput=True
                    )
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=host,
                        passwd=None,
                        srcPath=srcPath,
                        destPath=destPath2,
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput,
                        flag="-rhp --chmod=u=rwx,g=rwx,o=r"
                    )
        except Exception as e:
            logger.info("Exception occurs at gather_varlog_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
            logger.info("continue")

    @classmethod
    def gather_grpc_logs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        try:
            # this works only in Linux now
            srcPaths = ["/grid/0/hwqe-grpc-*/logs", "/grid/0/hwqe-grpc-*/conf"]
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            for host in ComponentLogUtil.getAllNodes(logoutput):
                destPath2 = destPath + "/" + host + "/hwqe-grpc"
                cls.createDirInternal(destHost, destUser, destPath2, passwd, logoutput=logoutput)
                for srcPath in srcPaths:
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=host,
                        passwd=None,
                        srcPath=srcPath,
                        destPath=destPath2,
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput,
                        flag="-rhp --chmod=u=rwx,g=rwx,o=r"
                    )
        except Exception as e:
            logger.info("Exception occurs at gather_grpc_logs. %s", e)
            logger.info(traceback.format_exc())

    @classmethod
    def gather_hiveClient_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather hiveClient logs to destination host with rsync.
        returns None
        '''
        try:
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            innerDestPath = destPath + "/hiveClient"
            cls.createDirInternal(destHost, destUser, innerDestPath, passwd, logoutput=logoutput)
            # the method works with Linux only. It is API caller responsibility to meet that condition.
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath="/tmp/hrt_qa/*",
                destPath=innerDestPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath="/tmp/hive/*",
                destPath=innerDestPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gatherhiveClient_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_OozieJobs_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, status, cleanupDirFirst=False, logoutput=False
    ):
        '''
            Gather failed Oozie jobs logs to destination host.
            returns None
        '''
        try:
            from beaver.component.oozie import Oozie
            failedJobs = Oozie.getOozieJobList(status)
            if failedJobs:
                # We create the remote directory just in case
                cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
                for job in failedJobs:
                    _stdout, report = Oozie.getOozieJobLog(job)
                    logFile = open(os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, job + '.log'), 'w+')
                    logFile.write(report)
                    logFile.close()
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=None,
                        passwd=None,
                        srcPath=os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, job + '.log'),
                        destPath=destPath,
                        flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput
                    )
        except Exception:
            logger.info("Get logs for %s oozie jobs is broken", str(status).lower())

    @classmethod
    def gather_config(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False, component_list=None
    ):
        '''
        Gather config to destination host with rsync.
        For ranger/argus/XaSecure config config path is /etc/ranger instead of /etc/ranger/conf
                as it contains multiple dirs correspondig to
        different ranger componets
        returns None
        '''
        try:
            # linux only at this moment
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            # Component configuration location map is defined with
            #       <component_name> :[linux_conf_location, Windows_conf_location]

            # The below config get has dependency of hdp installation
            SERVICE_CONF_MAP = collections.defaultdict(dict)
            SERVICE_CONF_MAP["kafka"] = ["/etc/kafka/conf", "/etc/kafka/conf"]
            SERVICE_CONF_MAP["phoenix"] = ["/etc/phoenix/conf", "/etc/phoenix/conf"]
            SERVICE_CONF_MAP["mahout"] = ["/etc/mahout/conf", "/etc/mahout/conf"]
            SERVICE_CONF_MAP["accumulo"] = ["/etc/accumulo/conf", "/etc/accumulo/conf"]
            SERVICE_CONF_MAP["ranger"] = ["/etc/ranger", "/etc/ranger"]
            SERVICE_CONF_MAP["Phoenix-QueryServer"] = ["/etc/phoenix/conf", "/etc/phoenix/conf"]
            SERVICE_CONF_MAP["atlas"] = ["/etc/atlas/conf", "/etc/atlas/conf"]
            SERVICE_CONF_MAP["phoenix-qs-concurr"] = ["/etc/phoenix/conf", "/etc/phoenix/conf"]
            SERVICE_CONF_MAP["hue"] = ["/etc/hue/conf", "/etc/hue/conf"]
            SERVICE_CONF_MAP["nifi"] = ["/etc/nifi/conf", "/etc/nifi/conf"]
            SERVICE_CONF_MAP["streamline"] = ["/etc/streamline/conf", "/etc/streamline/conf"]
            SERVICE_CONF_MAP["smm"] = ["/etc/streams-messaging-manager/conf", "/etc/streams-messaging-manager/conf"]

            try:
                SERVICE_CONF_MAP["hadoop"] = ["/etc/hadoop/conf", Config.get("hadoop", "HADOOP_CONF")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["storm"] = ["/etc/storm/conf", Config.get("storm", "STORM_CONF")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["tez"] = ["/etc/tez/conf", Config.get("tez", "TEZ_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["slider"] = [
                    "/usr/hdp/current/slider-client/conf",
                    Config.get("slider", "SLIDER_CONF_DIR")
                ]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["oozie"] = ["/etc/oozie/conf", Config.get("oozie", "OOZIE_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["hbase"] = ["/etc/hbase/conf", Config.get("hbase", "HBASE_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["zookeeper"] = ["/etc/zookeeper/conf", Config.get("zookeeper", "ZOOKEEPER_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["hive"] = ["/etc/hive/conf", Config.get("hive", "HIVE_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["hive-hcatalog"] = [
                    "/etc/hive-hcatalog/conf", Config.get("hcatalog", "HCATALOG_CONF")
                ]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["hive-webhcat"] = ["/etc/hive-webhcat/conf", Config.get("hcatalog", "HCATALOG_CONF")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["pig"] = ["/etc/pig/conf", Config.get("pig", "PIG_CONF_DIR")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["konx"] = ["/etc/knox/conf", Config.get("knox", "KNOX_CONF")]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["spark"] = ["/etc/spark/conf", Config.get('spark', 'SPARK_CONF')]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["zeppelin"] = ["/etc/zeppelin/conf", Config.get('zeppelin', 'ZEPPELIN_CONF')]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["livy"] = ["/etc/livy/conf", Config.get('livy', 'LIVY_CONF')]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["livy2"] = ["/etc/livy2/conf", Config.get('livy', 'LIVY2_CONF')]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)
            try:
                SERVICE_CONF_MAP["druid"] = ["/etc/druid/conf", Config.get('druid', 'DRUID_CONF_DIR')]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)

            try:
                from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
                SERVICE_CONF_MAP["falcon"] = ["/etc/falcon/conf", Falcon.getConfDir()]
            except Exception as e:
                logger.info("Exception occured during falcon conf collection : %s", e)
            try:
                SERVICE_CONF_MAP["bigsql"] = [
                    "/usr/ibmpacks/current/bigsql/bigsql/conf",
                    Config.get('bigsql', 'BIGSQL_CONF_DIR')
                ]
            except Exception as e:
                logger.info("Exception occured during Config.get : %s", e)

            if not component_list:
                configComponents = SERVICE_CONF_MAP.keys()
            else:
                configComponents = component_list.split(",")

            configPaths = []

            for key in configComponents:
                if Machine.isLinux():
                    path = SERVICE_CONF_MAP[key][0]
                else:
                    path = SERVICE_CONF_MAP[key][1]
                configPaths.append(path)

            for i, _configComponent in enumerate(configComponents):
                logger.info("config log for %s", _configComponent)
                config_path = configPaths[i]
                if _configComponent == "ranger":
                    config_path = cls.collect_ranger_config_locally(config_path)
                    if config_path is None:
                        continue
                if _configComponent == "zeppelin":
                    config_path = cls.collect_zeppelin_config_locally(config_path)
                    if config_path is None:
                        continue
                if _configComponent == "livy":
                    config_path = cls.collect_livy_config_locally(config_path)
                    if config_path is None:
                        continue
                if _configComponent == "livy2":
                    config_path = cls.collect_livy_config_locally(config_path, forceVersion="2")
                    if config_path is None:
                        continue
                if _configComponent == "streamline":
                    config_path = cls.collect_streamline_config_locally(config_path)
                    if config_path is None:
                        continue
                if _configComponent == "smm":
                    config_path = cls.collect_SMM_config_locally(config_path)
                    if config_path is None:
                        continue
                if _configComponent == "hive-webhcat":
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=util.getIpAddress(Config.get('templeton', 'TEMPLETON_HOST', 'localhost')),
                        passwd=None,
                        srcPath="%s%s*" % (config_path, os.path.sep),
                        destPath="%s/%s" % (destPath, _configComponent),
                        flag="-rhpL --chmod=u=rwx,g=rwx,o=r",
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput
                    )
                else:
                    RSync.rsyncToRemoteHost(
                        user=None,
                        host=None,
                        passwd=None,
                        srcPath="%s%s*" % (config_path, os.path.sep),
                        destPath="%s/%s" % (destPath, _configComponent),
                        flag="-rhpL --chmod=u=rwx,g=rwx,o=r",
                        destHost=destHost,
                        destUser=destUser,
                        logoutput=logoutput
                    )
                if _configComponent == "ranger" and config_path is not None:
                    Machine.rm(None, None, config_path, isdir=True)
        except Exception as e:
            logger.info("Exception occurs at gather_config. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def collect_ranger_config_locally(cls, ranger_config_path):
        '''
        Collects Ranger/Argus/XaSecure configs if installed from getPolicyAdminHost or XaSecure/ranger to
                local machune under articfacts dir
        So that they rsynced to log server
        :param ranger_config_path: Ranger config path
        :return: None if ranger/Xasecure not installed or copying failed. Other local path where
                ranger/XaSecure/Argus configs are copied.
        '''
        try:
            from beaver.component.xa import Xa
            if not Xa.isArgusInstalled():
                return None
            hadoopqa_user = Config.get('hadoop', 'HADOOPQA_USER', 'hrt_qa')
            artifacts_dir = Config.getEnv("ARTIFACTS_DIR")
            local_dir = os.path.join(
                artifacts_dir,
                'ranger_config',
            )
            Machine.makedirs(None, None, local_dir)
            Machine.copyToLocal(
                Machine.getAdminUser(), Xa.getPolicyAdminHost(), "%s%s*" % (ranger_config_path, os.path.sep), local_dir
            )
            cmd = "chown -R %s:%s %s" % (hadoopqa_user, hadoopqa_user, local_dir)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
            return os.path.join(local_dir)
        except Exception:
            logger.info("Exception occured whie gather ranger/argus logs")
            logger.info(traceback.format_exc())
        return None

    @classmethod
    def collect_zeppelin_config_locally(cls, zep_config_path):
        try:
            from beaver.component.zeppelin import ZeppelinServer
            hadoopqa_user = Config.get('hadoop', 'HADOOPQA_USER', 'hrt_qa')
            artifacts_dir = Config.getEnv("ARTIFACTS_DIR")
            local_dir = os.path.join(
                artifacts_dir,
                'zeppelin_config',
            )
            Machine.makedirs(None, None, local_dir)
            Machine.copyToLocal(
                Machine.getAdminUser(), ZeppelinServer.get_ip_address(), "%s%s*" % (zep_config_path, os.path.sep),
                local_dir
            )
            cmd = "chown -R %s:%s %s" % (hadoopqa_user, hadoopqa_user, local_dir)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
            return os.path.join(local_dir)
        except Exception:
            logger.info("Exception occured whie gather Zeppelin configs")
            logger.info(traceback.format_exc())
        return None

    @classmethod
    def collect_livy_config_locally(cls, livy_config_path, forceVersion=None):
        try:
            from beaver.component.livy import Livy
            hadoopqa_user = Config.get('hadoop', 'HADOOPQA_USER', 'hrt_qa')
            artifacts_dir = Config.getEnv("ARTIFACTS_DIR")
            local_dir = os.path.join(
                artifacts_dir,
                'livy_config',
            )
            Machine.makedirs(None, None, local_dir)
            Machine.copyToLocal(
                Machine.getAdminUser(),
                Livy.getLivyServerIpAddress(forceVersion=forceVersion),
                "%s%s*" % (livy_config_path, os.path.sep),
                local_dir
            )
            cmd = "chown -R %s:%s %s" % (hadoopqa_user, hadoopqa_user, local_dir)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
            return os.path.join(local_dir)
        except Exception:
            logger.info("Exception occured whie gather livy configs")
            logger.info(traceback.format_exc())
        return None

    @classmethod
    def collect_streamline_config_locally(cls, config_path, forceVersion=None):  # pylint: disable=unused-argument
        try:
            from beaver.component.streamline import Streamline  # pylint: disable=redefined-outer-name
            qa_user = Config.get('hadoop', 'HADOOPQA_USER', 'hrt_qa')
            artifacts_dir = Config.getEnv("ARTIFACTS_DIR")
            local_dir = os.path.join(
                artifacts_dir,
                'streamline_config',
            )
            Machine.makedirs(None, None, local_dir)
            Machine.copyToLocal(
                Machine.getAdminUser(),
                Streamline.get_hosts()[0], os.path.join(config_path, "*"), local_dir
            )
            cmd = "chown -R %s:%s %s" % (qa_user, qa_user, local_dir)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
            return os.path.join(local_dir)
        except Exception:
            logger.info("Exception occurred while gathering streamline configs")
            logger.info(traceback.format_exc())
        return None

    @classmethod
    def collect_SMM_config_locally(cls, config_path, forceVersion=None):  # pylint: disable=unused-argument
        try:
            from beaver.component.smm import SMM  # pylint: disable=redefined-outer-name
            qa_user = Config.get('hadoop', 'HADOOPQA_USER', 'hrt_qa')
            artifacts_dir = Config.getEnv("ARTIFACTS_DIR")
            local_dir = os.path.join(
                artifacts_dir,
                'smm_config',
            )
            Machine.makedirs(None, None, local_dir)
            Machine.copyToLocal(
                Machine.getAdminUser(),
                SMM.get_hosts()[0], os.path.join(config_path, "*"), local_dir
            )
            cmd = "chown -R %s:%s %s" % (qa_user, qa_user, local_dir)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
            return os.path.join(local_dir)
        except Exception:
            logger.info("Exception occurred while gathering SMM configs")
            logger.info(traceback.format_exc())
        return None

    @classmethod
    def gather_multicluster_logs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather multicluster logs to destination host with rsync.
        returns None
        '''
        try:
            # If any component class cannot import, let this throw an exception and catch.
            # In case of falcon which asks for this feature, there should be no exception.
            # TODO: Check the code below, does not match with the comment above
            try:
                from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
                # from beaver.component.hive import Hive
                # from beaver.component.oozie import Oozie
            except Exception:
                pass
            else:
                try:
                    cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
                    multiclusterComponents = ["hdfs", "hive", "knox", "mapreduce", "oozie", "webhcat", "yarn"]
                    multiclusterPaths = [
                        ComponentLogUtil.HDFS_getNNLogDir(),
                        ComponentLogUtil.Hive_getHiveLogDir(),
                        ComponentLogUtil.Knox_getLogDir(),
                        ComponentLogUtil.MAPRED_getHistoryServerLogDir(),
                        ComponentLogUtil.Oozie_getOozieLogDir(),
                        ComponentLogUtil.Hcatalog_getTempletonLogDir(),
                        ComponentLogUtil.MAPRED_getJTLogDir()
                    ]
                    LogUtilMachine.makedirs(destUser, destHost, destPath, passwd)
                    for i, _multiclusterComponent in enumerate(multiclusterComponents):
                        try:
                            logger.info("multicluster log for %s", _multiclusterComponent)
                            component = _multiclusterComponent
                            tmpDestPath1 = destPath + "/" + component
                            LogUtilMachine.makedirs(destUser, destHost, tmpDestPath1, passwd)
                            for node in Falcon.get_all_nodes():
                                tmpDestPath = destPath + "/" + component + "/" + node
                                LogUtilMachine.makedirs(destUser, destHost, tmpDestPath, passwd)
                                RSync.rsyncToRemoteHost(
                                    user=None,
                                    host=node,
                                    passwd=None,
                                    srcPath=os.path.join(multiclusterPaths[i], "*"),
                                    destPath=tmpDestPath,
                                    destHost=destHost,
                                    destUser=destUser,
                                    logoutput=logoutput
                                )
                        except Exception as e:
                            logger.info("Exception occurs at gather_multicluster_log. %s", e)
                            tb = traceback.format_exc()
                            logger.info(tb)
                except Exception as e:
                    logger.info("Exception occurs at gather_multicluster_log. %s", e)
                    tb = traceback.format_exc()
                    logger.info(tb)
        except Exception as e:
            logger.info("Exception occurs at gather_multicluster_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def collectAmbariAgentsLogs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Collects ambari agent logs from all hosts in the cluster
        '''
        try:
            logger.info("Gathering Ambari Agent logs from all hosts")
            Ambari_agent_dir = destPath + "/ambari-agent"
            cls.createDirInternal(destHost, destUser, Ambari_agent_dir, passwd, logoutput=logoutput)
            _all_nodes = cls.get_all_nodes()
            logger.info("Collecting ambari-agent logs from %s", str(_all_nodes))
            for machine in _all_nodes:
                machine = util.getIpAddress(machine)
                logger.info("Collecting ambari-agent logs from %s", machine)
                try:
                    dest_machine = Ambari_agent_dir + "/" + machine
                    cls.createDirInternal(destHost, destUser, dest_machine, passwd, logoutput=logoutput)
                    log_location = ["/var/log/ambari-agent/*", "/var/lib/ambari-agent/data"]
                    for logdir in log_location:
                        Machine.chmod(
                            "777", logdir, True, Machine.getAdminUser(), machine, Machine.getAdminPasswd(), True
                        )
                        RSync.rsyncToRemoteHost(
                            user=None,
                            host=machine,
                            passwd=None,
                            srcPath=logdir,
                            destPath=dest_machine,
                            flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                            destHost=destHost,
                            destUser=destUser,
                            logoutput=logoutput
                        )
                except Exception as e:
                    logger.info("Exception when try to collect agents logs for host %s, exc: %s", machine, e)
                    logger.info(traceback.format_exc())
        except Exception as e:
            logger.info("Exception occurs at collecAmbariAgentsLogs: %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def collectAmbariServerLogs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Collects ambari server related logs
        [ambari-server.log, ambari-server.out, bootstrap]
        returns None
        '''
        try:
            logger.info("Gathering Ambari Server logs")
            Ambari_server_dir = destPath + "/ambari-server"
            cls.createDirInternal(destHost, destUser, Ambari_server_dir, passwd, logoutput=logoutput)
            log_location = ["/var/log/ambari-server/*", "/var/run/ambari-server/bootstrap"]
            for logdir in log_location:
                RSync.rsyncToRemoteHost(
                    user=None,
                    host=None,
                    passwd=passwd,
                    srcPath=logdir,
                    destPath=Ambari_server_dir,
                    flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                    destHost=destHost,
                    destUser=destUser,
                    logoutput=logoutput
                )
        except Exception as e:
            logger.info("Exception occurs at collectAmbariServerLogs. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def collectAmbariBlueprintLogs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Collects ambari blueprint related logs
        [blueprint-cluster.json and blueprint-def.json]
        returns None
        '''
        try:
            logger.info("Gathering Ambari blueprint logs")
            Ambari_blueprint_dir = destPath + "/ambari-blueprint"
            cls.createDirInternal(destHost, destUser, Ambari_blueprint_dir, passwd, logoutput=logoutput)
            tmp_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "ambari-blueprint")
            Machine.makedirs(None, None, tmp_dir, passwd=None)
            log_location = ["/root/blueprint-cluster.json", "/root/blueprint-def.json"]
            for logdir in log_location:
                Machine.copyToLocal(Machine.getAdminUser(), None, logdir, tmp_dir, passwd=Machine.getAdminPasswd())
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=passwd,
                srcPath=os.path.join(tmp_dir, "*"),
                destPath=Ambari_blueprint_dir,
                flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at collectAmbariBlueprintLogs. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_Ambari_logs(cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False):
        '''
        Gather Ambari related logs such as ambari-agent/ambari-server/blueprint
        It follows below hierarchy structure.
        +service-logs
           ++Ambari-logs
              +++blueprint-logs
                 ++++blueprint-cluster.json
                 ++++blueprint-def.json
              +++ambari-server
                 ++++ambari-server.log
                 ++++ambari-server.out
                 ++++bootstrap
                    +++++Files/Dirs
              +++ambari-agent
                 ++++ambari-agent.log
                 ++++ambari-agent.out
                 ++++data
                    +++++Files/Dirs
        returns None
        '''
        try:
            logger.info("Gathering Ambari related logs ")
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            # collect Ambari agents logs
            cls.collectAmbariAgentsLogs(destHost, destUser, destPath, passwd, cleanupDirFirst, logoutput)
            # collect Ambari server logs
            cls.collectAmbariServerLogs(destHost, destUser, destPath, passwd, cleanupDirFirst, logoutput)
            # collect ambari blueprint logs
            cls.collectAmbariBlueprintLogs(destHost, destUser, destPath, passwd, cleanupDirFirst, logoutput)
        except Exception as e:
            logger.info("Exception occurs at gather_Ambari_logs. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_machine_conf(cls, dest_host, dest_user, dest_path, passwd, cleanup_dest_first=False, logoutput=False):
        """
        Gathers the machine configurations from each host and deposits them in the qelogserver.
            As of now it collects /etc/passwd, /etc/group
        :param dest_host: qelog host
        :param dest_user: user in qelog host
        :param dest_path: location under which the files are stored. It is now machineConf/<hostname>/{group,passwd}
        :param passwd: user password for qelog host
        :param cleanup_dest_first: cleans up the machineConf directory for each run
        :param logoutput: logs all the command output
        :return: None
        """
        try:
            if cleanup_dest_first:
                LogUtilMachine.rm(dest_user, dest_host, dest_user, isdir=True, passwd=passwd)

            _all_nodes = cls.get_all_nodes()
            logger.info("Collecting machine conf from %s", str(_all_nodes))
            for machine in _all_nodes:
                machine = util.getIpAddress(machine)
                logger.info("Collecting machine conf from %s", machine)
                try:
                    tmp_dest_path = dest_path + "/" + machine
                    LogUtilMachine.makedirs(dest_user, dest_host, dest_path, passwd)
                    LogUtilMachine.makedirs(dest_user, dest_host, tmp_dest_path, passwd)
                    for item in cls.MACHINE_CONFS_TO_COLLECT:
                        logger.info("Rsync-ing %s to qelog host.", item)
                        RSync.rsyncToRemoteHost(
                            user=None,
                            host=machine,
                            passwd=None,
                            srcPath=item,
                            destPath=tmp_dest_path,
                            destHost=dest_host,
                            destUser=dest_user,
                            logoutput=logoutput
                        )
                except Exception as e:
                    logger.info("Exception for host %s %s", machine, e)
                    logger.info(traceback.format_exc())
        except Exception as e:
            logger.info("Exception while gathering confs. %s", e)
            logger.info(traceback.format_exc())
        #finally:
        return None

    @classmethod
    def get_all_nodes(cls):
        if not Machine.isHumboldt():
            return ComponentLogUtil.getAllNodes()
        else:
            return util.getAllNodes()

    @classmethod
    def gather_log_collector_logs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        log_locations = ["/tmp/log-collector-output.json"]
        try:
            for logDir in log_locations:
                RSync.rsyncToRemoteHost(
                    user=None,
                    host=None,
                    passwd=passwd,
                    srcPath=logDir,
                    destPath=destPath,
                    flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                    destHost=destHost,
                    destUser=destUser,
                    logoutput=logoutput
                )
        except Exception as e:
            logger.info("Exception occurs at gather_misc_logs_from_gateway %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_misc_logs_from_gateway(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather miscellaneous logs from gateway
        returns None
        '''
        try:
            logger.info("Gathering miscellaneous logs from gateway")
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            log_locations = [
                "/tmp/hwqe-build-urls.log", "/tmp/pdsh_err_*.out", "/tmp/falcon-recipe/*", "/etc/krb5.conf",
                "/tmp/jenkins-build-params.properties"
            ]
            for logDir in log_locations:
                RSync.rsyncToRemoteHost(
                    user=None,
                    host=None,
                    passwd=passwd,
                    srcPath=logDir,
                    destPath=destPath,
                    flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                    destHost=destHost,
                    destUser=destUser,
                    logoutput=logoutput
                )
        except Exception as e:
            logger.info("Exception occurs at gather_misc_logs_from_gateway %s", e)
            tb = traceback.format_exc()
            logger.info(tb)

    @classmethod
    def gather_misc_test_logs(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather miscellaneous test logs
        returns None
        '''
        try:
            # Collect all the components misc test log paths (files/folders list)
            for component in SERVICE_MISC_TEST_LOG_MAP.iterkeys():
                # QE-4443: Add a try catch to the misc log path.
                try:
                    miscTestLogPaths = component.getMiscTestLogPaths(logoutput=True)
                    SERVICE_MISC_TEST_LOG_MAP[component] = miscTestLogPaths
                except Exception as e:
                    logger.info("Exception occurs at gather_misc_test_logs %s", e)
                    miscTestLogPaths = []
                    SERVICE_MISC_TEST_LOG_MAP[component] = miscTestLogPaths

            # Copy all the misc test log files/folders individually
            for component, miscTestLogPaths in SERVICE_MISC_TEST_LOG_MAP.iteritems():
                logger.info("Gathering miscellaneous test logs for component:%s", component.__name__)
                for miscTestLogPath in miscTestLogPaths:
                    if "*" in miscTestLogPath:
                        miscTestLogFolder = os.path.dirname(miscTestLogPath)
                        if Machine.pathExists(user=None, host=None, filepath=miscTestLogFolder, passwd=None):
                            logger.info(miscTestLogFolder)
                            RSync.rsyncToRemoteHost(
                                user=None,
                                host=None,
                                passwd=passwd,
                                srcPath=miscTestLogPath,
                                destPath=destPath,
                                flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                                destHost=destHost,
                                destUser=destUser,
                                logoutput=logoutput
                            )
                    else:
                        if Machine.pathExists(user=None, host=None, filepath=miscTestLogPath, passwd=None):
                            logger.info(miscTestLogPath)
                            RSync.rsyncToRemoteHost(
                                user=None,
                                host=None,
                                passwd=passwd,
                                srcPath=miscTestLogPath,
                                destPath=destPath,
                                flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                                destHost=destHost,
                                destUser=destUser,
                                logoutput=logoutput
                            )
        except Exception as e:
            logger.info("Exception occurs at gather_misc_test_logs %s", e)

        try:
            # Copy the other required files(console.log,) to test-logs/ folder
            consoleLogPath = os.path.join(Config.getEnv("WORKSPACE"), "console.log")
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=passwd,
                srcPath=consoleLogPath,
                destPath=destPath,
                flag="-rhp --chmod=u=rwx,g=rwx,o=r",
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput
            )
        except Exception as e:
            logger.info("Exception occurs at gather_misc_test_logs %s", e)
