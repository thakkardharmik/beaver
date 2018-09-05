#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os

from beaver import util
from beaver.component.ambari import Ambari
from beaver.component.hbase import HBase
from beaver.config import Config

logger = logging.getLogger(__name__)


class Metron(object):
    _host = None
    _es_host = None
    _storm_host = None
    _path = None
    _scheme = None
    _cluster_name = None
    _ambari_server_host = None
    _log_dir = None
    _is_secure = False
    _hbase_master = None
    _hbase_regionservers = None

    test_code_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'metron')
    regression_dir = os.path.join(test_code_dir, 'jumbo')
    test_properties_file = os.path.join(regression_dir, 'src', 'test', 'resources', 'metron-tests.properties')
    test_suite_xml = os.path.join(regression_dir, 'src', 'test', 'resources', 'Testsuite.xml')
    results_file = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'metron', 'metron-results.log')
    jaas_config_file = os.path.join(regression_dir, 'src', 'test', 'resources', 'client_jaas.conf')
    jaas_config_template = os.path.join(
        regression_dir, 'src', 'test', 'resources', 'templates', 'client_jaas_template.conf'
    )

    es_master = "ES_MASTER"
    storm_nimbus = "NIMBUS"
    metron_component = "METRON_PARSERS"
    kafka_broker = "KAFKA_BROKER"
    zeppelin_component = "ZEPPELIN_MASTER"
    namenode1_component = "NAMENODE"
    namenode2_component = "SECONDARY_NAMENODE"
    metron_config_type = "metron-env"
    metron_rest_config_type = "metron-rest-env"
    metron_config_tag = "version1"
    default_log_dir = "/var/log/metron"
    krb_config_file = "/etc/krb5.conf"

    def __init__(self):
        pass

    @classmethod
    def getMetronHost(cls):
        '''
        Return the host where metron is running
        :return: String
        '''
        cls._host = Ambari.getServiceHosts("METRON", cls.metron_component, cluster=cls.get_ambari_cluster_name())
        if not cls._host:
            logger.error('Metron instance is not installed on any of hosts in the cluster')
        return cls._host

    @classmethod
    def getESHost(cls):
        '''
        Return the host where Elasticsearch is running
        :return: String
        '''
        cls._es_host = Ambari.getServiceHosts("ELASTICSEARCH", cls.es_master, cluster=cls.get_ambari_cluster_name())
        if not cls._es_host:
            logger.error('Elasticsearch instance is not installed on any of hosts in the cluster')
        return cls._es_host

    @classmethod
    def getStormHost(cls):
        '''
        Return the host where Storm Nimbus service is running
        :return: String
        '''
        cls._storm_host = Ambari.getServiceHosts("STORM", cls.storm_nimbus, cluster=cls.get_ambari_cluster_name())
        if not cls._storm_host:
            logger.error('Storm Nimbus instance is not installed on any of hosts in the cluster')
        return cls._storm_host

    @classmethod
    def getZeppelinHost(cls):
        '''
        Return the host where Zeppelin is running
        :return: String
        '''
        cls._host = Ambari.getServiceHosts("ZEPPELIN", cls.zeppelin_component, cluster=cls.get_ambari_cluster_name())
        if not cls._host:
            logger.error('Zeppelin is not installed on any of hosts in the cluster')
        return cls._host

    @classmethod
    def getNameNode1Host(cls):
        '''
        Return the host where Primary Name Node is running
        :return: String
        '''
        cls._host = Ambari.getServiceHosts("HDFS", cls.namenode1_component, cluster=cls.get_ambari_cluster_name())
        if not cls._host:
            logger.error('Primary Namenode is not installed on the cluster')
        return cls._host

    @classmethod
    def getNameNode2Host(cls):
        '''
        Return the host where Secondary Name Node is running
        :return: String
        '''
        cls._host = Ambari.getServiceHosts("HDFS", cls.namenode2_component, cluster=cls.get_ambari_cluster_name())
        if not cls._host:
            logger.error('Secondary Namenode is not installed on the cluster')
        return cls._host

    @classmethod
    def get_ambari_cluster_name(cls):
        return Ambari.getClusterName()

    @classmethod
    def getMetronHome(cls):
        '''
        Return the path to Metron Home from Ambari config using REST API
        :return: String
        '''
        metron_home = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/configurations?type=%s&tag=%s" % (
            cls.get_ambari_cluster_name(), cls.metron_config_type, cls.metron_config_tag
        )
        retcode, retdata, _retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                metron_home = [hc['properties']['metron_home'] for hc in jsoncontent['items']]
            except Exception:
                logger.error('Metron Home variable is not set')
                metron_home = None
        return metron_home

    @classmethod
    def getVersion(cls):
        '''
        Extract the version number of Metron from the metron_home parameter
        :return: String
        '''
        metron_version = cls.getMetronHome()[0].split("/")[3]
        return metron_version

    @classmethod
    def checkKafkaStatusOnMetronHost(cls, metron_host):
        kafka_status = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/hosts/%s/host_components/%s" % (
            cls.get_ambari_cluster_name(), metron_host, cls.kafka_broker
        )
        retcode, retdata, _retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                kafka_status = jsoncontent['HostRoles']['state']
            except Exception:
                logger.error('Kafka broker is not running on the Metron host')
                kafka_status = None
        return kafka_status

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        '''
        Additional logs that we need captured for Metron system tests
        :return: list of string.
        '''
        miscTestLogPaths = []
        # get the testng logs
        miscTestLogPaths.append(os.path.join(cls.regression_dir, 'target', 'surefire-reports'))
        # get the properties file.
        miscTestLogPaths.append(cls.test_properties_file)
        # get the test suite xml
        miscTestLogPaths.append(cls.test_suite_xml)
        miscTestLogPaths.append(cls.results_file)
        if logoutput:
            logger.info("Metron misc test log paths are %s", miscTestLogPaths)
        return miscTestLogPaths

    @classmethod
    def getKafkaBrokers(cls):
        '''
        Return the hosts where kafka is running
        :return: Array
        '''
        cls.kafka_hosts = Ambari.getServiceHosts("KAFKA", cls.kafka_broker, cluster=cls.get_ambari_cluster_name())
        if not cls.kafka_hosts:
            logger.error('Kafka is not installed on any of hosts in the cluster')
        return cls.kafka_hosts

    @classmethod
    def isSecuredCluster(cls):
        '''
        Return whether the cluster is secured or not
        :return: boolean
        '''
        cls._is_secure = Ambari.isSecure()
        return cls._is_secure

    @classmethod
    def get_ambari_server_host(cls):
        if not cls._ambari_server_host:
            cls._ambari_server_host = Ambari.getHost()

        return cls._ambari_server_host

    @classmethod
    def getHbaseMasterNode(cls):
        '''
        Return the host which is HBase master node
        :return: String-
        '''
        cls._hbase_master = HBase.getMasterNode()
        if not cls._hbase_master:
            logger.error('HBase is not installed on any of hosts in the cluster')
        return cls._hbase_master

    @classmethod
    def getHbaseRegionServers(cls):
        '''
        Return the List of HBase regionserver nodes
        :return: String
        '''
        cls._hbase_regionservers = HBase.getRegionServers()
        if not cls._hbase_regionservers:
            logger.error('HBase is not installed on any of hosts in the cluster')
        return cls._hbase_regionservers

    @classmethod
    def getMetronRestProperty(cls, param_name):
        '''
        Return the property value for given Metron Rest property from Ambari config using REST API
        :return: List
        '''
        metron_rest_prop = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/configurations?type=%s&tag=%s" % (
            cls.get_ambari_cluster_name(), cls.metron_rest_config_type, cls.metron_config_tag
        )
        retcode, retdata, _retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
        try:
            metron_rest_prop = [hc['properties'][param_name] for hc in jsoncontent['items']]
        except Exception:
            logger.error('Metron rest property %s variable is not set', param_name)
        metron_rest_prop = None
        return metron_rest_prop
