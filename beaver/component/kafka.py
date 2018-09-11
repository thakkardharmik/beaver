#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import collections
import json
import logging
import os
import re
import socket
import tempfile
import time
import uuid
import requests
from requests.auth import HTTPBasicAuth

from beaver import configUtils
from beaver import util
from beaver.component.ambari import Ambari
from beaver.component.hdf_component import HdfComponent
from beaver.component.kafka_security.kafka_plaintext import kafka_plaintext
from beaver.component.kafka_security.kafka_sasl_plaintext import kafka_sasl_plaintext
from beaver.component.kafka_security.kafka_ssl import kafka_ssl
from beaver.component.zookeeper import Zookeeper
from beaver.config import Config
from beaver.kerberos import Kerberos
from beaver.machine import Machine
from beaver import beaverConfigs

SERVER_PROPERTIES = "server.properties"

logger = logging.getLogger(__name__)
KAFKA_HOME = Config.get('kafka', 'KAFKA_HOME', "KAFKA_NOT_INSTALLED")
ARTIFACTS = Config.getEnv('ARTIFACTS_DIR')
gateway = HdfComponent.get_gateway()
AMBARI_SERVER_URL = "https://%s:%s" % (gateway, HdfComponent.get_ambari_server_port())
ADMIN_USER = Machine.getAdminUser()
JAVA_HOME = Config.get('machine', 'JAVA_HOME')


class Kafka:
    beaverConfigs.setConfigs()
    _isSecure = Ambari.is_cluster_secure()
    _isEncrypted = HdfComponent.get_ambari_server_ssl()
    _groupId = "group1"
    _version = None
    # default the hadoop version to 2
    _splitNum = Config.get("common", "SPLIT_NUM", "1")
    _splitNum = int("1" if _splitNum == "" else _splitNum)
    _number_of_splits = Config.get("common", "NUM_OF_SPLITS", "1")
    _number_of_splits = int(_number_of_splits) if _number_of_splits else 1
    test_user = Config.get('hadoop', 'HADOOPQA_USER')
    jar_dir = os.path.join(KAFKA_HOME, 'libs')
    test_principle = "User:%s" % test_user
    if _isSecure:
        service_keytab = Machine.getServiceKeytab('kafka')
        realm = Machine.get_user_realm()
        kinit_template = "kinit -k -t " + service_keytab + " kafka/%s@" + realm
        kinit_cmd = kinit_template % Machine.getfqdn(gateway)

    if _isEncrypted:
        security_protocol = kafka_ssl()
    elif _isSecure:
        security_protocol = kafka_sasl_plaintext()
    else:
        security_protocol = kafka_plaintext()

    security_protocol_opt = "--security-protocol %s" % security_protocol.get_protocol_type()

    @classmethod
    def setupForSSL(cls, topic=None, group=None):
        '''
        1. Give permissions to all users on the topic
        2. Give permissions for consumer group on the topic
        From Dal-M20, if security is on, then Authorizer will be enabled.
        '''
        cmd = "--authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=%s -add --allow-principal User:* --topic %s --operation All" % (
            cls.getZKhostListwithPort(), topic
        )
        cls.runas(cmd=cmd, shell_file="kafka-acls.sh", user=cls.getkafkaAdminUser())
        cmd = "--authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=%s --add --allow-principal User:* --topic %s  --consumer --group %s" % (
            cls.getZKhostListwithPort(), topic, group
        )
        cls.runas(cmd=cmd, shell_file="kafka-acls.sh", user=cls.getkafkaAdminUser())

    @classmethod
    def kinit_user(cls, user):
        if not cls.isSecure():
            logger.info("Unsecure cluster: no kinit required.")
            return
        exit_code, stdout = Kerberos.kinitas(user)
        Machine.runas(user, "kinit -k -t " + Machine.getHeadlessUserKeytab(user) + " " + user + "@" + cls.realm)
        assert exit_code == 0, "Could not run kinit"

    @classmethod
    def kinit_kafka_admin(cls):
        # do a kinit for kafka admin user as we need to run commands as kafka admin
        if not Kafka.isSecure():
            logger.info("kinit is irrelevant for unsecure cluster")
            return
        kafka_admin = Kafka.getkafkaAdminUser()
        all_nodes = Kafka.get_all_nodes()
        for node in all_nodes:
            cmd = cls.kinit_template % Machine.getfqdn(node)
            logger.info("Going to kinit using cmd: %s on: %s" % (cmd, node))
            Machine.runas(user=kafka_admin, cmd=cmd, host=node)

    @classmethod
    def isSecure(cls):
        return cls._isSecure

    @classmethod
    def isEncrypted(cls):
        return cls._isEncrypted

    @classmethod
    def isSSLEnabled(cls):
        if cls._isSecure == True and cls._isEncrypted == True:
            return True

    # pilfered from xa_filecreationambari
    @classmethod
    def isRangerInstalled(cls):
        if Machine.isWindows():
            return False
        return os.path.exists("/etc/ranger/admin/conf/ranger-admin-site.xml")

    @classmethod
    def isRangerAndSSL(cls):
        return cls.isRangerInstalled() and cls.isSSLEnabled()

    @classmethod
    def getSplit(cls):
        return cls._splitNum

    @classmethod
    def getNumberOfSplits(cls):
        return cls._number_of_splits

    @classmethod
    def runSystemTest(cls):
        '''
        :return: True if we want to run system test else return False.
        '''
        return cls.getSplit() == cls.getNumberOfSplits()

    @classmethod
    def skip_non_apache_test(cls):
        """
        :return: string "True" if we want to skip non-Apache Tests else return string "False". To be used for tagging tests.
        """
        return str(cls.getSplit() != cls.getNumberOfSplits() and cls._number_of_splits != 1)

    @classmethod
    def getKafkaHome(cls):
        '''
        Returns KAFKA_HOME. Use cautiously in Linux.
        '''
        return KAFKA_HOME

    @classmethod
    def getkafkaAdminUser(cls):
        '''
        Returns kafka admin user
        '''
        return Config.get('kafka', 'KAFKA_ADMIN')

    @classmethod
    def getVersion(cls):
        '''
      Returns Kafka version as a string.
      '''
        files = util.findMatchingFiles(cls.jar_dir, "kafka_*.jar")
        logger.info(files)
        p = re.compile('kafka_.*-(\S+)-(\d+).jar')
        #logger.info(p)
        for file in files:
            m = p.search(file)
            if m:
                version = m.group(1) + "-" + m.group(2)
        #logger.info(version)
        return version

    @classmethod
    def getHDPVersion(cls):
        '''
      Returns HDP version as a string.
      '''
        ver = cls.getVersion()
        pat = re.compile("\d+\.\d+\.\d+.(.*)")
        m = pat.match(ver)
        return m.group(1)

    @classmethod
    def getModifiedConfigPath(cls):
        kafka_conf = os.path.join(Machine.getTempDir(), 'kafkaConf')
        if not os.path.exists(kafka_conf):
            os.mkdir(kafka_conf)
        return kafka_conf

    @classmethod
    def getKafkaConf(cls):
        '''
        returns kafka conf dir which is /etc/kafka/conf/ in Linux
        '''
        return Config.get('kafka', 'KAFKA_CONF')

    @classmethod
    def modifyConfig(cls, changes):
        '''
        Modify config.
        Returns None.
        Modify config takes source config (local /tmp/kafkaConf) as source and add entries from there.
        '''
        cls.retired_warning(cls.modifyConfig, cls.modify_config_and_restart)
        kafka_conf = cls.getKafkaConf()
        tmp_conf = os.path.join(Machine.getTempDir(), 'kafkaConf')
        brokers = cls.getKafkaBrokers()
        configUtils.modifyConfigRemote(changes, kafka_conf, tmp_conf, nodes=brokers)
        return tmp_conf

    @classmethod
    def retired_warning(cls, old_method, new_method):
        logger.error("Method %s has been retired, use %s instead" % (str(old_method), str(new_method)))

    @classmethod
    def modify_config_and_restart(cls, changes):
        """
        Modify config and restart brokers.
        :param changes: changes to make
        :return: version of old config - this can be used to rollback config
        """
        old_version = Ambari.getCurrentServiceConfigVersion("KAFKA")
        Ambari.setConfig(type="kafka-broker", config=changes["server.properties"])
        cls.restart_brokers()
        return old_version

    @classmethod
    def restart_brokers(cls):
        Ambari.start_stop_service("KAFKA", state="INSTALLED", waitForCompletion=True)
        Ambari.start_stop_service("KAFKA", state="STARTED", waitForCompletion=True)

    @classmethod
    def reset_config_and_restart(cls, version):
        """
        Modify config and restart brokers.
        :param version: version to return
        :param changes: changes to make
        :return: version of old config - this can be used to rollback config
        """
        old_version = Ambari.getCurrentServiceConfigVersion("KAFKA")
        Ambari.resetConfig("KAFKA", version=version)
        cls.restart_brokers()
        return old_version

    @classmethod
    def http_get_request(self, uri, **kwargs):
        '''
        This function executes the HTTP GET requests and returns the response
        '''
        host_url = kwargs.get('host_url', AMBARI_SERVER_URL)
        url = host_url + uri
        logger.info(url)
        print(url)
        basic_auth = HTTPBasicAuth('admin', 'admin')
        response = requests.get(url=url, auth=basic_auth, verify=False)
        return response

    @classmethod
    def http_put_post_request(self, uri, data, requestType, **kwargs):
        '''
        This function executes the HTTP PUT or POST requests based on the paramenters and returns the response
        '''
        host_url = kwargs.get('host_url', AMBARI_SERVER_URL)
        url = str(host_url) + uri
        #self.logger.info(url)
        basic_auth = HTTPBasicAuth('admin', 'admin')
        if requestType == 'PUT':
            response = requests.put(
                url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
            )
            return response
        elif requestType == 'POST':
            response = requests.post(
                url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
            )
            return response
        else:
            logger.info('ILLEGAL REQUEST')
            return None

    # This function starts or stop a given service and returns the response
    @classmethod
    def start_stop_service(self, service, state, **kwargs):
        data = '{"RequestInfo": {"context": "WE API ' + state + ' ' + service + '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        url = '/api/v1/clusters/cl1' + '/services/' + service
        response = self.http_put_post_request(url, data, 'PUT', host_url=AMBARI_SERVER_URL)
        return response

    # This function starts or stop a given component on a given host and returns the response
    @classmethod
    def start_stop_component(self, component, host, state):
        data = '{"RequestInfo": {"context": "WE API ' + state + ' ' + component + ' on ' + host + '"},"HostRoles": {"state": "' + state + '"}}'
        url = '/api/v1/clusters/cl1' + '/hosts/' + host + '/host_components/' + component
        response = self.http_put_post_request(url, data, 'PUT')
        return response._content

    @classmethod
    def get_request_current_state(self, request_id):
        url = '/api/v1/clusters/cl1' + '/requests/' + str(request_id)
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data['Requests']['request_status']

    @classmethod
    def get_services_components_with_stale_configs(self):
        '''
        returns a list of services and components which have stale configs (which need to be restarted)
        '''
        url = '/api/v1/clusters/cl1' + '/host_components?HostRoles/stale_configs=true&fields=HostRoles/service_name,HostRoles/state,HostRoles/host_name,HostRoles/stale_configs,&minimal_response=true'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        stale_service_components = []
        services = set()
        host_components = dict()
        components = []
        # self.logger.info(len(json_data['items']))
        for i in range(0, len(json_data['items'])):
            services.add(json_data['items'][i]['HostRoles']['service_name'])
            if host_components.has_key(json_data['items'][i]['HostRoles']['host_name']):
                components = host_components[json_data['items'][i]['HostRoles']['host_name']]
                components.append(json_data['items'][i]['HostRoles']['component_name'])
            else:
                components.append(json_data['items'][i]['HostRoles']['component_name'])
            host_components[json_data['items'][i]['HostRoles']['host_name']] = components
        stale_service_components.append(services)
        stale_service_components.append(host_components)
        return stale_service_components

    @classmethod
    def get_true_config_dir(cls):
        """
        Get true kafka config directory.
        :return:
        """
        true_config_dir = os.path.join(os.path.dirname(cls.getKafkaConf()), cls.getHDPVersion(), "0")
        logger.info("Kafka true config dir is: %s" % true_config_dir)
        return true_config_dir

    @classmethod
    def copy_kafka_configs_for_humboldt(cls):
        from beaver.component.hadoop import Hadoop
        kafka_conf_dir = Kafka.get_true_config_dir()  # source of truth
        kafka_dir_parent = os.path.dirname(kafka_conf_dir)
        root_user = Machine.getAdminUser()
        root_pass = Machine.getAdminPasswd()
        zk_hosts = Kafka.getZkHosts()
        gateway_fqdn = Machine.getfqdn()
        kafka_admin_user = cls.getkafkaAdminUser()
        hadoop_group = Hadoop.getHadoopGroup()
        chown_cmd = "chown -R %s:%s %s" % (kafka_admin_user, hadoop_group, kafka_conf_dir)
        for one_zk_host in zk_hosts:
            zk_fqdn = Machine.getfqdn(one_zk_host)
            temp_config_dir = os.path.join(Machine.getTempDir(), "tempKafkaConfig-%s" % zk_fqdn, "0")
            logger.info("Using temp_config_dir = %s" % temp_config_dir)
            Machine.makedirs(root_user, gateway, os.path.dirname(temp_config_dir), root_pass)
            Machine.makedirs(root_user, gateway, temp_config_dir, root_pass)
            Machine.rm(root_user, gateway, temp_config_dir, True, root_pass)
            Machine.copy(kafka_conf_dir, temp_config_dir, root_user, root_pass)
            Machine.runas(
                user=root_user,
                cmd="find %s -type f -exec sed -i 's/%s/%s/g' '{}' \;" % (temp_config_dir, gateway_fqdn, zk_fqdn)
            )
            Machine.rm(root_user, one_zk_host, kafka_conf_dir, True, root_pass)
            logger.info(
                "Copying temp_config_dir = %s to machine = %s at dir = %s" %
                (temp_config_dir, one_zk_host, kafka_conf_dir)
            )
            Machine.copyFromLocal(
                user=root_user, host=one_zk_host, srcpath=temp_config_dir, destpath=kafka_dir_parent, passwd=root_pass
            )
            Machine.chmod("777", kafka_conf_dir, True, root_user, one_zk_host, root_pass, True)
            Machine.runas(user=root_user, cmd=chown_cmd, host=one_zk_host, passwd=root_pass, logoutput=True)

    @classmethod
    def startBrokerOnGateway(cls):
        '''
        Install broker on gateway for Humboldt and ASV
        '''
        from beaver.component.hadoop import Hadoop
        logger.info("START KAFKA ON GATEWAY")
        root_user = Machine.getAdminUser()
        root_pass = Machine.getAdminPasswd()
        kafka_admin_user = cls.getkafkaAdminUser()
        hadoop_group = Hadoop.getHadoopGroup()
        kafka_conf_dir = Kafka.get_true_config_dir()  # source of truth
        if not os.path.exists(kafka_conf_dir):
            Machine.makedirs(root_user, gateway, kafka_conf_dir, root_pass)
            Machine.runas(user=root_user, cmd="chmod -R 755 %s" % kafka_conf_dir, passwd=root_pass)
            Machine.runas(
                user=root_user, cmd="chown -R %s:hadoop %s" % (kafka_admin_user, kafka_conf_dir), passwd=root_pass
            )
        gateway_fqdn = Machine.getfqdn()
        server_properties = os.path.join(kafka_conf_dir, "server.properties")
        kafka_broker = cls.getKafkaBrokers()[0]
        #make a backup of current configs
        cmd = "mv %s %s-%d" % (kafka_conf_dir, os.path.join(Machine.getTempDir(), "kafkaConfigBackup"), os.getpid())
        Machine.runas(user=root_user, cmd=cmd, passwd=root_pass)
        #Copy over the configs
        Machine.copyToLocal(
            user=root_user, host=kafka_broker, srcpath=kafka_conf_dir, destpath=kafka_conf_dir, passwd=root_pass
        )
        # fix configs
        values = {}
        values['broker.id'] = '2'
        values['kafka.ganglia.metrics.reporter.enabled'] = 'False'
        values['kafka.metrics.reporters'] = ''
        cmd = "chmod 777 %s" % server_properties
        Machine.runas(user=root_user, cmd=cmd, passwd=root_pass)
        util.writePropertiesToFile(server_properties, server_properties, values)
        Machine.runas(
            user=root_user,
            cmd="find %s -type f -exec sed -i 's/%s/%s/g' '{}' \;" % (kafka_conf_dir, kafka_broker, gateway_fqdn)
        )
        # make dirs for logs
        log_dirs_conf = util.getPropertyValueFromFile(os.path.join(kafka_conf_dir, "server.properties"), "log.dirs")
        log_dirs = [v.strip() for v in log_dirs_conf.split(',')]
        for one_dir in log_dirs:
            Machine.makedirs(user=root_user, host=None, filepath=one_dir, passwd=root_pass)
        # give permissions to log & config dir
        for dir in log_dirs + [kafka_conf_dir]:
            cmd = "chown -R %s:%s %s" % (kafka_admin_user, hadoop_group, dir)
            Machine.runas(user=root_user, cmd=cmd, passwd=root_pass)

        for dir in ['/var/run/kafka', '/var/log/kafka']:
            Machine.makedirs(user=root_user, host=None, filepath=dir, passwd=root_pass)
            Machine.runas(user=root_user, cmd="chmod 755 %s" % dir, passwd=root_pass)
            Machine.runas(
                user=root_user, cmd="chown -R %s:%s %s" % (kafka_admin_user, hadoop_group, dir), passwd=root_pass
            )

        #stop firewall
        cmd = "sudo service ufw stop"
        Machine.runas(user=root_user, cmd=cmd, passwd=root_pass)
        default_config = os.path.join(kafka_conf_dir, "server.properties")
        cmd = default_config
        cls.runInBackgroundAs(cmd=cmd, user=cls.getkafkaAdminUser(), host=None)
        #source_cmd = "source /etc/kafka/conf/kafka-ranger-env.sh"
        #start_cmd = os.path.join(KAFKA_HOME, "bin", "kafka-server-start.sh") + " " + default_config
        #Machine.runinbackgroundAs(kafka_admin_user, cmd=source_cmd + "; " + start_cmd)
        time.sleep(25)

    @classmethod
    def getServiceLogDir(cls):
        return os.path.join(KAFKA_HOME, "logs")

    @classmethod
    def restart_services_with_stale_configs(self):
        # Step 4: Find which host components have stale configs (need to be restarted or have config refreshed)
        stale_service_components = self.get_services_components_with_stale_configs()

        services_to_restart = stale_service_components[0]
        host_components = stale_service_components[1]

        # Step 5: Restart all components or services to have the config change take effect
        services_failed_to_install = []
        services_failed_to_start = []
        for service in services_to_restart:
            response = self.start_stop_service(service, 'INSTALLED')
            if response.status_code is 202:
                json_data = json.loads(response._content)
                request_id = json_data['Requests']['id']
                logger.info("Waiting for the service " + service + " to stop..")
                while not self.get_request_current_state(request_id) == 'COMPLETED':
                    if self.get_request_current_state(request_id) == 'FAILED':
                        services_failed_to_install.append(service)
                        break
                        #raise Exception("Installing service "+ service + " failed!")
                    time.sleep(35)

            response = self.start_stop_service(service, 'STARTED')
            if response.status_code is 202:
                json_data = json.loads(response._content)
                request_id = json_data['Requests']['id']
                logger.info("Waiting for the service " + service + " to start..")
                while not self.get_request_current_state(request_id) == 'COMPLETED':
                    if self.get_request_current_state(request_id) == 'FAILED':
                        services_failed_to_start.append(service)
                        break
                        #raise Exception("Starting service "+ service + " failed!")
                    time.sleep(35)

        if len(services_failed_to_install) != 0:
            print("Following services failed to Install:")
            for service1 in services_failed_to_install:
                print("- " + service1)

        if len(services_failed_to_start) != 0:
            print("Following services failed to Start:")
            for service2 in services_failed_to_start:
                print("- " + service2)

        if len(services_failed_to_install) != 0 or len(services_failed_to_start) != 0:
            raise Exception("Some Services Failed to Restart!!")

    @classmethod
    def modify_service_configs_and_restart(self, config_type, configs_dict):

        # Step 1: Find the latest version of the config type that you need to update.
        url = '/api/v1/clusters/cl1?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        tag = json_data['Clusters']['desired_configs'][config_type]['tag']
        new_tag = "version" + str(uuid.uuid4())
        stack = json_data['Clusters']['version']

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/cl1' + '/configurations?type=' + config_type + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        for config in configs_dict.keys():
            json_data['items'][0]['properties'][config] = configs_dict.get(config)

        json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
            json.dumps(json_data['items'][0]['properties'])
        ) + '}}}'

        # Step 3: Save a new version of the config and apply it using one call
        url = '/api/v1/clusters/cl1'
        response = self.http_put_post_request(url, json_data, 'PUT')
        #self.logger.info(response._content)

        time.sleep(10)

        self.restart_services_with_stale_configs()

    @classmethod
    def stopAllBrokers(cls):
        logger.info("***** STOP ALL BROKERS begins ****")
        brokers = cls.getKafkaBrokers()
        for node in brokers:
            if Machine.isLinux():
                exclFilters = ["grep", "bash"]
                filter = "kafka.Kafka.*server.properties"
                entries = None
                entries = Machine.getProcessListRemote(
                    node, filter=filter, logoutput=False, exclFilters=exclFilters, useEGrep=True
                )
                pid = int(entries[0].split()[1])
                logger.info("******  Killing broker at node : %s *******" % node)
                Machine.runas(user=ADMIN_USER, cmd="kill %d" % pid, host=node)
            else:
                raise AssertionError("Not implemented for non-linux environment.")
        util.sleep(20)
        logger.info("***** STOP ALL BROKERS ends ****")

    @classmethod
    def stopAllZKs(cls):
        nodes = util.getAllNodes()
        for node in nodes:
            if Machine.isHumboldt() and re.search('.*zk.*', node):
                exclFilters = ["grep", "bash"]
                filter = ".*java.*org.apache.zookeeper.server.quorum.QuorumPeerMain.*zoo.cfg"
                entries = Machine.getProcessListRemote(
                    hostIP=node, filter=filter, logoutput=False, exclFilters=exclFilters
                )
                pid = int(entries[0].split()[1])
                logger.info("******  Killing ZK at node : %s *******" % node)
                Machine.killProcessRemote(
                    pid=pid, host=node, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
                )
                time.sleep(10)

    @classmethod
    def get_server_properties(cls):
        return os.path.join(cls.getKafkaConf(), SERVER_PROPERTIES)

    @classmethod
    def get_broker_id_from_all_brokers(cls):
        return cls._get_server_property_from_all_brokers("broker.id")

    @classmethod
    def _get_server_property_from_all_brokers(cls, property_key_regex):
        BROKER_LIST = Kafka.getKafkaBrokers()
        ret_val = {}
        for kafka_broker in BROKER_LIST:
            ret_val[kafka_broker] = cls._get_server_property_from_one_host(kafka_broker, property_key_regex)
        return ret_val

    @classmethod
    def _get_server_property_from_one_host(cls, kafka_broker, property_key_regex):
        server_properties = os.path.join(cls.getKafkaConf(), SERVER_PROPERTIES)
        copy_to_dir = tempfile.mkdtemp("kafka_%s" % kafka_broker)
        Machine.copyToLocal(user=None, host=kafka_broker, srcpath=server_properties, destpath=copy_to_dir, passwd=None)
        copied_server_properties = os.path.join(copy_to_dir, SERVER_PROPERTIES)
        match = util.findMatchingPatternInFile(
            copied_server_properties, property_key_regex + "\s*=\s*(.*)", return0Or1=False
        )
        if len(match) > 0:
            logger.warning("more than one value applicable for property_key_regex=%s" % property_key_regex)
        ret_val = match[0].group(1) if len(match) > 0 else None
        return ret_val

    @classmethod
    def startAllBrokers(cls, brokerList):
        '''
        start brokers on nodes in brokerList
        :return: None
        '''
        logger.info("***** START ALL BROKERS ***")
        for node in brokerList:
            logger.info("******  Starting broker at node : %s *******" % node)
            default_config = Kafka.get_server_properties()
            cmd = default_config
            cls.runInBackgroundAs(cmd=cmd, user=cls.getkafkaAdminUser(), host=node)
        util.sleep(60)

    @classmethod
    def startAllZks(cls):
        ZK_HOSTS = []
        if Machine.isHumboldt():
            allNodes = util.getAllNodes()
            for node in allNodes:
                if re.search('.*zk.*', node):
                    ZK_HOSTS.append(node)
        Zookeeper.startZKServers(zkServers=ZK_HOSTS)
        time.sleep(10)

    @classmethod
    def restoreConfig(cls, changes=None):
        '''
        Restore config.
        changes is a list.
        '''
        cls.retired_warning(cls.restoreConfig, cls.reset_config_and_restart)
        kafka_conf = cls.getKafkaConf()
        brokers = cls.getKafkaBrokers()
        cls.stopAllBrokers()
        if Machine.isLinux():
            configUtils.restoreConfig(changes, kafka_conf, brokers)
            for node in brokers:
                logger.info("****** Start Kafka broker After Restoring*******")
                #if Machine.isHumboldt():
                for node in brokers:
                    logger.info("******  Restarting broker at node : %s *******" % node)
                    default_config = Kafka.get_server_properties()
                    cmd = default_config
                    cls.runInBackgroundAs(cmd=cmd, user=cls.getkafkaAdminUser(), host=node)
                    time.sleep(35)
                #else:
                #   curl_cmd = "curl -k -u admin:admin -i -H 'X-Requested-By: ambari' -X PUT -d '{\"HostRoles\": {\"state\": \"STARTED\"}}' '%s/api/v1/clusters/cl1/hosts/%s/host_components/KAFKA_BROKER'" %(AMBARI_SERVER_URL,node)
                #  ec, stdout = Machine.run(curl_cmd, env=None, logoutput=True)
        else:
            pass

    @classmethod
    def restartKafkaBrokers(cls, config=None, user=None):
        '''
        kill all the running brokers
        restart brokers with $config={config}
        :param brokers: hostname for kafka brokers
        :return: None
        '''
        brokers = cls.getKafkaBrokers()
        cls.stopAllBrokers()
        for node in brokers:
            logger.info("******  Starting broker at node : %s *******" % node)
            if config is None:
                default_config = Kafka.get_server_properties()
                cmd = default_config
            else:
                cmd = config
            cls.runInBackgroundAs(cmd=cmd, user=user, host=node)
        util.sleep(60)

    @classmethod
    def runas(
            cls,
            cmd,
            shell_file="kafka-server-start.sh",
            user=None,
            host=None,
            cwd=None,
            env=None,
            passwd=None,
            logoutput=False,
            pipe_message=None
    ):
        '''
        Kafka Runas method
        :param cmd: Command which needs to be run such as "--broker-list localhost:9092 --topic test-topic "
        :param shell_file: Shell file which should execute this command such as "kafka-console-producer.sh"
        :param user: User
        :param host: the host where this command should be executed
        :param cwd: cwd
        :param env: environment variable
        :param passwd: User password
        :param logoutput: print logoutput
        :param pipe_message: if any kafka api needs to pipe a command before running kafka command. Like In order to run kafka-console-prodcuer,
            an echo message should be passed. "echo 'hello' | ./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic "
        :return: (exit_code, stdout)
        '''

        kafka_bin = os.path.join(KAFKA_HOME, "bin", shell_file)
        kafka_cmd = "%s %s" % (kafka_bin, cmd)
        if pipe_message:
            kafka_cmd = pipe_message + " | " + kafka_cmd
        if not user:
            if Kafka.isSecure():
                user = cls.getkafkaAdminUser()
                cls.kinit_kafka_admin()
            else:
                user = Config.get('hadoop', 'HADOOPQA_USER')
        return Machine.runas(user=user, cmd=kafka_cmd, host=host, cwd=cwd, env=env, logoutput=True, passwd=passwd)

    # Returns subprocess.Popen object.
    @classmethod
    def runInBackground(cls, cmd, shell_file="kafka-server-start.sh", host=None, stdout=None, stderr=None, user=None):
        kafka_bin = os.path.join(KAFKA_HOME, "bin", shell_file)
        kafka_cmd = "%s %s" % (kafka_bin, cmd)
        return cls.runInBackgroundAs(
            kafka_cmd, shell_file=shell_file, host=host, stdout=stdout, stderr=stderr, user=user
        )

    # Returns subprocess.Popen object.
    @classmethod
    def runInBackgroundAs(
            cls, cmd, shell_file="kafka-server-start.sh", host=None, stdout=None, stderr=None, user=None
    ):
        kafka_bin = os.path.join(KAFKA_HOME, "bin", shell_file)
        kafka_cmd = "%s %s" % (kafka_bin, cmd)
        if Kafka.isSecure() and user is not Kafka.getkafkaAdminUser():
            user = Kafka.getkafkaAdminUser()
        return Machine.runinbackgroundAs(user, cmd=kafka_cmd, host=host, stdout=stdout, stderr=stderr)

    @classmethod
    def getZKhostListwithPort(cls):
        '''
        Comma separated list of zookeeper nodes with ports
        '''
        zk_hosts = cls.getZkHosts()
        zk_hosts_withport = ["%s:2181" % h for h in zk_hosts]
        return ",".join(zk_hosts_withport)

    @classmethod
    def getZkHosts(cls):
        if Machine.isHumboldt():
            allNodes = util.getAllNodes()
            zk_hosts = [node for node in allNodes if re.search('.*zk.*', node)]
            return zk_hosts
        zk_hosts = Zookeeper.getZKHosts()
        return zk_hosts

    @classmethod
    def getKafkaBrokers(cls):
        '''
        Returns hostname of all the kafka brokers
        '''
        hosts = []
        allNodes = cls.get_all_nodes()
        for node in allNodes:
            if Machine.isHumboldt() and re.search('.*zk.*', node):
                continue
            else:
                if Machine.isLinux():
                    exclFilters = ["grep", "bash"]
                    filter = "kafka.Kafka.*server.properties|java.*kafkaServer-gc.log"
                    result = Machine.getProcessListRemote(
                        node, filter=filter, exclFilters=exclFilters, logoutput=True, useEGrep=True
                    )
                    logger.info(result)
                else:
                    result = Machine.getProcessListWithPid(
                        node, "java.exe", "server.properties", logoutput=True, ignorecase=True
                    )
                if len(result) != 0:
                    hosts.append(node)
        logger.info("getKafkaBrokers returns %s" % hosts)
        return hosts

    @classmethod
    def stopZKAndKafkaBrokers(cls):
        '''
        Kafka ST require that the already started brokers and zk must be stopped before running STs
        '''
        Ambari.start_stop_service("KAFKA", state="INSTALLED", waitForCompletion=True)
        Ambari.start_stop_service("ZOOKEEPER", state="INSTALLED", waitForCompletion=True)

    @classmethod
    def getKafkaServerHostname(cls):
        '''
        Returns kafka server hostname
        If kafka server is not found, it will return None
        '''
        allNodes = cls.get_all_nodes()
        for node in allNodes:
            result = ""
            if Machine.isLinux():
                result = Machine.getProcessListRemote(
                    node, filter="kafka.Kafka.*server.properties|kafkaServer-gc.log", logoutput=True, useEGrep=True
                )
            else:
                result = Machine.getProcessListWithPid(
                    node, "java.exe", "server.properties", logoutput=True, ignorecase=True
                )
            if result != []:
                return node
        return None

    _broker_list = None

    @classmethod
    def get_broker_list(cls):
        '''
        Return broker list arg to pass to console consumer with option --broker-list
        '''
        # fixme : It's not actually a list as of now, although it will work as arg for --broker-list
        if cls._broker_list:
            return cls._broker_list
        server_host = cls.getKafkaServerHostname()
        logger.info("server_host = %s" % server_host)
        assert server_host, "Couldn't find a suitable broker."
        port = 6667
        if cls._isEncrypted:
            port = 6666
        if Machine.isHumboldt():
            port = 9092
        cls._broker_list = server_host + ":" + str(port)
        return cls._broker_list

    @classmethod
    def get_all_nodes(cls):
        all_nodes = util.getAllNodes()
        if Machine.isHumboldt():
            return all_nodes
        else:
            return [socket.gethostbyaddr(one_node)[0] for one_node in all_nodes]

    @classmethod
    def createTopic(cls, topicName, replication=1, partition=1, user=None, runallowuser=False, allowusername="*"):
        '''
        Create kafka topic
        :param topicName: Name of the topic like "test-topic"
        :param replication: Replication factor
        :param partition: Number of partitions
        :return: (exit_code, stdout)
        '''
        if cls.isSSLEnabled():
            # consumer group is group1
            cls.setupForSSL(topic=topicName, group=cls._groupId)
        zk_str = cls.getZKhostListwithPort()
        cmd = "--create --zookeeper %s --replication-factor %s --partitions %s --topic %s" % (
            zk_str, replication, partition, topicName
        )
        if user == None and Kafka.isSecure():
            exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-topics.sh", user=cls.getkafkaAdminUser())
        else:
            exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-topics.sh", user=user)
        if runallowuser:
            cmd = "--authorizer kafka.security.auth.SimpleAclAuthorizer -authorizer-properties zookeeper.connect=%s --add --allow-principal User:%s --topic %s" % (
                zk_str, allowusername, topicName
            )
            if user == None and Kafka.isSecure():
                exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-acls.sh", user=cls.getkafkaAdminUser())
            else:
                exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-acls.sh", user=user)
        return exit_code, stdout

    @classmethod
    def grantPermission(
            cls,
            permissionOn="topic",
            topicNameOrConsumerGroupNumber="",
            principles=None,
            operation="ALL",
            addOrRemove="add",
            user=None,
            extraOpt=""
    ):
        assert operation in ["READ", "WRITE", "CREATE", "DESCRIBE", "ALL", "IDEMPOTENTWRITE", "DESCRIBECONFIGS", "ALTERCONFIGS"],\
            "Invalid operation: %s" % operation
        # if the operation is read or write also allow describe
        # RangerKafkaAuthorizer requires this, verified with Parth
        user = cls.getkafkaAdminUser() if user is None else user
        valid_permission_ons = ["topic", "cluster", "group", "transactional-id"]
        assert permissionOn in valid_permission_ons, "permissionOn = %s must be one of %s" % (
            permissionOn, valid_permission_ons
        )
        auth_prop = "--authorizer-properties zookeeper.connect=%s" % cls.getZKhostListwithPort()
        add_remove_opt = "--%s" % addOrRemove
        permission_opt = "--%s %s" % (permissionOn, topicNameOrConsumerGroupNumber)
        principles_opt = "--allow-principal %s" % principles
        operation_opt = "--operation %s" % operation
        cmd = " ".join([auth_prop, add_remove_opt, principles_opt, operation_opt, extraOpt, permission_opt])
        ambari_node = Config.get("machine", "GATEWAY")
        return cls.runas(cmd=cmd, host=ambari_node, shell_file="kafka-acls.sh", user=user)

    @classmethod
    def grantAllPermissionOnTopic(cls, topicName):
        exit_code, stdout = cls.grantPermission(topicNameOrConsumerGroupNumber=topicName, principles="User:*")
        assert exit_code == 0, "Kafka grant permission failed for topic %s" % topicName

    @classmethod
    def grantReadPermissionToGroup(cls, group, principle=test_principle):
        """Grant sufficient permissions to make the given topic readable by given user & group."""
        cls.grantPermission(
            permissionOn="group", topicNameOrConsumerGroupNumber=group, principles=principle, operation="READ"
        )

    @classmethod
    def listPermission(cls, permissionOn="topic", topicNameOrConsumerGroupNumber="<mandatory>"):
        valid_permission_ons = ["topic", "cluster", "group"]
        assert permissionOn in valid_permission_ons, "permissionOn = %s must be one of %s" % (
            permissionOn, valid_permission_ons
        )
        user = cls.getkafkaAdminUser()
        list_opt = "--list"
        permission_opt = "--%s %s" % (permissionOn, topicNameOrConsumerGroupNumber)
        auth_prop = "--authorizer-properties zookeeper.connect=%s" % cls.getZKhostListwithPort()
        cmd = " ".join([auth_prop, list_opt, permission_opt])
        return cls.runas(cmd=cmd, shell_file="kafka-acls.sh", user=user)

    @classmethod
    def alterTopic(
            cls,
            topicName,
            config,
            user=None,
    ):
        '''
        ALter kafka topic
        :param topicName: Name of the topic like "test-topic"
        config: The config which needs to be altered
        '''
        zk_str = cls.getZKhostListwithPort()
        cmd = "--zookeeper %s --alter --entity-type topics --entity-name %s %s" % (zk_str, topicName, config)
        if user == None and Kafka.isSecure():
            user = cls.getkafkaAdminUser()
        exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-configs.sh", user=user)

    @classmethod
    def alterUser(
            cls,
            userName,
            config,
            user=None,
    ):
        '''
        ALter kafka topic
        :param topicName: Name of the topic like "test-topic"
        config: The config which needs to be altered
        '''
        zk_str = cls.getZKhostListwithPort()
        cmd = "--zookeeper %s --alter --entity-type users --entity-name %s --add-config %s" % (
            zk_str, userName, config
        )
        if user == None and Kafka.isSecure():
            user = cls.getkafkaAdminUser()
        exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-configs.sh", user=user)

    @classmethod
    def enableTopicDeletion(cls):
        changes = {'server.properties': {'delete.topic.enable': 'true'}}
        return cls.modify_config_and_restart(changes)

    @classmethod
    def deleteTopic(cls, topicName, user=None):
        '''
        Delete a topic
        '''
        if user is None:
            user = cls.getkafkaAdminUser()
        zk_str = cls.getZKhostListwithPort()
        cmd = "--delete --zookeeper %s --topic %s" % (zk_str, topicName)
        logger.info(cmd)
        exit_code, stdout = cls.runas(cmd=cmd, shell_file="kafka-topics.sh", user=user)
        return exit_code, stdout

    @classmethod
    def describeTopic(cls, topicName, user=None):
        '''
        describe a topic
        :param topicName: Name of the topic
        :return: (exitcode,stdout)
        '''
        zk_str = cls.getZKhostListwithPort()
        cmd = "--describe --zookeeper %s --topic %s" % (zk_str, topicName)
        if cls._isSecure:
            env = {
                "KAFKA_OPTS": "-Djava.security.auth.login.config=" + Kafka.getKafkaConf() + "/kafka_client_jaas.conf"
            }
            return cls.runas(cmd=cmd, shell_file="kafka-topics.sh", user=user, env=env)
        else:
            return cls.runas(cmd=cmd, shell_file="kafka-topics.sh", user=user)

    @classmethod
    def get_topic_description(cls, topic_name):
        exit_code, stdout = cls.describeTopic(topic_name)
        match = re.search(".*Topic:(\w+).*PartitionCount:(\d+).*ReplicationFactor:(\d+)", stdout)
        if not match:
            return None
        TopicDescription = collections.namedtuple('TopicDescription', 'Name PartitionCount ReplicationFactor')
        return TopicDescription(match.group(1), match.group(2), match.group(3))

    @classmethod
    def runConsoleProducer(
            cls,
            topicName,
            brokerlist=None,
            message="hello world",
            user=None,
            security_protocol=None,
            options=None,
            producer_properties=None
    ):
        '''
        Run Kafka console producer
        :param topicName: The topic name for which data is produced
        :param brokerlist: pass value for --broker-list
        :param message: the message which wants be produced
        :param user: the user used to run the commands
        :param security_protocol: security protocol to produce data
        :param options: Additional options needed to run the console producer
        :param producer_properties: Producer properties needed to run the console producer
        :return:
        '''
        options_string = ""
        producer_property_string = ""
        if not brokerlist:
            brokerlist = cls.get_broker_list()
        if not security_protocol:
            security_protocol = cls.security_protocol
        if producer_properties:
            producer_property_string = '--producer-property ' + \
                                       ' '.join(['%s=%s' % (key, value) for (key, value) in producer_properties.items()])

        if options:
            options_string = ' '.join(['--%s %s' % (key, value) for (key, value) in options.items()])

        # construct /tmp/client.properties file
        client_properties = os.path.join(Machine.getTempDir(), 'client.properties')
        if Machine.pathExists(None, None, client_properties, None):
            Machine.rm(None, None, filepath=client_properties, isdir=False)

        values = dict()
        values['security.protocol'] = security_protocol.get_protocol_type()
        values.update(security_protocol.get_client_properties())

        open(client_properties, "w")
        util.writePropertiesToFile(client_properties, client_properties, values)
        Machine.chmod(
            perm="777", filepath=client_properties, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
        )

        if cls._isSecure or cls.isEncrypted:
            options_string += " --security-protocol=%s " % values['security.protocol']
        pipe_cmd = "echo '%s'" % (message)
        prod_cmd = " --broker-list %s --topic %s %s %s" \
                   "--producer.config  %s " \
                   % (brokerlist, topicName, producer_property_string, options_string, client_properties)

        return cls.runas(
            cmd=prod_cmd, shell_file="kafka-console-producer.sh", pipe_message=pipe_cmd, logoutput=True, user=user
        )

    @classmethod
    def runConsoleConsumer(
            cls,
            topicName,
            options="--from-beginning --max-messages 1",
            security_protocol=None,
            user=None,
            timeout_sec=10,
            host=None,
            groupID=None,
            brokerlist=None
    ):
        if not brokerlist:
            brokerlist = cls.get_broker_list()
        client_properties = os.path.join(Machine.getTempDir(), 'client.properties')

        if Machine.pathExists(None, None, client_properties, None):
            Machine.rm(None, None, filepath=client_properties, isdir=False)

        if not security_protocol:
            security_protocol = cls.security_protocol

        group_id_opt = ""
        if "--group=" not in options and groupID:
            group_id_opt = "--group=%s" % groupID

        if options is None:
            options = ""
        values = dict()
        values['security.protocol'] = security_protocol.get_protocol_type()
        values.update(security_protocol.get_client_properties())

        open(client_properties, "w")
        util.writePropertiesToFile(client_properties, client_properties, values)
        Machine.chmod(
            perm="777", filepath=client_properties, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
        )
        timeout_opt = "--timeout-ms %d" % (timeout_sec * 1000)
        security_property = ""

        if cls._isSecure or cls.isEncrypted:
            security_property = "--security-protocol=%s" % values['security.protocol']

        consumer_cmd = " --bootstrap-server %s --topic %s --consumer.config %s %s %s %s %s" \
                           % (brokerlist, topicName, client_properties, security_property, timeout_opt, options, group_id_opt)
        return cls.runas(cmd=consumer_cmd, shell_file="kafka-console-consumer.sh", user=user, host=host)

    @classmethod
    def runKafkaRunClass(cls, topicName, package_class, time=-1, options="", user=None, host=None):
        consumer_cmd = "%s --topic %s %s %s --time %s --broker-list %s" % (
            package_class, topicName, options, cls.security_protocol.get_protocol_type, time, cls.get_broker_list()
        )
        return cls.runas(cmd=consumer_cmd, shell_file="kafka-run-class.sh", user=user, host=host)

    @classmethod
    def runSimpleConsumer(cls, topicName, options="", user=None, host=None):
        consumer_cmd = "--broker-list %s --topic %s %s %s" % (
            cls.get_broker_list(), topicName, options, cls.security_protocol.get_protocol_type
        )
        return cls.runas(cmd=consumer_cmd, shell_file="kafka-simple-consumer-shell.sh", user=user, host=host)

    @classmethod
    def download_source(cls):
        '''
        Downloads the Kafka source code matching the current installation.
        Returns the absolute path to the root dir of the code.
        '''
        branch = "HDF-3.1.0.0"
        url = "https://github.com/hortonworks/kafka/archive/%s.tar.gz" % branch
        api_user = Config.get("common", "GITHUB_APT_USER")
        api_token = Config.get("common", "GITHUB_APT_TOKEN")
        cred_args = " --user %s:%s" % (api_user, api_token)
        artifacts_dir = Config.getEnv('ARTIFACTS_DIR')
        tar_file_name = "kafka-src.tar.gz"
        src_tar = os.path.join(artifacts_dir, tar_file_name)
        dest_dir = os.path.join(artifacts_dir, "kafka-%s" % branch)
        dest_file_args = " -o %s" % src_tar
        logger.info("Kafka download_source url=%s src_tar=%s dest_dir=%s" % (url, src_tar, dest_dir))
        util.curlDirect(url, " -L " + cred_args + dest_file_args)
        exit_code, output = Machine.run("tar xf %s" % tar_file_name, cwd=artifacts_dir, logoutput=True)
        assert exit_code == 0, "Unexpected exit_code=%s (output=%s)" % (exit_code, output)
        return dest_dir

    @classmethod
    def generate_keytabs_for_humboldt(cls):
        """
        Generate keytabs on Gateway & Zookeeper nodes.
        Applicatble on humboldt only.
        :return:
        """
        assert Machine.isHumboldt(), "This should be executed only on Humboldt machines."
        assert cls._isSecure, "This should be executed only in secure Humboldt machines."
        # generate kafka service keytab if it is not present on the gateway
        if os.path.exists(cls.service_keytab):
            logger.info("Keytab already exists at: %s " % cls.service_keytab)
            return

        def downloadKeyTab(url, tarfile, num_attempt=20, sleep_sec=30):
            download_location = ARTIFACTS + '/' + tarfile
            download_url = url + '/' + tarfile
            import urllib, urllib2
            for i in range(num_attempt):
                util.sleep(sleep_sec)
                try:
                    logger.info("Downloading tar from: %s" % download_url)
                    urllib2.urlopen(download_url)
                    urllib.urlretrieve(download_url, download_location)
                    logger.info("Tar File downloaded to: %s" % download_location)
                    return download_location
                except IOError:
                    logger.info("Tar File not downloaded.")
            raise AssertionError("kafka service keytab generation failed, the tests can't make progress.")

        def get_kdc_admin_server():
            krb_conf_file = "/etc/krb5.conf"
            with open(krb_conf_file) as input_file:
                lines = input_file.readlines()
                admin_server_lines = [line for line in lines if "admin_server" in line and "FILE" not in line]
                assert len(
                    admin_server_lines
                ) == 1, "Expecting one line for admin_server, found: %s" % admin_server_lines
                admin_server_line = admin_server_lines[0]
                admin_server_value = admin_server_line.split('=')[1].strip()
                return admin_server_value

        assert Machine.isHumboldt(), "Expecting to be a humboldt machine."
        kdc_admin = get_kdc_admin_server()
        HOST = Machine.getfqdn(gateway)
        keytab_file_name = os.path.basename(cls.service_keytab)
        all_hosts = Kafka.getZkHosts()
        all_hosts.append(HOST)
        import random
        TAR_FILE = '%s-%s-kafka-AD_keytabs.tar.gz' % (HOST, str(random.randint(0, 1000)))

        # Execute the jenkins job on AD host to generate keytab
        principle_names = ','.join(["kafka/%s" % host for host in all_hosts])
        logger.info("generating keytab for principle_names: %s" % str(principle_names))
        keytab_file_names = ','.join([keytab_file_name] * len(all_hosts))
        JENKINS_URL = "http://%s:8080/job/AD_SPN_KT_CREATION_NEW/" % kdc_admin
        final_jk_url = "%sbuildWithParameters?PRINCIPAL_NAMES=%s&KEYTAB_NAMES=%s&REALM=%s&TAR_FILE=%s&USE_SAME_KEYTAB=false" % (
            JENKINS_URL, principle_names, keytab_file_names, cls.realm, TAR_FILE
        )
        exit_code, stdout = util.curlDirect(final_jk_url, args="", logoutput=True)
        assert exit_code == 0, "Curl call to generate keytab failed."

        # Now Download the tar file from AD host
        WEBSERVER_HOST = 'http://%s' % kdc_admin
        downloadKeyTab(WEBSERVER_HOST, TAR_FILE)
        download_location = os.path.join(ARTIFACTS, TAR_FILE)
        extract_tar = "tar -xf %s -C %s" % (download_location, ARTIFACTS)
        Machine.runas(Machine.getAdminUser(), extract_tar, passwd=Machine.getAdminPasswd(), logoutput=True)
        keytab_dir = os.path.join(ARTIFACTS, 'keytab')
        for host in all_hosts:
            Machine.chmod(
                "755",
                keytab_dir,
                recursive=True,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd(),
                logoutput=True
            )
            local_dir = os.path.join(keytab_dir, host)
            local_keytab = os.path.join(local_dir, keytab_file_name)
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=host,
                srcpath=local_keytab,
                destpath='/etc/security/keytabs',
                passwd=Machine.getAdminPasswd()
            )

        assert os.path.exists(cls.service_keytab), "Kafka keytab generation failed, tests can't proceed"

    @classmethod
    def update_security_props(cls, mechanisms, listeners, interbrokerprotocol=None, interbrokermechanism=None):
        custom_kafka_broker_props = dict()
        if interbrokerprotocol:
            custom_kafka_broker_props["security.inter.broker.protocol"] = interbrokerprotocol
        if interbrokermechanism:
            custom_kafka_broker_props["sasl.mechanism.inter.broker.protocol"] = interbrokermechanism

        custom_kafka_broker_props["sasl.enabled.mechanisms"] = mechanisms

        is_ambari_enc = Ambari.is_ambari_encrypted()
        kafka_broker = Ambari.getConfig(
            type="kafka-broker", webURL=Ambari.getWebUrl(is_hdp=False, is_enc=is_ambari_enc)
        )
        final_listeners = kafka_broker['listeners']
        print "Current listeners %s" % final_listeners

        for listener in listeners:
            listener_protocol = listener.split("://")[0]
            if listener_protocol == "SASL_PLAINTEXT":
                alternate_name_protocol = "PLAINTEXTSASL"
            if listener_protocol == "PLAINTEXTSASL":
                alternate_name_protocol = "SASL_PLAINTEXT"

            if listener_protocol not in final_listeners and alternate_name_protocol not in final_listeners:
                final_listeners = final_listeners + "," + listener
            elif alternate_name_protocol in final_listeners:
                final_listeners = re.sub(alternate_name_protocol + "://[0-9a-zA-Z.]+:\d+", listener, final_listeners)
            else:
                final_listeners = re.sub(listener_protocol + "://[0-9a-zA-Z.]+:\d+", listener, final_listeners)
        print "Final set listeners %s" % final_listeners

        custom_kafka_broker_props["listeners"] = final_listeners
        if Kafka._isSecure:
            custom_kafka_broker_props["super.users"] = "User:kafka"
        Ambari.setConfig(
            type="kafka-broker",
            config=custom_kafka_broker_props,
            webURL=Ambari.getWebUrl(is_hdp=False, is_enc=is_ambari_enc)
        )

    @classmethod
    def set_idempotence(cls, topicName, brokerlist=None, message="hello world", user=None, security_protocol=None):
        return cls.runConsoleProducer(
            topicName=topicName,
            brokerlist=brokerlist,
            message=message,
            user=user,
            security_protocol=security_protocol,
            producer_properties={"enable.idempotence": "true"},
            options={"request-required-acks": "all"}
        )

    @classmethod
    def kafka_producer_perf_test(
            cls,
            topicName,
            brokerlist=None,
            number_of_records=100,
            security_protocol=None,
            options=None,
            user=None,
            producer_properties=None
    ):
        '''
        Run kafka producer perf test script with provided options
        '''
        options_string = ""
        if not brokerlist:
            brokerlist = cls.get_broker_list()
        if not producer_properties:
            producer_properties = dict()
        producer_properties['bootstrap.servers'] = brokerlist

        if not security_protocol:
            security_protocol = cls.security_protocol

        if options:
            for option, value in options.iteritems():
                options_string += "--%s %s " % (option, value)

        producer_property_string = "--producer-props "

        if cls._isSecure or cls.isEncrypted:
            producer_properties["security.protocol"] = security_protocol.get_protocol_type()

        producer_property_string += ' '.join(['%s=%s' % (key, value) for (key, value) in producer_properties.items()])

        prod_cmd = " --topic %s --num-records %s %s %s" \
                   % (topicName, number_of_records, producer_property_string, options_string)

        return cls.runas(cmd=prod_cmd, shell_file="kafka-producer-perf-test.sh", logoutput=True, user=user)

    @classmethod
    def update_transactional_id(
            cls,
            topicName,
            brokerlist=None,
            security_protocol=None,
            options=None,
            user=None,
            number_of_records=100,
            producer_properties=None,
            transactional_id=1
    ):
        '''
        Update transaction id for a topic
        '''
        default_options = {
            "throughput": 100,
            "record-size": 10,
            "transaction-duration-ms": 10,
            "transactional-id": transactional_id
        }
        if not options:
            options = dict()
        for property, value in default_options.iteritems():
            if property not in options:
                options[property] = value
        return cls.kafka_producer_perf_test(
            topicName=topicName,
            brokerlist=brokerlist,
            producer_properties=producer_properties,
            security_protocol=security_protocol,
            options=options,
            number_of_records=number_of_records,
            user=user
        )

    @classmethod
    def run_java_cmd(
            cls,
            brokerlist=None,
            security_protocol=None,
            user=None,
            host=None,
            cwd=None,
            env=None,
            passwd=None,
            jar_file=None,
            class_name=None,
            args=None
    ):
        '''
        Run Kafka java api code
        '''
        if not brokerlist:
            brokerlist = cls.get_broker_list()
        if not security_protocol:
            security_protocol = cls.security_protocol
        if not user:
            if Kafka.isSecure():
                user = cls.getkafkaAdminUser()
                cls.kinit_kafka_admin()
            else:
                user = Config.get('hadoop', 'HADOOPQA_USER')

        args_string = " "
        if args:
            args_string = ' '.join(args)

        cmd = "%s/bin/java -Djava.security.auth.login.config=%s" \
              " -cp %s:%s:%s %s " \
              "%s %s %s" % (JAVA_HOME,
                            os.path.join(Kafka.getKafkaConf(), "kafka_client_jaas.conf"),
                            jar_file,
                            ':'.join(util.findMatchingFiles(cls.jar_dir, "kafka-clients-*.jar")),
                            ':'.join(util.findMatchingFiles(cls.jar_dir, "slf4j-api-*.jar")), class_name,
                               brokerlist, security_protocol.get_protocol_type(), args_string)
        return Machine.runas(user=user, cmd=cmd, host=host, cwd=cwd, env=env, logoutput=True, passwd=passwd)

    @classmethod
    def alter_topic_configs(
            cls,
            topic_name,
            brokerlist=None,
            security_protocol=None,
            user=None,
            host=None,
            cwd=None,
            env=None,
            passwd=None
    ):
        '''
        Run custom kafka class to alter topic configs
        '''
        return cls.run_java_cmd(
            jar_file=os.path.join(Config.getEnv('ARTIFACTS_DIR'), "..", "jars", "kafka", "kafkatest.jar"),
            class_name="kafka.examples.admin.TopicConfigsTest",
            brokerlist=brokerlist,
            security_protocol=security_protocol,
            user=user,
            host=host,
            cwd=cwd,
            env=env,
            passwd=passwd,
            args=[topic_name]
        )

    @classmethod
    def alter_broker_configs(
            cls, brokerlist=None, security_protocol=None, user=None, host=None, cwd=None, env=None, passwd=None
    ):
        '''
        Run custom kafka class to alter broker configs
        '''
        return cls.run_java_cmd(
            jar_file=os.path.join(Config.getEnv('ARTIFACTS_DIR'), "..", "jars", "kafka", "kafkatest.jar"),
            class_name="kafka.examples.admin.BrokerConfigsTest",
            brokerlist=brokerlist,
            security_protocol=security_protocol,
            user=user,
            host=host,
            cwd=cwd,
            env=env,
            passwd=passwd,
            args=["1001"]
        )
