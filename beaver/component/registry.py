#
# Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os
import re

from beaver import util
from beaver.component.ambari import Ambari
from beaver.component.hdf_component import HdfComponent
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class SchemaRegistry(HdfComponent):  # pylint: disable=no-init
    _version = None
    _hosts = None
    _scheme = None
    _rest_port = None
    _is_encrypted = None
    _api_version = None
    schema_registry_location = "/usr/hdf/current/registry"
    schema_registry_env_sh = os.path.join(schema_registry_location, "conf/registry-env.sh")
    schema_registry_server = "REGISTRY_SERVER"
    schema_registry_qe_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'schema-registry', 'schema-registry-qe')
    java_resource_dir = os.path.join(schema_registry_qe_dir, 'src', 'main', 'resources')
    test_properties_file = os.path.join(java_resource_dir, 'schema-registry.properties')
    test_suite_xml = os.path.join(schema_registry_qe_dir, 'src', 'test', 'resources', 'Testsuite.xml')
    mvn_target_dir = os.path.join(schema_registry_qe_dir, 'target')
    surefire_report_dir = os.path.join(mvn_target_dir, 'surefire-reports')
    testsuite_output = os.path.join(surefire_report_dir, 'schema-registry-qe-results.log')
    registry_properties = os.path.join(schema_registry_location, "conf", "registry.yaml")

    @classmethod
    def get_hosts(cls):
        """
        Return the hosts where registry is running. Detection is done using ambari.
        :return: String
        """
        cls._hosts = Ambari.getServiceHosts(
            "REGISTRY",
            cls.schema_registry_server,
            cluster=cls.get_ambari_cluster_name(),
            is_hdp=False,
            is_enc=cls.get_ambari_server_ssl()
        )
        if not cls._hosts:
            logger.error('Registry instance is not installed on any of hosts in the cluster')
        logger.info("Registry installed on: %s ", cls._hosts)
        return cls._hosts

    @classmethod
    def getVersion(cls, refresh=False):
        if refresh or not cls._version:
            version_cmd = "find /usr/hdf/current/registry/libs -iname registry-common-[0-9]*.jar"
            schema_registry_hosts = cls.get_hosts()[0]
            stdout = Machine.runasSuccessfully(user="root", host=schema_registry_hosts, cmd=version_cmd).split("\n")
            for line in stdout:
                match = re.match(r".*registry-common-([\d.-]+).jar", line)
                if match:
                    cls._version = match.group(1)
                    break
        return cls._version

    @classmethod
    def get_schema_registry_service_status(cls, cluster_name):
        service_state = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/services/REGISTRY" % cluster_name
        retcode, retdata, _retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            service_state = jsoncontent["ServiceInfo"]["state"]
        return service_state

    @classmethod
    def wait_for_schema_registry_service_start(cls, no_of_retries=10, sleep_duration=6):
        """
        Schema Registry service status will be checked on the value of NO_OF_RETRIES times.
        After every try the code will sleep for RETRY_SLEEP_DURATION seconds.
        If the schema registry service status is "STARTED" we will be returning True
        else the function will return a false
        """
        for i in range(no_of_retries):
            service_status = cls.get_schema_registry_service_status(cls.get_ambari_cluster_name())
            logger.info("Schema registry service status is : %s", service_status)
            if service_status == "STARTED":
                # If service started mark then return True.
                #    Assert in test_schema registry.py will handle assert Success
                return True
            logger.info("Schema registry service not started yet. Attempt %s", i)
            util.sleep(sleep_duration)
        logger.error('Schema registry service is not yet started')
        return False

    @classmethod
    def get_java_home(cls, refresh=False):
        if not cls._java_home or refresh:
            cls._java_home = util.getPropertyValueFromFile(cls.schema_registry_env_sh, "JAVA_HOME")
        return cls._java_home

    @classmethod
    def get_schema_registry_urls(cls):
        """
        Return the urls for schema registry instances
        :return: List of string
        """
        schema_registry_urls = [
            "%s://%s:%d/api/%s" % (cls.get_scheme(), one_host, cls.get_port(), cls.get_shema_api_version())
            for one_host in cls.get_hosts()
        ]
        return schema_registry_urls

    @classmethod
    def get_shema_api_version(cls):
        if not cls._api_version:
            cls._api_version = "v1"
        return cls._api_version

    @classmethod
    def get_port(cls):
        """
        Returns the port on which schema registry is running
        """
        if not cls._rest_port:
            conf_from_ambari = Ambari.getConfig(service='REGISTRY', type="registry-common")
            cls._rest_port = conf_from_ambari['port']
            logger.info("Schema registry server port is = %s", cls._rest_port)
            cls._rest_port = int(cls._rest_port)
        return cls._rest_port

    @classmethod
    def get_scheme(cls):
        '''
        Returns the scheme to be used for nifi request
        :return: String
        '''
        if not cls._scheme:
            cls._scheme = 'https' if cls.is_encrypted() else 'http'
        return cls._scheme

    @classmethod
    def is_encrypted(cls):
        '''
        Returns if registry is running in secure mode or not.
        :return: true if secure false otherwise.
        '''
        return False
        # TODO: Check the if the below code is required at all!!
        # ''' Work around as this is failing '''
        #if not cls._is_encrypted:
        #    import yaml
        #    temp_properties_location = "/tmp/registry.yaml"
        #    Machine.copyToLocal(
        #        Machine.getAdminUser(),
        #        cls.get_hosts()[0], cls.registry_properties, temp_properties_location
        #    )
        #    myfile = open(temp_properties_location, 'r')
        #    data = myfile.read()

        #    conf_dict = yaml.load(data)
        #    cls._is_encrypted = conf_dict['server']['applicationConnectors'][0]['type'] == 'https'
        #return cls._is_encrypted

    @classmethod
    def is_secure(cls):
        url = "%s/api/v1/clusters/%s" % (Ambari.getWebUrl(), Ambari.getClusterName())
        retcode = None
        for _ in range(10):
            retcode, retdata, _retheaders = Ambari.performRESTCall(url)
            if retcode == 200:
                jsoncontent = util.getJSON(retdata)
                try:
                    if jsoncontent['Clusters']['security_type'] == "KERBEROS":
                        return True
                except Exception:
                    pass
            util.sleep(5)
        return False
