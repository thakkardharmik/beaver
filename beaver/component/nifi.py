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
import fnmatch
import re

from beaver import util
from beaver.component.ambari import Ambari
from beaver.component.hdf_component import HdfComponent
from beaver.config import Config
from beaver.util import sleep
from beaver.machine import Machine
logger = logging.getLogger(__name__)


class Nifi(HdfComponent):
    _hosts = None
    _scheme = None
    _rest_port = None
    _cluster_name = None
    _configs = None
    _java_home = None
    _version = None
    _log_dir = None
    _flow_conf_file = None
    _is_encrypted = None
    _ranger_enabled = None
    _ranger_admin_hosts = None
    _ranger_scheme = None
    _ranger_service_port = None
    _ranger_encrypted = None
    _nifi_repo_dirs = None
    _integration_cluster_name = "hdpcl1"
    _login_identity_provider = None
    _user_authorizer = None
    _minifi_java_url = None
    _minifi_cpp_url = None

    test_code_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'nifi')
    regression_dir = os.path.join(test_code_dir, 'nifi-qe')
    nifi_qe_core_dir = os.path.join(regression_dir, "nifi-qe-core")
    test_properties_file = os.path.join(nifi_qe_core_dir, 'src', 'main', 'resources', 'nifi-tests.properties')
    test_suite_xml = os.path.join(nifi_qe_core_dir, 'src', 'main', 'resources', 'Testsuite.xml')
    configs_dir = os.path.join(test_code_dir, 'configs')
    kafka_clusters_json = os.path.join(configs_dir, "kafkaClusters.json")
    db_info_json = os.path.join(configs_dir, "dbInfo.json")
    splunk_clusters_json = os.path.join(configs_dir, "splunkClusters.json")
    hive_clusters_json = os.path.join(configs_dir, "HiveQLProcessorTest.json")
    hdfs_clusters_json = os.path.join(configs_dir, "HDFSProcessorTestClusters.json")
    hive_streaming_clusters_json = os.path.join(configs_dir, "HiveStreamingClusters.json")
    capture_change_mysql_clusters_json = os.path.join(configs_dir, "CaptureChangeMysql.json")
    additional_logs_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'nifi', 'logs')
    bulletins_logs_dir = os.path.join(additional_logs_dir, 'bulletins')
    azure_storage_json = os.path.join(configs_dir, "AzureStorageAccount.json")
    azure_event_hub_json = os.path.join(configs_dir, "AzureEventHub.json")
    aws_json = os.path.join(configs_dir, "AWSAccount.json")

    # In windows When path is read, java considers "\" as escape char and throws error. So escape "\" char.
    if Machine.isWindows():
        bulletins_logs_dir = os.path.join(additional_logs_dir, 'bulletins').replace('\\', "\\\\")

    results_file = os.path.join(additional_logs_dir, 'nifi-results.log')

    no_of_retries = 5  # Number of retries to check if Nifi service is up
    retry_sleep_duration = 10  # Duratio of sleep after which a retry is done
    nifi_master = "NIFI_MASTER"
    default_log_dir = "/var/log/nifi"
    nifi_flow_file_dir = "/var/lib/nifi/conf"

    # configs location for integration cluster.
    nifi_test_data_location = os.path.join('/tmp', 'nifi-test-data-ycloud')
    itc_configs_dir = os.path.join(nifi_test_data_location, _integration_cluster_name)

    # configs path changes based on platform
    if not Machine.isWindows():
        nifi_current_location = "/usr/hdf/current/nifi"
    else:
        folder_name = ""
        base_folder = os.path.join("c:/", "nifi")
        for name in os.listdir(base_folder):
            if fnmatch.fnmatch(name, 'nifi-*'):
                folder_name = name
                break
        nifi_current_location = os.path.join(base_folder, folder_name)

    nifi_env_sh = os.path.join(nifi_current_location, "bin", "nifi-env.sh")
    nifi_properties = os.path.join(nifi_current_location, "conf", "nifi.properties")

    @classmethod
    def get_port(cls):
        '''
        Returns the port on which nifi is running
        '''
        if not cls._rest_port:
            prop_name = 'nifi.web.https.port' if cls.is_encrypted() else 'nifi.web.http.port'
            cls._rest_port = util.getPropertyValueFromFile(cls.nifi_properties, prop_name)

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
    def get_hosts(cls):
        '''
        Return the host where atlas is running
        :return: String
        '''
        if Machine.isWindows():
            cls._hosts = ['localhost']
        else:
            cls._hosts = Ambari.getServiceHosts(
                "NIFI",
                cls.nifi_master,
                cluster=cls.get_ambari_cluster_name(),
                is_hdp=False,
                is_enc=cls.get_ambari_server_ssl()
            )
        if not cls._hosts:
            logger.error('NiFi instance is not installed on any of hosts in the cluster')
        return cls._hosts

    @classmethod
    def is_nifi_service_started(cls, no_of_retries=no_of_retries, sleep_duration=retry_sleep_duration):
        """
        Nifi service status will be checked on the value of NO_OF_RETRIES times.
        After every try the code will sleep for RETRY_SLEEP_DURATION seconds.
        If the nifi service status is "STARTED" we will be returning True
        else the function will return a false
        """
        status = False
        for i in range(cls.no_of_retries):
            service_status = cls.get_nifi_service_status(cls.get_ambari_cluster_name())

            logger.info("Host names : " + str(cls._hosts))
            logger.info("Nifi service status is : " + service_status)

            if service_status == "STARTED":
                # If service started mark then return True. Assert in test_nifi.py will handle assert Success
                status = True
                break
            logger.info("Nifi service not started yet. Attempt " + str(i))
            sleep(cls.retry_sleep_duration)
        if not status:
            logger.error('Nifi service has not been started/active on any of the hosts')
        return status

    @classmethod
    def get_nifi_service_status(cls, cluster_name):
        service_state = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/services/NIFI" % cluster_name

        retcode, retdata, retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            service_state = jsoncontent["ServiceInfo"]["state"]
        return service_state

    @classmethod
    def get_java_home(cls, refresh=False):
        if not cls._java_home or refresh:
            cls._java_home = util.getPropertyValueFromFile(cls.nifi_env_sh, "JAVA_HOME")
        return cls._java_home

    @classmethod
    def getVersion(cls, refresh=False):
        if refresh or not cls._version:
            # Since hdf3.0, version is not stored as property. However, for linux it is not a problem,
            # as Ambari write this version to property. For windows need to retrieve this from the
            # installed nifi folder name
            if Machine.isWindows():
                folderNamesplit = re.split("nifi-", cls.nifi_current_location)
                logger.info("Nifi version in windows platform : " + folderNamesplit[1])
                cls._version = folderNamesplit[1]
            else:
                cls._version = util.getPropertyValueFromFile(cls.nifi_properties, "nifi.version")

        return cls._version

    @classmethod
    def get_log_dir(cls, refresh=False):
        if refresh or not cls._log_dir:
            cls._log_dir = util.getPropertyValueFromFile(cls.nifi_env_sh, "NIFI_LOG_DIR")
        return cls._log_dir

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        '''
        Additional logs that we need captured for atlas system tests
        :return: list of string.
        '''
        miscTestLogPaths = []
        # get the testng logs
        miscTestLogPaths.append(os.path.join(cls.regression_dir, 'target', 'surefire-reports'))
        # get the properties file.
        miscTestLogPaths.append(cls.test_properties_file)
        # get the test suite xml
        miscTestLogPaths.append(cls.test_suite_xml)
        miscTestLogPaths.append(cls.kafka_clusters_json)
        miscTestLogPaths.append(cls.db_info_json)
        miscTestLogPaths.append(cls.splunk_clusters_json)
        miscTestLogPaths.append(cls.nifi_flow_file_dir)
        miscTestLogPaths.append(cls.additional_logs_dir)
        if logoutput:
            logger.info("NiFi misc test log paths are %s" % miscTestLogPaths)
        return miscTestLogPaths

    @classmethod
    def get_flow_config_file(cls):
        '''
        Returns the the file that contains the flow configuration file
        :return: String
        '''
        if not cls._flow_conf_file:
            cls._flow_conf_file = util.getPropertyValueFromFile(cls.nifi_properties, "nifi.flow.configuration.file")

        return cls._flow_conf_file

    @classmethod
    def get_prov_repo_dirs(cls):
        '''
        Returns the the file that contains the flow configuration file
        :return: String
        '''
        if not cls._nifi_repo_dirs:
            cls._nifi_repo_dirs = util.getPropertyValuesFromFile(
                cls.nifi_properties, "nifi.provenance.repository.directory"
            )

        return cls._nifi_repo_dirs

    @classmethod
    def is_encrypted(cls):
        '''
        Returns if nifi is running in secure mode or not.
        :return: true if secure false otherwise.
        '''
        if not cls._is_encrypted:
            cls._is_encrypted = util.getPropertyValueFromFile(cls.nifi_properties, "nifi.cluster.protocol.is.secure")
            if not cls._is_encrypted:
                cls._is_encrypted = False
            else:
                cls._is_encrypted = cls._is_encrypted.lower() == 'true'

        return cls._is_encrypted

    @classmethod
    def get_login_identifier(cls):
        if not cls._login_identity_provider:
            cls._login_identity_provider = util.getPropertyValueFromFile(
                cls.nifi_properties, "nifi.security.user.login.identity.provider"
            )

        return cls._login_identity_provider

    @classmethod
    def get_user_authorizer(cls):
        if not cls._user_authorizer:
            cls._user_authorizer = util.getPropertyValueFromFile(cls.nifi_properties, "nifi.security.user.authorizer")

        return cls._user_authorizer

    @classmethod
    def get_ranger_admin_host(cls):
        '''
        Return the host where ranger admin is running.
        :return: List
        '''

        if not cls._ranger_admin_hosts:
            cls._ranger_admin_hosts = Ambari.getServiceHosts(
                "RANGER",
                "RANGER_ADMIN",
                cluster=cls.get_ambari_cluster_name(),
                is_hdp=False,
                is_enc=cls.get_ambari_server_ssl()
            )

        return cls._ranger_admin_hosts

    @classmethod
    def get_ranger_enc(cls):
        if not cls._ranger_encrypted:
            # TODO. Determine how we can check if enc is on. Today if Ambari is SSL then we are assuming that ranger is SSL.
            cls._ranger_encrypted = cls.get_ambari_server_ssl()
        return cls._ranger_encrypted

    @classmethod
    def get_ranger_scheme(cls):
        if not cls._ranger_scheme:
            cls._ranger_scheme = 'https' if cls.get_ranger_enc() else 'http'

        return cls._ranger_scheme

    @classmethod
    def get_ranger_enabled(cls):
        if not cls._ranger_enabled:
            cls._ranger_enabled = True if len(cls.get_ranger_admin_host()) > 0 else False

        return cls._ranger_enabled

    @classmethod
    def get_ranger_service_port(cls):
        if not cls._ranger_service_port:
            cls._ranger_service_port = '6182' if cls.get_ranger_enc() else '6080'

        return cls._ranger_service_port

    @classmethod
    def get_minfi_java_url(cls):
        if not cls._minifi_java_url:
            repo_base_url = HdfComponent.get_repo_base_url()
            version = cls.getVersion()
            cls._minifi_java_url = '%s/tars/minifi/minifi-0.3.0.3.1.0.0-bin.zip' % repo_base_url
        return cls._minifi_java_url

    @classmethod
    def get_minfi_cpp_url(cls):
        # BUG-90500 minifi cpp is only on CentOS 7 right now.
        if Machine.isCentOs7() and not cls._minifi_cpp_url:
            repo_base_url = HdfComponent.get_repo_base_url()
            cls._minifi_cpp_url = '%s/tars/nifi-minifi-cpp/nifi-minifi-cpp-0.3.0-bin.tar.gz' % repo_base_url
        return cls._minifi_cpp_url
