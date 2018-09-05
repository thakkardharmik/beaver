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
import re
from beaver import util
from beaver.component.ambari import Ambari
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class SMM:
    _rest_port = None
    _isSecure = None
    _cluster_name = None
    _ambari_url = None
    _java_home = None
    _version = None
    _log_dir = None
    testsuite_output = None
    mvn_target_dir = None
    surefire_report_dir = None
    smm_master = "STREAMSMSGMGR"
    default_log_dir = "/var/log/streams-messaging-manager"
    smm_qe_dir = os.path.join(Config.getEnv("WORKSPACE"), 'smm-qe')
    test_resources_dir = os.path.join(smm_qe_dir, 'utilities', 'src', 'main', 'resources')
    test_properties_file = os.path.join(test_resources_dir, 'smm-tests.properties')
    installer_resources_dir = os.path.join(smm_qe_dir, 'installer', 'src', 'main', 'resources')
    installer_properties_file = os.path.join(installer_resources_dir, 'smm-installer.properties')
    smm_current_location = "/usr/smm/current/streams-messaging-manager"
    smm_env_sh = os.path.join(smm_current_location, "bin", "streams-messaging-manager-env.sh")

    @classmethod
    def get_port(cls):
        """
        Returns the port on which smm is running
        """
        if not cls._rest_port:
            conf_from_ambari = Ambari.getConfig(service='STREAMSMSGMGR', type="streams-messaging-manager-common")
            cls._rest_port = conf_from_ambari['adminPort']
            logger.info("Server port is = %s" % cls._rest_port)
            cls._rest_port = int(cls._rest_port)
        return cls._rest_port

    @classmethod
    def get_hosts(cls):
        """
        Return the hosts where smm is running. Detection is done using ambari.
        :return: String
        """
        hosts = Ambari.getServiceHosts(
            "STREAMSMSGMGR",
            cls.smm_master,
            cluster=cls.get_ambari_cluster_name(),
            is_hdp=False,
            is_enc=Ambari.is_ambari_encrypted()
        )
        if not hosts:
            logger.error('SMM instance is not installed on any of hosts in the cluster')
        logger.info("SMM installed on: %s " % hosts)
        return hosts

    @classmethod
    def get_smm_urls(cls):
        """
        Return the urls for smm instances
        :return: String
        """
        hosts = cls.get_hosts()
        port = cls.get_port()
        smm_urls = ["http://%s:%d" % (one_host, port) for one_host in hosts]
        return smm_urls

    @classmethod
    def is_smm_service_started(cls, no_of_retries=10, sleep_duration=6):
        """
        SMM service status will be checked on the value of NO_OF_RETRIES times.
        After every try the code will sleep for RETRY_SLEEP_DURATION seconds.
        If the smm service status is "STARTED" we will be returning True
        else the function will return a false
        """
        for i in range(no_of_retries):
            service_status = cls.get_smm_service_status(cls.get_ambari_cluster_name())
            logger.info("SMM service status is : " + service_status)
            if service_status == "STARTED":
                # If service started mark then return True. Assert in test_smm.py will handle assert Success
                return True
            logger.info("SMM service not started yet. Attempt " + str(i))
            util.sleep(sleep_duration)
        logger.error('SMM service is not yet started')
        return False

    @classmethod
    def get_smm_service_status(cls, cluster_name):
        service_state = None
        url = Ambari.getWebUrl(
            is_hdp=False, is_enc=Ambari.is_ambari_encrypted()
        ) + "/api/v1/clusters/%s/services/STREAMSMSGMGR" % cluster_name
        retcode, retdata, retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            service_state = jsoncontent["ServiceInfo"]["state"]
        return service_state

    @classmethod
    def is_smm_service_installed(cls, cluster_name):
        url = Ambari.getWebUrl(
            is_hdp=False, is_enc=Ambari.is_ambari_encrypted()
        ) + "/api/v1/clusters/%s/services/STREAMSMSGMGR" % cluster_name
        retcode, retdata, retheaders = Ambari.performRESTCall(url)
        if retcode == 404:
            return False
        return True

    @classmethod
    def get_ambari_cluster_name(cls, refresh=False):
        if not cls._cluster_name or refresh:
            cls._cluster_name = Ambari.getClusterName(is_hdp=False, is_enc=Ambari.is_ambari_encrypted())
        return cls._cluster_name

    @classmethod
    def get_java_home(cls, refresh=False):
        if not cls._java_home or refresh:
            zookeeper_env = os.path.join(Config.get('zookeeper', 'ZK_HOME'), "conf", "zookeeper-env.sh")
            cls._java_home = util.getPropertyValueFromFile(zookeeper_env, "JAVA_HOME")
        return cls._java_home

    @classmethod
    def getVersion(cls, refresh=False):
        if refresh or not cls._version:
            version_cmd = "find  /usr/smm/current/streams-messaging-manager/libs/kafka-admin-rest-service-[0-9]*.jar"
            smm_host = cls.get_hosts()[0]
            stdout = Machine.runasSuccessfully(user="root", host=smm_host, cmd=version_cmd).split("\n")
            for line in stdout:
                match = re.match(".*smm-common-([\d.-]+).jar", line)
                if match:
                    cls._version = match.group(1)
                    break
        return cls._version

    @classmethod
    def get_ambari_url(cls):
        if not cls._ambari_url:
            is_ambari_encrypted = Ambari.is_ambari_encrypted()
            cls._ambari_url = Ambari.getWebUrl(is_enc=is_ambari_encrypted)
            logger.info("ambari url is: %s" % cls._ambari_url)
        return cls._ambari_url

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        """
        Additional logs that we need captured for atlas system tests
        :return: list of string.
        """
        misc_test_log_paths = [
            cls.mvn_target_dir,
            cls.test_properties_file,
        ]
        # get the testng logs
        # get the properties file.
        # get the test suite xml
        if logoutput:
            logger.info("Nam misc test log paths are %s" % misc_test_log_paths)
        return misc_test_log_paths

    @classmethod
    def is_secure(cls):
        url = "%s/api/v1/clusters/%s" % (
            Ambari.getWebUrl(is_enc=Ambari.is_ambari_encrypted()), Ambari.getClusterName()
        )
        retcode = None
        for i in range(10):
            retcode, retdata, retheaders = Ambari.performRESTCall(url)
            if retcode == 200:
                jsoncontent = util.getJSON(retdata)
                try:
                    if jsoncontent['Clusters']['security_type'] == "KERBEROS":
                        return True
                except:
                    pass
            util.sleep(5)
        return False

    @classmethod
    def get_log_dir(cls, refresh=False):
        if refresh or not cls._log_dir:
            cls._log_dir = util.getPropertyValueFromFile(cls.smm_env_sh, "LOG_DIR")
        return cls._log_dir
