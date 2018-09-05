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
from beaver.component.nifi import Nifi
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class Streamline:
    _rest_port = None
    _isSecure = None
    _cluster_name = None
    _ambari_url = None
    _java_home = None
    _version = None
    _log_dir = None
    streamline_qe_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'streamline', 'streamline-qe')
    java_resource_dir = os.path.join(streamline_qe_dir, 'main', 'src', 'main', 'resources')
    test_properties_file = os.path.join(java_resource_dir, 'streamline-automation.properties')
    test_suite_xml = os.path.join(streamline_qe_dir, 'src', 'test', 'resources', 'Testsuite.xml')
    mvn_target_dir = os.path.join(streamline_qe_dir, 'main', "target")
    surefire_report_dir = os.path.join(mvn_target_dir, "surefire-reports")
    testsuite_output = os.path.join(surefire_report_dir, 'streamline-qe-results.log')
    streamline_master = "STREAMLINE_SERVER"
    default_log_dir = "/var/log/streamline"

    @classmethod
    def get_port(cls):
        """
        Returns the port on which streamline is running
        """
        if not cls._rest_port:
            conf_from_ambari = Ambari.getConfig(service='STREAMLINE', type="streamline-common")
            cls._rest_port = conf_from_ambari['port']
            logger.info("streamline server port is = %s" % cls._rest_port)
            cls._rest_port = int(cls._rest_port)
        return cls._rest_port

    @classmethod
    def get_hosts(cls):
        """
        Return the hosts where streamline is running. Detection is done using ambari.
        :return: String
        """
        hosts = Ambari.getServiceHosts(
            "STREAMLINE",
            cls.streamline_master,
            cluster=cls.get_ambari_cluster_name(),
            is_hdp=False,
            is_enc=Nifi.get_ambari_server_ssl()
        )
        if not hosts:
            logger.error('Streamline instance is not installed on any of hosts in the cluster')
        logger.info("Streamline installed on: %s " % hosts)
        return hosts

    @classmethod
    def get_streamline_urls(cls):
        """
        Return the urls for streamline instances
        :return: String
        """
        hosts = cls.get_hosts()
        port = cls.get_port()
        streamline_urls = ["http://%s:%d" % (one_host, port) for one_host in hosts]
        return streamline_urls

    @classmethod
    def is_streamline_service_started(cls, no_of_retries=10, sleep_duration=6):
        """
        Streamline service status will be checked on the value of NO_OF_RETRIES times.
        After every try the code will sleep for RETRY_SLEEP_DURATION seconds.
        If the streamline service status is "STARTED" we will be returning True
        else the function will return a false
        """
        for i in range(no_of_retries):
            service_status = cls.get_streamline_service_status(cls.get_ambari_cluster_name())
            logger.info("Streamline service status is : " + service_status)
            if service_status == "STARTED":
                # If service started mark then return True. Assert in test_streamline.py will handle assert Success
                return True
            logger.info("Streamline service not started yet. Attempt " + str(i))
            util.sleep(sleep_duration)
        logger.error('Streamline service is not yet started')
        return False

    @classmethod
    def get_streamline_service_status(cls, cluster_name):
        service_state = None
        url = Ambari.getWebUrl(is_hdp=False) + "/api/v1/clusters/%s/services/STREAMLINE" % cluster_name
        retcode, retdata, retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            service_state = jsoncontent["ServiceInfo"]["state"]
        return service_state

    @classmethod
    def get_ambari_cluster_name(cls, refresh=False):
        if not cls._cluster_name or refresh:
            cls._cluster_name = Ambari.getClusterName(is_hdp=False, is_enc=Nifi.get_ambari_server_ssl())
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
            version_cmd = "find /usr/hdf/current/streamline/libs -iname streamline-common-[0-9]*.jar"
            streamline_host = cls.get_hosts()[0]
            stdout = Machine.runasSuccessfully(user="root", host=streamline_host, cmd=version_cmd).split("\n")
            for line in stdout:
                match = re.match(".*streamline-common-([\d.-]+).jar", line)
                if match:
                    cls._version = match.group(1)
                    break
        return cls._version

    @classmethod
    def get_config_value(cls, *keys):
        streamline_host = cls.get_hosts()[0]
        streamline_yaml = "/etc/streamline/conf/streamline.yaml"
        destination_path = os.path.join(Machine.getTempDir(), "local_streamline.yaml")
        Machine.copyToLocal(user="root", host=streamline_host, srcpath=streamline_yaml, destpath=destination_path)
        import yaml
        with open(destination_path) as f:
            yaml_obj = yaml.load(f)
            for akey in keys:
                yaml_obj = yaml_obj.get(akey)
            return yaml_obj

    @classmethod
    def getDatabaseFlavor(cls):
        jdbc_url = cls.get_config_value(
            "storageProviderConfiguration", "properties", "db.properties", "dataSource.url"
        )
        db_type = jdbc_url.split(":")[1]
        if not db_type:
            return ""
        elif db_type.find("oracle") != -1:
            return "oracle"
        elif db_type.find("postgresql") != -1:
            m = re.search("jdbc:postgresql://(.*):.*", jdbc_url)
            db_host = Machine.getfqdn()
            if m and m.group(1):
                db_host = m.group(1)
            db_version = Machine.getDBVersion('postgres', host=db_host)
            if db_version:
                return "postgres-%s" % db_version
            else:
                return "postgres"
        elif db_type.find("derby") != -1:
            return "derby"
        elif db_type.find("mysql") != -1:
            return "mysql"
        return ""

    @classmethod
    def get_ambari_url(cls):
        if not cls._ambari_url:
            is_ambari_encrypted = Nifi.get_ambari_server_ssl()
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
            cls.surefire_report_dir,
            cls.test_properties_file,
            cls.test_suite_xml,
        ]
        # get the testng logs
        # get the properties file.
        # get the test suite xml
        if logoutput:
            logger.info("Nam misc test log paths are %s" % misc_test_log_paths)
        return misc_test_log_paths

    @classmethod
    def is_secure(cls):
        url = "%s/api/v1/clusters/%s" % (Ambari.getWebUrl(), Ambari.getClusterName())
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
            appenders = cls.get_config_value("logging", "appenders")
            for one_appender in appenders:
                if "currentLogFilename" in one_appender:
                    streamline_log = one_appender.get("currentLogFilename")
                    cls._log_dir = os.path.dirname(streamline_log)
        return cls._log_dir
