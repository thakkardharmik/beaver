#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import json
import logging
import os
import re
import time
import datetime
import random
from random import choice
from string import letters
import requests
from requests.auth import HTTPBasicAuth
from taskreporter.taskreporter import TaskReporter
from beaver import util
from beaver.component import knoxsso
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.zookeeper import Zookeeper
from beaver.component.kafka import Kafka
from beaver.component.sqoop import Sqoop
from beaver.component.ambari import Ambari
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class Atlas(object):
    _version = None
    _host = None
    _home_dir = None
    _conf_dir = None
    _log_dir = None
    _scheme = None
    _port = None
    _ssl_enabled = None
    _base_url = None
    _atlas_cluster_name = None
    _hadoop_jwt_cookie = None
    _knox_sso_enabled = None
    _knox_provider_url = None
    _notification_version = None
    _is_atlas_ldap_enabled = None

    test_code_dir = os.path.join(Config.getEnv("WORKSPACE"), 'tests', 'atlas', 'regression')
    test_user = Config.get('hadoop', 'HADOOPQA_USER')
    CLUSTER_NAME = None
    HADOOP_VERSION = None
    ATLAS_VERSION = None
    KAFKA_VERSION = None
    KAFKA_CONF = None
    KAFKA_BROKERS = None
    DATABASE_FLAVOR = None
    HDFS_USER_DIR = None
    HDFS_FALCON_DIR = None
    atlas_test_user_2 = None
    IS_HA_ENABLED = None
    WORKSPACE = None
    knox_hosts = None
    ATLAS_ACTIVE_HOST = None
    ATLAS_SCHEME = None
    ATLAS_PORT = None
    ATLAS_CLUSTER_NAME = None
    ATLAS_PROPERTIES = Ambari.getConfig(service="ATLAS", type="application-properties")
    ATLAS_LOGIN_NAME = 'admin'
    ATLAS_LOGIN_PASSWORD = 'admin123'
    ATLAS_CONFIGS_IN_AMBARI = {}
    KNOX_USERNAME = 'admin'
    KNOX_PASSWORD = 'admin-password'

    if Config.get('knox', "ENABLE_KNOX_SSO", "no") == 'yes':
        KNOX_USERNAME = 'hrt_qa'
        KNOX_PASSWORD = 'Horton!#works'

    if Config.get('atlas', 'ATLAS_INSTALLED') == 'yes':
        Kafka.grantReadPermissionToGroup("*")
        Kafka.grantAllPermissionOnTopic("ATLAS_ENTITIES")
        Kafka.grantAllPermissionOnTopic("ATLAS_HOOK")

    # Temporary check to revert password if it is a upgraded cluster.
    # Valid only for upgrades from 2.6.x to 3.0. This can be removed once 2.6.x support is stopped.
    DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
    uifrm_folder = "uifrm_old/uifrm"
    amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder, 'ambari.properties')
    if not os.path.isfile(amb_prop_file):
        uifrm_folder = "uifrm"
    amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder)
    from beaver.component.ambari_apilib import APICoreLib
    apicorelib = APICoreLib(amb_prop_file)

    def __init__(self):
        pass

    @classmethod
    @TaskReporter.report_test()
    def is_upgraded_from_HDP_2_6_to_3(cls):
        upgraded = False
        url = '/api/v1/clusters/cl1/upgrades?fields=Upgrade'
        response = Ambari.http_get_request(url)
        json_data = json.loads(response.content)
        try:
            if json_data["items"] and "ATLAS" in json_data["items"][0]["Upgrade"]["versions"]:
                versions = json_data["items"][0]["Upgrade"]["versions"]
                if str(versions["ATLAS"]["from_repository_version"]).startswith("2.") \
                        and str(versions["ATLAS"]["to_repository_version"]).startswith("3."):
                    upgraded = True
        except ValueError:
            logger.info("Atlas is not upgraded as part of this run")
        return upgraded

    @classmethod
    @TaskReporter.report_test()
    def get_atlas_login_password(cls):
        if (cls.apicorelib.get_last_upgrade_status() == "COMPLETED" and cls.is_upgraded_from_HDP_2_6_to_3()) or \
                Ambari.getHDPVersion()[6:].startswith('2.6'):
            cls.ATLAS_LOGIN_PASSWORD = 'admin'
        if cls.is_ldap_enabled_on_atlas():
            cls.ATLAS_LOGIN_PASSWORD = 'Horton!#works'
        return cls.ATLAS_LOGIN_PASSWORD

    @classmethod
    @TaskReporter.report_test()
    def setup_atlas_test_variables(cls):
        cls.CLUSTER_NAME = Ambari.getClusterName(is_enc=cls.ssl_enabled())
        cls.HADOOP_VERSION = Hadoop.getVersion()
        cls.ATLAS_VERSION = cls.getVersion() if not cls._version else cls._version
        cls.KAFKA_VERSION = Kafka.getVersion()
        cls.KAFKA_CONF = Kafka.getKafkaConf()
        cls.KAFKA_BROKERS = Kafka.get_broker_list()
        cls.DATABASE_FLAVOR = Sqoop.getDatabaseFlavor()
        cls.HDFS_USER_DIR = '/user/hrt_qa'
        cls.atlas_test_user_2 = Config.get('hadoop', 'OTHER_QA_USER')
        cls.IS_HA_ENABLED = Config.get('machine', 'IS_HA_TEST')
        cls.WORKSPACE = Config.getEnv('WORKSPACE')
        cls.KNOX_HOSTS = Ambari.getServiceHosts("KNOX", "KNOX_GATEWAY", cluster=cls.CLUSTER_NAME)
        cls.ATLAS_ACTIVE_HOST = cls.get_host() if not cls._host else cls._host
        cls.ATLAS_SCHEME = cls.get_scheme() if not cls._scheme else cls._scheme
        cls.ATLAS_PORT = str(cls.get_port()) if not cls._port else str(cls._port)
        cls.ATLAS_CLUSTER_NAME = cls.CLUSTER_NAME
        cls.ATLAS_CONFIGS_IN_AMBARI = Ambari.getConfig("application-properties", "ATLAS", cls.CLUSTER_NAME)
        cls.get_atlas_login_password()

    @classmethod
    def set_atlas_credentials(cls, username, password):
        cls.ATLAS_LOGIN_NAME = username
        cls.ATLAS_LOGIN_PASSWORD = password

    @classmethod
    @TaskReporter.report_test()
    def set_host(cls, host):
        cls._host = host

    @classmethod
    @TaskReporter.report_test()
    def get_user(cls):
        user = "admin"
        if Hadoop.isSecure():
            klist = Machine.run(cmd="klist", logoutput=True)[1]
            for info in klist.splitlines():
                userMatch = re.match(r'Default principal: (.*)@(.*)', info, re.M | re.I)
                if userMatch:
                    user = userMatch.group(1)
            if not user:
                user = Config.getEnv('USER')
                Machine.getKerberosTicket(user)
        return user

    @classmethod
    @TaskReporter.report_test()
    def is_secure(cls):
        env = {}
        if Hadoop.isSecure():
            user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            return True
        else:
            return False

    @classmethod
    @TaskReporter.report_test()
    def is_atlas_knox_sso_enabled(cls, reset=False):
        if reset:
            cls._knox_sso_enabled = None
        if cls._knox_sso_enabled is None:
            atlas_properties = Ambari.getConfig(service="ATLAS", type="application-properties")
            knox_sso_enabled = atlas_properties.get('atlas.sso.knox.enabled', False)
            if knox_sso_enabled and knox_sso_enabled.strip() == "true":
                cls._knox_sso_enabled = True
        return cls._knox_sso_enabled

    @classmethod
    def is_ldap_enabled_on_atlas(cls):
        atlas_properties = Ambari.getConfig(service="ATLAS", type="application-properties")
        atlas_ldap_enabled = atlas_properties.get('atlas.authentication.method.ldap', False)
        if atlas_ldap_enabled and atlas_ldap_enabled.strip() == "true":
            cls._is_atlas_ldap_enabled = True
        return cls._is_atlas_ldap_enabled

    @classmethod
    @TaskReporter.report_test()
    def get_notification_version(cls):
        if cls._notification_version:
            atlas_properties = Ambari.getConfig(service="ATLAS", type="application-properties")
            cls._notification_version = atlas_properties.get('atlas.notification.entity.version', "v2")
        return cls._notification_version

    @classmethod
    @TaskReporter.report_test()
    def get_knox_sso_provider_url(cls, reset=False):
        if reset:
            cls._knox_provider_url = None
        if not cls._knox_provider_url:
            atlas_properties = Ambari.getConfig(service="ATLAS", type="application-properties")
            knox_provider_url = atlas_properties.get('atlas.sso.knox.providerurl', None)
            if knox_provider_url and knox_provider_url.strip():
                cls._knox_provider_url = knox_provider_url
        return cls._knox_provider_url

    @classmethod
    @TaskReporter.report_test()
    def get_scheme(cls):
        '''
            Returns the scheme to be used for atlas request
        :return: String
        '''
        if not cls._scheme:
            if cls.ssl_enabled():
                cls._scheme = 'https'
            else:
                cls._scheme = 'http'
        return cls._scheme

    @classmethod
    @TaskReporter.report_test()
    def get_port(cls):
        '''
            Returns the port on which atlas is running
        :return: int
        '''
        if not cls._port:
            if cls.ssl_enabled():
                cls._port = 21443
            else:
                cls._port = 21000
        return cls._port

    @classmethod
    @TaskReporter.report_test()
    def isInstalled(cls, service="ATLAS"):
        '''
            Returns true if Service is installed on the cluster
        :return: Boolean
        '''
        # try:
        #     return Machine.pathExists(Machine.getAdminUser(), host, "/etc/%s/conf" % (service),
        #                               Machine.getAdminPasswd())
        # except IOError:
        #     return False

        url = '/api/v1/clusters/cl1/services/' + service.upper()
        response = Ambari.http_get_request(url)
        return response.status_code == 200

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        '''
        Return the version of atlas running
        :return: String
        '''
        # if version is not set set it
        if not cls._version:
            curl_string = 'curl -s -u {}:{}'.format(cls.ATLAS_LOGIN_NAME, cls.ATLAS_LOGIN_PASSWORD)
            if cls.ssl_enabled():
                curl_string += ' -k '
            # make sure curl runs in silent mode and use the -k flag to get https call to work.
            cmd = '%s %s://%s:%s/api/atlas/admin/version' % (
                curl_string, cls.get_scheme(), cls.get_host(), cls.get_port()
            )
            env = {}
            if Hadoop.isSecure():
                user = Config.getEnv('USER')
                kerbTicket = Machine.getKerberosTicket(user)
                env['KRB5CCNAME'] = kerbTicket
                cmd = 'curl -k -s --negotiate -u {}:{} {}://{}:{}/api/atlas/admin/version'.format(
                    cls.ATLAS_LOGIN_NAME, cls.ATLAS_LOGIN_PASSWORD, cls.get_scheme(), cls.get_host(), cls.get_port()
                )

            rc, stdout = Machine.run(cmd, env=env, logoutput=True)
            if rc != 0:
                logger.error('curl command failed. Running with verbose to capture more logs')
                rc, stdout = Machine.run(cmd + ' -verbose', env=env, logoutput=True)
                if rc != 0:
                    logger.error('Atlas version info is not found via REST API')
                    return None
            # should return data like
            # {
            #     Version: "0.1.0.2.3.0.0-1979-r75dad3000f9e41359955c11e33a239a801666c59"
            #     Name: "metadata-governance"
            #     Description: "Metadata Management and Data Governance Platform over Hadoop"
            # }
            response = json.loads(stdout)
            p = re.compile('^(.*)-r(.*)')
            m = p.search(response['Version'])
            if m and m.group(1):
                cls._version = m.group(1)

        return cls._version

    @classmethod
    @TaskReporter.report_test()
    def stop(cls):
        cmd = os.path.join(cls.get_home_dir(), "bin", "atlas_stop.py")
        env = {}
        user = 'atlas'
        if Hadoop.isSecure():
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket

        rc, _stdout = Machine.runas(user, cmd, cwd=None, env=env, logoutput=True)
        if rc != 0:
            logger.error('atlas stop failed')

    @classmethod
    @TaskReporter.report_test()
    def start(cls):
        cmd = os.path.join(cls.get_home_dir(), "bin", "atlas_start.py")
        env = {}
        user = 'atlas'
        if Hadoop.isSecure():
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket

        rc, _stdout = Machine.runas(user, cmd, cwd=None, env=env, logoutput=True)
        if rc != 0:
            logger.error('atlas start failed')

    @classmethod
    @TaskReporter.report_test()
    def get_host(cls, retry_count=5, reset=False, gateway=None):
        '''
        Return the host where atlas is running
        :param retry_count how to many times to retry for getting the hostname
        :param reset get the active host name even when the cls._host is set.
        :return: String
        '''
        hosts = []
        if reset:
            cls._host = None
        if not cls._host:
            if not gateway:
                gateway = cls.get_gateway_1()
            clusterName = Ambari.getClusterName(is_enc=cls.ssl_enabled(), weburl=gateway)
            hosts = cls.getServiceHosts(
                service="ATLAS", component="ATLAS_SERVER", gateway=gateway, cluster=clusterName
            )
            curl_string = 'curl -s -u {}:{}'.format(cls.ATLAS_LOGIN_NAME, cls.ATLAS_LOGIN_PASSWORD)
            if cls.ssl_enabled():
                curl_string += ' -k '
            for attempt in xrange(retry_count):
                logger.info("Getting atlas host info: attempt - %s", str(attempt))
                for host in hosts:
                    # make sure curl runs in silent mode and use the -k flag to get https call to work.
                    cmd = '%s %s://%s:%s/api/atlas/admin/status' % (
                        curl_string, cls.get_scheme(), host, cls.get_port()
                    )
                    env = {}
                    if Hadoop.isSecure():
                        user = Config.getEnv('USER')
                        kerbTicket = Machine.getKerberosTicket(user)
                        env['KRB5CCNAME'] = kerbTicket
                        cmd = 'curl -k -s --negotiate -u :  %s://%s:%s/api/atlas/admin/status' % (
                            cls.get_scheme(), host, cls.get_port()
                        )

                    rc, stdout = Machine.run(cmd + ' | grep Status', env=env, logoutput=True)
                    if rc != 0:
                        logger.error('curl command failed. Running with verbose to capture more logs')
                        rc, stdout = Machine.run(cmd + ' --verbose', env=env, logoutput=True)
                        if rc != 0:
                            logger.error(
                                'The Atlas instance on host: %s is not reachable, may be it is not up.'
                                'Please check the atlas application logs to know the root cause.', host
                            )
                            continue

                    response = json.loads(stdout)
                    if response["Status"] == "ACTIVE":
                        cls._host = host
                        break
                if cls._host:
                    break
        if not cls._host:
            logger.error('Atlas instance is not running/active on any of hosts in the cluster, exiting now')
        return cls._host

    @classmethod
    @TaskReporter.report_test()
    def getServiceHosts(cls, service="ATLAS", component="ATLAS_SERVER", gateway=None, cluster='cl1'):
        hosts = []
        if not gateway:
            gateway = cls.get_gateway_1()

        url = "%s/api/v1/clusters/%s/services/%s/components/%s" % (gateway, cluster, service, component)
        logger.info("url")
        logger.info(url)
        retcode, retdata, _retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                hosts = [hc['HostRoles']['host_name'] for hc in jsoncontent['host_components']]
            except Exception as e:
                logger.info(e.message)
                hosts = []
        return hosts

    @classmethod
    def get_gateway_1(cls):
        return cls.get_scheme() + '://' + Config.get('machine',
                                                     'GATEWAY') + ':' + str(8443 if cls.ssl_enabled() else 8080)

    @classmethod
    @TaskReporter.report_test()
    def get_gateway_2(cls):
        if Config.get('multicluster', 'Gateway1').strip() == Config.get('machine', 'GATEWAY').strip():
            gateway2 = Config.get('multicluster', 'Gateway2').strip()
        else:
            gateway2 = Config.get('multicluster', 'Gateway1').strip()
        return cls.get_scheme() + '://' + gateway2 + ':' + str(8443 if cls.ssl_enabled() else 8080)

    @classmethod
    @TaskReporter.report_test()
    def get_atlas_host_1(cls, reset=False):
        if reset:
            cls.ATLAS_HOST_1 = None
        if not cls.ATLAS_HOST_1:
            cls.ATLAS_HOST_1 = cls.get_host(gateway=cls.get_gateway_1(), reset=reset)
        return cls.ATLAS_HOST_1

    @classmethod
    @TaskReporter.report_test()
    def get_atlas_host_2(cls, reset=False):
        if reset:
            cls.ATLAS_HOST_2 = None
        if not cls.ATLAS_HOST_2:
            cls.ATLAS_HOST_2 = cls.get_host(gateway=cls.get_gateway_2(), reset=reset)
        return cls.ATLAS_HOST_2

    @classmethod
    @TaskReporter.report_test()
    def get_home_dir(cls):
        '''
        Get the home dir for atlas
        :return: String
        '''

        if not cls._home_dir:
            # TODO: Add support for windows
            cls._home_dir = '/usr/hdp/current/atlas-server'

        return cls._home_dir

    @classmethod
    @TaskReporter.report_test()
    def get_conf_dir(cls):
        '''
        Get the config location for atlas
        :return: String
        '''
        if not cls._conf_dir:
            cls._conf_dir = os.path.join(cls.get_home_dir(), 'conf')

        return cls._conf_dir

    @classmethod
    @TaskReporter.report_test()
    def get_log_dir(cls):
        '''
        Get the log dir for atlas
        :return: String
        '''
        if not cls._log_dir:
            # TODO: add windows specific code here
            env_file = os.path.join(cls.get_conf_dir(), 'atlas-env.sh')
            cls._log_dir = util.getPropertyValueFromBashFile(env_file, 'ATLAS_LOG_DIR')
            # if its not defined default to home/logs
            if not cls._log_dir:
                cls._log_dir = os.path.join(cls.get_home_dir(), 'logs')

        return cls._log_dir

    @classmethod
    @TaskReporter.report_test()
    def getMiscTestLogPaths(cls, logoutput=False):
        '''
        Additional logs that we need captured for atlas system tests
        :return: list of string.
        '''
        miscTestLogPaths = []
        miscTestLogPaths.append(os.path.join(cls.test_code_dir, 'target', 'surefire-reports'))
        env_file = os.path.join(cls.get_conf_dir(), 'atlas-env.sh')
        data_dir = util.getPropertyValueFromBashFile(env_file, 'ATLAS_DATA_DIR')
        if not data_dir:
            data_dir = os.path.join(cls.get_home_dir(), 'data')
        miscTestLogPaths.append(data_dir)

        if logoutput:
            logger.info("Atlas misc test log paths are %s", miscTestLogPaths)
        return miscTestLogPaths

    @classmethod
    @TaskReporter.report_test()
    def ssl_enabled(cls):
        '''

        :return: ssl enabled or disabled status
        '''
        if not cls._ssl_enabled:
            enable_tls = cls.ATLAS_PROPERTIES.get('atlas.enableTLS', None)
            cls._ssl_enabled = False
            if enable_tls and enable_tls.strip().lower() == 'true':
                cls._ssl_enabled = True
        return cls._ssl_enabled

    @classmethod
    @TaskReporter.report_test()
    def get_atlas_cluster_name(cls):
        '''

        :return: atlas.cluster.name property value is returned
        '''
        if not cls._atlas_cluster_name:
            atlas_cluster_name = Ambari.getClusterName(is_enc=cls.ssl_enabled())

            if atlas_cluster_name and atlas_cluster_name.strip():
                cls._atlas_cluster_name = atlas_cluster_name
        return cls._atlas_cluster_name

    @classmethod
    @TaskReporter.report_test()
    def set_base_url(cls, reset=False, with_host=None):
        '''
        Set base url and return string
        :return:
        '''
        if with_host:
            cls._base_url = cls.get_atlas_url(with_host)
        elif reset:
            cls._base_url = None
        if not cls._base_url:
            cls._base_url = cls.get_atlas_url(cls.get_host(reset=reset))
        return cls._base_url

    @classmethod
    def get_atlas_url(cls, host):
        if Machine.isOpenStack() and cls.is_atlas_knox_sso_enabled():
            host = host + ".com"
        url = cls.get_scheme() + '://' + host + ':' + str(cls.get_port())
        return url

    @classmethod
    @TaskReporter.report_test()
    def run_import_hive(cls):
        logger.info("-----------------------Running import hive---------------")
        import_cmd = "export JAVA_HOME=%s;/usr/hdp/current/atlas-server/hook-bin/import-hive.sh" % (
            Config.get("machine", "QA_CODE_JAVA_HOME")
        )
        environment_vars = None
        # set env variables for secure cluster
        environment_vars = cls.set_env_params_for_import_tests()
        # do kinit for hive user
        cmd = Machine.getKinitCmd()
        principal = Machine.get_user_principal(user="hive")
        keytab = Machine.getHeadlessUserKeytab(user="hive")
        kinitCmd = cmd + ' -kt ' + keytab + ' ' + principal
        Machine.runasSuccessfully(
            user="hive", cmd=kinitCmd, host=Ambari.getServiceHosts(service="HIVE", component="HIVE_SERVER")[0]
        )

        # run the import command as hrt_qa user
        status = Machine.runas(
            cmd=import_cmd,
            user="hive",
            host=Ambari.getServiceHosts(service="HIVE", component="HIVE_SERVER")[0],
            env=environment_vars,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

        logger.info(status)
        return status

    @classmethod
    @TaskReporter.report_test()
    def run_import_hbase(cls, namespace=None, table=None, filepath=None):
        logger.info("-----------------------Running import hbase---------------")
        import_cmd = "export JAVA_HOME=%s;/usr/hdp/current/atlas-server/hook-bin/import-hbase.sh" % (
            Config.get("machine", "QA_CODE_JAVA_HOME")
        )
        if namespace:
            import_cmd = "%s -n %s" % (import_cmd, namespace)
        if table:
            import_cmd = "%s -t %s" % (import_cmd, table)
        if filepath:
            import_cmd = "%s -f %s" % (import_cmd, filepath)
        environment_vars = None
        # set env variables for secure cluster
        environment_vars = cls.set_env_params_for_import_tests()

        # do kinit for hrt_qa user
        cmd = Machine.getKinitCmd()
        principal = Machine.get_user_principal(user="hbase")
        keytab = Machine.getHeadlessUserKeytab(user="hbase")
        kinitCmd = cmd + ' -kt ' + keytab + ' ' + principal
        Machine.runasSuccessfully(
            user="hbase", cmd=kinitCmd, host=Ambari.getServiceHosts(service="HBASE", component="HBASE_MASTER")[0]
        )
        # run the import command as hrt_qa user
        exit_code, status = Machine.runas(
            cmd=import_cmd,
            user="hbase",
            host=Ambari.getServiceHosts(service="HBASE", component="HBASE_MASTER")[0],
            env=environment_vars,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

        logger.info(exit_code)
        logger.info(status)
        return exit_code

    @classmethod
    @TaskReporter.report_test()
    def set_env_params_for_import_tests(cls):
        env = {}
        env["JAVA_HOME"] = Config.get("machine", "QA_CODE_JAVA_HOME")
        if Hadoop.isSecure():
            env['sun.security.jgss.debug'] = "true"
            env['javax.security.auth.useSubjectCredsOnly'] = "false"
            env['java.security.krb5.conf'] = "/etc/krb5.conf"
            env['java.security.auth.login.config'] = "/usr/hdp/current/kafka-broker/conf/kafka_client_jaas.conf"

    @classmethod
    def get_base_url(cls):
        return cls.set_base_url()

    @classmethod
    @TaskReporter.report_test()
    def set_jwt_cookie(cls, url):
        logger.info("GENERATING JWT COOKIE")
        try:
            basic_auth = HTTPBasicAuth(cls.KNOX_USERNAME, cls.KNOX_PASSWORD)
            url = "%s?originalUrl=%s" % (cls.get_knox_sso_provider_url(), url)
            response = requests.get(url=url, auth=basic_auth, verify=False, allow_redirects=False)
            cls._hadoop_jwt_cookie = response.cookies.get_dict().get("hadoop-jwt")
            logger.info("GENERATED JWT COOKIE")
            logger.info(cls._hadoop_jwt_cookie)
        except requests.exceptions.RequestException as e:
            logger.info("exception in http_get_request : %s ", str(e))

    @classmethod
    def get_jwt_cookie(cls):
        return cls._hadoop_jwt_cookie

    @classmethod
    def http_get_request(cls, url):
        '''
        Make get call to the give url , obtain response and return json from response
        :param url: input url to make the get call
        :return:
        '''
        logger.info(url)
        return cls.http_request(url=url, body=None, method='GET')

    @classmethod
    @TaskReporter.report_test()
    def get_type(cls, _type):
        '''
        Get type pass the parameter types created Eg: TRAIT,CLASS,STRUCT
        :param _type: string value of defined types
        :return: return json
        '''
        url = cls.get_base_url() + '/api/atlas/types?type=' + _type
        response, response_status = cls.http_get_request(url)
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def get_types(cls, types_name):
        '''
        Get types name as the argument Eg: tag name created
        :param types: string value of defined types
        :return: return json
        '''
        url = cls.get_base_url() + '/api/atlas/types/' + types_name
        response, response_status = cls.http_get_request(url)
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def get_entities(cls, entities_id):
        '''
        Get details about the entities created given the guid
        :param entities_id: GUID of the entity
        :return: json response
        '''
        url = cls.get_base_url() + '/api/atlas/entities/' + entities_id
        response, response_status = cls.http_get_request(url)
        return response, response_status

    @classmethod
    def http_put_post_request(cls, url, data=None, method='POST'):
        '''
        Make POST or PUT request depending on method
        :param url: url to post /put
        :param data: json data input
        :param header: header information to pass
        :param method: POST or PUT
        :return: json response
        '''
        return cls.http_request(url=url, body=data, method=method)

    @classmethod
    def http_delete_request(cls, url, body=None):
        '''
        Delete request
        :param url: delete request url
        :param body : delete request body
        :return: status code
        '''
        return cls.http_request(url=url, body=body, method='delete')

    @classmethod
    @TaskReporter.report_test()
    def http_request(cls, url, body, method):
        response_body = None
        status = None
        header = {'Content-Type': 'application/json'}
        try:
            if cls.is_atlas_knox_sso_enabled():
                response = requests.request(
                    method=method,
                    url=url,
                    headers=header,
                    verify=False,
                    allow_redirects=False,
                    data=body,
                    cookies={'hadoop-jwt': cls._hadoop_jwt_cookie}
                )
                status = response.status_code
                logger.info(status)
                if status == 401:
                    cls.set_jwt_cookie(url)
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=header,
                        verify=False,
                        allow_redirects=False,
                        data=body,
                        cookies={'hadoop-jwt': cls._hadoop_jwt_cookie}
                    )

            else:
                auth = HTTPBasicAuth(cls.ATLAS_LOGIN_NAME, cls.ATLAS_LOGIN_PASSWORD)
                if Hadoop.isSecure():
                    try:
                        from requests_kerberos import HTTPKerberosAuth, OPTIONAL
                        auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
                    except ImportError, e:
                        logging.error("requests_kerberos module is not present on this node.")
                response = requests.request(method=method, url=url, auth=auth, headers=header, verify=False, data=body)

            status = response.status_code
            # if method == 'delete' or status == 204:
            #     response_body=None
            if method == 'delete':
                return status
            elif status != 204:
                logger.info("status of response: %s", str(status))
                response_body = response.json()
        except requests.exceptions.RequestException as e:
            logger.info("exception in http request : %s", str(e))
        return response_body, status

    @classmethod
    @TaskReporter.report_test()
    def create_type(cls, input_json_file=None, input_json_string=None):
        '''
        Takes in input.json file to create type
        :param input_json_file: input.json
        :param input_json_string: json string
        :return: json response
        '''
        url = cls.get_base_url() + '/api/atlas/types'
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        if input_json_string:
            json_data = input_json_string
        response, response_status = cls.http_put_post_request(url, json_data, 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def update_type(cls, input_json_file=None, input_json_string=None):
        '''
        update type information
        :param input_json_file: input.json
        :param input_json_string: json string
        :return: json response
        '''
        url = cls.get_base_url() + '/api/atlas/types'
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        if input_json_string:
            json_data = input_json_string
        response, response_status = cls.http_put_post_request(url, json_data, 'PUT')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def create_entities(cls, input_json_file=None, input_json_string=None):
        '''
        Create atlas entities
        :param input_json_file: input.json
        :param input_json_string: json string
        :return: json response
        '''
        url = cls.get_base_url() + '/api/atlas/entities'
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        if input_json_string:
            json_data = input_json_string
        response, response_status = cls.http_put_post_request(url, json_data, 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def update_entities(cls, input_json_file=None, input_json_string=None):
        '''
        update atlas entities
        :param input_json_file: input.json
        :param input_json_string: json string
        :return:
        '''
        url = cls.get_base_url() + '/api/atlas/entities'
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        if input_json_string:
            json_data = input_json_string
        response, response_status = cls.http_put_post_request(url, json_data, 'PUT')
        return response, response_status

    @classmethod
    def get_entity_audits(cls, guid):
        url = cls.get_base_url() + '/api/atlas/entities/' + guid + '/audit'
        response, response_status = cls.http_get_request(url)
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def create_tag(
            cls, tag_name, attrib_map=None, description=None, parent_tag=None, entity_types=None,
            expected_resp_code=200
    ):
        '''

        :param tag_name: tag name
        :param attrib_map: attribute map example : {"name":"string","id":"int","dob":"date"}
        :param description: optional description
        :param parent_tag: parent tag can be list of tags already created or a single tag name
        :param expected_resp_code: expected response code
        :return:
        '''
        url = cls.get_base_url() + '/api/atlas/v2/types/typedefs?type=classification'
        tag_definition = Utils.construct_tag_def(tag_name, attrib_map, description, parent_tag, entity_types)
        response, response_status = cls.http_put_post_request(url, tag_definition, 'POST')
        assert response_status == expected_resp_code
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def get_tag_def(cls, tag_name):
        url = cls.get_base_url() + '/api/atlas/v2/types/typedefs?type=classification&name=' + tag_name
        response, _status = cls.http_get_request(url)
        logger.debug("response is %s", str(response))
        return response

    @classmethod
    @TaskReporter.report_test()
    def add_more_attributes_to_tag(cls, tag_name, attrib_map):
        url = cls.get_base_url() + '/api/atlas/v2/types/typedefs?type=classification'
        tag_def = cls.get_tag_def(tag_name)
        for attribute, datatype in attrib_map.items():
            # create column definition
            temp_file = "tests/resources/atlas_model_templates/trait/tag_attribute.json"
            json_file = open(temp_file, "r")
            attr_def = json.load(json_file)
            json_file.close()
            attr_def["name"] = attribute
            attr_def["typeName"] = datatype
            # add the constructed attribute definition to tag
            tag_def["classificationDefs"][0]["attributeDefs"].append(attr_def)
        response, response_status = cls.http_put_post_request(url, json.dumps(tag_def), 'PUT')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def associate_traits_to_entity(
            cls, trait_name, attrib_map=None, entity_guid=None, validity_periods=None, propagate=True
    ):
        '''
        associate traits with entities
        :param trait_name : name of trait
        :param entity_guid: guid of the entity to associate the trait.
        :param validity_periods - lists of dictionary. each dictionary is of format:
         {startTime:"" , endTime:"" , timeZone:""}
         to remove validity period completely , provide {}
         to untouch the validity period use None
        :param propagate : propagate flag to set propagation to True or False
        :return: response status
        '''

        url = cls.get_base_url() + '/api/atlas/v2/entity/guid/%s/classifications'
        url = url % (entity_guid)
        template_dir = "tests/resources/atlas_model_templatesatlas_model_templates"
        _input = template_dir + "/trait/associate_tag_template.json"
        json_data = json.loads(open(_input).read())
        logger.info(json.dumps(json_data))
        json_data[0]["typeName"] = trait_name
        if attrib_map:
            json_data[0]["attributes"].update(attrib_map)
        json_data[0]["propagate"] = propagate
        if validity_periods:
            json_data[0]["validityPeriods"] = validity_periods
        response, response_status = cls.http_put_post_request(url, json.dumps(json_data), 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def associate_traits(cls, entity_guid, input_json_file=None, input_json_string=None):
        '''
        associate traits with entities
        :param input_json_file: input.json
        :param input_json_string: json string
        :param entity_guid: guid of the entity to associate the trait with
        :return: json response
        '''
        url = cls.get_base_url() + '/api/atlas/entities/' + entity_guid + '/traits'
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        if input_json_string:
            json_data = input_json_string
        response, response_status = cls.http_put_post_request(url, json_data, 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def dis_associate_traits(cls, trait_name, entity_guid=None, type_name=None, qualified_name=None):
        '''
        remove associated trait
        :param trait_name : trait to disassociate
        :param entity_guid: entity guid to which trait is associated
        :param type_name : type_name of the entity : Ex : hive_table , hdfs_path, hbase_table, kafka_topic etc.,
        :param qualified_name : Qualified name of the entity
        :return: status code
        '''
        if entity_guid is None:
            if qualified_name is not None and type_name is not None:
                entity_guid = cls.get_GUID_of_entity(entityType=type_name, value=qualified_name, state="ACTIVE")
            else:
                return "Invalid Input. Either GUID or type_name with qualified_name must be provided"
        url = cls.get_base_url() + '/api/atlas/entities/' + entity_guid + '/traits/' + trait_name
        response = cls.http_delete_request(url)
        return response

    @classmethod
    @TaskReporter.report_test()
    def delete_entity(cls, guid):
        if guid is None:
            logger.error("Invalid input: GUID must be provided")
            return None
        url = cls.get_base_url() + '/api/atlas/entities?guid=' + guid
        status_code = cls.http_delete_request(url)
        if status_code == 200:
            return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def get_GUID_of_entity(cls, entityType, value="", state="ACTIVE"):
        '''
        get GUID of the entity with given qualified name
        :param entityType : type of the entity
        :param qualifiedName : Qualified Name of the entity
        :param state : state of the entity ACTIVE or DELETED
        :return: GUID of the entity
        '''
        response = cls.poll_till_entity_is_ingested_by_Atlas(entityType=entityType, value=value)
        entity_defs = response["entities"]
        guid = None
        for defn in entity_defs:
            entity_state = defn["status"]
            guid = defn["guid"]
            if entity_state == state:
                break
        if guid is None:
            raise ValueError('GUID is none. The entity is not generated in Atlas yet')
        else:
            return guid

    @classmethod
    @TaskReporter.report_test()
    def poll_till_entity_is_ingested_by_Atlas(cls, entityType, value, minutes=2):
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=minutes)
        response = None
        while datetime.datetime.now() < endTime:
            url = cls.get_base_url() + '/api/atlas/v2/search/basic?typeName=%s&query=%s' % (entityType, value)
            response, response_code = cls.http_get_request(url)
            assert response_code == 200
            logger.info(response)
            if "entities" in response:
                break
            else:
                logger.info("Entity is not yet found in Atlas . Sleeping for 5 secs (max %s minutes)", minutes)
                time.sleep(5)
                continue
        return response

    @classmethod
    @TaskReporter.report_test()
    def full_text_search(cls, query):
        '''
        :param query: query for the full text
        :return: map {guid,[qualifiedName,type]}
        '''
        url = cls.get_base_url() + '/api/atlas/discovery/search/fulltext?query=%s' % (query)
        response = cls.http_get_request(url)
        res = response[0]
        entities = res["results"]
        entity_info = {}
        for entity in entities:
            entity_info[entity["guid"]] = [entity["typeName"], cls.get_qualified_name(entity["guid"])]
        return entity_info

    @classmethod
    def get_entity_def(cls, guid):
        '''

        :param guid: GUID of the entity
        :return: JSON entity definition
        '''
        url = "%s/api/atlas/entities/%s" % (cls.get_base_url(), guid)
        return cls.http_get_request(url)

    @classmethod
    def get_entity_def_v2(cls, guid):
        '''

        :param guid: GUID of the entity
        :return: JSON entity definition
        '''
        url = "%s/api/atlas/v2/entity/guid/%s" % (cls.get_base_url(), guid)
        return cls.http_get_request(url)[0]

    @classmethod
    def check_status(cls, guid):
        '''
        check status of the entity with given GUID
        :param guid : GUID of the entity
        :return: status of the entity
        '''
        entityDef = cls.get_entity_def(guid)[0]
        return entityDef["definition"]["id"]["state"]

    @classmethod
    def get_qualified_name(cls, guid):
        '''

        :param guid: guid of entity
        :return: qualified name of the entity
        '''

        entityDef = cls.get_entity_def(guid)[0]
        return entityDef["definition"]["values"]["qualifiedName"]

    @classmethod
    @TaskReporter.report_test()
    def get_zookeeper_quorum(cls):
        retval = ""
        zknodes = Zookeeper.getZKHosts()
        zkport = Zookeeper.getZKPort()
        for host in zknodes:
            retval += host + ":" + str(zkport) + ","
        if retval != "":
            retval = retval[:-1]
        return retval

    @classmethod
    @TaskReporter.report_test()
    def get_database_credentials(cls, database_flavor):
        db_host, db_passwd, db_user = None, None, None
        if database_flavor == "mysql":
            db_user = "sqoop"
            db_passwd = "sqoop"
            db_host = Config.get('machine', 'MYSQL_HOST')
        elif database_flavor == "postgres":
            db_user = Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres')
            db_passwd = Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres')
            db_host = Config.get('machine', 'POSTGRES_HOST', default='postgres').strip()
        elif database_flavor == "db2":
            db_user = Config.get('machine', 'DB2_ROOT_USER', default='db2inst1')
            db_passwd = Config.get('machine', 'DB2_ROOT_PASSWD', default='db2inst1').strip()
            db_host = Config.get('machine', 'DB2_HOST', default='db2inst1').strip()
        return db_user, db_passwd, db_host

    @classmethod
    @TaskReporter.report_test()
    def restart_atlas_via_ambari(cls, clusterName, start_sleep_time=120):
        serviceHosts = Ambari.getServiceHosts("ATLAS", "ATLAS_SERVER", cluster=clusterName)
        for svcHost in serviceHosts:
            Ambari.stopComponent(svcHost, "ATLAS_SERVER")
        # Sleeping for 60 seconds to stop the server
        time.sleep(60)
        for svcHost in serviceHosts:
            Ambari.startComponent(svcHost, "ATLAS_SERVER")
        # Sleeping for 120 seconds to make the server up
        time.sleep(start_sleep_time)
        return 0

    @classmethod
    @TaskReporter.report_test()
    def create_hdfs_dir_for_sqoop(cls, HDFS_USER_DIR):
        logger.info("setting permission on %s", HDFS_USER_DIR)
        Hadoop.run("dfs -chmod 777 %s" % HDFS_USER_DIR)
        logger.info("change owner and the group for the directory %s", HDFS_USER_DIR)
        HDFS.chown(runasUser='hdfs', new_owner="hrt_qa:hdfs", directory=HDFS_USER_DIR, recursive=True)
        HDFS.chgrp(runasUser='hdfs', group='hdfs', directory=HDFS_USER_DIR, recursive=True)

    @classmethod
    @TaskReporter.report_test()
    def change_to_testng_marker_condition(cls, marker):
        testng_marker = ""
        markers = marker.split()
        for mark in markers:
            mark.strip()
            if mark == "(" or mark == ")":
                testng_marker += " " + mark + " "
            elif mark == "and":
                testng_marker += " &&"
            elif mark == "or":
                testng_marker += " ||"
            elif mark == "not":
                testng_marker += " !"
            else:
                testng_marker += " groups.containsKey( \"" + mark + "\" )"
        logger.info("TestNG specific marker condition is : %s", testng_marker)
        return testng_marker

    @classmethod
    @TaskReporter.report_test()
    def update_testng_suite_with_marker_condition(cls, marker, is_ui_test):
        import xml.dom.minidom
        import HTMLParser
        TEST_CODE_DIR = cls.test_code_dir

        htmlparser = HTMLParser.HTMLParser()
        doc = xml.dom.minidom.parse(TEST_CODE_DIR + '/src/test/resources/Testsuite.xml')
        suiteNode = doc.getElementsByTagName('suite')[0]

        if is_ui_test and not Machine.isIBMPower():
            testNode = suiteNode.getElementsByTagName('test')[0]
            suiteNode.removeChild(testNode)
            open(TEST_CODE_DIR + '/src/test/resources/Testsuite.xml', 'w').close()
            test = doc.createElement("test")
            test.setAttribute("name", "junitTestCases")
            test.setAttribute("junit", "true")
            packages = doc.createElement("packages")
            package = doc.createElement("package")
            package.setAttribute("name", "org.apache.atlas.regression.ui.tests")
            packages.appendChild(package)
            test.appendChild(packages)
            suiteNode.appendChild(test)

        # Creating listeners tag
        listeners = doc.createElement("listeners")
        listener = doc.createElement("listener")
        listener.setAttribute("class-name", "org.apache.atlas.regression.util.CustomAnnotationTransformer")
        listeners.appendChild(listener)
        suiteNode.appendChild(listeners)

        if marker and marker != "":
            # create method selectors tag
            method_selectors = doc.createElement("method-selectors")
            method_selector = doc.createElement("method-selector")
            script = doc.createElement("script")
            script.setAttribute("language", "beanshell")
            cdata = doc.createTextNode("<![CDATA[ " + marker + " ]]>")
            script.appendChild(cdata)
            method_selector.appendChild(script)
            method_selectors.appendChild(method_selector)
            testNode = doc.getElementsByTagName('test')[0]
            testNode.appendChild(method_selectors)

        with open(TEST_CODE_DIR + '/src/test/resources/Testsuite.xml', mode="w+") as fd:
            fd.write(htmlparser.unescape(doc.toprettyxml()))

    @classmethod
    @TaskReporter.report_test()
    def setup_atlas_knox_sso(cls):
        cls.WORKSPACE = Config.getEnv('WORKSPACE')
        if Config.get('knox', "ENABLE_KNOX_SSO", 'no') == 'no':
            # changing hosts file
            knoxsso.setup_Add_TLD()
            # setting 4 properties
            knoxsso.setupAtlasKnoxSSO()
            knoxsso.setup_KnoxSSO_form_module()
        atlas_prop_loc = os.path.join(
            cls.WORKSPACE, "tests", "atlas", "regression", "src", "test", "java", "org", "apache", "atlas",
            "regression", "util", "TestProperties.java"
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i -e 's/KNOX_AUTH.*/KNOX_AUTH=\"form\";/' %s" % atlas_prop_loc,
            host=Config.get('machine', 'GATEWAY'),
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        time.sleep(30)
        no_proxy = os.environ["no_proxy"]
        no_proxy = no_proxy + ",.site.com,.openstacklocal.com"
        os.environ["no_proxy"] = os.environ["no_proxy"] + no_proxy
        cls.is_atlas_knox_sso_enabled(reset=True)
        cls.get_knox_sso_provider_url(reset=True)
        cls.set_base_url(reset=True)

    @classmethod
    @TaskReporter.report_test()
    def setup_atlas_knox_sso_mvn_opts(cls, MAVEN_OPTS):
        MAVEN_OPTS['knox.host'] = Config.get('knox', 'KNOX_HOST').split(',')[0]
        MAVEN_OPTS['knox.sso.enabled'] = 'yes'
        MAVEN_OPTS['knox.auth'] = 'form'
        MAVEN_OPTS['knox.user.name'] = cls.KNOX_USERNAME
        MAVEN_OPTS['knox.user.password'] = cls.KNOX_PASSWORD
        return MAVEN_OPTS

    @classmethod
    @TaskReporter.report_test()
    def setup_atlas_knox_proxy(cls, MAVEN_OPTS):
        if Config.get('knox', "ENABLE_KNOX_PROXY", 'no') == 'no':
            knoxsso.setupKnoxProxyAtlas(
                scheme=cls.get_scheme(), atlas_host=cls.get_host(reset=True), atlas_port=cls.get_port()
            )
        MAVEN_OPTS['knox.host'] = Config.get('knox', 'KNOX_HOST').split(',')[0]
        MAVEN_OPTS['knox.proxy.enabled'] = 'yes'

    @classmethod
    @TaskReporter.report_test()
    def export_api(
            cls, items_to_export, output_file, username=ATLAS_LOGIN_NAME, password=ATLAS_LOGIN_PASSWORD, base_url=None
    ):
        if base_url:
            url = base_url + '/api/atlas/admin/export'
        else:
            url = cls.get_base_url() + '/api/atlas/admin/export'
        headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}
        auth = HTTPBasicAuth(username, password)
        if Hadoop.isSecure():
            try:
                from requests_kerberos import HTTPKerberosAuth, OPTIONAL
                auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
            except ImportError, _e:
                logging.error("requests_kerberos module is not present on this node.")

        response = requests.post(
            url=url,
            data=items_to_export,
            auth=auth,
            headers=headers,
            verify=False,
            stream=True,
            allow_redirects=False
        )
        status = response.status_code
        logger.info(url)
        if status == 200:
            # z = zipfile.ZipFile(io.BytesIO(response.content))
            # z.extractall(output_file)
            output = open(output_file, "w")
            output.write(response.content)
            output.close()
            logger.info("export zip is saved at %s", output_file)

        else:
            logger.info("Response code is : " + str(status) + ". So, nothing to export" + response.content)
            try:
                body = response.json()
                logger.info("Export error code is %s", str(body["errorCode"]))
                logger.info("Export error message is %s", body["errorMessage"])
            except ValueError:
                logger.info("No json body found in response")
        return response

    @classmethod
    @TaskReporter.report_test()
    def import_api(
            cls,
            data=None,
            request=None,
            base_url=None,
            username=ATLAS_LOGIN_NAME,
            password=ATLAS_LOGIN_PASSWORD,
            on_server=False
    ):
        base_path = base_url if base_url else cls.get_base_url()
        import_url = base_path + '/api/atlas/admin/import'
        auth = HTTPBasicAuth(username, password)
        if Hadoop.isSecure():
            try:
                from requests_kerberos import HTTPKerberosAuth, OPTIONAL
                auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
            except ImportError, _e:
                logging.error("requests_kerberos module is not present on this node.")
        if on_server:
            headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}
            import_url = base_path + '/api/atlas/admin/importfile'
            response = requests.post(
                url=import_url,
                data=request,
                auth=auth,
                verify=False,
                stream=True,
                headers=headers,
                allow_redirects=False
            )
        else:
            multipart_form_data = {}
            if data:
                multipart_form_data['data'] = open(data, "rb")
            if request:
                multipart_form_data['request'] = open(request, "r")
            response = requests.post(
                url=import_url, files=multipart_form_data, auth=auth, verify=False, stream=True, allow_redirects=False
            )

        status = response.status_code
        if status == 200:  # TODO check why 201
            print "Import successful with response code : " + str(status)
        else:
            print "Import failed with response code " + str(status)
            try:
                body = response.json()
                print "Import error code is " + str(body["errorCode"])
                print "Import error message is " + body["errorMessage"]
            except ValueError:
                print "No json body found in response"
        return response

    @classmethod
    @TaskReporter.report_test()
    def remove_atlas_knox_sso(cls):
        if cls.is_atlas_knox_sso_enabled():
            knoxsso.disableAtlasKnoxSSO()
            time.sleep(30)
            cls.is_atlas_knox_sso_enabled(reset=True)
            cls.get_knox_sso_provider_url(reset=True)
            cls.set_base_url(reset=True)
        else:
            logger.info("Atlas Knox SSO already disabled")

    @classmethod
    @TaskReporter.report_test()
    def remove_atlas_knox_sso_set_mvn_opts(cls, MAVEN_OPTS):
        MAVEN_OPTS['knox.host'] = None
        MAVEN_OPTS['knox.sso.enabled'] = 'no'
        MAVEN_OPTS['knox.auth'] = None
        return MAVEN_OPTS

    @classmethod
    def get_atlas_authentication_mode(cls):
        '''
        Get atlas authorization method. See http://atlas.apache.org/Authentication-Authorization.html
        :return:
        '''
        authorization_method = cls.ATLAS_CONFIGS_IN_AMBARI.get("atlas.authorizer.impl", "simple")
        logger.debug("Authorization method for atlas is %s", authorization_method)
        return authorization_method

    @classmethod
    @TaskReporter.report_test()
    def get_remote_policy_store_location(cls):
        '''
        Get file path for atlas.auth.policy.file
        :return:
        '''
        policy_file_location = cls.ATLAS_CONFIGS_IN_AMBARI.get(
            "atlas.auth.policy.file", "{{conf_dir}}/atlas-simple-authz-policy.json"
        )

        # If you know a better (ambari) way do detect actual value of {{config_dir}}, please fix this
        remote_policy_store = policy_file_location.replace("{{conf_dir}}", cls.get_conf_dir())
        logger.debug("Remote policy store: %s", remote_policy_store)
        return remote_policy_store

    @classmethod
    @TaskReporter.report_test()
    def update_atlas_simple_authz_policy(cls):
        '''
        Update atlas-simple-authz-policy.json file to include permissions for test user
        :return:None
        '''
        remote_policy_store = cls.get_remote_policy_store_location()
        local_path_policy_store = "/tmp/atlas-simple-authz-policy.json"

        hosts = Ambari.getServiceHosts(
            service="ATLAS", component="ATLAS_SERVER", cluster=cls.CLUSTER_NAME, is_enc=Ambari.is_ambari_encrypted()
        )

        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=hosts[0],
            srcpath=remote_policy_store,
            destpath=local_path_policy_store,
            passwd=Machine.getAdminPasswd()
        )

        # change permissions to modify file
        Machine.chmod('777', local_path_policy_store, user=Machine.getAdminUser())
        with open(local_path_policy_store, 'r') as jsonFile:
            data = json.load(jsonFile)

        role = dict()
        service = "ATLAS"
        role[cls.test_user] = ["ROLE_ADMIN"]
        data["userRoles"].update(role)

        logger.info("Updating atlas-simple-authz-policy.json for %s", cls.test_user)
        with open(local_path_policy_store, "w+") as jsonFile:
            jsonFile.write(json.dumps(data))

        no_of_clusters = int(Config.get("multicluster", "NUM_OF_CLUSTERS", 1))
        for i in range(no_of_clusters):
            gateway_str = "Gateway{}".format(str(i + 1))
            gateway = Config.get("multicluster", gateway_str)
            web_url = Ambari.getWebUrl(hostname=gateway)
            hosts = Ambari.getHostsForComponent("ATLAS_SERVER", weburl=web_url)

            for host in hosts:
                logger.debug("Modifying atlas-simple-authz-policy.json in host %s", host)
                # change permissions in each host
                Machine.chmod(
                    perm="777",
                    filepath=remote_policy_store,
                    recursive="False",
                    user=Machine.getAdminUser(),
                    host=host,
                    passwd=Machine.getAdminPasswd()
                )
                Machine.copyFromLocal(
                    user=Machine.getAdminUser(),
                    host=host,
                    srcpath=local_path_policy_store,
                    destpath=remote_policy_store,
                    passwd=Machine.getAdminPasswd()
                )
                # revert to old permissions in each host
                Machine.chmod(
                    perm="644",
                    filepath=remote_policy_store,
                    recursive="False",
                    user=Machine.getAdminUser(),
                    host=host,
                    passwd=Machine.getAdminPasswd()
                )

            logger.info("Adding policy for user %s and restarting Atlas", cls.test_user)
            logger.info("------------------------- Restarting %s -------------------------", service)
            logger.info("Stopping %s ...", service)
            Ambari.start_stop_service(
                service=service, state="INSTALLED", waitForCompletion=True, timeout=300, weburl=web_url
            )
            logger.info("Starting %s ...", service)
            Ambari.start_stop_service(
                service=service, state="STARTED", waitForCompletion=True, timeout=300, weburl=web_url
            )

        # revert to old permissions
        Machine.chmod(
            perm="644",
            filepath=local_path_policy_store,
            recursive="False",
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )

    @classmethod
    def send_messages_to_ATLAS_HOOK(cls, message):
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        return Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=message, user=Atlas.test_user
        )

    @classmethod
    @TaskReporter.report_test()
    def move_atlas_from_migration_to_active_mode(cls):
        logger.info("Moving atlas from migration to active state..")

        atlas_properties = Ambari.getConfig(service="ATLAS", type="application-properties")
        atlas_migration_path = atlas_properties.get('atlas.migration.data.filename', None)
        if not atlas_migration_path:
            logger.info("Atlas migration path is not set. So, not in migration mode, nothing to do..")
            return

        migration_host = None
        atlas_hosts = Ambari.getServiceHosts(service="ATLAS", component="ATLAS_SERVER")
        for host in atlas_hosts:
            if Machine.pathExists(user=Machine.getAdminUser(), host=host, filepath=atlas_migration_path,
                                  passwd=Machine.getAdminPasswd()):
                migration_host = host
                break
        if not migration_host:
            logger.info("None of the atlas hosts has migration path: %s", atlas_migration_path)
            return
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=10)
        scheme = 'https' if Atlas.ssl_enabled() else 'http'
        port = Atlas.get_port()
        url = 'api/atlas/admin/status'

        abs_url = "{}://{}:{}/{}".format(scheme, migration_host, port, url)
        while datetime.datetime.now() < end_time:
            response, response_code = Atlas.http_get_request(abs_url)
            assert response_code == 200
            logger.info(response)
            if response.get("status") == "ACTIVE":
                return
            if response.get("Status") == "MIGRATING":
                migration_status = response.get("MigrationStatus")
                logger.info("Migration status is : %s", migration_status.get("operationStatus"))
                if migration_status.get("operationStatus") == "SUCCESS":
                    Ambari.deleteConfig(type='application-properties', config={'atlas.migration.data.filename'})
                    Ambari.restart_service("ATLAS")
                    return
                elif migration_status.get("operationStatus") == "FAILED":
                    logger.info("Migration failed.")
                    return
                else:
                    logger.info("Waiting for migration to complete..")
                    time.sleep(30)
            else:
                logger.info("Atlas is not in migration mode")


class Atlas_model(object):
    def __init__(self):
        pass

    component_dir = "tests/resources/atlas_model_templatesatlas_model_templates"
    cluster_name = Atlas.CLUSTER_NAME if Atlas.CLUSTER_NAME else Ambari.getClusterName()

    @classmethod
    @TaskReporter.report_test()
    def get_basic_template(cls, model_template_file):
        basic_template_file = cls.component_dir + "/basic_template.json"
        jsonFile = open(basic_template_file, "r")
        basic_def = json.load(jsonFile)
        jsonFile.close()
        jsonFile = open(model_template_file, "r")
        model_def = json.load(jsonFile)
        jsonFile.close()
        return basic_def, model_def


class FS_model(Atlas_model):
    def __init__(self):
        Atlas_model.__init__(self)

    @classmethod
    @TaskReporter.report_test()
    def create_FS_path(cls, file_name=None, isFile=True, isSymLink=False):
        '''
        create an entity of type fs_path with given name
        if file_name is not given , creates a new file.
        :param fileName : fs file name (optional)
        :return: Qualified Name and GUID of the created entity
        '''
        rand = (''.join(choice(letters) for i in range(5)))
        if file_name is None:
            fs_file = "fs_file_%s" % (rand)
        else:
            fs_file = file_name
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        fs_file_qn = "%s@%s" % (fs_file, cls.cluster_name)
        fs_def_json = cls.__create_FS_entity_def(fs_file_qn, fs_file, isFile, isSymLink)
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=fs_def_json, user=Atlas.test_user
        )
        time.sleep(5)
        guid = Atlas.get_GUID_of_entity(entityType="fs_path", value=fs_file_qn, state="ACTIVE")
        return fs_file_qn, guid

    @classmethod
    def __create_FS_entity_def(cls, fs_file_qn, fs_file, isFile, isSymlink):
        '''
        create an entity definition for type hdfs_path from the template and return the entity definition JSON
        :param fs_file : file name
        :param fs_file_qn : fully qualified file name
        :param isFile : file or directory
        :param is a Symbolic link
        :return: entity definition (JSON) for the created entity
        '''
        # TODO determine the location of the beaver component
        _file = "%s/hdfs/fs_path_template.json" % (cls.component_dir)
        fs_def, kafka_message = cls.get_basic_template(_file)
        ##TODO Qualified name check
        kafka_message["values"]["qualifiedName"] = fs_file_qn
        kafka_message["values"]["path"] = fs_file_qn
        kafka_message["values"]["name"] = fs_file
        kafka_message["values"]["isFile"] = isFile
        kafka_message["values"]["isSymlink"] = isSymlink
        kafka_message["id"]["id"] = Utils.generate_random_guid()
        fs_def["message"]["entities"].append(kafka_message)
        return json.dumps(fs_def)


class HDFS_model(Atlas_model):
    def __init__(self):
        Atlas_model.__init__(self)

    @classmethod
    @TaskReporter.report_test()
    def create_HDFS_path(cls, file_name=None):
        '''
        create an entity of type hdfs_path with given name
        if file_name is not given , creates a new file.
        :param file_name : hdfs file name (optional)
        :return: Qualified Name and GUID of the created entity
        '''
        rand = (''.join(choice(letters) for i in range(5)))
        if file_name is None:
            hdfs_file = "hdfsFile_%s" % (rand)
        else:
            hdfs_file = file_name
        hdfsFileQn = hdfs_file
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        hdfsDefJSon = cls.__create_HDFS_entity_def(hdfs_file, hdfsFileQn)
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=hdfsDefJSon, user=Atlas.test_user
        )
        add_more_sleep_time = 0
        if Atlas.ssl_enabled():
            add_more_sleep_time = 10
        time.sleep(5 + add_more_sleep_time)
        guid = Atlas.get_GUID_of_entity(entityType="hdfs_path", value=hdfsFileQn)
        return hdfsFileQn, guid

    @classmethod
    def __create_HDFS_entity_def(cls, hdfs_file, hdfs_file_qn):
        '''
        create an entity definition for type hdfs_path from the template and return the entity definition JSON
        :param hdfs_file : file name
        :param hdfs_file_qn : fully qualified file name
        :return: entity definition (JSON) for the created entity
        '''
        #TODO determine the location of the beaver component

        _file = "%s/hdfs/hdfs_path_template.json" % (cls.component_dir)
        hdfsDef, kafkaMessage = cls.get_basic_template(_file)
        ##TODO Qualified name check
        kafkaMessage["values"]["qualifiedName"] = hdfs_file_qn
        kafkaMessage["values"]["path"] = hdfs_file_qn
        kafkaMessage["values"]["name"] = hdfs_file
        kafkaMessage["id"]["id"] = Utils.generate_random_guid()
        hdfsDef["message"]["entities"].append(kafkaMessage)
        return json.dumps(hdfsDef)


class Kafka_model(Atlas_model):
    def __init__(self):
        Atlas_model.__init__(self)

    @classmethod
    @TaskReporter.report_test()
    def create_kafka_model(cls, topic=None):
        '''

        :param topic: kafka topic to create
        :param partitions: num partitions
        :param replication_factor: count of replication factor
        :return: Qualified Name and Guid of the topic
        '''
        rand = (''.join(choice(letters) for i in range(5)))
        if topic is None:
            topic = "kafka_topic_%s" % (rand)
        qualified_name = "%s@%s" % (topic, cls.cluster_name)
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        #create the entity definiton for kafka topic
        kafkaTopicDef = cls.__create_Kafka_Topic_def(topic, qualified_name, cls.__get_zookeeper_quorum())
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=kafkaTopicDef, user=Atlas.test_user
        )
        time.sleep(5)
        guid = Atlas.get_GUID_of_entity(entityType="kafka_topic", value=qualified_name)
        return qualified_name, guid

    @classmethod
    def __create_Kafka_Topic_def(cls, topic, qualified_name, zk_quorum):
        '''

        :param topic: kafka topic name
        :param qualified_name: Qualified name of the topic
        :param zk_quorum: Zookeeper Quorum
        :return: Entity Definition of the topic
        '''
        # TODO determine the location of the beaver component

        _file = "%s/kafka/kafka_topic_template.json" % (cls.component_dir)
        kafkaDef, kafkaMessage = cls.get_basic_template(_file)
        ##TODO Qualified name check - cluster name check
        kafkaMessage["values"]["name"] = topic
        kafkaMessage["values"]["topic"] = topic
        kafkaMessage["values"]["qualifiedName"] = qualified_name
        kafkaMessage["id"]["id"] = Utils.generate_random_guid()
        kafkaMessage["values"]["uri"] = zk_quorum
        ##TODO create new file
        kafkaDef["message"]["entities"].append(kafkaMessage)
        return json.dumps(kafkaDef)

    @classmethod
    def __get_zookeeper_quorum(cls):
        retval = ""
        zknodes = Zookeeper.getZKHosts()
        zkport = Zookeeper.getZKPort()
        for host in zknodes:
            retval += host + ":" + str(zkport) + ","
        if retval != "":
            retval = retval[:-1]
        return retval


class Hbase_model(Atlas_model):
    def __init__(self):
        Atlas_model.__init__(self)

    class Hbase_table(object):
        def __init__(self):
            self.name = None
            self.guid = None
            self.col_family = []

    class HBaseColumnFamily(object):
        def __init__(self):
            self.name = None
            self.guid = None
            self.cols = []

    class HBaseColumn(object):
        def __init__(self):
            self.name = None
            self.guid = None

    @classmethod
    def __get_default_values(cls):
        rand = (''.join(choice(letters) for i in range(5)))
        hbase_table = "hbase_table_%s" % (rand)
        hbase_col_family_list = {'persData': ['name', 'age'], 'profData': ['companyid', 'designation']}
        return hbase_table, hbase_col_family_list

    @classmethod
    @TaskReporter.report_test()
    def create_Hbase_Model(cls, hbase_table=None, hbase_col_family_list=None):
        '''
        create an entity of type hbase table with given name and column family list
        :param hbase_table : hbase table name
        :param hbase_col_family_list : hbase column family list
        :return: hbase_table instance of type HBase_table
        '''
        defaultValues = cls.__get_default_values()
        if hbase_table is None:
            hbase_table = defaultValues[0]
        if hbase_col_family_list is None:
            hbase_col_family_list = defaultValues[1]
        hdfsDefJSon, hbase_table = cls.__create_HBase_entity_def(hbase_table, hbase_col_family_list)
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=hdfsDefJSon, user=Atlas.test_user
        )
        time.sleep(5)
        hbase_table.guid = Atlas.get_GUID_of_entity(entityType="hbase_table", value=hbase_table.name)
        colf_list = hbase_table.col_family
        for colf in colf_list:
            colf.guid = Atlas.get_GUID_of_entity(entityType="hbase_column_family", value=colf.name)
            cols = colf.cols
            for col in cols:
                col.guid = Atlas.get_GUID_of_entity(entityType="hbase_column", value=col.name)
        return hbase_table

    @classmethod
    def __create_HBase_entity_def(cls, hbaseTable, hbaseColfFamilyList):
        tableFile = cls.component_dir + "/hbase/hbase_table_template.json"
        hbaseDef, tableDef = cls.get_basic_template(tableFile)
        tableDef["values"]["name"] = hbaseTable
        tableDef["values"]["qualifiedName"] = hbaseTable + "@" + cls.cluster_name
        hbaseDef["message"]["entities"].append(tableDef)
        hbase_table_instance = cls.Hbase_table()
        hbase_table_instance.name = hbaseTable + "@" + cls.cluster_name
        col_family = hbase_table_instance.col_family
        for colFamily, columns in hbaseColfFamilyList.items():
            hbase_col_family_instance = cls.HBaseColumnFamily()
            colFile = cls.component_dir + "/hbase/hbase_column_family_template.json"
            jsonFile = open(colFile, "r")
            colFamilyDef = json.load(jsonFile)
            jsonFile.close()
            colFamilyDef["values"]["name"] = colFamily
            colFamilyDef["values"]["qualifiedName"] = hbaseTable + "." + colFamily + "@" + cls.cluster_name
            colFamilyDef["id"]["id"] = Utils.generate_random_guid()
            values = colFamilyDef["values"]
            values.update({"table": tableDef})
            hbaseDef["message"]["entities"].append(colFamilyDef)
            hbase_col_family_instance.name = hbaseTable + "." + colFamily + "@" + cls.cluster_name
            col_family.append(hbase_col_family_instance)
            cols = hbase_col_family_instance.cols
            # create definition for columns
            for column in columns:
                hbase_col_instance = cls.HBaseColumn()
                colFile = cls.component_dir + "/hbase/hbase_column_template.json"
                jsonFile = open(colFile, "r")
                colDef = json.load(jsonFile)
                jsonFile.close()
                colDef["values"]["name"] = column
                colDef["values"]["qualifiedName"] = \
                    hbaseTable + "." + colFamily + "." + column + "@" + cls.cluster_name
                hbase_col_instance.name = hbaseTable + "." + colFamily + "." + column + "@" + cls.cluster_name
                colDef["id"]["id"] = Utils.generate_random_guid()
                values = colDef["values"]
                values.update({"column_family": colFamilyDef})
                hbaseDef["message"]["entities"].append(colDef)
                cols.append(hbase_col_instance)
        return json.dumps(hbaseDef), hbase_table_instance


class Hive_model(Atlas_model):
    def __init__(self):
        Atlas_model.__init__(self)

    class Hive_table(object):
        def __init__(self):
            self.name = None
            self.guid = None
            self.columns = []

        class Hive_column(object):
            def __init__(self):
                self.name = None
                self.guid = None

    @classmethod
    def __create_hive_db_def(cls, hive_db):
        tempFile = "%s/hive/hive_database_template.json" % (cls.component_dir)
        kafkaDef, kafkaMessage = cls.get_basic_template(tempFile)
        qualifiedName = "%s@%s" % (hive_db, cls.cluster_name)
        ##TODO Qualified name check - cluster name check
        kafkaMessage["values"]["name"] = hive_db
        kafkaMessage["values"]["qualifiedName"] = qualifiedName
        kafkaMessage["id"]["id"] = Utils.generate_random_guid()
        kafkaDef["message"]["entities"].append(kafkaMessage)
        return json.dumps(kafkaMessage), json.dumps(kafkaDef)

    @classmethod
    def __create_hive_table_def(cls, hive_table, hive_columns, hive_db="default"):
        hive_db_def, hive_def = cls.__create_hive_db_def(hive_db)
        tempFile = "%s/hive/hive_table_template.json" % (cls.component_dir)
        jsonFile = open(tempFile, "r")
        table_def = json.load(jsonFile)
        jsonFile.close()
        table_qualified_name = "%s.%s@%s" % (hive_db, hive_table, cls.cluster_name)
        sd_qualified_name = "%s.%s@%s_storage" % (hive_db, hive_table, cls.cluster_name)
        ##TODO Qualified name check - cluster name check
        table_def["values"]["name"] = hive_table
        table_def["values"]["qualifiedName"] = table_qualified_name
        table_def["id"]["id"] = Utils.generate_random_guid()
        table_def["values"]["sd"]["values"]["qualifiedName"] = sd_qualified_name
        table_def["values"]["sd"]["values"]["table"] = table_def["id"]
        table_def["values"]["sd"]["id"]["id"] = Utils.generate_random_guid()
        table_def["values"].update({"db": json.loads(hive_db_def)})
        hive_table_instance = cls.Hive_table()
        hive_table_instance.name = table_qualified_name
        columns = hive_table_instance.columns
        for column, datatype in hive_columns.items():
            #create column definition
            tempFile = "%s/hive/hive_column_template.json" % (cls.component_dir)
            jsonFile = open(tempFile, "r")
            col_def = json.load(jsonFile)
            jsonFile.close()
            column_qualified_name = "%s.%s.%s@%s" % (hive_db, hive_table, column, cls.cluster_name)
            col_def["values"]["name"] = column
            col_def["values"]["qualifiedName"] = column_qualified_name
            col_def["values"]["table"] = table_def["id"]
            col_def["values"]["type"] = datatype
            col_def["id"]["id"] = Utils.generate_random_guid()
            #add the constructed column definition to table
            table_def["values"]["columns"].append(col_def)
            hive_column_instance = cls.Hive_table.Hive_column
            hive_column_instance.name = column_qualified_name
            columns.append(hive_column_instance)
        #add the table def to final json which contains hive database definition
        hive_def = json.loads(hive_def)
        hive_def["message"]["entities"].append(table_def)
        logger.debug(hive_def)
        return json.dumps(hive_def), hive_table_instance

    @classmethod
    @TaskReporter.report_test()
    def create_hive_table(cls, hive_table, hive_columns, hive_db="default"):
        '''

        :param hive_table: name of hive table
        :param hive_columns: dictionary of hive columns ex : {"id":"int","name":"string"}
        :param hive_db: optional
        :return: hive_table instance of type Hive_table
        '''
        table_qualified_name = "%s.%s@%s" % (hive_db, hive_table, cls.cluster_name)
        hive_def_json, hive_table = cls.__create_hive_table_def(hive_table, hive_columns, hive_db)
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=hive_def_json, user=Atlas.test_user
        )
        time.sleep(5)
        table_guid = Atlas.get_GUID_of_entity(entityType="hive_table", value=table_qualified_name)
        # TODO what to do if its DELETED
        if Atlas.check_status(table_guid) == "ACTIVE":
            hive_table.guid = table_guid
        for column in hive_table.columns:
            col_guid = Atlas.get_GUID_of_entity(entityType="hive_column", value=column.name)
            column.guid = col_guid
        return hive_table

    @classmethod
    @TaskReporter.report_test()
    def create_hive_db(cls, hive_db="default"):
        '''

        :param hive_db: name of hive_db optional parameter
        :return: Qualified name and guid of the table
        '''
        db_qualified_name = "%s@%s" % (hive_db, cls.cluster_name)
        hive_def_json = cls.__create_hive_db_def(hive_db)[1]
        kafka_brokers = Atlas.KAFKA_BROKERS if Atlas.KAFKA_BROKERS else Kafka.get_broker_list()
        Kafka.runConsoleProducer(
            topicName="ATLAS_HOOK", brokerlist=kafka_brokers, message=hive_def_json, user=Atlas.test_user
        )
        time.sleep(5)
        guid = Atlas.get_GUID_of_entity(entityType="hive_db", value=db_qualified_name)
        return db_qualified_name, guid


class Utils(object):
    def __init__(self):
        pass

    @classmethod
    def generate_random_guid(cls):
        rand = ''.join(str(random.randint(0, 9)) for _ in xrange(12))
        return "-" + str(rand)

    @classmethod
    @TaskReporter.report_test()
    def construct_tag_def(cls, tag_name, attrib_map=None, description=None, parent_tag=None, entity_types=None):
        temp_file = "tests/resources/atlas_model_templates/trait/create_tag.json"
        tag_def = json.loads(open(temp_file).read())
        tag_def["classificationDefs"][0]["name"] = tag_name
        tag_def["classificationDefs"][0]["guid"] = Utils.generate_random_guid()
        if description:
            tag_def["classificationDefs"][0]["description"] = description
        if entity_types:
            tag_def["classificationDefs"][0]["entityTypes"] = entity_types
        if parent_tag:
            if isinstance(parent_tag, list):
                tag_def["classificationDefs"][0]["superTypes"].extend(parent_tag)
            else:
                tag_def["classificationDefs"][0]["superTypes"].append(parent_tag)
        if attrib_map:
            logger.info("adding attributes")
            for attribute, datatype in attrib_map.items():
                # create column definition
                temp_file = "tests/resources/atlas_model_templates/trait/tag_attribute.json"
                json_file = open(temp_file, "r")
                attr_def = json.load(json_file)
                json_file.close()
                attr_def["name"] = attribute
                attr_def["typeName"] = datatype
                # add the constructed attribute definition to tag
                tag_def["classificationDefs"][0]["attributeDefs"].append(attr_def)
        return json.dumps(tag_def)
