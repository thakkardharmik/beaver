#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

from beaver.component.ambari import Ambari
import logging, requests, json, time, threading, decorator
from beaver.component.hadoop import Hadoop, HDFS
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.beacon.beaconHive import BeaconHive
from beaver.component.hive import Hive
from datetime import datetime
import re, os
import sys
from beaver.component.xa import Xa
from beaver.component.beacon.beaconRanger import BeaconRanger

logger = logging.getLogger(__name__)
job_user = Config.getEnv('USER')
hdfs_user = HDFS.getHDFSUser()

_ambari_host_1 = Config.get("multicluster", "AMBARI_GATEWAY1")
_ambari_host_2 = Config.get("multicluster", "AMBARI_GATEWAY2")

session = requests.session()
session.auth = ("admin", "admin")
session.headers['Accept'] = 'application/json'

source_weburl = Ambari.getWebUrl(hostname=_ambari_host_1)
target_weburl = Ambari.getWebUrl(hostname=_ambari_host_2)
source_datacenter = target_datacenter = "default"

primaryCluster = source_datacenter + "$" + Ambari.getClusterName(weburl=source_weburl)
backupCluster = target_datacenter + "$" + Ambari.getClusterName(weburl=target_weburl)


class Beacon:

    _source_cookie = None
    _target_cookie = None
    _knox_username = ""
    _knox_password = ""

    def __init__(self):
        pass

    @classmethod
    def cleanConfigStore(cls):
        # read it from config file
        matcher = re.compile('jdbc:mysql:\/\/(.*)\/')
        location = Ambari.getConfig(service="BEACON", type="beacon-env")['beacon_store_url']
        host = matcher.match(location).group(1)
        logger.info("beacondb db source host is {host}".format(host=host))

        # clean up local db
        Machine.runas('root', 'mysql -e \'drop database beacondb\'', host)

        location = Ambari.getConfig(service="BEACON", type="beacon-env", webURL=target_weburl)['beacon_store_url']
        host = matcher.match(location).group(1)
        logger.info("beacondb db target host is {host}".format(host=host))

        # clean up remote db
        Machine.runas('root', 'mysql -e \'drop database beacondb\'', host)

    @classmethod
    def getBeaconStatus(cls, beacon_url=None):
        try:
            if beacon_url is None:
                beacon_url = cls.getBeaconURL()

            if Beacon.isBeaconSSOEnabled() == "true":
                BeaconKnoxSSO.setCookie(beacon_url)
                logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

            statusAPI = "/api/beacon/admin/status"
            execute_query = beacon_url + statusAPI
            logger.info("Executing: " + execute_query)
            response = session.get(execute_query)
            Beacon.formatjson(response)
            logger.info("Get beacon status response: " + response._content)
            return response.json()['status']
        except:
            logger.error("get Beacon status API failed with error:" + sys.exc_info()[0])
            return "STOPPED"

    @classmethod
    def checkBeaconStatusRestart(cls, beacon_url=None):
        if beacon_url is None:
            logger.info("Beacon url is None, Checking source beacon status")
            beacon_url = cls.getBeaconURL()
        status = Beacon.getBeaconStatus(beacon_url=beacon_url)
        if status != 'RUNNING':
            logger.info("Beacon server is not running on: " + beacon_url + ", Restarting now.")
            Beacon.restartBeaconServer(beacon_url=beacon_url)
            status = Beacon.getBeaconStatus(beacon_url=beacon_url)
            if status != 'RUNNING':
                logger.error("Failed to Start Beacon server on: " + beacon_url)
                raise Exception('Failed to Start Beacon server on: ' + beacon_url)
        return status

    @classmethod
    def restartBeacon(cls):
        # restart the local beacon

        source_restart = threading.Thread(target=cls.restartBeaconServer, args=(source_weburl, ))
        target_restart = threading.Thread(target=cls.restartBeaconServer, args=(target_weburl, ))

        source_restart.start()
        target_restart.start()

        source_restart.join()
        target_restart.join()

        time.sleep(15)

    @classmethod
    def restartBeaconServer(cls, beacon_url=None):

        # restart the local beacon
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()
        cls.stopBeaconServer(beacon_url=beacon_url)
        cls.startBeaconServer(beacon_url=beacon_url)

        time.sleep(30)

    @classmethod
    def stopBeaconServer(cls, beacon_url=None):

        # restart the local beacon
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()
        clusterName = Ambari.getClusterName(weburl=beacon_url)

        hosts = Ambari.getHostsForComponent(component="BEACON_SERVER", cluster=clusterName, weburl=beacon_url)

        for host in hosts:
            Ambari.stopComponent(
                host=host, component="BEACON_SERVER", cluster=clusterName, waitForCompletion=True, weburl=beacon_url
            )
        time.sleep(30)

    @classmethod
    def startBeaconServer(cls, beacon_url=None):

        # restart the local beacon
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()
        clusterName = Ambari.getClusterName(weburl=beacon_url)

        hosts = Ambari.getHostsForComponent(component="BEACON_SERVER", cluster=clusterName, weburl=beacon_url)

        for host in hosts:
            Ambari.startComponent(
                host=host, component="BEACON_SERVER", cluster=clusterName, waitForCompletion=True, weburl=beacon_url
            )
        time.sleep(30)

    @classmethod
    def getTargetBeaconServer(cls):
        """
        :param clusterName: Ambari Cluster Name to find the Beacon Server
        :return: beacon Server Host Name
        """
        host_list = Ambari.getHostsForComponent(
            component="BEACON_SERVER", cluster=Ambari.getClusterName(weburl=target_weburl), weburl=target_weburl
        )
        if not host_list:
            logger.info("Beacon Server is not installed")
            return None
        else:
            logger.info("Source Beacon Server: " + host_list[0])
            return host_list[0]

    @classmethod
    def getSourceBeaconServer(cls):
        """
        :param clusterName: Ambari Cluster Name to find the Beacon Server
        :return: beacon Server Host Name
        """
        host_list = Ambari.getHostsForComponent(
            component="BEACON_SERVER", cluster=Ambari.getClusterName(weburl=source_weburl), weburl=source_weburl
        )
        if not host_list:
            logger.info("Beacon Server is not installed")
            return None
        else:
            logger.info("Source Beacon Server: " + host_list[0])
            return host_list[0]

    @classmethod
    def getBeaconServerPort(cls):
        """

        :param clusterName: Ambari Cluster Name to find the Beacon Server
        :return: beacon Server Port Name
        """
        beacon_port = Ambari.getConfig(service="BEACON", type="beacon-env")['beacon_port']
        logger.info("beacon port is %s" % beacon_port)
        return beacon_port

    @classmethod
    def getBeaconServerPort(cls, webURL=None):
        """

        :param clusterName: Ambari Cluster Name to find the Beacon Server
        :return: beacon Server Port Name
        """
        if webURL == None:
            beacon_port = Ambari.getConfig(service="BEACON", type="beacon-env")['beacon_port']
            logger.info("beacon port is %s" % beacon_port)
        else:
            beacon_port = Ambari.getConfig(service="BEACON", type="beacon-env", webURL=webURL)['beacon_port']
            logger.info("beacon port is %s" % beacon_port)
        return beacon_port

    @classmethod
    def getBeaconURL(cls, source=False, webURL=None):
        if source is False:
            beacon_url = "http://" + cls.getTargetBeaconServer() + ":" + cls.getBeaconServerPort(webURL=webURL)
        else:
            beacon_url = "http://" + cls.getSourceBeaconServer() + ":" + cls.getBeaconServerPort()
        return beacon_url

    @classmethod
    def getBeaconURLKnox(cls, source=False, webURL=None):
        if source is False:
            beacon_url = "http://" + cls.getTargetBeaconServer() + ".com:" + cls.getBeaconServerPort(webURL=webURL)
        else:
            beacon_url = "http://" + cls.getSourceBeaconServer() + ".com:" + cls.getBeaconServerPort()
        return beacon_url

    @classmethod
    def getErrorMessage(cls, response):
        content = response.json()
        if 'message' in content:
            return content['message']
        else:
            return None

    @classmethod
    def formatjson(cls, response, sort=True, indents=4):
        """

        :param response:
        :param sort:
        :param indents:
        :return:
        """
        logger.info("Response:")
        if response.status_code == 200:
            content = response.json()
            logger.info(json.dumps(content, indent=indents, sort_keys=sort))
        else:
            logger.info(response._content)

    @classmethod
    def isBeaconSSOEnabled(cls):
        beacon_sso_knox_authentication_enabled = Ambari.getConfig(
            service="BEACON", type="beacon-security-site", webURL=source_weburl
        )['beacon.sso.knox.authentication.enabled']
        return beacon_sso_knox_authentication_enabled

    @classmethod
    def doGetWithBeaconSession(cls, url=None, beacon_url=source_weburl):
        if url is None:
            logger.error("URL cannot be None")
            raise Exception("URL cannot be None")
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        execute_query = url
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        logger.info("Get beacon status response: " + response._content)
        return response


class BeaconKnoxSSO(Beacon):
    @classmethod
    def getSourceCookie(cls):

        if Beacon._source_cookie is None:
            no_proxy = os.environ["no_proxy"]
            no_proxy = no_proxy + ",.site.com,.openstacklocal.com"
            os.environ["no_proxy"] = os.environ["no_proxy"] + no_proxy
            source_beacon_url = Beacon.getBeaconURLKnox(source=True)

            source_beacon_sso_knox_providerurl = Ambari.getConfig(
                service="BEACON", type="beacon-security-site", webURL=source_weburl
            )['beacon.sso.knox.providerurl']

            source_beacon_sso_knox_query_param_originalurl = Ambari.getConfig(
                service="BEACON", type="beacon-security-site", webURL=source_weburl
            )['beacon.sso.knox.query.param.originalurl']

            source_knox_beacon_url = source_beacon_sso_knox_providerurl + "?"  + source_beacon_sso_knox_query_param_originalurl + "=" + source_beacon_url \
                               + "/api/beacon/cluster/list"

            logger.info("Executing: " + source_knox_beacon_url)
            response = session.get(source_knox_beacon_url)
            Beacon.formatjson(response)
            Beacon._source_cookie = session.cookies.get_dict().get("hadoop-jwt")
            logger.info("Source Cookie : " + Beacon._source_cookie)

        return Beacon._source_cookie

    @classmethod
    def getTargetCookie(cls):

        if Beacon._target_cookie is None:
            no_proxy = os.environ["no_proxy"]
            no_proxy = no_proxy + ",.site.com,.openstacklocal.com"
            os.environ["no_proxy"] = os.environ["no_proxy"] + no_proxy
            target_beacon_url = Beacon.getBeaconURLKnox(webURL=target_weburl)

            target_beacon_sso_knox_providerurl = Ambari.getConfig(
                service="BEACON", type="beacon-security-site", webURL=target_weburl
            )['beacon.sso.knox.providerurl']

            target_beacon_sso_knox_query_param_originalurl = Ambari.getConfig(
                service="BEACON", type="beacon-security-site", webURL=target_weburl
            )['beacon.sso.knox.query.param.originalurl']

            target_knox_beacon_url = target_beacon_sso_knox_providerurl + "?"  + target_beacon_sso_knox_query_param_originalurl + "=" + target_beacon_url \
                               + "/api/beacon/cluster/list"

            logger.info("Executing: " + target_knox_beacon_url)
            response = session.get(target_knox_beacon_url)
            Beacon.formatjson(response)

            Beacon._target_cookie = session.cookies.get_dict().get("hadoop-jwt")
            logger.info("Target Cookie : " + Beacon._target_cookie)

        return Beacon._target_cookie

    @classmethod
    def getKnoxUsername(cls):
        dp_use_test_ldap = Config.get('dataplane', 'DP_USE_TEST_LDAP')
        if cls.isBeaconSSOEnabled() == "true" and dp_use_test_ldap == "yes":
            cls._knox_username = "admin"
        elif cls.isBeaconSSOEnabled() and dp_use_test_ldap == "no":
            cls._knox_username = "admin1"
        return cls._knox_username

    @classmethod
    def getKnoxPassword(cls):
        dp_use_test_ldap = Config.get('dataplane', 'DP_USE_TEST_LDAP')
        if cls.isBeaconSSOEnabled() == "true" and dp_use_test_ldap == "yes":
            cls._knox_password = "admin-password"
        elif cls.isBeaconSSOEnabled() and dp_use_test_ldap == "no":
            cls._knox_password = "Horton!#works"
        return cls._knox_password

    @classmethod
    def setCookie(cls, beacon_url):
        session.auth = (cls.getKnoxUsername(), cls.getKnoxPassword())
        session.verify = False
        session.allow_redirects = False
        session.cookies.clear_session_cookies()

        if beacon_url == Beacon.getBeaconURL(source=True):
            logger.info("Source Cookie " + cls.getSourceCookie())
            session.cookies.set('hadoop-jwt', cls.getSourceCookie())
        else:
            logger.info("Target Cookie " + cls.getTargetCookie())
            session.cookies.set('hadoop-jwt', cls.getTargetCookie())


class Cluster(Beacon):
    def __init__(self):
        Beacon.__init__(self)

    @classmethod
    def submit(
            cls,
            clusterName,
            clusterDesc,
            fsEndpoint,
            beacon_endpoint,
            hive_endpoint,
            local,
            beacon_url=None,
            ranger_configs=None,
            source=False,
            haConfigs=None
    ):
        """

        :param clusterName: Name of the Source/DR Cluster
        :param fsEndpoint: Value of the fs.defaultFS
        :param clusterDesc: Description of the Source/DR Cluster
        :param beacon_endpoint: Beacon Server URL
        :return:
        """

        cluster_parameters =  "fsEndpoint=" + fsEndpoint + "\n"\
                              "hsEndpoint=" + hive_endpoint + "\n"\
                              "local=" + local + "\n"\
                              "description=" + clusterDesc + "\n"\
                              "beaconEndpoint=" + beacon_endpoint
        if Hadoop.isSecure():
            cluster_parameters = cluster_parameters  + "\n"\
                                 "dfs.namenode.kerberos.principal=" + HDFS.getNameNodePrincipal() + "\n" \
                                 "hive.server2.authentication.kerberos.principal=" + Hive.getConfigValue("hive.server2.authentication.kerberos.principal")

        if haConfigs != None:
            nameService = haConfigs["dfs_internal_nameservices_primary"]
            cluster_parameters = cluster_parameters  + "\n" \
            "dfs.nameservices=" + haConfigs["dfs_internal_nameservices_primary"] + "\n" \
            "dfs.ha.namenodes." + nameService + "=" + haConfigs["dfs_ha_namenodes_primary"] + "\n" \
            "dfs.namenode.rpc-address." + nameService + ".nn1=" + haConfigs["dfs_namenode_rpc_address_primary_nn1"] + "\n" \
            "dfs.namenode.rpc-address." + nameService +".nn2=" + haConfigs["dfs_namenode_rpc_address_primary_nn2"]

        if ranger_configs is not None:
            cluster_parameters = cluster_parameters + "\n" \
                                "rangerEndpoint="+ ranger_configs["ranger_url"] + "\n" \
                                "rangerHDFSServiceName="+ ranger_configs["hadoop_repo"] + "\n" \
                                "rangerHIVEServiceName="+ ranger_configs["hive_repo"] + "\n"

        submitAPI = "/api/beacon/cluster/submit/" + clusterName

        if beacon_url is None:
            beacon_url = cls.getBeaconURL(source=source)

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        execute_query = beacon_url + submitAPI
        logger.info("Executing: " + execute_query)
        logger.info("Body:\n" + cluster_parameters)
        logger.info(cluster_parameters)
        response = session.post(execute_query, data=cluster_parameters)
        Beacon.formatjson(response)
        return response

    @classmethod
    def list(cls, beacon_url=None):
        """

        :param beacon_url: Beacon Server URL
        :return:
        """
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        listAPI = "/api/beacon/cluster/list"
        execute_query = beacon_url + listAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def getStatus(cls, clusterName, beacon_url=None):
        """

        :param clusterName:
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        getStatusAPI = "/api/beacon/cluster/status/" + clusterName
        execute_query = beacon_url + getStatusAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def getDefinition(cls, clusterName, beacon_url=None):
        """

        :param clusterName:
        :param beacon_url: Beacon Server URL
        :return:
        """
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        getAPI = "/api/beacon/cluster/getEntity/" + clusterName
        execute_query = beacon_url + getAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def delete(cls, clusterName, beacon_url=None):
        """

        :param clusterName:
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        deleteAPI = "/api/beacon/cluster/delete/" + clusterName
        execute_query = beacon_url + deleteAPI
        logger.info("Executing: " + execute_query)
        response = session.delete(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def pair(cls, remoteClusterName, beacon_url=None):
        """

        :param remoteBeaconEndpoint:
        :param remoteClusterName:
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL(source=True)

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        pairAPI = "/api/beacon/cluster/pair?"
        DRClusterName = "remoteClusterName=" + remoteClusterName
        execute_query = beacon_url + pairAPI + DRClusterName
        logger.info("Executing: " + execute_query)
        response = session.post(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def alreadyRegistered(cls, response):
        message = Beacon.getErrorMessage(response)
        logger.info('Beacon error message were: {error_message}'.format(error_message=message))
        return 'already registered' in message or 'already exists' in message

    @classmethod
    def pairClusters(cls, source_cluster, target_cluster):
        source_weburl = Ambari.getWebUrl(hostname=source_cluster)
        target_weburl = Ambari.getWebUrl(hostname=target_cluster)

        source_hdfsConfig = Ambari.getConfig(type='core-site', service='HDFS', webURL=source_weburl)
        target_hdfsConfig = Ambari.getConfig(type='core-site', service='HDFS', webURL=target_weburl)

        source_hdfsSite = Ambari.getConfig(type='hdfs-site', service='HDFS', webURL=source_weburl)
        target_hdfsSite = Ambari.getConfig(type='hdfs-site', service='HDFS', webURL=target_weburl)
        if Xa.isArgusInstalled():
            BeaconRanger.disableOrEnableenableDenyAndExceptionsInPolicies(['hdfs', 'hive'])
        primaryClusterDesc = "Primary Cluster"
        source_hdfsEndpoint = source_hdfsConfig['fs.defaultFS']
        source_beaconEndpoint = Beacon.getBeaconURL(source=True)
        logger.info("source beacon endpoint is %s" % source_beaconEndpoint)
        source_hive_endpoint = BeaconHive.getHiveServer2UrlForDynamicServiceDiscovery(source_weburl)
        logger.info("Source Hive Endpoint" + source_hive_endpoint)
        primaryHAconfigs = None
        source_ranger_configs = BeaconRanger.getRangerConfigs(ambariWeburl=source_weburl)

        if HDFS.isHAEnabled():
            primaryHAconfigs = {}
            dfs_internal_nameservices_primary = source_hdfsSite['dfs.internal.nameservices']
            primaryHAconfigs["dfs_internal_nameservices_primary"] = dfs_internal_nameservices_primary
            primaryHAconfigs["dfs_ha_namenodes_primary"] = source_hdfsSite['dfs.ha.namenodes.'
                                                                           + dfs_internal_nameservices_primary]
            primaryHAconfigs["dfs_namenode_rpc_address_primary_nn1"
                             ] = source_hdfsSite['dfs.namenode.rpc-address.' + dfs_internal_nameservices_primary +
                                                 '.nn1']
            primaryHAconfigs["dfs_namenode_rpc_address_primary_nn2"
                             ] = source_hdfsSite['dfs.namenode.rpc-address.' + dfs_internal_nameservices_primary +
                                                 '.nn2']

        backupClusterDesc = "Backup Cluster"
        target_hdfsEndpoint = target_hdfsConfig['fs.defaultFS']
        target_beaconEndpoint = Beacon.getBeaconURL(webURL=target_weburl)
        logger.info("target beacon endpoint is %s" % target_beaconEndpoint)
        target_hive_endpoint = BeaconHive.getHiveServer2UrlForDynamicServiceDiscovery(target_weburl)
        logger.info("Target Hive Endpoint" + target_hive_endpoint)
        target_ranger_configs = BeaconRanger.getRangerConfigs(ambariWeburl=target_weburl)
        if Xa.isArgusInstalled() is True:
            logger.info(
                "Source Ranger Endpoint: " + source_ranger_configs["ranger_url"] +
                " ,Source ranger hdfs service name: " + source_ranger_configs["hadoop_repo"] +
                " Source ranger hive service name: " + source_ranger_configs["hive_repo"]
            )
            logger.info(
                "Target Ranger Endpoint: " + target_ranger_configs["ranger_url"] +
                " ,target ranger hdfs service name: " + target_ranger_configs["hadoop_repo"] +
                " target ranger hive service name: " + target_ranger_configs["hive_repo"]
            )
        secondaryHAconfigs = None

        if HDFS.isHAEnabled():
            secondaryHAconfigs = {}
            dfs_internal_nameservices_primary = target_hdfsSite['dfs.internal.nameservices']
            secondaryHAconfigs["dfs_internal_nameservices_primary"] = dfs_internal_nameservices_primary
            secondaryHAconfigs["dfs_ha_namenodes_primary"] = target_hdfsSite['dfs.ha.namenodes.'
                                                                             + dfs_internal_nameservices_primary]
            secondaryHAconfigs["dfs_namenode_rpc_address_primary_nn1"
                               ] = target_hdfsSite['dfs.namenode.rpc-address.' + dfs_internal_nameservices_primary +
                                                   '.nn1']
            secondaryHAconfigs["dfs_namenode_rpc_address_primary_nn2"
                               ] = target_hdfsSite['dfs.namenode.rpc-address.' + dfs_internal_nameservices_primary +
                                                   '.nn2']

        # send primary to primary
        response = Cluster.submit(
            primaryCluster,
            primaryClusterDesc,
            source_hdfsEndpoint,
            source_beaconEndpoint,
            source_hive_endpoint,
            ranger_configs=source_ranger_configs,
            source=True,
            local="true",
            haConfigs=primaryHAconfigs
        )
        assert response.status_code == 200 or cls.alreadyRegistered(response)

        # send primary to secondary
        response = Cluster.submit(
            primaryCluster,
            primaryClusterDesc,
            source_hdfsEndpoint,
            source_beaconEndpoint,
            source_hive_endpoint,
            ranger_configs=source_ranger_configs,
            local="false",
            haConfigs=primaryHAconfigs
        )
        assert response.status_code == 200 or cls.alreadyRegistered(response)

        # send secondary to primary
        response = Cluster.submit(
            backupCluster,
            backupClusterDesc,
            target_hdfsEndpoint,
            target_beaconEndpoint,
            target_hive_endpoint,
            ranger_configs=target_ranger_configs,
            source=True,
            local="false",
            haConfigs=secondaryHAconfigs
        )
        assert response.status_code == 200 or cls.alreadyRegistered(response)

        # send secondary to secondary
        response = Cluster.submit(
            backupCluster,
            backupClusterDesc,
            target_hdfsEndpoint,
            target_beaconEndpoint,
            target_hive_endpoint,
            ranger_configs=target_ranger_configs,
            local="true",
            haConfigs=secondaryHAconfigs
        )
        assert response.status_code == 200 or cls.alreadyRegistered(response)

        response = Cluster.pair(backupCluster)
        assert response.status_code == 200
        Cluster.verify_pairing(primaryCluster, backupCluster)

    @classmethod
    def verify_pairing(cls, primaryclusterName, DRclusterName):
        primaryEntityDef = Cluster.getDefinition(primaryclusterName).json()
        assert primaryEntityDef['peers'] == DRclusterName
        DREntityDef = Cluster.getDefinition(DRclusterName).json()
        assert DREntityDef['peers'] == primaryclusterName


class Admin(Beacon):
    VERSION_API = "/api/beacon/admin/version"

    @classmethod
    def version(cls, beacon_url=None):
        if beacon_url is None:
            beacon_url = Beacon.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        execute_query = beacon_url + cls.VERSION_API
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response.json()

    @classmethod
    def getVersion(cls):
        beacon_version_response = Admin.version()
        return beacon_version_response['version']


class Logs(Beacon):
    API_URL = '/api/beacon/logs'

    @classmethod
    def submit(cls, params, beacon_url=None):
        if beacon_url is None:
            beacon_url = Beacon.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        execute_query = beacon_url + cls.API_URL
        logger.info("Executing: " + execute_query + "   - PARAMS:" + str(params))
        response = session.get(execute_query, params=params)
        Beacon.formatjson(response)
        return response.json()


class EventEntityType:
    SYSTEM = 'system'
    CLUSTER = 'cluster'
    POLICY = 'policy'
    POLICYINSTANCE = 'policyinstance'


class EventName:
    STARTED = "started"
    STOPPED = "stopped"
    SUBMITTED = "submitted"
    DELETED = "deleted"
    PAIRED = "paired"
    UNPAIRED = "unpaired"
    SYNCED = "synced"
    SCHEDULED = "scheduled"
    SUCCEEDED = "succeeded"
    SUSPENDED = "suspend"
    RESUMED = "resumed"
    FAILED = "failed"
    IGNORED = "ignored"
    KILLED = "killed"


class Events(Beacon):
    @classmethod
    def internalGetEvents(cls, beacon_url=None, apiUrl=None, params=None):
        if beacon_url is None:
            beacon_url = cls.getBeaconURL() + "/"

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        execute_query = beacon_url + apiUrl
        logger.info("Executing: " + execute_query)
        if params is None:
            params = {}
        #this is workaround for the BUG-81408
        if 'start' not in params:
            params['start'] = '1970-01-01T13:23:07.82'
        if 'numResults' not in params:
            params['numResults'] = '1000'
        logger.info("query params:\n" + str(params))
        response = session.get(execute_query, params=params)
        cls.formatjson(response)
        return response

    @classmethod
    def getEventsByPolicyName(cls, beacon_url=None, params=None, policyName=None):
        return cls.internalGetEvents(None, "api/beacon/events/policy/" + policyName, params)

    @classmethod
    def getEventsByEventName(cls, beacon_url=None, params=None, eventName=None, eventEntityType=None):
        logger.info(
            "getEventsByEventName events/ params:%s, eventName:%s, eventEntityType:%s" %
            (str(params), str(eventName), str(eventEntityType))
        )

        if eventEntityType:
            if params:
                params['eventEntityType'] = eventEntityType
            else:
                params = {'eventEntityType': eventEntityType}
        return cls.internalGetEvents(None, "api/beacon/events/" + eventName, params)

    @classmethod
    def getEventsByEntityType(cls, beacon_url=None, params=None, entityType=None):
        return cls.internalGetEvents(None, "api/beacon/events/entity/" + entityType, params)

    @classmethod
    def getAllEvents(cls, beacon_url=None, params=None):
        return cls.internalGetEvents(None, "api/beacon/events/all", params)

    @classmethod
    def getEventsByInstanceId(cls, beacon_url=None, instanceId=None):
        params = {'instanceId': instanceId}
        return cls.internalGetEvents(None, "api/beacon/events/instance", params)

    @classmethod
    def getNumberofEvents(cls, response):
        if response.status_code == 200:
            content = response.json()
            if 'totalCount' in content:
                return content['totalCount']
            elif 'events' in content:
                return len(content['events'])
        return 0

    @classmethod
    def getLatestEvent(cls, response):
        if response.status_code == 200:
            content = response.json()
            events = content['events']
            dateTimeFormat = "%Y-%m-%d %H:%M:%S.%f"

            def keyfunc(item):
                return datetime.strptime(item['timestamp'], dateTimeFormat)

            return sorted(events, key=keyfunc, reverse=True)[0]
        else:
            return None


def generate_decorator(negation=False, diff=1, getevent_func=None, decorator_kwargs=None):
    def mydecorator(func):
        def func_wrapper(func, *args, **kwargs):
            logger.info("event decorator args:%s" % str(args))
            logger.info("event decorator kwargs:%s" % str(kwargs))
            logger.info("getting number of events/ func:%s, args:%s" % (func.__name__, str(decorator_kwargs)))
            num = Events.getNumberofEvents(getevent_func(**decorator_kwargs))
            logger.info("number of events/ before %s: %i" % (func.__name__, num))
            result = func(*args, **kwargs)
            num2 = Events.getNumberofEvents(getevent_func(**decorator_kwargs))
            logger.info(
                "number of events/ after %s: %i; events before:%i expected diff:%i" % (func.__name__, num2, num, diff)
            )
            if negation:
                assert not num2 >= num + diff
            else:
                assert num2 >= num + diff
            return result

        #wrapper will get the name of the wrapped func, this is needed for py.test setup_function
        func_wrapper.__name__ = func.__name__
        return decorator.decorator(func_wrapper, func)

    return mydecorator


def generate_decorator_with_type_arg(negation=False, diff=1, getevent_func=None, decorator_kwargs=None):
    def mydecorator(func):
        def func_wrapper(type):
            logger.info("getting number of events/ func:%s, args:%s" % (func.__name__, str(decorator_kwargs)))
            num = Events.getNumberofEvents(getevent_func(**decorator_kwargs))
            logger.info("number of events/ before %s: %i" % (func.__name__, num))
            result = func(type)
            num2 = Events.getNumberofEvents(getevent_func(**decorator_kwargs))
            logger.info(
                "number of events/ after %s: %i; events before:%i expected diff:%i" % (func.__name__, num2, num, diff)
            )
            if negation:
                assert not num2 >= num + diff
            else:
                assert num2 >= num + diff
            return result

        #wrapper will get the name of the wrapped func, this is needed for py.test setup_function
        func_wrapper.__name__ = func.__name__
        return func_wrapper

    return mydecorator


def event_name_incremented_p(eventName):
    """
    Decorator for the testcases parametrized by py.test with parameter type
    Will ensure that the number of events of the specified name
    are increased while executing the decorated test method

    :param eventName:
    :return:
    """
    args = {'eventName': eventName}
    return generate_decorator_with_type_arg(
        negation=False, getevent_func=Events.getEventsByEventName, decorator_kwargs=args
    )


def event_policy_incremented_p(policyName, diff):
    """
    Decorator for the testcases parametrized by py.test with parameter type
    Will ensure that the number of events of the specified policyName
    are increased by diff, while executing the decorated test method

    :param policyName:
    :param diff
    :return:
    """
    args = {'policyName': policyName}
    if not diff:
        diff = 1
    return generate_decorator_with_type_arg(
        negation=False, diff=diff, getevent_func=Events.getEventsByPolicyName, decorator_kwargs=args
    )


def event_entity_incremented_p(entityType, diff):
    """
    Decorator for the testcases parametrized by py.test with parameter type
    Will ensure that the number of events of the specified policyName
    are increased by diff, while executing the decorated test method

    :param entityType:
    :param diff
    :return:
    """
    args = {'entityType': entityType}
    if not diff:
        diff = 1
    return generate_decorator_with_type_arg(
        negation=False, diff=diff, getevent_func=Events.getEventsByEntityType, decorator_kwargs=args
    )


def event_name_incremented(eventName, diff=1):
    """
    Decorator for the testcases.
    Will ensure that the number of events of the specified name
    are increased while executing the decorated test method

    :param eventName:
    :return:
    """
    args = {'eventName': eventName}
    return generate_decorator(
        negation=False, diff=diff, getevent_func=Events.getEventsByEventName, decorator_kwargs=args
    )


def event_name_not_incremented(eventName):
    """
    Decorator for the testcases.
    Will ensure that the number of events of the specified name
    are NOT increased while executing the decorated test method

    :param eventName:
    :return:
    """
    args = {'eventName': eventName}
    return generate_decorator(negation=True, getevent_func=Events.getEventsByEventName, decorator_kwargs=args)


def event_policy_incremented(policyName, diff):
    """
    Decorator for the testcases.
    Will ensure that the number of events of the specified policyName
    are increased by diff, while executing the decorated test method

    :param policyName:
    :param diff
    :return:
    """
    args = {'policyName': policyName}
    if not diff:
        diff = 1
    return generate_decorator(
        negation=False, diff=diff, getevent_func=Events.getEventsByPolicyName, decorator_kwargs=args
    )


def event_policyinstance_incremented(instanceId, diff):
    """
    Decorator for the testcases.
    Will ensure that the number of events of the specified policy instanceId
    are increased by diff, while executing the decorated test method

    :param instanceId:
    :param diff
    :return:
    """
    args = {'instanceId': instanceId}
    if not diff:
        diff = 1
    return generate_decorator(
        negation=False, diff=diff, getevent_func=Events.getEventsByInstanceId, decorator_kwargs=args
    )


def event_entity_incremented(entityType, diff):
    """
    Decorator for the testcases.
    Will ensure that the number of events of the specified policyName
    are increased by diff, while executing the decorated test method

    :param entityType:
    :param diff
    :return:
    """
    args = {'entityType': entityType}
    if not diff:
        diff = 1
    return generate_decorator(
        negation=False, diff=diff, getevent_func=Events.getEventsByEntityType, decorator_kwargs=args
    )


class ReplicationPolicy(Beacon):
    default_freq = "60"

    @classmethod
    def submitHCFSPolicy(
            cls,
            policyName,
            wasbOnTarget,
            HDFSCluster,
            sourceDataset,
            targetDataset,
            frequencyInSec,
            distcpMaxMaps="1",
            distcpMapBandwidth="10",
            tdeEncryptionEnabled="false",
            preservePermission="true",
            preserveAcl="false",
            beacon_url=None,
            startTime=None,
            endTime=None,
            removeDeletedFiles="true"
    ):

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        policyType = "fs"
        policy_parameters =  "name=" + policyName + "\n"\
                             "type="+ policyType + "\n"\
                             "sourceDataset="+ sourceDataset + "\n"\
                             "targetDataset="+ targetDataset + "\n"\
                             "frequencyInSec="+ frequencyInSec + "\n"\
                             "distcpMaxMaps="+ distcpMaxMaps + "\n"\
                             "distcpMapBandwidth="+ distcpMapBandwidth + "\n"\
                             "tdeEncryptionEnabled="+ tdeEncryptionEnabled + "\n"\
                             "preservePermission="+ preservePermission + "\n"\
                             "preserveAcl="+ preserveAcl + "\n"\
                             "removeDeletedFiles="+removeDeletedFiles+"\n"\

        if wasbOnTarget == True:
            policy_parameters = policy_parameters + "sourceCluster=" + HDFSCluster
        else:
            policy_parameters = policy_parameters + "targetCluster=" + HDFSCluster

        submitAPI = "/api/beacon/policy/submit/" + policyName
        execute_query = beacon_url + submitAPI
        logger.info("Executing: " + execute_query)
        logger.info("Body:\n" + policy_parameters)
        response = session.post(execute_query, data=policy_parameters)
        Beacon.formatjson(response)
        return response

    @classmethod
    def submitHDFSPolicy(
            cls,
            policyName,
            dataset,
            sourceCluster,
            sourceDir,
            targetCluster,
            targetDir=None,
            frequencyInSec=default_freq,
            distcpMaxMaps="1",
            distcpMapBandwidth="10",
            tdeEncryptionEnabled="false",
            preservePermission="true",
            preserveAcl="false",
            beacon_url=None,
            startTime=None,
            endTime=None,
            removeDeletedFiles="true",
            queueName=None
    ):
        """

        :param policyName: Name of the Replication Policy
        :param dataset: Dataset is required as we want to have the API not to change based on the type of dataset.
                        Currently, we will not support the source and target directory to change.
                        In future if we decide to support it, then we will provide options in the dataset
                        to identify the location based on the cluster
        :param sourceCluster: Name of the Source Cluster entity
        :param sourceDir: Location of the Source Directory
        :param targetCluster: Name of the Target Cluster entity
        :param targetDir: Location of the Target Directory
        :param frequencyInSec:
        :param distcpMaxMaps:
        :param distcpMapBandwidth:
        :param tdeEncryptionEnabled:
        :param preservePermission:
        :param preserveAcl:
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        policyType = "fs"
        policy_parameters =  "name=" + policyName + "\n"\
                             "type="+ policyType + "\n"\
                             "dataset="+ dataset + "\n"\
                             "sourceCluster="+ sourceCluster + "\n"\
                             "sourceDataset="+ sourceDir + "\n"\
                             "targetCluster="+ targetCluster + "\n"\
                             "frequencyInSec="+ frequencyInSec + "\n"\
                             "distcpMaxMaps="+ distcpMaxMaps + "\n"\
                             "distcpMapBandwidth="+ distcpMapBandwidth + "\n"\
                             "tdeEncryptionEnabled="+ tdeEncryptionEnabled + "\n"\
                             "preservePermission="+ preservePermission + "\n"\
                             "preserveAcl="+ preserveAcl + "\n"\
                             "removeDeletedFiles="+removeDeletedFiles+"\n"

        if targetDir:
            policy_parameters += "targetDataset=" + targetDir + "\n"

        if startTime is not None:
            policy_parameters = policy_parameters + "startTime=" + startTime + "\n"

        if endTime is not None:
            policy_parameters = policy_parameters + "endTime=" + endTime + "\n"

        if queueName is not None:
            policy_parameters = policy_parameters + "queueName=" + queueName + "\n"

        submitAPI = "/api/beacon/policy/submit/" + policyName
        execute_query = beacon_url + submitAPI
        logger.info("Executing: " + execute_query)
        logger.info("Body:\n" + policy_parameters)
        response = session.post(execute_query, data=policy_parameters)
        Beacon.formatjson(response)
        return response

    @classmethod
    def submitHDFSSnapshotPolicy(
            cls,
            policyName,
            dataset,
            sourceCluster,
            sourceSnapshotDir,
            targetCluster,
            targetSnapshotDir=None,
            frequencyInSec=default_freq,
            distcpMaxMaps="1",
            distcpMapBandwidth="10",
            tdeEncryptionEnabled="false",
            preservePermission="true",
            preserveAcl="false",
            sourceRetentionAgeLimit="minutes(2)",
            sourceRetentionNumber="3",
            targetRetentionAgeLimit="minutes(2)",
            targetRetentionNumber="3",
            beacon_url=None,
            queueName=None
    ):
        """

        :param policyName: Name of the Replication Policy
        :param dataset: Dataset is required as we want to have the API not to change based on the type of dataset.
                        Currently, we will not support the source and target directory to change.
                        In future if we decide to support it, then we will provide options in the dataset
                        to identify the location based on the cluster
        :param sourceCluster: Name of the Source Cluster entity
        :param sourceSnapshotDir: Location of the Source Snapshot Directory
        :param targetCluster: Name of the Target Cluster entity
        :param targetSnapshotDir: Location of the Target Snapshot Directory
        :param frequencyInSec:
        :param distcpMaxMaps:
        :param distcpMapBandwidth:
        :param tdeEncryptionEnabled:
        :param preservePermission:
        :param preserveAcl:
        :param sourceRetentionAgeLimit:
        :param sourceRetentionNumber:
        :param targetRetentionAgeLimit:
        :param targetRetentionNumber:
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        policyType = "FS"
        policy_parameters =  "name=" + policyName + "\n"\
                             "type="+ policyType + "\n"\
                             "dataset="+ dataset + "\n"\
                             "sourceCluster="+ sourceCluster + "\n"\
                             "sourceDataset="+ sourceSnapshotDir + "\n"\
                             "targetCluster="+ targetCluster + "\n"\
                             "sourceSnapshotRetentionAgeLimit="+ sourceRetentionAgeLimit + "\n"\
                             "sourceSnapshotRetentionNumber="+ sourceRetentionNumber + "\n"\
                             "targetSnapshotRetentionAgeLimit="+ targetRetentionAgeLimit + "\n"\
                             "targetSnapshotRetentionNumber="+ targetRetentionNumber + "\n"\
                             "frequencyInSec="+ frequencyInSec + "\n"\
                             "distcpMaxMaps="+ distcpMaxMaps + "\n"\
                             "distcpMapBandwidth="+ distcpMapBandwidth + "\n"\
                             "tdeEncryptionEnabled="+ tdeEncryptionEnabled + "\n"\
                             "preservePermission="+ preservePermission + "\n"\
                             "preserveAcl="+ preserveAcl + "\n"

        if queueName is not None:
            policy_parameters += "queueName=" + queueName + "\n"
        if targetSnapshotDir:
            policy_parameters += "targetDataset=" + targetSnapshotDir + "\n"
        submitAPI = "/api/beacon/policy/submit/" + policyName
        execute_query = beacon_url + submitAPI
        logger.info("Executing: " + execute_query)
        logger.info("Body:\n" + policy_parameters)
        response = session.post(execute_query, data=policy_parameters)
        Beacon.formatjson(response)
        return response

    @classmethod
    def submitHivePolicy(
            cls,
            policyName,
            sourceCluster,
            sourceDatabase,
            targetCluster,
            frequencyInSec,
            beacon_url=None,
            startTime=None,
            endTime=None
    ):

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        policyType = "HIVE"
        policy_parameters =  "name=" + policyName + "\n"\
                             "type="+ policyType + "\n"\
                             "sourceDataset="+ sourceDatabase + "\n"\
                             "sourceDatabase="+ sourceDatabase + "\n"\
                             "sourceCluster="+ sourceCluster + "\n"\
                             "targetCluster="+ targetCluster + "\n"\
                             "frequencyInSec="+ frequencyInSec + "\n"

        if startTime is not None:
            policy_parameters = policy_parameters + "startTime=" + startTime + "\n"

        if endTime is not None:
            policy_parameters = policy_parameters + "endTime=" + endTime + "\n"

        submitAPI = "/api/beacon/policy/submit/" + policyName
        execute_query = beacon_url + submitAPI
        logger.info("Executing: " + execute_query)
        logger.info("Body:\n" + policy_parameters)
        response = session.post(execute_query, data=policy_parameters)
        Beacon.formatjson(response)
        return response

    @classmethod
    def schedule(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        scheduleAPI = "/api/beacon/policy/schedule/" + policyName
        execute_query = beacon_url + scheduleAPI
        logger.info("Executing: " + execute_query)
        response = session.post(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def suspend(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        suspendAPI = "/api/beacon/policy/suspend/" + policyName
        execute_query = beacon_url + suspendAPI
        logger.info("Executing: " + execute_query)
        response = session.post(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def resume(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        resumeAPI = "/api/beacon/policy/resume/" + policyName
        execute_query = beacon_url + resumeAPI
        logger.info("Executing: " + execute_query)
        response = session.post(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def abort(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        resumeAPI = "/api/beacon/policy/instance/abort/" + policyName
        execute_query = beacon_url + resumeAPI
        logger.info("Executing: " + execute_query)
        response = session.post(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def list(cls, beacon_url=None):
        """

        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        listAPI = "/api/beacon/policy/list/"
        execute_query = beacon_url + listAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def listInstance(cls, policyName, startTime=None, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        policyType = ReplicationPolicy.getPolicytype(policyName)

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        filter = {"type": policyType}
        if startTime:
            filter["startTime"] = startTime

        listinstanceAPI = (
            "/api/beacon/policy/instance/list/%s?filterBy=%s&sortOrder=ASC" %
            (policyName, ','.join("%s:%s" % (k, v) for k, v in filter.items()))
        )
        logger.info("list url is %s" % listinstanceAPI)
        execute_query = beacon_url + listinstanceAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def getStatus(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        statusAPI = "/api/beacon/policy/status/" + policyName
        execute_query = beacon_url + statusAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        if response.status_code != 200:
            logger.error("Policy status response code: " + str(response.status_code))
            logger.error("Policy status response code: " + response._content)
            return False
        Beacon.formatjson(response)
        return response

    @classmethod
    def checkStatus(cls, policyName, status, beacon_url=None):

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        response = ReplicationPolicy.getStatus(policyName, beacon_url=beacon_url)
        assert response.status_code == 200
        responsestatus = response.json()
        assert responsestatus['status'] == status

    @classmethod
    def getDefinition(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        getAPI = "/api/beacon/policy/getEntity/" + policyName
        execute_query = beacon_url + getAPI
        logger.info("Executing: " + execute_query)
        response = session.get(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def getPolicytype(cls, policyName, beacon_url=None):
        """
        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """
        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        response = ReplicationPolicy.getDefinition(policyName, beacon_url)
        policyresponse = response.json()
        return policyresponse["policy"][0]["type"]

    @classmethod
    def delete(cls, policyName, beacon_url=None):
        """

        :param policyName: Name of the Replication Policy
        :param beacon_url: Beacon Server URL
        :return:
        """

        if beacon_url is None:
            beacon_url = cls.getBeaconURL()

        if Beacon.isBeaconSSOEnabled() == "true":
            BeaconKnoxSSO.setCookie(beacon_url)
            logger.info("Session Cookie:" + session.cookies.get_dict().get("hadoop-jwt"))

        deleteAPI = "/api/beacon/policy/delete/" + policyName
        execute_query = beacon_url + deleteAPI
        logger.info("Executing: " + execute_query)
        response = session.delete(execute_query)
        Beacon.formatjson(response)
        return response

    @classmethod
    def getLatestPolicyId(cls, policyName, currentTime, beacon_url=None):
        currentTime = float(currentTime)
        logger.info("current time is %s" % currentTime)
        while True:
            instancelist = ReplicationPolicy.listInstance(policyName, beacon_url).json()
            for eachinstance in instancelist["instance"]:
                try:
                    latestid = eachinstance["id"]
                    startTime = str(eachinstance["startTime"])
                    logger.info(
                        "Policy Id: " + eachinstance["id"] + " Status: " + eachinstance["status"] + " Start time: " +
                        startTime
                    )
                    startTime = float(startTime)
                    if startTime > currentTime:
                        logger.info("Start time of %s is more than current time" % latestid)
                        return latestid
                except Exception:
                    logger.info("Policy Not Found")
                    return None
            logger.info("sleep sometime to wait for the next policy id")
            time.sleep(10)
        return latestid

    @classmethod
    def waitUntilFirstCompletion(cls, policy_name, timeout_secs=600, beacon_url=None):
        """
        Wait for a policy to complete it's first instance. If you're calling this after it already has a completed
        (not RUNNING) instance, you'll get the status of that first instance.
        :param policy_name:
        :param timeout_secs:
        :param beacon_url:
        :return:
        """
        return cls._internal_wait_until_completes(policy_name, None, timeout_secs, beacon_url)

    @classmethod
    def waitUntilCompletes(cls, policy_name, from_time=None, timeout_secs=600, beacon_url=None):
        """
        Wait for a policy to complete (to leave RUNNING status) and return its status. Only use this method if you're
        waiting for a policy that has already completed its first instance. If you'd like to wait for the first
        completed instance, use waitUntilFirstCompletion()!
        :param policy_name: Instance Id of the Policy
        :param from_time: provide this parameter if you don't want to start waiting right now (formatted date+time)
        :param timeout_secs: how long to wait before giving up (in seconds)
        :param beacon_url: Beacon Server URL
        :return:
        """
        from_time = from_time or time.strftime("%Y-%m-%dT%H:%M:%S")
        return cls._internal_wait_until_completes(policy_name, from_time, timeout_secs, beacon_url)

    @classmethod
    def _internal_wait_until_completes(cls, policy_name, from_time, timeout_secs, beacon_url):
        """
        !!! INTERNAL METHOD, DON'T CALL THIS FROM OUTSIDE OF THIS CLASS !!!
        Wait for a policy to complete (to leave RUNNING status) and return its status.
        :param policy_name: Instance Id of the Policy
        :param from_time: provide this parameter if you don't want to start waiting right now (formatted date+time)
        :param timeout_secs: how long to wait before giving up (in seconds)
        :param beacon_url: Beacon Server URL
        :return:
        """
        status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
        step_time_secs = 15
        tries = timeout_secs / step_time_secs
        count = 0
        while ((status == "RUNNING" or status == None) and count < tries):
            logging.info('Try %s of %s to determine %s instance job status.' % (count + 1, tries, policy_name))
            time.sleep(step_time_secs)
            status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
            count += 1

        if status == "RUNNING":
            # tries exhausted, give up
            response = ReplicationPolicy.abort(policy_name, beacon_url)
            assert response.status_code == 200, "Could not abort a timed out replication policy (status %d.)" % response.status_code

        return status

    @classmethod
    def waitUntilCompletesIgnoreSkipped(cls, policy_name, from_time=None, timeout_secs=600, beacon_url=None):
        """
        Wait for a policy to complete (to leave RUNNING status) and return its status. Only use this method if you're
        waiting for a policy that has already completed its first instance. If you'd like to wait for the first
        completed instance, use waitUntilFirstCompletion()!
        :param policy_name: Instance Id of the Policy
        :param from_time: provide this parameter if you don't want to start waiting right now (formatted date+time)
        :param timeout_secs: how long to wait before giving up (in seconds)
        :param beacon_url: Beacon Server URL
        :return:
        """
        from_time = from_time or time.strftime("%Y-%m-%dT%H:%M:%S")
        return cls._internal_wait_until_completesIgnoreSkipped(policy_name, from_time, timeout_secs, beacon_url)

    @classmethod
    def _internal_wait_until_completesIgnoreSkipped(cls, policy_name, from_time, timeout_secs, beacon_url):
        """
        !!! INTERNAL METHOD, DON'T CALL THIS FROM OUTSIDE OF THIS CLASS !!!
        Wait for a policy to complete (to leave RUNNING status) and return its status.
        :param policy_name: Instance Id of the Policy
        :param from_time: provide this parameter if you don't want to start waiting right now (formatted date+time)
        :param timeout_secs: how long to wait before giving up (in seconds)
        :param beacon_url: Beacon Server URL
        :return:
        """
        status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
        step_time_secs = 15
        tries = timeout_secs / step_time_secs
        count = 0
        while ((status == "SKIPPED" or status == None) and count < tries):
            logging.info(
                'Instance in SKIPPED state, wait for next running instance. Try %s of %s to determine %s instance job status.'
                % (count + 1, tries, policy_name)
            )
            time.sleep(step_time_secs)
            if status == "SKIPPED":
                from_time = time.strftime("%Y-%m-%dT%H:%M:%S")
            status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
            count += 1
        while ((status == "RUNNING" or status == None) and count < tries):
            logging.info('Try %s of %s to determine %s instance job status.' % (count + 1, tries, policy_name))
            time.sleep(step_time_secs)
            status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
            count += 1

        if status == "RUNNING":
            # tries exhausted, give up
            response = ReplicationPolicy.abort(policy_name, beacon_url)
            assert response.status_code == 200, "Could not abort a timed out replication policy (status %d.)" % response.status_code

        return status

    @staticmethod
    def ensure_policy_in_state(policy_name, desired_state, from_time, beacon_url=None):
        status = PolicyInstance.getInstanceStatus(policy_name, from_time, beacon_url)
        assert status == desired_state

    @classmethod
    def assert_policy_submission_failure(cls, response, policyName, error_msg):
        assert response.status_code == 400

        response_content = response.json()
        assert error_msg == response_content["message"]

        # Ensure that the policy is not listed
        getEntity_response = ReplicationPolicy.getDefinition(policyName)
        assert getEntity_response.status_code == 404


class PolicyInstance(Beacon):
    @classmethod
    def getInstanceStatus(cls, policyName, startTime, beacon_url=None):
        """
        Get the status of the first instance of a policy after a given point in time, or None if there's no such
        instance.
        :param policyName: Name of the policy.
        :param startTime: Get first instance after this point in time (formatted date+time).
        :param beacon_url: Beacon Server URL
        :return:
        """
        response = ReplicationPolicy.listInstance(policyName, startTime, beacon_url)
        if response.status_code == 200:
            instances = response.json()["instance"]
        else:
            return response

        if not instances:
            return None

        first_instance = instances[0]
        logger.info(
            "Instance Id: " + first_instance["id"] + ", Policy Name" + policyName + ", Status: " +
            first_instance["status"]
        )
        return first_instance["status"]

    @classmethod
    def getInstanceJobId(cls, policyName, startTime, beacon_url=None):
        """
        Get the status of the first instance of a policy after a given point in time, or None if there's no such
        instance.
        :param policyName: Name of the policy.
        :param startTime: Get first instance after this point in time (formatted date+time).
        :param beacon_url: Beacon Server URL
        :return:
        """
        response = ReplicationPolicy.listInstance(policyName, startTime, beacon_url)
        if response.status_code == 200:
            instances = response.json()["instance"]
        else:
            return response

        if not instances:
            return None

        first_instance = instances[0]
        job_id = json.loads(first_instance["trackingInfo"])["jobId"]
        logger.info(
            "Instance Id: " + first_instance["id"] + ", Policy Name" + policyName + ", Status: " +
            first_instance["status"] + ", Job Id: " + job_id
        )
        job_id = job_id.replace("job_", "")
        return job_id

    @classmethod
    def getInstanceMessage(cls, policyName, startTime, beacon_url=None):
        """
        Get the status of the first instance of a policy after a given point in time, or None if there's no such
        instance.
        :param policyName: Name of the policy.
        :param startTime: Get first instance after this point in time (formatted date+time).
        :param beacon_url: Beacon Server URL
        :return:
        """
        response = ReplicationPolicy.listInstance(policyName, startTime, beacon_url)
        if response.status_code == 200:
            instances = response.json()["instance"]
        else:
            return response

        if not instances:
            return None

        first_instance = instances[0]
        logger.info(
            "Instance Id: " + first_instance["id"] + ", Policy Name" + policyName + ", Status: " +
            first_instance["status"] + ", Message: " + first_instance["message"]
        )
        return first_instance["message"]

    @classmethod
    def getInstanceretryAttempted(cls, policyName, startTime=None, beacon_url=None):
        """
        Get the retryAttempted of the first instance of a policy
        :param policyName: Name of the policy.
        :param startTime: Get first instance by default
        :param beacon_url: Beacon Server URL
        :return:
        """
        response = ReplicationPolicy.listInstance(policyName, startTime, beacon_url)
        if response.status_code == 200:
            instances = response.json()["instance"]
        else:
            return response

        if not instances:
            return None

        first_instance = instances[0]
        logger.info(
            "Instance Id: " + first_instance["id"] + ", Policy Name" + policyName + ", Status: " +
            first_instance["status"] + ", Message: " + first_instance["message"]
        )
        return first_instance["retryAttempted"]
