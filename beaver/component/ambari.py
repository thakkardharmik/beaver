#pylint: disable=unused-import
#pylint: disable=wrong-import-order
#pylint: disable=ungrouped-imports
#pylint: disable=old-style-class
#pylint: disable=logging-not-lazy
#pylint: disable=singleton-comparison
#pylint: disable=logging-format-interpolation
#pylint: disable=unused-variable
#pylint: disable=len-as-condition
#pylint: disable=bare-except
#pylint: disable=unused-argument
#pylint: disable=line-too-long
#pylint: disable=protected-access
#pylint: disable=anomalous-backslash-in-string
#pylint: disable=redefined-builtin
#pylint: disable=bad-continuation
#pylint: disable=literal-comparison
#pylint: disable=superfluous-parens
#pylint: disable=multiple-imports
#pylint: disable=no-init
#pylint: disable=dangerous-default-value
#pylint: disable=inconsistent-return-statements

#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import socket
import uuid
import time
import json as jsonlib
import re
import logging
import requests
from requests.auth import HTTPBasicAuth
from beaver.machine import Machine
from beaver.config import Config
from beaver import util


logger = logging.getLogger(__name__)


class Ambari(object):

    def __init__(self):
        pass

    _os = None
    _isSecure = False
    _ambariDB = 'Postgres'
    _host = None
    _stack = None
    _version = None
    _hdpVersion = None
    _weburl = None
    _config_version_dic = {}
    _clusterName = None
    _is_cluster_secure = None
    _is_ambari_encrypted = None
    _ambari_properties = "/etc/ambari-server/conf/ambari.properties"
    _jwt_cookie = ""
    _is_sso_enabled = None
    token_map = {}

    @classmethod
    def getOS(cls):
        cls._os = str(Machine.getRelease())
        return cls._os

    @classmethod
    def getStack(cls):
        cls._stack = Config.get('ambari', 'STACK')
        return cls._stack

    @classmethod
    def getHost(cls):
        if Machine.isAmazonLinux():
            cls._host = Config.get('ambari', 'HOST', socket.gethostname())
        elif Machine.isHumboldt():
            cls._host = Config.get('machine', 'GATEWAY').replace("-ssh", "")
        elif Machine.getInstaller() == 'cloudbreak':
            cls._host = Config.get('machine', 'GATEWAY')
        else:
            cls._host = Config.get('machine', 'GATEWAY')
            #cls._host = Config.get('ambari', 'HOST', socket.gethostbyname(socket.gethostname()))
        return cls._host

    @classmethod
    def getCloudUrl(cls, hostname=None):
        logger.info("Getting Cloud URL")
        proxy_enabled = Config.get('hdc', 'USE_CLI')
        logger.info("proxy_enabled: %s", proxy_enabled)
        if proxy_enabled == 'yes':
            shared_cluster = Config.get('hdc', 'IS_SHARED_CLUSTER')
            epehemeral_cluster = Config.get('hdc', 'IS_EPHEMERAL_CLUSTER')
            if shared_cluster == 'yes':
                cluter_ip = Config.get('hdc', 'SHARED_CLUSTER_IP')
                cluster_name = Config.get('hdc', 'SHARED_CLUSTER_NAME')
                cls._weburl = "https://%s/%s/services/ambari" % (cluter_ip, cluster_name)
            elif epehemeral_cluster == 'yes':
                cluter_ip = Config.get('hdc', 'SHARED_CLUSTER_IP')
                cluster_name = Config.get('hdc', 'EPHEMERAL_CLUSTER_NAME')
                cls._weburl = "https://%s/%s/services/ambari" % (cluter_ip, cluster_name)
            else:
                cluster_name = Config.get('hdc', 'EPHEMERAL_CLUSTER_NAME')
                cls._weburl = "https://%s/%s/services/ambari" % (cls.getHost(), cluster_name)
        else:
            if hostname is None:
                host = cls.getHost()
            else:
                host = hostname
            cls._weburl = "https://%s/ambari" % host
        logger.info("Here is the url found : %s", cls._weburl)
        return cls._weburl

    @classmethod
    def getWebUrl(cls, is_hdp=True, is_enc=False, hostname=None):
        if hostname is None:
            host = cls.getHost()
        else:
            #    cls._weburl = None
            host = hostname
        if not cls._weburl or hostname:
            if is_hdp:
                from beaver.component.hadoop import Hadoop
                if Machine.getInstaller() == 'cloudbreak':
                    weburl = cls.getCloudUrl(host)
                elif Hadoop.isEncrypted() or cls.is_ambari_encrypted():
                    weburl = "https://%s:8443" % host
                elif Machine.isHumboldt():
                    weburl = "https://%s" % host
                else:
                    weburl = "http://%s:8080" % host
            else:
                if Machine.isHumboldt():
                    weburl = "https://%s" % host
                elif is_enc:
                    weburl = "https://%s:8443" % host
                else:
                    weburl = "http://%s:8080" % host
        if hostname:
            logger.info("Web URL With Hostname : {0}".format(weburl))
            return weburl
        if not cls._weburl and not hostname:
            cls._weburl = weburl
            logger.info("Web URL : {0}".format(cls._weburl))
        return cls._weburl

    @classmethod
    def is_cluster_secure(cls, refresh=False):
        # in hdf we don't have hadoop & can't depend on it for detecting security
        if Machine.isHumboldt():
            from beaver.component.hadoop import Hadoop
            return Hadoop.isSecure()
        if cls._is_cluster_secure is None or refresh:
            enc = cls.is_ambari_encrypted()
            web_url = cls.getWebUrl(False, is_enc=enc)
            url = "%s/api/v1/clusters/%s" % (web_url, cls.getClusterName(is_hdp=False, is_enc=enc, weburl=web_url))
            retcode, retdata = cls.performRESTCall(url)[0:2]
            for _ in range(60):
                if retcode == 200:
                    break
                util.sleep(5)
                retcode, retdata = cls.performRESTCall(url)[0:2]
            jsoncontent = util.getJSON(retdata)
            cls._is_cluster_secure = jsoncontent['Clusters']['security_type'].upper() == "KERBEROS"
        return cls._is_cluster_secure

    @classmethod
    def is_ambari_encrypted(cls, force=False):
        if cls._is_ambari_encrypted is None or force:
            ambari_server_properties = '/etc/ambari-server/conf/ambari.properties'
            ambari_server_ssl = util.getPropertyValueFromFile(ambari_server_properties, "api.ssl")
            cls._is_ambari_encrypted = ambari_server_ssl and ambari_server_ssl.lower() == 'true'
        return cls._is_ambari_encrypted

    @classmethod
    def checkIfHDPDeployed(cls, is_hdp=False, is_enc=False):
        url = "%s/api/v1/clusters" % cls.getWebUrl(is_hdp, is_enc)
        retcode, retdata = cls.performRESTCall(url)[0:2]
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                clusters = [cl['Clusters']['cluster_name'] for cl in jsoncontent['items']]
                if not clusters:
                    return False
                else:
                    return True
            except Exception:
                return False
        return False

    @classmethod
    def http_get_request(cls, uri, weburl=None):
        if weburl is None:
            weburl = cls.getWebUrl()
        url = weburl + uri
        if cls.is_sso_enabled():
            response = requests.get(url=url, cookies={'hadoop-jwt': cls.get_jwt_cookie(url)}, verify=False)
        else:
            basic_auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
            response = requests.get(url=url, auth=basic_auth, verify=False)
        return response

    @classmethod
    def http_put_post_request(cls, uri, data, requestType, weburl=None):
        if weburl is None:
            weburl = cls.getWebUrl()
        url = str(weburl) + uri
        basic_auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        if requestType == 'PUT':
            if cls.is_sso_enabled():
                response = requests.put(
                    url=url,
                    data=data,
                    headers={'X-Requested-By': 'ambari'},
                    cookies={'hadoop-jwt': cls.get_jwt_cookie(url)},
                    verify=False
                )
            else:
                response = requests.put(
                    url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
                )
            return response
        elif requestType == 'POST':
            if cls.is_sso_enabled():
                response = requests.post(
                    url=url,
                    data=data,
                    headers={'X-Requested-By': 'ambari'},
                    cookies={'hadoop-jwt': cls.get_jwt_cookie(url)},
                    verify=False
                )
            else:
                response = requests.post(
                    url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
                )
            return response
        else:
            return None

    @classmethod
    def performRESTCall(cls, url, headers=None, data=None, method='GET'):
        if headers is None:
            headers = {}
        if data is None:
            data = ''
        logger.debug("URL : %s and Method : %s with headers : %s and body : %s", url, method, headers, data)
        if cls.is_sso_enabled():
            response = requests.get(url=url, cookies={'hadoop-jwt': cls.get_jwt_cookie(url)}, verify=False)
            # cookie = 'hadoop-jwt='+cls.get_jwt_cookie(url)
            # retcode, retdata, retheaders = util.httpRequest(url,headers, data=data, method=method,cookie=cookie)
        else:
            basic_auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
            response = requests.get(url=url, auth=basic_auth, verify=False)

        return response.status_code, response.content, response.headers

    @classmethod
    def getClusterName(cls, is_hdp=True, is_enc=False, weburl=None):
        cls._clusterName = 'cl1'
        if weburl is None:
            weburl = cls.getWebUrl(is_hdp, is_enc)
        url = "%s/api/v1/clusters" % weburl
        retcode, retdata = cls.performRESTCall(url)[0:2]

        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                clusters = [cl['Clusters']['cluster_name'] for cl in jsoncontent['items']]
                if clusters:
                    cls._clusterName = clusters[0]
            except Exception:
                return cls._clusterName
        return cls._clusterName

    @classmethod
    def getServiceHosts(cls, service, component, cluster='cl1', is_hdp=True, is_enc=False):
        hosts = []
        url = "%s/api/v1/clusters/%s/services/%s/components/%s" % (
            cls.getWebUrl(is_hdp, is_enc), cluster, service, component
        )
        retcode, retdata = cls.performRESTCall(url)[0:2]
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                hosts = [hc['HostRoles']['host_name'] for hc in jsoncontent['host_components']]
            except Exception:
                hosts = []
        return hosts

    # State can be INSTALLED, STARTED, ..
    @classmethod
    def getServiceHostsWithState(cls, service, component, state, cluster='cl1', is_hdp=True):
        hosts = []
        for h in cls.getServiceHosts(service, component, cluster=cluster):
            url = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (cls.getWebUrl(is_hdp), cluster, h, component)
            retcode, retdata = cls.performRESTCall(url)[0:2]
            if retcode == 200:
                jsoncontent = util.getJSON(retdata)
                try:
                    if jsoncontent['HostRoles']['state'] == state:
                        hosts.append(h)
                except Exception:
                    hosts = []
        return hosts

    @classmethod
    def isSecure(cls):
        if Config.get('machine', 'IS_SECURE') == 'yes':
            cls._isSecure = True
        return cls._isSecure

    @classmethod
    def getVersion(cls):
        cls._version = Config.get('ambari', 'VERSION')
        return cls._version

    @classmethod
    def getAmbariServerVersion(cls):
        exit_code, output = Machine.runas(user="root", cmd="ambari-server --version")
        logger.info("output is : %s", output)
        if exit_code == 0:
            pattern = re.compile("\d{0,4}\.\d{0,4}\.\d{0,4}\.\d{0,4}\-\d{0,4}", re.M) #pylint: disable=W1401
            m = pattern.search(output)
            if m:
                cls._ambari_version = m.group(0)
            else:
                cls._ambari_version = ""
        logger.info("output is" + output + "the ambari-version is:- " + cls._ambari_version)
        return cls._ambari_version

    @classmethod
    def getDatabaseFlavor(cls):
        cls._ambariDB = Config.get('ambari', 'AMBARI_DB')
        return cls._ambariDB

    @classmethod
    def getHDPVersion(cls):
        exit_code, output = Machine.run("hadoop version")

        if exit_code == 0:
            # perform a multi line check
            pattern = re.compile("^Hadoop (\S+)", re.M) #pylint: disable=W1401
            m = pattern.search(output)
            if m:
                cls._hdpVersion = m.group(1)
            else:
                cls._hdpVersion = ""
        else:
            cls._hdpVersion = cls.getStack()
        return cls._hdpVersion

    @classmethod
    def getAdminUsername(cls):
        dp_use_test_ldap = Config.get('dataplane', 'DP_USE_TEST_LDAP', socket.gethostbyname(socket.gethostname()))
        if cls.is_sso_enabled() and dp_use_test_ldap == "no":
            return "admin1"
        return 'admin'

    @classmethod
    def getAdminPassword(cls):
        dp_use_test_ldap = Config.get('dataplane', 'DP_USE_TEST_LDAP', socket.gethostbyname(socket.gethostname()))
        if Machine.getInstaller() == 'cloudbreak':
            hdc = Config.get('hdc', 'USE_CLI')
            if hdc == 'yes':
                return 'cloudbreak1'
            else:
                return 'admin'
        elif Machine.isHumboldt():
            return 'HdpCli123!'
        elif cls.is_sso_enabled() and dp_use_test_ldap == "yes":
            return 'admin-password'
        elif cls.is_sso_enabled() and dp_use_test_ldap == "no":
            return 'Horton!#works'
        return 'admin'

    @classmethod
    def getCurrentServiceConfigVersion(cls, service, cluster=None, host=None):
        webURL = Ambari.getWebUrl(hostname=host)
        if cluster is None:
            cluster = cls.getClusterName()
        if host:
            cluster = cls.getClusterName(weburl=webURL)
        url = "%s/api/v1/clusters/%s/configurations/service_config_versions?service_name=%s&is_current=true" % (
            webURL, cluster, service
        )
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get configs."
        json = response.json()
        items = json['items']
        return items[0]['service_config_version'] if items else None

    @classmethod
    def getLatestConfigVersion(cls, type, cluster=None): #pylint: disable=W0622
        if cluster is None:
            cluster = cls.getClusterName()
        url = "%s/api/v1/clusters/%s?fields=Clusters/desired_configs" % (Ambari.getWebUrl(), cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get configs."
        json = response.json()
        return json['Clusters']['desired_configs'][type]['version']

    @classmethod
    def getConfig(cls, type, service=None, cluster=None, webURL=None): #pylint: disable=W0622
        if cluster is None:
            cluster = cls.getClusterName()
        if webURL is None:
            webURL = Ambari.getWebUrl()
        else:
            cluster = cls.getClusterName(weburl=webURL)
        url = "%s/api/v1/clusters/%s/configurations/service_config_versions?is_current=true" % (webURL, cluster)

        if service:
            url += "&service_name=%s" % service
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get configs."
        json = response.json()
        items = json['items']
        for item in items:
            for configs in item['configurations']:
                if configs['type'] == type:
                    return configs['properties']
        raise AssertionError("Failed to find type \"%s\"" % type)

    @classmethod
    def setConfig(cls, type, config, cluster=None, webURL=None): #pylint: disable=W0622
        if cluster is None:
            cluster = cls.getClusterName()
        if webURL is None:
            webURL = Ambari.getWebUrl()
        else:
            cluster = cls.getClusterName(weburl=webURL)
        orig_config = cls.getConfig(type, cluster=cluster, webURL=webURL)
        for key, value in config.items():
            orig_config[key] = value
        tag = "version-%s" % str(uuid.uuid1())
        data = {'Clusters': {'desired_config': {'type': type, 'tag': tag, 'properties': orig_config}}}
        json = jsonlib.dumps(data)
        if not webURL:
            webURL = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s" % (webURL, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.put(url=url, headers=head, auth=auth, data=json, verify=False)
        assert response.status_code == 200, "Failed to set config."

    @classmethod
    def deleteConfig(cls, type, config, cluster=None, webURL=None): #pylint: disable=W0622
        if cluster is None:
            cluster = cls.getClusterName()
        if webURL is None:
            webURL = Ambari.getWebUrl()
        else:
            cluster = cls.getClusterName(weburl=webURL)
        orig_config = cls.getConfig(type, cluster=cluster, webURL=webURL)
        for key, value in orig_config.items():
            if key in config:
                orig_config.pop(key)
                logger.debug("Key : %s Value : %s", key, value)
        tag = "version-%s" % str(uuid.uuid1())
        data = {'Clusters': {'desired_config': {'type': type, 'tag': tag, 'properties': orig_config}}}
        json = jsonlib.dumps(data)
        if not webURL:
            webURL = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s" % (webURL, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.put(url=url, headers=head, auth=auth, data=json, verify=False)
        assert response.status_code == 200, "Failed to Delete config."

    @classmethod
    def resetConfig(cls, service_name, version, cluster=None, webURL=None):
        if cluster is None:
            cluster = cls.getClusterName(weburl=webURL)
        if not webURL:
            webURL = Ambari.getWebUrl()
        data = {'Clusters': {'desired_service_config_versions': {}}}
        data['Clusters']['desired_service_config_versions']['service_name'] = service_name
        data['Clusters']['desired_service_config_versions']['service_config_version'] = version
        rollback_note = "Rollback to service config version %d (%s)" % (version, service_name)
        data['Clusters']['desired_service_config_versions']['service_config_version_note'] = rollback_note
        json = jsonlib.dumps(data)
        url = "%s/api/v1/clusters/%s" % (webURL, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.put(url=url, headers=head, auth=auth, data=json, verify=False)
        assert response.status_code == 200, "Failed to set config."

    @classmethod
    def startComponent(cls, host, component, cluster=None, waitForCompletion=False, timeout=300, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName()
        if weburl is None:
            weburl = Ambari.getWebUrl()
        u = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, host, component)
        h = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        d = '{"RequestInfo":{"context":"Start %s on %s"}, "HostRoles": { "state": "STARTED" } }' % (component, host)
        a = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        logger.info("Initiating start of %s at host %s, request url is: %s", component, host, u)
        r = requests.put(u, data=d, headers=h, auth=a, verify=False)
        assert(r.status_code == 200 or r.status_code == 202), \
            "Failed to start component %s on host %s, status=%d" % (component, host, r.status_code)
        if r.status_code == 202 and waitForCompletion:
            json_data = r.json()
            request_id = json_data['Requests']['id']
            status = cls.wait_until_request_complete(request_id, timeout=timeout, cluster=cluster, weburl=weburl)
            assert status == 'COMPLETED', "Failed to start component %s on host %s, status=%s" % (
                component, host, status
            )
        logger.info("Completed start of %s at host %s", component, host)

    @classmethod
    def stopComponent(cls, host, component, cluster=None, waitForCompletion=False, timeout=300, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName()
        if weburl is None:
            weburl = Ambari.getWebUrl()
        u = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, host, component)
        h = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        d = '{"RequestInfo":{"context":"Stop %s on %s"}, "HostRoles": { "state": "INSTALLED" } }' % (component, host)
        a = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        logger.info("Initiating stop of %s at host %s, request url is: %s", component, host, u)
        r = requests.put(u, data=d, headers=h, auth=a, verify=False)
        assert(r.status_code == 200 or r.status_code == 202), \
            "Failed to stop component %s on host %s, status=%d" % (component, host, r.status_code)
        if r.status_code == 202 and waitForCompletion:
            json_data = r.json()
            request_id = json_data['Requests']['id']
            status = cls.wait_until_request_complete(request_id, timeout=timeout, cluster=cluster, weburl=weburl)
            assert status == 'COMPLETED', "Failed to stop component %s on host %s, status=%s" % (
                component, host, status
            )
        logger.info("Completed stop of %s at host %s", component, host)

    @classmethod
    def addComponent(cls, host, service, component, cluster=None):
        if cluster is None:
            cluster = cls.getClusterName()
        u = "%s/api/v1/clusters/%s/services/%s/components/%s" % (Ambari.getWebUrl(), cluster, service, component)
        h = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        a = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        r = requests.post(u, headers=h, auth=a, verify=False)
        assert(r.status_code == 200 or r.status_code == 201 or r.status_code == 202), "Failed to add component" \
                                          " %s on host %s, status=%d" % (component, host, r.status_code)
        u = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (Ambari.getWebUrl(), cluster, host, component)
        r = requests.post(u, headers=h, auth=a, verify=False)
        assert(r.status_code == 200 or r.status_code == 201 or r.status_code == 202), "Failed to setup " \
                                          "component %s on host %s, status=%d" % (component, host, r.status_code)

    @classmethod
    def getHostsForComponent(cls, component, cluster=None, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName(weburl=weburl)
        url = "/api/v1/clusters/%s/host_components?HostRoles/component_name=%s" % (cluster, component)
        logger.info("Initiating fetch of hosts for %s, request url is: %s", component, url)
        response = cls.http_get_request(url, weburl=weburl)
        if response.status_code != 200:
            return None
        json = response.json()
        items = json['items']
        hosts = []
        for item in items:
            if item.has_key("HostRoles") and item["HostRoles"].has_key("host_name"):
                hosts.append(item["HostRoles"]["host_name"])
        logger.info("List of hosts for %s : %s", component, ",".join(hosts))
        return hosts

    @classmethod
    def getHostPerHAState(cls, component, ha_state, cluster=None, weburl=None):
        if not cluster:
            cluster = cls.getClusterName(weburl=weburl)
        url = "/api/v1/clusters/%s/host_components?HostRoles/component_name=%s&HostRoles/ha_state=%s" % (
            cluster, component, ha_state
        )
        logger.info("Initiating fetch for component=%s "
                    "and ha_state=%s, request_url=%s", component, ha_state, url)
        response = cls.http_get_request(url, weburl=weburl)
        if response.status_code != 200:
            return None
        json = response.json()
        items = json["items"]
        for item in items:
            if item.has_key("HostRoles") and item["HostRoles"].has_key("host_name"):
                return item["HostRoles"]["host_name"]
        return None

    @classmethod
    def getHosts(cls, weburl=None):
        url = "/api/v1/hosts"
        logger.info("Initiating fetch of hosts , request url is: %s", url)
        response = cls.http_get_request(url, weburl=weburl)
        if response.status_code != 200:
            return None
        json = response.json()
        items = json['items']
        hosts = []
        for item in items:
            if item.has_key("Hosts") and item["Hosts"].has_key("host_name"):
                hosts.append(item["Hosts"]["host_name"])
        logger.info("List of hosts for : %s", (",".join(hosts)))
        return hosts

    @classmethod
    def start_stop_service(cls, service, state, cluster=None, waitForCompletion=False, timeout=300, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName()
        if weburl is None:
            webURL = Ambari.getWebUrl()
            logger.debug("Web url : %s", webURL)
        else:
            cluster = cls.getClusterName(weburl=weburl)
        if service == 'HIVE':
            timeout = 720
        data = '{"RequestInfo": {"' \
               'context": "' + state + ' ' + service + '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        url = '/api/v1/clusters/' + cluster + '/services/' + service
        r = cls.http_put_post_request(url, data, 'PUT', weburl=weburl)
        if r.status_code == 202 and waitForCompletion:
            json_data = r.json()
            request_id = json_data['Requests']['id']
            status = cls.wait_until_request_complete(request_id, timeout=timeout, cluster=cluster, weburl=weburl)
            assert status == 'COMPLETED', "Service %s: FAILED for state %s" % (service, state)
            if service == 'HIVE':
                time.sleep(300)  # wait for port to open due to BUG-99441
        return r

    @classmethod
    def wait_until_request_complete(cls, request_id, timeout=300, interval=10, cluster=None, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName()
        starttime = time.time()
        status = 'PENDING'
        while (time.time() - starttime) < timeout:
            status = cls.get_request_current_state(request_id, cluster=cluster, weburl=weburl)
            if status in ('COMPLETED', 'FAILED'):
                break
            time.sleep(interval)
        return status

    @classmethod
    def get_request_current_state(cls, request_id, cluster=None, weburl=None):
        if cluster is None:
            cluster = cls.getClusterName()
        url = '/api/v1/clusters/' + cluster + '/requests/' + str(request_id)
        response = cls.http_get_request(url, weburl=weburl)
        json_data = jsonlib.loads(response.content)
        return json_data['Requests']['request_status']

    @classmethod
    def get_services_components_with_stale_configs(cls, cluster=None):
        if cluster is None:
            cluster = cls.getClusterName()
        url = '/api/v1/clusters/' + cluster + '' \
                                              '/host_components?HostRoles/stale_configs=true&' \
                                              'fields=HostRoles/service_name,HostRoles/state,' \
                                              'HostRoles/host_name,HostRoles/stale_configs,&minimal_response=true'
        response = cls.http_get_request(url)
        json_data = jsonlib.loads(response.content)
        stale_service_components = []
        services = set()
        host_components = dict()
        components = []
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
    def restart_services_with_stale_configs(cls):
        logger.info("Find which host components have stale configs (need to be restarted or have config refreshed.")
        stale_service_components = cls.get_services_components_with_stale_configs()

        services_to_restart = stale_service_components[0]
        list_of_services_to_restart = list(services_to_restart)

        if 'HDFS' in list_of_services_to_restart:
            list_of_services_to_restart.remove('HDFS')
            list_of_services_to_restart.insert(0, 'HDFS')
            # to make sure region servers don't go down because of HDFS restart
            if 'HBASE' in list_of_services_to_restart:
                hbase_index = list_of_services_to_restart.index('HBASE')
                list_of_services_to_restart.insert(1, list_of_services_to_restart.pop(hbase_index))
            else:
                from beaver.component.hbase import HBase
                if 'HBASE' not in list_of_services_to_restart and HBase.isInstalled():
                    list_of_services_to_restart.insert(1, 'HBASE')
        if 'HIVE' in list_of_services_to_restart:
            list_of_services_to_restart.remove('HIVE')
            list_of_services_to_restart.append('HIVE')
        if 'SPARK' in list_of_services_to_restart:
            list_of_services_to_restart.remove('SPARK')
            list_of_services_to_restart.append('SPARK')
        if 'SPARK2' in list_of_services_to_restart:
            list_of_services_to_restart.remove('SPARK2')
            list_of_services_to_restart.append('SPARK2')

        logger.info("Restart all components or services to have the config change take effect.")
        services_failed_to_install = []
        services_failed_to_start = []
        for service in list_of_services_to_restart:
            retry_install = 3
            while retry_install > 0:
                response = cls.start_stop_service(service, 'INSTALLED', None, True)
                if response.status_code == 202:
                    json_data = jsonlib.loads(response.content)
                    request_id = json_data['Requests']['id']
                    logger.info("Waiting for the service %s to stop..", service)
                    while not cls.get_request_current_state(request_id) == 'COMPLETED':
                        if cls.get_request_current_state(request_id) == 'FAILED':
                            break
                            # raise Exception("Installing service "+ service + " failed!")
                        time.sleep(35)
                    if cls.get_request_current_state(request_id) == 'COMPLETED':
                        break
                    elif retry_install == 1:
                        logger.info("Installing service %s failed!", service)
                        services_failed_to_install.append(service)
                    else:
                        retry_install -= 1
                else:
                    logger.info("%d is returned as %s is requested "
                                "to be installed", response.status_code, service)
                    retry_install -= 1

            if service == 'HIVE':
                from beaver.component.hive import Hive
                if not Hive.isLLAPEnabled():
                    Hive.stopService(services=['metastore'])
                Hive.stopService(services=['hiveserver2'])
            if service == 'YARN' or service == "HDFS":
                time.sleep(60)
            retry_start = 3
            while retry_start > 0:
                response = cls.start_stop_service(service, 'STARTED', None, True)
                if response.status_code == 202:
                    json_data = jsonlib.loads(response.content)
                    request_id = json_data['Requests']['id']
                    logger.info("Waiting for the service %s to start..", service)
                    while not cls.get_request_current_state(request_id) == 'COMPLETED':
                        if cls.get_request_current_state(request_id) == 'FAILED':
                            break
                            # raise Exception("Starting service "+ service + " failed!")
                        time.sleep(35)
                    if cls.get_request_current_state(request_id) == 'COMPLETED':
                        break
                    elif retry_start == 1:
                        logger.info("Starting service %s failed!", service)
                        # To-do: need to investigate why ATLS fail to restart on Hive2 cluster
                        if service != 'ATLAS':
                            services_failed_to_start.append(service)
                    else:
                        if service == 'HIVE':
                            from beaver.component.hive import Hive
                            if Hive.isLLAPEnabled():
                                Hive.stopService(services=['hiveserver2'])
                    retry_start -= 1
                else:
                    logger.info("%d is returned as %s is requested to "
                                "be started", response.status_code, service)
                    retry_start -= 1

            if service == 'HIVE':
                time.sleep(120)
            if service == 'HDFS':
                time.sleep(60)

        if services_failed_to_install:
            logger.info("Following services failed to Install:")
            for service1 in services_failed_to_install:
                logger.info("- %s", service1)

        if services_failed_to_start:
            logger.info("Following services failed to Start:")
            for service2 in services_failed_to_start:
                logger.info("- %s", service2)

        if services_failed_to_install:
            raise Exception("Some Services Failed to Restart!!")

    @classmethod
    def modify_service_configs_and_restart(cls, serviceName, config_type, configs_dict, cluster=None): #pylint: disable=W0622
        logger.debug("Updating configs for service : %s", serviceName)
        if cluster is None:
            cluster = cls.getClusterName()
        logger.info("Step 1: Find the latest version of the config type that you need to update.")
        url = '/api/v1/clusters/' + cluster + '?fields=Clusters/desired_configs,Clusters/version'
        response = cls.http_get_request(url)
        json_data = jsonlib.loads(response.content)

        tag = json_data['Clusters']['desired_configs'][config_type]['tag']
        new_tag = "version" + str(uuid.uuid4())

        logger.info("Step 2: Read the config type with correct tag.")
        url = '/api/v1/clusters/' + cluster + '/configurations?type=' + config_type + '&tag=' + tag
        response = cls.http_get_request(url)
        json_data = jsonlib.loads(response.content)
        for config in configs_dict.keys():
            json_data['items'][0]['properties'][config] = configs_dict.get(config)
        json_data = '{"Clusters":{"desired_config":{"' \
                                'tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
                                    jsonlib.dumps(json_data['items'][0]['properties'])) + '}}}'
        logger.info("Step 3: Save a new version of the config and apply it using one call.")
        url = '/api/v1/clusters/' + cluster
        response = cls.http_put_post_request(url, json_data, 'PUT')

        time.sleep(10)

        cls.restart_services_with_stale_configs()

    @classmethod
    def modify_configs_for_multiple_services(cls, changes, cluster=None):
        if cluster is None:
            cluster = cls.getClusterName()
        for service_name, service_changes in changes.items():
            logger.info("Modify configs for %s", service_name)
            cls._config_version_dic[service_name] = cls.getCurrentServiceConfigVersion(service_name)
            for config_type, configs_dict in service_changes.items(): #pylint: disable=W0622
                logger.info("Step 1: Find the latest version of the config type that you need to update.")
                url = '/api/v1/clusters/' + cluster + '?fields=Clusters/desired_configs'
                logger.info("Requesting %s ", url)
                response = cls.http_get_request(url)
                json_data = jsonlib.loads(response.content)

                tag = json_data['Clusters']['desired_configs'][config_type]['tag']
                new_tag = "version" + str(uuid.uuid4())

                logger.info("Step 2: Read the config type with correct tag.")
                url = '/api/v1/clusters/' + cluster + '/configurations?type=' + config_type + '&tag=' + tag
                logger.info("Requesting %s ", url)
                response = cls.http_get_request(url)
                json_data = jsonlib.loads(response.content)
                for config in configs_dict.keys():
                    json_data['items'][0]['properties'][config] = configs_dict.get(config)
                json_data = '{"Clusters":{"desired_config":{' \
                                        '"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + \
                            str(jsonlib.dumps(json_data['items'][0]['properties'])) + '}}}'

                logger.info("Step 3: Save a new version of the config and apply it using one call.")
                url = '/api/v1/clusters/' + cluster
                logger.info("Requesting %s", url)
                response = cls.http_put_post_request(url, json_data, 'PUT')

                time.sleep(10)

        cls.restart_services_with_stale_configs()

    @classmethod
    def restore_configs_for_multiple_services(cls):
        logger.info("Restoring configs for services via Ambari")
        for k, v in cls._config_version_dic.iteritems():
            logger.info('Reset %s to ver. %s', k, v)
            cls.resetConfig(k, v)
        cls.restart_services_with_stale_configs()

    @classmethod
    def get_jwt_cookie(cls, url):
        if "com" not in url and "hwx.site" in url:
            ambari_url = url.split(":")[0] + ":" + url.split(":")[1] + ".com:" + url.split(":")[2]
        else:
            ambari_url = url
        ip = cls.get_ip_from_url(ambari_url)
        logger.info("GENERATING JWT COOKIE")
        if not cls.is_sso_enabled():
            return ""
        if ip in cls.token_map:
            return cls.token_map[ip]
        try:
            basic_auth = HTTPBasicAuth(cls.getAdminUsername(), cls.getAdminPassword())
            logger.info(cls.getAdminUsername())
            logger.info(cls.getAdminPassword())
            url = "%s?originalUrl=%s" % (Ambari.get_knox_sso_provider_url(ip), ambari_url)
            response = requests.get(url=url, auth=basic_auth, verify=False, allow_redirects=False)
            logger.info("URL : %s", url)
            logger.info(response)
            logger.info(response.status_code)
            logger.info("***")
            logger.info(response.cookies)
            hadoop_jwt_cookie = response.cookies.get_dict().get("hadoop-jwt")
            logger.info("GENERATED JWT COOKIE = %s", hadoop_jwt_cookie)
            cls.token_map[ip] = hadoop_jwt_cookie
            return hadoop_jwt_cookie
        except requests.exceptions.RequestException as e:
            logger.info("exception in http_get_request : %s", str(e))

    @classmethod
    def is_sso_enabled(cls):
        if cls._is_sso_enabled is None:
            gateway = Config.get('machine', 'GATEWAY')
            cmd = 'cat ' + cls._ambari_properties + ' | grep authentication.jwt.enabled'
            ADMIN_USER = Machine.getAdminUser()
            code, output = Machine.runas(ADMIN_USER, cmd, host=gateway, passwd=Machine.getAdminPasswd())
            logger.debug("Return code : %s", str(code))
            if output:
                cls._is_sso_enabled = output.strip('\n').split("=")[1] == "true"
            else:
                cls._is_sso_enabled = False
        return cls._is_sso_enabled

    @classmethod
    def get_knox_sso_provider_url(cls, ip):
        if cls.is_sso_enabled():
            cmd = 'cat ' + cls._ambari_properties + ' | grep authentication.jwt.providerUrl'
            ADMIN_USER = Machine.getAdminUser()
            code, output = Machine.runas(ADMIN_USER, cmd, host=ip, passwd=Machine.getAdminPasswd())
            logger.debug("Return code : %d ", code)
            return output.strip('\n').split("=")[1]
        return ""

    @classmethod
    def get_ip_from_url(cls, url):
        p = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
        m = re.search(p, url)
        return m.group('host')

    @classmethod
    def restart_service(cls, service, timeout=300):
        logger.info("------------------------- Restarting %s -------------------------", service)
        logger.info("Stopping %s ...", service)
        cls.start_stop_service(service=service, state="INSTALLED", waitForCompletion=True, timeout=timeout)
        logger.info("Starting %s ...", service)
        cls.start_stop_service(service=service, state="STARTED", waitForCompletion=True, timeout=timeout)

    @classmethod
    def refreshQueuesFromAmbari(cls, cluster=None, webURL=None, expected_to_fail=False):
        if cluster is None:
            cluster = cls.getClusterName()
        if webURL is None:
            webURL = Ambari.getWebUrl()
        else:
            cluster = cls.getClusterName(weburl=webURL)
        rm_host = Ambari.getHostsForComponent('RESOURCEMANAGER')
        if len(rm_host) == 2:
            activeRM = rm_host[0]
            standbyRM = rm_host[1]
            host = "%s,%s" % (activeRM, standbyRM)
        else:
            activeRM = rm_host[0]
            host = activeRM

        data = {
            "RequestInfo": {
                "context": "Refresh YARN Capacity Scheduler",
                "command": "REFRESHQUEUES",
                "parameters/forceRefreshConfigTags": "capacity-scheduler"
            },
            "Requests/resource_filters": [
                {
                    "service_name": "YARN",
                    "component_name": "RESOURCEMANAGER",
                    "hosts": host
                }
            ]
        }
        json = jsonlib.dumps(data)

        url = '/api/v1/clusters/%s/requests/' % (cluster)
        logger.info("Requesting %s", url)
        response = cls.http_put_post_request(url, json, 'POST')

        if response.status_code == 202:
            json_data = response.json()
            request_id = json_data['Requests']['id']
            status = cls.wait_until_request_complete(request_id, timeout=300, cluster=cluster, weburl=webURL)
            if expected_to_fail:
                assert status == 'FAILED', 'RefreshQueues expected to fail'
            else:
                assert status == 'COMPLETED', "Service %s: FAILED to COMPLETE" % ("REFRESHQUEUES")

    # Returns knox host
    @classmethod
    def getKnoxHosts(cls):
        ambari_host_1 = Config.get("multicluster", "AMBARI_GATEWAY1")
        ambari_host_2 = Config.get("multicluster", "AMBARI_GATEWAY2")

        cluster1_KnoxHosts = util.getKnoxHostFromGatewayIP(ambari_host_1)
        cluster2_KnoxHosts = util.getKnoxHostFromGatewayIP(ambari_host_2)

        # Temporary passing knox host for both HA and Non-HA
        is_ha_enabled = Config.get('machine', 'IS_HA_TEST')
        if is_ha_enabled == "yes":
            cluster1_LB_Host = ambari_host_1 + ':444'
            cluster2_LB_Host = ambari_host_2 + ':444'
            logger.info("Cluster1 LB Host: %s", cluster1_LB_Host)
            logger.info("Cluster2 LB Host: %s", cluster2_LB_Host)
            return cluster1_KnoxHosts, cluster2_KnoxHosts, cluster1_LB_Host, cluster2_LB_Host

        return cluster1_KnoxHosts, cluster2_KnoxHosts

    @classmethod
    def getStackVersion(cls, cluster=None):
        if cluster is None:
            cluster = cls.getClusterName(weburl=cls.getWebUrl(is_enc=cls.is_ambari_encrypted()))
        url = '/api/v1/clusters/' + cluster + '?fields=Clusters/desired_configs,Clusters/version'
        response = cls.http_get_request(url)
        json_data = jsonlib.loads(response.content)
        return json_data['Clusters']['version']
