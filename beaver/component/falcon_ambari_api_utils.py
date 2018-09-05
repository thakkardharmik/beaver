#
#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
from beaver.component.hadoop import Hadoop, YARN, HDFS
from beaver.config import Config
import json, logging
from beaver.machine import Machine
import time, requests, os
from beaver.component.ambari import Ambari
import json as jsonlib
from requests.auth import HTTPBasicAuth
import urllib2, base64, socket, uuid
from beaver import util
from beaver.component.hbase import HBase

logger = logging.getLogger(__name__)
hdfs_user = HDFS.getHDFSUser()
job_user = Config.getEnv('USER')


class FalconAmbariAPIUtil:
    def __init__(self):
        pass

    # The following functions are related to the Ambari REST API

    @classmethod
    def getConfig(cls, host, type, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/configurations?type=%s" % (weburl, cluster, type)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get config versions."
        json = response.json()
        items = json['items']
        last = items[len(items) - 1]
        tag = last['tag']
        url = "%s/api/v1/clusters/%s/configurations?type=%s&tag=%s" % (weburl, cluster, type, tag)
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get config."
        json = response.json()
        items = json['items']
        last = items[len(items) - 1]
        config = last['properties']
        return config

    @classmethod
    def setConfig(cls, host, type, config, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        tag = "version-%s" % str(uuid.uuid1())
        data = {'Clusters': {'desired_config': {'type': type, 'tag': tag, 'properties': config}}}
        json = jsonlib.dumps(data)
        url = "%s/api/v1/clusters/%s" % (weburl, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.put(url=url, headers=head, auth=auth, data=json, verify=False)
        assert response.status_code == 200, "Failed to set config."

    # This function restarts all services
    @classmethod
    def restart_all_services(cls, host, type, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/services" % (weburl, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        json_data = json.loads(response._content)
        for i in range(0, len(json_data['items'])):
            service = json_data['items'][i]['ServiceInfo']['service_name']
            logger.info("Restarting service : %s " % service)
            cls.restart_service(service, host)

    #This function restarts a service
    @classmethod
    def restart_service(cls, service, host, cluster=None):
        services_failed_to_start = []
        response = cls.start_stop_service(host, service, 'INSTALLED', cluster)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + service + " to stop..")
            while not cls.get_request_current_state(host, request_id, cluster) == 'COMPLETED':
                if cls.get_request_current_state(host, request_id, cluster) == 'FAILED':
                    services_failed_to_start.append(service)
                    logger.info("FAILED to restart service " + service)
                    break
                time.sleep(5)

        response = cls.start_stop_service(host, service, 'STARTED', cluster)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + service + " to start..")
            while not cls.get_request_current_state(host, request_id, cluster) == 'COMPLETED':
                if cls.get_request_current_state(host, request_id, cluster) == 'FAILED':
                    services_failed_to_start.append(service)
                    break
                time.sleep(10)

    @classmethod
    def restart_mc_services(cls, host, cluster=None):
        services = ["HDFS", "MAPREDUCE2", "YARN", "OOZIE", "FALCON"]
        if HBase.isInstalled():
            services = ["HDFS", "MAPREDUCE2", "YARN", "OOZIE", "FALCON", "HBASE", "HIVE"]
        for service in services:
            cls.restart_service(service, host, cluster)
        return

    @classmethod
    def restart_core_services(cls, host, cluster=None):
        services = ["HDFS", "MAPREDUCE2", "YARN", "HIVE"]
        for service in services:
            cls.restart_service(service, host, cluster)
        return

    # This function gives the current state of the service using request ID
    @classmethod
    def get_request_current_state(cls, host, request_id, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/requests/%s" % (weburl, cluster, request_id)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        json_data = json.loads(response._content)
        return json_data['Requests']['request_status']

    # This function starts or stop a given service and returns the response
    @classmethod
    def start_stop_service(cls, host, service, state, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/services/%s" % (weburl, cluster, service)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{"RequestInfo": {"context": "FALCON API ' + state + ' ' + service + '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert ( response.status_code == 200 or response.status_code == 202 ),\
            "Failed to start/stop service %s on host %s, status=%d" % ( service, host, response.status_code )
        return response

    @classmethod
    def startComponent(cls, host, component, component_host, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, component_host, component)
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{ "HostRoles": { "state": "STARTED" } }'
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert (response.status_code == 200 or response.status_code == 202
                ), "Failed to start component %s on host %s, status=%d" % (component, host, response.status_code)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + component + " to start..")
            while not cls.get_request_current_state(host, request_id, cluster) == 'COMPLETED':
                if cls.get_request_current_state(host, request_id, cluster) == 'FAILED':
                    break
                time.sleep(10)
        logger.info("Completed the NameNode Start using REST API")

    @classmethod
    def stopComponent(cls, host, component, component_host, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, component_host, component)
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{ "HostRoles": { "state": "INSTALLED" } }'
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert (response.status_code == 200 or response.status_code == 202
                ), "Failed to stop component %s on host %s, status=%d" % (component, host, response.status_code)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + component + " to stop..")
            while not cls.get_request_current_state(host, request_id, cluster) == 'COMPLETED':
                if cls.get_request_current_state(host, request_id, cluster) == 'FAILED':
                    break
                time.sleep(10)

    @classmethod
    def restartRemoteActiveNN(cls, wait=10, host=None):

        if host == None:
            host = Config.get("falcon", "HOST2")

        active_namenode = cls.getActiveNN(host, "NAMENODE")
        logger.info("Current Active NameNode Before Killing %s" % active_namenode)
        Hadoop.killService('namenode', hdfs_user, active_namenode)
        cls.stopComponent(host, "NAMENODE", active_namenode)
        time.sleep(wait)
        cls.startComponent(host, "NAMENODE", active_namenode)
        logger.info("Active NameNode After Restart %s" % active_namenode)
        return

    @classmethod
    def restartRemoteActiveRM(cls, wait=20, host=None):
        if host == None:
            host = Config.get("falcon", "HOST2")

        active_RM = cls.getActiveRM(host, "RESOURCEMANAGER")
        logger.info("Current Active ResourceManager Before Killing %s" % active_RM)
        service = 'resourcemanager'
        yarn_user = Config.get('hadoop', 'YARN_USER')
        pidFile = None
        if not Machine.isWindows():
            pidFile = os.path.join(
                Config.get('hadoop', 'YARN_PID_DIR'), yarn_user, 'yarn-%s-%s.pid' % (yarn_user, service)
            )

        Hadoop.killService(service, yarn_user, active_RM, pidFile=pidFile)
        cls.stopComponent(host, "RESOURCEMANAGER", active_RM)
        time.sleep(wait)
        cls.startComponent(host, "RESOURCEMANAGER", active_RM)
        active_namenode = cls.getActiveRM(host, "RESOURCEMANAGER")
        logger.info("Active ResourceManager After Restart %s" % active_namenode)
        return

    # Method to get the Active Resourcemanager
    @classmethod
    def getActiveRM(cls, host, component, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        url = "%s/api/v1/clusters/%s/host_components?HostRoles/component_name=%s" % (weburl, cluster, component)
        auth = HTTPBasicAuth("admin", "admin")
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get config versions."
        json = response.json()
        items = json['items']
        lenght = len(items)
        for index in range(lenght):
            last = items[index]
            active_rm = last['HostRoles']['host_name']
            print active_rm
            rmurl = "http://%s:8088/ws/v1/cluster/info" % active_rm
            auth = HTTPBasicAuth("admin", "admin")
            head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
            rmresponse = requests.get(url=rmurl, headers=head, auth=auth, verify=False)
            assert rmresponse.status_code == 200, "Failed to get config versions."
            rmjson = rmresponse.json()
            rmstate = rmjson['clusterInfo']['haState']
            if rmstate == "ACTIVE":
                return active_rm

    # Method to get the Active Namenode
    @classmethod
    def getActiveNN(cls, host, component, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = cls.getweburl(host)
        count = 5
        length = 1
        while count > 0 and length >= 1:
            url = "%s/api/v1/clusters/%s/host_components?HostRoles/component_name=%s&metrics/dfs/FSNamesystem/HAState=active" % (
                weburl, cluster, component
            )
            auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
            head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
            response = requests.get(url=url, headers=head, auth=auth, verify=False)
            assert response.status_code == 200, "Failed to get config versions."
            json = response.json()
            items = json['items']
            length = len(items)
            count = count - 1
            time.sleep(10)
        last = items[-1]
        active_nn = last['HostRoles']['host_name']
        return active_nn

    @classmethod
    def getServiceHosts(cls, host, service, component, cluster='cl1'):
        hosts = []
        url = "%s/api/v1/clusters/%s/services/%s/components/%s" % (cls.getweburl(host), cluster, service, component)
        retcode, retdata, retheaders = Ambari.performRESTCall(url)
        if retcode == 200:
            jsoncontent = util.getJSON(retdata)
            try:
                hosts = [hc['HostRoles']['host_name'] for hc in jsoncontent['host_components']]
            except:
                hosts = []
        return hosts

    @classmethod
    def getweburl(cls, host):
        if Hadoop.isEncrypted():
            weburl = "https://%s:8443" % host
        else:
            weburl = "http://%s:8080" % host
        return weburl
