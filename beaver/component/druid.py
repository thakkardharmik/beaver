#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import base64
import collections
import datetime
import json
import logging
import os, re, time, requests

from beaver import configUtils
from beaver import util
from beaver.component.hadoop import Hadoop, YARN, HDFS
from beaver.config import Config
from beaver.machine import Machine
from beaver.component.ambari import Ambari
from requests.auth import HTTPBasicAuth
import json as jsonlib
import urllib2, base64, socket, uuid
from beaver import util
from taskreporter.taskreporter import TaskReporter
'''
Description:
Test API to work with Druid 

Prerequisite:
Druid deployed and running
'''

logger = logging.getLogger(__name__)
HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')


class Druid:
    _conf_dir = Config.get('druid', 'DRUID_CONF_DIR')

    @classmethod
    def getDruidHome(cls):
        '''
        Returns Druid_HOME
        '''
        return Config.get('druid', 'DRUID_HOME')

    @classmethod
    def getDruidUser(cls):
        '''
        Returns Druid user
        '''
        return Config.get('druid', 'DRUID_USER')

    @classmethod
    @TaskReporter.report_test()
    def getDruidLogDir(cls, logoutput=False):
        '''
        Returns Druid log directory (String).
        '''
        druidLogDir = cls.getConfig("druid-env", "druid_log_dir")
        logger.info("Druid.getDruidLogDir returns %s" % druidLogDir)

        if logoutput:
            logger.info("Druid.getDruidLogDir returns %s" % druidLogDir)
        return druidLogDir

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        '''
        Returns Druid user
        '''
        jarDir = os.path.join(cls.getDruidHome(), 'lib')
        logger.info(jarDir)
        files = util.findMatchingFiles(jarDir, "druid-server-*.jar")
        p = re.compile('druid-server-(\S+).jar')
        m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    @TaskReporter.report_test()
    def getDatabaseFlavor(cls):
        properties = Ambari.getConfig('druid-common', service='DRUID')
        connectURI = properties['druid.metadata.storage.connector.connectURI']
        if connectURI.find("oracle") != -1:
            return "oracle"
        elif connectURI.find("postgresql") != -1:
            m = re.search("jdbc:postgresql://(.*):.*", connectURI)
            dbHost = Machine.getfqdn()
            if m and m.group(1):
                dbHost = m.group(1)
            dbVersion = Machine.getDBVersion('postgres', host=dbHost)
            if dbVersion:
                return "postgres-%s" % dbVersion
            else:
                return "postgres"
        elif connectURI.find("derby") != -1:
            return "derby"
        elif connectURI.find("mysql") != -1:
            return "mysql"
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getDruidComponentHost(cls, component, service='DRUID', cluster=None):
        if cluster == None:
            cluster = 'cl1'
        host_names = []
        url = "%s/api/v1/clusters/%s/services/%s/components/%s" % (Ambari.getWebUrl(), cluster, service, component)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get config versions."
        json = response.json()
        host_components = json['host_components']
        for i in range(0, len(host_components)):
            host_names.append(host_components[i]['HostRoles']['host_name'])
        return host_names

    @classmethod
    @TaskReporter.report_test()
    def getConfig(cls, type, property_name, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
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
        config = last['properties'][property_name]
        return config

    @classmethod
    @TaskReporter.report_test()
    def getTypeConfig(cls, type, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
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
        #url = "%s/api/v1/clusters/%s/configurations?type=%s&tag=version1" % ( weburl, cluster, type )
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        assert response.status_code == 200, "Failed to get config."
        json = response.json()
        items = json['items']
        last = items[len(items) - 1]
        config = last['properties']
        return config

    @classmethod
    @TaskReporter.report_test()
    def setConfig(cls, type, config, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
        tag = "version-%s" % str(uuid.uuid1())
        data = {'Clusters': {'desired_config': {'type': type, 'tag': tag, 'properties': config}}}
        json = jsonlib.dumps(data)
        url = "%s/api/v1/clusters/%s" % (weburl, cluster)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.put(url=url, headers=head, auth=auth, data=json, verify=False)
        assert response.status_code == 200, "Failed to set config."

    # This function gives the current state of the service using request ID
    @classmethod
    @TaskReporter.report_test()
    def get_request_current_state(cls, request_id, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s/requests/%s" % (weburl, cluster, request_id)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        response = requests.get(url=url, headers=head, auth=auth, verify=False)
        json_data = json.loads(response._content)
        return json_data['Requests']['request_status']

    #This function restarts a service
    @classmethod
    @TaskReporter.report_test()
    def restart_service(cls, service):
        services_failed_to_start = []
        response = cls.start_stop_service(service, 'INSTALLED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + service + " to stop..")
            while not cls.get_request_current_state(request_id) == 'COMPLETED':
                if cls.get_request_current_state(request_id) == 'FAILED':
                    services_failed_to_start.append(service)
                    logger.info("FAILED to restart service " + service)
                    break
                time.sleep(5)

        response = cls.start_stop_service(service, 'STARTED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + service + " to start..")
            while not cls.get_request_current_state(request_id) == 'COMPLETED':
                if cls.get_request_current_state(request_id) == 'FAILED':
                    services_failed_to_start.append(service)
                    break
                time.sleep(10)

    # This function starts or stop a given service and returns the response
    @classmethod
    @TaskReporter.report_test()
    def start_stop_service(cls, service, state, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s/services/%s" % (weburl, cluster, service)
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{"RequestInfo": {"context": "DRUID API ' + state + ' ' + service + '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert ( response.status_code == 200 or response.status_code == 202 ),\
            "Failed to start/stop service %s , status=%d" % ( service, response.status_code )
        return response

    @classmethod
    @TaskReporter.report_test()
    def startComponent(cls, component, component_host, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, component_host, component)
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{ "HostRoles": { "state": "STARTED" } }'
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert (response.status_code == 200 or response.status_code == 202
                ), "Failed to start component %s , status=%d" % (component, response.status_code)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + component + " to start..")
            while not cls.get_request_current_state(request_id) == 'COMPLETED':
                if cls.get_request_current_state(request_id) == 'FAILED':
                    break
                time.sleep(10)

    @classmethod
    @TaskReporter.report_test()
    def stopComponent(cls, component, component_host, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        weburl = Ambari.getWebUrl()
        url = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % (weburl, cluster, component_host, component)
        head = {'X-Requested-By': 'ambari', 'Content-Type': 'text/plain'}
        data = '{ "HostRoles": { "state": "INSTALLED" } }'
        auth = HTTPBasicAuth(Ambari.getAdminUsername(), Ambari.getAdminPassword())
        response = requests.put(url, data=data, headers=head, auth=auth, verify=False)
        assert (response.status_code == 200 or response.status_code == 202
                ), "Failed to stop component %s , status=%d" % (component, response.status_code)
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            logger.info("Waiting for the service " + component + " to stop..")
            while not cls.get_request_current_state(request_id) == 'COMPLETED':
                if cls.get_request_current_state(request_id) == 'FAILED':
                    break
                time.sleep(10)

    @classmethod
    @TaskReporter.report_test()
    def getKerbPrincipalfromKeyTab(cls, keytab, cluster=None):
        if cluster == None:
            cluster = 'cl1'
        cmd = "%s -k -t %s " % ("klist", keytab)
        exit_code, output = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        kerb_principal = str(((output.split(os.linesep)[-1]).split(' ')[-1]))
        logger.info("Principal is " + kerb_principal)
        return kerb_principal.strip()
