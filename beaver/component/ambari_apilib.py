#pylint: disable=wrong-import-order
#pylint: disable=relative-import
#pylint: disable=old-style-class
#pylint: disable=consider-using-enumerate
#pylint: disable=protected-access
#pylint: disable=bad-continuation
#pylint: disable=unneeded-not
#pylint: disable=logging-not-lazy
#pylint: disable=literal-comparison
#pylint: disable=unused-argument
#pylint: disable=line-too-long
#pylint: disable=undefined-loop-variable
#pylint: disable=unused-variable
#pylint: disable=len-as-condition
#pylint: disable=superfluous-parens
#pylint: disable=logging-format-interpolation
#pylint: disable=bare-except
#pylint: disable=unreachable

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
import uuid
import requests
from requests.auth import HTTPBasicAuth
import time
import logging
import os
from ambari_commonlib import CommonLib
from ambari import Ambari
import sys


class APICoreLib:

    AmbariProperties = dict()

    MasterComponents = set(
        [
            "NameNode", "SNameNode", "ResourceManager", "App Timeline Server", "HistoryServer", "HBase Master",
            "Hive Metastore", "HiveServer3", "WebHCat Server", "Oozie Server", "Kafka Broker", "Knox Gateway",
            "ZooKeeper Server", "MySQL Server", "Nimbus", "DRPC Server", "Storm UI Server", "Storm REST API Server",
            "Falcon Server", "Metrics Collector", "Ranger Admin", "Ranger Usersync", "Spark History Server",
            "Accumulo GC", "Accumulo Master", "Accumulo Monitor", "Accumulo Tracer", "Atlas Server",
            "RANGER KMS SERVER"
        ]
    )

    def __init__(self, ambari_property_dir):
        if os.path.exists(ambari_property_dir):
            self.AmbariProperties = CommonLib.initialize_properties(ambari_property_dir)
            self.HOST_URL = CommonLib.get_host_uri(self.AmbariProperties)
            self.ambari_user = 'admin'
            self.ambari_pass = 'admin'
        else:
            self.AmbariProperties['CLUSTER_NAME'] = Ambari.getClusterName()
            self.HOST_URL = Ambari.getWebUrl()
            self.ambari_user = 'admin'
            self.ambari_pass = 'HdpCli123!'
        self.logger = logging.getLogger(__name__)  #None

        self.EXTERNAL_HOSTS = '/tmp/all_nodes'

    # This function gets all the internal hosts for the cluster
    def get_internal_hosts(self):
        lines = [line.rstrip('\n') for line in open(self.AmbariProperties.get('INTERNAL_HOSTS'))]
        return lines

    # This function gets all the external nodes for the cluster
    def get_external_hosts(self):
        externalHosts = [line.rstrip('\n') for line in open(self.EXTERNAL_HOSTS)]
        return externalHosts

    # This function returns the external host name for a given internal host
    def get_external_host(self, internalHost):
        internalHosts = self.get_internal_hosts()
        externalHosts = self.get_external_hosts()
        internal_external_host_map = dict()
        for i in range(0, len(internalHosts)):
            internal_external_host_map[internalHosts[i]] = externalHosts[i]

        return internal_external_host_map[internalHost]

    # This function executes the HTTP GET requests and returns the response
    def http_get_request(self, uri, **kwargs):
        host_url = kwargs.get('host_url', self.HOST_URL)
        url = host_url + uri
        #self.logger.info(url)
        #print(url)
        if Ambari.is_sso_enabled():
            response = requests.get(url=url, verify=False, cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)})
        else:
            basic_auth = HTTPBasicAuth(self.ambari_user, self.ambari_pass)
            response = requests.get(url=url, auth=basic_auth, verify=False)
        return response

    # This function executes the HTTP GET requests and returns the response
    def http_delete_request(self, uri, **kwargs):
        host_url = kwargs.get('host_url', self.HOST_URL)
        url = host_url + uri
        #self.logger.info(url)

        basic_auth = HTTPBasicAuth(self.ambari_user, self.ambari_pass)
        if Ambari.is_sso_enabled():
            response = requests.delete(
                url=url,
                verify=False,
                headers={'X-Requested-By': 'ambari'},
                cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)}
            )
        else:
            response = requests.delete(url=url, auth=basic_auth, verify=False, headers={'X-Requested-By': 'ambari'})
        return response

    # This function executes the HTTP PUT or POST requests based on the paramenters and returns the response
    def http_put_post_request(self, uri, data, requestType, **kwargs):
        host_url = kwargs.get('host_url', self.HOST_URL)
        contType = kwargs.get('contentType')
        url = str(host_url) + uri
        #self.logger.info(url)

        basic_auth = HTTPBasicAuth(self.ambari_user, self.ambari_pass)
        if requestType == 'PUT':
            if contType is not None:
                if Ambari.is_sso_enabled():
                    response = requests.put(
                        url=url,
                        data=data,
                        headers={
                            'X-Requested-By': 'ambari',
                            'content-type': contType
                        },
                        cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)},
                        verify=False
                    )
                else:
                    response = requests.put(
                        url=url,
                        data=data,
                        auth=basic_auth,
                        headers={
                            'X-Requested-By': 'ambari',
                            'content-type': contType
                        },
                        verify=False
                    )
            else:
                if Ambari.is_sso_enabled():
                    response = requests.put(
                        url=url,
                        data=data,
                        headers={'X-Requested-By': 'ambari'},
                        cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)},
                        verify=False
                    )
                else:
                    response = requests.put(
                        url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
                    )

            return response
        elif requestType == 'POST':
            if contType is not None:
                if Ambari.is_sso_enabled():
                    response = requests.post(
                        url=url,
                        data=data,
                        headers={
                            'X-Requested-By': 'ambari',
                            'content-type': contType
                        },
                        cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)},
                        verify=False
                    )
                else:
                    response = requests.post(
                        url=url,
                        data=data,
                        auth=basic_auth,
                        headers={
                            'X-Requested-By': 'ambari',
                            'content-type': contType
                        },
                        verify=False
                    )
            else:
                if Ambari.is_sso_enabled():
                    response = requests.post(
                        url=url,
                        data=data,
                        headers={'X-Requested-By': 'ambari'},
                        cookies={'hadoop-jwt': Ambari.get_jwt_cookie(url)},
                        verify=False
                    )
                else:
                    response = requests.post(
                        url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
                    )
            return response
        else:
            self.logger.info('ILLEGAL REQUEST')
            return None

    # This function returns a list of hosts for a given cluster
    def get_all_hosts(self):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")) + "/hosts"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        hosts = []
        for host in range(0, len(data['items'])):
            hosts.append(str(data['items'][host]['Hosts']["host_name"]))
        return hosts

    # This function returns a map of hosts and components for a given service
    def get_services_to_host_to_components_data(self, service):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/services?fields=components/host_components"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        serviceToHostDict = {}
        for i in range(0, len(data['items'])):
            serviceName = str(data['items'][i]['ServiceInfo']['service_name'])
            hosts = dict()
            for j in range(0, len(data['items'][i]['components'])):
                for k in range(0, len(data['items'][i]['components'][j]['host_components'])):
                    if not '_CLIENT' in str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']):
                        hostname = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['host_name']
                        )
                        if hostname in hosts:
                            components = hosts[hostname]
                            components.append(
                                str(
                                    data['items'][i]['components'][j]['host_components'][k]['HostRoles']
                                    ['component_name']
                                )
                            )
                        else:
                            components = []
                            components.append(
                                str(
                                    data['items'][i]['components'][j]['host_components'][k]['HostRoles']
                                    ['component_name']
                                )
                            )
                            hosts[hostname] = components
            serviceToHostDict[serviceName] = hosts
        return serviceToHostDict[service]

    # This function returns a map of hosts and components for a given service
    def get_services_to_components_to_host_data(self, service):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/services?fields=components/host_components"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        serviceToCompDict = {}
        for i in range(0, len(data['items'])):
            serviceName = str(data['items'][i]['ServiceInfo']['service_name'])
            components = dict()
            for j in range(0, len(data['items'][i]['components'])):
                for k in range(0, len(data['items'][i]['components'][j]['host_components'])):
                    if not '_CLIENT' in str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']):
                        hostname = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['host_name']
                        )
                        componentname = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']
                        )
                        if componentname in components:
                            hosts = components[componentname]
                            hosts.append(hostname)
                        else:
                            hosts = []
                            hosts.append(hostname)
                            components[componentname] = hosts
            serviceToCompDict[serviceName] = components
        return serviceToCompDict[service]

    # This function returns components for a given service
    def get_services_to_components_data(self, service):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/services?fields=components/host_components"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        ServiceToCompDict = {}
        for i in range(0, len(data['items'])):
            serviceName = str(data['items'][i]['ServiceInfo']['service_name'])
            for j in range(0, len(data['items'][i]['components'])):
                for k in range(0, len(data['items'][i]['components'][j]['host_components'])):
                    if not '_CLIENT' in str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']):
                        component = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']
                        )

                        if serviceName in ServiceToCompDict:
                            components = ServiceToCompDict[serviceName]
                            components.add(component)
                        else:
                            components = set()
                            components.add(component)
                            ServiceToCompDict[serviceName] = components
        return list(ServiceToCompDict[service])

    # This function returns components for a given host
    def get_hosts_to_components_data(self, host):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/services?fields=components/host_components"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        HostToCompDict = {}
        for i in range(0, len(data['items'])):
            for j in range(0, len(data['items'][i]['components'])):
                for k in range(0, len(data['items'][i]['components'][j]['host_components'])):
                    if not '_CLIENT' in str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']):
                        component = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['component_name']
                        )
                        hostname = str(
                            data['items'][i]['components'][j]['host_components'][k]['HostRoles']['host_name']
                        )
                        if hostname in HostToCompDict:
                            components = HostToCompDict[hostname]
                            components.add(component)
                        else:
                            components = set()
                            components.add(component)
                            HostToCompDict[hostname] = components
        return list(HostToCompDict[host])

    # This function gives the map of the master component of a given service to host
    def get_host_component_with_master(self, service):
        service_component_host = self.get_services_to_components_to_host_data(service)
        masterComponents = [x.replace(" ", "_").upper() for x in list(self.MasterComponents)]
        masterCompDict = dict()
        for component in service_component_host.keys():
            if masterComponents.__contains__(component):
                masterCompDict[component] = service_component_host[component]
        return masterCompDict

    # This function restarts all services
    def restart_all_services(self):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/services/'
        response = self.http_get_request(url)
        json_data1 = json.loads(response._content)
        for i in range(0, len(json_data1['items'])):
            service = json_data1['items'][i]['ServiceInfo']['service_name']
            self.logger.info("Restarting service : %s " % service)
            self.restart_service(service)

    #This function restarts a service
    def restart_service(self, service):
        services_failed_to_start = []
        services_failed_to_install = []
        response = self.start_stop_service(service, 'INSTALLED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            self.logger.info("Waiting for the service " + service + " to stop..")
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'FAILED':
                    services_failed_to_install.append(service)
                    break
                    #raise Exception("Installing service "+ service + " failed!")
                time.sleep(5)

        response = self.start_stop_service(service, 'STARTED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            self.logger.info("Waiting for the service " + service + " to start..")
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'FAILED':
                    services_failed_to_start.append(service)
                    break
                    #raise Exception("Starting service "+ service + " failed!")
                time.sleep(10)

    # This function starts or stop a given service and returns the response
    def start_stop_service(self, service, state, **kwargs):
        data = '{"RequestInfo": {"context": "WE API ' + state + ' ' + service + '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/services/' + service
        response = self.http_put_post_request(url, data, 'PUT', host_url=self.HOST_URL)
        return response

    # This function starts or stop a given component on a given host and returns the response
    def start_stop_component(self, component, host, state):
        data = '{"RequestInfo": {"context": "WE API ' + state + ' ' + component + ' on ' + host + '"},"HostRoles": {"state": "' + state + '"}}'
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/hosts/' + host + '/host_components/' + component
        response = self.http_put_post_request(url, data, 'PUT')
        if component == 'RANGER_ADMIN':
            print '------Ranger-sleep-time-start-----'
            time.sleep(60)
            print '------Ranger-sleep-time-end-------'
        return response._content

    # This function returns a mist of services and components which have stale configs (which need to be restarted)
    def get_services_components_with_stale_configs(self):
        url = '/api/v1/clusters/' + str(
            self.AmbariProperties.get('CLUSTER_NAME')
        ) + '/host_components?HostRoles/stale_configs=true&fields=HostRoles/service_name,HostRoles/state,HostRoles/host_name,HostRoles/stale_configs,&minimal_response=true'
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

    # This function gives the current state of the service
    def get_service_current_state(self, service):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/services/' + service + '?fields=ServiceInfo/state'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data['ServiceInfo']['state']

    # This function gives the current state of the component
    def get_component_current_state(self, component, host):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/hosts/' + host + '/host_components/' + component
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data['HostRoles']['state']

    # This function gives the current state of the service
    def get_request_current_state(self, request_id):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/requests/' + str(request_id)
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data['Requests']['request_status']

    def get_service_config(self, serviceName, config_type, config):
        # Step 1: Find the latest version of the config type that you need to update.
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        tag = json_data['Clusters']['desired_configs'][config_type]['tag']

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/configurations?type=' + config_type + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data['items'][0]['properties'][config]

    # This function validates the configurations of a cluster for given config type
    def validate_service_configs(self, serviceName, config_type, configs_dict):
        self.logger.info(
            "Validating the config changes for the service " + serviceName + " and config type " + config_type + " : "
        )
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        tag = json_data['Clusters']['desired_configs'][config_type]['tag']

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/configurations?type=' + config_type + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)

        for config in configs_dict.keys():
            if not json_data['items'][0]['properties'][config] == configs_dict.get(config):
                raise Exception("Config " + config + " did not modify")

        self.logger.info(
            "Validation passed for the config changes for the service " + serviceName + " and config type " +
            config_type + " : "
        )

    # This function modifies the configurations of a cluster for given config type and service
    def modify_service_configs(self, serviceName, config_type, configs_dict):
        # Step 1: Find the latest version of the config type that you need to update.
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        tag = json_data['Clusters']['desired_configs'][config_type]['tag']
        new_tag = "version" + str(uuid.uuid4())

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/configurations?type=' + config_type + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        try:
            for config in configs_dict.keys():
                json_data['items'][0]['properties'][config] = configs_dict.get(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
                json.dumps(json_data['items'][0]['properties'])
            ) + '}}}'
        except KeyError:
            properties = {}
            for config in configs_dict.keys():
                properties[config] = configs_dict.get(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
                json.dumps(properties)
            ) + '}}}'

        # Step 3: Save a new version of the config and apply it using one call
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME'))
        response = self.http_put_post_request(url, json_data, 'PUT')
        #self.logger.info(response._content)

        time.sleep(3)

    def modify_config(self, config_type, nodeSelection):
        # Step 1: Find the latest version of the config type that you need to update.
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        for filename in config_type.keys():
            filename = filename.split('.')[0]
        tag = json_data['Clusters']['desired_configs'][filename]['tag']
        version = json_data['Clusters']['desired_configs'][filename]['version']
        new_tag = "version" + str(uuid.uuid4())

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/configurations?type=' + filename + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        try:
            for fname in config_type.keys():
                for config in config_type.get(fname).keys():
                    json_data['items'][0]['properties'][config] = config_type.get(fname).get(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + filename + '", "properties":' + str(
                json.dumps(json_data['items'][0]['properties'])
            ) + ', "properties_attributes":' + str(json.dumps(json_data['items'][0]['properties_attributes'])) + '}}}'
        except KeyError:
            properties = {}
            for fname in config_type.keys():
                for config in config_type.get(fname).keys():
                    properties[config] = config_type.get(fname).get(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + fname + '", "properties":' + str(
                json.dumps(properties)
            ) + '}}}'
        # Step 3: Save a new version of the config and apply it using one call
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME'))
        response = self.http_put_post_request(url, json_data, 'PUT')

        time.sleep(3)
        self.restart_services_with_stale_configs()
        return version

    def restoreOrigConfig(self, serviceName, version):
        service_config_version_data = {}
        service_config_version_data['cluster'] = {}
        service_config_version_data['cluster']['desired_service_config_versions'] = {}
        service_config_version_data['cluster']['desired_service_config_versions']['service_config_version'] = version
        service_config_version_data['cluster']['desired_service_config_versions']['service_name'] = serviceName
        service_config_version_data['cluster']['desired_service_config_versions'
                                               ]['service_config_version_note'] = 'Restoring Original Configuration'
        json_data = json.dumps(service_config_version_data)
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME'))
        response = self.http_put_post_request(url, json_data, 'PUT')

    # This function finds the services with stale configs and restarts them
    def restart_services_with_stale_configs(self):
        # Step 4: Find which host components have stale configs (need to be restarted or have config refreshed)
        stale_service_components = self.get_services_components_with_stale_configs()

        services_to_restart = stale_service_components[0]
        host_components = stale_service_components[1]

        # Step 5: Restart all components or services to have the config change take effect
        services_failed_to_install = []
        services_failed_to_start = []
        for service in services_to_restart:
            self.restart_service(service)

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

    # This function modifies the configurations of a cluster for given config type and service and restarts the services with stale configs
    def modify_service_configs_and_resart(self, serviceName, config_type, configs_dict):

        # Step 1: Find the latest version of the config type that you need to update.
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '?fields=Clusters/desired_configs'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        tag = json_data['Clusters']['desired_configs'][config_type]['tag']
        new_tag = "version" + str(uuid.uuid4())
        stack = json_data['Clusters']['version']

        # Step 2: Read the config type with correct tag
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/configurations?type=' + config_type + '&tag=' + tag
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        for config in configs_dict.keys():
            json_data['items'][0]['properties'][config] = configs_dict.get(config)

        json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
            json.dumps(json_data['items'][0]['properties'])
        ) + '}}}'

        # Step 3: Save a new version of the config and apply it using one call
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME'))
        response = self.http_put_post_request(url, json_data, 'PUT')
        #self.logger.info(response._content)

        time.sleep(3)

        self.restart_services_with_stale_configs()

    # This function performs the service checks for all the installed services
    def service_checks(self):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/services/'
        response = self.http_get_request(url)
        json_data1 = json.loads(response._content)
        services_failed_service_check = []
        for i in range(0, len(json_data1['items'])):
            service = json_data1['items'][i]['ServiceInfo']['service_name']

            if service == 'AMBARI_METRICS':
                continue
            elif service == "ZOOKEEPER":
                service_check_command = 'ZOOKEEPER_QUORUM_SERVICE_CHECK'
            else:
                service_check_command = service + '_SERVICE_CHECK'
            self.logger.info('Beginning Service Check for ' + service + '...')
            body = '{\"RequestInfo\": {\"context\" :\"WE API ' + service + ' Service Check\", \"command\":\"' + service_check_command + '\"}, \"Requests/resource_filters\": [{\"service_name\": \"' + service + '\"}]}'
            url2 = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/requests'
            response = self.http_put_post_request(url2, body, 'POST')
            json_data = json.loads(response._content)
            if response.status_code is 202:
                while not self.get_request_current_state(json_data['Requests']['id']) == 'COMPLETED':
                    if (self.get_request_current_state(json_data['Requests']['id']) == 'FAILED'):
                        self.logger.info(service + " Service Check failed!")
                        services_failed_service_check.append(service)
                        #raise Exception (service + " Service Check failed!")
                        break
                    time.sleep(10)
                self.logger.info('Service Check for ' + service + ' Completed!')
            else:
                self.logger.info(service + " Service Check failed!")

        if len(services_failed_service_check) != 0:
            print("Following services failed Service Check:")
            for service1 in services_failed_service_check:
                print("- " + service1)

            raise Exception("Some Services Failed their Service Checks!!")

    # This function returns the CURRENT stack version
    def get_current_stack_version(self):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/stack_versions?ClusterStackVersions/state=CURRENT&fields=*"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        for i in range(0, len(data['items'])):
            for j in range(0, len(data['items'][i]['repository_versions'])):
                print str(data['items'][i]['repository_versions'][j]['href'])
                uri = str(data['items'][i]['repository_versions'][j]['href'])
                uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                                ) + "/stack_versions" + uri.split('stack_versions')[1]
                print uri

        result = self.http_get_request(uri)
        data = json.loads(result._content)

        return data['RepositoryVersions']['repository_version']

    def get_current_stack_name(self):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")
                                        ) + "/stack_versions?ClusterStackVersions/state=CURRENT&fields=*"
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        for i in range(0, len(data['items'])):
            for j in range(0, len(data['items'][i]['repository_versions'])):
                print "the reposirtory version " + str(data['items'][i]['repository_versions'][j]['href'])
                uri = str(data['items'][i]['repository_versions'][j]['href'])
                uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")) + "/stack_versions" + \
                      uri.split('stack_versions')[1]
                print uri
        result = self.http_get_request(uri)
        data = json.loads(result._content)
        return data['RepositoryVersions']['stack_name']

    # This function returns the Ambari version
    def get_current_ambari_version(self):
        uri = "/api/v1/services/AMBARI/components/AMBARI_SERVER"
        result = self.http_get_request(uri)
        data = json.loads(result._content)

        return data['RootServiceComponents']['component_version']

    # This function returns the request status of most recent upgrade
    def get_last_upgrade_status(self):
        uri = "/api/v1/clusters/" + str(self.AmbariProperties.get("CLUSTER_NAME")) + "/upgrades/"
        result = self.http_get_request(uri)
        data = json.loads(result._content)

        num_of_upgrades = len(data['items'])

        if num_of_upgrades > 0:
            last_upgrade_item = data['items'][num_of_upgrades - 1]  # Obtain the most recent upgrade
            last_upgrade_id = last_upgrade_item['Upgrade']['request_id']
            result = self.http_get_request(uri + str(last_upgrade_id))
            data = json.loads(result._content)
            return data['Upgrade']['request_status']
        else:
            self.logger.info("No upgrade history found")
            return None

    # Send a list of services eligible for restart to handle intermittent issues
    def retry_service_restart(self, retry_services):
        failed_tasks = self.get_failed_tasks()
        expected_command_strings = []
        for service in retry_services:
            expected_command_strings.append("{0} START".format(service))
        self.logger.debug("Default Service commands eligible for restart : {0}".format(expected_command_strings))
        restart_required = False
        for item in failed_tasks['items']:
            for task in item['tasks']:
                task_detail = task['Tasks']
                self.logger.debug("Failed command is : {0}".format(task_detail['command_detail']))
                if task_detail['command_detail'] in expected_command_strings:
                    restart_required = True
                    break
            if restart_required:
                break

        if restart_required:
            try:
                # Assuming first request is still being running. Abort the root request
                self.abort_all_requests_in_progress()
                self.start_all_services()
                return True
            except:
                e = sys.exc_info()
                self.logger.error("Abort All / Start All services failed with error: {0}".format(e))
        return False

    # This function gives the list of failed tasks for all requests
    def get_failed_tasks(self):
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')
                                        ) + '/requests?to=end&page_size=100&fields=tasks/*&tasks/Tasks/status=FAILED'
        response = self.http_get_request(url)
        json_data = json.loads(response._content)
        return json_data

    # This function gives the list of failed tasks for a given request id
    def get_failed_tasks_in_request(self, request_id):
        url = '/api/v1/clusters/' + str(
            self.AmbariProperties.get('CLUSTER_NAME')
        ) + '/requests/{0}?to=end&page_size=100&fields=tasks/*&tasks/Tasks/status=FAILED'
        response = self.http_get_request(url.format(request_id))
        json_data = json.loads(response._content)
        return json_data

    def start_all_services(self):
        self.logger.info("Issuing a Start All as 1st Start services request failed")
        data = '{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"' + str(
            self.AmbariProperties.get('CLUSTER_NAME')
        ) + '"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/services/'
        response = self.http_put_post_request(url, data, 'PUT', host_url=self.HOST_URL)
        json_data = json.loads(response._content)
        if response.status_code is 202:
            request_id = json_data['Requests']['id']
            self.logger.info("Waiting for all the services to Start..")
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'FAILED':
                    raise Exception("START all services failed Req id : {0}".format(request_id))
                    break
                time.sleep(10)
        else:
            self.logger.error(
                "START ALL Services failed with status code : {0} \n response message {1}".format(
                    response.status_code, json_data
                )
            )

    # This function aborts given request Id and returns the response
    def abort_request(self, request_id, **kwargs):
        data = '{"Requests":{"request_status":"ABORTED","abort_reason":"Aborted by DEPLOYNG"}}'
        url = '/api/v1/clusters/' + str(self.AmbariProperties.get('CLUSTER_NAME')) + '/requests/{0}'.format(request_id)
        response = self.http_put_post_request(url, data, 'PUT', host_url=self.HOST_URL)
        if response.status_code == 200:
            time.sleep(10)
        else:
            self.logger.error(
                "Abort request for request Id {0} failed with error {1}".format(request_id, response.status_code)
            )
            if response.json() is not None:
                self.logger.error(
                    "Abort request for request Id {0} failed with error message {1}".format(
                        request_id, response.json()
                    )
                )

    def abort_all_requests_in_progress(self):
        url = '/api/v1/clusters/' + str(
            self.AmbariProperties.get('CLUSTER_NAME')
        ) + '/requests?fields=Requests/request_status&Requests/request_status.in(IN_PROGRESS)'
        response = self.http_get_request(url, host_url=self.HOST_URL)
        json_data = json.loads(response._content)
        if response.status_code == 200:
            if len(json_data['items']) > 0:
                self.logger.info("Aborting all requests in IN_PROGRESS status")
                for i in range(0, len(json_data['items'])):
                    request_id = json_data['items'][i]['Requests']['id']
                    self.logger.debug("Aborting request : {0} ".format(request_id))
                    self.abort_request(request_id)
            else:
                self.logger.debug("No requests in progress. So Skipping Abort All")
        else:
            self.logger.error("Abort requests failed with error {0}".format(response.status_code))
            if response.json() is not None:
                self.logger.error("Abort requests failed with error message {0}".format(response.json()))
