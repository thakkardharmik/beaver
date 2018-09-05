#pylint: disable=wrong-import-order
#pylint: disable=ungrouped-imports
#pylint: disable=multiple-imports
#pylint: disable=old-style-class
#pylint: disable=singleton-comparison
#pylint: disable=bad-continuation
#pylint: disable=no-self-use
#pylint: disable=simplifiable-if-statement
#pylint: disable=unused-variable
#pylint: disable=unused-argument
#pylint: disable=line-too-long
#pylint: disable=redefined-builtin
#pylint: disable=protected-access
#pylint: disable=literal-comparison
#pylint: disable=superfluous-parens
#pylint: disable=bare-except
#pylint: disable=logging-not-lazy
from beaver.config import Config
import json
import os
import pycurl
from StringIO import StringIO as BytesIO
from beaver.machine import Machine
import uuid, time, copy
import requests
from requests.auth import HTTPBasicAuth
import sys, traceback, logging
from beaver.component.ambari import Ambari
from beaver.component.ambari_apilib import APICoreLib
from beaver.component.xa import Xa
from beaver.component.hadoop import Hadoop
from taskreporter.taskreporter import TaskReporter

apicorelib = None


class AmbariAPIUtil:
    def __init__(self, host='localhost', port=None, isHttps=False):
        if port is None:
            if (Hadoop.isEncrypted() or Ambari.is_ambari_encrypted() and Machine.isHumboldt() == False):
                port = 8443
                isHttps = True
            else:
                port = 8080
        if isHttps or self.isCloudbreak():
            self.baseUrl = 'https://' + host
        else:
            self.baseUrl = 'http://' + host

        if self.isCloudbreak():
            self.baseUrl = self.baseUrl + '/ambari'
        else:
            self.baseUrl = self.baseUrl + ':' + str(port)

        if Machine.isHumboldt():
            self.username_password = Config.get('ambari', 'AMBARI_USERNAME', 'admin') + ':HdpCli123!'
            ambari_gateway = Config.get('machine', 'GATEWAY').replace("-ssh", "")
            self.baseUrl = 'https://%s' % ambari_gateway
        elif Machine.getInstaller() == 'cloudbreak':
            self.username_password = Config.get('ambari', 'AMBARI_USERNAME', 'admin') + ':cloudbreak1'
        else:
            self.username_password = Config.get('ambari', 'AMBARI_USERNAME', 'admin'
                                                ) + ':' + Config.get('ambari', 'AMBARI_PASSWORD', 'admin')

        self.urlLogin = self.baseUrl + '#/login'
        self.urlGetClusters = self.baseUrl + '/api/v1/clusters'
        self.urlGetAmbClusters = self.baseUrl + '/api/v1/services'
        self.urlConfig = '/configurations'
        self.backupDataJson = dict()
        self.logger = logging.getLogger(__name__)

    global apicorelib
    DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
    uifrm_folder = "uifrm_old/uifrm"
    amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder, 'ambari.properties')
    if not os.path.isfile(amb_prop_file):
        uifrm_folder = "uifrm"
    amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder)
    apicorelib = APICoreLib(amb_prop_file)

    def isCloudbreak(self):
        if Machine.getInstaller() == 'cloudbreak':
            return True
        else:
            return False

    # get the clustername url using Ambari Rest call
    # Sample url returned will be like http://<host>:<port>/api/v1/clusters/cl1, here cl1 is the clustername
    @TaskReporter.report_test()
    def getClusterNameUrl(self):
        self.logger.info(self.urlGetClusters)
        hres, res = self.callPyCurlRequest(
            url=self.urlGetClusters, data=None, method='get', usernamepassword=self.username_password
        )
        d = json.loads(res)
        if Config.hasSection('hdc'):
            proxy_enabled = Config.get('hdc', 'USE_CLI')
            if proxy_enabled == 'yes':
                return str(d['items'][0]['href'])

        return Ambari.getWebUrl() + "/api/v1/clusters/" + Ambari.getClusterName()

    # get the clustername using Ambari Rest call
    # added to use cluster name as argument for configs.sh utility
    def getClusterName(self):
        return Ambari.getWebUrl() + "/api/v1/clusters/" + Ambari.getClusterName()

    # Returns a comma separated list of hosts where the specific component is installed
    def getServicesHost(self, component):
        host_list = Ambari.getHostsForComponent()
        hostList = ','.join([str(i) for i in host_list])
        return hostList

    # give input as the configuration type and propertyname, return the value of the property. Returns none if property not found.
    # Sample - getPropertyValue('ranger-admin-site','ranger.audit.solr.urls') returns the solr urls configured in ranger-admin-site
    def getPropertyValue(self, type, property):
        return apicorelib.get_service_config("", type, property)

    # A configuration might have got updated multiple times, so ambari maintains versions of it.
    # this method returns the 'tag' value for the latest version of the set of properties, which can used to build the full url to acces config data
    @TaskReporter.report_test()
    def getLatestVersionTag(self, url):
        #hres,resVer=self.callPyCurlRequest(url=url, data=None, method='get', usernamepassword=self.username_password)
        response = Ambari.http_get_request(url, "")
        d = json.loads(response._content)
        versions = list()
        tagVersions = list()
        for value in d['items']:
            versions.append(value['version'])
            tagVersions.append(str(value['version']) + ':' + value['tag'])
        versions.sort()
        len = versions.__len__()
        val = versions.pop(len - 1)
        tagVer = [s for s in tagVersions if str(val) + ':' in s]
        itms = list()
        itms = tagVer[0].split(':')
        itmLen = itms.__len__()
        latestTag = itms.pop(itmLen - 1)
        return latestTag

    # Returns a comma separated list of hosts where the specific component is installed
    def getComponentHosts(self, service, component):
        clust_name = Ambari.getClusterName()
        host_list = Ambari.getServiceHosts(service, component, cluster=clust_name, is_enc=Xa.isWireEncryptionOn())
        hostList = ','.join([str(i) for i in host_list])
        return hostList

    # Returns the state os service - INSTALLED,STARTED etc
    def getServiceState(self, service):
        return apicorelib.get_service_current_state(service)

    @TaskReporter.report_test()
    def modify_service_configs(self, serviceName, config_type, configs_dict, restart_service=True):
        # apicorelib.modify_service_configs(serviceName, config_type, configs_dict)
        # if restart_service == True:
        #     self.restart_service(serviceName)
        Ambari.modify_service_configs_and_restart(serviceName, config_type, configs_dict)
        baseurl = self.getClusterNameUrl()
        tag = self.getLatestVersionTag(baseurl + '/configurations?type=' + config_type)
        url = baseurl + '/configurations?type=' + config_type + '&tag=' + str(tag)
        response = Ambari.http_get_request(url, "")
        json_data = json.loads(response._content)
        if self.backupDataJson.get(serviceName) is None:
            self.backupDataJson[serviceName] = {}
            self.backupDataJson[serviceName][config_type] = copy.deepcopy(json_data)
        elif self.backupDataJson.get(serviceName).get(config_type) is None:

            self.backupDataJson[serviceName][config_type] = copy.deepcopy(json_data)

    def get_request_current_state(self, request_id):
        return apicorelib.get_request_current_state(request_id)

    def restart_service(self, service):
        apicorelib.restart_service(service)

    def start_stop_service(self, service, state):
        return apicorelib.start_stop_service(service, state)

    @TaskReporter.report_test()
    def stop_service(self, service):
        response = self.start_stop_service(service, 'INSTALLED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            print("Waiting on service " + service + " to stop")
            counter = 0
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'ABORTED':
                    print('service stop is aborted')
                    raise Exception('service stop is aborted!!')
                elif self.get_request_current_state(request_id) == 'FAILED':
                    print('service failed to stop')
                    raise Exception('service stop is failed!!')
                elif counter > 150:
                    print('operation exceeded the timeout')
                    raise Exception('timeout is exceeded!!')
                counter = counter + 1
                time.sleep(5)

    @TaskReporter.report_test()
    def start_service(self, service):
        response = self.start_stop_service(service, 'STARTED')
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            print("Waiting on service " + service + " to stop")
            counter = 0
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'ABORTED':
                    print('service start is aborted')
                    raise Exception('service start is aborted!!')
                elif self.get_request_current_state(request_id) == 'FAILED':
                    print('service failed to start')
                    raise Exception('service start is failed!!')
                elif counter > 150:
                    print('operation exceeded the timeout')
                    raise Exception('timeout is exceeded!!')
                counter = counter + 1
                time.sleep(5)

    @TaskReporter.report_test()
    def start_stop_service_on_host(self, host, component, state):
        url = self.getClusterNameUrl() + '/hosts/' + host + '/host_components/' + component
        data = '{"HostRoles": {"state": "' + state + '"}}'
        #response = self.http_put_post_request(url, data, method='put')
        response = Ambari.http_put_post_request(url, data, "PUT", "")
        print('response for the component: ' + component + ' state: ' + state + ' is: ' + str(response))
        if response.status_code is 202:
            json_data = json.loads(response._content)
            request_id = json_data['Requests']['id']
            print("Waiting for the component: " + component + " to be:" + state)
            counter = 0
            while not self.get_request_current_state(request_id) == 'COMPLETED':
                if self.get_request_current_state(request_id) == 'ABORTED':
                    print('service status change aborted!')
                    break
                elif self.get_request_current_state(request_id) == 'FAILED':
                    print('service status change failed!')
                    break
                elif counter > 100:
                    print('operation exceeded the timeout')
                    break
                counter = counter + 1
                time.sleep(5)
        return response

    @TaskReporter.report_test()
    def getServiceStateOnHost(self, host, component):
        url = self.getClusterNameUrl() + '/hosts/' + host + '/host_components/' + component
        #response = self.http_put_post_request(url,data=None, method='get')
        response = Ambari.http_get_request(url, "")
        json_data = json.loads(response._content)
        return json_data['HostRoles']['state']

    @TaskReporter.report_test()
    def restoreConf(self, config_type, serviceName, restart_service=True):
        baseurl = self.getClusterNameUrl()
        #tag =  self.getLatestVersionTag(baseurl + '/configurations?type=' + config_type)
        new_tag = "version" + str(uuid.uuid4())
        json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + '", "properties":' + str(
            json.dumps(self.backupDataJson[serviceName][config_type]['items'][0]['properties'])
        ) + '}}}'
        response = Ambari.http_put_post_request(baseurl, json_data, 'PUT', "")
        print("resonse for conf change after restore conf:" + str(response))
        if restart_service == True:
            self.restart_service(serviceName)

    # Return stack version
    def getVersion(self):
        return apicorelib.get_current_stack_version()

    def stop_service_on_host(self, host, component):
        print("Stopping component" + component + " on host:" + host)
        response = self.start_stop_service_on_host(host, component, 'INSTALLED')
        return response

    def start_service_on_host(self, host, component):
        print("Starting component" + component + " on host:" + host)
        response = self.start_stop_service_on_host(host, component, 'STARTED')
        return response

    #Invoke a curl request and return the header response and response

    @TaskReporter.report_test()
    def callPyCurlRequest(self, url, data, method, usernamepassword):

        try:
            buffer = BytesIO()
            header = BytesIO()

            # Creating PyCurl Requests
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            #c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json','Accept: application/json'])
            c.setopt(pycurl.USERPWD, usernamepassword)
            c.setopt(pycurl.VERBOSE, 0)
            c.setopt(pycurl.SSL_VERIFYPEER, False)
            c.setopt(pycurl.SSL_VERIFYHOST, False)
            c.setopt(c.WRITEFUNCTION, buffer.write)
            c.setopt(c.HEADERFUNCTION, header.write)
            # setting proper method and parameters
            if method == 'get':
                c.setopt(pycurl.HTTPGET, 1)
            elif method == 'post':
                c.setopt(c.POSTFIELDS, data)
            elif method == 'put':
                c.setopt(pycurl.CUSTOMREQUEST, "PUT")
                c.setopt(c.POSTFIELDS, str(data))
            elif method == 'delete':
                c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
                c.setopt(c.POSTFIELDS, str(data))
            else:
                print('xa_ambari_api_util  callCurlRequest : method is not get, post, put or delete')

            # making request
            c.perform()
            # getting response
            response = buffer.getvalue()

            headerResponse = header.getvalue()
            c.close()
            buffer.close()
            header.close()

            return headerResponse, response
        except:
            e = sys.exc_info()[0]
            self.logger.info("trace is : " + traceback.format_exc())

    @TaskReporter.report_test()
    def http_put_post_request(self, url, data, method):
        #self.logger.info(url)
        if (Machine.isHumboldt()):
            basic_auth = HTTPBasicAuth('admin', 'HdpCli123!')
        elif Machine.getInstaller() == 'cloudbreak':
            basic_auth = HTTPBasicAuth('admin', 'cloudbreak1')
        else:
            basic_auth = HTTPBasicAuth('admin', 'admin')
        if method == 'put':
            response = requests.put(
                url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
            )
            return response
        elif method == 'post':
            response = requests.post(
                url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
            )
            return response
        elif method == 'get':
            response = requests.get(
                url=url, data=data, auth=basic_auth, headers={'X-Requested-By': 'ambari'}, verify=False
            )
            return response
        else:
            return None
