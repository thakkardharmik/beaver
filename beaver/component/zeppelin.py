#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#

import copy
import json
import logging
import os
import pprint
import random
import re
import signal
import socket
import threading
import time
import uuid
import ConfigParser
import StringIO
import requests
from requests.auth import HTTPBasicAuth
from selenium import webdriver
from taskreporter.taskreporter import TaskReporter
from beaver import util
from beaver.component.ambari import Ambari
from beaver.component.hadoop import Hadoop, HDFS, YARN
from beaver.component.hive import Hive
from beaver.component.spark import Spark
from beaver.component.xa import Xa
from beaver.config import Config
from beaver.machine import Machine
from beaver.seleniumHDP import Selenium
import tests.zeppelin.data.zeppelin_xpaths

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

HDP_VERSION = ".".join(Ambari.getHDPVersion().split(".")[-4:-2])
ZEPPELIN_XPATHS = copy.deepcopy(tests.zeppelin.data.zeppelin_xpaths.ZEPPELIN_XPATHS)


class TimeoutException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


@TaskReporter.report_test()
def implement_timeout(secs, custom_logger=None, success_msg=None, failure_msg=None, timeout_exc=None):
    """
    This decorator allows to implement 'timeout' functionality in the paradigm like 'run/retry unless successful'
    In order to do this , this decorator starts the decorated method in a separate thread and monitors the thread.
    It then waits for the decorated method to finish successfully or the timeout to occur, whichever comes earlier
    In case of timeout, it sets the flag _stop=True for the underlying thread

    The decorated method is desired to have a continuous while loop which breaks
      1) when it is successful OR
      2) when it detects current thread's _stop = True which is being set externally by this decorator in timeout
      event.
    The decorated method will be like
      while not threading.currentThread().is_stopped():
          try:
              #do stuff
          except:
              #log exception and let it retry via above while loop
          else:
              #break out
    If decorated method detects _stop = True, it should come out of the continuous loop

    :param secs (int) : desired timeout value in seconds
    :param logger (:obj:`logger`, optional) :  logger object
    :param success_msg (str, optional) : message to be logged in case of successful completion
    :param failure_msg (str, optional) : message to be logged in case of timeout event
    :return:
    list [bool, obj] where 1st parameter indicates if decorated function finished successfully within time limit
    and the 2nd parameter is the result object returned by decorated method
    """

    def real_implement_timeout(func):
        def wrapper(*args, **kwargs):
            class StoppableThread(threading.Thread):
                def __init__(self, result):
                    threading.Thread.__init__(self)
                    self._stop = False
                    self.result = result

                def terminate(self):
                    self._stop = True

                def is_stopped(self):
                    return self._stop

                def run(self):
                    ret = False
                    try:
                        ret = func(*args, **kwargs)
                    except Exception, e:
                        if custom_logger:
                            custom_logger.error("runtime error: %s" % e)
                    finally:
                        self.result.append(ret)

            result = []
            worker = StoppableThread(result)
            worker.start()
            worker.join(secs)

            if worker.is_alive():
                if timeout_exc:
                    if failure_msg:
                        raise timeout_exc(failure_msg)
                    else:
                        raise timeout_exc("Timeout!!")
                else:
                    if custom_logger and failure_msg:
                        custom_logger.error(failure_msg)
                    worker.terminate()
                    worker.join()
                    return [False, result[0]]
            else:
                if custom_logger and success_msg and result[0]:
                    custom_logger.info(success_msg)
                return [True, result[0]]

        return wrapper

    return real_implement_timeout


@TaskReporter.report_test()
def synchronized(func):
    """
    This decorator allows to synchronize call to the decorated method among different threads
    :param func: method to be synchronized
    :return:
    """
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)

    return synced_func


class ZeppelinAmbariAPIUtil(object):
    """
    This class contains methods for ambari operations
    """

    def __init__(self):
        """
        Initializes ZeppelinAmbariAPIUtil object to handle ambari operations
        :param is_https (bool, optional): is https enabled for ambari server, default value = False
        """

        if Machine.getInstaller() == 'cloudbreak':
            self.base_url = Ambari.getWebUrl()
        else:
            if Machine.isHumboldt():
                self.host = 'headnodehost'
            else:
                self.host = 'localhost'

            self.is_https = bool(Hadoop.isEncrypted() or Ambari.is_ambari_encrypted())

            if self.is_https:
                self.base_url = 'https://' + self.host
                self.port = 8443
            else:
                self.base_url = 'http://' + self.host
                self.port = 8080

            self.base_url = self.base_url + ':' + str(self.port)

        if Machine.isHumboldt():
            self.basic_auth = HTTPBasicAuth('admin', 'HdpCli123!')
        elif Machine.getInstaller() == 'cloudbreak':
            self.basic_auth = HTTPBasicAuth('admin', 'cloudbreak1')
        else:
            self.basic_auth = HTTPBasicAuth('admin', 'admin')
        self.latest_version_tags_cache = {}
        self.url_cluster_name = None
        self.cluster_name = None
        self.backup_json = None

    def __http_req(self, url, method, data=None, log=True):
        """
        private method to send http request to ambari server, times out in maximum 30 secs
        :param url (str):  url
        :param data (str, optional): json string for data, default value = None
        :param method (str) : type of http method , one of : 'get','post','put'
        :param log (bool, optional): parameter to enable logging from this method, default value = True
        :return: (dict) response dict object
        """
        if log:
            logger.info("Sending '%s' request to url : %s", method, url)
        response = None

        max_attempts = 5
        num_attempts = 0
        wait_time = 5

        while num_attempts < max_attempts:
            try:
                if method == 'put':
                    response = requests.put(
                        url=url,
                        data=data,
                        auth=self.basic_auth,
                        headers={'X-Requested-By': 'ambari'},
                        verify=False,
                        timeout=30
                    )
                elif method == 'post':
                    response = requests.post(
                        url=url,
                        data=data,
                        auth=self.basic_auth,
                        headers={'X-Requested-By': 'ambari'},
                        verify=False,
                        timeout=30
                    )
                elif method == 'get':
                    response = requests.get(
                        url=url,
                        data=data,
                        auth=self.basic_auth,
                        headers={'X-Requested-By': 'ambari'},
                        verify=False,
                        timeout=30
                    )
                elif method == 'delete':
                    response = requests.delete(
                        url=url,
                        data=data,
                        auth=self.basic_auth,
                        headers={'X-Requested-By': 'ambari'},
                        verify=False,
                        timeout=30
                    )
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    logger.info(
                        "Attempt %s failed -> Exception while sending '%s' request to url : %s, msg: %s.."
                        "Retrying after %s seconds", (num_attempts + 1), method, url, e, wait_time
                    )
                    time.sleep(wait_time)
                else:
                    logger.info(
                        "Attempt %s failed -> Exception while sending '%s' request to url : %s, msg: %s",
                        (num_attempts + 1), method, url, e
                    )
                    logger.error(
                        "All %s attempts exhausted while sending '%s' request to url : %s", max_attempts, method, url
                    )
                num_attempts += 1
            else:
                break

        assert response is not None, "No response received for '%s' request to url : %s" % (method, url)
        return response

    @classmethod
    def generate_random_version_tag(cls):
        return "version%s" % int(time.time())

    @TaskReporter.report_test()
    def get_cluster_name(self):
        """
         method to find the cluster name
         :return: cluster_name (str) : name of the cluster
         """
        if not self.cluster_name:
            res = self.__http_req(url=self.base_url + "/api/v1/clusters", data=None, method='get')
            assert res.status_code == 200, "get_cluster_name failure, status_code != 200"
            self.cluster_name = res.json()['items'][0]['Clusters']['cluster_name']
        return self.cluster_name

    def __kerberos_sign_in(self):
        if Machine.isADMIT():
            credentials = '{ "Credential" : { "principal" : "ambariqatest@HWQE.HORTONWORKS.COM", "key" : ' \
                        '"Horton!#works", "type" : "temporary" } }'
        else:
            credentials = '{ "Credential" : { "principal" : "admin/admin@EXAMPLE.COM", "key" : ' \
                              '"admin", "type" : "temporary" } }'

        res = self.__http_req(
            url=self.__get_cluster_name_url() + "/credentials/kdc.admin.credential", method='post', data=credentials
        )
        try:
            assert res.status_code == 201, "Failed to do kerberos sign in to Ambari"
        except Exception:
            if "A credential with the alias of kdc.admin.credential already exists" in res.json()['message']:
                pass
        return True

    def __get_cluster_name_url(self):
        """
        private method to find the cluster name URL i.e. of the type /api/v1/clusters/<cluster_name>
        :return: (str) : cluster name URL
        """
        if not self.url_cluster_name:
            res = self.__http_req(url=self.base_url + '/api/v1/clusters', data=None, method='get')
            assert res.status_code == 200, "__get_cluster_name_url failure, status_code != 200"
            d = res.json()
            #if Machine.isCloud():
            self.cluster_name = d['items'][0]['Clusters']['cluster_name']
            self.url_cluster_name = self.base_url + '/api/v1/clusters/' + self.cluster_name
            #else:
            #    self.url_cluster_name = str(d['items'][0]['href'])
            assert self.url_cluster_name, "__get_cluster_name_url failure"
        return self.url_cluster_name

    def __get_request_current_state(self, request_id):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/requests/' + str(request_id), data=None, method='get', log=False
        )
        assert res.status_code == 200, "__get_request_current_state Failure for request_id: %s, status_code != 200" \
                                       % request_id
        try:
            return res.json()['Requests']['request_status']
        except KeyError:
            return None

    def __get_latest_version_tag(self, prop_type):
        if prop_type not in self.latest_version_tags_cache.keys():
            res = self.__http_req(
                url=self.__get_cluster_name_url() + '?fields=Clusters/desired_configs', data=None, method='get'
            )
            assert res.status_code == 200, "__get_latest_version_tag failed, status_code != 200"
            try:
                self.latest_version_tags_cache[prop_type] = res.json()['Clusters']['desired_configs'][prop_type]['tag']
            except KeyError:
                return None
        return self.latest_version_tags_cache[prop_type]

    @TaskReporter.report_test()
    def add_config_type_to_cluster(self, prop_type, config_dict):
        version_tag = ZeppelinAmbariAPIUtil.generate_random_version_tag()
        post_json_data = '{"type":"' + prop_type + '", "tag": "' + version_tag + \
                         '", "properties": ' + str(json.dumps(config_dict)) + '}'
        post = self.__http_req(
            url=self.__get_cluster_name_url() + "/configurations", method='post', data=post_json_data
        )
        assert post.status_code == 201, "Failure to post config type: '%s' to cluster" % prop_type

        add_json_data = '{"Clusters": {"desired_configs": {"type": "' + \
                        prop_type + '", "tag":"' + version_tag + '" }}}'
        add = self.__http_req(url=self.__get_cluster_name_url(), method='put', data=add_json_data)
        assert add.status_code == 200, "Failure to add config type: '%s' to cluster" % prop_type
        return True

    @TaskReporter.report_test()
    def get_current_configs_dict_for_service(self, service):
        configs_url = self.__get_cluster_name_url() + '/configurations/service_config_versions?service_name.in(' \
                      + service + ')&is_current=true'
        res = self.__http_req(url=configs_url, data=None, method='get')
        assert res.status_code == 200, "get_current_configs_dict_for_service failure, status_code != 200"
        configs_dict = {}
        for config in res.json()['items'][0]['configurations']:
            configs_dict[config['type']] = config['properties']
        return configs_dict

    @TaskReporter.report_test()
    def get_config_type(self, prop_type):
        property_url = self.__get_cluster_name_url() + '/configurations?type=' + prop_type + '&tag=' + \
                       str(self.__get_latest_version_tag(prop_type))
        res = self.__http_req(url=property_url, data=None, method='get')
        assert res.status_code == 200, "get_config_type failure, status_code != 200"
        try:
            return res.json()['items'][0]['properties']
        except Exception:
            return None

    @TaskReporter.report_test()
    def get_property_value(self, prop_type, service_property):
        """

        :param type (str) : ambari config section type for e.g. zeppelin-env or
        :param property (str) : name of the property
        :return:
        """
        conf_type = self.get_config_type(prop_type)
        assert conf_type, "Failed to read config type: %s from ambari" % prop_type
        try:
            return conf_type[service_property]
        except KeyError:
            return None

    @TaskReporter.report_test()
    def modify_service_configs(self, service, config_type, configs_dict):
        old_tag = self.__get_latest_version_tag(config_type)
        assert old_tag, "Failed to find latest version tag for service: %s, type: %s" % (service, config_type)
        new_tag = "version" + str(uuid.uuid4())

        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/configurations?type=' + config_type + '&tag=' + old_tag,
            data=None,
            method='get'
        )
        assert res.status_code == 200, "failed to read the original config: %s, status_code != 200" % config_type
        json_data = res.json()
        if self.backup_json is None:
            self.backup_json = copy.deepcopy(json_data)
        try:
            for config in configs_dict.keys():
                if configs_dict.get(config) is not None:
                    json_data['items'][0]['properties'][config] = configs_dict.get(config)
                else:
                    if config in json_data['items'][0]['properties']:
                        json_data['items'][0]['properties'].pop(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + \
                        '", "properties":' + str(json.dumps(json_data['items'][0]['properties']))+'}}}'
        except KeyError:
            properties = {}
            for config in configs_dict.keys():
                properties[config] = configs_dict.get(config)
            json_data = '{"Clusters":{"desired_config":{"tag":"' + new_tag + '", "type":"' + config_type + \
                        '", "properties":'+str(json.dumps(properties))+'}}}'

        res = self.__http_req(url=self.__get_cluster_name_url(), data=json_data, method='put')
        assert res.status_code == 200, "Failure to post modified config: %s, status_code != 200" % config_type
        if config_type in self.latest_version_tags_cache.keys():
            del self.latest_version_tags_cache[config_type]
        return True

    @TaskReporter.report_test()
    def is_service_absent(self, service):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service + '?fields=ServiceInfo/state',
            data=None,
            method='get'
        )
        try:
            if "The requested resource doesn't exist" in res.json()['message']:
                return True
        except KeyError:
            return False
        return False

    @TaskReporter.report_test()
    def get_service_state(self, service):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service + '?fields=ServiceInfo/state',
            data=None,
            method='get'
        )
        assert res.status_code == 200, "get_service_state failure, status_code != 200"
        try:
            return res.json()['ServiceInfo']['state']
        except KeyError:
            return None

    @TaskReporter.report_test()
    def get_service_version(self, service):
        res = self.__http_req(
            url=self.base_url + '/api/v1/stacks/HDP/versions/' + HDP_VERSION + "/services/" + service,
            data=None,
            method='get'
        )
        if res.status_code == 200:
            return res.json()['StackServices']['service_version']
        else:
            return ""

    def __start_stop_service(self, service, state):
        if state == 'INSTALLED':
            op = 'Stop'
        elif state == 'STARTED':
            op = 'Start'
        data = '{"RequestInfo": {"context": "REST API '+ op + ' ' + service + \
               '"}, "Body" : {"ServiceInfo": {"state": "' + state + '"}}}'
        res = self.__http_req(url=self.__get_cluster_name_url() + '/services/' + service, data=data, method='put')
        if res.status_code != 202:
            logger.error("start_stop_service Failure, status_code ! =202")
            return None
        else:
            try:
                return res.json()['Requests']['id']
            except KeyError:
                return None

    @TaskReporter.report_test()
    def stop_service(self, service, stop_wait=15, timeout=360, timeout_exc=None):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Service: %s stopped successfully" % service,
            failure_msg='Timeout occurred while stopping service: %s via Ambari' % service,
            timeout_exc=timeout_exc
        )
        def real_stop_service(self, service):
            stop_request_id = self.__start_stop_service(service, 'INSTALLED')
            if stop_request_id:
                logger.info("Waiting for the service %s to stop", service)
                while not self.__get_request_current_state(stop_request_id) == 'COMPLETED' and \
                        not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(stop_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(stop_request_id) == 'COMPLETED'
            else:
                logger.error("Failed to get the request id for stopping service: %s", service)
                return False
            return False

        success = False not in real_stop_service(self, service)
        if success:
            time.sleep(stop_wait)
        return success

    @TaskReporter.report_test()
    def start_service(self, service, start_wait=15, timeout=480, timeout_exc=None):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Service: %s started successfully" % service,
            failure_msg='Timeout occurred while starting service: %s via Ambari' % service,
            timeout_exc=timeout_exc
        )
        def real_start_service(self, service):
            start_request_id = self.__start_stop_service(service, 'STARTED')
            if start_request_id:
                logger.info("Waiting for the service %s to start", service)
                while not self.__get_request_current_state(start_request_id) == 'COMPLETED' \
                        and not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(start_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(start_request_id) == 'COMPLETED'
            else:
                logger.error("Failed to get the request id for starting service: %s", service)
                return False
            return False

        success = False not in real_start_service(self, service)
        if success:
            time.sleep(start_wait)
        return success

    @TaskReporter.report_test()
    def restart_service(self, service, restart_wait=15):
        if self.stop_service(service, stop_wait=restart_wait):
            return self.start_service(service, start_wait=0)
        else:
            return False

    @TaskReporter.report_test()
    def delete_service(self, service):
        if self.get_service_state(service) == "STARTED":
            stopped = self.stop_service(service)
        else:
            stopped = True
        assert stopped, "Failed to stop service: %s before deleting" % service
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service, data=None, method='delete', log=True
        )
        assert res.status_code == 200, "delete_service Failure for service: %s, status_code != 200" % service
        return True

    @TaskReporter.report_test()
    def install_service(self, service, configs_dict, component_to_host_map, timeout=300, timeout_exc=None, start=True):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Service: %s installed successfully" % service,
            failure_msg='Timeout occurred while installing service: %s via Ambari' % service,
            timeout_exc=timeout_exc
        )
        def real_install_service(self, service):
            # install service
            install_request_id = None
            res = self.__http_req(
                url=self.__get_cluster_name_url() + "/services/" + service,
                method='put',
                data='{"ServiceInfo": {"state" : "INSTALLED"}}'
            )
            assert res.status_code == 202, "Failure in ambari while putting service: %s to installed state, " \
                                           "status_code!=202" % service
            try:
                install_request_id = res.json()['Requests']['id']
            except Exception:
                logger.warn("No request id available while putting service: %s to installed state", service)

            if install_request_id:
                logger.info("Waiting for the service %s to install", component)
                while not self.__get_request_current_state(install_request_id) == 'COMPLETED' \
                        and not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(install_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(install_request_id) == 'COMPLETED'
            else:
                return True
            return False

        # add service and components to the cluster
        assert self.add_service_to_cluster(service), "Failed to add service: %s to cluster" % service
        for component in component_to_host_map.keys():
            logger.info("adding component: %s to service: %s", component, service)
            assert self.add_component_to_service(service, component), "Failed to add component: %s to service: %s" \
                                                                      % (component, service)

        # re-create configuration types and add them to cluster
        for config_type, configs in configs_dict.items():
            assert self.add_config_type_to_cluster(config_type, configs), \
                "Failed to add config type: '%s' to cluster after adding service: %s and its components: %s" % \
                (config_type, service, component_to_host_map.keys())

        # add components to respective hosts
        for component, hosts in component_to_host_map.items():
            hosts = hosts if isinstance(hosts, list) else [hosts]
            for host in hosts:
                logger.info("adding component: %s to host: %s", component, host)
                assert self.add_component_to_host(component, host), "Failed to add component: %s to host: %s" % \
                                                                             (component, host)

        # if cluster is secured, sign in with kerberos credentials
        if Hadoop.isSecure():
            assert self.__kerberos_sign_in(), "Failed to do kerberos sign in to Ambari"

        success = False not in real_install_service(self, service)
        assert success, "Failed to install service: %s" % service

        # start the service if required
        if start:
            assert self.start_service(service), "Failed to start service: %s after installing" % service
        return True

    @TaskReporter.report_test()
    def add_service_to_cluster(self, service):
        if self.is_service_absent(service):
            res = self.__http_req(
                url=self.__get_cluster_name_url() + '/services/' + service, data=None, method='post', log=True
            )
            assert res.status_code == 201, "add_service Failure for service: %s, status_code != 201" % service
        return True

    @TaskReporter.report_test()
    def get_component_hosts(self, service, service_component):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service + '/components/' + service_component,
            data=None,
            method='get'
        )
        assert res.status_code == 200, "get_component_hosts failure"
        try:
            host_list = ""
            for host_component in res.json()['host_components']:
                if not host_list:
                    host_list = host_component['HostRoles']['host_name']
                else:
                    host_list = host_list + "," + host_component['HostRoles']['host_name']
            return host_list
        except KeyError:
            return None

    @TaskReporter.report_test()
    def get_component_state(self, service_component, service=None, host=None):
        if service:
            component_hosts_str = self.get_component_hosts(service, service_component)
            if component_hosts_str:
                component_hosts = component_hosts_str.split(',')
            else:
                component_hosts = []
        else:
            component_hosts = []

        if host and component_hosts:
            if host in component_hosts:
                url = self.__get_cluster_name_url() + "/hosts/" + host + "/host_components/" + service_component
                res = self.__http_req(url=url, data=None, method='get')
                if res.status_code == 200:
                    return res.json()['HostRoles']['state']
                else:
                    logger.error("get_component_host failure for component: %s on host: %s", service_component, host)
                    return None
            else:
                logger.error(
                    "get_component_host failure Component: %s is not present on host: %s", service_component, host
                )
                return None
        elif component_hosts and not host:
            status = {}
            for fqdn in component_hosts:
                url = self.__get_cluster_name_url() + "/hosts/" + fqdn + "/host_components/" + service_component
                res = self.__http_req(url=url, data=None, method='get')
                if res.status_code == 200:
                    status[fqdn] = res.json()['HostRoles']['state']
                else:
                    logger.warn(
                        "get_component_host failure for component: %s on host: %s, returning None", service_component,
                        host
                    )
                    status[fqdn] = None
            if len(status.keys()) == 1:
                return status.values()[0]
            else:
                return status
        elif host:
            url = self.__get_cluster_name_url() + "/hosts/" + host + "/host_components/" + service_component
            res = self.__http_req(url=url, data=None, method='get')
            if res.status_code == 200:
                return res.json()['HostRoles']['state']
            else:
                logger.warn(
                    "get_component_host failure for component: %s on host: %s, returning None", service_component, host
                )
                return None
        return False

    @TaskReporter.report_test()
    def is_component_absent_from_service(self, service, component):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service + '/components/' + component,
            data=None,
            method='get'
        )
        try:
            if res.status_code != 200 or "The requested resource doesn't exist" in res.json()['message']:
                return True
        except KeyError:
            return False
        return False

    @TaskReporter.report_test()
    def is_component_absent_from_host(self, component, host):
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/hosts/' + host + '/host_components/' + component,
            data=None,
            method='get'
        )
        try:
            if res.status_code != 200 or "The requested resource doesn't exist" in res.json()['message']:
                return True
        except KeyError:
            return False
        return False

    def __start_stop_component(self, service, component, state):
        if state == 'INSTALLED':
            op = 'Stop'
        elif state == 'STARTED':
            op = 'Start'
        data = '{"RequestInfo": {"context": "REST API '+ op + ' ' + component + \
               '"}, "Body" : {"ServiceComponentInfo": {"state": "' + state + '"}}}'
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/services/' + service + '/components/' + component,
            data=data,
            method='put'
        )
        if res.status_code != 202:
            logger.error("start_stop_service_component Failure, status_code ! =202")
            return None
        else:
            try:
                return res.json()['Requests']['id']
            except KeyError:
                return None

    @TaskReporter.report_test()
    def stop_component(self, service, component, stop_wait=15, timeout=360, timeout_exc=None):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Component: %s stopped successfully" % component,
            failure_msg='Timeout occurred while stopping component: %s via Ambari' % component,
            timeout_exc=timeout_exc
        )
        def real_stop_component(self, service, component):
            stop_request_id = self.__start_stop_component(service, component, 'INSTALLED')
            if stop_request_id:
                logger.info("Waiting for the component %s to stop", component)
                while not self.__get_request_current_state(stop_request_id) == 'COMPLETED' \
                        and not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(stop_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(stop_request_id) == 'COMPLETED'
            else:
                logger.error("Failed to get the request id for stopping component: %s", component)
                return False
            return False

        success = False not in real_stop_component(self, service, component)
        if success:
            time.sleep(stop_wait)
        return success

    @TaskReporter.report_test()
    def start_component(self, service, component, start_wait=15, timeout=360, timeout_exc=None):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Component: %s started successfully" % component,
            failure_msg='Timeout occurred while starting component: %s via Ambari' % component,
            timeout_exc=timeout_exc
        )
        def real_start_component(self, service, component):
            start_request_id = self.__start_stop_component(service, component, 'STARTED')
            if start_request_id:
                logger.info("Waiting for the component %s to start", component)
                while not self.__get_request_current_state(start_request_id) == 'COMPLETED' \
                        and not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(start_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(start_request_id) == 'COMPLETED'
            else:
                logger.error("Failed to get the request id for starting component: %s", component)
                return False
            return False

        success = False not in real_start_component(self, service, component)
        if success:
            time.sleep(start_wait)
        return success

    @TaskReporter.report_test()
    def restart_component(self, service, component, restart_wait=15):
        if self.stop_component(service, component, stop_wait=restart_wait):
            return self.start_component(service, component, start_wait=0)
        else:
            return False

    @TaskReporter.report_test()
    def delete_component_from_host(self, service, component, host):
        # will remove component only from host and not from service
        if self.get_component_state(component, host=host) == "STARTED":
            stopped = self.stop_component(service, component)
        else:
            stopped = True
        assert stopped, "Failed to stop component: %s before deleting" % component
        res = self.__http_req(
            url=self.__get_cluster_name_url() + "/hosts/" + host + "/host_components/" + component,
            data=None,
            method='delete',
            log=True
        )
        assert res.status_code == 200, "delete_component_from_host Failure for component: %s on host: %s, " \
                                       "status_code != 200" % (component, host)
        return True

    @TaskReporter.report_test()
    def delete_component_from_service(self, service, component):
        # will remove component only from host and not from service
        ret = self.get_component_state(component, service=service)
        states = [ret] if not isinstance(ret, list) else [ret]
        any_started = [s == 'STARTED' for s in states]
        if any(any_started):
            stopped = self.stop_component(component)
        else:
            stopped = True
        assert stopped, "Failed to stop component: %s before deleting" % component
        res = self.__http_req(
            url=self.__get_cluster_name_url() + "/services/" + service + "/components/" + component,
            data=None,
            method='delete',
            log=True
        )
        assert res.status_code == 200, "delete_component_from_service Failure for component: %s , " \
                                       "status_code != 200" % component
        return True

    @TaskReporter.report_test()
    def add_component_to_service(self, service, component):
        assert not self.is_service_absent(service), \
            "add_component_to_service Failure for component: %s, service: %s does not exist" % (component, service)
        if self.is_component_absent_from_service(service, component):
            res = self.__http_req(
                url=self.__get_cluster_name_url() + '/services/' + service + '/components/' + component,
                data=None,
                method='post',
                log=True
            )
            assert res.status_code == 201, \
                "add_component_to_service Failure for component: %s onto service: %s, status_code != 201" % \
                (component, service)
            return True
        else:
            return True

    @TaskReporter.report_test()
    def add_component_to_host(self, component, host):
        json_data = '{"host_components" : [{"HostRoles":{"component_name":"' + component + '"}}] }'
        res = self.__http_req(
            url=self.__get_cluster_name_url() + '/hosts?Hosts/host_name=' + host, data=json_data, method='post'
        )
        assert res.status_code == 201, "add_component_to_host Failure for component: %s onto host: %s, status_code " \
                                           "!= 201" % (component, host)
        return True

    @TaskReporter.report_test()
    def install_component_to_host(self, component, service, host, timeout=300, timeout_exc=None, start=True):
        @implement_timeout(
            timeout,
            custom_logger=logger,
            success_msg="Component: %s installed successfully" % component,
            failure_msg='Timeout occurred while installing component: %s via Ambari' % component,
            timeout_exc=timeout_exc
        )
        def real_install_component(self, host, service, component):
            try:
                if self.is_component_absent_from_service(service, component):
                    assert self.add_component_to_service(
                        service, component
                    ), "Failed to add component: %s to service: %s" % (component, service)
                if self.is_component_absent_from_host(component, host):
                    assert self.add_component_to_host(component, host),\
                        "Failed to add component: %s to host: %s" % (component, host)
            except Exception, e:
                logger.error(e.message)
                return False

            install_component_url = self.__get_cluster_name_url() + "/hosts/" + host + "/host_components/" + component
            #add_res = self.__http_req(url=add_component_url, method='post', data=None)
            #if add_res.status_code == 201:
            install_data = '{"RequestInfo": {"context": "REST API INSTALL ' + component + \
                           '"}, "Body" : {"HostRoles": {"state": "INSTALLED"}}}'
            install_res = self.__http_req(url=install_component_url, method='put', data=install_data)
            if install_res.status_code == 202:
                install_request_id = install_res.json()['Requests']['id']
                while not self.__get_request_current_state(install_request_id) == 'COMPLETED' \
                        and not threading.currentThread().is_stopped():
                    if self.__get_request_current_state(install_request_id) == 'FAILED':
                        return False
                    time.sleep(2)
                return self.__get_request_current_state(install_request_id) == 'COMPLETED'
            else:
                logger.error("Failed to get the request id for installing component: %s", component)
                return False
            return False

        if Hadoop.isSecure():
            assert self.__kerberos_sign_in(), "Failed to do kerberos sign-in with Ambari " \
                                              "before installing component: %s" % component
        success = False not in real_install_component(self, host, service, component)
        if success:
            if start:
                started = self.start_component(service, component)
                return started
        return success


class AppUserGetter(threading.Thread):
    def __init__(self, thread_id, thread_name, timeout, users):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.old_running_app_ids = [app.split()[0] for app in YARN.getApplicationList("RUNNING", user='yarn')]
        self.thread_name = thread_name
        self.timeout = timeout
        self.users = users
        self.new_apps = []

    def thread_name_decorator(self, msg):
        return "thread_name : %s | %s" % (self.thread_name, msg)

    def run(self):
        start_time = time.time()
        curr_running_apps = YARN.getApplicationList("RUNNING", user='yarn')
        newly_started_app = [app for app in curr_running_apps if app.split()[0] not in self.old_running_app_ids]
        while (time.time() - start_time) < self.timeout and not newly_started_app:
            logger.info("Sleeping for 5 seconds for new app user to become available")
            time.sleep(5)
            curr_running_apps = YARN.getApplicationList("RUNNING", user='yarn')
            newly_started_app = [app for app in curr_running_apps if app.split()[0] not in self.old_running_app_ids]

        if newly_started_app:
            filtered_app_list = [app for app in newly_started_app if app.split()[2] == 'SPARK']
            self.new_apps.append(newly_started_app)
            if filtered_app_list:
                self.users.append(filtered_app_list[0].split()[3])
            else:
                self.users.append(None)
        else:
            self.users.append(None)


class ZeppelinTestUtils(object):
    def __init__(self):
        pass

    @classmethod
    @TaskReporter.report_test()
    def setup_for_livy2_knox_proxy(cls):
        knox_truststore_password = 'knoxsecret'
        knox_keystore_path = os.path.join("usr", "hdp", "current", "knox-server", "data", "security", "keystores")
        cacert_path = os.path.join(Config.get('machine', 'DEPLOYED_JAVA_HOME'), 'jre', 'lib', 'security', 'cacerts')
        cacert_password = 'changeit'
        ambari_connector = ZeppelinAmbariAPIUtil()

        knox_hosts_str = ambari_connector.get_component_hosts('KNOX', 'KNOX_GATEWAY')
        assert knox_hosts_str, "Failed to get knox_hosts str while enabling knox proxy for livy2 interpreter"
        knox_hosts = sorted(knox_hosts_str.split(','))
        zeppelin_host = ZeppelinServer.get_ip_address()

        for knox_host in knox_hosts:
            logger.info("Importing knox host: %s certificate to cacert on zeppelin host: %s", knox_host, zeppelin_host)
            # export knox gateway.jks cert
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="keytool -exportcert -alias gateway-identity -storepass %s -file %s -keystore %s" % (
                    knox_truststore_password, os.path.join(knox_keystore_path, "knox_%s.crt" % knox_host),
                    os.path.join(knox_keystore_path, "gateway.jks")
                ),
                host=knox_host,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )

            # copy from knox host to gateway node
            Machine.copyToLocal(
                user=Machine.getAdminUser(),
                host=knox_host,
                srcpath=os.path.join(knox_keystore_path, "knox_%s.crt" % knox_host),
                destpath="/tmp",
                passwd=Machine.getAdminPasswd()
            )

            # copy from gateway node to zeppelin node
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=zeppelin_host,
                srcpath=os.path.join('tmp', "knox_%s.crt" % knox_host),
                destpath="/tmp",
                passwd=Machine.getAdminPasswd()
            )

            # import into zeppelin host's cacert
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="keytool -import -file %s -keystore %s -storepass %s -noprompt" %
                (os.path.join('tmp', "knox_%s.crt" % knox_host), cacert_path, cacert_password),
                host=zeppelin_host,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )

        logger.info("Modifying livy2 superusers property to add knox user")
        livy2_superusers = ambari_connector.get_property_value('livy2-conf', 'livy.superusers')
        assert livy2_superusers, "Failed to read original livy.superusers property valur for livy2-conf"

        livy2_superusers = livy2_superusers + "," + Config.get('knox', 'KNOX_USER')

        ambari_connector.modify_service_configs(
            'SPARK2', 'livy2-conf', {'livy2-conf': {
                'livy.superusers': livy2_superusers
            }}
        )

    @classmethod
    def is_sparkllap_split(cls):
        component = util.get_TESTSUITE_COMPONENT()
        return component.lower() == "zeppelinspark2llap"

    @classmethod
    @TaskReporter.report_test()
    def is_python_gte_27_on_zeppelin_host(cls):
        ret, version = Machine.runas(
            user=Machine.getAdminUser(),
            cmd="python --version",
            host=ZeppelinServer.get_ip_address(),
            passwd=Machine.getAdminPasswd()
        )
        assert ret == 0, "Failed to find python version on Zeppelin Host"
        logger.info("Default Python version on Zeppelin Host: %s", version)
        return version >= "Python 2.7"

    @classmethod
    @TaskReporter.report_test()
    def get_random_host_from_cluster(cls, exclude_hosts=None):
        if not exclude_hosts:
            exclude_hosts = []
        all_internal_nodes = util.getAllInternalNodes()
        exclude_hosts_fqdn = []
        for host in exclude_hosts:
            if util.isIP(host):
                exclude_hosts_fqdn.append(socket.gethostbyaddr(host)[0])
            else:
                exclude_hosts_fqdn.append(host)

        all_internal_nodes = [n for n in all_internal_nodes if n not in exclude_hosts_fqdn]
        assert all_internal_nodes, "Can not find a random host from cluster"
        return all_internal_nodes[random.randint(0, len(all_internal_nodes) - 1)]

    @classmethod
    @TaskReporter.report_test()
    def move_livy_to_another_node(cls, spark_version, host=None):
        ambari_connector = ZeppelinAmbariAPIUtil()
        if spark_version == 1:
            spark_service_name = 'SPARK'
            livy_comp_name = 'LIVY_SERVER'
        elif spark_version == 2:
            spark_service_name = 'SPARK2'
            livy_comp_name = 'LIVY2_SERVER'

        livy_hosts = ambari_connector.get_component_hosts(spark_service_name, livy_comp_name).split(',')
        assert len(livy_hosts) == 1, "move_livy_to_another_node requires only one %s instance" % livy_comp_name
        livy_host = livy_hosts[0]
        host_to_move = None

        if not host:
            host_to_move = cls.get_random_host_from_cluster(exclude_hosts=livy_hosts)
        else:
            if util.isIP(host):
                try:
                    host_to_move = socket.gethostbyaddr(host)[0]
                except Exception:
                    pass
            else:
                host_to_move = host
        assert host_to_move, "Could not find fqdn of the host to which %s is to be moved" % livy_comp_name

        # install yarn, spark, spark2 and hdfs clients on this host if not present
        clients = {'YARN_CLIENT': 'YARN', 'SPARK2_CLIENT': 'SPARK2', 'HDFS_CLIENT': 'HDFS'}
        for client, service in clients.items():
            if ambari_connector.is_component_absent_from_host(component=client, host=host_to_move):
                logger.info("Installing %s on host %s", client, host_to_move)
                assert ambari_connector.install_component_to_host(component=client, service=service,
                                                                  host=host_to_move, start=False),\
                    "Failed to install %s on host: %s" % (client, host_to_move)

        assert ambari_connector.stop_component(service=spark_service_name, component=livy_comp_name), \
            "Failed to stop %s before moving from host: '%s' to host: '%s'" % (livy_comp_name, livy_host, host_to_move)
        assert ambari_connector.delete_component_from_host(service=spark_service_name, component=livy_comp_name,
                                                           host=livy_host), \
            "Failed to delete %s from host: '%s' before moving to host:'%s'"%(livy_comp_name, livy_host, host_to_move)
        assert ambari_connector.install_component_to_host(component=livy_comp_name, service=spark_service_name,
                                                          host=host_to_move, start=True), \
            "Failed to re-install %s on host: '%s'" % (livy_comp_name, host_to_move)
        return True

    @classmethod
    @TaskReporter.report_test()
    def get_spark_thrift_server_jdbc_url(cls, spark_version):
        ambari_connector = ZeppelinAmbariAPIUtil()
        if spark_version == 1:
            spark_service_name = 'SPARK'
            sts_comp_name = 'SPARK_THRIFTSERVER'
            spark_hive_site_config = 'spark-hive-site-override'
        elif spark_version == 2:
            spark_service_name = 'SPARK2'
            sts_comp_name = 'SPARK2_THRIFTSERVER'
            spark_hive_site_config = 'spark2-hive-site-override'

        try:
            sts_host = ambari_connector.get_component_hosts(spark_service_name, sts_comp_name)
        except Exception:
            return None
        else:
            transport_mode = ambari_connector.get_property_value(spark_hive_site_config, 'hive.server2.transport.mode')
            if transport_mode == 'http':
                sts_port = ambari_connector.get_property_value(spark_hive_site_config, 'hive.server2.thrift.http.port')
            elif transport_mode == 'binary':
                sts_port = ambari_connector.get_property_value(spark_hive_site_config, 'hive.server2.thrift.port')
            jdbc_url = "jdbc:hive2://%s:%s/" % (sts_host, sts_port)
            if transport_mode == "http":
                jdbc_url += ";transportMode=http;httpPath=cliservice"
            if Hadoop.isSecure():
                hive_principal = ambari_connector.get_property_value(
                    spark_hive_site_config, "hive.server2.authentication.kerberos.principal"
                )
                #if transport_mode != "http":
                #    jdbc_url += "/"
                jdbc_url += ";principal=" + hive_principal
            logger.info("Spark %s STS jdbc URL is: %s", spark_version, jdbc_url)
            return jdbc_url

    @classmethod
    @TaskReporter.report_test()
    def restart_interpreter_by_ui_edit_and_save(cls, interpreters, user=None, password=None, knox=False):
        ui_session = ZeppelinUIClientSession(knox=knox)
        assert ui_session.load_zeppelin_homepage(), "Failed to load zeppelin home page in test utils ui session"
        if ZeppelinServer.is_auth_enabled():
            assert user and password, "user and password must be specified in restart_interpreter_by_ui_edit_and_save"
            assert ui_session.login(user, password), "Failed to login as user: %s in test utils ui session" % user
        assert ui_session.click_edit_and_restart(interpreters), "Failed to restart interpreters: %s" % interpreters
        if ZeppelinServer.is_auth_enabled():
            assert ui_session.logout(), "Failed to logout user: %s" % user
        ui_session.delete_session()
        return True

    @classmethod
    @TaskReporter.report_test()
    def ui_login_and_restart_interpreters_within_notebook(
            cls, interpreters, notebook_name=None, user=None, password=None, knox=False
    ):
        ui_session = ZeppelinUIClientSession(knox=knox)
        assert ui_session.load_zeppelin_homepage(), "Failed to load zeppelin home page in test utils ui session"
        if ZeppelinServer.is_auth_enabled():
            assert user and password, "user and password must be specified in restart_interpreter_by_ui_edit_and_save"
            assert ui_session.login(user, password), "Failed to login as user: %s in test utils ui session" % user
        assert ui_session.restart_interpreters_within_notebook(interpreters, notebook_name=notebook_name), \
            "Failed to restart interpreters: %s from notebook: %s" % (interpreters, notebook_name)
        return True

    @classmethod
    @TaskReporter.report_test()
    def get_all_running_zeppelin_livy_yarn_apps(cls, user=None):
        curr_running_apps = YARN.getApplicationList("RUNNING", user='yarn')
        if user:
            livy_app_list = [app for app in curr_running_apps if 'livy' in app.split()[1] and app.split()[3] == user]
        else:
            livy_app_list = [app for app in curr_running_apps if 'livy' in app.split()[1]]
        return livy_app_list

    @classmethod
    @TaskReporter.report_test()
    def get_all_running_zeppelin_spark_yarn_apps(cls):
        curr_running_apps = YARN.getApplicationList("RUNNING", user='yarn')
        zep_app_list = [app for app in curr_running_apps if app.split()[1] == 'Zeppelin']
        return zep_app_list

    @classmethod
    def get_all_spark_interpreter_processes_pids(cls):
        int_processes = Machine.getProcessListRemote(
            hostIP=ZeppelinServer.get_ip_address(), filter=".*/zeppelin-server/interpreter/spark.*"
        )
        return [int(p.split()[1]) for p in int_processes if 'grep' not in p]

    @classmethod
    def get_all_livy_interpreter_processes_pids(cls):
        int_processes = Machine.getProcessListRemote(
            hostIP=ZeppelinServer.get_ip_address(), filter=".*/zeppelin-server/interpreter/livy.*"
        )
        return [int(p.split()[1]) for p in int_processes if not 'grep' in p]

    @classmethod
    def get_all_jdbc_interpreter_processes_pids(cls):
        int_processes = Machine.getProcessListRemote(
            hostIP=ZeppelinServer.get_ip_address(), filter=".*/zeppelin-server/interpreter/jdbc.*"
        )
        return [int(p.split()[1]) for p in int_processes if not 'grep' in p]

    @classmethod
    def get_all_sh_interpreter_processes_pids(cls):
        int_processes = Machine.getProcessListRemote(
            hostIP=ZeppelinServer.get_ip_address(), filter=".*/zeppelin-server/interpreter/sh.*"
        )
        return [int(p.split()[1]) for p in int_processes if not 'grep' in p]

    @classmethod
    @TaskReporter.report_test()
    def kill_all_zeppelin_livy_apps(cls):
        for livy_app in cls.get_all_running_zeppelin_livy_yarn_apps():
            YARN.killApplicationAs(livy_app.split()[0], user='yarn')
        for pid in cls.get_all_livy_interpreter_processes_pids():
            Machine.killProcessRemote(
                pid, host=ZeppelinServer.get_ip_address(), user=Machine.getAdminUser(), passwd=None, logoutput=True
            )

    @classmethod
    @TaskReporter.report_test()
    def kill_all_zeppelin_spark_apps(cls):
        for zep_app in cls.get_all_running_zeppelin_spark_yarn_apps():
            YARN.killApplicationAs(zep_app.split()[0], user='yarn')
        for pid in cls.get_all_spark_interpreter_processes_pids():
            Machine.killProcessRemote(
                pid, host=ZeppelinServer.get_ip_address(), user=Machine.getAdminUser(), passwd=None, logoutput=True
            )

    @classmethod
    @TaskReporter.report_test()
    def beeline_create_table(
            cls,
            table_structure,
            file_format,
            mode,
            user,
            input_file=None,
            field_delimiter=',',
            row_delimiter="\\n",
            external=False,
            table_name=None,
            partitioned_by=None,
            clustered_by=None,
            num_buckets=3,
            hdfs_location=None,
            table_properties=None,
            db_name=None,
            create_db=False,
            use_default_delimiters=False
    ):
        # for Hive mode
        # In this method, we are always going to create tables with HDFS location specified
        # External/Managed -> use specified directory name
        # If managed: change owner + permissions
        # for spark-mode
        # many spark tests require spark-submit/zeppelin access for the tables created
        # hence spark warehouse is currently kept to '777' which will be revisited later
        # for now, should create 'external' table for all spark/zeppelin tests except for the ones with ranger tests

        if not partitioned_by:
            partitioned_by = []
        if not clustered_by:
            clustered_by = []
        if not table_properties:
            table_properties = {}
        assert mode in ['spark', 'hive'], "mode should be either spark or hive"
        if mode == 'hive' and not external:
            assert user == 'hive', "managed Hive tables must be created as 'hive' user"
        if mode == 'spark' and not external:
            assert user == 'spark', "managed Spark tables must be created as 'spark' user"

        if not external and input_file:
            tmp_file = os.path.join("/tmp", os.path.basename(input_file) + "_%s" % int(time.time()))
        else:
            tmp_file = None

        if not table_name:
            assert input_file, "file must be provided"
            table_name = os.path.splitext(os.path.basename(input_file))[0]

        if not hdfs_location:
            if mode == 'hive':
                if external:
                    hdfs_location = Hive.getMetastoreWarehouseDir().replace("managed", "external") + "/" + table_name
                else:
                    hdfs_location = Hive.getMetastoreWarehouseDir() + "/" + table_name
            else:
                hdfs_location = Spark.getSpark2WarehouseDir() + "/" + table_name

        if external:
            assert HDFS.createDirectory(hdfs_location, user=user, perm=777, force=True)[0] == 0, \
                "Failed to create hdfs location: %s" % hdfs_location
            if input_file:
                assert HDFS.copyFromLocal(input_file, hdfs_location, user=user)[0] == 0, \
                    "Failed to copy local data file: %s into hdfs location: %s" % (input_file, hdfs_location)
        else:
            if input_file:
                HDFS.copyFromLocal(input_file, tmp_file, user=user)
                HDFS.chmod(mode, '777', tmp_file)

        query_str = ""
        if db_name:
            if create_db:
                query_str += "create database if not exists %s;\n" % db_name
            query_str += "use %s;\n" % db_name

        query_str += "drop table if exists %s;\n" % table_name
        if external:
            query_str += "create external table %s " % table_name
        else:
            query_str += "create table %s " % table_name
        table_struct_arr = []
        for col_dict in table_structure:
            table_struct_arr.append("%s %s" % (col_dict.keys()[0], col_dict.values()[0]))
        query_str += "(%s) " % ','.join(table_struct_arr)

        if partitioned_by:
            partition_arr = []
            for col_dict in partitioned_by:
                partition_arr.append("%s %s" % (col_dict.keys()[0], col_dict.values()[0]))
            query_str += "partitioned by ( %s ) " % ','.join(partition_arr)

        if clustered_by:
            query_str += "clustered by ( %s ) into %s buckets " % (','.join(clustered_by), num_buckets)

        if file_format == 'TEXTFILE':
            if not use_default_delimiters:
                query_str += "row format delimited fields terminated by '%s' lines terminated by " \
                             "'%s' stored as TEXTFILE " % (field_delimiter, row_delimiter)
            else:
                query_str += "stored as TEXTFILE "
        else:
            query_str += "stored as %s " % file_format

        query_str += "location '%s' " % hdfs_location

        if table_properties:
            props_arr = []
            for prop, val in table_properties.items():
                props_arr.append("'%s'='%s'" % (prop, val))
            query_str += "tblproperties (%s) " % ','.join(props_arr)
        query_str += ";\n"

        if not external and input_file:
            query_str += "load data inpath '%s' into table %s;\n" % (tmp_file, table_name)

        logger.info("running query_str: %s on %s beeline", query_str, mode)
        if mode == 'hive':
            (code, stdout, stderr) = Hive.runQueryOnBeeline(query_str, readFromFile=True, user=user)
        else:
            (code, stdout, stderr) = Spark.runQueryOnBeeline(query_str, readFromFile=True, user=user)
        logger.info("runQueryOnBeeline return code: %s", code)
        logger.info("runQueryOnBeeline stdout: %s", stdout)
        logger.info("runQueryOnBeeline stderr: %s", stderr)
        assert code == 0, "Failed to create table: %s" % table_name

    @classmethod
    @TaskReporter.report_test()
    def beeline_copy_all_data_to_table(cls, from_table, to_table, mode, user, overwrite=True, partitioned_by=None):
        if not partitioned_by:
            partitioned_by = []
        if overwrite:
            query_str = "insert overwrite table %s " % to_table
        else:
            query_str = "insert into table %s " % to_table
        if partitioned_by:
            query_str += "partition (%s) " % ','.join(partitioned_by)

        query_str += "select * from %s;" % from_table
        logger.info("running query_str: %s on %s beeline", query_str, mode)
        if mode == 'hive':
            (code, stdout, stderr) = Hive.runQueryOnBeeline(query_str, readFromFile=True, user=user)
        else:
            (code, stdout, stderr) = Spark.runQueryOnBeeline(query_str, readFromFile=True, user=user)
        logger.info("runQueryOnBeeline return code: %s", code)
        logger.info("runQueryOnBeeline stdout: %s", stdout)
        logger.info("runQueryOnBeeline stderr: %s", stderr)
        assert code == 0, "Failed to copy data from table '%s' into table '%s'" % (from_table, to_table)

    @classmethod
    @TaskReporter.report_test()
    def beeline_grant_table_permissions_to_user(cls, user, table, mode, runas, permissions=None):
        if not permissions:
            permissions = ['all']
        assert mode == 'hive', "only supported in hive mode"
        query_str = "grant %s on %s to user %s;" % (','.join(permissions), table, user)
        logger.info("running query_str: %s on %s beeline", query_str, mode)
        #if mode == 'hive':
        (code, stdout, stderr) = Hive.runQueryOnBeeline(query_str, readFromFile=False, user=runas)
        #else:
        #    (code, stdout, stderr) = Spark.runQueryOnBeeline(query_str, readFromFile=False, user=runas)
        logger.info("runQueryOnBeeline return code: %s", code)
        logger.info("runQueryOnBeeline stdout: %s", stdout)
        logger.info("runQueryOnBeeline stderr: %s", stderr)
        assert code == 0, "Failed to grant permissions:%s on table '%s' to user '%s'" % (permissions, table, user)
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(90)

    @classmethod
    @TaskReporter.report_test()
    def beeline_revoke_table_permissions_from_user(cls, user, table, mode, runas, permissions=None):
        if not permissions:
            permissions = ['all']
        assert mode == 'hive', "only supported in hive mode"
        query_str = "revoke %s on %s from user %s;" % (','.join(permissions), table, user)
        logger.info("running query_str: %s on %s beeline", query_str, mode)
        #if mode == 'hive':
        (code, stdout, stderr) = Hive.runQueryOnBeeline(query_str, readFromFile=False, user=runas)
        #else:
        #    (code, stdout, stderr) = Spark.runQueryOnBeeline(query_str, readFromFile=False, user=runas)
        logger.info("runQueryOnBeeline return code: %s", code)
        logger.info("runQueryOnBeeline stdout: %s", stdout)
        logger.info("runQueryOnBeeline stderr: %s", stderr)
        assert code == 0, "Failed to grant permissions:%s on table '%s' to user '%s'" % (permissions, table, user)
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(90)

    @classmethod
    @TaskReporter.report_test()
    def beeline_drop_table(cls, table, mode, user):
        query_str = "drop table if exists %s;" % (table)
        logger.info("running query_str: %s on %s beeline", query_str, mode)
        if mode == 'hive':
            (code, stdout, stderr) = Hive.runQueryOnBeeline(query_str, readFromFile=False, user=user)
        else:
            (code, stdout, stderr) = Spark.runQueryOnBeeline(query_str, readFromFile=False, user=user)
        logger.info("runQueryOnBeeline return code: %s", code)
        logger.info("runQueryOnBeeline stdout: %s", stdout)
        logger.info("runQueryOnBeeline stderr: %s", stderr)
        assert code == 0, "Failed to drop table '%s' " % table

    @classmethod
    @TaskReporter.report_test()
    def enable_hive_acid(cls):
        hive_changes = {
            'hive-env': {
                'hive_txn_acid': "on"
            },
            'hive-site': {
                'hive.compactor.initiator.on': 'true',
                'hive.compactor.worker.threads': '3',
                'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
                'hive.support.concurrency': 'true',
                'hive.enforce.bucketing': 'true',
                'hive.exec.dynamic.partition.mode': 'nonstrict'
            }
        }

        ambari_connector = ZeppelinAmbariAPIUtil()
        ambari_connector.modify_service_configs('HIVE', 'hive-env', hive_changes['hive-env'])
        ambari_connector.modify_service_configs('HIVE', 'hive-site', hive_changes['hive-site'])
        ambari_connector.restart_service('HIVE', restart_wait=90)

    @classmethod
    @TaskReporter.report_test()
    def get_ranger_policy_ids(cls, repo, regex):
        policy_url = "%s/policy" % Xa.getAdminRestApiUrl()
        basic_auth = HTTPBasicAuth(Xa.getPolicyAdminUsername(), Xa.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        response = requests.get(url=policy_url, auth=basic_auth, headers=headers, verify=False)
        if response.status_code == 200:
            logger.info("list of all ranger policies: %s", response.json()['vXPolicies'])
            policy_id_list = [
                policy['id']
                for policy in response.json()['vXPolicies']
                if policy['repositoryName'] == repo and re.match(regex, policy['policyName'])
            ]
        else:
            logger.error("Failed to get list of ranger policies from url: %s", policy_url)
            policy_id_list = []
        logger.info("list of ranger policies in repo: %s matching regex:'%s' are %s", repo, regex, policy_id_list)
        return policy_id_list

    @classmethod
    @TaskReporter.report_test()
    def create_ranger_policy(cls, policy_dict):
        addr = Xa.getPolicyAdminAddress()
        path = 'service/public/v2/api/policy'
        if Xa.isWireEncryptionOn():
            if 'https' in addr:
                policy_url = "%s/%s" % (addr, path)
            else:
                policy_url = "%s/%s" % (addr.replace('http', 'https'), path)
        else:
            policy_url = "%s/%s" % (addr, path)
        basic_auth = HTTPBasicAuth(Xa.getPolicyAdminUsername(), Xa.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            url=policy_url, auth=basic_auth, headers=headers, data=json.dumps(policy_dict), verify=False
        )
        assert response.status_code == 200, "Failed to create ranger policy: %s(%s) with url: %s" \
                                            % (policy_dict['name'], policy_dict['service'], policy_url)

    @classmethod
    @TaskReporter.report_test()
    def disable_ranger_policy(cls, repo, regex):
        ids = cls.get_ranger_policy_ids(repo, regex)
        for i in ids:
            policy_url = "%s/policy/%s" % (Xa.getAdminRestApiV2Url(), i)
            basic_auth = HTTPBasicAuth(Xa.getPolicyAdminUsername(), Xa.getPolicyAdminPassword())
            headers = {'Content-Type': 'application/json'}
            logger.info("Trying to disable ranger policy in repo: %s, regex: %s, url: %s", repo, regex, policy_url)
            get_res = requests.get(url=policy_url, auth=basic_auth, headers=headers, verify=False)
            if get_res.status_code == 200:
                json_data = get_res.json()
                json_data['isEnabled'] = False
                put_res = requests.put(
                    url=policy_url, auth=basic_auth, data=json.dumps(json_data), headers=headers, verify=False
                )
                assert put_res.status_code == 200, "Failed to disable policy: %s with url: %s" % (
                    json_data['policyName'], policy_url
                )
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(60)

    @classmethod
    @TaskReporter.report_test()
    def delete_ranger_policy(cls, repo, regex):
        logger.info("Trying to delete ranger policy in repo: %s, regex: %s", repo, regex)
        ids = cls.get_ranger_policy_ids(repo, regex)
        for i in ids:
            Xa.deletePolicy(i)
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(60)

    @classmethod
    @TaskReporter.report_test()
    def enable_ranger_policy(cls, repo, regex):
        ids = cls.get_ranger_policy_ids(repo, regex)
        for i in ids:
            policy_url = "%s/policy/%s" % (Xa.getAdminRestApiV2Url(), i)
            basic_auth = HTTPBasicAuth(Xa.getPolicyAdminUsername(), Xa.getPolicyAdminPassword())
            headers = {'Content-Type': 'application/json'}
            logger.info("Trying to enable ranger policy in repo: %s, regex: %s, url: %s", repo, regex, policy_url)
            get_res = requests.get(url=policy_url, auth=basic_auth, headers=headers, verify=False)
            if get_res.status_code == 200:
                json_data = get_res.json()
                json_data['isEnabled'] = True
                put_res = requests.put(
                    url=policy_url, auth=basic_auth, data=json.dumps(json_data), headers=headers, verify=False
                )
                assert put_res.status_code == 200, "Failed to enable policy: %s" % json_data['policyName']
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(60)


class ZeppelinServer(object):
    '''
    This class contains methods to perform all general operations for Zeppelin
    e.g. getting Zeppelin server public IP, reading/modifying zeppelin configurations
    and starting/stopping zeppelin server
    '''

    def __init__(self):
        pass

    _ambari_connector = None

    _ip_addr = None
    _port = None

    _principal = None
    _realm = None

    _is_ssl = None
    _is_auth_basic = None
    _is_auth_enabled = None
    _is_auth_ldap = None
    _is_auth_simple = None
    _log_dir = None

    _registered_sessions = {}

    @classmethod
    @TaskReporter.report_test()
    def register_session(cls, session):
        logger.info("Registering a %s session with id: %s", type(session), session.get_id())
        assert not session.get_id() in ZeppelinServer._registered_sessions.keys(), \
            "Failed to register a %s session with id: %s, id already exists" % (type(session), session.get_id())
        ZeppelinServer._registered_sessions[session.get_id()] = session

    @classmethod
    def __notify_session(cls, session_id, event):
        logger.info("Notifying ZeppelinServer event: '%s' to session with id: %s", event, session_id)
        ZeppelinServer._registered_sessions[session_id].notify(event)

    @classmethod
    def remove_session(cls, session):
        logger.info("Removing a %s session with id: %s", type(session), session.get_id())
        del ZeppelinServer._registered_sessions[session.get_id()]

    @classmethod
    def __get_ambari_connector(cls):
        if cls._ambari_connector is None:
            cls._ambari_connector = ZeppelinAmbariAPIUtil()
        return cls._ambari_connector

    @classmethod
    @TaskReporter.report_test()
    def move_zeppelin_master_to_another_host(cls, host=None):
        # Find FQDN of the node to which we need to move zeppelin notebook
        zeppelin_host_name = socket.gethostbyaddr(cls.get_ip_address())[0]
        host_to_move = None
        if not host:
            host_to_move = ZeppelinTestUtils.get_random_host_from_cluster(exclude_hosts=[zeppelin_host_name])
        else:
            if util.isIP(host):
                try:
                    host_to_move = socket.gethostbyaddr(host)[0]
                except Exception:
                    pass
            else:
                host_to_move = host

        assert host_to_move, "Could not find fqdn of the host to which zeppelin server is to be moved"

        assert cls.__get_ambari_connector().stop_component(service='ZEPPELIN', component='ZEPPELIN_MASTER'), \
            "Failed to stop Zepplin Notebook before moving from host: '%s' to host: '%s'" % (zeppelin_host_name,
                                                                                             host_to_move)
        assert cls.__get_ambari_connector().delete_component_from_host(service='ZEPPELIN', component='ZEPPELIN_MASTER',
                                                                       host=zeppelin_host_name), \
            "Failed to delete Zeppelin Notebook from host: '%s' before moving to host: '%s'" % (zeppelin_host_name,
                                                                                                host_to_move)
        assert cls.__get_ambari_connector().install_component_to_host(component='ZEPPELIN_MASTER', service='ZEPPELIN',
                                                                      host=host_to_move, start=True), \
            "Failed to re-install Zeppelin Notebook on host: '%s'" % host_to_move
        cls._ip_addr = util.getIpAddress(hostname=host_to_move)
        return True

    @classmethod
    @TaskReporter.report_test()
    def move_zeppelin_service_to_another_host(cls, host=None):
        # Find FQDN of the node to which we need to move zeppelin notebook
        zeppelin_host_name = cls.get_ip_address()
        host_to_move = None
        if not host:
            host_to_move = ZeppelinTestUtils.get_random_host_from_cluster(exclude_hosts=[zeppelin_host_name])
        else:
            if util.isIP(host):
                try:
                    host_to_move = socket.gethostbyaddr(host)[0]
                except Exception:
                    pass
            else:
                host_to_move = host

        assert host_to_move, "Could not find fqdn of the host to which zeppelin server is to be moved"

        configs_dict = cls.__get_ambari_connector().get_current_configs_dict_for_service('ZEPPELIN')

        logger.info("Old configurations before deleting Zeppelin service: %s", configs_dict)
        # Stop and delete zeppelin service
        logger.info(
            "Stopping zeppelin service before moving zeppelin from '%s' to '%s'", zeppelin_host_name, host_to_move
        )
        assert cls.stop_if_running(), "Failed to stop zeppelin service before moving zeppelin from '%s' to '%s'" % \
                                      (zeppelin_host_name, host_to_move)
        assert cls.__get_ambari_connector().delete_service('ZEPPELIN'), "Failed to delete zeppelin " \
                                                                    "service from '%s' before reinstalling on '%s'"\
                                                                        % (zeppelin_host_name, host_to_move)

        # re-install zeppelin on some other node with previous configs
        component_to_host_map = {'ZEPPELIN_MASTER': host_to_move}
        assert cls.__get_ambari_connector().install_service('ZEPPELIN', configs_dict, component_to_host_map), \
            "Failed to resintall Zeppelin on node: %s after deleting" % host_to_move
        cls._ip_addr = util.getIpAddress(hostname=host_to_move)
        return True

    @classmethod
    def getVersion(cls):
        return cls.__get_ambari_connector().get_service_version('ZEPPELIN')

    @classmethod
    @TaskReporter.report_test()
    def get_config_from_zeppelin_env_section(cls, prop_name, default_val=None):
        prop_val = cls.__get_ambari_connector().get_property_value('zeppelin-env', prop_name)
        if not prop_val:
            return default_val
        return prop_val

    @classmethod
    @TaskReporter.report_test()
    def get_config_from_zeppelin_env_file(cls, prop_name, default_val=None):
        zeppelin_env_text = cls.get_config_from_zeppelin_env_section('zeppelin_env_content')
        assert zeppelin_env_text, "Failed to read zeppelin-env.sh from Ambari"
        zeppelin_env_text = zeppelin_env_text.split("\n")
        for line in zeppelin_env_text:
            if not line.startswith('#'):
                if line.find("export " + prop_name) == 0:
                    m = re.search(r'(\S*)=(\S*)', line)
                    if m:
                        return m.group(2)
                    else:
                        return None
        return default_val

    @classmethod
    @TaskReporter.report_test()
    def get_config_from_zeppelin_site_xml(cls, prop_name, default_val=None):
        prop_val = cls.__get_ambari_connector().get_property_value('zeppelin-site', prop_name)
        if not prop_val:
            return default_val
        return prop_val

    @classmethod
    @TaskReporter.report_test()
    def get_config_from_zeppelin_shiro_ini(cls, section, property_name, default_val=None):
        config_parser = ConfigParser.ConfigParser()
        config_parser.optionxform = str
        shiro_ini_content = cls.__get_ambari_connector().get_property_value('zeppelin-shiro-ini', 'shiro_ini_content')
        assert shiro_ini_content, "Failed to read shiro_ini_content from Ambari"
        logger.info("shiro_ini_content: \n%s\n", shiro_ini_content)
        config_parser.readfp(StringIO.StringIO(shiro_ini_content))
        if config_parser.has_option(section, property_name):
            return config_parser.get(section, property_name)
        return default_val

    @classmethod
    def is_ssl_enabled(cls):
        if cls._is_ssl is None:
            cls._is_ssl = (cls.get_config_from_zeppelin_site_xml('zeppelin.ssl') == "true")
        return cls._is_ssl

    @classmethod
    @TaskReporter.report_test()
    def get_ip_address(cls, ip=True):
        logger.info("IP = %s", ip)
        if cls._ip_addr is None:
            fqdn = cls.__get_ambari_connector().get_component_hosts('ZEPPELIN', 'ZEPPELIN_MASTER')
            assert fqdn, "Failed to read ZEPPELIN_MASTER host via ambari"
            logger.info("Zeppelin server FQDN: %s", fqdn)

            if Machine.isAmazonLinux():
                # If cloud runs, util.getIpAddress(hostname=fqdn) does not return Public IP Address of Zeppelin Host
                # Hence correct way is to find the corresponding IP address from /tmp/all_nodes file
                all_int_nodes = util.getAllInternalNodes()
                all_nodes = util.getAllNodes()
                for i, n in enumerate(all_int_nodes): # pylint: disable=unused-variable
                    if fqdn == all_int_nodes[i]:
                        cls._ip_addr = all_nodes[i]
                        break
            else:
                cls._ip_addr = util.getIpAddress(hostname=fqdn)
            logger.info("Zeppelin server IP address: %s", cls._ip_addr)
        return cls._ip_addr

    @classmethod
    @TaskReporter.report_test()
    def get_log_dir(cls):
        if cls._log_dir is None:
            cls._log_dir = cls.get_config_from_zeppelin_env_section(
                'zeppelin_log_dir', default_val="/var/log/zeppelin"
            )
        return cls._log_dir

    @classmethod
    @TaskReporter.report_test()
    def get_port(cls):
        if cls._port is None:
            if cls.is_ssl_enabled():
                cls._port = cls.get_config_from_zeppelin_env_file(
                    'ZEPPELIN_SSL_PORT',
                    default_val=cls.get_config_from_zeppelin_site_xml('zeppelin.server.port', default_val='9995')
                )
            else:
                cls._port = cls.get_config_from_zeppelin_env_file(
                    'ZEPPELIN_PORT',
                    default_val=cls.get_config_from_zeppelin_site_xml('zeppelin.server.ssl.port', default_val='9995')
                )
            logger.info("Zeppelin server port: %s", cls._port)
        return cls._port

    @classmethod
    @TaskReporter.report_test()
    def get_zeppelin_principal(cls):
        if cls._principal is None:
            princ_text = cls.get_config_from_zeppelin_site_xml('zeppelin.server.kerberos.principal')
            assert princ_text, "'zeppelin.server.kerberos.principal' property could not be read"
            cls._principal = princ_text.split('@')[0]
            cls._realm = princ_text.split('@')[1]
        return cls._principal

    @classmethod
    @TaskReporter.report_test()
    def get_realm(cls):
        if cls._realm is None:
            princ_text = cls.get_config_from_zeppelin_site_xml('zeppelin.server.kerberos.principal')
            assert princ_text, "'zeppelin.server.kerberos.principal' property could not be read"
            cls._principal = princ_text.split('@')[0]
            cls._realm = princ_text.split('@')[1]
        return cls._realm

    @classmethod
    @TaskReporter.report_test()
    def get_base_url(cls, knox=False, is_sso=False):
        if knox:
            knox_hosts = cls.__get_ambari_connector().get_component_hosts('KNOX', 'KNOX_GATEWAY').split(',')
            topology = 'ui'
            if HDFS.isFederated():
                topology = 'ui_ns1'
            base_url = "https://{0}:8443/gateway/{1}/zeppelin/".format(knox_hosts[0], topology)
        else:
            if Machine.getInstaller() == 'cloudbreak':
                base_url = Ambari.getWebUrl().replace("ambari", "zeppelin") + "/"
            else:
                if cls.is_ssl_enabled():
                    prot = "https"
                else:
                    prot = "http"
                if is_sso:
                    fqdn = cls.__get_ambari_connector().get_component_hosts('ZEPPELIN', 'ZEPPELIN_MASTER')
                    base_url = "%s://%s:%s" % (prot, fqdn, cls.get_port())
                else:
                    base_url = "%s://%s:%s" % (prot, cls.get_ip_address(), cls.get_port())

        if not cls._log_dir:
            cls._log_dir = cls.get_log_dir()
        logger.info("base_url = %s", str(base_url))
        return base_url

    @classmethod
    @TaskReporter.report_test()
    def is_auth_enabled(cls):
        if cls._is_auth_enabled is None:
            auth = cls.get_config_from_zeppelin_shiro_ini('urls', "/**")
            if not auth or auth == 'anon':
                cls._is_auth_enabled = False
            else:
                cls._is_auth_enabled = True
        return cls._is_auth_enabled

    @classmethod
    @TaskReporter.report_test()
    def is_basic_auth_enabled(cls):
        if cls._is_auth_basic is None:
            cls._is_auth_basic = cls.get_config_from_zeppelin_shiro_ini('urls', "/**") == 'authcBasic'
        return cls._is_auth_basic

    @classmethod
    @TaskReporter.report_test()
    def is_ldap_auth_enabled(cls):
        if cls._is_auth_ldap is None:
            if cls.is_auth_enabled():
                realm = cls.get_config_from_zeppelin_shiro_ini('main', 'ldapRealm')
                logger.info("LDAP realm: %s", realm)
                cls._is_auth_ldap = realm == 'org.apache.shiro.realm.ldap.JndiLdapRealm'
            else:
                cls._is_auth_ldap = False
        return cls._is_auth_ldap

    @classmethod
    @TaskReporter.report_test()
    def is_simple_auth_enabled(cls):
        if cls._is_auth_simple is None:
            cls._is_auth_simple = (cls.is_auth_enabled() and not cls.is_ldap_auth_enabled())
        return cls._is_auth_simple

    @classmethod
    def __modify_zeppelin_env(cls, changes, restart=False):
        """
        allows to add/change and delete
        """
        old_zeppelin_env_text = cls.__get_ambari_connector().get_property_value(
            'zeppelin-env', 'zeppelin_env_content'
        ).split("\n")
        assert old_zeppelin_env_text, "Failed to read original zeppelin-env.sh text"
        new_zeppelin_env_text = ""
        for line in old_zeppelin_env_text:
            sline = line.strip()
            if not sline.startswith("#") and sline != "":
                m = re.search(r'export (\S+?)=', sline)
                if m:
                    if 'modify' in changes.keys() and m.group(1) in changes['modify'].keys():
                        new_zeppelin_env_text += "export %s=%s\n" % (m.group(1), changes['modify'][m.group(1)])
                    elif 'remove' in changes.keys() and m.group(1) in changes['remove']:
                        new_zeppelin_env_text += ('#' + sline + "\n")
                    else:
                        new_zeppelin_env_text += (sline + "\n")
                else:
                    new_zeppelin_env_text += (sline + "\n")
            else:
                # add empty/commented lines directly
                new_zeppelin_env_text += (sline + "\n")
        if 'add' in changes.keys():
            for prop, val in changes['add'].items():
                line = "export %s=%s\n" % (prop, val)
                new_zeppelin_env_text += line
        assert cls.__get_ambari_connector().modify_service_configs('ZEPPELIN', 'zeppelin-env',
                                                                   {'zeppelin_env_content': new_zeppelin_env_text}), \
            "Failed to modify zeppelin-env.sh"
        if restart:
            return cls.restart()
        else:
            return True

    @classmethod
    def __modify_zeppelin_site_xml(cls, changes, restart=False):
        """
        Only allows to change the existing property in zeppelin-site.xml, does not allow to add/delete
        """
        assert cls.__get_ambari_connector().modify_service_configs('ZEPPELIN', 'zeppelin-site', changes), \
            "Failed to modify zeppelin-site.xml"
        if restart:
            return cls.restart()
        else:
            return True

    @classmethod
    def __modify_zeppelin_shiro_ini(cls, changes, restart=False):
        """
        allows to add/change/delete existing property in shiro.ini
        :param changes : dictionary of the form {'modify':{..},'add':{..},'remove':[]}
        """
        shiro_ini_content = cls.__get_ambari_connector().get_property_value('zeppelin-shiro-ini', 'shiro_ini_content')
        assert shiro_ini_content, "Failed to read shiro_ini_content"
        config_parser = ConfigParser.ConfigParser()
        config_parser.optionxform = str
        config_parser.readfp(StringIO.StringIO(shiro_ini_content))

        if 'modify' in changes.keys():
            for key, val in changes['modify'].items():
                sec, name = key.split(':')
                if config_parser.has_option(sec, name):
                    config_parser.set(sec, name, val)
                else:
                    logger.warn("Property %s:%s doesnt exist in shiro.ini file, will ignore modification", sec, name)

        if 'add' in changes.keys():
            for key, val in changes['add'].items():
                sec, name = key.split(':')
                if config_parser.has_section(sec):
                    config_parser.set(sec, name, val)
                else:
                    config_parser.add_section(sec)
                    config_parser.set(sec, name, val)

        if 'remove' in changes.keys():
            for prop in changes['remove']:
                sec, name = prop.split(':')
                if name == "*":
                    if config_parser.has_section(sec):
                        config_parser.remove_section(sec)
                    else:
                        logger.warn("Section %s does not exist in shiro.ini file, will ignore removal", sec)
                else:
                    if config_parser.has_option(sec, name):
                        config_parser.remove_option(sec, name)
                    else:
                        logger.warn(
                            "Property %s:%s doesnt exist in shiro.ini file, will ignore modification", sec, name
                        )

        mod_shiro_ini_content_buf = StringIO.StringIO()
        config_parser.write(mod_shiro_ini_content_buf)
        assert cls.__get_ambari_connector().modify_service_configs(
            'ZEPPELIN', 'zeppelin-shiro-ini',
            {'shiro_ini_content': cls.__enforce_order_in_shiro_ini(mod_shiro_ini_content_buf).getvalue()}
        ), "Failed to modify shiro.ini"
        if restart:
            return cls.restart()
        else:
            return True

    @classmethod
    def __enforce_order_in_shiro_ini(cls, buf):
        main_section_order = [
            '[main]', 'knoxJwtRealm', 'knoxJwtRealm.providerUrl', 'knoxJwtRealm.login', 'knoxJwtRealm.publicKeyPath',
            'knoxJwtRealm.logoutAPI', 'knoxJwtRealm.logout', 'knoxJwtRealm.cookieName', 'knoxJwtRealm.redirectParam',
            'knoxJwtRealm.groupPrincipalMapping', 'knoxJwtRealm.principalMapping', 'authc', 'ldapRealm',
            'ldapRealm.userDnTemplate', 'ldapRealm.contextFactory.url',
            'ldapRealm.contextFactory.authenticationMechanism', 'sessionManager', 'securityManager.sessionManager',
            'securityManager.sessionManager.globalSessionTimeout', 'shiro.loginUrl'
        ]
        urls_section_order = ['[urls]', "/api/version", "/**"]
        main_section = []
        for _ in range(len(main_section_order)):
            main_section.append(None)
        users_section = []
        urls_section = [None, None, None]

        main_section_start = False
        users_section_start = False
        urls_section_start = False

        lines = buf.getvalue().split("\n")
        for line in lines:
            if line.startswith('[main]'):
                main_section[0] = line
                main_section_start = True
                users_section_start = False
                urls_section_start = False
            elif line.startswith('[urls]'):
                urls_section[0] = line
                main_section_start = False
                users_section_start = False
                urls_section_start = True
            elif line.startswith('[users]'):
                users_section.append(line)
                main_section_start = False
                users_section_start = True
                urls_section_start = False
            elif main_section_start:
                key = line.split("=")[0].strip()
                if key in main_section_order:
                    main_section[main_section_order.index(key)] = line
            elif users_section_start:
                users_section.append(line)
            elif urls_section_start:
                key = line.split("=")[0].strip()
                if key in urls_section_order:
                    urls_section[urls_section_order.index(key)] = line

        shiro_ini_str = ''
        for line in users_section:
            if line:
                shiro_ini_str = shiro_ini_str + line + "\n"
        for line in main_section:
            if line:
                shiro_ini_str = shiro_ini_str + line + "\n"
        for line in urls_section:
            if line:
                shiro_ini_str = shiro_ini_str + line + "\n"

        return StringIO.StringIO(shiro_ini_str)

    @classmethod
    @TaskReporter.report_test()
    def modify_zeppelin_conf(cls, changes):
        """
        allows to modify zeppelin configs via ambari and restarts zeppelin service
        :param changes: dictionary of the form {'shiro.ini':{'modify':{..},'add':{..},'remove':[]},
                        'zeppelin-site.xml':{},'zeppelin-env.sh':{}}
        """
        success = True
        if 'zeppelin-env.sh' in changes.keys():
            if 'zeppelin-site.xml' not in changes.keys() and 'shiro.ini' not in changes.keys():
                success = success and cls.__modify_zeppelin_env(changes['zeppelin-env.sh'], restart=True)
            else:
                success = success and cls.__modify_zeppelin_env(changes['zeppelin-env.sh'])

        if 'zeppelin-site.xml' in changes.keys():
            if 'shiro.ini' not in changes.keys():
                success = success and cls.__modify_zeppelin_site_xml(changes['zeppelin-site.xml'], restart=True)
            else:
                success = success and cls.__modify_zeppelin_site_xml(changes['zeppelin-site.xml'])

        if 'shiro.ini' in changes.keys():
            success = success and cls.__modify_zeppelin_shiro_ini(changes['shiro.ini'], restart=True)
        return success

    @classmethod
    @TaskReporter.report_test()
    def enable_simple_auth(cls, user_pwd_dict=None, removed_users=None, mode='bootstrap'):
        if not user_pwd_dict:
            user_pwd_dict = {}
        if not removed_users:
            removed_users = []
        logger.info("Enabling simple authentication and restarting Zeppelin server")
        mod_shiro_dict = {
            'modify': {},
            'remove': [],
            'add': {
                'main:sessionManager': 'org.apache.shiro.web.session.mgt.DefaultWebSessionManager',
                'main:securityManager.sessionManager': '$sessionManager',
                'main:securityManager.sessionManager.globalSessionTimeout': '86400000',
                'main:shiro.loginUrl': '/api/login'
            }
        }
        for user, pwd in user_pwd_dict.items():
            mod_shiro_dict['add']["users:%s" % user] = pwd

        for user in removed_users:
            mod_shiro_dict['remove'].append("users:%s" % user)

        if mode == "basic":
            mod_shiro_dict['modify']["urls:/**"] = 'authcBasic'
        elif mode == "bootstrap":
            mod_shiro_dict['modify']["urls:/**"] = 'authc'
        success = cls.modify_zeppelin_conf(
            {
                'shiro.ini': mod_shiro_dict,
                'zeppelin-site.xml': {
                    'zeppelin.anonymous.allowed': 'false'
                }
            }
        )
        if success:
            cls._is_auth_enabled = True
            cls._is_auth_simple = True
            cls._is_auth_basic = (mode == 'basic')
            cls._is_auth_ldap = False
        return success

    @classmethod
    @TaskReporter.report_test()
    def enable_ldap_auth(
            cls,
            ldap_server_ip='localhost',
            ldap_server_port='389',
            user_dn_template='cn={0},ou=Users,dc=hortonworks,dc=com',
            mode='bootstrap'
    ):
        logger.info("Enabling LDAP authentication and restarting Zeppelin server")
        if ldap_server_ip == '0.0.0.0' or ldap_server_ip == 'localhost':
            ip = util.getIpAddress(hostname=None)
        elif re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ldap_server_ip):
            ip = ldap_server_ip
        else:
            ip = util.getIpAddress(hostname=ldap_server_ip)
        mod_shiro_dict = {
            'modify': {},
            'remove': ["users:*"],
            'add': {
                'main:ldapRealm': 'org.apache.shiro.realm.ldap.JndiLdapRealm',
                'main:ldapRealm.userDnTemplate': user_dn_template,
                'main:ldapRealm.contextFactory.url': "ldap://%s:%s" % (ip, ldap_server_port),
                'main:ldapRealm.contextFactory.authenticationMechanism': 'SIMPLE',
                'main:sessionManager': 'org.apache.shiro.web.session.mgt.DefaultWebSessionManager',
                'main:securityManager.sessionManager': '$sessionManager',
                'main:securityManager.sessionManager.globalSessionTimeout': '86400000',
                'main:shiro.loginUrl': '/api/login'
            }
        }

        if mode == "basic":
            mod_shiro_dict['modify']["urls:/**"] = 'authcBasic'
        elif mode == "bootstrap":
            mod_shiro_dict['modify']["urls:/**"] = 'authc'

        success = cls.modify_zeppelin_conf(
            {
                'shiro.ini': mod_shiro_dict,
                'zeppelin-site.xml': {
                    'zeppelin.anonymous.allowed': 'false'
                }
            }
        )
        if success:
            cls._is_auth_enabled = True
            cls._is_auth_simple = False
            cls._is_auth_basic = (mode == 'basic')
            cls._is_auth_ldap = True
        return success

    @classmethod
    @TaskReporter.report_test()
    def disable_auth(cls):
        if not cls.is_auth_enabled():
            logger.info("Authentication already disabled")
            return True
        logger.info("Disabling authentication and restarting Zeppelin server")
        success = cls.modify_zeppelin_conf(
            {
                'shiro.ini': {
                    'add': {},
                    'remove': [],
                    'modify': {
                        'urls:/**': 'anon'
                    }
                },
                'zeppelin-site.xml': {
                    'zeppelin.anonymous.allowed': 'true'
                }
            }
        )
        if success:
            cls._is_auth_enabled = False
            cls._is_auth_simple = False
            cls._is_auth_basic = False
            cls._is_auth_ldap = False
        return success

    @classmethod
    @TaskReporter.report_test()
    def enable_auth(cls, simple_auth=False, auth_basic=False, auth_ldap=False):
        if cls.is_auth_enabled():
            logger.info("Authentication already enabled")
            return True
        logger.info("Enabling authentication and restarting Zeppelin server")
        success = cls.modify_zeppelin_conf(
            {
                'shiro.ini': {
                    'add': {},
                    'remove': [],
                    'modify': {
                        'urls:/**': 'authc'
                    }
                },
                'zeppelin-site.xml': {
                    'zeppelin.anonymous.allowed': 'false'
                }
            }
        )
        if success:
            cls._is_auth_enabled = True
            cls._is_auth_simple = simple_auth
            cls._is_auth_basic = auth_basic
            cls._is_auth_ldap = auth_ldap
        return success

    @classmethod
    @TaskReporter.report_test()
    def stop_if_running(cls):
        if not cls.is_running():
            return True
        success = cls.__get_ambari_connector().stop_service('ZEPPELIN')
        if success:
            cls._port = None
            for i in ZeppelinServer._registered_sessions:
                cls.__notify_session(i, 'stop')
        return success

    @classmethod
    @TaskReporter.report_test()
    def start_if_not_running(cls):
        if cls.is_running():
            return True
        success = cls.__get_ambari_connector().start_service('ZEPPELIN', start_wait=20)
        if success:
            for i in ZeppelinServer._registered_sessions:
                cls.__notify_session(i, 'start')
        return success

    @classmethod
    @TaskReporter.report_test()
    def restart(cls):
        stopped = cls.stop_if_running()
        while cls.is_running():
            logger.info("Zeppelin Server is not yet stopped, retrying to stop")
            stopped = cls.stop_if_running()
        started = cls.start_if_not_running()
        while not cls.is_running():
            logger.info("Zeppelin Server is not yet started, retrying to start")
            started = cls.start_if_not_running()
        return stopped and started

    @classmethod
    def is_running(cls):
        state = cls.__get_ambari_connector().get_service_state('ZEPPELIN')
        assert state, "Failed to get Zeppelin Service state from ambari"
        return state == 'STARTED'


class ZeppelinLogCollectionUtils(object):
    def __init__(self):
        pass

    @classmethod
    @TaskReporter.report_test()
    def get_zeppelin_server_ip_addr(cls):
        all_nodes = util.getAllNodes()
        for node in all_nodes:
            status = Machine.runas(
                user=Machine.getAdminUser(),
                cmd="ls /etc/zeppelin/conf/interpreter.json",
                host=node,
                passwd=Machine.getAdminPasswd()
            )
            if status[0] == 0:
                return node
        return None

    @classmethod
    @TaskReporter.report_test()
    def get_zeppelin_log_dir(cls):
        ip = cls.get_zeppelin_server_ip_addr()
        status = Machine.runas(
            user=Machine.getAdminUser(),
            cmd="cat /etc/zeppelin/conf/zeppelin-env.sh | grep LOG_DIR",
            host=ip,
            passwd=Machine.getAdminPasswd()
        )
        return status[1].split("=")[1]


class ZeppelinClient(object):
    _session_counter = 0

    def __init__(self):
        ZeppelinClient._session_counter += 1
        self.currently_logged_in_user = None
        self.id = '99999999'
        self.session_type = 'rest'
        self.is_zeppelin_server_running = ZeppelinServer.is_running()

    def get_id(self):
        return self.id

    def delete_session(self):
        ZeppelinServer.remove_session(self)

    def get_type(self):
        return self.session_type

    @classmethod
    @TaskReporter.report_test()
    def validate_notebook_output(cls, act_results, exp_results):
        for para_index in exp_results.keys():
            cls.validate_single_paragraph_output(
                act_results[para_index], exp_results[para_index], para_index=para_index
            )
        return True

    @classmethod
    @TaskReporter.report_test()
    def validate_single_paragraph_output(cls, para_results, para_exp_out, para_index=''):
        assert para_results['type'] == para_exp_out['type'], \
            "Type of the results for paragraph %s does not match" % (para_index)
        if para_results['type'] == 'TEXT':
            for exp_line in para_exp_out['content']:
                assert (True in [re.match(exp_line, act_line) is not None for act_line in para_results['content']
                                 if act_line]), \
                    "Failed to find line: %s in parsed textual results for paragraph %s" % \
                                                (exp_line, para_index)
        elif para_results['type'] == 'TABLE':
            assert len(para_exp_out['content']) == len(para_results['content']), \
                "length of table does not match for paragraph %s" % (para_index)
            for act_line in para_results['content']:
                if '' in act_line:
                    act_line.remove('')
            for exp_line in para_exp_out['content']:
                assert exp_line in para_results['content'], \
                    "Failed to find row: %s in parsed tabular results for paragraph %s" % (exp_line, para_index)
        else:
            assert para_results['content'] == para_exp_out['content'], \
                "Content of the results for paragraph %s does not match" % para_index
        return True


class ZeppelinRestClientSession(ZeppelinClient):
    """
    This class contains methods to communicate with Zeppelin Server using Rest APIs
    """

    def __init__(self, session_id=None):
        super(ZeppelinRestClientSession, self).__init__()
        self.session_cookies = None
        self.session_type = 'rest'
        if session_id:
            self.id = session_id
        else:
            self.id = ZeppelinRestClientSession._session_counter
        self.most_recently_created_notebook = {}
        ZeppelinServer.register_session(self)

    @TaskReporter.report_test()
    def notify(self, event):
        if event == 'stop':
            if self.is_zeppelin_server_running:
                self.most_recently_created_notebook = {}
                self.currently_logged_in_user = None
                self.session_cookies = None
            self.is_zeppelin_server_running = False
        elif event == 'start':
            self.is_zeppelin_server_running = True

    @classmethod
    def __get_rest_api_base_url(cls):
        return ZeppelinServer.get_base_url() + "/api/"

    @classmethod
    @synchronized
    def __http_req(cls, method, task, auth=None, data=None, cookies=None, log=True):
        logger.info("auth = %s", auth)
        url = ZeppelinRestClientSession.__get_rest_api_base_url() + task
        if log:
            logger.info("Sending '%s' request to url : %s", method, url)
        response = None
        time.sleep(2 * random.random())
        max_attempts = 5
        num_attempts = 0
        wait_time = 5

        while num_attempts < max_attempts:
            try:
                if method == 'get':
                    response = requests.get(url=url, data=data, cookies=cookies, verify=False, timeout=60)
                elif method == 'post':
                    response = requests.post(url=url, data=data, cookies=cookies, verify=False, timeout=60)
                elif method == 'delete':
                    response = requests.delete(url=url, data=data, cookies=cookies, verify=False, timeout=60)
                elif method == 'put':
                    response = requests.put(url=url, data=data, cookies=cookies, verify=False, timeout=60)
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    logger.info(
                        "Attempt %s failed -> Exception while sending '%s' request to url : %s, msg: %s.."
                        "Retrying after %s seconds", (num_attempts + 1), method, url, e, wait_time
                    )
                    time.sleep(wait_time)
                else:
                    logger.info(
                        "Attempt %s failed -> Exception while sending '%s' request to url : %s, msg: %s",
                        (num_attempts + 1), method, url, e
                    )
                    logger.error(
                        "All %s attempts exhausted while sending '%s' request to url : %s", max_attempts, method, url
                    )
                num_attempts += 1
            else:
                break

        assert response is not None, "Failed to receive response for %s' request to url : %s" % (method, url)
        if log:
            logger.info(
                "Response from %s request to %s is %s after num_attempts: %s", method, url, response, num_attempts
            )
        return response

    @TaskReporter.report_test()
    def login(self, user_name, password):
        if self.is_user_logged_in(user_name):
            return True
        elif self.is_someone_logged_in():
            logger.error("Logout user: %s first", self.currently_logged_in_user)
            return False

        if not ZeppelinServer.is_auth_enabled():
            logger.error("Failed to login, Authentication is not enabled for zeppelin")
            return False
        else:
            if ZeppelinServer.is_basic_auth_enabled():
                basic_auth = HTTPBasicAuth(user_name, password)
                response = ZeppelinRestClientSession.__http_req('post', 'login', auth=basic_auth)
            else:
                auth_dict = {'userName': user_name, 'password': password}
                response = ZeppelinRestClientSession.__http_req('post', 'login', data=auth_dict)

            if response.status_code == 200:
                self.currently_logged_in_user = user_name
                self.session_cookies = response.cookies
                return True
            else:
                logger.error("Failed to login")
                return False

    @TaskReporter.report_test()
    def logout(self):
        if not ZeppelinServer.is_auth_enabled():
            logger.error("Failed to logout, Authentication is not enabled for zeppelin")
            return False
        else:
            response = ZeppelinRestClientSession.__http_req('post', 'login/logout', cookies=self.session_cookies)
            if response.status_code == 401:
                logger.info("User: %s logged out successfully", self.currently_logged_in_user)
                self.currently_logged_in_user = None
                self.session_cookies = None
                return True
            else:
                logger.error("Failed to logout user: %s", self.currently_logged_in_user)
                return False

    @TaskReporter.report_test()
    def get_list_of_all_notebooks(self):
        notebook_list = {}
        response = ZeppelinRestClientSession.__http_req('get', 'notebook', cookies=self.session_cookies, log=True)
        if response.status_code == 200:
            for ndict in response.json()['body']:
                notebook_list[ndict['id']] = ndict['name']
            return notebook_list
        else:
            logger.error("Failed to get the list of currently present notebook")
            return None

    @TaskReporter.report_test()
    def create_new_notebook(self, notebook_name='', content=None, owners='', readers='', writers='', runners=''):
        """
        Allows creation of duplicate notebooks, but with different ids
        :param notebook_name name of the notebook
        :param content optional notebook content of the form {0:[line1, line2..],1:[line1, line2..]}
        """
        if not content:
            content = {}
        if owners == '':
            owners = self.currently_logged_in_user
        if 'admin' not in owners:
            owners += ",admin"
        data = {'name': notebook_name, 'paragraphs': []}
        for _, lines in content.items():
            data['paragraphs'].append({'text': "\n".join(lines)})
        if notebook_name == '':
            list_before_creation = self.get_list_of_all_notebooks()
        response = ZeppelinRestClientSession.__http_req(
            'post', 'notebook', data=json.dumps(data), cookies=self.session_cookies
        )
        if response.status_code == 201 or response.status_code == 200:
            logger.info("Successfully created a notebook %s with id %s", notebook_name, response.json()['body'])
            if notebook_name == '':
                list_after_creation = self.get_list_of_all_notebooks()
                notebook_name = "%s" % [n for n in list_after_creation if n not in list_before_creation][0]
            self.most_recently_created_notebook = {notebook_name: response.json()['body']}
            assert self.set_notebook_permissions(
                notebook_id=response.json()['body'], owners=owners, readers=readers, writers=writers, runners=runners
            )
            return True
        else:
            logger.error("Failed to create a new notebook %s", notebook_name)
            return False

    def bind_interpreter_to_notebook(self, notebook_id, interpreter_list):
        response = ZeppelinRestClientSession.__http_req(
            'put',
            'notebook/interpreter/bind/' + notebook_id,
            data=json.dumps(interpreter_list),
            cookies=self.session_cookies
        )
        return response.status_code == 201 or response.status_code == 200

    @TaskReporter.report_test()
    def get_notebook_id(self, notebook_name):
        return self.__get_notebook_id(notebook_name=notebook_name)

    def __get_notebook_id(self, notebook_name):
        ret_id = None
        notebook_list = self.get_list_of_all_notebooks()
        notebook_names = notebook_list.values()
        if notebook_names.count(notebook_name) == 0:
            logger.error("Failed to find id for the notebook: %s, doesn't exist", notebook_name)
        elif notebook_names.count(notebook_name) > 1:
            logger.error(
                "Failed to find id for the notebook: %s, multiple notebooks with the same name exist", notebook_name
            )
        else:
            for i, name in notebook_list.items():
                if name == notebook_name:
                    ret_id = i
                    break
        return ret_id

    @TaskReporter.report_test()
    def delete_notebook(self, notebook_name=None, notebook_id=None):
        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        response = ZeppelinRestClientSession.__http_req(
            'delete', 'notebook/%s' % notebook_id, cookies=self.session_cookies
        )
        if response.status_code == 200:
            logger.info("Successfully deleted the notebook: %s", notebook_name)
            return True
        else:
            logger.error("Failed to delete the notebook: %s", notebook_name)
            return False

    @TaskReporter.report_test()
    def export_notebook_into_file(self, notebook_name, output_file=None):
        notebook_id = self.__get_notebook_id(notebook_name)
        assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        response = ZeppelinRestClientSession.__http_req(
            'get', 'notebook/export/%s' % notebook_id, cookies=self.session_cookies
        )
        if response.status_code == 200:
            logger.info("Successfully exported the notebook: %s", notebook_name)
            if not output_file:
                output_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), notebook_name)
            with open(output_file, 'w') as exported:
                exported.write(json.dumps(response.json()['body']))
            return True
        else:
            logger.error("Failed to delete the notebook: %s", notebook_name)
            return False

    def __get_notebook_info(self, notebook_name=None, notebook_id=None):
        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        response = ZeppelinRestClientSession.__http_req(
            'get', 'notebook/%s' % notebook_id, cookies=self.session_cookies, log=True
        )
        if response.status_code == 200:
            return response.json()['body']
        else:
            logger.error("Failed to get the notebook information for notebook: %s(%s)", notebook_name, notebook_id)
            return None

    def __get_total_num_of_paragraphs(self, notebook_name=None, notebook_id=None):
        notebook_info = self.__get_notebook_info(notebook_name=notebook_name, notebook_id=notebook_id)
        if notebook_info:
            return len(notebook_info['paragraphs'])
        else:
            return None

    def __get_paragraph_id_by_index(self, index, notebook_name=None, notebook_id=None):
        notebook_info = self.__get_notebook_info(notebook_name=notebook_name, notebook_id=notebook_id)
        if notebook_info:
            paragraphs = notebook_info['paragraphs']
            if index > (len(paragraphs) - 1) or index < (-1 * len(paragraphs)):
                logger.error("Aborting get paragraph id by index : Index out of bound")
                return None
            else:
                pos_index = len(paragraphs) + index if index < 0 else index
                return paragraphs[pos_index]['id']
        else:
            return None

    def __get_paragraph_info(self, notebook_name=None, notebook_id=None, paragraph_id=None, index=None):
        if not ((notebook_name or notebook_id) and (paragraph_id or index)):
            raise ValueError, "Either notebook_name/notebook_id and paragraph_id/index must be provided"

        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        if not paragraph_id:
            paragraph_id = self.__get_paragraph_id_by_index(index, notebook_id=notebook_id)
            assert paragraph_id, "Could not find paragraph ID for paragraph: %s,notebook: %s%s" % (
                index, notebook_id, "(%s)" % notebook_name if notebook_name else ''
            )

        task = "/notebook/%s/paragraph/%s" % (notebook_id, paragraph_id)
        response = ZeppelinRestClientSession.__http_req('get', task, cookies=self.session_cookies)
        if response.status_code == 200:
            return response.json()['body']
        else:
            logger.error(
                "Failed to get the paragraph information for notebook: %s(%s), paragraph: %s(%s)", notebook_name,
                notebook_id, paragraph_id, index
            )
            return None

    def __get_paragraph_status_by_index(self, index, notebook_name=None, notebook_id=None):
        notebook_info = self.__get_notebook_info(notebook_name=notebook_name, notebook_id=notebook_id)
        if notebook_info:
            paragraphs = notebook_info['paragraphs']
            if index > (len(paragraphs) - 1) or index < (-1 * len(paragraphs)):
                logger.error("Aborting get paragraph id by index : Index out of bound")
                return None
            else:
                pos_index = len(paragraphs) + index if index < 0 else index
                return paragraphs[pos_index]['status']
        else:
            logger.error('Failed to get the current status of paragraph: %s from notebook: %s', index, notebook_name)
            return None

    def __wait_till_paragraph_finished_running(
            self, index, notebook_name=None, notebook_id=None, timeout=300, timeout_exc=None
    ):
        logger.info(
            "Waiting for paragraph: %s from notebook: %s(%s) to finish running", index, notebook_name, notebook_id
        )

        @implement_timeout(
            timeout,
            custom_logger=logger,
            failure_msg="Failed to run paragraph: %s from notebook: %s(%s) in timeout: %s sec" %
            (index, notebook_name, notebook_id, timeout),
            timeout_exc=timeout_exc
        )
        def real_wait_till_paragraph_finished_running(self, notebook_name, notebook_id, index):
            curr_status = self.__get_paragraph_status_by_index(
                notebook_name=notebook_name, notebook_id=notebook_id, index=index
            )
            logger.info(
                "current status of paragraph: %s from notebook: %s(%s): %s", index, notebook_name, notebook_id,
                curr_status
            )
            assert curr_status
            while ((curr_status == 'RUNNING' or curr_status == 'PENDING')
                   and not threading.currentThread().is_stopped()):
                time.sleep(5)
                curr_status = self.__get_paragraph_status_by_index(
                    notebook_name=notebook_name, notebook_id=notebook_id, index=index
                )
                assert curr_status
                logger.info(
                    "current status of paragraph: %s from notebook: %s(%s): %s", index, notebook_name, notebook_id,
                    curr_status
                )
            return curr_status == 'FINISHED'

        results = real_wait_till_paragraph_finished_running(self, notebook_name, notebook_id, index)
        return results

    @TaskReporter.report_test()
    def run_paragraph_by_index(
            self,
            index,
            notebook_name=None,
            notebook_id=None,
            timeout=300,
            app_user_getter=None,
            retries=4,
            timeout_exc=None
    ):
        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        paragraph_id = self.__get_paragraph_id_by_index(notebook_id=notebook_id, index=index)
        assert paragraph_id, "Could not find paragraph ID for paragraph: %s,notebook: %s" % (index, notebook_name)
        task = "notebook/job/%s/%s" % (notebook_id, paragraph_id)
        num_attempts = 0
        max_attempts = retries + 1

        while num_attempts < max_attempts:
            time.sleep(5 * random.random())
            response = ZeppelinRestClientSession.__http_req('post', task, cookies=self.session_cookies)
            if response.status_code == 200:
                if app_user_getter:
                    app_user_getter.start()

                if timeout_exc:
                    try:
                        para_results = self.__wait_till_paragraph_finished_running(
                            notebook_name=notebook_name,
                            notebook_id=notebook_id,
                            index=index,
                            timeout=timeout,
                            timeout_exc=timeout_exc
                        )
                        if app_user_getter:
                            app_user_getter.join()
                        if para_results[1]:
                            logger.info(
                                "Paragraph: %s from notebook: %s(%s) finished successfully", index, notebook_name,
                                notebook_id
                            )
                            return True
                        else:
                            logger.error(
                                "Paragraph: %s from notebook: %s(%s) resulted in error", index, notebook_name,
                                notebook_id
                            )
                            return False
                    except Exception:
                        if app_user_getter:
                            app_user_getter.join()
                        raise
                else:
                    para_results = self.__wait_till_paragraph_finished_running(
                        notebook_name=notebook_name,
                        notebook_id=notebook_id,
                        index=index,
                        timeout=timeout,
                        timeout_exc=timeout_exc
                    )
                    if para_results[0]:
                        if app_user_getter:
                            app_user_getter.join()
                        if para_results[1]:
                            logger.info(
                                "Paragraph: %s from notebook: %s(%s) finished successfully", index, notebook_name,
                                notebook_id
                            )
                            return True
                        else:
                            logger.error(
                                "Paragraph: %s from notebook: %s(%s) resulted in error", index, notebook_name,
                                notebook_id
                            )
                            return False
                    else:
                        if app_user_getter:
                            app_user_getter.join()
                        logger.error(
                            "Paragraph: %s from notebook: %s(%s) did not finish running within timeout: %s", index,
                            notebook_name, notebook_id, timeout
                        )
                        return False
            else:
                if num_attempts == retries:
                    logger.error(
                        "Failed to start running Paragraph: %s from notebook: %s(%s) after %s retries", index,
                        notebook_name, notebook_id, retries
                    )
                    return False
                num_attempts = num_attempts + 1

    @TaskReporter.report_test()
    def insert_new_paragraph(self, notebook_name=None, notebook_id=None, lines=None):
        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        task = "notebook/%s/paragraph" % notebook_id
        if lines:
            data = json.dumps({"text": "\n".join(lines)})
            response = ZeppelinRestClientSession.__http_req('post', task, data=data, cookies=self.session_cookies)
        else:
            response = ZeppelinRestClientSession.__http_req('post', task, cookies=self.session_cookies)
        if (response.status_code == 201 or response.status_code == 200):
            return True
        else:
            logger.error("Failed to insert a new paragraph in notebook: %s", notebook_name)
            return False

    @TaskReporter.report_test()
    def write_and_run_notebook(
            self, notebook_name='', content=None, timeout=1200, return_users=False, timeout_exc=None
    ):
        if not content:
            content = {}
        para_timeout = timeout / 2  #/len(content.keys())
        assert self.create_new_notebook(notebook_name=notebook_name, content=content)
        if return_users:
            users = []
            app_user_getter = AppUserGetter(1, "app_user_getter", 180, users)
        else:
            users = None
            app_user_getter = None
        for index in content.keys():
            logger.info(
                "Starting to run paragraph: %s in notebook: %s", index,
                self.most_recently_created_notebook.keys()[0]
            )
            para_result = self.run_paragraph_by_index(
                notebook_id=self.most_recently_created_notebook.values()[0],
                index=index,
                timeout=para_timeout,
                app_user_getter=app_user_getter,
                timeout_exc=timeout_exc
            )
            assert para_result
        return [True, users]

    @TaskReporter.report_test()
    def run_existing_notebook_sequentially(self, notebook_name, timeout=1200, return_users=False, timeout_exc=None):
        num_para = self.__get_total_num_of_paragraphs(notebook_name=notebook_name)
        para_timeout = timeout / 2  #/num_para
        if return_users:
            users = []
            app_user_getter = AppUserGetter(1, "app_user_getter", 180, users)
        else:
            users = None
            app_user_getter = None
        if num_para:
            for index in range(num_para):
                assert self.run_paragraph_by_index(
                    index=index,
                    notebook_name=notebook_name,
                    timeout=para_timeout,
                    app_user_getter=app_user_getter,
                    timeout_exc=timeout_exc
                )
            return (True, users)
        else:
            logger.error("Failed to find total number of paragraphs in notebook: %s", notebook_name)
            return (False, users)

    def is_user_logged_in(self, user):
        return self.currently_logged_in_user == user

    def is_someone_logged_in(self):
        return self.currently_logged_in_user is not None

    @TaskReporter.report_test()
    def set_notebook_permissions(
            self, notebook_name=None, notebook_id=None, owners='', readers='', writers='', runners=''
    ):
        if not ZeppelinServer.is_auth_enabled():
            logger.info("Setting up notebook permissions is not a valid operation in anonymous mode")
            return True
        if not notebook_id:
            notebook_id = self.__get_notebook_id(notebook_name)
            assert notebook_id, "Could not find notebook ID for notebook: %s" % notebook_name
        task = "notebook/%s/permissions" % notebook_id
        data = {'owners': [], 'readers': [], 'writers': [], 'runners': []}

        data['owners'] = owners.split(',')
        if readers != '':
            data['readers'] = readers.split(',')
        if writers != '':
            data['writers'] = writers.split(',')
        if runners != '':
            data['runners'] = runners.split(',')

        if data != {}:
            response = ZeppelinRestClientSession.__http_req(
                'put', task, data=json.dumps(data), cookies=self.session_cookies
            )
            if response.status_code == 200:
                logger.info(
                    "Notebook: %s%s permission is successfully set to %s%s%s%s", notebook_name
                    if notebook_name is not None else '', ("(%s)" % notebook_id), ("owners:%s, " % owners)
                    if owners is not None else '', ("readers:%s, " % readers)
                    if readers is not None else '', ("writers:%s, " % writers)
                    if writers is not None else '', ("runners:%s" % runners) if runners is not None else ''
                )
                return True
            else:
                logger.error(
                    "Failed to set permissions for Notebook: %s%s to %s%s%s%s", notebook_name
                    if notebook_name is not None else '', ("(%s)" % notebook_id), ("owners:%s, " % owners)
                    if owners is not None else '', ("readers:%s, " % readers)
                    if readers is not None else '', ("writers:%s, " % writers)
                    if writers is not None else '', ("runners:%s" % runners) if runners is not None else ''
                )
                return False
        else:
            return True

    @TaskReporter.report_test()
    def is_notebook_present(self, notebook_name):
        notebook_list = self.get_list_of_all_notebooks()
        return notebook_list and (notebook_name in notebook_list.values())

    def __get_all_registerd_interpreter_info(self):
        task = "interpreter/setting"
        response = ZeppelinRestClientSession.__http_req('get', task, cookies=self.session_cookies)
        if response.status_code == 200:
            info = {}
            for int_setting in response.json()['body']:
                info[int_setting['name']] = int_setting
            return info
        else:
            logger.error("Failed to get the interpreter settings information")
            return None

    def get_interpreter_info(self):
        return self.__get_all_registerd_interpreter_info()

    @TaskReporter.report_test()
    def get_list_of_supported_interpreters(self):
        task = "interpreter/setting"
        supported_interpreters = []
        response = ZeppelinRestClientSession.__http_req('get', task, cookies=self.session_cookies)
        if response.status_code == 200:
            for int_setting in response.json()['body']:
                supported_interpreters.append(int_setting['name'])
        else:
            logger.error("Failed to get the interpreter settings information")
        assert supported_interpreters, "Failed to get list of supported interpreters"
        return supported_interpreters

    def __get_interpreter_id(self, interpreter_name):
        int_info = self.__get_all_registerd_interpreter_info()
        if int_info and interpreter_name in int_info.keys():
            return int_info[interpreter_name]['id']
        else:
            return None

    def __get_interpreter_info_struct(self, interpreter_name):
        int_info = self.__get_all_registerd_interpreter_info()
        if int_info and interpreter_name in int_info.keys():
            return int_info[interpreter_name]
        else:
            return None

    @TaskReporter.report_test()
    def create_new_interpreter_setting(
            self,
            interpreter_name,
            base_interpreter,
            add_properties=None,
            remove_properties=None,
            modify_properties=None,
            add_dependencies=None,
            remove_dependencies=None,
            modify_dependencies=None
    ):
        if not add_properties:
            add_properties = {}
        if not remove_properties:
            remove_properties = []
        if not modify_properties:
            modify_properties = {}
        if not add_dependencies:
            add_dependencies = []
        if not remove_dependencies:
            remove_dependencies = []
        if not modify_dependencies:
            modify_dependencies = {}
        base_interpreter_info = self.__get_interpreter_info_struct(base_interpreter)
        data = {}
        if base_interpreter_info:
            data['name'] = interpreter_name
            data['group'] = base_interpreter_info['group']
            data['properties'] = base_interpreter_info['properties']
            data['interpreterGroup'] = base_interpreter_info['interpreterGroup']
            data['dependencies'] = base_interpreter_info['dependencies']
            for prop in remove_properties:
                if prop in data['properties']:
                    del data['properties'][prop]
                else:
                    logger.warn(
                        "Property: %s is not present for interpreter: %s, ignoring removal", prop, base_interpreter
                    )
            for prop, val_dict in add_properties.items():
                if not prop in data['properties']:
                    data['properties'][prop] = {'type': val_dict['type'], 'name': prop, 'value': val_dict['value']}
            for prop, val in modify_properties.items():
                if prop in data['properties']:
                    data['properties'][prop]['value'] = val
                else:
                    logger.warn(
                        "Property: %s is not present for interpreter: %s, ignoring modification", prop,
                        interpreter_name
                    )

            dep_json_items = [item['groupArtifactVersion'] for item in data['dependencies']]
            for dep in remove_dependencies:
                if dep in dep_json_items:
                    index = dep_json_items.index(dep)
                    data['dependencies'][index] = {}
                if {} in data['dependencies']:
                    data['dependencies'].remove({})
            for dep in add_dependencies:
                if not dep in dep_json_items:
                    data['dependencies'].append({'groupArtifactVersion': dep, 'local': False})
            for dep, val in modify_dependencies.items():
                if dep in dep_json_items:
                    index = dep_json_items.index(dep)
                    data['dependencies'][index]['groupArtifactVersion'] = dep
                else:
                    logger.warn(
                        "Dependency: %s is not present for interpreter: %s, ignoring modification", dep,
                        base_interpreter
                    )

            task = "interpreter/setting"
            response = ZeppelinRestClientSession.__http_req(
                'post', task, data=json.dumps(data), cookies=self.session_cookies
            )
            if (response.status_code == 201 or response.status_code == 200):
                return True
            else:
                logger.error("Failed to create settings for intrpreter: %s", interpreter_name)
                return False
        else:
            logger.error("Failed to read base interpreter settings for interpreter: %s", base_interpreter)
            return False

    def __edit_interpreter_settings(
            self,
            interpreter_name=None,
            interpreter_info=None,
            add_properties=None,
            remove_properties=None,
            modify_properties=None,
            add_dependencies=None,
            remove_dependencies=None,
            modify_dependencies=None,
            modify_options=None
    ):
        if not add_properties:
            add_properties = {}
        if not remove_properties:
            remove_properties = []
        if not modify_properties:
            modify_properties = {}
        if not add_dependencies:
            add_dependencies = []
        if not remove_dependencies:
            remove_dependencies = []
        if not modify_dependencies:
            modify_dependencies = {}
        if not modify_options:
            modify_options = {}
        if not interpreter_info:
            assert interpreter_name, "must supply interpreter_name or interpreter_info arg"
            interpreter_info = self.__get_interpreter_info_struct(interpreter_name)
            assert interpreter_info, "Failed to find interpreter settings for interpreter: %s" % interpreter_name
        if not interpreter_name:
            interpreter_name = interpreter_info['name']

        interpreter_id = interpreter_info['id']
        data = copy.deepcopy(interpreter_info)

        for prop in remove_properties:
            if prop in data['properties']:
                del data['properties'][prop]
            else:
                logger.warn(
                    "Property: %s is not present for interpreter: %s, ignoring removal", prop, interpreter_name
                )
        # while adding properties
        # change_map should look like
        # change_map = {'properties': {
        #                   'add':{
        #                       name:{ 'type': type, 'value': value },
        #                       name:{ 'type': type, 'value': value }....
        #                    }
        #                }
        #              }
        # possible zeppelin types are 'number','string', 'checkbox', 'textarea' etc
        # value field in the change_map must be python boolean when zeppelin type is 'checkbox',
        #  it will be python string type for rest of the zeppelin types
        for prop, val_dict in add_properties.items():
            if not prop in data['properties']:
                data['properties'][prop] = {'type': val_dict['type'], 'name': prop, 'value': val_dict['value']}
        # while modifying properties
        # change map should look like
        # change_map = {'properties': {
        #                   'modify':{
        #                       name: new_value,
        #                       name: new_value,....
        #                    }
        #                }
        #              }
        # value field in the change_map must be python boolean when modifying a checkbox,
        # it will be python string type for rest of the properties
        for prop, val in modify_properties.items():
            if prop in data['properties']:
                data['properties'][prop]['value'] = val
            else:
                logger.warn(
                    "Property: %s is not present for interpreter: %s, ignoring modification", prop, interpreter_name
                )

        dep_json_items = [item['groupArtifactVersion'] for item in data['dependencies']]
        for dep in remove_dependencies:
            if dep in dep_json_items:
                index = dep_json_items.index(dep)
                data['dependencies'][index] = {}
            if {} in data['dependencies']:
                data['dependencies'].remove({})
        for dep in add_dependencies:
            if not dep['name'] in dep_json_items:
                dep_dict = {'groupArtifactVersion': dep['name'], 'local': False}
                if 'exclusions' in dep.keys() and dep['exclusions']:
                    dep_dict['exclusions'] = dep['exclusions']
                data['dependencies'].append(dep_dict)
        for dep, val in modify_dependencies.items():
            if dep in dep_json_items:
                index = dep_json_items.index(dep)
                data['dependencies'][index]['groupArtifactVersion'] = val['name']
                if 'exclusions' in data['dependencies'][index].keys():
                    if 'exclusions' in val.keys():
                        if val['exclusions']:
                            data['dependencies'][index]['exclusions'] = val['exclusions']
                        else:
                            del data['dependencies'][index]['exclusions']
                    else:
                        del data['dependencies'][index]['exclusions']
                else:
                    if 'exclusions' in val.keys() and val['exclusions']:
                        data['dependencies'][index]['exclusions'] = val['exclusions']
            else:
                logger.warn(
                    "Dependency: %s is not present for interpreter: %s, ignoring modification", dep, interpreter_name
                )

        task = "interpreter/setting/%s" % interpreter_id
        msg = "\n********** list of modifications to interpreter:%s **********\n" % interpreter_name
        if add_properties:
            msg += "add properties: %s\n" % add_properties
        if modify_properties:
            msg += "modify properties: %s\n" % modify_properties
        if remove_properties:
            msg += "remove properties: %s\n" % remove_properties
        if add_dependencies:
            msg += "add dependencies: %s\n" % add_dependencies
        if modify_dependencies:
            msg += "modify dependencies: %s\n" % modify_dependencies
        if remove_dependencies:
            msg += "remove dependencies: %s\n" % remove_dependencies
        msg += "************************************************************************\n"
        logger.info(msg)

        for prop, val_dict in modify_options.items():
            data['option'][prop] = val_dict

        response = ZeppelinRestClientSession.__http_req(
            'put', task, data=json.dumps(data), cookies=self.session_cookies
        )
        if response.status_code == 200:
            msg = "\n********** interpreter: %s setting after successfully editing **********\n" % interpreter_name
            msg += "%s\n" % pprint.pformat(self.__get_interpreter_info_struct(interpreter_name), indent=4, width=100)
            msg += "************************************************************************\n"
            logger.info(msg)
            return True
        else:
            logger.error("Failed to change settings for intrpreter: %s", interpreter_info['name'])
            return False

    @TaskReporter.report_test()
    def edit_interpreters(self, change_map=None):
        if not change_map:
            change_map = {}
        all_interpreter_settings = self.__get_all_registerd_interpreter_info()

        for interpreter_name in change_map.keys():
            property_map = change_map[interpreter_name]['properties'] if 'properties' in change_map[interpreter_name
                                                                                                   ].keys() else None
            dep_map = change_map[interpreter_name]['dependencies'] if 'dependencies' in change_map[interpreter_name
                                                                                                  ].keys() else None
            if property_map:
                add_properties = property_map['add'] if 'add' in property_map.keys() else {}
                remove_properties = property_map['remove'] if 'remove' in property_map.keys() else []
                modify_properties = property_map['modify'] if 'modify' in property_map.keys() else {}
                options = property_map['option'] if 'option' in property_map.keys() else {}
            else:
                add_properties = {}
                remove_properties = []
                modify_properties = {}
                options = {}

            if dep_map:
                add_dependencies = dep_map['add'] if 'add' in dep_map.keys() else []
                remove_dependencies = dep_map['remove'] if 'remove' in dep_map.keys() else []
                modify_dependencies = dep_map['modify'] if 'modify' in dep_map.keys() else {}
            else:
                add_dependencies = []
                remove_dependencies = []
                modify_dependencies = {}

            if all_interpreter_settings:
                assert self.__edit_interpreter_settings(
                    interpreter_info=all_interpreter_settings[interpreter_name],
                    add_properties=add_properties,
                    remove_properties=remove_properties,
                    modify_properties=modify_properties,
                    add_dependencies=add_dependencies,
                    remove_dependencies=remove_dependencies,
                    modify_dependencies=modify_dependencies,
                    modify_options=options
                ), "Failed to edit interpreter '%s'" % interpreter_name
            else:
                assert self.__edit_interpreter_settings(
                    interpreter_name=interpreter_name,
                    add_properties=add_properties,
                    remove_properties=remove_properties,
                    modify_properties=modify_properties,
                    add_dependencies=add_dependencies,
                    remove_dependencies=remove_dependencies,
                    modify_dependencies=modify_dependencies,
                    modify_options=options
                ), "Failed to edit interpreter '%s'" % interpreter_name
        return True

    @TaskReporter.report_test()
    def get_interpreter_property(self, interpreter_name, interpreter_property):
        properties = self.get_all_interpreter_properties(interpreter_name)
        if properties and interpreter_property in properties.keys():
            return properties[interpreter_property]['value']
        else:
            return None

    @TaskReporter.report_test()
    def get_all_interpreter_properties(self, interpreter_name):
        interpreter_info = self.__get_interpreter_info_struct(interpreter_name)
        if interpreter_info:
            return interpreter_info['properties']
        else:
            return None

    @TaskReporter.report_test()
    def restart_interpreters(self, interpreter_names):
        interpreter_names = [interpreter_names] if not isinstance(interpreter_names, list) else interpreter_names
        for interpreter_name in interpreter_names:
            interpreter_id = self.__get_interpreter_id(interpreter_name)
            if interpreter_id:
                task = "interpreter/setting/restart/%s" % interpreter_id

                response = ZeppelinRestClientSession.__http_req('put', task, cookies=self.session_cookies)
                if not response.status_code == 200:
                    logger.error("Failed to restart interpreter: %s", interpreter_name)
                    return False
            else:
                logger.info("Failed to find interpreter: %s, ignoring restart", interpreter_name)
        return True

    @TaskReporter.report_test()
    def add_interpreter_repo(self, interpreter_id, url):
        task = 'interpreter/repository'
        data = {'id': interpreter_id, 'url': url, 'snapshot': False}
        response = ZeppelinRestClientSession.__http_req(
            'post', task, data=json.dumps(data), cookies=self.session_cookies
        )
        if (response.status_code == 201 or response.status_code == 200):
            return True
        else:
            logger.error("Failed to add interpreter repository url: %s", url)
            return False

    @TaskReporter.report_test()
    def delete_interpreter_repo(self, interpreter_id):
        task = 'interpreter/repository/%s' % interpreter_id
        response = ZeppelinRestClientSession.__http_req('delete', task, cookies=self.session_cookies)
        if response.status_code == 200:
            return True
        else:
            logger.error("Failed to delete interpreter repository: %s", interpreter_id)
            return False

    @TaskReporter.report_test()
    def parse_all_results(self, notebook_name):
        results = {}
        notebook_info = self.__get_notebook_info(notebook_name=notebook_name)
        if notebook_info:
            for index, paragraph in enumerate(notebook_info['paragraphs']):
                if paragraph['status'] != 'READY':
                    if ('results' in paragraph.keys()) and paragraph['results']:
                        if isinstance(paragraph['results'], dict) and ('msg' in paragraph['results'].keys()):
                            if paragraph['results']['msg']:
                                result_section = paragraph['results']['msg'][0]
                                if result_section['type'] == 'TEXT' or result_section['type'] == 'HTML':
                                    content = result_section['data'].split("\n")
                                    if '' in content:
                                        content.remove('')
                                    results[index] = {'type': result_section['type'], 'content': content}
                                elif result_section['type'] == 'TABLE':
                                    results[index] = {'type': result_section['type'], 'content': []}
                                    for row_num, row in enumerate(result_section['data'].split("\n")):
                                        if row_num != 0 and row != '':
                                            splits = row.split("\t")
                                            if splits.count('') != len(splits):
                                                results[index]['content'].append(splits)
                            else:
                                results[index] = {'type': None, 'content': None}
                        elif isinstance(paragraph['results'], unicode):
                            results[index] = {'type': 'TEXT', 'content': paragraph['results']}
                        else:
                            results[index] = {'type': None, 'content': None}
                    else:
                        results[index] = {'type': None, 'content': None}
            return results
        else:
            logger.error("Failed to parse the results for notebook: %s", notebook_name)
            return None

    @TaskReporter.report_test()
    def upsert_interpreter_property(self, interpreter_name, property_name, property_value, property_type='string'):
        """
        This method either edits the intepreter property speficied or adds it with the provided value.
        If the value to be set is None, the parameter is removed.
        It return the previous value of the property, if any, None otherwise.

        :param interpreter_name: the name of the interpreter to modify
        :param property_name: the name of the property to modify
        :param property_value: the new value to be set or None to delete the property
        :return: the previous value of the prorperty, if any, None otherwise
        """
        old_value = self.get_interpreter_property(interpreter_name, property_name)

        if property_value is None:
            assert self.edit_interpreters(
                {interpreter_name: {"properties": {"remove": [property_name]}}}), \
                "Failed to edit %s on interpreter %s" % (property_name, interpreter_name)
            return old_value

        if old_value:
            if old_value != property_value:
                assert self.edit_interpreters(
                    {interpreter_name: {"properties": {"modify": {property_name: property_value}}}}), \
                    "Failed to edit %s on interpreter %s" % (property_name, interpreter_name)
        else:
            assert self.edit_interpreters(
                {interpreter_name: {"properties": {"add": {property_name: {'type' : property_type,
                                                                           'value' : property_value}}}}}), \
                "Failed to add %s on interpreter %s" % (property_name, interpreter_name)

        return old_value


class ZeppelinUIClientSession(ZeppelinClient):

    # Central place to store and all zeppelin UI related xpaths
    __supported_visualizations = [
        'table', 'multiBarChart', 'pieChart', 'stackedAreaChart', 'lineChart', 'scatterChart'
    ]

    def __init__(self, knox=False, session_id=None, is_sso=False):
        super(ZeppelinUIClientSession, self).__init__()
        self._currently_open_notebook = None
        self._most_recently_created_notebook = None
        self._notebook_default_interpreter_order = {}
        self.session_type = 'ui'
        if session_id:
            self.id = session_id
        else:
            self.id = ZeppelinClient._session_counter
        self.knox = knox
        self.is_sso = is_sso
        self.driver = ZeppelinUIClientSession.__instantiate_zeppelin_webdriver()
        assert self.driver, "Could not initialize selenium webdriver"
        ZeppelinServer.register_session(self)

    def delete_session(self):
        self.quit_zeppelin_webdriver()
        ZeppelinServer.remove_session(self)

    @TaskReporter.report_test()
    def notify(self, event):
        if event == 'stop':
            if self.is_zeppelin_server_running:
                self._currently_open_notebook = None
                self._most_recently_created_notebook = None
                self.currently_logged_in_user = None
            self.is_zeppelin_server_running = False
        elif event == 'start':
            if not self.is_zeppelin_server_running:
                self.quit_zeppelin_webdriver()
                self.driver = ZeppelinUIClientSession.__instantiate_zeppelin_webdriver()
            self.is_zeppelin_server_running = True

    @TaskReporter.report_test()
    def get_screenshot_as_file(self, screen_shot_file, timeout=10):
        def timeout_handler(signum, frame):
            logger.info("signum = %s, frame = %s", signum, frame)
            raise TimeoutException("Timeout occurred while capturing screenshot: %s seconds" % timeout)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        try:
            self.driver.get_screenshot_as_file(screen_shot_file)
        except TimeoutException, e:
            logger.error("%s", e)
        finally:
            signal.alarm(0)

    @classmethod
    def __instantiate_zeppelin_webdriver(cls):
        os.environ['DISPLAY'] = ":99"
        Machine.runas(Machine.getAdminUser(), "dbus-uuidgen --ensure")
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.privatebrowsing.autostart", True)
        profile.set_preference("network.http.phishy-userpass-length", 255)
        profile.set_preference("network.automatic-ntlm-auth.trusted-uris", "x.x.x.x")
        profile.accept_untrusted_certs = True
        driver = None

        # def restart_xvfb():
        #     selenium_process_list = Machine.getProcessList(filter='selenium-server')
        #     selenium_pids = [int(p.split()[1]) for p in selenium_process_list]
        #     selenium_cmds = [' '.join(p.split()[3:]) for p in selenium_process_list]
        #     selenium_hub_cmd = None
        #     selenium_wd_cmd = None
        #     for cmd in selenium_cmds:
        #         if "role hub" in cmd:
        #             selenium_hub_cmd = cmd + "  > /tmp/selenium-hub.log 2>&1 &"
        #         if "role webdriver" in cmd:
        #             selenium_wd_cmd = cmd + " > /tmp/selenium-node.log 2>&1 &"
        #     assert selenium_hub_cmd and selenium_wd_cmd, "Failed to find selenium-server processes and restart them"
        #     for pid in selenium_pids:
        #         Machine.killProcessRemote(pid, host=None, user=Machine.getAdminUser(), passwd=None, logoutput=True)
        #     xvfb_pid = [int(p.split()[1]) for p in Machine.getProcessList(filter='Xvfb')]
        #     for pid in xvfb_pid:
        #         Machine.killProcessRemote(pid, host=None, user=Machine.getAdminUser(), passwd=None, logoutput=True)
        #     Machine.rm(Machine.getAdminUser(), None, "/tmp/.X99-lock", isdir=False, passwd=None)
        #     Machine.runas(Machine.getAdminUser(), selenium_hub_cmd, host=None)
        #     Machine.runas(Machine.getAdminUser(), selenium_wd_cmd, host=None)
        #     Machine.runinbackgroundAs(Machine.getAdminUser(), cmd="Xvfb :99 -ac -screen 0 1280x1024x24", host=None)
        #     time.sleep(10)

        num_attempts = 0
        max_attempts = 5
        while num_attempts < max_attempts:
            try:
                driver = Selenium.getWebDriver(browserType='firefox', platformType='LINUX', browser_profile=profile)
                break
            except Exception, e:
                if num_attempts < max_attempts - 1:
                    #restart_xvfb()
                    pass
                else:
                    logger.error("attempt : %s , Failed to get webdriver for Zeppelin : %s", num_attempts, e)
                time.sleep(2)
                num_attempts = num_attempts + 1
        return driver

    @TaskReporter.report_test()
    def quit_zeppelin_webdriver(self):
        try:
            Selenium.quitWebDriver(self.driver)
        except Exception:
            self.driver = None
            logger.warn("Ignoring webdriver quit failure")

    def __restart_zeppelin_webdriver(self):
        self.quit_zeppelin_webdriver()
        self.driver = ZeppelinUIClientSession.__instantiate_zeppelin_webdriver()

    def __is_driver_on_homepage(self):
        if self.knox:
            return self.driver.current_url == "%s#/" % ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso)
        else:
            return self.driver.current_url == "%s/#/" % ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso)

    def __is_driver_on_interpreterspage(self):
        if self.knox:
            return self.driver.current_url == "%s#/interpreter" % ZeppelinServer.get_base_url(
                knox=self.knox, is_sso=self.is_sso
            )
        else:
            return self.driver.current_url == "%s/#/interpreter" % ZeppelinServer.get_base_url(
                knox=self.knox, is_sso=self.is_sso
            )

    @TaskReporter.report_test()
    def is_zeppelin_connected(self):
        max_attempts = 5
        num_attempts = 0
        while num_attempts < max_attempts:
            if Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS["green_circle"]):
                return True
            else:
                logger.info("Connected status is not visible in trial: %s, refreshing page", (num_attempts + 1))
                try:
                    self.driver.refresh()
                    time.sleep(5)
                except Exception:
                    self.get_screenshot_as_file(
                        os.path.join(Config.getEnv('ARTIFACTS_DIR'), "trial_%s.png" % num_attempts)
                    )

                finally:
                    num_attempts += 1
        return False

    @TaskReporter.report_test()
    def is_zeppelin_disconnected(self):
        max_attempts = 5
        num_attempts = 0
        while num_attempts < max_attempts:
            if Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS["red_circle"]):
                return True
            else:
                logger.info("Connected status is not visible in trial: %s, refreshing page", (num_attempts + 1))
                try:
                    self.driver.refresh()
                    time.sleep(5)
                except Exception:
                    self.get_screenshot_as_file(
                        os.path.join(Config.getEnv('ARTIFACTS_DIR'), "trial_%s.png" % num_attempts)
                    )

                finally:
                    num_attempts = num_attempts + 1
        return False

    @TaskReporter.report_test()
    def load_ambari_homepage(self, knox=False):
        logger.info("knox = %s", knox)
        base_url = Ambari.getWebUrl()
        if self.is_sso:
            host = socket.gethostbyname(socket.gethostname())
            host_name = socket.gethostname()
            if ".hwx.site" not in host_name:
                host_name = host_name + ".hwx.site"
            base_url = base_url.replace(
                host, host_name
            )  # Change required for SSO. JWT cookie is associated only with hostname
        logger.info("Ambari URL = %s", str(base_url))
        self.driver.get(base_url)

    @TaskReporter.report_test()
    def load_zeppelin_homepage(self):
        max_attempts = 3
        num_attempts = 0
        while num_attempts < max_attempts:
            try:
                self.driver.get(ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso))
            except Exception:
                logger.info(
                    "trial %s : Failed to load zeppelin UI from URL : %s", num_attempts,
                    ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso)
                )
                num_attempts = num_attempts + 1
            else:
                Selenium.setWebDriverWinSize(self.driver, 1920, 1080)
                refresh_trial = 5
                for _ in range(refresh_trial):
                    try:
                        self.driver.refresh()
                    except Exception:
                        pass
                    time.sleep(3)
                time.sleep(10)
                if self.is_sso:
                    welcome_msg_visible = Selenium.isElementVisibleInSometime(
                        self.driver, xpath=ZEPPELIN_XPATHS["sso_login_page"], maxWait=60) or \
                                          Selenium.isElementVisibleInSometime(self.driver,
                                                                              xpath=ZEPPELIN_XPATHS["welcome_msg"],
                                                                              maxWait=60)

                    logger.info("is welcome_msg_visible = %s", str(welcome_msg_visible))
                else:
                    welcome_msg_visible = Selenium.isElementVisibleInSometime(
                        self.driver, xpath=ZEPPELIN_XPATHS["welcome_msg"], maxWait=60
                    )
                if welcome_msg_visible:
                    logger.info(
                        "trial %s : Successfully loaded zeppelin UI from URL : %s", num_attempts,
                        ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso)
                    )
                    self._currently_open_notebook = None
                    return True
                else:
                    logger.info(
                        "trial %s : Failed to load zeppelin UI from URL : %s", num_attempts,
                        ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso)
                    )
                    num_attempts = num_attempts + 1
                    logger.info("Restarting webdriver..")
                    self.__restart_zeppelin_webdriver()
                    logger.info("Restarting Zeppelin Webserver..")
                    assert ZeppelinServer.restart(), "Failed to restart Zeppelin webserver while " \
                                                     "trying to make more attempts to load zeppelin UI"

        logger.error(
            "Failed to load zeppelin UI from URL : %s after %s trials",
            ZeppelinServer.get_base_url(knox=self.knox, is_sso=self.is_sso), num_attempts
        )
        return False

    @TaskReporter.report_test()
    def load_zeppelin_interpreters_page(self):
        if self.__is_driver_on_interpreterspage():
            return True
        if not self.__is_driver_on_homepage():
            assert self.load_zeppelin_homepage(), "Failed to load zeppelin home page"
        if ZeppelinServer.is_auth_enabled():
            pattern_dropdown = ZEPPELIN_XPATHS['pattern_connected_username'] % self.currently_logged_in_user
        else:
            pattern_dropdown = ZEPPELIN_XPATHS['pattern_connected_username'] % 'anonymous'
        dropdown = Selenium.getElement(self.driver, xpath=pattern_dropdown)
        assert Selenium.isElementCurrentlyDisplayed(element=dropdown), "dropdown menu is not visible"
        Selenium.click(self.driver, dropdown)
        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_link']
        ), "interpreter link is not visible in dropdown"
        interpreter_link = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['interpreter_link'])
        Selenium.click(self.driver, interpreter_link)
        time.sleep(15)
        self._currently_open_notebook = None
        return True

    @TaskReporter.report_test()
    def navigate_back_to_homepage(self):
        back_link = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['navigate_back_to_homepg'])
        assert back_link, "Zeppelin logo is not visible"
        Selenium.click(self.driver, back_link)
        if Machine.isHumboldt():
            trial = 10
        else:
            trial = 5
        for _ in range(trial):
            try:
                self.driver.refresh()
            except Exception:
                pass
            time.sleep(3)
        time.sleep(10)
        assert self.__is_driver_on_homepage(), "Failed to navigate back to zeppelin home page"
        self._currently_open_notebook = None
        return True

    @TaskReporter.report_test()
    def login_using_sso(self, user_name, password):
        '''
        Method to use when SSO is enabled. It will login using knox login form

        :param user_name:
        :param password:
        :return:
        '''
        username_xpath = ZEPPELIN_XPATHS['sso_username_xpath']
        password_xpath = ZEPPELIN_XPATHS['sso_password_xpath']
        login_btn_xpath = ZEPPELIN_XPATHS['sso_login_btn_xpath']
        login_error_xpath = ZEPPELIN_XPATHS['sso_login_error']
        assert Selenium.isElementVisibleInSometime(self.driver, xpath=username_xpath), "username field is not visible"
        username_field = Selenium.getElement(self.driver, xpath=username_xpath)
        Selenium.sendKeys(username_field, user_name)

        assert Selenium.isElementVisibleInSometime(self.driver, xpath=password_xpath), "password field is not visible"
        password_field = Selenium.getElement(self.driver, xpath=password_xpath)
        Selenium.sendKeys(password_field, password)

        assert Selenium.isElementVisibleInSometime(self.driver, xpath=login_btn_xpath), "login button is not visible"
        login_button = Selenium.getElement(self.driver, xpath=login_btn_xpath)
        Selenium.click(self.driver, login_button)
        logger.info("Clicked on login button")
        Selenium.waitTillElementDisappears(driver=self.driver, xpath=username_xpath, maxWait=20)
        assert not Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=login_error_xpath), \
            "Login failed due to Invalid Username/password"
        cookies_list = self.driver.get_cookies()
        logger.info("Cookie list = %s", str(cookies_list))
        self.currently_logged_in_user = user_name

    @TaskReporter.report_test()
    def login_to_zeppelin_using_sso(self, user_name, password):
        '''
        Method to login to zeppelin when SSO is enabled

        :param user_name:
        :param password:
        :return:
        '''
        self.login_using_sso(user_name, password)
        pattern_connected_username = ZEPPELIN_XPATHS["pattern_connected_username"] % user_name
        return (
            Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=pattern_connected_username)
            and self.is_zeppelin_connected()
        )

    @TaskReporter.report_test()
    def login_to_ambari_using_sso(self, user_name, password):
        '''
        Method to login to Ambari when SSO is enabled.
        :param user_name:
        :param password:
        :return:
        '''
        if self.is_sso:
            ambari_loggedin_user_xpath = ZEPPELIN_XPATHS['ambari_loggedin_user'].format(user_name)
            self.login_using_sso(user_name, password)
            Selenium.waitTillElementBecomesVisible(driver=self.driver, xpath=ambari_loggedin_user_xpath, maxWait=30)
            return Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=ambari_loggedin_user_xpath)
        else:
            logger.error("login_to_ambari_using_sso should be exercised only when sso is enabled")
            return False

    @TaskReporter.report_test()
    def login(self, user_name, password):
        if self.is_user_logged_in(user_name):
            return True
        elif self.is_someone_logged_in():
            logger.error("Logout user: %s first", self.currently_logged_in_user)
            return False

        if not self.__is_driver_on_homepage():
            assert self.load_zeppelin_homepage(), "Failed to load zeppelin homepage"

        if self.is_sso:
            logger.info("**** Logging in using SSO")
            return self.login_to_zeppelin_using_sso(user_name, password)

        login_window_show_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS["login_window_show_button"])
        assert Selenium.isElementCurrentlyDisplayed(element=login_window_show_button), "LOGIN button not available"
        Selenium.click(self.driver, login_window_show_button)
        assert Selenium.waitTillElementBecomesVisible(
            self.driver, xpath=ZEPPELIN_XPATHS["login_window"]
        ), "Login window not visible"

        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['username_field']
        ), "username field is not visible"
        username_field = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['username_field'])
        Selenium.sendKeys(username_field, user_name)

        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['password_field']
        ), "password field is not visible"
        password_field = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['password_field'])
        Selenium.sendKeys(password_field, password)

        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS["login_button"]
        ), "login button is not visible"
        login_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS["login_button"])
        Selenium.click(self.driver, login_button)

        if not Selenium.waitTillElementDisappears(self.driver, xpath=ZEPPELIN_XPATHS["login_window"]):
            assert Selenium.isElementCurrentlyDisplayed(
                driver=self.driver, xpath=ZEPPELIN_XPATHS['login_error_alert']
            ), "login window does not disappear even though username/password alert not displayed"
            logger.error("username/password combination is incorrect")
            login_window_close_button = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['login_window_close_button']
            )
            assert Selenium.isElementCurrentlyDisplayed(
                element=login_window_close_button
            ), "close button not available on login window"
            Selenium.click(self.driver, login_window_close_button)
            assert Selenium.waitTillElementDisappears(
                self.driver, xpath=ZEPPELIN_XPATHS["login_window"]
            ), "login window does not disappear even though close button is clicked"
            return False
        else:
            pattern_connected_username = ZEPPELIN_XPATHS["pattern_connected_username"] % user_name
            if (Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=pattern_connected_username)
                    and self.is_zeppelin_connected()):
                self.currently_logged_in_user = user_name
                return True
            else:
                return False

    @TaskReporter.report_test()
    def is_user_logged_in(self, user):
        assert ZeppelinServer.is_auth_enabled(
        ), "Authentication disabled in shiro.ini, can not login as user: %s" % user
        pattern_connected_username = ZEPPELIN_XPATHS["pattern_connected_username"] % user
        logged = Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=pattern_connected_username)
        return logged

    @TaskReporter.report_test()
    def is_someone_logged_in(self):
        if Selenium.isElementCurrentlyDisplayed(
                driver=self.driver, xpath=ZEPPELIN_XPATHS['login_window_show_button']) or  \
                Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=ZEPPELIN_XPATHS['sso_login_page']):
            return False
        else:
            return True

    @TaskReporter.report_test()
    def is_login_button_visible(self):
        if self.__is_driver_on_homepage():
            element = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['login_window_show_button'])
            if element:
                return Selenium.isElementCurrentlyDisplayed(element=element)
            else:
                return False
        else:
            logger.error("driver is not on home page, is_login_button_visible = False")
            return False

    @TaskReporter.report_test()
    def logout(self):
        if not self.currently_logged_in_user:
            logger.error("No user logged in currently")
            return False

        pattern_connected_username = ZEPPELIN_XPATHS['pattern_connected_username'] % self.currently_logged_in_user
        dropdown = Selenium.getElement(self.driver, xpath=pattern_connected_username)
        if Selenium.isElementCurrentlyDisplayed(element=dropdown):
            Selenium.click(self.driver, dropdown)
            if Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['logout_link'], maxWait=30):
                logout = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['logout_link'])
                Selenium.click(self.driver, logout)
                if Machine.isHumboldt():
                    for _ in range(5):
                        self.driver.refresh()
                        time.sleep(5)
                    time.sleep(10)
                if Selenium.isElementVisibleInSometime(driver=self.driver,
                                                       xpath=ZEPPELIN_XPATHS['login_window_show_button'], maxWait=180):
                    logger.info("User : %s successfully logged out", self.currently_logged_in_user)
                    self.currently_logged_in_user = None
                    self._currently_open_notebook = None
                    return True
                else:
                    logger.error("Failed to logout User : %s", self.currently_logged_in_user)
                    return False
            else:
                logger.error("No logout link is available for logging out User : %s", self.currently_logged_in_user)
                return False
        else:
            logger.error("No dropdown menu available for logging out User : %s", self.currently_logged_in_user)
            return False

    def __check_and_dismiss_insuff_priv_box(self):
        box_present = False
        #cancel_button = False
        #close_button = False
        for _ in range(0, 1):
            #close_button = Selenium.isElementVisibleInSometime(driver= self.driver,
            #  xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_close_button'], maxWait=15)
            #if not close_button:
            #    cancel_button = Selenium.isElementVisibleInSometime(driver= self.driver,
            # xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_cancel_button'], maxWait=15)
            xpath = ZEPPELIN_XPATHS['insuff_notebook_priv_box_text']
            box_present = Selenium.isElementVisibleInSometime(driver=self.driver,
                                                              xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_box'],
                                                              maxWait=15) \
                          and Selenium.isElementVisibleInSometime(driver=self.driver, xpath=xpath, maxWait=15) \
                          #and (close_button or cancel_button)
            if box_present:
                break

        if box_present:
            is_dismiss_button_visible = Selenium.isElementVisibleInSometime(
                driver=self.driver, xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_close_button'], maxWait=30
            )
            if not is_dismiss_button_visible:
                is_dismiss_button_visible = Selenium.isElementVisibleInSometime(
                    driver=self.driver, xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_cancel_button'], maxWait=30
                )
                if is_dismiss_button_visible:
                    dismiss_button = Selenium.getElement(
                        self.driver, xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_cancel_button']
                    )
                else:
                    logger.error("Failed to dismiss insufficient privileges warning box")
                    return {'code': False, 'text': None}
            else:
                dismiss_button = Selenium.getElement(
                    self.driver, xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_close_button']
                )
            #if cancel_button:
            #    dismiss_button =Selenium.getElement(self.driver,
            # xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_cancel_button'])
            #elif close_button:
            #    dismiss_button =Selenium.getElement(self.driver,
            # xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_close_button'])
            insuff_priv_text = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_box_text']
            ).text.split("\n\n")
            Selenium.click(self.driver, dismiss_button)
            if Selenium.isElementDisappearInSometime(self.driver,
                                                     xpath=ZEPPELIN_XPATHS['insuff_notebook_priv_box'], maxWait=30) \
                  and Selenium.isElementVisibleInSometime(self.driver,
                                                          xpath=ZEPPELIN_XPATHS['navigate_back_to_homepg'],
                                                          maxWait=30):

                logger.info("parsed insufficient privileges message: %s", insuff_priv_text)
                return {'code': True, 'text': insuff_priv_text}
            else:
                logger.error("Failed to dismiss insufficient privileges warning box")
                return {'code': False, 'text': insuff_priv_text}
        else:
            logger.error("Failed to find insufficient privileges warning box")
            return {'code': False, 'text': None}

    @classmethod
    def __is_elem_dir(cls, elem):
        icon = elem.find_element_by_xpath(ZEPPELIN_XPATHS['link_icon']).get_attribute('class')
        return icon in ['fa fa-folder-open', 'fa fa-folder']

    @classmethod
    def __is_elem_notebook(cls, elem):
        href = elem.get_attribute('href')
        return href and 'notebook' in href

    def __expand_dir(self, link_text):
        elem = Selenium.getElement(self.driver, link_text=link_text)
        assert elem, "Failed to find directory element for Dir: %s" % link_text
        icon = elem.find_element_by_xpath(ZEPPELIN_XPATHS['link_icon']).get_attribute('class')
        if icon == 'fa fa-folder':
            Selenium.click(self.driver, elem)
            time.sleep(1)
        return True

    def __close_dir(self, link_text):
        elem = Selenium.getElement(self.driver, link_text=link_text)
        assert elem, "Failed to find directory element for Dir: %s" % link_text
        icon = elem.find_element_by_xpath(ZEPPELIN_XPATHS['link_icon']).get_attribute('class')
        if icon == 'fa fa-folder-open':
            Selenium.click(self.driver, elem)
            time.sleep(1)
        return True

    def __rec_traverse_dir(self, path_to_par, dir_link_text, notebook_names_so_far, level):
        level_subst = 'li'
        for _ in range(0, level - 1):
            level_subst = level_subst + "//li"
        notebook_names_this_iter = []
        assert self.__expand_dir(dir_link_text), "Failed to expand dir: %s" % dir_link_text
        pat_identifiers_this_level = ZEPPELIN_XPATHS['pattern_notebook_names'] % level_subst
        identifiers_elem_list = Selenium.getElement(self.driver, xpath=pat_identifiers_this_level)
        if identifiers_elem_list:
            identifiers_elem_list = [identifiers_elem_list
                                    ] if not isinstance(identifiers_elem_list, list) else identifiers_elem_list
            for elem in identifiers_elem_list:
                if self.__is_elem_dir(elem):
                    self.__rec_traverse_dir(
                        path_to_par + dir_link_text + "/", elem.text, notebook_names_so_far, level + 1
                    )
                elif self.__is_elem_notebook(elem):
                    notebook_names_this_iter.append(path_to_par + dir_link_text + "/" + elem.text)
        assert self.__close_dir(dir_link_text), "Failed to close dir: %s" % dir_link_text
        notebook_names_so_far.extend(notebook_names_this_iter)

    @TaskReporter.report_test()
    def is_notebook_names_present(self):
        if not self.__is_driver_on_homepage():
            self.navigate_back_to_homepage()
        return Selenium.isElementVisibleInSometime(
            driver=self.driver, xpath=ZEPPELIN_XPATHS['notebook_list'], maxWait=60
        )

    @TaskReporter.report_test()
    def get_list_of_all_notebooks(self):
        if self.is_notebook_names_present():
            notebook_names = []
            pat_identifiers_this_level = ZEPPELIN_XPATHS['pattern_notebook_names'] % 'li'
            identifiers_elem_list = Selenium.getElement(self.driver, xpath=pat_identifiers_this_level)
            if identifiers_elem_list:
                identifiers_elem_list = [identifiers_elem_list] if not isinstance(identifiers_elem_list, list) \
                                        else identifiers_elem_list
                for elem in identifiers_elem_list:
                    if self.__is_elem_dir(elem):
                        self.__rec_traverse_dir("/", elem.text, notebook_names, 2)
                    elif self.__is_elem_notebook(elem):
                        notebook_names.append(elem.text)
                notebook_names = [name for name in notebook_names if bool(name)]
                return notebook_names
            else:
                return None
        else:
            return None

    def __click_notebook_link(self, notebook_name):

        num_levels = notebook_name.count("/")
        base_name = notebook_name
        if num_levels > 1:
            dir_splits = notebook_name.split("/")
            if '' in dir_splits:
                dir_splits.remove('')
            base_name = dir_splits[-1]
            for level, name in enumerate(dir_splits[0:-1]):
                level_subst = 'li'
                for _ in range(0, level):
                    level_subst = level_subst + "//li"
                identifiers_at_this_level = ZEPPELIN_XPATHS['pattern_notebook_names'] % level_subst
                identifiers_elem_list = Selenium.getElement(self.driver, xpath=identifiers_at_this_level)
                identifiers_elem_list = [identifiers_elem_list
                                        ] if not isinstance(identifiers_elem_list, list) else identifiers_elem_list
                link = [i for i in identifiers_elem_list if i.text == name]
                if link:
                    assert self.__expand_dir(link[0].text), "Failed to expand dir: %s" % link[0].text
                    time.sleep(2)
                else:
                    logger.error("Couldn't find directory: %s", name)
                    return False
        level_subst = 'li'
        for i in range(0, num_levels - 1):
            level_subst = level_subst + "//li"
        identifiers_at_this_level = ZEPPELIN_XPATHS['pattern_notebook_names'] % level_subst
        identifiers_elem_list = Selenium.getElement(self.driver, xpath=identifiers_at_this_level)
        identifiers_elem_list = [identifiers_elem_list
                                ] if not isinstance(identifiers_elem_list, list) else identifiers_elem_list
        link = [i for i in identifiers_elem_list if i.text == base_name]
        if link:
            Selenium.click(self.driver, link[0])
            time.sleep(5)
            return True
        else:
            logger.error("Couldn't find notebook: %s", base_name)
            return False

    @TaskReporter.report_test()
    def is_notebook_present(self, notebook_name):
        visible_notebooks = self.get_list_of_all_notebooks()
        assert isinstance(visible_notebooks, list), "Failed to parse the list of all visible notebooks"
        if notebook_name in visible_notebooks:
            if visible_notebooks.count(notebook_name) > 1:
                logger.warn("Multiple notebooks with name '%s' are present", notebook_name)
                return True
            else:
                logger.info("Notebooks with name '%s' is present", notebook_name)
                return True
        else:
            logger.info("Notebooks with name '%s' is not present", notebook_name)
            return False

    @TaskReporter.report_test()
    def is_notebook_open(self, notebook_name):
        title_pattern = ZEPPELIN_XPATHS['pattern_notebook_title'] % notebook_name
        if Selenium.isElementCurrentlyDisplayed(driver=self.driver, xpath=title_pattern):
            logger.info("Notebook : %s is currently open", notebook_name)
            return True
        else:
            logger.info("Notebook : %s is not open", notebook_name)
            return False

    @TaskReporter.report_test()
    def open_notebook(self, notebook_name, check=True):
        if ZeppelinServer.is_auth_enabled():
            if not self.is_someone_logged_in():
                logger.error(
                    "Authentication is enabled and no user is logged in: Can't open the notebook %s", notebook_name
                )
        if not self.__is_driver_on_homepage():
            assert self.navigate_back_to_homepage(
            ), "Failed to load the home page while opening notebook: %s" % notebook_name

        if check:
            visible_notebooks = self.get_list_of_all_notebooks()
            assert isinstance(visible_notebooks, list), "Failed to parse the list of all visible notebooks"
            if notebook_name in visible_notebooks:
                if visible_notebooks.count(notebook_name) > 1:
                    logger.error("Failed to open notebook '%s' : Multiple notebooks with same name", notebook_name)
                    return False
            else:
                logger.error("Failed to open notebook '%s' : Notebook does not exist", notebook_name)
                return False

        assert self.__click_notebook_link(notebook_name), "Failed to click notebook link: %s" % notebook_name
        insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
        if insuff_priv_box['code'] and insuff_priv_box['text']:
            return False
        elif Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['paragraph_detect'], maxWait=60):
            logger.info("Notebook : %s is opened successfully", notebook_name)
            self._currently_open_notebook = notebook_name
            return True
        else:
            logger.info("Failed to open Notebook : %s", notebook_name)
            return False

    @staticmethod
    def get_users(actual_user, user_to_append):
        if actual_user != '':
            return actual_user + "," + user_to_append
        return user_to_append

    @TaskReporter.report_test()
    def create_new_notebook(self, notebook_name=None, owners='', readers='', writers='', runners=''):
        if not self.currently_logged_in_user in owners:
            owners = self.get_users(owners, self.currently_logged_in_user)
        if not self.currently_logged_in_user in readers:
            readers = self.get_users(readers, self.currently_logged_in_user)
        if not self.currently_logged_in_user in writers:
            writers = self.get_users(writers, self.currently_logged_in_user)
        if not self.currently_logged_in_user in runners:
            runners = self.get_users(runners, self.currently_logged_in_user)
        if 'admin' not in owners:
            owners += ",admin"
        if ZeppelinServer.is_auth_enabled():
            if not self.is_someone_logged_in():
                logger.error(
                    "Authentication is enabled and no user is logged in: Can't open the notebook %s", notebook_name
                )
        if not self.__is_driver_on_homepage():
            assert self.navigate_back_to_homepage(), "Failed to load the home page while opening notebook: %s" \
                                                     % notebook_name

        if Selenium.isElementVisibleInSometime(self.driver, maxWait=30, xpath=ZEPPELIN_XPATHS['create_note_link']):
            create_note_link = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['create_note_link'])
            assert Selenium.isElementCurrentlyDisplayed(
                element=create_note_link
            ), "Link 'Create new note' isn't visible"
            Selenium.click(self.driver, create_note_link)
            assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['create_note_box']), \
                "Aborting create notebook operation : 'Create new note' box is not visible"
            note_name_box = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['notebook_name_text_box'])
            if notebook_name:
                Selenium.sendKeys(note_name_box, notebook_name)

            create_note_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['create_note_button'])
            Selenium.click(self.driver, create_note_button)
            if Selenium.isElementVisibleInSometime(self.driver, maxWait=60, xpath=ZEPPELIN_XPATHS['paragraph_detect']):
                if notebook_name:
                    self._currently_open_notebook = notebook_name
                    self._most_recently_created_notebook = notebook_name
                    logger.info("Notebook : %s is created successfully", notebook_name)
                else:
                    self._most_recently_created_notebook = \
                        Selenium.getElement(self.driver,
                                            xpath=ZEPPELIN_XPATHS['latest_notebook_name']).text
                    self._currently_open_notebook = self._most_recently_created_notebook
                    logger.info("Notebook : %s is created successfully", self._most_recently_created_notebook)
                if not (owners == self.currently_logged_in_user and writers == self.currently_logged_in_user
                        and readers == self.currently_logged_in_user and runners == self.currently_logged_in_user):
                    if notebook_name == '':
                        notebook_name = self._most_recently_created_notebook
                    return self.set_notebook_permissions(
                        notebook_name, owners=owners, readers=readers, writers=writers, runners=runners
                    )
                else:
                    return True
            else:
                return False
        else:
            logger.error("Create notebook link is not visible")
            return False

    def get_most_recently_created_notebook(self):
        return self._most_recently_created_notebook

    def __clear_permissions_box(self, box):
        remove_elems = []
        while True:
            if box == 'writers':
                remove_elems = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['writers_box_clear'])
            elif box == 'readers':
                remove_elems = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['readers_box_clear'])
            elif box == 'owners':
                remove_elems = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['owners_box_clear'])
            elif box == 'runners':
                remove_elems = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['runners_box_clear'])
            remove_elems = [remove_elems] if not isinstance(remove_elems, list) else remove_elems
            if remove_elems:
                break
            else:
                Selenium.click(self.driver, remove_elems[0])
                time.sleep(2)

    @TaskReporter.report_test()
    def set_notebook_permissions(self, notebook_name=None, owners='', readers='', writers='', runners=''):
        if not ZeppelinServer.is_auth_enabled():
            logger.info("Setting up notebook permissions is not a valid operation in anonymous mode")
            return True
        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(notebook_name), \
                    "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        assert Selenium.isElementVisibleInSometime(self.driver,
                                                   maxWait=10, xpath=ZEPPELIN_XPATHS['toggle_permissions']), \
            "Permissions setup icon does not appear while trying to set up notebook permissions"

        toggle_perm_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['toggle_permissions'])

        Selenium.click(self.driver, toggle_perm_button)
        assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['toggle_permissions_box']), \
            "Permissions setup dialog box does not appear while trying to set up notebook permissions"

        self.__clear_permissions_box(box='writers')
        if writers != '':
            assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['writers_box']),\
                "'Writers' textbox not visible"
            writers_box = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['writers_box'])
            Selenium.sendKeys(writers_box, writers + ',')

        self.__clear_permissions_box(box='readers')
        if readers != '':
            assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['readers_box']), \
                "'Readers' textbox not visible"
            readers_box = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['readers_box'])
            Selenium.sendKeys(readers_box, readers + ',')

        self.__clear_permissions_box(box='owners')
        if owners != '':
            assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['owners_box']), \
                "'Owners' textbox not visible"
            owners_box = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['owners_box'])
            Selenium.sendKeys(owners_box, owners + ',')

        self.__clear_permissions_box(box='runners')
        if runners != '':
            assert Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['runners_box']), \
                "'Runners' textbox not visible"
            runners_box = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['runners_box'])
            Selenium.sendKeys(runners_box, runners + ',')

        assert Selenium.isElementVisibleInSometime(self.driver, maxWait=60,
                                                   xpath=ZEPPELIN_XPATHS['perm_save_button']), \
            "Permissions save button is not visible"
        perm_save_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['perm_save_button'])
        Selenium.click(self.driver, perm_save_button)

        insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
        if insuff_priv_box['code'] and insuff_priv_box['text']:
            return False
        else:
            assert Selenium.isElementVisibleInSometime(self.driver,
                                                       xpath=ZEPPELIN_XPATHS['perm_confirmation_box_ok_button'],
                                                       maxWait=60), \
                "Permissions confirmation box does not appear after clicking Save on notebook permissions"
            perm_conf_ok = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['perm_confirmation_box_ok_button'])
            perf_conf_text = Selenium.getElement(self.driver,
                                                 xpath=ZEPPELIN_XPATHS['perm_confirmation_box_body_text']).\
                text.split("\n\n")
            for line in perf_conf_text:
                field, valstr = line.replace(" ", '').split(':')
                val = sorted(valstr.split(','))
                if field == 'Owners':
                    if val != sorted(owners.replace(" ", '').split(',')):
                        logger.error(
                            "Error while setting owners permission for the notebook %s -> Expected : %s, Result : %s",
                            notebook_name, owners, valstr
                        )
                        return False
                elif field == 'Readers':
                    if val != sorted(readers.replace(" ", '').split(',')):
                        logger.info(
                            "Error while setting readers permission for the notebook %s -> Expected : %s, Result : %s",
                            notebook_name, readers, valstr
                        )
                        return False
                elif field == 'Writers':
                    if val != sorted(writers.replace(" ", '').split(',')):
                        logger.info(
                            "Error while setting writers permission for the notebook %s -> Expected : %s, Result : %s",
                            notebook_name, writers, valstr
                        )
                        return False
                elif field == 'Runners':
                    if val != sorted(runners.replace(" ", '').split(',')):
                        logger.info(
                            "Error while setting runners permission for the notebook %s -> Expected : %s, Result : %s",
                            notebook_name, runners, valstr
                        )
                        return False

            Selenium.click(self.driver, perm_conf_ok)
            assert Selenium.isElementDisappearInSometime(self.driver, xpath=ZEPPELIN_XPATHS['perm_confirmation_box']),\
                "Permissions confirmation box does not disappear while setting up notebook permissions"
            logger.info(
                "Notebook : %s permission changed successfully to Owners : %s, Readers: %s, Writers: %s, Runners: %s",
                notebook_name, owners, readers, writers, runners
            )
            return True

    @TaskReporter.report_test()
    def get_list_of_supported_interpreters(self):
        if not self.__is_driver_on_interpreterspage():
            assert self.load_zeppelin_interpreters_page(), "Failed to load zeppelin interpreters page"
        interpreter_titles = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['supported_interpreter_titles'])
        assert interpreter_titles, "Failed to find supported interpreters"
        supported_interpreters = [title.text.split(" %")[0] for title in interpreter_titles]
        return supported_interpreters

    @TaskReporter.report_test()
    def is_interpreter_selected(self, name):
        interpreter_selected_pattern = ZEPPELIN_XPATHS['pattern_individual_interpreter_selected'] % name
        if Selenium.isElementVisibleInSometime(driver=self.driver, xpath=interpreter_selected_pattern, maxWait=10):
            logger.info("Interpreter : %s is selected", name)
            return True
        else:
            logger.info("Interpreter : %s is not selected", name)
            return False

    @TaskReporter.report_test()
    def restart_interpreters_within_notebook(self, interpreter_name, notebook_name=None):
        if not isinstance(interpreter_name, list):
            interpreter_name = [interpreter_name]

        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        if not Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_section'],
                                                   maxWait=10):
            interpreter_settings_button = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_button']
            )
            Selenium.click(self.driver, interpreter_settings_button)
            if not Selenium.isElementVisibleInSometime(
                    self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_section'], maxWait=10):
                logger.info("Interpreter settings section not visible for Notebook : %s", notebook_name)
                return False

        if not self._notebook_default_interpreter_order:
            interpreter_setting_rows = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['notebook_interpreter_order']
            )
            for index, elem in enumerate(interpreter_setting_rows):
                self._notebook_default_interpreter_order[elem.text] = index

        interpreter_restart_buttons_in_notebook = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_restart_buttons_in_notebook']
        )

        for intp in interpreter_name:
            Selenium.click(
                self.driver, interpreter_restart_buttons_in_notebook[self._notebook_default_interpreter_order[intp]]
            )
            xpath = ZEPPELIN_XPATHS['pattern_interpreter_restart_dialog_box_in_notebook'] % intp
            if Selenium.isElementVisibleInSometime(self.driver, xpath=xpath, maxWait=30) and \
                    Selenium.isElementVisibleInSometime(self.driver,
                                                        xpath=ZEPPELIN_XPATHS['interpreter_restart_ok_button']):
                ok_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['interpreter_restart_ok_button'])
                Selenium.click(self.driver, ok_button)
                if not Selenium.isElementDisappearInSometime(
                        self.driver,
                        xpath=ZEPPELIN_XPATHS['pattern_interpreter_restart_dialog_box_in_notebook'] % intp,
                        maxWait=30):
                    logger.error("Failed to restart interpreter: %s from within the notebook: %s", intp, notebook_name)
                    return False
                else:
                    logger.info(
                        "Successfully restarted interpreter: %s from within the notebook: %s", intp, notebook_name
                    )
            else:
                logger.error("Failed to restart interpreter: %s from within the notebook: %s", intp, notebook_name)
                return False

        return True

    @TaskReporter.report_test()
    def change_interpreter_bindings(self, notebook_name=None, selected=None, deselected=None):
        if not selected:
            selected = []
        if not deselected:
            deselected = []
        all_interpreters = self.get_list_of_supported_interpreters()
        if selected:
            selected = [selected] if not isinstance(selected, list) else selected
            for sel in selected:
                assert sel in all_interpreters, "Interpreter: %s does not exist" % sel
        else:
            selected = all_interpreters

        if deselected:
            deselected = [deselected] if not isinstance(deselected, list) else deselected
            for desel in deselected:
                assert desel in all_interpreters, "Interpreter: %s does not exist" % desel
        else:
            deselected = [interpreter for interpreter in all_interpreters if not interpreter in selected]

        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        if not Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_section'],
                                                   maxWait=10):
            interpreter_settings_button = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_button']
            )
            Selenium.click(self.driver, interpreter_settings_button)
            if not Selenium.isElementVisibleInSometime(
                    self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_section'], maxWait=10):
                logger.info("Interpreter settings section not visible for Notebook : %s", notebook_name)
                return False

        for interpreter in deselected:
            if interpreter in all_interpreters:
                if self.is_interpreter_selected(interpreter):
                    toggle_xpath = ZEPPELIN_XPATHS['pattern_individual_interpreter'] % interpreter
                    toggle_interpreter = Selenium.getElement(self.driver, xpath=toggle_xpath)
                    Selenium.click(self.driver, toggle_interpreter)
                    if Selenium.isElementVisibleInSometime(
                            self.driver,
                            xpath=ZEPPELIN_XPATHS['pattern_individual_interpreter_deselected'] % interpreter,
                            maxWait=5):
                        logger.info("Deselected interpreter : %s", interpreter)
                    else:
                        logger.error("Failed to deselect interpreter : %s", interpreter)
                        return False
            else:
                logger.error("Interpreter: %s is not supported", interpreter)
                return False

        for interpreter in selected:
            if interpreter in all_interpreters:
                if not self.is_interpreter_selected(interpreter):
                    toggle_xpath = ZEPPELIN_XPATHS['pattern_individual_interpreter'] % interpreter
                    toggle_interpreter = Selenium.getElement(self.driver, xpath=toggle_xpath)
                    Selenium.click(self.driver, toggle_interpreter)
                    if Selenium.isElementVisibleInSometime(
                            self.driver,
                            xpath=ZEPPELIN_XPATHS['pattern_individual_interpreter_selected'] % interpreter, maxWait=5):
                        logger.info("selected interpreter : %s", interpreter)
                    else:
                        logger.error("Failed to select interpreter : %s", interpreter)
                        return False
            else:
                logger.error("Interpreter: %s is not supported", interpreter)
                return False

        interpreter_setting_save_button = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_save_button']
        )
        Selenium.click(self.driver, interpreter_setting_save_button)
        assert Selenium.waitTillElementDisappears(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_section'], maxWait=10
        ), "Failed to save interpreter settings"
        return True

    @TaskReporter.report_test()
    def close_current_notebook(self):
        result = self.navigate_back_to_homepage()
        if result:
            logger.info("Closed notebook : %s", self._currently_open_notebook)
            self._currently_open_notebook = None
        else:
            logger.error("Failed to close notebook : %s", self._currently_open_notebook)
        return result

    @TaskReporter.report_test()
    def delete_notebook(self, notebook_name):
        if not self.is_notebook_open(notebook_name):
            if not self.__is_driver_on_homepage():
                assert self.navigate_back_to_homepage(
                ), "Failed to navigate back to the homepage while deleting notebook"
            assert self.open_notebook(notebook_name), "Failed to open Notebook : %s for deletion" % notebook_name
        delete_note_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['remove_note_button'])
        Selenium.click(self.driver, delete_note_button)

        assert (Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['remove_note_dialog_box_msg'],
                                                    maxWait=30)
                or Selenium.isElementVisibleInSometime(self.driver, xpath=ZEPPELIN_XPATHS['remove_note_dialog_box_ok'],
                                                       maxWait=30)), \
            "Failed to delete Notebook : %s" % notebook_name
        ok_button = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['remove_note_dialog_box_ok'])
        Selenium.click(self.driver, ok_button)
        assert Selenium.isElementDisappearInSometime(self.driver, xpath=ZEPPELIN_XPATHS['remove_note_dialog_box_msg'],
                                                     maxWait=10), \
            "Failed to delete Notebook : %s" % notebook_name
        insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
        if insuff_priv_box['code'] and insuff_priv_box['text']:
            logger.error("Failed to delete Notebook : %s, not sufficient permissions", notebook_name)
            return False
        else:
            self._currently_open_notebook = None
            logger.info("Notebook : %s is deleted successfully", notebook_name)
            return True

    def __get_all_paragraphs_fm_curr_notebook(self):
        paragraphs = Selenium.getElement(self.driver, xpath=ZEPPELIN_XPATHS['paragraph_detect'])
        if paragraphs:
            paragraphs = paragraphs if isinstance(paragraphs, list) else [paragraphs]
        return paragraphs

    def __get_all_paragraphs_with_code_to_run(self):
        paragraphs = self.__get_all_paragraphs_fm_curr_notebook()
        paragraphs_with_code = []
        for paragraph in paragraphs:
            line_groups = paragraph.find_elements_by_xpath(ZEPPELIN_XPATHS['paragraph_lines'])
            if len(line_groups) > 2:
                paragraphs_with_code.append(paragraph)
            elif len(line_groups) == 2:
                if line_groups[1].find_elements_by_xpath(ZEPPELIN_XPATHS['paragraph_line_content']):
                    paragraphs_with_code.append(paragraph)
        return paragraphs_with_code

    def __get_paragraph_elem_by_index_fm_curr_notebook(self, index=-1, xpath=ZEPPELIN_XPATHS['paragraph_detect']):
        # Uses 0-based index system (both positive and negative indexing)
        # i.e. first para is index 0, 2nd para is index 1 and so on
        # last paragraph is index -1, second last is -2 and so on

        paragraphs = self.__get_all_paragraphs_fm_curr_notebook()
        assert paragraphs, "Aborting get_paragraph_elem_by_index_fm_curr_notebook :No paragraphs returned"

        if index > (len(paragraphs) - 1) or index < (-1 * len(paragraphs)):
            logger.error("Aborting get_paragraph_elem_by_index_fm_curr_notebook : Index out of bound")
            return None
        else:
            #xpaths use 1-based indexing
            pos_index = len(paragraphs) + index + 1 if index < 0 else index + 1
            elem_pattern = xpath % pos_index
            return (pos_index, Selenium.getElement(self.driver, xpath=elem_pattern))

    def __get_editor_id_by_index_fm_curr_notebook(self, index=-1):
        ind, editor = self.__get_paragraph_elem_by_index_fm_curr_notebook(
            index=index, xpath=ZEPPELIN_XPATHS["pattern_paragraph_editor"]
        )
        if editor:
            return (ind, editor.get_attribute("id"))
        else:
            return None

    def __get_paragraph_status_by_index_fm_curr_notebook(self, index=-1):
        _, status = self.__get_paragraph_elem_by_index_fm_curr_notebook(
            index=index, xpath=ZEPPELIN_XPATHS["pattern_paragraph_status"]
        )
        if status:
            return status.text
        else:
            return None

    @TaskReporter.report_test()
    def insert_new_paragraph_down(self, index=-1, notebook_name=None):
        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name, check=False
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        old_num_para = len(self.__get_all_paragraphs_fm_curr_notebook())
        new_index, settings_button = self.__get_paragraph_elem_by_index_fm_curr_notebook(
            index=index, xpath=ZEPPELIN_XPATHS['pattern_paragraph_settings']
        )
        if Selenium.isElementCurrentlyDisplayed(element=settings_button):
            Selenium.click(self.driver, settings_button)
            insert_button_xpath = ZEPPELIN_XPATHS['pattern_paragraph_insert_down'] % new_index
            if Selenium.isElementVisibleInSometime(self.driver, xpath=insert_button_xpath):
                insert_button = Selenium.getElement(self.driver, xpath=insert_button_xpath)
                Selenium.click(self.driver, insert_button)
                insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
                if insuff_priv_box['code']:
                    logger.info(
                        "Insufficient privillege box appeard and dismissed while inserting paragraph in notebook : %s",
                        notebook_name
                    )
                    return False
                else:
                    assert Selenium.isElementDisappearInSometime(
                        self.driver, xpath=insert_button_xpath
                    ), "Failed to collapse settings menu in paragrapg: %s" % new_index
            else:
                return False
            new_num_para = len(self.__get_all_paragraphs_fm_curr_notebook())
            assert new_num_para == old_num_para + 1, "Failed to insert a new paragraph"
            return True
        else:
            return False

    @TaskReporter.report_test()
    def insert_text_in_paragraph(self, text, index=-1, notebook_name=None):
        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name, check=False
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        ind, editorId = self.__get_editor_id_by_index_fm_curr_notebook(index=index)
        if editorId:
            lines = [text] if not isinstance(text, list) else text
            lines = "\\n".join(lines)
            num_attempts = 0
            max_attempts = 3
            result = None

            status_before_writing = self.__get_paragraph_status_by_index_fm_curr_notebook(index=index)
            assert status_before_writing in [
                'READY', 'ERROR', 'FINISHED'
            ], "status_before_writing for paragraph: %s in notebook: %s must be within [READY,FINISHED,ERROR]" % (
                ind, notebook_name
            )
            logger.info(
                "status for paragraph: %s in notebook: %s before writing: %s", ind, notebook_name,
                status_before_writing
            )

            while num_attempts < max_attempts:
                try:
                    self.driver.execute_script("ace.edit(\"" + editorId + "\").setValue(\"" + lines + "\")")
                    insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
                    if insuff_priv_box['code'] and insuff_priv_box['text']:
                        logger.info(
                            "Insufficient privillege box appeard and dismissed while inserting text in paragraph : %s "
                            "in notebook : %s",
                            index, notebook_name
                        )
                        result = {'status': False, 'text': insuff_priv_box['text']}
                        break
                    else:
                        result = {'status': True, 'text': None}
                        logger.info("Successfully edited paragraph: %s in notebook:%s", ind, notebook_name)
                        break
                except Exception, e:
                    if num_attempts < 2:
                        pass
                    else:
                        logger.error(
                            "Failed to edit paragraph : %s in notebook : %s due to : %s", ind, notebook_name, e
                        )
                        result = {'status': False, 'text': None}
                finally:
                    num_attempts = num_attempts + 1
            return result
        else:
            return {'status': False, 'text': None}
        return None

    @TaskReporter.report_test()
    def run_paragraph_by_index(
            self, notebook_name=None, index=-1, timeout=300, app_user_getter=None, timeout_exc=None
    ):
        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name, check=False
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        ind, run = self.__get_paragraph_elem_by_index_fm_curr_notebook(
            index=index, xpath=ZEPPELIN_XPATHS["pattern_paragraph_run"]
        )
        assert run, "Failed to find Run button for paragraph : %s from notebook : %s" % (ind, notebook_name)

        status_before_running = self.__get_paragraph_status_by_index_fm_curr_notebook(index=index)
        assert status_before_running in [
            'READY', 'ERROR', 'FINISHED'
        ], "status_before_running for paragraph: %s in notebook: %s must be within [READY,FINISHED,ERROR]" % (
            ind, notebook_name
        )
        logger.info(
            "status for paragraph: %s in notebook: %s before running: %s", ind, notebook_name, status_before_running
        )

        ready_paragraph_status = ZEPPELIN_XPATHS['pattern_paragraph_status_check'] % (ind, 'READY')
        running_paragraph_status = ZEPPELIN_XPATHS['pattern_paragraph_status_check'] % (ind, 'RUNNING')
        finished_paragraph_status = ZEPPELIN_XPATHS['pattern_paragraph_status_check'] % (ind, 'FINISHED')
        error_paragraph_status = ZEPPELIN_XPATHS['pattern_paragraph_status_check'] % (ind, 'ERROR')
        time.sleep(3 * random.random())
        Selenium.click(self.driver, run)
        if app_user_getter:
            app_user_getter.start()

        if status_before_running == 'READY':
            status_refreshed = Selenium.waitTillElementDisappears(
                self.driver, xpath=ready_paragraph_status, maxWait=30
            )
        elif status_before_running == 'FINISHED':
            status_refreshed = Selenium.waitTillElementDisappears(
                self.driver, xpath=finished_paragraph_status, maxWait=30
            )
        elif status_before_running == 'ERROR':
            status_refreshed = Selenium.waitTillElementDisappears(
                self.driver, xpath=error_paragraph_status, maxWait=30
            )

        if not status_refreshed:
            # check if run paragraph resulted in permissions issue
            insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
            if insuff_priv_box['code'] and insuff_priv_box['text']:
                logger.error(
                    "Paragraph : %s from notebook %s failed to run due to insufficient privilleges", ind, notebook_name
                )
                if app_user_getter:
                    app_user_getter.join()
                return False
            else:
                assert status_refreshed, "paragraph: %s failed to go out of %s status upon clicking RUN button" % (
                    index + 1, status_before_running
                )

    # the wait here is 120 sec to jump over PENDING status
        start = time.time()
        while (time.time() - start) < 120:
            running = Selenium.waitTillElementBecomesVisible(self.driver, xpath=running_paragraph_status, maxWait=3)
            if not running:
                finished = Selenium.waitTillElementBecomesVisible(
                    self.driver, xpath=finished_paragraph_status, maxWait=3
                )
                if not finished:
                    error = Selenium.waitTillElementBecomesVisible(
                        self.driver, xpath=error_paragraph_status, maxWait=3
                    )
                    if error:
                        break
                else:
                    break
            else:
                break

        if running:
            logger.info("Paragraph : %s from notebook : %s is running currently", ind, notebook_name)
            if Selenium.isElementDisappearInSometime(self.driver, xpath=running_paragraph_status, maxWait=timeout):
                if self.__get_paragraph_status_by_index_fm_curr_notebook(index=-2) == 'FINISHED':
                    logger.info("Finished running paragraph : %s from notebook : %s", ind, notebook_name)
                    if app_user_getter:
                        app_user_getter.join()
                    return True
                else:
                    logger.info("paragraph : %s from notebook : %s resulted in Error", ind, notebook_name)
                    if app_user_getter:
                        app_user_getter.join()
                    return False
            else:
                if timeout_exc:
                    raise timeout_exc(
                        "Paragraph : %s from notebook %s didn't finish running within %s sec" %
                        (ind, notebook_name, timeout)
                    )
                else:
                    logger.error(
                        "Paragraph : %s from notebook %s didn't finish running within %s sec", ind, notebook_name,
                        timeout
                    )
                    return False
        else:
            #insuff_priv_box = self.__check_and_dismiss_insuff_priv_box()
            #if insuff_priv_box['code'] and insuff_priv_box['text']:
            #    logger.error("Paragraph : %s from notebook %s failed to run
            # due to insufficient privilleges" % (ind,notebook_name))
            #    if app_user_getter:
            #        app_user_getter.join()
            #    return False
            #else:
            curr_status = self.__get_paragraph_status_by_index_fm_curr_notebook(index=-2)
            if curr_status != "FINISHED":
                logger.error("Status of paragraph: %s from notebook: %s is %s", ind, notebook_name, curr_status)
                if app_user_getter:
                    app_user_getter.join()
                return False
            else:
                logger.info("Finished running paragraph : %s from notebook : %s", ind, notebook_name)
                if app_user_getter:
                    app_user_getter.join()
                return True

    def __notebook_helper(
            self,
            content=None,
            notebook_name=None,
            write=True,
            run=True,
            timeout=300,
            return_users=False,
            timeout_exc=None
    ):
        try:
            if write:
                assert content, "Content to be written is not present"
            if return_users:
                users = []
                app_user_getter = AppUserGetter(1, "app_user_getter", 180, users)
            else:
                users = None
                app_user_getter = None
            if write:
                for index, lines in content.iteritems():
                    if write:
                        assert self.insert_text_in_paragraph(lines, index=index,
                                                             notebook_name=notebook_name)['status'],\
                            "Failed to insert text at paragraph: %s" % (index+1)
                        if not run:
                            assert self.insert_new_paragraph_down(index=index), "Failed to insert new paragraph"
                    if run:
                        assert self.run_paragraph_by_index(notebook_name=notebook_name, index=index, timeout=timeout,
                                                           app_user_getter=app_user_getter,
                                                           timeout_exc=timeout_exc), \
                            "Failed to run paragraph: %s" % (index+1)
            else:
                if run:
                    paragraphs_with_code = self.__get_all_paragraphs_with_code_to_run()
                    for index in range(0, len(paragraphs_with_code)):
                        assert self.run_paragraph_by_index(notebook_name=notebook_name, index=index, timeout=timeout,
                                                           app_user_getter=app_user_getter, timeout_exc=timeout_exc), \
                            "Failed to run paragraph: %s" % (index+1)
        except Exception, e:
            logger.error("%s", e)
            return (False, users)
        else:
            return (True, users)

    @TaskReporter.report_test()
    def write_and_run_notebook(
            self, notebook_name=None, content=None, timeout=300, return_users=False, timeout_exc=None
    ):
        para_timeout = timeout / 2  #/len(content.keys())
        return self.__notebook_helper(
            content=content,
            notebook_name=notebook_name,
            write=True,
            run=True,
            timeout=para_timeout,
            return_users=return_users,
            timeout_exc=timeout_exc
        )

    @TaskReporter.report_test()
    def run_existing_notebook_sequentially(self, notebook_name, timeout=300, return_users=False, timeout_exc=None):
        if not self.is_notebook_open(notebook_name):
            assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
            assert self.open_notebook(
                notebook_name, check=False
            ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name
        para_timeout = timeout / 2  #/(len(self.__get_all_paragraphs_fm_curr_notebook())-1)
        return self.__notebook_helper(
            notebook_name=notebook_name,
            write=False,
            run=True,
            timeout=para_timeout,
            return_users=return_users,
            timeout_exc=timeout_exc
        )

    @TaskReporter.report_test()
    def write_notebook(self, content, notebook_name=None, timeout=300, timeout_exc=None):
        return self.__notebook_helper(
            content=content,
            notebook_name=notebook_name,
            write=True,
            run=False,
            timeout=timeout,
            timeout_exc=timeout_exc
        )

    @TaskReporter.report_test()
    def change_visualization_type(self, vis, index=-1, notebook_name=None):
        if vis not in ZeppelinUIClientSession.__supported_visualizations:
            logger.error("vis: %s is not supported", vis)
            return False

        if not notebook_name:
            notebook_name = self._currently_open_notebook
            assert notebook_name, "notebook name is not provided and no notebook is open"
        else:
            if not self.is_notebook_open(notebook_name):
                assert self.navigate_back_to_homepage(), "Failed to navigate back to zeppelin home page"
                assert self.open_notebook(
                    notebook_name
                ), "Failed to open Notebook : %s for setting interpreter bindings" % notebook_name

        change_button_xpath = ZEPPELIN_XPATHS['pattern_change_visualization'] % ("%s", vis)
        ind, change_button = self.__get_paragraph_elem_by_index_fm_curr_notebook(
            index=index, xpath=change_button_xpath
        )
        vis_xpath = "(//div[@ng-controller='ParagraphCtrl'])[%s]//div[contains(@src,'paragraph-graph.html')]//" \
                    "div[@ng-if=\"getGraphMode()=='%s'\"]" % (ind, vis)

        if Selenium.isElementVisibleInSometime(self.driver, xpath=vis_xpath):
            logger.info(
                "Visualization for paragraph : %s in notebook : %s is already set to type: %s", ind, notebook_name, vis
            )
            return True

        if change_button:
            Selenium.click(self.driver, change_button)
            if Selenium.isElementVisibleInSometime(self.driver, xpath=vis_xpath):
                logger.info(
                    "Visualization for paragraph : %s in notebook : %s successfully changed to type: %s", ind,
                    notebook_name, vis
                )
                return True
            else:
                logger.error(
                    "Failed to change visualization for paragraph: %s in notebook : %s to type: %s", ind,
                    notebook_name, vis
                )
                return False
        else:
            logger.error(
                "Button to change visualization to type : %s is not present in notebook : %s paragraph : %s", vis,
                notebook_name, ind
            )
            return False

    def __get_interpreter_property_index(self, interpreter, interpreter_property):
        prop_name_elems = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_properties_name'] % interpreter
        )
        if prop_name_elems:
            prop_name_elems = prop_name_elems if isinstance(prop_name_elems, list) else [prop_name_elems]
            prop_names = [elem.text for elem in prop_name_elems]
            if not interpreter_property in prop_names:
                logger.error("Property: %s doesnt exist for interpreter: %s", interpreter_property, interpreter)
                return None
            else:
                return prop_names.index(interpreter_property) + 2
        else:
            return None

    def __get_last_property_index_for_interpreter(self, interpreter):
        return self.__get_interpreter_property_index(interpreter, '')

    def __add_properties_for_interpreter(self, interpreter, add_properties=None):
        if not add_properties:
            add_properties = {}
        for key, val in add_properties.iteritems():
            last_index = self.__get_last_property_index_for_interpreter(interpreter)
            name_tab = Selenium.getElement(
                self.driver,
                xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_property_name'] % (interpreter, last_index)
            )
            Selenium.sendKeys(name_tab, key)
            val_tab = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_property_val'] % (interpreter, last_index)
            )
            Selenium.sendKeys(val_tab, val)
            add_button = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_property_clk'] % (interpreter, last_index)
            )
            Selenium.click(self.driver, add_button)
            time.sleep(10)
        return True

    def __remove_properties_for_interpreter(self, interpreter, remove_properties=None):
        if not remove_properties:
            remove_properties = []
        for key in remove_properties:
            prop_index = self.__get_interpreter_property_index(interpreter, key)
            rem_button = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_rem_property_clk'] % (interpreter, prop_index)
            )
            Selenium.click(self.driver, rem_button)
            time.sleep(10)
        return True

    def __modify_properties_for_interpreter(self, interpreter, modify_properties=None):
        if not modify_properties:
            modify_properties = {}
        for key, val in modify_properties.iteritems():
            prop_index = self.__get_interpreter_property_index(interpreter, key)
            val_tab = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_property_val'] % (interpreter, prop_index)
            )
            val_tab.clear()
            Selenium.sendKeys(val_tab, val)
            time.sleep(10)
        return True

    def __add_dependencies_for_interpreter(self, interpreter, order=None, add_dependencies=None):
        if not order:
            order = []
        if not add_dependencies:
            add_dependencies = []
        for dep in add_dependencies:
            last_index = len(order) + 2
            dep_tab = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_dependency'] % (interpreter, last_index)
            )
            dep_tab.clear()
            Selenium.sendKeys(dep_tab, dep['name'])
            if 'exclusions' in dep.keys() and dep['exclusions']:
                ex_tab = Selenium.getElement(
                    self.driver,
                    xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_dep_exclusion'] % (interpreter, last_index)
                )
                ex_tab.clear()
                Selenium.sendKeys(ex_tab, ','.join(dep['exclusions']))
            add_button = Selenium.getElement(
                self.driver,
                xpath=ZEPPELIN_XPATHS['pattern_interpreter_add_dependency_clk'] % (interpreter, last_index)
            )
            Selenium.click(self.driver, add_button)
            time.sleep(10)
            order.append(dep)
        return True

    def __remove_dependencies_for_interpreter(self, interpreter, order=None, remove_dependencies=None):
        if not order:
            order = []
        if not remove_dependencies:
            remove_dependencies = []
        for key in remove_dependencies:
            if key in order:
                dep_index = order.index(key) + 2
                rem_button = Selenium.getElement(
                    self.driver,
                    xpath=ZEPPELIN_XPATHS['pattern_interpreter_rem_dependency_clk'] % (interpreter, dep_index)
                )
                Selenium.click(self.driver, rem_button)
                time.sleep(10)
                del order[dep_index - 2]
        return True

    def __modify_dependencies_for_interpreter(self, interpreter, order=None, modify_dependencies=None):
        if not order:
            order = []
        if not modify_dependencies:
            modify_dependencies = {}
        for key, val in modify_dependencies.iteritems():
            if key in order:
                dep_index = order.index(key) + 2
                dep_tab = Selenium.getElement(
                    self.driver,
                    xpath=ZEPPELIN_XPATHS['pattern_interpreter_modify_dependency'] % (interpreter, dep_index)
                )
                dep_tab.clear()
                Selenium.sendKeys(dep_tab, val['name'])
                ex_tab = Selenium.getElement(
                    self.driver,
                    xpath=ZEPPELIN_XPATHS['pattern_interpreter_modify_dep_exclusions'] % (interpreter, dep_index)
                )
                ex_tab.clear()
                if 'exclusions' in val.keys() and val['exclusions']:
                    Selenium.sendKeys(ex_tab, ','.join(val['exclusions']))
                time.sleep(10)
                order[dep_index - 2] = val['name']
        return True

    def __edit_interpreter_settings(
            self,
            interpreter,
            add_properties=None,
            remove_properties=None,
            modify_properties=None,
            add_dependencies=None,
            remove_dependencies=None,
            modify_dependencies=None
    ):
        if not add_properties:
            add_properties = {}
        if not remove_properties:
            remove_properties = []
        if not modify_properties:
            modify_properties = {}
        if not add_dependencies:
            add_dependencies = []
        if not remove_dependencies:
            remove_dependencies = []
        if not modify_dependencies:
            modify_dependencies = {}
        self.load_zeppelin_interpreters_page()
        dep_names = []
        dep_name_elems = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_dependencies_name'] % interpreter
        )
        if dep_name_elems:
            dep_name_elems = dep_name_elems if isinstance(dep_name_elems, list) else [dep_name_elems]
            dep_names = [elem.text for elem in dep_name_elems]

        interpreter_edit_button_xpath = ZEPPELIN_XPATHS['pattern_interpreter_edit_button'] % interpreter
        assert Selenium.isElementVisibleInSometime(self.driver, xpath=interpreter_edit_button_xpath, maxWait=30), \
            "Interpreter: %s edit button not visible" % interpreter
        interpreter_edit_button = Selenium.getElement(self.driver, xpath=interpreter_edit_button_xpath)
        Selenium.click(self.driver, interpreter_edit_button)
        time.sleep(15)

        if add_properties:
            assert self.__add_properties_for_interpreter(
                interpreter, add_properties=add_properties
            ), "Failed to add properties: %s in '%s' interpreter" % (add_properties, interpreter)
        if remove_properties:
            assert self.__remove_properties_for_interpreter(
                interpreter, remove_properties=remove_properties
            ), "Failed to remove properties: %s in '%s' interpreter" % (remove_properties, interpreter)
        if modify_properties:
            assert self.__modify_properties_for_interpreter(
                interpreter, modify_properties=modify_properties
            ), "Failed to modify properties: %s in '%s' interpreter" % (modify_properties, interpreter)

        if add_dependencies:
            assert self.__add_dependencies_for_interpreter(
                interpreter, order=dep_names, add_dependencies=add_dependencies
            ), "Failed to add dependencies: %s in '%s' interpreter" % (add_dependencies, interpreter)
        if remove_dependencies:
            assert self.__remove_dependencies_for_interpreter(
                interpreter, order=dep_names, remove_dependencies=remove_dependencies
            ), "Failed to remove dependencies: %s in '%s' interpreter" % (remove_dependencies, interpreter)
        if modify_dependencies:
            assert self.__modify_dependencies_for_interpreter(
                interpreter, order=dep_names, modify_dependencies=modify_dependencies
            ), "Failed to modify dependencies: %s in '%s' interpreter" % (modify_dependencies, interpreter)

        interpreter_setting_save_button_xpath = ZEPPELIN_XPATHS['pattern_interpreter_setting_save_button'] % \
                                                interpreter
        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=interpreter_setting_save_button_xpath, maxWait=30
        ), "Interpreter: %s save button not visible" % interpreter
        interpreter_setting_save_button = Selenium.getElement(self.driver, xpath=interpreter_setting_save_button_xpath)
        Selenium.click(self.driver, interpreter_setting_save_button)
        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_save_dialog_box']
        ), "Interpreter setting save confirmation box is not available"
        assert Selenium.isElementVisibleInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_save_ok_button']
        ), "Interpreter setting ok button is not visible"
        interpreter_setting_save_ok_button = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_save_ok_button']
        )
        Selenium.click(self.driver, interpreter_setting_save_ok_button)
        assert Selenium.isElementDisappearInSometime(
            self.driver, xpath=ZEPPELIN_XPATHS['interpreter_setting_save_dialog_box'], maxWait=120
        ), "Interpreter setting save confirmation box did not disappear"

        return True

    @TaskReporter.report_test()
    def edit_interpreters(self, change_map=None):
        if not change_map:
            change_map = {}
        for interpreter in change_map.keys():
            property_map = change_map[interpreter]['properties'] if 'properties' in change_map[interpreter].keys() \
                else None
            dep_map = change_map[interpreter]['dependencies'] if 'dependencies' in change_map[interpreter].keys() \
                else None
            if property_map:
                add_properties = property_map['add'] if 'add' in property_map.keys() else {}
                remove_properties = property_map['remove'] if 'remove' in property_map.keys() else []
                modify_properties = property_map['modify'] if 'modify' in property_map.keys() else {}
            else:
                add_properties = {}
                remove_properties = []
                modify_properties = {}

            if dep_map:
                add_dependencies = dep_map['add'] if 'add' in dep_map.keys() else []
                remove_dependencies = dep_map['remove'] if 'remove' in dep_map.keys() else []
                modify_dependencies = dep_map['modify'] if 'modify' in dep_map.keys() else {}
            else:
                add_dependencies = []
                remove_dependencies = []
                modify_dependencies = {}

            assert self.__edit_interpreter_settings(
                interpreter, add_properties, remove_properties, modify_properties, add_dependencies,
                remove_dependencies, modify_dependencies
            ), "Failed to edit interpreter '%s'" % interpreter
        return True

    @TaskReporter.report_test()
    def get_interpreter_property(self, interpreter, interpreter_property):
        if not self.__is_driver_on_interpreterspage():
            assert self.load_zeppelin_interpreters_page(), "Failed to load zeppelin interpreters page"
        prop_index = self.__get_interpreter_property_index(interpreter, interpreter_property)
        val = Selenium.getElement(
            self.driver, xpath=ZEPPELIN_XPATHS['pattern_interpreter_read_property_val'] % (interpreter, prop_index)
        ).text
        return val

    @TaskReporter.report_test()
    def restart_interpreters(self, interpreter_names):
        if not self.__is_driver_on_interpreterspage():
            self.load_zeppelin_interpreters_page()
        interpreter_names = [interpreter_names] if not isinstance(interpreter_names, list) else interpreter_names
        for interpreter_name in interpreter_names:
            interpreter_restart_button_xpath = ZEPPELIN_XPATHS['pattern_interpreter_restart_button'] % interpreter_name
            assert Selenium.isElementVisibleInSometime(
                self.driver, xpath=interpreter_restart_button_xpath, maxWait=30
            ), "Interpreter: %s restart button not visible" % interpreter_name
            interpreter_restart_button = Selenium.getElement(self.driver, xpath=interpreter_restart_button_xpath)
            Selenium.click(self.driver, interpreter_restart_button)
            assert Selenium.isElementVisibleInSometime(
                self.driver, xpath=ZEPPELIN_XPATHS['interpreter_restart_confirmation_box_ok_button'], maxWait=90
            ), "Interpreter: %s restart confirmation box not visible" % interpreter_name
            conf_box = Selenium.getElement(
                self.driver, xpath=ZEPPELIN_XPATHS['interpreter_restart_confirmation_box_ok_button']
            )
            Selenium.click(self.driver, conf_box)
            assert Selenium.isElementDisappearInSometime(
                self.driver, xpath=ZEPPELIN_XPATHS['interpreter_restart_confirmation_box_ok_button'], maxWait=120
            ), "Interpreter: %s restart confirmation box does not disappear" % interpreter_name
            time.sleep(60)
        return True

    @TaskReporter.report_test()
    def click_edit_and_restart(self, interpreter_names):
        interpreter_names = [interpreter_names] if not isinstance(interpreter_names, list) else interpreter_names
        for interpreter_name in interpreter_names:
            assert self.edit_interpreters(change_map={interpreter_name:{}}), \
                "Failed to click edit and restart interpreter: %s" % interpreter_name
        return True


class CreateWriteAndRunNotebookSession(threading.Thread):
    def __init__(
            self,
            thread_id,
            result_queue,
            content,
            thread_name,
            user_name=None,
            password=None,
            exp_results=None,
            ui=False,
            session=None,
            timeout=2000,
            timeout_exc=None,
            knox=False,
            is_sso=False
    ):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.thread_name = thread_name
        self.notebook_name = thread_name
        self.content = content
        self.user_name = user_name
        self.password = password
        if session:
            if ui:
                assert session.get_type() == 'ui', "Expecting UI session, got REST session"
            else:
                assert session.get_type() == 'rest', "Expecting REST session, got UI session"
            self.session = session
        else:
            if ui:
                self.session = ZeppelinUIClientSession(knox=knox, is_sso=is_sso)
            else:
                self.session = ZeppelinRestClientSession()
        self.ui = ui
        self.result_queue = result_queue
        self.timeout = timeout
        self.timeout_exc = timeout_exc
        self.exp_results = exp_results

    def thread_name_decorator(self, msg):
        return "thread_name : %s | %s" % (self.thread_name, msg)

    def run(self):
        try:
            logger.info(self.thread_name_decorator("Started running thread: %s" % self.thread_name))
            if self.ui:
                assert self.session.load_zeppelin_homepage(), "Failed to load zeppelin homepage"
            if ZeppelinServer.is_auth_enabled():
                if not self.session.is_someone_logged_in():
                    assert self.user_name and self.password, \
                        self.thread_name_decorator("User name and password must be provided")
                    assert self.session.login(self.user_name, self.password), \
                        self.thread_name_decorator("Failed to login")
                    logger.info(self.thread_name_decorator("Successfully logged in as user: %s" % self.user_name))
            if self.ui:
                assert self.session.create_new_notebook(notebook_name=self.notebook_name), \
                    self.thread_name_decorator("Failed to create the notebook: %s" % self.notebook_name)
            assert self.session.write_and_run_notebook(notebook_name=self.notebook_name, content=self.content,
                                                       timeout=self.timeout, timeout_exc=self.timeout_exc)[0], \
                self.thread_name_decorator("Failed to run zeppelin notebook : %s" % self.notebook_name)
            logger.info(self.thread_name_decorator("Successfully finished running notebook: %s" % self.notebook_name))

            if self.ui:
                parse_result_session = ZeppelinRestClientSession()
                if ZeppelinServer.is_auth_enabled():
                    assert self.user_name and self.password, \
                        self.thread_name_decorator("User name and password must be provided")
                    assert parse_result_session.login(self.user_name, self.password), \
                        self.thread_name_decorator("Failed to login")
                act_results = parse_result_session.parse_all_results(self.notebook_name)
                assert parse_result_session.logout(), \
                    self.thread_name_decorator("Failed to logout user: %s from parse_result_session" % self.user_name)
            else:
                act_results = self.session.parse_all_results(self.notebook_name)
            logger.info("act_results: %s", act_results)

            if self.exp_results:
                logger.info("exp_results: %s", self.exp_results)
                try:
                    ZeppelinClient.validate_notebook_output(act_results, self.exp_results)
                except Exception, e:
                    logger.error("%s", e)
                    self.result_queue[self.thread_name] = False
                else:
                    logger.info(self.thread_name_decorator('Notebook output matches expected output'))
                    self.result_queue[self.thread_name] = True
            else:
                logger.info(self.thread_name_decorator('No exp_output found and notebook finished successfully'))
                self.result_queue[self.thread_name] = True
        except Exception, e:
            logger.error("Thread: %s with Id: %s resulted in failure : %s", self.thread_name, self.thread_id, e)
            self.result_queue[self.thread_name] = False
        finally:
            self.session.export_notebook_into_file(self.notebook_name)
            if ZeppelinServer.is_auth_enabled() and self.session.is_someone_logged_in():
                assert self.session.logout(), self.thread_name_decorator("Failed to logout: %s user" % self.user_name)
            self.session.delete_session()
