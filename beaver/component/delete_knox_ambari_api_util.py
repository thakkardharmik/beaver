#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

from beaver.config import Config
import json, logging
import pycurl
from StringIO import StringIO as BytesIO
from beaver.machine import Machine

logger = logging.getLogger(__name__)
logger.info("------------- Debug ------------------")


class AmbariAPIUtil:
    def __init__(self, host='localhost', port=8080, isHttps=False):
        logger.info("****Method called***")
        if isHttps:
            self.baseUrl = 'https://' + host + ':' + str(port)
        else:
            self.baseUrl = 'http://' + host + ':' + str(port)
        if (Machine.isHumboldt()):
            self.username_password = Config.get('ambari', 'AMBARI_USERNAME', 'admin') + ':HdpCli123!'
        else:
            self.username_password = Config.get('ambari', 'AMBARI_USERNAME', 'admin'
                                                ) + ':' + Config.get('ambari', 'AMBARI_PASSWORD', 'admin')
        self.urlLogin = self.baseUrl + '#/login'
        self.urlGetClusters = self.baseUrl + '/api/v1/clusters'
        self.urlConfig = '/configurations'
        self.urlGetAmbClusters = self.baseUrl + '/api/v1/services'

    # get the clustername url using Ambari Rest call
    # Sample url returned will be like http://<host>:<port>/api/v1/services, here cl1 is the clustername
    def getAmbariClusterNameUrl(self):
        hres, res = self.callPyCurlRequest(
            url=self.urlGetAmbClusters, data=None, method='get', usernamepassword=self.username_password
        )
        d = json.loads(res)
        return str(d['items'][0]['href'])

    # get the clustername url using Ambari Rest call
    # Sample url returned will be like http://<host>:<port>/api/v1/clusters/cl1, here cl1 is the clustername
    def getClusterNameUrl(self):
        hres, res = self.callPyCurlRequest(
            url=self.urlGetClusters, data=None, method='get', usernamepassword=self.username_password
        )
        d = json.loads(res)
        return str(d['items'][0]['href'])

    # give input as the configuration type and propertyname, return the value of the property. Returns none if property not found.
    # Sample - getPropertyValue('ranger-admin-site','ranger.audit.solr.urls') returns the solr urls configured in ranger-admin-site
    def getPropertyValue(self, type, property):
        clustName = self.getClusterNameUrl()
        latestVer = self.getLatestVersionTag(url=clustName + self.urlConfig + '?type=' + type)
        actualUrl = clustName + self.urlConfig + '?type=' + type + '&tag=' + str(latestVer)
        hres, resVal = self.callPyCurlRequest(
            url=actualUrl, data=None, method='get', usernamepassword=self.username_password
        )
        d = json.loads(resVal)
        for val in d['items']:
            if isinstance(val, list) or isinstance(val, dict):
                for vl in val['properties']:
                    if vl == property:
                        return val['properties'][vl]
        return None

    # A configuration might have got updated multiple times, so ambari maintains versions of it.
    # this method returns the 'tag' value for the latest version of the set of properties, which can used to build the full url to acces config data
    def getLatestVersionTag(self, url):
        hres, resVer = self.callPyCurlRequest(
            url=url, data=None, method='get', usernamepassword=self.username_password
        )
        d = json.loads(resVer)
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
        clustName = self.getClusterNameUrl()
        hres, res = self.callPyCurlRequest(
            url=clustName + '/services/' + service + '/components/' + component,
            data=None,
            method='get',
            usernamepassword=self.username_password
        )
        d = json.loads(res)
        hostList = None
        cnt = 0
        logger.info("------------ * d json result: %s * ------------ " % (d))
        for val in d['host_components']:
            if isinstance(val, list) or isinstance(val, dict):
                for vl in val['HostRoles']:
                    if vl == 'host_name':
                        if cnt < 1:
                            hostList = val['HostRoles'][vl]
                        else:
                            hostList = hostList + ',' + val['HostRoles'][vl]
                        cnt += 1
        return hostList

    # Returns a comma separated list of hosts where the specific component is installed
    def getServicesHost(self, component):
        clustName = self.getAmbariClusterNameUrl()

        hres, res = self.callPyCurlRequest(
            url=clustName + '/components/' + component,
            data=None,
            method='get',
            usernamepassword=self.username_password
        )
        d = json.loads(res)
        hostList = None
        cnt = 0
        for val in d['hostComponents']:
            if isinstance(val, list) or isinstance(val, dict):
                for vl in val['RootServiceHostComponents']:
                    if vl == 'host_name':
                        if cnt < 1:
                            hostList = val['RootServiceHostComponents'][vl]
                        else:
                            hostList = hostList + ',' + val['RootServiceHostComponents'][vl]
                        cnt += 1
        return hostList


# Returns the state os service - INSTALLED,STARTED etc

    def getServiceState(self, service):
        clustName = self.getClusterNameUrl()
        hres, res = self.callPyCurlRequest(
            url=clustName + '/services/' + service + '?fields=ServiceInfo/state',
            data=None,
            method='get',
            usernamepassword=self.username_password
        )
        d = json.loads(res)
        for val in d['ServiceInfo']:
            if (val == 'state'):
                state = d['ServiceInfo'][val]
        return state

    #Invoke a curl request and return the header response and response
    def callPyCurlRequest(self, url, data, method, usernamepassword):
        buffer = BytesIO()
        header = BytesIO()

        logger.info(url)
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
