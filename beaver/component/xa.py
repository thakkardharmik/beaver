#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
import os
import time
import sys
import logging
import re
import json
import socket
import ConfigParser
from taskreporter.taskreporter import TaskReporter
import requests
from requests.auth import HTTPBasicAuth
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import HDFS
from beaver import util
from beaver.component.hadoop import Hadoop
from beaver.component.ambari import Ambari

# import xa_testenv from XaAgents test directory
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(current_dir, '../../tests/xasecure/xa-agents')))

logger = logging.getLogger(__name__)

XA_ADMIN_HOME = Config.get('xasecure', 'XA_ADMIN_HOME')
XA_USERSYNC = '/usr/hdp/current/ranger-usersync'
XA_ADMIN_HOST = Config.get('xasecure', 'XA_ADMIN_HOST')

KMS_HOME = Config.get('kms', 'KMS_HOME')
JAVA_HOME = Config.get('machine', 'JAVA_HOME')


class Xa(object):
    def __init__(self):
        pass

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        '''
        Returns XA version
        '''
        jarDir = os.path.join(Config.get("hadoop", "HADOOP_HOME"), 'lib')
        files = util.findMatchingFiles(jarDir, "ranger-plugins-audit-*.jar")
        p = re.compile('ranger-plugins-audit-(\\S+).jar')
        m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    def isArgus(cls):
        return Xa.isArgusInstalled()

    @classmethod
    def isWireEncryptionOn(cls):
        return HDFS.isEncrypted()

    @classmethod
    @TaskReporter.report_test()
    def isArgusInstalled(cls):
        if Machine.isWindows():
            import glob
            return bool(
                glob.glob(
                    os.path.join(
                        (Xa.getEnv('XA_SECURE_BASE', 'D:\\hdp\\ranger\\ranger-admin')),
                        'ranger*admin', 'ews', 'webapp',
                        'WEB-INF', 'classes', 'conf', 'ranger-admin-site.xml'
                    )
                )
            )
        else:
            srvcState = Xa.getServiceState('RANGER')
            return srvcState == "STARTED"

    @classmethod
    def isHdfsInstalled(cls):
        srvcState = Xa.getServiceState('HDFS')
        return srvcState == "STARTED"

    @classmethod
    def isHiveInstalled(cls):
        srvcState = Xa.getServiceState('HIVE')
        return srvcState == "STARTED"

    @classmethod
    def isHBaseInstalled(cls):
        srvcState = Xa.getServiceState('HBASE')
        return srvcState == "STARTED"

    @classmethod
    def isKnoxInstalled(cls):
        srvcState = Xa.getServiceState('KNOX')
        return srvcState == "STARTED"

    @classmethod
    def isStormInstalled(cls):
        srvcState = Xa.getServiceState('STORM')
        return srvcState == "STARTED"

    @classmethod
    def isSecurityEnabled(cls):
        return Hadoop.isSecure()

    @classmethod
    def getRealmName(cls):
        realm = Ambari.getConfig('kerberos-env')['realm']
        return realm

    @classmethod
    def getPolicyAdminHost(cls):
        return Xa.getPolicyAdminAddress().split('//', 1)[1].split(':', 1)[0]

    @classmethod
    def getPolicyAdminPort(cls):
        return Xa.getPolicyAdminAddress().split('//', 1)[1].split(':', 1)[1]

    @classmethod
    def getEnvBool(cls, name, defValue=False, section=None):
        ret = Xa.getEnv(name, defValue, section)

        return ret if isinstance(ret, bool) else (str(ret).lower() == 'true')

    @classmethod
    def getEnvInt(cls, name, defValue=None, section=None):
        ret = Xa.getEnv(name, defValue, section)

        return ret if isinstance(ret, int) else int(ret)

    @classmethod
    @TaskReporter.report_test()
    def getEnv(cls, name, defValue=None, section=None):
        iniConfig = ConfigParser.ConfigParser()
        scriptDir = os.path.dirname(os.path.realpath(__file__))
        testEnvDir = os.path.join(scriptDir, '..', '..', '..', '..', 'xasecure', 'testenv')
        testRootDir = os.path.join(scriptDir, '..', '..')
        if Hadoop.isAmbari():
            iniFileList = [os.path.join(testRootDir, 'xa_testenv.ini')]
        else:
            iniFileList = [os.path.join(scriptDir, 'xa_testenv.ini'), os.path.join(testEnvDir, 'xa_testenv.ini')]
        iniConfig.read(iniFileList)
        beaverConfig = None
        try:
            import beaver.config
            beaverConfig = beaver.config.Config
        except Exception, excp:
            logger.log.info('failed to load env from beaver', excp=excp)
        ret = None
        name = name.lower()
        if ret is None and iniConfig is not None:
            if iniConfig.has_section(section):
                if iniConfig.has_option(section, name):
                    ret = iniConfig.get(section, name)
            elif iniConfig.has_section('xasecure'):
                if iniConfig.has_option('xasecure', name):
                    ret = iniConfig.get('xasecure', name)

        if ret is None and beaverConfig is not None and beaverConfig.hasOption(section, name):
            ret = beaverConfig.get(section, name)

        if ret is None:
            ret = defValue

        return ret

    @classmethod
    @TaskReporter.report_test()
    def isLocalhost(cls, hostname):
        if hostname is None:
            return False
        elif hostname in ['localhost', socket.gethostname(), socket.getfqdn()]:
            return True
        else:
            return False

    @classmethod
    def isNotLocalhost(cls, hostname):
        return not Xa.isLocalhost(hostname)

    @classmethod
    @TaskReporter.report_test()
    def getPolicyMgrLogs(cls, logoutput=True):
        """
        Return a path of policy manager logs.
        :param logoutput: String: True will log output to console.
        :return: String;
        """
        result = None
        if Machine.isLinux():
            result = "/var/log/ranger/admin"
        else:
            result = "TODO"
        if logoutput:
            logger.info("getPolicyMgrLogs returns %s", result)
        return result

    @classmethod
    @TaskReporter.report_test()
    def getUserSyncLogs(cls, logoutput=True):
        """
        Return a path of usersync logs.
        :param logoutput: String: True will log output to console.
        :return: String;
        """
        result = None
        if Machine.isLinux():
            result = "/var/log/ranger/usersync"
        else:
            result = "TODO"
        if logoutput:
            logger.info("getAuthLog returns %s", result)
        return result

    @classmethod
    @TaskReporter.report_test()
    def getRangerNodes(cls, logoutput=True):
        """
        Return list of hosts of ranger nodes
        :param logoutput: String: True will log output to console.
        :return: list of str
        """
        result = []
        result.append(Xa.getPolicyAdminHost())
        if logoutput:
            logger.info("getRangerNodes returns %s", result)
        return result

    @classmethod
    @TaskReporter.report_test()
    def startRangerAdmin(cls, wait=180):
        if Machine.type() == 'Windows':
            Machine.service('ranger-admin', 'start', host=XA_ADMIN_HOST)
        else:
            cmd = os.path.join(XA_ADMIN_HOME, 'ews', 'start-ranger-admin.sh')
            startCmd = "export JAVA_HOME=%s; %s" % (JAVA_HOME, cmd)
            logger.info('RA: %s', str(startCmd))
            Machine.runas(Machine.getAdminUser(), startCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def stopRangerAdmin(cls, wait=5):
        if Machine.type() == 'Windows':
            Machine.service('ranger-admin', 'stop', host=XA_ADMIN_HOST)
        else:
            cmd = os.path.join(XA_ADMIN_HOME, 'ews', 'stop-ranger-admin.sh')
            stopCmd = "export JAVA_HOME=%s; %s" % (JAVA_HOME, cmd)
            logger.info('RA: %s', str(stopCmd))
            Machine.runas(Machine.getAdminUser(), stopCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    def restartRangerAdmin(cls):
        cls.stopRangerAdmin()
        time.sleep(5)
        cls.startRangerAdmin()

    @classmethod
    @TaskReporter.report_test()
    def startRangerUserSync(cls, wait=5):
        if Machine.type() == 'Windows':
            Machine.service('ranger-usersync', 'start', host=XA_ADMIN_HOST)
        else:
            cmd = os.path.join(XA_USERSYNC, 'ranger-usersync-start')
            startCmd = "export JAVA_HOME=%s; %s" % (JAVA_HOME, cmd)
            logger.info('RU: %s', str(startCmd))
            Machine.runas(Machine.getAdminUser(), startCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def stopRangerUserSync(cls, wait=10):
        if Machine.type() == 'Windows':
            Machine.service('ranger-usersync', 'stop', host=XA_ADMIN_HOST)
        else:
            cmd = os.path.join(XA_USERSYNC, 'ranger-usersync-stop')
            stopCmd = "export JAVA_HOME=%s; %s" % (JAVA_HOME, cmd)
            logger.info('RU: %s', str(stopCmd))
            Machine.runas(Machine.getAdminUser(), stopCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def startRangerKMS(cls, wait=180):
        if Machine.type() == 'Windows':
            Machine.service('ranger-kms', 'start', host=KMS_HOME)
        else:
            startCmd = os.path.join(KMS_HOME, 'ranger-kms start')
            logger.info('KMS: %s', str(startCmd))
            Machine.runas(Machine.getAdminUser(), startCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def stopRangerKMS(cls, wait=5):
        if Machine.type() == 'Windows':
            Machine.service('ranger-admin', 'stop', host=KMS_HOME)
        else:
            stopCmd = os.path.join(KMS_HOME, ' ranger-kms stop')
            logger.info('KMS: %s', str(stopCmd))
            Machine.runas(Machine.getAdminUser(), stopCmd, logoutput=True)
            time.sleep(wait)

    @classmethod
    @TaskReporter.report_test()
    def getKMSLogs(cls, logoutput=True):
        """
        Return a path of KMS logs.
        :param logoutput: String: True will log output to console.
        :return: String;
        """
        result = None
        if Machine.isLinux():
            result = "/var/log/ranger/kms"
        else:
            result = "TODO when kms is enabled in windows"
        if logoutput:
            logger.info("getKMSlog returns %s", result)
        return result

    @classmethod
    def restartRangerKMS(cls):
        cls.stopRangerKMS()
        cls.startRangerKMS()

    @classmethod
    def getPolicyAdminAddress(cls, ambariWeburl=None):
        return Ambari.getConfig('admin-properties', webURL=ambariWeburl)['policymgr_external_url']

    @classmethod
    @TaskReporter.report_test()
    def getAdminRestApiUrl(cls, ambariWeburl=None):
        addr = cls.getPolicyAdminAddress(ambariWeburl)
        path = 'service/public/api'
        return "%s/%s" % (addr, path)

    @classmethod
    @TaskReporter.report_test()
    def getAdminRestApiV2Url(cls, ambariWeburl=None):
        addr = cls.getPolicyAdminAddress(ambariWeburl)
        path = 'service/public/v2/api'
        return "%s/%s" % (addr, path)

    @classmethod
    def isPolicyAdminRunning(cls):
        srvcState = Xa.getServiceState('RANGER')
        return srvcState == "STARTED"

    @classmethod
    def getPolicyAdminUsername(cls):
        username = Config.get('xasecure', 'xa_admin_username', 'admin')
        return username

    @classmethod
    def getPolicyAdminPassword(cls):
        password = Config.get('xasecure', 'xa_admin_password', 'Rangerpassword@123')
        return password

    @classmethod
    @TaskReporter.report_test()
    def createPolicyRepository(cls, repo, config, retry=5, delay=10):
        copy = repo.copy()
        copy['config'] = json.dumps(config)
        url = cls.getAdminRestApiUrl() + "/repository"
        #print url
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(copy)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.post(url=url, auth=auth, headers=headers, data=data, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                break
            time.sleep(delay)
        assert status < 300, "Failed to create repository %d\n%s" % (status, text)
        #print text
        return response

    @classmethod
    @TaskReporter.report_test()
    def deleteRepository(cls, repoid):
        url = cls.getAdminRestApiUrl() + "/repository/" + repoid
        #print url
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        response = requests.delete(url=url, auth=auth, verify=False)
        assert response.status_code < 300 or response.status_code == 404, \
            "Failed to delete repository %d\n%s" % (response.status_code, response.text)
        #print response.text

    @classmethod
    @TaskReporter.report_test()
    def findRepositories(cls, nameRegex="^.*$", type=None, status=None, ambariWeburl=None): # pylint: disable=redefined-builtin
        repos = []
        url = cls.getAdminRestApiUrl(ambariWeburl) + "/repository"
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        #print "URL=" + url
        response = requests.get(url=url, auth=auth, verify=False)
        #print "TEXT=" + response.text
        vxrepo = response.json()
        if vxrepo['totalCount'] > 0:
            for repo in vxrepo['vXRepositories']:
                found = True
                found = found and (status is None or status == repo['isActive'])
                found = found and (type is None or type.lower() == repo['repositoryType'].lower())
                found = found and (nameRegex is None or not re.search(nameRegex, repo['name']) is None)
                if found:
                    repos.append(repo)
        return repos

    @classmethod
    @TaskReporter.report_test()
    def createPolicy(cls, policy, retry=5, delay=30):
        url = cls.getAdminRestApiUrl() + "/policy"
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        policy = json.dumps(policy)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.post(url=url, auth=auth, headers=headers, data=policy, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        assert status < 300, "Failed to create policy %d\n%s" % (status, text)
        return response

    @classmethod
    @TaskReporter.report_test()
    def deletePolicy(cls, polid):
        url = cls.getAdminRestApiUrl() + "/policy/" + str(polid)
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        response = requests.delete(url=url, auth=auth, verify=False)
        assert response.status_code < 300 or response.status_code == 404, \
            "Failed to delete policy %d\n%s" % (response.status_code, response.text)
        #print response.text

    @classmethod
    @TaskReporter.report_test()
    def import_policy_in_multicluster(
            cls, cluster_id, inputFilePath, isOverrideTrue='false', serviceTypeList=None, servicesMapJson=None
    ):
        from public_api_v2 import PublicAPIsv2
        importInfo = PublicAPIsv2.instance(cluster_id).importPoliciesInJsonFile(
            inputFilePath,
            isOverrideTrue=isOverrideTrue,
            serviceTypeList=serviceTypeList,
            servicesMapJson=servicesMapJson
        )
        return importInfo

    @classmethod
    @TaskReporter.report_test()
    def export_policy_in_multicluster(cls, cluster_id, outputfile, export_filter=None):
        from public_api_v2 import PublicAPIsv2
        exportInfo = PublicAPIsv2.instance(cluster_id).exportPoliciesInJsonFile(outputfile,
                                                                                filter=export_filter)
        return exportInfo

    @classmethod
    @TaskReporter.report_test()
    def getServiceState(cls, service):
        we = Xa.isWireEncryptionOn()
        clustNameUrl = Ambari.getWebUrl(is_enc=we) + "/api/v1/clusters/" + Ambari.getClusterName(is_enc=we)
        svcStateUrl = clustNameUrl + '/services/' + service + '?fields=ServiceInfo/state'
        retcode, retdata, _ = Ambari.performRESTCall(svcStateUrl)
        state = 'NOT INSTALLED'
        if retcode == 200:
            d = util.getJSON(retdata)
            for val in d['ServiceInfo']:
                if val == 'state':
                    state = d['ServiceInfo'][val]
        return state


    @classmethod
    @TaskReporter.report_test()
    def importPoliciesInJsonFile(
            cls,
            filepath,
            serviceTypeList=None,
            servicesMapJson=None,
            username=None,
            password=None,
            ambariWeburl=None,
            updateIfExists=False,
            policyActivationWaitTime=30,
            assertPolicyCreationFailure=True,
            polResource=None,
            isOverRideTrue=True
    ):
        if updateIfExists:
            url = Xa.getPolicyAdminAddress(
                ambariWeburl=ambariWeburl
            ) + '/service/plugins/policies/importPoliciesFromFile' + '?updateIfExists=true&polResource=' + polResource
        elif isOverRideTrue:
            url = Xa.getPolicyAdminAddress(
                ambariWeburl=ambariWeburl
            ) + '/service/plugins/policies/importPoliciesFromFile' + '?isOverride=True'
        else:
            url = Xa.getPolicyAdminAddress(
                ambariWeburl=ambariWeburl
            ) + '/service/plugins/policies/importPoliciesFromFile?'

        if serviceTypeList is not None:
            url = url + '&serviceType=' + serviceTypeList if updateIfExists is not None or isOverRideTrue is not None \
                else url + 'serviceType=' + serviceTypeList

        user = Machine.getAdminUser()

        if (username is not None) and (password is not None):
            curlUser = username
            curlPwd = password
        else:
            curlUser = Xa.getPolicyAdminUsername()
            curlPwd = Xa.getPolicyAdminPassword()
        if Hadoop.isEncrypted():
            if servicesMapJson is not None:
                cmd = "curl -i -k -u " + curlUser + ":" + curlPwd + \
                      " -H \"Accept: application/json\" -H \"Content-Type: multipart/form-data\" -X " \
                      "POST -F 'file=@" + str(filepath) + "' -F 'servicesMapJson=@" + \
                      servicesMapJson + "' \"" + url + "\""
            else:
                cmd = "curl -i -k -u " + curlUser + ":" + curlPwd + \
                      " -H \"Accept: application/json\" -H \"Content-Type: multipart/form-data\" -X " \
                      "POST -F 'file=@" + str(filepath) + "' \"" + url + "\""
        else:
            if servicesMapJson is not None:
                cmd = "curl -i -u " + curlUser + ":" + curlPwd + \
                      " -H \"Accept: application/json\" -H \"Content-Type: multipart/form-data\" -X " \
                      "POST -F 'file=@" + str(filepath) + "' -F 'servicesMapJson=@" + \
                      servicesMapJson + "' \"" + url + "\""
            else:
                cmd = "curl -i -u " + curlUser + ":" + curlPwd +\
                      " -H \"Accept: application/json\" -H \"Content-Type: multipart/form-data\" -X POST" \
                      " -F 'file=@" + str(filepath) + "' \"" + url + "\""
        time.sleep(policyActivationWaitTime)
        returncode, stdout = Machine.runas(user, cmd)
        if assertPolicyCreationFailure:
            assert returncode == 0, "Import of policy is failed"
            assert 'HTTP/1.1 400' not in str(stdout)
            return returncode, stdout
        else:
            return returncode, stdout

    @classmethod
    @TaskReporter.report_test()
    def getRangerPolicyIds(cls, repo, regex):
        policy_url = "%s/policy" % Xa.getAdminRestApiUrl()
        basic_auth = HTTPBasicAuth(Xa.getPolicyAdminUsername(), Xa.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}

        response = requests.get(url=policy_url, auth=basic_auth, headers=headers, verify=False)
        if response.status_code == 200:
            if regex is not None:
                policy_id_list = [
                    policy['id']
                    for policy in response.json()['vXPolicies']
                    if policy['repositoryName'] == repo and re.match(regex, policy['policyName'])
                ]
            else:
                policy_id_list = [
                    policy['id'] for policy in response.json()['vXPolicies'] if policy['repositoryName'] == repo
                ]
        else:
            policy_id_list = []
        return policy_id_list

    @classmethod
    @TaskReporter.report_test()
    def deleteRangerPolicies(cls, repo, regex):
        ids = cls.getRangerPolicyIds(repo, regex)
        for pol_id in ids:
            Xa.deletePolicy(pol_id)
        logger.info("Waiting for Ranger permissions to percolate")
        time.sleep(60)

    @classmethod
    @TaskReporter.report_test()
    def createPolicy_api_v2(
            cls, policy, retry=2, delay=30, policyActivationWaitTime=30, weburl=None, assertPolicyCreationFailure=True
    ):
        url = "%s/policy" % cls.getAdminRestApiV2Url(ambariWeburl=weburl)
        logger.info('policy creation url %s', str(url))
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        policy = json.dumps(policy)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.post(url=url, auth=auth, headers=headers, data=policy, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                time.sleep(policyActivationWaitTime)
                return response.status_code, response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info('policy creation response: %s ', str(response))
        if assertPolicyCreationFailure:
            assert status < 300, "Failed to create policy %d\n%s" % (status, text)
            return response
        else:
            return response.status_code, text


    @classmethod
    @TaskReporter.report_test()
    def getPolicy_api_v2(cls, servicetype=None, service_name=None, retry=1, delay=30, weburl=None):
        serviceName = "hadoop" if servicetype == "hdfs" else servicetype
        if servicetype is None:
            url = "%s/policy" % cls.getAdminRestApiV2Url(ambariWeburl=weburl)
        else:
            if service_name is None:
                service_name = Xa.findRepositories(
                    nameRegex="^.*_" + serviceName + "$", type=servicetype, status=True, ambariWeburl=weburl
                )[0]['name']
            url = "%s/service/%s/policy" % (
                cls.getAdminRestApiV2Url(ambariWeburl=weburl),
                service_name
            )
        logger.info('get policy url %s', str(url))
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.get(url=url, auth=auth, headers=headers, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info('policy creation response: %s', str(response))
        return text

    @classmethod
    @TaskReporter.report_test()
    def UpdatePolicy_api_v2(cls, policy, retry=2, delay=30, weburl=None):
        url = "%s/policy/%s" % (cls.getAdminRestApiV2Url(ambariWeburl=weburl), str(policy["id"]))
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        logger.info('url is : %s', str(url))
        logger.info('data is : %s', str(policy))
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.put(url=url, auth=auth, headers=headers, data=json.dumps(policy), verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info('policy updation response: %s', str(response))
        return text

    @classmethod
    @TaskReporter.report_test()
    def deletePolicy_by_id_api_v2(cls, policyid, retry=2, delay=30, weburl=None):
        url = "%s/policy/%s" % (cls.getAdminRestApiV2Url(ambariWeburl=weburl), str(policyid))
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        logger.info("delete policy url is: %s", url)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.delete(url=url, auth=auth, headers=headers, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return True
            elif status == 404:
                print text
                break
            time.sleep(delay)
        return text

    #currently only hdfs and hive supported
    @classmethod
    @TaskReporter.report_test()
    def getPoliciesForResources(cls, serviceType, serviceName=None, retry=1, delay=30, ambariWeburl=None, **resources):
        url = "%s/service/plugins/policies/exportJson?serviceType=%s" % (
            cls.getPolicyAdminAddress(ambariWeburl), serviceType
        )
        if serviceName is not None:
            url = url + '&serviceName=' + serviceName
        if serviceType == 'hive':
            db = resources.get('database')
            url = url + '&polResource=' + db + '&resource%3Adatabase=' + db
            if resources.get('table') is not None:
                url = url + '&resource%3Atable=' + resources.get('table')
                if resources.get('column') is not None:
                    url = url + '&resource%3Acolumn=' + resources.get('column')
        elif serviceType == 'hdfs':
            url = url + '&polResource=' + resources.get('path')
        logger.info("get policy url is: %s ", url)
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.get(url=url, auth=auth, headers=headers, verify=False)
            status = response.status_code

            text = response.text
            if status == 200:
                return response.json()
            elif status == 204:
                logger.info("No policy is present")
                return None
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info("response is %s", str(response))
        return text

    @classmethod
    @TaskReporter.report_test()
    def changePolicyInterval(cls, service, interval, wait=15, webURL=None):
        propsToSet = {'ranger.plugin.hive.policy.pollIntervalMs': interval}
        Ambari.setConfig("ranger-" + service.lower() + "-security", propsToSet, webURL=webURL)
        Ambari.start_stop_service(service, 'INSTALLED', waitForCompletion=True, weburl=webURL)
        logger.info("---- Done stopping %s cluster", service)
        time.sleep(wait)
        Ambari.start_stop_service(service, 'STARTED', waitForCompletion=True, weburl=webURL)
        logger.info("---- Done restarting %s HBase cluster", service)

    @classmethod
    @TaskReporter.report_test()
    def deleteAllRangerPolicyOtherThanInList(cls, serviceType, service_name=None, ambariWeburl=None,
                                             policiesToSkipFromDelete=None):
        policies_to_delete = Xa.getPolicy_api_v2(serviceType, service_name=service_name, weburl=ambariWeburl)
        if policies_to_delete is not None:
            for policy in policies_to_delete:
                if policiesToSkipFromDelete is not None:
                    if policy["name"] not in policiesToSkipFromDelete:
                        Xa.deletePolicy_by_id_api_v2(policy["id"], weburl=ambariWeburl)

    @classmethod
    @TaskReporter.report_test()
    def getRangerPolicyByName(cls, policyname, serviceName, weburl=None, retry=2, delay=30):
        url = "%s/service/%s/policy/%s" % (cls.getAdminRestApiV2Url(ambariWeburl=weburl), serviceName, str(policyname))
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        status = 0
        text = ''
        logger.info("get policy url is: %s", url)
        for _ in range(retry):
            response = requests.get(url=url, auth=auth, headers=headers, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info('get policy response: %s', str(text))
        return text

    @classmethod
    @TaskReporter.report_test()
    def updatePolicyByJsonFile(cls, inputjson, serviceType, serviceName=None, weburl=None, policyActivationTime=30):
        if serviceName is None:
            serviceName = \
                Xa.findRepositories(nameRegex="^.*_" + serviceType + "$", type=serviceType, status=True,
                                    ambariWeburl=weburl)[0]['name']
        with open(inputjson) as data_file:
            data = json.load(data_file)
        for policy in data["policies"]:
            existingPolicy = Xa.getRangerPolicyByName(policy["name"], serviceName, weburl=weburl)
            policy["id"] = existingPolicy["id"]
            policy["service"] = existingPolicy["service"]
            Xa.UpdatePolicy_api_v2(policy, weburl=weburl)
        #wait for policies to be active
        time.sleep(policyActivationTime)

    @classmethod
    @TaskReporter.report_test()
    def addInternalUser(cls, userjson, ambariWeburl=None, retry=2):
        url = cls.getPolicyAdminAddress(ambariWeburl=ambariWeburl) + '/service/xusers/secure/users'
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        userjson = json.dumps(userjson)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.post(url=url, auth=auth, headers=headers, data=userjson, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response
            elif status == 404:
                print text
                break
        logger.info('user creation response: %s', str(response))
        return response

    @classmethod
    @TaskReporter.report_test()
    def addInternalGroup(cls, groupjson, ambariWeburl=None, retry=2):
        url = cls.getPolicyAdminAddress(ambariWeburl=ambariWeburl) + '/service/xusers/secure/groups'
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        groupjson = json.dumps(groupjson)
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.post(url=url, auth=auth, headers=headers, data=groupjson, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response
            elif status == 404:
                print text
                break
        logger.info('user creation response: %s', str(response))
        return response

    @classmethod
    @TaskReporter.report_test()
    def getRangerServiceDefByServiceType(cls, serviceType, weburl=None, retry=2, delay=30):
        url = cls.getAdminRestApiV2Url(ambariWeburl=weburl) + "/servicedef/name/" + serviceType
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        status = 0
        text = ''
        logger.info("get policy url is: %s", url)
        for _ in range(retry):
            response = requests.get(url=url, auth=auth, headers=headers, verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                print text
                break
            time.sleep(delay)
        logger.info('get service Def response: %s', str(text))
        return text

    @classmethod
    @TaskReporter.report_test()
    def updateRangerServiceDef(cls, serviceType, serviceDef, weburl=None, retry=2, delay=30):
        url = cls.getAdminRestApiV2Url(ambariWeburl=weburl) + "/servicedef/name/" + serviceType
        auth = HTTPBasicAuth(cls.getPolicyAdminUsername(), cls.getPolicyAdminPassword())
        headers = {'Content-Type': 'application/json'}
        logger.info('url is : %s', str(url))
        logger.info('data is : %s', str(serviceDef))
        status = 0
        text = ''
        for _ in range(retry):
            response = requests.put(url=url, auth=auth, headers=headers, data=json.dumps(serviceDef), verify=False)
            status = response.status_code
            text = response.text
            if status < 300:
                return response.json()
            elif status == 404:
                logger.exception('Response for status code 404: %s ', text)
                break
            time.sleep(delay)
        logger.info('service def updation response: %s ', str(response))
        status = response.status_code
        if status != 200:
            logger.exception('Service Def updation FAILED with status code: '+str(status)+' RESPONSE: '+str(response))
        return text

    @classmethod
    @TaskReporter.report_test()
    def disableOrEnableenableDenyAndExceptionsInPolicies(
            cls, serviceList, enableenableDenyAndExceptionsInPolicies=True, weburl=None
    ):
        for service in serviceList:
            servicedef = Xa.getRangerServiceDefByServiceType(service, weburl=weburl)
            if servicedef["options"]["enableDenyAndExceptionsInPolicies"] != enableenableDenyAndExceptionsInPolicies:
                servicedef["options"]["enableDenyAndExceptionsInPolicies"] = enableenableDenyAndExceptionsInPolicies
                Xa.updateRangerServiceDef(service, servicedef, weburl=weburl)
                # waiting for policies to active after service def change
            time.sleep(30)

    #This method changes Hive service def to enable 'Resources-Accessed-Together'
    #  and 'Resources-Not-Accessed-Together' policy conditions.
    @classmethod
    @TaskReporter.report_test()
    def enableRsrcsAccessedTogetherPolCondInHivePolicies(cls, isDenyAndExceptionsToBeTurnedOn=True):
        servicedef = Xa.getRangerServiceDefByServiceType('hive')
        #logger.info("['policyConditions'][0] --> "+str(servicedef["policyConditions"][0]))
        if servicedef["policyConditions"] == [] or\
                        servicedef["policyConditions"][0]["name"] != "resources-accessed-together":
            servicedef["policyConditions"] = [{"itemId": 1, "name": "resources-accessed-together",
                                               "description": "Resources Accessed Together?",
                                               "label": "Resources Accessed Together?",
                                               "evaluator": "org.apache.ranger.plugin.conditionevaluator."
                                                            "RangerHiveResourcesAccessedTogetherCondition"},
                                              {"itemId":2, "name": "not-accessed-together",
                                               "description": "Resources Not Accessed Together?",
                                               "label": "Resources Not Accessed Together?",
                                               "evaluator": "org.apache.ranger.plugin.conditionevaluator."
                                                            "RangerHiveResourcesNotAccessedTogetherCondition"}]
            if isDenyAndExceptionsToBeTurnedOn:
                servicedef["options"]["enableDenyAndExceptionsInPolicies"] = True
            Xa.updateRangerServiceDef('hive', servicedef)
        else:
            logger.info("resources-accessed-together policy condition already present, nothing to be done")
        # waiting for policies to have the new policy condition after service def change
        time.sleep(30)

    #This method changes Hive service def to remove 'Resources-Accessed-Together'
    # and 'Resources-Not-Accessed-Together' policy conditions.
    #Also disables Deny And Exceptions in Hive policies that were turned on while adding policy conditions...
    @classmethod
    @TaskReporter.report_test()
    def disableRsrcsAccessedTogetherPolCondInHivePolicies(cls, isDenyAndExceptionsToBeTurnedOn=False):
        servicedef = Xa.getRangerServiceDefByServiceType('hive')
        servicedef["policyConditions"] = []
        if not isDenyAndExceptionsToBeTurnedOn:
            servicedef["options"]["enableDenyAndExceptionsInPolicies"] = False
        Xa.updateRangerServiceDef('hive', servicedef)
        # waiting for policies to have the new policy condition after service def change
        time.sleep(30)

    #This method is used to enable a ranger policy, provided the policyname - If the policyname arg is a regex
    #  and there are multiple policies with that name, all those will be enabled
    @classmethod
    @TaskReporter.report_test()
    def enableRangerPolicyByName(cls, repo, regex):
        ids = cls.getRangerPolicyIds(repo, regex)
        for pol_id in ids:
            policy_url = Xa.getAdminRestApiV2Url() + "/policy/" + pol_id
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
                if put_res.status_code != 200:
                    logger.error("Failed to enable policy: %s  with url: %s", json_data['policyName'], policy_url)
                else:
                    logger.info("Enabled policy with id: %s", pol_id)
        logger.info("Waiting for Ranger policy changes to be in effect")
        time.sleep(30)

    # This method is used to disable a ranger policy, provided the policyname - If the policyname arg is a regex
    #  and there are multiple policies with that name, all those will be disabled
    @classmethod
    @TaskReporter.report_test()
    def disableRangerPolicyByName(cls, repo, regex):
        ids = cls.getRangerPolicyIds(repo, regex)
        for pol_id in ids:
            policy_url = Xa.getAdminRestApiV2Url() + "/policy/" + pol_id
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
                if put_res.status_code != 200:
                    logger.error("Failed to disable policy: %s  with url: %s", json_data['policyName'], policy_url)
                else:
                    logger.info("Disabled policy with id: %s", pol_id)
        logger.info("Waiting for Ranger policy changes to be in effect")
        time.sleep(30)


class XaPolicy(object):
    PERMISSION_READ = 'Read'
    PERMISSION_WRITE = 'Write'
    PERMISSION_EXECUTE = 'Execute'
    PERMISSION_ADMIN = 'Admin'
    PERMISSION_CREATE = 'Create'
    PERMISSION_SELECT = 'Select'
    PERMISSION_UPDATE = 'Update'
    PERMISSION_DROP = 'Drop'
    PERMISSION_ALTER = 'Alter'
    PERMISSION_INDEX = 'Index'
    PERMISSION_LOCK = 'Lock'
    PERMISSION_ALL = 'All'
    PERMISSION_ALLOW = 'Allow'
    PERMISSION_REPL_ADMIN = 'REPLADMIN'

    PERMISSION_SUBMIT_TOPOLOGY = 'Submit Topology'
    PERMISSION_FILE_UPLOAD = 'File Upload'
    PERMISSION_FILE_DOWNLOAD = 'File Download'
    PERMISSION_KILL_TOPOLOGY = 'Kill Topology'
    PERMISSION_REBALANCE = 'Rebalance'
    PERMISSION_ACTIVATE = 'Activate'
    PERMISSION_DEACTIVATE = 'Deactivate'
    PERMISSION_GET_TOPOLOGY_CONF = 'Get Topology Conf'
    PERMISSION_GET_TOPOLOGY = 'Get Topology'
    PERMISSION_GET_USER_TOPOLOGY = 'Get User Topology'
    PERMISSION_GET_TOPOLOGY_INFO = 'Get Topology Info'
    PERMISSION_UPLOAD_NEW_CREDENTIAL = 'Upload New Credential'
    PERMISSION_PUBLISH = 'publish'
    PERMISSION_CONSUME = 'consume'
    PERMISSION_IDEMPOTENT_WRITE = 'idempotent_write'
    PERMISSION_DESCRIBE_CONFIG = 'describe_configs'
    PERMISSION_ALTER_CONFIG = 'alter_configs'
    PERMISSION_CONFIGURE = 'configure'
    PERMISSION_DESCRIBE = 'describe'
    PERMISSION_CREATE = 'create'
    PERMISSION_DELETE = 'delete'
    PERMISSION_KAFKA_ADMIN = 'kafka_admin'
    HDFS_ALL_PERMISSIONS = [PERMISSION_READ, PERMISSION_WRITE, PERMISSION_EXECUTE, PERMISSION_ADMIN]
    WASB_ALL_PERMISSIONS = [PERMISSION_READ, PERMISSION_WRITE]
    HIVE_ALL_PERMISSIONS = [
        PERMISSION_SELECT, PERMISSION_UPDATE, PERMISSION_CREATE, PERMISSION_DROP, PERMISSION_ALTER, PERMISSION_INDEX,
        PERMISSION_LOCK, PERMISSION_ALL, PERMISSION_REPL_ADMIN
    ]
    HBASE_ALL_PERMISSIONS = [PERMISSION_READ, PERMISSION_WRITE, PERMISSION_CREATE, PERMISSION_ADMIN]
    KNOX_ALL_PERMISSIONS = [PERMISSION_ALLOW, PERMISSION_ADMIN]
    STORM_ALL_PERMISSIONS = [
        PERMISSION_SUBMIT_TOPOLOGY, PERMISSION_FILE_UPLOAD, PERMISSION_FILE_DOWNLOAD, PERMISSION_KILL_TOPOLOGY,
        PERMISSION_REBALANCE, PERMISSION_ACTIVATE, PERMISSION_DEACTIVATE, PERMISSION_GET_TOPOLOGY_CONF,
        PERMISSION_GET_TOPOLOGY, PERMISSION_GET_USER_TOPOLOGY, PERMISSION_GET_TOPOLOGY_INFO,
        PERMISSION_UPLOAD_NEW_CREDENTIAL, PERMISSION_ADMIN
    ]
    KAFKA_ALL_PERMISSIONS = [
        PERMISSION_PUBLISH, PERMISSION_CONSUME, PERMISSION_CONFIGURE, PERMISSION_DESCRIBE, PERMISSION_CREATE,
        PERMISSION_DELETE, PERMISSION_KAFKA_ADMIN, PERMISSION_IDEMPOTENT_WRITE, PERMISSION_DESCRIBE_CONFIG,
        PERMISSION_ALTER_CONFIG
    ]

    def __init__(self):
        pass

    #currently support is there only for hdfs and hive
    @classmethod
    @TaskReporter.report_test()
    def getPolicyJson(
            cls,
            name,
            serviceType,
            policyItems,
            descritpion='',
            isEnabled=True,
            isRecursive=True,
            isAuditEnabled=True,
            ambariWeburl=None,
            **resources
    ):
        policy = {}
        repos = Xa.findRepositories(
            nameRegex="^.*_" + serviceType + "$", type=serviceType, status=True, ambariWeburl=ambariWeburl
        )
        repo = repos[0]
        policy['service'] = repo['name']
        policy['name'] = name
        policy['description'] = descritpion
        if serviceType == "hdfs":
            policy["resources"] = {
                "path": {
                    "values": [resources.get('path')],
                    "isRecursive": isRecursive,
                    "isExcludes": False
                }
            }
        elif serviceType == "hive":
            if resources.get('database') is not None:
                isExcludeForDB = False if resources.get("isExcludeForDB") is None else resources.get("isExcludeForDB")
                if resources.get('table') is not None:
                    isExcludeForTable = False if resources.get('isExcludeForDB') is None \
                        else resources.get('isExcludeForTable')
                    isExcludeForColumn = False if resources.get('isExcludeForDB') is None \
                        else resources.get('isExcludeForColumn')
                    policy['resources'] = {
                        "database": {
                            "values": [resources.get('database')],
                            "isExcludes": isExcludeForDB
                        },
                        "table": {
                            "values": [resources.get('table')],
                            "isExcludes": isExcludeForTable
                        },
                        "column": {
                            "values": [resources.get('column')],
                            "isExcludes": isExcludeForColumn
                        }
                    }
                else:
                    isExcludeForUdf = False if resources.get('isExcludeForUdf') is None \
                        else resources.get('isExcludeForUdf')
                    policy['resources'] = {
                        "database": {
                            "values": [resources.get('database')],
                            "isExcludes": isExcludeForDB
                        },
                        "udf": {
                            "values": [resources.get('udf')],
                            "isExcludes": isExcludeForUdf
                        }
                    }
            else:
                isExcludeForUrl = False if resources.get('isExcludeForDB') is None \
                    else resources.get('isExcludeForUrl')
                policy['resources'] = {
                    "url": {
                        "values": [resources.get('url')],
                        "isRecursive": isRecursive,
                        "isExcludes": isExcludeForUrl
                    }
                }
        policy['isEnabled'] = isEnabled
        policy['isAuditEnabled'] = isAuditEnabled
        policy['policyItems'] = policyItems
        return policy

    @classmethod
    @TaskReporter.report_test()
    def createPolicyItem(cls, userList=None, groupList=None, PermissionList=None):
        policyItems = {}
        accesses = []
        for perm in PermissionList:
            accesses.append({"type": perm, "isAllowed": True})
        policyItems["accesses"] = accesses
        if userList is not None:
            policyItems["users"] = userList
        if groupList is not None:
            policyItems["groups"] = groupList
        return policyItems
