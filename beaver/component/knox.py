#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from Cheetah.Template import Template
import requests
from requests.auth import HTTPBasicAuth
from beaver.component.hcatalog import Hcatalog
from beaver.component.xa import Xa
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
import hashlib, httplib
import json as jsonlib
import jsonpath_rw
import logging, os
import random, re
import shutil, string, sys
import tempfile, time
from datetime import datetime

#changes being done for HA enable
from beaver.component.hadoop import HDFS, Hadoop
from ConfigParser import ConfigParser
from beaver.component.xa_ambari_api_util import AmbariAPIUtil
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)
JAVA_HOME = Config.get('machine', 'JAVA_HOME')
JAVA_CMD = os.path.join(JAVA_HOME, 'bin', 'java')
HADOOP_HOME = Config.get('hadoop', 'HADOOP_HOME')

KNOX_HOME = Config.get('knox', 'KNOX_HOME')
KNOX_HOST = Config.get('knox', 'KNOX_HOST').split(',')[0]  # Just use the first Knox instance in the list for now.
KNOX_USER = Config.get('knox', 'KNOX_USER')
KNOX_CONF = Config.get('knox', 'KNOX_CONF')
knox_host_list = [Config.get('knox', 'KNOX_HOST')]  #Initialized with Knox Host

proxy_enabled = 'no'
#HDC Config
if Config.hasSection('hdc'):
    proxy_enabled = Config.get('hdc', 'USE_CLI')

#get list of hosts running Knox Gateway
if proxy_enabled == 'no':
    if (Hadoop.isEncrypted() and Machine.isHumboldt() == False):
        newAmbariUtil = AmbariAPIUtil(port=8443, isHttps=True)
    else:
        newAmbariUtil = AmbariAPIUtil(port=8080, isHttps=False)

if ((Machine.isHumboldt() == False) and (proxy_enabled == 'no')):
    # If multiple Knox Hosts, get the List using Ambari API
    knoxHosts = newAmbariUtil.getComponentHosts('KNOX', 'KNOX_GATEWAY')
    if knoxHosts is not None:
        knox_host_list = knoxHosts.split(",")
    logger.info("List of Hosts running Knox Gateway * knoxHosts: %s " % knoxHosts)

if (len(KNOX_HOST.split()) == 1):
    # If only one Knox Host, get the info from Config (takes care of HDC and Knox Proxy setup)
    logger.info("Knox Host is running on * KNOX_HOST = %s " % KNOX_HOST)
elif (len(knox_host_list) > 1):
    logger.info("List of Hosts running Knox Gateway ** knox_host_list: %s **" % knox_host_list)
else:
    logger.info("Not have Knox Host information. KNOX_HOST = %s " % KNOX_HOST)

if Machine.type() == 'Linux':
    KNOX_LOG = Config.get('knox', 'KNOX_LOG')
    KNOX_PID = Config.get('knox', 'KNOX_PID')


@TaskReporter.report_test()
def re_match(reg_exp, match_str):
    pattern = re.compile(reg_exp)
    return pattern.search(match_str)


class Knox:
    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        '''
        Returns Knox version
        '''
        KNOX_VERSION_CMD = os.path.join(KNOX_HOME, "bin", cls.getKnoxCli()) + " version"
        VERSION_CMD = "export JAVA_HOME=%s; %s" % (JAVA_HOME, KNOX_VERSION_CMD)
        if Machine.type() == 'Windows':
            VERSION_CMD = KNOX_VERSION_CMD
        exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), VERSION_CMD, host=KNOX_HOST, env=None, logoutput=True
        )
        logger.info("stdout = " + stdout)
        if exit_code == 0:
            pattern = re.compile("Apache Knox: (\S+)")
            m = pattern.search(stdout)
            if m:
                cls._version = m.group(1)
            else:
                cls._version = ""
        else:
            cls._version = ""

        return cls._version

    @classmethod
    def getKnoxCli(cls):
        '''
        Returns KnoxCli
        '''
        if Machine.isLinux():
            return 'knoxcli.sh'
        else:
            return 'knoxcli.cmd'

    @classmethod
    def getLdapScript(cls):
        '''
        Returns ldap script name
        '''
        if Machine.isLinux():
            return 'ldap.sh'
        else:
            return 'ldap.cmd'

    @classmethod
    def restartLdap(cls):
        cls.stopLdap()
        cls.startLdap()

    @classmethod
    @TaskReporter.report_test()
    def getHadoopJarDir(cls, component=None):
        '''
        Returns jar directory for hadoop
        '''
        if Machine.isLinux():
            if component == 'mapreduce':
                return '/usr/hdp/current/hadoop-mapreduce'
            else:
                return HADOOP_HOME
        else:
            return os.path.join(HADOOP_HOME, 'share', 'hadoop')

    @classmethod
    @TaskReporter.report_test()
    def resetHBaseRestService(cls, action, user=None, host=None, config=None):
        from beaver.component.hbase import HBase
        if user is None: user = Config.get('hbase', 'HBASE_USER')
        if host is None: host = HBase.getMasterNode()
        service = "rest"
        if Machine.isWindows():
            service = "hbrest"
        if action == "start":
            HBase.resetService(user, host, service, "stop", config=config)
        HBase.resetService(user, host, service, action, config=config)
        if action == "start":
            util.waitForPortToOpen(host, HBase.getRestServicePort())

    @classmethod
    @TaskReporter.report_test()
    def __runServiceScript__(cls, operation="start", knoxService="gateway", serUser=KNOX_USER):

        if Machine.type() == 'Windows':
            logger.info("__runServiceScript__: SERVICE=%s, OPERATION=%s" % (knoxService, operation))
            if (len(knox_host_list) > 1):
                for x in range(len(knox_host_list)):
                    exit_code, stdout = Machine.service(knoxService, operation, host=knox_host_list[x])
                    logger.info("__runServiceScript__: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            elif (len(KNOX_HOST.split()) == 1):
                exit_code, stdout = Machine.service(knoxService, operation, host=KNOX_HOST)
                logger.info("__runServiceScript__: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            else:
                logger.info("KNOX_HOST is not set. %s" % (KNOX_HOST))
            return stdout
        else:
            serviceScript = os.path.join(KNOX_HOME, "bin", knoxService + ".sh")
            SER_CMD = "export JAVA_HOME=%s; %s %s" % (JAVA_HOME, serviceScript, operation)
            logger.info("__runServiceScript__: COMMAND=%s" % SER_CMD)
            #put a loop to stop and start on all knox hosts
            if (len(knox_host_list) > 1):
                for x in range(len(knox_host_list)):
                    exit_code, stdout = Machine.runas(
                        serUser, "%s" % SER_CMD, host=knox_host_list[x], passwd=Machine.getAdminPasswd()
                    )
                    logger.info("__runServiceScript__: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            elif (len(KNOX_HOST.split()) == 1):
                exit_code, stdout = Machine.runas(
                    serUser, "%s" % SER_CMD, host=KNOX_HOST, passwd=Machine.getAdminPasswd()
                )
                logger.info("__runServiceScript__: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            else:
                logger.info("KNOX_HOST is not set. %s" % (KNOX_HOST))
            return stdout

    @classmethod
    @TaskReporter.report_test()
    def stopLdap(cls):
        if Machine.type() == 'Windows':
            stdout = cls.__runServiceScript__(operation="stop", knoxService="ldap", serUser=Machine.getAdminUser())
            time.sleep(20)
        else:
            ldapScript = os.path.join(KNOX_HOME, "bin", "ldap.sh")
            LDAP_CMD = "export JAVA_HOME=%s; %s" % (JAVA_HOME, ldapScript)
            #put a loop to stop and start on all knox hosts
            if (len(knox_host_list) > 1):
                for x in range(len(knox_host_list)):
                    exit_code, stdout = Machine.runas(
                        Machine.getAdminUser(),
                        "%s stop" % LDAP_CMD,
                        host=knox_host_list[x],
                        passwd=Machine.getAdminPasswd()
                    )
                    time.sleep(10)
                    logger.info(" Stop LDAP: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            elif (len(KNOX_HOST.split()) == 1):
                exit_code, stdout = Machine.runas(
                    Machine.getAdminUser(), "%s stop" % LDAP_CMD, host=KNOX_HOST, passwd=Machine.getAdminPasswd()
                )
                time.sleep(10)
                logger.info(" Stop LDAP: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            else:
                logger.info("Inside stopLdap() KNOX_HOST is not set. %s" % (KNOX_HOST))
            return stdout

    @classmethod
    @TaskReporter.report_test()
    def startLdap(cls):
        if Machine.type() == 'Windows':
            stdout = cls.__runServiceScript__(operation="start", knoxService="ldap", serUser=Machine.getAdminUser())
            util.waitForPortToOpen(KNOX_HOST, 33389)
        else:
            ldapScript = os.path.join(KNOX_HOME, "bin", "ldap.sh")
            LDAP_CMD = "export JAVA_HOME=%s; %s" % (JAVA_HOME, ldapScript)
            #put a loop to stop and start on all knox hosts
            if (len(knox_host_list) > 1):
                for x in range(len(knox_host_list)):
                    exit_code, stdout = Machine.runas(
                        Machine.getAdminUser(),
                        "%s start" % LDAP_CMD,
                        host=knox_host_list[x],
                        passwd=Machine.getAdminPasswd()
                    )
                    time.sleep(10)
                    logger.info(" Start LDAP: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            elif (len(KNOX_HOST.split()) == 1):
                exit_code, stdout = Machine.runas(
                    Machine.getAdminUser(), "%s start" % LDAP_CMD, host=KNOX_HOST, passwd=Machine.getAdminPasswd()
                )
                time.sleep(10)
                logger.info(" Start LDAP: EXIT_CODE=%d, STDOUT=%s" % (exit_code, stdout))
            else:
                logger.info("Inside startLdap() KNOX_HOST is not set. %s" % (KNOX_HOST))
            return stdout

    @classmethod
    def getLdapStatus(cls):
        if Machine.type() == 'Windows':
            return cls.__runServiceScript__(operation="query", knoxService="ldap", serUser=Machine.getAdminUser())
        else:
            return cls.__runServiceScript__(operation="status", knoxService="ldap", serUser=Machine.getAdminUser())

    @classmethod
    def stopKnox(cls):
        return cls.__runServiceScript__(operation="stop", knoxService="gateway", serUser=KNOX_USER)

    @classmethod
    def startKnox(cls):
        return cls.__runServiceScript__(operation="start", knoxService="gateway", serUser=KNOX_USER)

    @classmethod
    @TaskReporter.report_test()
    def getKnoxStatus(cls):
        if Machine.type() == 'Windows':
            return cls.__runServiceScript__(operation="query", knoxService="gateway", serUser=KNOX_USER)
        else:
            return cls.__runServiceScript__(operation="status", knoxService="gateway", serUser=KNOX_USER)

    @classmethod
    @TaskReporter.report_test()
    def restartKnox(cls, restartApacheDS=False):
        cls.stopKnox()
        if restartApacheDS:
            cls.stopLdap()
            cls.startLdap()
        cls.startKnox()
        time.sleep(10)

    @classmethod
    @TaskReporter.report_test()
    def getLdapPid(cls):
        if Machine.type() == 'Windows':
            return None
        else:
            exit_code, stdout = Machine.runas(
                Machine.getAdminUser(),
                "cat %s" % os.path.join(KNOX_PID, 'ldap.pid'),
                host=KNOX_HOST,
                passwd=Machine.getAdminPasswd()
            )
            if not exit_code == 0:
                return None
            else:
                stdout = (re.sub(r'Warning\: Permanently.*list of known hosts.', '', stdout)).strip()
                return stdout

    @classmethod
    @TaskReporter.report_test()
    def getKnoxPid(cls):
        if Machine.type() == 'Windows':
            return None
        else:
            exit_code, stdout = Machine.runas(
                Machine.getAdminUser(),
                "cat %s" % os.path.join(KNOX_PID, 'gateway.pid'),
                host=KNOX_HOST,
                passwd=Machine.getAdminPasswd()
            )
            if not exit_code == 0:
                return None
            else:
                stdout = (re.sub(r'Warning\: Permanently.*list of known hosts.', '', stdout)).strip()
                return stdout

    @classmethod
    def getAdminUsername(cls):
        return "admin"

    @classmethod
    def getAdminPassword(cls):
        return "admin-password"

    @classmethod
    @TaskReporter.report_test()
    def getAdminUrl(cls, host):
        scheme = "https"
        port = 8443
        baseUrl = "%s://%s:%s/gateway" % (scheme, host, port)
        adminUrl = "%s/%s/%s/%s" % (baseUrl, 'admin', 'api', 'v1')
        return adminUrl

    @classmethod
    @TaskReporter.report_test()
    def retrieveGatewaySiteConfig(cls):
        # Retrieve the remote file to a local temp file.
        timestamp = str(time.time())
        tempConfFile = os.path.join(Machine.getTempDir(), "gateway-site.xml_%s" % timestamp)
        remoteConfFile = os.path.join(Config.get('knox', 'KNOX_CONF'), 'gateway-site.xml')
        Machine.copyToLocal(user=None, host=KNOX_HOST, srcpath=remoteConfFile, destpath=tempConfFile)
        assert os.stat(
            tempConfFile
        ).st_size > 0, "Failed to copy Knox config from remote host, file empty %s" % tempConfFile
        # Parse the local temp file
        conf = util.readDictFromConfigXMLFile(tempConfFile)
        # Delete the local temp file
        Machine.rm(filepath=tempConfFile, user=None, host=None)
        assert not os.path.isfile(tempConfFile), "Failed to delete temp Knox config file %s" & tempConfFile
        # Return the dict
        return conf

    @classmethod
    @TaskReporter.report_test()
    def updateGatewaySiteConfig(cls, config, restartKnox=True):
        timestamp = str(time.time())
        # Write the config to a local config xml file
        localTempName = "gateway-site_source-%s.xml" % timestamp
        localTempConfFile = os.path.join(Machine.getTempDir(), localTempName)
        util.writeDictToConfigXmlFile(config, localTempConfFile)
        assert os.stat(
            localTempConfFile
        ).st_size > 0, "Failed to write Knox config to local host, file empty %s" % localTempConfFile
        # Transfer the local config xml file to a remote temp file
        remoteTempName = "gateway-site_target-%s.xml" % timestamp
        remoteTempFile = Machine.copyFileFromLocalToRemoteTmpDir(
            KNOX_HOST,
            localTempConfFile,
            remoteFileName=remoteTempName,
            madeExecutable=False,
            appendTimeToRemotePath=False,
            logoutput=True
        )
        # Delete the local temp file
        Machine.rm(filepath=localTempConfFile, user=None, host=None)
        assert not os.path.isfile(
            localTempConfFile
        ), "Failed to delete local temp Knox config file %s" & localTempConfFile
        # Backup the remote config file.
        remoteConfFile = os.path.join(Config.get('knox', 'KNOX_CONF'), 'gateway-site.xml')
        remoteConfFileBackup = "%s_%s" % (remoteConfFile, timestamp)
        Machine.rename(
            src=remoteConfFile,
            dest=remoteConfFileBackup,
            host=KNOX_HOST,
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )
        exists = Machine.pathExists(
            filepath=remoteConfFileBackup,
            host=KNOX_HOST,
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )
        assert exists, "Failed to backup %s to %s on host %s" % (remoteTempFile, remoteConfFile, KNOX_HOST)
        try:
            # Move the remote temp file over the target file
            Machine.rename(
                src=remoteTempFile,
                dest=remoteConfFile,
                host=KNOX_HOST,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd()
            )
            exists = Machine.pathExists(
                filepath=remoteConfFile, host=KNOX_HOST, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
            )
            assert exists, "Failed to move remote temp conf file %s to %s on host %s" % (
                remoteTempFile, remoteConfFile, KNOX_HOST
            )
            # Restart Knox.
            if restartKnox:
                cls.restartKnox()
        except:
            # Restore the remote backup config file.
            Machine.rename(
                src=remoteConfFileBackup,
                dest=remoteConfFile,
                host=KNOX_HOST,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd()
            )
            raise

    @classmethod
    @TaskReporter.report_test()
    def deployLdif(cls, host, ldif):
        target_ldif = os.path.join(KNOX_CONF, "users.ldif")
        backup_ldif = target_ldif + ".bk"

        # Copy test LDIF file to remote machine.
        tempName = "users-%d.ldif" % time.time()
        tempFile = Machine.copyFileFromLocalToRemoteTmpDir(
            host, ldif, remoteFileName=tempName, madeExecutable=False, appendTimeToRemotePath=False, logoutput=True
        )
        assert Machine.pathExists(
            user=Machine.getAdminUser(), host=host, filepath=tempFile, passwd=Machine.getAdminPasswd()
        ), "Failed to copy %s to %s on %s" % (ldif, tempFile, host)
        # Backup original LDIF file.
        exists = Machine.pathExists(
            user=Machine.getAdminUser(), host=host, filepath=target_ldif, passwd=Machine.getAdminPasswd()
        )
        if exists:
            if Machine.isWindows():
                if Machine.pathExists(filepath=backup_ldif, host=host, user=Machine.getAdminUser(),
                                      passwd=Machine.getAdminPasswd()):
                    Machine.rm(
                        filepath=backup_ldif, host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
                    )
            Machine.rename(
                user=Machine.getAdminUser(),
                host=host,
                src=target_ldif,
                dest=backup_ldif,
                passwd=Machine.getAdminPasswd()
            )
            assert Machine.pathExists(user=Machine.getAdminUser(),host=host, filepath=backup_ldif,
                                      passwd=Machine.getAdminPasswd() ), "Failed to backup %s to %s on %s" % \
                                                                         (target_ldif, backup_ldif, host)
        # Rename temp LDIF file to target name.
        if Machine.isWindows():
            if Machine.pathExists(filepath=target_ldif, host=host, user=Machine.getAdminUser(),
                                  passwd=Machine.getAdminPasswd()):
                Machine.rm(
                    filepath=target_ldif, host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
                )
        Machine.rename(
            user=Machine.getAdminUser(), host=host, src=tempFile, dest=target_ldif, passwd=Machine.getAdminPasswd()
        )
        assert Machine.pathExists(user=Machine.getAdminUser(),host=host, filepath=target_ldif,
                                  passwd=Machine.getAdminPasswd() ), "Failed to rename %s to %s on %s" % \
                                                                     (tempFile, target_ldif, host)
        # Set LDIF permissions.
        Machine.runas(Machine.getAdminUser(), "chown -R %s %s" % ("root", target_ldif))

        logger.info("KnoxLdp LDIF file %s copied to %s" % (ldif, target_ldif))

        # Restart Knox test LDAP server.
        Knox.restartLdap()

    @classmethod
    @TaskReporter.report_test()
    def deployTopology(
            cls,
            host,
            topology,
            template,
            params,
            topo_url="",
            username="admin",
            password="admin-password",
            poll_status_code=200,
            header={
                'Accept': 'application/json',
                'Accept-Encoding': 'identity'
            },
            skipAuth=False,
            remoteTopology=""
    ):
        logger.info("Start deploying topology")
        if len(remoteTopology) == 0:
            remoteTopology = topology

        logger.info("LOCAL TOPOLOGY_Name=%s" % topology)
        logger.info("REMOTE TOPOLOGY_Name=%s" % remoteTopology)
        logger.info("TOPOLOGY_FILE=%s" % template)
        logger.info("TOPOLOGY_DATA=%s" % params)
        # Load the template
        topo_tmpl = Template(file=template, searchList=[params])
        topo_xml = str(topo_tmpl)

        # Write template to a local temp file
        local_temp_name = os.path.join(Machine.getTempDir(), "%s-%d" % (topology, time.time()))
        with open(local_temp_name, "w") as local_temp_file:
            local_temp_file.write(topo_xml)
        assert os.path.isfile(local_temp_name), "Failed create local temp topology file %s" % (local_temp_name)

        # Copy the local temp file to a remote temp file
        if Machine.isSameHost(host):
            remote_temp_name = local_temp_name
        else:
            remote_temp_name = Machine.copyFileFromLocalToRemoteTmpDir(
                localPath=local_temp_name, host=host, madeExecutable=False, appendTimeToRemotePath=False
            )
            exists = Machine.pathExists(
                filepath=remote_temp_name, host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
            )
            assert exists, "Failed to copy local temp topology file %s to %s on host %s" % (
                local_temp_name, remote_temp_name, host
            )

        # Move the remote temp file to the topologies dir
        remote_topo_name = os.path.join(Config.get('knox', 'KNOX_CONF'), 'topologies', remoteTopology + ".xml")
        if Machine.isWindows():
            if Machine.pathExists(filepath=remote_topo_name, host=host, user=Machine.getAdminUser(),
                                  passwd=Machine.getAdminPasswd()):
                Machine.rm(
                    filepath=remote_topo_name, host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
                )
        Machine.rename(
            user=Machine.getAdminUser(),
            host=host,
            src=remote_temp_name,
            dest=remote_topo_name,
            passwd=Machine.getAdminPasswd()
        )
        exists = Machine.pathExists(
            user=Machine.getAdminUser(), host=host, filepath=remote_topo_name, passwd=Machine.getAdminPasswd()
        )
        assert exists, "Failed to move remote temp topology file %s to %s on host %s" % (
            remote_temp_name, remote_topo_name, host
        )

        # Poll until the topology url is found or timeout.
        found = False
        delay = 2
        count = 15
        status = 0

        logger.info("Continue to poll the topology status using admin version url %s" % topo_url)
        authentication = HTTPBasicAuth(username, password)
        for n in xrange(0, count):
            time.sleep(delay)
            if len(topo_url) == 0:
                topo_url = "%s/%s/%s" % (Knox.getAdminUrl(host=host), 'topologies', remoteTopology)
                response = requests.get(url=topo_url, verify=False, auth=authentication)
            else:
                if skipAuth:
                    response = requests.get(topo_url, headers=header, verify=False)
                else:
                    response = requests.get(topo_url, headers=header, verify=False, auth=authentication)

            status = response.status_code
            logger.info("Polling status : %d" % status)
            #print status
            if status == poll_status_code:
                found = True
                # An extra sleep because we were still getting a 404 sometimes.
                time.sleep(delay)
                break
        time.sleep(60)
        assert found, "Failed to deploy topology %s after %d seconds, status %s" % (
            remoteTopology, delay * count, status
        )

    @classmethod
    @TaskReporter.report_test()
    def undeployTopology(cls, host, topology):
        # Delete the remote topology file file
        topo_file_name = os.path.join(Config.get('knox', 'KNOX_CONF'), 'topologies', topology + ".xml")
        Machine.rm(filepath=topo_file_name, host=host, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
        exists = Machine.pathExists(
            user=Machine.getAdminUser(), host=host, filepath=topo_file_name, passwd=Machine.getAdminPasswd()
        )
        assert not exists, "Failed to remove remote topology file %s on host %s" % (topo_file_name, host)

        # Poll until the topology url is gone or timeout.
        gone = False
        delay = 1
        count = 10
        status = 0
        topo_url = "%s/%s/%s" % (Knox.getAdminUrl(host=host), 'topologies', topology)
        for n in xrange(0, count):
            time.sleep(delay)
            response = requests.get(
                url=topo_url, verify=False, auth=HTTPBasicAuth(Knox.getAdminUsername(), Knox.getAdminPassword())
            )
            status = response.status_code
            #print status
            if status == 204 or status == 404:
                gone = True
                break
        assert gone, "Failed to undeploy topology %s after %d seconds, status %s" % (topology, delay * count, status)

    @classmethod
    @TaskReporter.report_test()
    def setupOpenRangerKnoxPolicy(cls):
        logger.info(
            "============================== %s.%s =============================" %
            (__name__, sys._getframe().f_code.co_name)
        )
        logger.info("setupOpenRangerKnoxPolicy: Begin")
        repos = Xa.findRepositories(nameRegex="^.*_knox$", type="Knox", status=True)
        if len(repos) == 0:
            repo = {}
            repo['repositoryType'] = 'Knox'
            repo['name'] = "%s%d" % ('knox_test_knox_repo_', time.time())
            repo['description'] = 'Knox Test Knox Repo'
            repo['version'] = '0.1.0'
            repo['isActive'] = True
            config = {}
            config['username'] = Knox.getAdminUsername()
            config['password'] = Knox.getAdminPassword()
            config['knox.url'] = 'https://%KNOX_HOST%:8443/gateway/admin/api/v1/topologies'
            config['commonNameForCertificate'] = ''
            repo = Xa.createPolicyRepository(repo, config)
        else:
            assert len(repos
                       ) == 1, "Found wrong number of Knox Ranger policy repos. Expected 1, found %d." % len(repos)
            repo = repos[0]

        t = time.time()
        policy = {}
        policy['repositoryName'] = repo['name']
        policy['repositoryType'] = repo['repositoryType']
        policy['policyName'] = "%s%s%d" % (repo['name'], '_open_public_test_policy_', t)
        policy['description'] = 'Knox Open Public Test Policy'
        policy['topologies'] = "*,%d" % t
        policy['services'] = "*,%d" % t
        policy['isEnabled'] = True
        policy['isRecursive'] = True
        policy['isAuditEnabled'] = True
        policy['permMapList'] = [{'groupList': ['public'], 'permList': ['allow']}]
        #print "CREATE=" + jsonlib.dumps(policy)
        result = Xa.createPolicy(policy)
        #print "CREATED=" + jsonlib.dumps(result)
        logger.info("setupOpenRangerKnoxProxy: %s" % jsonlib.dumps(result))
        return result

    @classmethod
    @TaskReporter.report_test()
    def setupOpenRangerHivePolicy(cls):
        logger.info(
            "============================== %s.%s =============================" %
            (__name__, sys._getframe().f_code.co_name)
        )
        logger.info("setupOpenRangerHivePolicy: Begin")
        repos = Xa.findRepositories(nameRegex="^.*_hive$", type="Hive", status=True)
        if len(repos) == 0:
            repo = {}
            repo['repositoryType'] = 'Hive'
            repo['name'] = "%s%d" % ('knox_test_hive_repo_', time.time())
            repo['description'] = 'Knox Test Hive Repo'
            repo['version'] = '0.4.0.2.2.2.0-2509'
            repo['isActive'] = True
            config = {}
            config['username'] = 'hiveuser@EXAMPLE.COM'
            config['password'] = 'hivepassword'
            config['jdbc.driverClassName'] = 'org.apache.hive.jdbc.HiveDriver'
            config[
                'jdbc.url'
            ] = 'jdbc:hive2://ip-172-31-37-219.ec2.internal:10000/default;principal=hive/ip-172-31-37-219.ec2.internal@EXAMPLE.COM'
            config['commonNameForCertificate'] = ''
            config['isencrypted'] = True
            repo = Xa.createPolicyRepository(repo, config)
        else:
            assert len(repos
                       ) == 1, "Found wrong number of Hive Ranger policy repos. Expected 1, found %d." % len(repos)
            repo = repos[0]

        #print "REPO=" + jsonlib.dumps(repo,indent=4)
        t = time.time()
        policy = {}
        policy['repositoryName'] = repo['name']
        policy['repositoryType'] = repo['repositoryType']
        policy['policyName'] = "%s%s%d" % (repo['name'], '_open_public_test_policy_', t)
        policy['description'] = 'Open Knox Public Test Policy'
        policy['databases'] = '*, default'
        policy['tables'] = "*,%d" % t
        policy['columns'] = "*,%d" % t
        policy['isEnabled'] = True
        policy['isAuditEnabled'] = True
        policy['tableType'] = 'Inclusion'
        policy['columnType'] = 'Inclusion'
        policy['permMapList'] = {
            'groupList': ['public'],
            'permList': ['select', 'update', 'create', 'drop', 'alter', 'index', 'lock', 'all', 'admin']
        },
        #print "CREATE=" + jsonlib.dumps(policy)
        result = Xa.createPolicy(policy)
        logger.info("setupOpenRangerHivePolicy: %s" % jsonlib.dumps(result))
        return result

    ###############################################################################
    @classmethod
    @TaskReporter.report_test()
    def extractJsonPath(cls, json, expr):
        e = jsonpath_rw.parse(expr)
        r = e.find(json)
        a = [match.value for match in r]
        return a

    ###############################################################################
    @classmethod
    def randomString(cls, length):
        return ''.join(random.choice(string.ascii_uppercase) for i in range(length))

    ###############################################################################
    @classmethod
    @TaskReporter.report_test()
    def hashFile(cls, fileName):
        hasher = hashlib.md5()
        blockSize = 8 * 1024
        with open(fileName, 'rb') as fileDesc:
            block = fileDesc.read(blockSize)
            while len(block) > 0:
                hasher.update(block)
                block = fileDesc.read(blockSize)
        return hasher.hexdigest()

    ###############################################################################
    @classmethod
    @TaskReporter.report_test()
    def logConfig(cls, config):
        logger.info(
            "============================== %s.%s =============================" %
            (__name__, sys._getframe().f_code.co_name)
        )
        Config.config.write(sys.stdout)
        sys.stdout.flush()
        logger.info("[config]")
        logger.info(jsonlib.dumps(obj=config, sort_keys=True, indent=3))

    ###############################################################################
    @classmethod
    @TaskReporter.report_test()
    def setupHttpRequestsVerboseLogging(cls):
        logger.info(
            "============================== %s.%s =============================" %
            (__name__, sys._getframe().f_code.co_name)
        )
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        httplib.HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    ###############################################################################
    @classmethod
    @TaskReporter.report_test()
    def setupWebHCat(cls):
        #doing conf change through ambari rest api
        logger.info("Setup WebHCat config")
        propsToSet = {
            'templeton.exec.timeout': 60000,
        }
        newAmbariUtil.modify_service_configs('HIVE', 'webhcat-site', propsToSet)
