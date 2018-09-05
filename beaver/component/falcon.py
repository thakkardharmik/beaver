#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import collections
import os
from xml.dom import minidom

from beaver.component.hadoop import Hadoop, YARN, HDFS
from beaver.component.falcon_ambari_api_utils import FalconAmbariAPIUtil
from beaver.component.oozie import Oozie
from beaver.component.hive import Hive
from beaver.machine import Machine
from beaver.config import Config
from ConfigParser import ConfigParser
from beaver import util
from taskreporter.taskreporter import TaskReporter
import re
import json
import logging

import time, requests
from beaver.component.ambari import Ambari
import json as jsonlib
from requests.auth import HTTPBasicAuth
import urllib2, base64, socket, uuid

logger = logging.getLogger(__name__)

# Get the user information for hdfs, falcon and user going to run the job
hdfs_user = HDFS.getHDFSUser()
job_user = Config.getEnv('USER')

# Get the Current Directory
current_dir = os.path.dirname(os.path.realpath(__file__))
# Get the Artifact Directory to store the cluster xmls
artifacts_dir = Config.getEnv('ARTIFACTS_DIR')
base_falcon_dir = '/user/%s/falcon-regression' % job_user


class Falcon:
    _falcon_user = Config.get('falcon', 'FALCON_USER')
    IS_HA_ENABLED = Config.get("machine", "IS_HA_TEST")

    def __init__(self):
        pass

    _falcon_home = Config.get("falcon", "FALCON_HOME")

    if Machine.type() == 'Windows':
        falcon_cmd = os.path.join(_falcon_home, 'bin', 'falcon.py')
    else:
        falcon_cmd = os.path.join(_falcon_home, 'bin', 'falcon')
        falcon_start_cmd = os.path.join(_falcon_home, 'bin', 'falcon-start')
        falcon_stop_cmd = os.path.join(_falcon_home, 'bin', 'falcon-stop')

    # set the defaults
    _falcon_host_1 = Machine.getfqdn()
    _falcon_host_2 = Config.get("falcon", "HOST2")
    _falcon_host_3 = Config.get("falcon", "HOST3")

    # QE-3811: support the new parameters and keep them backwards compatible
    _falcon_server_host = None

    # set of masters for the first cluster
    _falcon_nn_1 = Config.get_default_when_empty("falcon", "FALCON_NN_1", _falcon_host_1)
    _falcon_rm_1 = Config.get_default_when_empty("falcon", "FALCON_RM_1", _falcon_host_1)
    _falcon_oozie_1 = Config.get_default_when_empty("falcon", "FALCON_OOZIE_1", _falcon_host_1)
    _falcon_hive_1 = Config.get_default_when_empty("falcon", "FALCON_HIVE_1", _falcon_host_1)
    _falcon_server_1 = Config.get_default_when_empty("falcon", "FALCON_SERVER_1", _falcon_host_1)

    _cluster_1_mapping = {}
    _cluster_1_mapping['nn'] = _falcon_nn_1
    _cluster_1_mapping['rm'] = _falcon_rm_1
    _cluster_1_mapping['hive'] = _falcon_hive_1
    _cluster_1_mapping['oozie'] = _falcon_oozie_1
    _cluster_1_mapping['falcon'] = _falcon_server_1

    # set of masters for the second cluster
    _falcon_nn_2 = Config.get_default_when_empty("falcon", "FALCON_NN_2", _falcon_host_2)
    _falcon_rm_2 = Config.get_default_when_empty("falcon", "FALCON_RM_2", _falcon_host_2)
    _falcon_oozie_2 = Config.get_default_when_empty("falcon", "FALCON_OOZIE_2", _falcon_host_2)
    _falcon_hive_2 = Config.get_default_when_empty("falcon", "FALCON_HIVE_2", _falcon_host_2)
    _falcon_server_2 = Config.get_default_when_empty("falcon", "FALCON_SERVER_2", _falcon_host_2)

    _cluster_2_mapping = {}
    _cluster_2_mapping['nn'] = _falcon_nn_2
    _cluster_2_mapping['rm'] = _falcon_rm_2
    _cluster_2_mapping['hive'] = _falcon_hive_2
    _cluster_2_mapping['oozie'] = _falcon_oozie_2
    _cluster_2_mapping['falcon'] = _falcon_server_2

    # set of masters for the third cluster
    _falcon_nn_3 = Config.get_default_when_empty("falcon", "FALCON_NN_3", _falcon_host_3)
    _falcon_rm_3 = Config.get_default_when_empty("falcon", "FALCON_RM_3", _falcon_host_3)
    _falcon_oozie_3 = Config.get_default_when_empty("falcon", "FALCON_OOZIE_3", _falcon_host_3)
    _falcon_hive_3 = Config.get_default_when_empty("falcon", "FALCON_HIVE_3", _falcon_host_3)
    _falcon_server_3 = Config.get_default_when_empty("falcon", "FALCON_SERVER_3", _falcon_host_3)

    _cluster_3_mapping = {}
    _cluster_3_mapping['nn'] = _falcon_nn_3
    _cluster_3_mapping['rm'] = _falcon_rm_3
    _cluster_3_mapping['hive'] = _falcon_hive_3
    _cluster_3_mapping['oozie'] = _falcon_oozie_3
    _cluster_3_mapping['falcon'] = _falcon_server_3

    if IS_HA_ENABLED == 'yes':
        hdfsConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_1, type='core-site')
        yarnConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_1, type='yarn-site')
        oozieConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_1, type='oozie-site')
        hiveConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_1, type='hive-site')
        falconConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_1, type='falcon-startup.properties')

        _cluster_1_mapping['nn'] = hdfsConfig['fs.defaultFS'].split('/')[2].split(':')[0]
        _cluster_1_mapping['rm'] = yarnConfig['yarn.resourcemanager.address'].split(':')[0]
        _cluster_1_mapping['hive'] = hiveConfig['hive.metastore.uris'].split(',')[0].rsplit(':', 1)[0].split('/')[2]
        _cluster_1_mapping['oozie'] = oozieConfig['oozie.base.url'].rsplit(':', 1)[0].split('/')[2]
        _cluster_1_mapping['falcon'] = falconConfig['*.broker.url'].rsplit(':', 1)[0].split('/')[2]

        logger.info("HOST2" + _falcon_host_2)
        if _falcon_host_2:
            hdfsConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_2, type='core-site')
            yarnConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_2, type='yarn-site')
            oozieConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_2, type='oozie-site')
            hiveConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_2, type='hive-site')
            falconConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_2, type='falcon-startup.properties')

            _cluster_2_mapping['nn'] = hdfsConfig['fs.defaultFS'].split('/')[2].split(':')[0]
            _cluster_2_mapping['rm'] = yarnConfig['yarn.resourcemanager.address'].split(':')[0]
            _cluster_2_mapping['hive'] = hiveConfig['hive.metastore.uris'].split(',')[0].rsplit(':',
                                                                                                1)[0].split('/')[2]
            _cluster_2_mapping['oozie'] = oozieConfig['oozie.base.url'].rsplit(':', 1)[0].split('/')[2]
            _cluster_2_mapping['falcon'] = falconConfig['*.broker.url'].rsplit(':', 1)[0].split('/')[2]

        if _falcon_host_3:
            hdfsConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_3, type='core-site')
            yarnConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_3, type='yarn-site')
            oozieConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_3, type='oozie-site')
            hiveConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_3, type='hive-site')
            falconConfig = FalconAmbariAPIUtil.getConfig(_falcon_host_3, type='falcon-startup.properties')

            _cluster_3_mapping['nn'] = hdfsConfig['fs.defaultFS'].split('/')[2].split(':')[0]
            _cluster_3_mapping['rm'] = yarnConfig['yarn.resourcemanager.address'].split(':')[0]
            _cluster_3_mapping['hive'] = hiveConfig['hive.metastore.uris'].split(',')[0].rsplit(':',
                                                                                                1)[0].split('/')[2]
            _cluster_3_mapping['oozie'] = oozieConfig['oozie.base.url'].rsplit(':', 1)[0].split('/')[2]
            _cluster_3_mapping['falcon'] = falconConfig['*.broker.url'].rsplit(':', 1)[0].split('/')[2]

    @classmethod
    def run(cls, cmd, logoutput=True, cwd=None, env={}):
        return cls.runas(None, cmd, cwd, env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, cwd=None, env={}, logoutput=True):
        run_cmd = Falcon.falcon_cmd + " " + cmd
        # get kerberos ticket
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            set_readonly = "chmod 400 " + kerbTicket
            Machine.runas(user, set_readonly, cwd=cwd, env=env, logoutput=logoutput)
            user = None

        return Machine.runas(user, run_cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        exit_code, output = cls.run("admin -version")
        if exit_code == 0:
            pattern = re.compile("Falcon server build version: (\S+)")
            m = pattern.search(output)
            if m:
                json_string = m.group(1)
                version_info = json.loads(json_string)
                version_info = version_info["properties"]
                version = mode = ""
                for item in version_info:
                    if str(item["key"]).lower() == "version":
                        version = item["value"]
                    if str(item["key"]).lower() == "mode":
                        mode = item["value"]

                version_split = version.split("-")
                return version_split[0] + "-" + version_split[1] + "-" + mode
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getRecipePath(cls):
        client_properties = Falcon.getClientPropertiesPath()
        val = util.getPropertyValueFromFile(client_properties, "falcon.recipe.path")
        return "" if val is None else val

    @classmethod
    def getClientPropertiesPath(cls):
        return os.path.join(cls.getConfDir(), "client.properties")

    @classmethod
    def getConfDir(cls):
        if Machine.type() == 'Windows':
            return os.path.join(cls._falcon_home, 'conf')
        else:
            return '/etc/falcon/conf'

    @classmethod
    def stop(cls, user, hostname):
        if Machine.type() == 'Windows':
            Machine.service("falcon", "stop", host=hostname)
        else:
            Machine.runas(user, cls.falcon_stop_cmd, host=hostname, cwd=None, env={}, logoutput=True)

    @classmethod
    def start(cls, user, hostname):
        if Machine.type() == 'Windows':
            Machine.service("falcon", "start", host=hostname)
        else:
            Machine.runas(user, cls.falcon_start_cmd, host=hostname, cwd=None, env={}, logoutput=True)

    @classmethod
    def getFalconUser(cls):
        return cls._falcon_user

    @classmethod
    @TaskReporter.report_test()
    def get_falcon_server(cls):
        if not cls._falcon_server_host:
            # read the config and determine what the host is
            prop_file = os.path.join(cls.getConfDir(), 'client.properties')
            prop_name = 'falcon.url'
            # read property
            falcon_url = util.getPropertyValueFromFile(prop_file, prop_name)
            # if property is not set or empty assume that we are running on the same node
            if not falcon_url or falcon_url.strip() == '':
                logger.error(
                    "Defaulting to machine fqdn as property %s not set or empty in file %s." % (prop_name, prop_file)
                )
                cls._falcon_server_host = Machine.getfqdn()
            else:
                regex = '^https?://(.*):.*'
                p = re.compile(regex)
                m = p.search(falcon_url)
                if m and m.group(1):
                    cls._falcon_server_host = m.group(1)
                else:
                    logger.error(
                        "Defaulting to machine fqdn as we did not match regex pattern %s in value %s" %
                        (regex, falcon_url)
                    )
                    cls._falcon_server_host = Machine.getfqdn()

        logger.info('Falcon Server: %s' % cls._falcon_server_host)
        return cls._falcon_server_host

    @classmethod
    def get_cluster_1_masters(cls):
        logger.info('Cluster #1: %s' % cls._cluster_1_mapping)
        return cls._cluster_1_mapping

    @classmethod
    def get_cluster_2_masters(cls):
        logger.info('Cluster #2: %s' % cls._cluster_2_mapping)
        return cls._cluster_2_mapping

    @classmethod
    def get_cluster_3_masters(cls):
        logger.info('Cluster #3: %s' % cls._cluster_3_mapping)
        return cls._cluster_3_mapping

    @classmethod
    @TaskReporter.report_test()
    def isTlsEnabled(cls):
        '''
        Returns if tls is enabled (String).
        '''
        return_value = "false"
        if Machine.isLinux():
            # gateway should have the startup.properties
            matchObjectList = util.findMatchingPatternInFile(
                "/etc/falcon/conf/startup.properties", "\*.falcon.enableTLS=(.*)", return0Or1=False
            )
            if len(matchObjectList) > 0:
                return_value = matchObjectList[0].group(1)
                logger.info("found *.falcon.enableTLS in startup.properties set to: " + return_value)
            else:
                logger.info("*.falcon.enableTLS is not set in startup.properties")
        else:
            return_value = "false"
        return return_value.lower() == "true"

    @classmethod
    @TaskReporter.report_test()
    def isAuthEnabled(cls):
        '''
        Returns if tls is enabled (String).
        '''
        return_value = "false"
        # gateway should have the startup.properties
        matchObjectList = util.findMatchingPatternInFile(
            os.path.join(cls._falcon_home, 'conf', 'startup.properties'),
            "\*.falcon.security.authorization.enabled=(.*)",
            return0Or1=False
        )
        if len(matchObjectList) > 0:
            return_value = matchObjectList[0].group(1)
            logger.info("found falcon.security.authorization.enabled in startup.properties set to: " + return_value)
        else:
            logger.info("*.falcon.security.authorization.enabled is not set in startup.properties")
        return return_value.lower() == "true"

    @classmethod
    @TaskReporter.report_test()
    def check_if_component_is_multicluster(cls):
        '''
        Checks whether NAT/tests are running with COMPONENT set to Falcon-Multicluster in conf/report.conf
        :return: True if component is Falcon-Multicluster else False
        '''
        report_conf = os.path.join(Config.getEnv('WORKSPACE'), 'conf', 'report.conf')
        if os.path.isfile(report_conf):
            logging.info("Going to Parse file " + str(report_conf))
            config = ConfigParser()
            config.optionxform = str
            config.read(report_conf)
            section = "HW-QE-PUBLISH-REPORT"
            if config.has_section(section):
                component = config.get(section, "COMPONENT")
                logging.info("File %s contains %s with value %s" % (report_conf, section, component))
                if "Falcon-Multicluster" in str(component):
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def getLogDir(cls, logoutput=False):
        '''
        Returns Falcon log directory (String).
        '''
        return_value = None
        if Machine.isLinux():
            # gateway should have the config file.
            matchObjList = util.findMatchingPatternInFile(
                "/etc/falcon/conf/falcon-env.sh", "export FALCON_LOG_DIR=(.*)", return0Or1=False
            )
            if len(matchObjList) > 0:
                return_value = matchObjList[0].group(1)
        else:
            # QE-3304: For now hard code the config location. I am not able to figure out where the config is defined
            # in windows.
            return_value = os.path.join('D:\\', 'hadoop', 'logs', 'falcon')
        if logoutput:
            logger.info("Falcon.getLogDir returns %s" % return_value)
        return return_value

    @classmethod
    @TaskReporter.report_test()
    def modify_pom_file(cls, pom_file, repo_url):
        """modify pom file to meet our needs
        :param falcon_version: version of the falcon client to put in pom
        """
        falcon_version = cls.getVersion().rsplit("-", 1)[0]
        pom_bak = os.path.realpath(pom_file) + "_bak"
        if not os.path.isfile(pom_bak):
            os.rename(os.path.realpath(pom_file), pom_bak)

        pom_obj = minidom.parse(pom_bak)
        # adding property
        properties = pom_obj.getElementsByTagName("properties")[0]
        output_property = pom_obj.createElement("redirectConsoleOutput")
        output_property.appendChild(pom_obj.createTextNode("true"))
        properties.appendChild(output_property)
        #modifying forkMode
        fork_mode = pom_obj.getElementsByTagName("forkMode")[0]
        fork_mode.firstChild.replaceWholeText("once")

        # modifying hw repo url
        for one_repo in\
                pom_obj.getElementsByTagName("repositories")[0].childNodes:
            if one_repo.nodeType == one_repo.ELEMENT_NODE\
                    and one_repo.tagName == "repository":
                is_horton_repo = False
                for repo_child in one_repo.childNodes:
                    if repo_child.nodeType == repo_child.ELEMENT_NODE\
                            and repo_child.tagName == "id" \
                            and repo_child.firstChild.wholeText\
                            == "hortonworks.repo":
                        is_horton_repo = True
                    if is_horton_repo\
                            and repo_child.nodeType == one_repo.ELEMENT_NODE\
                            and repo_child.tagName == "url":
                        repo_child.firstChild.replaceWholeText(repo_url)

        # modify version
        for depMgmt in pom_obj.getElementsByTagName("dependencyManagement"):
            for dependencies in depMgmt.childNodes:
                if dependencies.nodeType == dependencies.ELEMENT_NODE\
                        and dependencies.tagName == "dependencies":
                    for one_dep in dependencies.childNodes:
                        isFalconDependency = False
                        for dep_elem in one_dep.childNodes:
                            if dep_elem.nodeType == dep_elem.ELEMENT_NODE\
                                    and dep_elem.tagName == "groupId"\
                                    and dep_elem.firstChild.wholeText == "org.apache.falcon":
                                isFalconDependency = True
                            if isFalconDependency\
                                    and dep_elem.nodeType == dep_elem.ELEMENT_NODE\
                                    and dep_elem.tagName == "version":
                                dep_elem.firstChild.replaceWholeText(falcon_version)
        # writing the file
        with open(pom_file, 'w') as file:
            file.write(pom_obj.toxml())

    @classmethod
    @TaskReporter.report_test()
    def get_application_log(
            cls,
            host,
            appId,
            appOwner=None,
            nodeAddress=None,
            containerId=None,
            logoutput=True,
            grepFilter=None,
            pipeToFileOutput=None
    ):
        current_host = Machine.getfqdn()
        # reconfigure hadoop to use the new host
        core_site_changes = {}
        yarn_site_changes = {}
        yarn_site_changes['yarn.resourcemanager.hostname'] = host

        current_fs_default_fs = Hadoop.getConfigValue("fs.defaultFS", None)
        logger.info("Local Hadoop Conf value:" + current_fs_default_fs)

        logger.info("Host Name:" + host)
        remote_core_site_location = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "core-site.xml")
        logger.info("Remote Core Site Location:" + remote_core_site_location)
        local_core_site_location = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "core_site_" + host + ".xml")
        logger.info("Local Core Site Location:" + local_core_site_location)
        Machine.copyToLocal(None, host, remote_core_site_location, local_core_site_location, None)
        logger.info("Copied from " + host + " to local Machine")
        fs_default_fs = util.getPropertyValueFromConfigXMLFile(local_core_site_location, "fs.defaultFS", None)
        logger.info("NameNode for :" + host + "is " + fs_default_fs)

        regex_to_find_existing_namenode = "(hdfs|wasb)://(.*):(.*)"
        m = re.search(regex_to_find_existing_namenode, current_fs_default_fs)
        if m:
            current_host = m.group(2).strip()
        n = re.search(regex_to_find_existing_namenode, fs_default_fs)
        if n:
            namenode_host = m.group(2).strip()
        logger.info("Current NameNode :" + current_host)
        logger.info("Modifying NameNode :" + namenode_host)
        core_site_changes['fs.defaultFS'] = fs_default_fs.replace(current_host, namenode_host)

        import random
        tmp_conf_location = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'hadoopconf-%d' % int(999999 * random.random())
        )
        util.copyReadableFilesFromDir(Config.get('hadoop', 'HADOOP_CONF'), tmp_conf_location)
        util.writePropertiesToConfigXMLFile(
            os.path.join(tmp_conf_location, "core-site.xml"), os.path.join(tmp_conf_location, "core-site.xml"),
            core_site_changes
        )
        util.writePropertiesToConfigXMLFile(
            os.path.join(tmp_conf_location, "yarn-site.xml"), os.path.join(tmp_conf_location, "yarn-site.xml"),
            yarn_site_changes
        )

        exit_code, stdout = YARN.getLogsApplicationID(
            appId,
            appOwner=appOwner,
            containerId=containerId,
            logoutput=logoutput,
            grepFilter=grepFilter,
            pipeToFileOutput=pipeToFileOutput,
            config=tmp_conf_location
        )
        # Restore the config
        Machine.rm(user=None, host=Machine.getfqdn(), filepath=tmp_conf_location, isdir=True, passwd=None)
        return exit_code, stdout

    @classmethod
    @TaskReporter.report_test()
    def get_all_nodes(cls):
        nodes = []
        # on windows assume its 3 single node clusters
        if Machine.isWindows():
            nodes.append(cls._falcon_nn_1)
            nodes.append(cls._falcon_nn_2)
            nodes.append(cls._falcon_nn_3)
        else:
            # read the file /tmp/all_internal_nodes to determine all nodes
            with open('/tmp/all_internal_nodes') as f:
                nodes = [x.strip('\n') for x in f.readlines()]

        return list(set(nodes))

    @classmethod
    @TaskReporter.report_test()
    def getMiscTestLogPaths(cls, logoutput=False):
        HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
        miscTestLogPaths = [
            os.path.join(
                HADOOPQE_TESTS_DIR, "tests", "falcon", "falcon-regression", "merlin", "target", "surefire-reports"
            ),
            os.path.join(
                HADOOPQE_TESTS_DIR, "tests", "falcon", "falcon-regression", "falcon-regression", "merlin", "target",
                "surefire-reports"
            ),
            os.path.join(
                HADOOPQE_TESTS_DIR, "tests", "falcon", "falcon-regression", "falcon-regression", "merlin", "src",
                "main", "resources", "Merlin.properties"
            ),
            os.path.join(HADOOPQE_TESTS_DIR, "tests", "falcon", "falcon-regression", "pom.xml"),
            os.path.join(HADOOPQE_TESTS_DIR, "tests", "falcon", "captured_logs")
        ]

        if logoutput:
            logger.info("Falcon.getMiscTestLogPaths returns %s" % str(miscTestLogPaths))
        return miscTestLogPaths

    @classmethod
    @TaskReporter.report_test()
    def submitClusterEntity(cls, clusterXml, user=None):
        logger.info("submitClusterEntity: Started")
        cls.displayFile(clusterXml)
        CMD = " entity -type cluster -submit -file " + clusterXml
        exit_code, output = cls.runas(user, CMD)
        logger.info("submitClusterEntity: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def submitUpdatedClusterEntity(cls, name, clusterXml, user=None):
        logger.info("submitUpdatedClusterEntity: Started")
        cls.displayFile(clusterXml)
        CMD = " entity -type cluster -name " + name + " -update -file " + clusterXml
        exit_code, output = cls.runas(user, CMD)
        logger.info("submitUpdatedClusterEntity: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def updateClusterDependents(cls, name, user=None):
        logger.info("updateClusterDependents: Started")
        CMD = " entity -updateClusterDependents -cluster " + name
        exit_code, output = cls.runas(user, CMD)
        logger.info("updateClusterDependents: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def enableDisableSafeMode(cls, setsafemode):
        logger.info("enableDisableSafeMode: Started")
        CMD = " admin -setsafemode " + setsafemode
        exit_code, output = cls.runas(cls._falcon_user, CMD)
        logger.info("enableDisableSafeMode: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def createFalconDirectory(cls):

        logger.info("createFalconDirectory: Started")

        temp_dir = '/tmp'
        falcon_staging_dir = '/tmp/falcon-regression/staging'
        falcon_working_dir = '/tmp/falcon-regression/working'

        # Create falcon dirs for copying the test scripts
        logger.info("setup_module : Creating the %s", base_falcon_dir)
        exit_code, output = HDFS.deleteDirectory(base_falcon_dir, user=hdfs_user)
        exit_code, output = HDFS.createDirectory(base_falcon_dir, user=job_user, perm='777')

        # Create staging and working dirs for falcon
        HDFS.chmod(hdfs_user, "777", temp_dir, True)
        logger.info("setup_module : Creating the %s", falcon_staging_dir)
        HDFS.deleteDirectory(falcon_staging_dir, user=hdfs_user)
        HDFS.createDirectory(falcon_staging_dir, user=cls._falcon_user, perm=777, force=True)
        logger.info("setup_module : Creating the %s", falcon_working_dir)
        exit_code, output = HDFS.deleteDirectory(falcon_working_dir, user=hdfs_user)
        exit_code, output = HDFS.createDirectory(falcon_working_dir, user=cls._falcon_user, perm=755, force=True)
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def deleteEntity(cls, entityType, EntityFileName, user=None):

        logger.info("deleteEntity: Started")
        # Submit Feed or Process
        logger.info("deleteEntity: Deleting the entity " + entityType + " " + EntityFileName)
        CMD = " entity -type " + entityType + " -delete -name " + EntityFileName
        exit_code, output = cls.runas(user, CMD)
        logger.info("deleteEntity: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def deleteExtension(cls, extensionName, jobName, user=None):

        logger.info("deleteEntity: Started")
        # Submit Feed or Process
        logger.info("deleteEntity: Deleting the entity " + extensionName + " " + jobName)
        CMD = " extension -delete -extensionName " + extensionName + " -jobName " + jobName
        exit_code, output = cls.runas(user, CMD)
        logger.info("deleteEntity: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def displayFile(cls, fileName):
        logger.info("displayFile: Started")
        f = open(fileName, 'r')
        logger.info(f.read())
        f.close()
        logger.info("displayFile: Completed")
        return

    @classmethod
    @TaskReporter.report_test()
    def submitAndScheduleEntity(cls, entityType, entityFileName, entityName, user=None):

        logger.info("submitAndScheduleEntity: Started")

        # Display the Entity
        logger.info("Content of " + entityType + " xml %s" % entityFileName)
        cls.displayFile(entityFileName)

        # Submit Feed or Process
        exit_code, output = cls.submitEntity(entityType, entityFileName, user)
        # Schedule Feed or Process
        exit_code, output = cls.scheduleEntity(entityType, entityName, user)

        logger.info("submitAndScheduleEntity: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def submitEntity(cls, entityType, entityFileName, user=None):
        # Submit Feed or Process
        logger.info("submitEntity: Submitting the entity " + entityType + " " + entityFileName)
        CMD = " entity -type " + entityType + " -submit -file " + entityFileName
        exit_code, output = cls.runas(user, CMD)
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def scheduleEntity(cls, entityType, entityName, user=None):
        logger.info("scheduleEntity: Scheduling the entity -- " + entityType + ": " + entityName)
        CMD = " entity -type " + entityType + " -schedule -name " + entityName
        exit_code, output = cls.runas(user, CMD)
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def submitAndScheduleExtension(cls, extensionName, entityFile, user=None):

        logger.info("submitAndScheduleExtension: Started")

        # Display the Entity
        logger.info("Content of " + entityFile + " %s" % entityFile)
        cls.displayFile(entityFile)

        CMD = " extension -extensionName " + extensionName + " -submitAndSchedule -file " + entityFile
        exit_code, output = cls.runas(user, CMD)

        logger.info("submitAndScheduleExtension: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def entityDefiniton(cls, entityType, entityName, user=None):
        logger.info("entityDefiniton: Started")
        CMD = " entity -type " + entityType + "  -name " + entityName + " -definition"
        exit_code, output = cls.runas(user, CMD)
        logger.info("entityDefiniton: Completed")
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def createSubmitClusterEntity(cls, colo, desc, name, oozieUrl=None, Safemode=False):

        logger.info("createClusterEntities: Starting creating the cluster entity ")

        #This is the path to your NameNode service, for writing to HDFS when Falcon
        write_endpoint = Hadoop.getFSDefaultValue()
        logger.info("write_endpoint: %s ", write_endpoint)

        webhdfs_scheme = 'webhdfs'
        if HDFS.isHttpsEnabled():
            webhdfs_scheme = 'swebhdfs'

        #This is the path to your Namenode service, for talking to the NameNode from Falcon.
        #read_endpoint = '%s://%s:%s' %(webhdfs_scheme, write_endpoint.split('/')[2].split(':')[0], HDFS.getNNWebPort())
        read_endpoint = '%s://%s:20070' % (webhdfs_scheme, write_endpoint.split('/')[2].split(':')[0])
        logger.info("read_endpoint: %s ", read_endpoint)

        #This is the path to your Resource Manager service, for help in scheduling Map/Reduce jobs.
        execute_endpoint = YARN.getResourceManager()
        logger.info("execute_endpoint: %s ", execute_endpoint)

        #This is the path to your Oozie Service installation, for help in scheduling Oozie workflows.
        if oozieUrl is None:
            oozieUrl = Oozie.getOozieUrl()
            logger.info("oozieUrl: %s ", oozieUrl)

        #Getting the Falcon server information to specify in messaging endpoint
        falconNode = cls.get_falcon_server()
        logger.info("falconNode: %s ", falconNode)

        hcat_uri = Hive.getConfigValue("hive.metastore.uris").split(',')[0]
        logger.info("hcat uri: %s ", hcat_uri)

        hcat_server = hcat_uri.rsplit(':', 1)[0].split('/')[2]
        hive_server_2_uri = "hive2://" + hcat_server + ":10000"
        logger.info("hiveserver2 : %s ", hive_server_2_uri)

        entityText = "<?xml version=\"1.0\"?>" \
                     "<cluster colo=\"" + colo + "\" description=\"" + desc + "\" name=\"" + name + "\" " \
                     "xmlns=\"uri:falcon:cluster:0.1\"> " \
                        "<interfaces> " \
                            "<interface type=\"readonly\" endpoint=\""+read_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"write\" endpoint=\""+write_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"execute\" endpoint=\"" + execute_endpoint + "\" version=\"0.20.2\"/> " \
                            "<interface type=\"workflow\" endpoint=\"" + oozieUrl + "\" version=\"3.1\"/>" \
                            "<interface type=\"messaging\" endpoint=\"" \
                                "tcp://" + falconNode + ":61616?daemon=true\" version=\"5.1.6\"/>" \
                            "<interface type=\"registry\" endpoint=\"" + hcat_uri + "\" version=\"0.11.0\"/>" \
                        "</interfaces>" \
                        "<locations>"  \
                            "<location name=\"staging\" path=\"/tmp/falcon-regression/staging\" />" \
                            "<location name=\"temp\" path=\"/tmp\" />" \
                            "<location name=\"working\" path=\"/tmp/falcon-regression/working\" />"  \
                        "</locations>" \
                        "<ACL owner=\"" + job_user + "\" group=\"users\" permission=\"0755\"/>"

        entityText += "<properties>"
        if Hadoop.isSecure():
            realm = HDFS.getConfigValue('dfs.namenode.kerberos.principal').split('@')[1]
            entityText += "<property name=\"dfs.namenode.kerberos.principal\" value=\"nn/_HOST@" + realm + "\"/> "
            entityText += "<property name=\"hive.metastore.kerberos.principal\" value=\"hive/_HOST@" + realm + "\"/> "
            entityText += "<property name=\"hive.metastore.sasl.enabled\" value=\"true\"/> "

        entityText += "<property name=\"hadoop.rpc.protection\" value=\"authentication\"/>"
        entityText += "<property name=\"hive.metastore.uris\" value=\"" + hcat_uri + "\"/>"
        entityText += "<property name=\"hive.server2.uri\" value=\"" + hive_server_2_uri + "\"/>"
        entityText += "</properties>"
        entityText += "</cluster>"
        textFile = open(os.path.join(artifacts_dir, name + ".xml"), "w")
        textFile.write("%s" % entityText)
        textFile.close()

        #Submit the cluster entity
        clusterXml = os.path.join(artifacts_dir, name + ".xml")
        logger.info("Submitting the cluster entity %s" % clusterXml)
        if Safemode is True:
            exit_code, output = cls.submitUpdatedClusterEntity(name, clusterXml, cls._falcon_user)
            assert exit_code == 0
        else:
            exit_code, output = cls.submitClusterEntity(clusterXml)
        logger.info("createClusterEntities: completed the creation and submission of cluster entity ")

        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def createFalconDirectories(cls, host):

        logger.info("createFalconDirectory: Started")

        temp_dir = '/tmp'
        falcon_staging_dir = '/tmp/falcon-regression/staging'
        falcon_working_dir = '/tmp/falcon-regression/working'

        hdfsConfig = FalconAmbariAPIUtil.getConfig(host, type='core-site')
        endpoint = hdfsConfig['fs.defaultFS'].split('/')[2].split(':')[0]

        logger.info("Creating user directory on " + endpoint)
        test_user_home = 'hdfs://%s:8020/user/%s' % (endpoint, job_user)
        HDFS.createDirectoryAsUser(test_user_home, HDFS.getHDFSUser(), job_user, perm='711')

        # Create falcon dirs for copying the test scripts
        logger.info("setup_module : Creating the %s", base_falcon_dir)
        exit_code, output = HDFS.deleteDirectory('hdfs://' + endpoint + ':8020/%s' % base_falcon_dir, user=hdfs_user)
        exit_code, output = HDFS.createDirectory(
            'hdfs://' + endpoint + ':8020/%s' % base_falcon_dir, user=job_user, perm='777'
        )

        # Create staging and working dirs for falcon
        HDFS.chmod(hdfs_user, "777", 'hdfs://' + endpoint + ':8020/%s' % temp_dir, True)
        logger.info("setup_module : Creating the %s", falcon_staging_dir)
        HDFS.deleteDirectory('hdfs://' + endpoint + ':8020/%s' % falcon_staging_dir, user=hdfs_user)
        HDFS.createDirectory(
            'hdfs://' + endpoint + ':8020/%s' % falcon_staging_dir, user=cls._falcon_user, perm=777, force=True
        )
        logger.info("setup_module : Creating the %s", falcon_working_dir)
        exit_code, output = HDFS.deleteDirectory(
            'hdfs://' + endpoint + ':8020/%s' % falcon_working_dir, user=hdfs_user
        )
        exit_code, output = HDFS.createDirectory(
            'hdfs://' + endpoint + ':8020/%s' % falcon_working_dir, user=cls._falcon_user, perm=755, force=True
        )
        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def createClusterEntityREST(cls, colo, desc, name, host):

        logger.info("createClusterEntitiesREST: Starting creating the cluster entity ")

        #This is the path to your NameNode service, for writing to HDFS when Falcon
        hdfsConfig = FalconAmbariAPIUtil.getConfig(host, type='core-site')
        write_endpoint = hdfsConfig['fs.defaultFS']
        logger.info("write_endpoint: %s ", write_endpoint)

        webhdfs_scheme = 'webhdfs'
        if HDFS.isHttpsEnabled():
            webhdfs_scheme = 'swebhdfs'

        #This is the path to your Namenode service, for talking to the NameNode from Falcon.
        read_endpoint = '%s://%s:20070' % (webhdfs_scheme, write_endpoint.split('/')[2].split(':')[0])
        logger.info("read_endpoint: %s ", read_endpoint)

        #This is the path to your Resource Manager service, for help in scheduling Map/Reduce jobs.
        yarnConfig = FalconAmbariAPIUtil.getConfig(host, type='yarn-site')
        execute_endpoint = yarnConfig['yarn.resourcemanager.address']
        logger.info("execute_endpoint: %s ", execute_endpoint)

        #This is the path to your Oozie Service installation, for help in scheduling Oozie workflows.
        oozieConfig = FalconAmbariAPIUtil.getConfig(host, type='oozie-site')
        oozieUrl = oozieConfig['oozie.base.url']
        logger.info("oozieUrl: %s ", oozieUrl)

        #Getting the Falcon server information to specify in messaging endpoint
        falconConfig = FalconAmbariAPIUtil.getConfig(host, type='falcon-startup.properties')
        messaging = falconConfig['*.broker.url']
        logger.info("Falcon messaging: %s ", messaging)

        #Getting the hcat url information to specify in hive endpoint
        hiveConfig = FalconAmbariAPIUtil.getConfig(host, type='hive-site')
        hcat_uri = hiveConfig['hive.metastore.uris'].split(',')[0]
        logger.info("hcat uri: %s ", hcat_uri)

        hcat_server = hcat_uri.rsplit(':', 1)[0].split('/')[2]
        hive_server_2_uri = "hive2://" + hcat_server + ":10000"
        logger.info("hiveserver2 : %s ", hive_server_2_uri)

        entityText = "<?xml version=\"1.0\"?>" \
                     "<cluster colo=\"" + colo + "\" description=\"" + desc + "\" name=\"" + name + "\" " \
                     "xmlns=\"uri:falcon:cluster:0.1\"> " \
                        "<interfaces> " \
                            "<interface type=\"readonly\" endpoint=\""+read_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"write\" endpoint=\""+write_endpoint+"\" version=\"0.20.2\"/> " \
                            "<interface type=\"execute\" endpoint=\"" + execute_endpoint + "\" version=\"0.20.2\"/> " \
                            "<interface type=\"workflow\" endpoint=\"" + oozieUrl + "\" version=\"3.1\"/>" \
                            "<interface type=\"messaging\" endpoint=\"" +  messaging + "\" version=\"5.1.6\"/>" \
                            "<interface type=\"registry\" endpoint=\"" + hcat_uri + "\" version=\"0.11.0\"/>" \
                        "</interfaces>" \
                        "<locations>"  \
                            "<location name=\"staging\" path=\"/tmp/falcon-regression/staging\" />" \
                            "<location name=\"temp\" path=\"/tmp\" />" \
                            "<location name=\"working\" path=\"/tmp/falcon-regression/working\" />"  \
                        "</locations>" \
                        "<ACL owner=\"" + job_user + "\" group=\"users\" permission=\"0755\"/>"

        entityText += "<properties>"
        if Hadoop.isSecure():
            realm = HDFS.getConfigValue('dfs.namenode.kerberos.principal').split('@')[1]
            entityText += "<property name=\"dfs.namenode.kerberos.principal\" value=\"nn/_HOST@" + realm + "\"/> "
            entityText += "<property name=\"hive.metastore.kerberos.principal\" value=\"hive/_HOST@" + realm + "\"/> "
            entityText += "<property name=\"hive.metastore.sasl.enabled\" value=\"true\"/> "

        entityText += "<property name=\"hadoop.rpc.protection\" value=\"authentication\"/>"
        entityText += "<property name=\"hive.metastore.uris\" value=\"" + hcat_uri + "\"/>"
        entityText += "<property name=\"hive.server2.uri\" value=\"" + hive_server_2_uri + "\"/>"
        entityText += "</properties>"
        entityText += "</cluster>"
        textFile = open(os.path.join(artifacts_dir, name + ".xml"), "w")
        textFile.write("%s" % entityText)
        textFile.close()

        #Submit the cluster entity
        clusterXml = os.path.join(artifacts_dir, name + ".xml")
        logger.info("Submitting the cluster entity %s" % clusterXml)
        exit_code, output = cls.submitClusterEntity(clusterXml)
        logger.info("createClusterEntities: completed the creation and submission of cluster entity " + clusterXml)

        return exit_code, output

    @classmethod
    @TaskReporter.report_test()
    def redistributeMultiClusterKeytabs(cls):
        logger.info("Copy the hdfs keytab from third cluster into first cluster")
        HDFS_USER = Config.get('hadoop', 'HDFS_USER')
        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=Falcon._falcon_host_3,
            srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            destpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            passwd=Machine.getAdminPasswd()
        )
        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=Falcon._falcon_host_3,
            srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            destpath=Machine.getServiceKeytabsDir(),
            passwd=Machine.getAdminPasswd()
        )
        hosts = HDFS.getDatanodes()
        for host in hosts:
            logger.info("Copy the hdfs keytab to host " + host)
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=host,
                srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
                destpath=Machine.getServiceKeytabsDir(),
                passwd=Machine.getAdminPasswd()
            )

        logger.info("Copy the hdfs keytab from first cluster into second cluster")
        Machine.copyFromLocal(
            user=Machine.getAdminUser(),
            host=Falcon._falcon_host_2,
            srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            destpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            passwd=Machine.getAdminPasswd()
        )
        Machine.copyFromLocal(
            user=Machine.getAdminUser(),
            host=Falcon._falcon_host_2,
            srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
            destpath=Machine.getServiceKeytabsDir(),
            passwd=Machine.getAdminPasswd()
        )
        hosts = FalconAmbariAPIUtil.getServiceHosts(Falcon._falcon_host_2, 'HDFS', 'DATANODE')

        for host in hosts:
            logger.info("Copy the hdfs keytab to host " + host)
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=host,
                srcpath=Machine.getHeadlessUserKeytab(HDFS_USER),
                destpath=Machine.getServiceKeytabsDir(),
                passwd=Machine.getAdminPasswd()
            )

    @classmethod
    @TaskReporter.report_test()
    def getSubworkfloId(cls, processName, instanceId):
        logger.info("Bundle Name:FALCON_PROCESS_" + processName)
        exit_code, stdout = Oozie.runOozieJobsCmd(
            "-jobtype bundle -filter \"status=RUNNING;name=FALCON_PROCESS_" + processName + "\""
        )
        bundleId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-B)", stdout, re.MULTILINE)[0]
        logger.info("Bundle ID : " + bundleId)

        stdout = Oozie.getJobInfo(bundleId)
        coordinatorId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-C)", stdout, re.MULTILINE)[0]
        logger.info("Co-ordinator ID : " + coordinatorId)

        time.sleep(60)

        stdout = Oozie.getJobInfo(coordinatorId + "@" + instanceId)
        workflowId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-W)", stdout, re.MULTILINE)[0]
        logger.info("Workflow ID : " + workflowId)

        stdout = Oozie.getJobInfo(workflowId)
        subworkflowId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-W)", stdout, re.MULTILINE)[2]
        logger.info("SubWorkflow ID : " + subworkflowId)

        workflowIds = []
        workflowIds.append(subworkflowId)
        workflowIds.append(workflowId)

        return workflowIds

    @classmethod
    @TaskReporter.report_test()
    def getHiveServer(self, host):
        #Getting the hcat url information to specify in hive endpoint
        hiveConfig = FalconAmbariAPIUtil.getConfig(host, type='hive-site')
        hcat_uri = hiveConfig['hive.metastore.uris'].split(',')[0]
        logger.info("hcat uri: %s ", hcat_uri)

        hcat_server = hcat_uri.rsplit(':', 1)[0].split('/')[2]
        hive_server_2_uri = "hive2://" + hcat_server + ":10000"
        logger.info("hiveserver2 : %s ", hive_server_2_uri)
        return hive_server_2_uri

    @classmethod
    @TaskReporter.report_test()
    def find_replace(cls, recipefilename, key, value):
        file_data = ''
        try:
            file = open(recipefilename, 'r')
            file_data = file.read()
        finally:
            file.close()

        file_data = file_data.replace(key, value)
        try:
            file = open(recipefilename, 'w')
            file.write(file_data)
        finally:
            file.close()

    @classmethod
    @TaskReporter.report_test()
    def getWorkdlowId(cls, falconProcessName):

        logger.info("Bundle Name: " + falconProcessName)
        exit_code, stdout = Oozie.runOozieJobsCmd(
            "-jobtype bundle -filter \"status=RUNNING;name=" + falconProcessName + "\""
        )
        assert exit_code == 0
        bundleId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-B)", stdout, re.MULTILINE)[0]
        assert bundleId != None
        logger.info("Bundle ID : " + bundleId)

        stdout = Oozie.getJobInfo(bundleId)
        coordinatorId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-C)", stdout, re.MULTILINE)[0]
        logger.info("Co-ordinator ID : " + coordinatorId)
        assert coordinatorId != None

        time.sleep(15)

        stdout = Oozie.getJobInfo(coordinatorId + "@1")
        workflowId = re.findall(".*(\d{7}-\d{15}-oozie-\w{4}-W)", stdout, re.MULTILINE)[0]
        logger.info("Workflow ID : " + workflowId)
        assert workflowId != None

        workflowIds = []
        workflowIds.append(workflowId)
        return workflowIds

    @classmethod
    @TaskReporter.report_test()
    def getOozieServerName(cls, host):
        oozieConfig = FalconAmbariAPIUtil.getConfig(host, type='oozie-site')
        oozieUrl = oozieConfig['oozie.base.url']
        oozie_server = oozieUrl.rsplit(':')[1].split('/')[2]
        return oozie_server
