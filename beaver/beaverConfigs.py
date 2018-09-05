#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os
import re

from beaver import util
from beaver.component.ambari import Ambari
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)
WIN_BASE_PATH = Config.get('machine', 'BASE_PATH', default='')
PKG_QUERY_CMD = 'rpm -ql'
if Machine.isDebian() or Machine.isUbuntu():
    PKG_QUERY_CMD = 'dpkg -L'

HDP_STACK_INSTALLED = True
Config.setEnv("HDP_STACK_INSTALLED", "True")

CURRENT_DIR = None


def setAmbariConfigs():
    #TODO provide better way to determine javaHome
    #Machine.run("export JAVA_HOME=`find . -maxdepth 8 -type d -name 'jdk1.6.0_31'`")
    #exit_code, javaHome = Machine.run("echo $JAVA_HOME")
    #Machine.run("export PATH=\"$PATH:$JAVA_HOME/jre/bin\"")
    #javaHome = '/grid/0/jdk1.6.0_31/'
    from beaver.component.ambariHelper import Java2
    javaHome = Java2.setupJava()
    Config.set('machine', 'JAVA_HOME', javaHome, overwrite=False)
    Config.set('machine', 'JAVA_CMD', os.path.join(javaHome, 'bin', 'java'), overwrite=False)
    Machine.run('export JAVA_HOME=' + javaHome)
    _exit_code, host = getFirstExternalHost()
    Config.set('ambari', 'HOST', host, overwrite=False)
    _exit_code, version = Machine.run('rpm -qa | grep ambari-server')
    if Machine.isDebian() or Machine.isUbuntu():
        _exit_code, version = Machine.run('dpkg -l | grep ambari-server')
        version = " ".join(version.split()[1:-2])
    Config.set('ambari', 'VERSION', version, overwrite=False)
    if 'AMBARI_DB' in os.environ:
        Config.set('ambari', 'AMBARI_DB', os.environ['AMBARI_DB'], overwrite=False)
    else:
        Config.set('ambari', 'AMBARI_DB', 'Postgres', overwrite=False)
    if 'REALM' in os.environ:
        Config.set('ambari', 'REALM', os.environ['REALM'], overwrite=False)
    else:
        Config.set('ambari', 'REALM', '', overwrite=False)

    if 'USER_REALM' in os.environ:
        Config.set('ambari', 'USER_REALM', os.environ['USER_REALM'], overwrite=False)
    else:
        Config.set('ambari', 'USER_REALM', '', overwrite=False)

    if 'KERBEROS_SERVER_TYPE' in os.environ:
        Config.set('ambari', 'KERBEROS_SERVER_TYPE', os.environ['KERBEROS_SERVER_TYPE'], overwrite=False)
    else:
        Config.set('ambari', 'KERBEROS_SERVER_TYPE', 'MIT', overwrite=False)

    if 'USER_KERBEROS_SERVER_TYPE' in os.environ:
        Config.set('ambari', 'USER_KERBEROS_SERVER_TYPE', os.environ['USER_KERBEROS_SERVER_TYPE'], overwrite=False)
    else:
        Config.set('ambari', 'USER_KERBEROS_SERVER_TYPE', 'MIT', overwrite=False)

    if 'AD_SERVER_HOST' in os.environ:
        Config.set('ambari', 'AD_SERVER_HOST', os.environ['AD_SERVER_HOST'], overwrite=False)
    else:
        Config.set('ambari', 'AD_SERVER_HOST', '', overwrite=False)

    if 'JDK_64_LINK' in os.environ:
        Config.set('ambari', 'JDK_DOWNLOADURL', os.environ['JDK_64_LINK'], overwrite=True)
    else:
        Config.set('ambari', 'JDK_DOWNLOADURL', None, overwrite=True)
    if 'MVN_DWNLD_URL' in os.environ:
        Config.set('ambari', 'MAVEN_DOWNLOADURL', os.environ['MVN_DWNLD_URL'], overwrite=True)
    else:
        Config.set('ambari', 'MAVEN_DOWNLOADURL', None, overwrite=True)
    if 'STACK' in os.environ:
        stack = os.environ['STACK']
    else:
        stack = 'default'
    Config.set('ambari', 'STACK', stack, overwrite=False)

    stack_type = ""
    if 'STACK_TYPE' in os.environ:
        stack_type = os.environ['STACK_TYPE']

    if not stack_type:
        stack_type = 'HDP'
    Config.set('ambari', 'STACK_TYPE', stack_type, overwrite=False)

    if 'JDK_VERSION' in os.environ:
        jdk_version = os.environ['JDK_VERSION']
    else:
        jdk_version = 'default'
    Config.set('ambari', 'JDK_VERSION', jdk_version, overwrite=False)
    if 'UMASK' in os.environ:
        umask = os.environ['UMASK']
    else:
        umask = 'default'
    Config.set('ambari', 'UMASK', umask, overwrite=False)
    if 'AMBARI_CUSTOM_USER' in os.environ:
        ambari_user = os.environ['AMBARI_CUSTOM_USER']
    else:
        ambari_user = 'root'
    Config.set('ambari', 'AMBARI_CUSTOM_USER', ambari_user, overwrite=False)
    if 'AMBARI_AGENT_USER' in os.environ:
        ambari_agent_user = os.environ['AMBARI_AGENT_USER']
    else:
        ambari_agent_user = 'root'
    Config.set('ambari', 'AMBARI_AGENT_USER', ambari_agent_user, overwrite=False)
    if 'PWD_ENCRYPT' in os.environ:
        pwd_encrypt = os.environ['PWD_ENCRYPT']
    else:
        pwd_encrypt = 'false'
    Config.set('ambari', 'PWD_ENCRYPT', pwd_encrypt, overwrite=False)
    if 'RUN_MARKER_VERSION' in os.environ:
        run_marker_version = os.environ['RUN_MARKER_VERSION']
    else:
        run_marker_version = ''
    Config.set('ambari', 'RUN_MARKER_VERSION', run_marker_version, overwrite=False)
    if 'RUN_MARKER_LIST' in os.environ:
        run_marker_list = os.environ['RUN_MARKER_LIST']
    else:
        run_marker_list = ''
    Config.set('ambari', 'RUN_MARKER_LIST', run_marker_list, overwrite=False)
    if 'CUSTOMIZED_SERVICES_USERS' in os.environ:
        customized_services_users = os.environ['CUSTOMIZED_SERVICES_USERS']
    else:
        customized_services_users = 'false'
    Config.set('ambari', 'CUSTOMIZED_SERVICES_USERS', customized_services_users, overwrite=False)
    if 'HADOOPQA_USER' in os.environ:
        h_user = os.environ['HADOOPQA_USER']
    else:
        h_user = 'hrt_qa'
    Config.set('ambari', 'HADOOPQA_USER', h_user, overwrite=True)
    if 'CUSTOM_PIDS' in os.environ:
        custom_pids = os.environ['CUSTOM_PIDS']
    else:
        custom_pids = 'false'
    Config.set('ambari', 'CUSTOM_PIDS', custom_pids, overwrite=False)
    if 'AMBARI_2WAY_SSL' in os.environ:
        ambari_2way_ssl = os.environ['AMBARI_2WAY_SSL']
    else:
        ambari_2way_ssl = 'no'
    Config.set('ambari', 'AMBARI_2WAY_SSL', ambari_2way_ssl, overwrite=False)
    if 'IS_TMP_NOEXEC' in os.environ:
        is_tmp_noexec = os.environ['IS_TMP_NOEXEC']
    else:
        is_tmp_noexec = 'no'
    Config.set('ambari', 'IS_TMP_NOEXEC', is_tmp_noexec, overwrite=False)
    if 'NUMBER_OF_BASE_HOSTS' in os.environ:
        number_of_base_hosts = os.environ['NUMBER_OF_BASE_HOSTS']
    else:
        number_of_base_hosts = ''
    Config.set('ambari', 'NUMBER_OF_BASE_HOSTS', number_of_base_hosts, overwrite=False)
    if 'NUM_OF_CLUSTERS' in os.environ:
        num_of_clusters = os.environ['NUM_OF_CLUSTERS']
    else:
        num_of_clusters = '1'
    Config.set('ambari', 'NUM_OF_CLUSTERS', num_of_clusters, overwrite=False)
    if 'ADDITIONAL_AMBARI_PROPS' in os.environ:
        additional_ambari_props = os.environ['ADDITIONAL_AMBARI_PROPS']
    else:
        additional_ambari_props = ''
    Config.set('ambari', 'ADDITIONAL_AMBARI_PROPS', additional_ambari_props, overwrite=False)
    if 'DN_NONROOT' in os.environ:
        dn_nonroot = os.environ['DN_NONROOT']
    else:
        dn_nonroot = 'no'
    Config.set('ambari', 'DN_NONROOT', dn_nonroot, overwrite=False)
    if 'MOTD_ENABLE' in os.environ:
        motd_enable = os.environ['MOTD_ENABLE']
    else:
        motd_enable = 'false'
    Config.set('ambari', 'MOTD_ENABLE', motd_enable, overwrite=False)
    if 'HDP_REPO_BASEURL' in os.environ:
        Config.set('ambari', 'HDP_REPO_BASEURL', os.environ['HDP_REPO_BASEURL'], overwrite=False)
    if 'HDP_UTILS_REPO_BASEURL' in os.environ:
        Config.set('ambari', 'HDP_UTILS_REPO_BASEURL', os.environ['HDP_UTILS_REPO_BASEURL'], overwrite=False)
    if 'HDF_REPO_BASEURL' in os.environ:
        Config.set('ambari', 'HDF_REPO_BASEURL', os.environ['HDF_REPO_BASEURL'], overwrite=False)
    if 'MANAGEMENT_PACK_LINK' in os.environ:
        Config.set('ambari', 'MANAGEMENT_PACK_LINK', os.environ['MANAGEMENT_PACK_LINK'], overwrite=False)
    if 'REGISTRY_DB' in os.environ:
        Config.set('ambari', 'REGISTRY_DB', os.environ['REGISTRY_DB'], overwrite=False)
    if 'STREAMLINE_DB' in os.environ:
        Config.set('ambari', 'STREAMLINE_DB', os.environ['STREAMLINE_DB'], overwrite=False)
    if 'IS_SECURE' in os.environ and os.environ['IS_SECURE'] == 'yes':
        Config.set('machine', 'IS_SECURE', 'yes', overwrite=False)
    else:
        Config.set('machine', 'IS_SECURE', 'no', overwrite=False)
    #TODO read from os.environ
    if 'CLIENT' in os.environ:
        Config.set('ambari', 'CLIENT', os.environ['CLIENT'], overwrite=True)
    else:
        Config.set('ambari', 'CLIENT', 'localhost', overwrite=True)
    if 'CLIENT_OS' in os.environ:
        Config.set('ambari', 'CLIENT_OS', os.environ['CLIENT_OS'], overwrite=True)
    else:
        Config.set('ambari', 'CLIENT_OS', 'LINUX', overwrite=True)
    if 'CLIENT_PORT' in os.environ:
        Config.set('ambari', 'CLIENT_PORT', os.environ['CLIENT_PORT'], overwrite=True)
    else:
        Config.set('ambari', 'CLIENT_PORT', '5566', overwrite=True)
    if 'BROWSER' in os.environ:
        Config.set('ambari', 'BROWSER', os.environ['BROWSER'], overwrite=True)
    else:
        Config.set('ambari', 'BROWSER', 'firefox', overwrite=True)
    if 'DATABASE_FLAVOR' in os.environ:
        Config.set('ambari', 'DATABASE_FLAVOR', os.environ['DATABASE_FLAVOR'], overwrite=True)
    else:
        Config.set('ambari', 'DATABASE_FLAVOR', 'default', overwrite=True)
    if 'DATABASE_HOST' in os.environ:
        Config.set('ambari', 'DATABASE_HOST', os.environ['DATABASE_HOST'], overwrite=True)
    else:
        Config.set('ambari', 'DATABASE_HOST', 'default', overwrite=True)
    if 'XA_DATABASE_FLAVOR' in os.environ:
        Config.set('ambari', 'XA_DATABASE_FLAVOR', os.environ['XA_DATABASE_FLAVOR'], overwrite=True)
    else:
        Config.set('ambari', 'XA_DATABASE_FLAVOR', 'default', overwrite=True)
    if 'USE_BAKED_IMAGE' in os.environ:
        Config.set('ambari', 'USE_BAKED_IMAGE', os.environ['USE_BAKED_IMAGE'], overwrite=True)
    else:
        Config.set('ambari', 'USE_BAKED_IMAGE', 'no', overwrite=True)
    if 'USE_BLUEPRINT' in os.environ:
        Config.set('ambari', 'USE_BLUEPRINT', os.environ['USE_BLUEPRINT'], overwrite=True)
    else:
        Config.set('ambari', 'USE_BLUEPRINT', 'default', overwrite=True)
    if 'MR_FRAMEWORK' in os.environ:
        Config.set('ambari', 'MR_FRAMEWORK', os.environ['MR_FRAMEWORK'], overwrite=True)
    else:
        Config.set('ambari', 'MR_FRAMEWORK', 'yarn', overwrite=True)
    if 'IS_HA_TEST' in os.environ:
        Config.set('ambari', 'IS_HA_TEST', os.environ['IS_HA_TEST'], overwrite=True)
    else:
        Config.set('ambari', 'IS_HA_TEST', 'no', overwrite=True)
    if 'ENABLE_HA_COMPONENTS' in os.environ:
        Config.set('ambari', 'ENABLE_HA_COMPONENTS', os.environ['ENABLE_HA_COMPONENTS'], overwrite=True)
    else:
        Config.set('ambari', 'ENABLE_HA_COMPONENTS', 'all', overwrite=True)
    if 'ENABLE_MIXED_OS' in os.environ:
        Config.set('ambari', 'ENABLE_MIXED_OS', os.environ['ENABLE_MIXED_OS'], overwrite=True)
    else:
        Config.set('ambari', 'ENABLE_MIXED_OS', 'no', overwrite=True)
    if 'WIRE_ENCRYPTION' in os.environ:
        Config.set('ambari', 'WIRE_ENCRYPTION', os.environ['WIRE_ENCRYPTION'], overwrite=True)
    else:
        Config.set('ambari', 'WIRE_ENCRYPTION', 'no', overwrite=True)
    if 'AMBARI_VERSION' in os.environ:
        Config.set('ambari', 'AMBARI_VERSION', os.environ['AMBARI_VERSION'], overwrite=True)
    else:
        Config.set('ambari', 'AMBARI_VERSION', ' ', overwrite=True)
    if 'UPGRADE_TO' in os.environ:
        Config.set('ambari', 'UPGRADE_TO', os.environ['UPGRADE_TO'], overwrite=True)
    else:
        Config.set('ambari', 'UPGRADE_TO', None, overwrite=True)
    if 'STACK_UPGRADE_TO' in os.environ:
        Config.set('ambari', 'STACK_UPGRADE_TO', os.environ['STACK_UPGRADE_TO'], overwrite=True)
    else:
        Config.set('ambari', 'STACK_UPGRADE_TO', None, overwrite=True)
    if 'UPGRADE_FROM_REPO' in os.environ:
        Config.set('ambari', 'UPGRADE_FROM_REPO', os.environ['UPGRADE_FROM_REPO'], overwrite=True)
    else:
        Config.set('ambari', 'UPGRADE_FROM_REPO', None, overwrite=True)
    if 'SPLIT_NUM' in os.environ:
        Config.set('ambari', 'SPLIT_NUM', os.environ['SPLIT_NUM'], overwrite=True)
    else:
        Config.set('ambari', 'SPLIT_NUM', '1', overwrite=True)
    if 'COMPONENT' in os.environ:
        Config.set('ambari', 'COMPONENT', os.environ['COMPONENT'], overwrite=True)
    else:
        Config.set('ambari', 'COMPONENT', '', overwrite=True)
    if 'TESTSUITE_FILE' in os.environ:
        Config.set('ambari', 'TESTSUITE_FILE', os.environ['TESTSUITE_FILE'], overwrite=True)
    else:
        Config.set('ambari', 'TESTSUITE_FILE', '', overwrite=True)
    if 'AMBARI_TESTSNAMES' in os.environ:
        Config.set('ambari', 'AMBARI_TESTSNAMES', os.environ['AMBARI_TESTSNAMES'], overwrite=True)
    else:
        Config.set('ambari', 'AMBARI_TESTSNAMES', '', overwrite=True)
    if 'CLUSTER_TYPE' in os.environ:
        Config.set('ambari', 'CLUSTER_TYPE', os.environ['CLUSTER_TYPE'], overwrite=True)
    else:
        Config.set('ambari', 'CLUSTER_TYPE', '', overwrite=True)
    if 'CLUSTER' in os.environ:
        Config.set('ambari', 'CLUSTER_NAME', os.environ['CLUSTER'], overwrite=True)
    else:
        Config.set('ambari', 'CLUSTER_NAME', 'cl1', overwrite=True)
    if 'USER_REALM' in os.environ:
        Config.set('ambari', 'USER_REALM', os.environ['USER_REALM'], overwrite=True)
    else:
        Config.set('ambari', 'USER_REALM', 'EXAMPLE.COM', overwrite=True)
    if 'AD_SERVER_HOST' in os.environ:
        Config.set('ambari', 'AD_SERVER_HOST', os.environ['AD_SERVER_HOST'], overwrite=True)
    else:
        Config.set('ambari', 'AD_SERVER_HOST', 'ad-nano.qe.hortonworks.com', overwrite=True)
    if 'USER_KERBEROS_SERVER_TYPE' in os.environ:
        Config.set('ambari', 'USER_KERBEROS_SERVER_TYPE', os.environ['USER_KERBEROS_SERVER_TYPE'], overwrite=True)
    else:
        Config.set('ambari', 'USER_KERBEROS_SERVER_TYPE', 'MIT', overwrite=True)
    if 'AMBARI_SERVER_HTTPS' in os.environ:
        Config.set('ambari', 'AMBARI_SERVER_HTTPS', os.environ['AMBARI_SERVER_HTTPS'], overwrite=True)
    else:
        Config.set('ambari', 'AMBARI_SERVER_HTTPS', 'no', overwrite=True)
    if 'VIDEO_RECORDING' in os.environ:
        Config.set('ambari', 'VIDEO_RECORDING', os.environ['VIDEO_RECORDING'], overwrite=True)
    else:
        Config.set('ambari', 'VIDEO_RECORDING', 'yes', overwrite=True)


# Get first external host name from file /tmp/all_nodes. Needed for EC2 clusters
def getFirstExternalHost():
    try:
        file_path = "/tmp/all_nodes"
        host = open(file_path).read().split()[0]
        exit_code = 0
    except IOError:
        logger.info("!! ERROR:Could not read file with External host names: /tmp/all_nodes !!")
        exit_code, host = Machine.run('hostname -i')
    logger.info("RUNNING: %s", host)
    return exit_code, host


#  base method to call to set all the configs
def do_hdf_settings():
    java_home = Config.get('machine', 'DEPLOYED_JAVA_HOME')
    Config.set('machine', 'JAVA_HOME', java_home, overwrite=False)


def setConfigs():
    if Machine.isLinux():
        setHadoopConfigs()
        setCloudbreakConfigs()
        # requires HDP stack to be installed
        if HDP_STACK_INSTALLED:
            setHbaseConfigs()
            setSliderConfigs()
            setHiveConfigs()
            setSqoopConfigs()
            setSqoop2Configs()
            setFlumeConfigs()
            setMahoutConfigs()
            setOozieConfigs()
            setFirefoxConfigs()
            setZookeeperConfigs()
            setPigConfigs()
            setHcatalogConfigs()
            setMachineConfigs()
            setTempletonConfigs()
            setAccumuloConfigs()
            setTezConfigs()
            setStormConfigs()
            setHueConfigs()
            setPhoenixConfigs()
            setKnoxConfigs()
            setFalconConfigs()
            setSparkConfigs()
            setZeppelinConfigs()
            setLivyConfigs()
            setXAConfigs()
            setKafkaConfigs()
            setKMSConfigs()
            setDruidConfigs()
            setBeaconConfigs()
            setBigsqlConfigs()
        if is_hdf_cluster():
            setMachineConfigs()
            do_hdf_settings()
            setZookeeperConfigs()
            setKafkaConfigs()
            setStormConfigs()


def is_hdf_cluster():
    """ Check if this is a hdf cluster. Note: a cluster can be both hdp & hdf cluster. """
    return os.path.exists("/usr/hdf")


def setHDPStackInstalled():
    # set HDP_STACK_INSTALLED to false if hadoop is not installed.
    # currently this address windows platform. In future, if this is needed for linux, the method need to address it.
    if Machine.isWindows():
        hadoop_conf = Config.get('hadoop', 'HADOOP_CONF')
        if hadoop_conf is None or not os.path.exists(hadoop_conf) or not os.path.isfile(os.path.join(hadoop_conf,
                                                                                                     "core-site.xml")):
            logger.info(
                "In windows hadoop is not installed. Hence HDP_STACK_INSTALLED will be set to False (Config.setEnv)!"
            )
            global HDP_STACK_INSTALLED
            HDP_STACK_INSTALLED = False
            Config.setEnv("HDP_STACK_INSTALLED", "False")


def setHadoopConfigs():
    if Machine.type() == "Linux":
        setHadoopLinuxConfigs()
    else:
        setHadoopWindowsConfigs()

    logger.info("DEBUG HDP_STACK_INSTALLED is %s", HDP_STACK_INSTALLED)
    if HDP_STACK_INSTALLED:
        hadoopHome = Config.get('hadoop', 'HADOOP_HOME')

        logger.info(" DEBUG HADOOPQA USER SET CALLED")
        Config.set('hadoop', 'HADOOP_CMD', os.path.join(hadoopHome, 'bin', 'hadoop'), overwrite=False)
        Config.set('hadoop', 'HADOOP_CONF_EXCLUDE', 'True', overwrite=False)
        Config.set('hadoop', 'HADOOPQA_USER', Config.getEnv('USER'), overwrite=False)
        Config.set('hadoop', 'HADOOP_GROUP', 'hadoop', overwrite=False)

        #  import Hadoop after HADOOP_CONF has been set
        #  otherwise the import will fail
        #  this is set configs that are specific to Hadoop1 and 2
        from beaver.component.hadoop import Hadoop
        if Hadoop.isHadoop2():
            setHadoop2Configs()
        else:
            setHadoop1Configs()


def get_current_stack_name():
    try:
        DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
        logger.info("deploy code dir %s", DEPLOY_CODE_DIR)
        uifrm_folder = "uifrm_old/uifrm"
        amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder, 'ambari.properties')
        logger.info("ambari prop file %s", amb_prop_file)
        if not os.path.isfile(amb_prop_file):
            uifrm_folder = "uifrm"
        amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder)
        logger.info("final prop dir %s", amb_prop_file)
        from beaver.component.ambari_apilib import APICoreLib
        apicorelib = APICoreLib(amb_prop_file)
        logger.info("stack name is %s", apicorelib.get_current_stack_name())
        return apicorelib.get_current_stack_name()
    except Exception:
        logger.error(
            "Error while retrieving stack, default to HDP"
        )  #TODO:  This can come for HDI, better fix by detecting platform
        return "HDP"


def setHadoopLinuxConfigs():
    global HDP_STACK_INSTALLED
    #if 'STACK' in os.environ:
    #    stack = os.environ['STACK']
    #else:
    #    stack = 'default'
    install_status = Config.get('common', 'INSTALL_STATUS')
    if 'failed' in install_status:
        HDP_STACK_INSTALLED = False
        Config.setEnv("HDP_STACK_INSTALLED", "False")
        return

    stack_name = get_current_stack_name()
    logger.info("the current stack name is %s", stack_name)
    global CURRENT_DIR
    if "BigInsights" in stack_name:
        CURRENT_DIR = "/usr/iop"
    elif "HDP" in stack_name:
        CURRENT_DIR = "/usr/hdp"
    else:
        CURRENT_DIR = "/usr/hdp"
    logger.info("the current dir is set to %s", CURRENT_DIR)

    #  if this file exists assume you are on bigtop
    if os.path.isfile('/etc/hadoop/conf/core-site.xml'):
        logger.info("/etc/hadoop/conf/core-site.xml is present, stack install was passed.")
        Config.set('hadoop', 'HADOOP_CONF', '/etc/hadoop/conf', overwrite=False)
        Config.set('hadoop', 'HADOOP_HOME', '%s/current/hadoop-client' % CURRENT_DIR, overwrite=False)
    elif os.path.isfile('/etc/hadoop/core-site.xml'):
        logger.info("/etc/hadoop/core-site.xml is present, stack install was passed.")
        Config.set('hadoop', 'HADOOP_CONF', '/etc/hadoop', overwrite=False)
        Config.set('hadoop', 'HADOOP_HOME', '/usr', overwrite=False)
    # no hadoop installed
    else:
        logger.info(
            "HADOOP_CONF and HADOOP_HOME will be set. /etc/hadoop/conf/core-site.xml will be set to HADOOP_CONF."
        )
        Config.set('hadoop', 'HADOOP_CONF', '/etc/hadoop/conf', overwrite=False)
        Config.set('hadoop', 'HADOOP_HOME', '%s/current/hadoop-client' % CURRENT_DIR, overwrite=False)
        logger.info("hadoop conf XML files are not present, stack install was not passed.")
        logger.info("HDP_STACK_INSTALLED will be set to False (Config.setEnv) !")
        HDP_STACK_INSTALLED = False
        Config.setEnv("HDP_STACK_INSTALLED", "False")
        return

    Config.set('hadoop', 'OTHER_QA_USER', 'hrt_1', overwrite=False)
    Config.set('hadoop', 'HADOOP1_USER', 'hrt_1', overwrite=False)
    Config.set('hadoop', 'HDFS_USER', 'hdfs', overwrite=False)
    Config.set('hadoop', 'MAPRED_USER', 'mapred', overwrite=False)

    #  get JAVA_HOME from the hadoop configs
    hadoopConfDir = Config.get('hadoop', 'HADOOP_CONF')
    hadoopEnvFile = os.path.join(hadoopConfDir, 'hadoop-env.sh')
    javaHome = util.getPropertyValueFromFile(hadoopEnvFile, 'JAVA_HOME')
    Config.set('machine', 'JAVA_HOME', javaHome, overwrite=False)
    Config.set('machine', 'JAVA_CMD', os.path.join(javaHome, 'bin', 'java'), overwrite=False)

    #  get HADOOP_LOG_DIR
    dirName = util.getPropertyValueFromFile(hadoopEnvFile, 'HADOOP_LOG_DIR')
    if dirName != None:
        logDir = os.path.dirname(dirName)
    else:
        logDir = None
    Config.set('hadoop', 'HADOOP_LOG_DIR', logDir, overwrite=False)
    #  get HADOOP_PID_DIR
    dirName = util.getPropertyValueFromFile(hadoopEnvFile, 'HADOOP_PID_DIR')
    if dirName != None:
        pidDir = os.path.dirname(dirName)
    else:
        pidDir = None
    Config.set('hadoop', 'HADOOP_PID_DIR', pidDir, overwrite=False)
    if 'TESTSUITE_FILE' in os.environ:
        suiteFile = os.environ['TESTSUITE_FILE']
        if 'ambari' in suiteFile:
            ambari_server_properties = '/etc/ambari-server/conf/ambari.properties'
            #from beaver.component.ambari_apilib import CommonLib
            ambari_server_ssl = util.getPropertyValueFromFile(ambari_server_properties, "api.ssl")
            status = Ambari.checkIfHDPDeployed(False, ambari_server_ssl)
            logger.info("DEBUG checkIfHDPDeployed,status is %s", status)
            if status is False:
                HDP_STACK_INSTALLED = False
                Config.setEnv("HDP_STACK_INSTALLED", "False")


#  configurations specific to hadoop on windows
def setHadoopWindowsConfigs():
    hadoopHome = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=WIN_BASE_PATH,
        searchstr='hadoop-*-SNAPSHOT',
        passwd=Machine.getAdminPasswd()
    )[0]
    Config.set('hadoop', 'HADOOP_HOME', hadoopHome, overwrite=False)
    Config.set('hadoop', 'HADOOP_CONF', os.path.join(hadoopHome, 'conf'), overwrite=False)
    Config.set('hadoop', 'HADOOP_LOG_DIR', os.path.join(hadoopHome, 'log'), overwrite=False)
    Config.set('hadoop', 'WIN_UTILS', os.path.join(hadoopHome, 'bin', 'winutils.exe'), overwrite=False)
    #  get JAVA_HOME from the env
    javaHome = os.environ['JAVA_HOME']
    Config.set('machine', 'JAVA_HOME', javaHome, overwrite=False)
    Config.set('machine', 'JAVA_CMD', os.path.join(javaHome, 'bin', 'java'), overwrite=False)


def setHadoop1Configs():
    if Machine.type() == "Linux":
        setHadoop1LinuxConfigs()
    else:
        setHadoop1WindowsConfigs()


def setHadoop2Configs():
    if Machine.type() == "Linux":
        setHadoop2LinuxConfigs()
    else:
        setHadoop2WindowsConfigs()


#  configurations specific to hadoop 1
def setHadoop1LinuxConfigs():
    #  from the hadoop rpm determine the location of the varios jars
    _exit_code, examplesJar = Machine.run("%s hadoop | grep 'hadoop-examples.*.jar$' | head -n1" % PKG_QUERY_CMD)
    _exit_code, testJar = Machine.run("%s hadoop | grep 'hadoop-test.*.jar$' | head -n1" % PKG_QUERY_CMD)
    _exit_code, streamingJar = Machine.run("%s hadoop | grep 'hadoop-streaming.*.jar$' | head -n1" % PKG_QUERY_CMD)
    _exit_code, toolsJar = Machine.run("%s hadoop | grep 'hadoop-tools.*.jar$' | head -n1" % PKG_QUERY_CMD)
    _exit_code, gridMixJar = Machine.run("%s hadoop | grep 'hadoop-gridmix.*.jar$' | head -n1" % PKG_QUERY_CMD)

    Config.set('hadoop', 'HADOOP_EXAMPLES_JAR', examplesJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_TEST_JAR', testJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_STREAMING_JAR', streamingJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_TOOLS_JAR', toolsJar, overwrite=False)
    Config.set('hadoop', 'GRIDMIX_JAR', gridMixJar, overwrite=False)


#  configurations specific to hadoop 1 on windows
def setHadoop1WindowsConfigs():
    hadoopHome = Config.get('hadoop', 'HADOOP_HOME')
    examplesJar = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=hadoopHome,
        searchstr='hadoop-examples-*.jar',
        passwd=Machine.getAdminPasswd()
    )[0]
    testJar = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=hadoopHome,
        searchstr='hadoop-test-*.jar',
        passwd=Machine.getAdminPasswd()
    )[0]
    streamingJar = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=hadoopHome,
        searchstr='hadoop-streaming-*.jar',
        passwd=Machine.getAdminPasswd()
    )[0]
    toolsJar = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=hadoopHome,
        searchstr='hadoop-tools-*.jar',
        passwd=Machine.getAdminPasswd()
    )[0]
    gridMixJar = Machine.find(
        user=Machine.getAdminUser(),
        host='',
        filepath=hadoopHome,
        searchstr='hadoop-gridmix-*.jar',
        passwd=Machine.getAdminPasswd()
    )[0]

    Config.set('hadoop', 'HADOOP_EXAMPLES_JAR', examplesJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_TEST_JAR', testJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_STREAMING_JAR', streamingJar, overwrite=False)
    Config.set('hadoop', 'HADOOP_TOOLS_JAR', toolsJar, overwrite=False)
    Config.set('hadoop', 'GRIDMIX_JAR', gridMixJar, overwrite=False)


#  configurations specific to hadoop 2 on linux
def setHadoop2LinuxConfigs():
    Config.set(
        'hadoop', 'HADOOP_CMD', os.path.join(Config.get('hadoop', 'HADOOP_HOME'), 'bin', 'hadoop'), overwrite=False
    )
    Config.set('hadoop', 'HDFS_HOME', '%s/current/hadoop-hdfs-client' % CURRENT_DIR, overwrite=False)
    Config.set('hadoop', 'HDFS_CMD', os.path.join(Config.get('hadoop', 'HDFS_HOME'), 'bin', 'hdfs'), overwrite=False)
    Config.set('hadoop', 'MAPRED_HOME', '%s/current/hadoop-mapreduce-client' % CURRENT_DIR, overwrite=False)
    Config.set(
        'hadoop', 'MAPRED_CMD', os.path.join(Config.get('hadoop', 'MAPRED_HOME'), 'bin', 'mapred'), overwrite=False
    )
    Config.set('hadoop', 'YARN_HOME', '%s/current/hadoop-yarn-client' % CURRENT_DIR, overwrite=False)
    Config.set('hadoop', 'YARN_ATS_HOME', '%s/current/hadoop-yarn-timelineserver' % CURRENT_DIR, overwrite=False)
    Config.set('hadoop', 'YARN_CMD', os.path.join(Config.get('hadoop', 'YARN_HOME'), 'bin', 'yarn'), overwrite=False)
    Config.set('hadoop', 'YARN_USER', 'yarn', overwrite=False)
    Config.set(
        'hadoop', 'HADOOP_LIBEXEC', os.path.join(Config.get('hadoop', 'HADOOP_HOME'), 'libexec'), overwrite=False
    )

    _hadoopHome = Config.get('hadoop', 'HADOOP_HOME')
    mapredHome = Config.get('hadoop', 'MAPRED_HOME')
    _yarnHome = Config.get('hadoop', 'YARN_HOME')
    #  determine the yarn env file
    yarnEnvFile = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), 'yarn-env.sh')
    #  get YARN_LOG_DIR
    dirName = util.getPropertyValueFromBashFile(yarnEnvFile, 'HADOOP_LOG_DIR')
    if dirName != None:
        logDir = os.path.dirname(dirName)
    else:
        logDir = None
    Config.set('hadoop', 'YARN_LOG_DIR', logDir, overwrite=False)
    #  get YARN_PID_DIR
    dirName = util.getPropertyValueFromFile(yarnEnvFile, 'HADOOP_PID_DIR')
    if dirName != None:
        pidDir = os.path.dirname(dirName)
    else:
        pidDir = None
    Config.set('hadoop', 'YARN_PID_DIR', pidDir, overwrite=False)

    envList = ['HADOOP_EXAMPLES_JAR', 'HADOOP_TEST_JAR', 'HADOOP_STREAMING_JAR', 'GRIDMIX_JAR', 'HADOOP_RUMEN_JAR']
    searchStrList = [
        'hadoop-mapreduce-examples.jar', 'hadoop-mapreduce-client-jobclient-*-tests.jar', 'hadoop-streaming.jar',
        'hadoop-gridmix.jar', 'hadoop-rumen.jar'
    ]
    for env, searchStr in zip(envList, searchStrList):
        jarFound = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=mapredHome,
            searchstr=searchStr,
            passwd=Machine.getAdminPasswd()
        )
        jar = jarFound[0] if jarFound else None
        Config.set('hadoop', env, jar, overwrite=False)


#  configurations specific to hadoop 2 on windows
def setHadoop2WindowsConfigs():
    logger.info("Set configs for windows hadoop2")


def setHbaseConfigs():
    if Machine.type() == 'Linux':
        Config.set('hbase', 'HBASE_HOME', '/usr/hdp/current/hbase-client', overwrite=False)
        Config.set('hbase', 'HBASE_USER', 'hbase', overwrite=False)
    else:
        hbaseHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='hbase-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('hbase', 'HBASE_HOME', hbaseHome, overwrite=False)

    hbaseHome = Config.get('hbase', 'HBASE_HOME')
    Config.set('hbase', 'HBASE_CMD', os.path.join(hbaseHome, 'bin', 'hbase'), overwrite=False)
    Config.set('hbase', 'HBASE_CONF_DIR', os.path.join(hbaseHome, 'conf'), overwrite=False)
    Config.set('hbase', 'HBASE_LIB_DIR', os.path.join(hbaseHome, 'lib'), overwrite=False)


def setSliderConfigs():
    if Machine.type() == 'Linux':
        Config.set('slider', 'SLIDER_HOME', '/usr/hdp/current/slider-client', overwrite=False)
        #  Config.set('slider', 'SLIDER_HOME', '/usr/hdp/current/slider/slider', overwrite = False)
        Config.set('slider', 'SLIDER_USER', 'slider', overwrite=False)
    else:
        sliderHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='slider-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('slider', 'SLIDER_HOME', sliderHome, overwrite=False)

    sliderHome = Config.get('slider', 'SLIDER_HOME')
    Config.set('slider', 'SLIDER_CMD', os.path.join(sliderHome, 'bin', 'slider'), overwrite=False)
    Config.set('slider', 'SLIDER_CONF_DIR', os.path.join(sliderHome, 'conf'), overwrite=False)
    Config.set('slider', 'SLIDER_LIB_DIR', os.path.join(sliderHome, 'lib'), overwrite=False)


def setKafkaConfigs():
    if Machine.type() == 'Linux':
        kafka_home = get_first_existing_path('/usr/hdf/current/kafka-broker', '/usr/hdp/current/kafka-broker')
        if not kafka_home:
            logger.info("Kafka not installed on cluster")
            return
        Config.set('kafka', 'KAFKA_HOME', kafka_home, overwrite=False)
        Config.set('kafka', 'KAFKA_ADMIN', 'kafka', overwrite=False)
        Config.set('kafka', 'KAFKA_CONF', '/etc/kafka/conf', overwrite=False)


def setDruidConfigs():
    if Machine.type() == 'Linux':
        # druidUser = 'druid'
        Config.set('druid', 'DRUID_HOME', '/usr/hdp/current/druid-broker', overwrite=False)
        Config.set('druid', 'DRUID_ADMIN', 'druid', overwrite=False)
        Config.set('druid', 'DRUID_USER', 'druid', overwrite=False)
        Config.set('druid', 'DRUID_CONF_DIR', '/etc/druid/conf', overwrite=False)
        # hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR', default='/var/log/hadoop')
        # druidLogDir = os.path.join(os.path.dirname(hadoopLogDir), druidUser)


def setBeaconConfigs():
    if Machine.type() == 'Linux':
        Config.set('beacon', 'BEACON_HOME', '/usr/hdp/current/beacon', overwrite=False)
        Config.set('beacon', 'BEACON_USER', 'beacon', overwrite=False)
        Config.set('beacon', 'BEACON_CONF', '/etc/beacon/conf', overwrite=False)


def setHiveConfigs():
    if Machine.type() == 'Linux':
        hiveUser = 'hive'
        if Config.get('hive', 'HIVE2', 'False') != "True" or not os.path.exists('/usr/hdp/current/hive-server2-hive2'):
            Config.set('hive', 'HIVE_HOME', '/usr/hdp/current/hive-client', overwrite=False)
        else:
            Config.set('hive', 'HIVE_HOME', '/usr/hdp/current/hive-server2-hive2', overwrite=False)
        Config.set('hive', 'HIVE_USER', hiveUser, overwrite=False)
        #  get the Hadoop Log Dir
        hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR', default='/var/log/hadoop')
        hiveLogDir = os.path.join(os.path.dirname(hadoopLogDir), hiveUser)
        Config.set('hive', 'HIVE_LOG_DIR', hiveLogDir, overwrite=False)
        Config.set('hive', 'HIVE_TEST_GROUP', 'hivetest', overwrite=False)
        Config.set('hive', 'HIVE_GROUP_USER', 'hrt_1', overwrite=False)
        Config.set('hive', 'HIVE_OTHER_GROUP_USER', 'hrt_2', overwrite=False)
    else:
        hiveHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='hive-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('hive', 'HIVE_HOME', hiveHome, overwrite=False)
        hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR')
        hiveLogDir = os.path.join(hadoopLogDir, 'hive')
        Config.set('hive', 'HIVE_LOG_DIR', hiveLogDir, overwrite=False)
        Config.set('hive', 'HIVE_TEST_GROUP', 'hivetest', overwrite=False)
        Config.set('hive', 'HIVE_GROUP_USER', 'hrt_1', overwrite=False)
        Config.set('hive', 'HIVE_OTHER_GROUP_USER', 'hrt_2', overwrite=False)

    if Machine.type() == 'Linux':
        hiveHome = Config.get('hive', 'HIVE_HOME')
        # determine whether a server conf exists, and use it
        conf_dir = os.path.join(hiveHome, 'conf')
        if os.path.exists(hiveHome):
            hiveMetaUri = util.getPropertyValueFromConfigXMLFile(
                os.path.join('/usr/hdp/current/hive-client', 'conf', "hive-site.xml"),
                "hive.metastore.uris",
                defaultValue=""
            )
            m = re.findall("thrift://([^,]+):([0-9]+)", hiveMetaUri)
            if m:
                hiveHost = m[0][0]
                if (Config.get('hive', 'HIVE2', 'False') != "True"
                        or not os.path.exists('/usr/hdp/current/hive-server2-hive2')):
                    conf_server = '/etc/hive/conf.server'
                    conf_conf_server = '/etc/hive/conf/conf.server'
                    conf_dir = os.path.join(hiveHome, 'conf')
                    admin_user = Machine.getAdminUser()
                    if Machine.pathExists(admin_user, hiveHost, conf_server):
                        Config.set('hive', 'HIVE_SERVER_CONF_DIR', conf_server, overwrite=False)
                    elif Machine.pathExists(admin_user, hiveHost, conf_conf_server):
                        Config.set('hive', 'HIVE_SERVER_CONF_DIR', conf_conf_server, overwrite=False)
                else:
                    conf_server = '/etc/hive2/conf.server'
                    conf_conf_server = '/etc/hive2/conf/conf.server'
                    conf_dir = '/etc/hive2/conf'
                    # Assume that HSI host is gateway. Need to improve
                    if os.path.exists(conf_conf_server):
                        Config.set('hive', 'HIVE_SERVER_CONF_DIR', conf_conf_server, overwrite=False)
                    elif os.path.exists(conf_server):
                        Config.set('hive', 'HIVE_SERVER_CONF_DIR', conf_server, overwrite=False)

        Config.set('hive', 'HIVE_CONF_DIR', conf_dir, overwrite=False)
        Config.set('hive', 'HIVE_CMD', os.path.join(hiveHome, 'bin', 'hive'), overwrite=False)


def setSqoopConfigs():
    if Machine.type() == 'Linux':
        Config.set('sqoop', 'SQOOP_HOME', '/usr/hdp/current/sqoop-client', overwrite=False)
    else:
        sqoopHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='sqoop-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('sqoop', 'SQOOP_HOME', sqoopHome, overwrite=False)

    Config.set('sqoop', 'SQOOP_CMD', os.path.join(Config.get('sqoop', 'SQOOP_HOME'), 'bin', 'sqoop'), overwrite=False)


def setSqoop2Configs():
    if Machine.type() == 'Linux':
        Config.set('sqoop2', 'SQOOP2_HOME', '/usr/hdp/current/sqoop2', overwrite=False)
        Config.set('sqoop2', 'SQOOP2_USER', 'sqoop', overwrite=False)
        hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR', default='/var/log/hadoop')
        sqoop2LogDir = os.path.join(os.path.dirname(hadoopLogDir), 'sqoop2')
        Config.set('sqoop2', 'SQOOP2_LOG_DIR', sqoop2LogDir, overwrite=False)


#  else:
#    sqoopHome = Machine.find(
#        user=Machine.getAdminUser(),
#        host='',
#        filepath=WIN_BASE_PATH,
#        searchstr='sqoop-*',
#        passwd=Machine.getAdminPasswd()
#    )[0]
#    Config.set('sqoop2','SQOOP_HOME', sqoopHome, overwrite=False)
    Config.set(
        'sqoop2', 'SQOOP2_CMD', os.path.join(Config.get('sqoop2', 'SQOOP2_HOME'), 'bin', 'sqoop2'), overwrite=False
    )


def setFlumeConfigs():
    if Machine.type() == 'Linux':
        Config.set('flume-ng', 'FLUME_HOME', '/usr/hdp/current/flume-server', overwrite=False)
    else:
        flumeHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='apache-flume-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('flume-ng', 'FLUME_HOME', flumeHome, overwrite=False)

    flumeHome = Config.get('flume-ng', 'FLUME_HOME')
    Config.set('flume-ng', 'FLUME_CONF', os.path.join(flumeHome, 'conf'), overwrite=False)
    Config.set('flume-ng', 'FLUME_CMD', os.path.join(flumeHome, 'bin', 'flume-ng'), overwrite=False)


def setMahoutConfigs():
    if Machine.type() == 'Linux':
        Config.set('mahout', 'MAHOUT_HOME', '/usr/hdp/current/mahout-client', overwrite=False)
    else:
        mahoutHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='mahout-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('mahout', 'MAHOUT_HOME', mahoutHome, overwrite=False)

    Config.set(
        'mahout', 'MAHOUT_CMD', os.path.join(Config.get('mahout', 'MAHOUT_HOME'), 'bin', 'mahout'), overwrite=False
    )


def setOozieConfigs():
    if Machine.type() == 'Linux':
        OOZIE_HOME = '/usr/hdp/current/oozie-client'
        Config.set('oozie', 'OOZIE_HOME', OOZIE_HOME, overwrite=False)
        Config.set('oozie', 'OOZIE_USER', 'oozie', overwrite=False)
        try:
            oozieExamples = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('oozie', 'OOZIE_HOME'),
                searchstr='oozie-examples.tar.gz',
                passwd=Machine.getAdminPasswd()
            )[0]
            Config.set('oozie', 'OOZIE_EXAMPLES_LOCATION', oozieExamples, overwrite=False)
        except Exception:
            pass
    else:
        oozieBase = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='oozie-*-incubating',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('oozie', 'OOZIE_BASE', oozieBase, overwrite=False)
        oozieHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=oozieBase,
            searchstr='oozie-*-incubating',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('oozie', 'OOZIE_HOME', oozieHome, overwrite=False)
        Config.set(
            'oozie',
            'OOZIE_EXAMPLES_LOCATION',
            os.path.join(Config.get('oozie', 'OOZIE_HOME'), 'examples'),
            overwrite=False
        )

    Config.set('oozie', 'OOZIE_CONF_DIR', os.path.join(Config.get('oozie', 'OOZIE_HOME'), 'conf'), overwrite=False)
    Config.set('oozie', 'OOZIE_PORT', '11000', overwrite=False)
    if Machine.type() == 'Linux':
        try:
            from beaver.component.oozie import Oozie  # pylint: disable=unused-variable
            Config.set(
                'oozie',
                'OOZIE_LOG_DIR',
                util.getPropertyValueFromBashFile(
                    os.path.join(Config.get('oozie', 'OOZIE_CONF_DIR'), 'oozie-env.sh'), 'OOZIE_LOG'
                ),
                overwrite=False
            )
        except Exception:
            Config.set('oozie', 'OOZIE_LOG_DIR', '')


def setFirefoxConfigs():
    if Machine.type() == 'Linux':
        Config.set('firefox', 'FIREFOX_PATH', '/usr/bin/firefox', overwrite=False)
        Config.set(
            'firefox',
            'PROFILE_FOLDER',
            os.path.join('/', 'home', Config.getEnv('USER'), '.mozilla', 'firefox'),
            overwrite=False
        )
        Config.set('firefox', 'FIREFOX_DISPLAY', '25', overwrite=False)


def setZookeeperConfigs():
    if Machine.type() == 'Linux':
        zk_home = get_first_existing_path('/usr/hdf/current/zookeeper-client', '/usr/hdp/current/zookeeper-client')
        if not zk_home:
            logger.info("Zookeeper not installed on cluster")
            return
        Config.set('zookeeper', 'ZK_HOME', zk_home, overwrite=False)
        Config.set('zookeeper', 'ZOOKEEPER_USER', 'zookeeper', overwrite=False)
    else:
        zkHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='zookeeper-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('zookeeper', 'ZK_HOME', zkHome, overwrite=False)
        Config.set('zookeeper', 'ZOOKEEPER_NETCAT_HOME', 'C:\\Windows\\nc111nt\\nc.exe', overwrite=False)

    Config.set(
        'zookeeper', 'ZOOKEEPER_CONF_DIR', os.path.join(Config.get('zookeeper', 'ZK_HOME'), 'conf'), overwrite=False
    )


def setPigConfigs():
    if Machine.type() == 'Linux':
        Config.set('pig', 'PIG_HOME', '/usr/hdp/current/pig-client', overwrite=False)
    else:
        pigHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='pig-*-SNAPSHOT',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('pig', 'PIG_HOME', pigHome, overwrite=False)

    pigHome = Config.get('pig', 'PIG_HOME')
    Config.set('pig', 'PIG_CONF_DIR', os.path.join(pigHome, 'conf'), overwrite=False)
    Config.set('pig', 'PIG_CMD', os.path.join(pigHome, 'bin', 'pig'), overwrite=False)
    piggybankjar = os.path.join(pigHome, "piggybank.jar")
    if not os.path.exists(piggybankjar):
        piggybankjar = os.path.join(pigHome, "lib", "piggybank.jar")
    Config.set('pig', 'PIGGYBANK_JAR', piggybankjar, overwrite=False)


def setHcatalogConfigs():
    if Machine.type() == 'Linux':
        Config.set('hcatalog', 'HCATALOG_HOME', '/usr/hdp/current/hive-webhcat', overwrite=False)
        Config.set('hcatalog', 'HCATALOG_USER', 'hcat', overwrite=False)
    else:
        hcatHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='hcatalog-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('hcatalog', 'HCATALOG_HOME', hcatHome, overwrite=False)

    Config.set(
        'hcatalog',
        'HCATALOG_CONF',
        os.path.join(Config.get('hcatalog', 'HCATALOG_HOME'), 'etc', 'webhcat'),
        overwrite=False
    )


def setMachineConfigs():
    if Machine.type() == 'Linux':
        homeDir = os.path.expanduser("~")
        keytabsDir = os.path.join(homeDir, 'hadoopqa', 'keytabs')
        Config.set('machine', 'MYSQL_HOST', Machine.getfqdn(), overwrite=False)
        Config.set('machine', 'KEYTAB_FILES_DIR', keytabsDir, overwrite=False)
        Config.set('machine', 'MYSQL_ROOT_USER', 'root', overwrite=False)
        Config.set('machine', 'MYSQL_ROOT_PASSWD', '', overwrite=False)
        Config.set('machine', 'DB2_HOST', Machine.getfqdn(), overwrite=False)
        Config.set('machine', 'DB2_ROOT_USER', 'db2inst1', overwrite=False)
        Config.set('machine', 'DB2_ROOT_PASSWD', 'db2inst1', overwrite=False)
        Config.set('machine', 'PLATFORM', 'hdfs', overwrite=False)
        Config.set(
            'machine',
            'ADDITIONAL_TEST_USERS',
            'hrt_1,hrt_2,hrt_3,hrt_4,hrt_5,hrt_6,hrt_7,hrt_8,hrt_9',
            overwrite=False
        )


def setTempletonConfigs():
    #  we only need to do this for windows
    if Machine.type() != 'Linux':
        templetonHome = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=WIN_BASE_PATH,
            searchstr='templeton-*',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('templeton', 'TEMPLETON_HOME', templetonHome, overwrite=False)
        Config.set(
            'templeton',
            'TEMPLETON_CONF',
            os.path.join(Config.get('templeton', 'TEMPLETON_HOME'), 'conf'),
            overwrite=False
        )

    else:
        templetonServiceDir = "webhcat"  #Linux Flubber log is at /grid/0/var/log/webhcat
        templetonHost = Machine.getfqdn()
        Config.set('templeton', 'TEMPLETON_CONF', '/etc/hive-webhcat/conf', overwrite=False)
        templetonPort = None
        hiveSiteXml = "/etc/hive/conf/hive-site.xml"
        if os.path.exists(hiveSiteXml):
            hiveMetaUri = util.getPropertyValueFromConfigXMLFile(hiveSiteXml, "hive.metastore.uris", defaultValue="")
            m = re.findall("thrift://([^,]+):([0-9]+)", hiveMetaUri)
            if m:
                templetonHost = m[0][0]  # Typically Webhcat and Hive Metastore are on same host,
                #      in a HA cluster assuming it is the first one
                localWebhcatSiteXml = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'webhcat-site.xml')
                remoteWebhcatSiteXml = "/etc/hive-webhcat/conf/webhcat-site.xml"
                Machine.copyToLocal(Machine.getAdminUser(), templetonHost, remoteWebhcatSiteXml, localWebhcatSiteXml)
                Machine.chmod('777', localWebhcatSiteXml, user=Machine.getAdminUser())
                if os.path.exists(localWebhcatSiteXml):
                    templetonPort = util.getPropertyValueFromConfigXMLFile(localWebhcatSiteXml, "templeton.port")
                    Config.set('templeton', 'TEMPLETON_PORT', templetonPort, overwrite=False)
        if not templetonPort:
            if Config.get('machine', 'PLATFORM', 'hdfs') == 'ASV':
                Config.set('templeton', 'TEMPLETON_PORT', '30111', overwrite=False)
            else:
                Config.set('templeton', 'TEMPLETON_PORT', '50111', overwrite=False)
        Config.set('templeton', 'TEMPLETON_HOST', templetonHost, overwrite=False)
        # Set templeton log dir from based on Hadoop Log Dir
        hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR', default='/var/log/hadoop')
        templetonLogDir = os.path.join(os.path.dirname(hadoopLogDir), templetonServiceDir)
        Config.set('templeton', 'TEMPLETON_LOG_DIR', templetonLogDir, overwrite=False)


def setAccumuloConfigs():
    if Machine.type() == 'Linux':
        accumuloUser = 'accumulo'
        Config.set('accumulo', 'ACCUMULO_HOME', '/usr/hdp/current/accumulo-client', overwrite=False)
        Config.set('accumulo', 'ACCUMULO_CONF_DIR', '/etc/accumulo/conf', overwrite=False)
        Config.set(
            'accumulo',
            'ACCUMULO_TEST',
            os.path.join(Config.get('accumulo', 'ACCUMULO_HOME'), 'test'),
            overwrite=False
        )
        Config.set(
            'accumulo',
            'ACCUMULO_CMD',
            os.path.join(Config.get('accumulo', 'ACCUMULO_HOME'), 'bin', 'accumulo'),
            overwrite=False
        )
        Config.set('accumulo', 'ACCUMULO_USER', 'accumulo', overwrite=False)
        hadoopLogDir = Config.get('hadoop', 'HADOOP_LOG_DIR', default='/var/log/hadoop')
        accumuloLogDir = os.path.join(os.path.dirname(hadoopLogDir), accumuloUser)
        Config.set('accumulo', 'ACCUMULO_LOG_DIR', accumuloLogDir, overwrite=False)
    else:
        pass


def setTezConfigs():
    from beaver.component.hadoop import YARN
    HDP_VERSION = Ambari.getHDPVersion()[6:]
    if Machine.type() == 'Linux':
        #Config.set('tez', 'TEZ_CONF_DIR', os.path.join(Config.get('tez', 'TEZ_HOME'),'conf') , overwrite = False)
        if not YARN.check_if_component_is_tez_v2():
            Config.set('tez', 'TEZ_HOME', '/usr/hdp/current/tez-client', overwrite=False)
            Config.set('tez', 'TEZ_CONF_DIR', '/etc/tez/conf', overwrite=False)
        if YARN.check_if_component_is_tez_v2():
            Config.set('tez', 'TEZ_CONF_DIR', '/etc/tez_hive2/conf', overwrite=False)
            Config.set('tez', 'TEZ_HOME', '/usr/hdp/%s/tez_hive2' % HDP_VERSION, overwrite=False)
        try:
            examplesJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('tez', 'TEZ_HOME'),
                searchstr='tez-tests-*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]
            Config.set('tez', 'TEZ_EXAMPLES_JAR', examplesJar, overwrite=False)
            examplesActualJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('tez', 'TEZ_HOME'),
                searchstr='tez-examples-*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]
            Config.set('tez', 'TEZ_ACTUAL_EXAMPLES_JAR', examplesActualJar, overwrite=False)
        except Exception:
            pass
    else:
        pass


def setStormConfigs():
    if Machine.type() == 'Linux':
        storm_home = get_first_existing_path('/usr/hdf/current/storm-client', '/usr/hdp/current/storm-client')
        # QE-14606: if storm is not installed skip it.
        if not storm_home:
            logger.info("Storm not installed on system.")
            return
        Config.set('storm', 'STORM_HOME', storm_home, overwrite=False)
        Config.set('storm', 'STORM_SLIDER_CLIENT_HOME', '/usr/hdp/current/storm-slider-client', overwrite=False)
        Config.set(
            'storm', 'STORM_CMD', os.path.join(Config.get('storm', 'STORM_HOME'), 'bin', 'storm'), overwrite=False
        )
        Config.set('storm', 'STORM_USER', 'storm', overwrite=False)
        Config.set('storm', 'STORM_CONF', '/etc/storm/conf', overwrite=False)
        try:
            from beaver.component.storm import Storm
            Config.set('storm', 'STORM_LOG_DIR', Storm.getLogDir(logoutput=True), overwrite=False)
        except Exception:
            Config.set('storm', 'STORM_LOG_DIR', '/var/log/storm')
        try:
            stormStarterJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('storm', 'STORM_HOME'),
                searchstr='storm-starter-topologies-*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]

            Config.set('storm', 'STORM_STARTER_JAR', stormStarterJar, overwrite=False)
            kafkaExampleJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('storm', 'STORM_HOME'),
                searchstr='storm-kafka-example*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]
            Config.set('storm', 'STORM_KAFKA_EXAMPLE_JAR', kafkaExampleJar, overwrite=False)
        except Exception:
            pass
    else:
        Config.set('storm', 'STORM_USER', 'hadoop', overwrite=False)


def get_first_existing_path(*paths):
    for one_path in paths:
        if os.path.exists(one_path):
            return one_path
    return None


def setHueConfigs():
    if Machine.type() == 'Linux':
        Config.set('hue', 'HUE_HOME', '/usr/lib/hue', overwrite=False)
        Config.set('hue', 'HUE_CONF', '/etc/hue/conf', overwrite=False)
        Config.set('hue', 'HUE_BUILD_BIN', '/usr/lib/hue/build/env/bin', overwrite=False)
        Config.set('hue', 'HUE_USER', 'hue', overwrite=False)

        Config.set('hue', 'HUE_ORCL_USER', 'hue', overwrite=False)
        Config.set('hue', 'HUE_ORCL_PSWD', 'huepassword', overwrite=False)
        Config.set('hue', 'ORCL_ENGINE', 'oracle', overwrite=False)
        Config.set('hue', 'ORCL_USER', 'sys', overwrite=False)
        Config.set('hue', 'ORCL_PSWD', 'root', overwrite=False)
        Config.set('hue', 'ORCL_ROLE', 'sysdba', overwrite=False)
        Config.set('hue', 'ORCL_PORT', '1521', overwrite=False)
        Config.set('hue', 'ORCL_SID', 'XE', overwrite=False)

    else:
        pass


def setPhoenixConfigs():
    if Machine.type() == 'Linux':
        Config.set('phoenix', 'PHOENIX_HOME', '/usr/hdp/current/phoenix-client', overwrite=False)
        Config.set(
            'phoenix', 'PHOENIX_LIB_DIR', os.path.join(Config.get('phoenix', 'PHOENIX_HOME'), 'lib'), overwrite=False
        )
    else:
        pass


def setKnoxConfigs():
    if Machine.type() == 'Linux':
        Config.set('knox', 'KNOX_HOME', '/usr/hdp/current/knox-server', overwrite=False)
        Config.set('knox', 'KNOX_CONF', '/etc/knox/conf', overwrite=False)
        Config.set('knox', 'KNOX_LOG', '/var/log/knox', overwrite=False)
        Config.set('knox', 'KNOX_PID', '/var/run/knox', overwrite=False)
        Config.set('knox', 'KNOX_USER', 'knox', overwrite=False)
        Config.set(
            'knox',
            'KNOX_KEYSTORE_DIR',
            os.path.join(Config.get('knox', 'KNOX_HOME'), 'data/security/keystores'),
            overwrite=False
        )

        # Machine.getTempDir() returns empty hence hardcoding
        Config.set('knox', 'KNOX_LB_CERT', os.path.join('/tmp/knoxPem'), overwrite=False)

        is_ha_enabled = Config.get('machine', 'IS_HA_TEST')
        if is_ha_enabled == "yes":
            cluster1_knoxHosts, cluster2_knoxHosts, cluster1_LB_Host, cluster2_LB_Host = Ambari.getKnoxHosts()
        else:
            cluster1_knoxHosts, cluster2_knoxHosts = Ambari.getKnoxHosts()

        if cluster1_knoxHosts:
            Config.set('knox', 'CLUSTER1_KNOX_HOST', cluster1_knoxHosts, overwrite=False)

            #[QE-18467]Fetch only the host list from the knoxHost:port comma separated string
            knoxhost_list = ""
            knoxhost_port_list = cluster1_knoxHosts.split(",")
            for knoxhost_port in knoxhost_port_list:
                knoxhost = knoxhost_port.split(":")
                if knoxhost_list != "":
                    knoxhost_list += "," + knoxhost[0]
                else:
                    knoxhost_list += knoxhost[0]
            Config.set('knox', 'KNOX_HOST', knoxhost_list, overwrite=False)

            if is_ha_enabled == "yes":
                Config.set('knox', 'CLUSTER1_LOADBALANCER_HOST', cluster1_LB_Host, overwrite=False)
        else:
            #If knox is not installed, setting KNOX_HOST to gateway_host to maintain backward compatability
            Config.set('knox', 'KNOX_HOST', Machine.getfqdn(), overwrite=False)

        if cluster2_knoxHosts:
            Config.set('knox', 'CLUSTER2_KNOX_HOST', cluster2_knoxHosts, overwrite=False)
            if is_ha_enabled == "yes":
                Config.set('knox', 'CLUSTER2_LOADBALANCER_HOST', cluster2_LB_Host, overwrite=False)
    else:
        pass


def setFalconConfigs():
    try:
        oozie_sharelib_path = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=Config.get('oozie', 'OOZIE_HOME'),
            searchstr='oozie-sharelib-oozie.*.jar$',
            passwd=Machine.getAdminPasswd()
        )[0]
        Config.set('falcon', 'OOZIE_SHARELIB_PATH', oozie_sharelib_path, overwrite=False)
    except Exception:
        pass

    Config.set('falcon', 'FALCON_USER', "falcon", overwrite=False)

    if os.path.isdir("/usr/hdp/current/falcon-distributed"):
        Config.set('falcon', 'FALCON_HOME', "/usr/hdp/current/falcon-distributed", overwrite=False)
    if os.path.isdir("/usr/hdp/current/falcon-server"):
        Config.set('falcon', 'FALCON_HOME', "/usr/hdp/current/falcon-server", overwrite=False)


def setSparkConfigs():
    if Machine.type() == 'Linux':
        try:
            Config.set('spark', 'SPARK_HOME', '/usr/hdp/current/spark-client', overwrite=False)
            Config.set('spark', 'SPARK_CONF', '/etc/spark/conf', overwrite=False)
            Config.set('spark', 'SPARK2_HOME', '/usr/hdp/current/spark2-client', overwrite=False)
            Config.set('spark', 'SPARK2_CONF', '/etc/spark2/conf', overwrite=False)
            try:
                SparkExampleJar = Machine.find(
                    user=Machine.getAdminUser(),
                    host='',
                    filepath=os.path.join(Config.get('spark', 'SPARK_HOME'), "lib"),
                    searchstr='spark-examples-*.jar',
                    passwd=Machine.getAdminPasswd()
                )[0]
                Config.set('spark', 'SPARK_EXAMPLE_JAR', SparkExampleJar, overwrite=False)
            except Exception:
                pass
            try:
                SparkAssemblyJar = Machine.find(
                    user=Machine.getAdminUser(),
                    host='',
                    filepath=os.path.join(Config.get('spark', 'SPARK_HOME'), "lib"),
                    searchstr='spark-assembly*.jar',
                    passwd=Machine.getAdminPasswd()
                )[0]
                Config.set('spark', 'SPARK_ASSEMBLY_JAR', SparkAssemblyJar, overwrite=False)
            except Exception:
                pass
            Config.set('spark', 'SPARK_USER', 'spark', overwrite=False)
            #if os.path.exists('/usr/hdp/current/hive-server2-hive2'):
            Config.set('spark', 'SPARK_LLAP_HOME', '/usr/hdp/current/hive_warehouse_connector', overwrite=False)
            try:
                SparkLLAPJar = Machine.find(
                    user=Machine.getAdminUser(),
                    host='',
                    filepath=Config.get('spark', 'SPARK_LLAP_HOME'),
                    searchstr='hive-warehouse-connector\\*.jar',
                    passwd=Machine.getAdminPasswd()
                )[0]
                Config.set('spark', 'SPARK_LLAP_JAR', SparkLLAPJar, overwrite=False)
                SparkLLAPZip = Machine.find(
                    user=Machine.getAdminUser(),
                    host='',
                    filepath=Config.get('spark', 'SPARK_LLAP_HOME'),
                    searchstr='pyspark_hwc\\*.zip',
                    passwd=Machine.getAdminPasswd()
                )[0]
                Config.set('spark', 'SPARK_LLAP_PY_ZIP', SparkLLAPZip, overwrite=False)
            except Exception:
                pass
        except Exception:
            pass
    else:
        pass


def setZeppelinConfigs():
    if Machine.type() == 'Linux':
        try:
            Config.set('zeppelin', 'ZEPPELIN_HOME', '/usr/hdp/current/zeppelin-server', overwrite=False)
            Config.set('zeppelin', 'ZEPPELIN_CONF', '/etc/zeppelin/conf', overwrite=False)
            Config.set('zeppelin', 'ZEPPELIN_USER', 'zeppelin', overwrite=False)
            Config.set('zeppelin', 'ZEPPELIN_LOGDIR', '/var/log/zeppelin', overwrite=False)
        except Exception:
            pass
    else:
        pass


def setLivyConfigs():
    if Machine.type() == 'Linux':
        try:
            Config.set('livy', 'LIVY_HOME', '/usr/hdp/current/livy-server', overwrite=False)
            Config.set('livy', 'LIVY_CONF', '/etc/livy/conf', overwrite=False)
            Config.set('livy', 'LIVY_USER', 'livy', overwrite=False)
            Config.set('livy', 'LIVY_LOGDIR', '/var/log/livy', overwrite=False)
            Config.set('livy', 'LIVY2_HOME', '/usr/hdp/current/livy2-client/', overwrite=False)
            Config.set('livy', 'LIVY2_CONF', '/etc/livy2/conf/', overwrite=False)
            Config.set('livy', 'LIVY2_LOGDIR', '/var/log/livy2', overwrite=False)
        except Exception:
            pass
    else:
        pass


def setXAConfigs():
    if Machine.type() == 'Linux':
        Config.set('xasecure', 'XA_ADMIN_HOST', Machine.getfqdn(), overwrite=False)
        Config.set('xasecure', 'XA_DB_HOST', Machine.getfqdn(), overwrite=False)
        Config.set('xasecure', 'XA_ADMIN_HOME', '/usr/hdp/current/ranger-admin', overwrite=False)
    else:
        pass


def setKMSConfigs():
    if Machine.type() == 'Linux':
        Config.set('kms', 'KMS_HOME', '/usr/hdp/current/ranger-kms', overwrite=False)
        Config.set('kms', 'KMS_CONF', '/etc/ranger/kms/conf', overwrite=False)
    else:
        pass


def setCloudbreakConfigs():
    Config.set(
        'cloudbreak',
        'CLOUDBREAK_ITEST_MAVEN_REPO',
        'https://s3-eu-west-1.amazonaws.com/maven.sequenceiq.com/releases/com/sequenceiq/cloudbreak-integration-test',
        overwrite=False
    )


def setBigsqlConfigs():
    if Machine.type() == 'Linux':
        Config.set('bigsql', 'BIGSQL_HOME', '/usr/ibmpacks/current/bigsql/bigsql', overwrite=False)
        Config.set('bigsql', 'BIGSQL_USER', 'bigsql', overwrite=False)
        Config.set('bigsql', 'BIGSQL_CONF_DIR', '/usr/ibmpacks/current/bigsql/bigsql/conf', overwrite=False)
