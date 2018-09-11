import json
import logging
import os
import platform
import re
import socket
import sys
import traceback
from ConfigParser import ConfigParser

from beaver import util
from beaver.config import Config
from beaver.machine import Machine
from beaver.reportHelper import ReportHelper

SECTION = "HW-QE-PUBLISH-REPORT"
COMPONENT_VERSION_MAP = {
    'Hadoop': 'beaver.component.hadoop.Hadoop',
    'HBase': 'beaver.component.hbase.HBase',
    'HBase_1': 'beaver.component.hbase.HBase',
    'HBase_2': 'beaver.component.hbase.HBase',
    'HBase_3': 'beaver.component.hbase.HBase',
    'Pig': 'beaver.component.pig.Pig',
    'Pig_1': 'beaver.component.pig.Pig',
    'Pig_2': 'beaver.component.pig.Pig',
    'Oozie': 'beaver.component.oozie.Oozie',
    'Oozie_1': 'beaver.component.oozie.Oozie',
    'Oozie_2': 'beaver.component.oozie.Oozie',
    'Mahout': 'beaver.component.mahout.Mahout',
    'Hive': 'beaver.component.hive.Hive',
    'HiveServer2': 'beaver.component.hive.Hive',
    'Hive_1': 'beaver.component.hive.Hive',
    'Hive_2': 'beaver.component.hive.Hive',
    'Hive_3': 'beaver.component.hive.Hive',
    'HiveServer2_1': 'beaver.component.hive.Hive',
    'HiveServer2_2': 'beaver.component.hive.Hive',
    'HiveServer2_3': 'beaver.component.hive.Hive',
    'HiveServer2Concurr': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLDAP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLDAPHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrSSL': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrSSLHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrTPUser': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLongRunning': 'beaver.component.hive.Hive',
    'HSIHiveServer2': 'beaver.component.hive.Hive',
    'HSIHiveServer2Concurr': 'beaver.component.hive.Hive',
    'HSIHiveServer2_chaosMonkey': 'beaver.component.hive.Hive',
    'Hcatalog': 'beaver.component.hcatalog.Hcatalog',
    'WebHcat': 'beaver.component.hcatalog.Hcatalog',
    'Druid': 'beaver.component.druid.Druid',
    'Sqoop': 'beaver.component.sqoop.Sqoop',
    'Zookeeper': 'beaver.component.zookeeper.Zookeeper',
    'SqoopDb2': 'beaver.component.sqoop.Sqoop',
    'FlumeNG': 'beaver.component.flume.FlumeNG',
    'Hadoop-HA': 'beaver.component.hadoop.Hadoop',
    'Pig-HA': 'beaver.component.pig.Pig',
    'HBase-HA': 'beaver.component.hbase.HBase',
    'Oozie-HA': 'beaver.component.oozie.Oozie',
    'Hive-HA': 'beaver.component.hive.Hive',
    'Hive-HA_1': 'beaver.component.hive.Hive',
    'Hive-HA_2': 'beaver.component.hive.Hive',
    'Tez_1': 'beaver.component.tez.Tez',
    'Tez_2': 'beaver.component.tez.Tez',
    'Tez_3': 'beaver.component.tez.Tez',
    'Tez': 'beaver.component.tez.Tez',
    'Tez_v15_1': 'beaver.component.tez.Tez',
    'Tez_v15_2': 'beaver.component.tez.Tez',
    'Tez_v15_3': 'beaver.component.tez.Tez',
    'Tez_v15': 'beaver.component.tez.Tez',
    'Tez_v2': 'beaver.component.tez.Tez',
    'Sqoop2': 'beaver.component.sqoop2.Sqoop2',
    'Hdfs': 'beaver.component.hadoop.Hadoop',
    'Yarn': 'beaver.component.hadoop.Hadoop',
    'Mapreduce': 'beaver.component.hadoop.Hadoop',
    'Storm': 'beaver.component.storm.Storm',
    'Storm-HA': 'beaver.component.storm.Storm',
    'Phoenix': 'beaver.component.phoenix.Phoenix',
    'Phoenix-Slider': 'beaver.component.phoenix.Phoenix',
    'Phoenix-QueryServer': 'beaver.component.phoenix.Phoenix',
    'Phoenix-qs-Concurr': 'beaver.component.phoenix.Phoenix',
    'Phoenix-qs-ha': 'beaver.component.phoenix.Phoenix',
    'HadoopLongRunning': 'beaver.component.hadoop.Hadoop',
    'Knox': 'beaver.component.knox.Knox',
    'OpenStack': 'beaver.component.sahara.Sahara',
    'Knox-HA': 'beaver.component.knox.Knox',
    'Falcon': 'beaver.component.falcon.Falcon',
    'Falcon-MultiCluster': 'beaver.component.falcon.Falcon',
    'Falcon-HA': 'beaver.component.falcon.Falcon',
    'Falcon-MultiCluster-HA': 'beaver.component.falcon.Falcon',
    'Accumulo': 'beaver.component.accumulo.Accumulo',
    'Accumulo_1': 'beaver.component.accumulo.Accumulo',
    'Accumulo_2': 'beaver.component.accumulo.Accumulo',
    'Accumulo_3': 'beaver.component.accumulo.Accumulo',
    'Ambari': 'beaver.component.ambari.Ambari',
    'Xa': 'beaver.component.xa.Xa',
    'Hdfs-HA': 'beaver.component.hadoop.Hadoop',
    'Yarn-HA': 'beaver.component.hadoop.Hadoop',
    'Accumulo-Slider': 'beaver.component.accumulo.Accumulo',
    'Yarn_1': 'beaver.component.hadoop.Hadoop',
    'Yarn_2': 'beaver.component.hadoop.Hadoop',
    'Yarn_3': 'beaver.component.hadoop.Hadoop',
    'Yarn_4': 'beaver.component.hadoop.Hadoop',
    'Yarn_5': 'beaver.component.hadoop.Hadoop',
    'Yarn_6': 'beaver.component.hadoop.Hadoop',
    'Hdfs_1': 'beaver.component.hadoop.Hadoop',
    'Hdfs_2': 'beaver.component.hadoop.Hadoop',
    'Data-Connectors': 'beaver.component.hadoop.Hadoop',
    'HBase-Slider': 'beaver.component.hbase.HBase',
    'HBase-Slider_1': 'beaver.component.hbase.HBase',
    'HBase-Slider_2': 'beaver.component.hbase.HBase',
    'HBase-Slider_3': 'beaver.component.hbase.HBase',
    'Storm-Slider': 'beaver.component.storm.Storm',
    'Slider': 'beaver.component.slider.Slider',
    'Spark': 'beaver.component.spark.Spark',
    'SparkHive': 'beaver.component.spark.Spark',
    'Zeppelin': 'beaver.component.zeppelin.ZeppelinServer',
    'Spark2': 'beaver.component.spark.Spark',
    'Spark2Hive': 'beaver.component.spark.Spark',
    'RollingUpgrade': 'beaver.component.hadoop.Hadoop',
    'Mapreduce_1': 'beaver.component.hadoop.Hadoop',
    'Mapreduce_2': 'beaver.component.hadoop.Hadoop',
    'RollingUpgrade-HA': 'beaver.component.hadoop.Hadoop',
    'Slider-HA': 'beaver.component.slider.Slider',
    'Rollback': 'beaver.component.hadoop.Hadoop',
    'Rollback-HA': 'beaver.component.hadoop.Hadoop',
    'Kafka': 'beaver.component.kafka.Kafka',
    'Hue': 'beaver.component.hue.Hue',
    'atlas': 'beaver.component.atlas.Atlas',
    'atlas_hs_interactive': 'beaver.component.atlas.Atlas',
    'atlas_ui': 'beaver.component.atlas.Atlas',
    'SmartSense': 'beaver.component.smartsense.SmartSense',
    'NiFi': 'beaver.component.nifi.Nifi',
    'Metron': 'beaver.component.metron.Metron',
    'Streamline': 'beaver.component.streamline.Streamline',
    'XaAgents': 'beaver.component.xa.Xa',
    'XaAgents-Multicluster': 'beaver.component.xa.Xa',
    'XaAgents-Hive2': 'beaver.component.xa.Xa',
    'XaAgents-HA': 'beaver.component.xa.Xa',
    'xaagents-knoxsso': 'beaver.component.xa.Xa',
    'Beacon': 'beaver.component.beacon.beacon.Admin',
    'DPCore-UI': 'beaver.component.beacon.beacon.Admin',
    'DLM-UI': 'beaver.component.beacon.beacon.Admin',
    'BeaconHive2': 'beaver.component.beacon.beacon.Admin',
    'BigSql': 'beaver.component.bigsql.BigSQL'
}

COMPONENT_DATABASE_MAP = {
    'Oozie': 'beaver.component.oozie.Oozie',
    'Hive': 'beaver.component.hive.Hive',
    'HiveServer2': 'beaver.component.hive.Hive',
    'Hive_1': 'beaver.component.hive.Hive',
    'Hive_2': 'beaver.component.hive.Hive',
    'Hive_3': 'beaver.component.hive.Hive',
    'HiveServer2_1': 'beaver.component.hive.Hive',
    'HiveServer2_2': 'beaver.component.hive.Hive',
    'HiveServer2_3': 'beaver.component.hive.Hive',
    'HiveServer2Concurr': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLDAP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLDAPHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrSSL': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrSSLHTTP': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrTPUser': 'beaver.component.hive.Hive',
    'HiveServer2ConcurrLongRunning': 'beaver.component.hive.Hive',
    'Hcatalog': 'beaver.component.hive.Hive',
    'WebHcat': 'beaver.component.hive.Hive',
    'Oozie-HA': 'beaver.component.oozie.Oozie',
    'Hive-HA': 'beaver.component.hive.Hive',
    'Hive-HA_1': 'beaver.component.hive.Hive',
    'Hive-HA_2': 'beaver.component.hive.Hive',
    'Sqoop': 'beaver.component.sqoop.Sqoop',
    'Sqoop2': 'beaver.component.sqoop2.Sqoop2',
    'SqoopDb2': 'beaver.component.sqoop.Sqoop',
    'FlumeNG': 'beaver.component.flume.FlumeNG',
    'Druid': 'beaver.component.druid.Druid',
    'ambari-installer': 'beaver.component.ambari.Ambari',
    'ambari-monitoring1': 'beaver.component.ambari.Ambari',
    'ambari-monitoring2': 'beaver.component.ambari.Ambari',
    'ambari-monitoring3': 'beaver.component.ambari.Ambari',
    'ambari-core': 'beaver.component.ambari.Ambari',
    'ambari-services': 'beaver.component.ambari.Ambari',
    'ambari-hosts': 'beaver.component.ambari.Ambari',
    'ambari-metric-monit': 'beaver.component.ambari.Ambari',
    'ambari-roll-maint': 'beaver.component.ambari.Ambari',
    'ambari-nf-core': 'beaver.component.ambari.Ambari',
    'ambari-nf-restarting': 'beaver.component.ambari.Ambari',
    'ambari-monitoring-newfeature': 'beaver.component.ambari.Ambari',
    'ambari-heavyweight1': 'beaver.component.ambari.Ambari',
    'ambari-heavyweight2': 'beaver.component.ambari.Ambari',
    'Streamline': 'beaver.component.streamline.Streamline',
    'xa-agents': 'beaver.component.xa.Xa',
    'xa-policymgr': 'beaver.component.xa.Xa',
    'Beacon': 'beaver.component.beacon.beacon.Beacon',
    'BeaconHive2': 'beaver.component.beacon.beacon.Beacon',
    'Hue': 'beaver.component.hue.Hue',
    'BigSql': 'beaver.component.bigsql.BigSQL'
}

#List of all the AD HOSTS
AD_HOSTS_LIST = ['ad-nano.qe.hortonworks.com', 'ad-ec2.qe.hortonworks.com']
logger = logging.getLogger(__name__)


def generateTestReportConf(infile, outfile, results, startTime, endTime):
    config = ConfigParser()
    config.optionxform = str
    config.read(infile)

    if config.has_section(SECTION):
        # set the version to 2.0 so new keys are processed
        config.set(SECTION, 'report_version', '2.0')
        # Stores the original component value, some testsuite runs like HiveServer2Concurr, Sqoop
        # change this for reporting, but we need to preserve for log archiving for uniqueness
        config.set(SECTION, "TESTSUITE_COMPONENT", config.get(SECTION, "COMPONENT"))
        for option, value in config.items(SECTION):
            try:
                if ((option != "SECURE" and value != "")
                        or (Config.getEnv("HDP_STACK_INSTALLED").lower() == "false" and value != "")):
                    continue
                elif option == "BUILD_ID":
                    # if REPO_URL is not set, set the BUILD_ID to 0
                    # otherwise get the BUILD_ID from the file
                    if config.get(SECTION, "REPO_URL") == "" or not config.has_option(SECTION, "REPO_URL"):
                        config.set(SECTION, option, 0)
                    else:
                        config.set(SECTION, option, getBuildId(config.get(SECTION, "REPO_URL")))
                elif option == "HOSTNAME":
                    config.set(SECTION, option, socket.getfqdn())
                elif option == "COMPONENT_VERSION":
                    if not config.has_option(SECTION, "COMPONENT") or config.get(SECTION, "COMPONENT") == "":
                        config.set(SECTION, "COMPONENT", "Hadoop")
                    if "ambarieu-hdf" in config.get(SECTION, "COMPONENT"):
                        config.set(SECTION, option, getComponentVersion(config.get(SECTION, "COMPONENT")))
                    elif "ambari" in config.get(SECTION, "COMPONENT"):
                        config.set(SECTION, option, getComponentVersion("Ambari"))
                    else:
                        config.set(SECTION, option, getComponentVersion(config.get(SECTION, "COMPONENT")))
                elif option == "OS":
                    if Machine.isWindows():
                        cmd = 'powershell (Get-WmiObject -class Win32_OperatingSystem).Caption'
                        _exit_code, stdout = Machine.runasDeprecated(
                            user=Machine.getAdminUser(), cmd=cmd, passwd=Machine.getAdminPasswd()
                        )
                        config.set(SECTION, option, stdout)
                        continue

                    osname = platform.dist()[0]
                    # hack to check for oracle os as there
                    # is no diff for python
                    if os.path.exists('/etc/oracle-release'):
                        osname = 'oracle'
                    ver = platform.dist()[1]
                    # Need a hack for SLES as python cannot determine 11.1 vs 11.3
                    if osname.lower() == 'suse':
                        # read the file /etc/SuSE-release and determine the patch version.
                        f = open('/etc/SuSE-release', 'r')
                        txt = f.read()
                        f.close()
                        # get the patch level. For example
                        # PATCHLEVEL = 3
                        m = re.search('PATCHLEVEL = (.*)', txt, re.MULTILINE)
                        # if you find a match append to the version string
                        if m and m.group(1):
                            ver = '%s.%s' % (ver, m.group(1))

                    arch = platform.architecture()[0]

                    if os.path.exists('/etc/os-release'):
                        try:
                            f = open('/etc/os-release', 'r')
                            txt = f.read()
                            f.close()
                            m = re.search('NAME="(.*)"', txt, re.MULTILINE)
                            if m and m.group(1):
                                if m.group(1) == "Amazon Linux":
                                    osname = "amazonlinux"
                                    m = re.search('VERSION="(.*)"', txt, re.MULTILINE)
                                    if m and m.group(1):
                                        ver = m.group(1)
                                        if "2 (2017.12)" in ver:
                                            ver = "2"
                                    # the amzn ami which qe team is using is of 64 bit
                                    arch = "64bit"
                        except Exception:
                            logger.error(traceback.format_exc())

                    config.set(SECTION, option, '%s-%s-%s' % (osname, ver, arch))
                elif option == "HDP_STACK":
                    if "ambari" in config.get(SECTION, "COMPONENT"):
                        from beaver.component.ambari import Ambari
                        hdpVersion = Ambari.getHDPVersion()
                        if hdpVersion and hdpVersion[0] in ('1', '2'):
                            config.set(SECTION, option, "h" + hdpVersion[0])
                        else:
                            config.set(SECTION, option, 'h2')
                    else:
                        hadoopVersion = getComponentVersion("Hadoop")
                        if hadoopVersion and hadoopVersion[0] in ('1', '2'):
                            config.set(SECTION, option, "h" + hadoopVersion[0])
                elif option == "TDE":
                    from beaver.component.hadoop import HDFS2
                    if HDFS2.isKMSEnabled():
                        config.set(SECTION, option, "on")
                    else:
                        config.set(SECTION, option, "off")

                elif option == "SECURE":
                    if "ambari" in config.get(SECTION, "COMPONENT"):
                        from beaver.component.ambari import Ambari

                        config.set(SECTION, option, str(Ambari.isSecure()).lower())
                        secure_str = str(Ambari.isSecure()).lower()
                    else:
                        from beaver.component.hadoop import Hadoop
                        secure_str = str(Hadoop.isSecure()).lower()
                        if config.get(SECTION, "COMPONENT") == "HiveServer2Concurr":
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            if Hadoop.isSecure():
                                config.set(SECTION, "hs2_authentication", "Kerberos")
                            else:
                                config.set(SECTION, "hs2_authentication", "Unsecure")
                            config.set(SECTION, "hs2_transport", "Binary")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrHTTP":
                            if Hadoop.isEncrypted():
                                secure_str += "-http-en"
                            else:
                                secure_str += "-http"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            if Hadoop.isSecure():
                                config.set(SECTION, "hs2_authentication", "Kerberos")
                            else:
                                config.set(SECTION, "hs2_authentication", "Unsecure")
                            config.set(SECTION, "hs2_transport", "HTTP")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrLDAP":
                            if Hadoop.isEncrypted():
                                secure_str += "-ldap-en"
                            else:
                                secure_str += "-ldap"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            config.set(SECTION, "hs2_authentication", "LDAP")
                            config.set(SECTION, "hs2_transport", "Binary")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrLDAPHTTP":
                            if Hadoop.isEncrypted():
                                secure_str += "-ldap-http-en"
                            else:
                                secure_str += "-ldap-http"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            config.set(SECTION, "hs2_authentication", "LDAP")
                            config.set(SECTION, "hs2_transport", "HTTP")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrSSL":
                            if Hadoop.isEncrypted():
                                secure_str += "-ssl-en"
                            else:
                                secure_str += "-ssl"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            config.set(SECTION, "hs2_authentication", "Unsecure")
                            config.set(SECTION, "hs2_transport", "Binary")
                            config.set(SECTION, "hs2_ssl", "true")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrSSLHTTP":
                            if Hadoop.isEncrypted():
                                secure_str += "-ssl-http-en"
                            else:
                                secure_str += "-ssl-http"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            config.set(SECTION, "hs2_authentication", "Unsecure")
                            config.set(SECTION, "hs2_transport", "HTTP")
                            config.set(SECTION, "hs2_ssl", "true")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrTPUser":
                            if Hadoop.isEncrypted():
                                secure_str += "-tpuser-en"
                            else:
                                secure_str += "-tpuser"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            config.set(SECTION, "hs2_authentication", "Kerberos")
                            config.set(SECTION, "hs2_transport", "Binary")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "true")
                        elif config.get(SECTION, "COMPONENT") == "HiveServer2ConcurrLongRunning":
                            if Hadoop.isEncrypted():
                                secure_str += "-longrun-en"
                            else:
                                secure_str += "-longrun"
                            config.set(SECTION, "hs2_authorization", "SQL Standard")
                            if Hadoop.isSecure():
                                config.set(SECTION, "hs2_authentication", "Kerberos")
                            else:
                                config.set(SECTION, "hs2_authentication", "Unsecure")
                            config.set(SECTION, "hs2_transport", "Binary")
                            config.set(SECTION, "hs2_ssl", "false")
                            config.set(SECTION, "hs2_trusted_proxy", "false")
                        elif config.get(SECTION, "COMPONENT") == "SqoopDb2":
                            config.set(SECTION, "COMPONENT", "Sqoop")
                        else:
                            if Hadoop.isEncrypted():
                                secure_str += '-en'
                        config.set(SECTION, option, secure_str)
                elif option == "BLOB":
                    pass
                elif option == "RAN":
                    # dont add skipped, just pass + fail + aborted
                    config.set(SECTION, option, results[0] + len(results[1]) + results[3])
                elif option == "PASS":
                    config.set(SECTION, option, results[0])
                elif option == "FAIL":
                    config.set(SECTION, option, len(results[1]))
                elif option == "SKIPPED":
                    config.set(SECTION, option, results[2])
                elif option == "ABORTED":
                    config.set(SECTION, option, results[3])
                elif option == "FAILED_TESTS":
                    failedTests = ",".join(results[1])
                    failureSummary = ReportHelper.getFailureSummary(failedTests)
                    config.set(SECTION, "FAILURE_SUMMARY", failureSummary)
                    tmpFailedTests = ReportHelper.getGroupedFailedTests(failedTests)
                    config.set(SECTION, option, ReportHelper.getMergedFailedTests(tmpFailedTests, failureSummary))
                elif option == "NUM_OF_DATANODES":
                    if "ambari" in config.get(SECTION, "COMPONENT"):
                        config.set(SECTION, option, "N/A")
                    else:
                        from beaver.component.hadoop import HDFS

                        config.set(SECTION, option, HDFS.getDatanodeCount())
                elif option == "BUILD_URL":
                    if 'BUILD_URL' in os.environ:
                        config.set(SECTION, option, os.environ['BUILD_URL'])
                elif option == "HDP_RELEASE":
                    # If RU/RB, we must override HDP_RELEASE
                    #   (we can't fix this with product front. Discussed in BUG-31369.)
                    if config.get(SECTION, "TESTSUITE_COMPONENT").lower() in ["rollingupgrade", "rollback",
                                                                              "rollingupgrade-ha", "rollback-ha"]:
                        config.set(SECTION, option, "dal")
                    else:
                        config.set(SECTION, option, getRepoId(config.get(SECTION, "REPO_URL")))
                elif option == "JDK":
                    config.set(SECTION, option, Machine.getJDK())
                elif option == "DB":
                    if not config.has_option(SECTION, "COMPONENT") or config.get(SECTION, "COMPONENT") == "":
                        config.set(SECTION, "COMPONENT", "Hadoop")
                    config.set(SECTION, option, getDatabaseFlavor(config.get(SECTION, "COMPONENT")))
            except Exception as error:
                logger.error("ERROR processing option: %s", option)
                logger.error("Exception: %s", error)
        # make sure Hadoop is installed before append Tez to the component name
        if Config.getEnv("HDP_STACK_INSTALLED").lower() == "true" and config.has_option(SECTION, "COMPONENT"):
            if "ambari" in config.get(SECTION, "COMPONENT"):
                kerberos_server_type = 'n/a'
                from beaver.component.ambari import Ambari
                if Ambari.isSecure():
                    kerberos_server_type = 'mit'
                config.set(SECTION, 'kerberos_server_type', kerberos_server_type)
            else:
                from beaver.component.hadoop import Hadoop, HDFS
                from beaver.component.slider import Slider

                # set execution_framework. New columns for dashboard v2
                # TODO: This needs to be improved to be component specific.
                if Hadoop.isTez():
                    if Slider.isInstalled():
                        config.set(SECTION, 'execution_framework', 'tez-slider')
                    else:
                        config.set(SECTION, 'execution_framework', 'tez')
                else:
                    if Slider.isInstalled():
                        config.set(SECTION, 'execution_framework', 'mr-slider')
                    else:
                        config.set(SECTION, 'execution_framework', 'mr')
                # set wire_encryption
                # TODO: This needs to be improved to be component specific.
                if Hadoop.isEncrypted():
                    config.set(SECTION, 'wire_encryption', 'true')
                else:
                    config.set(SECTION, 'wire_encryption', 'false')
                # set kerberos_server_type
                kerberos_server_type = 'n/a'
                if Hadoop.isSecure():
                    kerberos_server_type = 'mit'
                    # add a check for AD
                    if Machine.isLinux():
                        gateway = Config.get("machine", "GATEWAY")
                        Machine.copyToLocal(Machine.getAdminUser(), gateway, '/etc/krb5.conf', '/tmp/krb5.conf')
                        f = open('/tmp/krb5.conf', 'r')
                        txt = f.read()
                        f.close()
                        #Finding all the admin_server in the krb5.conf with ports, if any
                        p = re.compile('admin_server = ((?!FILE).*)')
                        admin_server_list_with_ports = p.findall(txt)
                        admin_server_list = []
                        for admin_server_with_port in admin_server_list_with_ports:
                            admin_server_list.append(admin_server_with_port.split(':')[0])
                        #If len is greater than 1, first checking if one of the admin server is AD host,
                        #  than to ensure that not all the hosts are AD hosts, checking if one of the admin
                        #  server is not in AD Hosts Lists.
                        if len(admin_server_list) > 1:
                            for ad_host in AD_HOSTS_LIST:
                                if ad_host in admin_server_list:
                                    for admin_server in admin_server_list:
                                        if admin_server not in AD_HOSTS_LIST:
                                            kerberos_server_type = 'ad+mit'
                                            break
                        else:
                            for ad_host in AD_HOSTS_LIST:
                                if ad_host in admin_server_list:
                                    kerberos_server_type = 'ad'
                                    break
                config.set(SECTION, 'kerberos_server_type', kerberos_server_type)

                try:
                    from beaver.component.xa import Xa
                    # set argus. New column for dashboard v2
                    if Xa.isArgus():
                        config.set(SECTION, 'argus', 'true')
                    else:
                        config.set(SECTION, 'argus', 'false')
                except Exception as error:
                    logger.error("ERROR processing argus")
                    logger.error("Exception: %s", error)

                #set TDE
                if HDFS.isKMSEnabled():
                    config.set(SECTION, 'tde', 'true')
                else:
                    config.set(SECTION, 'tde', 'false')

        config.set(SECTION, 'START_TIME', startTime)
        config.set(SECTION, 'END_TIME', endTime)
        coverage_summary_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "coverage_summary.json")
        if os.path.exists(coverage_summary_file):
            fp = open(coverage_summary_file, "r")
            json_str = "\n".join(fp.readlines())
            fp.close()
            coverage_summary = json.loads(json_str)
            for key, value in coverage_summary.items():
                config.set(SECTION, key, value)
        config.write(open(outfile, 'w'))

    elif config.has_section('SANDBOX'):
        out_config = ConfigParser()
        out_config.optionxform = str
        out_config.add_section(SECTION)

        sb_type = config.get('SANDBOX', 'vm_env')
        out_config.set(SECTION, 'BUILD_ID', '0')
        ova_uri = ''
        if sb_type == 'VBOX':
            ova_uri = config.get(sb_type, 'vbox_ova_uri')
        elif sb_type == 'FUSION':
            ova_uri = config.get(sb_type, 'fus_ova_uri')
        if sb_type == 'HYPERV':
            ova_uri = config.get(sb_type, 'hyperv_ova_uri')
        out_config.set(SECTION, 'REPO_URL', ova_uri)
        sb_host = ''
        if os.name != 'nt':
            sb_host = os.popen("hostname -f").read().strip()
            sb_host = sb_host + '(' + os.popen("ifconfig en0 | grep 'inet ' | awk -F ' ' '{print $2}'"
                                               ).read().strip() + ')'
        else:
            sb_host = 'Kiev local host'
        out_config.set(SECTION, 'HOSTNAME', sb_host)

        out_config.set(SECTION, 'HDP_STACK', "h" + (config.get('VERSIONS', 'hadoop_version')[0]))
        out_config.set(SECTION, 'COMPONENT', 'SANDBOX')
        out_config.set(SECTION, 'TESTSUITE_COMPONENT', 'SANDBOX')

        if sb_type == 'HYPERV':
            sb_ver = 'hyper-v'
        else:
            tmp = ['%20', 'Hortonworks', 'VirtualBox', '.ova', 'VMware', '_']
            sb_ver = ova_uri.split('/')[5]
            for rstr in tmp:
                sb_ver = sb_ver.replace(rstr, '')

        out_config.set(SECTION, 'COMPONENT_VERSION', sb_ver)
        out_config.set(SECTION, 'CHECKSUM', 'N/A')

        ver_num = os.popen("sw_vers | grep 'ProductVersion:' | awk -F ' ' '{print $2}'").read().strip()
        if sb_type == 'HYPERV':
            out_config.set(SECTION, 'OS', 'Windows 8.1')
        else:
            out_config.set(SECTION, 'OS', 'MAC OS X ' + ver_num)
        out_config.set(SECTION, 'SECURE', 'false')
        out_config.set(SECTION, 'TYPE', 'System, UI')
        out_config.set(SECTION, 'BLOB', 'N/A')
        out_config.set(SECTION, 'PKG', 'OVA')
        if sb_type == 'VBOX':
            out_config.set(SECTION, 'INSTALLER', 'Oracle VirtualBox')
        elif sb_type == 'FUSION':
            out_config.set(SECTION, 'INSTALLER', 'VMWare Fusion')
        elif sb_type == 'HYPERV':
            out_config.set(SECTION, 'INSTALLER', 'Windows Hyper-V')
        out_config.set(SECTION, 'RAN', results[0] + len(results[1]) + results[3])
        out_config.set(SECTION, 'PASS', results[0])
        out_config.set(SECTION, 'FAIL', len(results[1]))
        out_config.set(SECTION, 'SKIPPED', results[2])
        out_config.set(SECTION, 'ABORTED', results[3])
        out_config.set(SECTION, 'FAILED_DEPENDENCY', 'N/A')
        out_config.set(SECTION, 'FAILED_TESTS', ",".join(results[1]))

        out_config.set(SECTION, 'NUM_OF_DATANODES', '1')
        out_config.set(SECTION, 'HDP_RELEASE', ova_uri.split('/')[4])
        out_config.set(SECTION, 'JDK', '1.6.0_51')
        out_config.set(SECTION, 'DB', 'N/A')
        out_config.set(SECTION, 'BROWSER', config.get('SANDBOX', 'test_browser'))

        out_config.write(open(outfile, 'w'))


def getBuildId(url):
    output = util.getURLContents(url + "/build.id")
    build_id = util.getPropertyValue(output, "TIMESTAMP", delimiter=":")
    #Added check in case REPO_URL is not valid. This will ensure results get posted to Dashboard
    if build_id is None or build_id == "":
        build_id = 0
    return build_id


def getRepoId(url):
    try:
        output = util.getURLContents(url + "/build.id")
        releaseName = util.getPropertyValue(output, "RELEASE_NAME", delimiter=":")
        # if release name is found return it, else try the logic below
        if releaseName:
            return releaseName
        # support for urls like http://public-repo-1.hortonworks.com/HDP-1.2.0/repos/suse11
        p = re.compile('^http://.*/HDP-(.*?)/.*')
        m = p.search(url)
        if not m:
            # this is for url like http://public-repo-1.hortonworks.com.s3.amazonaws.com/HDP/centos5/1.x/updates/1.2.1
            p = re.compile('^http://.*/HDP/.*/.*/updates/(.*)')
            m = p.search(url)
            if not m:
                # support for urls like http://public-repo-1.hortonworks.com/HDP/centos5/1.x/GA
                p = re.compile('^http://.*/HDP/.*/(.*)/.*')
                m = p.search(url)

        if not m:
            return 'N/A'
        else:
            return 'HDP-' + m.group(1)
    except ValueError:
        return 'N/A'


def getComponentVersion(component):
    logger.info("Got component version : %s", component)
    # check that ambarieu is present in "component"
    if "ambarieu-hdf" not in component and not COMPONENT_VERSION_MAP.has_key(component):
        return ""
    try:
        if component == "RollingUpgrade" or component == "RollingUpgrade-HA":
            from beaver.component.rollingupgrade.ruCommon import hdpRelease
            new_version = hdpRelease.getLatestRelease()
            return new_version
        elif "ambarieu" in component:
            # We could have used the APICorelib here to get the current stack version but not done for 2 reasons.
            # 1. We will have to get the uifrm path as its a parameter to APICorelib.
            #    This will be difficult to ascertain from this code
            # 2. Making an ambari call might fail if ambari is down. So it is better not to do so.
            from beaver.component.upgrades.expressupgrade import UpgradeReleaseInfo
            logger.info("Getting EU release info")

            # check if we want to get min value or max value. For downgrade we would want the min value
            downgrade_keys = ["dwngd", "downgrade"]
            min_or_max = "min" if any(s for s in downgrade_keys if s in component) else "max"

            # Get the Current upgraded stack version
            upgradeReleaseInfo = UpgradeReleaseInfo()
            logger.info("release type is of : %s", min_or_max)
            component_version = upgradeReleaseInfo.get_ambari_stack_version_after_upgrade()
            logger.info("Latest release is : %s", component_version)

            # return the component string
            return component_version
        else:
            module, clsname = COMPONENT_VERSION_MAP[component].rsplit(".", 1)
            __import__(module)
            imod = sys.modules[module]
            icls = getattr(imod, clsname)
            return getattr(icls, "getVersion")()
    except (ImportError, AttributeError):
        return ""


def getDatabaseFlavor(component):
    if not COMPONENT_DATABASE_MAP.has_key(component):
        return "NA"
    try:
        module, clsname = COMPONENT_DATABASE_MAP[component].rsplit(".", 1)
        __import__(module)
        imod = sys.modules[module]
        icls = getattr(imod, clsname)
        return getattr(icls, "getDatabaseFlavor")()
    except (ImportError, AttributeError):
        return ""


# parse the output
FUNCREG = re.compile(r"\${(.*?)}")


def parseTestDescription(text, rargs=None):
    if not text:
        return {}
    if not rargs:
        rargs = {}
    cat = "description"
    info = {}
    indent = ""
    details = ""
    lines = text.split("\n")
    for line in lines:
        if line.strip() in ["Name:", "Description:"]:
            if details != "":
                info[cat] = details.strip()
                details = ""
            cat = line.strip()[:-1].lower()
            indent = " " * line.find(line.strip())
        else:
            line = line.rstrip()
            if indent != "" and line.find(indent) == 0:
                line = line.split(indent, 1)[1]
            if line and rargs:
                m = FUNCREG.findall(line)
                for var in m:
                    if rargs.has_key(var):
                        line = line.replace("${" + str(var) + "}", str(rargs[var]))
            details += line + "\n"
    if details != "":
        info[cat] = details.strip()
    return info


def parseTestsuiteFromXmlFile(xmlfile, tsreport=None):
    if not tsreport:
        tsreport = {}
    from xml.dom import minidom
    xmldoc = minidom.parse(xmlfile)
    tsnodes = xmldoc.getElementsByTagName("testsuite")
    tscount = 0
    tcount = 0
    for node in tsnodes:
        tsname = node.getAttribute("name")
        if tsreport.has_key(tsname):
            continue
        tscount += 1
        tsreport[tsname] = {'tests': []}
        for tschildnode in node.childNodes:
            if tschildnode.nodeType == tschildnode.ELEMENT_NODE and tschildnode.nodeName == "test":
                tcount += 1
                testmap = {}
                testname = tschildnode.getAttribute("name")
                testmap['name'] = testname
                if tschildnode.hasChildNodes():
                    for tccnode in tschildnode.childNodes:
                        if tccnode.nodeType == tccnode.ELEMENT_NODE and tccnode.nodeName == "description":
                            tsdesc = tccnode.childNodes[0].nodeValue
                            testmap['description'] = tsdesc
                tsreport[tsname]['tests'].append(testmap)
    return tscount, tcount


def writeTestlistReport(report, mode="text"):
    if mode == "excel":
        try:
            from xlwt import Workbook, easyxf
        except ImportError:
            logger.warn("Module xlwt not found, will write testlist report in text mode")
            mode = "text"
    if mode == "excel":
        book = Workbook()
        sheet = book.add_sheet('Testlist')
        style_header = easyxf('font: bold 1;' 'align: horizontal center;')
        style_desc = easyxf('align: wrap 1;')
        sheet.write(0, 0, 'Testsuite', style_header)
        sheet.col(0).width = 256 * 20
        sheet.write(0, 1, 'Testcase', style_header)
        sheet.col(1).width = 256 * 25
        sheet.write(0, 2, 'Description', style_header)
        sheet.col(2).width = 256 * 60
        row_index = 1
        for name, testsuite in report.items():
            row = sheet.row(row_index)
            if testsuite.has_key('name'):
                row.write(0, testsuite['name'])
            else:
                if name[-3:] == ".py":
                    name = name[:-3]
                row.write(0, name)
            if testsuite.has_key('description'):
                row.write(2, testsuite['description'])
            row_index += 1
            if testsuite.has_key('tests'):
                for test in testsuite['tests']:
                    row = sheet.row(row_index)
                    if test.has_key('name'):
                        row.write(1, test['name'])
                    if test.has_key('description'):
                        row.write(2, test['description'], style_desc)
                    sheet.flush_row_data()
                    row_index += 1
        book.save('testlist.xls')
    elif mode == "xml":
        from xml.sax.saxutils import escape

        outf = open('testlist.xml', 'w')
        outf.write("<testsuites>\n")
        for name, testsuite in report.items():
            if testsuite.has_key('name'):
                name = testsuite['name']
            else:
                if name[-3:] == ".py":
                    name = name[:-3]
            outf.write("  <testsuite name=\"%s\">\n" % escape(name))
            if testsuite.has_key('description'):
                outf.write("    <description>%s</description>\n" % escape(testsuite['description']))
            if testsuite.has_key('tests'):
                for test in testsuite['tests']:
                    outf.write("    <test name=\"%s\">\n" % escape(test['name']))
                    if test.has_key('description'):
                        outf.write("      <description>%s</description>\n" % escape(test['description']))
                    outf.write("    </test>\n")
            outf.write("  </testsuite>\n")
        outf.write("</testsuites>\n")
        outf.close()
    else:
        outf = open('testlist.txt', 'w')
        for name, testsuite in report.items():
            if testsuite.has_key('name'):
                name = testsuite['name']
            else:
                if name[-3:] == ".py":
                    name = name[:-3]
            outf.write("TESTSUITE: %s\n" % name)
            if testsuite.has_key('description'):
                outf.write("Description:\n%s\n" % testsuite['description'])
            if testsuite.has_key('tests'):
                for test in testsuite['tests']:
                    if test.has_key('name'):
                        outf.write("TESTCASE: %s\n" % test['name'])
                    if test.has_key('description'):
                        outf.write("Description:\n%s\n" % test['description'])
        outf.close()
