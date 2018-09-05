#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import getpass
import json
import logging
import os
import random
import re
import socket
import time
import csv
import requests
import pytest
from taskreporter.taskreporter import TaskReporter
from beaver import configUtils
from beaver import util
from beaver.config import Config
from beaver.component.ambari import Ambari
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component.hive import Hive
from beaver.HTMLParser_v002 import HTMLParser
from beaver.machine import Machine

# Description:
# Test API to work with Spark

#pylint: disable=redefined-outer-name,super-init-not-called

QE_EMAIL = util.get_qe_group_email()
logger = logging.getLogger(__name__)

def python_modules_permission(mode="777"):
    command = "chmod " + mode + " -R " + "/usr/lib/python2.6/site-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/local/lib64/python2.6" + "; "
    command += "chmod " + mode + " -R " + "/usr/local/lib/python2.7/site-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/local/lib/python2.7/dist-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/local/lib/python2.6/site-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/local/lib/python2.6/dist-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/lib/python2.7/site-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/lib/python2.7/dist-packages" + "; "
    command += "chmod " + mode + " -R " + "/usr/lib64/python2.7" + "; "
    command += "chmod " + mode + " -R " + "/base/tools/python-2.7.14/lib/python2.7/site-packages" + "; "
    Machine.runas(user=Machine.getAdminUser(), cmd=command)

try:
    import pexpect
except ImportError, i:
    logger.info("ImportError in import pexpect, trying to explicitly install")
    cmd = "/base/symlink/pip27 install pexpect"
    Machine.runas(user=Machine.getAdminUser(), cmd=cmd)
    python_modules_permission()
    import pexpect


class Spark(object):
    def __init__(self):
        pass

    _ambariSparkServiceConfigVersion = None
    _ambariSpark2ServiceConfigVersion = None
    _ambariConfigMap = {
        'SPARK': {
            'hive-site.xml': 'spark-hive-site-override',
            'spark-thrift-sparkconf.conf': 'spark-thrift-sparkconf',
            'spark-defaults.conf': 'spark-defaults'
        },
        'SPARK2': {
            'hive-site.xml': 'spark2-hive-site-override',
            'spark-thrift-sparkconf.conf': 'spark2-thrift-sparkconf',
            'spark-defaults.conf': 'spark2-defaults'
        }
    }

    @classmethod
    @TaskReporter.report_test()
    def getSparkHome(cls):
        '''
        Returns SPARK_HOME
        '''
        if cls.isSpark2():
            return Config.get('spark', 'SPARK2_HOME')
        else:
            return Config.get('spark', 'SPARK_HOME')

    @classmethod
    def getSparkUser(cls):
        '''
        Returns spark user
        '''
        return Config.get('spark', 'SPARK_USER')

    @classmethod
    def isSpark2(cls):
        '''
        Return if spark2 is present in cluster
        '''
        return True

    @classmethod
    def isSpark2_2_or_beyond(cls):
        '''
        Check if spark2 has 2.2 or beyond
        :return:
        '''
        version = cls.getSparkVersion()
        return version.startswith("2.3") or version.startswith("2.2")

    @classmethod
    def getSparkConfDir(cls):
        '''
        Returns location of Spark Conf
        '''
        if cls.isSpark2():
            return Config.get('spark', 'SPARK2_CONF')
        else:
            return Config.get('spark', 'SPARK_CONF')

    @classmethod
    def getSparkDefaultConfFile(cls):
        '''
        Returns location of spark-env.sh
        '''
        return os.path.join(cls.getSparkConfDir(), "spark-defaults.conf")

    @classmethod
    @TaskReporter.report_test()
    def getSparkVersion(cls):
        command = " --version"
        _, stdout = cls.submitApplication(command)
        logger.info("stdout = %s", stdout)
        m = re.search("version (.*)", stdout)
        if m:
            version = m.group(1)
            return version
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def get_qe_examples_source(cls, logoutput=False):
        '''
        Download Qe-Examples repo
        :return: return the location of qe-example repo
        '''
        qe_examples_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "qe-examples-source")
        version = cls.getShortVersion()
        if not Machine.pathExists(Machine.getAdminUser(), None, qe_examples_dir, Machine.getAdminPasswd()):
            Machine.makedirs(Machine.getAdminUser(), None, qe_examples_dir, Machine.getAdminPasswd())
            Machine.chmod("777", qe_examples_dir, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
            os_name = Machine.getOS()
            url = "http://dev.hortonworks.com.s3.amazonaws.com/HDP/%s/3.x/BUILDS/%s/tars/qe-examples/" \
                  "qe-examples-0.1.0.%s-source.tar.gz" % (os_name, version, version)
            if logoutput:
                logger.info("Downloading Qe-example repo from %s", url)
            tarball = os.path.join(qe_examples_dir, "qe-examples-0.1.0.%s-source.tar.gz" % version)
            util.downloadUrl(url, tarball)
            Machine.run("tar xf %s" % tarball, cwd=qe_examples_dir, logoutput=logoutput)
        return os.path.join(qe_examples_dir, "qe-examples-0.1.0.%s" % version)

    @classmethod
    @TaskReporter.report_test()
    def getSparkExampleJar(cls, forceSparkVersion=None, logoutput=False):
        '''
        :return: Spark Example Jar location
        '''
        if (not forceSparkVersion and cls.isSpark2()) or (forceSparkVersion == "2"):
            version = cls.getSparkVersion()
            logger.info("Spark version = %s", version)
            url = "http://nexus-private.hortonworks.com/nexus/content/groups/public/com/hortonworks/" \
                  "qe-examples/spark2-examples-assembly/" + version + "/spark2-examples-assembly-" + version + ".jar"
            tmp_example_assembly_path = os.path.join(
                Config.getEnv('ARTIFACTS_DIR'), "spark2-examples-assembly-" + version + ".jar"
            )
            Lib_dir = os.path.join(cls.getSparkHome(), "lib")
            final_example_assembly_path = os.path.join(Lib_dir, "spark2-examples-assembly-" + version + ".jar")
            if not Machine.pathExists(Machine.getAdminUser(), None, final_example_assembly_path,
                                      Machine.getAdminPasswd()):
                util.downloadUrl(url, tmp_example_assembly_path)
                Machine.makedirs(Machine.getAdminUser(), None, Lib_dir, Machine.getAdminPasswd())
                Machine.copy(
                    tmp_example_assembly_path,
                    os.path.join(Lib_dir, "spark2-examples-assembly-" + version + ".jar"),
                    user=Machine.getAdminUser(),
                    passwd=Machine.getAdminPasswd()
                )
            cls.get_qe_examples_source(logoutput=logoutput)

            return final_example_assembly_path
        else:
            return Config.get('spark', 'SPARK_EXAMPLE_JAR')

    @classmethod
    def getSparkAssemblyJar(cls):
        '''
        :return: Spark Example Jar location
        '''
        return Config.get('spark', 'SPARK_ASSEMBLY_JAR')

    @classmethod
    def getSparkExampleDataDir(cls):
        '''
        :return: Spark example data dir '/usr/hdp/current/spark-client/data'
        '''
        return os.path.join(cls.getSparkHome(), "data")

    @classmethod
    def getSparkATSjar(cls):
        '''
        :return: spark yarn ats jar
        '''
        return Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=os.path.join(cls.getSparkHome(), "hdpLib"),
            searchstr="spark-yarn-timeline-ats-plugin_*.jar",
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    def getSparkLogDir(cls):
        '''
        :return: Spark LogDir location
        '''
        return Spark.getConfigFromSparkEnv('SPARK_LOG_DIR', defaultValue='/var/log/spark')

    @classmethod
    @TaskReporter.report_test()
    def getPropertyValueFromSparkDefaultConfFile(cls, filename, name, defaultValue=None):
        f = None
        result = defaultValue
        try:
            f = open(filename, 'r')
            patternObj = re.compile('%s (.*)' % name, re.MULTILINE)

            for line in f:
                matchObj = patternObj.search(line)
                if matchObj:
                    result = matchObj.group(1)
                    break
            return result
        finally:
            if f is not None:
                f.close()

    @classmethod
    @TaskReporter.report_test()
    def getConfigFromSparkEnv(cls, propertyValue, defaultValue=None):
        sparkEnv = os.path.join(cls.getSparkConfDir(), "spark-env.sh")
        file_content = open(sparkEnv, "r")
        for line in file_content:
            if line.find("export " + propertyValue) == 0:
                m = re.search(r'(\S*)=(\S*)', line)
                if m:
                    return m.group(2)
                else:
                    return None
        return defaultValue

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        sparkHome = cls.getSparkHome()
        if cls.isSpark2():
            files = util.findMatchingFiles(os.path.join(sparkHome, "jars"), "spark-core*.jar")
            m = re.search("spark-core[^-]*-(.*).jar", files[0])
        else:
            files = util.findMatchingFiles(sparkHome, "spark-examples-*.jar")
            m = re.search("spark-examples-(.*)-hadoop(.*).jar", files[0])
        return m.group(1) if m else ""

    @classmethod
    @TaskReporter.report_test()
    def getHDWVersion(cls):
        hdwHome = Config.get('spark', 'SPARK_LLAP_HOME')
        if cls.isSpark2():
            filename = util.findMatchingFiles(hdwHome, "hive-warehouse-connector*.jar")
            m = re.search(".*hive-warehouse-connector-assembly-(.*).jar", filename[0])
            return m.group(1) if m else ""
        else:
            return ""

    @classmethod
    def getShortVersion(cls):
        '''
        Return short version.
        If version = 2.7.1.2.3.0.0-1675,
        :return: 2.3.0.0-1675
        '''
        version = cls.getVersion()
        return version.split(".", 3)[-1]

    @classmethod
    def getSparkHistoryServerAddress(cls):
        '''
        Gets Spark History server address <hostname>:<port>
        :return: <hostanme>:<port>
        '''
        return cls.getPropertyValueFromSparkDefaultConfFile(
            cls.getSparkDefaultConfFile(), "spark.yarn.historyServer.address"
        ).strip()

    @classmethod
    def isSparkSSLEnabled(cls):
        '''
        checks if SSL is enabled in Spark
        returns true if SSL is enabled otherwise false
        '''
        we = cls.getPropertyValueFromSparkDefaultConfFile(
            cls.getSparkDefaultConfFile(), "spark.ssl.enabled", defaultValue=None
        )
        return True if we == "true" or we else False

    @classmethod
    @TaskReporter.report_test()
    def getSparkHistoryServerUrl(cls, forceSparkVersion=None):
        '''
        Returns spark History server url
        '''
        if forceSparkVersion == "1":
            url = cls.getSpark1HistoryServerUrl()
            return "http://" + url
        elif forceSparkVersion == "2":
            url = cls.getSpark2HistoryServerUrl()
        else:
            url = cls.getSparkHistoryServerAddress()
        if cls.isSparkSSLEnabled() and cls.isSpark2():
            return "https://" + url
        else:
            return "http://" + url

    @classmethod
    def getSpark1HistoryServerUrl(cls):
        '''
        Returns spark1 history server url
        '''
        return cls.getPropertyValueFromSparkDefaultConfFile(
            os.path.join(Config.get('spark', 'SPARK_CONF'), "spark-defaults.conf"), "spark.yarn.historyServer.address"
        ).strip()

    @classmethod
    def getSpark2HistoryServerUrl(cls):
        '''
        Returns spark2 history server url
        '''
        return cls.getPropertyValueFromSparkDefaultConfFile(
            os.path.join(Config.get('spark', 'SPARK2_CONF'), "spark-defaults.conf"), "spark.yarn.historyServer.address"
        ).strip()

    @classmethod
    @TaskReporter.report_test()
    def getSparkHistoryServerHostname(cls):
        '''
        Get Spark history server host name
        :return: Hostname or None
        '''
        host_address = cls.getSparkHistoryServerAddress()
        if host_address:
            return host_address.split(":")[0]
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getSparkHistoryServerPort(cls):
        '''
        Get spark history server port
        :return: Returns port number for Spark HS.
        '''
        host_address = cls.getSparkHistoryServerAddress()
        if host_address:
            return host_address.split(":")[1]
        else:
            return "18080"

    @classmethod
    @TaskReporter.report_test()
    def getSpark2WarehouseDir(cls):
        major_version = Hadoop.getVersion()[0]
        if major_version >= '3' and cls.isSpark2():
            return Spark.getPropertyValueFromSparkDefaultConfFile(
                Spark.getSparkDefaultConfFile(), "spark.sql.warehouse.dir", defaultValue="/apps/spark/warehouse"
            )
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def validateEntryOfAppinSparkHS(cls, application_id, forceSparkVersion=None):
        '''
        Function validates entry of application id in Spark History Server
        :param application_id: Application Id
        :return: True if application is found in http://<host>:<port>. otherwise False
        '''
        url = cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion)
        content = util.getURLContents(url, outfile="", headers=None, logoutput=False)
        logger.info(content)
        return content.find(application_id) >= 0

    @classmethod
    @TaskReporter.report_test()
    def validateSparkAppHistoryServerUrl(cls, application_id, mode, attempt=1):
        '''
        Validate that below history server pages are accessible for an AppId.
        If any of the page is not accessible, the assert will fail
        *  http://<host>:<port>/history/<appId>/jobs/
        *  http://<host>:<port>/history/<appId>/stages/
        *  http://<host>:<port>/history/<appId>/storage/
        *  http://<host>:<port>/history/<appId>/environment/
        *  http://<host>:<port>/history/<appId>/executors/
        :param application_id: Application Id
        '''
        msg = ""
        url_dict = {"jobs": False, "stages": False, "storage": False, "environment": False, "executor": False}
        for key in url_dict:
            if mode == "yarn-cluster":
                url = "%s/history/%s/%s/%s" % (cls.getSparkHistoryServerUrl(), application_id, attempt, key)
            else:
                url = "%s/history/%s/%s" % (cls.getSparkHistoryServerUrl(), application_id, key)
            logger.info("*** testing %s ***", url)
            content = util.getURLContents(url)
            logger.info(content)
            if content != "":
                url_dict[key] = True
            else:
                msg = msg + " %s failed, " % url
        assert msg == "", "%s" % msg

    @classmethod
    @TaskReporter.report_test()
    def getLzoJar(cls):
        if Machine.isLinux():
            LzoJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('hadoop', 'HADOOP_HOME'),
                searchstr='hadoop-lzo-*[0-9].jar',
                passwd=Machine.getAdminPasswd()
            )[0]
        else:
            LzoJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath=Config.get('hadoop', 'HADOOP_HOME'),
                searchstr='hadoop-lzo-*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]
        return LzoJar

    @classmethod
    def run(cls, cmd, env=None, logoutput=True):
        '''
        Runs spark command with current user.
        Returns (exit_code, stdout)
        '''
        return cls.runas(None, cmd, env=env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, host=None, env=None, logoutput=True, inbackground=False, cwd=None):
        '''
        Runs spark command with target user.
        Returns (exit_code, stdout)
        '''

        if not env:
            env = {}
        # get kerberos ticket
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        if inbackground:
            return Machine.runinbackgroundAs(user, cmd, host, cwd, env, stdout=None, stderr=None)
        else:
            return Machine.runas(user, cmd, host=host, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def runinbackground(cls, cmd, cwd=None, env=None):
        '''
        Runs spark  command in background
        '''
        cls.runas(None, cmd, env=env, inbackground=True, cwd=cwd)

    @classmethod
    def runinbackgroundAs(cls, user, cmd, host=None, cwd=None, env=None):
        '''
        Runs spark command in background with Target User
        '''
        cls.runas(user, cmd, host, env, True, True, cwd)

    @classmethod
    @TaskReporter.report_test()
    def submitApplication(
            cls,
            cmd,
            shell=False,
            user=None,
            host=None,
            env=None,
            inBackground=False,
            logoutput=True,
            timeout=180,
            isR=False,
            forceSparkVersion=None,
            cwd=None
    ):
        """
        Submits Spark Application using spark_submit
        :param cmd: spark application run command
        :param user: User which will run the application
        :param host: Machine from where application should run
        :param env: Environment variables
        :param config: Configuration location
        :param inBackground: if application should be running on background, set inBackground = True
        :param logoutput: Print output
        :return: (exit_code, stdout) if inBackground=False , retun appid if inBackground=True
        """

        if shell:
            bin_file = "spark-shell"
        else:
            bin_file = "spark-submit"

        if isR:
            bin_file = "sparkR"
        if isR and cls.isSpark2():
            bin_file = "spark-submit"
        cmd = bin_file + " " + cmd

        # Current working directory(cwd) is not picked up when spark-submit is run a different user
        # (hive user incase of humboldt).
        # Added this as a fix for https://hwxmonarch.atlassian.net/browse/QE-904
        if cwd and user:
            cmd = "cd " + cwd + " ;" + cmd

        # set env variables necessary for Spark
        if Machine.isLinux():
            ld_lib_val = MAPRED.getConfigValue("mapreduce.admin.user.env")
            version = Hadoop.getShortVersion()
            new_ld = ld_lib_val.replace('${hdp.version}', version).split("=")[1]
            if env is None:
                env = {"LD_LIBRARY_PATH": new_ld}
            else:
                env["LD_LIBRARY_PATH"] = new_ld
        else:
            env = {}
        if not forceSparkVersion:
            if cls.isSpark2():
                env["SPARK_MAJOR_VERSION"] = "2"
        else:
            env["SPARK_MAJOR_VERSION"] = forceSparkVersion
        if inBackground:
            m = re.search(r"2>&1 \| tee(.*)", cmd)
            if m:
                job_client_file = m.group(1).strip()
            else:
                job_client_file = cls.createTmpClientFile("default_clientfile")
                cmd = "%s 2>&1 | tee %s" % (cmd, job_client_file)

            cls.runinbackgroundAs(user, cmd, host=host, cwd=cwd, env=env)
            time.sleep(timeout)
            return YARN.getApplicationIDFromFile(job_client_file)
        else:
            return cls.runas(user, cmd, host, env, logoutput, cwd=cwd)

    @classmethod
    @TaskReporter.report_test()
    def submitSparkPiApplication(
            cls,
            mastername,
            arg,
            num_executor=None,
            driver_memory=None,
            executor_memory=None,
            executor_core=None,
            user=None,
            host=None,
            env=None,
            inBackground=False,
            logoutput=True
    ):
        '''
        Submits SparkPi application with classname = org.apache.spark.examples.SparkPi
        :param mastername: Cluster mastername
        :param arg: Arguments
        :param num_executor: number of executors
        :param driver_memory: memory requirement for Driver
        :param executor_memory: memory setting for executor
        :param executor_core: number of executors core
        :return: (exit_code, stdout) if inBackground=False , retun appid if inBackground=True
        '''
        cmd = " --class org.apache.spark.examples.SparkPi --master %s" % mastername
        if num_executor:
            cmd = cmd + " --num-executor %s" % num_executor
        if driver_memory:
            cmd = cmd + " --driver-memory %s" % driver_memory
        if executor_memory:
            cmd = cmd + " --executor-memory %s" % executor_memory
        if executor_core:
            cmd = cmd + " --executor-cores %s" % executor_core

        cmd = cmd + " " + cls.getSparkExampleJar() + " " + arg
        return cls.submitApplication(
            cmd, user=user, host=host, env=env, inBackground=inBackground, logoutput=logoutput
        )

    @classmethod
    @TaskReporter.report_test()
    def submitSparkHiveApplication(
            cls, clsName, appJar, master="yarn", deployMode="cluster", arg=" ", jars=None, env=None
    ):
        '''
        Submits Spark applications using HiveContext
        :param clsName: Application class
        :param appJar: Application jar
        :param master: spark://host:port, mesos://host:port, yarn, or local
        :param deployMode: client or cluster
        '''
        if not jars:
            jars = []
        cmd = "--master %s --deploy-mode %s --class %s" % (master, deployMode, clsName)
        addedjars = ",".join(jars)
        if deployMode == "cluster":
            spark_lib_dir = os.path.join(cls.getSparkHome(), 'lib')
            dn_jars = util.findMatchingFiles(spark_lib_dir, "datanucleus*.jar")
            if dn_jars:
                if addedjars != "":
                    addedjars += ","
                addedjars += ",".join(dn_jars)
            spark_conf_dir = cls.getSparkConfDir()
            cmd += " --files %s" % os.path.join(spark_conf_dir, 'hive-site.xml')
        if addedjars != "":
            cmd += " --jars " + addedjars
        cmd += " %s" % appJar
        cmd += " " + arg
        return cls.submitApplication(cmd, env=env, timeout=300)

    @classmethod
    @TaskReporter.report_test()
    def submitSparkLLAPApplication(
            cls,
            clsName=None,
            appJar=None,
            pyFile=None,
            deployMode="cluster",
            arg="",
            user=None,
            host=None,
            env=None,
            cwd=None,
            clientfile=None,
            timeout=300
    ):
        '''
        Submits a spark LLAP application (python or scala)
        :return:
        '''
        assert (clsName and appJar) or pyFile, "(clsName and appJar) or pyFile must be specified "
        cmd = "--master yarn --deploy-mode %s" % deployMode
        if deployMode == "cluster":
            cmd += " --conf spark.security.credentials.hiveserver2.enabled=true"
        else:
            cmd += " --conf spark.security.credentials.hiveserver2.enabled=false"
        spark_llap_jar = Config.get('spark', 'SPARK_LLAP_JAR')
        if clsName:
            cmd += " --class %s" % clsName
            cmd += " --jars %s" % spark_llap_jar
            if not HDFS.isASV() and not Machine.isCloud():
                cmd += ",%s" % Spark.getLzoJar()
            cmd += " %s" % appJar
        else:
            cmd += " --py-files %s" % Config.get('spark', 'SPARK_LLAP_PY_ZIP')
            cmd += " --jars %s" % spark_llap_jar
            if not HDFS.isASV() and not Machine.isCloud():
                cmd += ",%s" % Spark.getLzoJar()
            cmd += " %s" % pyFile
        cmd += " " + arg

        if not clientfile:
            clientfile = cls.createTmpClientFile("default_clientfile")
        cmd = cmd + "  2>&1 | tee %s" % (clientfile)
        app_id = cls.submitApplication(cmd, user=user, host=host, env=env, inBackground=True, cwd=cwd)
        assert app_id, "Failed to get the application ID for Spark LLAP application"
        app_state = 'UNDEFINED'
        starttime = time.time()
        while (((time.time() - starttime) < timeout) and not (app_state == 'FINISHED' or app_state == 'FAILED')):
            time.sleep(5)
            app_state = YARN.getAppStateFromID(app_id, user=user)
        return app_state == 'FINISHED'

    @classmethod
    @TaskReporter.report_test()
    def convertPseudoCode(
            cls, pseudoFile, mode, append_spark=True, appName="submit", args_map=None, return_file=False
    ):
        if not args_map:
            args_map = {}
        fr = open(pseudoFile, 'r')
        lines = fr.readlines()
        fr.close()

        if mode == "pyspark" and append_spark:
            new_lines = ["from pyspark.sql import SparkSession",
                         "spark = SparkSession.builder.appName(\"%s\").getOrCreate()" % appName] + lines + \
                        ["spark.stop()"]
        else:
            new_lines = lines

        if return_file:
            new_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "tmp-%s.py" % int(time.time()))
            fw = open(new_file, 'w')
        else:
            ret_lines = []

        for line in new_lines:
            args = re.findall(r"\{%(.*?)%\}", line)

            for arg in args:
                is_arr = re.match(r"^([a-zA-Z_]+)\[(\d+)\]$", arg)
                is_map = re.match(r'^([a-zA-Z_]+)\[["|\']?([a-zA-Z_]+)["|\']?\]$', arg)
                is_var = re.match(r"^([a-zA-Z_]+)$", arg)
                assert is_arr or is_map or is_var, "Failed to replace arguments while creating code from " \
                                                              "pseudo code for appName: %s (%s)" % (appName, mode)
                if is_arr:
                    name = is_arr.group(1)
                    index = int(is_arr.group(2))
                    assert name in args_map.keys(), "Failed to find arg: %s in args_map while creating code from " \
                                                    "pseudo code for appName: %s (%s)" % (name, appName, mode)
                    assert isinstance(args_map[name], list), "arg: %s is expected to be a list" % arg
                    assert index < len(args_map[name]), "index: %s must be less than length of list argument: %s" \
                                                        % (index, len(args_map[name]))
                    line = line.replace("{%" + arg + "%}", str(args_map[name][index]))
                elif is_map:
                    name = is_map.group(1)
                    key = is_map.group(2)
                    assert name in args_map.keys(), "Failed to find arg: %s in args_map while creating code from " \
                                                    "pseudo code for appName: %s (%s)" % (name, appName, mode)
                    assert isinstance(args_map[name], dict), "arg: %s is expected to be a dict" % arg
                    assert key in args_map[name].keys(), "key: %s must be present in map argument: %s" % (key, name)
                    line = line.replace("{%" + arg + "%}", str(args_map[name][key]))
                elif is_var:
                    assert arg in args_map.keys(), "Failed to create %s code from pseudo code for " \
                                                   "appName: %s (%s)" % (mode, appName, mode)
                    line = line.replace("{%" + arg + "%}", str(args_map[arg]))
            if return_file:
                fw.write(line + os.linesep)
            else:
                ret_lines.append(line.strip())

        if return_file:
            fw.close()
            return new_file
        else:
            return ret_lines

    @classmethod
    @TaskReporter.report_test()
    def submitSparkApplication(
            cls,
            clsName,
            mastername,
            arg=" ",
            repl_mode=None,
            num_executor=None,
            driver_memory=None,
            executor_memory=None,
            executor_core=None,
            conf=None,
            files=None,
            jars=None,
            repl=None,
            user=None,
            host=None,
            env=None,
            inBackground=False,
            logoutput=True,
            propertyFile=None,
            timeout=180,
            driver_java_options=None,
            package=None,
            getClientLog=False,
            clientfile=None,
            keytab=None,
            principal=None,
            SparkJar=None,
            forceSparkVersion=None,
            repositories=None,
            cwd=None
    ):
        '''
        Submits SparkPi application with classname = org.apache.spark.examples.SparkPi
        :param clsName: classname
        :param mastername: Cluster mastername
        :param arg: Arguments
        :param repl_mode: repl mode for master without classname
        :param num_executor: number of executors
        :param driver_memory: memory requirement for Driver
        :param executor_memory: memory setting for executor
        :param executor_core: number of executors core
        :param conf: its a comma seperated configuration like 'spark.pyspark.virtualenv.bin.path=
        /usr/bin/virtualenv,spark.pyspark.virtualenv.requirements=/tmp/requirements.txt'
        :param files: add --files option
        :param jars: add --jars option
        :param user: user who should run the job
        :param host: host on which application can run
        :param env: env variables to be set for application
        :param config: use --config option to point to a specific file
        :param inBackground: run application in background
        :param propertyFile: use --property-file option
        :param driver_java_options: use --driver-java-options
        :param package: use --package option
        :param getClientLog: enable getclient log
        :param clientfile: clientfile where job client log can be stored
        :param keytab: use --keytab option
        :param principal: use --principal option
        :param sparkJar: its sparkJar, the default is spark example jar
        :param forceSparkVersion: if you want to force a spark version ( either 1 or 2 ) instead of
        relying on cluster setup
        :param repositories: use --repository option
        :return: (exit_code, stdout) if inBackground=False , return appId  if inBackground=True
        '''
        if repl_mode:
            cmd = " --master yarn-client"
        else:
            cmd = " --class %s" % clsName
            if mastername:
                cmd = cmd + " --master %s" % mastername
        if package:
            cmd = cmd + " --packages %s" % package
        if num_executor:
            cmd = cmd + " --num-executors %s" % num_executor
        if driver_memory:
            cmd = cmd + " --driver-memory %s" % driver_memory
        if executor_memory:
            cmd = cmd + " --executor-memory %s" % executor_memory
        if executor_core:
            cmd = cmd + " --executor-cores %s" % executor_core
        if propertyFile:
            cmd = cmd + " --properties-file %s" % propertyFile
        if files:
            cmd = cmd + " --files %s" % files
        if driver_java_options:
            cmd = cmd + " --driver-java-options %s" % driver_java_options
        if repositories:
            cmd = cmd + " --repositories %s" % repositories
        if conf:
            if Machine.type() == 'Windows':
                cmd = cmd + " --conf \"%s\"" % conf
            else:
                import cStringIO as c
                confs = csv.reader(c.StringIO(conf), delimiter=',', escapechar='\\').next()
                for i_conf in confs:
                    cmd = cmd + " --conf %s" % i_conf
        if jars:
            cmd = cmd + " --jars %s" % jars
        if keytab:
            cmd = cmd + " --keytab %s" % keytab
        if principal:
            cmd = cmd + " --principal %s" % principal
        if not SparkJar:
            cmd = cmd + " " + cls.getSparkExampleJar(forceSparkVersion=forceSparkVersion)
        else:
            cmd = cmd + " " + SparkJar
        if repl_mode:
            cmd = cmd + " -i %s" % repl
            return cls.submitApplication(
                cmd,
                True,
                user,
                host,
                env,
                inBackground,
                logoutput,
                timeout=timeout,
                forceSparkVersion=forceSparkVersion,
                cwd=cwd
            )
        else:
            cmd = cmd + " " + arg
            if getClientLog:
                if not clientfile:
                    clientfile = cls.createTmpClientFile("default_clientfile")
                cmd = cmd + "  2>&1 | tee %s" % (clientfile)
            return cls.submitApplication(
                cmd,
                False,
                user,
                host,
                env,
                inBackground,
                logoutput,
                timeout=timeout,
                forceSparkVersion=forceSparkVersion,
                cwd=cwd
            )

    @classmethod
    def getPythonExampleSrcDir(cls):
        '''
        Find python example src dir
        :return: /usr/hdp/current/spark-client/examples/src/main/python
        '''
        return os.path.join(cls.getSparkHome(), "examples", "src", "main", "python")

    @classmethod
    @TaskReporter.report_test()
    def findPythonExamplePath(cls, pythonTestFile, user=None, host=None, passwd=None, srcDir=None):
        '''
        Find Absolute path for Python example
        :param pythonTestFile: Example Python file name such as correlations.py
        :param user: User
        :param host: host machine
        :param passwd: passwd for user
        :param srcDir: Dir inside which python test file needs to be search. bydefault its python example dir
        :return: Absolute path of the file such as
        /usr/hdp/current/spark-client/examples/src/main/python/mllib/correlations.py
        '''
        if pythonTestFile[0] == '/':
            # it is an absolute path already
            return pythonTestFile
        if not user:
            user = Machine.getAdminUser()
        if not passwd:
            passwd = Machine.getAdminPasswd()
        if not srcDir:
            srcDir = cls.getPythonExampleSrcDir()

        found = Machine.find(user, host, srcDir, pythonTestFile, passwd, logoutput=False)
        if not found:
            Exception("Failed to find %s under %s: " % (pythonTestFile, srcDir))
        return found[0]

    @classmethod
    @TaskReporter.report_test()
    def submitSparkPyApplication(
            cls,
            pythonFile,
            mode="",
            arg="",
            user=None,
            host=None,
            env=None,
            jars=None,
            inBackground=False,
            logoutput=True,
            driverClassPath=None,
            srcDir=None,
            driver_mem=None,
            executor_mem=None,
            timeout=180,
            getClientLog=False,
            clientfile=None,
            conf=None,
            num_executor=None,
            py_modules=None,
            keytab=None,
            principal=None
    ):
        '''
        Submits Spark Python application
        :param py_file_path: Python example file path such as correlations.py
        :param mode: Cluster mode such as yarn-client, yarn-cluster
        :param arg: Arguments
        :param user: User who starts the application
        :param host: Hostname where application should start
        :param jars: add --jars option
        :param env: Environment variables
        :param config: Configuration file
        :param config: Configuration file
        :param inBackground: Whether application runs in background or not
        :param logoutput: print log messages

        :return: (exit_code, stdout) if inBackground=False , return appId if inBackground=True
        '''
        python_file_path = cls.findPythonExamplePath(pythonFile, srcDir=srcDir)
        cmd = ""
        if driverClassPath:
            cmd = " --driver-class-path %s" % (cls.getSparkExampleJar())
        if mode != "local" and mode != "":
            cmd = cmd + " --master %s" % mode
        if principal:
            cmd = cmd + " --principal %s" % principal
        if keytab:
            cmd = cmd + " --keytab %s" % keytab
        if driver_mem:
            cmd = cmd + " --driver-memory %s" % driver_mem
        if executor_mem:
            cmd = cmd + " --executor-memory %s" % executor_mem
        if num_executor:
            cmd = cmd + " --num-executors %s" % num_executor
        if jars:
            if Machine.isWindows():
                # as per BUG-39343, if multiple jars is passed in Windows, it must be in """jar1,jar2""" format
                if jars.find(",") >= 0:
                    cmd = cmd + " --jars \"\"\"%s\"\"\"" % jars
                else:
                    cmd = cmd + " --jars %s" % jars
            else:
                cmd = cmd + " --jars %s" % jars
        if py_modules:
            cmd = cmd + " --py-files %s" % py_modules
        if conf:
            if Machine.type() == 'Windows':
                cmd = cmd + " --conf \"%s\"" % conf
            else:
                import cStringIO as c
                confs = csv.reader(c.StringIO(conf), delimiter=',', escapechar='\\').next()
                for tmp_conf in confs:
                    cmd = cmd + " --conf %s" % tmp_conf
        cmd = cmd + " %s %s" % (python_file_path, arg)
        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            cmd = cmd + "  2>&1 | tee %s" % (clientfile)
        logger.info("submitSparkPyApplication command : %s", cmd)
        return cls.submitApplication(cmd, False, user, host, env, inBackground, logoutput, timeout=timeout)

    @classmethod
    def getSparkRExampleDir(cls):
        '''
        get Spark R example location
        :return:
        '''
        return os.path.join(cls.getSparkHome(), "examples", "src", "main", "r")

    @classmethod
    @TaskReporter.report_test()
    def findRExamplePath(cls, RFile, srcDir=None, host=None):
        '''
        Find abosulte path of R example file
        :param RFile: example R file
        :param srcDir: Source dir
        :return:
        '''
        if not srcDir:
            srcDir = cls.getSparkRExampleDir()

        return Machine.find(Machine.getAdminUser(), host, srcDir, RFile, Machine.getAdminPasswd(), logoutput=False)[0]

    @classmethod
    @TaskReporter.report_test()
    def submitSparkRApplication(
            cls,
            RFile,
            mode="",
            arg=None,
            user=None,
            host=None,
            env=None,
            inBackground=False,
            logoutput=True,
            timeout=180,
            getClientLog=False,
            clientfile=None,
            srcDir=None,
            conf=None,
            jars=None
    ):
        '''
        Submits Spark Python application
        :param RFile: R example file path such as ml.R
        :param mode: Cluster mode such as yarn-client, yarn-cluster
        :param arg: Arguments
        :param user: User who starts the application
        :param host: Hostname where application should start
        :param env: Environment variables
        :param config: Configuration
        :param inBackground: Whether application runs in background or not
        :param logoutput: print log messages
        :return: (exit_code, stdout) if inBackground=False , return appId if inBackground=True
        '''
        if srcDir:
            R_file_path = cls.findRExamplePath(RFile, srcDir)
        else:
            R_file_path = cls.findRExamplePath(RFile, srcDir=cls.getSparkRExampleDir())
        cmd = ""
        if mode != "local" and mode != "":
            cmd = cmd + " --master %s" % mode
        if conf:
            if Machine.type() == 'Windows':
                cmd = cmd + " --conf \"%s\"" % conf
            else:
                import cStringIO as c
                confs = csv.reader(c.StringIO(conf), delimiter=',', escapechar='\\').next()
                for tmp_conf in confs:
                    cmd = cmd + " --conf %s" % tmp_conf
        if jars:
            cmd = cmd + " --jars %s" % jars

        cmd = cmd + " %s" % (R_file_path)
        if arg:
            cmd = cmd + " %s" % arg
        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            cmd = cmd + "  2>&1 | tee %s" % (clientfile)
        return cls.submitApplication(cmd, False, user, host, env, inBackground, logoutput, timeout=timeout, isR=True)

    @classmethod
    @TaskReporter.report_test()
    def submitInteractivePyspark(
            cls,
            cmd,
            user=None,
            host=None,
            getClientLog=False,
            clientfile=None,
            mode="pyspark",
            env=None,
            master=None,
            forceSparkVersion=None,
            py_modules=None
    ):
        """
        Runs pyspark command in Interactive Shell mode
        :param cmd: Pass the command
        :param user: User who starts the pyspark shell
        :param passwd: password
        :param host: Hostname
        :param logoutput: print log messages
        :param getClientLog: If True, the clientlogs will be redirected to clientfile
        :param clientfile: local file to store client logs
        :param mode: Interactive Pyspark can be run through "pyspark" or "Ipython"
        :param env: Environemnt variable
        :param master : its whether yarn-client or yarn-cluster mode of execution
        :return:
        """

        finalcmd = ''
        if not env:
            env = {}
        if mode == "pyspark":
            if HDFS.isASV() or Machine.isCloud():
                finalcmd = 'echo "%s" | %s ' % (cmd, cls.getPysparkPath())
            else:
                finalcmd = 'echo "%s" | %s --jars %s ' % (cmd, cls.getPysparkPath(), cls.getLzoJar())
        else:
            if HDFS.isASV() or Machine.isCloud():
                finalcmd = 'echo "%s" | IPYTHON=1 pyspark ' % (cmd)
            else:
                finalcmd = 'echo "%s" | IPYTHON=1 pyspark --jars %s ' % (cmd, cls.getLzoJar())
            env = {'PYSPARK_PYTHON': 'python', 'HADOOP_CONF_DIR': Config.get('hadoop', 'HADOOP_CONF')}
        if not forceSparkVersion:
            if cls.isSpark2():
                env["SPARK_MAJOR_VERSION"] = "2"
        else:
            env["SPARK_MAJOR_VERSION"] = forceSparkVersion
        if master:
            finalcmd = finalcmd + "--master %s" % (master)
        else:
            finalcmd = finalcmd + "--master yarn-client"
        if py_modules:
            finalcmd = finalcmd + " --py-files %s" % py_modules
        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            finalcmd = finalcmd + "  2>&1 | tee %s" % (clientfile)
        return cls.runas(user, finalcmd, host, env, True)

    @classmethod
    def getSparkShellPath(cls):
        return "spark-shell"

    @classmethod
    def getPysparkPath(cls):
        return "pyspark"

    @classmethod
    @TaskReporter.report_test()
    def submitSparkShellwithFile(
            cls,
            scalafile=None,
            user=None,
            host=None,
            getClientLog=False,
            clientfile=None,
            env=None,
            mode="local",
            jars=None
    ):
        '''
        Run Sparkshell command using -i option
        :param scalafile: scala file to be executed
        :param user: User who starts spark shell
        :param passwd: password
        :param host: Hostname
        :param getClientLog: If True, the clientlogs will be redirected to clientfile
        :param clientfile: local file to store client logs
        :param env: Environment variable
        :param mode: mode to be executed. local, yarn-client or yarn-cluster
        :param jars: jar files
        :return:
        '''
        if Machine.isLinux():
            ld_lib_val = MAPRED.getConfigValue("mapreduce.admin.user.env")
            version = Hadoop.getShortVersion()
            new_ld = ld_lib_val.replace('${hdp.version}', version).split("=")[1]
            if env is None:
                env = {"LD_LIBRARY_PATH": new_ld}
            else:
                env["LD_LIBRARY_PATH"] = new_ld

        finalcmd = cls.getSparkShellPath()
        if mode != "local":
            finalcmd = finalcmd + " --master %s" % mode
        if mode == "local" and HDFS.isASV():
            finalcmd = finalcmd + " --master %s" % mode
        if jars:
            finalcmd = finalcmd + " --jars %s" % jars
        else:
            if not HDFS.isASV() and not Machine.isCloud():
                finalcmd = finalcmd + " --jars %s" % (cls.getLzoJar())

        finalcmd = finalcmd + " -i %s" % scalafile
        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            finalcmd = finalcmd + "  2>&1 | tee %s" % (clientfile)
        return cls.runas(user, finalcmd, host, env, logoutput=True)

    @classmethod
    @TaskReporter.report_test()
    def sparkSubmitWithFile(
            cls,
            filename=None,
            user=None,
            host=None,
            mode="local",
            env=None,
            getClientLog=False,
            clientfile=None,
            logoutput=False,
            forceSparkVersion=None,
            jars=None,
            files=None,
            cwd=None
    ):
        '''
        run py or scala files using spark-submit
        :param filename: Py or Scala files
        :param user: User name
        :param host: Host
        :param mode: Mode (yarn-client, yarn-cluster, local)
        :param env: env variable
        :return:
        '''
        if not jars:
            jars = []
        bin_file = "spark-submit"
        # set env variables necessary for Spark
        if Machine.isLinux():
            ld_lib_val = MAPRED.getConfigValue("mapreduce.admin.user.env")
            version = Hadoop.getShortVersion()
            new_ld = ld_lib_val.replace('${hdp.version}', version).split("=")[1]
            if not env:
                env = {"LD_LIBRARY_PATH": new_ld}
            else:
                env["LD_LIBRARY_PATH"] = new_ld
        else:
            env = {}
        if not forceSparkVersion:
            if cls.isSpark2():
                env["SPARK_MAJOR_VERSION"] = "2"
        else:
            env["SPARK_MAJOR_VERSION"] = forceSparkVersion
        cmd = bin_file
        if mode != "local":
            cmd = cmd + "  --master " + mode
        if jars != []:
            app_jars = ",".join(jars)
            cmd = cmd + " --jars %s" % app_jars
        if files:
            cmd = cmd + " --files %s" % files

        cmd = cmd + " " + filename

        # Added this as a fix for https://hwxmonarch.atlassian.net/browse/QE-904
        if cwd and user:
            cmd = "cd " + cwd + " ;" + cmd

        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            cmd = cmd + "  2>&1 | tee %s" % (clientfile)

        return cls.runas(user, cmd, host, env, logoutput, cwd=cwd)

    @classmethod
    @TaskReporter.report_test()
    def submitInteractiveSparkshell(
            cls,
            cmd,
            user=None,
            host=None,
            getClientLog=False,
            clientfile=None,
            env=None,
            mode="local",
            jars=None,
            aboveTryCatch=None,
            propertyFile=None,
            forceSparkVersion=None
    ):
        '''
        Runs spark-shell command in Interactive mode
        :param cmd: Pass the command
        :param user: User who starts the spark shell
        :param passwd: password
        :param host: Hostname
        :param logoutput:  print log messages
        :param getClientLog: If True, the clientlogs will be redirected to clientfile
        :param clientfile: local file to store client logs
        :param env: Environemnt variable
        :return:
        '''
        finalcmd = ""
        if Machine.isLinux():
            if not env:
                ld_lib_val = MAPRED.getConfigValue("mapreduce.admin.user.env")
                version = Hadoop.getShortVersion()
                new_ld = ld_lib_val.replace('${hdp.version}', version).split("=")[1]
                env = {"LD_LIBRARY_PATH": new_ld}
            if not forceSparkVersion:
                if cls.isSpark2():
                    env["SPARK_MAJOR_VERSION"] = "2"
            else:
                env["SPARK_MAJOR_VERSION"] = forceSparkVersion
            if mode == "local":
                finalcmd = 'echo "%s" | %s' % (cmd, cls.getSparkShellPath())
            else:
                finalcmd = 'echo "%s" | %s --master %s' % (cmd, cls.getSparkShellPath(), mode)
            if jars:
                finalcmd = finalcmd + " --jars %s" % jars
            else:
                if not HDFS.isASV() and not Machine.isCloud():
                    finalcmd = finalcmd + " --jars %s" % (cls.getLzoJar())
            if propertyFile is not None:
                finalcmd += " --properties-file %s" % str(propertyFile)
        else:
            from beaver.component.hbase import HBase
            tmp_file = HBase.getTempFilepath()
            logger.info("Write command to tmp file : %s", tmp_file)
            cmd = cmd.replace('\\"', '"')
            tf = open(tmp_file, 'w')
            if aboveTryCatch:
                tf.write(aboveTryCatch)
                tf.write("\n")
            tf.write("try{\n")
            tf.write("    %s" % cmd)
            tf.write("\n} finally {\n")
            if HDFS.isASV():
                tf.write("    sys.exit();}")
            else:
                tf.write("    exit;}")
            tf.close()

            finalcmd = cls.getSparkShellPath()
            if mode != "local":
                finalcmd = finalcmd + " --master %s" % mode
            if mode == "local" and HDFS.isASV():
                finalcmd = finalcmd + " --master %s" % mode
            if jars:
                finalcmd = finalcmd + " --jars %s" % jars
            else:
                if not HDFS.isASV() and not Machine.isCloud():
                    finalcmd = finalcmd + " --jars %s" % (cls.getLzoJar())
            if propertyFile is not None:
                finalcmd += " --properties-file %s" % str(propertyFile)

            finalcmd = finalcmd + " -i %s" % tmp_file

        if getClientLog:
            if not clientfile:
                clientfile = cls.createTmpClientFile("default_clientfile")
            finalcmd = finalcmd + "  2>&1 | tee %s" % (clientfile)
        return cls.runas(user, finalcmd, host=host, cwd=None, env=env, logoutput=True)

    @classmethod
    @TaskReporter.report_test()
    def validateStreamingApplication(
            cls, appId, mode, pattern, client_log_file, useFlag=False, retry=80, appOwner=None, count=1
    ):
        '''
        Validate wordCount application from syslog/ client logs
        If mode = yarn-cluster, pattern will be searched from Application Syslog
        If mode = yarn-client, pattern will be searched from Client logs
        :param appId : application Id
        :param mode: either yarn-cluster or yarn-client
        :param pattern: Regex to look for in syslog/client log
        :param client_log_file: The client log file
        :return: Returns True if pattern is found in syslog/client log. Returns False otherwise
        '''
        if not pattern:
            return False
        if mode == "yarn-cluster":
            logs = YARN.getApplicationLogWithTimedOut(
                appId, appOwner=appOwner, retry=retry, check_for_log_aggregation_status=True
            )[1]
        else:
            f = open(client_log_file)
            logs = f.read()

        if count == 1:
            if not useFlag:
                m = re.search(pattern, logs)
            else:
                m = re.search(pattern, logs, re.DOTALL)
            return bool(m)
        else:
            regex = re.compile(r"%s" % pattern)
            match = regex.findall(logs)
            return count == len(match)

    @classmethod
    @TaskReporter.report_test()
    def getNetcatServerHost(cls):
        '''
        Get the External IP for Netcat Server Host.
        :return: External ip of Gateway
        '''
        if Machine.isLinux():
            cmd = "hostname"
            _, gateway_host = Machine.runas(
                Machine.getAdminUser(),
                cmd,
                host="",
                cwd=None,
                env=None,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )
            if Machine.isHumboldt():
                all_internal_nodes = "/tmp/all_nodes"
            else:
                all_internal_nodes = "/tmp/all_internal_nodes"
            f = open(all_internal_nodes)
            index = 0
            for index, line in enumerate(f):
                if re.search(gateway_host, line):
                    break
            line_no = index
            all_nodes = "/tmp/all_nodes"
            fp = open(all_nodes)
            for index, line in enumerate(fp):
                if index == line_no:
                    return line.rstrip()
            return HDFS.getGateway()
        else:
            return HDFS.getGateway()

    @classmethod
    @TaskReporter.report_test()
    def startNetcatServerinBackground(cls, host=None, port="9999", message="hello world", user=None):
        '''
        Start netcat server in background by passing message
        :param host: host on which netcat server should be starting
        :param port : port on which netcat server should be running
        :param message : message to pass through netcat server
        :param user : user who runs this server
        '''
        if Machine.isWindows():
            nc_exe = os.path.join(Machine.getTempDir(), "netcat", "nc.exe")
            cmd = 'printf "%s\\n" | %s -L -p %s' % (message, nc_exe, port)
        else:
            cmd = 'printf "%s\\n" | nc -lk %s' % (message, port)
        Machine.runinbackgroundAs(user, cmd, host=host, cwd=None, env=None, stdout=None, stderr=None)

    @classmethod
    @TaskReporter.report_test()
    def stopNetcatServer(cls, host=None, port="9999", message="hello world", user=None):
        '''
        Stop netcat server
        :param host: host on which netcat server is started
        :param port : port on which netcat server is started
        :param message : message passed through netcat server
        :param user : user who runs this server
        '''
        if not host:
            host = HDFS.getGateway()
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        if Machine.isLinux():
            pids = Machine.getProcessListRemote(
                host, format="%U %p %P %a", filter="'%s'" % message, user=user, logoutput=True
            )
            pid = Machine.getPidFromString(pids[0], user)
            Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
            pids = Machine.getProcessListRemote(
                host, format="%U %p %P %a", filter="'nc -lk %s'" % port, user=user, logoutput=True
            )
            pid = Machine.getPidFromString(pids[0], user)
            Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
        else:
            logger.info("Kill nc.exe")
            pid = Machine.getProcessListWithPid(host, "nc.exe", "netcat", logoutput=True, ignorecase=True)
            Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)

    @classmethod
    @TaskReporter.report_test()
    def killLocalApplications(cls, filter_message, user=None, host=None):
        '''
        kill applications started in Local mode.
        :param filter_message: message which helps to find the pid. such as applicationname "PageViewStream"
        :param user: user who started application
        :param host: Host machine
        '''
        if not host:
            host = HDFS.getGateway()
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        if Machine.isLinux():
            processes = Machine.getProcessListRemote(
                host,
                format="%U %p %P %a",
                filter="'%s'" % filter_message,
                user=user,
                exclFilters=["pytest", "py.test"],
                logoutput=True
            )
            for _ in range(10):
                for one_process in processes:
                    pid = Machine.getPidFromString(one_process)
                    if pid:
                        Machine.killProcessRemote(
                            int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True
                        )
                processes = Machine.getProcessListRemote(
                    host,
                    format="%U %p %P %a",
                    filter="'%s'" % filter_message,
                    user=user,
                    exclFilters=["pytest", "py.test"],
                    logoutput=True
                )
                if not processes:
                    break
        else:
            pid = Machine.getProcessListWithPid(host, "java.exe", filter_message, logoutput=True, ignorecase=True)
            if pid:
                Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)

    @classmethod
    @TaskReporter.report_test()
    def startFeederActor(cls, host="", port="9999"):
        '''
        Start FeederActor in Gateway
        '''
        if host == "":
            host = HDFS.getGateway()
        cmd = " --class org.apache.spark.examples.streaming.FeederActor "
        cmd = cmd + cls.getSparkExampleJar()
        cmd = cmd + " %s %s 2>&1 | tee %s" % (host, port, cls.createTmpClientFile("feeder.txt"))
        return cls.submitApplication(
            cmd, shell=False, user=None, host=None, env=None, inBackground=True, logoutput=True, timeout=70
        )

    @classmethod
    @TaskReporter.report_test()
    def stopFeederActor(cls, host=""):
        '''
        Stop FeederActor
        '''
        if not host:
            host = HDFS.getGateway()
        user = Config.get('hadoop', 'HADOOPQA_USER')
        if Machine.isLinux():
            for _ in range(0, 2):
                pids = Machine.getProcessListRemote(
                    host,
                    format="%U %p %P %a",
                    filter="org.apache.spark.examples.streaming.FeederActor",
                    user=user,
                    logoutput=True
                )
                pid = Machine.getPidFromString(pids[0], user)
                Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
        else:
            pid = Machine.getProcessListWithPid(
                host, "java.exe", "org.apache.spark.examples.streaming.FeederActor", logoutput=True, ignorecase=True
            )
            Machine.killProcessRemote(int(pid), host, Machine.getAdminUser(), Machine.getAdminPasswd(), True)

    @classmethod
    @TaskReporter.report_test()
    def startHistoryServer(cls, config=None):
        '''
        Start history server
        '''
        host = cls.getSparkHistoryServerHostname()
        user = cls.getSparkUser()
        if Machine.isLinux():
            startCmd = os.path.join(cls.getSparkHome(), 'sbin', 'start-history-server.sh')
            if config:
                env = {'SPARK_CONF_DIR': config}
            else:
                env = None
            if Hadoop.isSecure():
                kinitCmd = '%s -kt %s %s/%s' % (Machine.getKinitCmd(), Machine.getServiceKeytab(user), user, host)
                startCmd = kinitCmd + "; " + startCmd
            Machine.runas(user, startCmd, host=host, env=env)

    @classmethod
    @TaskReporter.report_test()
    def stopHistoryServer(cls):
        '''
        stop History server
        '''
        host = cls.getSparkHistoryServerHostname()
        user = cls.getSparkUser()
        if Machine.isLinux():
            startCmd = os.path.join(cls.getSparkHome(), 'sbin', 'stop-history-server.sh')
            if Hadoop.isSecure():
                kinitCmd = '%s -kt %s %s/%s' % (Machine.getKinitCmd(), Machine.getServiceKeytab(user), user, host)
                startCmd = kinitCmd + "; " + startCmd
            Machine.runas(user, startCmd, host=host)

    @classmethod
    @TaskReporter.report_test()
    def restartHistoryServer(cls, config=None):
        '''
        restart history server
        '''
        cls.stopHistoryServer()
        time.sleep(10)
        cls.startHistoryServer(config)

    @classmethod
    @TaskReporter.report_test()
    def startThriftServerUsingAmbari(cls, port='10015'):
        host = Hive.getHiveHost()
        if Machine.getInstaller() == 'cloudbreak':
            logger.info("Checking if Spark ThriftServer is already running at " + host + ":" + port)
            if not util.isPortOpen(host, int(port)):
                if cls.isSpark2():
                    Ambari.addComponent(host=host, service='SPARK2', component='SPARK2_THRIFTSERVER')
                    time.sleep(15)
                    Ambari.stopComponent(host=host, component='SPARK2_THRIFTSERVER')
                    time.sleep(15)
                    Ambari.startComponent(host=host, component='SPARK2_THRIFTSERVER')
                else:
                    Ambari.addComponent(host=host, service='SPARK', component='SPARK_THRIFTSERVER')
                    time.sleep(15)
                    Ambari.stopComponent(host=host, component='SPARK_THRIFTSERVER')
                    time.sleep(15)
                    Ambari.startComponent(host=host, component='SPARK_THRIFTSERVER')

    @classmethod
    @TaskReporter.report_test()
    def startThriftServer(cls, port='10002'):
        logger.info("Starting the Spark ThriftServer")
        host = cls.getThriftServerHost()
        if Hadoop.isAmbari():
            if cls.isSpark2():
                Ambari.stopComponent(host=host, component='SPARK2_THRIFTSERVER')
                time.sleep(15)
                Ambari.startComponent(host=host, component='SPARK2_THRIFTSERVER')
                exit_code = 0
            else:
                Ambari.stopComponent(host=host, component='SPARK_THRIFTSERVER')
                time.sleep(15)
                Ambari.startComponent(host=host, component='SPARK_THRIFTSERVER')
                exit_code = 0
        elif Machine.type() == 'Windows':
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="sc config yarnsparkhiveserver2 start= demand",
                host=host,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )
            exit_code, _ = Machine.service('yarnsparkhiveserver2', 'start', host=host)
        else:
            startCmd = os.path.join(cls.getSparkHome(), 'sbin', 'start-thriftserver.sh')
            startCmd += " --master yarn-client --executor-memory 512m -hiveconf hive.server2.thrift.port=" + port
            lzoJar = Spark.getLzoJar()
            if lzoJar:
                startCmd += " --jars " + lzoJar
            user = Config.get('hive', 'HIVE_USER')
            sparkLogDir = Spark.getConfigFromSparkEnv('SPARK_LOG_DIR', defaultValue='/var/log/spark')
            Machine.chmod("777", sparkLogDir, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd(), host=host)
            sparkPidDir = Spark.getConfigFromSparkEnv('SPARK_PID_DIR', defaultValue='/var/run/spark')
            Machine.chmod("777", sparkPidDir, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd(), host=host)
            if Hadoop.isSecure():
                kinitCmd = '%s -kt %s %s/%s' % (Machine.getKinitCmd(), Machine.getServiceKeytab(user), user, host)
                startCmd = kinitCmd + "; " + startCmd
            exit_code, _ = Machine.runas(user, startCmd, host=host)
        if exit_code == 0:
            util.waitForPortToOpen(host, int(port), timeout=180)
            time.sleep(10)

    @classmethod
    @TaskReporter.report_test()
    def stopThriftServer(cls):
        logger.info("Stopping the Spark ThriftServer")
        host = Hive.getHiveHost()
        if Machine.type() == 'Windows':
            return Machine.service('yarnsparkhiveserver2', 'stop', host=host)
        stopCmd = os.path.join(cls.getSparkHome(), 'sbin', 'stop-thriftserver.sh')
        user = Config.get('hive', 'HIVE_USER')
        if Hadoop.isSecure():
            kinitCmd = '%s -kt %s %s/%s' % (Machine.getKinitCmd(), Machine.getServiceKeytab(user), user, host)
            stopCmd = kinitCmd + "; " + stopCmd
        return Machine.runas(user, stopCmd, host=host)

    @classmethod
    @TaskReporter.report_test()
    def getThriftServerHost(cls):
        hosts = cls.getServiceHosts('sparkthriftserver')
        tshost = hosts[0] if hosts else Hive.getHiveHost()
        return tshost

    @classmethod
    @TaskReporter.report_test()
    def getThriftServerPort(cls):
        tsport = '10016' if cls.isSpark2() else '10002'
        transportMode = cls.getHiveProperty("hive.server2.transport.mode", defaultValue="binary")
        hivePortProperty = "hive.server2.thrift.http.port" if transportMode == "http" else "hive.server2.thrift.port"
        return cls.getHiveProperty(hivePortProperty, defaultValue=tsport)

    @classmethod
    @TaskReporter.report_test()
    def getHiveProperty(cls, propertyName, defaultValue=None):
        sparkHiveSite = os.path.join(cls.getSparkConfDir(), "hive-site.xml")
        if os.path.exists(sparkHiveSite):
            return util.getPropertyValueFromConfigXMLFile(sparkHiveSite, propertyName, defaultValue=defaultValue)
        else:
            return defaultValue

    @classmethod
    @TaskReporter.report_test()
    def getThriftServerJdbcUrl(cls):
        spark2_hive_site_override = Ambari.getConfig(type='spark2-hive-site-override', service='SPARK2')
        transportMode = spark2_hive_site_override["hive.server2.transport.mode"]
        jdbcUrl = "jdbc:hive2://%s:%s" % (cls.getThriftServerHost(), cls.getThriftServerPort())
        if transportMode == "http":
            jdbcUrl += "/;transportMode=http;httpPath=cliservice"
        if Hadoop.isSecure():
            sparkPrincipal = spark2_hive_site_override["hive.server2.authentication.kerberos.principal"]
            if transportMode != "http":
                jdbcUrl += "/"
            jdbcUrl += ";principal=" + sparkPrincipal
        return jdbcUrl

    @classmethod
    @TaskReporter.report_test()
    def getBeelineCmd(cls):
        return os.path.join(cls.getSparkHome(), 'bin', 'beeline')

    @classmethod
    @TaskReporter.report_test()
    def runQueryOnBeeline(
            cls,
            query,
            user=None,
            passwd="pwd",
            logoutput=True,
            queryIsFile=False,
            readFromFile=False,
            hiveconf=None,
            cwd=None
    ):
        if not hiveconf:
            hiveconf = {}
        if user is None:
            user = Config.getEnv('USER')
        cmd = cls.getBeelineCmd()
        cmd += " -n %s -p %s -u \"%s\" --outputformat=tsv" % (user, passwd, cls.getThriftServerJdbcUrl())
        for key, value in hiveconf.items():
            cmd += " --hiveconf %s=%s" % (key, value)
        if queryIsFile:
            cmd += " -f %s" % query
        elif readFromFile:
            qfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d' % int(999999 * random.random()))
            qf = open(qfile, 'w')
            qf.write(query)
            qf.close()
            cmd += " -f %s" % qfile
        else:
            cmd += " -e \"%s\"" % query
        env = {}
        if Hadoop.isSecure():
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
        return Machine.runex(cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def createTmpClientFile(cls, clientfile):
        '''
        Creates temporary client log file in Artifacts dir
        :param clientfile: The name of file
        :return: absolute path
        '''
        ADMIN_USER = Machine.getAdminUser()
        ADMIN_PWD = Machine.getAdminPasswd()
        tmp_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "Spark_clientLogs")
        Machine.makedirs(None, None, tmp_dir, passwd=None)
        Machine.chmod(perm="777", filepath=tmp_dir, recursive=True, user=ADMIN_USER, passwd=ADMIN_PWD, logoutput=True)
        Local_clientlog = os.path.join(tmp_dir, clientfile)
        if Machine.pathExists(ADMIN_USER, None, Local_clientlog, ADMIN_PWD):
            Machine.rm(ADMIN_USER, None, Local_clientlog, isdir=False, passwd=ADMIN_PWD)
        return Local_clientlog

    @classmethod
    def get_TmpClientFile(cls, clientfile="default"):
        '''
        Get the location for Temporary client log file
        :param clientfile:
        :return:
        '''
        tmp_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "Spark_clientLogs")
        return os.path.join(tmp_dir, clientfile)

    @classmethod
    def get_requirements_txt(cls, logoutput=False):
        '''
        Get the location for requirements file
        :return:
        '''
        if logoutput:
            Machine.cat(host=None, path=os.path.join(Machine.getTempDir(), "requirements.txt"))
        return os.path.join(Machine.getTempDir(), "requirements.txt")

    @classmethod
    def update_requirements_txt(cls, message):
        '''
        Update requirements txt with message
        :param message:
        :return:
        '''
        # create requirement.txt
        f = open(cls.get_requirements_txt(), 'w')
        f.write(message)
        f.close()

    @classmethod
    @TaskReporter.report_test()
    def install_anaconda2(cls, hostlist):
        '''
        Install Anaconda2
        :return:
        '''
        anaconda_installer = "http://qe-repo.s3.amazonaws.com/Anaconda2-4.3.0-Linux-x86_64.sh?AWSAccessKeyId=" \
                             "AKIAIDZB7JJ5YBYI23KQ&Expires=1573515258&Signature=VRI1dazwnHubsPetFgTHpUyUJes%3D"
        Local_anaconda_dir = os.path.join(Machine.getTempDir(), "anaconda")
        for host in hostlist:
            Machine.makedirs(None, host, Local_anaconda_dir)
            Machine.chmod(
                "777",
                Local_anaconda_dir,
                recursive=True,
                user=Machine.getAdminUser(),
                host=host,
                passwd=Machine.getAdminPasswd(),
                logoutput=True
            )
        Machine.installPackages("bzip2", hosts=hostlist)
        Local_anaconda_tar_file = os.path.join(Local_anaconda_dir, "Anaconda2-4.3.0-Linux-x86_64.sh")
        util.downloadUrl(anaconda_installer, Local_anaconda_tar_file)
        for host in hostlist:
            if host != HDFS.getGateway():
                Machine.copyFromLocal(
                    Machine.getAdminUser(), host, Local_anaconda_tar_file, Local_anaconda_tar_file,
                    Machine.getAdminPasswd()
                )

            install_conda = "cd %s; bash Anaconda2-4.3.0-Linux-x86_64.sh -b" % (Local_anaconda_dir)
            Machine.runas(Machine.getAdminUser(), install_conda, host=host, passwd=Machine.getAdminPasswd())
            Machine.runas(Machine.getAdminUser(), "chmod -R 755 /root", host=host, passwd=Machine.getAdminPasswd())
            anaconda_bin_file = os.path.join("/root", "anaconda2", "bin", "conda")
            assert Machine.pathExists(Machine.getAdminUser(), host, anaconda_bin_file, passwd=None)
            default_conda = cls.get_conda_bin()
            Machine.runas(
                Machine.getAdminUser(), "rm -rf " + default_conda, host=host, passwd=Machine.getAdminPasswd()
            )
            update_default_conda = "ln -s %s %s" % (anaconda_bin_file, default_conda)
            Machine.runas(Machine.getAdminUser(), update_default_conda, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    @TaskReporter.report_test()
    def get_conda_bin(cls):
        '''
        Get conda binary file
        :return:
        '''
        if Machine.isUbuntu():
            filepath = "/usr/local"
        else:
            filepath = "/usr"
        outputs = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath=filepath,
            searchstr="conda",
            passwd=Machine.getAdminPasswd(),
            logoutput=False,
            type="f"
        )
        for out in outputs:
            if Machine.pathExists(Machine.getAdminUser(), None, out, Machine.getAdminPasswd()):
                return out
        return os.path.join(filepath, "bin", "conda")

    @classmethod
    @TaskReporter.report_test()
    def get_virtual_env_bin(cls):
        '''
        Get virualenv bin file
        :return:
        '''
        if Machine.isUbuntu():
            filepath = "/usr/local"
        else:
            filepath = "/usr"
        outputs = Machine.find(
            user=Machine.getAdminUser(),
            host=None,
            filepath=filepath,
            searchstr="virtualenv",
            passwd=Machine.getAdminPasswd(),
            logoutput=False,
            type="f"
        )
        for out in outputs:
            if Machine.pathExists(Machine.getAdminUser(), None, out, Machine.getAdminPasswd()):
                return out
        return os.path.join(filepath, "bin", "virtualenv")

    @classmethod
    @TaskReporter.report_test()
    def setSparkWarehouse(cls, warehouse_dir="/tmp/warehouse"):
        '''
        Add workaround to set spark sql ware house for BUG 67005
        '''
        if cls.isSpark2():
            modified_conf = {'spark.sql.warehouse.dir': warehouse_dir}
            Spark.updateSparkDefaultConf(modified_conf)
            HDFS.createDirectory(warehouse_dir)

    @classmethod
    @TaskReporter.report_test()
    def updateSparkDefaultConf(cls, modified_conf):
        '''
        Updates /etc/spark/spark-defaults.conf
        '''
        new_prop_file = cls.createTmpSparkDefaultConf(modified_conf)
        orig_location = Spark.getSparkDefaultConfFile()
        Machine.copy(new_prop_file, orig_location, Machine.getAdminUser(), Machine.getAdminPasswd())

    @classmethod
    @TaskReporter.report_test()
    def createTmpSparkDefaultConf(cls, modified_conf):
        '''
        Create Temp Spark Default conf file with modified config
        :param modified_conf: Dictonary object with new values. ({prop1:val1, prop2:val2}).
                            If prop1 is not present, it will be added.
                            If prop1 is present, it will be altered.
        :return: Tmp Spark-defaults.conf location
        '''
        tmp_location = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "spark_defaults.conf")
        original_conf = Spark.getSparkDefaultConfFile()
        # copy original spark-defaults.conf to tmp_location
        Machine.copy(original_conf, tmp_location)
        # modify Config on Tmp File
        util.writePropertiesToConfFile(tmp_location, tmp_location, modified_conf)
        return tmp_location

    @classmethod
    @TaskReporter.report_test()
    def removeConfFromSparkDefaultConf(cls, infile, removed_conf):
        propstr = ""
        if infile is not None:
            for line in open(infile).readlines():
                sline = line.strip()
                if not sline.startswith("#") and sline.strip() != "":
                    if sline.find("spark.driver.extraJavaOptions") < 0 and \
                            sline.find("spark.yarn.am.extraJavaOptions") < 0:
                        try:
                            key, _ = sline.split(" ", 1)
                        except ValueError:
                            key = sline
                        key = key.strip()
                        if key not in removed_conf:
                            propstr += line
                else:
                    propstr += line
        outf = open(infile, 'w')
        try:
            outf.write(propstr)
        finally:
            outf.close()

    @classmethod
    @TaskReporter.report_test()
    def setupIpython(cls, hostlist):
        '''
        Install Ipython 1.2.1 on hostlist
        :param hostlist: List of hosts where Ipython needs to be installed.
        :return:
        '''
        S3_ipython_tar = "http://qe-repo.s3.amazonaws.com/ipython-1.2.1.tar.gz?AWSAccessKeyId=" \
                         "AKIAIDZB7JJ5YBYI23KQ&Expires=1791969346&Signature=xi8pMfInULhPojltaYmHML00IYQ%3D"
        Local_ipyhon_dir = os.path.join(Machine.getTempDir(), "Ipython")
        for host in hostlist:
            Machine.makedirs(None, host, Local_ipyhon_dir)
            Machine.chmod(
                "777",
                Local_ipyhon_dir,
                recursive=True,
                user=Machine.getAdminUser(),
                host=host,
                passwd=Machine.getAdminPasswd(),
                logoutput=True
            )
        Local_ipython_tar_file = os.path.join(Local_ipyhon_dir, "ipython-1.2.1.tar.gz")
        util.downloadUrl(S3_ipython_tar, Local_ipython_tar_file)
        for host in hostlist:
            if host != HDFS.getGateway():
                Machine.copyFromLocal(Local_ipython_tar_file, Local_ipython_tar_file)
            extract_tar = "tar -xf %s -C %s" % (Local_ipython_tar_file, Local_ipyhon_dir)
            Machine.runas(Machine.getAdminUser(), extract_tar, host=host, passwd=Machine.getAdminPasswd())
            Machine.chmod(
                "777",
                Local_ipyhon_dir,
                recursive=True,
                user=Machine.getAdminUser(),
                host=host,
                passwd=Machine.getAdminPasswd(),
                logoutput=True
            )
            Ipython_setup = "cd %s; python setup.py install" % os.path.join(Local_ipyhon_dir, "ipython-1.2.1")
            Machine.runas(
                Machine.getAdminUser(),
                Ipython_setup,
                host=host,
                cwd=os.path.join(Local_ipyhon_dir, "ipython-1.2.1"),
                passwd=Machine.getAdminPasswd()
            )

    @classmethod
    @TaskReporter.report_test()
    def createParquetFilefromJson(cls, json_file, parquet_dir=None, mode="local"):
        '''
        Create Parquet file from Json file
        :param json_file: Json source file on HDFS
        :return: Parquet file location on HDFS
        '''
        if not parquet_dir:
            parquet_dir = "/tmp/parquet_src"
        HDFS.deleteDirectory(parquet_dir)
        cmd = "val sqlContext = new org.apache.spark.sql.SQLContext(sc); val df = sqlContext.jsonFile(\\\"%s\\\"); " \
              "val df2 = df.repartition(1); df2.saveAsParquetFile(\\\"%s\\\")" % (json_file, parquet_dir)
        exit_code, _ = cls.submitInteractiveSparkshell(cmd, mode=mode)
        if exit_code == 0:
            _, stdout = HDFS.lsr(parquet_dir, recursive=False)
            m = re.search("part-r-00000.*.parquet", stdout)
            if m:
                return parquet_dir + "/" + m.group()
            else:
                return None
        else:
            return None

    @classmethod
    @TaskReporter.report_test()
    def getSelectedNodes(cls, selection):
        '''
        Find out selected nodes to modify config using "services" or "nodes"
        :param selection:
        :return:
        '''
        selnodes = []
        if selection.has_key('nodes'):
            selnodes.extend(selection['nodes'])
        if selection.has_key('services'):
            for service in selection['services']:
                if service == 'all':
                    return cls.getSelectedNodes(
                        {
                            'services': [
                                'jobtracker', 'tasktrackers', 'historyserver', 'gateway', 'sparkhistoryserver'
                            ]
                        }
                    )
                elif service == 'sparkhistoryserver':
                    selnodes.append(cls.getSparkHistoryServerHostname())
                elif service == 'jobtracker':
                    selnodes.append(MAPRED.getJobtracker())
                elif service == 'tasktrackers':
                    selnodes.extend(MAPRED.getTasktrackers())
                elif service == 'historyserver':
                    selnodes.append(MAPRED.getHistoryserver())
                elif service == 'gateway':
                    selnodes.append(HDFS.getGateway())
                elif service == 'timelineserver':
                    selnodes.append(YARN.getATSHost())
                elif service == 'sparkthriftserver':
                    selnodes.append(cls.getThriftServerHost())
        return list(set(selnodes))

    @classmethod
    @TaskReporter.report_test()
    def modifyConfig(cls, changes, nodeSelection, isFirstUpdate=True, makeCurrConfBackupInWindows=True):
        '''
        Modify spark configs and modified config will be saved at /tmp/sparkConf
        :param changes:
        :param nodeSelection:
        :param isFirstUpdate:
        :param makeCurrConfBackupInWindows:
        :param regenServiceXmlsInWindows:
        :return:
        '''
        nodes = cls.getSelectedNodes(nodeSelection)
        spark_conf = cls.getSparkConfDir()
        tmp_conf = cls.getModifiedConfigPath()
        configUtils.modifyConfig(
            changes,
            spark_conf,
            tmp_conf,
            nodes,
            isFirstUpdate,
            makeCurrConfBackupInWindows=makeCurrConfBackupInWindows,
            id="2"
        )

    @classmethod
    def getAmbariServiceName(cls):
        if cls.isSpark2():
            return 'SPARK2'
        else:
            return 'SPARK'

    @classmethod
    @TaskReporter.report_test()
    def getAmbariComponentName(cls, service):
        if service == 'sparkthriftserver':
            if cls.isSpark2():
                return 'SPARK2_THRIFTSERVER'
            else:
                return 'SPARK_THRIFTSERVER'
        return None

    @classmethod
    def getServiceHosts(cls, service):
        return Ambari.getHostsForComponent(cls.getAmbariComponentName(service))

    @classmethod
    @TaskReporter.report_test()
    def startService(cls, services):
        for service in services:
            hosts = cls.getServiceHosts(service)
            for host in hosts:
                Ambari.startComponent(host, cls.getAmbariComponentName(service), waitForCompletion=True)
        time.sleep(30)

    @classmethod
    @TaskReporter.report_test()
    def stopService(cls, services):
        for service in services:
            hosts = cls.getServiceHosts(service)
            for host in hosts:
                Ambari.stopComponent(host, cls.getAmbariComponentName(service), waitForCompletion=True)
        time.sleep(15)

    @classmethod
    @TaskReporter.report_test()
    def restartServices(cls, services):
        cls.stopService(services)
        cls.startService(services)

    @classmethod
    @TaskReporter.report_test()
    def getStartingAmbariServiceConfigVersion(cls, update=False):
        if cls.isSpark2():
            if cls._ambariSpark2ServiceConfigVersion is None or update:
                cls._ambariSpark2ServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(
                    cls.getAmbariServiceName()
                )
            return cls._ambariSpark2ServiceConfigVersion
        else:
            if cls._ambariSparkServiceConfigVersion is None or update:
                cls._ambariSparkServiceConfigVersion = Ambari.getCurrentServiceConfigVersion(
                    cls.getAmbariServiceName()
                )
            return cls._ambariSparkServiceConfigVersion

    @classmethod
    @TaskReporter.report_test()
    def modifyConfigUsingAmbari(cls, changes, services, restartService=True):
        logger.info("Before Modify Service Config Version: %s", cls.getStartingAmbariServiceConfigVersion())
        svcName = cls.getAmbariServiceName()
        for key, value in changes.items():
            if cls._ambariConfigMap[svcName].has_key(key):
                key = cls._ambariConfigMap[svcName][key]
            else:
                logger.warn("Unknown config \"%s\" change requested, ignoring", key)
                continue
            Ambari.setConfig(key, value)
        if restartService:
            cls.restartServices(services)

    @classmethod
    @TaskReporter.report_test()
    def restoreConfigUsingAmbari(cls, services, restartService=True):
        resetVersion = cls.getStartingAmbariServiceConfigVersion()
        logger.info("Restoring Service Config Version to: %s", resetVersion)
        Ambari.resetConfig(cls.getAmbariServiceName(), resetVersion)
        if restartService:
            cls.restartServices(services)

    @classmethod
    @TaskReporter.report_test()
    def getSparkJarsForDoAsEnabled(cls):
        objenesis_jar = os.path.join(Config.get('hadoop', 'YARN_HOME'), 'lib', 'objenesis-2.1.jar')
        spark_lib_dir = os.path.join(cls.getSparkHome(), "lib")
        datanucleus_jars = util.findMatchingFiles(spark_lib_dir, "datanucleus-*.jar", depth=1)
        datanucleus_jars.append(objenesis_jar)
        return (",").join(datanucleus_jars)

    @classmethod
    def getModifiedConfigPath(cls):
        '''
        :return: modified spark conf location
        '''
        return os.path.join(Machine.getTempDir(), 'sparkConf')

    @classmethod
    @TaskReporter.report_test()
    def restoreConfig(cls, changes, nodeSelection):
        '''
        Restore config.
        changes is a list.
        '''
        spark_conf = cls.getSparkConfDir()
        nodes = cls.getSelectedNodes(nodeSelection)
        if Machine.isLinux():
            configUtils.restoreConfig(changes, spark_conf, nodes)
        else:
            if Machine.pathExists(user=None, host=None, filepath=configUtils.getBackupConfigLocation(), passwd=None):
                configUtils.restoreConfig(changes, spark_conf, nodes, id="2")

    @classmethod
    @TaskReporter.report_test()
    def copyConfFileToSparkConf(cls, src_files, hosts=None):
        '''
        Copy over config files such as hbase-site.xml to /etc/spark/conf
        :param src_files: list of config files which needs to be copied over /etc/spark/conf such as
        ['hdfs-site.xml', 'hbase-site.xml']
        :param hosts: list of hosts where copy operation should happen. By default, it will be copied over on all hosts
        :return:
        '''
        if not hosts:
            hosts = Hadoop.getAllNodes()
        for host in hosts:
            for filename in src_files:
                Machine.copyFromLocal(
                    Machine.getAdminUser(), host, filename, cls.getSparkConfDir(), passwd=Machine.getAdminPasswd()
                )

    @classmethod
    @TaskReporter.report_test()
    def setupHBaseHost2(cls, host_2, user):
        '''
        Setup Hbase Host2 in Multicluster env.
        This function will create /users/<user> dir on hdfs with proper permissions
        Also grant permission for <user> in hbase-shell.
        :param host_2:  Host2 is Hbase2 cluster gateway
        :param user: The user which need to be set
        :return:
        '''
        create_user_dir = "hdfs dfs -mkdir /user/" + user
        chown_cmd = "hdfs dfs -chown %s:hadoop /user/%s" % (user, user)
        chmod_cmd = "hdfs dfs -chmod 755 /user/%s" % user
        if Hadoop.isSecure():
            hdfskinitcmd = '%s -kt %s %s' % (
                Machine.getKinitCmd(), Machine.getHeadlessUserKeytab(HDFS.getHDFSUser()), HDFS.getHDFSUser()
            )
            create_user_dir = hdfskinitcmd + ";" + create_user_dir
            chown_cmd = hdfskinitcmd + ";" + chown_cmd
            chmod_cmd = hdfskinitcmd + ";" + chmod_cmd

        exit_code, _ = Machine.runas(HDFS.getHDFSUser(), create_user_dir, host=host_2)
        exit_code, _ = Machine.runas(HDFS.getHDFSUser(), chown_cmd, host=host_2)
        exit_code, _ = Machine.runas(HDFS.getHDFSUser(), chmod_cmd, host=host_2)
        # Grant hrt_qa user permission in Remote Hbase cluster
        from beaver.component.hbase import HBase
        grant_permission = "grant '" + user + "', 'RWXCA'"
        exit_code, _ = HBase.runShellCmds([grant_permission], user=Config.get('hbase', 'HBASE_USER'), host=host_2)
        assert exit_code == 0, "Hbase grant permission failed on remote gateway (%s)" % host_2

    @classmethod
    def getNexusPrivateRepo(cls):
        '''
        Get nexus private repo url
        :return:
        '''
        return "http://nexus-private.hortonworks.com/nexus/content/repositories/IN-QA/"

    @classmethod
    def getSHCNexusRepo(cls):
        '''
        get SHC nexus repo url
        :return:
        '''
        return "http://nexus-private.hortonworks.com/nexus/content/groups/public"

    @classmethod
    @TaskReporter.report_test()
    def getSHCPackage(cls):
        '''
        Get SHC package name
        :return:
        '''
        if cls.isSpark2():
            if cls.isSpark2_2_or_beyond():
                PACKAGE = "com.hortonworks.shc:shc-examples:1.1.0.%s,org.apache.hadoop:hadoop-aws:3.1.1.%s" \
                          % (cls.getShortVersion(), cls.getShortVersion())
            else:
                PACKAGE = "com.hortonworks:shc-examples:1.1.3-2.3-s_2.11"
        else:
            PACKAGE = "com.hortonworks:shc-examples:1.1.1-1.6-s_2.10"
        return PACKAGE

    @classmethod
    @TaskReporter.report_test()
    def getSparkATSURL(cls):
        '''
        :return: http://<timelinehost>:<port>/ws/v1/timeline/spark_event_v01
        '''
        return YARN.get_ats_web_app_address() + "/ws/v1/timeline/spark_event_v01/"

    @classmethod
    @TaskReporter.report_test()
    def getSparkATSAppUrl(cls, appId, mode="yarn-client"):
        '''
        Returns ATS url for appId
        :param appId:  application Id
        :return: http://<timelinehost>:<port>/wz/v1/timeline/spark_event_v01/<appId>
        '''
        if mode == "yarn-cluster":
            attemptIdStr = YARN.createAttemptIdFromAppId(appId, "1")
            return cls.getSparkATSURL() + attemptIdStr
        else:
            return cls.getSparkATSURL() + appId

    @classmethod
    @TaskReporter.report_test()
    def hitSparkURL(cls, forceSparkVersion=None):
        time.sleep(90)
        url = cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion) + "/?page=1&showIncomplete=false"
        tmp_file = cls.createTmpClientFile("tmp_content")
        util.getURLContents(url, outfile=tmp_file, headers=None, logoutput=False)
        time.sleep(60)

    @classmethod
    @TaskReporter.report_test()
    def getCorrectApplicationJsonData(cls, appId, spark_ATS_url=None, user=None, gatherAppSpecificJson=True):
        '''
        Returns Json dataset for applicationId
        :param appId: application_Id
        :return:
        '''
        if not spark_ATS_url:
            spark_ATS_url = cls.getSparkATSURL()
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        _, stdout, _ = util.query_yarn_web_service(spark_ATS_url, user)
        if gatherAppSpecificJson:
            for entities in stdout["entities"]:
                if appId == entities["entity"]:
                    return entities
        else:
            return stdout
        return False

    @classmethod
    @TaskReporter.report_test()
    def getMetricsJsonData(cls, appId, user="hrt_qa"):
        keytabFile = Machine.getHeadlessUserKeytab(user)
        kinitCmd = '%s -R -kt %s %s' % (Machine.getKinitCmd(), keytabFile, Machine.get_user_principal(user))
        exitCode, _ = Machine.runas("hrt_qa", kinitCmd)
        assert exitCode == 0, "Unable to execute the kinit cmd: " + kinitCmd
        from requests_kerberos import HTTPKerberosAuth, OPTIONAL
        if Hadoop.isUIKerberozied:
            auth = HTTPKerberosAuth(
                mutual_authentication=OPTIONAL, sanitize_mutual_error_response=False, force_preemptive=True
            )
        else:
            auth = None
        metrics_json_url = "%s/proxy/%s/metrics/json" % (YARN.getResourceManagerWebappAddress(), appId)
        logger.info("metrics JSON URL: %s", metrics_json_url)
        r = requests.get(metrics_json_url, auth=auth, verify=False)
        hadoop_auth_cookie = re.findall('"([^"]*)"', r.headers['Set-Cookie'])[0]
        logger.info("**** hadoop_auth_cookie = %s", str(hadoop_auth_cookie))
        cookie = {'hadoop.auth': hadoop_auth_cookie}
        r = requests.get(metrics_json_url, auth=auth, cookies=cookie, verify=False)
        assert r.status_code == 200, "Failed to get hadoop_auth_cookie, Response code = " + str(r.status_code)
        logger.info("**** r.content = %s", str(r.content))
        return r.content

    @classmethod
    @TaskReporter.report_test()
    def waitTillBatchesCompleted(cls, appId, num, user="hrt_qa"):
        compl_batches = 0
        while compl_batches < num:
            metrics = cls.getMetricsJsonData(appId, user=user)
            matches = re.search(r"totalCompletedBatches\"\:\{\"value\"\:(\d+)\}", metrics)
            if matches:
                compl_batches = int(matches.group(1))
            logger.info("completed batches: %s", compl_batches)

    @classmethod
    @TaskReporter.report_test()
    def matchparamater(cls, actualoutput, regex):
        '''
        Function to validate if actualout fit into regex
        :param actualoutput: "1234567891234"
        :param regex: "[0-9]{13}"
        :return: Returns True if actualoutput fit into regex, otherwise False
        '''
        m = re.search(regex, str(actualoutput))
        return bool(m)

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerJobEnd(cls, entities):
        '''
        Validate 'SparkListenerJobEnd' event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerJobEnd":
                assert eventtype["eventinfo"]["Job Result"]["Result"] == "JobSucceeded"
                assert cls.matchparamater(eventtype["eventinfo"]["Completion Time"], "[0-9]{13}")

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerStageCompleted(cls, entities):
        '''
        Validate SparkListenerStageCompleted
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerStageCompleted":
                assert eventtype["eventinfo"]["Event"] == "SparkListenerStageCompleted"
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Submission Time"], "[0-9]{13}")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Number of Tasks"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Stage ID"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Completion Time"], "[0-9]{13}")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["RDD ID"], "[0-9]+")
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Use Disk"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Use Memory"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Deserialized"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Replication"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Number of Partitions"], "[0-9]+"
                )

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerTaskEnd(cls, entities):
        '''
        Validate SparkListenerTaskEnd event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerTaskEnd":
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Input Metrics"], "None")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Result Size"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Memory Bytes Spilled"], "0")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Disk Bytes Spilled"], "0")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Executor Run Time"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Shuffle Read Metrics"], "None")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Metrics"]["Shuffle Write Metrics"], "None")
                assert cls.matchparamater(eventtype["eventinfo"]["Task End Reason"]["Reason"], "Success")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Attempt"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Index"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Locality"], "PROCESS_LOCAL")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Finish Time"], "[0-9]{13}")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Failed"], "False")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Speculative"], "False")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Getting Result Time"], "0")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Attempt"], "[0-9]+")

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerTaskStart(cls, entities):
        '''
        Validate SparkListenerTaskStart event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerTaskStart":
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Attempt"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Index"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Locality"], "PROCESS_LOCAL")
                #assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Finish Time"], "[0-9]{13}")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Failed"], "False")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Speculative"], "False")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Getting Result Time"], "0")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Attempt"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Task Info"]["Task ID"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Attempt ID"], "[0-9]+")

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerBlockManagerAdded(cls, entities):
        '''
        Validate SparkListenerBlockManagerAdded event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerBlockManagerAdded":
                assert cls.matchparamater(eventtype["eventinfo"]["Block Manager ID"]["Port"], "[0-9]+")
                #assert cls.matchparamater(eventtype["eventinfo"]["Block Manager ID"]["Executor ID"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Maximum Memory"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Timestamp"], "[0-9]{13}")

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerStageSubmitted(cls, entities):
        '''
        Validate SparkListenerStageSubmitted event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerStageSubmitted":
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Completion Time"], "None")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Stage ID"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["Number of Tasks"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["RDD ID"], "[0-9]+")
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Use Disk"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Use Memory"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Deserialized"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Storage Level"]["Replication"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Number of Cached Partitions"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Info"]["RDD Info"][0]["Number of Partitions"], "[0-9]+"
                )

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerJobStart(cls, entities):
        '''
        Validate SparkListenerJobStart event
        :param entities: Its output from getCorrectApplicationJsonData
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerJobStart":
                assert cls.matchparamater(eventtype["eventinfo"]["Stage IDs"], "[0]")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["Completion Time"], "None")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["Stage ID"], "[0-9]+")
                #assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["Number of Task"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["Stage Attempt ID"], "[0-9]+")
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Name"], "[0-9]+")
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Disk Size"], "[0-9]+"
                )
                assert cls.matchparamater(eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["RDD ID"], "[0-9]+")
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Tachyon Size"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Number of Cached Partitions"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Memory Size"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Number of Partitions"], "[0-9]+"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Storage Level"]["Use Disk"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Storage Level"]["Use Memory"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Storage Level"]["Deserialized"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Storage Level"]["Use Tachyon"], "False"
                )
                assert cls.matchparamater(
                    eventtype["eventinfo"]["Stage Infos"][0]["RDD Info"][0]["Storage Level"]["Replication"], "[0-9]+"
                )

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerApplicationStart(cls, entities, user=None, appId=None, appName=None):
        '''
        Validate SparkListenerApplicationStart event
        :param entities:
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"].find("SparkListenerApplicationStart") != -1:
                logger.info("start event: %s", eventtype["eventtype"])
                assert cls.matchparamater(eventtype["eventinfo"]["User"], user), \
                    "Failed to validate User name in SparkListenerApplicationStart"
                assert cls.matchparamater(eventtype["eventinfo"]["App ID"], appId), \
                    "Failed to validate Application ID in SparkListenerApplicationStart"
                assert cls.matchparamater(eventtype["eventinfo"]["App Name"], appName), \
                    "Failed to validate Application Name in SparkListenerApplicationStart"
                assert cls.matchparamater(eventtype["eventinfo"]["Timestamp"], "[0-9]{13}"), \
                    "Failed to validate Timestamp in SparkListenerApplicationStart"

    @classmethod
    @TaskReporter.report_test()
    def validate_SparkListenerApplicationEnd(cls, entities):
        '''
        Validate SparkListenerApplicationEnd event
        :param entities:
        :return:
        '''
        for eventtype in entities["events"]:
            if eventtype["eventtype"] == "SparkListenerApplicationEnd":
                assert cls.matchparamater(eventtype["eventinfo"]["Timestamp"], "[0-9]{13}")
                assert cls.matchparamater(eventtype["eventinfo"]["Event"], "SparkListenerApplicationEnd")

    @classmethod
    @TaskReporter.report_test()
    def validate_ApplicationEntry(cls, entities, appId, appName, appUser, mode="yarn-client"):
        '''
        Validate Application entry
        :param entities: Its output from getCorrectApplicationJsonData
        :param appId: Application Id
        :param appName: Application name
        :param appUser: Application user
        :return:
        '''
        if mode == "yarn-cluster":
            assert entities["entity"] == YARN.createAttemptIdFromAppId(appId, "1")
        else:
            assert entities["entity"] == appId
        assert entities["domain"] == "DEFAULT"
        assert entities["entitytype"] == "spark_event_v01"
        #assert entities["primaryfilters"]["appName"] == [appName] or appName
        #assert entities["primaryfilters"]["appUser"] == [appUser]
        if not Machine.isHumboldt():
            assert entities["primaryfilters"]["endApp"] == ['SparkListenerApplicationEnd']
            assert entities["primaryfilters"]["startApp"] == ['SparkListenerApplicationStart']
        if not Machine.isLinux() and appName == "Spark Pi":
            assert entities["otherinfo"]["appName"] == "SparkPi"
        else:
            assert entities["otherinfo"]["appName"] == appName
        assert entities["otherinfo"]["appUser"] == appUser
        assert cls.matchparamater(entities["otherinfo"]["startTime"], "[0-9]{13}")
        assert cls.matchparamater(entities["otherinfo"]["endTime"], "[0-9]{13}")

    @classmethod
    @TaskReporter.report_test()
    def validateApplicationJsonResponse(cls, appId, appName, appUser, url=None, mode="yarn-client"):
        '''
        Validate below fields are present in SPARK ATS response
        :param response: Response from getCorrectApplicationJsonData
        :param appId: ApplicationId
        :param appName: Application Name
        :param appUser: Application User
        :param url: http://<timelinehost>:<port>/wz/v1/timeline/spark_event_v01 or
        http://<timelinehost>:<port>/wz/v1/timeline/spark_event_v01/appId
        :return:
        '''
        if not url:
            entities = cls.getCorrectApplicationJsonData(appId)
        else:
            entities = cls.getCorrectApplicationJsonData(appId, url, gatherAppSpecificJson=False)

        logger.info(entities)
        cls.validate_ApplicationEntry(entities, appId, appName, appUser, mode)
        cls.validate_SparkListenerJobEnd(entities)
        cls.validate_SparkListenerStageCompleted(entities)
        cls.validate_SparkListenerTaskEnd(entities)
        cls.validate_SparkListenerTaskStart(entities)
        cls.validate_SparkListenerBlockManagerAdded(entities)
        cls.validate_SparkListenerStageSubmitted(entities)
        cls.validate_SparkListenerJobStart(entities)
        cls.validate_SparkListenerApplicationStart(entities, user=appUser, appId=appId, appName=appName)
        cls.validate_SparkListenerApplicationEnd(entities)

    @classmethod
    @TaskReporter.report_test()
    def validateRowFromTable(cls, row, dict_exp):
        '''
        Validate Table Row from a table
        :param row: ['application_1432682719087_0034', 'Spark Pi', '2015/05/29 02:17:30', '2015/05/29 02:18:06',
        '36 s', 'hrt_qa', '2015/05/29 02:18:06']
        :param dict_exp: Expected dictionary - {"0": appId, "1":appName, "5":appUser}
        :return: True if its met or False
        '''
        isPresent = {}
        for key in dict_exp:
            logger.info(key)
            logger.info(dict_exp[key])
            logger.info(row[int(key)])
            if row[int(key)] == dict_exp[key]:
                isPresent[key] = "True"
            else:
                isPresent[key] = "False"
        logger.info(isPresent)
        for item in isPresent:
            if isPresent[item] == "False":
                return False
        return True

    @classmethod
    @TaskReporter.report_test()
    def validateSparkHSURL(cls, url, dict_exp):
        '''
        Its common function to validate spark HS
        :param url: URL
        :param dict_exp: dict_to_expect
        :return:
        '''
        tmp_file = cls.createTmpClientFile("tmp_url_content")
        util.getURLContents(url, outfile=tmp_file, headers=None, logoutput=False)
        parser = SparkHTMLParser()
        f = open(tmp_file)
        t = f.read()
        parser.feed(t)

        for row in parser.rowData:
            logger.info("row of url contents --- %s", row)
            if '' in row:
                row = [x for x in row if x != '']
            result = cls.validateRowFromTable(row, dict_exp)
            if result:
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def validateSparkHSCompletedApps(cls, appId, appName, appUser):
        '''
        Validate if application is present in http://<host>:<port>/?page=1&showIncomplete=false
        It checkes Appid, AppName and AppUser
        :param appId: Application Id
        :param appName: Application Name
        :param appUser: Application User
        :return:
        '''
        # dictonary with expected output from Table
        # dic_exp = {"0":appId} which means index 0 from table is expected to have appId
        dict_exp = {"0": appId, "1": appName, "5": appUser}
        url = cls.getSparkHistoryServerUrl() + "/?page=1&showIncomplete=false"
        logger.info("url = %s", url)
        return cls.validateSparkHSURL(url, dict_exp)

    @classmethod
    @TaskReporter.report_test()
    def validateSparkHSIncompletedApps(cls, appId, appName, appUser):
        '''
        Validate if application is present in http://<host>:<port>/?page=1&showIncomplete=true
        :param appId:
        :param appName:
        :param appUser:
        :return:
        '''
        dict_exp = {"0": appId, "1": appName, "5": appUser}
        url = cls.getSparkHistoryServerUrl() + "/?page=1&showIncomplete=true"
        return cls.validateSparkHSURL(url, dict_exp)

    @classmethod
    @TaskReporter.report_test()
    def validateSparkHSJobs(cls, appId, stages, task, attempt=1):
        '''
        Validate http://host:18080/history/<appId>/1/jobs/ page with expected stages and task
        :param appId:
        :param stages:
        :param task:
        :return:
        '''
        dict_exp = {"4": stages, "5": task}
        url = "%s/history/%s/%s/jobs" % (cls.getSparkHistoryServerUrl(), appId, attempt)
        # "http://"+cls.getSparkHistoryServerAddress()+"/history/"+appId+"/1/jobs/"
        return cls.validateSparkHSURL(url, dict_exp)

    @classmethod
    @TaskReporter.report_test()
    def validateSparkStages(cls, appId, stages, mode, attempt=1):
        '''
        Validate http://host:18080/history/appId/stages/
        :param appId:
        :param stages:
        :return:
        '''
        dict_exp = {"4": stages}
        if mode == "yarn-cluster":
            url = "%s/history/%s/%s/stages" % (cls.getSparkHistoryServerUrl(), appId, attempt)
        else:
            url = "%s/history/%s/stages" % (cls.getSparkHistoryServerUrl(), appId)
            # "http://"+cls.getSparkHistoryServerAddress()+"/history/"+appId+"/stages"
        return cls.validateSparkHSURL(url, dict_exp)

    @classmethod
    @TaskReporter.report_test()
    def find_active_executor_host(cls, appId, amhost, mode="yarn-client", attempt=1, forceSparkVersion=None):
        '''
        Function to find host running active executor
        :param appId: application Id
        :param amhost: AM Host, which will be skipped while finding host with an active exec
        :return: hostname where active executor is running
        '''
        if mode == "yarn-client":
            url = "%s/api/v1/applications/%s/executors" % (
                cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion), appId
            )
        else:
            url = "%s/api/v1/applications/%s/%s/executors" % (
                cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion), appId, attempt
            )
        logger.info("*** Getting Executors from %s ***", url)
        tmp_file = cls.createTmpClientFile("tmp_json.json")
        util.getURLContents(url, outfile=tmp_file, headers=None, logoutput=False)
        text = open(tmp_file).read()
        data = json.loads(text)
        for executor in data:
            if executor["id"].isdigit():
                activeTasks = int(executor["activeTasks"])
                host = executor["hostPort"].split(":")[0]
                logger.info("host %s and num_active_task %s", host, activeTasks)
                if activeTasks > 0 and host != amhost:
                    return host
        return None

    @classmethod
    @TaskReporter.report_test()
    def find_count_of_executors(cls, appId, mode, attempt=1, forceSparkVersion=None):
        '''
        Find total number of executors for an Application from SparkHS
        :return:
        '''
        count = 0
        if mode == "yarn-client":
            url = "%s/api/v1/applications/%s/executors" % (
                cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion), appId
            )
        else:
            url = "%s/api/v1/applications/%s/%s/executors" % (
                cls.getSparkHistoryServerUrl(forceSparkVersion=forceSparkVersion), appId, attempt
            )
        logger.info("*** Getting Executors from %s ***", url)
        tmp_file = cls.createTmpClientFile("tmp_json.json")
        util.getURLContents(url, outfile=tmp_file, headers=None, logoutput=False)
        text = open(tmp_file).read()
        data = json.loads(text)
        for executor in data:
            if executor["id"].isdigit():
                logger.info(executor)
                count = count + 1
        return count

    @classmethod
    @TaskReporter.report_test()
    def validateDRAExecutorsNumbers(cls, appId, mode, config=None, exp_failures=False):
        '''
        Validate DRA Executors number is between (min, max)
        :return:
        '''
        if not config:
            config = cls.getSparkConfDir()
        cls.hitSparkURL()
        count = cls.find_count_of_executors(appId, mode)
        minimum = int(
            cls.getPropertyValueFromSparkDefaultConfFile(
                os.path.join(config, "spark-defaults.conf"), "spark.dynamicAllocation.minExecutors"
            )
        )
        maximum = int(
            cls.getPropertyValueFromSparkDefaultConfFile(
                os.path.join(config, "spark-defaults.conf"), "spark.dynamicAllocation.maxExecutors"
            )
        )
        logger.info("Total executors = %s", count)
        if (count < minimum and exp_failures):
            pytest.xfail("Expected failure due to race condition as mentioned in QE-7630")
        else:
            assert int(count) in range(minimum, maximum + 1), "executors are not in range"
        # container num can not be accurate. thus removing this check
        #containers_allocated = YARN.countContinersinApp(appId)
        #assert containers_allocated in range(min, max + 2), "containers launched by RM is in range"

    @classmethod
    @TaskReporter.report_test()
    def InstallR(cls, allHosts=None):
        '''
        Set up for Spark R
        Install R on all hosts.
        :return:
        '''
        ADMIN_USER = Machine.getAdminUser()
        ADMIN_PWD = Machine.getAdminPasswd()
        if not allHosts:
            if Machine.isHumboldt():
                allHosts = util.getAllNodes()
            else:
                allHosts = Hadoop.getAllNodes()

        if (Machine.isUbuntu() and not Machine.isUbuntu16()) or Machine.isDebian():
            packagename = "gfortran"
            Machine.installPackages(packages=packagename, hosts=allHosts, logoutput=True)
            if Machine.isHumboldt():
                packagename = "g77"
                Machine.installPackages(packages=packagename, hosts=allHosts, logoutput=True)
            for host in allHosts:
                if Machine.isDebian():
                    utf_cmd = "localedef -i en_US -f UTF-8 en_US.UTF-8"
                    Machine.runas(user=ADMIN_USER, cmd=utf_cmd, host=host, passwd=ADMIN_PWD)
                R_install_dir = os.path.join(Machine.getTempDir(), "myR")
                Machine.makedirs(user=ADMIN_USER, host=host, filepath=R_install_dir, passwd=ADMIN_PWD)
                Machine.chmod(
                    perm="777", filepath=R_install_dir, recursive=True, user=ADMIN_USER, host=host, passwd=ADMIN_PWD
                )
                wget_cmd = "cd %s; wget http://s3.amazonaws.com/dev.hortonworks.com/ARTIFACTS/R-3.2.2.tar.gz" % \
                           R_install_dir
                Machine.runas(user=ADMIN_USER, cmd=wget_cmd, host=host, passwd=ADMIN_PWD)
                Machine.chmod(
                    perm="777", filepath=R_install_dir, recursive=True, user=ADMIN_USER, host=host, passwd=ADMIN_PWD
                )
                extract_tar = "tar -xf %s -C %s" % (os.path.join(R_install_dir, "R-3.2.2.tar.gz"), R_install_dir)
                Machine.runas(user=ADMIN_USER, cmd=extract_tar, host=host, passwd=ADMIN_PWD)
                cmd = "cd %s; ./configure --with-x=no --with-readline=no; make; make install" % os.path.join(
                    R_install_dir, "R-3.2.2"
                )
                Machine.runas(user=ADMIN_USER, cmd=cmd, host=host, passwd=ADMIN_PWD)
        elif Machine.isUbuntu16():
            packagename = "r-base"
            Machine.installPackages(packages=packagename, hosts=allHosts, logoutput=True)
        elif Machine.isSuse_12():
            add_repo = "zypper addrepo -f https://download.opensuse.org/repositories/devel:/languages:/R:/released" \
                       "/SLE_12 R-core "
            refresh_cmd = "zypper --gpg-auto-import-keys refresh"
            for host in allHosts:
                Machine.runas(user=ADMIN_USER, cmd=add_repo, host=host, passwd=ADMIN_PWD)
                Machine.runas(user=ADMIN_USER, cmd=refresh_cmd, host=host, passwd=ADMIN_PWD)
            packagename = "R"
            Machine.installPackages(packages=packagename, hosts=allHosts, logoutput=True)
        else:
            packagename = "R"
            Machine.installPackages(packages=packagename, hosts=allHosts, logoutput=True)

    @classmethod
    @TaskReporter.report_test()
    def InstallNumpy(cls, allHosts=None):
        '''
        Install Numpy 1.6.1 on all nodes
        :param allHosts:
        :return:
        '''
        if not allHosts:
            if Machine.isHumboldt():
                allHosts = util.getAllNodes()
            else:
                allHosts = Hadoop.getAllNodes()

        if Machine.isLinux():
            if Machine.isHumboldt():
                cmd = "pip install numpy==1.6.1 --upgrade"
                for host in allHosts:
                    (_, _) = Machine.runas(
                        user=Machine.getAdminUser(),
                        cmd=cmd,
                        host=host,
                        cwd=None,
                        env=None,
                        logoutput=True,
                        passwd=Machine.getAdminPasswd()
                    )

            elif Machine.isIBMPowerPC():
                Machine.installPackages(packages="numpy.ppc64le", hosts=allHosts, logoutput=True)

            elif Machine.isSuse():
                logger.info("Install numpy 1.6.1 using tar gz")
                url = "http://qe-repo.s3.amazonaws.com/numpy-1.6.1.tar.gz?AWSAccessKeyId=AKIAIDZB7JJ5YBYI23KQ"
                Local_numpy_dir = os.path.join(Machine.getTempDir(), "numpy")
                for host in allHosts:
                    Machine.makedirs(None, host, Local_numpy_dir)
                    Machine.chmod(
                        "777",
                        Local_numpy_dir,
                        recursive=True,
                        user=Machine.getAdminUser(),
                        host=host,
                        passwd=Machine.getAdminPasswd(),
                        logoutput=True
                    )
                Local_numpy_tar_file = os.path.join(Local_numpy_dir, "numpy-1.6.1.tar.gz")
                util.downloadUrl(url, Local_numpy_tar_file)
                for host in allHosts:
                    if not Machine.isSameHost(host, HDFS.getGateway()):
                        Machine.copyFromLocal(None, host, Local_numpy_tar_file, Local_numpy_tar_file)
                    extract_tar = "tar -xf %s -C %s" % (Local_numpy_tar_file, Local_numpy_dir)
                    Machine.runas(Machine.getAdminUser(), extract_tar, host=host, passwd=Machine.getAdminPasswd())
                    Machine.chmod(
                        "777",
                        Local_numpy_dir,
                        recursive=True,
                        user=Machine.getAdminUser(),
                        host=host,
                        passwd=Machine.getAdminPasswd(),
                        logoutput=True
                    )
                    numpy_build = "cd %s; python setup.py build" % os.path.join(Local_numpy_dir, "numpy-1.6.1")
                    Machine.runas(
                        Machine.getAdminUser(), numpy_build, host=host, cwd=None, passwd=Machine.getAdminPasswd()
                    )
                    numpy_setup = "cd %s; python setup.py install" % os.path.join(Local_numpy_dir, "numpy-1.6.1")
                    Machine.runas(
                        Machine.getAdminUser(), numpy_setup, host=host, cwd=None, passwd=Machine.getAdminPasswd()
                    )
            else:
                Machine.installPackageWithPip(packages="numpy==1.6.1", hosts=allHosts, logoutput=True)

    @classmethod
    @TaskReporter.report_test()
    def InstallNetcat(cls):
        logger.info("Install Netcat in Gateway")
        host = HDFS.getGateway()
        url = "http://qe-repo.s3.amazonaws.com/nc111nt.zip?AWSAccessKeyId=" \
              "AKIAIDZB7JJ5YBYI23KQ&Expires=1521055986&Signature=8WAyg2O8fi6x%2Bwy9X2xcQI0yBy8%3D"
        Local_Netcat_dir = os.path.join(Machine.getTempDir(), "netcat")
        Machine.makedirs(None, host, Local_Netcat_dir)
        Local_Netcat_zip_file = os.path.join(Local_Netcat_dir, "nc111nt.zip")
        util.downloadUrl(url, Local_Netcat_zip_file)
        Machine.unzipExtractAll(Local_Netcat_zip_file, Local_Netcat_dir)
        Machine.chmod(
            "777",
            Local_Netcat_dir,
            recursive=True,
            user=Machine.getAdminUser(),
            host=host,
            passwd=Machine.getAdminPasswd(),
            logoutput=True
        )

    @classmethod
    @TaskReporter.report_test()
    def find_shuffle_data_file(cls, appId, user=None, stdout=True):
        '''
        Find shuffle data files for an application.
        Typically shuffle files can be found from yarn local logs of one of the containers
        Example path : <Yarn local dir>/usercache/hrt_qa/appcache/application_1489461639669_0012/
        blockmgr-77a97a8b-155b-419a-9f27-492f781167d9/0c/shuffle_0_0_0.data
        :param appId: ApplicationId
        :return: (host, Path of the shuffle data file)
        '''
        yarn_local_dirs = YARN.getLocalDirs()
        if stdout:
            logger.info("Yarn local dirs = %s", yarn_local_dirs)
        attemptIdList = YARN.getAttemptIdList(appId)
        if stdout:
            logger.info("AttemptId List = %s", attemptIdList)
        container_host_map = YARN.getAllContainerHostMapForFinishedApp(appId, attemptIdList[0], user)
        if stdout:
            logger.info("Container host map")
            logger.info(container_host_map)
        for container, host in container_host_map.iteritems():
            if stdout:
                logger.info("Iterating %s in %s", container, host)
            for yarn_local_dir in yarn_local_dirs:
                container_local_dir = os.path.join(yarn_local_dir, "usercache", user, "appcache", appId)
                shuffle_files = Machine.find(
                    Machine.getAdminUser(), host, container_local_dir, "shuffle*data", passwd=Machine.getAdminPasswd()
                )
                for shuffle_file in shuffle_files:
                    if Machine.pathExists(Machine.getAdminUser(), host, shuffle_file, Machine.getAdminPasswd()):
                        if stdout:
                            logger.info("Shuffle file found host=%s shufflefile=%s", host, shuffle_file)
                        return host, shuffle_file
        if stdout:
            logger.info("No shuffle file found")
        return None, None

    @classmethod
    @TaskReporter.report_test()
    def validate_shuffle_file_for_encryption(cls, appId, patterns, user=None):
        '''
        Validate if shuffle file is encrypted or not
        If one of the patterns (expected data) are found in shuffle file, that indicates that shuffle file is not
        encrypted
        IF none of of the patterns is found in shuffle file, that indicates that shuffle file is encrypted
        :param appId: Application Id
        :param user: User who launched application
        :param patterns: List of expected data which can be present in shuffle files
        :return:
        '''
        host, shuffle_file = cls.find_shuffle_data_file(appId, user)
        # if shuffle file is not found return false
        if not shuffle_file:
            #suggestion = {"suggestion": "shuffle file is not found"}
            return False
        # copy over shuffle file to gateway
        local_shuffle_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), shuffle_file.split("/")[-1])
        Machine.copyToLocal(Machine.getAdminUser(), host, shuffle_file, local_shuffle_file, Machine.getAdminPasswd())
        Machine.chmod("777", local_shuffle_file, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

        # Look for pattern in the shuffle file
        for pattern in patterns:
            is_present = util.checkTextInFile(pattern, local_shuffle_file)
            if is_present:
                logger.info("%s found in %s", pattern, local_shuffle_file)
                #suggestion = {"suggestion": "%s pattern is not found in %s" % (pattern, local_shuffle_file)}
                return False
        return True


class InteractiveSparkSession(object):
    BASH_EXP = [r".*#", r".*\$", r".*>"]
    STATUS_EXP = [r".*\?.*\$", r".*\?.*#", r".*\?.*>"]
    INTERACTIVE_SHELL_EXP = [">>>", "scala>"]

    # type can be spark-shell, pyspark, ipython
    # mode can be yarn or local
    def __init__(
            self,
            session_type="spark-shell",
            mode="yarn",
            jars=None,
            pymodules=None,
            conf=None,
            propertiesfile=None,
            scalafile=None,
            logfile="default.log",
            user=Config.get('hadoop', 'HADOOPQA_USER'),
            cwd=None,
            env=None,
            host=None,
            force_spark_version='2'
    ):
        if not jars:
            jars = []
        if not pymodules:
            pymodules = []
        if not conf:
            conf = {}
        self.type = session_type
        self.mode = mode
        self.jars = jars
        self.pymodules = pymodules
        self.conf = conf
        self.propfile = propertiesfile
        self.scalafile = scalafile
        self.logfile = logfile
        self.user = user
        self.cwd = cwd
        self.env = env
        self.host = host
        self.force_spark_version = force_spark_version
        self.logs_write_handler = None
        self.logs_read_handler = None
        self.session = self.__init_bash_session()
        assert self.session, "Failed to initialize bash session"
        assert self.__init_shell(), "Failed to start interactive spark shell"

    @TaskReporter.report_test()
    def close(self):
        try:
            if self.session:
                self.session.close()
        except Exception:
            pass
        try:
            if self.logs_write_handler:
                self.logs_write_handler.close()
        except Exception:
            pass
        try:
            if self.logs_read_handler:
                self.logs_read_handler.close()
        except Exception:
            pass

    @TaskReporter.report_test()
    def execute_interactive_spark_cmd(
            self,
            cmd,
            expected_output=None,
            success_pattern=None,
            failure_pattern=r'(?i).*(error|exception).*',
            filter_patterns=None,
    ):
        if not expected_output:
            expected_output = []
        if not filter_patterns:
            filter_patterns = [r'(?i).*warn.*', r'.*Stage\s\d+\:>.*']
        assert not (expected_output and success_pattern), "expected_output and success_pattern can't be specified " \
                                                          "together"
        is_success = False
        self.session.sendline(cmd)
        try:
            self.session.expect(InteractiveSparkSession.INTERACTIVE_SHELL_EXP, timeout=600)
            output = self.logs_read_handler.readlines()
            filtered_output = []

            for line in output:
                filter_required = False
                for pattern in filter_patterns:
                    if re.match(pattern, line):
                        filter_required = True
                        break
                if not filter_required:
                    cline = re.sub(r"\s+", " ", line).strip()
                    if cline:
                        filtered_output.append(cline)

            logger.info("output of current spark command: %s", filtered_output)
            is_success = True

            if expected_output:
                try:
                    for line in expected_output:
                        assert line in filtered_output, "Failed to find line: %s in actual output" % line
                except Exception:
                    is_success = False
            elif success_pattern:
                is_success = False
                for line in filtered_output:
                    if re.match(success_pattern, line):
                        is_success = True
                        break

            if failure_pattern:
                for line in filtered_output:
                    if re.match(failure_pattern, line):
                        logger.error("Interactive spark command: %s execution failed due to error: %s", cmd, line)
                        is_success = False
                        break
        except Exception, py_ex:
            logger.error("Interactive spark command: %s execution failed due to error: %s", cmd, py_ex)

        return is_success

    @TaskReporter.report_test()
    def __get_output_of_current_cmd(self):
        ESC = r'\x1b'
        CSI = ESC + r'\['
        OSC = ESC + r'\]'
        CMD = '[@-~]'
        ST = ESC + r'\\'
        BEL = r'\x07'
        pattern = '(' + CSI + '.*?' + CMD + '|' + OSC + '.*?' + '(' + ST + '|' + BEL + ')' + ')'

        pruned_lines = []
        op_lines = self.logs_read_handler.readlines()[2:-1]
        for line in op_lines:
            pruned_line = re.sub(pattern, '', line).strip()
            if pruned_line:
                pruned_lines.append(pruned_line)
        return pruned_lines

    @TaskReporter.report_test()
    def __execute_command_into_bash(self, session, cmd):
        session.sendline(cmd)
        try:
            session.expect(InteractiveSparkSession.BASH_EXP)
            stdout = self.__get_output_of_current_cmd()
            logger.info("stdout: %s", stdout)
        except Exception, cmd_exc:
            logger.error("Failed to execute command: %s due to error: %s", cmd, cmd_exc)
            return None, None
        else:
            session.sendline("echo $?")
            try:
                session.expect(InteractiveSparkSession.STATUS_EXP)
                status_out = self.__get_output_of_current_cmd()
                logger.info("status_out: %s", status_out)
                status = int(status_out[-1])
            except Exception, exit_exc:
                logger.error("Failed to get exit code for command: %s due to error: %s", cmd, exit_exc)
                return None, None
        return status, stdout

    @TaskReporter.report_test()
    def __init_bash_session(self):
        command = "/bin/bash"
        assert Machine.makedirs(user=None, host=None, filepath=os.path.join(Config.getEnv('ARTIFACTS_DIR'),
                                                                            "Spark_clientLogs")), \
            "Failed to create spark client logs directory"

        if self.user and getpass.getuser() != self.user:
            command = "sudo su - -c \"%s\" %s" % (command, self.user)

        if not Machine.isSameHost(self.host):
            command = "ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s \"%s\"" % \
                      (socket.getfqdn(self.host.strip()), command.replace('"', '\\"'))
        logger.info("bash command for opening interactive spark shell: %s", command)
        self.logs_write_handler = open(Spark.get_TmpClientFile(clientfile=self.logfile), 'wb')
        self.logs_read_handler = open(Spark.get_TmpClientFile(clientfile=self.logfile), 'r')
        session = pexpect.spawn(command, timeout=180, logfile=self.logs_write_handler, cwd=self.cwd, env=self.env)
        try:
            session.expect(InteractiveSparkSession.BASH_EXP)
            logger.info("Logged into bash shell in child process")
            if Hadoop.isSecure():
                exit_code, output = Machine.runas(user=Machine.getAdminUser(), cmd="which kinit", host=self.host)
                assert exit_code == 0, "Failed to find kinit location"
                kinit_loc = "\n".join(
                    util.prune_output(output.split("\n"), Machine.STRINGS_TO_IGNORE, prune_empty_lines=True)
                )
                keytab_file = os.path.join(Config.get('machine', 'KEYTAB_FILES_DIR'), "%s.headless.keytab" % self.user)
                assert Machine.pathExists(
                    user=Machine.getAdminUser(), host=self.host, filepath=keytab_file, passwd=Machine.getAdminPasswd
                ), "Failed to find keytab file"
                kinit_cmd = "%s -k -t %s %s" % (kinit_loc, keytab_file, Machine.get_user_principal(self.user))
                logger.info("executing kinit command: %s", kinit_cmd)
                exit_code, stdout = self.__execute_command_into_bash(session, kinit_cmd)
                logger.info("output of kinit: %s", stdout)
                logger.info("exit code for kinit: %s", exit_code)
                assert exit_code == 0, "Failed to kinit as user %s before starting interactive spark shell" % self.user
                logger.info("Successfully kinited as user: %s", self.user)
            logger.info("Successfully finished bash session initialization")
        except Exception:
            logger.error("Failed to initialize bash session")
            self.close()
            return None
        else:
            return session

    @TaskReporter.report_test()
    def __init_shell(self):
        if self.type == "spark-shell":
            ld_lib_val = MAPRED.getConfigValue("mapreduce.admin.user.env")
            version = Hadoop.getShortVersion()
            new_ld = ld_lib_val.replace('${hdp.version}', version).split("=")[1]
            os.environ["LD_LIBRARY_PATH"] = new_ld
            if HDFS.isASV() or Machine.isCloud():
                final_cmd = '%s' % Spark.getSparkShellPath()
            else:
                final_cmd = '%s --jars %s' % (Spark.getSparkShellPath(), Spark.getLzoJar())
        elif self.type == "pyspark":
            if HDFS.isASV() or Machine.isCloud():
                final_cmd = '%s' % Spark.getPysparkPath()
            else:
                final_cmd = '%s --jars %s' % (Spark.getPysparkPath(), Spark.getLzoJar())
        else:
            if HDFS.isASV() or Machine.isCloud():
                final_cmd = 'IPYTHON=1 pyspark'
            else:
                final_cmd = 'IPYTHON=1 pyspark --jars %s' % Spark.getLzoJar()
            os.environ['PYSPARK_PYTHON'] = 'python'
            os.environ['HADOOP_CONF_DIR'] = Config.get('hadoop', 'HADOOP_CONF')
        if not self.force_spark_version:
            if Spark.isSpark2():
                os.environ["SPARK_MAJOR_VERSION"] = '2'
        else:
            os.environ["SPARK_MAJOR_VERSION"] = self.force_spark_version
        if 'jars' in final_cmd:
            if self.jars:
                final_cmd += ",%s" % ','.join(self.jars)
        else:
            if self.jars:
                final_cmd += " --jars %s" % ','.join(self.jars)

        final_cmd += " --master %s" % self.mode
        if self.mode == "yarn":
            final_cmd += " --deploy-mode client"

        if self.conf:
            for key, val in self.conf.items():
                final_cmd += " --conf %s=%s" % (key, val)

        if self.type != "spark-shell" and self.pymodules:
            final_cmd += " --py-files %s" % ','.join(self.pymodules)

        if self.propfile:
            final_cmd += " --properties-file %s" % self.propfile

        if self.type == "spark-shell" and self.scalafile:
            final_cmd += " -i %s" % self.scalafile

        logger.info("spark interactive shell command: %s", final_cmd)
        self.session.sendline(final_cmd)
        try:
            self.session.expect(InteractiveSparkSession.INTERACTIVE_SHELL_EXP, timeout=180)
            output_lines = self.__get_output_of_current_cmd()
            is_success = False
            for line in output_lines:
                if "available as 'spark'" in line:
                    is_success = True
            assert is_success, "could not find spark session init log"
        except Exception, py_exc:
            logger.error("Failed to start spark interactive due to error: %s", py_exc)
            self.close()
            return False
        else:
            logger.info("Successfully started interactive spark session")
            return True


class SparkHTMLParser(HTMLParser):
    tableDetected = False
    rowData = []
    rowCount = 0
    cColCount = 0
    currentTag = ''

    def __init__(self):
        self.rowData = []
        self.rowCount = 0
        self.cColCount = 0
        self.currentTag = ''
        self.tableDetected = False
        HTMLParser.reset(self)

    @TaskReporter.report_test()
    def handle_starttag(self, tag, attrs):
        self.currentTag = tag
        if tag == 'table':
            print 'Table Detected'
            logger.info("Table detected")
            self.tableDetected = True
        if tag == 'tr' and self.tableDetected:
            self.rowCount = self.rowCount + 1
            self.rowData.append([])
            self.cColCount = 0

    @TaskReporter.report_test()
    def handle_data(self, data):
        if self.currentTag == 'td' and self.tableDetected:
            self.rowData[self.rowCount - 1].append(data.strip())
            self.cColCount = self.cColCount + 1
        if self.currentTag == 'th' and self.tableDetected:
            if self.rowCount == 0:
                self.rowCount = self.rowCount + 1
                self.rowData.append([])
                self.cColCount = 0
            self.rowData[self.rowCount - 1].append(data.strip())
            self.cColCount = self.cColCount + 1
        if self.currentTag == 'a' and self.tableDetected:
            self.rowData[self.rowCount - 1].append(data.strip())
        if self.currentTag == 'span' and self.tableDetected:
            self.rowData[self.rowCount - 1].append(data.strip())

    @TaskReporter.report_test()
    def handle_endtag(self, tag):
        self.currentTag = ''
        if tag == 'table' and self.tableDetected:
            self.tableDetected = False
