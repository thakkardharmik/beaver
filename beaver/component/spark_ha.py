#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, collections
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, MAPRED, YARN, HDFS
from beaver.java import Java
from beaver import util
from beaver import configUtils
import os
import logging
import pytest
from beaver.component.hive import Hive
from beaver.component.spark import Spark
from beaver.HTMLParser_v002 import HTMLParser
import shutil
import subprocess
import re
from taskreporter.taskreporter import TaskReporter
'''
Description:
Test API to work with Spark

'''

logger = logging.getLogger(__name__)


class Spark_Ha:
    @classmethod
    @TaskReporter.report_test()
    def start_LongRunning_HDFS_stream_job(
            cls, inputDir, num_executor, mode="yarn-client", inBackground=True, clientfile=None
    ):
        '''
          Start Spark-HDFS Streaming application
          '''
        className = "org.apache.spark.examples.streaming.HdfsWordCount"
        if mode == "yarn-client" and not HDFS.isASV():
            jars = Spark.getLzoJar()
        else:
            jars = None
        if clientfile == None:
            Local_clientlog = Spark.createTmpClientFile(className + "_" + mode)
        else:
            Local_clientlog = Spark.createTmpClientFile(clientfile)
        arg = " %s 2>&1 | tee %s" % (inputDir, Local_clientlog)
        if Hadoop.isSecure():
            keytab = Machine.getHeadlessUserKeytab(Config.get('hadoop', 'HADOOPQA_USER'))
            principal = Machine.get_user_principal(Config.get('hadoop', 'HADOOPQA_USER'))
        else:
            keytab = None
            principal = None

        Spark.submitSparkApplication(
            className,
            mode,
            arg,
            jars=jars,
            num_executor=num_executor,
            inBackground=inBackground,
            timeout=120,
            keytab=keytab,
            principal=principal
        )
        f = open(Local_clientlog, "r")
        stdout = f.read()
        f.close()
        appId = YARN.getApplicationIDFromStdout(stdout)
        return appId, Local_clientlog

    @classmethod
    def start_LongRunning_Federation_HDFS_stream_job(
            cls,
            inputDir,
            outputDir,
            num_executor,
            mode="yarn-client",
            inBackground=True,
            clientfile=None,
            pythonFile="federation_hdfs_wordcount.py",
            srcDir=None,
            keytab=None,
            principal=None
    ):
        """
          Starts Spark-HDFS Streaming application using python file
          :param inputDir:
          :param outputDir:
          :param num_executor:
          :param mode:
          :param inBackground:
          :param clientfile:
          :param pythonFile: Python file which need to be run as spark streaming application
          :param srcDir: Path of the Python file
          :return: (application ID, Local client log)
          """
        if clientfile == None:
            Local_clientlog = Spark.createTmpClientFile(pythonFile + "_" + mode)
        else:
            Local_clientlog = Spark.createTmpClientFile(clientfile)

        if pythonFile == "federation_hdfs_wordcount.py":
            srcDir = os.path.join(Config.getEnv("WORKSPACE"), "tests", "spark", "examples", "streaming")

        arg = " %s %s 2>&1 | tee %s" % (inputDir, outputDir, Local_clientlog)

        Spark.submitSparkPyApplication(
            pythonFile,
            mode,
            arg,
            num_executor=num_executor,
            inBackground=inBackground,
            srcDir=srcDir,
            timeout=120,
            clientfile=clientfile,
            conf=None,
            keytab=keytab,
            principal=principal
        )

        f = open(Local_clientlog, "r")
        stdout = f.read()
        f.close()
        appId = YARN.getApplicationIDFromStdout(stdout)
        return appId, Local_clientlog

    @classmethod
    @TaskReporter.report_test()
    def validate_HDFS_stream_job(cls, appId, mode, patterns, expected_count, clientfile=None):
        '''
          count the occurance of word in the yarn logs.
            -> check clientfile for yarn-client mode
            -> check yarn logs for yarn-cluster mode

          appId : application Id
          mode : mode of execution 
          patterns : list of words to check in log
          expected_count : the expected number of occurence for each word in patterns
          clientfile : jobclient output for app
          '''
        if mode == "yarn-client":
            file_to_read = clientfile
        else:
            file_to_read = Spark.createTmpClientFile(appId + ".log")
            YARN.getLogsApplicationID(
                appId,
                appOwner=None,
                nodeAddress=None,
                containerId=None,
                logoutput=False,
                grepFilter=None,
                pipeToFileOutput=file_to_read,
                config=None
            )

        count = 0
        word_count = {}
        # initialize word_count dictonary
        for p in patterns:
            word_count[p] = 0
        with open(file_to_read) as f:
            for line in f:
                words = line.split()
                for word in words:
                    if word in word_count.keys():
                        word_count[word] = word_count[word] + 1

        logger.info(word_count)
        for key, value in word_count.iteritems():
            assert value >= expected_count, "%s wordcount is %s. expected_count is %s" % (key, value, expected_count)

    @classmethod
    def validate_wordcount_written_to_HDFS(cls, hdfs_dir, patterns, expected_count, appId=None):
        """
          Validate the wordcount results, written into HDFS directories by a streaming job.
          Use wildcards in the 'hdfs_dir' to recursively read sub-directories.

          :param hdfs_dir: HDFS directory from where contents will be read
          :param patterns: list of words to check
          :param expected_count: the expected number of occurence for each word in the 'patterns'
          :param appId: application ID (Optional parameter)
          :return:
          """
        count = 0
        word_count = {}
        # initialize word_count dictonary
        for p in patterns:
            word_count[p] = 0

        exit_code, cat_content = HDFS.cat(hdfs_dir, logoutput=True)
        assert exit_code == 0, "Could not read from %s, Error: %s, appId: %s" % (hdfs_dir, cat_content, appId)
        for line in cat_content:
            words = line.split()
            for word in words:
                if word in word_count.keys():
                    word_count[word] = word_count[word] + 1

        logger.info(word_count)
        for key, value in word_count.iteritems():
            assert value >= expected_count, "%s wordcount is %s. expected_count is %s, appId: %s" % \
                                            (key, value, expected_count, appId)
