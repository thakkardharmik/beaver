#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
'''
QE HDP Stack Log Aggregation Framework
'''
import logging
import os
import traceback
from ConfigParser import ConfigParser

from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class logUtilCloudbreak(object):
    '''
    LogUtilCloudbreak Class
    '''

    def __init__(self):
        pass

    @classmethod
    def createDirInternal(cls, host, user, filepath, passwd, perm="777", logoutput=False):
        '''
        Create log dir and grant permission on log server only.
        Returns None
        '''
        #permission of 644 won't succeed to rsync.
        from beaver.logUtilMachine import LogUtilMachine
        LogUtilMachine.makedirs(user=user, host=host, filepath=filepath, passwd=passwd)
        LogUtilMachine.chmod(
            perm=perm, filepath=filepath, recursive=False, user=user, host=host, passwd=passwd, logoutput=logoutput
        )

    @classmethod
    def isCloudbreak(cls):
        '''
        Find out if cluster is cloudbreak
        '''
        config = ConfigParser()
        reportconf = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'test_report.conf')
        SECTION = "HW-QE-PUBLISH-REPORT"
        config.optionxform = str
        config.read(reportconf)
        COMPONENT = ''
        if config.has_option(SECTION, 'TESTSUITE_COMPONENT'):
            COMPONENT = config.get(SECTION, 'TESTSUITE_COMPONENT')
        return COMPONENT

    @classmethod
    def get_log_aggregation_Dir_Locations(cls):
        '''
        Gets base dir to for log aggregation.
        Returns a tuple of (str, str, str, str).
        Returns a tuple of (cluster name, temporary cluster dir for logs,
                            temporary dir for applications, test component)
        '''
        config = ConfigParser()
        reportconf = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'test_report.conf')
        SECTION = "HW-QE-PUBLISH-REPORT"
        config.optionxform = str
        config.read(reportconf)
        CLUSTER_NAME = config.get(SECTION, "CLUSTER_NAME")
        logUtilCloudbreak.LOCAL_TMP_CLUSTER_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), CLUSTER_NAME)
        logUtilCloudbreak.LOCAL_TMP_APP_STORAGE = os.path.join(cls.LOCAL_TMP_CLUSTER_DIR, "Application-logs")
        logUtilCloudbreak.COMPONENT = ''
        if config.has_option(SECTION, 'TESTSUITE_COMPONENT'):
            logUtilCloudbreak.COMPONENT = config.get(SECTION, 'TESTSUITE_COMPONENT')
            logger.info("Set logUtilCloudbreak.COMPONENT to %s", logUtilCloudbreak.COMPONENT)
        return (
            CLUSTER_NAME, logUtilCloudbreak.LOCAL_TMP_CLUSTER_DIR, logUtilCloudbreak.LOCAL_TMP_APP_STORAGE,
            logUtilCloudbreak.COMPONENT
        )

    # TODO: Fix the redefined-builtin - Rename the type parameter
    @classmethod
    def create_Logs_directories(  # pylint: disable=redefined-builtin,unused-argument
            cls,
            logHost,
            logHostUser,
            logHostBasePath,
            passwd=None,
            logoutput=False,
            type="all"
    ):
        '''
        Create log directories for log collection.
        type : Choose the type to create directory
               if type = "service-logs" , <logHostBasepath>/service-logs dir will be created on logHost
               if type = "app-logs", <logHostBasepath>/app-logs dir will be created on logHost
               if type == "test-logs", <logHostBasepath>/test-logs dir will be created on logHost
               if type == "artifacts", <logHostBasepath>/artifacts dir will be created on logHost
               if type == "test-logs", <logHostBasepath>/test-logs dir will be created on logHost
               if type == "jenkins-logs", <logHostBasepath>/jenkins-logs dir will be created on logHost
               if type == all, all of above directories will be created.
        Returns None.
        '''
        cls.get_log_aggregation_Dir_Locations()
        # create temp dir in gateway for app logs
        if not Machine.pathExists(None, logHost, cls.LOCAL_TMP_CLUSTER_DIR, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_CLUSTER_DIR, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_CLUSTER_DIR, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )
        if not Machine.pathExists(None, None, cls.LOCAL_TMP_APP_STORAGE, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_APP_STORAGE, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_APP_STORAGE, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )
        cls.createDirInternal(logHost, logHostUser, logHostBasePath, passwd, logoutput=logoutput)
        # create base dirs in log server
        cls.createDirInternal(logHost, logHostUser, logHostBasePath + "/" + "artifacts", passwd, logoutput=logoutput)

    # TODO: Remove the unused argument cleanupDirFirst
    @classmethod
    def gather_cloudbreakartifacts_log(  # pylint: disable=unused-argument
            cls, destHost, destUser, destPath, passwd, cleanupDirFirst=False, logoutput=False
    ):
        '''
        Gather artifacts to destination host with rsync.
        returns None
        '''
        try:
            srcPath = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "*")
            cls.createDirInternal(destHost, destUser, destPath, passwd, logoutput=logoutput)
            from beaver.rsync import RSync
            RSync.rsyncToRemoteHost(
                user=None,
                host=None,
                passwd=None,
                srcPath=srcPath,
                destPath=destPath,
                destHost=destHost,
                destUser=destUser,
                logoutput=logoutput,
                flag="-rhp --chmod=u=rwx,g=rwx,o=r"
            )
        except Exception as e:
            logger.info("Exception occurs at gather_artifacts_log. %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
