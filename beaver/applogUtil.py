#
# Copyright  (c) 2011-2018, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
'''
App Log Aggregation Framework
'''

import logging
import json
import time
import ast
import os
import re
import traceback
import collections
from threading import Thread
from beaver.config import Config
from beaver import util
from beaver.componentLogUtil import ComponentLogUtil
from beaver.machine import Machine
from beaver.timeout import timelimit

logger = logging.getLogger(__name__)

NUM_OF_THREADS = 5
FALCON_STR = "falcon"

class AppLogUtil(object):

    LOCAL_TMP_APP_STORAGE = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "Application-logs")

    @classmethod
    def get_testcase_status_txt(cls):
        """
        Get test_case_status.txt location
        :return:
        """
        return os.path.join(os.path.join(Config.getEnv('ARTIFACTS_DIR'), "test_case_status.txt"))

    @classmethod
    def convert_to_epoch(cls, timestamp):
        """
        Convert time to epoch
        :param timestamp:2018-07-12 20:38:18,813
        :return: epoch = 1531427898
        """
        time_slice = timestamp.split(",")[0]
        return time.mktime(time.strptime(time_slice, "%Y-%m-%d %H:%M:%S"))

    @classmethod
    def get_failed_aborted_test_with_timestamp(cls):
        """
        Get list of failed and aborted tests with start and endtime
        :return: a dictonary with key = testcase name , value = [starttime, endtime, status]
        """
        testcase_map = {}
        testcase_filename = cls.get_testcase_status_txt()
        with open(testcase_filename) as f:
            for line in f:
                json_data = ast.literal_eval(line)
                data = json.dumps(json_data)
                d = json.loads(data)
                status = d["status"]
                if status in ["fail", "aborted"]:
                    inner_dict = {}
                    inner_dict["start_time"] = cls.convert_to_epoch(d["start_time"])
                    inner_dict["end_time"] = cls.convert_to_epoch(d["end_time"])
                    inner_dict["status"] = status
                    testcase_map[d["name"]] = inner_dict
        return testcase_map

    @classmethod
    def get_local_job_summary_logs(cls, component):
        """
        Copy Job_summary Logs to local dirs [artifacts/job_summary_local.log]
        parameter: component : Component name for which log collection is taking place
        return: List of Local copies of Job summary log
        Note: Some components need special handling where there are multiple Job Summary Log files
              such as HA and Falcon
        """
        LocalJobSummaryLogs = []
        try:
            if component == FALCON_STR:
                from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
                host1 = Falcon.get_cluster_1_masters()['rm']
                host2 = Falcon.get_cluster_2_masters()['rm']
                host3 = Falcon.get_cluster_3_masters()['rm']
                for host in [host1, host2, host3]:
                    JobSummaryLog = ComponentLogUtil.MAPRED_getJobSummaryLogFile(host)
                    LocalJobSummaryLog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "jobsummary_" + host + ".log")
                    Machine.copyToLocal(None, host, JobSummaryLog, LocalJobSummaryLog, None)
                    if Machine.pathExists(None, None, LocalJobSummaryLog, passwd=None):
                        LocalJobSummaryLogs.append(LocalJobSummaryLog)
            else:
                for host in ComponentLogUtil.YARN_getRMHANodes():
                    JobSummaryLog = ComponentLogUtil.MAPRED_getJobSummaryLogFile(host)
                    LocalJobSummaryLog = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "jobsummary_" + host + ".log")
                    Machine.copyToLocal(Machine.getAdminUser(), host, JobSummaryLog, LocalJobSummaryLog,
                                        Machine.getAdminPasswd())
                    Machine.chmod("777", LocalJobSummaryLog, user=Machine.getAdminUser(),
                                  passwd=Machine.getAdminPasswd())
                    if Machine.pathExists(Machine.getAdminUser(), None, LocalJobSummaryLog,
                                          passwd=Machine.getAdminPasswd()):
                        LocalJobSummaryLogs.append(LocalJobSummaryLog)
            return LocalJobSummaryLogs
        except Exception as e:
            logger.info("Exception occurs at job_summary_log collection %s", e)
            tb = traceback.format_exc()
            logger.info(tb)
            return LocalJobSummaryLogs

    @classmethod
    def gather_applicationId_user_mapping(  # pylint: disable=unused-argument
            cls, JobSummaryLogfiles, startTime, endTime, ignoreStartEndTime=False):
        '''
        Function to Find application-user-jobsummaryLog mapping to gather app logs
        :param JobSummaryLogfiles: List of JobSummary Files
        :param startTime: Test Start Time
        :param endTime: Test end time
        :param isFalcon: if falcon is component
        :param ignoreStartEndTime:  Ignore startend time (for --gatherAllAppLogs/--gatherAllTestAppLogs)
        :return:A dictionary object in below format for which app logs needs to be collected
        {'app1':['user1', 'jobsummaryfile'], 'app2':['user2', 'jobsummaryfile']}
        '''
        appId_user_map = collections.defaultdict(dict)
        for JobSummaryLogfile in JobSummaryLogfiles:
            logger.info("Reading from %s", JobSummaryLogfile)
            f = open(JobSummaryLogfile)
            # pylint: disable=line-too-long
            msg = r"(\d{1,4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}),\d{1,3}.*ApplicationSummary: appId=(.*),name=.*,user=(.*),queue=.*,state=.*,trackingUrl=.*,appMasterHost=.*,submitTime=(.*),startTime=[0-9]{13},finishTime=[0-9]{13}"
            # pylint: enable=line-too-long
            # Find out applicationIds between start/end time stamp
            for line in f:
                m = re.search(msg, line)
                if m:
                    appId = m.group(2)
                    user = m.group(3)
                    submitTime = float(m.group(4)) / 1000.0
                if not ignoreStartEndTime:
                    if submitTime >= startTime and submitTime <= endTime:
                        appId_user_map[appId] = [user, JobSummaryLogfile]
                else:
                    appId_user_map[appId] = [user, JobSummaryLogfile]
            f.close()
        return appId_user_map

    @classmethod
    def create_parallel_thread_pool(cls, appId_user_map, numofthreads):
        '''
        Create Parallel Thread pool from collection of applications.
        Example:
        :param appId_user_map: {
            app1: [user1, jobsummaryfile],
            app2:[user2,jobsummaryfile],
            app3:[user3,jobsummaryfile],
            app4:[user4,jobsummaryfile],
            app5:[user5,jobsummaryfile]
        }
        :param numofthreads: 2
        This function will create sets of 2 applicationId objects
        1 => app1 , app2
        2 => app3, app4
        3 => app5
        output = {
            "1": {"app1": ["user1", "jobsummaryfile"], "app2": ["user2", "jobsummaryfile"]},
            "2": {"app3": ["user3", "jobsummaryfile"], "app4":["user4", "jobsummaryfile"]},
            "3": {"app5": ["user5", "jobsummaryfile"]}
        }
        '''
        count = 0
        key = 0
        parallel_appid_user_map = collections.defaultdict(dict)
        for appId, user in appId_user_map.items():
            if count % numofthreads == 0:
                key = key + 1
            parallel_appid_user_map[str(key)][appId] = user
            count = count + 1
        return parallel_appid_user_map

    @classmethod
    def collect_application_log_locally(cls, appId, user):
        '''
        Collects application log and save it in Local Dir with <appId>.log filename
        :param appId: Application Id
        :param user: Application Id owner
        '''
        try:
            from beaver.component.hadoop import YARN
            filename = os.path.join(cls.LOCAL_TMP_APP_STORAGE, appId + ".log")
            if not Machine.pathExists(None, None, filename, None):
                logger.info("Storing syslog of %s in %s", appId, filename)
                YARN.getLogsApplicationID(appId, user, None, None, False, None, filename)
            else:
                logger.info("%s already present at %s", appId, filename)
        except Exception:
            logger.error("Exception occured during collect_application_log_locally() call")
            logger.error(traceback.format_exc())

    @classmethod
    def gather_application_log_from_JobSummary(cls, JobSummaryLogfiles, startTime, endTime, isFalcon=False,
                                               ignoreStartEndTime=False, numofthreads=NUM_OF_THREADS):
        '''
        Function to Find applications from Jobsummary log and gather application logs which started
                between startTime and endTime
        startTime and endTime will be in epoch format
        Returns None
        '''
        appId_user_map = cls.gather_applicationId_user_mapping(
            JobSummaryLogfiles, startTime, endTime, ignoreStartEndTime=ignoreStartEndTime
        )
        pool = cls.create_parallel_thread_pool(appId_user_map, numofthreads)
        for iter_, appsuserjs in pool.iteritems():
            logger.info("*** Start Pool number : %s ***", iter_)
            threads = []
            logger.info(threads)
            for app in appsuserjs:
                appId = app
                user = appsuserjs[app][0]
                js = appsuserjs[app][1]
                logger.info("Gather app log for %s", appId)
                if isFalcon:
                    thread = Thread(target=cls.collect_application_log_for_Falcon_locally, args=(js, appId, user))
                else:
                    thread = Thread(target=cls.collect_application_log_locally, args=(appId, user))
                threads.append(thread)
                thread.start()
            logger.info("*** Wait for threads from pool %s to Finish", iter_)
            for thread in threads:
                thread.join()

    @classmethod
    def collect_application_log_for_Falcon_locally(cls, JobSummaryLogfile, appId, user):
        '''
        Collects application logs for Falcon component and save it in Local Dir with <appId>.log filename
        '''
        host = re.search("jobsummary_(.*).log", JobSummaryLogfile).group(1)
        if not Machine.pathExists(None, None, os.path.join(cls.LOCAL_TMP_APP_STORAGE, host), None):
            Machine.makedirs(None, None, os.path.join(cls.LOCAL_TMP_APP_STORAGE, host), None)
            Machine.chmod(
                "777",
                os.path.join(cls.LOCAL_TMP_APP_STORAGE, host),
                recursive=True,
                user=None,
                host=None,
                passwd=None,
                logoutput=True
            )
        filename = os.path.join(cls.LOCAL_TMP_APP_STORAGE, host, appId + ".log")
        try:
            from beaver.component.falcon import Falcon  # pylint: disable=redefined-outer-name
            Falcon.get_application_log(
                host,
                appId,
                appOwner=user,
                nodeAddress=None,
                containerId=None,
                logoutput=False,
                grepFilter=None,
                pipeToFileOutput=filename
            )
        except Exception:
            logger.error("Exception occured during collect_application_log_for_Falcon_locally() call")
            logger.error(traceback.format_exc())
            logger.info("Get application log for Falcon is broken")

    @classmethod
    @timelimit(2000)
    def collect_application_logs_for_failed_aborted_tests(cls):
        """
        Collect application logs for failed and aborted tests
        1) Read test_case_status.log to collected failed and aborted tests
        2) Gather Jobsummary log files for all RMs
        3) List down the applicationIds for failed and aborted tests
        4) Gather logs
        :return:
        """
        curr_component = util.get_TESTSUITE_COMPONENT()
        logger.info(curr_component)
        m = re.search(FALCON_STR, curr_component)
        isFalcon = bool(m)

        LocalJobSummaryLogs = cls.get_local_job_summary_logs(curr_component)
        logger.info(LocalJobSummaryLogs)

        testcase_map = cls.get_failed_aborted_test_with_timestamp()
        logger.info(testcase_map)

        if not Machine.pathExists(None, None, cls.LOCAL_TMP_APP_STORAGE, None):
            Machine.makedirs(Machine.getAdminUser(), None, cls.LOCAL_TMP_APP_STORAGE, Machine.getAdminPasswd())
            Machine.chmod(
                "777", cls.LOCAL_TMP_APP_STORAGE, False, Machine.getAdminUser(), None, Machine.getAdminPasswd(), True
            )

        for testcase in testcase_map:
            testcase_data = testcase_map[testcase]
            cls.gather_application_log_from_JobSummary(LocalJobSummaryLogs, testcase_data["start_time"],
                                                       testcase_data["end_time"], isFalcon=isFalcon)
