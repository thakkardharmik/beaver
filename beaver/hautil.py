import logging, time
from beaver import util
from beaver.config import Config
from beaver.component.hadoop import YARN, HDFS, Hadoop, MAPRED
from beaver.component.oozie import Oozie
from beaver.machine import Machine
logger = logging.getLogger(__name__)


def resetHdfs(skip_check=False):
    # updates for Hadoop 2
    # only do this on nano as we these services are unreliable on nano
    if HDFS.isHAEnabled():
        logger.info("Resetting HDFS...")
        if skip_check or (Hadoop.isHadoop2() and Machine.isLinux() and Machine.isNano()):
            HDFS.resetHANamenodes('stop')
            HDFS.resetJournalNodes('stop')
            HDFS.resetZkfc('stop')
            time.sleep(10)
            HDFS.resetJournalNodes('start')
            HDFS.resetZkfc('start')
            HDFS.resetHANamenodes('start')
            time.sleep(10)

        # make sure we are out of safemode after every test
        HDFS.waitForActiveAndStandbyNNOutOfSafemode()
        logger.info("Resetting HDFS Completed.")


def resetYarn(skip_check=False):
    # updates for Hadoop 2
    if YARN.isHAEnabled():
        logger.info("Resetting YARN...")
        # only do this on nano as we these services are unreliable on nano
        tasktrackers = MAPRED.getTasktrackers()
        if skip_check or (Hadoop.isHadoop2() and Machine.isLinux() and Machine.isNano()):
            YARN.restartHARMNodes()
            # add sleep to give RM enough time to register all the nodes
            # and be ready
            MAPRED.waitForNMToRegister(len(tasktrackers))

        logger.info("Resetting YARN Completed.")


def getJMXData(url, modelerType, metric, defaultValue=None, tries=5, wait_time=15, user=None):
    count = 0
    my_data = 0
    if not user:
        user = Config.get('hadoop', 'YARN_USER', 'yarn')
    while (my_data is None or my_data is 0 or my_data is '') and count < tries:
        JMX_DATA = util.getJSONContent(url, user=user)
        for data in JMX_DATA['beans']:
            # check for None
            if data is not None and data['modelerType'] is not None and data['modelerType'] == modelerType:
                my_data = data[str(metric)]
                break
        count += 1
        time.sleep(wait_time)

    if my_data is None or my_data is 0 or my_data is '':
        return defaultValue
    else:
        return my_data


def getSucceededFinishedApps(prefix, startTime, endTime):
    d = {}
    appFound = 0
    apps = YARN.getApplicationsInfo(startedTimeBegin=startTime, finishedTimeEnd=endTime)
    if not apps:
        return appFound, d

    # traverse through all the apps and find the ones which we are looking for
    for app in apps:
        if prefix in app['name']:
            d[str(app['id'])] = str(app['finalStatus'])

    # check succeeded apps after the dict object is created in order
    # to not count duplicate entries
    # https://hortonworks.jira.com/browse/BUG-14532
    for key, value in d.items():
        if 'SUCCEEDED' == value:
            appFound += 1

    return appFound, d


def cleanseJobIds(jobIds):
    '''
  Windows is appending \r to jobIds and this is resulting in http calls to job history server to return
  incorrect information there by causing test failures. This function removes the character for the jobIds.
  :param jobIds: JobIds that need to be cleansed.
  :return: A list of jobIds.
  '''
    if Machine.isWindows():
        logger.info("Jobids before replacing <CR> character: %s" % jobIds)
        import string
        jobIds = [string.replace(jobId, '\r', '') for jobId in jobIds]
        logger.info("Jobids after replacing <CR> character: %s" % jobIds)
    return jobIds


def verifyAppsAndJobsSucceeded(appLogSearchPrefix, stdout, localDir, testMap=None, user=None):
    '''
  :param appLogSearchPrefix: The prefix using which the app logs are going to be searched.
  :param stdout: stdout from the app.
  :param localDir: Path to current dir.
  :param testMap: map containing the service(s) names and the kwargs of the services being restarted in the test.
  :return: success status and a dict with the relevant info.
  '''
    d = {}
    status = True

    if appLogSearchPrefix is None or localDir is None:
        status = False

    # Check if all the Jobs and the apps succeeded.
    if Hadoop.isHadoop2():
        dLog = jobLog = wprdLog = {}
        appStatus = jobStatus = True
        appIds, jobIds = YARN.getAppAndJobIdsFromConsole(stdout)
        appStatus, dLog = YARN.checkAppsSucceeded(appIds, logPrefix=appLogSearchPrefix, localDir=localDir)
        jobStatus, jobLog = YARN.checkJobsSucceeded(jobIds, user)
        d.update(dLog)
        d.update(jobLog)

        jobIds = cleanseJobIds(jobIds)

        # Performing WPR validations.
        wprStatus, wprdLog = verifyWorkPreservingRMRestart(jobIds, testMap)
        d.update(wprdLog)

        # Check if all the validations succeeded.
        if appStatus is False or jobStatus is False or wprStatus is False:
            d[appLogSearchPrefix] = "appStatus: %s jobStatus: %s wprStatus: %s" % (appStatus, jobStatus, wprStatus)
            status = False

    # Prepend the method names to all the keys in the dict.
    tempd = {}
    for k, v in d.items():
        tempd["%s: %s" % ("verifyAppsAndJobsSucceeded", k)] = v

    return status, tempd


def verifyWorkPreservingRMRestart(jobIds, testMap=None):
    '''
  Validates the functionality related to Work Preserving RM Restart feature in the context of HA.
  :param jobids: List of job ids on which the validation needs to be performed.
  :param testMap: map containing the service(s) names and the kwargs of the services being restarted in the test.
  :return: success status and a dict with the relevant info.
  '''
    d = {}
    status = True

    if jobIds is None or len(jobIds) <= 0:
        status = False

    jobIds = cleanseJobIds(jobIds)

    # If AM is being restarted, skip WPR validations.
    validateWpr = False
    if testMap:
        if testMap.has_key("services"):
            # The service names are separated by ,.
            serviceList = testMap["services"].split(",")
            for service in serviceList:
                if testMap[service].has_key("kwargs"):
                    if testMap[service]["kwargs"]["service"] == "applicationmaster":
                        validateWpr = False
                        d["ApplicationMaster"] = "ApplicationMaster is being restarted. Skipping WPR validations."
                        status = True
                        break
                    # check if RM is being restarted in the test and only validate WPR if it is.
                    if testMap[service]["kwargs"]["service"] == "resourcemanager":
                        validateWpr = True

    if validateWpr and YARN.isWorkPreservingRMRestartEnabled():
        status, d = YARN.verifyJobSuccessWithWorkPreservingRMRestart(jobIds)
    else:
        d['WPR-Check-Off'] = "RM was not restarted in the test so no need to check WPR!"

    # Prepend the method names to all the keys in the dict.
    tempd = {}
    for k, v in d.items():
        tempd["%s: %s" % ("verifyWorkPreservingRMRestart", k)] = v

    return status, tempd


def runOozieCmdsUntilStatus(propertiesFiles, expectedStatus, timeoutSecs=7200):
    '''
  Runs oozie cmds and waits for all workflows to complete or until timeout is reached.
  :param propertiesFiles: List of propertiesFiles to be used to run oozie cmds.
  :param expectedStatus: Check if all workflows have achieved this status. Returns false otherwise.
  :param timeout: Stop checking for expectedStatus after the timeout is reached and return.
  :return: (List of workflows, oozie status)
  '''
    dLog = {}
    status = True
    # submit all properties files and get the workflow ids
    workflowIds = []
    for prop_file in propertiesFiles:
        cmd = " -config %s -run" % (prop_file)
        exit_code, stdout = Oozie.runOozieJobCmd(cmd, retry=True)
        if exit_code != 0:
            status = False
        workflowIds.append(Oozie.getWorkflowID(stdout))

    if status is True:
        time.sleep(10)
        status, dLog = waitForOozieJobsUntilStatus(workflowIds, expectedStatus, timeoutSecs)

    for k, v in dLog.items():
        logger.info("%s -> %s" % (k, v))

    logger.info("Workflow ids: %s Status: %s" % (workflowIds, status))
    return workflowIds, status


def waitForOozieJobsUntilStatus(workflowIds, expectedStatus, timeoutSecs=7200, oozie_server=None):
    dLog = {}
    status = True
    # make sure all jobs are done running
    for workflowId in workflowIds:
        oozie_status = Oozie.getJobStatus(workflowId, retry=True, oozie_server=oozie_server)
        stepTimeSecs = 15
        tries = timeoutSecs / stepTimeSecs
        count = 0
        while (oozie_status == "RUNNING" and count < tries):
            logger.info('Try %s of %s to determine oozie job status.' % (count + 1, tries))
            time.sleep(stepTimeSecs)
            oozie_status = Oozie.getJobStatus(workflowId, retry=True, oozie_server=oozie_server)
            logger.info("WorkflowStatus %s: %s" % (workflowId, oozie_status))
            count += 1
        # store the status of all oozie jobs
        dLog[str(workflowId)] = oozie_status

        # if the oozie status is not as expected set the status to false
        logger.info("Oozie status: %s" % (oozie_status))
        if oozie_status != expectedStatus:
            status = False
    return status, dLog


def verifyOozieAppsAndJobsSucceeded(workflowIds, logPrefix, localDir, testMap, action_name='wc', checkJob=True):
    '''
  Verifies if all apps and jobs submitted/created via. Oozie have succeed all the validations.
  :param workflowIDs: List of workflow ids to verify.
  :param logPrefix: log prefix for YARN app logs.
  :param localDir: Path to local log dir.
  :return: Bool status indicating if validation succeeded.
  '''
    appIds = []
    jobIds = []
    dLog = {}
    appStatus = True
    jobStatus = True
    wprStatus = True

    # check the job and app status for each workflow we launched.
    if Hadoop.isHadoop2():
        # get all the app and job ids
        for workflowId in workflowIds:
            if action_name != 'None':
                stdout = Oozie.getJobInfo('%s@%s' % (workflowId, action_name), verbose=True, retry=True)
            else:
                stdout = Oozie.getJobInfo('%s' % (workflowId), verbose=True, retry=True)
            ids = Oozie.getJobAndAppIds(stdout)
            for id in ids:
                appIds.append(id['application'])
                jobIds.append(id['job'])
        # get the app and job status for all the jobs we found
        appStatus, appLog = YARN.checkAppsSucceeded(appIds, logPrefix=logPrefix, localDir=localDir)
        dLog.update(appLog)
        if checkJob:
            jobStatus, jobLog = YARN.checkJobsSucceeded(jobIds)
            dLog.update(jobLog)
        for key, value in dLog.items():
            logger.info("%s -> %s" % (key, value))

        wprStatus, d = verifyWorkPreservingRMRestart(jobIds, testMap)
        for k, v in d.items():
            logger.info("%s -> %s" % (k, v))

    logger.info("appStatus: %s jobStatus: %s wprStatus: %s" % (appStatus, jobStatus, wprStatus))
    return appStatus and jobStatus and wprStatus
