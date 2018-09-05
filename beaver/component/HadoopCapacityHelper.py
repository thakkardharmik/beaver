import time, logging, re, os, math
from hadoop import Hadoop, MAPRED, YARN, HDFS
from ..machine import Machine
from ..config import Config
from ..java import Java
from .. import util

logger = logging.getLogger(__name__)


def reconfigureQueue(capacityProperties, yarnSiteProperty, mod_conf_path):
    '''
    Reconfigures queue.
    If a key is present in two properties dicts, capacityProperties value takes precedence.
    '''
    for key1 in yarnSiteProperty:
        if key1 not in capacityProperties:
            capacityProperties[key1] = yarnSiteProperty[key1]
        else:
            for key2 in yarnSiteProperty[key1]:
                if key2 not in capacityProperties[key1]:
                    capacityProperties[key1][key2] = yarnSiteProperty[key1][key2]
    #capacityProperties.update(yarnSiteProperty)

    Hadoop.modifyConfig(capacityProperties, {'services': ['all']}, makeCurrConfBackupInWindows=False)
    MAPRED.restartJobtracker(mod_conf_path)
    #YARN.refreshRMQueues(capacityProperties, makeCurrConfBackupInWindows=False,
    #                     host=YARN.getResourceManagerHost())


def _findPartitionFromPartitionElement(partition, partitionName):
    if partition == None:
        return
    partitionList = util.getElement("JSON", partition, "queueCapacitiesByPartition")
    for partitions in partitionList:
        if util.getValue("JSON", util.getElement("JSON", partitions, "partitionName")) == partitionName:
            return partitions
    return None


def _findQueueFromQueuesElement(queues, queueName, partitionName=None):
    '''
    Finds JSON element of specific queue name from queues JSON element.
    '''
    if queues == None:
        return
    queueList = util.getElement("JSON", queues, "queue", False)
    if queueList:
        for queue in queueList:
            if util.getValue("JSON", util.getElement("JSON", queue, "queueName")) == queueName:
                if partitionName != None:
                    capacities = util.getElement("JSON", queue, "capacities")
                    res = _findPartitionFromPartitionElement(capacities, partitionName)
                    return res
                else:
                    return queue
            else:
                subQueues = util.getElement("JSON", queue, "queues")
                res = _findQueueFromQueuesElement(subQueues, queueName)
                if partitionName != None:
                    capacities = util.getElement("JSON", res, "capacities")
                    res = _findPartitionFromPartitionElement(capacities, partitionName)
                if (res != None):
                    return res
        return None


def getQueue(queueName, partitionName=None):
    '''
    Gets JSON element of specific queue name.
    '''
    rmSchedulerUrl = YARN.getRMUrl() + "/ws/v1/cluster/scheduler"
    res = util.getHTTPResponse("JSON", rmSchedulerUrl)
    scheduler = util.getElement("JSON", res, "scheduler")
    schedulerInfo = util.getElement("JSON", scheduler, "schedulerInfo")
    queuesElement = util.getElement("JSON", schedulerInfo, "queues")
    return _findQueueFromQueuesElement(queuesElement, queueName, partitionName)


def waitForResourceToDecrease(
        queueName,
        resourceOfInterest,
        stopThreshold,
        partitionName=None,
        timeout=300,
        logoutput=True,
        logStatusEveryNumIterations=5
):
    '''
    Wait for resource in given queue to decrease below stop threshold.
    Returns True if resource drops below the threshold.
    '''
    stopThreshold = math.ceil(float(stopThreshold))
    starttime = time.time()
    iteration = 0
    value = float(0)
    while (time.time() - starttime) < timeout:
        queue = getQueue(queueName, partitionName)
        if queue is not None:
            value = float(util.getValue("JSON", util.getElement("JSON", queue, resourceOfInterest)))
            if logoutput and iteration % logStatusEveryNumIterations == 0:
                logger.info(
                    "(Queue=%s), (%s=%s), waiting for dropping under %s" %
                    (queueName, resourceOfInterest, value, stopThreshold)
                )
            if value <= stopThreshold:
                break
        time.sleep(2)
        if (time.time() - starttime > timeout):
            if logoutput:
                logger.info("waitForResourceToDecrease")
                logger.info(
                    "Timed out: (Queue=%s), (%s=%s), does not see value below %s" %
                    (queueName, resourceOfInterest, value, stopThreshold)
                )
            return False
        iteration += 1
    if logoutput:
        logger.info("waitForResourceToDecrease")
        logger.info("Success: (Queue=%s), (%s=%s)" % (queueName, resourceOfInterest, value))
    return True


def waitForResourceToIncrease(
        queueName,
        resourceOfInterest,
        stopThreshold,
        partitionName=None,
        timeout=300,
        logoutput=True,
        logStatusEveryNumIterations=5
):
    '''
    Wait for resource in given queue to increase above stop threshold.
    Returns True if resource increases above the threshold.
    '''
    stopThreshold = math.floor(float(stopThreshold))
    starttime = time.time()
    iteration = 0
    value = float(0)
    while (time.time() - starttime) < timeout:
        queue = getQueue(queueName, partitionName)
        if queue is not None:
            value = float(util.getValue("JSON", util.getElement("JSON", queue, resourceOfInterest)))
            if logoutput and iteration % logStatusEveryNumIterations == 0:
                logger.info(
                    "(Queue=%s), (%s=%s), waiting for increasing over %s" %
                    (queueName, resourceOfInterest, value, stopThreshold)
                )
            if value >= stopThreshold:
                break
        time.sleep(2)
        if (time.time() - starttime > timeout):
            if logoutput:
                logger.info("waitForResourceToIncrease")
                logger.info(
                    "Timed out: (Queue=%s), (%s=%s), does not see value above %s" %
                    (queueName, resourceOfInterest, value, stopThreshold)
                )
            return False
        iteration += 1
    if logoutput:
        logger.info("waitForResourceToIncrease")
        logger.info("Success: (Queue=%s), (%s=%s)" % (queueName, resourceOfInterest, value))
    return True


def isResourceInRange(queueName, resourceOfInterest, minRange, maxRange, partitionName=None, logoutput=True):
    '''
    Is resource in given range?
    Returns True/False.
    Returns None is queue JSON element is not found.
    '''
    value = getResourceValue(queueName, resourceOfInterest, partitionName, logoutput)
    logger.info("Got resource value for %s %s: %s" % (queueName, resourceOfInterest, str(value)))
    if value is None:
        if logoutput:
            logger.info("isResourceInRange value = None")
        return None
    logger.info(
        "Checking resource value for %s %s: %s,%s" %
        (queueName, resourceOfInterest, str(value), str(math.ceil(value)))
    )
    if minRange <= math.ceil(value) and value <= maxRange:
        if logoutput:
            logger.info("isResourceInRange returns True.")
        return True
    else:
        if logoutput:
            logger.info("isResourceInRange returns False.")
        return False


def getResourceValue(queueName, resourceOfInterest, partitionName=None, logoutput=True):
    '''
    Gets value of interested resource.
    Returns None is queue JSON element is not found.
    '''
    queue = getQueue(queueName, partitionName)
    if queue is not None:
        value = float(util.getValue("JSON", util.getElement("JSON", queue, resourceOfInterest)))
        if logoutput:
            logger.info("getResourceValue %s %s = %s" % (queueName, resourceOfInterest, value))
        return value
    else:
        if logoutput:
            logger.info("getResourceValue %s %s = None" % (queueName, resourceOfInterest))
        return None


def getApp(appID, logoutput=True):
    '''
    Gets JSON element of specific application ID.
    '''
    if Hadoop.isEncrypted():
        rmAppsUrl = YARN.getRMUrl() + "/ws/v1/cluster/apps"
    else:
        rmAppsUrl = YARN.getRMUrl() + "/ws/v1/cluster/apps"
    res = util.getHTTPResponse("JSON", rmAppsUrl)
    apps = util.getElement("JSON", res, "apps", False)
    appList = util.getElement("JSON", apps, "app", False)
    for app in appList:
        if util.getValue("JSON", util.getElement("JSON", app, "id")) == appID:
            if logoutput:
                logger.info("getApp Found: app = %s" % app)
            return app
    if logoutput:
        logger.info("getApp returns None")
    return None


def getAppData(appID, xmlField, logoutput=True):
    '''
    Returns int value of xml entry queries from RM application web services.
    '''
    app = getApp(appID, logoutput=logoutput)
    if logoutput:
        logger.info("getAppData app = %s" % app)
    if app is None:
        return None
    value = int(util.getValue("JSON", util.getElement("JSON", app, xmlField)))
    if logoutput:
        logger.info("getAppData %s %s = %s" % (app, xmlField, value))
    return value


def getPreemptedContainersFromWebService(appID):
    '''
    get number of  preempted "AM-containers" and "non-AM-containers" for running application
    :param appID:
    :return:
    '''
    rmServerUrl = YARN.getResourceManagerWebappAddress() + "/ws/v1/cluster"
    rmAppsUrl = rmServerUrl + "/apps/%s" % appID
    res = util.getHTTPResponse("JSON", rmAppsUrl)
    app = util.getElement("JSON", res, "app", False)
    nonAMPreemptedContainers = int(util.getValue("JSON", util.getElement("JSON", app, "numNonAMContainerPreempted")))
    amPreemptedContainers = int(util.getValue("JSON", util.getElement("JSON", app, "numAMContainerPreempted")))
    logger.info("Non AM PreemptedContainers - %s" % nonAMPreemptedContainers)
    logger.info("AM PreemptedContainers - %s" % amPreemptedContainers)
    return (nonAMPreemptedContainers, amPreemptedContainers)


def getPreemptedContainers(appID, logoutput=True):
    '''
    Get preempted containers.
    Return a list of (String, String, String, String) or [].
    Each element is:
    1. "AM" or "Non-AM"
    2. app attempt ID
    3. container ID
    4. resource  
    '''
    #2014-07-17 23:09:11,069 INFO  attempt.RMAppAttemptMetrics (RMAppAttemptMetrics.java:updatePreemptionInfo(63)) - Non-AM container preempted, current appAttemptId=appattempt_1405638511922_0002_000001, containerId=container_1405638511922_0002_01_000002, resource=<memory:1000, vCores:1>
    result = []
    appAttemptID = appID.replace("application", "appattempt")
    localRMLog = MAPRED.copyJTLogToLocalMachine()
    regexPattern = "\s+AM container preempted.*appAttemptId=%s(.*), containerId=(.*), resource=(.*)" % appAttemptID
    matchObjList = util.findMatchingPatternInFile(localRMLog, regexPattern, return0Or1=False)
    for matchObj in matchObjList:
        result.append(("AM", appAttemptID + matchObj.group(1), matchObj.group(2), matchObj.group(3)))
    regexPattern = "Non-AM container preempted.*appAttemptId=%s(.*), containerId=(.*), resource=(.*)" % appAttemptID
    matchObjList = util.findMatchingPatternInFile(localRMLog, regexPattern, return0Or1=False)
    for matchObj in matchObjList:
        result.append(("Non-AM", appAttemptID + matchObj.group(1), matchObj.group(2), matchObj.group(3)))
    if result:
        logger.info("getPreemptedContainers returns %s" % result)
    return result
