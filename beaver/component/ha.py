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
import sys
import threading
import time

from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class HA(threading.Thread):
    service = None
    keepAlive = threading.Event()
    initialWait = 0
    # default up time is 5 mins
    upTime = 5 * 60
    # default down time is 10 seconds
    downTime = 10
    # default max time is 3 hours
    maxTime = 3 * 60 * 60
    # step time default to 10s
    stepTime = 10
    # service map
    serviceMap = {}
    # maximum number of times to bounce the service
    maxTimes = None
    # provide custom kwargs to the service start method
    startKwargs = None
    # provide custom kwargs to the service stop method
    stopKwargs = None

    # map for the namenode service
    serviceMap['namenode'] = {}
    serviceMap['namenode']['class'] = 'HDFS'
    serviceMap['namenode']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode']['start'] = {}
    serviceMap['namenode']['start']['method'] = 'startNamenode'
    serviceMap['namenode']['start']['kwargs'] = {'safemodeWait': False}
    serviceMap['namenode']['stop'] = {}
    serviceMap['namenode']['stop']['method'] = 'stopNamenode'
    serviceMap['namenode']['stop']['kwargs'] = {}

    # map for the namenode service in hadoop 2
    # Always do kill on HDFS2
    serviceMap['namenode2'] = {}
    serviceMap['namenode2']['class'] = 'HDFS'
    serviceMap['namenode2']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode2']['stop'] = {}
    serviceMap['namenode2']['stop']['method'] = 'restartActiveNamenode'
    serviceMap['namenode2']['stop']['kwargs'] = {'wait': 10, 'kill': True, 'waitForSafemodeExit': False}

    # map for the namenode service in hadoop 2 for standby namenode
    # Always do kill on HDFS2
    serviceMap['namenode2-standby'] = {}
    serviceMap['namenode2-standby']['class'] = 'HDFS'
    serviceMap['namenode2-standby']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode2-standby']['stop'] = {}
    serviceMap['namenode2-standby']['stop']['method'] = 'restartHANamenode'
    serviceMap['namenode2-standby']['stop']['kwargs'] = {
        'state': 'standby',
        'wait': 10,
        'kill': True,
        'waitForSafemodeExit': False
    }

    serviceMap['namenode2-stop-wait-start'] = {}
    serviceMap['namenode2-stop-wait-start']['class'] = 'HDFS'
    serviceMap['namenode2-stop-wait-start']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode2-stop-wait-start']['stop'] = {}
    serviceMap['namenode2-stop-wait-start']['stop']['method'] = 'restartActiveNamenode'
    serviceMap['namenode2-stop-wait-start']['stop']['kwargs'] = {
        'wait': 45,
        'kill': True,
        'waitForSafemodeExit': False
    }

    serviceMap['namenode2-restart'] = {}
    serviceMap['namenode2-restart']['class'] = 'HDFS'
    serviceMap['namenode2-restart']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode2-restart']['stop'] = {}
    serviceMap['namenode2-restart']['stop']['method'] = 'restartActiveNamenode'
    serviceMap['namenode2-restart']['stop']['kwargs'] = {'wait': 15, 'kill': True, 'waitForSafemodeExit': True}

    #   map for the namenode service to enter/exit safe mode
    serviceMap['namenode-safemode'] = {}
    serviceMap['namenode-safemode']['class'] = 'HDFS'
    serviceMap['namenode-safemode']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode-safemode']['stop'] = {}
    serviceMap['namenode-safemode']['stop']['method'] = 'enterSafemode'
    serviceMap['namenode-safemode']['stop']['kwargs'] = {}
    serviceMap['namenode-safemode']['start'] = {}
    serviceMap['namenode-safemode']['start']['method'] = 'exitSafemode'
    serviceMap['namenode-safemode']['start']['kwargs'] = {}

    # map for the namenode service with the kill option
    serviceMap['namenode-kill'] = {}
    serviceMap['namenode-kill']['class'] = 'HDFS'
    serviceMap['namenode-kill']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode-kill']['start'] = {}
    serviceMap['namenode-kill']['start']['method'] = 'startNamenode'
    serviceMap['namenode-kill']['start']['kwargs'] = {'safemodeWait': False}
    serviceMap['namenode-kill']['stop'] = {}
    serviceMap['namenode-kill']['stop']['method'] = 'killNameNode'
    serviceMap['namenode-kill']['stop']['kwargs'] = {}

    # map for the jobtracker service
    # kill the service by defaut
    serviceMap['jobtracker'] = {}
    serviceMap['jobtracker']['class'] = 'MAPRED'
    serviceMap['jobtracker']['package'] = 'beaver.component.hadoop'
    serviceMap['jobtracker']['start'] = {}
    serviceMap['jobtracker']['start']['method'] = 'startJobtracker'
    serviceMap['jobtracker']['start']['kwargs'] = {}
    serviceMap['jobtracker']['stop'] = {}
    serviceMap['jobtracker']['stop']['method'] = 'killJobtracker'
    serviceMap['jobtracker']['stop']['kwargs'] = {}

    # map for the jobtracker service use stop instead of kill
    serviceMap['jobtracker-stop'] = {}
    serviceMap['jobtracker-stop']['class'] = 'MAPRED'
    serviceMap['jobtracker-stop']['package'] = 'beaver.component.hadoop'
    serviceMap['jobtracker-stop']['start'] = {}
    serviceMap['jobtracker-stop']['start']['method'] = 'startJobtracker'
    serviceMap['jobtracker-stop']['start']['kwargs'] = {}
    serviceMap['jobtracker-stop']['stop'] = {}
    serviceMap['jobtracker-stop']['stop']['method'] = 'stopJobtracker'
    serviceMap['jobtracker-stop']['stop']['kwargs'] = {}

    # map for the resource manager service
    # kill the service by defaut
    serviceMap['resourcemanager'] = {}
    serviceMap['resourcemanager']['class'] = 'YARN'
    serviceMap['resourcemanager']['package'] = 'beaver.component.hadoop'
    serviceMap['resourcemanager']['stop'] = {}
    serviceMap['resourcemanager']['stop']['method'] = 'restartActiveRM'
    serviceMap['resourcemanager']['stop']['kwargs'] = {'wait': 10}

    # map to kill all running application masters
    serviceMap['applicationmaster'] = {}
    serviceMap['applicationmaster']['class'] = 'YARN'
    serviceMap['applicationmaster']['package'] = 'beaver.component.hadoop'
    serviceMap['applicationmaster']['stop'] = {}
    serviceMap['applicationmaster']['stop']['method'] = 'killAllRunningAMs'
    serviceMap['applicationmaster']['stop']['kwargs'] = {}

    # map for the hive services
    serviceMap['hive-metastore'] = {}
    serviceMap['hive-metastore']['class'] = 'Hive'
    serviceMap['hive-metastore']['package'] = 'beaver.component.hive'
    serviceMap['hive-metastore']['start'] = {}
    serviceMap['hive-metastore']['start']['method'] = 'startService'
    serviceMap['hive-metastore']['start']['kwargs'] = {'services': 'metastore'}
    serviceMap['hive-metastore']['stop'] = {}
    serviceMap['hive-metastore']['stop']['method'] = 'stopService'
    serviceMap['hive-metastore']['stop']['kwargs'] = {'services': 'metastore'}

    # map for the hbase-master service
    serviceMap['hbase-master'] = {}
    serviceMap['hbase-master']['class'] = 'HBase'
    serviceMap['hbase-master']['package'] = 'beaver.component.hbase'
    serviceMap['hbase-master']['stop'] = {}
    serviceMap['hbase-master']['stop']['method'] = 'restartHAHBaseMasterNode'
    serviceMap['hbase-master']['stop']['kwargs'] = {'wait': 60}

    # map for the oozie service
    # kill the service by defaut
    serviceMap['oozie'] = {}
    serviceMap['oozie']['class'] = 'Oozie'
    serviceMap['oozie']['package'] = 'beaver.component.oozie'
    serviceMap['oozie']['stop'] = {}
    serviceMap['oozie']['stop']['method'] = 'KillAndStartHAOozie'
    serviceMap['oozie']['stop']['kwargs'] = {'suspend': False, 'wait': 10}

    # map for the namenode service on Remote Machine
    serviceMap['restartremoteactivenn'] = {}
    serviceMap['restartremoteactivenn']['class'] = 'FalconAmbariAPIUtil'
    serviceMap['restartremoteactivenn']['package'] = 'beaver.component.falcon_ambari_api_utils'
    serviceMap['restartremoteactivenn']['stop'] = {}
    serviceMap['restartremoteactivenn']['stop']['method'] = 'restartRemoteActiveNN'
    serviceMap['restartremoteactivenn']['stop']['kwargs'] = {'wait': 10}

    # map for the Resourcemanger on Remote Machine
    serviceMap['restartremoteactiverm'] = {}
    serviceMap['restartremoteactiverm']['class'] = 'FalconAmbariAPIUtil'
    serviceMap['restartremoteactiverm']['package'] = 'beaver.component.falcon_ambari_api_utils'
    serviceMap['restartremoteactiverm']['stop'] = {}
    serviceMap['restartremoteactiverm']['stop']['method'] = 'restartRemoteActiveRM'
    serviceMap['restartremoteactiverm']['stop']['kwargs'] = {'wait': 10}

    # service - which service to bounce
    # initialWait - wait before we start the processing, default 30s
    # upTime - how long do you want the service to live, default 5 mins
    # downTime - how long do you want to service to remain down, default 10s
    # maxTime - how long do you want the thread to run, default 3 hours
    # stepTime - sleep time increment for the up time sleep, default to 10s
    # maxTimes - max number of times you want to bounce a service, if present this
    #            will over-ride the time options. If None this will be disregraded
    # startKwargs - any additional kewword args that you want to pass to the start method
    def __init__(
            self,
            service,
            initialWait=30,
            upTime=5 * 60,
            downTime=10,
            maxTime=3 * 60 * 60,
            stepTime=10,
            maxTimes=None,
            startKwargs=None,
            stopKwargs=None
    ):
        logger.info('Setting up HA thread for Service %s', service)
        self.keepAlive.set()
        self.initialWait = int(initialWait)
        self.upTime = int(upTime)
        self.downTime = int(downTime)
        self.maxTime = int(maxTime)
        self.service = service.lower()
        self.stepTime = int(stepTime)
        if maxTimes:
            self.maxTimes = int(maxTimes)
        if startKwargs:
            self.startKwargs = startKwargs
        if stopKwargs:
            self.stopKwargs = stopKwargs
        # Tracks the number of stops performed on this service.
        self.numStops = 0
        # Tracks the number of starts performed on this service.
        self.numStarts = 0
        threading.Thread.__init__(self)

    def run(self):
        logger.info('Starting HA thread for Service %s', self.service)
        logger.info('Initial wait for Service %s: %s', self.service, self.initialWait)
        time.sleep(self.initialWait)
        endTime = int(round(time.time())) + self.maxTime
        service = self.service
        serviceMap = self.serviceMap[service]
        # import the class
        __import__(serviceMap['package'])
        module = sys.modules[serviceMap['package']]
        cls = getattr(module, serviceMap['class'])
        # get the start method if it exists
        startMethod = None
        if 'start' in serviceMap and 'method' in serviceMap['start']:
            startMethod = getattr(cls, serviceMap['start']['method'])
        # get the stop method if it exists
        stopMethod = None
        if 'stop' in serviceMap and 'method' in serviceMap['stop']:
            stopMethod = getattr(cls, serviceMap['stop']['method'])
        i = 0
        while self.keepAlive.isSet() and endTime > int(round(time.time())):
            # only stop if a method is found
            if stopMethod:
                logger.info('stop %s', service)
                stopArgs = dict(serviceMap['stop']['kwargs'])
                # append stop kwargs
                if self.stopKwargs:
                    stopArgs.update(self.stopKwargs)
                # stop the service
                stopMethod(**stopArgs)
                self.numStops += 1
            # now sleep for down time
            logger.info('waiting for down time %s seconds for service %s', self.downTime, self.service)
            time.sleep(self.downTime)
            # only do the start logic if start method is found
            if startMethod:
                # now start the serivce
                logger.info('start %s', service)
                startArgs = dict(serviceMap['start']['kwargs'])
                # append start kwargs
                if self.startKwargs:
                    startArgs.update(self.startKwargs)
                startMethod(**startArgs)
                self.numStarts += 1
            i += 1
            if self.maxTimes and self.maxTimes <= i:
                logger.info(
                    'Service %s already bounced max number of times %s coming out of the thread.', service,
                    self.maxTimes
                )
                break
            # now wait for up time
            logger.info('Uptime remaining for service %s: %s seconds', self.service, self.upTime)
            counter = self.upTime
            while self.keepAlive.isSet() and counter > 0:
                time.sleep(self.stepTime)
                counter -= self.stepTime
                logger.info('Uptime remaining for service %s: %s seconds', self.service, counter)


class HAUtils:
    serviceMap = {}

    # for namenode
    serviceMap['namenode'] = {}
    serviceMap['namenode']['class'] = 'HDFS'
    serviceMap['namenode']['package'] = 'beaver.component.hadoop'
    serviceMap['namenode']['method'] = 'waitForNNOutOfSafemode'
    serviceMap['namenode']['kwargs'] = {}

    @classmethod
    @TaskReporter.report_test()
    def terminate(self, thread, timeout=10.0 * 60):
        # we could do some service specific termination here
        logger.info('Terminate HA thread for Service %s' % thread.service)
        thread.keepAlive.clear()
        thread.join(timeout)

        # TODO: Handle the case when the process is alive after join
        # if the thread is still alive after join kill it
        #if thread.isAlive():

        # if the service is not in the map then we
        # dont want to do anything
        if thread.service not in self.serviceMap:
            return

        # otherwise run the method that has been configured
        serviceMap = self.serviceMap[thread.service]
        __import__(serviceMap['package'])
        module = sys.modules[serviceMap['package']]
        cls = getattr(module, serviceMap['class'])
        method = getattr(cls, serviceMap['method'])
        method(**serviceMap['kwargs'])

    @classmethod
    @TaskReporter.report_test()
    def getResetCountersForAllServices(cls, threadList):
        '''
        Returns a map of service names and the # of starts and stops performed per service.
        :param threadList: A list of thread objs.
        :return: A map -> { service: [# starts, # stops]}
        '''
        if not threadList:
            return None

        tmpMap = {}
        for thread in threadList:
            tmpMap[thread.service] = [thread.numStarts, thread.numStops]
        return tmpMap

    @classmethod
    @TaskReporter.report_test()
    def getResetCounters(cls, threadList, service):
        '''
        Returns the # of starts and stops performed on a given service.
        :param threadList: A list of thread objs.
        :param service: Name of the service for which the counters need to be retrieved.
        :return: (# starts for the service, # stops for the service)
        '''
        if not threadList or not service:
            return None

        for thread in threadList:
            if thread.service == service:
                return (thread.numStarts, thread.numStops)
            return (0, 0)
