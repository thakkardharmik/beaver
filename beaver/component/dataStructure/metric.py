#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import os, logging, time
from ...config import Config
from ... import util
from ...machine import Machine
from ..hadoop import Hadoop, MAPRED, YARN

#initialize constants
logger = logging.getLogger(__name__)


#This class is to manage JMXTerm Tool call.
class JMXTerm:
    def __init__(self, yarnUser, rmHost, jmxPort):
        self.yarnUser = yarnUser
        self.rmHost = rmHost
        self.jmxPort = str(jmxPort)
        self.BEAN_DEFAULT_QUEUE = self.getQueueBeanName("root")

    #Returns path of JMXTerm Tool in data directory
    @classmethod
    def getJMXTermJarPath(cls):
        return os.path.join(Config.getEnv('WORKSPACE'), 'data', 'metrics', 'jmxterm-1.0-SNAPSHOT-uber.jar')

    #Returns open command used for JMXTerm connection
    def _getOpenCommand(self):
        return "open " + YARN.getResourceManagerHost() + ":" + self.jmxPort

    # Restart RM with metric enabled
    def restartRMEnableMetric(self, restoreConfigFirst=True):
        '''
        If running in Windows and restoreConfigFirst is True, restore config first.
        '''
        if Machine.isWindows() and restoreConfigFirst:
            self.restoreRMConfig()
        if Machine.isLinux():
            oldParamName = '$YARN_RESOURCEMANAGER_OPTS'
            quote = '"'
        else:
            oldParamName = '%YARN_RESOURCEMANAGER_OPTS%'
            quote = ''
        rmJmxOpts='%s YARN_RESOURCEMANAGER_OPTS=%s-Dcom.sun.management.jmxremote ' % (Machine.getExportCmd(), quote)+ \
                  '-Dcom.sun.management.jmxremote.port=%s -Dcom.sun.management.jmxremote.authenticate=false ' % self.jmxPort + \
                  '-Dcom.sun.management.jmxremote.ssl=false %s%s' % (oldParamName, quote)
        mod_conf_path = Hadoop.getModifiedConfigPath()
        Hadoop.modifyConfig(
            {
                Hadoop.getHadoopEnvSh(): [rmJmxOpts]
            }, {'services': ['jobtracker']},
            makeCurrConfBackupInWindows=False,
            regenServiceXmlsInWindows=True
        )
        MAPRED.restartJobtracker(config=mod_conf_path)
        logger.info("restartRMEnableMetric sleeps for 5 sec")
        time.sleep(5)

    # Restores RM config and restarts RM.
    def restoreRMConfig(self):
        Hadoop.restoreConfig([Hadoop.getHadoopEnvSh()], {'services': ['jobtracker']})
        MAPRED.restartJobtracker()

    #Runs JMXTerm Tool with given command list
    def run(self, cmdList, showCommands=True, logOutput=True):
        if showCommands:
            logger.info("Running JMXTerm with these commands:")
            for index in xrange(len(cmdList)):
                logger.info("%s: %s" % (index + 1, cmdList[index]))
        jmxTermJarPath = self.getJMXTermJarPath()
        tempFile = util.writeToTempFile('\n'.join(cmdList))
        Machine.chmod("777", tempFile)
        (exit_code, stdout) = Machine.runJarAs(self.yarnUser, jmxTermJarPath, ["-i", tempFile], logoutput=logOutput)
        if logOutput:
            logger.info("JMXTerm.runJarAs returns (%s,%s)" % (exit_code, stdout))
        os.remove(tempFile)
        tmpOutput = stdout.split('\n')[4:]
        tmpOutput = [line for line in tmpOutput if line.startswith("#") == False]
        cutOutput = '\n'.join(tmpOutput)
        return (exit_code, cutOutput)

    # Get queue bean name for queues
    # queues is a list of queue name or a queue name.
    @classmethod
    def getQueueBeanName(cls, queues):
        bean = "Hadoop:name=QueueMetrics"
        if type(queues) == list:
            for index in range(len(queues)):
                bean += ",q%d=%s" % (index, queues[index])
        else:
            bean += ",q0=%s" % queues
        bean += ",service=ResourceManager"
        return bean

    #Uses JMXTerm to get attributes available in given bean
    #Returns list of tuple (index,"-", attribute name, java type)
    def getBeanInfo(self, bean, domain="Hadoop", logOutput=True):
        cmdList = []
        cmdList.append(self._getOpenCommand())
        cmdList.append("domain %s" % domain)
        cmdList.append("bean " + bean)
        cmdList.append("info")
        (_, stdout) = self.run(cmdList, logOutput=logOutput)
        lines = stdout.split('\n')
        result = []
        for line in lines:
            lineTuple = line.split()
            newLineTuple = ()
            for index in range(len(lineTuple)):
                newElem = lineTuple[index]
                newElem = newElem.replace(",", "")
                newElem = newElem.replace("(", "")
                newElem = newElem.replace(")", "")
                newLineTuple = newLineTuple + (newElem, )
            result.append(newLineTuple)
        return result

    #Use JMXTerm to get bean list
    #Returns list of beans
    def getBeans(self, domain="Hadoop", logOutput=True):
        cmdList = []
        cmdList.append(self._getOpenCommand())
        cmdList.append("domain %s" % domain)
        cmdList.append("beans")
        (_, stdout) = self.run(cmdList, logOutput=logOutput)
        return stdout.split('\n')

    #Uses JMXTerm to get value of given bean and attribute.
    #Return attributeValue
    def getAttribute(self, bean, attr, logOutput=False):
        if bean is None:
            bean = self.BEAN_DEFAULT_QUEUE
        cmdList = []
        cmdList.append(self._getOpenCommand())
        cmdList.append("bean " + bean)
        cmdList.append("get " + attr)
        (exit_code, stdout) = self.run(cmdList, logOutput=logOutput)
        if logOutput:
            logger.info("metric.getAttribute (exit_code,stdout)=(%s,%s)" % (exit_code, stdout))
        if stdout.find("=") == -1:
            return None
        else:
            return stdout[stdout.find("=") + 1:].strip()[:-1]

    # Get AllocatedMB metric as a string
    def getAllocatedMB(self, bean=None):
        return self.getAttribute(bean, "AllocatedMB")

    # Get AvailableMB metric as a string
    def getAvailableMB(self, bean=None):
        return self.getAttribute(bean, "AvailableMB")

    # Get AppsRunning metric as a string
    def getAppsRunning(self, bean=None):
        return self.getAttribute(bean, "AppsRunning")

    # Get AppsSubmitted metric as a string
    def getAppsSubmitted(self, bean=None):
        return self.getAttribute(bean, "AppsSubmitted")

    # Get AppsCompleted metric as a string
    def getAppsCompleted(self, bean=None):
        return self.getAttribute(bean, "AppsCompleted")
