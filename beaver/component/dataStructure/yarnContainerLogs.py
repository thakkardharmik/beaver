import re, copy, logging, collections

logger = logging.getLogger(__name__)


# This class contains stdout, stderr, syslog, AppMaster.stdout, AppMaster.stderr for each container.
# One-Container logs
class OneContainerLogs:
    def __init__(self):
        self.__numEntries = 0
        self.__logTypes = []
        self.__logLengths = []
        self.__logContents = []

    @property
    def numEntries(self):
        return self.__numEntries

    @property
    def logTypes(self):
        return self.__logTypes

    # Log length as appeared in $ yarn logs output
    @property
    def logLengths(self):
        return self.__logLengths

    # Log contents after the class strip off some blank lines
    @property
    def logContents(self):
        return self.__logContents

    def addNewEntry(self):
        self.__numEntries += 1
        self.__logTypes.append("")
        self.__logLengths.append(0)
        self.__logContents.append("")

    def setLastEntryLogType(self, logType):
        self.__logTypes[-1] = logType

    def setLastEntryLogLength(self, logLength):
        self.__logLengths[-1] = int(logLength)

    # append a string of log contents to last log entries
    def appendLastEntryLogContents(self, logContentsToAppend):
        self.__logContents[-1] += logContentsToAppend

    def getLastEntryLogContents(self):
        return self.__logContents[-1]

    # Returns True is there is worker log in this container.
    def isWorker(self):
        return not self.isAppMaster()

    # Returns True is there is app master log in this container.
    def isAppMaster(self):
        #In distributed shell app, log types are marked as AppMaster.stdout
        foundAppMaster = any(logType in ("AppMaster.stdout", "AppMaster.stderr") for logType in self.__logTypes)
        #In real MR job, it uses stdout, stderr and syslog like other containers.
        #But in syslog, first line is "INFO [main]org.apache.hadoop.mapreduce.v2.app.MRAppMaster: Created MRAppMaster for application appattempt_1379914899697_0001_000001".
        if foundAppMaster:
            return True
        for entry in self.logContents:
            # Optimization is needed. Regex matching is expensive.
            if len(entry) > 155:
                entry = entry[0:156]
                if entry is not None and (re.search("Created MRAppMaster", entry) is not None
                                          or re.search("Created DAGAppMaster", entry) is not None):
                    foundAppMaster = True
                    break
        return foundAppMaster

    class LogLengthContents(collections.namedtuple('LogLengthContents', ['type', 'length', 'contents'])):
        def __str__(self):
            s = ""
            s += "LogType:%s\n" % self.type
            s += "LogLength:%s\n" % self.length
            s += "Log Contents:\n%s\n" % self.contents
            return s

    # Returns (length, contents) of log of target type.
    # Returns None if targetLogType is not found
    def getLogLengthContent(self, targetLogType):
        for i in range(self.__numEntries):
            if self.__logTypes[i] == targetLogType:
                return OneContainerLogs.LogLengthContents(targetLogType, self.__logLengths[i], self.__logContents[i])
        return None

    # Returns (length, contents) of stdout log found in this container.
    # Returns None if stdout is not found
    def getStdout(self):
        return self.getLogLengthContent("stdout")

    # Returns (length, contents) of stderr log found in this container.
    # Returns None if stderr is not found
    def getStderr(self):
        return self.getLogLengthContent("stderr")

    # Returns (length, contents) of syslog log found in this container.
    # Returns None if syslog is not found
    def getSyslog(self):
        return self.getLogLengthContent("syslog")

    # Deletes last N lines from last entry's log contents.
    # If deletion until empty occurs, the contents are "".
    def deleteLastNLinesFromLastEntryLogContents(self, n):
        logContents = self.__logContents[-1]
        lineList = logContents.split("\n")
        oldLineList = copy.copy(lineList)
        lineList = lineList[0:len(lineList) - n]
        self.__logContents[-1] = '\n'.join(lineList)

    def __str__(self):
        sList = ["OneContainerLogs: Number of container log entries: %s" % self.numEntries]
        for j in range(self.numEntries):
            sList.append("Log entry no. %s/%s" % (j + 1, self.numEntries))
            sList.append("LogType:%s" % self.logTypes[j])
            sList.append("LogLength:%s" % self.logLengths[j])
            if self.logContents[j] == "":
                sList.append("LogContents:<blank string>")
            else:
                sList.append("LogContents:")
                numLinesShown = 3
                tmp = self.logContents[j].split('\n')
                tmpS = '(first %s lines)\n' % numLinesShown
                for line in tmp[0:numLinesShown]:
                    tmpS += line + '\n'
                sList.append(tmpS)
            sList.append("")
        sList.append("")
        return '\n'.join(sList)


# This class is a data structure for YARN logs result.
class YarnContainerLogs:
    def __init__(self):
        self.__numContainers = 0
        self.__containerIDs = []
        self.__hosts = []
        self.__ports = []
        #a list of OneContainerLogs object
        self.__containerLogs = []

    @property
    def numContainers(self):
        return self.__numContainers

    @property
    def containerIDs(self):
        return self.__containerIDs

    @property
    def hosts(self):
        return self.__hosts

    @property
    def ports(self):
        return self.__ports

    @property
    def containerLogs(self):
        return self.__containerLogs

    def addLogContents(self, tmpLogContents, logParsing=False):
        numDeleteLines = 3
        for lineNo in range(numDeleteLines):
            if len(tmpLogContents) > 0 and tmpLogContents[-1] == "":
                tmpLogContents.pop()
        lastContainerLogIndex = len(self.__containerLogs) - 1
        while lastContainerLogIndex >= 0 and self.__containerLogs[lastContainerLogIndex].numEntries == 0:
            lastContainerLogIndex -= 1
        if lastContainerLogIndex >= 0:
            containerLogObj = self.__containerLogs[lastContainerLogIndex]
            containerLogObj.appendLastEntryLogContents('\n'.join(tmpLogContents))
            if logParsing:
                logger.info(
                    "AddLogContents: adding the entry to containerLogs index %s. numEntries=%s" %
                    (lastContainerLogIndex, containerLogObj.numEntries)
                )

    # Returns an object for given logs, which are supposed to be stdout of $ yarn logs command
    # In Windows, \r and spaces are at end of line. Recommended to always stripLine=True in Windows.
    # It is using rstrip for now to limit negative impact.
    def parse(self, logs, logParsing=False, rstripLine=False):
        lines = logs.split("\n")
        inLogContents = False
        tmpLogContents = []
        for line in lines:
            if rstripLine:
                line = line.rstrip()
            if logParsing == True:
                logger.info("yarn container log parsing. line=%s" % line)
            matchObj = re.search("Container: (.+) on (.+)", line)
            if matchObj is not None:
                while self.__numContainers > len(self.__containerLogs):
                    self.__containerLogs.append(OneContainerLogs())
                inLogContents = False
                self.__numContainers += 1
                self.__containerIDs.append(matchObj.group(1))
                self.__hosts.append(matchObj.group(2).split("_")[0])
                self.__ports.append(matchObj.group(2).split("_")[1])

            matchObj = re.search("LogType:(.+)", line)
            if matchObj is not None:
                if len(self.__containerLogs) > 0:
                    self.addLogContents(tmpLogContents, logParsing)
                    tmpLogContents = []
                inLogContents = False
                if len(self.__containerLogs) < self.__numContainers:
                    self.__containerLogs.append(OneContainerLogs())
                self.__containerLogs[-1].addNewEntry()
                self.__containerLogs[-1].setLastEntryLogType(matchObj.group(1))

            matchObj = re.search("LogLength:(.+)", line)
            if matchObj is not None:
                self.__containerLogs[-1].setLastEntryLogLength(matchObj.group(1))
            matchObj = re.search("Log Contents:", line)
            if matchObj is not None:
                inLogContents = True
            elif inLogContents:
                tmpLogContents.append(line)
        while self.__numContainers > len(self.__containerLogs):
            self.__containerLogs.append(OneContainerLogs())
        if len(self.__containerLogs) > 0:
            self.addLogContents(tmpLogContents, logParsing)

    def __str__(self):
        sList = ["Number of containers: %s" % self.__numContainers]
        for i in range(self.__numContainers):
            sList.append("%s. %s at %s:%s" % (i, self.__containerIDs[i], self.__hosts[i], self.__ports[i]))
            containerLogs = self.__containerLogs[i]
            for j in range(containerLogs.numEntries):
                sList.append("LogType:%s" % containerLogs.logTypes[j])
                sList.append("LogLength:%s" % containerLogs.logLengths[j])
                if containerLogs.logContents[j] == "":
                    sList.append("LogContents:N/A (it is empty string.)")
                else:
                    sList.append("LogContents:")
                    sList.append(containerLogs.logContents[j])
        return '\n'.join(sList)

    class WorkerAMLog(collections.namedtuple('WorkerAMLog', ['containerID', 'host', 'port', 'containerLogs'])):
        def __str__(self):
            s = ""
            s += "Container ID: %s\n" % self.containerID
            s += "Host/Port: %s:%s\n" % (self.host, self.port)
            s += "Container Logs:\n%s\n" % self.containerLogs
            return s

    # Returns list of (containerID, host, port, OneContainerLogs object) of worker containers
    # or [] if no worker logs is found.
    def getWorkerLogs(self):
        result = []
        for i in range(self.__numContainers):
            if self.__containerLogs[i].isWorker():
                result.append(
                    YarnContainerLogs.WorkerAMLog(
                        self.__containerIDs[i], self.__hosts[i], self.__ports[i], self.containerLogs[i]
                    )
                )
        '''
        print("getWorkerLogs")
        for i in range(len(result)):
            print(result[i])
        '''
        return result

    # Returns list of (containerID, host, port, OneContainerLogs object) of AppMaster container
    # or None if no AppMaster logs is found.
    def getAppMasterLogs(self, logoutput=True):
        #print "getAppMasterLog"
        result = None
        #print "number of containers = %s" % self.__numContainers
        for i in range(self.__numContainers):
            #print "container no. %s" % i
            #print "container %s on %s:%s" % (self.__containerIDs[i], self.__hosts[i], self.__ports[i])
            #print self.__containerLogs[i]
            if not self.__containerLogs[i].isWorker():
                if result is None:
                    result = []
                result.append(
                    YarnContainerLogs.WorkerAMLog(
                        self.__containerIDs[i], self.__hosts[i], self.__ports[i], self.containerLogs[i]
                    )
                )
        if logoutput:
            logger.info("getAppMasterLogs")
            if result is not None:
                for i in range(len(result)):
                    logger.info(result[i])
            else:
                logger.info("returns None.")
        return result

    # Returns (length, contents) of first (by default) AppMaster's stdout or None if not found.
    def getAppMasterStdout(self, amIndex=0, logoutput=False):
        if self.getAppMasterLogs() is None:
            return None
        else:
            containerLogs = self.getAppMasterLogs(logoutput)[amIndex][3]
            if containerLogs.getLogLengthContent("AppMaster.stdout") is not None:
                return containerLogs.getLogLengthContent("AppMaster.stdout")
            else:
                return containerLogs.getLogLengthContent("stdout")

    # Returns (length, contents) of first (by default) AppMaster's stderr or None if not found.
    def getAppMasterStderr(self, amIndex=0, logoutput=False):
        if self.getAppMasterLogs() is None:
            return None
        else:
            containerLogs = self.getAppMasterLogs(logoutput)[amIndex][3]
            if containerLogs.getLogLengthContent("AppMaster.stderr") is not None:
                return containerLogs.getLogLengthContent("AppMaster.stderr")
            else:
                return containerLogs.getLogLengthContent("stderr")

    # Returns (length, contents) of first (by default) AppMaster's syslog or None if not found.
    def getAppMasterSyslog(self, amIndex=0, logoutput=False):
        if self.getAppMasterLogs() is None:
            return None
        else:
            containerLogs = self.getAppMasterLogs(logoutput)[amIndex][3]
            if containerLogs.getLogLengthContent("syslog") is not None:
                return containerLogs.getLogLengthContent("syslog")
            else:
                return None
