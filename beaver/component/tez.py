#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, collections, urllib, pytest
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, MAPRED, YARN, HDFS
from beaver.java import Java
from beaver import util
from beaver import configUtils
from beaver.component.dataStructure.yarnContainerLogs import YarnContainerLogs
from beaver.component.ambari import Ambari
from beaver.component.hive import Hive

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))


class Tez:
    __vertexId_dagId = {}
    __gStartTime = 0
    __gEndTime = 0

    DAGHistoryEventTaskLog = collections.namedtuple(
        'DAGHistoryEventTaskLog', [
            'vertexName', 'taskAttemptId', 'status', 'fileBytesWritten', 'mapInputRecords', 'mapOutputRecords',
            'mapOutputBytes'
        ]
    )
    DAGHistoryEventVertexLog = collections.namedtuple(
        'DAGHistoryEventVertexLog', [
            'vertexName', 'vertexId', 'status', 'fileBytesWritten', 'mapInputRecords', 'mapOutputRecords',
            'mapOutputBytes'
        ]
    )

    @classmethod
    def getVersion(cls):
        # determine the tez version from the jar that is deployed
        if YARN.check_if_component_is_tez_v2():
            HDP_VERSION = Ambari.getHDPVersion()[6:]
            jarDir = os.path.join(os.path.sep, 'usr', 'hdp', HDP_VERSION, 'tez_hive2')
        else:
            jarDir = Config.get('tez', 'TEZ_HOME')
        files = util.findMatchingFiles(jarDir, "tez-common-*.jar")
        p = re.compile('tez-common-(\S+).jar')
        m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    def getConfigValue(cls, propertyName, defaultValue=None):
        '''
        Gets value of specified property name from /etc/hadoop/conf/tez-site.xml
        '''
        tezSiteXml = os.path.join(Config.get('tez', 'TEZ_CONF_DIR'), "tez-site.xml")
        if os.path.exists(tezSiteXml):
            return util.getPropertyValueFromConfigXMLFile(tezSiteXml, propertyName, defaultValue=defaultValue)
        elif YARN.check_if_component_is_tez_v2():
            config = Ambari.getConfig("tez-interactive-site")
            if config.has_key(propertyName):
                return config[propertyName]
        return defaultValue

    @classmethod
    def getHDFSTezLibPath(cls):
        '''
        Returns a string of HDFS path for Tez lib directory.
        Should be the value set in tez.lib.uris property.
        
        "/apps/tez/lib" (hdfs:users, 644)
        '''
        return "/apps/tez/lib"

    @classmethod
    def copyJarIntoHDFSTezLibPath(cls, localJarPath, dochmod=False, logoutput=True):
        '''
        Copies a jar into HDFS Tez lib directory.
        Returns HDFS path of the jar. ("/apps/tez/lib/myfile.jar")
        If dochmod == True, local jar path is given 777 before hdfs copy.
        '''
        hdfsJarPath = cls.getHDFSTezLibPath() + "/" + localJarPath.split(os.sep)[-1]
        if dochmod:
            Machine.chmod("777", localJarPath, logoutput=logoutput)
        HDFS.copyFromLocal(localJarPath, hdfsJarPath, user=HDFS.getHDFSUser())
        HDFS.chmod(runasUser=HDFS.getHDFSUser(), perm=644, directory=hdfsJarPath)
        return hdfsJarPath

    @classmethod
    def getExampleJar(cls, logoutput=False):
        '''
        Returns full path of tez example jar.
        e.g. "/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples-2.2.0.2.1.0.0-71.jar"
        '''
        examplesJar = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=Config.get('tez', 'TEZ_HOME'),
            searchstr='tez-tests-*.jar',
            passwd=Machine.getAdminPasswd()
        )[0]
        return examplesJar

    @classmethod
    def getTestsJar(cls, logoutput=False):
        '''
        Returns full path of tez tests jar.
        e.g. "/usr/hdp/current/tez-client/tez-tests-0.7.0.2.3.0.0-1496.jar"
        '''
        jar = Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=Config.get('tez', 'TEZ_HOME'),
            searchstr='tez-tests-*.jar',
            passwd=Machine.getAdminPasswd()
        )[0]
        return jar

    @classmethod
    def getCrossproductJar(cls):
        '''
        Returns full path of tez example jar.
        e.g. "/usr/hdp/<version>/tez_hive2/tez-examples-2.2.0.2.1.0.0-71.jar"
        '''
        HDP_VERSION = Ambari.getHDPVersion()[6:]
        if YARN.check_if_component_is_tez_v2():
            examplesJar = Machine.find(
                user=Machine.getAdminUser(),
                host='',
                filepath='/usr/hdp/%s/tez_hive2/' % HDP_VERSION,
                searchstr='tez-examples-*.jar',
                passwd=Machine.getAdminPasswd()
            )[0]
            return examplesJar
        else:
            return None

    @classmethod
    def runTezExampleJar(cls, args, env=None, logoutput=True, config=None, runInBackground=False, user=None):
        '''
        Run tez product example jar.
        Returns (exit_code, stdout) if running in foreground.
        Returns pOpenObj if running in background.
        '''
        # -Duser.name=<user> must be set already in args.
        # Otherwise, /tmp/${current_user}/staging will be used. You can get /tmp/hrt_qa with <the user> as an owner.
        if YARN.check_if_component_is_tez_v2():
            logger.info("get HSI host : %s " % Hive.getHiveHost())
            logger.info("local Machine host: %s" % Machine.getfqdn())
            HDP_VERSION = Ambari.getHDPVersion()[6:]
            HDP_VERSION_PATH = os.path.join(os.path.sep, 'usr', 'hdp', HDP_VERSION)
            TEZ_HIVE2_SITE = os.path.join(HDP_VERSION_PATH, 'tez_hive2', 'conf', 'tez-site.xml')
            if Hive.getHiveHost() != Machine.getfqdn():
                if not os.path.exists(TEZ_HIVE2_SITE):
                    Machine.copyToLocal(
                        Machine.getAdminUser(), Hive.getHiveHost(), TEZ_HIVE2_SITE, TEZ_HIVE2_SITE,
                        Machine.getAdminPasswd()
                    )
        cmd = "jar " + cls.getExampleJar() + " " + args
        if YARN.check_if_component_is_tez_v2():
            HDP_VERSION = Ambari.getHDPVersion()[6:]
            HDP_VERSION_PATH = os.path.join(os.path.sep, 'usr', 'hdp', HDP_VERSION)
            env = {
                'IS_HIVE2': 'true',
                'HADOOP_CLASSPATH': '${HADOOP_CLASSPATH}:' + HDP_VERSION_PATH + '/tez_hive2/*:' + HDP_VERSION_PATH +
                '/tez_hive2/lib/*:' + HDP_VERSION_PATH + '/tez_hive2/conf'
            }
        if not runInBackground:
            if user is None:
                return Hadoop.run(cmd, env=env, logoutput=logoutput, config=config)
            else:
                return Hadoop.runas(user, cmd, env=env, logoutput=logoutput, config=config)
        else:
            if user is None:
                return Hadoop.runInBackground(cmd, env=env, config=config)
            else:
                return Hadoop.runInBackgroundAs(user, cmd, env=env, config=config)

    @classmethod
    def runTezCrossProductExample(cls, args, env=None, logoutput=True, config=None, runInBackground=False, user=None):
        '''
        run only for Tez_v2
        '''
        if YARN.check_if_component_is_tez_v2():
            logger.info("get HSI host : %s " % Hive.getHiveHost())
            logger.info("local Machine host: %s" % Machine.getfqdn())
            HDP_VERSION = Ambari.getHDPVersion()[6:]
            HDP_VERSION_PATH = os.path.join(os.path.sep, 'usr', 'hdp', HDP_VERSION)
            TEZ_HIVE2_SITE = os.path.join(HDP_VERSION_PATH, 'tez_hive2', 'conf', 'tez-site.xml')
            if Hive.getHiveHost() != Machine.getfqdn():
                if not os.path.exists(TEZ_HIVE2_SITE):
                    Machine.copyToLocal(
                        Machine.getAdminUser(), Hive.getHiveHost(), TEZ_HIVE2_SITE, TEZ_HIVE2_SITE,
                        Machine.getAdminPasswd()
                    )
            env = {
                'IS_HIVE2': 'true',
                'HADOOP_CLASSPATH': '${HADOOP_CLASSPATH}:' + HDP_VERSION_PATH + '/tez_hive2/*:' + HDP_VERSION_PATH +
                '/tez_hive2/lib/*:' + HDP_VERSION_PATH + '/tez_hive2/conf'
            }
        cmd = "jar " + cls.getCrossproductJar() + " " + args
        if not runInBackground:
            if user is None:
                return Hadoop.run(cmd, env=env, logoutput=logoutput, config=config)
            else:
                return Hadoop.runas(user, cmd, env=env, logoutput=logoutput, config=config)
        else:
            if user is None:
                return Hadoop.runInBackground(cmd, env=env, config=config)
            else:
                return Hadoop.runInBackgroundAs(user, cmd, env=env, config=config)

    @classmethod
    def getModifiedTezExampleDir(cls):
        '''
        Returns WORKSPACE/beaver/java/tez
        '''
        return Java.getBeaverJavaTezExamplesDir()

    @classmethod
    def getTezMRExamplesDir(cls):
        '''
        Returns tez-mapreduce-examples.
        This can be used with getModifiedTezExampleDir(cls) to find 
        jar built by maven. 
        '''
        return 'tez-mapreduce-examples'

    @classmethod
    def getModifiedTezExamplesJar(cls, baseDir):
        '''
        This is to get custom tez example jar. Use it only when you know the difference.
        To get standard tez example jar, use getTezExamplesJar.
        '''
        findResult = Machine.find(
            user=None,
            host=None,
            filepath=baseDir,
            searchstr="tez-mapreduce-examples-*-SNAPSHOT.jar",
            passwd=None,
            logoutput=True
        )
        if findResult is None or len(findResult) == 0:
            return None
        else:
            return findResult[0]

    @classmethod
    def _getLogsTaskAttemptFinished(cls, appID, logoutput=False):
        time.sleep(10)
        return YARN.getLogsApplicationID(appID, logoutput=logoutput, grepFilter="TASK_ATTEMPT_FINISHED")

    @classmethod
    def getLogsTaskAttemptFinished(cls, appID, logoutput=False):
        (exit_code, stdout) = cls._getLogsTaskAttemptFinished(appID, logoutput=True)
        if exit_code != 0:
            return None
        lines = stdout.split('\n')
        result = []
        for line in lines:
            if re.search("HistoryEventHandler", line) is not None:
                (vertexName, taskAttemptId,
                 status) = re.findall("vertexName=(.*?),.*?taskAttemptId=(.*?),.*?status=(.*?),", line)[0]
                fileBytesWritten = re.findall("FILE: BYTES_WRITTEN=(.*?),", line)
                mapInputRecords = re.findall("INPUT_RECORDS_PROCESSED=(.*?),", line)
                mapOutputRecords = re.findall("OUTPUT_RECORDS=(.*?),", line)
                mapOutputBytes = re.findall("OUTPUT_BYTES=(.*?),", line)
                if fileBytesWritten == []:
                    fileBytesWritten = None
                else:
                    fileBytesWritten = int(fileBytesWritten[0])
                if mapInputRecords == []:
                    mapInputRecords = None
                else:
                    mapInputRecords = int(mapInputRecords[0])
                if mapOutputRecords == []:
                    mapOutputRecords = None
                else:
                    mapOutputRecords = int(mapOutputRecords[0])
                if mapOutputBytes == []:
                    mapOutputBytes = None
                else:
                    mapOutputBytes = int(mapOutputBytes[0])
                entry = Tez.DAGHistoryEventTaskLog(
                    vertexName, taskAttemptId, status, fileBytesWritten, mapInputRecords, mapOutputRecords,
                    mapOutputBytes
                )
                result.append(entry)
        return result

    @classmethod
    def _getLogsVertexFinished(cls, appID, logoutput=False):
        return YARN.getLogsApplicationID(appID, logoutput=logoutput, grepFilter="VERTEX_FINISHED")

    @classmethod
    def getLogsVertexFinished(cls, appID, logoutput=False):
        (exit_code, stdout) = cls._getLogsVertexFinished(appID, logoutput=logoutput)
        if exit_code != 0:
            return None
        lines = stdout.split('\n')
        result = []
        for line in lines:
            if re.search("HistoryEventHandler", line) is not None:
                (vertexName, vertexId,
                 status) = re.findall("vertexName=(.*?),.*?vertexId=(.*?),.*?status=(.*?),", line)[0]
                fileBytesWritten = re.findall("FILE: BYTES_WRITTEN=(.*?),", line)
                #this works correctly only if the vertex is a mapper. Needs further fix.
                mapInputRecords = re.findall("INPUT_RECORDS_PROCESSED=(.*?),", line)
                mapOutputRecords = re.findall("OUTPUT_RECORDS=(.*?),", line)
                mapOutputBytes = re.findall("OUTPUT_BYTES=(.*?),", line)
                if fileBytesWritten == []:
                    fileBytesWritten = None
                else:
                    fileBytesWritten = int(fileBytesWritten[0])
                if mapInputRecords == []:
                    mapInputRecords = None
                else:
                    mapInputRecords = int(mapInputRecords[0])
                if mapOutputRecords == []:
                    mapOutputRecords = None
                else:
                    mapOutputRecords = int(mapOutputRecords[0])
                if mapOutputBytes == []:
                    mapOutputBytes = None
                else:
                    mapOutputBytes = int(mapOutputBytes[0])
                entry = Tez.DAGHistoryEventVertexLog(
                    vertexName, vertexId, status, fileBytesWritten, mapInputRecords, mapOutputRecords, mapOutputBytes
                )
                result.append(entry)
        return result

    @classmethod
    def getAppIDFromCompletedAppStdout(cls, stdout):
        #client.TezSession: Shutting down Tez Session, sessionName=OrderedWordCountSession, applicationId=application_1384284330310_0155
        matchObj = re.search("Shutting down Tez Session.*applicationId=(.*)", stdout)
        if matchObj is None:
            return None
        else:
            return matchObj.group(1)

    @classmethod
    def getAppIdforSleepJob(cls, stdout):
        #matchObj = re.search("client.TezClient: Submitting DAG to YARN, applicationId=(.*)", stdout)
        matchObj = re.search("client.TezClient: Submitting DAG application with id:\s*(.*)", stdout)
        if matchObj is None:
            return None
        else:
            return matchObj.group(1)

    @classmethod
    def getCountTaskAttemptFinished(cls, appID, vertexName, logoutput=True, status=None):
        '''
        Gets count of task attempt finished of given status from Tez AM logs.
        Returns an int.
        '''
        count = 0
        logs = Tez.getLogsTaskAttemptFinished(appID)
        if logs == None:
            time.sleep(10)
            logs = Tez.getLogsTaskAttemptFinished(appID)
        for entry in logs:
            if logoutput:
                logger.info(entry)
            if entry.vertexName == vertexName:
                if status is None or status == entry.status:
                    count += 1
        return count

    @classmethod
    def getTotalMapOutputBytesVertexFinished(cls, appID, vertexName, logoutput=True, status=None):
        '''
        Gets sum of map output bytes of finished vertices of given status from Tez AM logs.
        Returns an int.
        '''
        total = 0
        logs = Tez.getLogsVertexFinished(appID)
        for entry in logs:
            if logoutput:
                logger.info(entry)
            if entry.vertexName == vertexName:
                if status is None or status == entry.status:
                    total += entry.mapOutputBytes
        return total

    # This function creates a collection of tuple to get Vertex Name, AttemptId and ContainerId
    @classmethod
    def getContainerListforDAG(cls, dagId, Amlog):
        DAGHistoryEventContainerLog = collections.namedtuple(
            'DAGHistoryEventContainerLog', ['vertexName', 'attemptId', 'containerId']
        )
        lines = Amlog.split('\n')
        result = []
        for line in lines:
            if re.search("HistoryEventHandler", line) is not None and re.search(
                    "Event:TASK_ATTEMPT_STARTED", line) is not None and re.search("DAG:" + dagId, line) is not None:
                logger.info(line)
                (vertexName, attemptId,
                 containerId) = re.findall("vertexName=(.*?), taskAttemptId=(.*?),.*?containerId=(.*?),", line)[0]
                entry = DAGHistoryEventContainerLog(vertexName, attemptId, containerId)
                result.append(entry)
            if re.search("Ignoring unknown container", line) is not None:
                logger.info(line)
                containerId = re.findall("Ignoring unknown container: (\w*)", line)[0]
                entry = DAGHistoryEventContainerLog("unusedcontainer", "noattempt", containerId)
                result.append(entry)
        return result

    @classmethod
    def getDAGIds(cls, AMlog):
        lines = AMlog.split('\n')
        result = []
        for line in lines:
            m = re.search(r"DAGImpl\|?:? (dag_.*?) transitioned from NEW to INITED", line)
            if m:
                result.append(m.groups()[0])
        return result

    @classmethod
    def getDAGIDFromAppID(cls, appID, dagNo=1, logoutput=False):
        dagID = appID.replace("application", "dag")
        dagID = "%s_%s" % (dagID, dagNo)
        return dagID

    @classmethod
    def confirmContainerReleased(cls, AmlogFile, containerSet, Dags):
        # Only 2 Dag is supporrted
        NumDag = 0
        with open(AmlogFile, mode='r', buffering=-1) as f:
            for line in f:
                if re.search("impl.DAGImpl\|?:? " + Dags[0] + " transitioned from NEW to INITED", line) is not None:
                    NumDag = 1
                if NumDag == 1 and re.search("rm.YarnTaskSchedulerService\|?:? Releasing unused container:", line
                                             ) is not None:
                    containerid = re.findall("Releasing unused container: (\w*)", line)[0]
                    logger.info("Container Released")
                    check = containerid in containerSet
                    if check:
                        containerSet.remove(containerid)
                    else:
                        logger.info(containerid + " is not expected container")
                if re.search("impl.DAGImpl\|?:? " + Dags[1] + " transitioned from NEW to INITED", line
                             ) is not None and NumDag == 1:
                    NumDag = 0
                    break
        f.close()
        if not containerSet:
            logger.info("set is empty and all expected containers are released")
            return True
        else:
            return False

    @classmethod
    def confirmContainerReuseInPreWarm(cls, containerSetDag1, containerSetDag2):
        assert containerSetDag1.issubset(containerSetDag2)

    # This fuction tells if container is being reused between map and reduce tasks
    # input : containerList generated by getContainerListforDAG function
    # output: True if container is reused otherwise false
    @classmethod
    def confirmcontainerResueBwMapReduce(cls, containerLists, maptaskName):
        mapContainers = set()
        reduceContainers = set()
        for entry in containerLists:
            if entry.vertexName == maptaskName:
                mapContainers.add(entry.containerId)
            else:
                reduceContainers.add(entry.containerId)
        common = reduceContainers.intersection(mapContainers)
        logger.info("===========")
        logger.info(mapContainers)
        logger.info(reduceContainers)
        logger.info(common)
        logger.info("===========")
        return len(common) > 0

    @classmethod
    def confirmBadDatanodeIssue(cls, AmLogFile):
        '''
        Function to confirm that the issue is BUG-15286
        Pass AM log file
        returns True if issue is due to BUG-15286 or else returns False
        '''
        if not AmLogFile:
            return False
        msg = "Failed to replace a bad datanode on the existing pipeline due to no more good datanodes being available to try"
        f = open(AmLogFile)
        for i, line in enumerate(f):
            if re.search(msg, line):
                return True
        return False

    @classmethod
    def isDag2started(cls, output_dag2, user=None):
        '''
        Confirms if Dag2 has started
        Pass output dir of Dag2. If this dir is created, dag2 has started
        '''
        return HDFS.fileExists(output_dag2, user)

    @classmethod
    def isinbetweenDags(cls, output_dag1, output_dag2, user=None):
        '''
        Confirms if application is sleeping between 2 dags
        Pass output dir of dag1 and dag2.
        It verifies if output_dag1/_SUCCESS path exists which confirms that dag1 has finished
        and also check if output_dag2 is not present which confirms that application is sleeping between 2 dags
        '''
        success_filepath = output_dag1 + "/_SUCCESS"
        if HDFS.fileExists(success_filepath, user):
            if not cls.isDag2started(output_dag2, user):
                return True
        return False

    @classmethod
    def verifyDagsPassed(cls, dagNo, filename):
        """
        Verifies if Dag has passed from jobclient output of job
        """
        msg = "%s completed. FinalState=SUCCEEDED" % dagNo
        f = open(filename)
        for i, line in enumerate(f):
            if re.search(msg, line):
                return True
        return False

    @classmethod
    def verifyRecoveryWithAM(
            cls,
            appID,
            am1_log_file,
            am2_log_file,
            container_am1=None,
            container_am2=None,
            isSession=False,
            isDag2=False,
            previous_am_recovery_count=0
    ):
        '''
        Function verifies that if all the tasks are recovered or not
        Parameters:
           appID = application ID
           am1_log_file = Temporary file to store AM log of container1 
           am2_log_file = Temporary file to store AM log of container2 
           container_am1 = default value is none. If this parameter is not passed, container_<appid>_01_000001 is taken
           container_am2 = default value is none. If this parameter is not passed, container_<appid>_02_000001 is taken
           isSession = pass True if session is enabled
           isDag2 = if amrecovery of dag2 needs to be tested pass True
        This function finds out count1 = Task finished
                                count2 = Task recovered
        returns (count1, count2)
        '''
        if not container_am1:
            container_am1 = MAPRED.getAMContainerID(
                appID.replace("application", "job"), amContainerNum="000001", attemptNum="01"
            )
        if not container_am2:
            container_am2 = MAPRED.getAMContainerID(
                appID.replace("application", "job"), amContainerNum="000001", attemptNum="02"
            )
        logger.info("******** containers *******")
        logger.info(container_am1)
        logger.info(container_am2)
        am1_hostname = MAPRED.getAMHostForContainerID(container_am1)
        am2_hostname = MAPRED.getAMHostForContainerID(container_am2)
        port = YARN.getNodeManagerAddress().split(":")[1]
        exit_code, stdout = YARN.getLogsApplicationID(
            appID, None, am1_hostname + ":" + port, container_am1, False, None
        )
        if os.path.exists(am1_log_file):
            Machine.rm(None, None, am1_log_file, False, None)
        f = open(am1_log_file, "w")
        f.write(stdout)
        f.close()
        exit_code, stdout = YARN.getLogsApplicationID(
            appID, None, am2_hostname + ":" + port, container_am2, False, None
        )
        if os.path.exists(am2_log_file):
            Machine.rm(None, None, am2_log_file, False, None)
        f = open(am2_log_file, "w")
        f.write(stdout)
        f.close()
        count1 = 0
        if isDag2:
            message = "Handling recovery event of type DAG_SUBMITTED"
            flag = 0
            f = open(am1_log_file)
            for i, line in enumerate(f):
                if line.find(message) >= 0:
                    flag = flag + 1
                if flag == 2:
                    if line.find("Handling recovery event") >= 0:
                        count1 = count1 + 1
            f.close()
        else:
            if isSession:
                cmd = "grep 'Handling recovery event' %s | wc -l" % (am1_log_file)
            else:
                cmd = "grep 'Handling recovery event' %s | grep TASK_ATTEMPT_FINI | wc -l" % (am1_log_file)
            exit_code, count1 = Machine.run(cmd, None, None, True)
        if isSession:
            cmd = "grep 'Recovering from event' %s | wc -l" % (am2_log_file)
        else:
            cmd = "grep \"app.RecoveryParser|: Recovering from even\" %s | grep TASK_ATTEMPT_FINIS | wc -l" % (
                am2_log_file
            )
        exit_code, count2 = Machine.run(cmd, None, None, True)
        logger.info("****Counts*****")
        logger.info(count1)
        logger.info(count2)
        total_count1 = int(count1) + int(previous_am_recovery_count)
        logger.info("New counts1: After addning any previous counts: %d" % total_count1)
        if not int(total_count1) - int(count2) in range(0, 2):
            pytest.xfail("intermittent")
        return [count1, count2]

    @classmethod
    def __get_all_types_of_ids_from_tez_app_log__(cls, app_id, owner):
        '''
        Parser application logs for HistoryEventHandler Dag new to inited line in application logs of Tez App
        to find Dag/Vertex/Task/TaskAttempt/AppAttempt/Container Ids
        '''
        if app_id is None:
            return None
        if re.match("application_\d+_\d+", app_id) is None:
            return None
        (exit_code, std_out) = YARN.getLogsApplicationID(app_id, appOwner=owner, logoutput=False)
        if exit_code != 0:
            return None
        if std_out is None:
            return None
        lines = std_out.split('\n')
        if lines is None or len(lines) < 1:
            return None
        result = dict()
        result['TEZ_APPLICATION_ATTEMPT'] = {}
        result['TEZ_DAG_ID'] = {}
        result['TEZ_VERTEX_ID'] = {}
        result['TEZ_TASK_ID'] = {}
        result['TEZ_TASK_ATTEMPT_ID'] = {}
        result['TEZ_CONTAINER_ID'] = {}
        for line in lines:
            if re.search('HistoryEventHandler.*\[Event:AM_LAUNCHED\]', line) is not None:
                (app_attempt_id1,
                 submit_time) = re.findall("appAttemptId=(.*),\s*appSubmitTime=(.*),\s*launchTime=", line)[0]
                result['TEZ_APPLICATION_ATTEMPT']["tez_" + app_attempt_id1] = submit_time
            m = re.search(r"DAGImpl: (.*?) transitioned from NEW to INITED", line)
            if m:
                result['TEZ_DAG_ID'][m.groups()[0]] = 1
            if re.search('HistoryEventHandler.*\[Event:DAG_FINISHED\]', line):
                f = re.findall(",.*?finishTime=(.*),.*?timeTaken=", line)[0]
                if cls.__gEndTime < f:
                    cls.__gEndTime = f
            if re.search('HistoryEventHandler.*\[Event:VERTEX_FINISHED\]', line) is not None:
                (dag_id, vertex_name, vertex_id) = re.findall(
                    "\[DAG:(.*)\]\[Event:VERTEX_FINISHED\]:\s*vertexName=(.*),\s*vertexId=(.*),\s*initRequestedTime=",
                    line
                )[0]
                if vertex_id not in result['TEZ_VERTEX_ID']:
                    result['TEZ_VERTEX_ID'][vertex_id] = 1
                if vertex_id not in cls.__vertexId_dagId:
                    cls.__vertexId_dagId[vertex_id] = {}
                cls.__vertexId_dagId[vertex_id][dag_id] = vertex_name
            if re.search('HistoryEventHandler.*\[Event:TASK_FINISHED\]', line) is not None:
                (task_id, finish_time) = re.findall("taskId=(.*),\s*startTime=.*finishTime=(.*),\s*timeTaken", line)[0]
                if task_id not in result['TEZ_TASK_ID']:
                    result['TEZ_TASK_ID'][task_id] = finish_time
                else:
                    if finish_time != result['TEZ_TASK_ID'][task_id]:
                        result['TEZ_TASK_ID'][task_id] = finish_time
            if re.search('HistoryEventHandler.*\[Event:TASK_ATTEMPT_FINISHED\]', line) is not None:
                (task_attempt_id,
                 finish_time) = re.findall("taskAttemptId=(.*),\s*startTime=.*,\s*finishTime=(.*),\s*timeTaken",
                                           line)[0]
                if task_attempt_id not in result['TEZ_TASK_ATTEMPT_ID']:
                    result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id] = finish_time
                else:
                    if finish_time != result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id]:
                        result['TEZ_TASK_ATTEMPT_ID'][task_attempt_id] = finish_time
            if re.search('HistoryEventHandler.*\[Event:CONTAINER_LAUNCHED\]', line) is not None:
                (container_id, launch_time) = re.findall("containerId=(.*),\s*launchTime=(.*)", line)[0]
                result['TEZ_CONTAINER_ID']["tez_" + container_id] = launch_time
        return result

    @classmethod
    def __validate_json_related_identities__(cls, entity_type, eid, related_identities, part_uri):
        '''
        Iterates over TEZ_*_IDs in related Identities and validate that each item matches expected pattern
        '''

        if entity_type in ['TEZ_TASK_ATTEMPT_ID', 'TEZ_CONTAINER_ID']:
            assert len(related_identities
                       ) == 0, "Expected relatedentities to be empty for %s , but got %s in response %s%s" % (
                           eid, str(len(related_identities)), entity_type, part_uri
                       )
            return

        expected_pattern = ''
        related_identity = ''
        if entity_type == "TEZ_DAG_ID":
            expected_pattern = eid.replace("dag", "vertex") + '_\d+'
        elif entity_type == "TEZ_VERTEX_ID":
            expected_pattern = eid.replace("vertex", "task") + '_\d{6}'
            related_identity = 'TEZ_TASK_ID'
        elif entity_type == 'TEZ_TASK_ID':
            expected_pattern = eid.replace("task", "attempt") + '_\d+'
            related_identity = 'TEZ_TASK_ATTEMPT_ID'
        elif entity_type == 'TEZ_APPLICATION_ATTEMPT':
            expected_pattern = eid.replace("appattempt", "container(_e\d+)*")[0:-7] + '_\d+_\d{6}'
            related_identity = 'TEZ_CONTAINER_ID'

        if related_identity in related_identities:
            for i in related_identities[related_identity]:
                logger.info(eid + " contains " + i)
                assert re.match(expected_pattern,i) is not None, \
                    "Got %s which does not match %s, In response from %s%s" % (i, expected_pattern, entity_type, part_uri)
        if entity_type == 'TEZ_APPLICATION_ATTEMPT' and 'TEZ_DAG_ID' in related_identities:
            expected_pattern = eid.replace("tez_appattempt", "dag")[0:-7] + '_\d+'
            for d in related_identities['TEZ_DAG_ID']:
                assert re.match(expected_pattern, d
                                ) is not None, "Got %s which does not match %s, In response from %s%s" % (
                                    d, expected_pattern, entity_type, part_uri
                                )

    @classmethod
    def __validate_json_primary_filters__(
            cls,
            entity_type,
            app_id,
            eid,
            ids_dict,
            owner,
            primary_filters,
            part_uri,
            user,
            use_user_auth_in_un_secure_mode,
            delegation_token,
            cookie=None
    ):
        '''
        Validates that user of dag/appattempt is same owner passed in method invocation
        And dn for dag is not Noe and empty string
        Iterates over TEZ_TASK_IDs, TEZ_VERTEX_IDs, TEZ_DAG_IDs in primaryfilters if any is there
        validates task/attempt/vertex id equal to expected ids
        Also access ATS WS API path using primary filters
        '''
        if entity_type == 'TEZ_CONTAINER_ID':
            assert len(primary_filters
                       ) >= 0, "Expected primaryfilers is zero-length array/dict but got %s in response of %s%s" % (
                           str(len(primary_filters)), entity_type, part_uri
                       )
            if len(primary_filters) > 0:
                assert 'exitStatus' in primary_filters or 'applicationId' in primary_filters
            if 'applicationId' in primary_filters:
                assert app_id in primary_filters['applicationId']
            return
        urls_to_access = []
        if entity_type in ['TEZ_DAG_ID', 'TEZ_APPLICATION_ATTEMPT']:
            part_uri2 = "&windowStart=" + str(cls.__gStartTime) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
            o_user = primary_filters['user'][0]
            assert o_user == owner
            urls_to_access.append("?primaryFilter=user:" + o_user + part_uri2)
            if entity_type == 'TEZ_APPLICATION_ATTEMPT':
                part_uri3 = "?primaryFilter=user:" + o_user + "&secondaryFilter=appSubmitTime:" + ids_dict[eid]
                urls_to_access.append(part_uri3)
            if entity_type == 'TEZ_DAG_ID':
                dn = primary_filters['dagName'][0]
                logger.info(eid + " having dagName " + dn)
                assert dn is not None and len(dn) > 0
                dn = urllib.quote_plus(dn)
                urls_to_access.append("?primaryFilter=dagName:" + dn + part_uri2)
        if entity_type in ['TEZ_VERTEX_ID', 'TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            dag_id_prefix = app_id.replace("application", "dag")
            dag_id_prefix += '_\d+'
            for d in primary_filters['TEZ_DAG_ID']:
                logger.info(eid + " dagId : " + d)
                assert re.match(dag_id_prefix, d
                                ) is not None, "Got DAG Id %s which does not match %s in response of %s%s" % (
                                    d, dag_id_prefix, entity_type, part_uri
                                )
                if not Machine.isWindows():
                    part_uri2 = d + "&windowStart=" + str(cls.__gStartTime
                                                          ) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                    urls_to_access.append("?primaryFilter=TEZ_DAG_ID:" + part_uri2)
        if entity_type in ['TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            vid = ""
            if entity_type == 'TEZ_TASK_ATTEMPT_ID':
                vid = eid.replace("attempt", "task")[0:-2]
                vid = vid.replace("task", "vertex")[0:-7]
            else:
                vid = eid.replace("task", "vertex")[0:-7]
            if 'TEZ_VERTEX_ID' in primary_filters:
                for v in primary_filters['TEZ_VERTEX_ID']:
                    assert v == vid, "Expected vertexId %s whereas got %s in response of %s%s" % (
                        vid, v, entity_type, part_uri
                    )
                    if not Machine.isWindows():
                        part_uri2 = v + "&windowStart=" + str(cls.__gStartTime
                                                              ) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                        urls_to_access.append("?primaryFilter=TEZ_VERTEX_ID:" + part_uri2)
                        part_uri3 = "?primaryFilter=TEZ_VERTEX_ID:" + v + "&secondaryFilter=endTime:" + ids_dict[eid]
                        urls_to_access.append(part_uri3)
        if entity_type == 'TEZ_TASK_ATTEMPT_ID' and 'TEZ_TASK_ID' in primary_filters:
            tid = eid.replace("attempt", "task")[0:-2]
            for t in primary_filters['TEZ_TASK_ID']:
                assert t == tid, "Expected taskId %s whereas got %s in response of %s%s" % (
                    tid, t, entity_type, part_uri
                )
                if not Machine.isWindows():
                    part_uri2 = t + "&windowStart=" + str(cls.__gStartTime
                                                          ) + "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                    urls_to_access.append("?primaryFilter=TEZ_TASK_ID:" + part_uri2)

        for u in urls_to_access:
            YARN.access_ats_ws_path(
                entity_type + u,
                ids_dict,
                app_id,
                user,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                delegation_token=delegation_token,
                cookie=cookie
            )

    @classmethod
    def __validate_ats_ws_json_dag_plan__(cls, dag_plan, url_comp):
        '''
        Validates data contained by dagPlan
        If dagPlan contains vertices, validates that vertexName is not None and processorClass starts with org.apache.tez
        If dagPlan vertices contains outEdgeIds then each outEdgeId is number
        If dagPlan vertices contains inEdgeIds then each inEdgeId is number
        if dagPlan contains edges then validates edgeSourceClass, edgeDestinationClass, dataMovementType, schedulingType, dataSourceType
        Also validates dagPlan edges inputVertexName and outputVertexName is not None
        '''
        if 'vertices' in dag_plan:
            for v in dag_plan['vertices']:
                logger.debug(
                    "vertices Processor class : " + v['processorClass'] + " vertices About vertexName : " +
                    str(v['vertexName'])
                )
                assert 'processorClass' in v and v['processorClass'] is not None, \
                    "Expected processorClass in key vertices of DagPlan and processorClass not None in response" + url_comp
                assert 'vertexName' in v and v['vertexName'] is not None, \
                    "Expected vertexName in key vertices of DagPlan and vertexName not None in response" + url_comp
                if 'outEdgeIds' in v:
                    logger.debug("outEdgeIds = " + " ".join(v['outEdgeIds']))
                    for oe in v['outEdgeIds']:
                        assert re.match("^\d+$", oe
                                        ) is not None, "Expected outEdgeIds as number but got %s in response of %s" % (
                                            str(oe), url_comp
                                        )
                if 'inEdgeIds' in v:
                    logger.debug(" inEdgeIds = " + " ".join(v['inEdgeIds']))
                    for ie in v['inEdgeIds']:
                        assert re.match("^\d+$", ie
                                        ) is not None, "Expected inEdgeIds as number but got %s in response of %s" % (
                                            str(ie), url_comp
                                        )
        if 'edges' in dag_plan:
            for e in dag_plan['edges']:
                logger.debug(
                    "edges edgeId : " + e['edgeId'] + " edges edgeSourceClass : " + e['edgeSourceClass'] +
                    " edges edgeDestinationClass : " + e['edgeDestinationClass']
                )
                assert re.match(
                    "^\d+$", e['edgeId']
                ) is not None, "Expected edgeIds as number but got %s in response of %s" % (str(e), url_comp)
                assert 'edgeSourceClass' in e and e['edgeSourceClass'] is not None, \
                    "Expected edgeSourceClass in key edges of dagPlan and edgeSourceClass is not None but got %s in response of %s" % (
                        str(e['inputVertexName']), url_comp)
                assert 'edgeDestinationClass' in e and e['edgeDestinationClass'] is not None, \
                    "Expected edgeDestinationClass in key edges of dagPlan and edgeDestinationClass is not None but got %s in response " \
                    "of %s" % (str(e['outputVertexName']), url_comp)

                logger.debug(
                    "edges dataMovementType : " + e['dataMovementType'] + " edges schedulingType : " +
                    e['schedulingType'] + " edges dataSourceType : " + e['dataSourceType']
                )
                assert e['dataMovementType'] in ['ONE_TO_ONE', 'BROADCAST', 'SCATTER_GATHER', 'CUSTOM'], \
                    "Expected dataMovementType in on of 'ONE_TO_ONE,BROADCAST,SCATTER_GATHER,CUSTOME', but got %s in response of %s" % (
                        str(e['dataMovementType']), url_comp)
                assert e['schedulingType'] in ['SEQUENTIAL', 'CONCURRENT'], \
                    "Expected schedulingType in one of 'SEQUENTIAL,CONCURRENT' but got %s in response of %s" % (str(e['schedulingType']),
                                                                                                                url_comp)
                assert e['dataSourceType'] in ['PERSISTED', 'PERSISTED_RELIABLE', 'EPHEMERAL'], \
                    "Expected dataSourceType in one of 'PERSISTED,PERSISTED_RELIABLE,EPHEMERAL' but got %s in response of %s" % (
                        str(e['dataSourceType']), url_comp)
                logger.debug(
                    "edges About inputVertex : " + str(e['inputVertexName']) + " edges About outputVertex : " +
                    str(e['outputVertexName'])
                )
                assert 'inputVertexName' in e and e['inputVertexName'] is not None, \
                    "Expected inputVertexName in key edges of dagPlan and inputVertexName is not None but got %s in response of %s" % (
                        str(e['inputVertexName']), url_comp)
                assert 'outputVertexName' in e and e['outputVertexName'] is not None, \
                    "Expected outputVertexName in key edges of dagPlan and outputVertexName is not None but got %s in response of %s" % (
                        str(e['outputVertexName']), url_comp)

    @classmethod
    def __validate_ats_ws_json_vertex_stats__(cls, vtx_id, stats, url_comp):
        '''
        Validates contents of stats of vertex JSON:
        Validation includes : maxTaskDuration, maxTaskDuration, minTaskDuration, avgTaskDuration, lastTaskFinishTime
                             firstTasksToStart, lastTasksToFinish, shortestDurationTasks, longestDurationTasks

        '''
        logger.debug(
            "maxTaskDuration : " + str(stats['maxTaskDuration']) + " minTaskDuration : " +
            str(stats['minTaskDuration'])
        )
        assert re.match("^-?\d+$", str(stats['maxTaskDuration'])) is not None, \
            "Expected maxTaskDuration as number but got %s in response of %s" % (str(stats['maxTaskDuration']), url_comp)
        assert re.match("^-?\d+$", str(stats['minTaskDuration'])) is not None, \
            "Expected minTaskDuration as number but got %s in response of %s" % (str(stats['minTaskDuration']), url_comp)
        logger.debug(
            "avgTaskDuration : " + str(stats['avgTaskDuration']) + " firstTaskStartTime : " +
            str(stats['firstTaskStartTime'])
        )
        assert re.match("^-?\d+\.?\d*$", str(stats['avgTaskDuration'])) is not None, \
            "Expected avgTaskDuration as number but got %s in response of %s" % (str(stats['avgTaskDuration']), url_comp)
        assert re.match("^-?\d+$", str(stats['firstTaskStartTime'])) is not None, \
            "Expected firstTaskStartTime as number but got %s in response of %s" % (str(stats['firstTaskStartTime']), url_comp)
        logger.debug("lastTaskFinishTime : " + str(stats['lastTaskFinishTime']))
        assert re.match("^-?\d+$", str(stats['lastTaskFinishTime'])) is not None, \
            "Expected lastTaskFinishTime as number but got %s in response of %s" % (str(stats['lastTaskFinishTime']), url_comp)

        task_id_prefix = vtx_id.replace("vertex", "task")
        other_keys = ['firstTasksToStart', 'lastTasksToFinish', 'shortestDurationTasks', 'longestDurationTasks']
        for k in other_keys:
            if k in stats:
                tasks = stats[k]
                if len(tasks) > 0:
                    for t in tasks:
                        assert re.match(task_id_prefix + "_\d{6}",t) is not None, \
                            'Expected task id like %s_\d{6} but got %s in response %s' % (task_id_prefix, t, url_comp)

    @classmethod
    def __validate_json_for_ids__(
            cls,
            entity_type,
            app_id,
            eid,
            ids_dict,
            owner,
            url_comp_to_query=None,
            fields_to_compare=None,
            again=False,
            other_user=None,
            json_data=None,
            assert_fail_on_error=False,
            use_user_auth_in_un_secure_mode=False,
            delegation_token=None,
            cookie=None
    ):
        '''
        Validates content returned by ws/v1/TEZ_DAG_ID... calls
        By Default it queries Id=${entity_type} ws/v1/${entity_type}/${eid},
        but if url query component=${url_comp_to_query} is also specified the it queries ws/v1/${entity_type}+url_comp_to_query
        By defaults this API compares contents of entitype, event, events, starttime, relatedidentities, primaryfilters and otherinfo
        returned by JSON
        But if fields_to_compare=${fields_to_compare} is specified then it compares fields specified by fields_to_compare
        out of 'entitype, event, events, starttime, relatedidentities, primaryfilters and otherinfo returned  by JSON,
        used when we query specific filters in query

        '''
        ats_store_class = YARN.getConfigValue('yarn.timeline-service.store-class', '')
        #ats_store_class2 = Hadoop.getmodifiedConfigValue('yarn-site.xml','yarn.timeline-service.store-class','')
        #if 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore'in [ats_store_class, ats_store_class2]:
        #Temproray workaround for BUG-43520, Which going fixed on 2.3-next
        if 'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore' == ats_store_class:
            if entity_type in ['TEZ_APPLICATION_ATTEMPT', 'TEZ_APPLICATION']:
                return True
        part_uri = "/" + eid
        if url_comp_to_query is not None:
            part_uri = url_comp_to_query
        (
            entity_type_comp, entity_comp, start_time_comp, events_comp, related_identities_comp, primary_filters_comp,
            other_info_comp
        ) = (True, True, True, True, True, True, True)
        if fields_to_compare is not None:
            if re.search("entityType", fields_to_compare, re.I) is None:
                entity_type_comp = False
            if re.search("entityId", fields_to_compare, re.I) is None:
                entity_comp = False
            if re.search("starttime", fields_to_compare, re.I) is None:
                start_time_comp = False
            if re.search("events", fields_to_compare, re.I) is None:
                events_comp = False
            if re.search("relatedentities", fields_to_compare, re.I) is None:
                related_identities_comp = False
            if re.search("primaryfilters", fields_to_compare, re.I) is None:
                primary_filters_comp = False
            if re.search("otherinfo", fields_to_compare, re.I) is None:
                other_info_comp = False
        user = owner
        if other_user is not None:
            user = other_user
        p = YARN.get_ats_json_data(
            entity_type + part_uri,
            user,
            use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
            user_delegation_token=delegation_token,
            cookie=cookie
        )
        logger.info(p)
        if json_data is not None and type(json_data) == dict:
            p = json_data
        len_of_data = 0
        if p is not None and type(p) == dict and 'exception' not in p:
            len_of_data = len(p)
            if 'events' in p and '/events?entityId' in part_uri:
                len_of_data = len(p['events'])
        if p is None:
            if assert_fail_on_error:
                assert p is not None, 'Got empty JSON %s for query with user=%s' % (str(p), user)
            else:
                logger.error('Got empty JSON %s for query with user=%s' % (str(p), user))
                return False
        if type(p) != dict:
            if assert_fail_on_error:
                assert type(p) == dict, 'For query with user=%s, Response is not in JSON: %s' % (user, str(p))
            else:
                logger.error('For query with user=%s, Response is not in JSON: %s' % (user, str(p)))
                return False

        if 'exception' in p or len_of_data <= 0:
            if assert_fail_on_error:
                assert 'exception' not in p, 'Got exception JSON %s for query with user=%s' % (str(p), user)
                assert len_of_data > 0, 'Got exmpty response %s for query with user=%s' % (str(p), user)
            else:
                logger.error('Got empty or exception JSON %s for query with user=%s' % (str(p), user))
                return False
        if entity_type_comp:
            '''
             Validates entitytype got JSON equal entityType specified during method call
            '''
            e_type = []
            if again:
                for e in p['events']:
                    e_type.append(e['entitytype'])
            else:
                e_type.append(p['entitytype'])
            logger.info(eid + " entityType = " + str(e_type))
            assert len(e_type) > 0, 'Got empty entitytype in repsonse of %s%s' % (entity_type, part_uri)
            found = False
            for etp in e_type:
                if etp == entity_type:
                    found = True
                    break
            assert found is True, "Expected entitytype for '%s' but got '%s' in response of %s%s" % (
                entity_type, str(e_type), entity_type, part_uri
            )
        if entity_comp:
            '''
            Validates entity == eid specified during method call
            '''
            ety = []
            if again:
                for et in p['events']:
                    ety.append(et['entity'])
            else:
                ety.append(p['entity'])
            logger.info(eid + " entity = " + str(ety))
            assert len(ety) > 0, 'Got empty entity in response of %s%s' % (entity_type, part_uri)
            found = False
            for ey in ety:
                if json_data is not None:
                    m = eid[:-3]
                    if re.search(m, ey) is not None:
                        logger.info(
                            "Using regex search instead of pattern equal to " + m + " " + str(re.search(m, ey))
                        )
                        found = True
                        break
                if eid == ey:
                    found = True
                    break
            assert found is True, "Expected entity '%s' but got '%s' in response of %s%s" % (
                eid, str(ety), entity_type, part_uri
            )
        if start_time_comp:
            logger.info(eid + " starttime = " + str(p['starttime']))
            assert re.match("^\d+$", str(p['starttime'])
                            ) is not None, "Expected number but got %s in response of %s%s " % (
                                str(p['starttime']), entity_type, part_uri
                            )
        if events_comp:
            '''
            Validates events returned by JSON contains either of expected events
            and event time stamp is integer value
            '''
            events = []
            if again:
                for ev in p['events']:
                    events.append(ev['events'])
                    logger.info(eid + " Event found " + str(len(ev['events'])))
                    assert len(ev['events']) > 0
            else:
                events.append(p['events'])
                logger.info(eid + " Event found " + str(len(events[0])))
                assert len(events[0]) > 0
            expected_event_types = ['AM_STARTED', 'AM_LAUNCHED']
            if entity_type == 'TEZ_DAG_ID':
                expected_event_types = [
                    'DAG_SUBMITTED', 'DAG_INITIALIZED', 'DAG_STARTED', 'DAG_FINISHED', 'DAG_RECOVERED'
                ]
            elif entity_type == 'TEZ_VERTEX_ID':
                expected_event_types = [
                    'VERTEX_INITIALIZED', 'VERTEX_STARTED', 'VERTEX_FINISHED', 'VERTEX_CONFIGURE_DONE'
                ]
            elif entity_type == 'TEZ_TASK_ID':
                expected_event_types = ['TASK_STARTED', 'TASK_FINISHED']
            elif entity_type == 'TEZ_TASK_ATTEMPT_ID':
                expected_event_types = ['TASK_ATTEMPT_STARTED', 'TASK_ATTEMPT_FINISHED']
            elif entity_type == 'TEZ_CONTAINER_ID':
                expected_event_types = ['CONTAINER_LAUNCHED', 'CONTAINER_STOPPED']
            for evs in events:
                for i in evs:
                    logger.info(eid + " EventType " + i['eventtype'])
                    assert i['eventtype'] in expected_event_types is not None, \
                        "Got event %s where as expected is one of from %s in reponse %s%s" % (str(i['eventtype']),
                                                                                              str(expected_event_types), entity_type,
                                                                                              part_uri)
                    logger.info(eid + " Event Type: " + i['eventtype'] + " time: " + str(i['timestamp']))
                    assert re.match("^\d+$", str(i['timestamp'])
                                    ) is not None, "Expected number but got %s in response of %s%s" % (
                                        str(i['timestamp']), entity_type, part_uri
                                    )
        if related_identities_comp:
            if 'relatedentities' in p:
                cls.__validate_json_related_identities__(entity_type, eid, p['relatedentities'], part_uri)

        if primary_filters_comp:
            if 'primaryfilters' in p:
                cls.__validate_json_primary_filters__(
                    entity_type,
                    app_id,
                    eid,
                    ids_dict,
                    owner,
                    p['primaryfilters'],
                    part_uri,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )

        if not other_info_comp:
            return True
        '''
        Validates otherinfo retuned by JSON response
        Validation includes timeTaken, status, startTime and endTime
        If entityType is TEZ_DAG_ID or TEZ_VERTEX_ID then also validates  initTime
        If enityType is TEZ_DAG_ID then validation inclu desapplicationId and existance of dagPlan calls DagPlan validotr method
        If entityType is TEZ_VETEX_ID then Validation includes startRequestedTime, startRequestedTime, initRequestedTime,
        vertexName, numTasks, processorClassName and then calls the StatsValidator
        If entityType is TEZ_APPLICATION_ATTEMPT then valides appSubmitTime
        And if entityType is TEZ_CONTAINER_ID then validates that otherinfo is empty array

        '''
        if entity_type == 'TEZ_CONTAINER_ID':
            othic = p['otherinfo']
            if 'endTime' in othic:
                assert re.match("^\d+$", str(othic['endTime'])) is not None, \
                    "Expected Number as endTime value but got %s in response of %s%s" % (str(othic['endTime']), entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains endTime" % (entity_type, part_uri)
                )
            if 'exitStatus' in othic:
                assert re.match("^-?\d+$", str(othic['exitStatus'])) is not None, \
                    "Expected Number as exitStatus value but got %s in response of %s%s" % (str(othic['exitStatus']), entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains exitStatus" % (entity_type, part_uri)
                )
            return True

        othi = p['otherinfo']
        # logger.info(Id + "timeTaken = " + str(othi['timeTaken']) + " initTime = " + str(othi['initTime']))
        if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_TASK_ID', 'TEZ_TASK_ATTEMPT_ID']:
            if 'timeTaken' in othi:
                assert re.match("^\d+$", str(othi['timeTaken'])) is not None, \
                    "Expected Number as timeTaken value but got %s in response of %s%s" % (str(othi['timeTaken']), entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains timeTaken" % (entity_type, part_uri)
                )
            if 'endTime' in othi:
                assert re.match("^\d+$", str(othi['endTime'])) is not None, \
                    "Expected Number as endTime value but got %s in response of %s%s" % (str(othi['endTime']), entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains endTime" % (entity_type, part_uri)
                )
            if 'startTime' in othi:
                assert re.match("^\d+$", str(othi['startTime'])) is not None, \
                    "Expected Number as startTime value but got %s in response of %s%s" % (str(othi['startTime']), entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains startTime" % (entity_type, part_uri)
                )
            if 'status' in othi:
                assert othi['status'] in ['SCHEDULED', 'RUNNING', 'SUCCEEDED', 'KILLED', 'FAILED'], \
                    "Expected either 'SCHEDULED,RUNNING,SUCCEEDED,KILLED,FAILED' as status value but got %s in response of %s%s" % (str(othi['status']),
                                                                                                                                    entity_type, part_uri)
            else:
                logger.error(
                    "In JSON Response for %s%s key='otherinfo' does not contains status" % (entity_type, part_uri)
                )

            if 'counters' in othi and 'counterGroups' in othi['counters'] and len(othi['counters']['counterGroups']
                                                                                  ) > 0:
                for counterGroup in othi['counters']['counterGroups']:
                    if 'counters' in counterGroup and len(counterGroup['counters']) > 0:
                        for counter in counterGroup['counters']:
                            if 'counterName' in counter and 'counterValue' in counter:
                                logger.debug(
                                    "counter name" + str(counter['counterName']) + " value " +
                                    str(counter['counterValue'])
                                )
                                if counter['counterValue'] is not None and len(str(counter['counterValue'])) > 0:
                                    assert re.match("^-?\d+$", str(counter[
                                        'counterValue'])) is not None, \
                                        "Expected counter value to be number, but got %s for counterName %s in reponse JSON of %s%s" % (
                                            str(counter['counterValue']), str(counter['counterName']), entity_type, part_uri)
                                else:
                                    logger.error(
                                        eid + " Got counter value None or empty for counterName: " +
                                        str(counter['counterName']) + " value :" + str(counter['counterValue'])
                                    )
                            else:
                                logger.error(eid + " Either counterName and counterValue is not there " + str(counter))

            if entity_type == 'TEZ_DAG_ID' or entity_type == 'TEZ_VERTEX_ID':
                if 'initTime' in othi:
                    assert re.match("^\d+$", str(othi['initTime'])
                                    ) is not None, "Expected initTime as number but got %s in response of %s%s" % (
                                        str(othi['initTime']), entity_type, part_uri
                                    )
                else:
                    logger.error(
                        "In JSON Response for %s%s key='otherinfo' does not contains initTime" %
                        (entity_type, part_uri)
                    )

        if entity_type == 'TEZ_DAG_ID':
            assert othi['applicationId'] == app_id, "Expected applicdation id %s, but got %s in response of %s%s" % (
                app_id, str(othi['applicationId']), entity_type, part_uri
            )
            assert 'dagPlan' in othi
            cls.__validate_ats_ws_json_dag_plan__(othi['dagPlan'], entity_type + part_uri)

        if entity_type == 'TEZ_TVERTEX_ID':
            logger.debug(
                "startRequestedTime = " + str(othi['startRequestedTime']) + " initRequestedTime  = " +
                str(othi['initRequestedTime'])
            )
            assert re.match("^\d+$", str(othi['startRequestedTime'])) is not None, \
                "Expected startRequestedTime as number but got %s in response of %s%s" % (str(othi['startRequestedTime']), entity_type,
                                                                                          part_uri)
            assert re.match("^\d+$", str(othi['initRequestedTime'])) is not None, \
                "Expected initTime as number but got %s in response of %s%s" % (str(othi['initRequestedTime']), entity_type, part_uri)
            logger.debug(
                "vertexName = " + othi['vertexName'] + " numTasks = " + str(othi['numTasks']) +
                " processorClassName = " + othi['processorClassName']
            )
            assert othi['vertexName'] is not None and len(othi['vertexName']) > 0, \
                "Expected vextexName not None and non-zero length string, but got %s in response of %s%s" % (str(othi['vertexName']),
                                                                                                             entity_type, part_uri)
            assert re.match("^\d+$", str(othi['numTasks'])
                            ) is not None, "Expected numTasks as number but got %s in response of %s%s" % (
                                str(othi['numTasks']), entity_type, part_uri
                            )
            assert re.match("^org\.apache\.tez\..*$", othi['processorClassName']) is not None, \
                "Expected processorClass containing org.apache.tez but got %s in response of %s%s" % (str(othi['processorClass']),
                                                                                                      entity_type, part_uri)
            if 'stats' in othi:
                cls.__validate_ats_ws_json_vertex_stats__(eid, othi['stats'], entity_type + part_uri)

        if entity_type == 'TEZ_APPLICATION_ATTEMPT':
            if 'appSubmitTime' in othi:
                logger.debug(str(othi['appSubmitTime']))
                assert re.match("^\d+$", str(othi['appSubmitTime'])) is not None, \
                    "Expected appSubmitTime as number but got %s in response of %s%s" % (str(othi['appSubmitTime']), entity_type, part_uri)
            else:
                logger.error("'appSubmitTime' does exists in reponse of %s%s" % (entity_type, part_uri))

        return True

    @classmethod
    def __validate_entity_type_ws_api__(
            cls,
            entity_type,
            app_id,
            ids_dict,
            owner,
            other_user=None,
            use_user_auth_in_un_secure_mode=False,
            privileged_users=None,
            delegation_token=None,
            cookie=None
    ):
        '''
        Access entityType=${entity_type} with different IDs and different queries e.g fields, event and calls Ids JSON validator
        method. Also accesses entityType for DAG/VERTEX/APPLICATION_ATTEMPT with parameters such fromId, fromTs and primary,
        secondaryfilters
        '''
        user = owner
        all_privileged_users = [owner]
        if privileged_users is not None and type(privileged_users) == list:
            for u in privileged_users:
                all_privileged_users.append(u)
        assert_fail_on_error = False
        if other_user is not None:
            user = other_user
        if entity_type == 'TEZ_DAG_ID' or entity_type == 'TEZ_APPLICATION_ATTEMPT':
            if user in all_privileged_users:
                assert_fail_on_error = True

        fields_array = [
            'events', 'otherinfo', 'primaryfilters', 'otherinfo,primaryfilters', 'otherinfo,relatedentities',
            'otherinfo,primaryfilters,relatedentities', 'otherinfo,primaryfilters', 'relatedentities,otherinfo'
        ]

        count = 0
        for Id in ids_dict.keys():
            if count >= 10:
                break
            r = cls.__validate_json_for_ids__(
                entity_type,
                app_id,
                Id,
                ids_dict,
                owner,
                other_user=user,
                assert_fail_on_error=assert_fail_on_error,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                delegation_token=delegation_token,
                cookie=cookie
            )
            count += 1
            if other_user is not None and user not in all_privileged_users:
                assert not r, "Expected call failing where it has passed"
            if r is True and user is all_privileged_users:
                continue

            if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
                YARN.access_ats_ws_path(
                    entity_type + "?fromId=" + Id,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    cookie=cookie
                )
                if entity_type == 'TEZ_VERTEX_ID':
                    for d in cls.__vertexId_dagId[Id].keys():
                        pu = "?primaryFilter=TEZ_DAG_ID:" + d + "&secondaryFilter=vertexName:" + \
                             urllib.quote_plus(cls.__vertexId_dagId[Id][d]) + "&windowStart=" + str(cls.__gStartTime) + \
                             "&windowEnd=" + str(cls.__gEndTime) + "&limit=200"
                        p = YARN.get_ats_json_data(
                            "TEZ_VERTEX_ID" + pu,
                            user,
                            use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                            user_delegation_token=delegation_token,
                            cookie=cookie
                        )
                        if p is not None and type(p) == dict and 'entities' in p and len(p['entities']) > 0:
                            for jd in p['entities']:
                                fields_to_compare = 'entitytype,events,entityId,starttime,otherinfo,primaryfilter'
                                cls.__validate_json_for_ids__(
                                    entity_type,
                                    app_id,
                                    Id,
                                    ids_dict,
                                    owner,
                                    url_comp_to_query=pu,
                                    fields_to_compare=fields_to_compare,
                                    other_user=user,
                                    json_data=jd,
                                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                                    delegation_token=delegation_token,
                                    cookie=cookie
                                )
                u_comp = "/" + Id + "?fields="
                for f in fields_array:
                    cls.__validate_json_for_ids__(
                        entity_type,
                        app_id,
                        Id,
                        ids_dict,
                        owner,
                        url_comp_to_query=u_comp + f,
                        fields_to_compare=f,
                        other_user=user,
                        use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                        delegation_token=delegation_token,
                        cookie=cookie
                    )
                u_comp = '/events?entityId=' + Id
                cls.__validate_json_for_ids__(
                    entity_type,
                    app_id,
                    Id,
                    ids_dict,
                    owner,
                    url_comp_to_query=u_comp,
                    fields_to_compare='events,entityType,entityId',
                    again=True,
                    other_user=user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
        #Due to BUG-44902, if yarn.timeline-service.store-class=org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore then fromTs is not
        #supported in WebService Qurey in Timelineserver, so skipping fromTs if yarn.timeline-service.store-class is not equal to LeveldbTimelineStore
        #or EntityFileTimelineStore
        timeline_store = YARN.getConfigValue('yarn.timeline-service.store-class', '')
        use_fromts = True
        if timeline_store not in ['org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore',
                                  'org.apache.hadoop.yarn.server.timeline.EntityFileTimelineStore']:
            use_fromts = False
        if entity_type in ['TEZ_DAG_ID', 'TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
            YARN.access_ats_ws_path(
                entity_type,
                ids_dict,
                app_id,
                user,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                delegation_token=delegation_token,
                cookie=cookie
            )
            if entity_type == 'TEZ_DAG_ID':
                from_ts_str = '?fromTs=' + cls.__gEndTime
                if not use_fromts:
                    from_ts_str = ''
                YARN.access_ats_ws_path(
                    entity_type + from_ts_str,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
                YARN.access_ats_ws_path(
                    entity_type + 'fields=relatedentities,otherinfo&secondaryFilter=applicationId:' + app_id,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
            elif entity_type in ['TEZ_VERTEX_ID', 'TEZ_APPLICATION_ATTEMPT']:
                part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime) + '&limit=200'
                YARN.access_ats_ws_path(
                    part_uri,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
                part_uri = entity_type + '?windowEnd=' + str(cls.__gEndTime) + '&limit=200'
                YARN.access_ats_ws_path(
                    part_uri,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
                part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime
                                                               ) + '&windowEnd=' + str(cls.__gEndTime) + '&limit=200'
                YARN.access_ats_ws_path(
                    part_uri,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
                if use_fromts:
                    part_uri = entity_type + '?fromTs=' + str(cls.__gEndTime) + '&limit=200'
                else:
                    part_uri = entity_type + '?limit=200'
                YARN.access_ats_ws_path(
                    part_uri,
                    ids_dict,
                    app_id,
                    user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
        elif entity_type in ['TASK_TASK_ID', 'TASK_TASK_ATTEMPT_ID', 'TEZ_CONTAINER_ID']:
            YARN.access_ats_ws_path(
                entity_type + '?limit=1000',
                ids_dict,
                app_id,
                user,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                delegation_token=delegation_token,
                cookie=cookie
            )
            part_uri = entity_type + '?windowStart=' + str(cls.__gStartTime
                                                           ) + '&windowEnd=' + str(cls.__gEndTime) + '&limit=200'
            YARN.access_ats_ws_path(
                part_uri,
                ids_dict,
                app_id,
                user,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                delegation_token=delegation_token,
                cookie=cookie
            )

    @classmethod
    def validate_ws_api(
            cls,
            app_id,
            owner,
            other_user=None,
            validate_only_with_other_user=False,
            use_user_auth_in_un_secure_mode=False,
            privileged_users=None,
            delegation_token=None,
            cookie=None,
            all_ids_dict=None
    ):
        '''
        Package public method.
        Takes application Id and owner (user who submitted application).
        Get Application logs and parse it for getting DAG/VERTEX/TASK/TASK_ATTEMPT/APPLICTIO_ATTEMPT/CONTAINER IDs
        call WS JSON validators form type of IDs
        '''
        assert app_id is not None, "Got None as application Id"
        app_id = app_id.strip('\r').strip('\n').strip()
        if not Hadoop.isHadoop2() and not Hadoop.isTez():
            return None
        if type(all_ids_dict) != dict or len(all_ids_dict.keys()) <= 0:
            all_ids_dict = cls.__get_all_types_of_ids_from_tez_app_log__(app_id, owner)
        else:
            cls.__gStartTime = all_ids_dict['GLOBAL_START_TIME']
            cls.__gEndTime = all_ids_dict['GLOBAL_END_TIME']
        if type(all_ids_dict) != dict or len(all_ids_dict.keys()) <= 0:
            return None
        cls.__gStartTime = str(cls.__gStartTime)
        cls.__gEndTime = str(cls.__gEndTime)
        for ids_type in all_ids_dict.keys():
            if ids_type in ['GLOBAL_START_TIME', 'GLOBAL_END_TIME']:
                continue
            if other_user is not None and validate_only_with_other_user is True:
                cls.__validate_entity_type_ws_api__(
                    ids_type,
                    app_id,
                    all_ids_dict[ids_type],
                    owner,
                    other_user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    privileged_users=privileged_users,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
                continue
            cls.__validate_entity_type_ws_api__(
                ids_type,
                app_id,
                all_ids_dict[ids_type],
                owner,
                use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                privileged_users=privileged_users,
                delegation_token=delegation_token,
                cookie=cookie
            )
            if other_user is not None:
                cls.__validate_entity_type_ws_api__(
                    ids_type,
                    app_id,
                    all_ids_dict[ids_type],
                    owner,
                    other_user,
                    use_user_auth_in_un_secure_mode=use_user_auth_in_un_secure_mode,
                    privileged_users=privileged_users,
                    delegation_token=delegation_token,
                    cookie=cookie
                )
        all_ids_dict['GLOBAL_START_TIME'] = str(cls.__gStartTime)
        all_ids_dict['GLOBAL_END_TIME'] = str(cls.__gEndTime)
        return all_ids_dict

    @classmethod
    def modifyConfig(
            cls, changes, isFirstUpdate=True, makeCurrConfBackupInWindows=True, regenServiceXmlsInWindows=False
    ):
        admin_user = Machine.getAdminUser()
        admin_passwd = Machine.getAdminPasswd()
        gw_host = Hadoop.getSelectedNodes({'services': ['gateway']})
        logger.info("Gateway Host: %s" % str(gw_host))
        tez_conf = Config.get('tez', 'TEZ_CONF_DIR')
        tmp_conf = os.path.join(Machine.getTempDir(), 'tezConf')
        if Machine.isWindows():
            tmp_conf = tez_conf
        configUtils.modifyConfig(
            changes, tez_conf, tmp_conf, gw_host, isFirstUpdate=isFirstUpdate, makeCurrConfBackupInWindows=False
        )
        if Machine.isLinux():
            src_tez_conf_path = os.path.join(tmp_conf, 'tez-site.xml')
            dest_path = os.path.join(tez_conf, 'tez-site.xml')
            for host in gw_host:
                Machine.copyFromLocal(admin_user, host, src_tez_conf_path, dest_path, admin_passwd)
                Machine.chmod("755", dest_path, user=admin_user, host=host, passwd=admin_passwd)

    @classmethod
    def deploy_tez_configs_for_ats_v1_5(cls):
        '''
        Deploys Tez configs on Gateway node by Over-writting original tez-site.xml on Gateway
        CAUTION :This method deploys ATS V1.5 configs relate config in tez-site.xml on Gateway by over-writting original file,
                  without taking any backup
        :return:
        '''
        tez_config_changes = {
            'tez-site.xml': {
                'tez.history.logging.service.class': 'org.apache.tez.dag.history.logging.ats.ATSV15HistoryLoggingService'
            }
        }
        admin_user = Machine.getAdminUser()
        admin_passwd = Machine.getAdminPasswd()
        gw_host = Hadoop.getSelectedNodes({'services': ['gateway']})
        logger.info("Gateway Host: %s" % str(gw_host))
        tez_conf = Config.get('tez', 'TEZ_CONF_DIR')
        tmp_conf = os.path.join(Machine.getTempDir(), 'tezConf')
        if Machine.isWindows():
            tmp_conf = tez_conf
        configUtils.modifyConfig(
            tez_config_changes, tez_conf, tmp_conf, gw_host, isFirstUpdate=True, makeCurrConfBackupInWindows=False
        )
        if Machine.isLinux():
            src_tez_conf_path = os.path.join(tmp_conf, 'tez-site.xml')
            dest_path = os.path.join(tez_conf, 'tez-site.xml')
            for host in gw_host:
                Machine.copyFromLocal(admin_user, host, src_tez_conf_path, dest_path, admin_passwd)
                Machine.chmod("755", dest_path, user=admin_user, host=host, passwd=admin_passwd)

    @classmethod
    def check_atsv1_5and_deploy_yarntez_configs_atsv1_5(cls):
        '''
        Checks whether NAT/tests are running with COMPONENT set to Tez_v15 in conf/report.conf
        Deploys ATS V1.5 related confings of Hadoop on Cluster and default config location (without taking backup of configs)and restart ATS server and RM
        Also deploys ATS V1.5 related new entries in tez-site.xml by over-writting original file and defaurl TEZ_CONF_DIR location on gateway
        without taking backup of configs
        CAUTION: This method deploy configs by over-writting original configs without taking backup. So it must only when NAT wide chages needed to be made
        :return:
        '''
        if YARN.check_if_component_is_tez_v15():
            YARN.deploy_yarn_configs_for_ats_v1_5(restart_resource_manager=True, wait=20, sleep_time_after_restart=20)
            cls.deploy_tez_configs_for_ats_v1_5()

    @classmethod
    def deploy_and_use_Tez_v2(cls):
        '''
        HDP 2.5 onwards we have 2 versions of Tez when slider and HiveServerInteractive is enabled.
        '''
        HDP_VERSION = Ambari.getHDPVersion()[6:]
        if YARN.check_if_component_is_tez_v2():
            os.environ["IS_HIVE2"] = "true"
            hdp_dir = os.path.join('usr', 'hdp', HDP_VERSION)
            os.environ["HDP_DIR"] = "%s" % hdp_dir
            os.environ[
                "HADOOP_CLASSPATH"
            ] = "${HADOOP_CLASSPATH}:${HDP_DIR}/tez_hive2/*:${HDP_DIR}/tez_hive2/lib/*:${HDP_DIR}/tez_hive2/conf"
            HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')
            Machine.runas(HADOOPQA_USER, cmd='export IS_HIVE2=true', logoutput=True, env={'IS_HIVE2': ':true'})
            Machine.runas(HADOOPQA_USER, cmd='export HDP_DIR=%s' % hdp_dir, logoutput=True)
            Machine.runas(
                HADOOPQA_USER,
                cmd=
                'export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${HDP_DIR}/tez_hive2/*:${HDP_DIR}/tez_hive2/lib/*:${HDP_DIR}/tez_hive2/conf',
                logoutput=True
            )
            logger.info(os.environ['IS_HIVE2'])

    @classmethod
    def wait_for_ATS_data(cls, dagId):
        HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')
        p = YARN.get_ats_json_data(
            "TEZ_DAG_ID/" + dagId,
            HADOOPQA_USER,
            user_delegation_token=YARN.fetch_timeline_delegation_token(user=HADOOPQA_USER)
        )
        starttime = time.time()
        while not (p is not None and type(p) == dict):
            logger.info("Its none or not dict")
            p = YARN.get_ats_json_data(
                "TEZ_DAG_ID/" + dagId,
                HADOOPQA_USER,
                user_delegation_token=YARN.fetch_timeline_delegation_token(user=HADOOPQA_USER)
            )
            if (time.time() - starttime) > 90:
                break
