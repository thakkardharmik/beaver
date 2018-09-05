import time, logging, re, os
from beaver.component.hadoop import Hadoop, MAPRED, YARN, HDFS, MAPRED2
from beaver.component.tez import Tez
from ..machine import Machine
from ..config import Config
from ..java import Java
from beaver.htmlparser import MRHTMLParser
from beaver.component.ambari import Ambari
from subprocess import Popen, PIPE
from threading import Thread, current_thread
import Queue
from beaver import util
from taskreporter.taskreporter import TaskReporter
from taskreporter.taskreporter import update_task_result

logger = logging.getLogger(__name__)

defaultMapSleepTime = 5 * 1000
defaultReduceSleepTime = 5 * 1000
defaultIntReduceSleepTime = 5 * 1000
defaultWaitForJobIDInterval = 3
HADOOP_EXAMPLES_JAR = Config.get('hadoop', 'HADOOP_EXAMPLES_JAR')
QE_EMAIL = util.get_qe_group_email()


#Returns java arguments from given dict
def getJobArg(mapredProperties):
    s = ""
    for key in mapredProperties.keys():
        s += "\"-D" + key + "=" + str(mapredProperties[key]) + "\" "
    return s.strip()


#Runs stream job
def runStreamJob(
        mapper,
        reducer,
        inputdir,
        outputdir,
        files=None,
        numMapper=1,
        numReducer=1,
        config=None,
        extraJobArg="",
        env=None,
        proposedJobName=None
):
    '''
    Runs the specialized streaming job for checking multiple version classpath
    '''
    if type(extraJobArg) == dict:
        extraJobArg = getJobArg(extraJobArg)
    HADOOP_STREAMING_JAR = Config.get('hadoop', 'HADOOP_STREAMING_JAR')
    streamCmd = ""
    if config is not None:
        streamCmd += ' --config ' + config
    streamCmd += "  jar " + HADOOP_STREAMING_JAR
    if files is not None:
        streamCmd += ' -files ' + files
    if proposedJobName is not None:
        streamCmd += ' -Dmapred.job.name=' + proposedJobName
    streamCmd += " -Dmapreduce.job.maps=" + str(numMapper) + " -Dmapreduce.job.reduces=" + str(
        numReducer
    ) + " " + extraJobArg + " -input " + inputdir + " -mapper " + mapper + "  -reducer " + reducer + " -output " + outputdir
    pOpenObj = Hadoop.runInBackground(streamCmd, env=env, stdout=PIPE, stderr=PIPE)

    appID = None
    queue = Queue.Queue()
    process = Thread(
        target=util.processPIPEOutput, args=(
            pOpenObj,
            queue,
            streamCmd,
            Config.getEnv('ARTIFACTS_DIR'),
        )
    )
    process.daemon = True
    process.start()

    appID = YARN.waitForAppIdFromQueue(queue, timeout=60, interval=5)
    jobId = appID.replace("application", "job")
    return jobId


def checkAndRunStreamJob(
        mapper,
        reducer,
        inputdir,
        outputdir,
        files=None,
        numMapper=1,
        numReducer=1,
        sleepTimeAfterJobSubmission=15,
        timeoutInSec=20,
        config=None,
        forRU=False,
        extraJobArg="",
        env=None
):
    '''
    Checks there is no job running. Runs sleep job in background.
    Checks the job is running. mapSleepTime is millisecond.
    Returns (jobID, appID).
    '''
    jobID = None
    appID = None
    #assert no running application
    #assert not isAnyAppsRunning()
    #run application
    jobID = runStreamJob(mapper, reducer, inputdir, outputdir, files, numMapper, numReducer, config, extraJobArg, env)
    time.sleep(sleepTimeAfterJobSubmission)

    if Machine.isLinux() and timeoutInSec == 20:
        timeoutInSec = 20
    else:
        if timeoutInSec == 20:
            timeoutInSec = 40

    # Extend timeout for wire-encrypted clusters, for localizing the mapred
    # tarball
    if Hadoop.isEncrypted():
        timeoutInSec = 90

    assert MAPRED.waitForJobRunningOrTimeout(
        jobID, timeoutInSec=timeoutInSec, sleepIntervalInSec=0
    ), "Couldn't get running job"

    appId = jobID.replace("job", "application")
    return (jobID, appId)


#Runs sleep job
#extraJobArg can be either string or dict
def runSleepJob(
        numOfMaps,
        numOfReduce,
        mapSleepTime=defaultMapSleepTime,
        reduceSleepTime=defaultReduceSleepTime,
        extraJobArg="",
        user=None,
        config=None,
        runInBackground=True,
        directoutput=False,
        outputFile=None,
        am_log_level=None,
        map_log_level=None,
        reduce_log_level=None
):
    if type(extraJobArg) == dict:
        extraJobArg = getJobArg(extraJobArg)
    optionParams = " %s -m %s -r %s -mt %s  -rt %s" % (
        extraJobArg, numOfMaps, numOfReduce, mapSleepTime, reduceSleepTime
    )
    if user == None:
        job_user = Config.get('hadoop', 'HADOOPQA_USER')
    else:
        job_user = user
    jobCmd = "jar " + MAPRED.sleepJobJar() + " sleep \"-Dmapreduce.job.user.name=" + job_user + "\" " + optionParams
    if am_log_level:
        jobCmd += ' -Dyarn.app.mapreduce.am.log.level=%s' % am_log_level
    if map_log_level:
        jobCmd += ' -Dmapreduce.map.log.level=%s' % map_log_level
    if reduce_log_level:
        jobCmd += ' -Dmapreduce.reduce.log.level=%s' % reduce_log_level
    return runJob(
        cmd=jobCmd,
        user=user,
        config=config,
        runInBackground=runInBackground,
        directoutput=directoutput,
        outputFile=outputFile
    )


def checkAndRunSleepJob(
        numOfMaps,
        numOfReduce,
        mapSleepTime=defaultMapSleepTime,
        reduceSleepTime=defaultReduceSleepTime,
        sleepTimeAfterJobSubmission=5,
        extraJobArg="",
        user=None,
        timeoutInSec=20
):
    '''
    Checks there is no job running. Runs sleep job in background.
    Checks the job is running. mapSleepTime is millisecond.
    Returns (jobID, appID).
    '''
    jobID = None
    appID = None

    #run application
    jobID = runSleepJob(numOfMaps, numOfReduce, mapSleepTime, reduceSleepTime, extraJobArg, user, runInBackground=True)

    appID = jobID.replace("job", "application")
    assert MAPRED.waitForJobRunningOrTimeout(
        jobID, user=user, timeoutInSec=timeoutInSec, sleepIntervalInSec=0
    ), "Couldn't get running job"

    logger.info("Application ID is %s" % appID)
    return (jobID, appID)


#Checks if there is any YARN application running
def isAnyAppsRunning(user=None):
    appList = YARN.getApplicationList("RUNNING", user=user)
    return len(appList) != 0


#Kills all mapred jobs
def killJobs():
    MAPRED.killAllJobs()
    flag = True
    while (flag):
        if len(MAPRED.getJobList()) != 0:
            logger.info("There are still some jobs. Will sleep another 5 seconds")
            logger.info("Sleeping for 5 seconds for job to be killed")
            time.sleep(5)
        else:
            logger.info("There are no more job")
            flag = False


# Wait until application ID is available for the given app name
def waitForAppIdOnName(appName, timeout=40, interval=defaultWaitForJobIDInterval):
    if Machine.isWindows():
        timeout = timeout * 2
    starttime = time.time()
    appId = None
    while (time.time() - starttime) < timeout and appId is None:
        logger.info("Sleeping for %d seconds for app id to become available" % interval)
        time.sleep(interval)
        appId = YARN.getAppIDFromAppName(appName, state="RUNNING")
    return appId


# Wait until job ID is available.
def waitForJobId(timeout=40, interval=defaultWaitForJobIDInterval):
    if Machine.isWindows() or Machine.isHumboldt():
        timeout = timeout * 2
    starttime = time.time()
    jobs = MAPRED.getJobList()
    while (time.time() - starttime) < timeout and len(jobs) == 0:
        logger.info("Sleeping for %d seconds for job id to become available" % interval)
        time.sleep(interval)
        jobs = MAPRED.getJobList()
    if len(jobs) > 0:
        return jobs[0]
    return None


def getMRJobIdAndAppIdFromJobClientOutput(output, timeout=60):
    jobId = None
    appId = None
    startTime = time.time()
    while (time.time() - startTime <= timeout):
        time.sleep(3)
        with open(output) as f:
            for line in f:
                logger.info("Waiting for jobId and appId. " + line)
                if appId is None:
                    res = re.search(".*YarnClientImpl: Submitted application (.*)", line)
                    if res:
                        appId = res.group(1)
                if jobId is None:
                    res = re.search(".*mapreduce.Job: Running job: (.*)", line)
                    if res:
                        jobId = res.group(1)
                if jobId and appId:
                    return jobId.strip(), appId.strip()
    return jobId, appId


@TaskReporter.report_task(name="RunDistributedShell", owner=QE_EMAIL)
def runDistributedShell(dShellParam="", hadoopConfigPath=None, user=None):
    '''
    Runs distributed shell with given parameter in blocking manner
    '''
    cmd = ""
    if hadoopConfigPath is not None:
        cmd += "--config %s" % hadoopConfigPath
    cmd += " org.apache.hadoop.yarn.applications.distributedshell.Client -jar %s %s" \
              % (YARN.getDistributedShellJar(), dShellParam)
    if user is None:
        (exit_code, stdout) = YARN.run(cmd)
    else:
        (exit_code, stdout) = YARN.runas(user, cmd)
    matchObj = re.search("Submitted application (.*)$", stdout, re.M)
    appID = None
    if matchObj is not None:
        appID = matchObj.group(1)
    matchObj = re.search("appMasterHost=(.*),.*yarnAppState=RUNNING", stdout)
    amHost = None
    #If amHost is None, it means the property is not found or is empty
    if matchObj is not None:
        amHost = matchObj.group(1)
    logger.info(
        "runDistributedShell returns (exit_code, stdout, appID, amHost) = " + "(%s, <not shown>, %s, %s)" %
        (exit_code, appID, amHost)
    )
    if exit_code == 0:
        update_task_result(status="successful", message="Distributedshell application passed")
    else:
        update_task_result(status="failed", message="Distributedshell application failed")
    return (exit_code, stdout, appID, amHost)


def runYarnJar(dshellParams="", user=None):
    """
    Run dshell application using yarn jar
    :param dshellParams:
    :param hadoopConfigPath:
    :param user:
    :return:
    """
    cmd = " jar %s %s -jar %s" % (YARN.getDistributedShellJar(), dshellParams, YARN.getDistributedShellJar())
    if user is None:
        (exit_code, stdout) = YARN.run(cmd)
    else:
        (exit_code, stdout) = YARN.runas(user, cmd)
    matchObj = re.search("Submitted application (.*)$", stdout, re.M)
    appID = None
    if matchObj is not None:
        appID = matchObj.group(1)
    matchObj = re.search("appMasterHost=(.*),.*yarnAppState=RUNNING", stdout)
    amHost = None
    #If amHost is None, it means the property is not found or is empty
    if matchObj is not None:
        amHost = matchObj.group(1)
    logger.info(
        "runDistributedShell returns (exit_code, stdout, appID, amHost) = " + "(%s, <not shown>, %s, %s)" %
        (exit_code, appID, amHost)
    )
    return (exit_code, stdout, appID, amHost)


def runDistributedShellInBackground(
        dShellParam="",
        stdout=PIPE,
        stderr=PIPE,
        assertNoAppsRunningFirst=True,
        hadoopConfigPath=None,
        user=None,
        waitForAppId=True,
        directoutput=False,
        outputFile=None
):
    '''
    Runs distributed shell with given parameter in background
    Returns (pOpenObj, appID).
    appID would be None if the application is not found.
    If there is running app before new app is submitted, returned app ID might be incorrect.
    '''
    cmd = ""
    if hadoopConfigPath is not None:
        cmd += "--config %s" % hadoopConfigPath
    cmd += " org.apache.hadoop.yarn.applications.distributedshell.Client -jar %s %s" \
              % (YARN.getDistributedShellJar(), dShellParam)
    if directoutput:
        cmd = cmd + " 2>&1 | tee " + outputFile
    appID = None
    #assert no running application
    if assertNoAppsRunningFirst:
        assert not isAnyAppsRunning()
    if user is None:
        pOpenObj = YARN.runInBackground(cmd, stdout=stdout, stderr=stderr)
    else:
        pOpenObj = YARN.runInBackgroundAs(user, cmd, stdout=stdout, stderr=stderr)

    appID = None
    queue = Queue.Queue()
    process = Thread(
        target=util.processPIPEOutput, args=(
            pOpenObj,
            queue,
            cmd,
            Config.getEnv('ARTIFACTS_DIR'),
        )
    )
    process.daemon = True
    process.start()

    if waitForAppId:
        appID = YARN.waitForAppIdFromQueue(queue, timeout=60, interval=5)
    else:
        appID = None
    return (pOpenObj, appID)


def runYarnJarInBackground(
        dShellParam="",
        stdout=PIPE,
        stderr=PIPE,
        assertNoAppsRunningFirst=True,
        user=None,
        waitForAppId=True,
        directoutput=False,
        outputFile=None
):
    '''
    Runs distributed shell with yarn jar in background
    Returns (pOpenObj, appID).
    appID would be None if the application is not found.
    If there is running app before new app is submitted, returned app ID might be incorrect.
    '''
    cmd = " jar %s %s -jar %s" % (YARN.getDistributedShellJar(), dShellParam, YARN.getDistributedShellJar())
    if directoutput:
        cmd = cmd + " 2>&1 | tee " + outputFile
    appID = None
    #assert no running application
    if assertNoAppsRunningFirst:
        assert not isAnyAppsRunning()
    if user is None:
        pOpenObj = YARN.runInBackground(cmd, stdout=stdout, stderr=stderr)
    else:
        pOpenObj = YARN.runInBackgroundAs(user, cmd, stdout=stdout, stderr=stderr)

    appID = None
    queue = Queue.Queue()
    process = Thread(
        target=util.processPIPEOutput, args=(
            pOpenObj,
            queue,
            cmd,
            Config.getEnv('ARTIFACTS_DIR'),
        )
    )
    process.daemon = True
    process.start()

    if waitForAppId:
        appID = YARN.waitForAppIdFromQueue(queue, timeout=60, interval=5)
    else:
        appID = None
    return (pOpenObj, appID)


def runDistributedShell2(
        shellCommand=None,
        shellArgs=None,
        numContainers=None,
        timeout=None,
        containerMemory=None,
        amMemory=None,
        queue=None,
        runInBackground=False,
        hadoopConfigPath=None,
        logoutput=True,
        user=None,
        labelExpression=None,
        waitForAppId=True,
        directoutput=False,
        outputFile=None,
        master_vcores=None,
        container_vcores=None,
        appname=None,
        priority=None,
        container_resources=None,
        shell_script=None,
        shell_env=[],
        useYarnJar=False,
        flow_name=None,
        flow_version=None,
        flow_run_id=None,
        placement_spec=None,
        keep_containers=None,
        container_retry_policy=None,
        container_max_retry=None
):
    '''
    Run distributed shell.
    Returns (pOpenObj, appID) if runInBackground=True.
    Returns (exit_code, stdout) if runInBackground=False,
    Returned app ID in background=True is unreliable.
    
    num_containers can be float.
    '''
    cmd = ""
    if shellCommand is not None:
        cmd += " -shell_command %s" % shellCommand
    if shellArgs is not None:
        cmd += " -shell_args %s" % shellArgs
    if appname is not None:
        cmd += " -appname %s" % appname
    if numContainers is not None:
        cmd += " -num_containers %s" % int(numContainers)
    if timeout is not None:
        cmd += " -timeout %s" % timeout
    if containerMemory is not None:
        cmd += " -container_memory %s" % containerMemory
    if amMemory is not None:
        cmd += " -master_memory %s" % amMemory
    if queue is not None:
        cmd += " -queue %s" % queue
    if labelExpression is not None:
        cmd += " -node_label_expression \"%s\"" % labelExpression
    if container_vcores is not None:
        cmd += " -container_vcores %s" % container_vcores
    if master_vcores is not None:
        cmd += " -master_vcores %s" % master_vcores
    if priority:
        cmd += " -priority %s" % priority
    if container_resources is not None:
        cmd += " -container_resources %s" % container_resources
    if shell_script is not None:
        cmd += " -shell_script %s" % shell_script
    if shell_env != []:
        for items in shell_env:
            cmd += " -shell_env %s" % items
    if flow_name:
        cmd += " -flow_name %s" % flow_name
    if flow_version:
        cmd += " -flow_version %s" % flow_version
    if flow_run_id:
        cmd += " -flow_run_id %s" % flow_run_id
    if placement_spec:
        cmd += " -placement_spec %s" % placement_spec
    if container_max_retry:
        cmd += " -container_max_retries %s" % container_max_retry
    if container_retry_policy:
        cmd += " -container_retry_policy %s" % container_retry_policy
    if keep_containers:
        cmd += " -keep_containers_across_application_attempts"
    if logoutput:
        logger.info("runDistributedShell2 cmd=%s" % cmd)
        logger.info("runDistributedShell2 number of containers = %s" % numContainers)
        logger.info("runDistributedShell2 container memory = %s" % containerMemory)
    if useYarnJar:
        if runInBackground:
            return runYarnJarInBackground(dShellParam=cmd, user=user)
        else:
            return runYarnJar(cmd, user=user)
    if runInBackground:
        return runDistributedShellInBackground(
            cmd,
            stdout=PIPE,
            stderr=PIPE,
            assertNoAppsRunningFirst=False,
            hadoopConfigPath=hadoopConfigPath,
            user=user,
            waitForAppId=waitForAppId,
            directoutput=directoutput,
            outputFile=outputFile
        )
    else:
        return runDistributedShell(cmd, hadoopConfigPath=hadoopConfigPath, user=user)


def compileCustomWordWriter(targetDir):
    '''
    Compiles CustomWordWriter.
    targetDir is target root directory of generated class file.
    '''
    sourceFiles = os.path.join(Java.getQEUtilsSrcDir(), '*.java')
    return Java.runJavac(sourceFiles, targetDir, classPath=None)


def runCustomWordWriter(classDir, outputDir, numFiles, numUniqueWords, numWordsPerFile):
    '''
    Runs CustomWordWriter.
    classDir is root directory of generated class file.
    outputDir is output directory of custom word writer.
    '''
    compileCustomWordWriter(targetDir=classDir)
    return Java.runJava(
        workingDir=classDir,
        fqClassName="com.hortonworks.qe.utils.CustomWordWriter",
        classPath=None,
        cmdArgs=[outputDir, numFiles, numUniqueWords, numWordsPerFile]
    )


def runJob(
        cmd, user=None, config=None, runInBackground=False, env=None, logoutput=False, directoutput=False,
        outputFile=None
):
    '''
    Runs a job.
    If runInBackground is True, return jobId.
    If runInBackground is False, return (exit_code, stdout)
    '''
    if directoutput and runInBackground == False:
        if outputFile:
            cmd = cmd + " 2>&1 | tee " + outputFile
        else:
            logger.info("Need to pass jobClient redirect file")
            return None, None
    if runInBackground:
        pOpenObj = Hadoop.runInBackgroundAs(user, cmd, config=config, env=env, stdout=PIPE, stderr=PIPE)

        appID = None
        queue = Queue.Queue()
        process = Thread(
            target=util.processPIPEOutput, args=(
                pOpenObj,
                queue,
                cmd,
                Config.getEnv('ARTIFACTS_DIR'),
            )
        )
        process.daemon = True
        process.start()

        appID = YARN.waitForAppIdFromQueue(queue, timeout=90, interval=5)

        assert appID != None, "Couldn't get the job Id"
        if directoutput:
            return appID.replace("application", "job"), os.path.join(Config.getEnv('ARTIFACTS_DIR'), process.name)
        else:
            return appID.replace("application", "job")

    else:
        return Hadoop.runas(user, cmd, env=env, logoutput=True, config=config, host=None, skipAuth=False)


def _runRandomWriterJob(
        outputDir, writerName, jobArg="", user=None, config=None, runInBackground=False, redirect_file=None
):
    if Hadoop.isHadoop1():
        return
    optionParams = " %s %s" % (jobArg, outputDir)
    if user == None:
        job_user = Config.get('hadoop', 'HADOOPQA_USER')
    else:
        job_user = user
    jobCmd = "jar " + HADOOP_EXAMPLES_JAR + " %s \"-Dmapreduce.job.user.name=" % writerName + \
                  job_user + "\" " + optionParams
    if redirect_file:
        jobCmd = jobCmd + " 2>&1 | tee %s" % redirect_file
    return runJob(cmd=jobCmd, user=user, config=config, runInBackground=runInBackground)


def runRandomTextWriterJob(
        outputDir,
        totalBytes,
        bytesPerMap=1024 * 1024 * 100,
        mapsPerHost=1,
        jobArg="",
        user=None,
        config=None,
        runInBackground=False,
        redirect_file=None
):
    '''
    Runs RandomTextWriter.
    
    totalBytesToWrite and bytesPerMap take precedence over mapsPerHost. See job java code.
    mapsPerHost can't be zero or bytesPerMap will be ignored. 
    '''
    if Hadoop.isHadoop1():
        return
    if type(jobArg) == dict:
        jobArg['mapreduce.randomtextwriter.totalbytes'] = totalBytes
        jobArg['mapreduce.randomtextwriter.bytespermap'] = bytesPerMap
        jobArg["mapreduce.randomtextwriter.mapsperhost"] = mapsPerHost
        jobArg = getJobArg(jobArg)
    else:
        jobArg += ' "-Dmapreduce.randomtextwriter.totalbytes=%s"' % totalBytes
        jobArg += ' "-Dmapreduce.randomtextwriter.bytespermap=%s"' % bytesPerMap
        jobArg += ' "-Dmapreduce.randomtextwriter.mapsperhost=%s"' % mapsPerHost
    return _runRandomWriterJob(outputDir, "randomtextwriter", jobArg, user, config, runInBackground, redirect_file)


def runRandomWriterJob(
        outputDir,
        totalBytes,
        bytesPerMap=1024 * 1024 * 100,
        mapsPerHost=1,
        jobArg="",
        user=None,
        config=None,
        runInBackground=False
):
    if Hadoop.isHadoop1():
        return
    if type(jobArg) == dict:
        jobArg['mapreduce.randomwriter.totalbytes'] = totalBytes
        jobArg['mapreduce.randomwriter.bytespermap'] = bytesPerMap
        jobArg["mapreduce.randomwriter.mapsperhost"] = mapsPerHost
        jobArg = getJobArg(jobArg)
    else:
        jobArg += ' "-Dmapreduce.randomwriter.totalbytes=%s"' % totalBytes
        jobArg += ' "-Dmapreduce.randomwriter.bytespermap=%s"' % bytesPerMap
        jobArg += ' "-Dmapreduce.randomwriter.mapsperhost=%s"' % mapsPerHost
    return _runRandomWriterJob(outputDir, "randomwriter", jobArg, user, config, runInBackground)


def runSortJob(
        inputDir,
        outputDir,
        numReducers=None,
        inFormat="",
        outFormat="",
        outKey="",
        outValue="",
        jobArg="",
        user=None,
        config=None,
        runInBackground=False,
        directoutput=False,
        outputFile=None
):
    '''
    Runs sort job.
    If runInBackground is True, return jobId.
    If runInBackground is False, return (exit_code, stdout)
    '''
    if Hadoop.isHadoop1():
        return
    if type(jobArg) == dict:
        jobArg = getJobArg(jobArg)
    if inFormat != "":
        inFormat = "-inFormat %s" % inFormat
    if outFormat != "":
        outFormat = "-outFormat %s" % outFormat
    if outKey != "":
        outKey = "-outKey %s" % outKey
    if outValue != "":
        outValue = "-outValue %s" % outValue
    if numReducers is not None:
        numReducers = "-r %s" % numReducers
    optionParams = " %s %s %s %s %s %s %s %s" % (
        jobArg, numReducers, inFormat, outFormat, outKey, outValue, inputDir, outputDir
    )
    if user == None:
        job_user = Config.get('hadoop', 'HADOOPQA_USER')
    else:
        job_user = user
    jobCmd = "jar " + HADOOP_EXAMPLES_JAR + " sort \"-Dmapreduce.job.user.name=" + \
                  job_user + "\" " + optionParams
    return runJob(
        cmd=jobCmd,
        user=user,
        config=config,
        runInBackground=runInBackground,
        directoutput=directoutput,
        outputFile=outputFile
    )


def runMRJob(
        jarLocation=HADOOP_EXAMPLES_JAR,
        jobClass="",
        jobArg1={},
        jobArg2="",
        user=None,
        config=None,
        env=None,
        runInBackground=False,
        logoutput=False
):
    '''
    Runs MR job.
    If runInBackground is True, return jobId.
    If runInBackground is False, return (exit_code, stdout)
    
    Run ${jarLocation} ${jobClass} ${jobArg1} ${jobArg2}.
    Can run custom jar.
    '''
    if Hadoop.isHadoop1():
        return
    jobOptionParams = "%s %s" % (getJobArg(jobArg1), jobArg2)
    if user == None:
        job_user = Config.get('hadoop', 'HADOOPQA_USER')
    else:
        job_user = user
    jobCmd = "jar %s %s %s" % (jarLocation, jobClass, jobOptionParams)
    return runJob(
        cmd=jobCmd, user=job_user, config=config, runInBackground=runInBackground, env=env, logoutput=logoutput
    )


def runSliveJob(
        baseDir,
        numMaps,
        numReduces,
        duration,
        resFile,
        queue=None,
        replication="1,1",
        append="0,uniform",
        create="0,uniform",
        delete="0,uniform",
        ls="0,uniform",
        mkdir="0,uniform",
        read="0,uniform",
        rename="0,uniform",
        sleep="100,1000",
        extraJobArg="",
        user=None,
        config=None,
        runInBackground=False,
        logoutput=False
):
    '''
    Runs slive job.
    If runInBackground is True, return jobId.
    If runInBackground is False, return (exit_code, stdout)
    '''
    if type(extraJobArg) == dict:
        extraJobArg = getJobArg(extraJobArg)
    if queue is None:
        queue = ""
    else:
        queue = "-queue %s" % queue
    optionParams = " %s -baseDir %s -maps %s -reduces %s -duration %s -resFile %s" \
                    % (extraJobArg, baseDir, numMaps, numReduces, duration, resFile) + \
                   " %s -replication %s -append %s -create %s -delete %s -ls %s -mkdir %s" \
                    % (queue, replication, append, create, delete, ls, mkdir) + \
                   " -read %s -rename %s -sleep %s" % (read, rename, sleep)
    jobCmd = "org.apache.hadoop.fs.slive.SliveTest " + \
             "\"-Dmapreduce.job.user.name=" + user + "\" " + optionParams
    return runJob(cmd=jobCmd, user=user, config=config, runInBackground=runInBackground)


def getNUMCompletedMRTasks(jobID):
    """
   This function greps number of Maps/recuder completed while job is running
   It finds this information from AM URL of the jobId
   """
    progress = []
    app = jobID.split("job_")
    url = YARN.getResourceManagerWebappAddress() + "/proxy/application_" + str(app[1]) + "/"
    logger.info(url)
    actFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "getMRProgress.txt")
    util.getURLContents(url, actFile)
    f = open(actFile, 'r')
    data = f.read()
    f.close()
    parser = MRHTMLParser()
    parser.feed(data)
    for row in parser.rowData:
        logger.info(row)
        if len(row) == 9:
            print "JobID :", row[0], " ,  Maps Completed:", int(row[5]), " , Reduces Completed:", int(row[8])
            progress.append(int(row[5]))
            progress.append(int(row[8]))
    parser.reset()
    return progress


def waitForNumCompletedTasks(jobID, numTasks, timeout):
    '''
    Wait for numTasks to completed including both map task and reduce task.
    '''
    startTime = time.time()
    while (time.time() - startTime < timeout):
        taskList = getNUMCompletedMRTasks(jobID)
        if len(taskList) > 0:
            total = taskList[0] + taskList[1]
            if (total >= numTasks):
                return True
            logger.info("Waiting for %s num tasks,currently %s " % (numTasks, total))
        if MAPRED2.isJobComplete(jobID):
            logger.info("Job %s completed. " % (jobID))
            return True
        logger.info("Waiting for %s tasks" % (numTasks))
        time.sleep(3)
    return False


def submitLongSleepJobAndCheckHosts(
        expectHost=None,
        q=None,
        labelExpression=None,
        numContainer=10,
        killJob=True,
        containerMem=256,
        amMemory=256,
        originalRunning=0,
        sleeptime=1000000
):
    assert originalRunning == len(YARN.getApplicationList("RUNNING"))
    runDistributedShell2(
        "\"sleep %s\"" % sleeptime, None, numContainer, None, containerMem, amMemory, q, True, None, True, None,
        labelExpression, False
    )

    # get appId (wait for at most 1min
    obtainedAppId = False
    apps = None
    sec = 0
    while (sec < 120):
        apps = YARN.getApplicationList("RUNNING")
        if len(apps) == originalRunning + 1:
            obtainedAppId = True
            break
        sec = sec + 1
        time.sleep(1)
    assert apps != None
    assert obtainedAppId, "Failed to obtain application Id in 120 sec"

    maxi = -1
    appId = None

    for app in apps:
        curAppId = app.split()[0]
        if int(curAppId[len(curAppId) - 1:]) > maxi:
            maxi = int(curAppId[len(curAppId) - 1:])
            appId = curAppId

    print "===========appId:" + appId

    # sleep for 30s and see if all containers on nms[1]
    time.sleep(30)
    (containerIds, hosts) = YARN.getContainerIdAndHosts(appId)
    assert len(hosts) > 0

    if expectHost != None:
        for host in hosts:
            if isinstance(expectHost, set):
                assert expectHost.__contains__(host)
            else:
                assert host == expectHost

    if killJob:
        (exitCode, _) = YARN.killApplication(appId)
        assert exitCode == 0

    return appId


def submitJobAndCheckPending(q=None, labelExpression=None):
    # run a long sleep job in queue A
    assert 0 == len(YARN.getApplicationList("ACCEPTED"))
    runDistributedShell2("echo", None, 1, 60 * 1000, 128, 128, q, True, None, True, None, labelExpression, False)
    time.sleep(30)
    assert 1 == len(YARN.getApplicationList("ACCEPTED"))


def submitLongSleepJobAndCheckNotAllocateOnHosts(notExpectedHosts, q=None, numContainer=10, killJob=True):
    assert 0 == len(YARN.getApplicationList("RUNNING"))
    runDistributedShell2(
        "\"sleep 1000000\"", None, numContainer, None, 128, 128, q, True, None, True, None, None, False
    )

    # get appId (wait for at most 1min
    apps = None
    sec = 0
    while (sec < 60):
        apps = YARN.getApplicationList("RUNNING")
        if len(apps) > 0:
            break
        sec = sec + 1
        time.sleep(1)
    assert apps != None
    appId = apps[0].split()[0]
    print "appId=" + appId

    # sleep for 30s and see if all containers on nms[1]
    time.sleep(30)
    (containerIds, hosts) = YARN.getContainerIdAndHosts(appId)
    assert len(hosts) > 0
    for host in hosts:
        assert not notExpectedHosts.__contains__(host)

    if killJob:
        (exitCode, _) = YARN.killApplication(appId)
        assert exitCode == 0


def runTezSleepJob(
        numOfMaps,
        numOfReduce,
        mapSleepTime=defaultMapSleepTime,
        reduceSleepTime=defaultReduceSleepTime,
        extraJobArg="",
        user=None,
        config=None,
        runInBackground=True,
        directoutput=False,
        outputFile=None
):
    if type(extraJobArg) == dict:
        extraJobArg = getJobArg(extraJobArg)
    optionParams = " %s -m %s -r %s -mt %s  -rt %s" % (
        extraJobArg, numOfMaps, numOfReduce, mapSleepTime, reduceSleepTime
    )
    if user == None:
        job_user = Config.get('hadoop', 'HADOOPQA_USER')
    else:
        job_user = user
    jobCmd = "jar " + MAPRED.sleepJobJar(
    ) + " sleep \"-Duser.name=" + job_user + "\" \"-Dmapreduce.job.user.name=" + job_user + "\" " + optionParams
    return runTezJob(
        cmd=jobCmd,
        user=user,
        config=config,
        runInBackground=runInBackground,
        directoutput=directoutput,
        outputFile=outputFile
    )


"""
WIP: mrrsleepjob cant accept job args.

def runTezSleepJob2(numOfMaps, numOfReduce, numOfIntReduce,
                mapSleepTime=defaultMapSleepTime, reduceSleepTime=defaultReduceSleepTime,
                intReduceSleepTime=defaultIntReduceSleepTime,
                extraJobArg = "",
                user = None,
                config = None,
                runInBackground = True, directoutput = False, outputFile = None):
    if type(extraJobArg) == dict:
        extraJobArg = getJobArg(extraJobArg)
    optionParams = " %s -m %s -ir %s -r %s -mt %s  -rt %s -irt %s" % (extraJobArg, numOfMaps, numOfIntReduce,
                       numOfReduce, mapSleepTime, reduceSleepTime, intReduceSleepTime)

    jobCmd = "jar " + Tez.getTestsJar() + " mrrsleep " + optionParams
    return runTezJob(cmd=jobCmd, user=user, config=config, runInBackground=runInBackground, directoutput=directoutput, outputFile=outputFile)
"""


def runTezJob(
        cmd, user=None, config=None, runInBackground=False, env=None, logoutput=False, directoutput=False,
        outputFile=None
):
    '''
    Runs a job.
    If runInBackground is True, return None.
    If runInBackground is False, return (exit_code, stdout)
    '''
    if YARN.check_if_component_is_tez_v2():
        HDP_VERSION = Ambari.getHDPVersion()[6:]
        HDP_VERSION_PATH = os.path.join(os.path.sep, 'usr', 'hdp', HDP_VERSION)
        env = {
            'IS_HIVE2': 'true',
            'HADOOP_CLASSPATH': '${HADOOP_CLASSPATH}:' + HDP_VERSION_PATH + '/tez_hive2/*:' + HDP_VERSION_PATH +
            '/tez_hive2/lib/*:' + HDP_VERSION_PATH + '/tez_hive2/conf'
        }
    if directoutput:
        if outputFile:
            cmd = cmd + " 2>&1 | tee " + outputFile
        else:
            logger.info("Need to pass jobClient redirect file")
            return None, None
    if runInBackground:
        Hadoop.runInBackgroundAs(user, cmd, config=config, env=env)
        return None
    else:
        return Hadoop.runas(user, cmd, env=env, logoutput=True, config=config, host=None, skipAuth=False)
