#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import inspect, os
import time
import logging
import json

from beaver.component.hadoop import Hadoop, HDFS
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruHDFS:
    _base_hdfs_dir = None
    _SmokeInputDir = None
    _queue = 'hdfs'
    _lgTestDataDir = None
    _lgTestOutputDir = None
    _lgStructureDir = None
    _bgJobId = None
    testFileTr = inspect.getfile(inspect.currentframe())
    localTestFileTr = os.path.join(Machine.getTempDir(), "testFileTr")

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Upload Data to HDFS before Upgrade starts
        Creates /user/hrt_qa/test_rollingupgrade dir on HDFS
        Upload 20 files to /user/hrt_qa/test_rollingupgrade
        '''
        if not cls._base_hdfs_dir:
            cls._base_hdfs_dir = '/user/%s/test_rollingupgrade' % Config.get('hadoop', 'HADOOPQA_USER')
        exit_code, stdout = HDFS.createDirectory(cls._base_hdfs_dir, force=True)
        ruAssert("HDFS", exit_code == 0, '[BGJobSetup] could not create dir on hdfs.')
        LOCAL_WORK_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'HDFS_RU_TEST')
        localTestWorkDir1 = os.path.join(LOCAL_WORK_DIR, "Temp_data")
        HadoopJobHelper.runCustomWordWriter(LOCAL_WORK_DIR, localTestWorkDir1, 20, 40, 1000)
        HDFS.copyFromLocal(os.path.join(localTestWorkDir1, "*"), cls._base_hdfs_dir)

        # set up for loadGenerator
        cls._lgTestDataDir = cls._base_hdfs_dir + '/testData'
        cls._lgTestOutputDir = cls._base_hdfs_dir + '/lg_job'
        cls._lgStructureDir = Machine.getTempDir() + "/structure"
        # test dir setup
        HDFS.deleteDirectory(cls._lgTestDataDir)
        HDFS.deleteDirectory(cls._lgTestOutputDir)
        command = "rm -rf " + cls._lgStructureDir
        exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
        )
        command = "mkdir " + cls._lgStructureDir
        stdout = Machine.runas(None, command, None, None, None, "True", None)
        Machine.chmod("777", cls._lgStructureDir, "True", Machine.getAdminUser(), None, Machine.getAdminPasswd())

        HADOOP_TEST_JAR = cls.get_hadoop_test_jar()
        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')
        # structure generator
        jobCmd = 'jar %s NNstructureGenerator -maxDepth 5 -minWidth 2 -maxWidth 5 -numOfFiles 100 -avgFileSize 3 -outDir %s' % (
            HADOOP_TEST_JAR, cls._lgStructureDir
        )
        exit_code, stdout = Hadoop.run(jobCmd)
        ruAssert("HDFS", exit_code == 0, "[BGJobSetup] StructureGenerator failed")
        # data generator
        jobCmd = 'jar %s NNdataGenerator -inDir %s -root %s' % (
            HADOOP_TEST_JAR, cls._lgStructureDir, cls._lgTestDataDir
        )
        exit_code, stdout = Hadoop.run(jobCmd)
        ruAssert("HDFS", exit_code == 0, "[BGJobSetup] DataGenerator failed")

        if runSmokeTestSetup:
            logger.info("**** Running HDFS Smoke Test Setup ****")
            cls.smoke_test_setup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup function for HDFS smoke test
        '''
        if not cls._SmokeInputDir:
            cls._SmokeInputDir = cls._base_hdfs_dir + "/smokeHdfsInput"
        HDFS.deleteDirectory(cls._SmokeInputDir, Config.get('hadoop', 'HADOOPQA_USER'))
        jobCmd = 'jar %s randomtextwriter \"-D%s=%s\" \"-D%s=%s\" %s' % (
            Config.get('hadoop', 'HADOOP_EXAMPLES_JAR'), "mapreduce.randomtextwriter.totalbytes", "4096",
            "mapred.job.queue.name", cls._queue, cls._SmokeInputDir
        )
        exit_code, stdout = Hadoop.run(jobCmd)
        ruAssert("HDFS", exit_code == 0, '[SmokeSetup] Randomtextwriter job failed and could not create data on hdfs')

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None, flagFile="/tmp/flagFile"):
        '''
        Uploads Files to HDFS before upgrade starts and runs long running sleep job in background
        :return:  number of application started
        '''
        # start long running application which performs I/O operations (BUG-23838)
        #from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #UpgradePerNode.reportProgress("### Background application for HDFS started ####")
        #jobArgs = {"mapred.job.queue.name" : cls._queue}
        #HadoopJobHelper.runSleepJob(numOfMaps = 1, numOfReduce = 1, mapSleepTime = "10000000", reduceSleepTime = "100", extraJobArg = jobArgs, runInBackground = True, config = config, directoutput = False )
        #MAPRED.triggerSleepJob("1", "0", "100000", "1000000", 1, background = True)
        # load generator
        HADOOP_TEST_JAR = cls.get_hadoop_test_jar()
        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')
        HDFS.deleteDirectory(flagFile)
        slavelist = HDFS.getDatanodes()
        jobCmd = 'jar %s NNloadGenerator -Dmapred.job.queue.name=%s -mr 3 %s -root %s -numOfThreads 5 -maxDelayBetweenOps 1000 -elapsedTime 36000 -flagFile %s' % (
            HADOOP_TEST_JAR, cls._queue, cls._lgTestOutputDir, cls._lgTestDataDir, flagFile
        )
        Hadoop.runInBackground(jobCmd)
        time.sleep(15)
        return 1

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup of HDFS background job
        '''
        HDFS.deleteDirectory(cls._base_hdfs_dir)
        command = "rm -rf " + cls._lgStructureDir
        exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
        )

    @classmethod
    def verifyLongRunningJob(cls, config=None):
        '''
        Verify long running background jobs
        :param cls:
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run word count as HDFS smoke test
        - Create file of 4096 bytes using randomwriter job
        - Run wordcount job
        '''
        logger.info("Running HDFS Smoke test")
        # make sure base hdfs dir is set.
        if not cls._base_hdfs_dir:
            cls._base_hdfs_dir = '/user/%s/test_rollingupgrade' % Config.get('hadoop', 'HADOOPQA_USER')

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HDFS][Smoke] Smoke test for HDFS started ")
        HDFS.runas(
            Config.get('hadoop', 'HADOOPQA_USER'),
            "dfs -ls /user/hrt_qa",
            env=None,
            logoutput=True,
            config=None,
            host=None,
            skipAuth=False
        )
        if not cls._SmokeInputDir:
            cls._SmokeInputDir = cls._base_hdfs_dir + "/smokeHdfsInput"
        SmokeOutputDir = cls._base_hdfs_dir + '/smokeHdfsOutput' + str(smoketestnumber)
        HDFS.deleteDirectory(SmokeOutputDir, Config.get('hadoop', 'HADOOPQA_USER'))
        jobCmd = 'jar %s wordcount \"-Dmapreduce.reduce.input.limit=-1\" \"-D%s=%s\" %s %s' % (
            Config.get('hadoop', 'HADOOP_EXAMPLES_JAR'), "mapred.job.queue.name", cls._queue, cls._SmokeInputDir,
            SmokeOutputDir
        )
        exit_code, stdout = Hadoop.run(jobCmd)
        ruAssert("HDFS", exit_code == 0, "[Smoke] Hdfs smoketest failed")
        HDFS.deleteDirectory(SmokeOutputDir)
        ruAssert("HDFS", exit_code == 0, "[Smoke] could not delete: " + SmokeOutputDir)
        UpgradePerNode.reportProgress("[INFO][HDFS][Smoke] Smoke test for HDFS Finished ")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run wordcount Job passing env variables
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("**** Running HDFS CLI Test ****")
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HDFS][ClientSmoke] CLI test for HDFS started ")
        if not cls._SmokeInputDir:
            cls._SmokeInputDir = cls._base_hdfs_dir + "/smokeHdfsInput"
        SmokeOutputDir = cls._base_hdfs_dir + '/smokeHdfsOutput_cli'
        HDFS.deleteDirectory(SmokeOutputDir, Config.get('hadoop', 'HADOOPQA_USER'))
        jobCmd = 'jar %s wordcount \"-Dmapreduce.reduce.input.limit=-1\" \"-D%s=%s\" %s %s' % (
            Config.get('hadoop', 'HADOOP_EXAMPLES_JAR'), "mapred.job.queue.name", cls._queue, cls._SmokeInputDir,
            SmokeOutputDir
        )
        exit_code, stdout = Hadoop.run(jobCmd, env=env)
        ruAssert("HDFS", exit_code == 0, "[ClientSmoke] Hdfs smoketest failed")
        HDFS.deleteDirectory(SmokeOutputDir)
        ruAssert("HDFS", exit_code == 0, "[ClientSmoke] could not delete: " + SmokeOutputDir)
        UpgradePerNode.reportProgress("[INFO][HDFS][ClientSmoke] CLI test for HDFS Finished ")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades HDFS master services: Namenode and Secondary Namenode
        :param version: latest version
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if HDFS.isHAEnabled():
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] HA Journal Node Upgrade Started ")
            hdfs_user = Config.get('hadoop', 'HDFS_USER')
            # flip the Journal Nodes first
            jn_nodes = HDFS.getJournalNodes()
            if len(jn_nodes) < 3:
                UpgradePerNode.reportProgress(
                    "[INFO][HDFS][Upgrade] Less than three Journal Nodes. Not going to do Upgrade "
                )
                return

            cls.ensure_all_jns_are_up(jn_nodes)
            #Loop through all the JNs and stop flip start one at a time
            for node in jn_nodes:
                Hadoop.resetService(hdfs_user, node, "journalnode", 'stop', binFolder="sbin")
                hdpSelect.changeVersion("hadoop-hdfs-journalnode", version, node)
                Hadoop.resetService(hdfs_user, node, "journalnode", 'start', binFolder="sbin")
                time.sleep(5)
                cls.ensure_all_jns_are_up(jn_nodes)

            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] HA Journal Node Upgrade Finished ")

            nodes = []
            nodes.append(HDFS.getNamenodeByState('standby'))
            nodes.append(HDFS.getNamenodeByState('active'))
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] HA Namenode Upgrade Started")
            for node in nodes:
                HDFS.resetNamenode('stop', host=node)
                HDFS.resetZkfc('stop', hosts=node.split())
                # BUG-25534: There is no package for zkfc. So just updating the NN is enough.
                hdpSelect.changeVersion("hadoop-hdfs-namenode", version, node)
                HDFS.resetNamenode('start', config=config, host=node, option=" -rollingUpgrade started")
                HDFS.resetZkfc('start', hosts=node.split())
                # lets make sure the NN is out of safemode before we proceed to the next namenode
                HDFS.waitForNNOutOfSafemode(options='-fs hdfs://%s:8020' % node)

            cls.ensure_nn_is_active()
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade]HA Namenode Upgrade Finished ")
        else:
            ## TODO add code to upgrade SNN
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] Namenode Upgrade Started ")
            node = HDFS.getNamenode()
            HDFS.stopNamenode()
            hdpSelect.changeVersion("hadoop-hdfs-namenode", version, node)
            HDFS.startNamenode(config=config, option=" -rollingUpgrade started")
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] Namenode Upgrade Finished ")
            # upgrade SNN
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] Secondary Namenode Upgrade started ")
            node = HDFS.getSecondaryNamenode()
            HDFS.stopSecondaryNamenode()
            hdpSelect.changeVersion("hadoop-hdfs-secondarynamenode", version, node)
            HDFS.startSecondaryNamenode(config=config)
            UpgradePerNode.reportProgress("[INFO][HDFS][Upgrade] Secondary Namenode Upgrade Finished ")

    @classmethod
    def ensure_nn_is_active(cls, timeout=11 * 60):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        active_nn = None
        curr_time = int(time.time())
        end_time = int(time.time()) + timeout
        while not active_nn and curr_time <= end_time:
            active_nn = HDFS.getNamenodeByState('active')
            # we we found active NN return it
            if active_nn:
                UpgradePerNode.reportProgress("[INFO][HDFS] Active Namenode is %s" % active_nn)
                return

            # wait for 30s
            time.sleep(30)
            curr_time = int(time.time())

        if not active_nn:
            UpgradePerNode.reportProgress("[FAILED][HDFS] No Active Namenode")
            assert active_nn

        return

    @classmethod
    def ensure_all_jns_are_up(cls, nodes):
        # run roll edits
        HDFS.rollEdits()
        time.sleep(5)
        # capture LastAppliedOrWrittenTxId from the NN JMX.
        nn_url = HDFS.getNNWebAppAddress() + '/jmx'
        nn_data = util.getJMXData(nn_url, 'Hadoop:service=NameNode,name=NameNodeInfo', 'JournalTransactionInfo')
        json_data = json.loads(nn_data)
        last_tx_id = int(json_data['LastAppliedOrWrittenTxId'])
        logger.info('******************** NN LAST TX ID: %s *************************' % last_tx_id)
        cls.ensure_jns_have_new_txn(nodes, last_tx_id)

    @classmethod
    def ensure_jns_have_new_txn(cls, nodes, last_tx_id):
        num_of_jns = len(nodes)
        actual_tx_ids = {}
        jns_updated = 0
        protocol = 'http'
        jn_port = '8480'
        if Hadoop.isEncrypted():
            protocol = 'https'
            jn_port = '8481'

        # time out of 3 mins
        time_out = 3 * 60
        # stop time for 10s
        step_time = 10

        itr = int(time_out / step_time)

        for i in range(itr):
            logger.info(
                '******************** Check if all Journal Nodes are updated Iteration %s or %s *************************'
                % (i + 1, itr)
            )
            for node in nodes:
                # if all JNS are updated break
                if jns_updated == num_of_jns:
                    return

                try:
                    # if JN is already ahead skip it
                    if actual_tx_ids[node] and int(actual_tx_ids[node]) >= last_tx_id:
                        continue
                except KeyError:
                    pass

                # other wise get the data and compare it
                url = '%s://%s:%s/jmx' % (protocol, node, jn_port)
                actual_tx_ids[node] = util.getJMXData(
                    url, 'Hadoop:service=JournalNode,name=Journal-', 'LastWrittenTxId'
                )
                logger.info(
                    '******************** JN: %s LAST TX ID: %s *************************' % (node, last_tx_id)
                )
                if int(actual_tx_ids[node]) >= last_tx_id:
                    jns_updated += 1

            # if all JNS are updated break
            if jns_updated == num_of_jns:
                return

            time.sleep(step_time)

        ruAssert("HDFS", jns_updated == num_of_jns)

    @classmethod
    def wait4DNLive(cls, node):
        i = 1
        maxTries = 30  # ie 150sec   - note the delay in QE configs for initial BR is 120sec
        logger.info('*** Waiting for DN %s to become live ****' % node)
        while i < maxTries:
            livenodes = HDFS.getDatanodesFromJmx()
            if node in livenodes:
                return True
            # saw strange behavious where the dns were ip addresses sometimes; convert
            livenodesIp = []
            for iNode in livenodes:  # convert to ip addresses
                livenodesIp.append(util.getIpAddress(iNode))
            if node in livenodesIp:
                return True
            logger.info('*** Waiting for DN %s to become live ****' % node)
            logger.info('*** Live nodes list is: %s  %s ****' % (livenodes, livenodesIp))
            time.sleep(5)
            i = i + 1

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "[WARNING][HDFS][XXX] Datanode %s did not become live after 150 secs of restart, continuing " % node
        )
        return False

    @classmethod
    def upgrade_slave(cls, version, node, config=None, logText="Upgrade"):
        '''
        Upgrade HDFS slave sevice: Datanode
        :param version: latestVersion.
        :param node: The node name where DN is running
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress(
            "[INFO][HDFS][%s] Datanode %s for node %s to version %s started " % (logText, logText, node, version)
        )
        ipcPort = HDFS.getDataNodeIPCPort()
        cmd = "dfsadmin -shutdownDatanode %s:%s upgrade" % (node, ipcPort)
        exitcode, stdout = HDFS.runas(
            Config.get('hadoop', 'HDFS_USER'), cmd, env=None, logoutput=True, config=None, host=None, skipAuth=False
        )
        if exitcode != 0:
            UpgradePerNode.reportProgress(
                "[INFO][HDFS][%s] Datanode shutdownDatanode command failed for %s " % (logText, node)
            )

        HDFS.waitForDNDown(node, ipcPort, "ipc.client.connect.max.retries=1")
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("hadoop-hdfs-datanode", version, node)
        HDFS.startDatanodes(config=config, nodes=[node])
        cls.wait4DNLive(node)
        UpgradePerNode.reportProgress(
            "[INFO][HDFS][%s] Datanode %s for node %s to version %s finished " % (logText, logText, node, version)
        )

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
        Prepare Namenode to save State for Upgrade
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HDFS][Prepare] Preparing state for HDFS upgrade")
        # BUG-26726: we need to be in safemode only in non HA cluster
        if not HDFS.isHAEnabled():
            exit_code, output = HDFS.runasAdmin("dfsadmin -safemode enter")
            ruAssert("HDFS", exit_code == 0, '[Preparation] enter safemode failed')

        exit_code, output = HDFS.runas(
            Config.get('hadoop', 'HDFS_USER'),
            "dfsadmin -Ddfs.client.test.drop.namenode.response.number=0 -rollingUpgrade prepare"
        )
        ruAssert("HDFS", exit_code == 0, '[Preparation] -rollingUpgrade prepare failed')
        if not HDFS.isHAEnabled():
            exit_code, output = HDFS.runasAdmin("dfsadmin -safemode leave")
            ruAssert("HDFS", exit_code == 0, '[Preparation] leave safemode failed')
        UpgradePerNode.reportProgress("[INFO][HDFS][Prepare] Preparing state for HDFS upgrade finished ")

    @classmethod
    def ru_rollback_state(cls):
        '''
        Saved state is rolled back - upgrade is abandonded
        NOTE: this command will not return until namenode shuts down
        :return:
        '''
        logger.info("[INFO][HDFS][Upgrade] HA Journal Node Upgrade Started ")
        hdfs_user = Config.get('hadoop', 'HDFS_USER')

        nodes = []
        nodes.append(HDFS.getNamenodeByState('standby'))
        nodes.append(HDFS.getNamenodeByState('active'))
        logger.info("[INFO][HDFS][Upgrade] HA Namenode Upgrade Started")
        for node in nodes:
            HDFS.resetZkfc('stop', hosts=node.split())
            HDFS.resetNamenode('stop', host=node)

        HDFS.resetNamenode('start', config=None, host=nodes[0], option=" -rollingUpgrade rollback")
        HDFS.resetZkfc('start', hosts=nodes[0].split())
        # lets make sure the NN is out of safemode before we proceed to the next namenode
        HDFS.waitForNNOutOfSafemode(options='-fs hdfs://%s:8020' % nodes[0])

        command = "sudo su - -c 'hadoop namenode -bootstrapStandby -force' hdfs"
        (exitcode, stdout) = Machine.runas(
            Machine.getAdminUser(), command, nodes[1], None, None, "True", Machine.getAdminPasswd()
        )
        ruAssert("HDFS", exitcode == 0, "hadoop namenode -bootstrapStandby -force")
        HDFS.resetNamenode('start', config=None, host=nodes[1], option="")
        HDFS.resetZkfc('start', hosts=nodes[1].split())
        # lets make sure the NN is out of safemode before we proceed to the next namenode
        HDFS.waitForNNOutOfSafemode(options='-fs hdfs://%s:8020' % nodes[1])

    @classmethod
    def ru_downgrade_state(cls):
        '''
        Downgrades Namenode
        A downgrade is done - may need to convert state to previous version or state is compatible - again upgrade is being abandoned
        NOTE: this command will not return until namenode shuts down
        '''
        command = "sudo su - -c 'hadoop namenode -rollingUpgrade downgrade' hdfs"
        if HDFS.isHAEnabled():
            nodes = []
            nodes.append(HDFS.getNamenodeByState('standby'))
            nodes.append(HDFS.getNamenodeByState('active'))
            for node in nodes:
                HDFS.resetNamenode('stop', host=node)
                (exitcode, stdout) = Machine.runas(
                    Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd()
                )
                ruAssert(
                    "HDFS", exitcode == 0, "[NNDowngrade] hadoop namenode -rollingUpgrade downgrade command failed"
                )
            return

        HDFS.stopNamenode()
        node = HDFS.getNamenode()
        (exitcode,
         stdout) = Machine.runas(Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd())
        ruAssert("HDFS", exitcode == 0, "[NNDowngrade] hadoop namenode -rollingUpgrade downgrade command failed")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade HDFS Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if HDFS.isHAEnabled():
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] HA Namenode Downgrade Started ")
            nodes = []
            nodes.append(HDFS.getNamenodeByState('standby'))
            nodes.append(HDFS.getNamenodeByState('active'))
            for node in nodes:
                HDFS.resetNamenode('stop', host=node)
                HDFS.resetZkfc('stop', hosts=node.split())
                # BUG-25534: There is no package for zkfc. So just updating the NN is enough.
                hdpSelect.changeVersion("hadoop-hdfs-namenode", version, node)
                HDFS.resetNamenode('start', config=config, host=node)
                HDFS.resetZkfc('start', hosts=node.split())
                # lets make sure the NN is out of safemode before we proceed to the next namenode
                HDFS.waitForNNOutOfSafemode(options='-fs hdfs://%s:8020' % node)

            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] HA Namenode Downgrade Finished ")
            jn_nodes = HDFS.getJournalNodes()
            if len(jn_nodes) < 3:
                UpgradePerNode.reportProgress(
                    "[INFO][HDFS][Downgrade] Less than three Journal Nodes. Not going to do Downgrade "
                )
                return

            #Loop through all the JNs and stop flip start one at a time
            hdfs_user = Config.get('hadoop', 'HDFS_USER')
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] HA Journal Node Upgrade Started")
            cls.ensure_all_jns_are_up(jn_nodes)
            #Loop through all the JNs and stop flip start one at a time
            for node in jn_nodes:
                Hadoop.resetService(hdfs_user, node, "journalnode", 'stop', binFolder="sbin")
                hdpSelect.changeVersion("hadoop-hdfs-journalnode", version, node)
                Hadoop.resetService(hdfs_user, node, "journalnode", 'start', binFolder="sbin")
                time.sleep(5)
                cls.ensure_all_jns_are_up(jn_nodes)

            cls.ensure_nn_is_active()
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] HA Journal Node Downgrade Finished ")
        else:
            ## TODO add code to upgrade SNN
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] Namenode Downgrade Started ")
            node = HDFS.getNamenode()
            HDFS.stopNamenode()
            hdpSelect.changeVersion("hadoop-hdfs-namenode", version, node)
            HDFS.startNamenode(config=config)
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] Namenode Downgrade Finished ")
            # upgrade SNN
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade]Secondary Namenode Downgrade Started ")
            node = HDFS.getSecondaryNamenode()
            HDFS.stopSecondaryNamenode()
            hdpSelect.changeVersion("hadoop-hdfs-secondarynamenode", version, node)
            HDFS.startSecondaryNamenode(config=config)
            UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] Secondary Namenode Downgrade Finished")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        downgrade HDFS slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] Downgrade of Datanode %s started  " % node)
        cls.upgrade_slave(version, node, config=config, logText="Downgrade")
        UpgradePerNode.reportProgress("[INFO][HDFS][Downgrade] Downgrade of Datanode %s finished " % node)

    @classmethod
    def ru_finalize_state(cls):
        '''
        Upgrade is completed and finalized - save state (if any) can be discarded
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("[INFO][HDFS][Finalize] Calling finalize for HDFS ")
        # make sure we do not drop response
        exit_code, output = HDFS.runas(
            Config.get('hadoop', 'HDFS_USER'),
            "dfsadmin -Ddfs.client.test.drop.namenode.response.number=0 -rollingUpgrade finalize"
        )
        ruAssert("HDFS", exit_code == 0, '[Finalize] -rollingUpgrade finalize failed')
        UpgradePerNode.reportProgress("[INFO][HDFS][Finalize] Namenode finalized finished ")

    @classmethod
    def testAfterAllSlavesRestarted(cls, config=None):
        '''
        Function to test upgrade is done properly after all slaves are upgraded for HDFS
        '''
        logger.info("TODO : need to implement this function")

    @classmethod
    def runLoadGenerator(cls, numOfNodes=1, elapsedTime=100):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        flagFile = UpgradePerNode._HDFS_FLAG_FILE
        # get the jar again as we dont know what version we will be running this job with.
        HADOOP_TEST_JAR = cls.get_hadoop_test_jar()
        TEST_USER = Config.get('hadoop', 'HADOOPQA_USER')

        # load generator
        jobCmd = 'jar %s NNloadGenerator -Dmapred.job.queue.name=hdfs -mr %s %s -root %s -numOfThreads 5 -maxDelayBetweenOps 1000 -elapsedTime %s -flagFile %s' % (
            HADOOP_TEST_JAR, numOfNodes, cls._lgTestOutputDir, cls._lgTestDataDir, elapsedTime, flagFile
        )
        exit_code, stdout = Hadoop.run(jobCmd)
        ruAssert("HDFS", exit_code == 0, "[BGJobSetup] LoadGenerator failed")

    @classmethod
    def get_hadoop_test_jar(cls):
        return Machine.find(
            user=Machine.getAdminUser(),
            host='',
            filepath=Config.get('hadoop', 'MAPRED_HOME'),
            searchstr='hadoop-mapreduce-client-jobclient-*-tests.jar',
            passwd=Machine.getAdminPasswd()
        )[0]

    @classmethod
    def createState4Rollback1(cls):
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -rm -skipTrash rollback_state1")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -rm -skipTrash rollback_state2")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -rm -skipTrash testFileTr")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -touchz rollback_state1")
        ruAssert("HDFS", exit_code == 0, "can't get create file rollback_state1")
        command = "dfs -put " + cls.testFileTr + " testFileTr"
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), command)
        ruAssert("HDFS", exit_code == 0, "can't upload" + cls.testFileTr)

    @classmethod
    def createState4Rollback2(cls):
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -rm -skipTrash rollback_state1")
        ruAssert("HDFS", exit_code == 0, "can't get remove file rollback_state1")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -touchz rollback_state2")
        ruAssert("HDFS", exit_code == 0, "can't get create file rollback_state2")
        # truncate the file and validate the truncated size
        logger.info("**** Truncate file to 1 byte ****")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -truncate 1 testFileTr")
        ruAssert("HDFS", exit_code == 0, "can't truncate file testFileTr")
        if os.path.isfile(cls.localTestFileTr):
            os.remove(cls.localTestFileTr)

        logger.info("**** Wait 30 second for file to be recovered ****")
        time.sleep(30)
        command = "dfs -copyToLocal testFileTr " + cls.localTestFileTr
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), command)
        ruAssert("HDFS", exit_code == 0, "can't copy file testFileTr")
        size = os.path.getsize(cls.localTestFileTr)
        ruAssert("HDFS", size == 1, "size not 1. Actual size:" + ` size `)

    @classmethod
    def checkState4Rollback(cls):
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -ls rollback_state1")
        ruAssert("HDFS", exit_code == 0, "can't find file rollback_state1")
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), "dfs -ls rollback_state2")
        ruAssert("HDFS", exit_code != 0, "file rollback_state2 shouldn't exist anymore")
        # the truncated should have been recovered
        if os.path.isfile(cls.localTestFileTr):
            os.remove(cls.localTestFileTr)

        command = "dfs -copyToLocal testFileTr " + cls.localTestFileTr
        exit_code, stdout = HDFS.runas(Config.get('hadoop', 'HADOOPQA_USER'), command)
        ruAssert("HDFS", exit_code == 0, "can't copy file testFileTr")
        testFileTrSize = os.path.getsize(cls.testFileTr)
        size = os.path.getsize(cls.localTestFileTr)
        ruAssert(
            "HDFS", size == testFileTrSize,
            "size not recovered. Actual size:" + ` size ` + " expected:" + ` testFileTrSize `
        )
