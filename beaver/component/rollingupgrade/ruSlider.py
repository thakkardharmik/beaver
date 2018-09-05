#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, urllib2, collections, datetime, json, random
import urllib, sys
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN

from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
from beaver import configUtils
from beaver.maven import Maven

logger = logging.getLogger(__name__)
new_conf_path = os.path.join(Machine.getTempDir(), 'slider-hbaseConf')
if not os.path.exists(new_conf_path):
    os.mkdir(new_conf_path)
HBASE_CONF_DIR = Config.get('hbase', 'HBASE_CONF_DIR')
util.copyReadableFilesFromDir(HBASE_CONF_DIR, new_conf_path)
buildNo = ""


class ruSlider:
    _LOCAL_CONF_DIR = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'slider-conf')
    _LOCAL_WORK_DIR = ""
    global _LOCAL_PATH_APPCONFIG
    _LOCAL_PATH_APPCONFIG = ""
    templatesPath = os.path.join(os.getcwd(), "tests", "rolling_upgrade", "slider-hbase", "templates")
    HBASE_USER = Config.get('hbase', 'HBASE_USER')
    HDFS_USER = Config.get('hadoop', 'HDFS_USER')
    TABLE_NAME = "longrunning"
    SCAN_TABLE_CMD = ["scan '" + TABLE_NAME + "'"]

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        #        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #        UpgradePerNode.reportProgress("###  Starting background job setup for Slider Hbase application  ####")
        #        from beaver.component.slider import Slider
        #        from beaver.component.sliderhbase import SliderHBase
        #        from beaver.component.hbase import HBase
        #
        #        HDFS.deleteDirectory("/user/hbase/.slider/cluster/sliderhbase",cls.HBASE_USER)
        #        global buildNo
        #        hbaseversion = HBase.getVersionFromBuild()
        #        buildNo=re.search("\d+\.\d+.\d+.(\S+)-.*",hbaseversion).group(1)
        #        buildNo,zipFile = SliderHBase.getZipFileAndBuild(buildNo)
        #        HBASE_VER = SliderHBase.getSliderHbaseVersion()
        #
        #        HDFS.createDirectoryAsUser("/slider", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        HDFS.copyFromLocal(zipFile, "/slider/slider-hbase-app-package-"+ HBASE_VER+"-"+buildNo+"-hadoop2.zip", user=cls.HBASE_USER)
        #        HDFS.createDirectoryAsUser("/user/hbase", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #
        #        HDFS.createDirectoryAsUser("/slider/agent", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        HDFS.createDirectoryAsUser("/slider/agent/conf", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        agentPath=os.path.join(Slider.getSliderHome(), "agent")
        #        confPth=os.path.join(agentPath, "conf")
        #        HDFS.copyFromLocal(os.path.join(confPth, "agent.ini"), "/slider/agent/conf", user=cls.HBASE_USER)
        #        HDFS.copyFromLocal(os.path.join(agentPath, "slider-agent.tar.gz"), "/slider/agent", user=cls.HBASE_USER)
        #
        #        HDFS.createDirectoryAsUser("/apps/slhb", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        HDFS.createDirectoryAsUser("/apps/slhb/data", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        HDFS.chmod(cls.HDFS_USER, 777, "/apps/slhb/data")
        #        HDFS.createDirectoryAsUser("/apps/slhb/staging", adminUser=cls.HDFS_USER, user=cls.HBASE_USER)
        #        HDFS.chmod(cls.HDFS_USER, 777, "/apps/slhb/staging")
        #
        #        from beaver.component.hbase import HBase
        #
        #        APP_CONFIG="appConfig.json"
        #        JAVA_HOME = Config.get('machine', 'JAVA_HOME')
        #        if Hadoop.isSecure():
        #            APP_CONFIG="appConfig-secure.json"
        #        global _LOCAL_PATH_APPCONFIG
        #        _LOCAL_PATH_APPCONFIG=os.path.join(cls.templatesPath, APP_CONFIG)
        #        variables = { "TODO-JAVAHOME" : JAVA_HOME,
        #                    "TODO-BUILDNO" : buildNo,
        #                    "TODO-HBASE_VER" : HBASE_VER,
        #                  }
        #        variablesSecure = { "TODO-JAVAHOME" : JAVA_HOME,
        #                    "TODO-BUILDNO" : buildNo,
        #                    "TODO-HBASE_VER" : HBASE_VER,
        #                    "TODO-RS-PRINCIPAL" : HBase.getConfigValue("hbase.regionserver.kerberos.principal"),
        #                    "TODO-RS-KEYTAB" : HBase.getConfigValue("hbase.regionserver.keytab.file"),
        #                    "TODO-HEADLESS-KEYTAB" : Machine.getHeadlessUserKeytab(cls.HBASE_USER),
        #                    "TODO-HEADLESS-PRINCIPAL" : cls.HBASE_USER+"@"+Machine.get_user_realm(),
        #                    "TODO-MASTER-PRINCIPAL" : HBase.getConfigValue("hbase.master.kerberos.principal"),
        #                    "TODO-MASTER-KEYTAB" : HBase.getConfigValue("hbase.master.keytab.file"),
        #                    "TODO-REST-AUTH-KEYTAB" : HBase.getConfigValue("hbase.rest.authentication.kerberos.keytab"),
        #                    "TODO-REST-PRINCIPAL" : HBase.getConfigValue("hbase.rest.kerberos.principal"),
        #                    "TODO-REST-KEYTAB" : HBase.getConfigValue("hbase.rest.keytab.file"),
        #                    "TODO-THRIFT-PRINCIPAL" : HBase.getConfigValue("hbase.master.kerberos.principal"),
        #                    "TODO-THRIFT-KEYTAB" : HBase.getConfigValue("hbase.master.keytab.file"),
        #                    "TODO-NN-PRINCIPAL" : HDFS.getConfigValue("dfs.namenode.kerberos.principal"),
        #                    "TODO-NN-SPNEGO-PRINCIPAL" : HDFS.getConfigValue("dfs.namenode.kerberos.internal.spnego.principal"),
        #                    "TODO-SNN-PRINCIPAL" : HDFS.getConfigValue("dfs.secondary.namenode.kerberos.principal"),
        #                    "TODO-SNN-SPNEGO-PRINCIPAL" : HDFS.getConfigValue("dfs.secondary.namenode.kerberos.internal.spnego.principal"),
        #                    "TODO-DN-PRINCIPAL" : HDFS.getConfigValue("dfs.datanode.kerberos.principal"),
        #                  }
        #        if Hadoop.isSecure():
        #            util.copyFile(_LOCAL_PATH_APPCONFIG, _LOCAL_PATH_APPCONFIG, variablesSecure)
        #        else:
        #            util.copyFile(_LOCAL_PATH_APPCONFIG, _LOCAL_PATH_APPCONFIG, variables)
        #        UpgradePerNode.reportProgress("###  Finished background job setup for Slider Hbase application  ####")
        logger.info("background_job_setup - Slider Application is disabled.")

        if runSmokeTestSetup:
            cls.smoke_test_setup()

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        from beaver.component.slider import Slider
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        HADOOPQA_USER = Config.get('hadoop', 'HADOOPQA_USER')
        UpgradePerNode.reportProgress("###  Starting set up for Slider smoke test  ####")
        if Hadoop.isSecure():
            keytabFile = Machine.getHeadlessUserKeytab(HADOOPQA_USER)
            kinitloc = Machine.which("kinit", "root")
            cmd = "%s  -k -t %s %s" % (kinitloc, keytabFile, Machine.get_user_principal(HADOOPQA_USER))
            exit_code, stdout = Machine.run(cmd)
            if exit_code != 0:
                UpgradePerNode.reportProgress("###  smoke test setup for Slider failed due to kinit failed  ####")

        # Local directory in artifacts that we'll run tests from
        # it is possible the download_source() will fail
        try:
            cls._LOCAL_WORK_DIR = Slider.download_source(useHDPBaseRepoFile=False, isRUcluster=True)
            logger.info("Local work dir = %s" % cls._LOCAL_WORK_DIR)
        except TypeError as err:
            UpgradePerNode.reportProgress("[FAILED][Slider][Smoke] Slider Source download fail.")
            return
        if not cls._LOCAL_WORK_DIR:
            UpgradePerNode.reportProgress("[FAILED][Slider][Smoke] Slider Source Code missing.")
            return
        # Local conf directory with modified conf for tests
        if not os.path.exists(cls._LOCAL_CONF_DIR):
            os.makedirs(cls._LOCAL_CONF_DIR)
        slider_conf = os.path.join(Slider.getSliderHome(), 'conf')
        logger.info("slider_conf = %s" % slider_conf)
        logger.info("os path exist slider_conf = %s" % os.path.exists(slider_conf))
        if os.path.exists(slider_conf):
            Machine.copy(
                os.path.join(slider_conf, 'log4j.properties'), os.path.join(cls._LOCAL_CONF_DIR, 'log4j.properties')
            )
            Machine.copy(
                os.path.join(slider_conf, 'slider-env.sh'), os.path.join(cls._LOCAL_CONF_DIR, 'slider-env.sh')
            )
        else:
            UpgradePerNode.reportProgress("[FAILED][Slider][Smoke] Slider Conf %s missing" % slider_conf)
            return

        if Hadoop.isSecure():
            util.writePropertiesToConfigXMLFile(
                os.path.join(Slider.getSliderHome(), 'conf', 'slider-client.xml'),
                os.path.join(cls._LOCAL_CONF_DIR, 'slider-client.xml'), {
                    "slider.funtest.enabled": "true",
                    "slider.test.agent.enabled": "true",
                    "HADOOP_CONF_DIR": Config.get('hadoop', 'HADOOP_CONF'),
                    "slider.am.keytab.local.path": Machine.getHeadlessUserKeytab(HADOOPQA_USER),
                    "slider.keytab.principal.name": Machine.get_user_principal(HADOOPQA_USER)
                }
            )
        else:
            util.writePropertiesToConfigXMLFile(
                os.path.join(Slider.getSliderHome(), 'conf', 'slider-client.xml'),
                os.path.join(cls._LOCAL_CONF_DIR, 'slider-client.xml'), {
                    "slider.funtest.enabled": "true",
                    "HADOOP_CONF_DIR": Config.get('hadoop', 'HADOOP_CONF'),
                    "slider.test.agent.enabled": "true"
                }
            )

        logger.info("Local work dir = %s" % cls._LOCAL_WORK_DIR)
        # Check that the precondition is met and the source is available
        if not os.path.exists(cls._LOCAL_WORK_DIR) or not os.path.exists(os.path.join(cls._LOCAL_WORK_DIR, 'pom.xml')):
            logger.info('Slider source does not appear to exist at %s' % (cls._LOCAL_WORK_DIR))
            UpgradePerNode.reportProgress(
                "###  Slider source does not appear to exist at %s ####" % (cls._LOCAL_WORK_DIR)
            )
        logger.info("Local work dir = %s" % cls._LOCAL_WORK_DIR)
        if cls._LOCAL_WORK_DIR == None:
            logger.info("ERROR: cls._LOCAL_WORK_DIR is None")
        # Install first so isolated modules can be tested
        exit_code, stdout = Maven.run(
            "clean install -DskipTests "
            "-Dhadoop.version=%s "
            "-Dprivate.repo.url=%s " % (Hadoop.getVersion(), Maven.getPublicRepoUrl()),
            cwd=cls._LOCAL_WORK_DIR
        )
        if exit_code != 0:
            UpgradePerNode.reportProgress("### Error installing Slider source : %d: %s ####" % (exit_code, stdout))
        else:
            UpgradePerNode.reportProgress("### Slider source install passed ####")

    @classmethod
    def write_hbase_site(cls, config=None):
        '''
        Obtain hbase-site.xml from Slider HBase and write to modified config path
        '''
        global new_conf_path
        from beaver.component.sliderhbase import SliderHBase
        from beaver.component.slider import Slider

        tmpHBaseConfFile = os.path.join(Machine.getTempDir(), "hbase-site.xml")
        if Hadoop.isSecure():
            hbasesite = Slider.registry(
                "sliderhbase",
                flags="--getconf hbase-site --out " + tmpHBaseConfFile,
                format="xml",
                user=cls.HBASE_USER,
                userFlag=cls.HBASE_USER
            )
        else:
            hbasesite = Slider.registry(
                "sliderhbase",
                flags="--getconf hbase-site --out " + tmpHBaseConfFile,
                format="xml",
                user=cls.HBASE_USER
            )
        propertyMap = {'hbase.tmp.dir': '/tmp/hbase-tmp'}
        generated_hbase_conf = os.path.join(new_conf_path, "hbase-site.xml")
        # QE-3108
        # if the hbase app is created successfully, a /tmp/hbase-site.xml will be
        # generated from the app. Otherwise if the /tmp/hbase-site.xml is missing
        # it means the slider app creation fails
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if os.path.isfile(generated_hbase_conf):
            util.writePropertiesToConfigXMLFile(tmpHBaseConfFile, generated_hbase_conf, propertyMap)
        else:
            UpgradePerNode.reportProgress(
                "[FAILED][Slider][background] Slider hbase background setup failed due to hbase-site.xml not generated"
            )

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        #        from beaver.component.slider import Slider
        #        from beaver.component.sliderhbase import SliderHBase
        #        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #        UpgradePerNode.reportProgress("### Starting hbase-slider app ####")
        #
        #        CREATE_TABLE_CMD = ["create '"+cls.TABLE_NAME+"', 'family1', 'family2', 'family3'"]
        #        ##insert data into table using put operation
        #        INSERT_DATA_CMDS =  ["put '"+cls.TABLE_NAME+"', 'row1', 'family1:col1', 'Tahoe'",
        #               "put '"+cls.TABLE_NAME+"', 'row1', 'family2:col1', 'Zurich'",
        #               "put '"+cls.TABLE_NAME+"', 'row1', 'family3:col1', 'Dal'"]
        #
        #        global new_conf_path
        #        global _LOCAL_PATH_APPCONFIG
        #        from beaver.component.hbase import HBase
        #        (exit_code, stdout) = Slider.create("sliderhbase", _LOCAL_PATH_APPCONFIG, os.path.join(cls.templatesPath, "resources.json"), user=cls.HBASE_USER, queue="slider")
        #        if exit_code == 0:
        #            UpgradePerNode.reportProgress("### Starting hbase-slider did NOT succeed ####")
        #        else:
        #            UpgradePerNode.reportProgress("### Starting hbase-slider SUCCEEDED ####")
        #
        #        appID = YARN.waitForAppId(timeout=120, interval=5, targetState="RUNNING")
        #        if appID is None:
        #            UpgradePerNode.reportProgress("### Slider app does not reach RUNNING state. ####")
        #        else:
        #            UpgradePerNode.reportProgress("### Slider app reached RUNNING state. ####")
        #            time.sleep(120)
        #            cls.write_hbase_site()
        #            exit_code, stdout = HBase.runShellCmds(CREATE_TABLE_CMD, user=cls.HBASE_USER, configPath=new_conf_path)
        #            UpgradePerNode.reportProgress("created " + cls.TABLE_NAME)
        #            exit_code, stdout = HBase.runShellCmds(INSERT_DATA_CMDS, user=cls.HBASE_USER, configPath=new_conf_path)
        #            UpgradePerNode.reportProgress("inserted data into " + cls.TABLE_NAME)
        #        return 1
        logger.info("run_background_job - Slider Application is disabled.")
        return 0

    @classmethod
    def id_generator(cls, size=6, chars=string.ascii_lowercase + string.digits):
        return 'table_' + ''.join(random.choice(chars) for x in range(size))

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''
        global new_conf_path
        global buildNo
        from beaver.component.hbase import HBase
        # Run slider agent labels funtests
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Slider smoke test started ####")
        exit_code, stdout = HBase.runShellCmds(cls.SCAN_TABLE_CMD, user=cls.HBASE_USER, configPath=new_conf_path)
        UpgradePerNode.reportProgress("### Slider smoke test scanned " + cls.TABLE_NAME)

        hbase_ver = HBase.getVersion(configPath=new_conf_path)
        if buildNo not in hbase_ver:
            UpgradePerNode.reportProgress("### smoke test failed: " + buildNo + " not found in " + hbase_ver)
        else:
            UpgradePerNode.reportProgress("### smoke test passed: " + buildNo + " found in " + hbase_ver)
        UpgradePerNode.reportProgress("scanned " + cls.TABLE_NAME)
        TABLE_NM = cls.id_generator(10)
        CREATE_TABLE_CMD = ["create '" + TABLE_NM + "', 'family1', 'family2', 'family3'"]
        exit_code, stdout = HBase.runShellCmds(CREATE_TABLE_CMD, user=cls.HBASE_USER, configPath=new_conf_path)
        if exit_code == 0:
            UpgradePerNode.reportProgress("created " + TABLE_NM)
        else:
            UpgradePerNode.reportProgress("creation of " + TABLE_NM + "failed")
        if not cls._LOCAL_WORK_DIR:
            UpgradePerNode.reportProgress(
                "[FAILED][Slider][Smoke] Slider smoke test failed due to Slider source code unavailability"
            )
            return

        exit_code, stdout = Maven.run(
            "verify -Dslider.conf.dir=%s "
            "-Dhadoop.version=%s "
            "-Dprivate.repo.url=%s "
            "-Dit.test=AppsUpgradeIT" % (cls._LOCAL_CONF_DIR, Hadoop.getVersion(), Maven.getPublicRepoUrl()),
            cwd=os.path.join(cls._LOCAL_WORK_DIR, 'slider-funtest')
        )
        testresults = {}
        testResultFiles = []
        TEST_RESULT = {}

        # get a list of all the test result files
        for name, dirs, files in os.walk(cls._LOCAL_WORK_DIR):
            if os.path.basename(name) == 'target':
                # Add in each failsafe-report we find -- this test only runs failsafe reports
                testResultFiles.extend(util.findMatchingFiles(os.path.join(name, 'failsafe-reports'), 'TEST-*.xml'))

        for resultFile in testResultFiles:
            testresults.update(util.parseJUnitXMLResult(resultFile))
        for key, value in testresults.items():
            TEST_RESULT[key] = value

        logger.info("=======================")
        logger.info(TEST_RESULT)
        logger.info("=======================")
        TestCases = TEST_RESULT.keys()
        for testcase in TestCases:
            result = TEST_RESULT[testcase]['result']
            if result == "pass":
                UpgradePerNode.reportProgress("[PASSED][Slider][Smoke] Slider smoke test passed")
            else:
                UpgradePerNode.reportProgress("[FAILED][Slider][Smoke] Slider smoke test failed")

    @classmethod
    def rerun_background_job_with_newversion(cls):
        #        from beaver.component.slider import Slider
        #        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #        from beaver.component.slider import Slider
        #        from beaver.component.sliderhbase import SliderHBase
        #
        #        global buildNo
        #        buildNo,zipFile = SliderHBase.getZipFileAndBuild()
        #        HBASE_VER = SliderHBase.getSliderHbaseVersion()
        #        HDFS.copyFromLocal(zipFile, "/slider/slider-hbase-app-package-"+ HBASE_VER+"-"+buildNo+"-hadoop2.zip", user=cls.HBASE_USER)
        #
        #        UpgradePerNode.reportProgress("### Get the app_config.json and modify with latest build ####")
        #        #COPY BACK FROM HDFS
        #        ARTIFACTS_DIR=Machine.getTempDir()
        #        HDFS.copyToLocal("/user/hbase/.slider/cluster/sliderhbase/app_config.json", ARTIFACTS_DIR, cls.HBASE_USER)
        #        # MODIFY THE FILE WITH NEW APP PACKAGE
        #        APP_CONFIG_FILE_TMP = os.path.join(Config.getEnv('ARTIFACTS_DIR'),
        #                               'appConfig-modified-tmp.json')
        #        util.writePropertiesToConfigJSONFile(os.path.join(ARTIFACTS_DIR,"app_config.json"),
        #                                   APP_CONFIG_FILE_TMP,
        #                                   {
        #                                     "application.def" :  "/slider/slider-hbase-app-package-"+ HBASE_VER+"-"+buildNo+"-hadoop2.zip",
        #                                      "site.global.app_root": "${AGENT_WORK_ROOT}/app/install/hbase-"+ HBASE_VER+"-"+buildNo+"-hadoop2",
        #                                      "package_list":  "files/hbase-"+ HBASE_VER+"-"+buildNo+"-hadoop2.tar.gz"},
        #                                    ["global"], "")
        #        HDFS.deleteFile("/user/hbase/.slider/cluster/sliderhbase/app_config.json",cls.HBASE_USER)
        #        HDFS.copyFromLocal(APP_CONFIG_FILE_TMP,"/user/hbase/.slider/cluster/sliderhbase/app_config.json", cls.HBASE_USER)
        #
        #        UpgradePerNode.reportProgress("### START the app ####")
        #
        #        Slider.start("sliderhbase", user=cls.HBASE_USER)
        #        time.sleep(120)
        #        UpgradePerNode.reportProgress("### Slider Hbase started####")
        logger.info("rerun_background_job_with_newversion - Slider Application is disabled.")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        logger.info("background_job_teardown: Slider app is disabled")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        logger.info("verifyLongRunningJob - Slider app is disabled")

    @classmethod
    def stopSliderLongRunningJob(cls):
        '''
        Stop background job after end of all component upgrade
        '''
        #        from beaver.component.slider import Slider
        #        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        #        UpgradePerNode.reportProgress("### Stopping Slider Hbase ####")
        #
        #        Slider.stop("sliderhbase", user=cls.HBASE_USER, force=True)
        #        UpgradePerNode.reportProgress("### Slider Hbase stopped####")
        logger.info("stopSliderLongRunningJob - Slider Application is disabled.")

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("No master service for Slider")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("No slave service for Slider")

    @classmethod
    def upgrade_client_gateway(cls, version, node, config=None):
        '''
        Upgrades client services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        cls.stopSliderLongRunningJob()
        cls.upgrade_client(version, node, config)
        cls.rerun_background_job_with_newversion()

    @classmethod
    def upgrade_client(cls, version, node, config=None):
        '''
        Upgrades client services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("slider-client", version, HDFS.getGateway())

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("No master service for Slider")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("No slave service for Slider")

    @classmethod
    def downgrade_client_gateway(cls, version, node, config=None):
        '''
        Downgrades client services :
        :param version: Version to be downgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        cls.stopSliderLongRunningJob()
        cls.upgrade_client(version, node, config)
        cls.rerun_background_job_with_newversion()

    @classmethod
    def downgrade_client(cls, version, node, config=None):
        '''
        Downgrade client services :
        :param version: Version to be downgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("slider-client", version, HDFS.getGateway())

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        cls.run_smoke_test("001", config=None)

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        cls.run_smoke_test("001", config=None)
