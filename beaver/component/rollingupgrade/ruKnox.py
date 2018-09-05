#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, string, time, socket, logging, platform, urllib2, collections, datetime, json
import urllib, sys
from beaver.component.hadoop import Hadoop, HDFS, MAPRED, YARN
from beaver.component.knox import Knox
from beaver.component import HadoopJobHelper
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.xa import Xa
from beaver import util
from beaver import configUtils

logger = logging.getLogger(__name__)

KNOX_HOST = Config.get('knox', 'KNOX_HOST').split(',')[0]
KNOX_GUEST_USER = "guest"
KNOX_TOPOLOGY = os.path.join(Config.get('knox', 'KNOX_HOME'), 'conf', 'topologies')
RANGER_KNOX_POLICY = None


class ruKnox:
    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        created new topology file to check service registry does not persist after upgrade
        '''
        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=KNOX_HOST,
            srcpath=os.path.join(KNOX_TOPOLOGY, 'default.xml'),
            destpath=os.path.join(KNOX_TOPOLOGY, 'default_new.xml'),
            passwd=Machine.getAdminPasswd()
        )
        time.sleep(15)
        #Knox.setupHttpRequestsVerboseLogging()
        cls.setupRanger()
        logger.info("TODO")

    @classmethod
    def run_webhdfs_test(cls):
        '''
        Knox WebHDFS Smoke test
        '''
        cmd = "curl -I -k -u guest:guest-password -X GET 'https://%s:8443/gateway/default/webhdfs/v1/?op=LISTSTATUS'" % (
            KNOX_HOST
        )
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
        #Fix for intermittent 401 issue - QE-14368
        if "401 Unauthorized" in stdout:
            logger.info("LDAP taking it's own time? WebHDFS smoke test via Knox")
            grep_ldap_cmd = "ps aux | grep ldap.jar"
            exit_code, cmdout = Machine.runas(Machine.getAdminUser(), "%s" % (grep_ldap_cmd))
            if "/usr/hdp/current/knox-server/bin/ldap.jar" in cmdout:
                exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
                logger.info("---Knox Demo LDAP is running. Retried command. %s --- %s ---" % (cmdout, stdout))

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if "200 OK" in stdout:
            logger.info("WebHDFS smoke test via Knox Passed")
            UpgradePerNode.reportProgress("### [PASSED][Knox][WebHDFS] Knox Webhdfs Smoke Test Passed ####")
        else:
            logger.info("Smoke failed")
            UpgradePerNode.reportProgress("### [FAILED][Knox][WebHDFS] Knox Webhdfs Smoke Test Failed ####")

    @classmethod
    def run_webhdfs_new_topo_test(cls):
        '''
        Knox WebHDFS Smoke test with new topology deployed after upgrade
        '''
        cmd = "curl -I -k -u guest:guest-password -X GET 'https://%s:8443/gateway/default_new/webhdfs/v1/?op=LISTSTATUS'" % (
            KNOX_HOST
        )
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if "200 OK" in stdout:
            logger.info("WebHDFS smoke test via Knox with new cluster topology Passed")
            UpgradePerNode.reportProgress(
                "### [PASSED][Knox][WebHDFS] Knox Webhdfs with new cluster name Smoke Test Passed ####"
            )
        else:
            logger.info("Smoke failed")
            UpgradePerNode.reportProgress(
                "### [FAILED][Knox][WebHDFS] Knox Webhdfs with new cluster name Test Failed ####"
            )

    @classmethod
    def run_webhcat_test(cls):
        '''
        Knox Webhcat Smoke test
        '''
        cmd = "curl -I -k -u guest:guest-password -X GET 'https://%s:8443/gateway/default/templeton/v1/status'" % (
            KNOX_HOST
        )
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
        # Fix for intermittent 401 issue - QE-14368
        if "401 Unauthorized" in stdout:
            logger.info("LDAP taking it's own time? WebHCat smoke test via Knox")
            grep_ldap_cmd = "ps aux | grep ldap.jar"
            exit_code, cmdout = Machine.runas(Machine.getAdminUser(), "%s" % (grep_ldap_cmd))
            if "/usr/hdp/current/knox-server/bin/ldap.jar" in cmdout:
                exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
                logger.info("---Knox Demo LDAP is running. Retried command. %s --- %s ---" % (cmdout, stdout))

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if "200 OK" in stdout:
            logger.info("Webhcat smoke test via Knox Passed")
            UpgradePerNode.reportProgress("### [PASSED][Knox][WebHcat] Knox Webhcat Smoke Test Passed ####")
        else:
            logger.info("Smoke failed")
            UpgradePerNode.reportProgress("### [FAILED][Knox][WebHcat] Knox Webhcat Smoke Test Failed ####")

    @classmethod
    def run_webhbase_test(cls):
        '''
        Knox Webhbase Smoke test
        '''
        cmd = "curl -I -k -u guest:guest-password -X GET 'https://%s:8443/gateway/default/hbase/version?user.name=%s'" % (
            KNOX_HOST, KNOX_GUEST_USER
        )
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), "%s" % (cmd))
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        if "200 OK" in stdout:
            logger.info("Webhbase smoke test via Knox Passed")
            assertTrue(True)
            UpgradePerNode.reportProgress("### [PASSED][Knox][WebHbase] Knox WebHBase Smoke Test Passed ####")
        else:
            logger.info("Smoke failed")
            UpgradePerNode.reportProgress("### [FAILED][Knox][WebHbase] Knox WebHBase Smoke Test Failed ####")

    @classmethod
    def run_smoke_test(cls, smoketestnumber, config=None):
        '''
        Run smoke test for knox
        :param smoketestnumber: Used for unique output log location
        '''
        logger.info("TODO")
        Knox.restartKnox(True)
        cls.run_webhdfs_test()
        cls.run_webhcat_test()
        #cls.run_webhbase_test()
        #TODO : Add oozie , yarn test
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Knox Smoke Tests completed ####")
        cls.resetRanger()

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Knox Upgrade started ####")
        #Stop knox gateway and apacheDS
        Knox.stopKnox()
        Knox.stopLdap()

        node = Config.get('knox', 'KNOX_HOST').split(',')[0]

        #Switch to the new version
        hdpSelect.changeVersion("knox-server", version, node)

        #Start apacheDs and knox gateway service
        Knox.startLdap()
        Knox.startKnox()

        time.sleep(10)

        UpgradePerNode.reportProgress("### Knox Upgrade Finished ####")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress("### Knox Downgrade started ####")

        #Stop knox gateway and apacheDS
        Knox.stopKnox()
        Knox.stopLdap()
        node = Config.get('knox', 'KNOX_HOST').split(',')[0]
        #Switch to the new version
        hdpSelect.changeVersion("knox-server", version, node)

        #Start apacheDs and knox gateway service
        Knox.startLdap()
        Knox.startKnox()

        time.sleep(10)

        UpgradePerNode.reportProgress("### Knox Downgrade Finished ####")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("TODO")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("TODO : Implement separate tests or simply run smoke test of Knox")

    @classmethod
    def setupRanger(cls):
        if Xa.isPolicyAdminRunning():
            RANGER_KNOX_POLICY = Knox.setupOpenRangerKnoxPolicy()
            Knox.restartKnox()

    @classmethod
    def resetRanger(cls):
        policy = RANGER_KNOX_POLICY
        if not policy == None:
            Xa.deletePolicy(policy['id'])
            Knox.restartKnox()
