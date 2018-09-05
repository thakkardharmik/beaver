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
import os, sys, logging
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.xa import Xa
import fileinput

dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(dir, '../../../tests/xasecure/xa-agents')))
from xa_config_watcher_test import XACWTestCase
from argusSmokeTestBase import RUTestCase
import xa_testenv
from xa_logger import *
logger = logging.getLogger(__name__)


class ruArgusAdmin:
    _base_argus_dir = None
    _SmokeInputDir = None
    hdp_home = '/usr/hdp'
    TEST_CASE = None

    @classmethod
    def background_job_setup(cls, components, runSmokeTestSetup=True, config=None):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### call for background-job setup for argus admin started ####')
        if runSmokeTestSetup:
            # componentList = ["hive","hbase","hdfs","knox","storm"]
            componentList = []
            if Xa.isHdfsInstalled():
                componentList.append('hdfs')
            if Xa.isHiveInstalled():
                componentList.append('hive')
            if Xa.isHBaseInstalled():
                componentList.append('hbase')
            if Xa.isKnoxInstalled():
                componentList.append('knox')
            if Xa.isStormInstalled():
                componentList.append('storm')
            cls.smoke_test_setup(componentList)
        UpgradePerNode.reportProgress('#### call for background-job setup for argus admin done ####')

    @classmethod
    def smoke_test_setup(cls, components_to_test):
        '''
		setting up environment to run tests for argus admin and user-sync plugins.
		'''
        cls.TEST_CASE = RUTestCase()
        cls.TEST_CASE.setUpModule(components_to_test)

    @classmethod
    def run_smoke_test(cls, components_to_test):
        '''
		running tests for argus plugins.
		'''
        '''
		***** HDFS TEST CASES *****
		'''
        if 'hdfs' in components_to_test:

            cls.TEST_CASE.HDFSTestCase_01()
            cls.TEST_CASE.HDFSTestCase_02()
            cls.TEST_CASE.HDFSTestCase_03()
        '''
		***** HIVE TEST CASES *****
		'''
        #if 'hive' in components_to_test:
        #	cls.TEST_CASE.HIVETestCase_01()
        #	cls.TEST_CASE.HIVETestCase_02()
        #	cls.TEST_CASE.HIVETestCase_03()
        #	cls.TEST_CASE.HIVETestCase_04()
        #	cls.TEST_CASE.HIVETestCase_05()
        #	cls.TEST_CASE.HIVETestCase_06()
        #	cls.TEST_CASE.HIVETestCase_07()
        '''
		***** HBASE TEST CASES *****
		'''
        if 'hbase' in components_to_test:
            cls.TEST_CASE.HBASETestCase_01()
            cls.TEST_CASE.HBASETestCase_02()
            cls.TEST_CASE.HBASETestCase_03()
            cls.TEST_CASE.HBASETestCase_04()
            cls.TEST_CASE.HBASETestCase_05()
            cls.TEST_CASE.HBASETestCase_06()
            cls.TEST_CASE.HBASETestCase_07()
        '''
		***** KNOX TEST CASES *****
		'''
        #if 'knox' in components_to_test:
        #	cls.TEST_CASE.KNOXTestCase_01()
        #	cls.TEST_CASE.KNOXTestCase_02()

    @classmethod
    def background_job_teardown(cls):
        '''
		Cleanup for admin and user-sync test cases.
		'''
        if cls.TEST_CASE is not None:
            cls.TEST_CASE.tearDownModule()
        cls.TEST_CASE = None

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
		Upgrades slave services :
		:param version: Version to be upgraded to
		:param node: Slave Node
		:param config: Config location
		:return:
		'''
        logger.info("TODO")

    ## this call is now moved to ruArgus.py, but is kept to preserve standard calls
    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
		Prepare Argus Admin to save State for Upgrade
		'''
        #localTestWorkDir1 = os.path.join(Config.getEnv('ARTIFACTS_DIR'))
        #cls.get_argus_admin_backup(localTestWorkDir1)
        pass

    @classmethod
    def get_argus_admin_backup(cls):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        localTestWorkDir1 = os.path.join(Config.getEnv('ARTIFACTS_DIR'))
        ## to have logic for repos backup
        UpgradePerNode.reportProgress('#### getting repositories backup for ####')
        if cls.get_repositories_backup(localTestWorkDir1):
            UpgradePerNode.reportProgress('#### repositories backup successful ####')
        ## to have logic for policies backup
        UpgradePerNode.reportProgress('#### getting policies backup ####')
        if cls.get_policies_backup(localTestWorkDir1):
            UpgradePerNode.reportProgress('#### policies backup successful ####')

    @classmethod
    def get_repositories_backup(cls, localTestWorkDir1):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### saving existing repositories for backup ####')
        cwObject = XACWTestCase()
        baseUrl = 'http://' + xa_testenv.getEnv('XA_ADMIN_HOST', 'localhost'
                                                ) + ':' + str(xa_testenv.getEnvInt('XA_ADMIN_PORT', 6080))
        urlReposUsingApi = baseUrl + '/service/public/api/repository/'
        username_password = xa_testenv.getEnv('XA_ADMIN_USER', 'admin'
                                              ) + ':' + xa_testenv.getEnv('XA_ADMIN_PASS', 'admin')
        headerResponse, response = cwObject.callPyCurlRequest(
            url=urlReposUsingApi, data=None, method='get', usernamepassword=username_password
        )

        UpgradePerNode.reportProgress('#### get all repositories, headerResponse is : %s ####' % headerResponse)
        UpgradePerNode.reportProgress('#### get all repositories, response is : %s ####' % response)
        if 'HTTP/1.1 200 OK' in headerResponse:
            file_for_repositories = os.path.join(localTestWorkDir1, 'repositories.bak')
            UpgradePerNode.reportProgress('#### saving repositories to path = %s  ####' % str(file_for_repositories))
            openReposFile = open(file_for_repositories, 'wb')
            openReposFile.write(response)
            openReposFile.close()
        else:
            UpgradePerNode.reportProgress('#### unable to save repositories for backup ####')
            return False
        UpgradePerNode.reportProgress('#### repositories back-up file saved successfully ####')
        return True

    @classmethod
    def get_policies_backup(cls, localTestWorkDir1):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### saving existing policies for backup ####')
        cwObject = XACWTestCase()
        baseUrl = 'http://' + xa_testenv.getEnv('XA_ADMIN_HOST', 'localhost'
                                                ) + ':' + str(xa_testenv.getEnvInt('XA_ADMIN_PORT', 6080))
        urlPoliciesUsingApi = baseUrl + '/service/public/api/policy'
        username_password = xa_testenv.getEnv('XA_ADMIN_USER', 'admin'
                                              ) + ':' + xa_testenv.getEnv('XA_ADMIN_PASS', 'admin')
        headerResponse, response = cwObject.callPyCurlRequest(
            url=urlPoliciesUsingApi, data='', method='get', usernamepassword=username_password
        )
        UpgradePerNode.reportProgress('#### get policy headerResponse is : %s ####' % headerResponse)
        if 'HTTP/1.1 200 OK' in headerResponse:

            file_for_policies = os.path.join(localTestWorkDir1, 'policies.bak')
            UpgradePerNode.reportProgress('#### saving policies to path = %s ####' % str(file_for_policies))
            openReposFile = open(file_for_policies, 'wb')
            openReposFile.write(response)
            openReposFile.close()
        else:
            UpgradePerNode.reportProgress('unable to save policies for backup')
            return False
        UpgradePerNode.reportProgress('#### policies back-up file saved successfully ####')
        return True

    @classmethod
    def set_properties_for_install(cls, source_properties_file, destination_properties_file):
        open(destination_properties_file, 'wb')

        for each_line in source_properties_file.read().split('\n'):
            if each_line is not None:
                if len(each_line.strip()) == 0:
                    continue
                if '#' in each_line[0]:
                    continue
                key, value = each_line.strip().split("=", 1)
                key = key.strip()
                value = value.strip()
                cls.ModConfig(destination_properties_file, key, value)

    @classmethod
    # def upgrade_argus(cls):
    def upgrade_master(cls, latestVersion, config, currVersion):
        cls.upgrade_argus_admin(latestVersion, config, currVersion)
        # cls.upgrade_argus_usersync()

    @classmethod
    def upgrade_argus_admin(cls, latestVersion, config, currVersion):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### install argus admin. ####')
        # 'knox.crt'
        # 'cacertswithknox'
        node = Xa.getPolicyAdminHost()
        user = Machine.getAdminUser()
        xa_admin_stop_cmd = 'sudo service ranger-admin stop'
        xa_admin_start_cmd = 'sudo service ranger-admin start'
        ranger_old_version = '/usr/hdp/' + currVersion + '/ranger-admin/'
        ranger_new_version = '/usr/hdp/' + latestVersion + '/ranger-admin/'

        localTestWorkDir1 = os.path.join(Config.getEnv('ARTIFACTS_DIR'))
        knox_cert_files = '{knox.crt,cacertswithknox}'
        source_files_to_copy = os.path.join(ranger_old_version, knox_cert_files)
        command_copy_knoxcerts = 'cp -f %s %s' % (source_files_to_copy, localTestWorkDir1)
        exit_code, stdout = Machine.runas(user, command_copy_knoxcerts, host=node, logoutput=True)
        logger.info('*************************** admin copy command_copy_knoxcerts exit_code = ' + str(exit_code))
        logger.info('*************************** admin copy command_copy_knoxcerts stdout = ' + str(stdout))

        exit_code, stdout = Machine.runas(user, xa_admin_stop_cmd, host=node, logoutput=True)
        logger.info('****************** xa admin stop exit_code = ' + str(exit_code))

        source_properties_file = os.path.join(ranger_old_version, 'install.properties')
        destination_properties_file = os.path.join(ranger_new_version, 'install.properties')
        command = 'cp -f %s %s' % (source_properties_file, destination_properties_file)
        exit_code, stdout = Machine.runas(user, command, host=node, logoutput=True)
        logger.info('*************************** admin copy command exit_code = ' + str(exit_code))
        logger.info('*************************** admin copy command stdout = ' + str(stdout))

        command = '(cd %s && export JAVA_HOME=%s && ./setup.sh)' % (ranger_new_version, Machine.getJavaHome())
        UpgradePerNode.reportProgress('#### ranger-admin: installing new version with command %s ###' % command)
        exit_code, stdout = Machine.runas(user, command, host=node, logoutput=True)

        if exit_code == 0 and 'Installation of XASecure PolicyManager Web Application is completed.' in stdout:
            UpgradePerNode.reportProgress('#### ranger-admin: installation successful ###')

            from beaver.component.rollingupgrade.ruCommon import hdpSelect
            hdpSelect.changeVersion("ranger-admin", latestVersion, node)
            logger.info('*************************** ranger-admin: hdp-select to new version done = ')

            source_files_to_copy = os.path.join(localTestWorkDir1, knox_cert_files)
            command_copy_knoxcerts = 'cp -f %s %s' % (source_files_to_copy, ranger_new_version)
            exit_code, stdout = Machine.runas(user, command_copy_knoxcerts, host=node, logoutput=True)
            logger.info(
                '*************************** admin copy command_copy_knoxcerts back exit_code = ' + str(exit_code)
            )
            logger.info('*************************** admin copy command_copy_knoxcerts back stdout = ' + str(stdout))

            UpgradePerNode.reportProgress(
                '#### ranger-admin: starting new version with command %s ###' % xa_admin_start_cmd
            )
            exit_code, stdout = Machine.runas(user, xa_admin_start_cmd, host=node, logoutput=True)
            logger.info('****************** xa admin start exit_code = ' + str(exit_code))
            logger.info('****************** xa admin start stdout = ' + str(stdout))

            if exit_code == 0:
                UpgradePerNode.reportProgress('#### ranger-admin: new version started successfully ####')
            else:
                UpgradePerNode.reportProgress(
                    '#### ranger-admin: failed to start new version! exit_code=%d ####' % exit_code
                )
        else:
            logger.info('****************** setup.sh script failed for admin ******* ')
            UpgradePerNode.reportProgress(
                '#### ranger-admin: installation of new version failed! exit_code=%d ###' % exit_code
            )

    @classmethod
    # def downgrade_argus(cls):
    def downgrade_master(cls, latestVersion, config, currVersion):
        cls.downgrade_argus_admin(latestVersion, config, currVersion)

    @classmethod
    def downgrade_argus_admin(cls, latestVersion, config, currVersion):
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### ranger-admin: downgrading to old version.. ####')
        # 'knox.crt'
        # 'cacertswithknox'
        node = Xa.getPolicyAdminHost()
        user = Machine.getAdminUser()
        xa_admin_stop_cmd = 'sudo service ranger-admin stop'
        xa_admin_start_cmd = 'sudo service ranger-admin start'

        UpgradePerNode.reportProgress('#### ranger-admin: stopping with command %s ####' % xa_admin_stop_cmd)
        exit_code, stdout = Machine.runas(user, xa_admin_stop_cmd, host=node, logoutput=True)
        logger.info('****************** xa admin stop exit_code = ' + str(exit_code))

        node = Xa.getPolicyAdminHost()
        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("ranger-admin", latestVersion, node)
        logger.info('*************************** hdp-select to new version done = ')

        UpgradePerNode.reportProgress('#### ranger-admin: starting with command %s ####' % xa_admin_start_cmd)
        exit_code, stdout = Machine.runas(user, xa_admin_start_cmd, host=node, logoutput=True)
        logger.info('****************** xa admin start exit_code = ' + str(exit_code))
        logger.info('****************** xa admin start stdout = ' + str(stdout))

        if exit_code == 0:
            UpgradePerNode.reportProgress('#### ranger-admin: downgrade successful ####')
        else:
            UpgradePerNode.reportProgress('#### ranger-admin: downgrade failed! startup exit_code=%d ####' % exit_code)

    @classmethod
    def ru_downgrade_state(cls):
        '''
		Downgrades Namenode
		A downgrade is done - may need to convert state to previous version or state is compatible - again upgrade is being abandoned
		NOTE: this command will not return until namenode shuts down
		'''
        logger.info('TODO')

    @classmethod
    def ru_finalize_state(cls):
        '''
		Upgrade is completed and finalized - save state (if any) can be discarded
		'''
        logger.info('TODO')

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
		Function to test upgrade is done properly after all slaves are upgraded for HDFS
		'''
        logger.info("TODO : need to implement this function")

    @classmethod
    def get_current_release(cls):
        '''
		get current version in action
		:return: current version in use Like 2.2.0.1-177
		'''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        argus_root = "/usr/lib/xapolicymgr"

        linkname = os.path.join(argus_root)
        UpgradePerNode.reportProgress('#### linkname = %s ####' % linkname)
        if os.path.islink(linkname):
            (head, tail) = os.path.split(os.readlink(linkname))
            UpgradePerNode.reportProgress('#### head = %s ####' % head)
            UpgradePerNode.reportProgress('#### tail = %s ####' % tail)
            argus_version = tail.split('-', 1)[1][6:len(tail.split('-', 1)[1])]
            return argus_version
        else:
            UpgradePerNode.reportProgress("#### No valid symlink for argus ####")
            return None

    @classmethod
    def ModConfig(cls, File, Variable, Setting):
        '''
		Modify Config file variable with new setting
		'''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        VarFound = False
        AlreadySet = False
        V = str(Variable)
        S = str(Setting)
        '''
		use quotes if setting has spaces
		'''
        if ' ' in S:
            S = '"%s"' % S

        for line in fileinput.input(File, inplace=1):
            '''
			process lines that look like config settings #
			'''
            if not line.lstrip(' ').startswith('#') and '=' in line:
                _infile_var = str(line.split('=')[0].rstrip(' '))
                _infile_set = str(line.split('=')[1].lstrip(' ').rstrip())
                '''
				only change the first matching occurrence
				'''
                if VarFound == False and _infile_var.rstrip(' ') == V:
                    VarFound = True
                    # don't change it if it is already set #
                    if _infile_set.lstrip(' ') == S:
                        AlreadySet = True
                    else:
                        line = "%s=%s\n" % (V, S)

            sys.stdout.write(line)

        # Append the variable if it wasn't found #
        if not VarFound:
            # logger.info( "Variable '%s' not found.  Adding it to %s" % (V, File), "debug")
            with open(File, "a") as f:
                f.write("%s=%s\n" % (V, S))
        elif AlreadySet == True:
            UpgradePerNode.reportProgress("Variable '%s' unchanged" % (V), "debug")
        else:
            UpgradePerNode.reportProgress("Variable '%s' modified to '%s'" % (V, S), "debug")

        return
