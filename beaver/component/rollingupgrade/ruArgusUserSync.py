#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, sys, re, string, time, socket, logging, platform, urllib, collections, datetime, json
from beaver.machine import Machine
# from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
import fileinput

logger = logging.getLogger(__name__)


class ruArgusUserSync:

    _base_argus_dir = None
    _SmokeInputDir = None
    hdp_home = '/usr/hdp'

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True):
        logger.info('currently not needed as test-cases for user-sync are run simultaneously with argus admin tests')

    @classmethod
    def smoke_test_setup(cls):
        '''
        setting up environment to run tests for argus plugins.
        '''
        logger.info('currently not needed as test-cases for user-sync are run simultaneously with argus admin tests')

    @classmethod
    def run_smoke_test(cls):
        '''
        running tests for argus plugins.
        '''
        logger.info('currently not needed as test-cases for user-sync are run simultaneously with argus admin tests')

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for User-Sync test cases
        '''
        logger.info('currently not needed as test-cases for user-sync are run simultaneously with argus admin tests')

    @classmethod
    def ru_prepare_save_state_for_upgrade(cls):
        '''
        Nothing to do for prepare save state for upgrade
        '''
        logger.info("Not needed for prepare save state for Argus UserSync upgrade  ")

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
                cls.mod_config(destination_properties_file, key, value)

    @classmethod
    def upgrade_master(cls, latestVersion, config, currVersion):
        from beaver.component.xa import Xa
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### install argus admin. ####')
        # 'knox.crt'
        # 'cacertswithknox'
        user = Machine.getAdminUser()
        xa_usersync_stop_cmd = 'sudo service ranger-usersync stop'
        xa_usersync_start_cmd = 'sudo service ranger-usersync start'
        ranger_old_version = '/usr/hdp/' + currVersion + '/ranger-usersync/'
        ranger_new_version = '/usr/hdp/' + latestVersion + '/ranger-usersync/'

        exit_code, stdout = Machine.runas(user, xa_usersync_stop_cmd, host=Xa.getPolicyAdminHost(), logoutput=True)
        logger.info('****************** xa usersync stop exit_code = ' + str(exit_code))

        source_properties_file = os.path.join(ranger_old_version, 'install.properties')
        destination_properties_file = os.path.join(ranger_new_version, 'install.properties')
        command = 'cp -f %s %s' % (source_properties_file, destination_properties_file)
        exit_code, stdout = Machine.runas(user, command, host=Xa.getPolicyAdminHost(), logoutput=True)
        logger.info('*************************** usersync copy command exit_code = ' + str(exit_code))
        logger.info('*************************** usersync copy command stdout = ' + str(stdout))

        command = '(cd %s && export JAVA_HOME=%s && ./setup.sh)' % (ranger_new_version, Machine.getJavaHome())
        UpgradePerNode.reportProgress('#### ranger-usersync: installing new version with command %s ###' % command)
        exit_code, stdout = Machine.runas(user, command, host=Xa.getPolicyAdminHost(), logoutput=True)

        if exit_code == 0:
            UpgradePerNode.reportProgress('#### ranger-usersync: installation successful ###')

            node = Xa.getPolicyAdminHost()
            from beaver.component.rollingupgrade.ruCommon import hdpSelect
            hdpSelect.changeVersion("ranger-usersync", latestVersion, node)
            logger.info('*************************** hdp-select to new version done = ')

            UpgradePerNode.reportProgress(
                '#### ranger-usersync: starting new version with command %s ###' % xa_usersync_start_cmd
            )
            exit_code, stdout = Machine.runas(user, xa_usersync_start_cmd, host=node, logoutput=True)
            logger.info('****************** xa usersync start exit_code = ' + str(exit_code))
            logger.info('****************** xa usersync start stdout = ' + str(stdout))

            if exit_code == 0 and 'UnixAuthenticationService has started successfully.' in stdout:
                UpgradePerNode.reportProgress('#### ranger-usersync: new version started successfully ###')
            else:
                UpgradePerNode.reportProgress(
                    '#### ranger-usersync: new version failed to start! exit_code=%d ###' % exit_code
                )
        else:
            logger.info('****************** setup.sh script failed for usersync ******* ')
            UpgradePerNode.reportProgress(
                '#### ranger-usersync: installation of new version failed! exit_code=%d ###' % exit_code
            )

    @classmethod
    def downgrade_master(cls, latestVersion, config, currVersion):
        from beaver.component.xa import Xa
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        UpgradePerNode.reportProgress('#### ranger-usersync: downgrading to old version.. ####')
        # 'knox.crt'
        # 'cacertswithknox'
        user = Machine.getAdminUser()
        xa_usersync_stop_cmd = 'sudo service ranger-usersync stop'
        xa_usersync_start_cmd = 'sudo service ranger-usersync start'

        node = Xa.getPolicyAdminHost()

        UpgradePerNode.reportProgress('#### ranger-usersync: stopping with command %s ####' % xa_usersync_stop_cmd)
        exit_code, stdout = Machine.runas(user, xa_usersync_stop_cmd, host=node, logoutput=True)
        logger.info('****************** xa usersync stop exit_code = ' + str(exit_code))

        from beaver.component.rollingupgrade.ruCommon import hdpSelect
        hdpSelect.changeVersion("ranger-usersync", latestVersion, node)
        logger.info('*************************** hdp-select to new version done = ')

        UpgradePerNode.reportProgress('#### ranger-usersync: starting with command %s ####' % xa_usersync_start_cmd)
        exit_code, stdout = Machine.runas(user, xa_usersync_start_cmd, host=node, logoutput=True)
        logger.info('****************** xa usersync start exit_code = ' + str(exit_code))
        logger.info('****************** xa usersync start stdout = ' + str(stdout))

        if exit_code == 0 and 'UnixAuthenticationService has started successfully.' in stdout:
            UpgradePerNode.reportProgress('#### ranger-usersync: downgrade successful ####')
        else:
            UpgradePerNode.reportProgress(
                '#### ranger-usersync: downgrade failed! startup error_code=%d ####' % exit_code
            )

    @classmethod
    def ru_downgrade_state(cls):
        '''
        Downgrades Argus UserSync version
        '''
        ## TODO add code to downgrade Argus UserSync
        logger.info("TODO add code to downgrade Argus UserSync")

    @classmethod
    def ru_finalize_state(cls):
        '''
        Upgrade is completed and finalized - save state (if any) can be discarded
        '''
        logger.info("Upgrade is completed and finalized - save state (if any) can be discarded")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after Argus UserSync has
        '''
        logger.info("TODO : need to implement this function")

    @classmethod
    def get_current_release(cls):
        '''
        get current version in action
        :return: current version in use Like 2.2.0.1-177
        '''
        # argus_usersync_root = "/usr/lib/uxugsync"
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        argus_usersync_root = "/usr/lib/argus-usersync"

        linkname = os.path.join(argus_usersync_root)
        UpgradePerNode.reportProgress('#### linkname = %s ####' % linkname)
        if os.path.islink(linkname):
            (head, tail) = os.path.split(os.readlink(linkname))
            UpgradePerNode.reportProgress('#### head = %s ####' % head)
            UpgradePerNode.reportProgress('#### tail = %s ####' + tail)
            # argus_usersync_version = tail.split('-',1)[1][6:len(tail.split('-',1)[1])]
            # argus_usersync_version = tail.split('-',1)[1][6:len(tail.split('-',1)[1])]
            argus_usersync_version = tail.split('-', 2)[2][6:len(tail.split('-', 1)[1])]
            return argus_usersync_version
        else:
            UpgradePerNode.reportProgress("#### No valid symlink for argus ####")
            return None

    @classmethod
    def mod_config(cls, File, Variable, Setting):
        '''
        Modify Config file variable with new setting
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        VarFound = False
        AlreadySet = False
        V = str(Variable)
        S = str(Setting)
        '''
        Use quotes if setting has spaces
        '''
        if ' ' in S:
            S = '"%s"' % S

        for line in fileinput.input(File, inplace=1):
            '''
            Process lines that look like config settings #
            '''
            if not line.lstrip(' ').startswith('#') and '=' in line:
                _infile_var = str(line.split('=')[0].rstrip(' '))
                _infile_set = str(line.split('=')[1].lstrip(' ').rstrip())
                '''
                Only change the first matching occurrence
                '''
                if VarFound == False and _infile_var.rstrip(' ') == V:
                    VarFound = True
                    '''
                    Don't change it if it is already set
                    '''
                    if _infile_set.lstrip(' ') == S:
                        AlreadySet = True
                    else:
                        line = "%s=%s\n" % (V, S)
            sys.stdout.write(line)
        '''
        Append the variable if it wasn't found
        '''
        if not VarFound:
            with open(File, "a") as f:
                f.write("%s=%s\n" % (V, S))
        elif AlreadySet == True:
            UpgradePerNode.reportProgress("#### Variable '%s' unchanged ####" % (V), "debug")
        else:
            UpgradePerNode.reportProgress("#### Variable '%s' modified to '%s' ####" % (V, S), "debug")
        return
