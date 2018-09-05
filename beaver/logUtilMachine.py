#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
'''
QE HDP Stack Log Aggregation Framework (Machine class)
'''
import logging
import os
import ConfigParser
from ConfigParser import NoOptionError

from beaver.machine import Machine

logger = logging.getLogger(__name__)


def get_rsync_parameters():
    config = ConfigParser.ConfigParser()
    suiteconf = os.path.join('conf', 'suite.conf')
    config.read(suiteconf)
    try:
        _ssh_port_number = str(config.get(section='logserver', option='LOGSERVER_SSH_PORT'))
    except NoOptionError:
        _ssh_port_number = "22"
    try:
        _retry_count = int(config.get(section='logserver', option='NO_OF_RETRIES'))
    except NoOptionError:
        _retry_count = 10
    try:
        _retry_sleep_time = int(config.get(section='logserver', option='RETRIES_SLEEP_TIME'))
    except NoOptionError:
        _retry_sleep_time = 10
    return _ssh_port_number, _retry_count, _retry_sleep_time


ssh_port_number, retry_count, retry_sleep_time = get_rsync_parameters()


class LogUtilMachine(object):
    '''
    Machine class for Log Util
    '''
    WINDOWS_KEYPAIR_LOCATION = "c:\\testtools\\id_rsa_log_server_hrt_qa"
    LINUX_KEYPAIR_LOCATION = "/home/hrt_qa/.ssh/id_rsa_log_server_hrt_qa"

    if Machine.isHumboldt():
        # for secure cluster in humboldt path is different
        if os.path.exists(LINUX_KEYPAIR_LOCATION) is False:
            LINUX_KEYPAIR_LOCATION = "/home/HDINSIGHT/hrt_qa/.ssh/id_rsa_log_server_hrt_qa"

    def __init__(self):
        pass

    @classmethod
    def makedirs(cls, user, host, filepath, passwd=None, logoutput=True):  # pylint: disable=unused-argument
        if Machine.isLinux():
            # pylint: disable=line-too-long
            cmd = 'ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %s %s@%s "mkdir -p %s/"' % (
                LogUtilMachine.LINUX_KEYPAIR_LOCATION, ssh_port_number, user, host, filepath
            )
            # pylint: enable=line-too-long
            if logoutput:
                logger.info("LogUtilMachine.rm cmd=%s", cmd)
            return Machine.run(cmd, logoutput=logoutput, retry_count=retry_count, retry_sleep_time=retry_sleep_time)
        else:
            cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s  -p %s %s@%s "mkdir %s/"' % (
                LogUtilMachine.WINDOWS_KEYPAIR_LOCATION, ssh_port_number, user, host, filepath
            )
            return Machine.run(cmd, logoutput=logoutput, retry_count=retry_count, retry_sleep_time=retry_sleep_time)

    @classmethod
    def rm(cls, user, host, filepath, isdir=False, passwd=None, logoutput=True):  # pylint: disable=unused-argument
        if Machine.isLinux():
            # pylint: disable=line-too-long
            cmd = 'ssh -i %s  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  -p %s %s@%s "rm -rf %s/"' % (
                LogUtilMachine.LINUX_KEYPAIR_LOCATION, ssh_port_number, user, host, filepath
            )
            # pylint: enable=line-too-long
            if logoutput:
                logger.info("LogUtilMachine.rm cmd=%s", cmd)
            return Machine.run(cmd, logoutput=logoutput, retry_count=retry_count, retry_sleep_time=retry_sleep_time)

        else:
            cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s -p %s %s@%s "mkdir %s/"' % (
                LogUtilMachine.WINDOWS_KEYPAIR_LOCATION, ssh_port_number, user, host, filepath
            )
            if logoutput:
                logger.info("LogUtilMachine.rm cmd=%s", cmd)
        return None

    @classmethod
    def chmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None, logoutput=True):
        pass

    @classmethod
    def runas(  # pylint: disable=unused-argument
            cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None, doEscapeQuote=True
    ):
        if Machine.isLinux():
            cmd = 'ssh -i %s  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %s %s@%s "%s/"' % (
                LogUtilMachine.LINUX_KEYPAIR_LOCATION, ssh_port_number, user, host, cmd
            )
            return Machine.run(cmd, logoutput=logoutput, retry_count=retry_count, retry_sleep_time=retry_sleep_time)

        else:
            cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s %s@%s "%s/"' % (
                LogUtilMachine.WINDOWS_KEYPAIR_LOCATION, user, host, cmd
            )
            return Machine.run(cmd, logoutput=logoutput, retry_count=retry_count, retry_sleep_time=retry_sleep_time)
