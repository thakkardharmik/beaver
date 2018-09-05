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
APIs for rsync
'''
import logging
import os
import time
import ConfigParser
from ConfigParser import NoOptionError

from beaver import util
from beaver.config import Config
from beaver.dynamicLogCollector import DynamicLogCollector
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
    try:
        _is_log_collection_v2 = str(config.get(section='logserver', option='LOG_COLLECTION_V2'))
    except NoOptionError:
        _is_log_collection_v2 = "no"
    return _ssh_port_number, _retry_count, _retry_sleep_time, _is_log_collection_v2


ssh_port_number, retry_count, retry_sleep_time, is_log_collection_v2 = get_rsync_parameters()


class RSync(object):
    def __init__(self):
        pass

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True):  # pylint: disable=unused-argument
        # TODO: Unused arguments
        return cls.runas(None, cmd, None, cwd=None, env=None, logoutput=logoutput, passwd=None)

    @classmethod
    def runas(cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None):
        if Machine.isLinux():
            if user is None and passwd is None:
                user = Machine.getAdminUser()
                keypairLocation = "/home/hrt_qa/.ssh/id_rsa_log_server_hrt_qa"
                if Machine.isHumboldt():
                    # for secure cluster in humboldt path is different
                    if not os.path.exists(keypairLocation):
                        keypairLocation = "/home/HDINSIGHT/hrt_qa/.ssh/id_rsa_log_server_hrt_qa"
                # pylint: disable=line-too-long
                cmd = "rsync -e 'ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  -p %s -o PreferredAuthentications=publickey' %s" % (
                    keypairLocation, ssh_port_number, cmd
                )
                # pylint: enable=line-too-long
            else:
                # pylint: disable=line-too-long
                cmd = "rsync -e \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %s -o PreferredAuthentications=publickey\" %s" % (
                    ssh_port_number, cmd
                )
                # pylint: enable=line-too-long
            if logoutput:
                logger.info("RSync.runas cmd=%s", cmd)
            return Machine.runas(user, cmd, host, cwd, env, logoutput, passwd, retry_count, retry_sleep_time)
        else:
            rsyncHome = "/cygdrive/c/testtools/cwrsync"
            rsyncLocation = "c:\\testtools\\cwrsync\\rsync.exe"

            #must use c:\\ so remote powershell can pick this up.
            keypairLocation = "c:\\testtools\\id_rsa_log_server_hrt_qa"

            # pylint: disable=line-too-long
            cmd = "%s -e \"%s/ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %s -o PreferredAuthentications=publickey\" %s" % \
                  (rsyncLocation, rsyncHome, keypairLocation, ssh_port_number, cmd)
            # pylint: enable=line-too-long

            if logoutput:
                logger.info("RSync.runas cmd=%s", cmd)
            if Machine.isSameHost(host, None):
                pass
            else:
                # remote
                # I cant find a way to call this on the fly.
                # The only way it can work is to create bat file for the command and run the file.
                batchFilename = 'rsync-%s.bat' % str(int(time.time()))
                tmpLocalFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), batchFilename)
                logger.info("write to %s. contents=%s", tmpLocalFile, cmd)
                util.writeToFile(cmd, tmpLocalFile)
                destPath = os.path.join(Machine.getTempDir(), batchFilename)
                Machine.copyFromLocal(Machine.getAdminUser(), host, tmpLocalFile, destPath, Machine.getAdminPasswd())
                cmd = destPath
            return Machine.runas(
                Machine.getAdminUser(),
                cmd,
                host,
                cwd,
                env,
                logoutput,
                passwd=Machine.getAdminPasswd(),
                retry_count=retry_count,
                retry_sleep_time=retry_sleep_time
            )

    @classmethod
    def rsyncToRemoteHost(
            cls,
            user,
            host,
            passwd,
            srcPath,
            destPath,
            flag="-zrhp --timeout=3600 --chmod=u=rwx,g=rwx,o=r",
            destHost=None,
            destUser=None,
            logoutput=True,
            cwd=None,
            env=None
    ):
        '''
        Call rsync client at host to transfer local files to remote location.
        Returns (exit_code, stdout).
        '''
        DynamicLogCollector.add_log_entry(source=srcPath, destination=destPath, ipaddress=host)
        if is_log_collection_v2 == "no":
            if logoutput:
                logger.info("dorsyncRemote destHost=%s destUser=%s", destHost, destUser)
            cmd = flag
            if Machine.isWindows():
                # convert c:\dir1 to /cygdrive/c/dir1
                srcPath = srcPath.replace("\\", "/")
                srcPath = srcPath.replace("c:", "/cygdrive/c")
                srcPath = srcPath.replace("d:", "/cygdrive/d")
                srcPath = srcPath.replace("e:", "/cygdrive/e")
                srcPath = srcPath.replace("C:", "/cygdrive/c")
                srcPath = srcPath.replace("D:", "/cygdrive/d")
                srcPath = srcPath.replace("E:", "/cygdrive/e")
                # srcPath can be \\maint22-yarn24\D$\hadoop\logs\hadoop (originally)
                # or //maint22-yarn24/D$/hadoop/logs/hadoop/* at this point
                srcPath = util.convertWindowsDollarPathToCygdrivePath(srcPath)
            # In Windows, some get-log-dir APIs are not valid and will yield *.
            # We want to skip this. This is a hack that works.
            if srcPath != "*":
                cmd += " " + srcPath
                if (destHost is None and destUser is not None) or (destHost is not None and destUser is None):
                    # Only host appears or only user appears.
                    # this is an error except when host is localhost and user is None.
                    return (-1, "wrong parameter call")
                cmd += " " + destUser + "@" + destHost + ":" + destPath
                return cls.runas(user, cmd, host, cwd=cwd, env=env, logoutput=logoutput, passwd=passwd)
        return None
