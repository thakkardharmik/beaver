#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
# TODO: Fix these errors!
# pylint: disable=arguments-differ
import getpass
import logging
import os
import platform
import re
import shutil
import socket
import subprocess
import threading
import time
import unicodedata
import uuid

from beaver import util
from beaver.config import Config

REC = re.compile(r"(\r\n|\n)$")
SECURE_PROTOCOL = 'TLSv1'

logger = logging.getLogger(__name__)


class BaseMachine(object):
    """Base class for OS"""
    _adminUser = None
    _defaultAdminUser = None
    _adminPasswd = None
    _tmpDir = None
    _os_name = platform.dist()[0].lower()
    _os_release = None
    _machine_name = platform.machine().lower()
    _user_realm = None

    def __init__(self):
        pass

    @classmethod
    def get_user_realm(cls):
        '''
        Get the user realm from config.
        :return: user realm
        '''
        if cls._user_realm:
            return cls._user_realm

        # default the realm to empty
        cls._user_realm = ''
        if Config.hasOption('machine', 'USER_REALM'):
            cls._user_realm = Config.get('machine', 'USER_REALM', '')
        return cls._user_realm

    @classmethod
    def get_user_principal(cls, user):
        '''
        :param user: For which user you want to generate the principal
        :return:
        Returns principal name with the realm if set. Will return the principal as is
        if the user name has a / in it
        '''
        realm = cls.get_user_realm()
        if user == "admin" and Machine.isHumboldt():
            cluster = ''
            if Config.hasOption('dataconnectors', 'S3_BUCKET_NAME'):
                cluster = Config.get('dataconnectors', 'S3_BUCKET_NAME')
            return '%s-%s' % (user, cluster)
        if not '/' in user and realm and realm != '':
            return '%s@%s' % (user, realm)

        return user

    @classmethod
    def touch(cls, fname, times=None):
        fhandle = file(fname, 'a')
        try:
            os.utime(fname, times)
        finally:
            fhandle.close()

    #touch given absolute file path remotely
    @classmethod
    def touchRemote(  # pylint: disable=unused-argument
            cls,
            host,
            filePath,
            times=None,
            user=None,
            cwd=None,
            env=None,
            logoutput=True,
            passwd=None
    ):
        return cls.run(cls._buildcmd("touch %s" % filePath, user, host, passwd, env=env), cwd, env, logoutput)

    # Runs a command as current user on current host
    # Provides a separate stdout, stderr
    @classmethod
    def runex(cls, cmd, cwd=None, env=None, logoutput=True):
        demarkation = "|" + str(uuid.uuid4()) + "|"
        cmd = cls._decoratedcmd(cmd)
        logger.info(demarkation + "RUNNING: " + cmd)
        streams = {'stdout': "", 'stderr': ""}
        osenv = None
        if env:
            osenv = os.environ.copy()
            for key, value in env.items():
                osenv[key] = value
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=cwd, env=osenv)

        def tee_pipe(pipe, pstr):
            while True:
                procState = proc.poll()
                line = pipe.readline()
                if line:
                    streams[pstr] += line
                    if logoutput:
                        logger.info(demarkation + line.strip())
                else:
                    if procState is not None:
                        break

        t1 = threading.Thread(target=tee_pipe, args=(proc.stdout, 'stdout'))
        t2 = threading.Thread(target=tee_pipe, args=(proc.stderr, 'stderr'))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        stdout = REC.sub("", streams['stdout'], 1)
        # QE-9058: Handle MOTD message in stdout or QE-199 [Monarch Jira]
        if 'This is MOTD message' in stdout\
                or 'Inappropriate ioctl for device' in stdout \
                or 'Configuration ha.zookeeper.quorum is missing.' in stdout \
                or 'coveragedata.CoverageDataFileHandler' in stdout:
            STRINGS_TO_IGNORE = []
            STRINGS_TO_IGNORE.append('######## Hortonworks #############')
            STRINGS_TO_IGNORE.append('This is MOTD message, added for testing in qe infra')
            STRINGS_TO_IGNORE.append('mesg: ttyname failed: Inappropriate ioctl for device')
            STRINGS_TO_IGNORE.append('coveragedata.CoverageDataFileHandler')
            STRINGS_TO_IGNORE.append('Configuration ha.zookeeper.quorum is missing.')
            tmpResult = stdout.split("\n")
            if 'coveragedata.CoverageDataFileHandler' in stdout:
                stdout = "\n".join(util.prune_output(tmpResult, STRINGS_TO_IGNORE, prune_empty_lines=False))
            else:
                stdout = "\n".join(util.prune_output(tmpResult, STRINGS_TO_IGNORE, prune_empty_lines=True))
        stderr = REC.sub("", streams['stderr'], 1)
        if proc.returncode == 0:
            logger.info(demarkation + "Exit Code: " + str(proc.returncode))
        else:
            logger.error(demarkation + "Exit Code: " + str(proc.returncode))
        return proc.returncode, stdout, stderr

    # Runs a command as current user on current host.
    # In Windows, use command prompt to run the command.
    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True, retry_count=0, retry_sleep_time=10):
        if not cmd.startswith("ssh -i") and not cmd.startswith("scp "):
            cmd = cls._buildcmd(cmd,
                          Machine.getAdminUser(),
                          Config.get("machine", "GATEWAY"),
                          Machine.getAdminPasswd(), env=env)
        for i in range(0, retry_count + 1):
            demarkation = "|GUID=" + str(uuid.uuid4()) + "|"
            cmd = cls._decoratedcmd(cmd)
            logger.info(demarkation + "RUNNING: " + cmd)
            stdout = ""
            osenv = None
            if env:
                osenv = os.environ.copy()
                for key, value in env.items():
                    osenv[key] = value
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, cwd=cwd, env=osenv
            )
            while proc.poll() is None:
                stdoutline = proc.stdout.readline()
                if stdoutline:
                    stdout += stdoutline
                    if logoutput:
                        logger.info(demarkation + stdoutline.strip())
            remaining = proc.communicate()
            remaining = remaining[0].strip()
            if remaining != "":
                stdout += remaining
                if logoutput:
                    for line in remaining.split("\n"):
                        logger.info(demarkation + line.strip())

            stdout = REC.sub("", stdout, 1)
            # QE-9058: Handle MOTD message in stdout or QE-199 [Monarch Jira]
            if 'This is MOTD message' in stdout or 'Inappropriate ioctl for device' in stdout \
                    or 'coveragedata.CoverageDataFileHandler' in stdout \
                    or 'Configuration ha.zookeeper.quorum is missing.' in stdout:
                STRINGS_TO_IGNORE = []
                STRINGS_TO_IGNORE.append('######## Hortonworks #############')
                STRINGS_TO_IGNORE.append('This is MOTD message, added for testing in qe infra')
                STRINGS_TO_IGNORE.append('mesg: ttyname failed: Inappropriate ioctl for device')
                STRINGS_TO_IGNORE.append('coveragedata.CoverageDataFileHandler')
                STRINGS_TO_IGNORE.append('Configuration ha.zookeeper.quorum is missing.')
                tmpResult = stdout.split("\n")
                if 'coveragedata.CoverageDataFileHandler' in stdout:
                    stdout = "\n".join(util.prune_output(tmpResult, STRINGS_TO_IGNORE, prune_empty_lines=False))
                else:
                    stdout = "\n".join(util.prune_output(tmpResult, STRINGS_TO_IGNORE, prune_empty_lines=True))

            if proc.returncode == 0:
                logger.info(demarkation + "Exit Code: " + str(proc.returncode))
            else:
                logger.info(demarkation + "Exit Code: " + str(proc.returncode))

            if proc.returncode == 0 or proc.returncode == 23:
                break
            else:
                if (i == retry_count - 1 and retry_count > 0) or retry_count == 0:
                    logger.info("Command %s failed after %d retries ", cmd, retry_count)
                else:
                    time.sleep(retry_sleep_time)
        return proc.returncode, stdout

    @classmethod
    def runPython(cls, cmd, cwd=None, env=None, logoutput=True):
        '''
        Runs python scripts.
        Returns (exit_code, stdout).
        '''
        # some platforms might need to call python2.6 or python.
        cmd = "python %s" % cmd
        return cls.run(cmd, cwd, env, logoutput)

    @classmethod
    def runPythonAs(cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None):
        '''
        Runs python scripts.
        Returns (exit_code, stdout).
        '''
        # some platforms might need to call python2.6 or python.
        cmd = "python %s" % cmd
        return cls.runas(user, cmd, host, cwd, env, logoutput, passwd)

    #Run jar file by java command line with current user
    #Returns (exit_code, stdout)
    @classmethod
    def runJar(cls, jar, argList=None, cwd=None, env=None, logoutput=True):
        cmd = "java -jar " + jar
        for arg in argList:
            cmd += ' "' + arg + '"'
        return cls.run(cmd, cwd, env, logoutput)

    #Run jar file by java command line with given user
    #Returns (exit_code, stdout)
    @classmethod
    def runJarAs(cls, user, jar, argList=None, cwd=None, env=None, logoutput=True):
        cmd = os.path.join(Config.get("machine", "JAVA_HOME"), "bin", "java") + " -jar " + jar
        for arg in argList:
            cmd += ' ' + arg + ''
        return cls.runas(user, cmd, '', cwd, env, logoutput)

    @classmethod
    def _decoratedcmd(cls, cmd):
        return cmd

    # Gets actual command to run particular command with other user.
    @classmethod
    def sudocmd(cls, cmd, user, passwd=None, env=None, doEscapeQuote=True):
        pass

    @classmethod
    def _renamecmd(cls, src, dest, host=None):
        pass

    # Gets actual command to run particular command in target host.
    # Only called if running a command in other host.
    @classmethod
    def sshcmd(cls, cmd, host):
        pass

    @classmethod
    def chmod(cls, perm, filepath, recursive=False, user=None, passwd=None, host=None):
        pass

    @classmethod
    def winUtilsChmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None):
        pass

    # Returns a string of command.
    # Depending on target host/user, sudocmd/sshcmd functions are used to get the command.
    @classmethod
    def _buildcmd(cls, cmd, user, host, passwd=None, env=None, doEscapeQuote=True):
        """
        BaseMachine._buildcmd

        :param cmd:
        :param user:
        :param host:
        :param passwd:
        :param env:
        :param doEscapeQuote:
        :return:
        """
        if host is None:
            host = Config.get("machine", "GATEWAY")
        if not isLoggedOnUser(user):
            cmd = cls.sudocmd(cmd, user, passwd, env=env, doEscapeQuote=doEscapeQuote)
        if not cls.isSameHost(host):
            cmd = cls.sshcmd(cmd, host)
        return cmd

    # Runs a command. The actual command to be run is from _buildcmd function.
    @classmethod
    def runas(
            cls,
            user,
            cmd,
            host="",
            cwd=None,
            env=None,
            logoutput=True,
            passwd=None,
            doEscapeQuote=True,
            retry_count=0,
            retry_sleep_time=10
    ):
        """
        BaseMachine.runas

        :param user:
        :param cmd:
        :param host:
        :param cwd:
        :param env:
        :param logoutput:
        :param passwd:
        :param doEscapeQuote:
        :return:
        """
        return cls.run(
            cls._buildcmd(cmd, user, host, passwd, env=env, doEscapeQuote=doEscapeQuote), cwd, env, logoutput,
            retry_count, retry_sleep_time
        )

    # Runs a command. The actual command to be run is from _buildcmd function.
    @classmethod
    def runasSuccessfully(
            cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None, doEscapeQuote=True
    ):
        """
        Similar to Machine.runas() + assert that it was successful & return only stdout.
        """
        exit_code, stdout = cls.runas(user, cmd, host, cwd, env, logoutput, passwd, doEscapeQuote)
        message = "Command failed: cmd = %s user = %s host = %s cwd= %s env=%s logoutput=%s passwd=%s" \
                 " doEscapeQuote=%s exit_code = %s stdout = %s" %\
                  (cmd, user, host, cwd, env, logoutput, passwd, doEscapeQuote, exit_code, stdout)
        if exit_code != 0:
            logger.error(message)
        assert exit_code == 0, message
        return stdout

    @classmethod
    def runexas(cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None):
        return cls.runex(cls._buildcmd(cmd, user, host, passwd, env=env), cwd, env, logoutput)

    # Runs given command in background.
    # Returns subprocess.Popen object.
    # Be cautious to use stdout=subprocess.PIPE. If there are too many output, buffer can block child process progress.
    # communicate() cannot be used to read stdout of running process.
    @classmethod
    def runinbackground(cls, cmd, cwd=None, env=None, stdout=None, stderr=None):
        logger.info("RUNNING IN BACKGROUND: %s", cmd)
        osenv = None
        if env:
            osenv = os.environ.copy()
            for key, value in env.items():
                osenv[key] = value

        # null = open(os.devnull, 'w')
        process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, shell=True, cwd=cwd, env=osenv)
        return process

    @classmethod
    def runinbackgroundAs(cls, user, cmd, host="", cwd=None, env=None, stdout=None, stderr=None):
        return cls.runinbackground(cls._buildcmd(cmd, user, host), cwd, env, stdout=stdout, stderr=stderr)

    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest, passwd=None):
        pass

    @classmethod
    def _rmcmd(cls, filepath, isdir=False, user=None, host=None):
        pass

    @classmethod
    def isIBMPowerPC(cls):
        lscpuCmd = "lscpu"
        _exit_code, stdout = cls.run(lscpuCmd)
        return bool('ppc64' in stdout)

    @classmethod
    def isOpenstackWithDeployNG(cls):
        is_cluster_need_restart_command = "cat /etc/openstack_deploy_ng"
        try:
            _exit_code, stdout = cls.run(is_cluster_need_restart_command)
            return bool('restart_supported' in stdout)
        except Exception:
            logger.info("could find the file hence returning false")
            return False

    @classmethod
    def getRelease(cls):
        if cls._os_release != None:
            return cls._os_release
        if os.path.exists("/etc/redhat-release"):
            catcmd = "cat /etc/redhat-release"
            cls._os_release = cls.run(catcmd)
        elif os.path.exists("/etc/SuSE-release"):
            catcmd = "cat /etc/SuSE-release"
            cls._os_release = cls.run(catcmd)
        elif os.path.exists("/etc/debian_version"):
            catcmd = "cat /etc/debian_version"
            cls._os_release = cls.run(catcmd)
            cls._os_release = 'Debian ' + str(cls._os_release)
        elif os.path.exists("/etc/system-release"):
            catcmd = "cat /etc/system-release"
            cls._os_release = cls.run(catcmd)
        elif os.path.exists("/etc/lsb-release"):
            catcmd = "cat /etc/lsb-release"
            cls._os_release = cls.run(catcmd)
        else:
            cls._os_release = "It is not SuSE, Rehat, Debian, Ubuntu or CentOS"
        return cls._os_release

    @classmethod
    def getAdminUser(cls):
        if not cls._adminUser:
            if Config.hasOption('machine', 'ROOT_USER'):
                cls._adminUser = Config.get('machine', 'ROOT_USER')
            else:
                cls._adminUser = cls._defaultAdminUser
        return cls._adminUser

    @classmethod
    def getAdminPasswd(cls):
        if not cls._adminPasswd:
            if Config.hasOption('machine', 'ROOT_PASSWD'):
                cls._adminPasswd = Config.get('machine', 'ROOT_PASSWD')
            elif Config.hasOption('machine', 'DEFAULT_PASSWD'):
                cls._adminPasswd = Config.get('machine', 'DEFAULT_PASSWD')
        return cls._adminPasswd

    @classmethod
    def getInstaller(cls):
        if Config.hasOption('machine', 'RUN_INSTALLER'):
            return Config.get('machine', 'RUN_INSTALLER')
        else:
            return None

    @classmethod
    def rm(cls, user, host, filepath, isdir=False, passwd=None):
        '''
        Removes a file or directory.
        Returns None always.
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd
        try:
            if isLoggedOnUser(user) and cls.isSameHost(host):
                if isdir:
                    shutil.rmtree(filepath, ignore_errors=True)
                else:
                    os.remove(filepath)
            else:
                cls.runas(user, cls._rmcmd(filepath, isdir=isdir, user=user, host=host), host=host, passwd=passwd)
        except Exception:
            pass

    @classmethod
    def rename(cls, user, host, src, dest, passwd=None):
        if isLoggedOnUser(user) and cls.isSameHost(host):
            os.rename(src, dest)
        else:
            cls.runas(user, cls._renamecmd(src, dest, host), passwd=passwd)

    #Returns True if path does exist.
    @classmethod
    def pathExists(cls, user, host, filepath, passwd=None):
        if isLoggedOnUser(user) and cls.isSameHost(host):
            return os.path.exists(filepath)
        else:
            return cls._pathExists(user, host, filepath, passwd=passwd)

    @classmethod
    def makedirs(cls, user, host, filepath, passwd=None):
        '''
        Makes a directory owned by user.
        Returns True if making directory succeeds or the directory already exists.
        '''
        if not cls.pathExists(user, host, filepath, passwd):
            if isLoggedOnUser(user) and cls.isSameHost(host):
                try:
                    logger.info("Creating directory %s locally", filepath)
                    os.makedirs(filepath)
                except OSError:
                    return False
                return True
            else:
                exit_code = 255
                count = 0
                # Adding retry logic to handle ssh failures while doing mkdir for log collection
                while exit_code == 255 and count < 10:
                    logger.info("Creating directory %s on %s with %s, attempt:%s", filepath, host, user, count)
                    (exit_code, _stdout) = cls._makedirs(user, host, filepath, passwd=passwd)
                    if exit_code != 255:
                        break
                    count += 1
                    time.sleep(5)
                if cls.pathExists(user, host, filepath, passwd):
                    return True
                else:
                    logger.info("Path %s does not exist at %s after creation.", filepath, host)
                    return False
        else:
            return True
        return None

    @classmethod
    def copyToLocal(cls, user, host, srcpath, destpath, passwd=None):
        cls._performcopy(user, host, srcpath, destpath, localdest=True, passwd=passwd)

    @classmethod
    def copyFromLocal(cls, user, host, srcpath, destpath, passwd=None):
        cls._performcopy(user, host, srcpath, destpath, localdest=False, passwd=passwd)

    @classmethod
    def _performcopy(cls, user, host, srcpath, destpath, localdest, passwd=None):
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if isLoggedOnUser(user) and cls.isSameHost(host):
            cls.copy(srcpath, destpath)
        else:
            cls.runas(user, cls._copycmd(user, host, srcpath, destpath, localdest, passwd=passwd), passwd=passwd)

    @classmethod
    def _makedirs(cls, user, host, filepath, passwd=None):
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd
        logger.info(host)
        cmd = "mkdir -p %s " % (filepath)
        return cls.runas(user, cmd, host, cwd=None, env=None, logoutput=True, passwd=passwd)

    @classmethod
    def copy(cls, srcpath, destpath, user=None, passwd=None):
        '''
        Copies files locally from source path to destination path.
        Returns None.
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if isLoggedOnUser(user):
            if os.path.isdir(srcpath):
                shutil.copytree(srcpath, destpath)
            else:
                logger.info("local copy from %s to %s", srcpath, destpath)
                shutil.copy(srcpath, destpath)
        else:
            cls.runas(user, cls._copycmd(user, None, srcpath, destpath, localdest=False, passwd=passwd), passwd=passwd)

    @classmethod
    def find_checksum(cls, node, user, filename):
        """
        Find checksum of filename on node
        Pass user = root, for using Admin User and pass user = hdfs, for using HDFS_USER
        """
        command = "md5sum " + filename
        if user == "root":
            exit_code, stdout = cls.runas(cls.getAdminUser(), command, node, None, None, "True", cls.getAdminPasswd())
        if user == "hdfs":
            exit_code, stdout = cls.runas(Config.get('hadoop', 'HDFS_USER'), command, node, None, None, "True", None)
        assert exit_code == 0, "Cant find checksum"
        myoutput = stdout.split("\n")
        finaloutput = ""
        for line in myoutput:
            if line.find("Warning") >= 0 or line.find("is not a tty") >= 0:
                logger.info("skip this line: %s", line)
            else:
                finaloutput = finaloutput + line
        checksum = finaloutput.split(" ")
        logger.info("Checksum of %s is : %s", filename, checksum[0])
        return checksum[0]

    @classmethod
    def skipMsg(cls, stdout, searchStr):
        lines = stdout.split("\n")
        final_output = ""
        for line in lines:
            if line.find(searchStr) >= 0:
                print "skipping %s messages" % searchStr
            else:
                final_output = final_output + "\n" + line
        return final_output.strip()

    @classmethod
    def skipWarnMsg(cls, stdout):
        lines = stdout.split("\n")
        final_output = ""
        for line in lines:
            if (line.find("WARN") >= 0 or line.find("Warning:") >= 0 or line.find("stdin:") >= 0
                    or line.find("stty:") >= 0):
                print "skiping warning messages"
            else:
                final_output = final_output + "\n" + line
        return final_output.strip()

    @classmethod
    def skipAzureWarnMsg(cls, stdout):
        lines = stdout.split("\n")
        final_output = ""
        for line in lines:
            if (line.find("azurefs2 has a full queue and can't consume the given metrics") >= 0
                    or line.find("WARN azure.AzureFileSystemThreadPoolExecutor: Disabling threads for ") >= 0
                    or line.find("INFO azure.AzureFileSystemThreadPoolExecutor: Time taken for ") >= 0
                    or line.find("WARN impl.MetricsSystemImpl: Sink sink2 already exists!") >= 0
                    or line.find("INFO util.log: Logging initialized") >= 0):
                print "skiping azure full queue warning messages"
            else:
                final_output = final_output + "\n" + line
        return final_output.strip()

    @classmethod
    def grep(cls, user, host, filepath, searchstr, regex=True, passwd=None):
        pass

    @classmethod
    def find(cls, user, host, filepath, searchstr, passwd=None, logoutput=False):
        pass

    @classmethod
    def service(cls, sname, action, user=None, host=None, passwd=None, options=""):
        pass

    @classmethod
    def stopService(cls, sname, user=None, host=None, passwd=None):
        return cls.service(sname, "stop", user=user, host=host, passwd=passwd)

    @classmethod
    def startService(cls, sname, user=None, host=None, passwd=None):
        return cls.service(sname, "start", user=user, host=host, passwd=passwd)

    @classmethod
    def getProcessList(cls, format=None):  # pylint: disable=redefined-builtin
        pass

    @classmethod
    def _pathExists(cls, user, host, filepath, passwd=None):
        pass

    @classmethod
    def getProcessListRemote(cls):
        pass

    @classmethod
    def sendSIGSTOP(cls, user, host, pid):
        pass

    @classmethod
    def sendSIGCONT(cls, user, host, pid):
        pass

    @classmethod
    def getTempDir(cls):
        return cls._tmpDir

    @classmethod
    def findProcess(cls, cmd):
        plist = cls.getProcessList(format="%a")
        for pitem in plist:
            if pitem.find(cmd) != -1:
                return pitem
        return ""

    @classmethod
    def getPIDByPort(cls, port):
        pass

    @classmethod
    def killProcess(cls, pid):
        pass

    @classmethod
    def killProcessRemote(cls, pid, host=None, user=None, passwd=None, logoutput=True):
        pass

    # Create a tar archive
    # mode - For no compression use 'w:', gzip use 'w:gz', bzip2 compression use 'w:bz2'
    # filepath - Tar file path
    # outpath - Path to extract the archive to
    @classmethod
    def tarCreateAdd(cls, filepath, inputpaths=None, mode='w'):
        if not inputpaths:
            inputpaths = []
        import tarfile
        tar = tarfile.open(filepath, mode)
        for fileName in inputpaths:
            tar.add(fileName)
        tar.close()

    # Extract a tar archive
    # mode - For no compression use 'r:', gzip use 'r:gz', bzip2 compression use 'r:bz2'
    # filepath - Tar file path
    # outpath - Path to extract the archive to
    @classmethod
    def tarExtractAll(cls, filepath, outpath, mode='r'):
        import tarfile
        tar = tarfile.open(filepath, mode)
        tar.extractall(path=outpath)
        tar.close()

    # Extract a zip
    # filepath - full path to the zip
    # outpath - where to unzip the file
    @classmethod
    def unzipExtractAll(cls, filepath, outpath):
        import zipfile
        zip_ = zipfile.ZipFile(filepath)
        zip_.extractall(path=outpath)
        zip_.close()

    @classmethod
    def getfqdn(cls, name=""):
        pass

    # Check whether the hosts are same
    # If using with one parameter, it is asking if the host is local host.
    # If using with 2 parameters, it is asking if two hosts are the same. Be careful if passing None.
    # host1 - remote host, can take in hostname, fqdn, ipaddress
    # host2 - another host, not specifying it will assume itself
    @classmethod
    def isSameHost(cls, host1, host2=""):
        '''
        Checks if machines are same host or not.
        Strange thing is, if host 1 is None, it is always True.
        '''
        return not host1 or host1 == "" or cls.getfqdn(host1) == cls.getfqdn(host2)

    @classmethod
    def sed(cls, user, host, filepath, sedfilepath, outfilepath=None, passwd=None, logoutput=False):
        pass

    @classmethod
    def getPrivilegedUser(cls):
        pass

    @classmethod
    def getServiceKeytabsDir(cls):
        return '/etc/security/keytabs'

    @classmethod
    def isADMIT(cls):
        my_user_realm = ""
        my_user_realm = cls.get_user_realm()
        return bool(my_user_realm == "HWQE.HORTONWORKS.COM")

    @classmethod
    def isWindows(cls):
        return Machine.type() == 'Windows'

    @classmethod
    def isLinux(cls):
        return not cls.isWindows()

    @classmethod
    def isSuse(cls):
        return 'suse' in cls._os_name

    @classmethod
    def isSuse11(cls):
        if cls.isSuse():
            _, rel = cls.getRelease()
            return 'VERSION = 11' in rel
        return False

    @classmethod
    def isSuse12(cls):
        if cls.isSuse():
            _, rel = cls.getRelease()
            return 'VERSION = 12' in rel
        return False

    @classmethod
    def isCentOs(cls):
        return cls._os_name.find("centos") >= 0

    @classmethod
    def isAmazonLinux(cls):
        rel = cls.getRelease()[1]
        return bool(rel.find("Amazon Linux") >= 0)

    @classmethod
    def isCloud(cls):
        return cls.isAmazonLinux()

    @classmethod
    def isCentOs5(cls):
        if cls.isCentOs():
            rel = cls.getRelease()[1]
            return bool(rel.find("CentOS release 5") >= 0)
        else:
            return False

    @classmethod
    def isCentOs6(cls):
        if cls.isCentOs():
            rel = cls.getRelease()[1]
            return bool(rel.find("CentOS release 6") >= 0)
        else:
            return False

    @classmethod
    def isCentOs7(cls):
        if cls.isCentOs():
            rel = cls.getRelease()[1]
            return bool(rel.find("Linux release 7") >= 0)
        else:
            return False

    @classmethod
    def isIBMPower(cls):
        return cls._machine_name.find("ppc") >= 0

    @classmethod
    def isDebian(cls):
        return cls._os_name.find("debian") >= 0

    @classmethod
    def isDebian7(cls):
        from decimal import Decimal
        try:
            version = Decimal(platform.dist()[1])
            return bool(version >= 7)
        except Exception:
            return False

    @classmethod
    def isDebian9(cls):
        from decimal import Decimal
        try:
            version = Decimal(platform.dist()[1])
            return bool(version >= 9)
        except Exception:
            return False

    @classmethod
    def isUbuntu(cls):
        return cls._os_name.find("ubuntu") >= 0

    @classmethod
    def isUbuntu14(cls):
        if cls.isUbuntu():
            version = platform.dist()[1].split(".")[0]
            return bool(version == "14")
        return None

    @classmethod
    def isUbuntu16(cls):
        if cls.isUbuntu():
            version = platform.dist()[1].split(".")[0]
            return bool(version == "16")
        return None

    @classmethod
    def isRedHat(cls):
        return cls._os_name == 'redhat'

    @classmethod
    def isRedHat7(cls):
        if cls.isRedHat():
            rel = cls.getRelease()[1]
            return bool(rel.find("Linux Server release 7") >= 0)
        else:
            return False

    @classmethod
    def isFlubber(cls):
        myHost = socket.gethostname()
        logger.info(myHost)
        return bool(myHost.find("ygridcore.net") >= 0)

    @classmethod
    def isNano(cls):
        '''
      Determines if the current machine is on nano.
      :return: Bool success status.
      '''
        myHost = socket.getfqdn()
        logger.info(myHost)
        return bool(myHost.find("cs1cloud.internal") >= 0)

    @classmethod
    def isEC2(cls):
        '''
      Determines if the current machine is on ec2.
      :return: Bool success status.
      '''
        myHost = socket.getfqdn()
        logger.info(myHost)
        return bool(myHost.find("ec2.internal") >= 0)

    @classmethod
    def isOpenStack(cls):
        '''
      Determines if the current machine is on OpenStack.
      :return: Bool success status.
      '''
        myHost = socket.getfqdn()
        logger.info(myHost)
        #return myHost.endswith(".hw.local")
        return myHost.endswith(".novalocal") or myHost.endswith(".openstacklocal")

    @classmethod
    def isYCloud(cls):
        '''
      Determines if the current machine is on yCloud.
      :return: Bool success status
      '''
        myHost = socket.getfqdn()
        logger.info(myHost)

        return myHost.endswith("hwx.site")

    _isHumboldt = None

    @classmethod
    def isHumboldt(cls):
        '''
      Determines if the current machine is on humboldt.
      :return: Bool success status.
      '''
        if cls._isHumboldt is not None:
            return cls._isHumboldt
        if cls.isLinux():
            myHost = socket.getfqdn()
            if util.isIP(myHost):
                myHost = socket.gethostname()
            logger.info(myHost)
            cls._isHumboldt = bool(
                cls.isUbuntu()
                and (myHost.startswith("headnode") or myHost.startswith("hn0") or myHost.startswith("hn1"))
            )
        else:
            cls._isHumboldt = False
        return cls._isHumboldt

    @classmethod
    def isCabo(cls):
        '''
        Determines if the current machine is on ADLS. Cabo is now deprecated.
        :return: bool success status.
        '''
        if cls.isWindows():
            myHost = socket.getfqdn()
            logger.info(myHost)
            return bool(myHost.lower().find("adl") > -1)
        else:
            return False

    @classmethod
    def isSuse_12(cls):
        '''
        Returns True if the machine has Suse12,
        else returns False
        '''
        if os.path.exists("/etc/SuSE-release"):
            exitCode, stdout = cls.cat(path="/etc/SuSE-release")
            assert exitCode == 0, "Cant read /etc/SuSE-release"
            version = None
            patch = None
            version = re.search("VERSION = (.*)", stdout)
            patch = re.search("PATCHLEVEL = (.*)", stdout)

            if version and patch:
                return bool(version.group(1) == '12')
        return None

    @classmethod
    def isSuse_11_3(cls):
        '''
        Returns True if the machine has Suse11.3,
        else returns False
        '''
        if os.path.exists("/etc/SuSE-release"):
            exitCode, stdout = cls.cat(path="/etc/SuSE-release")
            assert exitCode == 0, "Cant read /etc/SuSE-release"
            version = None
            patch = None
            version = re.search("VERSION = (.*)", stdout)
            patch = re.search("PATCHLEVEL = (.*)", stdout)

            if version and patch:
                return bool(version.group(1) == '11' and patch.group(1) == '3')
        return None

    @classmethod
    def getOS(cls):
        '''
        Get OS name
        :return:
        '''
        if cls.isCentOs5():
            return "centos5"
        elif cls.isCentOs6():
            return "centos6"
        elif cls.isIBMPower():
            return "centos7-ppc"
        elif cls.isCentOs7():
            return "centos7"
        elif cls.isUbuntu():
            if cls.isUbuntu16():
                return "ubuntu16"
            elif cls.isUbuntu14():
                return "ubuntu14"
            else:
                return "ubuntu14"
        elif cls.isSuse_11_3():
            return "suse113"
        elif cls.isSuse_12():
            return "sles12"
        elif cls.isDebian():
            if cls.isDebian9():
                return "debian9"
            elif cls.isDebian7():
                return "debian7"
            else:
                #Since hdp3 no longer support debian7, assigning the default value to debian9
                return "debian9"
        elif cls.isAmazonLinux():
            return "amazonlinux2"
        else:
            return None

    @classmethod
    def isMoonCake(cls):
        if Machine.isWindows() and Config.get('machine', 'PLATFORM') == 'ASV':
            return bool(os.getenv('_JAVA_OPTIONS') == "-Dfile.encoding=UTF-8")
        else:
            return False

    @classmethod
    def is_upgraded_cluster(cls):
        """
        API to check if its a upgrade cluster
        :return:
        """
        DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
        uifrm_folder = "uifrm_old/uifrm"
        amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder, 'ambari.properties')
        if not os.path.isfile(amb_prop_file):
            uifrm_folder = "uifrm"
        amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder)
        from beaver.component.ambari_apilib import APICoreLib
        apicorelib = APICoreLib(amb_prop_file)
        return apicorelib.get_last_upgrade_status() == "COMPLETED"

    @classmethod
    def getHttpsURLContent(cls, url, outputfile=""):
        '''
      Same as util.getURLContent for HTTPS.
      Specifically works for Ubuntu Nano env
      It gets the content of URL using wget and save it in outputfile if requiered
      '''
        if outputfile == "":
            tmp_output = os.path.join(cls.getTempDir(), "tmpUI")
        else:
            tmp_output = outputfile
        cmd = "wget --no-check-certificate --secure-protocol=%s --output-document=%s %s" % (
            SECURE_PROTOCOL, tmp_output, url
        )
        exit_code, std_out = cls.runas(None, cmd, None, None, None, True, None)
        logger.info("Command '%s', exit_code: %d, std_out: '%s'", cmd, exit_code, std_out)
        f = open(tmp_output, "r")
        return f.read()

    @classmethod
    def type(cls):
        return "Base"

    @classmethod
    def getDBVersion(cls, dbType, host):
        pass

    @classmethod
    def getExportCmd(cls):
        '''
        Return either "export" or "set".
        '''
        pass

    @classmethod
    def du(cls, dirPath, logoutput=True):
        return cls.run("du -b %s" % dirPath, cwd=None, env=None, logoutput=logoutput)

    @classmethod
    def getProcessMemory(cls, pid, host=None, logoutput=True):
        pass

    @classmethod
    def cat(cls, host=None, path='/', logoutput=True):
        pass

    @classmethod
    def copyFileFromLocalToRemoteTmpDir(
            cls, host, localPath, remoteFileName=None, madeExecutable=True, appendTimeToRemotePath=True, logoutput=True
    ):
        '''
        Copies a file from local host to remote temp directory.
        Returns a String (full path of remote file).
        '''
        if remoteFileName is None:
            fileNameEntries = localPath.split(os.sep)
            remoteFileName = fileNameEntries[-1]
        remoteFilePath = cls.getTempDir() + os.sep + remoteFileName
        if appendTimeToRemotePath:
            #directory must have no "."
            fileNameEntries = remoteFilePath.split('.')
            remoteFilePath = '.'.join(fileNameEntries[0:-1]) + "_" + str(int(time.time())) + \
                             '.' + fileNameEntries[-1]
        Machine.copyFromLocal(user=None, host=host, srcpath=localPath, destpath=remoteFilePath, passwd=None)
        if madeExecutable:
            Machine.chmod(
                perm="777",
                filepath=remoteFilePath,
                recursive="False",
                user=Machine.getAdminUser(),
                host=host,
                passwd=Machine.getAdminPasswd()
            )
        if logoutput:
            logger.info("copyFileFromLocalToRemoteTmpDir returns %s", remoteFilePath)
        return remoteFilePath

    @classmethod
    def tail(cls, filename, user=None, passwd=None, host=None, args=None, env=None, logoutput=True):
        '''
        Runs tail command.
        implemented in overridden method
        '''
        pass

    @classmethod
    def getJDKWindows(cls):
        javaHome = Config.get('machine', 'JAVA_HOME')
        # if no java home is found return n/a
        if not javaHome:
            return 'n/a'

        # run java -version command
        rc, stdout = Machine.run(os.path.join(javaHome, 'bin', 'java') + ' -version')
        # if the cmd fails return n/a
        if rc != 0:
            return 'n/a'

        p = re.compile('^java version \"(.*)\"', re.M)
        m = p.search(stdout)
        if m and m.group(1):
            return 'oracle-' + m.group(1)

        p = re.compile('^openjdk version \"(.*)\"', re.M)
        m = p.search(stdout)
        if m and m.group(1):
            return 'openjdk-' + m.group(1)

        return 'n/a'

    @classmethod
    def getJDK(cls):
        if Machine.type() == "Windows":
            return cls.getJDKWindows()

        javaHome = Config.get('machine', 'JAVA_HOME')
        # if no java home is found return n/a
        if not javaHome:
            return 'n/a'

        # run java -version command
        rc, stdout = Machine.run(os.path.join(javaHome, 'bin', 'java') + ' -version')
        # if the cmd fails return n/a
        if rc != 0:
            return 'n/a'

        jdkVersion = ''
        jdkType = 'oracle'
        p = re.compile('^java version \"(.*)\"', re.M)
        m = p.search(stdout)
        # if the pattern is matched set the version
        # else return n/a
        if m and m.group(1):
            jdkVersion = m.group(1)
            return jdkType + '-' + jdkVersion

        p = re.compile('^OpenJDK', re.I | re.M)
        if re.search(p, stdout):
            jdkType = 'openjdk'
            # openjdk8 prints version like openjdk version "1.8.0_91"
            # if version is still empty lets try to find it
            if not jdkVersion or jdkVersion == '':
                p = re.compile('^openjdk version \"(.*)\"', re.I | re.M)
                m = p.search(stdout)
                if m and m.group(1):
                    jdkVersion = m.group(1)

        # determine if we are on open jdk
        return jdkType + '-' + jdkVersion

    @classmethod
    def resetTimeZone(cls, host=None):
        pass

    @classmethod
    def resetTimeZoneOnCluster(cls):
        if Machine.type() != "Windows" and not Machine.isFlubber():
            lines = [line.rstrip() for line in open('/tmp/all_nodes')]
            for host in lines:
                Machine.resetTimeZone(host=host)

    @classmethod
    def getTimeZone(cls, host):
        cmd = "date +%Z"
        exit_code, stdout = cls.runas(None, cmd, host, None, None, True, None)
        if exit_code == 0:
            tmp_stdout = cls.skipWarnMsg(stdout)
            return tmp_stdout
        else:
            return None

    @classmethod
    def validateTimeZoneSame(cls, host1, host2):
        tz1 = cls.getTimeZone(host1)
        tz2 = cls.getTimeZone(host2)
        if not tz1 or not tz2:
            return False
        return bool(tz1.strip() == tz2.strip())

    @classmethod
    def getWrappedServicePIDRemote(cls, service):
        '''
        BaseMachine method
        '''
        pass

    @classmethod
    def getJavaHome(cls, logoutput=False):  # pylint: disable=unused-argument
        '''
        Gets JAVA_HOME.
        '''
        return Config.get('machine', 'JAVA_HOME')

    @classmethod
    def ls(cls, path, user=None, passwd=None, host=None, args=None, env=None, logoutput=True):
        '''
        Runs ls command.
        implemented in overridden method
        '''
        pass

    @classmethod
    def checkCGroupsSupport(cls):
        '''
        implemented in overridden method
        '''
        pass

    @classmethod
    def getFreeDiskSpace(cls):
        pass

    @classmethod
    def modify_remote_file(cls, host, filename, worker_func):
        '''
        Made any required change on a remote file with custom defined callable

        This function copies remote file locally to a temporary file. After that it calls the worker_func
        with the local filename.
        When the worker_func finished remote file replaced with the modified one and the temporary file removed
        :param host: remote hostname
        :param filename: remote file location
        :param worker_func: callable with one parameter which is the temporary filepath
        :return:
        '''
        temp_id = str(int(round(time.time() * 1000)))
        temp_name = "policy-store-{}".format(temp_id)
        local_copy = os.path.join(Machine.getTempDir(), temp_name)
        cls.copyToLocal(Machine.getAdminUser(), host, filename, local_copy)
        cls.chmod("777", local_copy, user=cls.getAdminUser(), passwd=cls.getAdminPasswd())
        worker_func(local_copy)
        cls.copyFromLocal(Machine.getAdminUser(), host, local_copy, filename)
        cls.rm(Machine.getAdminUser(), Machine.getfqdn(), local_copy)


class LinuxMachine(BaseMachine):
    _kinitloc = None
    _kdestroyloc = None
    _defaultAdminUser = "root"
    _tmpDir = "/tmp"
    # QE-3878: get the keypair from the config
    _privileged_key_file = None
    _scp_cmd = "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r"

    # all messages we can ignore when doing ssh or remote calls
    STRINGS_TO_IGNORE = []
    STRINGS_TO_IGNORE.append('Warning: ')
    STRINGS_TO_IGNORE.append('stdin: is not a tty')
    STRINGS_TO_IGNORE.append('The system cannot find the file specified')
    STRINGS_TO_IGNORE.append('No such file or directory')
    STRINGS_TO_IGNORE.append('######## Hortonworks #############')
    STRINGS_TO_IGNORE.append('This is MOTD message, added for testing in qe infra')
    STRINGS_TO_IGNORE.append('Configuration ha.zookeeper.quorum is missing.')
    #"""Linux machine"""
    # Gets actual command to run particular command with other user.
    # Overridden method
    @classmethod
    def sudocmd(cls, cmd, user, passwd=None, env=None, doEscapeQuote=True):
        runCmd = ''
        if env:
            for key in env.keys():
                runCmd += 'export %s=%s;' % (key, env[key])
        if doEscapeQuote:
            runCmd += cmd.replace('"', '\\"')
        else:
            runCmd += cmd
        return "sudo su - -c \"%s\" %s" % (runCmd, user)

    # Gets actual command to run particular command in target host.
    # Only called if running a command in other host.
    # Overridden method
    @classmethod
    def sshcmd(cls, cmd, host):
        host = socket.getfqdn(host.strip())
        keypair = Config.get('machine', 'KEY_PAIR_FILE', None)
        # escale all double quotes
        return "ssh -i " + keypair + " -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s \"%s\"" % (
            host, cmd.replace('"', '\\"')
        )

    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest, passwd=None):
        privileged_scp_cmd = cls._scp_cmd
        # if key file is not set read it and set it.
        if not cls._privileged_key_file:
            cls._privileged_key_file = Config.get('machine', 'KEY_PAIR_FILE', None)

        # only update the privileged command if key exists
        if cls._privileged_key_file and cls._privileged_key_file != '':
            privileged_scp_cmd += ' -i %s' % cls._privileged_key_file

        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if host is None:
            return "cp -r %s %s" % (srcpath, destpath)
        elif localdest:
            if user == "root":
                return "%s %s@%s:%s %s" % (privileged_scp_cmd, user, host, srcpath, destpath)
            else:
                return "%s %s@%s:%s %s" % (cls._scp_cmd, user, host, srcpath, destpath)
        else:
            if user == "root":
                return "%s %s %s@%s:%s" % (privileged_scp_cmd, srcpath, user, host, destpath)
            else:
                return "%s %s %s@%s:%s" % (cls._scp_cmd, srcpath, user, host, destpath)

    @classmethod
    def echocmd(cls, text):
        return "echo \"%s\"" % text

    @classmethod
    def chmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None, logoutput=True):
        '''
        Linux chmod.
        Returns (exit_code, stdout).
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        cmd = "chmod "
        if recursive:
            cmd += "-R "
        cmd += perm + " " + filepath
        return cls.runas(user, cmd, host=host, logoutput=logoutput)

    @classmethod
    def winUtilsChmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None):
        pass

    @classmethod
    def _rmcmd(cls, filepath, isdir=False, user=None, host=None):
        cmd = "rm "
        if isdir:
            cmd += "-rf "
        cmd += filepath
        return cmd

    @classmethod
    def _pathExists(cls, user, host, filepath, passwd=None):
        exit_code, _stdout = cls.runas(user, "ls " + filepath, host=host)
        return exit_code == 0

    @classmethod
    def grep(cls, user, host, filepath, searchstr, regex=True, passwd=None, logoutput=False):
        cmd = "grep "
        if regex:
            cmd += "-E "
        cmd += "'" + searchstr + "' " + filepath
        return cls.runas(user, cmd, host=host, logoutput=logoutput)

    @classmethod
    def find(  # pylint: disable=redefined-builtin
            cls, user, host, filepath, searchstr, passwd=None, logoutput=False, modifiedWitninMin=0, type=None
    ):
        '''
        Overridden method

        Returns a list of file names matching with searchstr.
        searchstr is a parameter to find -name. Can be * or ?.
        '''
        cmd = "find -L %s -name %s" % (filepath, searchstr)
        if modifiedWitninMin != 0:
            cmd = cmd + " -mmin -%s" % (modifiedWitninMin)
        if type:
            cmd = cmd + " -type %s" % (type)
        _exit_code, stdout = cls.runas(user, cmd, host=host, logoutput=logoutput)

        tmpResult = stdout.split("\n")

        return util.prune_output(tmpResult, cls.STRINGS_TO_IGNORE, prune_empty_lines=True)

    @classmethod
    def find_path(  # pylint: disable=unused-argument
            cls, user, host, search_dir, path_pattern, passwd=None, logoutput=False
    ):
        '''
        Overridden method

        Returns a list of file names matching with searchstr.
        searchstr is a parameter to find -name. Can be * or ?.
        '''
        _exit_code, stdout = cls.runas(
            user, "find -L %s -path %s" % (search_dir, path_pattern), host=host, logoutput=logoutput
        )
        tmpResult = stdout.split("\n")
        return util.prune_output(tmpResult, cls.STRINGS_TO_IGNORE, prune_empty_lines=True)

    # Get list of running processes
    # Format - Process list format, default user pid ppid cmd
    # Returns a list in the specified format
    @classmethod
    def getProcessList(  # pylint: disable=redefined-builtin
            cls, format="%U %p %P %a", filter=None, exclFilters=None, logoutput=False
    ):
        cmd = "ps -eo \"%s\" --no-headers" % format
        _exit_code, stdout = cls.run(cmd, logoutput=logoutput)
        tmpResult = None
        if filter:
            tmpResult = [line for line in stdout.split("\n") if line.find(filter) != -1]
        else:
            tmpResult = stdout.split("\n")
        result = []
        if exclFilters is not None:
            for entry in tmpResult:
                for exclFilter in exclFilters:
                    matchExclFilter = False
                    if re.search(exclFilter, entry) is not None:
                        matchExclFilter = True
                        if logoutput:
                            logger.info("getProcessListRemote match exclusion filter=%s entry=%s", filter, entry)
                        break
                    if not matchExclFilter:
                        result.append(entry)
        else:
            result = tmpResult
        return result

    @classmethod
    def getProcessListRemote(  # pylint: disable=redefined-builtin
            cls, hostIP,
            format="%U %p %P %a",
            filter=None,
            user=None,
            logoutput=False,
            useEGrep=False,
            exclFilters=None
    ):
        """
        Returns a list of lines of output of ps command.
        Returns [''] if stdout is empty.
        :param hostIP:
        :param format:
        :param filter: grep pattern
        :param user:
        :param logoutput:
        :param useEGrep: use egrep
        :param exclFilters: python regex patterns for exclusion
        :return:
        """
        if filter is None:
            _exit_code, stdout = cls.runas(
                user, "ps -eo \"%s\" --no-headers" % (format), host=hostIP, logoutput=logoutput
            )
        else:
            if useEGrep:
                grep = "grep -Ei '%s'" % filter
            else:
                grep = "grep -i %s" % filter
            _exit_code, stdout = cls.runas(
                user, "ps -eo \"%s\" --no-headers | %s" % (format, grep), host=hostIP, logoutput=logoutput
            )
        tmpResult = stdout.split("\n")
        result = []
        for entry in tmpResult:
            if entry.find("grep") == -1 and re.search("Warning: Permanently added.*known hosts", entry) is None:
                matchExclFilter = False
                if exclFilters is not None:
                    for exclFilter in exclFilters:
                        if re.search(exclFilter, entry) is not None:
                            matchExclFilter = True
                            if logoutput:
                                logger.info("getProcessListRemote match exclusion filter=%s entry=%s", filter, entry)
                            break
                if not matchExclFilter:
                    result.append(entry)
        if result == ['']:
            logger.info("Warning: getProcessListRemote returns ['']")
        if logoutput:
            logger.info("getProcessListRemote returns %s", result)
        return result

    @classmethod
    def killProcess(cls, pid):
        return cls.run("kill -9 %d" % pid)

    @classmethod
    def getPidFromString(cls, pid_string, user=None, isPidStringWithoutuser=False):
        '''
        Find pid from "hrt_qa   28774 28772 nc -lk 9999"
        :param pid_string: "hrt_qa   28774 28772 nc -lk 9999"
        :return: returns PID (28774)
        '''
        if not user:
            user = Config.get('hadoop', 'HADOOPQA_USER')
        pattern = r"%s(\s)*([0-9]*).*" % user
        if isPidStringWithoutuser:
            pattern = r"^([0-9]*)\s*([0-9]*).*"
        m = re.search(pattern, pid_string)
        if m:
            return m.group(2)
        else:
            return None

    @classmethod
    def shutdownProcessRemote(cls, pid, host=None, user=None, passwd=None, logoutput=True):
        '''
        overridden method
        Kills a process with -15 option remotely/locally.
        Returns (exit_code, stdout).
        If user is None, use admin user.
        If host is None, use localhost.
        '''
        if user is None:
            user = Machine.getAdminUser()
        return cls.runas(user=user, cmd="kill -15 %d" % pid, host=host, logoutput=logoutput, passwd=passwd)

    @classmethod
    def killProcessRemote(cls, pid, host=None, user=None, passwd=None, logoutput=True):
        '''
        overridden method
        Kills a process remotely/locally.
        Returns (exit_code, stdout).
        If user is None, use admin user.
        If host is None, use localhost.
        '''
        if logoutput:
            logger.info("killProcessRemote pid=%s host=%s user=%s", pid, host, user)
        if user is None:
            user = Machine.getAdminUser()
        if isinstance(pid, str):
            pid = int(pid)
        return cls.runas(user=user, cmd="kill -9 %d" % pid, host=host, logoutput=logoutput, passwd=passwd)

    @classmethod
    def getPIDByPort(cls, port, user=None, host=None):
        pattern = re.compile(r"^tcp\d?\s+\d\s+\d\s+.*:%d.*LISTEN\s+(\d+)/\w" % port)
        _exit_code, stdout = cls.runas(user, "netstat -ntlp", host=host, logoutput=False)
        nslist = stdout.split("\n")
        for nsitem in nslist:
            match = pattern.match(nsitem)
            if match:
                return int(match.group(1))
        return None

    @classmethod
    def sendSIGSTOP(cls, user, host, pid):
        return cls.runas(user, "kill -SIGSTOP %s" % (pid), host)

    @classmethod
    def sendSIGCONT(cls, user, host, pid):
        return cls.runas(user, "kill -SIGCONT %s" % (pid), host)

    @classmethod
    def disconnectMachine(cls, hostmachine, port, blockedmachineip):
        '''
        Simulates connection error between two machines for perticular ports
        example: block "blockedmachineip" from connecting to "hostmachine" on "port"
        :param hostmachine: Machine who is Listening to X port
        :param port: Port needs to be blocked
        :param blockedmachineip: This machine will be blocked to talk to port from hostmachine
        :return:
        '''
        if not util.isIP(blockedmachineip):
            util.getIpAddress(hostname=blockedmachineip)
        cmd = "iptables -A INPUT -p tcp --dport %s -s %s -j DROP" % (port, blockedmachineip)
        return cls.runas(
            cls.getAdminUser(), cmd, host=hostmachine, cwd=None, env=None, logoutput=True, passwd=cls.getAdminPasswd()
        )

    @classmethod
    def revert_disconnectMachine(cls, hostmachine, port, blockedmachineip):
        '''
        Reverts connection error simulation between two machines for perticular ports
        :param hostmachine: Machine who is Listening to X port
        :param port: Port needs to be blocked
        :param blockedmachineip: This machine will be blocked to talk to port from hostmachine
        :return:
        '''
        if not util.isIP(blockedmachineip):
            util.getIpAddress(hostname=blockedmachineip)
        cmd = "iptables -D INPUT -p tcp --dport %s -s %s -j DROP" % (port, blockedmachineip)
        return cls.runas(
            cls.getAdminUser(), cmd, host=hostmachine, cwd=None, env=None, logoutput=True, passwd=cls.getAdminPasswd()
        )

    @classmethod
    def validate_disconnectRuleExists(cls, hostmachine, port, blockedmachineip):  # pylint: disable=unused-argument
        '''
        Validate if disconnect machine rule exists on host machine
        :param hostmachine: Machine who is Listening to X port
        :param port: Port needs to be blocked
        :param blockedmachineip: This machine will be blocked to talk to port from hostmachine
        :return:
        '''
        if not util.isIP(blockedmachineip):
            blockedmachineip = util.getIpAddress(hostname=blockedmachineip)
        cmd = "iptables -L"
        _exit_code, stdout = cls.runas(
            cls.getAdminUser(), cmd, host=hostmachine, cwd=None, env=None, logoutput=True, passwd=cls.getAdminPasswd()
        )
        for line in stdout.split("\n"):
            if "DROP" in line:
                if blockedmachineip in line or util.getShortHostnameFromIP(blockedmachineip) in line:
                    return True
        return False

    @classmethod
    def extractWarfile(cls, warfile, destdir, host):
        '''
        Extract tez ui war
        '''
        cls.makedirs(user=cls.getAdminUser(), host=host, filepath=destdir, passwd=cls.getAdminPasswd())
        cls.chmod(
            "777",
            destdir,
            recursive=True,
            user=cls.getAdminUser(),
            host=host,
            passwd=cls.getAdminPasswd(),
            logoutput=True
        )
        jarpath = os.path.join(Config.get("machine", "JAVA_HOME"), "bin", "jar")
        cmd = "cd " + destdir + "; " + jarpath + " xvf " + warfile
        cls.runas(user=None, cmd=cmd, host=host, logoutput=True)

    @classmethod
    def service(cls, sname, action, user=None, host=None, passwd=None, options=""):
        if not user:
            user = cls.getAdminUser()
        return cls.runas(user, "service %s %s" % (sname, action), host=host)

    @classmethod
    def type(cls):
        return 'Linux'

    @classmethod
    def which(cls, executable, user=None):
        if user is None:
            user = Config.getEnv('USER')
        exit_code, output = cls.runas(user, "which %s" % executable)
        if exit_code == 0:
            return "\n".join(util.prune_output(output.split("\n"), cls.STRINGS_TO_IGNORE, prune_empty_lines=True))
        return ""

    @classmethod
    def getKerberosTicket(cls, user=None, rmIfExists=False, kerbTicketFile=None):
        if user is None:
            user = Config.getEnv('USER')
        if cls._kinitloc is None:
            cls._kinitloc = cls.which("kinit", "root")
        kerbTicketDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'kerberosTickets')
        if not os.path.exists(kerbTicketDir):
            os.mkdir(kerbTicketDir)
            Machine.chmod(perm='777', filepath=kerbTicketDir)
        kerbTicket = os.path.join(
            kerbTicketDir, ("%s.kerberos.ticket" % user) if kerbTicketFile is None else kerbTicketFile
        )
        if Config.hasOption('machine', 'KEYTAB_FILES_DIR'):
            keytabFile = cls.getHeadlessUserKeytab(user)
            user_principal = cls.get_user_principal(user)
            try:
                from beaver.component.hadoop import YARN
                ATS_USER = YARN.YARN_ATS_USER
                if user == ATS_USER:
                    keytabFile = YARN.get_yarn_ats_keytab_file()
                    user_principal = YARN.get_yarn_ats_user_principal()
            except ImportError:
                logger.info("Yarn ATS user check failed")
                ATS_USER = "yarn-ats"

            if os.path.isfile(kerbTicket) and not rmIfExists:
                return kerbTicket
            cmd = "%s -c %s -k -t %s %s" % (cls._kinitloc, kerbTicket, keytabFile, user_principal)
            if user != ATS_USER:
                exit_code, output = Machine.run(cmd)
            else:
                exit_code, output = Machine.runas(user=cls.getAdminUser(), cmd=cmd)
                Machine.chmod(user=cls.getAdminUser(), perm='777', filepath=kerbTicket)
                logger.info("exit_code = %d output = %s", exit_code, output)
            if exit_code == 0:
                return kerbTicket
        return ""

    # method to return the keytab for a given user
    @classmethod
    def getHeadlessUserKeytab(cls, user):
        return os.path.join(Config.get('machine', 'KEYTAB_FILES_DIR'), "%s.headless.keytab" % user)

    # method to return service keytab
    @classmethod
    def getServiceKeytab(cls, service):
        return os.path.join(cls.getServiceKeytabsDir(), "%s.service.keytab" % service)

    # method to get the kinit cmd
    @classmethod
    def getKinitCmd(cls):
        if cls._kinitloc is None:
            cls._kinitloc = cls.which("kinit", "root")
        return cls._kinitloc

    # method to get the kdestroy cmd
    @classmethod
    def getKdestroyCmd(cls):
        if cls._kdestroyloc is None:
            cls._kdestroyloc = cls.which("kdestroy", "root")
        return cls._kdestroyloc

    @classmethod
    def getfqdn(cls, name=""):
        if name is None:
            name = ""
        host = None
        if name == "" and cls.isHumboldt():
            exit_code, stdout = cls.run("hostname -f")
            if exit_code == 0:
                host = stdout.strip()
        if not host:
            host = socket.getfqdn(name)
        return host

    @classmethod
    def filepathToURI(cls, *paths):
        return "file://" + "/".join(paths)

    @classmethod
    def sed(cls, user, host, filepath, sedfilepath, outfile=None, passwd=None, logoutput=False):
        cmd = "cat %s | sed -f %s " % (filepath, sedfilepath)
        if outfile:
            cmd = cmd + "> %s" % outfile
        return cls.runas(user, cmd, host=host, logoutput=logoutput)

    @classmethod
    def getPrivilegedUser(cls):
        # on linux world do this as the user running the tests
        return Config.getEnv('USER'), None

    # Adds a user into specified group. If group is None, group with the same name as user will be used.
    # Group is automatically added.
    # Returns True if useradd succeeds.
    @classmethod
    def addUser(cls, username, group=None, host=None):
        #add group
        if group is None:
            cls.addGroup(username, host=host)
        else:
            cls.addGroup(group, host=host)
        #add user into the group created earlier
        cmd = "useradd %s" % username
        if group is not None:
            cmd += " " + "-g " + group
        else:
            cmd += " " + "-g " + username
        (exit_code, _) = cls.runas("root", cmd, host=host)
        return exit_code == 0

    # Adds a group.
    # Returns True if groupadd has exit code of 0.
    @classmethod
    def addGroup(cls, group, host=None):
        (exit_code, _) = cls.runas("root", "groupadd " + group, host=host)
        return exit_code == 0

    # Deletes a user. By default, deletes group with the same name afterwards.
    # Returns True if user deleting succeeds.
    @classmethod
    def deleteUser(cls, username, deleteItsGroupToo=True, host=None):
        #delete user
        cmd = "userdel -r %s" % username
        (exit_code, _) = cls.runas("root", cmd, host=host)
        #delete group
        if deleteItsGroupToo:
            cls.deleteGroup(username, host=host)
        return exit_code == 0

    # Deletes a group.
    # Returns True if group deletion succeeds.
    @classmethod
    def deleteGroup(cls, group, host=None):
        cmd = "groupdel %s" % group
        (exit_code, _) = cls.runas("root", cmd, host=host)
        return exit_code == 0

    # Given existing user and existing group, add the user to the group.
    # If any does not exist, use it at your own risk.
    # Returns True if adding user to the group succeeds.
    @classmethod
    def addUserToGroup(cls, username, group, host=None):
        if platform.platform().lower().find("suse") != -1 and not cls.isSuse_12():
            #suse
            cmd = "groupmod -A %s %s" % (username, group)
        else:
            #redhat/centos
            #linux-2.6.32-220.23.1.el6.yahoo.20120713.x86_64-x86_64-with-redhat-6.2-santiago
            cmd = "usermod -a -G %s %s" % (group, username)
        (exit_code, _) = cls.runas("root", cmd, host=host)
        return exit_code == 0

    # Remove the user from the group.
    # Returns True if removal does succeed.
    @classmethod
    def removeUserFromGroup(cls, username, group, host=None):
        if Machine.isSuse11():
            cmd = "usermod -R %s %s" % (group, username)
        else:
            cmd = "gpasswd -d %s %s" % (username, group)
        exit_code, _ = cls.runas("root", cmd, host=host)
        return exit_code == 0

    # Set a password for user.
    # Returns True if user addition  succeeds.
    @classmethod
    def setUserPasswd(cls, username, password, host=None):
        cmd = "echo -e '%s\\n%s' | passwd %s" % (password, password, username)
        (exit_code, _) = cls.runas("root", cmd, host=host)
        return bool(exit_code == 0)

    @classmethod
    def get_user_id(cls, user_name, host=None):
        cmd = 'id -u %s' % user_name
        exit_code, stdout = cls.runas('root', cmd, host)
        if exit_code == 0:
            return stdout.strip()
        return -1

    @classmethod
    def getDBVersion(cls, dbType, host):
        dbVersion = None
        if dbType == 'postgres':
            dbVersion = cls.getPostgresqlVersion(host=host)

        return dbVersion

    @classmethod
    def getPostgresqlVersion(cls, host):
        dbVersion = None
        pattern = r'postgres \(PostgreSQL\) (\d+.*)'
        #Version cmds for postgres 8.x and 9.1
        version_cmds = []
        # different platforms have different locations where postgres binary is
        # run through all commands and the first one we find that works is the version
        # we will report. This makes the assumption that you have only one postgres installed
        # and configured to run.
        version_cmds.append('postgres --version')
        version_cmds.append('/usr/lib/postgresql/8.4/bin/postgres --version')
        version_cmds.append('/usr/lib/postgresql/9.1/bin/postgres --version')
        version_cmds.append('/usr/lib/postgresql/9.3/bin/postgres --version')
        version_cmds.append('/usr/lib/postgresql93/bin/postgres --version')
        version_cmds.append('/usr/local/postgresql-9.1.9/bin/postgres --version')
        version_cmds.append('/usr/pgsql-9.1/bin/postgres --version')
        version_cmds.append('/usr/pgsql-9.3/bin/postgres --version')

        for cmd in version_cmds:
            exit_code, stdout = cls.runas(cls.getAdminUser(), cmd, host=host)
            if exit_code == 0:
                m = re.search(pattern, stdout)
                if m and m.group(1):
                    dbVersion = m.group(1)
                break

        return dbVersion

    @classmethod
    def checkifMySQLisInstalled(cls, returndefault=True):
        '''
        Assuming Database is always installed on last node of the cluster
        This function return (True, hostname)/(False, None) on whether MySQL is installed or not
        '''
        lastnode = cls.getLastNodefromAllnodes()
        if Machine.isWindows():
            pid = Machine.getProcessListWithPid(lastnode, "mysqld", "mysql", True)
        else:
            pid = Machine.getProcessListRemote(lastnode, format="%U %p %P %a", filter="mysql")
        logger.info(pid)
        if pid:
            if returndefault:
                return (True, "localhost")
            else:
                return (True, lastnode)
        else:
            return (False, None)

    @classmethod
    def getLastNodefromAllnodes(cls):
        '''
        Get last node from /tmp/all_nodes
        '''
        # read /tmp/all_nodes
        f = open("/tmp/all_nodes")
        t = f.read()
        m = t.split("\n")
        if "" in m:
            m.remove("")
        lastnode = m[-1]
        return Machine.getfqdn(name=lastnode)

    # Extract a bz2 archine
    # filepath - Tar file path
    # outpath - Path to extract the archive to
    @classmethod
    def tarBZ2ExtractAll(cls, user, host, filepath, outpath):
        cmd = 'tar -xvjf ' + filepath + " -C " + outpath
        _exit_code, _stdout = cls.runas(user, cmd, host=host)

    @classmethod
    def getExportCmd(cls):
        return 'export'

    @classmethod
    def getProcessMemory(cls, pid, host=None, logoutput=True):
        '''
        Overridden
        '''
        (exit_code, stdout) = cls.cat(host=host, path='/proc/%s/status' % pid, logoutput=logoutput)
        if exit_code != 0:
            return None
        else:
            matchObj = re.search(r'VmSize:\s*(\d+) kB', stdout)
            if matchObj is None:
                return None
            else:
                return int(matchObj.group(1))

    @classmethod
    def cat(cls, host=None, path='/', logoutput=True, user=None, passwd=None):
        '''
        overridden
        '''
        if user is None:
            user = cls.getAdminUser()
            passwd = cls.getAdminPasswd()
        return Machine.runas(
            user=user, cmd='cat %s' % path, host=host, cwd=None, env=None, logoutput=logoutput, passwd=passwd
        )

    @classmethod
    def rename(cls, user, host, src, dest, passwd=None):
        #overridden method
        cmd = "mv %s %s" % (src, dest)
        if isLoggedOnUser(user) and cls.isSameHost(host):
            return cls.run(cmd=cmd, logoutput=False)
        else:
            return cls.runas(user=user, cmd=cmd, host=host, cwd=None, env=None, logoutput=False, passwd=passwd)

    @classmethod
    def tail(cls, filename, user=None, passwd=None, host=None, args=None, env=None, logoutput=True):
        '''
        Runs tail command.
        overridden method
        Returns (exit_code, stdout)
        '''
        if args is None:
            cmd = "tail"
        else:
            cmd = "tail %s" % args
        cmd += " %s" % filename
        (exit_code, stdout) = cls.runas(
            user=user, cmd=cmd, host=host, cwd=None, env=env, logoutput=logoutput, passwd=passwd
        )
        if logoutput:
            logger.info("tail returns %s", exit_code)
            logger.info(stdout)
        return (exit_code, stdout)

    @classmethod
    def resetTimeZone(cls, host=None):
        '''
        Reset the timezone to UTC
        '''
        # pylint: disable=line-too-long
        cmd = "rm -f /etc/localtime; ln -s /usr/share/zoneinfo/UTC /etc/localtime; echo 'ZONE=\"UTC\"' > /etc/sysconfig/clock; echo 'UTC=true' >> /etc/sysconfig/clock; echo 'ARC=false' >> /etc/sysconfig/clock;"
        # pylint: enable=line-too-long
        return cls.runas(cls.getAdminUser(), cmd, host=host)

    @classmethod
    def getWrappedServicePIDRemote(cls, service):
        '''
        LinuxMachine method
        '''
        pass

    @classmethod
    def setup_nfs_debian(cls, nfs_client, nfs_server):
        '''
        Function to disable system portmap/nfs-kernel-server and install nfs-common on nfs client and server
        nfs client is list of nfs clients and nfs server is string
        '''
        nfs_client.append(nfs_server)
        nodes = list(set(nfs_client))
        for node in nodes:
            command = "apt-get -y install nfs-common"
            exit_code, _stdout = cls.runas(
                Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd()
            )
            assert exit_code == 0, "Unable to install nfs-common"
            # stop nfs demons
            cmd = "service nfs-kernel-server stop"
            _stdout = cls.runas(Machine.getAdminUser(), cmd, node, None, None, "True", Machine.getAdminUser())
            cmd = "service portmap stop"
            _stdout = cls.runas(Machine.getAdminUser(), cmd, node, None, None, "True", Machine.getAdminUser())

    @classmethod
    def install_dig(cls, hosts=None):
        """
        install dig on hosts
        :param hosts: list of hosts or None
        :return:
        """
        if cls.isRedHat() or cls.isCentOs() or cls.isAmazonLinux() or cls.isSuse():
            packages = "bind-utils"
        elif cls.isUbuntu() or cls.isDebian():
            packages = "dnsutils"
        cls.installPackages(packages=packages, hosts=hosts)

    @classmethod
    def create_file(cls, size, directory, filename, count=1):
        '''
        Create file of defined size at defined location
        input = (1M, /tmp, myfile) = create 1MB file at /tmp/myfile
        return True if file is created
        return False otherwise
        '''
        cls.rm(cls.getAdminUser(), None, filename, False, cls.getAdminPasswd())
        input_devices = ["/dev/sda", "/dev/sda1", "/dev/urandom"]
        present_input_device = ""
        for id_ in input_devices:
            if cls.pathExists(cls.getAdminUser(), None, id_, cls.getAdminPasswd()):
                present_input_device = id_
                break
        if present_input_device == "":
            logger.info("%s can't be created due to no input device is present", filename)
            return False
        cls.makedirs(cls.getAdminUser(), None, directory, cls.getAdminPasswd())
        cmd = "dd if=%s of=%s bs=%s count=%s" % (present_input_device, os.path.join(directory, filename), size, count)
        exit_code, _stdout = cls.runas(cls.getAdminUser(), cmd, None, None, None, True, cls.getAdminPasswd())
        return bool(exit_code == 0)

    @classmethod
    def installPackages(cls, packages="", hosts=None, logoutput=True):
        if Machine.isRedHat() or Machine.isCentOs() or Machine.isAmazonLinux():
            cmd = "yum install -y %s" % packages
        elif Machine.isUbuntu() or Machine.isDebian():
            cmd = "apt-get --force-yes install -y %s" % packages
        elif Machine.isSuse():
            cmd = "zypper install -y %s" % packages
        if hosts is None:
            hosts = [None]
        allSucceeded = True
        for host in hosts:
            (exit_code, _) = Machine.runas(
                user=Machine.getAdminUser(),
                cmd=cmd,
                host=host,
                cwd=None,
                env=None,
                logoutput=logoutput,
                passwd=Machine.getAdminPasswd()
            )
            if exit_code != 0:
                allSucceeded = False
        return allSucceeded

    @classmethod
    def installPackageWithPip(cls, packages="", hosts=None, logoutput=True):
        '''
        Install package using Pip install <package>
        '''
        if not hosts:
            hosts = [None]
        # install pip package on hosts
        cmd_pip = "easy_install pip"
        # install package using pip on hosts
        cmd = "pip install %s" % packages
        allSucceeded = True
        for host in hosts:
            (exit_code, _) = Machine.runas(
                user=Machine.getAdminUser(),
                cmd=cmd_pip,
                host=host,
                cwd=None,
                env=None,
                logoutput=logoutput,
                passwd=Machine.getAdminPasswd()
            )
            (exit_code, _) = Machine.runas(
                user=Machine.getAdminUser(),
                cmd=cmd,
                host=host,
                cwd=None,
                env=None,
                logoutput=logoutput,
                passwd=Machine.getAdminPasswd()
            )
            if exit_code != 0:
                allSucceeded = False
        return allSucceeded

    @classmethod
    def _getRunnablePythonCmd(cls, modulename, command):
        rpcmd = "python -c \""
        if modulename != "":
            rpcmd += "import %s; " % modulename
        rpcmd += command + " \""
        return rpcmd

    @classmethod
    def ls(cls, path, user=None, passwd=None, host=None, args=None, env=None, logoutput=True):
        '''
        Runs ls command.
        overridden method
        Returns (exit_code, stdout)
        '''
        if args is None:
            cmd = "ls"
        else:
            cmd = "ls %s" % args
        cmd += " %s" % path
        exit_code, stdout = cls.runas(
            user=user, cmd=cmd, host=host, cwd=None, env=env, logoutput=logoutput, passwd=passwd
        )
        if logoutput:
            logger.info("ls returns %s", exit_code)
            logger.info(stdout)
        return exit_code, stdout

    @classmethod
    def getUID(cls, user):
        '''
        Return user id in int
        '''
        cmd = "id -u %s;" % user
        exit_code, stdout = cls.runas(cls.getAdminUser(), cmd)
        uid = -1
        if exit_code == 0:
            uid = int(stdout)
        return uid

    @classmethod
    def checkCGroupsSupport(cls):
        if cls.isLinux():
            if cls.isDebian():
                return False  # BUG-26708 We do not support CGroups on Debian 6
            kernel_details = platform.uname()[2].split('.')
            if int(kernel_details[0]) > 2:
                return True
            elif int(kernel_details[0]) == 2:
                if int(kernel_details[1]) > 6:
                    return True
                elif int(kernel_details[1]) == 6:
                    revision_patch_details = kernel_details[2].split("-")
                    if len(revision_patch_details) >= 1 and float(revision_patch_details[0]) >= 24:
                        return True
        return False

    @classmethod
    def unzip(cls, zipFile, destDir="", logoutput=True):  # pylint: disable=unused-argument
        """
        LinuxMachine
        :param zipFile:
        :param destDir:
        :param logoutput:
        :return:
        """
        cmd = 'unzip %s' % zipFile
        if destDir != "":
            cmd += " -d %s" % destDir
        (exit_code, stdout) = cls.run(cmd)
        return (exit_code, stdout)

    @classmethod
    def getFreeDiskSpace(cls, mountPoint, logoutput=True):
        """
        LinuxMachine get free disk space of the mount point
        :return:
        """
        cmd = "df"
        (_, stdout) = cls.run(cmd)
        lines = stdout.split("\n")
        result = None
        for line in lines:
            if line.find(mountPoint) > -1:
                tokens = line.split()
                result = int(tokens[3]) * 1024
        if logoutput:
            logger.info("LinuxMachine.getFreeDiskSpace returns %s", result)
        return result


class WindowsMachine(BaseMachine):
    """Windows machine"""
    _defaultAdminUser = "Administrator"
    _tmpDir = "c:\\windows\\temp"
    # all messages we can ignore when doing ssh or remote calls
    STRINGS_TO_IGNORE = []
    STRINGS_TO_IGNORE.append('File Not Found')
    STRINGS_TO_IGNORE.append('Volume in drive')
    STRINGS_TO_IGNORE.append('Volume serial')
    STRINGS_TO_IGNORE.append('Directory of')
    STRINGS_TO_IGNORE.append('The system cannot find the file specified')

    @classmethod
    def create_file(cls, size, directory, filename, count=1):  # pylint: disable=unused-argument
        '''
        Create file of defined size at defined location
        input = (1M, /tmp, myfile) = create 1MB file at /tmp/myfile
        return True if file is created
        return False otherwise
        '''
        cls.makedirs(cls.getAdminUser(), None, directory, cls.getAdminPasswd())
        cmd = "fsutil file createnew %s %s" % (os.path.join(directory, filename), str(size))
        exit_code, _stdout = cls.runas(cls.getAdminUser(), cmd, None, None, None, True, cls.getAdminPasswd())
        return exit_code == 0

    @classmethod
    def getServiceProcessesList(cls, serviceName, node, logOutput=False):
        """
        Returns ProcessIds for service on remote\\local node
        Returns empty list if no process found
        """
        PIDS = []
        statement = "name='java.exe' and commandline like '%%-%s-%%'" % serviceName
        cmd = 'wmic process where \\"%s\\" get ProcessId' % statement
        user, passwd = Machine.getPrivilegedUser()
        _exit_code, output = Machine.runas(
            user=user, cmd=cmd, host=node, cwd=None, env=None, logoutput=True, passwd=passwd
        )
        if logOutput:
            logger.info("geServiceProcessesList | cmd = '%s'", cmd)
            logger.info("geServiceProcessesList | serviceName = '%s'", serviceName)
            logger.info("geServiceProcessesList | node = '%s'", node)
            logger.info("geServiceProcessesList | logOutput = '%s'", logOutput)
            logger.info("geServiceProcessesList | output = '%s'", output)
        output = output.replace("\r", "")
        if output.find("ProcessId") >= 0:
            outLines = output.split("\n")
            for line in outLines:
                if line.replace(" ", "") not in ["", "ProcessId"]:
                    logger.info("Line: '%s'", line)
                    PID = filter(str.isdigit, line)
                    logger.info("PID for service '%s' on '%s'= '%s'", serviceName, node, PID)
                    PIDS.append(PID)
        if not PIDS:
            logger.info("geServiceProcessesList | Cannot get PID for service '%s' on '%s'", serviceName, node)
        return PIDS

    @classmethod
    def _decoratedcmd(cls, cmd):
        return "call " + cmd

    @classmethod
    def _buildcmd(cls, cmd, user, host, passwd=None, env=None, doEscapeQuote=True):
        '''
        WindowsMachine._buildcmd

        Returns a string of command.
        Depending on target host/user, sudocmd/sshcmd functions are used to get the command.
        Always use powershell to run a particular command.
        It is user's responsibility to ecsape characters to match the API.
        '''
        #if host is None or "", it means same host.
        if cmd is None:
            return None
        if cls.isSameHost(host):
            #is same host. Use executeAs.
            #due to an issue in UNCFilePath, we can't automatically escape \.
            #    For example, you need dir d:\\ in executeas when enclosing " in command.
            #cmd = cmd.replace("\\", "\\\\")
            cmd = cmd.replace("\"", "\\\"")
            cmd = cls.getExecuteAsCmd(cmd, user, passwd, cmdEnclosingChar="\"")
        else:
            #is other host. Use powershell + cmd.
            if user == Config.get('machine', 'ROOT_USER'):
                #is admin user
                #enclosing "" for command here has many issues. Use no enclosing double-quote.
                #cmd = cmd.replace("\\", "\\\\")
                #cmd = cmd.replace("\"", "\\\"")
                cmd = 'cmd /c %s' % cmd
                cmd = cls.getPowershellRunCmd(cmd, host)
            else:
                #is not admin user
                assert False, "should not reach this line. Building remote command with non-admin user is unsupported."
        return cmd

    @classmethod
    def runas(
            cls,
            user,
            cmd,
            host="",
            cwd=None,
            env=None,
            logoutput=True,
            passwd=None,
            doEscapeQuote=True,
            retry_count=0,
            retry_sleep_time=10
    ):
        '''
        WindowsMachine.runas

        Runs given command locally/remotely with command prompt with given user privilege.
        Remote execution is supported only with Administrator user.
        '''
        if cmd is None:
            if host == "" or host is None:
                return cls.run(None, cwd, env, logoutput)
            else:
                assert False, "This line should not be reached."
        else:
            if user != Config.get('machine', 'ROOT_USER') and host is not None and host != "":
                return (-1, "runas on other host with non-admin user is not supported.")
            if (host == "" or host is None or host == "localhost" or host == socket.gethostname()) \
               and isLoggedOnUser(user):
                return cls.run(cmd, cwd, env, logoutput, retry_count, retry_sleep_time)
            else:
                return cls.run(
                    cls._buildcmd(cmd, user, host, passwd, env=env, doEscapeQuote=doEscapeQuote), cwd, env, logoutput,
                    retry_count, retry_sleep_time
                )
        return None

    # Runs given command locally with powershell with current user
    # It is user's responsibility to ecsape characters to match the API.
    @classmethod
    def runPowershell(cls, cmd, cwd=None, env=None, logoutput=True):
        if cmd is None:
            return (-1, "No command given")
        #cmd = cmd.replace("\\", "\\\\")
        cmd = cmd.replace('"', '\\"')
        cmd = 'powershell "& {%s}"' % cmd
        return cls.run(cmd, cwd, env, logoutput)

    @classmethod
    def runPowershellAs(cls, cmd, user=None, host="", cwd=None, env=None, logoutput=True, passwd=None):
        '''
        Runs given command locally/remotely with powershell with given user privilege.
        If user is None, use Administrator user. Password can be None for Administrator.
            The password will be automatically retrieved.
        Remote execution is supported only with Administrator user.
        It is user's responsibility to ecsape characters to match the API.
        '''
        if cmd is None:
            return (-1, "No command given")
        #if same host and current user, use runPowershell
        if (host == "" or host is None) and isLoggedOnUser(user):
            return cls.runPowershell(cmd, cwd, env, logoutput)
        if user is None:
            user = Config.get('machine', 'ROOT_USER')
            passwd = Config.get('machine', 'ROOT_PASSWD')
        if user != Config.get('machine', 'ROOT_USER') and host is not None and host != "":
            #command should work. There is additional config needed on host to pass through access denied.
            return (-1, "runPowershellAs on other host with non-admin user is not supported.")
        if host is not None and host != "" and passwd is None:
            return (-1, "Password is not given.")
        #cmd = cmd.replace("\\", "\\\\")
        cmd = cmd.replace("\"", "\\\"")
        cmd = cls.getPowershellRunCmd(cmd, host, user, passwd)
        return cls.run(cmd, cwd, env, logoutput)

    @classmethod
    def runPowershellAsAdminWithExecuteAsAdmin(cls, cmd, host=None, cwd=None, env=None, logoutput=True):
        '''
        Runs >ExecuteAs -u administrator ... call powershell \"..<target machine>.. <cmd>\"
        If we don't do execute as first, command lines would not be shown up.
        '''
        user = Config.get('machine', 'ROOT_USER')
        passwd = Config.get('machine', 'ROOT_PASSWD')
        cmd = cmd.replace("\"", "\\\"")
        cmd = cls.getPowershellRunCmd(cmd, host=host, user=user, passwd=passwd, cmdEnclosingChar='\\"')
        cmd = cls.getExecuteAsCmd(cmd, user=user, passwd=passwd, cmdEnclosingChar='')
        return cls.run(cmd, cwd, env, logoutput)

    # Overridden method
    @classmethod
    def sudocmd(cls, cmd, user, passwd=None, env=None, doEscapeQuote=True):
        assert False, 'dont use this in Windows'

    # Gets ExecuteAs command to run particular command with other user.
    # Escaping cmd argument is not done inside this method.
    # The cmd given is assumed to have " replaced with \" and \ replaced with \\ already.
    # Overridden method
    @classmethod
    def getExecuteAsCmd(cls, cmd, user, passwd=None, cmdEnclosingChar="\""):
        # TODO figure out how to set the env for windows
        if passwd is None:
            if user == Config.get('machine', 'ROOT_USER'):
                passwd = Config.get('machine', 'ROOT_PASSWD')
            else:
                passwd = Config.get('machine', 'DEFAULT_PASSWD')
        # >executeAs dir d:\ works.
        # >executeAs "dir d:\\" works.
        # this method uses enclosing quote command. Characters in given cmd parameter should be escaped already.
        # for instance, cmd = "\"hello 1\" \"world\""
        # returns
        #   executeAs "echo \"hello 1\" \"world\"" (python enclosing "" is omitted)

        if user is None:
            return os.path.join(Config.getEnv("WORKSPACE"), "tools", "ExecuteAs.exe") + \
                   " " + cmdEnclosingChar + cmd + cmdEnclosingChar
        else:
            return os.path.join(Config.getEnv("WORKSPACE"), "tools", "ExecuteAs.exe") + \
                   " -u \"" + user + "\" -p \"" + passwd + "\" " + \
                   " " + cmdEnclosingChar + cmd + cmdEnclosingChar

    #overridden method
    @classmethod
    def sshcmd(cls, cmd, host, user=None, password=None):  # pylint: disable=unused-argument
        assert False, 'dont use this in Windows'

    @classmethod
    def getPowershellRunCmd(cls, cmd, host, user=None, passwd=None, cmdEnclosingChar="\""):
        '''
        Gets Powershell command to run particular command in these cases:
        a) locally with other user.
        b) remotely with admin user
        Escaping cmd argument is not done inside this method.
        The cmd given is assumed to have " replaced with \" and \\ replaced with \\ already.
        If user is None or "", administrator is used.
        If using admin, password can be None. Password is automatically set.
        For host, if is IP, use IP. Otherwise, use short hostname.
        '''
        if user is None or user == "":
            user = Config.get('machine', 'ROOT_USER')
        #if user is administrator, automatically guess this password if not given.
        if user == Config.get('machine', 'ROOT_USER') and passwd is None:
            passwd = Config.get('machine', 'ROOT_PASSWD')
        #if host is not IP, use short hostname.
        if not util.isIP(host):
            host = host.split('.')[0]
        #find credential user
        if host is not None and host != '':
            credUser = "%s\\%s" % (host, user)
        else:
            host = "localhost"
            credUser = user
        #These powershell settings have to be set already at remote host.
        # >Enable-PSRemoting
        # >set-item WSMan:\localhost\Client\allowunencrypted $true
        cmd = 'powershell ' + cmdEnclosingChar + '& {$pass = convertto-securestring ' + \
              '\\"%s\\" -asplaintext -force; ' % passwd + \
              '$mycred = new-object -typename System.Management.Automation.PSCredential ' + \
              '-argumentlist \\"%s\\",$pass; ' % credUser + \
              'invoke-command -ComputerName \\"%s\\" -credential $mycred ' % host + \
              '-scriptblock { %s };}' % cmd + cmdEnclosingChar
        return cmd

    # Deprecated method for Windows _buildcmd
    @classmethod
    def _buildcmdDeprecated(cls, cmd, user, host, passwd=None, env=None):
        if not isLoggedOnUser(user):
            cmd = cls.sudocmdDeprecated(cmd, user, passwd, env=env)
        if not cls.isSameHost(host):
            cmd = cls.sshcmdDeprecated(cmd, host)
        return cmd

    # Deprecated method for Windows runas
    @classmethod
    def runasDeprecated(cls, user, cmd, host="", cwd=None, env=None, logoutput=True, passwd=None):
        return cls.run(cls._buildcmdDeprecated(cmd, user, host, passwd, env=env), cwd, env, logoutput)

    # Deprecated method for Windows sudocmd
    @classmethod
    def sudocmdDeprecated(cls, cmd, user, passwd=None, env=None):  # pylint: disable=unused-argument
        # TODO figure out how to set the env for windows
        if passwd is None:
            passwd = Config.get('machine', 'DEFAULT_PASSWD')
        return os.path.join(Config.getEnv("WORKSPACE"), "tools", "ExecuteAs.exe") + " -u " + user + " -p \"" + \
            passwd + "\" " + cmd.replace("\"", "\\\"")

    # Deprecated method for Windows sshcmd
    @classmethod
    def sshcmdDeprecated(cls, cmd, host, user=None, password=None):  # pylint: disable=unused-argument
        return cmd

    @classmethod
    def echocmd(cls, text):
        return "echo %s" % text

    #Overriden method
    @classmethod
    def _performcopy(cls, user, host, srcpath, destpath, localdest, passwd=None):
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if isLoggedOnUser(user) and cls.isSameHost(host):
            cls.copy(srcpath, destpath)
        else:
            cls.runasDeprecated(
                user, cls._copycmd(user, host, srcpath, destpath, localdest, passwd=passwd), passwd=passwd
            )

    @classmethod
    def copy(cls, srcpath, destpath, user=None, passwd=None):
        '''
        Overridden method

        Copies files locally from source path to destination path.
        Returns None.
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if isLoggedOnUser(user):
            if os.path.isdir(srcpath):
                shutil.copytree(srcpath, destpath)
            else:
                shutil.copy(srcpath, destpath)
        else:
            cls.runasDeprecated(
                user, cls._copycmd(user, None, srcpath, destpath, localdest=False, passwd=passwd), passwd=passwd
            )
            # make sure we make the file 777 if we are running as some other user so current user can delete the file
            cls.chmod("777", destpath, recursive=True, user=user, host=None)

    # Copies a file from local to remote host and grant full access to everyone.
    # Returns None always.
    # If copying to remote host, the method automatically uses admin user.
    # Remote target directory must exist before calling this method.
    #overridden method
    @classmethod
    def copyFromLocal(cls, user, host, srcpath, destpath, passwd=None):
        if (host is not None and host != "") and user != cls.getAdminUser():
            logger.info("WARNING: copyFromLocal works only with admin user when doing remote. Using admin.")
            user = cls.getAdminUser()
            passwd = cls.getAdminPasswd()
        cls._performcopy(user, host, srcpath, destpath, localdest=False, passwd=passwd)
        cls.chmod("777", destpath, recursive=True, user=user, host=host)

    @classmethod
    def copyToLocal(cls, user, host, srcpath, destpath, passwd=None):
        '''
        Overridden method
        Copies a file from remote to local host and grant full access to everyone.
        If copying to remote host, the method automatically uses admin user.
        Returns None always.
        '''
        if (host is not None and host != "") and user != cls.getAdminUser():
            logger.info("WARNING: copyToLocal works only with admin user when doing remote. Using admin.")
            user = cls.getAdminUser()
            passwd = cls.getAdminPasswd()
        cls._performcopy(user, host, srcpath, destpath, localdest=True, passwd=passwd)
        cls.chmod("777", destpath, recursive=True, user=user, host=None)

    @classmethod
    def _getRunnablePythonCmd(cls, modulename, command):
        rpcmd = "python -c \""
        if modulename != "":
            rpcmd += "import %s; " % modulename
        rpcmd += command + " \""
        return rpcmd.replace("\\", "\\\\")

    #filepath can be UNC.
    @classmethod
    def _isDir(cls, user, filepath, passwd=None):
        _exit_code, stdout = cls.runasDeprecated(
            user, cls._getRunnablePythonCmd("os", "print os.path.isdir('%s')" % filepath), passwd=passwd
        )
        return stdout.strip() == "True"

    # filepath will be converted to UNC automatically.
    @classmethod
    def _pathExists(cls, user, host, filepath, passwd=None):
        filepath = cls.getUNCFilePath(filepath, host)
        if user:
            _exit_code, stdout = cls.runasDeprecated(
                user, cls._getRunnablePythonCmd("os", "print os.path.exists('%s')" % filepath), passwd=passwd
            )
            return stdout.strip() == "True"
        else:
            return os.path.exists(filepath)

    # Internal method for making directories.
    # Given filepath should be non-UNC.
    # Returns (exit_code, stdout)
    @classmethod
    def _makedirs(cls, user, host, filepath, passwd=None):
        if not cls.isSameHost(host):
            filepath = Machine.getUNCFilePath(filepath, host)
        return cls.runasDeprecated(
            user, cls._getRunnablePythonCmd("os", "os.makedirs('%s')" % (filepath)), passwd=passwd
        )

    #srcpath, destpath can be UNC.
    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest, passwd=None):
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if host:
            if localdest:
                srcpath = cls.getUNCFilePath(srcpath, host)
            else:
                destpath = cls.getUNCFilePath(destpath, host)
        if cls._isDir(user, srcpath, passwd=passwd):
            return cls._getRunnablePythonCmd("shutil", "shutil.copytree('%s', '%s')" % (srcpath, destpath))
        else:
            return cls._getRunnablePythonCmd("shutil", "shutil.copy('%s', '%s')" % (srcpath, destpath))

    # chmod on Windows using icacls. Doing grant only. It is not real chmod.
    # Returns (exit_code, stdout)
    # Filepath is not UNC. The method will get UNC automatically.
    # perm must be a string.
    # There is no concept of user/group/others. It grants only on everyone (3 digits are the same)
    #       or a user otherwise.
    # user is the user to get permissions being set.
    # icacls command is run by Administrator user.
    # Grant replace read by /grant:r(replace) <user>:r does not imply denying write.
    #       Use chmodDeny method for explicit deny.
    @classmethod
    def chmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None, logoutput=True):
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if filepath.find("file:") == 0:
            filepath = filepath.split("///")[1].replace("/", "\\")
        cmd = "icacls %s /q " % cls.getUNCFilePath(filepath, host)
        if recursive:
            cmd += "/t"
        cmd += " /grant:r "
        perm = str(perm)
        #if 3 digits are the same, grant this permission similarly to everyone.
        if perm[0] == perm[1] and perm[1] == perm[2]:
            cmd += "everyone"
        else:
            cmd += user
        if perm[0] == "6":
            cmd += ":rw"
        elif perm[0] == "5":
            cmd += ":rx"
        elif perm[0] == "4":
            cmd += ":r"
        elif perm[0] == "2":
            cmd += ":w"
        else:
            cmd += ":f"
        return cls.runasDeprecated(cls.getAdminUser(), cmd, logoutput=logoutput, passwd=cls.getAdminPasswd())

    @classmethod
    def chmodDeny(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None):
        '''
        chmod on Windows using icacls. Doing deny only. It is not real chmod.
        Returns (exit_code, stdout)
        Filepath is not UNC. The method will get UNC automatically.
        perm can be either string or number. Either 777 or "777" is fine.
        There is no concept of user/group/others. It denies only on everyone (3 digits are the same)
                or a user otherwise.
        user is the user to get permissions being set.
        icacls command is run by Administrator user.
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd

        if filepath.find("file:") == 0:
            filepath = filepath.split("///")[1].replace("/", "\\")
        cmd = "icacls %s /q " % cls.getUNCFilePath(filepath, host)
        if recursive:
            cmd += "/t"
        cmd += " /deny "
        perm = str(perm)
        #if 3 digits are the same, deny this permission similarly to everyone.
        if perm[0] == perm[1] and perm[1] == perm[2]:
            cmd += "everyone"
        else:
            cmd += user
        if perm[0] == "6":
            cmd += ":rw"
        elif perm[0] == "5":
            cmd += ":rx"
        elif perm[0] == "4":
            cmd += ":r"
        elif perm[0] == "2":
            cmd += ":w"
        else:
            cmd += ":f"
        return cls.runasDeprecated(cls.getAdminUser(), cmd, passwd=cls.getAdminPasswd())

    @classmethod
    def chmodRevoke(cls, perm, filepath, recursive=False, user="Everyone", host=None, passwd=None):
        '''
        chmod on Windows using icacls. Doing revoked only. It is not real chmod.
        Returns (exit_code, stdout)
        Filepath is not UNC. The method will get UNC automatically.
        perm can be only string that starts with "d"(deny) or "g"(grant).
        user is the user to get permissions being revoked.
        icacls command is run by Administrator user.
        '''
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd
        perm = perm[0].lower()
        if filepath.find("file:") == 0:
            filepath = filepath.split("///")[1].replace("/", "\\")
        cmd = "icacls %s /q " % cls.getUNCFilePath(filepath, host)
        if recursive:
            cmd += "/t"
        cmd += " /remove:%s %s" % (perm, user)
        return cls.runasDeprecated(cls.getAdminUser(), cmd, passwd=cls.getAdminPasswd())

    @classmethod
    def winUtilsChmod(cls, perm, filepath, recursive=False, user=None, host=None, passwd=None):
        WIN_UTILS = Config.get('hadoop', 'WIN_UTILS')
        cmd = WIN_UTILS + " chmod "
        if recursive:
            cmd += "-R "
        cmd += perm + " " + cls.getUNCFilePath(filepath, host)
        return cls.runasDeprecated(user, cmd, passwd=passwd)

    @classmethod
    def _rmcmd(cls, filepath, isdir=False, user=None, host=None):
        filepath = cls.getUNCFilePath(filepath, host)
        if isdir:
            return cls._getRunnablePythonCmd("shutil", "shutil.rmtree('%s', ignore_errors=True)" % filepath)
        else:
            return cls._getRunnablePythonCmd("os", "os.remove('%s')" % filepath)

    @classmethod
    def rm(cls, user, host, filepath, isdir=False, passwd=None):
        '''
        Overridden method in WindowsMachine

        Removes a file or directory.
        Returns None always.
        '''
        # set the user and pwd
        defUser, defPwd = cls.getPrivilegedUser()
        if not user:
            user = defUser
        if not passwd:
            passwd = defPwd
        try:
            if isLoggedOnUser(user) and cls.isSameHost(host):
                if isdir:
                    shutil.rmtree(filepath, ignore_errors=True)
                else:
                    os.remove(filepath)
            else:
                cls.runasDeprecated(
                    user, cls._rmcmd(filepath, isdir=isdir, user=user, host=host), host=host, passwd=passwd
                )
        except Exception:
            pass

    @classmethod
    def _renamecmd(cls, src, dest, host=None):
        srcfilepath = cls.getUNCFilePath(src, host)
        destfilepath = cls.getUNCFilePath(dest, host)
        return cls._getRunnablePythonCmd("os", "os.rename('%s','%s')" % (srcfilepath, destfilepath))

    #overridden method
    @classmethod
    def rename(cls, user, host, src, dest, passwd=None):
        if isLoggedOnUser(user) and cls.isSameHost(host):
            os.rename(src, dest)
        else:
            cls.runasDeprecated(user, cls._renamecmd(src, dest, host), passwd=passwd)

    #Run jar file by java command line with given user
    #Returns (exit_code, stdout)
    #Overridden method
    @classmethod
    def runJarAs(cls, user, jar, argList=None, cwd=None, env=None, logoutput=True):
        cmd = os.path.join(Config.get("machine", "JAVA_HOME"), "bin", "java") + " -jar " + jar
        for arg in argList:
            cmd += ' ' + arg + ''
        #This should fit new runas. Require more testing.
        return cls.runasDeprecated(user, cmd, '', cwd, env, logoutput)

    # touch given absolute file path remotely
    # overridden method
    @classmethod
    def touchRemote(cls, host, filePath, times=None, user=None, cwd=None, env=None, logoutput=True, passwd=None):
        return cls.run(
            cls._buildcmdDeprecated("touch %s" % filePath, user, host, passwd, env=env), cwd, env, logoutput
        )

    @classmethod
    def grep(cls, user, host, filepath, searchstr, regex=True, passwd=None, logoutput=False):
        cmd = "findstr "
        if regex:
            cmd += "/R "
        cmd += "/C:\"" + searchstr + "\" " + cls.getUNCFilePath(filepath, host)
        return cls.runasDeprecated(user, cmd, passwd=passwd, logoutput=logoutput)

    @classmethod
    def find(cls, user, host, filepath, searchstr, passwd=None, logoutput=False):
        '''
        Overridden method

        Returns a list of file names matching with searchstr.
        searchstr is a parameter to dir command.
        '''
        if filepath.find("file:") == 0:
            filepath = filepath.split("///")[1].replace("/", "\\")
        if host is None or host == "":
            cmd = "dir %s\\%s /S /B" % (filepath, searchstr)
        else:
            if user != cls.getAdminUser():
                logger.info("WARNING: remote find works only with admin user. Using admin.")
                user = cls.getAdminUser()
                passwd = cls.getAdminPasswd()
            cmd = "dir %s\\%s /S /B" % (cls.getUNCFilePath(filepath, host), searchstr)

        _exit_code, stdout = cls.runasDeprecated(user, cmd, passwd=passwd, logoutput=logoutput)
        #File Not Found is returned if the file is not found.
        tmpResult = stdout.strip().replace('\r', '').split("\n")
        return util.prune_output(tmpResult, cls.STRINGS_TO_IGNORE, prune_empty_lines=True)

    @classmethod
    def service(cls, sname, action, user=None, host=None, passwd=None, options=""):
        NET_COMMANDS = [
            "ACCOUNTS", "COMPUTER", "CONTINUE", "FILE", "GROUP", "HELP", "HELPMSG", "LOCALGROUP", "PAUSE", "SESSION",
            "SHARE", "START", "STATISTICS", "STOP", "TIME", "USE", "USER", "VIEW"
        ]
        if not user:
            user = cls.getAdminUser()
            passwd = cls.getAdminPasswd()
        if cls.isSameHost(host) and options != "" and str(action).upper().split()[0] in NET_COMMANDS:
            return cls.runasDeprecated(user, "net %s %s %s" % (action, sname, options), passwd=passwd)
        else:
            if host != None:
                return cls.runasDeprecated(user, "sc \\\\%s %s %s %s" % (host, action, sname, options), passwd=passwd)
            else:
                return cls.runasDeprecated(user, 'sc %s %s %s' % (action, sname, options), passwd=passwd)

    @classmethod
    def type(cls):
        return 'Windows'

    # Added by:Logigear, 22-Aug-2012
    # Get list of running processes
    # Returns list of processes with option .
    #option : v, m, svc, s,...
    # java.exe is not shown with full command line and arguments.
    @classmethod
    def getProcessList(cls, option="v", logoutput=False):
        _exit_code, stdout = cls.run("tasklist /%s" % option, logoutput=logoutput)
        return stdout.split("\n")

    # Must have PsExec tool installed on OS to use this method
    # java.exe is not shown with full command line and arguments.
    @classmethod
    def getProcessListRemote(cls, hostIP, option="v", logoutput=True):
        # warning API signature is different from LinuxMachine.
        _exit_code, stdout = cls.run("psexec \\\\%s tasklist /%s" % (hostIP, option), logoutput=logoutput)
        return stdout.split("\n")

    @classmethod
    def getProcessListWithPid(  # pylint: disable=unused-argument
            cls, host=None, filterProcessName=None, filterCmdLine=None, logoutput=True, ignorecase=False
    ):
        '''
        Run wmic /OUTPUT=<ARTIFACTS_DIR>\\<node>_p2.txt PROCESS get Caption,Commandline,Processid
        To run on remotemachine wmic /node:Server1 /OUTPUT:<ARTIFACTS_DIR>\\<node>_p2.txt PROCESS
                get Caption,Commandline,Processid
        returns PID if found or else return None
        '''
        # add the pid to the file name as multiple processes might want to make this call.
        pid = str(os.getpid())
        outfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "%s_p2_%s.txt" % (host, pid))
        if cls.isSameHost(host) or host is None:
            host = None
            outfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "localhost_p2_%s.txt" % pid)
            cmd = "wmic /OUTPUT:\"%s\" PROCESS get Caption,Commandline,Processid" % outfile
        else:
            host = Machine.getfqdn(name=host)
            cmd = 'wmic  /node:"%s" /OUTPUT:\"%s\" PROCESS get Caption,Commandline,Processid' % (host, outfile)
        _exit_code, _stdout = cls.runas(cls.getAdminUser(), cmd, None, None, None, True, cls.getAdminPasswd())
        # change the permission of the file so every one can write to it.
        # this is helpful in the case where we want to delete the artifacts dir post run as user hadoopqa
        cls.chmod("777", outfile, recursive=True, user=cls.getAdminUser(), host=None)
        flags = 0
        if ignorecase:
            flags = re.IGNORECASE
        pattern = r"%s.*%s.*(\s)+(\d+)" % (filterProcessName, filterCmdLine)
        file_ = open(outfile, 'rb').read()
        mytext = file_.decode('utf-16')
        for line in mytext.split("\n"):
            if line.find(filterProcessName) >= 0:
                m = re.findall(pattern, line, flags=flags)
                if m:
                    logger.info(m[0][1])
                    return m[0][1]
        return None

    @classmethod
    def getProcessListPowershell(cls, host=None, filterProcessName=None, filterCmdLine=None, logoutput=True):
        '''
        Gets process list with powershell with get-wmiobject.
        Returns a list of (process name, command line, str(processID)).
        Returns None if exit code is non-zero.
        Command line returned in tuple entry can have spaces in between. It is result returned by Windows.
        This API does not automatically remove them. How do you know a space is real or false?
        Avoid using command line part of output if possible.
        filters are case-insensitive.
        '''
        # we need to make sure we use the short hostname
        # for example if we send arpitg-hdp2ha2.arpitg-hdp2ha.d2.internal.cloudapp.net the cmd will fail.
        # we need to send just arpitg-hdp2ha2
        # Added this hack so we dont have to make changes every where this method is called
        host = Machine.getfqdn(name=host)
        (exit_code, stdout) = cls._runGetProcessListPowershell(
            host, filterProcessName, filterCmdLine, logoutput=logoutput
        )
        if exit_code != 0:
            return None
        p = r"ProcessName\s+?:\s+?(\S+).*?CommandLine\s+?:\s+?(.*?)\nCreationClassName" + \
            r".*?\sProcessId\s+?:\s+?(\d+)?"
        result = re.findall(p, stdout, re.DOTALL)
        for index in range(len(result)):  # pylint: disable=consider-using-enumerate
            entry = result[index]
            entry = (entry[0], entry[1].replace("\r", ''), entry[2])
            entry = (entry[0], entry[1].replace("\n", ''), entry[2])
            result[index] = entry
            if entry[1] == "":
                logger.info("Command line at index %s is empty. Does Windows command return empty?", index)
        if logoutput:
            logger.info(result)
        return result

    @classmethod
    def _runGetProcessListPowershell(cls, host, filterProcessName, filterCmdLine, logoutput=True):
        '''
        Internal method to get process list with powershell with get-wmiobject.
        Returns (exit_code, stdout).
        '''
        if filterProcessName is None and filterCmdLine is None:
            cmd = "get-wmiobject win32_process"
        else:
            cmd = "get-wmiobject win32_process | where { "
            if filterProcessName is not None:
                cmd += "$_.name -like '*%s*'" % filterProcessName
                if filterCmdLine is not None:
                    cmd += " -and "
            if filterCmdLine is not None:
                cmd += "$_.commandline -like '*%s*'" % filterCmdLine
            cmd += " }"
        #To get process list locally remotely, we have to call powershell with executeAs admin.
        #Sometimes command lines returned from powershell are blank. It varies from machine to machine/timing.
        #I have no way to fix this yet. In bad case, during development, I run 3 times.
        #    1 of them get command lines. Other 2 are blank.
        if host is None:
            (exit_code, stdout) = cls.runPowershellAsAdminWithExecuteAsAdmin(
                cmd, host=host, cwd=None, env=None, logoutput=logoutput
            )
        else:
            (exit_code, stdout) = cls.runPowershellAs(
                cmd, user=None, host=host, cwd=None, env=None, logoutput=logoutput, passwd=None
            )
        return (exit_code, stdout)

    @classmethod
    def getPIDByPort(cls, port):
        pattern = re.compile(r"^\s+\w+\s+[0-9\.]+:%d.*\s+(\d+)" % port)
        _exit_code, stdout = cls.run("netstat -ano", logoutput=False)
        nslist = stdout.split("\n")
        for nsitem in nslist:
            match = pattern.match(nsitem)
            if match:
                return int(match.group(1))
        return None

    @classmethod
    def killProcess(cls, pid, immediate=False, timeout=30, interval=10):
        iter_ = 0
        killcmd = "taskkill /pid %d /t" % pid
        if immediate:
            return cls.run(killcmd + " /f")
        cls.run(killcmd)
        while (timeout - (iter_ * interval)) > 0:
            logger.info("Polling for process %d to finish...", pid)
            if cls.processExists(pid):
                time.sleep(interval)
            else:
                return None
            iter_ += 1
        if not immediate:
            logger.warn("Issuing a hard kill to pid %d", pid)
            return cls.killProcess(pid, immediate=True)
        return None

    @classmethod
    def killProcessRemote(cls, pid, host=None, user=None, passwd=None, logoutput=True, immediate=False):
        if host is None:
            cls.killProcess(pid, immediate)
        else:
            killcmd = "taskkill /pid %d /t /f" % pid
            cls.runas(user=user, cmd=killcmd, host=host, cwd=None, env=None, logoutput=logoutput, passwd=passwd)

    @classmethod
    def killProcessRemotePsexec(cls, PID, Node, logoutput=False):
        user, passw = cls.getPrivilegedUser()
        return cls.runas(
            user=user,
            cmd="psexec /accepteula \\\\%s -d -u %s -p %s taskkill /pid %s /t /f" % (Node, user, passw, PID),
            host=None,
            cwd=None,
            logoutput=logoutput,
            passwd=passw
        )

    @classmethod
    def killProcessTree(cls, pid):
        # pylint: disable=line-too-long
        killcmd = 'Get-WmiObject -Class Win32_Process -Filter "ParentProcessID=%d" | %% {$_.processid} | %% { Get-WmiObject -Class Win32_Process -Filter "ParentProcessID=$_" } | %%{stop-process $_.processid}' % pid
        # pylint: enable=line-too-long
        cls.runPowershell(killcmd)

    @classmethod
    def processExists(cls, pid):
        exit_code, output = cls.run("tasklist /v /fi \"PID eq %d\" /fo list" % pid, logoutput=False)
        if exit_code == 0:
            pattern = re.compile("^Status:.*", re.M)
            m = pattern.search(output)
            if m:
                return True
        return False

    @classmethod
    def getfqdn(cls, name=""):
        # In Windows, we use short hostname.
        if name is None or name == "":
            name = socket.gethostname().split('.')[0]
            return name
        else:
            if util.isIP(name):
                #is IP
                logger.info(
                    "WARNING: name argument passed to getfqdn is an IP. It should be full/short hostname. name=%s",
                    name
                )
                if util.getShortHostnameFromIP(name) is not None:
                    hostname = util.getShortHostnameFromIP(name)
                    logger.info("automatically convert to short hostname. Using %s", name)
                else:
                    hostname = name
                    logger.info("automatically retrieving shost hostname fails. getfqdn returns %s", hostname)
                return hostname
            else:
                #is not IP
                (containsIP, matchObj) = util.containsIP(name)
                if containsIP:
                    #contain IP. Returning IP.
                    # pylint: disable=line-too-long
                    logger.info(
                        "WARNING: getfqdn takes %s. It is not an IP but containing IP. Returning the IP. In Windows, we should use short hostname.",
                        name
                    )
                    # pylint: enable=line-too-long
                    return matchObj.group(0)
                else:
                    #does not contain any IP. Returning first item after splitting name with ".".
                    return name.split('.')[0]

    @classmethod
    def getUNCFilePath(cls, filepath, host):
        if filepath.find(":") == 1:
            filepath = "\\\\" + cls.getfqdn(host) + "\\" + filepath.replace(":", "$")
        return filepath

    @classmethod
    def filepathToURI(cls, *paths):
        return "file:///" + "/".join([p.replace('\\', '/') for p in paths])

    @classmethod
    def getPrivilegedUser(cls):
        return cls.getAdminUser(), cls.getAdminPasswd()

    @classmethod
    def sendSIGSTOP(cls, user, host, pid):
        '''
        Use pssuspend.exe to suspend a process.
        Always use administrator user in Windows so user does not matter.
        pid can be number of string. If host is None, means localhost.
        Returns (exit_code, stdout)
        '''
        psSuspendPath = os.path.join(Config.getEnv("WORKSPACE"), "tools", "psSuspend.exe")
        if cls.isSameHost(host):
            cmd = psSuspendPath + " %s /accepteula" % pid
        else:
            cmd = psSuspendPath + " \\\\%s -u %s -p %s %s /accepteula" % (
                host, cls.getAdminUser(), cls.getAdminPasswd(), pid
            )
        #run the command locally
        return Machine.runas(
            user=cls.getAdminUser(),
            cmd=cmd,
            host=None,
            cwd=None,
            env=None,
            logoutput=True,
            passwd=cls.getAdminPasswd()
        )

    @classmethod
    def sendSIGCONT(cls, user, host, pid):
        '''
        Use pssuspend.exe to resume a process.
        Always use administrator user in Windows so user does not matter.
        pid can be number of string. If host is None, means localhost.
        Returns (exit_code, stdout)
        '''
        psSuspendPath = os.path.join(Config.getEnv("WORKSPACE"), "tools", "psSuspend.exe")
        if cls.isSameHost(host):
            cmd = psSuspendPath + " -r %s /accepteula" % pid
        else:
            cmd = psSuspendPath + " \\\\%s -u %s -p %s -r %s /accepteula" % (
                host, cls.getAdminUser(), cls.getAdminPasswd(), pid
            )
        #run the command locally
        return Machine.runas(
            user=cls.getAdminUser(),
            cmd=cmd,
            host=None,
            cwd=None,
            env=None,
            logoutput=True,
            passwd=cls.getAdminPasswd()
        )

    @classmethod
    def getDBVersion(cls, dbType, host):
        # TODO: Need to implement for Windows
        return None

    # Adds a user into Users group and given grouop.
    # Returns True if successful.
    # You cannot add a user with _ in the name.
    @classmethod
    def addUser(cls, username, group=None, host=None):
        userPasswd = Config.get('machine', 'DEFAULT_PASSWD')
        adminUser = Config.get('machine', 'ROOT_USER')

        cmd = "net user %s %s /add" % (username, userPasswd)
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        if exit_code != 0:
            return False
        # In Windows if group is none. the user is automatically added to Users group when creating.
        if group is None:
            return True
        else:
            #add given group and add user to the group
            #group might exists already.
            cls.addGroup(group, host)
            if cls.addUserToGroup(username, group, host) is False:
                #cls.deleteGroup(group, host)
                #dangling newly created host might exist. It is user responsibilty to clean up.
                cls.deleteUser(username, deleteItsGroupToo=False, host=host)
                return False
            else:
                return True

    # Adds a group.
    # Returns True if successful
    # You cannot add a group with _ in the name.
    @classmethod
    def addGroup(cls, group, host=None):
        adminUser = Config.get('machine', 'ROOT_USER')
        cmd = "net localgroup %s /add" % group
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        return exit_code == 0

    # Deletes a user. By default, deletes group with the same name afterwards.
    # Returns True if successful
    @classmethod
    def deleteUser(cls, username, deleteItsGroupToo=True, host=None):
        adminUser = Config.get('machine', 'ROOT_USER')
        cmd = "net user %s /delete" % (username)
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        if exit_code != 0:
            return False
        if deleteItsGroupToo:
            cls.deleteGroup(group=username, host=host)
        return True

    # Deletes a group.
    # Returns True if successful
    @classmethod
    def deleteGroup(cls, group, host=None):
        adminUser = Config.get('machine', 'ROOT_USER')
        cmd = "net localgroup %s /delete" % group
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        return exit_code == 0

    # Given existing user and existing group, add the user to the group.
    # Returns True if successful
    @classmethod
    def addUserToGroup(cls, username, group, host=None):
        adminUser = Config.get('machine', 'ROOT_USER')
        cmd = "net localgroup %s %s /add" % (group, username)
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        return exit_code == 0

    # Remove the user from the group.
    # Returns True if successful
    @classmethod
    def removeUserFromGroup(cls, username, group, host=None):
        adminUser = Config.get('machine', 'ROOT_USER')
        cmd = "net localgroup %s %s /delete" % (group, username)
        (exit_code, _) = cls.runas(adminUser, cmd, host)
        return exit_code == 0

    #overridden method
    @classmethod
    def runinbackgroundAs(cls, user, cmd, host="", cwd=None, env=None, stdout=None, stderr=None):
        return cls.runinbackground(cls._buildcmdDeprecated(cmd, user, host), cwd, env, stdout=stdout, stderr=stderr)

    #Create temp directory and grant full access to everyone.
    #Returns True if the temp directory does exist after this method runs and chmod does succeed.
    @classmethod
    def createTempDir(cls, host=None):
        tempDir = cls.getTempDir()
        adminUser = cls.getAdminUser()
        cls.makedirs(adminUser, host, tempDir, cls.getAdminPasswd())
        (exit_code, _stdout) = cls.chmod(777, tempDir, recursive=True, user=adminUser, host=host)
        return exit_code == 0

    @classmethod
    def getExportCmd(cls):
        return 'set'

    @classmethod
    def getProcessMemory(cls, pid, host=None, logoutput=True):
        '''
        overridden
        '''
        assert False, "unimplemented"

    @classmethod
    def cat(cls, host=None, path='/', logoutput=True):
        '''
        overridden
        '''
        assert False, "unimplemented"

    @classmethod
    def tail(cls, filename, user=None, passwd=None, host=None, args=None, env=None, logoutput=True):
        '''
        Runs tail command.
        overridden method
        '''
        assert False, "unimplemented"

    @classmethod
    def getWrappedServicePIDRemote(cls, host, service, logoutput=True):
        '''
        WindowsMachine method.

        Returns PID of java process launched by service on remote host.
        Returns None if command does not return valid PID.
        Requires getWrappedServicePIDRemote.cmd file to be placed to <WORKSPACE>\tools
        Requires Psexec.exe to be added to the PATH variable
        '''
        user = cls.getAdminUser()
        passwd = cls.getAdminPasswd()
        out_file_name = "%s_%s.txt" % (host, service)
        ARRIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')
        TMP_DIR = os.path.join("c:\\", "tmp")
        cls.makedirs(user, host, TMP_DIR, passwd)
        script_source = os.path.join(Config.getEnv("WORKSPACE"), "tools", "getWrappedServicePIDRemote.cmd")
        script_dest = os.path.join(TMP_DIR, "getWrappedServicePIDRemote.cmd")
        cls.copyFromLocal(user=user, host=host, srcpath=script_source, destpath=script_dest, passwd=passwd)
        cmd = 'Psexec.exe /accepteula \\\\%s -d %s %s' % (host, script_dest, service)
        _exit_code, _stdout = cls.runas(cmd=cmd, user=user, passwd=passwd, logoutput=logoutput)
        time.sleep(5)
        out_file_path = os.path.join(TMP_DIR, out_file_name)
        local_file_path = os.path.join(ARRIFACTS_DIR, time.strftime('%X').replace(":", "_") + "_" + out_file_name)
        Machine.copyToLocal(user=user, host=host, srcpath=out_file_path, destpath=local_file_path, passwd=passwd)
        if os.path.exists(local_file_path):
            input_ = open(local_file_path).read().decode('UTF-16')
            cls.rm(user=user, host=host, filepath=out_file_path, isdir=False, passwd=passwd)
            cls.rm(user=user, host=host, filepath=script_dest, isdir=False, passwd=passwd)
            stdout2 = unicodedata.normalize('NFKD', input_).encode('ASCII', 'ignore')
            stdout_str = stdout2.splitlines(True)
            if len(stdout_str) > 1:
                last_string = stdout_str[-1]
                last_string_arr = last_string.split(" ")
                PID = last_string_arr[0]
                PID = PID.rstrip(".")
            else:
                logger.info("GETWRAPPEDSERVICEPIDREMOTE: Output file '%s' is empty.", local_file_path)
                PID = ""
        else:
            logger.info("GETWRAPPEDSERVICEPIDREMOTE: Cannot find output file '%s'", local_file_path)
            PID = ""
        if re.match(r'\d+', PID):
            logger.info("GETWRAPPEDSERVICEPIDREMOTE: PID for service '%s' on '%s' : '%s'", service, host, PID)
            return PID
        else:
            logger.info("GETWRAPPEDSERVICEPIDREMOTE: PID for service '%s' on '%s' : not available", service, host)
            return None

    @classmethod
    def ls(  # pylint: disable=unused-argument
            cls, filename, user=None, passwd=None, host=None, args=None, env=None, logoutput=True
    ):
        '''
        Runs ls command.
        overridden method
        '''
        assert False, "unimplemented"

    @classmethod
    def checkCGroupsSupport(cls):
        '''
        not supported in windows. this method should not even be invoked in
        tests that run on windows.
        '''
        assert False, "Do not use this method in Windows"

    @classmethod
    def unzip(cls, zipFile, destDir="", logoutput=True):
        """
        WindowsMachine
        :param zipFile:
        :param destDir:
        :param logoutput:
        :return:
        """
        pass

    @classmethod
    def getFreeDiskSpace(cls):
        pass


# Check whether the specified user is logged on user
# user - user to check
def isLoggedOnUser(user):
    return not user or user == "" or getpass.getuser() == user


if platform.system() == 'Windows':
    Machine = WindowsMachine()
else:
    Machine = LinuxMachine()
