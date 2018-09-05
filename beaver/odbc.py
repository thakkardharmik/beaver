#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import ConfigParser
import logging
import os
import re
import sys

from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)

LINUX_SYSTEM_DATASOURCES = '/etc/odbc.ini'


class BaseODBC(object):
    def __init__(self):
        pass

    @classmethod
    def addSystemDataSource(cls, name, properties):  # pylint: disable=unused-argument
        # pylint: disable=protected-access
        assert False, "Abstract function %s not implemented." % sys._getframe().f_code.co_name

    @classmethod
    def deleteSystemDataSource(cls, name):  # pylint: disable=unused-argument
        # pylint: disable=protected-access
        assert False, "Abstract function %s not implemented." % sys._getframe().f_code.co_name

    @classmethod
    def installHiveDriver(cls):
        # pylint: disable=protected-access
        assert False, "Abstract function %s not implemented." % sys._getframe().f_code.co_name

    @classmethod
    def getHiveDriverName(cls):
        # pylint: disable=protected-access
        assert False, "Abstract function %s not implemented." % sys._getframe().f_code.co_name


class LinuxODBC(BaseODBC):
    @classmethod
    def addSystemDataSource(cls, name, properties):
        if Machine.pathExists(filepath=LINUX_SYSTEM_DATASOURCES, user=Machine.getAdminUser(),
                              passwd=Machine.getAdminPasswd(), host=None):
            Machine.chmod(
                perm="666",
                filepath=LINUX_SYSTEM_DATASOURCES,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd()
            )
        config = ConfigParser.ConfigParser()
        config.read(LINUX_SYSTEM_DATASOURCES)
        config.add_section(name)
        for n, v in properties.items():
            config.set(name, n, v)
        with open(LINUX_SYSTEM_DATASOURCES, 'w') as f:
            config.write(f)

    @classmethod
    def deleteSystemDataSource(cls, name):
        if Machine.pathExists(filepath=LINUX_SYSTEM_DATASOURCES, user=Machine.getAdminUser(),
                              passwd=Machine.getAdminPasswd(), host=None):
            Machine.chmod(
                perm="666",
                filepath=LINUX_SYSTEM_DATASOURCES,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd()
            )
        config = ConfigParser.ConfigParser()
        config.read(LINUX_SYSTEM_DATASOURCES)
        config.remove_section(name)
        with open(LINUX_SYSTEM_DATASOURCES, 'w') as f:
            config.write(f)

    @classmethod
    def installHiveDriver(cls):
        installUser = Config.get('hadoop', 'HADOOPQA_USER')
        installScript = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'odbc_install_hive_driver.sh')
        if os.path.exists("/usr/lib/hive/lib/native/hiveodbc"):
            logger.info("Hive ODBC driver already installed")
        else:
            logger.info("Installing Hive ODBC Driver")
            python_binary = sys.executable
            command = "bash %s %s %s" % (installScript, installUser, python_binary)
            exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
            )
            assert exit_code == 0, 'Hive ODBC Driver installation failed'
            stdoutLines = stdout.replace("\r", "")
            stdoutLines = stdoutLines.split("\n")
            for line in stdoutLines:
                print line
                match = re.match(r'Installed (.*pyodbc-.*egg)', line)
                if match is not None:
                    break
            pyodbc_location = match.group(1)
            sys.path.append(pyodbc_location)

    @classmethod
    def getHiveDriverName(cls):
        return "/usr/lib/hive/lib/native/Linux-amd64-64/libhortonworkshiveodbc64.so"


class WindowsODBC(BaseODBC):
    # pylint: disable=protected-access,unused-argument
    # TODO: Cleanup atleast the unused arguments
    @classmethod
    def addSystemDataSource(cls, name, properties):
        assert False, "Function %s not implemented in %s" % (sys._getframe().f_code.co_name, cls.__name__)

    @classmethod
    def deleteSystemDataSource(cls, name):
        assert False, "Function %s not implemented in %s" % (sys._getframe().f_code.co_name, cls.__name__)

    @classmethod
    def installHiveDriver(cls):
        assert False, "Function %s not implemented in %s" % (sys._getframe().f_code.co_name, cls.__name__)

    @classmethod
    def getHiveDriverName(cls):
        assert False, "Function %s not implemented in %s" % (sys._getframe().f_code.co_name, cls.__name__)


if Machine.isWindows():
    ODBC = WindowsODBC()
else:
    ODBC = LinuxODBC()

#ODBC.deleteSystemDataSource( 'KnoxHive' )
#props = { 'HOST':'some-host' }
#ODBC.addSystemDataSource( 'KnoxHive', props )
