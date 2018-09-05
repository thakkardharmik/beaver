#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os
import random
import logging
import re
from taskreporter.taskreporter import TaskReporter
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.zookeeper import Zookeeper
from beaver import util

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
ACCUMULO_HOME = Config.get('accumulo', 'ACCUMULO_HOME')
ACCUMULO_CONF_DIR = Config.get('accumulo', 'ACCUMULO_CONF_DIR')
ACCUMULO_CLIENT_CONF = os.path.join(ACCUMULO_CONF_DIR, 'client.conf')
ACCUMULO_USER = Config.get('accumulo', 'ACCUMULO_USER')
ACCUMULO_CMD = Config.get('accumulo', 'ACCUMULO_CMD')
TOOL_SCRIPT = os.path.join(ACCUMULO_HOME, 'bin', 'tool.sh')
ACCUMULO_START_JAR_REGEX = os.path.join(ACCUMULO_HOME, 'lib', 'accumulo-start.*.jar')
ACCUMULO_TEST_JAR = os.path.join(ACCUMULO_HOME, 'lib', 'accumulo-test.jar')

# These must match up with what gsInstaller is configuring
ACCUMULO_ROOT_USER = 'root'
if Hadoop.isAmbari():
    # Ambari sets the Accumulo root user's password to "password"
    ACCUMULO_ROOT_PASSWORD = 'password'
    ACCUMULO_CONF_DIR = "/etc/accumulo/conf/server"
    # Make sure the user running tests can access this directory
    Machine.chmod(
        '755', ACCUMULO_CONF_DIR, recursive=True, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd()
    )
else:
    # gsInstaller sets "secret". When gsInstaller is removed,
    # we can get rid of this branch
    ACCUMULO_ROOT_PASSWORD = 'secret'

# Log some of the important variables
logger.info("ACCUMULO_HOME=%s", ACCUMULO_HOME)
logger.info("ACCUMULO_CONF_DIR=%s", ACCUMULO_CONF_DIR)
logger.info("ACCUMULO_CMD=%s", ACCUMULO_CMD)

ACCUMULO_INSTANCE_NAME = 'accumulo'
ACCUMULO_AMBARI_INSTANCE_NAME = 'hdp-accumulo-instance'

MASTER = "master"
MONITOR = "monitor"
TSERVER = "tserver"
TRACER = "tracer"
GC = "gc"

# Zookeeper hosts
ZOOKEEPERS = ','.join(Zookeeper.getZKHosts(ignoreError=True))

# Continuous ingest parameters
NUM_ENTRIES = Config.get('accumulo', 'CI_NUM_ENTRIES', default=25000000)
MAPPERS = Config.get('accumulo', 'CI_MAPPERS', default=6)
REDUCERS = Config.get('accumulo', 'CI_REDUCERS', default=1)

# Test user
DEFAULT_USER = Config.get('hadoop', 'HADOOPQA_USER')

# Verification MR output directory
CI_OUTPUT_DIR = "ci_output"

PERMISSIONS_LIST = [
    'System.ALTER_NAMESPACE', 'System.ALTER_TABLE', 'System.ALTER_USER', 'System.CREATE_NAMESPACE',
    'System.CREATE_TABLE', 'System.CREATE_USER', 'System.DROP_NAMESPACE', 'System.DROP_TABLE', 'System.DROP_USER',
    'System.GRANT', 'System.OBTAIN_DELEGATION_TOKEN', 'System.SYSTEM'
]

if Machine.type() == 'Windows':
    pass
else:
    ACCUMULO_START_SERVER_CMD = os.path.join(ACCUMULO_HOME, 'bin', 'start-server.sh')
    ACCUMULO_STOP_SERVER_CMD = os.path.join(ACCUMULO_HOME, 'bin', 'stop-server.sh')

if Hadoop.isSecure():
    # Will fully-qualify with realm
    ACCUMULO_SUPERUSER = Machine.get_user_principal('accumulo')
else:
    ACCUMULO_SUPERUSER = ACCUMULO_ROOT_USER


class Accumulo(object):
    @classmethod
    @TaskReporter.report_test()
    def set_home(cls, home):
        global ACCUMULO_HOME, ACCUMULO_CMD, ACCUMULO_CONF_DIR, TOOL_SCRIPT
        global ACCUMULO_TEST_JAR
        ACCUMULO_HOME = home
        ACCUMULO_CMD = os.path.join(ACCUMULO_HOME, 'bin', 'accumulo')
        ACCUMULO_CONF_DIR = os.path.join(ACCUMULO_HOME, 'conf')
        TOOL_SCRIPT = os.path.join(ACCUMULO_HOME, 'bin', 'tool.sh')
        ACCUMULO_TEST_JAR = os.path.join(ACCUMULO_HOME, 'lib', 'accumulo-test.jar')

    @classmethod
    def run(cls, cmd, env=None, logoutput=True):
        user = None
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            # The split on a string that doesn't contain @ is the original string
            kerbTicket = Machine.getKerberosTicket(user.split('@')[0])
            if not kerbTicket:
                raise ValueError('Could not get Kerberos ticket for ' + user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        logger.info('Env for Accumulo run: %s', env)
        return cls.runas(user, cmd=cmd, env=env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, env=None, logoutput=True):
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            # Performs kinit
            # The split on a string that doesn't contain @ is the original string
            kerbTicket = Machine.getKerberosTicket(user.split('@')[0])
            if not kerbTicket:
                raise ValueError('Could not get Kerberos ticket for ' + user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        logger.info('Env for Accumulo run: %s', env)
        return Machine.runas(user, ACCUMULO_CMD + " " + cmd, env=env, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def runas_withinput(cls, user, cmd, input_param, env=None, logoutput=True):
        if not env:
            env = {}
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            # Performs kinit
            # The split on a string that doesn't contain @ is the original string
            kerbTicket = Machine.getKerberosTicket(user.split('@')[0])
            if not kerbTicket:
                raise ValueError('Could not get Kerberos ticket for ' + user)
            env['KRB5CCNAME'] = kerbTicket
            user = None
        logger.info('Env for Accumulo run: %s', env)
        return Machine.runas(user, "echo '%s' | %s" % (input_param, cmd), env=env, logoutput=logoutput)

    @classmethod
    def run_init(cls, logoutput=True):
        return cls.runas_withinput(
            ACCUMULO_USER,
            "%s %s" % (ACCUMULO_CMD, " init --clear-instance-name"),
            "%s\n%s\n%s" % (ACCUMULO_INSTANCE_NAME, ACCUMULO_ROOT_PASSWORD, ACCUMULO_ROOT_PASSWORD),
            logoutput=logoutput
        )

    ###############################################
    # Methods to get a list of hosts for a role
    ###############################################
    @classmethod
    def get_tservers(cls, logoutput=True):
        return cls.get_hosts(os.path.join(ACCUMULO_CONF_DIR, 'slaves'), logoutput=logoutput)

    @classmethod
    def get_masters(cls, logoutput=True):
        return cls.get_hosts(os.path.join(ACCUMULO_CONF_DIR, 'masters'), logoutput=logoutput)

    @classmethod
    def get_tracers(cls, logoutput=True):
        return cls.get_hosts(os.path.join(ACCUMULO_CONF_DIR, 'tracers'), logoutput=logoutput)

    @classmethod
    def get_gcs(cls, logoutput=True):
        return cls.get_hosts(os.path.join(ACCUMULO_CONF_DIR, 'gc'), logoutput=logoutput)

    @classmethod
    def get_monitors(cls, logoutput=True):
        return cls.get_hosts(os.path.join(ACCUMULO_CONF_DIR, 'monitor'), logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def get_hosts(cls, hosts_file, logoutput=True):
        # Shouldn't be necessary, but works around people messing with ambari while the tests are running
        if os.path.exists(hosts_file):
            host = None  # localhost
        else:
            # Ambari gateway does not have any accumulo servers since files are used for test;
            # however, client files can lead us to that server to get other host file details.
            # We use a recursive call to get that other server
            host = cls.get_hosts(os.path.join(Config.get('accumulo', 'ACCUMULO_CONF_DIR'), 'masters'))[0]

        # will chmod either /etc/accumulo/conf/ or /etc/accumulo/conf/servers
        Machine.chmod(
            '755',
            os.path.dirname(hosts_file),
            recursive=True,
            host=host,
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )

        _, hosts_file_list = Machine.cat(
            path=hosts_file, host=host, user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd(), logoutput=logoutput
        )

        hosts = []
        for host in hosts_file_list.split("\n"):
            host = host.strip()
            if re.match(r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)"
                        r"*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$", host):
                hosts.append(host)
        return hosts

    ######################
    # start/stop methods
    ######################
    @classmethod
    @TaskReporter.report_test()
    def start_all(cls, logoutput=True):
        cls.start_monitors(logoutput=logoutput)
        cls.start_tservers(logoutput=logoutput)
        # Set conf dir
        env = {}
        env['ACCUMULO_CONF_DIR'] = ACCUMULO_CONF_DIR
        # Make sure the master knows that it's supposed to come up
        Machine.runas(
            ACCUMULO_USER,
            "%s org.apache.accumulo.master.state.SetGoalState NORMAL" % (ACCUMULO_CMD),
            host=cls.get_masters(logoutput=logoutput)[0],
            env=env,
            logoutput=logoutput
        )
        cls.start_masters(logoutput=logoutput)
        cls.start_gcs(logoutput=logoutput)
        cls.start_tracers(logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_monitors(cls, logoutput=True):
        monitor_hosts = cls.get_monitors(logoutput=logoutput)
        for host in monitor_hosts:
            cls.start_server(MONITOR, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_masters(cls, logoutput=True):
        master_hosts = cls.get_masters(logoutput=logoutput)
        for host in master_hosts:
            cls.start_server(MASTER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_tservers(cls, logoutput=True):
        tserver_hosts = cls.get_tservers(logoutput=logoutput)
        for host in tserver_hosts:
            cls.start_server(TSERVER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_tracers(cls, logoutput=True):
        tracer_hosts = cls.get_tracers(logoutput=logoutput)
        for host in tracer_hosts:
            cls.start_server(TRACER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_gcs(cls, logoutput=True):
        gc_hosts = cls.get_gcs(logoutput=logoutput)
        for host in gc_hosts:
            cls.start_server(GC, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def start_server(cls, server_type, host, logoutput=True):
        env = {}
        if Hadoop.isSecure():
            user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
        # Set conf dir
        env['ACCUMULO_CONF_DIR'] = ACCUMULO_CONF_DIR
        return Machine.runas(
            ACCUMULO_USER,
            "%s %s %s" % (ACCUMULO_START_SERVER_CMD, host, server_type),
            host=host,
            env=env,
            logoutput=logoutput
        )

    @classmethod
    @TaskReporter.report_test()
    def stop_monitors(cls, logoutput=True):
        monitor_hosts = cls.get_monitors(logoutput=logoutput)
        for host in monitor_hosts:
            cls.stop_server(MONITOR, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_masters(cls, logoutput=True):
        master_hosts = cls.get_masters(logoutput=logoutput)
        for host in master_hosts:
            cls.stop_server(MASTER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_tservers(cls, logoutput=True):
        tserver_hosts = cls.get_tservers(logoutput=logoutput)
        for host in tserver_hosts:
            cls.stop_server(TSERVER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_tracers(cls, logoutput=True):
        tracer_hosts = cls.get_tracers(logoutput=logoutput)
        for host in tracer_hosts:
            cls.stop_server(TRACER, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_gcs(cls, logoutput=True):
        gc_hosts = cls.get_gcs(logoutput=logoutput)
        for host in gc_hosts:
            cls.stop_server(GC, host, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_all(cls, env=None, logoutput=True):
        # Issue a graceful stop
        env = {}
        if Hadoop.isSecure():
            user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
        # Set conf dir
        env['ACCUMULO_CONF_DIR'] = ACCUMULO_CONF_DIR
        Machine.runas(
            ACCUMULO_USER,
            "%s admin stopAll" % (ACCUMULO_CMD),
            host=cls.get_masters(logoutput=logoutput)[0],
            env=env,
            logoutput=logoutput
        )

        # Make sure they're all actually killed
        cls.stop_monitors(logoutput=logoutput)
        cls.stop_tservers(logoutput=logoutput)
        cls.stop_masters(logoutput=logoutput)
        cls.stop_gcs(logoutput=logoutput)
        cls.stop_tracers(logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def stop_server(cls, server_type, host, logoutput=True):
        env = {}
        if Hadoop.isSecure():
            user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
        # Manually get the pid, don't care about trying to find the "right" pid (if there were multiple instances
        # running on the node) but try to avoid slider instances
        _, stdout = Machine.runas(
            ACCUMULO_USER,
            r"ps aux | fgrep accumulo | fgrep -v grep | fgrep -v 'application_' | fgrep \"%s\" | awk '{print \$2}'" %
            server_type,
            host=host,
            env=env,
            logoutput=logoutput
        )
        # For each pid, kill -9 it
        for pid in stdout.split('\n'):
            pid = pid.strip()
            # Make sure that we actually got a non-empty string
            if pid:
                Machine.runas(
                    ACCUMULO_USER, "kill -9 %s" % pid, host=host, env=env, logoutput=logoutput
                )

    # TODO Expand Accumulo classmethods to accept instance name and zookeepers
    # to help avoid duplciation of Accumulo/Accumulo-Slider methods
    @classmethod
    def dropTable(
            cls,
            tablename,
            logoutput=True,
            user=None,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        cls.run_shellCommand(
            "deletetable -f %s" % (tablename),
            logoutput=logoutput,
            user=user,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def getEntries(cls, tablename, accumuloUser=ACCUMULO_SUPERUSER, accumuloPassword=ACCUMULO_ROOT_PASSWORD):
        exit_code, output = cls.run_shellCommand(
            'scan -t %s -np' % tablename,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword,
            logoutput=False
        )
        if exit_code == 0:
            entries = output.split('\n')
            # Remove lines which are empty, whitespace only or contain a log4j level
            entries = [
                entry for entry in entries
                if not entry.isspace() and not entry == '' and not 'FATAL' in entry and not 'ERROR' in entry
                and not 'WARN' in entry and not 'INFO' in entry and not 'DEBUG' in entry and not 'TRACE' in entry
            ]
            logger.debug('Records from %s', tablename)
            for entry in entries:
                logger.debug(entry)
            return entries
        else:
            logger.warn('Could not scan %s, output: %s', tablename, output)
            return None

    @classmethod
    @TaskReporter.report_test()
    def countEntries(cls, tablename, accumuloUser=ACCUMULO_SUPERUSER, accumuloPassword=ACCUMULO_ROOT_PASSWORD):
        entries = cls.getEntries(tablename, accumuloUser=accumuloUser, accumuloPassword=accumuloPassword)
        if entries is not None:
            return len(entries)
        else:
            return -1

    @classmethod
    def createTable(
            cls,
            tablename,
            logoutput=True,
            user=None,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        cls.run_shellCommand(
            ["createtable %s" % (tablename)],
            logoutput=logoutput,
            user=user,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def dropAndCreateTable(
            cls,
            tablename,
            logoutput=True,
            user=None,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        # Drop the table as the root user to avoid permission issues
        cls.dropTable(tablename, logoutput=logoutput, user=user)
        cls.createTable(
            tablename, logoutput=logoutput, user=user, accumuloUser=accumuloUser, accumuloPassword=accumuloPassword
        )

    @classmethod
    def getTableColumnValues(cls, tablename, columnFamily, column):
        pass

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(
            os.path.join(ACCUMULO_CONF_DIR, "accumulo-site.xml"), propertyValue, defaultValue=defaultValue
        )

    @classmethod
    def resetService(cls, user):
        pass

    @classmethod
    def isInstalled(cls):
        return cls.getVersion() != ""

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        exit_code, output = cls.run("version", env={'JAVA_HOME': Config.get('machine', 'JAVA_HOME')})
        if exit_code == 0:
            lines = output.split('\n')
            if len(lines) > 1:
                output = lines[-1]
            return output
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getTableId(cls, tableName):
        """
        Get the Accumulo table ID for the given table name. None if the table doesn't exist
        """
        exit_code, output = cls.run_shellCommand("tables -l")

        if exit_code == 0:
            # output is multiple lines of the form "tablename<whitespace>=><whitespace>tableId"
            pattern = re.compile(r"(\S+)\s+(\S+)\s+(\S+)")
            lines = output.split('\n')
            for line in lines:
                match = pattern.search(line)
                if match and tableName == match.group(1):
                    return match.group(3)
        return None

    @classmethod
    def getTempFilepath(cls):
        return os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'accumulo-tmp-%d' % int(999999 * random.random()))

    @classmethod
    @TaskReporter.report_test()
    def run_shellCommand(
            cls,
            cmd,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD,
            instanceName=None,
            zookeepers=None
    ):
        cmds = []

        # If we got something that acts like a list, use that list
        if not isinstance(cmd, basestring):
            cmds = cmd
        else:
            # Otherwise, add in the command to the list
            cmds.append(cmd)

        if cmds[-1] != 'exit':
            logger.debug('Appending "exit" command to list of commands')
            cmds.append('exit')

        return cls.run_shellCommands(
            cmds,
            user,
            logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword,
            instanceName=instanceName,
            zookeepers=zookeepers
        )

    @classmethod
    @TaskReporter.report_test()
    def run_shellCommands(
            cls,
            cmds,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD,
            instanceName=None,
            zookeepers=None
    ):
        """
        Runs a series of commands within the Accumulo shell.

        Commands are written to a file to avoid starting a JVM for every
        provided command. By default, commands are run as the Accumulo root
        user.
        """
        # Dump the commands into a file
        tfpath = cls.getTempFilepath()
        logger.info("Writing commands to file %s ", tfpath)
        tf = open(tfpath, 'w')
        for cmd in cmds:
            logger.debug(" '%s'", cmd)
            tf.write(cmd + "\n")
        tf.write("exit\n")
        tf.close()
        logger.info("Executing commands in %s", tfpath)

        user_password_flag = ""
        if not Hadoop.isSecure():
            user_password_flag = "-u %s -p %s" % (accumuloUser, accumuloPassword)
        if instanceName is None or zookeepers is None:
            instance_zk_flag = ""
        else:
            instance_zk_flag = "-zi %s -zh %s" % (instanceName, zookeepers)
        # Run the shell with the commands in a file as an argument
        return cls.runas(user, "shell %s %s -f %s" % (instance_zk_flag, user_password_flag, tfpath), None, logoutput)

    @classmethod
    @TaskReporter.report_test()
    def config(
            cls, cmd, user=None, logoutput=True, accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        cmds = cls.commandsAsList(cmd)
        return cls.run_shellCommands(
            ['config %s' % (c) for c in cmds],
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def tableConfig(
            cls,
            table,
            cmd,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        cmds = cls.commandsAsList(cmd)
        return cls.run_shellCommands(
            ['config -t %s %s' % (table, c) for c in cmds],
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def commandsAsList(cls, cmd):
        """
        Ensures that a list of commands is always returned.

        If a single command was provided, it is wrapped in a list. If something that
        acts as a list was provided, that object is return as-is.
        """
        # If we got something that acts like a list, use that list
        if not isinstance(cmd, basestring):
            return cmd
        else:
            # Otherwise, make a list with the single command
            return [cmd]

    @classmethod
    def grantWrite(
            cls,
            table,
            grantUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Grants the write permission to grantUser on the given table
        """

        return cls.grant(
            table,
            grantUser,
            'Table.WRITE',
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def grantRead(
            cls,
            table,
            grantUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Grants the read permission to grantUser on the given table
        """

        return cls.grant(
            table,
            grantUser,
            'Table.READ',
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def grantAlter(
            cls,
            table,
            grantUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Grants the alter table permission to grantUser on the given table
        """

        return cls.grant(
            table,
            grantUser,
            'Table.ALTER_TABLE',
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def grant(
            cls,
            table,
            grantUser,
            permission,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Grants the given permission to grantUser on the given table
        """

        return cls.run_shellCommand(
            'grant -t %s -u %s %s' % (table, grantUser, permission),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def grantSystem(
            cls,
            grantUser,
            permission,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Grants the user the given system permission
        """

        return cls.run_shellCommand(
            'grant -u %s -s %s' % (grantUser, permission),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def revokeWrite(
            cls,
            table,
            revokeUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Revokes the write permission to revokeUser on the given table
        """

        return cls.revoke(
            table,
            revokeUser,
            'Table.WRITE',
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def revokeRead(
            cls,
            table,
            revokeUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Revokes the read permission to revokeUser on the given table
        """

        return cls.revoke(
            table,
            revokeUser,
            'Table.READ',
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def revoke(
            cls,
            table,
            revokeUser,
            permission,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Revokes the given permission from revokeUser on the given table
        """

        return cls.run_shellCommand(
            'revoke -t %s -u %s %s' % (table, revokeUser, permission),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    def dropUser(
            cls,
            dropUserName,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Drops the Accumulo user with the given name
        """

        return cls.run_shellCommand(
            'dropuser -f %s' % (dropUserName),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def createUser(
            cls,
            createUserName,
            password,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Creates an Accumulo user with the given name
        """

        if Hadoop.isSecure():
            return cls.runas(user, 'shell -e \'createuser %s\'' % (createUserName), logoutput=logoutput)
        else:
            # Pipe the password (twice) into `accumulo shell -u root -p password -e "createuser username"`
            return cls.runas_withinput(
                user,
                '%s shell -u %s -p %s -e \'createuser %s\'' %
                (ACCUMULO_CMD, accumuloUser, accumuloPassword, createUserName),
                '%s\n%s\n' % (password, password),
                logoutput=logoutput
            )

    @classmethod
    @TaskReporter.report_test()
    def dropAndCreateUser(
            cls,
            userName,
            password,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Drop and then create the given user
        """

        cls.dropUser(
            userName, user=user, logoutput=logoutput, accumuloUser=accumuloUser, accumuloPassword=accumuloPassword
        )
        cls.createUser(
            userName,
            password,
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def getInstanceName(cls):
        """
        Get the instance name for Accumulo
        """
        if Hadoop.isAmbari():
            return ACCUMULO_AMBARI_INSTANCE_NAME
        return ACCUMULO_INSTANCE_NAME

    @classmethod
    @TaskReporter.report_test()
    def getRootUser(cls):
        """
      The Accumulo user shortname with superuser/admin privileges. 'root' for unsecure
      and whatever Ambari/gsInstaller configures for secure.
      """
        if Hadoop.isSecure():
            # Will fully-qualify with realm
            return 'accumulo'
        return ACCUMULO_ROOT_USER

    @classmethod
    @TaskReporter.report_test()
    def getRootPrincipal(cls):
        """
      The Accumulo user with superuser/admin privileges. 'root' for unsecure
      and whatever Ambari/gsInstaller configures for secure.
      """
        if Hadoop.isSecure():
            # Will fully-qualify with realm
            return Machine.get_user_principal('accumulo')
        return ACCUMULO_ROOT_USER

    @classmethod
    def getRootPassword(cls):
        """
        Returns the root user's password
        """

        return ACCUMULO_ROOT_PASSWORD

    @classmethod
    @TaskReporter.report_test()
    def get_superuser_keytab(cls):
        """
        Get they keytab for the admin user. Raises an error is security is not enabled
        """
        if not Hadoop.isSecure():
            raise ValueError("Security is not enabled!")
        principal = cls.getRootPrincipal()
        if '@' in principal:
            principal = principal.split('@')[0]
        # principal needs to just be the primary, not the primary/[instance]@REALM
        return Machine.getHeadlessUserKeytab(principal)

    @classmethod
    @TaskReporter.report_test()
    def get_auth_options(cls, password=None, keytab=None):
        """
        Command line options for the Accumulo tests given unsecure or secure
        """
        if Hadoop.isSecure():
            if keytab is not None:
                return '--keytab %s' % keytab
            else:
                # In secure mode, shouldn't have to pass in a keytab if you're kinit'ed
                return ''
        if password is None:
            password = ACCUMULO_ROOT_PASSWORD
        return '-p %s' % password

    @classmethod
    @TaskReporter.report_test()
    def addSplits(
            cls,
            table,
            splits,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_SUPERUSER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Add split points to the given table
        """

        tfpath = cls.getTempFilepath()
        logger.info("Writing splits to file %s", tfpath)
        tf = open(tfpath, 'w')
        for split in splits:
            tf.write(split + "\n")
        tf.close()

        # Run the shell with the commands in a file as an argument
        if Hadoop.isSecure():
            return cls.run_shellCommand(
                "addsplits -t %s -sf %s" % (table, tfpath),
                user=user,
                accumuloUser=accumuloUser,
                accumuloPassword=accumuloPassword,
                logoutput=logoutput
            )
        else:
            return cls.runas(
                user, "shell -u %s -p %s -e 'addsplits -t %s -sf %s'" %
                (accumuloUser, accumuloPassword, table, tfpath), None, logoutput
            )

    @classmethod
    @TaskReporter.report_test()
    def get_auths(
            cls,
            targetUser,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_ROOT_USER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Fetch the authorizations for a given user.

        `accumuloUser` must have the System.ALTER_USER property.

        Returns a list of Authorizations the `targetUser` has, if any.
        """
        exit_code, output = cls.run_shellCommand(
            'getauths -u %s' % targetUser, user=user, logoutput=logoutput,
            accumuloUser=accumuloUser, accumuloPassword=accumuloPassword
        )
        assert exit_code == 0
        output = output.strip()

        if not output:
            # Handle no auths
            return []
        else:
            auths = []
            for auth in output.split(',')[1:]:
                if not ("INFO" in auth or "WARN" in auth or "ERROR" in auth):
                    auths.append(auth)
            return auths

    @classmethod
    @TaskReporter.report_test()
    def add_auth(
            cls,
            targetUser,
            auth,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_ROOT_USER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Add a new authorization to a user. Attempts to add an already assigned authorization will succeed.

        `accumuloUser` must have the System.ALTER_USER property.
        """
        # Get the existing authorizations
        auths = cls.get_auths(
            targetUser, user=user, logoutput=logoutput, accumuloUser=accumuloUser, accumuloPassword=accumuloPassword
        )
        new_auths = None
        if not auths:
            new_auths = auth
        else:
            # Compute the new authorization string, given the user input.
            new_auths = '%s,%s' % (','.join(auths), auth)
        # setauths with the new comma-separated authorizations
        cls.run_shellCommand(
            'setauths -u %s -s %s' % (targetUser, new_auths),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def set_auths(
            cls,
            targetUser,
            auths,
            user=None,
            logoutput=True,
            accumuloUser=ACCUMULO_ROOT_USER,
            accumuloPassword=ACCUMULO_ROOT_PASSWORD
    ):
        """
        Sets the authorizations for a user (destructive).
        Replaces any existing authorizations that are assigned to a user.
        """
        new_auths = None
        if not isinstance(auths, basestring):
            # Join elements of the list
            new_auths = ','.join(auths)
        else:
            new_auths = auths

        # Issue the setauths with the new authorizations
        cls.run_shellCommand(
            'setauths -u %s -s %s' % (targetUser, new_auths),
            user=user,
            logoutput=logoutput,
            accumuloUser=accumuloUser,
            accumuloPassword=accumuloPassword
        )

    @classmethod
    @TaskReporter.report_test()
    def getAccumuloLogDir(cls, logoutput=False):
        """
        Returns Accumulo log directory (String).
        """
        returnValue = Config.get('accumulo', 'ACCUMULO_LOG_DIR', default='')
        if logoutput:
            logger.info("Accumulo.getAccumuloLogDir returns %s", returnValue)
        return returnValue

    @classmethod
    @TaskReporter.report_test()
    def getAccumuloHome(cls, logoutput=False):
        """
       Returns ACCUMULO_HOME (String).
       """
        retval = Config.get('accumulo', 'ACCUMULO_HOME', default='')
        if logoutput:
            logger.info('Accumulo.getAccumuloHome returns %s', retval)
        return retval

    @classmethod
    @TaskReporter.report_test()
    def getAccumuloConfDir(cls, logoutput=False):
        """
       Returns ACCUMULO_CONF_DIR (String).
       """
        if logoutput:
            logger.info('Accumulo.getAccumuloConfDir returns %s', ACCUMULO_CONF_DIR)
        return ACCUMULO_CONF_DIR

    @classmethod
    @TaskReporter.report_test()
    def grant_permissions(  # pylint: disable=W0102
            cls, user, accumulo_home=ACCUMULO_HOME, client_config=ACCUMULO_CLIENT_CONF,
            permissions=PERMISSIONS_LIST
    ):
        """
        Grant a set of permissions to the provided user. Meant to service the MIT+AD use case
        where Accumulo is initialized with a user in one realm while the tests run as a user
        in a separate realm.
        """
        # Security is enabled and the user realm isn't the normal EXAMPLE.COM
        if Hadoop.isSecure() and Config.get('machine', 'USER_REALM') != 'EXAMPLE.COM':
            krbTicket = Accumulo.get_kerberos_ticket(user)
            if not krbTicket:
                raise ValueError('Could not get Kerberos service ticket for %s' % user)
            env = {}
            env['KRB5CCNAME'] = krbTicket
            accumulo_cmd = os.path.join(accumulo_home, 'bin', 'accumulo')
            user_realm = Machine.get_user_realm()
            qualified_user = "%s@%s" % (user, user_realm)
            # Shortcircuit the logic that would run w/ special kerberos credentials
            Machine.runas(
                None,
                "%s shell --config-file %s -e 'createuser %s'" % (accumulo_cmd, client_config, qualified_user),
                env=env,
                logoutput=True
            )
            for perm in permissions:
                Machine.runas(
                    None,
                    "%s shell --config-file %s -e 'grant -s %s -u %s'" %
                    (accumulo_cmd, client_config, perm, qualified_user),
                    env=env,
                    logoutput=True
                )

    @classmethod
    @TaskReporter.report_test()
    def get_kerberos_ticket(cls, user):
        """
        Create a cached Kerberos ticket for the service user instead of the test user. Useful
        for MIT+AD deployments.
        """
        # Copied from machine.py, uses the service keytab and realm instead of user variants
        kinitloc = Machine.getKinitCmd()
        kerbTicketDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'kerberosServiceTickets')
        if not os.path.exists(kerbTicketDir):
            os.mkdir(kerbTicketDir)
        kerbTicket = os.path.join(kerbTicketDir, "%s.kerberos.ticket" % user)

        # If it already exists, return it
        if os.path.isfile(kerbTicket):
            return kerbTicket

        keytabFile = os.path.join(Machine.getServiceKeytabsDir(), "%s.headless.keytab" % user)
        if not os.path.isfile(keytabFile):
            raise ValueError('Could not find expected keytab: %s' % keytabFile)

        # Make sure we can actually read the keytab
        Machine.chmod('444', keytabFile, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

        # Get service username from keytab file
        cmd = "%s -k -t %s " % ("klist", keytabFile)
        exit_code, output = Machine.run(cmd)
        user = str(((output.split(os.linesep)[-1]).split(' ')[-1]))
        logger.info("Username is %s", user)

        # get REALM from machine configs
        cmd = "%s -c %s -k -t %s %s" % (kinitloc, kerbTicket, keytabFile, '%s' % user)
        exit_code, output = Machine.run(cmd)
        if exit_code == 0:
            return kerbTicket
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getMiscTestLogPaths(cls, logoutput=False):
        HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
        miscTestLogPaths = [
            os.path.join(HADOOPQE_TESTS_DIR, "artifacts", "accumulo"),
            os.path.join(HADOOPQE_TESTS_DIR, "artifacts", "tmp_slider_*"),
        ]
        if logoutput:
            logger.info("Accumulo.getMiscTestLogPaths returns %s", miscTestLogPaths)
        return miscTestLogPaths


class KeyValue(object):
    def __init__(self, data, has_timestamp=False):
        """
        Accepts a line of data, typically from the output of the `scan` command in the Accumulo shell,
        and attempts to parse it into the Key-Value components.

        Parsing will only occur as intended when there are no spaces in the Key or Value components
        """
        components = data.strip().split(None, 3 if has_timestamp else 4)
        cfcq = components[1].split(':')

        self.rowid = components[0]
        self.cf = cfcq[0]
        self.cq = cfcq[1]
        self.cv = components[2][1:-1]
        if has_timestamp:
            self.ts = components[3]
            self.value = components[4]
        else:
            self.ts = None
            self.value = components[3]

    def get_row(self):
        return self.rowid

    def get_column_family(self):
        return self.cf

    def get_column_qualifier(self):
        return self.cq

    def get_column_visibility(self):
        return self.cv

    def get_timestamp(self):
        return self.ts

    def get_value(self):
        return self.value


class ContinuousIngest(object):
    """
    A very thorough Accumulo system test which is capable of stressing ingest, query,
    durability and availability of Accumulo. A verification MapReduce job can be run
    after ingest is complete to identify any missing or inaccurate data.
    """

    def __init__(
            self,
            instance_name,
            user=None,
            shell_user=ACCUMULO_SUPERUSER,
            shell_password=ACCUMULO_ROOT_PASSWORD,
            shell_user_keytab=None,
            table="ci",
            zookeepers=ZOOKEEPERS
    ):
        """
        Creates a table for continuous ingest and add some basic splits
        """
        self.user = user
        if user is None:
            self.user = DEFAULT_USER
        self.instance_name = instance_name
        self.shell_user = shell_user
        self.table = table
        self.zookeepers = zookeepers

        # create table used for test
        password_or_keytab_flag = "-p %s" % shell_password
        if Hadoop.isSecure():
            if shell_user_keytab is None:
                raise ValueError("shell_user_keytab must be specified")
            password_or_keytab_flag = "--keytab %s" % shell_user_keytab
        self.password_or_keytab_flag = password_or_keytab_flag
        Accumulo.run_shellCommands(
            ["deletetable -f %s" % table,
             "createtable %s" % table,
             "addsplits 1 2 3 4 5 6 7 8 9 -t %s" % table],
            user=self.user,
            accumuloUser=shell_user,
            accumuloPassword=shell_password,
            instanceName=instance_name,
            zookeepers=zookeepers
        )

    @TaskReporter.report_test()
    def ingest(self):
        """
        Ingests entries into Accumulo.
        Returns returncode, stdout.
        """
        ingest_command = "org.apache.accumulo.test.continuous.ContinuousIngest " \
                         "-i %s -z %s -u %s %s --table %s --num %s --min 0 " \
                         "--max 9223372036854775807 --maxColF 32767 --maxColQ 32767 " \
                         "--batchMemory 100000000 --batchLatency 600000 " \
                         "--batchThreads 4 --addCheckSum" \
                         % (self.instance_name, self.zookeepers, self.shell_user,
                            self.password_or_keytab_flag, self.table, NUM_ENTRIES)
        return Accumulo.runas(self.user, ingest_command)

    @TaskReporter.report_test()
    def verify(self):
        """
        Runs verification MapReduce job.
        Returns returncode, stdout.
        """
        HDFS.deleteDirectory(CI_OUTPUT_DIR, user=self.user)
        verify_command = "%s %s " \
                         "org.apache.accumulo.test.continuous.ContinuousVerify " \
                         "-libjars %s -i %s -z %s -u %s %s --table %s " \
                         "--output %s --maxMappers %s --reducers %s" \
                         % (TOOL_SCRIPT, ACCUMULO_TEST_JAR, ACCUMULO_TEST_JAR, self.instance_name,
                            self.zookeepers, self.shell_user, self.password_or_keytab_flag,
                            self.table, CI_OUTPUT_DIR, MAPPERS, REDUCERS)
        env = {}
        if Hadoop.isSecure():
            # The split on a string that doesn't contain @ is the original string
            kerbTicket = Machine.getKerberosTicket(self.user.split('@')[0])
            if not kerbTicket:
                raise ValueError('Could not get Kerberos ticket for ' + self.user)
            env['KRB5CCNAME'] = kerbTicket
        logger.info('Env for verify run: %s', env)
        return Machine.run(verify_command, env=env)

    @staticmethod
    @TaskReporter.report_test()
    def assert_expected_output(output):
        """
        Utility method for determining verification output is as expected.
        """
        pattern = re.compile("UNDEFINED", re.M)
        m = pattern.search(output)
        assert not m, "Found UNDEFINED counter in test output"
        pattern = re.compile("CORRUPT", re.M)
        m = pattern.search(output)
        assert not m, "Found CORRUPT counter in test output"
        pattern = re.compile("REFERENCED=", re.M)
        m = pattern.search(output)
        assert m, "Didn't find expect REFERENCED counter in test output"
