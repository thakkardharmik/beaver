#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop
from beaver import config
from taskreporter.taskreporter import TaskReporter
import os


class Sqoop:
    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, sqoopenv=None, logoutput=True, stderr_as_stdout=True):
        sqoop_cmd = Config.get('sqoop', 'SQOOP_CMD')
        sqoop_cmd += " " + cmd
        osenv = {"JAVA_HOME": Config.get('machine', 'JAVA_HOME')}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            osenv['KRB5CCNAME'] = kerbTicket
            user = None

        if env:
            for key, value in env.items():
                osenv[key] = value

        if sqoopenv:
            if Machine.type() == 'Windows':
                sqoopenvcmd = "set SQOOP_CMD_OPTS=" + sqoopenv
                sqoop_cmd = sqoopenvcmd + "&" + sqoop_cmd
            else:
                sqoopenvcmd = "export SQOOP_CMD_OPTS=" + "\"" + sqoopenv + "\""
                sqoop_cmd = sqoopenvcmd + ";" + sqoop_cmd

        if stderr_as_stdout:
            return Machine.runas(user, sqoop_cmd, cwd=cwd, env=osenv, logoutput=logoutput)
        else:
            return Machine.runexas(user, sqoop_cmd, cwd=cwd, env=osenv, logoutput=logoutput)

    @classmethod
    def run(cls, cmd, cwd=None, env=None, sqoopenv=None, logoutput=True, stderr_as_stdout=True):
        return cls.runas(
            None, cmd, cwd=cwd, env=env, sqoopenv=sqoopenv, logoutput=logoutput, stderr_as_stdout=stderr_as_stdout
        )

    @classmethod
    @TaskReporter.report_test()
    def runInBackgroundAs(cls, user, cmd, cwd=None, env=None, sqoopenv=None):
        sqoop_cmd = Config.get('sqoop', 'SQOOP_CMD')
        sqoop_cmd += " " + cmd

        if sqoopenv:
            if Machine.type() == 'Windows':
                sqoopenvcmd = "set SQOOP_CMD_OPTS=" + sqoopenv
                sqoop_cmd = sqoopenvcmd + "&" + sqoop_cmd
            else:
                sqoopenvcmd = "export SQOOP_CMD_OPTS=" + "\"" + sqoopenv + "\""
                sqoop_cmd = sqoopenvcmd + ";" + sqoop_cmd

        return Machine.runinbackgroundAs(user, sqoop_cmd, cwd=cwd, env=env)

    @classmethod
    def runInBackground(cls, cmd, cwd=None, env=None, sqoopenv=None):
        return cls.runInBackgroundAs(None, cmd, cwd=cwd, env=env, sqoopenv=sqoopenv)

    @classmethod
    @TaskReporter.report_test()
    def getJdbcOperationCmd(
            cls, operation, rdbs, database, user, passwd, host, addlargs="", cwd=None, env=None, options=None,
            utf8=False
    ):
        if options:
            options = " %s" % options
            paswdfile_option = "org.apache.sqoop.credentials.loader.class"
            alias_option = "hadoop.security.credential.provider.path"
            if paswdfile_option in options:
                passwd_format = "--password-file"
            elif alias_option in options:
                passwd_format = "--password-alias"
            else:
                passwd_format = "--password"
        else:
            passwd_format = "--password"

        if rdbs == "mysql":
            cmdargs = " --connection-manager org.apache.sqoop.manager.MySQLManager"
            if utf8:
                cmdargs += " --connect \"jdbc:mysql://%s/%s?useUnicode=yes&characterEncoding=UTF-8\"" % (
                    host, database
                )
            else:
                cmdargs += " --connect jdbc:mysql://%s/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "mssql":
            cmdargs = ' --connect "jdbc:sqlserver://%s:9433;databaseName=%s;user=%s;password=%s"' % (
                host, database, user, passwd
            )
        elif rdbs == "oracle":
            if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521:%s" % (host, database)
            else:
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521/%s" % (host, database)
            cmdargs += " --username %s " % user.upper() + passwd_format + " %s" % passwd
        elif rdbs == "oraoop":
            #cmdargs = " --connection-manager org.apache.sqoop.manager.oracle.OraOopConnManager"
            if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521:%s" % (host, database)
            else:
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521/%s" % (host, database)
            cmdargs += " --username %s " % user.upper() + passwd_format + " %s" % passwd + " --direct"
        elif rdbs == "oracle_wallet" or rdbs == "oraoop_wallet":
            cmdargs = " --connect jdbc:oracle:thin:@w_orcl"
        elif rdbs == "teradata":
            cmdargs = " --connect jdbc:teradata://%s/Database=%s" % (host, database)
            cmdargs += " --connection-manager org.apache.sqoop.teradata.TeradataConnManager"
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "db2":
            cmdargs = " --connect jdbc:db2://%s:50000/%s" % (host, database)
            #cmdargs += " --driver com.ibm.db2.jcc.DB2Driver "
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "netezza":
            cmdargs = " --connect jdbc:netezza://%s:5480/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "postgres":
            cmdargs = " --connection-manager org.apache.sqoop.manager.PostgresqlManager"
            cmdargs += " --connect jdbc:postgresql://%s/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd

        if options:
            return str(operation + options + cmdargs + " " + addlargs)
        else:
            return str(operation + cmdargs + " " + addlargs)

    @classmethod
    @TaskReporter.report_test()
    def jdbcOperation(
            cls,
            operation,
            rdbs,
            database,
            user,
            passwd,
            host,
            addlargs="",
            cwd=None,
            env=None,
            options=None,
            utf8=False,
            sqoopenv=None,
            runas=None
    ):
        if options:
            options = " %s" % options
            paswdfile_option = "org.apache.sqoop.credentials.loader.class"
            alias_option = "hadoop.security.credential.provider.path"
            if paswdfile_option in options:
                passwd_format = "--password-file"
            elif alias_option in options:
                passwd_format = "--password-alias"
            else:
                passwd_format = "--password"
        else:
            passwd_format = "--password"

        if rdbs == "mysql":
            cmdargs = " --connection-manager org.apache.sqoop.manager.MySQLManager"
            if utf8:
                cmdargs += " --connect \"jdbc:mysql://%s/%s?useUnicode=yes&characterEncoding=UTF-8\"" % (
                    host, database
                )
            else:
                cmdargs += " --connect jdbc:mysql://%s/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "mssql":
            cmdargs = ' --connect "jdbc:sqlserver://%s:9433;databaseName=%s;user=%s;password=%s"' % (
                host, database, user, passwd
            )
        elif rdbs == "oracle":
            if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521:%s" % (host, database)
            else:
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521/%s" % (host, database)
            cmdargs += " --username %s " % user.upper() + passwd_format + " %s" % passwd
        elif rdbs == "oraoop":
            if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521:%s" % (host, database)
            else:
                cmdargs = " --connect jdbc:oracle:thin:@%s:1521/%s" % (host, database)
            cmdargs += " --username %s " % user.upper() + passwd_format + " %s" % passwd + " --direct"
        elif rdbs == "oracle_wallet" or rdbs == "oraoop_wallet":
            cmdargs = " --connect jdbc:oracle:thin:@w_orcl"
        elif rdbs == "teradata":
            cmdargs = " --connect jdbc:teradata://%s/Database=%s" % (host, database)
            cmdargs += " --connection-manager org.apache.sqoop.teradata.TeradataConnManager"
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "db2":
            cmdargs = " --connect jdbc:db2://%s:50000/%s" % (host, database)
            #cmdargs += " --driver com.ibm.db2.jcc.DB2Driver "
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "netezza":
            cmdargs = " --connect jdbc:netezza://%s:5480/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        elif rdbs == "postgres":
            cmdargs = " --connection-manager org.apache.sqoop.manager.PostgresqlManager"
            cmdargs += " --connect jdbc:postgresql://%s/%s" % (host, database)
            cmdargs += " --username %s " % user + passwd_format + " %s" % passwd
        if options:
            return cls.runas(
                runas, operation + options + cmdargs + " " + addlargs, cwd=cwd, env=env, sqoopenv=sqoopenv
            )
        else:
            return cls.runas(runas, operation + cmdargs + " " + addlargs, cwd=cwd, env=env, sqoopenv=sqoopenv)

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        exit_code, output = cls.run("version")
        if exit_code == 0:
            import re
            pattern = re.compile("^Sqoop (\S+)", re.M)
            m = pattern.search(output)
            if m:
                return m.group(1)
        return ""

    @classmethod
    @TaskReporter.report_test()
    def getDatabaseFlavor(cls):
        SQOOP_HOME = Config.get("sqoop", "SQOOP_HOME")
        SQOOP_LIB = os.path.join(SQOOP_HOME, 'lib')
        db = ''
        if (config.find(SQOOP_LIB, "mysql-connector-java*.jar")):
            db = "mysql"

        # special handling for postgres version
        if (config.find(SQOOP_LIB, "postgresql91-jdbc.jar")):
            db = 'postgres-9.1'
        elif (config.find(SQOOP_LIB, "postgresql93-jdbc.jar")):
            db = "postgres-9.3"
        elif (config.find(SQOOP_LIB, "postgresql*.jar")):
            db = "postgres-8"

        if (config.find(SQOOP_LIB, "db2jcc*.jar")):
            db = "db2"

        if (config.find(SQOOP_LIB, "ojdbc*.jar")):
            db = "oracle"

        if (config.find(SQOOP_LIB, "oraoop*.jar")):
            db = "oraoop"

        if (config.find(SQOOP_LIB, "nzjdbc*.jar")):
            if db == '':
                db = "netezza"
            else:
                db += ", netezza"

        if (config.find(SQOOP_LIB, "sqljdbc*.jar")):
            if db == '':
                db = "mssql"
            else:
                db += ", mssql"
        return db
