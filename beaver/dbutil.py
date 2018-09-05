#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import fnmatch
import logging
import os

from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)
ROOT_USER = Machine.getAdminUser()
CWD = os.path.dirname(os.path.realpath(__file__))
JDBC_CLIENT = os.path.join(CWD, '..', 'tools', 'DBUtil', 'dbutils-0.1.jar')


class Netezza(object):
    def __init__(self):
        pass

    @classmethod
    def runCmd(cls, cmd, host, database, user, passwd, cwd=None):
        jdbcConnector = Config.get('machine', 'JDBC_CONNECTOR')
        driver = "org.netezza.Driver"
        javaHome = Config.get('machine', 'JAVA_HOME')
        classpath = jdbcConnector + os.pathsep + JDBC_CLIENT
        connectionURL = "jdbc:netezza://%s:5480/%s" % (host, database)
        run_cmd = (
            os.path.join(javaHome, 'bin', 'java') + " com.hw.util.JdbcClient -c \"" + connectionURL + "\" -d " +
            driver + " -u " + user + " -p " + passwd + " -q \"" + cmd + "\""
        )
        return Machine.run(run_cmd, cwd=cwd, env={'CLASSPATH': classpath})


class Oracle(object):
    _envReady = False

    def __init__(self):
        pass

    @classmethod
    def prepareEnv(cls):
        if cls._envReady or Machine.type() == 'Windows':
            return
        if os.path.exists("/var/opt/teradata"):
            oracleHome = "/var/opt/teradata/oracle/product/11.2.0/xe"
        else:
            oracleHome = "/u01/app/oracle/product/11.2.0/xe"
        os.environ["ORACLE_HOME"] = oracleHome
        os.environ["PATH"] += ":%s/bin" % oracleHome
        cls._envReady = True

    @classmethod
    def runJdbcCmd(  # pylint: disable=unused-argument
            cls,
            cmd,
            host="",
            database="",
            user="",
            passwd="",
            cwd=None,
            wallet=False
    ):
        jdbcConnector = ""
        for _root, _dirnames, filenames in os.walk(os.path.join(Config.get("sqoop", "SQOOP_HOME"), 'lib')):
            for filename in fnmatch.filter(filenames, 'ojdbc*.jar'):
                jdbcConnector = os.path.join(Config.get("sqoop", "SQOOP_HOME"), 'lib', filename)
        driver = "oracle.jdbc.driver.OracleDriver"
        javaHome = Config.get('machine', 'JAVA_HOME')
        classpath = jdbcConnector + os.pathsep + JDBC_CLIENT
        if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
            connectionURL = "jdbc:oracle:thin:@%s:1521:%s" % (host, database)
        else:
            connectionURL = "jdbc:oracle:thin:@%s:1521/%s" % (host, database)
        run_cmd = (
            os.path.join(javaHome, 'bin', 'java') + " com.hw.util.JdbcClient -c \"" + connectionURL + "\" -d " +
            driver + " -u " + user + " -p " + passwd + " -q \"" + cmd + "\""
        )
        return Machine.run(run_cmd, cwd=cwd, env={'CLASSPATH': classpath})

    @classmethod
    def runSysCmd(cls, cmd, oracle_host, user=ROOT_USER):
        if Machine.isEC2():
            keypair = "/tmp/ec2-keypair"
        else:
            keypair = "/tmp/ycloud.pem"
        run_cmd = "ssh -i %s -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -t -t %s@%s 'sudo %s'" % (
            keypair, user, oracle_host, cmd
        )
        return Machine.runas(ROOT_USER, run_cmd)

    @classmethod
    def runCmd(  # pylint: disable=unused-argument
            cls,
            cmd,
            host="",
            database="",
            user="",
            passwd="",
            cwd=None,
            wallet=False
    ):

        cls.prepareEnv()
        if database == "":
            database = Config.get('machine', 'ORACLE_DB', default='xe')
        if wallet is False:
            run_cmd = Machine.echocmd(cmd) + " | sqlplus %s/%s@%s" % (user, passwd, database)
            if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
                oracleHome = "/u01/app/oracle/product/12.1.0/dbhome_1/"
                database = 'orcl'
                ORCL_USER = "root"
                if Machine.isEC2():
                    keypair = "/tmp/ec2-keypair"
                else:
                    keypair = "/tmp/ycloud.pem"
                # pylint: disable=line-too-long
                run_cmd = "ssh -i %s -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -t -t %s@%s 'echo exit|sudo ORACLE_SID=ORCL ORACLE_HOME=%s %s/bin/sqlplus %s/%s %s'" % (
                    keypair, ORCL_USER, host, oracleHome, oracleHome, user, passwd, cmd
                )
                # pylint: enable=line-too-long
                host = ""
        else:
            run_cmd = Machine.echocmd(cmd) + " | sqlplus /@w_orcl"
        return Machine.runas(ROOT_USER, run_cmd, host, doEscapeQuote=False)

    @classmethod
    def runAsRoot(cls, cmd, host="", database=""):
        passwd = Config.get('machine', 'ORACLE_ROOT_PASSWD', default='root').strip()
        return cls.runJdbcCmd(cmd, user="system", passwd=passwd, host=host, database=database)

    @classmethod
    def createUser(cls, user, passwd, host="", database=""):
        cls.runAsRoot("CREATE USER %s IDENTIFIED BY %s" % (user, passwd), host=host, database=database)
        cls.grantPrivilege(user, role="DBA", host=host, database=database)

    @classmethod
    def dropUser(cls, user, host="", database=""):
        return cls.runAsRoot("DROP USER %s CASCADE" % user, host=host, database=database)

    @classmethod
    def grantPrivilege(cls, user, role="ALL PRIVILEGES", host="", database=""):
        return cls.runAsRoot("GRANT %s TO %s" % (role, user), host=host, database=database)

    @classmethod
    def backupSchema(cls, backupfile, user, passwd, database='xe'):
        cls.prepareEnv()
        run_cmd = "exp %s/%s@%s file=%s" % (user, passwd, database, backupfile)
        return Machine.run(run_cmd)

    @classmethod
    def importDmp(cls, dmpfile, host="", fromuser="", touser="", touserpasswd="", database='xe'):
        cls.prepareEnv()
        if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
            oracleHome = "/u01/app/oracle/product/12.1.0/dbhome_1/"
            oracleSid = "ORCL"
            database = 'orcl'
            ORCL_USER = "root"
            if Machine.isEC2():
                keypair = "/tmp/ec2-keypair"
            else:
                keypair = "/tmp/ycloud.pem"
            # pylint: disable=line-too-long
            run_cmd = "ssh -i %s -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -t -t %s@%s 'sudo ORACLE_SID=%s ORACLE_HOME=%s %s/bin/imp %s/%s file=%s" % (
                keypair, ORCL_USER, host, oracleSid, oracleHome, oracleHome, touser, touserpasswd, dmpfile
            )
            # pylint: enable=line-too-long
            host = ""
        else:
            run_cmd = "/u01/app/oracle/product/11.2.0/xe/bin/imp %s/%s@%s file=%s" % (
                touser, touserpasswd, database, dmpfile
            )
        if fromuser != "":
            run_cmd += " fromuser=" + fromuser
        if touser != "":
            run_cmd += " touser=" + touser
        if Config.get("sqoop", "SQOOP_RUN_WITH_ORACLE12") == 'true':
            run_cmd += "'"
        return Machine.runas(ROOT_USER, run_cmd, host)


class DB2(object):
    def __init__(self):
        pass

    @classmethod
    def runCmd(cls, cmd, host="", database="", user="", passwd="", cwd=None):  # pylint: disable=unused-argument
        return Machine.run("su %s -c \"%s\" " % (user, cmd), cwd=cwd)

    @classmethod
    def runAsRoot(cls, cmd, cwd=None):
        dbrootuser = Config.get('machine', 'DB2_ROOT_USER', default='db2inst1')
        _passwd = Config.get('machine', 'DB2_ROOT_PASSWD', default='db2inst1').strip()
        cmd = '\\"%s\\"' % cmd
        dbcmd = "su - " + dbrootuser + " -c " + cmd
        print(dbcmd)  # pylint: disable=superfluous-parens
        return Machine.run("sudo sh -c \"%s\"" % (dbcmd), cwd=cwd)

    @classmethod
    def runAsSqoop(cls, cmd, cwd=None):
        cmd = '\\"%s\\"' % cmd
        sqpcmd = "su - sqoop" + " -c " + cmd
        print(sqpcmd)  # pylint: disable=superfluous-parens
        return Machine.run("sudo sh -c \"%s\"" % (sqpcmd), cwd=cwd)

    @classmethod
    def createUser(cls, user, passwd):
        return Machine.run(
            "sudo adduser --disabled-password --gecos \"\" %s;sudo echo -e \"%s\\n%s\" | sudo passwd %s" %
            (user, passwd, passwd, user)
        )

    @classmethod
    def dropUser(cls, user):
        return Machine.run("sudo  deluser %s" % user)

    @classmethod
    def grantAllPrivileges(cls, user, database="", hosts=""):  # pylint: disable=unused-argument
        return cls.runAsRoot(
            "db2 CONNECT TO %s;db2 GRANT CONNECT,ACCESSCTRL,DATAACCESS,DBADM,SECADM ON DATABASE TO USER %s;" %
            (database, user)
        )

    @classmethod
    def grantPrivilege(cls, user, database="", hosts=""):  # pylint: disable=unused-argument
        return cls.runAsRoot(
            "db2 CONNECT TO %s;db2 GRANT CONNECT,ACCESSCTRL,DATAACCESS,DBADM,SECADM ON DATABASE TO USER %s;" %
            (database, user)
        )

    @classmethod
    def recreateDatabase(cls, database):
        cls.runAsRoot("db2 DROP DATABASE %s;" % (database))
        return cls.runAsRoot("db2 CREATE DATABASE %s;" % (database))

    @classmethod
    def createUserWithAllPriveleges(cls, user, passwd, database="", hosts=""):  # pylint: disable=unused-argument
        cls.createUser(user, passwd)
        cls.grantAllPrivileges(user, database=database)

    @classmethod
    def recreateDatabaseAndUser(cls, database, user, passwd):
        cls.recreateDatabase(database)
        cls.dropUser(user)
        cls.createUser(user, passwd)
        cls.grantAllPrivileges(user, database=database)


class MySQL(object):
    def __init__(self):
        pass

    @classmethod
    def runCmd(cls, cmd, host="", database="", user="", passwd="", cwd=None):
        if passwd == "":
            return Machine.run("mysql -h%s -u%s -e \"%s\" %s" % (host, user, cmd, database), cwd=cwd)
        else:
            return Machine.run("mysql -h%s -u%s -p%s -e \"%s\" %s" % (host, user, passwd, cmd, database), cwd=cwd)

    @classmethod
    def runAsRoot(cls, cmd, database=None):
        mysql_cmd = Machine.echocmd(cmd) + " | mysql -u " + Config.get('machine', 'MYSQL_ROOT_USER')
        mysql_root_passwd = Config.get('machine', 'MYSQL_ROOT_PASSWD', default='').strip()
        if mysql_root_passwd != "":
            mysql_cmd += " -p" + mysql_root_passwd
        if database:
            mysql_cmd += " " + database
        return Machine.run(mysql_cmd)

    @classmethod
    def backupDatabase(cls, database, backupfile):
        mysql_cmd = "mysqldump -u " + Config.get('machine', 'MYSQL_ROOT_USER')
        mysql_root_passwd = Config.get('machine', 'MYSQL_ROOT_PASSWD', default='').strip()
        if mysql_root_passwd != "":
            mysql_cmd += " -p" + mysql_root_passwd
        mysql_cmd += " %s > %s" % (database, backupfile)
        return Machine.run(mysql_cmd)

    @classmethod
    def recreateDatabase(cls, database):
        return cls.runAsRoot("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (database, database))

    @classmethod
    def recreateDatabaseAndUser(cls, database, user, passwd, hosts=None, flush=False):
        if not hosts:
            hosts = ['localhost']

        cls.recreateDatabase(database)
        for hostname in hosts:
            cls.dropUser(user, host=hostname)
            cls.createUser(user, passwd, host=hostname)
            cls.grantAllPrivileges(user, host=hostname, database=database)
        if flush:
            cls.flushPriveleges()

    @classmethod
    def createUserWithAllPriveleges(cls, user, passwd, hosts=None, database="*", flush=False, drop=False):
        if not hosts:
            hosts = ['localhost']

        for hostname in hosts:
            if drop:
                cls.dropUser(user, host=hostname)
            cls.createUser(user, passwd, host=hostname)
            cls.grantAllPrivileges(user, host=hostname, database=database)
        if flush:
            cls.flushPriveleges()

    @classmethod
    def createUser(cls, user, passwd, host='localhost'):
        return cls.runAsRoot("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (user, host, passwd))

    @classmethod
    def dropUser(cls, user, host='localhost'):
        return cls.runAsRoot("DROP USER '%s'@'%s';" % (user, host))

    @classmethod
    def grantAllPrivileges(cls, user, host='localhost', database="*"):
        return cls.runAsRoot("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s';" % (database, user, host))

    @classmethod
    def flushPriveleges(cls):
        return cls.runAsRoot("FLUSH PRIVILEGES;")

    @classmethod
    def resetTimeZone(cls, tz='+00:00'):
        return cls.runAsRoot("SET GLOBAL time_zone = '%s';" % tz)


class MSSQL(object):
    @classmethod
    def createLogin(cls, user, passwd):
        return cls.runAsRoot("CREATE LOGIN %s WITH PASSWORD = '%s', check_policy = off;" % (user, passwd))

    @classmethod
    def createDBOwner(cls, database, user):
        return cls.runAsRoot(
            "USE %s; CREATE USER %s FOR LOGIN %s; EXEC sp_addrolemember N'db_owner', N'%s'" %
            (database, user, user, user)
        )

    @classmethod
    def createUserWithAllPriveleges(cls, databases, user):
        for database in databases:
            cls.createDBOwner(database, user)

    @classmethod
    def dropDatabase(cls, database):
        return cls.runAsRoot(
            "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'%s') DROP DATABASE [%s];" %
            (database, database)
        )

    @classmethod
    def dropTable(cls, database, table):
        return cls.runAsRoot(
            "USE %s; IF OBJECT_ID('dbo.%s', 'U') IS NOT NULL DROP TABLE dbo.%s;" % (database, table, table)
        )

    @classmethod
    def grantSysAdmin(cls, user):
        return cls.runAsRoot("EXEC sp_addsrvrolemember @loginame = N'%s', @rolename = N'sysadmin'" % user)

    @classmethod
    def recreateDatabase(cls, database):
        # pylint: disable=line-too-long
        return cls.runAsRoot(
            "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'%s') DROP DATABASE [%s]; CREATE DATABASE %s;"
            % (database, database, database)
        )
        # pylint: enable=line-too-long

    @classmethod
    def restoreDatabaseFromBAK(cls, database):
        bk_file_name_with_extention = database + '.bak'
        logical_file_name = database.split("_")[0]
        bk_dir1 = os.path.join(Config.get('machine', 'MSSQL_BAK_DIR'), bk_file_name_with_extention)
        # pylint: disable=line-too-long
        exit_code, stdout = cls.runAsRoot(
            "RESTORE DATABASE %s FROM DISK='%s' WITH MOVE '%s' TO 'c:\\tmp\\%s.mdf', MOVE '%s_log' TO 'c:\\tmp\\%s.ldf', REPLACE;"
            % (database, bk_dir1, logical_file_name, database, logical_file_name, database)
        )
        # pylint: enable=line-too-long
        if stdout.find("The system cannot find the path specified") != -1:
            bk_dir2 = os.path.join(
                "C:\\Program Files\\Microsoft SQL Server\\MSSQL11.MSSQLSERVER\\MSSQL\\Backup",
                bk_file_name_with_extention
            )
            return cls.runAsRoot("RESTORE DATABASE %s FROM DISK='%s' WITH REPLACE;" % (database, bk_dir2))
        else:
            return exit_code, stdout

    @classmethod
    def dropConnection(cls, database, host):
        user = Config.get('machine', 'MSSQL_ROOT_USER')
        sql_lines = (
            "DECLARE @kill varchar(8000) = '';", "SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'",
            "FROM sys.sysprocesses", "where loginame != '%s'" % user, "EXEC(@kill);"
        )
        sql = '\n'.join(sql_lines)
        passwd = Config.get('machine', 'MSSQL_ROOT_PASSWD', default='').strip()
        return Machine.run('sqlcmd -U%s -P%s -S%s -d%s -Q"%s"' % (user, passwd, host, database, sql))

    @classmethod
    def runCmd(cls, cmd, host="", database="", user="", passwd="", cwd=None):
        return Machine.run('sqlcmd -U%s -P%s -S%s,9433 -d%s -Q"%s"' % (user, passwd, host, database, cmd), cwd=cwd)

    @classmethod
    def runScript(cls, sql_dir, database):
        mssql_cmd = "sqlcmd -U " + Config.get('machine', 'MSSQL_ROOT_USER')
        mssql_root_passwd = Config.get('machine', 'MSSQL_ROOT_PASSWD', default='').strip()
        if mssql_root_passwd != "":
            mssql_cmd += " -P" + mssql_root_passwd
        mssql_host = Config.get('machine', 'MSSQL_HOST', default='').strip()
        if mssql_host != "":
            mssql_cmd += " -S" + mssql_host + ",9433"
        mssql_cmd += ' -i "%s"' % sql_dir
        mssql_cmd += ' -d "%s"' % database
        return Machine.run(mssql_cmd)

    @classmethod
    def runAsRoot(cls, cmd):
        mssql_cmd = Machine.echocmd(cmd) + " | sqlcmd -U " + Config.get('machine', 'MSSQL_ROOT_USER')
        mssql_root_passwd = Config.get('machine', 'MSSQL_ROOT_PASSWD', default='').strip()
        if mssql_root_passwd != "":
            mssql_cmd += " -P" + mssql_root_passwd
        mssql_host = Config.get('machine', 'MSSQL_HOST', default='').strip()
        if mssql_host != "":
            mssql_cmd += " -S" + mssql_host + ",9433"
        return Machine.run(mssql_cmd)


class Postgres(object):
    def __init__(self):
        pass

    @classmethod
    def runSource(cls, file_, host, database, cwd=None):
        return Machine.run(
            "psql -h%s -U%s -d%s -f%s" %
            (host, Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres'), database, file_),
            cwd=cwd
        )

    @classmethod
    def runCmd(cls, cmd, host=None, database=None, user=None, passwd=None):
        if not user:
            user = Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres')
        psql_cmd = Machine.echocmd(cmd) + " | psql -U%s" % user
        if host:
            psql_cmd += " -h" + host
        if database:
            psql_cmd += " -d" + database
        if not passwd:
            passwd = Config.get('machine', 'POSTGRES_ROOT_PASSWD', default='').strip()
        env = {}
        if passwd != "":
            env['PGPASSWORD'] = passwd
        return Machine.run(psql_cmd, env=env)

    @classmethod
    def importDmp(cls, dmpfile, database, host=None):
        pg_cmd = "psql -U%s" % Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres')
        if host:
            pg_cmd += " -h" + host
        pg_cmd += " -d%s -f %s" % (database, dmpfile)
        return Machine.run(pg_cmd)

    @classmethod
    def dropDatabase(cls, database, host):
        return cls.runCmd('DROP DATABASE IF EXISTS %s;' % database, host, 'template1')

    @classmethod
    def recreateDatabase(cls, database):
        return cls.runCmd("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;" % (database, database))

    @classmethod
    def backupDatabase(cls, database, backupfile, host=None):
        pg_cmd = "pg_dump -U%s" % Config.get('machine', 'POSTGRES_ROOT_USER', default='postgres')
        if host:
            pg_cmd += " -h" + host
        pg_cmd += " %s > %s" % (database, backupfile)
        return Machine.run(pg_cmd)

    @classmethod
    def restoreDatabase(cls, file_, host):
        cls.runSource(file_, host, 'template1')

    #return Machine.runas(Config.get('machine', 'POSTGRES_ROOT_USER'),'psql -f %s' % dump_file)

    @classmethod
    def createUser(cls, user, passwd):
        return cls.runCmd("CREATE USER %s WITH PASSWORD '%s';" % (user, passwd))

    @classmethod
    def dropUser(cls, user):
        return cls.runCmd("DROP USER %s;" % user)

    @classmethod
    def grantAllPrivileges(cls, user, database):
        return cls.runCmd("GRANT ALL PRIVILEGES ON DATABASE %s TO %s;" % (database, user))


class Teradata(object):
    def __init__(self):
        pass

    @classmethod
    def runCmd(cls, cmd, host, database, user, passwd, cwd=None):
        # JDBC_CONNECTOR in an installed env could be
        # /usr/hdp/current/sqoop/lib/terajdbc4.jar:/usr/hdp/current/sqoop/lib/tdgssconfig.jar
        jdbcConnector = Config.get('machine', 'JDBC_CONNECTOR')
        driver = "com.teradata.jdbc.TeraDriver"
        javaHome = Config.get('machine', 'JAVA_HOME')
        classpath = jdbcConnector + os.pathsep + JDBC_CLIENT
        logger.info("CLASSPATH: %s", classpath)
        connectionURL = "jdbc:teradata://%s/DATABASE=%s" % (host, database)
        run_cmd = (
            os.path.join(javaHome, 'bin', 'java') + " com.hw.util.JdbcClient -c \"" + connectionURL + "\" -d " +
            driver + " -u " + user + " -p " + passwd + " -q \"" + cmd + "\""
        )
        return Machine.run(run_cmd, cwd=cwd, env={'CLASSPATH': classpath})
