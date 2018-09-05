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
from beaver import util
from httplib import *
from taskreporter.taskreporter import TaskReporter

import os
import tempfile
import json
import re

USER = Config.getEnv('USER')
SQOOP2_HOME = Config.get("sqoop2", "SQOOP2_HOME")
SQOOP2_SCRIPT = os.path.join(SQOOP2_HOME, "bin/sqoop.sh")
SQOOP2_SERVER = SQOOP2_SCRIPT + " server "
SQOOP2_CLIENT = SQOOP2_SCRIPT + " client "


class Sqoop2:
    @classmethod
    @TaskReporter.report_test()
    def serverStart(cls, cwd=None, env=None, logoutput=True):
        cmd = SQOOP2_SERVER + "start"  #Config.get('sqoop2', 'SQOOP2_CMD')
        osenv = {"JAVA_HOME": Config.get('machine', 'JAVA_HOME')}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            osenv['KRB5CCNAME'] = kerbTicket
            user = None

        if env:
            for key, value in env.items():
                osenv[key] = value
        return Machine.run(cmd, cwd=cwd, env=osenv, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def serverStop(cls, cwd=None, env=None, logoutput=True):
        cmd = SQOOP2_SERVER + "stop"  #Config.get('sqoop2', 'SQOOP2_CMD')
        osenv = {"JAVA_HOME": Config.get('machine', 'JAVA_HOME')}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            osenv['KRB5CCNAME'] = kerbTicket
            user = None

        if env:
            for key, value in env.items():
                osenv[key] = value
        return Machine.run(cmd, cwd=cwd, env=osenv, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def clientOperation(cls, cmd, user=None, host=None, cwd=None, env=None, logoutput=True):
        tfile = tempfile.NamedTemporaryFile(delete=False)
        if host is not None:
            tfile.write("set server --host %s --port 12000\n" % host)
        tfile.write(cmd)
        tfile.close()
        Machine.runas(
            Machine.getAdminUser(), "chown -R %s %s" % (config.Config.get('sqoop2', 'SQOOP2_USER'), tfile.name)
        )
        Machine.runas(Machine.getAdminUser(), "chmod 755 %s" % tfile.name)
        cmd = SQOOP2_CLIENT + tfile.name
        osenv = {"JAVA_HOME": Config.get('machine', 'JAVA_HOME')}
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            osenv['KRB5CCNAME'] = kerbTicket
            #user = None

        if env:
            for key, value in env.items():
                osenv[key] = value

        if user is not None:
            return Machine.runas(user, cmd, cwd=cwd, env=osenv, logoutput=logoutput)
        else:
            return Machine.run(cmd, cwd=cwd, env=osenv, logoutput=logoutput)

    @classmethod
    @TaskReporter.report_test()
    def breakUrl(cls, sqoop_url):
        #converts http://localhost:12000/sqoop/v1 to
        #["localhost:12000", "sqoop/v1"]
        #getting rid of the http:// part
        r = sqoop_url.split("://")
        #splitting the rest
        host, url_part = r[1].split("/", 1)
        return host, "/" + url_part

    @classmethod
    @TaskReporter.report_test()
    def restGET(cls, sqoop_url, request):
        host, url_part = cls.breakUrl(sqoop_url + request)
        conn = HTTPConnection(host)
        conn.request("GET", url_part)
        resp = conn.getresponse()
        body = resp.read()
        return json.loads(body)

    @classmethod
    @TaskReporter.report_test()
    def restPUT(cls, sqoop_url, request):
        host, url_part = cls.breakUrl(sqoop_url + request)
        conn = HTTPConnection(host)
        #TODO incomplete

    @classmethod
    @TaskReporter.report_test()
    def restDELETE(cls, sqoop_url, request):
        host, url_part = cls.breakUrl(sqoop_url + request)
        conn = HTTPConnection(host)
        conn.request("DELETE", url_part)
        resp = conn.getresponse()
        body = resp.read()
        return json.loads(body)

    @classmethod
    @TaskReporter.report_test()
    def restPOST(cls, sqoop_url, request, body):
        host, url_part = cls.breakUrl(sqoop_url + request)
        conn = HTTPConnection(host)
        #TODO incomplete

    @classmethod
    @TaskReporter.report_test()
    def getVersion(cls):
        # determine the sqoop version from the jar that is deployed
        jarDir = os.path.join(Config.get('sqoop2', 'SQOOP2_HOME'), 'shell-lib')
        files = util.findMatchingFiles(jarDir, "sqoop-client-*.jar")
        p = re.compile('sqoop-client-(\S+).jar')
        if files:
            m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    @classmethod
    @TaskReporter.report_test()
    def getDatabaseFlavor(cls):
        SQOOP2_HOME = Config.get("sqoop2", "SQOOP2_HOME")
        SQOOP2_LIB = os.path.join(SQOOP2_HOME, 'sqoop-server', 'lib')
        db = ''
        if (config.find('/usr/share/java', "mysql-connector-java*.jar")):
            db = "mysql"

        # special handling for postgres version
        if (config.find('/usr/share/java', "postgresql91-jdbc.jar")):
            db = 'postgres-9.1'
        elif (config.find(SQOOP_LIB, "postgresql93-jdbc.jar")):
            db = "postgres-9.3"
        elif (config.find('/usr/share/java', "postgresql*.jar")):
            db = "postgres-8"

        if (config.find(SQOOP2_LIB, "ojdbc*.jar")):
            db = "oracle"

        if (config.find(SQOOP2_LIB, "nzjdbc*.jar")):
            if db == '':
                db = "netezza"
            else:
                db += ", netezza"

        return db
