#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.component.hive import Hive
from beaver.machine import Machine
from beaver.config import Config
from beaver import util
import os, re, random, time


class FlumeNG:
    _version = None

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True):
        flume_cmd = Config.get('flume-ng', 'FLUME_CMD')
        flume_cmd += " " + cmd
        if not env:
            env = {}
        # make sure JAVA_HOME is the env
        env['JAVA_HOME'] = Config.get('machine', 'JAVA_HOME')
        return Machine.runas(user, flume_cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True):
        return cls.runas(None, cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def runInBackgroundAs(cls, user, cmd, cwd=None, env=None, runOnHost=None):
        flume_cmd = Config.get('flume-ng', 'FLUME_CMD')
        flume_cmd += " " + cmd
        if not env: env = {}
        env['JAVA_HOME'] = Config.get('machine', 'JAVA_HOME')
        if runOnHost != None:
            if not Machine.isSameHost(runOnHost):
                flume_cmd = "export JAVA_HOME=%s; %s" % (Config.get('machine', 'JAVA_HOME'), flume_cmd)
        return Machine.runinbackgroundAs(user, flume_cmd, cwd=cwd, env=env, host=runOnHost)

    @classmethod
    def runAvroClient(cls, host, port, conffile):
        flume_conf = Config.get('flume-ng', 'FLUME_CONF')
        cmd = "avro-client -H %s -p %s" % (host, port)
        if Machine.type() == 'Linux':
            cmd += " -c %s -F %s" % (flume_conf, conffile)
        else:
            cmd += " -conf %s -filename %s" % (flume_conf, conffile)
        return cls.run(cmd)

    @classmethod
    def getVersion(cls):
        if not cls._version:
            exit_code, output = cls.run("version")
            if exit_code == 0:
                import re
                pattern = re.compile("^Flume (\S+)", re.M)
                m = pattern.search(output)
                if m:
                    cls._version = m.group(1)
            if not cls._version:
                cls._version = " "
        return cls._version

    @classmethod
    def getDatabaseFlavor(cls):
        dbdriver = Hive.getConfigValue("javax.jdo.option.ConnectionDriverName")
        if ("oracle" in dbdriver):
            return "oracle"
        elif ("postgresql" in dbdriver):
            dbUrl = Hive.getConfigValue("javax.jdo.option.ConnectionURL")
            m = re.search('jdbc:postgresql://(.*):.*', dbUrl)
            dbHost = Machine.getfqdn()
            if m and m.group(1):
                dbHost = m.group(1)
            dbVersion = Machine.getDBVersion('postgres', host=dbHost)
            if dbVersion:
                return "postgres-%s" % dbVersion
            else:
                return "postgres"
        elif ("derby" in dbdriver):
            return "derby"
        elif ("mysql" in dbdriver):
            return "mysql"
        return ""


class Agent(FlumeNG):
    _process = None
    _cwd = None
    _env = None
    _mport = None

    def __init__(self, cwd, env=None, monitoring=False):
        self._cwd = cwd
        self._env = env
        if monitoring:
            self._mport = util.getNextAvailablePort(Machine.getfqdn(), 34000)
            self._mport += random.randint(0, 50)

    def start(
            self,
            name,
            conffile,
            flume_conf=None,
            addlClasspath=None,
            addlParams=None,
            enableDebugLogOnConsole=True,
            runOnHost=None,
            GatherAgentLog=False
    ):
        if flume_conf is None:
            flume_conf = Config.get('flume-ng', 'FLUME_CONF')
        cmd = "agent -n %s" % name
        if Machine.type() == 'Linux':
            cmd += " -c %s" % flume_conf
        else:
            cmd += " -conf %s" % flume_conf
        cmd += " -f %s" % conffile
        if addlClasspath:
            cmd += " -C %s" % addlClasspath
        if addlParams:
            cmd += " " + addlParams
        properties = []
        if enableDebugLogOnConsole:
            properties.append("flume.root.logger=DEBUG,console")
        if self._mport:
            properties.append("flume.monitoring.type=http")
            properties.append("flume.monitoring.port=%d" % self._mport)
        if Machine.type() == 'Linux':
            cmd += "".join([" -D" + x for x in properties])
        else:
            cmd += " -property " + ";".join(properties)
        if GatherAgentLog:
            flume_agent_log_file = os.path.join(Machine.getTempDir(), "flumeagent.log")
            cmd = cmd + " > " + flume_agent_log_file + " 2>&1 "
        self._process = self.runInBackgroundAs(None, cmd, cwd=self._cwd, env=self._env, runOnHost=runOnHost)
        if self._mport:
            util.waitForPortToOpen(Machine.getfqdn(), self._mport)

    def waitForSuccessfulEvents(self, metrics, timeout=120, interval=10):
        modmetrics = []
        if not self._mport:
            return
        for metric in metrics:
            items = metric.split()
            assert len(items) == 4, "Invalid metric definition: " + metric
            type = items[0].upper()
            assert type in ('SOURCE', 'CHANNEL',
                            'SINK'), "Invalid metric type '%s' in definition: %s" % (items[0], metric)
            if type == 'SOURCE':
                metricType = 'EventAcceptedCount'
            elif type == 'CHANNEL':
                metricType = 'EventTakeSuccessCount'
            else:
                metricType = 'EventDrainSuccessCount'
            modmetric = "int(jsoncontent['%s.%s']['%s']) %s %s" % (type, items[1], metricType, items[2], items[3])
            modmetrics.append(modmetric)

        starttime = time.time()
        url = "http://%s:%d/metrics" % (Machine.getfqdn(), self._mport)
        while time.time() - starttime < timeout:
            retcode, retdata, retheaders = util.httpRequest(url)
            if retcode == 200:
                jsoncontent = util.getJSON(retdata)
                satisfy = True
                for metric in modmetrics:
                    try:
                        if not eval(metric):
                            satisfy = False
                            break
                    except KeyError:
                        satisfy = False
                if satisfy:
                    return
            time.sleep(interval)

    def isalive(self):
        return self._process and self._process.returncode is None

    def stop(self):
        import time
        if self.isalive():
            if Machine.type() == 'Linux':
                # fix for Debian and ubuntu to stop the process
                if Machine.isDebian() or Machine.isUbuntu():
                    command = "pgrep -f %s -P %d | xargs kill" % (
                        Config.get('flume-ng', 'FLUME_CONF'), self._process.pid
                    )
                    Machine.runas(Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd())
                else:
                    self._process.terminate()
                time.sleep(2)
                Machine.killProcess(self._process.pid)
                time.sleep(5)
            else:
                Machine.killProcessTree(self._process.pid)
                time.sleep(5)

    def stopOnRemotehost(self, runOnHost, pattern):
        if Machine.isLinux():
            pids = Machine.getProcessListRemote(
                runOnHost, format="%U %p %P %a", filter="'%s'" % pattern, user=None, logoutput=True
            )
            pid = Machine.getPidFromString(pids[0], None)
            Machine.killProcessRemote(int(pid), runOnHost, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
        else:
            pid = Machine.getProcessListWithPid(runOnHost, "java.exe", pattern, logoutput=True, ignorecase=True)
            Machine.killProcessRemote(int(pid), runOnHost, Machine.getAdminUser(), Machine.getAdminPasswd(), True)
