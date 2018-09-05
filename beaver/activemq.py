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
import shutil
import time

from beaver import util
from beaver.config import Config
from beaver.machine import Machine


class ActiveMQ(object):  # pylint: no-init
    _workspace = Config.getEnv('WORKSPACE')
    _javahome = Config.get('machine', 'JAVA_HOME')
    _toolsdir = os.path.join(_workspace, 'tools')
    _activemq_home = os.path.join(_toolsdir, "apache-activemq-5.8.0")
    _downloadurlunix = Config.get('machine', 'ACTIVEMQ_LINUX')
    _downloadurlwin = Config.get('machine', 'ACTIVEMQ_WINDOWS')
    _clientjar = os.path.join(_toolsdir, 'JMSUtil', 'jmsutil-0.1.jar')

    @classmethod
    def start(cls, cfg=None):
        cls.setupActiveMQ()
        cmd = os.path.join("bin", "activemq")
        if Machine.type() == 'Linux':
            cmd += " start"
        if cfg and os.path.isfile(cfg):
            shutil.copyfile(cfg, os.path.join(cls._activemq_home, 'conf', 'activemq-customcfg.xml'))
            cmd += " xbean:conf/activemq-customcfg.xml"
        Machine.runinbackground(cmd, cwd=cls._activemq_home, env={'JAVA_HOME': cls._javahome})
        assert util.waitForPortToOpen(Machine.getfqdn(), 61616)

    @classmethod
    def stop(cls):
        cls.setupActiveMQ()
        if os.path.isdir(cls._activemq_home):
            if Machine.type() == 'Linux':
                cmd = "%s stop" % os.path.join("bin", "activemq")
                _exitcode, output = Machine.run(cmd, cwd=cls._activemq_home, env={'JAVA_HOME': cls._javahome})
                assert output.find("FINISHED") != -1
            else:
                pid = Machine.getPIDByPort(61616)
                if pid:
                    Machine.killProcess(pid, immediate=True)
                    time.sleep(5)

    @classmethod
    def status(cls):
        cls.setupActiveMQ()
        if Machine.type() == 'Linux':
            cmd = "%s status" % os.path.join("bin", "activemq")
            _exitcode, output = Machine.run(cmd, cwd=cls._activemq_home, env={'JAVA_HOME': cls._javahome})
            return output.find("ActiveMQ is running") != -1
        else:
            return Machine.getPIDByPort(61616) is not None

    @classmethod
    def setupActiveMQ(cls):
        # if already downloaded, skip
        if os.path.isdir(cls._activemq_home):
            return

        # download and install
        if Machine.type() == "Linux":
            tarballpath = os.path.join(cls._toolsdir, "apache-activemq-5.8.0-bin.tar.gz")
            if not os.path.isfile(tarballpath):
                assert util.downloadUrl(cls._downloadurlunix, tarballpath)
            Machine.tarExtractAll(tarballpath, cls._toolsdir, mode='r:gz')
        else:
            zippath = os.path.join(cls._toolsdir, "apache-activemq-5.8.0-bin.zip")
            if not os.path.isfile(zippath):
                assert util.downloadUrl(cls._downloadurlwin, zippath)
            Machine.unzipExtractAll(zippath, cls._toolsdir)
        assert os.path.exists(cls._activemq_home)

    @classmethod
    def libPath(cls):
        return os.path.join(cls._activemq_home, 'lib')

    @classmethod
    def runClient(cls, operation, type_, name, message=None, properties=None, logoutput=False):
        if not properties:
            properties = []
        cls.setupActiveMQ()
        classpath = "*" + os.pathsep + cls._clientjar
        run_cmd = os.path.join(cls._javahome, 'bin', 'java') + " -classpath " + classpath + " com.hw.util.JMSUtil"
        run_cmd += " --operation " + operation + " --type " + type_ + " --name " + name
        if message:
            run_cmd += " --message \"" + message + "\""
            for entry in properties:
                run_cmd += " --property \"" + entry + "\""
        return Machine.run(run_cmd, cwd=os.path.join(cls._activemq_home, 'lib'), logoutput=logoutput)

    @classmethod
    def createQueue(cls, name):
        return cls.runClient("create", "queue", name)

    @classmethod
    def createTopic(cls, name):
        return cls.runClient("create", "topic", name)

    @classmethod
    def deleteQueue(cls, name):
        return cls.runClient("delete", "queue", name)

    @classmethod
    def deleteTopic(cls, name):
        return cls.runClient("delete", "topic", name)

    @classmethod
    def sendMessageToQueue(cls, name, message, properties=None):
        if not properties:
            properties = []
        return cls.runClient("message", "queue", name, message, properties=properties)

    @classmethod
    def sendMessageToTopic(cls, name, message, properties=None):
        if not properties:
            properties = []
        return cls.runClient("message", "topic", name, message, properties=properties)
