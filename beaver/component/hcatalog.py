#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.component.hadoop import Hadoop, HDFS
from beaver import util
from beaver.config import Config
from beaver import configUtils
from beaver.machine import Machine
import os, re, time, logging

logger = logging.getLogger(__name__)


class Hcatalog:
    _hcat_home = Config.get('hcatalog', 'HCATALOG_HOME')
    _hcat_cmd = None
    #  set hcatalog home based on os, because on linux we need to use
    #  the bigtop hcat cmd so hadoop env is set
    if Machine.type() == 'Windows':
        _hcat_cmd = os.path.join(_hcat_home, 'bin', 'hcat.py')
    else:
        _hcat_cmd = 'hcat'

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True):
        hcat_cmd = cls._hcat_cmd + " " + cmd
        #  initialize env
        if not env:
            env = {}
        #  get kerberos ticket
        if Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        return Machine.runas(user, hcat_cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True, user=None):
        return cls.runas(user, cmd, cwd, env, logoutput)

    @classmethod
    def getVersion(cls):
        #  determine the hive version from the jar that is deployed
        jarDir = os.path.join(cls._hcat_home, 'share', 'hcatalog')
        files = util.findMatchingFiles(jarDir, "*hcatalog-core-*.jar")
        p = re.compile('hcatalog-core-(\S+).jar')
        m = p.search(files[0])
        if m:
            return m.group(1)
        else:
            return ""

    #  method to reset HCatalog configs
    @classmethod
    def modifyConfigs(cls, changes):
        #  get the new config location
        name_node = HDFS.getNamenode()
        node_list = [name_node]
        HCAT_CONF_DIR = Config.get('hcatalog', 'HCATALOG_CONF', '/etc/hive-webhcat/conf')
        updatedConf = os.path.join(
            os.path.join(Machine.getTempDir(), 'hcatalogConf_') + str(int(round(time.time() * 1000)))
        )
        configUtils.modifyConfig(changes, HCAT_CONF_DIR, updatedConf, node_list)
        return updatedConf

    @classmethod
    def start(cls, host, hcat_confdir=None):
        if Machine.type() == 'Windows':
            if not hcat_confdir == None:
                HCAT_CONF_DIR = Config.get('hcatalog', 'HCATALOG_CONF')
                Machine.copyFromLocal(
                    Machine.getAdminUser(),
                    host,
                    os.path.join(hcat_confdir, 'webhcat-site.xml'),
                    HCAT_CONF_DIR,
                    passwd=Machine.getAdminPasswd()
                )
            Machine.service("templeton", "start", host=host)
        else:
            env = {}
            if not hcat_confdir == None:
                env['WEBHCAT_CONF_DIR'] = hcat_confdir

            hcat_user = Config.get('hcatalog', 'HCATALOG_USER', 'hcat')
            hcat_bin = os.path.join(Config.get('hcatalog', 'HCATALOG_HOME', '/usr/hdp/current/hive-webhcat'), 'sbin')
            start_cmd = os.path.join(hcat_bin, 'webhcat_server.sh') + ' start'
            exit_code, stdout = Machine.runas(hcat_user, start_cmd, host, env=env, passwd=Machine.getAdminPasswd())
            assert exit_code == 0, "Unable to start webhcat_server.sh on %s \n %s " % (host, start_cmd)

    @classmethod
    def stop(cls, host):
        if Machine.type() == 'Windows':
            Machine.service("templeton", "stop", host=host)
        else:
            hcat_user = Config.get('hcatalog', 'HCATALOG_USER', 'hcat')
            hcat_home = Config.get('hcatalog', 'HCATALOG_HOME', '/usr/hdp/current/hive-webhcat')
            hcat_bin = os.path.join(hcat_home, 'sbin')
            stop_cmd = os.path.join(hcat_bin, 'webhcat_server.sh') + ' stop'

            exit_code, stdout = Machine.runas(hcat_user, stop_cmd, host, passwd=Machine.getAdminPasswd())
            assert exit_code == 0, "Unable to stop webhcat_server.sh on %s \n %s" % (host, stop_cmd)

    @classmethod
    def restart(cls, host, hcat_confdir=None):
        cls.stop(host)
        cls.start(host, hcat_confdir)

    @classmethod
    def getTempletonLogDir(cls, logoutput=False):
        '''
      Returns Templeton log directory (String).
      '''
        returnValue = Config.get('templeton', 'TEMPLETON_LOG_DIR', default='')
        if logoutput:
            logger.info("Hcatalog.getTempletonLogDir returns %s" % returnValue)
        return returnValue

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
        miscTestLogPaths = [
            os.path.join(
                HADOOPQE_TESTS_DIR, "templeton", "src", "test", "e2e", "templeton", "testdist", "test_harnesss_*"
            )
        ]

        if logoutput:
            logger.info("Hcatalog.getMiscTestLogPaths returns %s" % str(miscTestLogPaths))
        return miscTestLogPaths
