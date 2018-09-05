#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import os, socket, time
from beaver.machine import Machine
from beaver.config import Config
from beaver import util


class Solr:
    _workspace = Config.getEnv('WORKSPACE')
    _javahome = Config.get('machine', 'JAVA_HOME')
    _toolsdir = os.path.join(_workspace, 'tools')
    _downloadurl = Config.get('machine', 'SOLAR_LINUX')
    _solr_version = "4.3.1"
    _solr_home = os.path.join(_toolsdir, "solr-%s" % _solr_version, "example")
    _solr_http_port = 8983
    _solr_http_url = "http://localhost:%d/solr" % _solr_http_port

    @classmethod
    def start(cls):
        cls.setupSolr()
        # Start the server with setting not to persist data between restarts
        cmd = os.path.join(cls._javahome, "bin", "java") + " -jar start.jar"
        Machine.runinbackground(cmd, cwd=cls._solr_home, env={'JAVA_HOME': cls._javahome})
        assert util.waitForPortToOpen(Machine.getfqdn(), cls._solr_http_port)

    @classmethod
    def stop(cls):
        cls.setupSolr()
        pid = Machine.getPIDByPort(cls._solr_http_port)
        if pid:
            Machine.killProcess(pid)
            time.sleep(2)

    @classmethod
    def setupSolr(cls):
        # if already downloaded, skip
        if os.path.isdir(cls._solr_home):
            return

        # download and install
        tarballpath = os.path.join(cls._toolsdir, "solr-%s.tgz" % cls._solr_version)
        if not os.path.isfile(tarballpath):
            assert util.downloadUrl(cls._downloadurl, tarballpath)
        Machine.tarExtractAll(tarballpath, cls._toolsdir, mode='r:gz')

    @classmethod
    def _update(cls, data, commit=False):
        updateurl = cls._solr_http_url + "/update"
        if commit: updateurl += "/?commit=true"
        headers = {'Content-Type': 'text/xml', 'charset': 'utf-8'}
        return util.httpRequest(updateurl, headers=headers, data=data)

    @classmethod
    def clearAllIndexes(cls):
        return cls._update("<delete><query>*:*</query></delete>", commit=True)

    @classmethod
    def commit(cls):
        return cls._update("", commit=True)

    @classmethod
    def runQuery(cls, qparams={}):
        searchurl = cls._solr_http_url + "/collection1/select"
        if qparams:
            searchurl += "?" + "&".join([key + "=" + value for key, value in qparams.items()])
        code, data, headers = util.httpRequest(searchurl)
        assert code == 200
        return data
