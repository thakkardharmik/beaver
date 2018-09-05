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
import time

from beaver import util
from beaver.config import Config
from beaver.machine import Machine


class ElasticSearch(object):
    _workspace = Config.getEnv('WORKSPACE')
    _javahome = Config.get('machine', 'JAVA_HOME')
    _toolsdir = os.path.join(_workspace, 'tools')
    _downloadurlunix = Config.get('machine', 'ELASTICSEARCH_LINUX')
    _downloadurlwin = Config.get('machine', 'ELASTICSEARCH_WINODWS')
    _es_version = "0.90.6"
    _es_home = os.path.join(_toolsdir, "elasticsearch-%s" % _es_version)
    _es_transport_port = 9300
    _es_http_port = 9200
    _es_http_url = "http://localhost:%d" % _es_http_port
    _lucene_jars_dir = os.path.join(Config.getEnv('WORKSPACE'), 'data', 'flume', 'lucene-4.5.1', 'lib')

    def __init__(self):
        pass

    @classmethod
    def start(cls):
        cls.setupElasticSearch()
        # Start the server with setting not to persist data between restarts
        if Machine.type() == 'Windows':
            cmd = os.path.join(cls._es_home, "bin", "elasticsearch.bat") + " -Des.index.store.type=memory"
        else:
            cmd = os.path.join("bin", "elasticsearch") + " -Des.index.store.type=memory"

        Machine.runinbackground(cmd, cwd=cls._es_home, env={'JAVA_HOME': cls._javahome})
        # Make sure that all required ports are open
        assert util.waitForPortToOpen(Machine.getfqdn(), cls._es_transport_port)
        assert util.waitForPortToOpen(Machine.getfqdn(), cls._es_http_port)
        # Clear any indexes left from previous run
        cls.clearAllIndexes()

    @classmethod
    def stop(cls):
        cls.setupElasticSearch()
        if os.path.isdir(cls._es_home):
            pid = Machine.getPIDByPort(cls._es_transport_port)
            if pid:
                Machine.killProcess(pid)
                time.sleep(2)

    @classmethod
    def setupElasticSearch(cls):
        # if already downloaded, skip
        if os.path.isdir(cls._es_home):
            return

        # download and install
        if Machine.type() == "Linux":
            tarballpath = os.path.join(cls._toolsdir, "elasticsearch-%s.tar.gz" % cls._es_version)
            if not os.path.isfile(tarballpath):
                assert util.downloadUrl(cls._downloadurlunix, tarballpath)
            Machine.tarExtractAll(tarballpath, cls._toolsdir, mode='r:gz')
        else:
            zippath = os.path.join(cls._toolsdir, "elasticsearch-%s.zip" % cls._es_version)
            if not os.path.isfile(zippath):
                assert util.downloadUrl(cls._downloadurlwin, zippath)
            Machine.unzipExtractAll(zippath, cls._toolsdir)
        assert os.path.exists(cls._es_home)

    @classmethod
    def elasticjar(cls):
        if Machine.type() == "Linux":
            return os.path.join(cls._es_home, 'lib', 'elasticsearch-%s.jar:%s/*'
                                ) % (cls._es_version, cls._lucene_jars_dir)
        else:
            flist = [cls._lucene_jars_dir + "\\" + name for name in os.listdir(cls._lucene_jars_dir)]
            files = ';'.join(flist)
            return os.path.join(cls._es_home, 'lib', 'elasticsearch-%s.jar;%s') % (cls._es_version, files)

    @classmethod
    def clearAllIndexes(cls):
        util.httpRequest(cls._es_http_url + "/_all", method='DELETE')

    @classmethod
    def runQuery(cls, qparams=None):
        if not qparams:
            qparams = {}
        searchurl = cls._es_http_url + "/_search"
        if qparams:
            searchurl += "?" + "&".join([key + "=" + value for key, value in qparams.items()])
        code, data, _headers = util.httpRequest(searchurl)
        assert code == 200
        return util.getJSON(data)
