#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, time, logging
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.slider import Slider
from beaver.component.hbase import HBase
from beaver import util
import re

logger = logging.getLogger(__name__)


class SliderHBase:
    _isSecure = None
    _version = None
    # default the hadoop version to 2
    _isHadoop2 = True
    _clusterName = "hbasesliderapp"
    HBASE_VER = None

    @classmethod
    def setClusterName(cls, clName):
        '''
      Sets cluster name which would be used in subsequent method calls
      '''
        cls._clusterName = clName

    @classmethod
    def get_hbase_site(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Returns hbase-site.xml as string.
      '''
        am_url = Slider.get_info_am_web_url(clusterName, user, logoutput=logoutput)

        attempt = 0
        hbase_site = ""
        while attempt < max_attempts:
            hbase_site = util.getHTTPResponse("XML", am_url + "ws/v1/slider/publisher/slider/hbase-site.xml")
            if hbase_site != "":
                break
            attempt += 1
            time.sleep(sleep_duration)
        if hbase_site == "":
            raise Exception("Never able to read hbase_site")
        if logoutput:
            logger.info("Read hbase_site")
        return util.toString("XML", hbase_site)

    @classmethod
    def get_rest_server(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Gets name of REST gateway for the cluster.
      '''
        if clusterName is None:
            clusterName = cls._clusterName
        links = Slider.get_quicklinks(clusterName, user, logoutput, max_attempts, sleep_duration)
        if not links is None:
            url = links["entries"]["org.apache.slider.hbase.rest"]
            if not url is None:
                host = url.split(':')[1][2:]
                return host
        return None

    @classmethod
    def get_thrift_server(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Gets name of THRIFT gateway for the cluster.
      '''
        if clusterName is None:
            clusterName = cls._clusterName
        links = Slider.get_quicklinks(clusterName, user, logoutput, max_attempts, sleep_duration)
        if not links is None:
            url = links["entries"]["org.apache.slider.hbase.thrift"]
            if not url is None:
                host = url.split(':')[1][2:]
                return host
        return None

    @classmethod
    def get_thrift2_server(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Gets name of THRIFT2 gateway for the cluster.
      '''
        if clusterName is None:
            clusterName = cls._clusterName
        links = Slider.get_quicklinks(clusterName, user, logoutput, max_attempts, sleep_duration)
        if not links is None:
            url = links["entries"]["org.apache.slider.hbase.thrift2"]
            if not url is None:
                host = url.split(':')[1][2:]
                return host
        return None

    @classmethod
    def get_jmx(cls, cluster_name, user=None, max_attempts=20, sleep_duration=5, logoutput=True):
        '''
      Retrieves JMX metrics for an HBase cluster.
      Returns JSON.
      '''
        attempt = 0
        while attempt < max_attempts:
            quicklinks = Slider.get_quicklinks(cluster_name, user, logoutput=logoutput)
            if 'org.apache.slider.jmx' in quicklinks['entries']:
                jmx_url = quicklinks['entries']['org.apache.slider.jmx']
                break
            attempt += 1
        if attempt == max_attempts:
            raise Exception("Unable to retrieve JMX URL")

        attempt = 0
        jmx = ""
        while attempt < max_attempts:
            try:
                jmx = util.getHTTPResponse("JSON", jmx_url)
                if jmx != "":
                    break
            except:
                pass
            attempt += 1
            time.sleep(sleep_duration)
        if jmx == "":
            raise Exception("Never able to read HBase JMX")
        return jmx

    @classmethod
    def getZipFile(cls, version=HBase.getVersionFromBuild(), isRU=False):
        # download for linux, no download for windows
        HBASE_VER_BUILD = version

        if Machine.isWindows():
            zipFile = os.path.join(
                Config.get('slider', 'SLIDER_HOME'), "app-packages",
                "slider-hbase-app-win-package-%s.zip" % HBASE_VER_BUILD
            )
            return zipFile

        pkg_list = "pkg-list_qe.txt"
        path = os.path.join(Config.getEnv('ARTIFACTS_DIR'), pkg_list)
        #pkgUrl = Config.get('slider','APP_PKG_LIST')
        pkgUrl = Slider.getAppPackageBaseUrl(isRU) + "/slider-app-packages/" + pkg_list
        util.downloadUrl(pkgUrl, path)
        with open(path, 'r') as f:
            for line in f:
                if line.startswith("hbase_pkg_url="):
                    url = line.strip()[14:]
                    break
        zipFile = os.path.join(os.getcwd(), "slider-hbase-app-package-%s.zip" % HBASE_VER_BUILD)
        logger.info("downloading " + url)
        util.downloadUrl(url, zipFile)
        return zipFile

    @classmethod
    def getZipFileAndBuild(cls, buildNo=""):
        """
        This method is deprecated and used in ruSlider.py only
        DO NOT USE this method.
        """
        zipFile = ""

        path = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "pkg-list_qe.txt")
        #pkgUrl = Config.get('slider','APP_PKG_LIST')
        pkgUrl = Slider.getAppPackageBaseUrl() + "/slider-app-packages/pkg-list_qe.txt"
        if buildNo != "":
            pkgUrl = "http://s3.amazonaws.com/dev.hortonworks.com/HDP/" + Slider.getAppPackageOS(
            ) + "/2.x/BUILDS/" + buildNo + "/slider-app-packages/pkg-list_bn.txt"
        util.downloadUrl(pkgUrl, path)
        f = open(path, "r")
        buildNo = 0
        for line in f:
            if line.startswith("hbase_pkg_url="):
                line = line.strip()
                url = line[14:]
                # hbase_ver has the format like 0.98.4.2.2.1.0
                # we are not using cls.HBASE_VER here because in rollung-upgrade
                # the buildNo are different for current and base build
                get = re.search("\d\.\d{2}\.\d\.\d\.\d\.\d.\d", url)
                hbase_ver = get.group(0)
                index = line.find(hbase_ver)
                # skip version
                index = index + 15
                buildNo = line[index:-12]
                logger.info("build# is " + buildNo)
                zipFile = os.path.join(
                    os.getcwd(), "slider-hbase-app-package-" + hbase_ver + "-" + buildNo + "-hadoop2.zip"
                )
                logger.info("downloading " + url)
                util.downloadUrl(url, zipFile)
        f.close()
        return buildNo, zipFile

    @classmethod
    def getSliderHbaseVersion(cls):
        if not cls.HBASE_VER:
            # eg. 0.98.4.2.2.0.0-2041-hadoop2
            HBASE_VER_BUILD = HBase.getVersionFromBuild()
            # eg. 0.98.4.2.2.0.0
            cls.HBASE_VER = HBASE_VER_BUILD[0:14]
        return cls.HBASE_VER

    @classmethod
    def write_hbase_site(cls, hbase_site):
        tmpHBaseConfFile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "hbase-site.xml")
        fout = open(tmpHBaseConfFile, "w")
        fout.write(hbase_site)
        fout.close()
        propertyMap = {'hbase.tmp.dir': '/tmp/hbase-tmp'}
        util.writePropertiesToConfigXMLFile(tmpHBaseConfFile, tmpHBaseConfFile, propertyMap)
        hbase_conf = Config.get('hbase', 'HBASE_CONF_DIR')
        hbaseConfFile = os.path.join(hbase_conf, "hbase-site.xml")
        Machine.copy(tmpHBaseConfFile, hbaseConfFile, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
        return tmpHBaseConfFile
