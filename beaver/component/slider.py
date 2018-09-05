#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os, re, time, logging, collections
from beaver.machine import Machine
from beaver.config import Config
import beaver.configUtils as configUtils
from beaver.component.hadoop import Hadoop, HDFS
from beaver import util

logger = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.realpath(__file__))
JAVA_HOME = Config.get('machine', 'JAVA_HOME')
HDFS_USER = Config.get('hadoop', 'HDFS_USER')
SLIDER_HOME = Config.get('slider', 'SLIDER_HOME')
SLIDER_CMD = os.path.join(SLIDER_HOME, 'bin', 'slider')
DEFAULT_USER = Config.get('hadoop', 'HADOOPQA_USER')


class Slider:
    _isSecure = None
    _version = None
    # default the hadoop version to 2
    _isHadoop2 = True
    HDFS_PATH_SLIDER_HOME = "/user/slider"
    _STORM_CLUSTER_NAME = "storm1"

    @classmethod
    def getSliderHome(cls):
        '''
        Returns SLIDER_HOME. Use cautiously in Linux.
        '''
        return SLIDER_HOME

    # checks whether its slider component
    @classmethod
    def isSlider(cls):
        component = util.get_TESTSUITE_COMPONENT()
        logger.info("component = %s" % component)
        if "slider" in component.lower():
            return True
        else:
            return False

    # TODO: sending host in secure mode wont work as we need to refactor how
    # we get kerberos credentials on a specific host
    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True, host=None, skipAuth=False):
        '''
        Runs Slider command as a given user.
        Returns returncode, stdout.
        '''
        if Hadoop.isSecure() and not skipAuth:
            if user is None: user = DEFAULT_USER
            kerbTicket = Machine.getKerberosTicket(user)
            if not env:
                env = {}
            env['KRB5CCNAME'] = kerbTicket
            user = None
        if Machine.isWindows():
            return Machine.runas(
                user, " %s.py %s " % (SLIDER_CMD, cmd), cwd=cwd, env=env, logoutput=logoutput, host=host
            )
        else:
            return Machine.runas(
                user,
                "JAVA_HOME=%s %s %s" % (JAVA_HOME, SLIDER_CMD, cmd),
                cwd=cwd,
                env=env,
                logoutput=logoutput,
                host=host
            )

    @classmethod
    def run(cls, cmd, env=None, logoutput=True, cwd=None):
        '''
        Runs Slider command.
        Returns returncode, stdout.
        '''
        return cls.runas(None, cmd, env=env, logoutput=logoutput, cwd=cwd)

    @classmethod
    def getVersion(cls, logoutput=True):
        '''
      Returns Slider version as a string.
      '''
        # only get the version if its not already determined
        if not cls._version:
            exit_code, output = cls.run("version", logoutput=logoutput)
            if exit_code == 0:
                # perform a multi line check
                pattern = re.compile(" Slider Core-(\S+) Built", re.M)
                m = pattern.search(output)
                if m:
                    cls._version = m.group(1)

        return cls._version

    @classmethod
    def getShortVersion(cls):
        '''
      Return short version.
      If version = 2.7.1.2.3.0.0-1675,
      :return: 2.3.0.0-1675
      '''
        version = cls.getVersion()
        return version.split(".", 3)[-1]

    @classmethod
    def install_package(cls, name, package, user=None, logoutput=True, replace=False, version=None):
        '''
      Uploads a given app package of a given app name to the user's home
      directory in HDFS.  Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER

        replaceOption = ""
        if replace:
            replaceOption = "--replacepkg"
        versionOption = ""
        if version is not None:
            versionOption = "--version " + version
        return cls.runas(
            user,
            "package --install --name %s --package %s %s %s" % (name, package, versionOption, replaceOption),
            logoutput=logoutput
        )

    @classmethod
    def create(
            cls,
            clusterName,
            appConfigFile,
            resourcesFile,
            user=None,
            group=None,
            logoutput=True,
            options="",
            queue="default"
    ):
        '''
      Creates a Slider cluster using specified appConfig and resources files.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER

        # create user directory in HDFS if necessary
        user_dir = "/user/%s" % user
        if not HDFS.fileExists(user_dir):
            if logoutput:
                logger.info("Creating user HDFS dir /user/%s" % user)
            HDFS.createUserDirWithGroup("/user/%s" % user, adminUser=HDFS_USER, user=user, group=group)

        # execute slider create command
        if logoutput:
            logger.info("Creating Slider cluster %s as user %s" % (clusterName, user))
        return cls.runas(
            user,
            "create %s "
            "--template %s --resources %s --queue %s %s" % (clusterName, appConfigFile, resourcesFile, queue, options),
            logoutput=logoutput
        )

    @classmethod
    def upgradeSpec(cls, clusterName, appConfigFile, resourcesFile, user=None, logoutput=True, options=""):
        '''
      Upgrade spec of a Slider cluster using new appConfig and resources files.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER

        # execute slider upgrade spec command
        if logoutput:
            logger.info("Upgrading specification of Slider cluster %s as user %s" % (clusterName, user))
        return cls.runas(
            user,
            "upgrade %s --template %s --resources %s %s" % (clusterName, appConfigFile, resourcesFile, options),
            logoutput=logoutput
        )

    @classmethod
    def upgradeContainers(cls, clusterName, containers="", components="", user=None, logoutput=True, options=""):
        '''
      Upgrade the application containers of a Slider cluster for the specified
      container ids and/or all containers of the specified components.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER

        # execute slider upgrade containers/components command
        if logoutput:
            logger.info("Upgrading containers of Slider cluster %s as user %s" % (clusterName, user))
        containerOption = ""
        if containers != "":
            containerOption = "--containers %s" % (containers)
        componentOption = ""
        if components != "":
            componentOption = "--components %s" % (components)
        return cls.runas(
            user,
            "upgrade %s %s %s %s" % (clusterName, containerOption, componentOption, options),
            logoutput=logoutput
        )

    @classmethod
    def list(cls, clusterName, user=None, logoutput=True):
        '''
      Runs Slider list command for a cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        return cls.runas(user, "list %s" % clusterName, logoutput=logoutput)

    @classmethod
    def status(cls, clusterName, user=None, logoutput=True):
        '''
      Runs Slider status command for a cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        return cls.runas(user, "status %s" % clusterName, logoutput=logoutput)

    @classmethod
    def exists(cls, clusterName, state=None, user=None, logoutput=True):
        '''
      Runs Slider status command for a cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        if state is None:
            return cls.runas(user, "exists %s" % clusterName, logoutput=logoutput)
        else:
            return cls.runas(user, "exists %s --state %s" % (clusterName, state), logoutput=logoutput)

    @classmethod
    def get_property_from_status(
            cls, clusterName, property, valuePattern, user=None, logoutput=True, max_attempts=10, sleep_duration=3
    ):
        '''
      Retrieves Slider cluster AM URL as a string.
      '''
        if user is None: user = DEFAULT_USER

        attempt = 0
        stdout = ""
        while attempt < max_attempts:
            exit_code, stdout = cls.runas(user, "status %s" % clusterName, logoutput=logoutput)
            if exit_code == 0 and stdout != "":
                break
            attempt += 1
            time.sleep(sleep_duration)

        if stdout == "":
            raise Exception("Error getting " + property + " from status")

        pattern = re.compile('"' + property + '" : ' + valuePattern, re.M)
        m = pattern.search(stdout)
        if m:
            if logoutput:
                logger.info("Got " + property + " %s" % stdout)
            return m.group(1)
        else:
            raise Exception("Could not parse " + property + " from status")

    @classmethod
    def wait_for_all_requests_satisfied(
            cls, clusterName, user=None, logoutput=True, max_attempts=18, sleep_duration=10
    ):
        attempt = 0
        value = "false"
        while attempt < max_attempts:
            value = cls.get_property_from_status(
                clusterName, "allRequestsSatisfied", "([^ ,]+)", user=user, logoutput=logoutput
            )
            if value == "true":
                break
            attempt += 1
            time.sleep(sleep_duration)

        if value != "true":
            raise Exception("Error, all requests were not satisfied")
        else:
            return

    @classmethod
    def get_info_am_web_url(cls, clusterName, user=None, logoutput=True, max_attempts=10, sleep_duration=3):
        return cls.get_property_from_status(
            clusterName,
            "info.am.web.url",
            '"([^"]+)"',
            user=user,
            logoutput=logoutput,
            max_attempts=max_attempts,
            sleep_duration=sleep_duration
        )

    @classmethod
    def get_json_from_am_url(cls, clusterName, path, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        url = cls.get_info_am_web_url(clusterName, user=user, logoutput=logoutput) + path
        attempt = 0
        response = ""
        while attempt < max_attempts:
            response = util.getHTTPResponse("JSON", url)
            if response != "":
                break
            attempt += 1
            time.sleep(sleep_duration)
        if response == "":
            raise Exception("Never able to read AM URL")
        if logoutput:
            logger.info("Read AM URL %s" % util.toString("JSON", response))
        return response

    @classmethod
    def get_quicklinks(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Returns quick links as JSON instance.
      '''
        return cls.get_json_from_am_url(
            clusterName,
            "ws/v1/slider/publisher/slider/quicklinks",
            user=user,
            logoutput=logoutput,
            max_attempts=max_attempts,
            sleep_duration=sleep_duration
        )

    @classmethod
    def get_live_components(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Returns live components JSON instance.
      '''
        return cls.get_json_from_am_url(
            clusterName,
            "ws/v1/slider/application/live/components",
            user=user,
            logoutput=logoutput,
            max_attempts=max_attempts,
            sleep_duration=sleep_duration
        )

    @classmethod
    def get_live_containers(cls, clusterName, user=None, logoutput=True, max_attempts=12, sleep_duration=5):
        '''
      Returns live containers as JSON instance.
      '''
        return cls.get_json_from_am_url(
            clusterName,
            "ws/v1/slider/application/live/containers",
            user=user,
            logoutput=logoutput,
            max_attempts=max_attempts,
            sleep_duration=sleep_duration
        )

    @classmethod
    def stop(cls, clusterName, user=None, force=False, logoutput=True):
        '''
      Stops a Slider cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        if logoutput:
            logger.info("Stopping Slider cluster %s" % clusterName)
        forceFlag = ""
        if force:
            forceFlag = "--force"
        return cls.runas(user, "stop %s %s" % (clusterName, forceFlag), logoutput=logoutput)

    @classmethod
    def start(cls, clusterName, user=None, logoutput=True):
        '''
      Starts a Slider cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        if logoutput:
            logger.info("Starting Slider cluster %s" % clusterName)
        return cls.runas(user, "start %s " % (clusterName), logoutput=logoutput)

    @classmethod
    def destroy(cls, clusterName, user=None, force=False, logoutput=True):
        '''
      Destroys a Slider cluster.
      Returns returncode, stdout.
      '''
        if user is None: user = DEFAULT_USER
        if logoutput:
            logger.info("Destroying Slider cluster %s" % clusterName)
        forceFlag = ""
        if force:
            forceFlag = "--force"
        return cls.runas(user, "destroy %s %s" % (clusterName, forceFlag), logoutput=logoutput)

    @classmethod
    def isInstalled(cls):
        '''
        Returns True if Slider is installed.
        '''
        return cls.isSlider()

    @classmethod
    def getAppPackageBaseUrl(cls, isRU=False):
        '''
      Returns the app package base url string.
      '''
        if Machine.isCentOs5():
            os = "centos5"
        elif Machine.isUbuntu():
            if Machine.isUbuntu14():
                os = "ubuntu14"
            elif Machine.isUbuntu16():
                os = "ubuntu16"
            else:
                os = "ubuntu12"
        elif Machine.isDebian():
            if Machine.isDebian7():
                os = "debian7"
            else:
                os = "debian6"
        elif Machine.isSuse():
            if Machine.isSuse_11_3():
                os = "suse11sp3"
            elif Machine.isSuse12():
                os = "sles12"
            else:
                os = "sles11sp1"
        else:
            os = "centos6"
        if isRU:
            return "http://dev.hortonworks.com.s3.amazonaws.com/HDP/" + os + "/2.x/updates/2.2.0.0"
        else:
            return "http://dev.hortonworks.com.s3.amazonaws.com/HDP/" + os + "/2.x/BUILDS/" + str(
                cls.getShortVersion()
            )

    @classmethod
    def getSourceDownloadBaseUrl(cls):
        '''
      Returns the app package base url string.
      '''
        if Machine.isCentOs5():
            os = "centos5"
        elif Machine.isUbuntu():
            if Machine.isUbuntu14():
                os = "ubuntu14"
            elif Machine.isUbuntu16():
                os = "ubuntu16"
            else:
                os = "ubuntu12"
        elif Machine.isDebian():
            if Machine.isDebian7():
                os = "debian7"
            else:
                os = "debian6"
        elif Machine.isSuse():
            if Machine.isSuse_11_3():
                os = "suse11sp3"
            elif Machine.isSuse12():
                os = "sles12"
            else:
                os = "sles11sp1"
        else:
            os = "centos6"
        ''' Note: build id needs to be added to the end by the caller method '''
        baseUrl = "http://s3.amazonaws.com/dev.hortonworks.com/HDP/" + os + "/2.x/BUILDS/" + str(
            cls.getShortVersion().split("-")[0]
        ) + "-"
        logger.info("Harcoded fallback base download URL - %s" % (baseUrl))
        return baseUrl

    @classmethod
    def getStormClusterName(cls, logoutput=True):
        '''
        Returns slider cluster name for storm.
        '''
        result = "stormsliderapp"
        if logoutput:
            logger.info("Slider.getStormClusterName = %s" % result)
        return result

    @classmethod
    def registry(cls, clusterName, flags="", format="json", user=None, logoutput=True, userFlag=None):
        '''
        Calls slider registry server
        Returns returncode, stdout.
        :user user who executes the command.
        :userFlag user for --user flag in slider registry.
        '''
        if user is None:
            user = DEFAULT_USER
        cmd = "registry --name %s %s --format %s" % (clusterName, flags, format)
        if userFlag is not None:
            cmd += " --user %s" % userFlag
        if logoutput:
            logger.info("Slider.registry cmd=%s" % cmd)
        return cls.runas(user, cmd, logoutput=logoutput)

    @classmethod
    def getContainerLogs(cls, clusterName, user=None, logoutput=True, userFlag=None):
        '''
        Gets log of each location for slider app.
        Returns a list of 4 tuples ('component', 'host', 'logType', 'directory path').
        Returns [] otherwise.
        :user user who executes slider registry command.
        :userFlag user for --user flag in slider registry.
        '''
        tmpOutputFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-slider-registry-%s.log' % (str(int(time.time())))
        )
        (exit_code, stdout) = \
                Slider.registry(clusterName, flags="--getexp container_log_dirs --out %s" % tmpOutputFile,
                format="json", user=user, logoutput=logoutput, userFlag=userFlag)
        jsonText = util.getTextInFile(tmpOutputFile, logoutput=True)

        logger.info("Slider.getContainerLogs jsonText = %s" % jsonText)
        jsonObj = util.getJSON(jsonText)
        logger.info("Slider.getContainerLogs jsonObj = %s" % jsonObj)
        SliderContainerLogs = collections.namedtuple('SliderContainerLogs', ['component', 'host', 'logType', 'path'])
        result = []
        for key in jsonObj.keys():
            for value in jsonObj[key]:
                logger.info("Slider.getContainerLogs key,value = %s,%s" % (key, value))
                component = key

                host = value['value'].split(':')[0]
                if Machine.isLinux():
                    path = value['value'].split(':')[1]
                else:
                    path = ':'.join([value['value'].split(':')[1], value['value'].split(':')[2]])
                logType = "AGENT_LOG_ROOT"
                entry = SliderContainerLogs(str(component), str(host), str(logType), str(path))
                # e.g. path = /grid/0/hdp/yarn/local/usercache/storm/appcache/application_1410556100652_0005/container_1410556100652_0005_01_000005
                result.append(entry)
        if logoutput:
            logger.info("Slider.getContainerLogs result = %s" % result)
        return result

    @classmethod
    def download_source(cls, dir=None, logoutput=True, subdir=None, useHDPBaseRepoFile=False, isRUcluster=False):
        '''
      Downloads the slider source code matching the current installation.
      Returns the absolute path to the root dir of the code.
      '''
        logger.info(
            "Slider download_source dir=%s subdir=%s useHDPBaseRepoFile=%s" % (dir, subdir, useHDPBaseRepoFile)
        )
        if dir is None: dir = Config.getEnv('ARTIFACTS_DIR')
        if not subdir is None:
            dir = os.path.join(dir, subdir)
            os.makedirs(dir)

        if Machine.isWindows():
            dest_dir = os.path.join(dir, "src")
            original_src = os.path.join(cls.getSliderHome(), "src")
            Machine.chmod(
                "777",
                original_src,
                recursive=True,
                user=Machine.getAdminUser(),
                host=None,
                passwd=Machine.getAdminPasswd()
            )
            Machine.copy(original_src, dest_dir, user=None, passwd=None)
            return dest_dir

        slider_version = cls.getVersion(logoutput=logoutput)
        hdpBaseUrl = util.getHDPBuildsBaseUrlFromRepos(useHDPBaseRepoFile, isRUcluster=isRUcluster)
        if hdpBaseUrl is None:
            ''' now fallback to hardcoded path for slider source '''
            version_build = slider_version
            version_build = version_build[version_build.rfind("-") + 1:]
            hdpBaseUrl = cls.getSourceDownloadBaseUrl() + version_build
            logger.info("Fallback to hard coded HDP base URL for slider source - %s" % (hdpBaseUrl))

        # slider source url
        if hdpBaseUrl.split("/")[7] < "2.5.0.0":
            url = "%s/tars/slider-%s.source.tar.gz" % (hdpBaseUrl, slider_version)
            source = os.path.join(dir, "slider-%s.source.tar.gz" % slider_version)

        else:
            url = "%s/tars/slider/slider-%s-source.tar.gz" % (hdpBaseUrl, slider_version)
            source = os.path.join(dir, "slider-%s-source.tar.gz" % slider_version)
        logger.info("slider source url=%s" % url)
        # download source tarball
        if not util.downloadUrl(url, source):
            logger.error("Download of source failed, attempting to use build.id file")
            build_id_url = "%s/build.id" % (hdpBaseUrl)
            build_id_path = os.path.join(dir, "build.id")
            if not util.downloadUrl(build_id_url, build_id_path):
                ''' now fallback to hardcoded path for build.id '''
                version_build = slider_version
                version_build = version_build[version_build.rfind("-") + 1:]
                hdpBaseUrl = cls.getSourceDownloadBaseUrl() + version_build
                build_id_url = "%s/build.id" % (hdpBaseUrl)
                logger.info(
                    "Download of build.id failed. Fallback to hard coded HDP build.id URL - %s" % (build_id_url)
                )
                if not util.downloadUrl(build_id_url, build_id_path):
                    # let's not raise exception here, just in case the caller method has alternatives
                    logger.error("Error downloading build.id file from %s - giving up" % build_id_url)
                    return ""
            f = None
            try:
                f = open(build_id_path, "r")
                build_id = ''
                for line in f:
                    if line.startswith("BUILD_NUMBER:"):
                        build_id = line[14:][:-1]
                        break
                p = re.compile('(-\d+)')
                version = p.sub('-%s' % build_id, slider_version)
                if hdpBaseUrl.split("/")[7] < "2.5.0.0":
                    url = "%s/tars/slider-%s.source.tar.gz" % (hdpBaseUrl, version)
                    source = os.path.join(dir, "slider-%s.source.tar.gz" % version)
                else:
                    url = "%s/tars/slider/slider-%s-source.tar.gz" % (hdpBaseUrl, version)
                    source = os.path.join(dir, "slider-%s-source.tar.gz" % version)
                if not util.downloadUrl(url, source):
                    # let's not raise exception here, just in case the caller method has alternatives
                    logger.error("Error downloading slider source from %s - giving up" % url)
                    return ""
            finally:
                if f:
                    f.close()

        # untar tarball
        Machine.run("tar xf %s" % source, cwd=dir, logoutput=logoutput)
        if hdpBaseUrl.split("/")[7] < "2.5.0.0":
            version_minus_build = slider_version
            version_minus_build = version_minus_build[0:version_minus_build.rfind("-")]
            return os.path.join(dir, "slider-%s" % version_minus_build)
        else:
            return os.path.join(dir, "slider-%s" % slider_version)

    @classmethod
    def modifyConfig(
            cls,
            changes,
            nodeSelection,
            isFirstUpdate=True,
            makeCurrConfBackupInWindows=True,
            regenServiceXmlsInWindows=False
    ):
        '''
        Modify config. 
        Returns None.
        makeCurrConfBackupInWindows argument is very important in Windows.
        Taking good copy as backup and not replacing it with bad copy is needed.
        Current config means the config before modifications.
        changes is a map.
        
        Modify config takes source config (local /etc/hadoop/conf) as source and add entries from there.
        '''
        nodes = [HDFS.getGateway()]
        slider_conf = Config.get('slider', 'SLIDER_CONF_DIR')
        tmp_conf = os.path.join(Machine.getTempDir(), 'sliderConf')
        configUtils.modifyConfig(
            changes,
            slider_conf,
            tmp_conf,
            nodes,
            isFirstUpdate,
            makeCurrConfBackupInWindows=makeCurrConfBackupInWindows
        )
        if Machine.isWindows() and cls.getHadoopEnvSh() in changes.keys():
            if regenServiceXmlsInWindows:
                cls.regenServiceXmlsInWindows(nodeSelection)
            else:
                logger.info("WARNING: modifyConfig in Windows without propery xml generation.")

    @classmethod
    def getAppPackageOS(cls):
        '''
      Returns the app package base url string.
      '''
        if Machine.isCentOs5():
            os = "centos5"
        elif Machine.isUbuntu():
            os = "ubuntu12"
        elif Machine.isDebian():
            os = "debian6"
        elif Machine.isSuse_11_3():
            os = "suse11sp3"
        elif Machine.isSuse():
            if not Machine.isSuse_11_3():
                os = "sles11sp1"
        else:
            os = "centos6"
        return os
