#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import os, logging
import traceback
import sys
from beaver import util
from beaver.machine import Machine
from beaver.config import Config
from tempfile import mkstemp
from shutil import move
from os import remove, close

logger = logging.getLogger(__name__)


class Java2:
    #set the java version
    _version = '1.8.0_112'
    _worksapce = Config.getEnv('WORKSPACE')
    #determine tools path
    _tools_path = os.path.join(_worksapce, 'tools')
    #determine java home
    _java_home = Config.get('machine', 'QA_CODE_JAVA_HOME')
    _jdk_download_url = Config.get('machine', 'JDK8_64_URL')
    _jdk_download_url_7 = Config.get('machine', 'JDK7_80_URL')
    _java_cmd = os.path.join(_java_home, 'bin', 'java')

    #method to setup java
    @classmethod
    def setupJava(cls):
        # if dir exists return as its already setup
        if os.path.isdir(cls._java_home):
            logger.info("JDK already installed, skipping setup of JDK")
            return cls._java_home
        # check if tarball exists or not before downloading it
        tarName = "jdk-8u112-linux-x64.tar.gz"
        tarballPath = os.path.join(cls._tools_path, tarName)
        if not os.path.isfile(tarballPath):
            # download java
            assert util.downloadUrl(cls._jdk_download_url, tarballPath)
        # now install java
        Machine.run('chmod 755 ' + tarballPath)
        Machine.run('cd ' + cls._tools_path + '; tar zxvf ' + tarballPath)
        #Machine.run('echo A | .' + tarballPath + ' -noregister 2>&1')
        #Machine.tarExtractAll(filepath=tarballPath, outpath = cls._tools_path, mode='r:gz')
        print cls._java_home
        assert os.path.isfile(cls._java_cmd)
        return cls._java_home

    @classmethod
    def setupJava7(cls):
        # if dir exists return as its already setup
        # check if tarball exists or not before downloading it
        tarName = "jdk-7u80-linux-x64.tar.gz"
        jdk7_folder_name = "jdk1.7.0_80"
        jdk7_java_home = os.path.join(cls._tools_path, jdk7_folder_name)
        if os.path.isdir(jdk7_java_home):
            logger.info("JDK 7 already installed, skipping setup of JDK 7")
            return jdk7_java_home
        tarballPath = os.path.join(cls._tools_path, tarName)
        if not os.path.isfile(tarballPath):
            # download java
            logger.info("JDK 7 downloading")
            assert util.downloadUrl(cls._jdk_download_url_7, tarballPath)
        # now install java
        Machine.run('chmod 755 ' + tarballPath)
        Machine.run('cd ' + cls._tools_path + '; tar zxvf ' + tarballPath)
        return jdk7_java_home

    @classmethod
    def runJar(cls, jarName, workingDir=os.getcwd()):
        (exit_code, output) = Machine.run(
            cls._java_cmd + " -jar " + jarName, cwd=workingDir, env=None, logoutput=False
        )
        return (exit_code, output)


class Maven2:
    # set the maven version
    _version = '3.0.4'
    # determine java home
    _java_home = Java2._java_home
    _worksapce = Config.getEnv('WORKSPACE')
    # determine tools path
    _tools_path = os.path.join(_worksapce, 'tools')
    # determine maven home
    _maven_home = Config.get('machine', 'MAVEN_HOME')
    _maven_cmd = os.path.join(_maven_home, 'bin', 'mvn')
    # what url to download maven from
    _maven_download_url = Config.get('machine', 'MAVEN_URL')

    #method to run maven cmd
    @classmethod
    def run(cls, cmd, cwd=None, env=None, mavenOpts=None, logoutput=True, user=None):
        # make sure maven is setup before its run
        cls.setupMaven()

        # initialize env
        if not env:
            env = {}

        # determine if MAVEN_OPTS need to be set
        if mavenOpts:
            opts = os.environ.get('MAVEN_OPTS')
            if not opts:
                opts = mavenOpts
            else:
                opts = ' '.join([opts, mavenOpts])
            env['MAVEN_OPTS'] = opts

        env['JAVA_HOME'] = cls._java_home
        env['M2_HOME'] = cls._maven_home

        # print the env so we can see what we are setting
        logger.info('Env for mvn cmd')
        logger.info(env)

        maven_cmd = "%s %s" % (cls._maven_cmd, cmd)
        exit_code, stdout = Machine.run(maven_cmd, cwd=cwd, env=env, logoutput=logoutput)
        return exit_code, stdout

    # method to setup maven
    @classmethod
    def setupMaven(cls):
        # if dir exists return as its already setup
        if not cls._maven_home:
            logger.error("MAVEN_HOME parameter not found. It seems like maven is not installed!!")
            sys.exit(-1)
        else:
            logger.info("_maven_home : %s" % cls._maven_home)
            logger.info("Maven already present at %s. Skipping install" % cls._maven_home)

    #Method to update pom.xml file entry with the annotations specified - both inlcude and exclude
    @classmethod
    def setProjectCategories(cls, path, groupsValue, excludedGroupsValue):
        try:
            fh, abs_path = mkstemp()
            with open(abs_path, 'w') as new_file:
                with open(path) as old_file:
                    for line in old_file:
                        new_file.write(
                            line.replace("${testcase.include.groups}", str(groupsValue))
                            .replace("${testcase.exclude.groups}", str(excludedGroupsValue))
                        )
            close(fh)
            remove(path)
            move(abs_path, path)
        except:
            logger.error("Exception occured during setProjectCategories to update pom.xml")
            logger.error(traceback.format_exc())
