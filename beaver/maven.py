#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import logging
import os

import pytest
from beaver import util
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class Maven(object):
    # set the maven version
    _version = '3.0.4'
    # determine java home
    _java_home = Config.get('machine', 'JAVA_HOME')
    _worksapce = Config.getEnv('WORKSPACE')
    # determine tools path
    _tools_path = os.path.join(_worksapce, 'tools')
    # determine maven home
    _maven_home = os.path.join(_tools_path, 'apache-maven-' + _version)
    _maven_cmd = os.path.join(_maven_home, 'bin', 'mvn')
    # what url to download maven from
    # until a solution is set for windows the url will live here
    _maven_download_url = Config.get('machine', 'MAVEN_URL')

    def __init__(self):
        pass

    #method to run maven cmd
    @classmethod
    def run(
            cls,
            cmd,
            cwd=None,
            env=None,
            mavenOpts=None,
            logoutput=True,
            user=None,
            version=_version,
            mvn_url=_maven_download_url
    ):

        # Overriding this function with run2
        # Thre reason being that newer HW products may or may not include Hadoop as a part of the package
        # Having import of Hadoop module crashes the run because the Hadoop configurations are not being found.
        # In order to overcome that we set the krb5ccname in run and pass the same to the run2 function
        # which then executes the maven command

        # if running a secure cluster get a kerb ticket
        krb5ccname = None
        #Notifying that java tests are being executed.
        pytest.java_tests_wrapped = True
        from beaver.component.ambari import Ambari
        if Ambari.is_cluster_secure():
            krb5ccname = Machine.getKerberosTicket(user)

        exit_code, stdout = cls.run2(
            cmd,
            cwd=cwd,
            env=env,
            mavenOpts=mavenOpts,
            logoutput=logoutput,
            user=user,
            version=version,
            mvn_url=mvn_url,
            krb5ccname=krb5ccname
        )
        return exit_code, stdout

    # method to setup maven
    @classmethod
    def setupMaven(cls, version=_version, mvn_url=_maven_download_url):
        # if dir exists return as its already setup

        mvn_home = os.path.join(cls._tools_path, 'apache-maven-' + version)
        if os.path.isdir(mvn_home):
            return
        #if os.path.isdir(cls._maven_home):
        #  return

        # check if tarball exists or not before downloading it
        tarName = "apache-maven-" + version + ".tar.gz"
        #tarName = "apache-maven-" +cls._version + ".tar.gz"

        tarballPath = os.path.join(cls._worksapce, tarName)
        if not os.path.isfile(tarballPath):
            # download maven
            assert util.downloadUrl(mvn_url, tarballPath)
            #assert util.downloadUrl(cls._maven_download_url, tarballPath)

        # now extract maven
        Machine.tarExtractAll(filepath=tarballPath, outpath=cls._tools_path, mode='r:gz')

        mvn_cmd = os.path.join(mvn_home, 'bin', 'mvn')
        assert os.path.isfile(mvn_cmd)

    @classmethod
    def getLocalMavenRepoDir(cls, logoutput=True):
        '''
      Returns a string of full path of local maven repository.

      In flubber Linux, it should be "/users/hrt_qa/.m2/repository"
      In nano centos Linux, it should be "/home/hrt_qa/.m2/repository
      '''
        #this ConfigParser get converts my parameter to lower-case. Using os.environ directly.
        #HOME_DIR = Config.getEnv("HOME")
        HOME_DIR = os.environ["HOME"]
        if Machine.isWindows():
            HOME_DIR = os.environ["USERPROFILE"]

        #next line might be incorrect for custom config.
        result = os.path.join(HOME_DIR, ".m2", "repository")

        if logoutput:
            logger.info("getLocalMavenRepoDir returns %s", result)
        return result

    @classmethod
    def getPublicRepoUrl(cls):
        '''
    Returns a string representing RE public maven repo.
    '''
        return "http://nexus-private.hortonworks.com/nexus/content/groups/public/"

    @classmethod
    def run2(  # pylint: disable=unused-argument
            cls,
            cmd,
            cwd=None,
            env=None,
            mavenOpts=None,
            logoutput=True,
            user=None,
            version=_version,
            mvn_url=_maven_download_url,
            krb5ccname=None,
            addlargs=None
    ):
        # make sure maven is setup before its run
        cls.setupMaven(version, mvn_url)

        # initialize env
        if not env:
            env = {}

        # Setting the security of system in the environment vars
        if krb5ccname is not None and krb5ccname != "":
            env['KRB5CCNAME'] = krb5ccname

        # determine if MAVEN_OPTS need to be set
        if mavenOpts:
            opts = os.environ.get('MAVEN_OPTS')
            if not opts:
                opts = mavenOpts
            else:
                opts = ' '.join([opts, mavenOpts])
            env['MAVEN_OPTS'] = opts

        env['JAVA_HOME'] = cls._java_home

        mvn_home = os.path.join(cls._tools_path, 'apache-maven-' + version)
        env['M2_HOME'] = mvn_home

        # print the env so we can see what we are setting
        logger.info('Env for mvn cmd')
        logger.info(env)

        mvn_cmd = os.path.join(mvn_home, 'bin', 'mvn')
        if addlargs is None:
            maven_cmd = "%s %s" % (mvn_cmd, cmd)
        else:
            maven_cmd = "%s %s %s" % (mvn_cmd, cmd, addlargs)

        exit_code, stdout = Machine.run(maven_cmd, cwd=cwd, env=env, logoutput=logoutput)
        return exit_code, stdout
