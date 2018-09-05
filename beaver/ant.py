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
from beaver.component.hadoop import Hadoop
from beaver.machine import Machine
from beaver.config import Config
from beaver import util


class Ant(object):
    # set the ant version
    _version = '1.8.4'
    # determine java home
    _java_home = Config.get('machine', 'JAVA_HOME')
    _worksapce = Config.getEnv('WORKSPACE')
    # determine tools path
    _tools_path = os.path.join(_worksapce, 'tools')
    # determine ant home
    _ant_home = os.path.join(_tools_path, 'apache-ant-' + _version)
    _ant_cmd = os.path.join(_ant_home, 'bin', 'ant')
    # what url to download ant from
    # need to find a solution for windows until then the url will live here
    _ant_download_url = Config.get('machine', 'ANT_URL')

    def __init__(self):
        pass

    #method to run ant cmd
    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True, user=None):
        # make sure maven is setup before its run
        cls.setupAnt()

        # initialize env
        if not env:
            env = {}

        # if running a secure cluster get a kerb ticket
        if Hadoop.isSecure():
            env['KRB5CCNAME'] = Machine.getKerberosTicket(user)

        env['JAVA_HOME'] = cls._java_home
        env['ANT_HOME'] = cls._ant_home
        run_cmd = "%s -Dbuild.compiler=javac1.7 %s" % (cls._ant_cmd, cmd)
        exit_code, stdout = Machine.run(run_cmd, cwd=cwd, env=env, logoutput=logoutput)
        return exit_code, stdout

    # method to setup maven
    @classmethod
    def setupAnt(cls):
        # if dir exists return as its already setup
        if os.path.isdir(cls._ant_home):
            return

        # check if tarball exists or not before downloading it
        tarName = "apache-ant-" + cls._version + ".tar.gz"
        tarballPath = os.path.join(cls._worksapce, tarName)
        if not os.path.isfile(tarballPath):
            # download ant
            assert util.downloadUrl(cls._ant_download_url, tarballPath)

        # now untar ant on windows you have to run the tar cmd from the dir where
        # the file exists else it fails
        Machine.tarExtractAll(filepath=tarballPath, outpath=cls._tools_path, mode='r:gz')
        assert os.path.isfile(cls._ant_cmd)
