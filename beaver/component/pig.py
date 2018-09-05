#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import re
import logging
import os
from beaver.component.hadoop import Hadoop
from beaver.machine import Machine
from beaver.config import Config

logger = logging.getLogger(__name__)


class Pig(object):
    @classmethod
    def run(cls, cmd, logoutput=True, cwd=None, env=None, runInBackground=False):
        return cls.runas(None, cmd, cwd, env, logoutput=logoutput, runInBackground=runInBackground)

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True, runInBackground=False):
        runCmd = Config.get('pig', 'PIG_CMD') + " " + cmd
        # initialize env
        if not env:
            env = {}
        # get kerberos ticket
        if Hadoop.isSecure():
            if user is None:
                user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        if runInBackground:
            return Machine.runinbackgroundAs(user, runCmd, cwd=cwd, env=env)
        else:
            return Machine.runas(user, runCmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("-version")
        if exit_code == 0:
            pattern = re.compile(r"Apache Pig version (\S+)")
            m = pattern.search(output)
            if m:
                return m.group(1)
        return ""

    @classmethod
    def getMiscTestLogPaths(cls, logoutput=False):
        if Machine.isLinux():
            HADOOPQE_TESTS_DIR = Config.getEnv("WORKSPACE")
            miscTestLogPaths = [
                os.path.join(HADOOPQE_TESTS_DIR, "pig", "test", "e2e", "pig", "testdist", "out", "log"),
                os.path.join(HADOOPQE_TESTS_DIR, "pig", "test", "e2e", "pig", "testdist", "out", "pigtest")
            ]
        else:
            PIG_HOME = Config.get('pig', 'PIG_HOME')
            miscTestLogPaths = [
                os.path.join(PIG_HOME, "test", "e2e", "pig", "testdist", "out", "log"),
                os.path.join(PIG_HOME, "test", "e2e", "pig", "testdist", "out", "pigtest")
            ]
        if logoutput:
            logger.info("Pig.getMiscTestLogPaths returns %s", str(miscTestLogPaths))
        return miscTestLogPaths
