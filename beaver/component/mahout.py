#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.hadoop import Hadoop
import re


class Mahout:
    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True):
        return cls.runas(None, cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True, disableAuth=False):
        if not env:
            env = {}
        # if disable auth is requsted set the env
        # var to empty only for a secure cluster
        if disableAuth and Hadoop.isSecure():
            env['KRB5CCNAME'] = ''
        # if disableAuth is false and we are running a secure cluster get
        # the credentials
        elif Hadoop.isSecure():
            if user is None: user = Config.getEnv('USER')
            kerbTicket = Machine.getKerberosTicket(user)
            env['KRB5CCNAME'] = kerbTicket
            user = None

        mahout_cmd = Config.get('mahout', 'MAHOUT_CMD')
        mahout_cmd += " " + cmd
        osenv = {"JAVA_HOME": Config.get('machine', 'JAVA_HOME')}
        if env:
            for key, value in env.items():
                osenv[key] = value
        return Machine.runas(user, mahout_cmd, cwd=cwd, env=osenv, logoutput=logoutput)

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("", logoutput=False)
        if Machine.type() == "Windows":
            pattern = re.compile("MAHOUT_JOB:.*examples-(.*)-job.*")
        else:
            pattern = re.compile("MAHOUT-JOB:.*examples-(.*)-job.*")
        m = pattern.search(output)
        if m:
            return m.group(1)
        return ""
