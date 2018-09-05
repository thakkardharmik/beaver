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
import re
from ConfigParser import ConfigParser

from beaver import util


class Config(object):  # pylint: disable:no-init
    config = ConfigParser()
    ENV_SECTION = 'env'

    def __call__(self):
        return self

    def parseConfig(self, cfile):
        self.config.read(cfile)
        resolveFuncs(self.config)

    def get(self, section, option, default=None):
        if default is not None and not self.config.has_option(section, option):
            return default
        return self.config.get(section, option)

    def get_default_when_empty(self, section, option, default=None):
        val = self.get(section, option, default=default)
        if val != '':
            return val
        else:
            return default

    def hasSection(self, section):
        return self.config.has_section(section)

    def hasOption(self, section, option):
        return self.config.has_option(section, option)

    def setEnv(self, env, val):
        if not self.config.has_section(self.ENV_SECTION):
            self.config.add_section(self.ENV_SECTION)
        self.config.set(self.ENV_SECTION, env, val)

    def getEnv(self, env):
        return self.config.get(self.ENV_SECTION, env)

    # method to set the config
    def set(self, section, option, value, overwrite=False):
        # first make sure if the config has the section
        # if not add the section
        if not self.config.has_section(section):
            self.config.add_section(section)

        if overwrite:
            self.config.set(section, option, value)
        else:
            # only write if the config option does not exist
            # or if the config value is empty
            if not self.hasOption(section, option) or not self.get(section, option):
                self.config.set(section, option, value)

    # method to return boolean value
    def getboolean(self, section, option, default=False):
        if not self.config.has_option(section, option):
            return default
        return self.config.getboolean(section, option)


Config = Config()


def resolveFuncs(config):
    FUNCREG = re.compile(r"(\${.*})")
    for section in config.sections():
        for item in config.items(section):
            option, value = item
            m = FUNCREG.match(value)
            if m:
                for group in m.groups():
                    out = recursiveEval(group)
            else:
                out = value
            config.set(section, option, out)


def recursiveEval(group):
    funccall = group[2:-1]
    if funccall.count("$") > 0:
        innerfunc = funccall[funccall.find("$"):funccall.rfind("}") + 1]
        funccall = funccall.replace(innerfunc, recursiveEval(innerfunc))
    out = eval(funccall)  # pylint: disable=eval-used
    return out


def find(basedir, matchstr, isDirectory=False, isRecursive=True):
    if isRecursive:
        matches = util.findMatchingFiles(basedir, matchstr, isDirectory)
    else:
        matches = util.findMatchingFiles(basedir, matchstr, isDirectory, 1)
    if matches:
        return matches[0]
    return ""


def join(*args):
    return os.sep.join(list(args))


def getIpAddress():
    return util.getIpAddress()
