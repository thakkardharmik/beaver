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
import time

from beaver import util
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)
PYTHON_CONFIG_NAME = 'configutils'
TMP_CONF_DIR_VAR = 'TMP_CONF_DIR'
ARTIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')
BACKUP_CONFIG_LOCATION = os.path.join(ARTIFACTS_DIR, 'HDPStackBackupConfigs')


# method to modify the config
def modifyConfig(  # pylint: disable=redefined-builtin
        changes, confDir, updatedConfDir, nodes, isFirstUpdate=True, makeCurrConfBackupInWindows=True, id=None
):
    '''
    Modifies hadoop config or config with similar structure.
    Returns None.

    Linux:
    1. Create tmpModifyConfDir_<time> in artifacts dir based on source config directory in gateway
    2. Modify contents in created directory.
    3. Copy the directory to /tmp/hadoopConf in target machines

    Windows:
    1. If makeCurrConfBackupInWindows is True, backup current config first.
       Copy current config to artifacts/HDPStackBackupConfig
    2. Create tmpModifyConfDir_<time> in gateway.
    3. Modify contents in created directory.
    4. Copy the directory to target machines. Replace config in default locations in remote machines.

    Calling modifyConfig twice, changes will be cumulative.
    '''
    backuploc = getBackupConfigLocation(id=id)
    if Machine.type() == 'Windows' and makeCurrConfBackupInWindows:
        # clean up the backup
        Machine.rm(None, Machine.getfqdn(), backuploc, isdir=True, passwd=None)
        util.copyReadableFilesFromDir(confDir, backuploc)

    if isFirstUpdate:
        tmpConfDir = os.path.join(ARTIFACTS_DIR, 'tmpModifyConfDir_' + str(int(round(time.time() * 1000))))
        Config.set(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR, tmpConfDir, overwrite=True)

    tmpConfDir = Config.get(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR)
    if isFirstUpdate:
        util.copyReadableFilesFromDir(confDir, tmpConfDir)
    for filename, values in changes.items():
        filepath = os.path.join(tmpConfDir, filename)
        if os.path.isfile(filepath):
            logger.info("Modifying file: %s", filepath)
            _fname, fext = os.path.splitext(filepath)
            if fext == ".xml":
                util.writePropertiesToConfigXMLFile(filepath, filepath, values)
            elif fext == ".json":
                util.writePropertiesToConfigJSONFile(filepath, filepath, values, ["global"], "site.hbase-site.")
            elif fext == ".properties":
                util.writePropertiesToFile(filepath, filepath, values)
            elif fext == ".cfg":
                util.writePropertiesToFile(filepath, filepath, values)
            elif fext == ".conf":
                util.writePropertiesToConfFile(filepath, filepath, values)
            elif fext == ".ini":
                # 'shiro.ini : {'section:prop' : 'val}
                util.writePropertiesToIniFile(filepath, filepath, values)
            elif fext == ".sh":
                text = ""
                for value in values:
                    text += "\n" + value
                util.writeToFile(text, filepath, isAppend=True)
            elif fext == ".yaml":
                text = ""
                for k, v in values.iteritems():
                    text += k + " : " + v
                util.writeToFile(text, filepath, isAppend=True)
            elif fext == ".cmd":
                text = ""
                for value in values:
                    text += "\n" + value
                util.writeToFile(text, filepath, isAppend=True)
            elif fext is None or fext == "" or fext == ".include":
                text = ""
                isFirst = True
                for value in values:
                    if isFirst:
                        text += value
                    else:
                        text += "\n" + value
                        isFirst = False
                util.writeToFile(text, filepath, isAppend=True)

    # in windows world copy the configs back to the src location
    if Machine.type() == 'Windows':
        for node in nodes:
            for filename in changes.keys():
                Machine.copyFromLocal(
                    None, node, os.path.join(tmpConfDir, filename), os.path.join(confDir, filename), passwd=None
                )
    else:
        for node in nodes:
            Machine.rm(
                user=Machine.getAdminUser(),
                host=node,
                filepath=updatedConfDir,
                isdir=True,
                passwd=Machine.getAdminPasswd()
            )
            Machine.copyFromLocal(None, node, tmpConfDir, updatedConfDir)


def modifyConfigRemote(changes, OriginalConfDir, ConfDir, nodes, id=None):  # pylint: disable=redefined-builtin
    '''
    Modifies hadoop config or config with similar structure.
    Returns None.

    Linux:
    1. Create tmpModifyConfDir_<time> in artifacts dir based on source config directory in gateway
    2. Modify contents in created directory.
    3. Copy the directory to /tmp/hadoopConf in target machines

    '''
    _backuploc = getBackupConfigLocation(id=id)
    tmpConfDir = os.path.join(ARTIFACTS_DIR, 'tmpModifyConfDir_' + str(int(round(time.time() * 1000))))
    Config.set(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR, tmpConfDir, overwrite=True)
    tmpConfDir = Config.get(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR)
    for node in nodes:
        Machine.rm(Machine.getAdminUser(), node, ConfDir, isdir=True)
        Machine.rm(Machine.getAdminUser(), Machine.getfqdn(), tmpConfDir, isdir=True)
        logger.info("*** COPY ORIGINAL CONFIGS FROM REMOTE TO LOCAL ***")
        Machine.copyToLocal(None, node, OriginalConfDir, tmpConfDir)
        #if node == Machine.getfqdn():
        #   Machine.copy(OriginalConfDir,tmpConfDir)
        for filename, values in changes.items():
            filepath = os.path.join(tmpConfDir, filename)
            if os.path.isfile(filepath):
                logger.info("Modifying file locally: %s", filepath)
                _fname, fext = os.path.splitext(filepath)
                if fext == ".xml":
                    util.writePropertiesToConfigXMLFile(filepath, filepath, values)
                elif fext == ".json":
                    util.writePropertiesToConfigJSONFile(filepath, filepath, values, ["global"], "site.hbase-site.")
                elif fext == ".properties":
                    util.writePropertiesToFile(filepath, filepath, values)
                elif fext == ".cfg":
                    util.writePropertiesToFile(filepath, filepath, values)
                elif fext == ".conf":
                    util.writePropertiesToConfFile(filepath, filepath, values)
                elif fext == ".sh":
                    text = ""
                    for value in values:
                        text += "\n" + value
                    util.writeToFile(text, filepath, isAppend=True)
                elif fext == ".yaml":
                    text = ""
                    for k, v in values.iteritems():
                        text += k + " : " + v
                    util.writeToFile(text, filepath, isAppend=True)
                elif fext == ".cmd":
                    text = ""
                    for value in values:
                        text += "\n" + value
                    util.writeToFile(text, filepath, isAppend=True)
                elif fext is None or fext == "" or fext == ".include":
                    text = ""
                    isFirst = True
                    for value in values:
                        if isFirst:
                            text += value
                        else:
                            text += "\n" + value
                            isFirst = False
                    util.writeToFile(text, filepath, isAppend=True)
        logger.info("****** Copy back the configs to remote ******")
        #if node!=Machine.getfqdn():
        Machine.copyFromLocal(None, node, tmpConfDir, ConfDir)
        Machine.chmod('777', ConfDir, recursive=True, host=node)


def deleteConfig(  # pylint: disable=redefined-builtin
        changes, confDir, updatedConfDir, nodes, isFirstUpdate=True, id=None
):
    backuploc = getBackupConfigLocation(id=id)
    if Machine.type() == 'Windows':
        # clean up the backup
        Machine.rm(None, Machine.getfqdn(), backuploc, isdir=True, passwd=None)
        util.copyDir(confDir, backuploc)

    if isFirstUpdate:
        tmpConfDir = os.path.join(ARTIFACTS_DIR, 'tmpModifyConfDir_' + str(int(round(time.time() * 1000))))
        Config.set(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR, tmpConfDir, overwrite=True)

    tmpConfDir = Config.get(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR)

    if isFirstUpdate:
        util.copyDir(confDir, tmpConfDir)
    for filename, values in changes.items():
        filepath = os.path.join(tmpConfDir, filename)
        if os.path.isfile(filepath):
            logger.info("Modifying file: %s", filepath)
            #only supports xml file for now
            _fname, fext = os.path.splitext(filepath)
            if fext == ".xml":
                util.deletePropertyFromXML(filepath, filepath, values)
            elif fext == ".properties":
                logger.info(".properties file not supported for deleting config")
            elif fext == ".sh":
                logger.info(".sh format not supported for deleting config")
            elif fext is None or fext == "" or fext == ".include":
                logger.info(".incluse format not supported for deleting config")
    # in windows world copy the configs back to the src location
    if Machine.type() == 'Windows':
        for node in nodes:
            for filename in changes.keys():
                Machine.copyFromLocal(
                    None, node, os.path.join(tmpConfDir, filename), os.path.join(confDir, filename), passwd=None
                )
    else:
        for node in nodes:
            Machine.rm(None, node, updatedConfDir, isdir=True)
            Machine.copyFromLocal(None, node, tmpConfDir, updatedConfDir)


def modifyConfigChaosMonkey(changes, confDir, updatedConfDir, nodes, isFirstUpdate=True):
    if Machine.type() == 'Windows':
        # clean up the backup
        Machine.rm(None, Machine.getfqdn(), BACKUP_CONFIG_LOCATION, isdir=True, passwd=None)
        util.copyDir(confDir, BACKUP_CONFIG_LOCATION)

    if isFirstUpdate:
        tmpConfDir = os.path.join(ARTIFACTS_DIR, 'tmpModifyConfDir_' + str(int(round(time.time() * 1000))))
        Config.set(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR, tmpConfDir, overwrite=True)

    tmpConfDir = Config.get(PYTHON_CONFIG_NAME, TMP_CONF_DIR_VAR)

    if isFirstUpdate:
        util.copyDir(confDir, tmpConfDir)
    for filename, values in changes.items():
        filepath = os.path.join(tmpConfDir, filename)
        if os.path.isfile(filepath):
            logger.info("Modifying file: %s", filepath)
            _fname, fext = os.path.splitext(filepath)
            if fext == ".xml":
                lines = file(filepath, 'r').readlines()
                del lines[-1]
                file(filepath, 'w').writelines(lines)
                logger.info("Modifying file: %s", filepath)
                util.writeToFile(values, filepath, isAppend=True)

    # in windows world copy the configs back to the src location
    if Machine.type() == 'Windows':
        for node in nodes:
            for filename in changes.keys():
                Machine.copyFromLocal(
                    None, node, os.path.join(tmpConfDir, filename), os.path.join(confDir, filename), passwd=None
                )
    else:
        for node in nodes:
            Machine.rm(None, node, updatedConfDir, isdir=True)
            Machine.copyFromLocal(None, node, tmpConfDir, updatedConfDir)


def restoreConfig(changes, restoreLocation, nodes, id=None):  # pylint: disable=redefined-builtin
    '''
    Restores hadoop config or config with similar structure.
    Return None.

    Linux: do nothing because default config is not altered.
    Windows: copy config from BACKUP_CONFIG_LOCATION back to default config directories in target machines.
    '''
    backuploc = getBackupConfigLocation(id=id)
    if Machine.type() != 'Windows':
        return

    for node in nodes:
        for filename in changes:
            Machine.copyFromLocal(
                None, node, os.path.join(backuploc, filename), os.path.join(restoreLocation, filename), passwd=None
            )


def getBackupConfigLocation(id=None):  # pylint: disable=redefined-builtin
    backuploc = BACKUP_CONFIG_LOCATION
    if id:
        backuploc += "-" + id
    return backuploc
