# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
# This script contains methods used by ambari-common.py for following purpose:
# a. Enable integration between API framework and uifrm
# b. Read values from EC2 job and populate input file (config.properties) of API frameowork
# c. Detect status of upgrade specific tests so that next action can be take accordingly

import os
from beaver.config import Config
import logging
from beaver.component.ambariHelper import Maven2
from beaver.marker import get_annotation_dict
from beaver.util import getPomPathesToAnnotations
from beaver.component.ambari_apilib import CommonLib

logger = logging.getLogger(__name__)


def isTestClassPresent(testclass, path):
    if "#" in testclass:
        testclass = testclass.split("#")[0]

    for root, directories, filenames in os.walk(path + '/src'):
        for filename in filenames:
            if filename.endswith("java"):
                fileToMatch = filename.split('.')
                if fileToMatch[0] == testclass:
                    logger.info('Found matching test class in current dir')
                    return True
    logger.info('Not Found matching test class in current dir')
    return False


def updateProperties():
    if CommonLib.is_ambari_security_enabled(Config.get('ambari', 'AMBARI_SERVER_HTTPS')
                                            ) or CommonLib.is_ambari_security_enabled(Config.get('ambari',
                                                                                                 'WIRE_ENCRYPTION')):
        port = 8443
        https = "yes"
    else:
        port = 8080
        https = "no"

    DEFAULT_TEST_TIMEOUT = 45  # default value of 45 minutes, will extend this later to allow as an input through ADDITIONAL_AMBARI_PROPS if user wants to

    #populate the config properties file for api test code
    configPropChanges = {}
    configPropChanges['HOST'] = Config.get('ambari', 'HOST')
    configPropChanges['PORT'] = port

    # Below check is only done for API framework as part of post Upgrade, hence a safe assumption to use STACKNAME as HDP (since the calls are made after upgrade from BigInsights to HDP)
    if 'BigInsights'.lower() in Config.get('ambari', 'STACK_TYPE').lower():
        logger.info("STACK_TYPE = BigInsights, changing to HDP for post upgrade operations")
        configPropChanges['STACKNAME'] = 'HDP'
    else:
        configPropChanges['STACKNAME'] = Config.get('ambari', 'STACK_TYPE')

    configPropChanges['STACKVERSION_TO_UPGRADE'] = Config.get('ambari', 'STACK_UPGRADE_TO')
    configPropChanges['BUILDNUMBER_TO_UPGRADE'] = Config.get('ambari', 'STACK_UPGRADE_TO')
    configPropChanges['MOTD_ENABLE'] = Config.get('ambari', 'MOTD_ENABLE')
    configPropChanges['AMBARI_DB'] = Config.get('ambari', 'AMBARI_DB')
    configPropChanges['AMBARI_SERVER_HTTPS'] = https

    configPropChanges['SECURITY'] = Config.get('machine', 'IS_SECURE')
    configPropChanges['KERBEROS_SERVER_TYPE'] = Config.get('ambari', 'KERBEROS_SERVER_TYPE')
    configPropChanges['REALM'] = Config.get('ambari', 'REALM')
    configPropChanges['USER_KERBEROS_SERVER_TYPE'] = Config.get('ambari', 'USER_KERBEROS_SERVER_TYPE')
    configPropChanges['AD_SERVER_HOST'] = Config.get('ambari', 'AD_SERVER_HOST')
    configPropChanges['USER_REALM'] = Config.get('ambari', 'USER_REALM')
    configPropChanges['CUSTOMIZED_SERVICES_USERS'] = Config.get('ambari', 'CUSTOMIZED_SERVICES_USERS')
    configPropChanges['AMBARI_2WAY_SSL'] = Config.get('ambari', 'AMBARI_2WAY_SSL')
    configPropChanges['WIRE_ENCRYPTION'] = Config.get('ambari', 'WIRE_ENCRYPTION')
    configPropChanges['DEFAULT_TESTCASE_TIMEOUT'] = DEFAULT_TEST_TIMEOUT
    configPropChanges['RUN_INSTALLER'] = Config.get('machine', 'RUN_INSTALLER')

    # Added to read additional ambari props esp. for Patch Upgrade related properties like SERVICES_TO_UPGRADE, UPGRADE_TYPE
    logger.info("Checking if any ADDITIONAL_AMBARI_PROPS need to be updated")
    ADDITIONAL_AMBARI_PROPS = Config.get('ambari', 'ADDITIONAL_AMBARI_PROPS')
    if ADDITIONAL_AMBARI_PROPS:
        parameter_map = ADDITIONAL_AMBARI_PROPS.split(",")
        for parameter in parameter_map:
            key_value = parameter.split("=")
            key = key_value[0]
            value = key_value[1]
            print "Reading key :%s = value :%s" % (key, value)
            configPropChanges[key] = value

    return configPropChanges


def switchDirectory(currentDirectory, component):
    if isCurrentDirectoryAPIFramework(currentDirectory):
        path = os.path.join(Config.getEnv('ARTIFACTS_DIR'), component)
        logger.info('Switched current dir to: %s ' % (path))
        return path
    else:
        path = os.path.join(Config.getEnv('ARTIFACTS_DIR'), component, 'apitestframework')
        logger.info('Switched current dir to: %s ' % (path))
        return path


def isCurrentDirectoryAPIFramework(currentDirectory):
    if "apitestframework" in currentDirectory:
        return True
    else:
        return False


def isUpgradeSuccess(testName, exit_code):
    if "E2E_EU_AmbariSuite" in testName or "Upgrade" in testName:
        if exit_code == 0:
            return True
        else:
            return False
    else:
        return True


def isUpgradeTest(targetAmbariVersion, targetStackVersion):
    if targetAmbariVersion:
        logger.info("Ambari Upgrade Test")
        return True
    if targetStackVersion:
        logger.info("Stack Upgrade Test")
        return True
    else:
        return False


def update_pom_xml_with_markers(current_directory):
    annotations_dict = get_annotation_dict()

    if len(annotations_dict[True]) == 0 and len(annotations_dict[False]) == 0:
        print "No Markers specified for the test"
        return

    # This is required annotation (is used for tests used with all markers, like InstallHadoop etc.)
    if len(annotations_dict[True]) > 0:
        annotations_dict[True].append('RunAlways')

    # For API framework (TestNG) just update groups info in pom.xml
    # For uifrm (JUnit) update the Annotation class (interface) name in pom.xml
    if "apitestframework" in current_directory:
        Maven2.setProjectCategories(
            os.path.join(current_directory, "pom.xml"), get_groups_list(annotations_dict[True]),
            get_groups_list(annotations_dict[False])
        )
    else:
        Maven2.setProjectCategories(
            os.path.join(current_directory, "pom.xml"),
            getPomPathesToAnnotations(current_directory, annotations_dict[True]),
            getPomPathesToAnnotations(current_directory, annotations_dict[False])
        )


def get_groups_list(groups):
    groups_to_match = []

    for group in groups:
        groups_to_match.append(group)

    if len(groups_to_match) == 0:
        return ''

    groups_comma_separated_path = ",".join(groups_to_match)
    return groups_comma_separated_path
