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

from beaver import util
from beaver.machine import Machine
from beaver.component.hadoop import Hadoop

logger = logging.getLogger(__name__)


class SmartSense:
    @classmethod
    def getVersion(cls):
        # Get reported SmartSense version
        smartsemse_hosts = cls.getSmartSenseServiceNodes()
        exit_code, stdout = Machine.runas('root', 'hst --version', smartsemse_hosts[0])
        if exit_code == 0:
            hst_version = str(stdout).rpartition('tty\n')[2]
            return hst_version
        else:
            logger.error("Cannot retrieve SmartSense version for QE dashboard.")
            return ""

    @classmethod
    def getSmartSenseServiceLogDir(cls):
        return '/var/log/hst'

    @classmethod
    def getActivityComponentsLogDir(cls):
        return '/var/log/smartsense-activity'

    @classmethod
    def getSmartSenseServiceConfDir(cls):
        return '/etc/hst/conf'

    @classmethod
    def getActivityComponentsConfDir(cls):
        return '/etc/smartsense-activity/conf'

    @classmethod
    def getAllNodes(cls):
        '''
        Get all nodes according to the platform used
        '''
        if Machine.isHumboldt():
            allNodes = util.getAllNodes()
        else:
            allNodes = Hadoop.getAllNodes()
        return allNodes

    @classmethod
    def getSmartSenseServiceNodes(cls):
        '''
        Get all nodes having installed SmartSense Server or SmartSense Agent
        '''
        allNodes = cls.getAllNodes()
        logger.info("All nodes: " + str(allNodes))
        hstHosts = []
        for node in allNodes:
            exit_code, stdout = Machine.runas('root', 'ls -l /var/log/hst', node)
            if exit_code == 0:
                hstHosts.append(node)
        logger.info("SmartSense service nodes: " + str(hstHosts))
        return hstHosts

    @classmethod
    def getActivityComponentsNodes(cls):
        '''
        Get all nodes having installed Activity Explorer or Activity Analyzer
        '''
        allNodes = cls.getAllNodes()
        logger.info("All nodes: " + str(allNodes))
        activityHosts = []
        for node in allNodes:
            exit_code, stdout = Machine.runas('root', 'ls -l /var/log/smartsense-activity', node)
            if exit_code == 0:
                activityHosts.append(node)
        logger.info("SmartSense Activity components nodes: " + str(activityHosts))
        return activityHosts
