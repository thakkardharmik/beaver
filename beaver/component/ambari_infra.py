#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import base64
import collections
import datetime
import json
import logging
import os, re, time, requests

from beaver import configUtils
from beaver import util
from beaver.component.hadoop import Hadoop, YARN, HDFS
from beaver.config import Config
from beaver.machine import Machine
from beaver.component.ambari import Ambari
from requests.auth import HTTPBasicAuth
import json as jsonlib
import urllib2, base64, socket, uuid
from beaver import util

logger = logging.getLogger(__name__)


class AmbariInfra:
    default_log_dir = "/var/log/ambari-infra-solr"

    @classmethod
    def getAmbariInfraLogDir(cls, logoutput=False):
        '''
        Returns Druid log directory (String).
        '''
        ambariInfraLogDir = cls.getConfig("infra-solr-env", "infra_solr_log_dir")
        logger.info("AmbariInfra.getAmbariInfraLogDir returns %s" % ambariInfraLogDir)

        if logoutput:
            logger.info("AmbariInfra.getAmbariInfraLogDir returns %s" % ambariInfraLogDir)
        return ambariInfraLogDir

    @classmethod
    def getAmbariInfraClientLogDir(cls, logoutput=False):
        '''
        Returns Druid log directory (String).
        '''
        ambariInfraClientLogDir = cls.getConfig("infra-solr-client-log4j", "infra_solr_client_log_dir")
        logger.info("AmbariInfra.getAmbariInfraClientLogDir returns %s" % ambariInfraClientLogDir)

        if logoutput:
            logger.info("AmbariInfra.getAmbariInfraClientLogDir returns %s" % ambariInfraClientLogDir)
        return ambariInfraClientLogDir
