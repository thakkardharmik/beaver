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
from beaver.config import Config
from beaver.component.hadoop import Hadoop, HDFS
import socket
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


@TaskReporter.report_test()
def getKnoxHDFSURL(nameservice2=False):
    KNOX_HOST = Config.get('knox', 'KNOX_HOST').split(',')[0]
    if HDFS.isFederated():
        if nameservice2:
            baseUrl = "https://%s:8443/gateway/ui_%s/hdfs/" % (KNOX_HOST, HDFS.getNameServices()[1])
        else:
            baseUrl = "https://%s:8443/gateway/ui_%s/hdfs/" % (KNOX_HOST, HDFS.getNameServices()[0])
    else:
        baseUrl = "https://%s:8443/gateway/ui/hdfs/" % KNOX_HOST
    baseUrlWithNNHost = "%s?host=%s" % (baseUrl, getNameNodeURL(nameservice2))
    logger.info("URL being returned is - %s" % baseUrlWithNNHost)
    return baseUrl, baseUrlWithNNHost


@TaskReporter.report_test()
def getNameNodeURL(nameservice2=False):
    if Hadoop.isEncrypted():
        baseUrl = "https://%s" % (HDFS.getNamenodeHttpsAddress(nameservice2))
    else:
        baseUrl = "http://%s" % (HDFS.getNamenodeHttpAddress(nameservice2))
    logger.info("URL being returned is - %s" % baseUrl)
    return baseUrl


@TaskReporter.report_test()
def getAmbariURL():
    ambariHost = socket.getfqdn()
    if Hadoop.isEncrypted():
        baseUrl = "https://%s:8443" % (ambariHost)
    else:
        baseUrl = "http://%s:8080" % (ambariHost)
    logger.info("URL being returned is - %s" % baseUrl)
    return baseUrl


@TaskReporter.report_test()
def getSSOLoginURL():
    ambariHost = socket.getfqdn()
    return "https://%s:444/gateway/knoxsso/knoxauth/login.html" % ambariHost
