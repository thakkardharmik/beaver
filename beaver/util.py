#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import datetime
import fileinput
import fnmatch
import glob
import hashlib
import httplib
import json
import logging
import os
import random
import re
import shutil
import socket
import ssl
import sys
import tempfile
import time
import traceback
import urllib2

import ConfigParser
from ConfigParser import NoOptionError
import string
from string import Template
from threading import current_thread
from urllib import quote
import xml.etree.ElementTree as ET
from xml.dom import minidom
from xml.dom.minidom import parseString

import requests

logger = logging.getLogger(__name__)
#constants
jsonHeader = {
    "Accept": "application/json",
}
xmlHeader = {
    "Accept": "application/xml",
}
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_REGEX_DATETIME_FORMAT = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"

# custom HTTPS opener BUG-26352


class HTTPSConnectionV3(httplib.HTTPSConnection):
    def __init__(self, *args, **kwargs):
        httplib.HTTPSConnection.__init__(self, *args, **kwargs)

    def get_ssl_versions(self):  # pylint: disable=no-self-use
        ssl_versions = ['PROTOCOL_TLSv1', 'PROTOCOL_TLSv1_2', 'PROTOCOL_SSLv23']
        for ssl_version in ssl_versions:
            try:
                yield getattr(ssl, ssl_version)
            except AttributeError:
                logger.error("No %s, skipping", ssl_version)

    def connect(self):
        last_error = ssl.SSLError("Unkown error, check util.py")
        for ssl_version in self.get_ssl_versions():
            try:
                sock = socket.create_connection((self.host, self.port), self.timeout)
                if self._tunnel_host:
                    self.sock = sock
                    self._tunnel()
                self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl_version)
                break
            except ssl.SSLError, e:
                sock.close()
                self.sock = None
                last_error = e

        if not self.sock:
            logger.error(e)
            logger.error("Cannot connect to %s:%s", self.host, self.port)
            raise last_error


class HTTPSHandlerV3(urllib2.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(HTTPSConnectionV3, req)


# install opener
urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))


def handle_proxy():
    proxy_settings = dict()
    for k, v in os.environ.items():
        if k.rfind('_proxy') > -1:
            proxy_settings[k] = v
    logger.debug(proxy_settings)
    proxy_handler = urllib2.ProxyHandler(proxy_settings)
    opener = urllib2.build_opener(proxy_handler)
    urllib2.install_opener(opener)


def setClassPath(basedir, matchstr, depth=999):
    matchFile = findMatchingFiles(basedir, matchstr, depth=depth)
    classpath = (os.pathsep).join(matchFile)
    return classpath


def findMatchingFiles(basedir, matchstr, isDirectory=False, depth=999):
    matchFiles = []
    numsep = basedir.count(os.path.sep)
    for root, dirnames, filenames in os.walk(basedir):
        cnumsep = root.count(os.path.sep)
        if (cnumsep - numsep) > (depth - 1):
            del dirnames[:]
            continue
        if not isDirectory:
            for filename in fnmatch.filter(filenames, matchstr):
                matchFiles.append(os.path.join(root, filename))
        else:
            for dirname in fnmatch.filter(dirnames, matchstr):
                matchFiles.append(os.path.join(root, dirname))
    return matchFiles


def compareFilesIgnoreDupsAndOrder(act_file, exp_file):
    '''
    Compares contents of two files. ignores duplicate lines and order of lines
    Returns True if comparison does match.
    '''
    # check if the files are valid
    if not os.path.isfile(act_file) or not os.path.isfile(exp_file):
        return False

    # get the lines in an array
    actLines = set([line.rstrip('\r\n') for line in open(act_file).readlines()])
    expLines = set([line.rstrip('\r\n') for line in open(exp_file).readlines()])
    if actLines == expLines:
        logger.info("both lines had matching content")
        return True
    logger.info("Following lines were missing in actual output")
    for line in sorted(expLines - actLines):
        logger.info(line)
    logger.info("Following additional unexpected lines found in actual output")
    for line in sorted(actLines - expLines):
        logger.info(line)
    return False


def compareFilesIgnoreOrder(act_file, exp_file):
    if not os.path.isfile(act_file) or not os.path.isfile(exp_file):
        logger.info("Failed to find the files to compare")
        return False
    # get the lines in an array
    actLines = [line.rstrip('\r\n') for line in open(act_file).readlines()]
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    if sorted(actLines) == sorted(expLines):
        logger.info("Files match")
        return True
    else:
        missing_lines = []
        logger.info("Following lines were missing in actual output")
        for pos, line in enumerate(expLines):
            if pos < len(actLines) and line != actLines[pos]:
                missing_lines.append(line)
        for line in missing_lines:
            logger.info(line)
        logger.info("Following additional unexpected lines found in actual output")
        missing_lines = []
        for pos, line in enumerate(actLines):
            if pos < len(expLines) and line != expLines[pos]:
                missing_lines.append(line)
        for line in missing_lines:
            logger.info(line)
        return False


def compareOutputToFileIgnoreDupsAndOrder(act_out, exp_file):
    '''
    Compares contents of stdout and file. ignores duplicate lines and order of lines
    Returns True if comparison does match.
    '''
    # check if the expected output file is valid
    if not os.path.isfile(exp_file):
        return False
    actLines = act_out
    if not isinstance(act_out, list):
        actLines = act_out.replace("\r", "")
        actLines = actLines.split("\n")
    actLines = set(actLines)
    expLines = set([line.rstrip('\r\n') for line in open(exp_file).readlines()])
    if actLines == expLines:
        logger.info("both lines had matching content")
        return True
    logger.info("Following lines were missing in actual output")
    for line in sorted(expLines - actLines):
        logger.info(line)
    logger.info("Following additional unexpected lines found in actual output")
    for line in sorted(actLines - expLines):
        logger.info(line)
    return False


def compareOutputsIgnoreDupsAndOrder(act_out, exp_out):
    '''
    Compares contents of stdout and file. ignores duplicate lines and order of lines
    Returns True if comparison does match.
    '''
    actLines = act_out
    if not isinstance(act_out, list):
        actLines = act_out.replace("\r", "")
        actLines = actLines.split("\n")
    actLines = set(actLines)
    expLines = exp_out
    if not isinstance(exp_out, list):
        expLines = exp_out.replace("\r", "")
        expLines = expLines.split("\n")
    expLines = set(expLines)
    if actLines == expLines:
        logger.info("both lines had matching content")
        return True
    logger.info("Following lines were missing in actual output")
    for line in sorted(expLines - actLines):
        logger.info(line)
    logger.info("Following additional unexpected lines found in actual output")
    for line in sorted(actLines - expLines):
        logger.info(line)
    return False


# TODO: Rename the vars variable
def compareOutputToFile(  # pylint: disable=redefined-builtin
        act_out, exp_file, regex=True, vars=None, inbetween=False, rstrip=False
):
    '''
    Compares a string and contents of a file.
    Returns True if comparison does match.
    '''
    if not vars:
        vars = {}
    if not os.path.isfile(exp_file):
        logger.info("compareOutputToFile finds no exp_file. Returning False.")
        return False
    actLines = act_out
    if not isinstance(act_out, list):
        actLines = act_out.replace("\r", "")
        actLines = actLines.split("\n")
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    return compareLines(actLines, expLines, regex, vars, inbetween, rstrip=rstrip)


# TODO: Rename the vars variable
def compareFiles(act_file, exp_file, regex=True, vars=None, inbetween=False):  # pylint: disable=redefined-builtin
    '''
    Compares contents of two files.
    Returns True if comparison does match.
    '''
    if not vars:
        vars = {}
    # check if the files are valid
    if not os.path.isfile(act_file) or not os.path.isfile(exp_file):
        return False

    # get the lines in an array
    actLines = [line.rstrip('\r\n') for line in open(act_file).readlines()]
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    return compareLines(actLines, expLines, regex, vars, inbetween)


def removeUnwantedLines(output):
    outputLines = output.replace("\r\n", "\n").split("\n")
    for line in outputLines:
        if (re.search(".*Microsoft.*", line) != None
                or re.search("azurefs2 has a full queue and can.*t consume the given metrics", line) != None
                or re.search(".*Not enabling OAuth2 in WebHDFS.*", line) != None
                or re.search(".*S3FileSystem is deprecated and will be removed in future releases.*", line) != None
                or re.search(".*INFO util.NativeCodeLoader:.*", line) != None
                or re.search(".*WARN erasurecode.ErasureCodeNative:.*", line) != None
                or re.search(".*WARN security.UserGroupInformation:.*", line) != None
                or re.search(".*WARNING: HADOOP_SECURE_DN_USER has been.*", line) != None):
            output = output.replace("%s\r\n" % line, "")
            # On Humboldt there is no carriage return character \r, hence need to do below
            output = output.replace("%s\n" % line, "")
            # In some cases when the unwanted line appears at the end, without \n
            output = output.split("\n%s" % line)[0]
    return output


#Given stdout of $hdfs dfs -ls as input string, return similar stdout with lines sorted by file size
def getSortedHDFSLsOutputByFileSize(inputString):
    inputString = removeUnwantedLines(inputString)
    lines = inputString.split("\n")
    nonFileEntryLineList = []
    fileEntryLineList = []
    pattern = r"[rwx\-]+   .+ .+ .+ \d+ [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}.*"
    for line in lines:
        if re.search(pattern, line) is not None:
            fileEntryLineList.append(line)
        else:
            nonFileEntryLineList.append(line)

    #sort entries based on file size numerically
    fileEntryLineList.sort(key=lambda line: int(line.split()[4]))

    if nonFileEntryLineList:
        return '\n'.join(nonFileEntryLineList) + '\n' + '\n'.join(fileEntryLineList)
    else:
        return '\n'.join(nonFileEntryLineList) + '\n'.join(fileEntryLineList)


# Dump logs of lines 1 and lines 2 to console with number of lines limit
# Default line limit is mainly to prevent mistaken use. We don't want to flood console logs with this output.
def dumpLinesLogs(
        lines1,
        lines2,
        prefixLine1=None,
        prefixLine2=None,
        lineLimit=20,
        dumpLines1AtTheEnd=False,
        dumpLines2AtTheEnd=False
):
    #set default prefix of console output of line 1 and line 2
    if prefixLine1 is None:
        prefixLine1 = "param 1 line"
    if prefixLine2 is None:
        prefixLine2 = "param 2 line"
    #shift array to index 1 to n from index 0 to n-1
    #this method does not modify original lines.
    tmpLines1 = [None] + lines1[0:]
    tmpLines2 = [None] + lines2[0:]
    #maxNumLines = n
    maxNumLines = max(len(lines1), len(lines2))
    lineNo = 1
    dumpLog = []
    foundDifference = False
    while lineNo <= maxNumLines:
        outputLine = "--Line " + str(lineNo) + "--"
        if ((lineNo > len(lines1) and lineNo <= len(lines2)) or (lineNo <= len(lines1) and lineNo > len(lines2))
                or tmpLines1[lineNo] != tmpLines2[lineNo]):
            if not foundDifference:
                outputLine += " *** DIFFERENCE BEGINS TO OCCUR HERE ***"
            foundDifference = True

        #if two lines are the same, print the line only once
        if (lineNo < len(tmpLines1) and lineNo < len(tmpLines2) and tmpLines1[lineNo] == tmpLines2[lineNo]):
            outputLine = "Line %s: %s" % (lineNo, tmpLines1[lineNo])
        else:
            rightAlignmentNum = max(len(prefixLine1), len(prefixLine2))
            outputLine += "\n" + ('{0:>%d}' % (rightAlignmentNum)).format(prefixLine1) + ": "
            if lineNo < len(tmpLines1):
                outputLine += tmpLines1[lineNo]
            else:
                outputLine += "N/A"
            outputLine += "\n" + ('{0:>%d}' % (rightAlignmentNum)).format(prefixLine2) + ": "
            if lineNo < len(tmpLines2):
                outputLine += tmpLines2[lineNo]
            else:
                outputLine += "N/A"
        dumpLog.append(outputLine)
        lineNo += 1
    #print logs if line limit is less than max number of lines
    if lineLimit < maxNumLines:
        logger.info("dumping first %s lines out of %s lines", lineLimit, maxNumLines)
    logger.info('\n'.join(dumpLog[0:lineLimit]))

    #print lines 1 or lines 2 at the end
    if dumpLines1AtTheEnd:
        #print logs of lines 1
        logger.info('==========%s==========', prefixLine1)
        logger.info('\n'.join(lines1[0:lineLimit]))
    if dumpLines2AtTheEnd:
        #print logs of lines 2
        logger.info('==========%s==========', prefixLine2)
        logger.info('\n'.join(lines2[0:lineLimit]))


# Compare lines
# Lines are a list of strings. Don't give out just string. That is incorrect usage.
# TODO: Replace name of the argument vars
def compareLines(  # pylint: disable=redefined-builtin
        act_lines, exp_lines, regex=True, vars=None, inbetween=False, rstrip=False, logoutput=False
):
    '''
    Compare two list of strings.
    Returns True if comparison does match.
    Regex is supported when regex is True.
    Variable replacement is supported when vars is not empty.
    if inbetween is True, and line count of act_lines are more than exp_lines, cut act_lines
    short and continue to comparison code path.
    '''
    if not vars:
        vars = {}
    # count the number of lines and they should match
    actCount = len(act_lines)
    expCount = len(exp_lines)
    if logoutput:
        logger.info("compareLines actCount = %s expCount = %s", actCount, expCount)
    if actCount != expCount:
        if inbetween and actCount > expCount:
            act_lines = act_lines[(actCount - expCount):]
        else:
            logger.info("actCount does not equal expCount. actCount=%s, expCount=%s", actCount, expCount)
            dumpLinesLogs(act_lines, exp_lines, "actual", "expected", 500)
            return False

    # compare line by line
    if regex:
        for count in xrange(expCount):
            if rstrip:
                matchObj = re.match(
                    replaceVars(exp_lines[count], vars).rstrip(), act_lines[count].replace("\r", "").rstrip()
                )
            else:
                matchObj = re.match(replaceVars(exp_lines[count], vars), act_lines[count].replace("\r", ""))
            if not matchObj:
                logger.info("--regex on line-by-line comparison does not match at line %s.--", count + 1)
                logger.info("-expected line with variable-\n%s", exp_lines[count])
                logger.info("-expected line with variable replaced-\n%s", replaceVars(exp_lines[count], vars))
                logger.info("-actual line-\n%s", act_lines[count])
                dumpLinesLogs(act_lines, exp_lines, "actual", "expected", 500)
                return False
    else:
        for count in xrange(expCount):
            if rstrip:
                compareResult = replaceVars(exp_lines[count], vars).rstrip() != act_lines[count].replace("\r", ""
                                                                                                         ).rstrip()
            else:
                compareResult = replaceVars(exp_lines[count], vars) != act_lines[count].replace("\r", "")
            if compareResult:
                logger.info("--regex off line-by-line comparison does not match at line %s.--", count + 1)
                logger.info("-expected line with variable-\n%s", exp_lines[count])
                logger.info("-expected line with variable replaced-\n%s", replaceVars(exp_lines[count], vars))
                logger.info("-actual line-\n%s", act_lines[count])
                dumpLinesLogs(act_lines, exp_lines, "actual", "expected", 500)
                return False
    return True


# TODO: Replace name of the argument vars
def replaceVars(text, vars):  # pylint: disable=redefined-builtin
    intext = text
    for key, value in vars.items():
        intext = intext.replace("${%s}" % key, value)
    return intext


# TODO: Replace name of the argument vars
def replaceVarsInFile(fileName, vars):  # pylint: disable=redefined-builtin
    '''
    Replace key in ${vars} with value in file ${filename}
    '''
    filedata = ''
    try:
        file_ = open(fileName, 'r')
        filedata = file_.read()
    finally:
        file_.close()

    for key, value in vars.items():
        newdata = filedata.replace("${%s}" % key, value)

    try:
        file_ = open(fileName, 'w')
        file_.write(newdata)
    finally:
        file_.close()


# TODO: Rename the vars variable
def copyFile(src, dest, vars=None):  # pylint: disable=redefined-builtin
    if not vars:
        vars = {}
    srctext = open(src).read()
    desttext = replaceVars(srctext, vars)
    writeToFile(desttext, dest)


# Write text into a file
# wtext - Text to write
def writeToFile(wtext, outfile, isAppend=False):
    mode = 'w'
    if isAppend:
        mode = 'a+'
    outf = open(outfile, mode)
    try:
        outf.write(wtext)
    finally:
        outf.close()


def removeLineFromFile(wtext, infile):
    from beaver.machine import Machine
    if Machine.isWindows():
        file_name = infile.split('\\')[-1]
    else:
        file_name = infile.split('/')[-1]
    logger.info("file name = %s", file_name)
    tmp_file = os.path.join(Machine.getTempDir(), file_name)
    if Machine.pathExists(user=None, host=None, filepath=tmp_file, passwd=None):
        Machine.rm(user=None, host=None, filepath=tmp_file, isdir=False, passwd=None)
    f_out = open(tmp_file, "w")
    with open(infile, "r") as f:
        for line in f:
            if not re.search(wtext, line):
                f_out.write(line)
    f.close()
    f_out.close()
    Machine.copy(tmp_file, infile, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())


# Write given text to a temporary file.
# The file is not automatically deleted. API user needs to remove the file manually.
def writeToTempFile(wtext):
    f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    try:
        f.write(wtext)
    finally:
        f.close()
    return filename


def extractAndPlot(textToSearch, regex, plotfile):
    m = re.match(regex, textToSearch, re.DOTALL)
    if not m:
        return
    value = m.group(1)
    writeToPlotfile(value, plotfile)


def writeToPlotfile(textToWrite, plotfile):
    writeToFile("YVALUE=%s" % textToWrite, plotfile)


# Get the property value from configuration file
# xmlfile - Path of the config file
# name - Property name
# default - Property value in case not found
def getPropertyValueFromConfigXMLFile(xmlfile, name, defaultValue=None):
    xmldoc = minidom.parse(xmlfile)
    propNodes = [
        node.parentNode for node in xmldoc.getElementsByTagName("name") if node.childNodes[0].nodeValue == name
    ]
    if propNodes:
        for node in propNodes[0].childNodes:
            if node.nodeName == "value":
                if node.childNodes:
                    return node.childNodes[0].nodeValue
                else:
                    return ''
    return defaultValue


def getPropertyValueFromIniFile(inifile, section, name, defaultValue=None):
    Config = ConfigParser.ConfigParser()
    Config.optionxform = str
    Config.read(inifile)
    if Config.has_option(section, name):
        return Config.get(section, name)
    return defaultValue


# Parse the JUnit testresults and return a flat testcase result map
# junitxmlfile - junit testresults
# output will be of the following form:
# {tcid1: {result: 'pass|fail', failure: ''},...}
def parseJUnitXMLResult(junitxmlfile):
    xmldoc = minidom.parse(junitxmlfile)
    testresult = {}
    tsnodes = xmldoc.getElementsByTagName("testsuite")
    counter = 1
    for node in tsnodes:
        tsname = node.getAttribute("name")
        for tschildnode in node.childNodes:
            if tschildnode.nodeType == tschildnode.ELEMENT_NODE and tschildnode.nodeName == "testcase":
                if tschildnode.getAttribute("name") == "f":
                    testname = tschildnode.getAttribute("classname")
                    if str(tsname + "-" + testname) in testresult.keys():
                        counter = counter + 1
                        testname = testname + "_" + str(counter)
                else:
                    testname = tschildnode.getAttribute("name")
                tcid = str(tsname + "-" + testname)

                tcresult = {'result': 'pass', 'failure': ''}
                if tschildnode.hasChildNodes():
                    for tccnode in tschildnode.childNodes:
                        if tccnode.nodeType == tccnode.ELEMENT_NODE and tccnode.nodeName == "failure":
                            tcresult['failure'] = tccnode.getAttribute("message")
                            tcresult['result'] = 'fail'
                        elif tccnode.nodeType == tccnode.ELEMENT_NODE and tccnode.nodeName == "skipped":
                            tcresult['result'] = 'skip'
                        elif tccnode.nodeType == tccnode.ELEMENT_NODE and tccnode.nodeName == "error":
                            tcresult['failure'] = tccnode.getAttribute("message")
                            tcresult['result'] = 'fail'
                            for subNode in tccnode.childNodes:
                                if subNode.nodeType == subNode.TEXT_NODE:
                                    tcresult['failure'] += "\n" + subNode.nodeValue.encode('utf-8')
                if tcid in testresult.keys():
                    repeat_count = 2
                    tcid = tcid.encode('utf-8')
                    while tcid + "-" + str(repeat_count) in testresult.keys():
                        repeat_count += 1
                    testresult[tcid + "-" + str(repeat_count)] = tcresult
                else:
                    testresult[tcid] = tcresult
    return testresult


def deletePropertyFromXML(infile, outfile, propertyList):
    tree = ET.parse(infile)
    root = tree.getroot()
    for prop in root.findall('property'):
        name = prop.find('name').text
        if name in propertyList:
            root.remove(prop)
    wtext = "<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
    writeToFile(wtext, outfile)
    writeToFile(ET.tostring(root), outfile, True)


def createBlankConfigXML(filename):
    '''
    Creates a config xml with given filename.
    Returns None always.

    Contents are:
    <configuration/>
    '''
    configuration = ET.Element("configuration")
    tree = ET.ElementTree(configuration)
    tree.write(filename)


# TODO: Rename the file variable
def addPropertyToXML(file, propertyMap):  # pylint: disable=redefined-builtin
    '''
    Adds properties to config XML.
    Returns None always.

    The XML file must have <configuration> </configuration> tag beforehand.
    '''
    xmldoc = minidom.parse(file)
    cfgnode = xmldoc.getElementsByTagName("configuration")
    if not cfgnode:
        raise Exception("Invalid Config XML file: " + file)
    cfgnode = cfgnode[0]
    for prop in propertyMap:
        pn = xmldoc.createElement("property")
        nn = xmldoc.createElement("name")
        ntn = xmldoc.createTextNode(prop)
        nn.appendChild(ntn)
        pn.appendChild(nn)
        vn = xmldoc.createElement("value")
        vtn = xmldoc.createTextNode(str(propertyMap[prop]))
        vn.appendChild(vtn)
        pn.appendChild(vn)
        cfgnode.appendChild(pn)
    writeToFile(xmldoc.toxml(), file)


# Update the XML configuration properties and write to another file
# infile - Input config XML file
# outfile - Output config XML file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToConfigXMLFile(infile, outfile, propertyMap):
    xmldoc = minidom.parse(infile)
    cfgnode = xmldoc.getElementsByTagName("configuration")
    if not cfgnode:
        raise Exception("Invalid Config XML file: " + infile)
    cfgnode = cfgnode[0]
    propertyMapKeys = propertyMap.keys()
    removeProp = []
    modified = []
    for node in xmldoc.getElementsByTagName("name"):
        name = node.childNodes[0].nodeValue.strip()
        if name in propertyMapKeys:
            modified.append(name)
            for vnode in node.parentNode.childNodes:
                if vnode.nodeName == "value":
                    if vnode.childNodes == []:
                        removeProp.append(name)
                        modified.remove(name)
                    else:
                        vnode.childNodes[0].nodeValue = propertyMap[name]
    remaining = list(set(propertyMapKeys) - set(modified))
    # delete properties whose value is set to None e.g.<value></value>
    for node in xmldoc.getElementsByTagName("name"):
        name = node.childNodes[0].nodeValue.strip()
        if name in removeProp:
            parent = node.parentNode
            super_ = parent.parentNode
            super_.removeChild(parent)
    for property_ in remaining:
        pn = xmldoc.createElement("property")
        nn = xmldoc.createElement("name")
        ntn = xmldoc.createTextNode(property_)
        nn.appendChild(ntn)
        pn.appendChild(nn)
        vn = xmldoc.createElement("value")
        vtn = xmldoc.createTextNode(str(propertyMap[property_]))
        vn.appendChild(vtn)
        pn.appendChild(vn)
        cfgnode.appendChild(pn)
    writeToFile(xmldoc.toxml(), outfile)


# Read the XML configuration properties into a dict objct
# xmlfile - String - Input config XML file
# returns - Dictionary - {'name1':'value1', 'name2':'value2',...}
def readDictFromConfigXMLFile(xmlfile):
    dict_ = {}
    doc = minidom.parse(xmlfile)
    nodes = doc.getElementsByTagName("configuration")
    if not nodes:
        raise Exception("Invalid Config XML file: " + xmlfile)
    for nnode in doc.getElementsByTagName("name"):
        name = nnode.childNodes[0].nodeValue.strip()
        dict_[name] = None
        for vnode in nnode.parentNode.childNodes:
            if vnode.nodeName == "value":
                try:
                    dict_[name] = vnode.childNodes[0].nodeValue.strip()
                except IndexError:
                    dict_[name] = " "
                break
    return dict_


# Writes a dictionary to an XML configuration properties format file
# dict - Dictionary - {'name1':'value1', 'name2':'value2',...}
# xmlfile - String - Output filename. Will be overwritten.
# TODO: Rename the dict variable!
def writeDictToConfigXmlFile(dict, xmlfile):  # pylint: disable=redefined-builtin
    doc = minidom.Document()
    config = doc.createElement("configuration")
    doc.appendChild(config)
    for name in dict.keys():
        pn = doc.createElement("property")
        nn = doc.createElement("name")
        ntn = doc.createTextNode(name)
        nn.appendChild(ntn)
        pn.appendChild(nn)
        vn = doc.createElement("value")
        vtn = doc.createTextNode(str(dict[name]))
        vn.appendChild(vtn)
        pn.appendChild(vn)
        config.appendChild(pn)
    xmlstr = doc.toxml()
    outfile = open(xmlfile, 'w')
    try:
        outfile.write(xmlstr)
    finally:
        outfile.close()


# Add and/or update JSON configuration properties and write to another file
# infile - Input config JSON file
# outfile - Output config JSON file
# propertyMap - Properties to add and/or update
#               {'name1':'value1', 'name2':'value2', 'name3':['value31','value32'], 'name4':{'key4':'value4'}, ...}
# parentPath - The complete path from top-level to the parent of the properties to be updated.
#              Pass empty array [] if the properties to be updated are top-level attributes.
# destPropertyPrefix - The prefix to be added to the keys of propertyMap prior to lookup in infile.
#                      Pass empty string "" for no prefix.
def writePropertiesToConfigJSONFile(infile, outfile, propertyMap, parentPath, destPropertyPrefix):
    '''
    Add and/or update JSON config properties
    Returns None always.

    The JSON file must contain the parentPath starting from the top-level.
    Specify a non-empty destPropertyPrefix string if it needs to be
    prepended before lookup in the JSON file.

    If properties need to be added/updated at different parent paths then
    call this method multiple times once for each unique parent path.

    A sample call:
    writePropertiesToConfigJSONFile(infile, outfile, {'hfile.format.version':'3'}, ["global"], "site.hbase-site.")
    '''
    fInfile = open(infile)
    jsonData = json.load(fInfile)
    fInfile.close()
    currentParent = jsonData
    for path in parentPath:
        if path in currentParent:
            currentParent = currentParent[path]
        else:
            raise Exception("Invalid parentPath %s, or invalid JSON file: %s" % (parentPath, infile))
    for key in propertyMap:
        currentParent[destPropertyPrefix + key] = propertyMap.get(key)
    logger.info("writePropertiesToConfigJSONFile writing to outfile: %s", outfile)
    writeToFile(json.dumps(jsonData, sort_keys=True, indent=2, separators=(',', ': ')), outfile)


def writePropertiesToConfigJSONFileMulti(infile, outfile, pathPropertiesList, logoutput=False):
    '''
    Add and/or update JSON config properties
    Returns None always.
    writePropertiesToConfigJSONFileMulti(infile, outfile, [
                                         (["global"],
                                           {"java_home": "/usr/jdk6"}),
                                         (["components", "slider-appmaster"],
                                           {"jvm.heapsize" : "512M"} )])
    '''
    fInfile = open(infile)
    jsonData = json.load(fInfile)
    fInfile.close()
    for pathProperties in pathPropertiesList:
        if logoutput:
            logger.info("pathProperties[0], pathProperties[1]  = (%s,%s)", pathProperties[0], pathProperties[1])
        currentParent = jsonData
        skip = False
        for path in pathProperties[0]:
            if path in currentParent:
                currentParent = currentParent[path]
            else:
                skip = True
        if not skip:
            propertiesMap = pathProperties[1]
            for propertyKey in propertiesMap.keys():
                if logoutput:
                    logger.info("setting %s to %s", propertyKey, propertiesMap.get(propertyKey))
                currentParent[propertyKey] = propertiesMap.get(propertyKey)
    logger.info("writePropertiesToConfigJSONFileMulti writing to outfile: %s", outfile)
    writeToFile(json.dumps(jsonData, sort_keys=True, indent=2, separators=(',', ': ')), outfile)


# Get the value for a property in Java style properties file
# does not work for multiline properties
# propfile - property filepath
# key - property name
def getPropertyValueFromFile(propfile, key):
    try:
        proptext = open(propfile).read()
        return getPropertyValue(proptext, key)
    except IOError:
        logger.info("Error: File Not Found")
        return None


# finds all properties matching keypart and returns their values as comma separated string
# ex. /var/lib/nifi/provenance_repository,/var/lib/nifi/repo1
# propfile - property filepath
# keypart - partial (or full) property name
# delimiter - string separating the key and value
def getPropertyValuesFromFile(propfile, keypart, delimiter="\\="):
    try:
        proptext = open(propfile).read()
        regx = re.compile(r"[^#]?%s.*\s*%s(.*)\n?" % (keypart, delimiter))
        values = re.findall(regx, proptext)
        if not values:
            logger.info("properties matching keypart: %s were not found in %s", keypart, propfile)
            return None
        return ",".join(values)
    except IOError:
        logger.error("Error: File Not Found ")
        return None


# Get the value for a property in bash style properties file.
# Given key = YARN_LOG_DIR, look for value of export YARN_LOG_DIR=
# If the line if in the form of KEY=${KEY:-defaultvalue}, return defaultvalue.
#    This is common in hadoop-env.sh or yarn-env.sh.
# This method does not work for multiline properties.
# propfile - property filepath
# key - property name
def getPropertyValueFromBashFile(propfile, key):
    try:
        proptext = open(propfile).read()
        tmp = getPropertyValue(proptext, "export " + key)
        matchObj = re.search("[$]{%s:-(.*)}" % key, tmp)
        if matchObj is not None:
            tmp = matchObj.group(1)
        return tmp
    except Exception as e:
        logger.info("Error: %s", str(e))
        return None


# Parse a key <delimiter> value combinations in multiline
# does not work for multiline properties
# contents - string to search in
# key - property name to search value for
# delimiter - string separating the key and value
def getPropertyValue(contents, key, delimiter="\\="):
    m = re.search(r"[^#]*%s\s*%s(.*)\n?" % (key, delimiter), contents)
    if m:
        return m.group(1).strip()
    return None


# Update the Java style properties file with the key, values from propertyMap
# infile - Input properties file
# outfile - Output properties file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToFile(infile, outfile, propertyMap):
    modified = []
    propstr = ""
    key = ""
    foundslash = False
    if infile is not None:
        for line in open(infile).readlines():
            sline = line.strip()
            if not foundslash:
                if sline.startswith("#"):
                    propstr += line
                elif sline.find("=") != -1:
                    key, _value = sline.split("=", 1)
                    key = key.strip()
                    if propertyMap.has_key(key):
                        if propstr and propstr[-1] != '\n':
                            propstr += "\n"
                        propstr += "%s=%s\n" % (key, propertyMap[key])
                        modified.append(key)
                    else:
                        propstr += line
            else:
                if key != "" and not propertyMap.has_key(key):
                    propstr += line
            foundslash = sline.endswith("\\")
    remaining = list(set(propertyMap.keys()) - set(modified))
    if propstr and propstr[-1] != '\n':
        propstr += "\n"
    for property_ in remaining:
        propstr += "%s=%s\n" % (property_, propertyMap[property_])
    writeToFile(propstr, outfile)


# Update the INI style properties file with the key, values from propertyMap
# infile - Input properties file
# outfile - Output properties file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToIniFile(infile, outfile, propertyMap):
    Config = ConfigParser.ConfigParser()
    Config.optionxform = str
    Config.read(infile)

    for key, val in propertyMap.items():
        sec, name = key.split(':')
        if Config.has_section(sec):
            Config.set(sec, name, val)
        else:
            Config.add_section(sec)
            Config.set(sec, name, val)
    outf = open(outfile, 'w')
    try:
        Config.write(outf)
    finally:
        outf.close()


# Update conf file for component like Spark
# The format of Conf file is  "<property> <value>"
# infile - Input properties file
# outfile - Output properties file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToConfFile(infile, outfile, propertyMap):
    propstr = ""
    modified = []
    if infile is not None:
        for line in open(infile).readlines():
            sline = line.strip()
            if not sline.startswith("#") and sline.strip() != "":
                key = sline.split(" ", 1)[0]
                key = key.strip()
                if propertyMap.has_key(key):
                    if propstr and propstr[-1] != '\n':
                        propstr += "\n"
                    propstr += "%s %s\n" % (key, propertyMap[key])
                    modified.append(key)
                else:
                    propstr += line
            else:
                propstr += line
    remaining = list(set(propertyMap.keys()) - set(modified))
    if propstr and propstr[-1] != '\n':
        propstr += "\n"
    for property_ in remaining:
        propstr += "%s %s\n" % (property_, propertyMap[property_])
    writeToFile(propstr, outfile)


# Get the content of the URL and optionally write to file
# url - URL
# outfile - output file
def getURLContents(url, outfile="", headers=None, logoutput=False, kerberosAuthDisabled=False, user="hrt_qa"):
    try:
        req = None
        handle_proxy()
        from beaver.component.hadoop import Hadoop
        if Hadoop.isUIKerberozied():
            response = requests.get(
                url, headers=headers, auth=_kerberosAuth(kerberosAuthDisabled, user=user), verify=False
            ).content

        else:
            if headers:
                req = urllib2.Request(url, headers=headers)
            else:
                req = urllib2.Request(url)
            u = urllib2.urlopen(req)
            response = u.read()
            u.close()
        if logoutput:
            logger.info("getURLContents url=%s", url)
            logger.info("getURLContents response=%s", response)
    except urllib2.URLError, e:
        # HTTPError is subclass of URLError. Determine if the instance is a http error and add appropriate log
        if isinstance(e, urllib2.HTTPError):
            logger.error("Error loading url %s", url)
            logger.error("HTTP Code: %s", e.code)
            logger.error("HTTP Data: %s", e.read())
        else:
            logger.error("Error loading url(%s): %s", url, e.reason)
        response = ""
    if outfile != "":
        writeToFile(response, outfile)
    return response


# method to downlod the contents of the url to a file
def downloadUrl(url, filePath, blockSize=8192):
    logger.info("Downloading from url -> %s", url)
    expFileSize = 0
    actFileSize = 0
    try:
        handle_proxy()
        req = urllib2.Request(url)
        u = urllib2.urlopen(req)
        meta = u.info()
        expFileSize = int(meta.getheaders("Content-Length")[0])
        logger.info("Expected file size = %s", str(expFileSize))
        # open the file for writing
        f = open(filePath, 'wb')
        bufferRead = u.read(blockSize)
        while bufferRead:
            actFileSize += len(bufferRead)
            f.write(bufferRead)
            bufferRead = u.read(blockSize)
        f.close()
        logger.info("Actual file size = %s", str(actFileSize))
        logger.info("filePath = %s", filePath)
        # make sure file sizes match
        return expFileSize == actFileSize

    except urllib2.URLError, e:
        logger.error(e)
        logger.error("Failed to download file from %s, filePath %s .", url, filePath)
        return False


# Serializes the URL content into a Python object
# url - URL
def getJSONContent(url, headers=None, logoutput=False, kerberosAuthDisabled=False, user="hrt_qa"):
    contents = getURLContents(
        url, headers=headers, logoutput=logoutput, kerberosAuthDisabled=kerberosAuthDisabled, user=user
    )
    return getJSON(contents)


def getJSON(content):
    try:
        jsoncontent = json.loads(content)
    except ImportError:
        # pylint: disable=eval-used
        jsoncontent = eval(content.replace('null', 'None').replace('true', 'True').replace('false', 'False'))
    except ValueError:
        jsoncontent = None
    return jsoncontent


def getJSONCurl(url, logoutput=True):
    """
    Get JSON object with output from curl API.
    :param url:
    :param logoutput:
    :return: a dict
    """
    (statusCode, stdout) = curl(url, logoutput=logoutput)
    if logoutput:
        logger.info("getJSONCurl (statusCode, stdout) = (%s,%s)", statusCode, stdout)
    j = getJSON(stdout)
    if logoutput:
        logger.info("getJSONCurl j = %s", j)
    return j


def getJSONWithOptionalSPNego(url, isSecured, logoutput=True):
    """
    Get json object from the url. If secured, use SPNego curl.
    :param url:
    :param isSecured:
    :param logoutput:
    :return: a dict if succeeded
             Otherwise, can be None or a dict (need more data point).
    """
    if logoutput:
        logger.info("getJSONWithOptionalSPNego start")
    if isSecured:
        j = getJSONCurl(url, logoutput=logoutput)
    else:
        j = getJSONContent(url, logoutput=logoutput)
    if logoutput:
        logger.info("j = %s", j)
    return j


# Performs HTTP operations
# data - Data to PUT, POST
# method - HTTP protocols GET, PUT, HEAD, POST, DELETE, ...
def httpRequest(url, headers=None, data='', method='GET', retry=1, cookie=None):
    if not headers:
        headers = {}
    retcode = -1
    retdata = None
    retheaders = {}
    for _ in range(retry):
        try:
            handle_proxy()
            opener = urllib2.build_opener(HTTPSHandlerV3())
            urllib2.install_opener(opener)
            request = urllib2.Request(url)
            for hkey, hvalue in headers.items():
                request.add_header(hkey, hvalue)
            if cookie:
                opener.addheaders.append(('Cookie', cookie))
            if data != "":
                request.add_data(data)
            request.get_method = lambda: method
            response = opener.open(request)
            retcode = response.getcode()
            retdata = response.read()
            retheaders = response.headers
            response.close()
            break
        except urllib2.URLError, e:
            if isinstance(e, urllib2.HTTPError):
                retcode = e.code
                retdata = e.msg
                retheaders = e.headers
                break
            else:
                if retry > 1:
                    logger.info("Sleep 10 secs due to URL error before retry")
                    time.sleep(10)
    return retcode, retdata, retheaders


# Performs HTTP operations
# data - Data to PUT, POST
# method - HTTP protocols GET, PUT, HEAD, POST, DELETE, ...
def httpsRequest(url, headers=None, data='', method='GET', retry=1):
    if not headers:
        headers = {}
    retcode = -1
    retdata = None
    retheaders = {}
    for _ in range(retry):
        try:
            opener = urllib2.build_opener(urllib2.HTTPSHandler)
            request = urllib2.Request(url)
            for hkey, hvalue in headers.items():
                request.add_header(hkey, hvalue)
            if data != "":
                request.add_data(data)
            request.get_method = lambda: method
            response = opener.open(request)
            retcode = response.getcode()
            retdata = response.read()
            retheaders = response.headers
            response.close()
            break
        except urllib2.URLError, e:
            if isinstance(e, urllib2.HTTPError):
                retcode = e.code
                retdata = e.msg
                retheaders = e.headers
                break
            else:
                if retry > 1:
                    logger.info("Sleep 10 secs due to URL error before retry")
                    time.sleep(10)
    return retcode, retdata, retheaders


# Create a empty file of the given size
def createFileOfSize(size, filename):
    f = open(filename, "wb")
    f.seek(size - 1)
    f.write("\0")
    f.close()


def findMatchingPatternInFile(filename, pattern, return0Or1=True, regexFlags=None):
    '''
    Function to find matching pattern in a file.
    If return0Or1 is True (default), returns 0 if a match is found. 1 otherwise.
    If return0Or1 is False, return a list of regex matching obj if found or empty list.
    '''
    f = None
    try:
        f = open(filename, 'r')
        if regexFlags is None:
            regex = re.compile(pattern)
        else:
            regex = re.compile(pattern, regexFlags)
        if not return0Or1:
            result = []
        for line in f:
            if regex.search(line):
                if return0Or1:
                    return 0
                else:
                    result.append(regex.search(line))
        if return0Or1:
            return 1
        else:
            return result
    finally:
        if f is not None:
            f.close()


def getTextInFile(filename, logoutput=True):
    f = None
    try:
        f = open(filename, 'r')
        result = f.read()
    finally:
        if f is not None:
            f.close()
        if logoutput:
            logger.info("util.getTextInFile returns %s", result)
    return result


def findMatchingPatternInFileMultiLine(filename, pattern, regexFlags=re.MULTILINE | re.DOTALL):
    """
    Function to find matching pattern in a file.
    Returns regex match object.
    The API reads all characters in a file at once, hence allowing regex match for multiline.
    """
    f = None
    try:
        f = open(filename, 'r')
        regex = re.compile(pattern, regexFlags)
        line = f.read()
        return regex.search(line)
    finally:
        if f is not None:
            f.close()


def dumpText(filename, header="", footer="", logoutput=True):  # pylint: disable=unused-argument
    """
    Dump contents from text file to logger.
    File must fit in memory.
    :param filename:
    :param header
    :param footer
    :param logoutput:
    :return: None
    """
    f = None
    lines = ""
    try:
        f = open(filename)
        lines = f.read()
    finally:
        f.close()
    logger.info("")
    if header != "":
        logger.info(header)
    logger.info(lines)
    if footer != "":
        logger.info(footer)


def dumpTextString(v, header="", footer="", logoutput=True):  # pylint: disable=unused-argument
    """
    Dump variable to logger.
    :param v: anything printable
    :param header
    :param footer
    :param logoutput:
    :return: None
    """
    logger.info("")
    if header != "":
        logger.info(header)
    if isinstance(v, dict):
        for key in v:
            logger.info("%s : %s", key, v[key])
    else:
        logger.info(v)
    if footer != "":
        logger.info(footer)


def ignoreNonReadableFiles(path, names):
    ignoreList = []
    for name in names:
        if not os.access(os.path.join(path, name), os.R_OK):
            ignoreList.append(name)
    return ignoreList


# Copy a directory, ignoring any non-reaable files
def copyReadableFilesFromDir(srcpath, destpath):
    copyDir(srcpath, destpath, ignore=ignoreNonReadableFiles)


# Function to copy a directory. If the detination already exists, it will be deleted
def copyDir(srcpath, destpath, symlinks=False, ignore=None):
    if os.path.isdir(destpath):
        if os.path.islink(destpath):
            os.remove(destpath)
        else:
            shutil.rmtree(destpath)
    shutil.copytree(srcpath, destpath, symlinks, ignore)


# Added by Logigear, 13-Aug-2012
# Check a pattern contain in text
def doesContainText(act_text, pattern, regex=True, regexDotAll=False, regexIgnoreCase=False):
    result = False
    if regex:
        flags = 0
        if regexDotAll is True:
            flags = flags | re.DOTALL
        if regexIgnoreCase is True:
            flags = flags | re.IGNORECASE
        if re.search(pattern, act_text, flags):
            result = True
    else:
        if pattern in act_text:
            result = True

    return result


# Pass the testcase name from non-pytest suites to get the list of non-Pytest suites
# So, when pytest launches the command:
# py.test -s test_suite1.py::test_case1[aaa] test_suite2.py::test_case2[aaa]
# call getApplicableTestcases("test_case1") will give ['aaa']
# py.test -s test_suite1.py::test_case[aaa] test_suite1.py::test_case[bbb]
# call getApplicableTestcases("test_case") will give ['aaa', 'bbb']
def getApplicableTestcases(tcwrapper):
    tcs = []
    import inspect
    import pytest
    frm = inspect.stack()[1]
    thisfile = inspect.getmodule(frm[0]).__file__
    for elem in pytest.original_file_or_dir_option:
        m = re.search(r"(\S+)::(\S+)\[(\S+)\]", elem)
        if m is not None and thisfile.find(m.group(1)) != -1 and m.group(2) == tcwrapper:
            tcs.append(m.group(3))
    return tcs


# Parse the Python Unittest framework testresults and return a flat testcase result map
# logfile - testresults generated by Python Unittest framework
# output will be of the following form:
# Time     runTest (tcid.subid.subtest)  ...................................... ok|ERROR
# 22:16:47 runTest (stress.manyScanners.CreateManyScannersTest) ............... ok
def parsePythonUnitTestFrameworkResult(logfile):
    testresult = {}
    logoutput = open(logfile.rstrip('\r\n')).read()
    for line in re.finditer(r"([\d:]+) runTest \(([^)]+)\) \.+ ok", logoutput):
        tcid = line.group(2)
        tcresult = {'result': 'pass', 'failure': ''}
        testresult[str(tcid)] = tcresult

    for line in re.finditer(r"^([\d:]+) runTest \(([^\)]+)\) \.+? FAIL[^:].+?^$", logoutput, re.MULTILINE | re.DOTALL):
        tcresult = {'result': 'fail', 'failure': ''}
        tcid = line.group(2)
        tcresult['failure'] = line.group(0)
        testresult[str(tcid)] = tcresult

    # ERROR (without the colon) are really failures
    for line in re.finditer(r"^([\d:]+) runTest \(([^\)]+)\) \.+? ERROR[^:].+?^$", logoutput,
                            re.MULTILINE | re.DOTALL):
        tcresult = {'result': 'fail', 'failure': ''}
        tcid = line.group(2)
        tcresult['failure'] = line.group(0)
        testresult[str(tcid)] = tcresult

    # ERROR: are true script errors, but pytest doesn't differentiate
    for line in re.finditer(r"^([\d:]+) runTest \(([^\)]+)\) \.+? ERROR:.+?^$.+?^$", logoutput,
                            re.MULTILINE | re.DOTALL):
        tcresult = {'result': 'fail', 'failure': ''}
        tcid = line.group(2)
        tcresult['failure'] = line.group(0)
        testresult[str(tcid)] = tcresult

    return testresult


def parsePerlFrameworkTestOutput(antoutput, testdir, component=None):  # pylint: disable=unused-argument
    if not antoutput:
        return {}
    testresult = {}
    m = re.search("LOGGING RESULTS TO (.*)", antoutput)
    if m:
        logfile = m.group(1)
        replaced_logfile = logfile.replace("/", os.sep)
        if 'cygdrive' in replaced_logfile:
            from beaver.machine import Machine
            logfileName = ''.join(replaced_logfile.split(os.sep)[-1:])
            outfiles = Machine.find(user=None, host='', filepath=testdir, searchstr=logfileName, passwd=None)
            if outfiles:
                replaced_logfile = outfiles[0]
        logger.info("Logfile: %s", replaced_logfile)
        if replaced_logfile and os.path.isfile(replaced_logfile):
            testresult = parsePerlFrameworkResult(replaced_logfile)
    return testresult


# Parse the Perl framework testresults and return a flat testcase result map
# logfile - testresults generated by Perl framework
# output will be of the following form:
# {tcid1: {result: 'pass|fail', failure: ''},...}
def parsePerlFrameworkResult(logfile, component=None):
    testresult = {}
    logoutput = open(logfile.rstrip('\r\n')).read()
    if component == "Templeton":
        matchPattern = "TestDriverCurl"
    else:
        matchPattern = "TestDriver"
    # pig on hadoop 1 has runTestGroup thus added .* to the regex
    for line in re.finditer("INFO: " + matchPattern + r"::run.*\(\) at [0-9]+:Test (.*) (SUCCEEDED)(.*)", logoutput):
        tcid = line.group(1)
        tcresult = {'testCaseStatus': 'pass', 'failure': ''}
        testresult[str(tcid)] = tcresult

    for line in re.finditer("INFO: " + matchPattern + r"::run.*\(\) at [0-9]+:Test (.*) (FAILED)(.*)", logoutput):
        tcresult = {'testCaseStatus': 'fail', 'failure': ''}
        tcid = line.group(1)
        tcresult['failure'] = line.group(0, 1, 2, 3)
        testresult[str(tcid)] = tcresult

    for line in re.finditer("Skipping test " + "(.*), (.*)", logoutput):
        tcresult = {'testCaseStatus': 'fail', 'failure': ''}
        tcid = line.group(1)
        tcresult['failure'] = line.group(0, 1, 2)
        testresult[str(tcid)] = tcresult

    for line in re.finditer("INFO: " + matchPattern + r"::run.*\(\) at [0-9]+:Test (.*) (SKIPPED)(.*)", logoutput):
        tcid = line.group(1)
        tcresult = {'testCaseStatus': 'skipped'}
        testresult[str(tcid)] = tcresult

    for line in re.finditer("ERROR " + matchPattern + "::run.* at : [0-9]+ Failed to run test ([a-z|A-Z|0-9|_]*) (.*)",
                            logoutput):
        tcresult = {'testCaseStatus': 'fail', 'failure': ''}
        tcid = line.group(1)
        tcresult['failure'] = line.group(2)
        testresult[str(tcid)] = tcresult

    for line in re.finditer(r"Annotation: (.*) (<[a-z|A-Z|0-9|_|,|\s]+>)", logoutput):
        tcid = line.group(1)
        annotation_list = line.group(2)[1:-1].split()
        testresult[str(tcid)].update(testCaseAnnotations=annotation_list)

    if component == "Templeton":
        time_regex = r"\d+"
    else:
        time_regex = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+"
    for line in re.finditer("Beginning test (.*) (at) (%s)" % time_regex, logoutput):
        tcid = line.group(1)
        if tcid == "run":
            continue
        testStartTime = line.group(3)
        if component == "Templeton":
            testStartTime = time.strftime("%Y-%m-%d %H:%M:%S,000", time.localtime(float(testStartTime)))
        testresult[str(tcid)].update(testCaseStartTime=testStartTime)

    for line in re.finditer("Ending test (.*) (at) (%s)" % time_regex, logoutput):
        tcid = line.group(1)
        if tcid == "run":
            continue
        testEndTime = line.group(3)
        if component == "Templeton":
            testEndTime = time.strftime("%Y-%m-%d %H:%M:%S,000", time.localtime(float(testEndTime)))
        testresult[str(tcid)].update(testCaseEndTime=testEndTime)

    return testresult


# Parse the KAFKA framework testresults and return a flat testcase result map
# logfile - testresults generated by kafka framework
# output will be of the following form:
# {tcid: {result: 'pass|fail', failure: ''},...}
def parseKafkaFrameworkResult(logfile):
    from beaver.config import Config
    testresult = {}
    final_status = ""  # pass,fail,skips
    with open(logfile) as f:
        lines = f.readlines()
    logger.info("here2")
    line_number = 0
    skipped_tests = []
    for line in lines:
        # get a list of all skipped tests
        skip = re.search("Skipping : (.*)", line)
        if skip:
            skipped_tests.append(skip.group(1))
        # move the line number till end of file
        if line_number < len(lines):
            myline = lines[line_number]
        else:
            break
        myline = myline.replace(' ', '').replace('\n', ' ').replace('\r', '').replace('\t', '')
        # get the tcid
        testcase_id_regex = re.search("_test_case_name:(.*)", myline)
        if testcase_id_regex:
            logger.info(testcase_id_regex.group(1))
            tcid = testcase_id_regex.group(1).strip()
        # get the test class name
        test_class_regex = re.search("_test_class_name:(.*)", myline)
        if test_class_regex:
            logger.info(test_class_regex.group(1))
            testclass = test_class_regex.group(1).strip()
        # get the validation status
        validation_status = re.search("validation_status:.*", myline)
        if validation_status:
            status = []
            while re.search("==*", lines[line_number + 1]) is None:
                logger.info(lines[line_number].strip())
                status.append(lines[line_number].strip())
                line_number = line_number + 1
            # if validation status length is more than 1 line, it is not skipped
            if status:
                for s in status:
                    # PASSED = re.search(".*PASSED.*", s)
                    FAILED = re.search("(.*)FAILED.*", s)
                    if FAILED:
                        final_status = "fail"
                        failure_message = FAILED.group()
                        logger.info(failure_message)
                        break
                    else:
                        if tcid not in skipped_tests:
                            final_status = "pass"
                            failure_message = ""
            else:
                if tcid in skipped_tests:
                    final_status = "skips"
                    failure_message = "SKIPPED"
                if tcid not in skipped_tests:
                    final_status = "fail"
                    failure_message = "ABORTED"
            testcase_name = testclass + "-" + tcid
            testresult[testcase_name] = {"result": final_status, "failure": failure_message}
        else:
            line_number = line_number + 1
    kafka_json = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "kafka_data.json")
    with open(kafka_json, 'w') as out:
        json.dump(testresult, out, indent=4)
    return testresult


# Parse the results of the hive server2 concurrency tests
# logfile - testresults generated by Java tests
# output will be of the following form:
# {tcid1: {result: 'pass|fail', failure: ''},...}
def parseHiveServer2ConcurrResults(logfile):
    logoutput = open(logfile.rstrip('\r\n')).read()
    return parseCustomJdbcClientResults(logoutput)


# Parse the results of the hive server2 concurrency tests
# logfile - testresults generated by Java tests
# output will the total number of tests executed in that suite
def parseHiveServer2ConcurrForTestCount(logfile):
    logoutput = open(logfile.rstrip('\r\n')).read()
    match = re.search("Finished [0-9]+ tests", logoutput)
    testCount = match.group().split(" ")[1]
    return testCount


# Parse the results of the custom JDBC client tests
# output - stdout from the custom client
# output will be of the following form:
# {tcid1: {result: 'pass|fail', failure: ''},...}
def parseCustomJdbcClientResults(output):
    testresult = {}
    for line in re.finditer(r"\[(.*)\] TEST (.*) in [0-9]+ seconds.", output):
        tcresult = {}
        tcid = line.group(1).split(":")[0]
        tcStatus = line.group(2)
        if tcStatus == "PASSED":
            tcresult['testCaseStatus'] = 'pass'
        else:
            tcresult['testCaseStatus'] = 'fail'
        tcresult['failure'] = ''
        tcresult['testCaseAnnotations'] = None
        testresult[str(tcid)] = tcresult
    for line in re.finditer(r"\[(.*)\] Annotations: \[(.*)\]", output):
        tcid = line.group(1).split(":")[0]
        annotation_list = line.group(2).split(',')
        testresult[str(tcid)].update(testCaseAnnotations=annotation_list)
    for line in re.finditer(r"\[(.*)\] Beginning Test at (.*)", output):
        tcid = line.group(1).split(":")[0]
        testStartTime = line.group(2)
        testresult[str(tcid)].update(testCaseStartTime=testStartTime)
    for line in re.finditer(r"\[(.*)\] Ending Test at (.*)", output):
        tcid = line.group(1).split(":")[0]
        testEndTime = line.group(2)
        testresult[str(tcid)].update(testCaseEndTime=testEndTime)
    return testresult


def parseODPiTestResults(logfile):
    logoutput = open(logfile.rstrip('\r\n')).read()
    testresult = {}
    for line in re.finditer("<a href=\"classes.*]\">(.*)</a>", logoutput):
        tcid = line.group(1)
        testresult[str(tcid)] = 'fail'
    return testresult


def getPathForBlockID(location, blockID):
    filePath = ""
    current_dir = os.path.join(location, "current")
    # Recursively search the current_dir to find the blockID
    for root, _dirnames, filenames in os.walk(current_dir):
        for filename in fnmatch.filter(filenames, blockID):
            filePath = os.path.join(root, filename)
    logger.info("location of the block is %s", filePath)

    return filePath


def getMethodInvokerCommand(workspace, classname, methodname, options=None, config=None, version=None):
    methodInvokerCommand = os.path.join(workspace, "methodInvoker.py") + " -c " + classname + " -m " + methodname
    if options != None:
        methodInvokerCommand += " -o " + options
    if config != None:
        methodInvokerCommand += " --config " + config
    if version != None:
        methodInvokerCommand += " --version " + version

    return "python %s" % methodInvokerCommand


def getIpAddress(hostname=None):
    if hostname is None:
        return socket.gethostbyname(socket.gethostname())
    else:
        return socket.gethostbyname(hostname)


def isPortOpen(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ind = s.connect_ex((host, port))
    s.close()
    return ind == 0


def getNextAvailablePort(host, baseport, interval=100, maxattempts=10):  # pylint: disable=unused-argument
    for i in range(maxattempts):
        newport = baseport + (100 * i)
        if not isPortOpen(host, newport):
            return newport
    return None


def waitForPortToOpen(host, port, interval=2, timeout=60):
    starttime = time.time()
    portOpen = isPortOpen(host, port)
    while (time.time() - starttime < timeout) and (not portOpen):
        time.sleep(interval)
        portOpen = isPortOpen(host, port)
    return portOpen


def waitForPortToClose(host, port, interval=2, timeout=60):
    starttime = time.time()
    portOpen = isPortOpen(host, port)
    while (time.time() - starttime < timeout) and portOpen:
        time.sleep(interval)
        portOpen = isPortOpen(host, port)
    return not portOpen


# method to replace strings in multiple files at once
def replaceTextInMultipleFiles(files, searchExp, replaceExp):
    for line in fileinput.input(
            files, inplace=1):  # Does a list of files, and writes redirects STDOUT to the file in question
        sys.stdout.write(re.sub(searchExp, replaceExp, line))


# method to copy contents of one file to another
def copyFileToAnotherFile(srcFile, dstFile, appendMode='a'):
    srcf = open(srcFile, 'r')
    outf = open(dstFile, appendMode)
    while True:
        data = srcf.read(65536)
        if data:
            outf.write(data)
        else:
            break
    srcf.close()
    outf.close()


# method to check if text exists in a file
def checkTextInFile(text, path):
    fh = open(path)
    for line in fh:
        if text in line:
            fh.close()
            return True
    fh.close()
    return False


# Get total bytes of hdfs from hadoop fs -ls command
# stdout - stdout from hadoop fs -ls command
# number of lines you expected except for "Found * items"
def getTotalHDFSBytes(stdout, num_lines):
    bytes_ = 0
    lines = stdout.split('\n')
    p1 = re.compile(r'Found \d items')
    for line in lines:
        if p1.match(line):
            lines.remove(line)
    p2 = re.compile('.*logs')
    for line in lines:
        if p2.match(line):
            lines.remove(line)
    if len(lines) != num_lines:
        logger.info("The expected number of output lines does not match")
        return -1
    else:
        for line in lines:
            bytes_ += int(line.split()[4])
    return bytes_


# method to compare output to file with expected result
# This method ignores "Found N items" message from output
# TODO: The vars variable should be renamed!
def compareOutputToFileIgnoreFoundNItems(  # pylint: disable=redefined-builtin
        act_out, exp_file, regex=True, vars=None, inbetween=False
):
    if not vars:
        vars = {}
    if not os.path.isfile(exp_file):
        return False
    actLines = act_out
    if not isinstance(act_out, list):
        actLines = act_out.replace("\r", "")
        actLines = actLines.split("\n")
    p = re.compile(r'Found \d items')
    for actLine in actLines:
        if p.match(actLine):
            actLines.remove(actLine)
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    for expLine in expLines:
        if p.match(expLine):
            expLines.remove(expLine)
    return compareLines(actLines, expLines, regex, vars, inbetween)


def _kerberosAuth(kerberosAuthDisabled=False, user="hrt_qa"):
    from beaver.machine import Machine
    keytabFile = Machine.getHeadlessUserKeytab(user)
    kinitCmd = '%s -R -kt %s %s' % (Machine.getKinitCmd(), keytabFile, Machine.get_user_principal(user))
    exitCode, _stdout = Machine.runas("hrt_qa", kinitCmd)
    assert exitCode == 0
    from requests_kerberos import HTTPKerberosAuth, REQUIRED, DISABLED
    if kerberosAuthDisabled:
        return HTTPKerberosAuth(mutual_authentication=DISABLED, sanitize_mutual_error_response=False)
    else:
        return HTTPKerberosAuth(mutual_authentication=REQUIRED, sanitize_mutual_error_response=False)


def checkIfUrlExists(url):
    try:
        r = requests.get(url, auth=_kerberosAuth(), verify=False)
        assert r.status_code == 200
    except Exception:
        assert False


# utility to get http response
def getHTTPResponse(resType, url, kerberosAuthDisabled=False):
    logger.info("requesting url : %s", str(url))
    ret = None
    try:
        handle_proxy()
        from beaver.component.hadoop import Hadoop
        if isXMLType(resType):
            if Hadoop.isUIKerberozied():
                ret = parseString(
                    requests.get(url, headers=xmlHeader, auth=_kerberosAuth(kerberosAuthDisabled),
                                 verify=False).content
                )
            else:
                ret = parseString(urllib2.urlopen(urllib2.Request(url, None, xmlHeader)).read())
        else:
            if Hadoop.isUIKerberozied():
                ret = json.loads(
                    requests.get(url, headers=jsonHeader, auth=_kerberosAuth(kerberosAuthDisabled),
                                 verify=False).content
                )
            else:
                ret = json.loads(urllib2.urlopen(urllib2.Request(url, None, jsonHeader)).read())
    except Exception:
        logger.info("response : ")
        logger.info(toString(resType, ret))
        ret = None
    if ret != None:
        logger.info("response : ")
        logger.info(toString(resType, ret))
    return ret


def getElement(resType, node, key, returnFirst=True):
    '''
    Get element from XML or Json format. if returnFirst equals to True and the element is a list.
    It will return the first element of the list.
    '''
    if isXMLType(resType):
        _element_ = node.getElementsByTagName(key)
        if _element_:
            if returnFirst:
                return _element_[0]
            else:
                return _element_
        else:
            return None
    else:
        try:
            return node[key]
        except Exception:
            return None


# get value of the node
def getValue(resType, node):
    if isXMLType(resType):
        return node.firstChild.nodeValue
    else:
        return node


# get attribute of the node
def getAttribute(resType, node, key):
    if isXMLType(resType):
        return node.attributes["xsi:" + key].nodeValue
    else:
        return node[key]


def isXMLType(resType):
    return resType == "XML"


def toString(resType, node):
    if isXMLType(resType):
        return node.toxml()
    else:
        return node


# Checks if timeout exceeded or not
# startTime should be time.time() at beginning
# timeout is in seconds
def isTimeout(startTime, timeout):
    return time.time() - startTime >= timeout


# Returns True if value is in range
# e.g. value = 3, range_ = "2-3", returns True
# e.g. value = "3", range_ = "<4", returns True
# e.g. value = "3", range_ = "<=3", returns True
# e.g. value = "3", range_ = ">1", returns True
# e.g. value = "3", range_ = ">=3", returns True
# range_ must be a string. Value can be int or string.
# Returns False if ValueError occurs.
def isValueInRange(value, range_):
    try:
        value = int(value)
        if re.search(r"^\d+-\d+$", range_) is not None:
            lowEnd = range_.split("-")[0]
            highEnd = range_.split("-")[1]
            lowEnd = int(lowEnd)
            highEnd = int(highEnd)
            return lowEnd <= value and value <= highEnd
        elif re.search(r"^<=\d+$", range_) is not None:
            range_ = int(range_[2:])
            return value <= range_
        elif re.search(r"^<\d+$", range_) is not None:
            range_ = int(range_[1:])
            return value < range_
        elif re.search(r"^>=\d+$", range_) is not None:
            range_ = int(range_[2:])
            return value >= range_
        elif re.search(r"^>\d+$", range_) is not None:
            range_ = int(range_[1:])
            return value > range_
    except ValueError:
        return False

    return False


def isFlubber():
    host = socket.gethostname()
    return "ygridcore.net" in host


def parseHadoopJMX(jsondata, name, keys):
    '''
  Method to parse hadoop jmx. Provide the name of the bean
  and a list of keys. Multiple keys mean that they are children, so the order is  important.
  We will retreive the value of the last key and return it.
  '''
    myNode = None
    data = jsondata
    if not data or not data['beans']:
        return None

    for node in data['beans']:
        if node['name'] == name:
            myNode = node
            for k in keys:
                myNode = myNode[str(k)]
    return myNode


def isIP(address):
    if address is None:
        return False
    return re.search(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", address) is not None


# Returns (boolean, matchObject)
def containsIP(address):
    matchObj = re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", address)
    return (matchObj is not None, matchObj)


def getShortHostname(host):
    '''
    Returns short host name.
    '''
    return host.split('.')[0]


def getShortHostnameFromIP(ip):
    if not isIP(ip):
        return None
    #Next call would return a triple (hostname, aliaslist, ipaddrlist)
    try:
        apiResult = socket.gethostbyaddr(ip)
    except socket.herror:
        apiResult = ()
        logger.info(
            "WARNING: getShortHostnameFromIP fails. IP=%s. gethostbyaddr returns socket.herror. Return the IP.", ip
        )
        return ip
    if not apiResult:
        return None
    if isIP(apiResult[0]):
        logger.info("WARNING: getShortHostnameFromIP fails. IP=%s. gethostbyaddr=%s", ip, apiResult[0])
    hostname = apiResult[0]
    return hostname.split('.')[0]


def getFullHostnameFromIP(ip):
    if not isIP(ip):
        return None
    return socket.gethostbyaddr(ip)[0]


def getFilesizeInMB(size):
    return '%s MB' % (size / (1024 * 1024))


def prune_output(lines, prune_list, prune_empty_lines=False):
    pruned_lines = []
    pruned_lines.extend(lines)
    for line in lines:
        # remove empty lines if selected
        if prune_empty_lines and (line.isspace() or len(line.strip()) < 1):
            pruned_lines.remove(line)
            continue
        # if any str in the prune list is in the line remove it
        if any(pl.lower() in line.lower() for pl in prune_list):
            pruned_lines.remove(line)
    return pruned_lines


def convertFileURIToLocalPath(uri, isWindows=True):
    '''
    Returns "c:\\hdfs\\nn" for uri="file://c:/hdfs/nn".
    If isWindows is True, conversion of / to \\ does happen.
    '''
    p = "file://.*"
    matchObj = re.match(p, uri)
    if matchObj is not None:
        uri = uri.replace("file:///", "")
        uri = uri.replace("file://", "")
        if isWindows:
            uri = uri.replace("/", "\\")
    return uri


def getXMLValue(url, key, logoutput=False):
    '''
    Get XML property value from simple non-overlapping keys such as NM WS.
    Returns node value or None. Node value should be text, though not enforced.
    '''
    handle_proxy()
    from beaver.component.hadoop import Hadoop

    if Hadoop.isUIKerberozied():
        httpOutput = requests.get(url, headers=xmlHeader, auth=_kerberosAuth(), verify=False).content
    else:
        httpOutput = urllib2.urlopen(urllib2.Request(url, None, {
            "Accept": "application/xml",
        })).read()
    if logoutput:
        logger.info("util.getXMLValue httpOutput=%s", httpOutput)
    domDoc = parseString(httpOutput)
    elem = domDoc.getElementsByTagName(key)
    if not elem:
        #key not found
        result = None
    else:
        tmp = elem[0]
        if tmp.firstChild is None:
            #key found but no contents. e.g. <healthReport/>
            result = None
        else:
            result = tmp.firstChild.nodeValue
    return result


def getDatetimeObject(
        timestamp, dateTimeFormat=DEFAULT_DATETIME_FORMAT, regexCheckFormat=DEFAULT_REGEX_DATETIME_FORMAT,
        logoutput=False
):
    '''
    Get datetime object of given String timestamp.
    Returns datetime object or None if timestamp does not start with the pattern.
    '''
    result = matchObj = None
    #check at line beginning only.
    if timestamp is None or len(timestamp) == 0:  # pylint: disable=len-as-condition
        result = None
    else:
        matchObj = re.match("(%s)" % regexCheckFormat, timestamp)
        if matchObj is None:
            result = None
        else:
            result = datetime.datetime.strptime(matchObj.group(1), dateTimeFormat)
    if logoutput:
        logger.info("getDatetimeObject %s returns %s", timestamp, result)
    return result


def isTimestampInRange(
        timestamp,
        lowerEndTimestamp,
        upperEndTimestamp,
        dateTimeFormat=DEFAULT_DATETIME_FORMAT,
        regexCheckFormat=DEFAULT_REGEX_DATETIME_FORMAT,
        logoutput=False
):
    '''
    Returns True/False whether the timestamp is in given range.
    Returns None if timestamp is in invalid format.
    lowerEndTimestamp/upperEndTimestamp can be String or datetime objects.
    timestamp can be a string or datetime object. String here can be a log line or just timestamp string.
    '''
    if isinstance(lowerEndTimestamp, str):
        lowerEndTimestampObj = getDatetimeObject(lowerEndTimestamp, dateTimeFormat, regexCheckFormat, logoutput)
    if isinstance(upperEndTimestamp, str):
        upperEndTimestampObj = getDatetimeObject(upperEndTimestamp, dateTimeFormat, regexCheckFormat, logoutput)
    if isinstance(timestamp, str):
        timestampObj = getDatetimeObject(timestamp, dateTimeFormat, regexCheckFormat, logoutput)
    else:
        timestampObj = timestamp
    result = None
    if lowerEndTimestampObj is None or upperEndTimestampObj is None or \
        timestampObj is None:
        result = None
    else:
        result = lowerEndTimestampObj <= timestampObj and timestampObj <= upperEndTimestampObj
    if logoutput:
        logger.info("isTimestampInRange %s returns %s", timestamp, result)
    return result


def writeOutputFileMatchingTimestamp(
        inFile,
        outFile,
        lowerEndTimestamp=None,
        upperEndTimestamp=None,
        appendOut=False,
        useLastSeenTimestamp=True,
        logoutput=False
):
    '''
    Reads from input file and write lines that match timestamp range to output file.
    Returns None.
    inFile is a String (path to input file).
    outFile is a String (path to output file).
    lowerEndTimestamp/upperEndTimestamp are either String or datetime objects.
    appendOut indicates whether to use append or not.
    useLastSeenTimestamp indicates whether to use last-seen timestamp for line with no timestamp.
    Resource-wise, this API should consume no memory since it reads/writes per line basis.
    '''
    inFileObj = None
    outFileObj = None
    outMode = 'w'
    if appendOut:
        outMode = 'a+'
    try:
        inFileObj = open(inFile, 'r')
        outFileObj = open(outFile, outMode)
        lastSeenTimestamp = None
        #python doc says using for-loop is memory efficient.
        for line in inFileObj:
            #get current line
            curTimestamp = getDatetimeObject(
                line,
                dateTimeFormat=DEFAULT_DATETIME_FORMAT,
                regexCheckFormat=DEFAULT_REGEX_DATETIME_FORMAT,
                logoutput=logoutput
            )
            #update lastSeenTimestamp to any most recent valid timestamp.
            if curTimestamp is not None:
                lastSeenTimestamp = curTimestamp
            writeTheLine = False
            #make decision to write the line based on timestamp or last-seen timestamp
            if not useLastSeenTimestamp:
                if isTimestampInRange(line, lowerEndTimestamp, upperEndTimestamp, logoutput=logoutput):
                    writeTheLine = True
            else:
                if isTimestampInRange(lastSeenTimestamp, lowerEndTimestamp, upperEndTimestamp, logoutput=logoutput):
                    writeTheLine = True
            #write the line
            if writeTheLine:
                if logoutput:
                    logger.info("writeOutputFileMatchingTimestamp writes %s", line)
                outFileObj.write(line)
    finally:
        if inFileObj is not None:
            inFileObj.close()
        if outFileObj is not None:
            outFileObj.close()
    return None


def findMatchingPatternInFileAfterTimestamp(
        filename, regex, timestamp, logoutput=True, returnLine=False, dateTimeFormat=None
):
    '''
    Find matching regex pattern in file after given timestamp.
    If returnLine=False, returns True if any pattern is found.
    If returnLine=True, returns a str or None if pattern is not found.
    For timestamp parameter, it has to be string and corresponds to timestamp format.
    Sample timestamp format is "2014-01-01 01:01:01".

    Requirements:
    In any line of given file having length >= 19, the line has to start with the timestamp format.

    :param: dateTimeFormat (optional) - date time format of each line amd timestamp
    '''
    #Storm worker log: 2014-01-24 19:25:32 b.s.d.task [INFO] Emitting: word __system ["startup"]
    if logoutput:
        logger.info("findMatchingPatternInFileAfterTimestamp")
        logger.info("filename=%s, regex=%s, timestamp=%s", filename, regex, timestamp)
    expectedTimestampPattern1 = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    expectedTimestampPattern2 = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
    if (re.match(expectedTimestampPattern1, timestamp) is None
            and re.match(expectedTimestampPattern2, timestamp) is None):
        return None
    if dateTimeFormat is None:
        dateTimeFormat = "%Y-%m-%d %H:%M:%S"
    givenTimestampDateTimeObj = datetime.datetime.strptime(timestamp, dateTimeFormat)

    f = None
    try:
        f = open(filename, 'r')
        patternObj = re.compile(regex)
        if returnLine:
            result = None
        else:
            result = False
        for line in f:
            if patternObj.search(line):
                if len(line) >= 19:
                    lineTimestampStr = line[0:19]
                    lineTimestampDateTimeObj = datetime.datetime.strptime(lineTimestampStr, dateTimeFormat)
                    if lineTimestampDateTimeObj >= givenTimestampDateTimeObj:
                        if returnLine:
                            result = line
                        else:
                            result = True
                        break
        if logoutput:
            logger.info("findMatchingPatternInFileAfterTimestamp returns %s", result)
        return result
    finally:
        if f is not None:
            f.close()


def get_redirect_url(url):
    try:
        handle_proxy()
        request = urllib2.Request(url)
        res = urllib2.urlopen(request)
        logger.info(res)
        finalurl = res.geturl()
    except urllib2.HTTPError, e:
        logger.info(e.args)
        logger.info("HTTP error")
        return "NoURL"
    except urllib2.URLError, e:
        logger.info(e.args)
        logger.info("URL connection error")
        return "NoURL"
    return finalurl


def render_template(template_file_name, dst_file_name, properties):
    """render a given template file using the supplied properties and write it"""
    logger.info("working on: %s", dst_file_name)
    dst_file = open(dst_file_name, "w")
    with open(template_file_name, "r") as template_file:
        template = template_file.read()
        prop_content = Template(template).substitute(properties)
        dst_file.write(prop_content)
    dst_file.close()
    logger.info("%s is created.", dst_file_name)


def getYamlValue(yamlFilepath, attributeRegex, defaultValue=None, stripQuotes=True, logoutput=True):
    '''
    Gets a value from yaml file.
    Returns a string.
    Returns None if not found.
    '''
    f = None
    result = defaultValue
    try:
        f = open(yamlFilepath, 'r')
        patternObj = re.compile('%s: (.*)' % attributeRegex, re.MULTILINE)

        for line in f:
            matchObj = patternObj.search(line)
            #if logoutput:
            #    logger.info("Storm.getYamlValue matchObj=%s", matchObj)
            if matchObj:
                result = matchObj.group(1)
                if stripQuotes:
                    result = result.strip("'")
                    result = result.strip('"')
                break
        if logoutput:
            logger.info("util.getYamlValue passed round 1 result=%s", result)
        # this is not pretty code but works.
        if result == defaultValue:
            try:
                f.close()
            finally:
                pass
            f = open(yamlFilepath, 'r')
            patternObj = re.compile('%s : (.*)' % attributeRegex, re.MULTILINE)
            for line in f:
                matchObj = patternObj.search(line)
                #if logoutput:
                #    logger.info("Storm.getYamlValue matchObj=%s", matchObj)
                if matchObj:
                    result = matchObj.group(1)
                    if stripQuotes:
                        result = result.strip("'")
                        result = result.strip('"')
                    break
        if logoutput:
            logger.info("util.getYamlValue returns %s", result)
        return result
    finally:
        if f is not None:
            f.close()


def getDir(filename, logoutput=False):  # pylint: disable=unused-argument
    '''
    Gets directory from filename with no ending slash.
    Returns a string.
    e.g. If filename = '/tmp/file1.log', returns '/tmp'
    Pre-condition: don't pass a directory name as filename.
    '''
    return os.sep.join(filename.split(os.sep)[0:-1])


def findRegexForIps(ip1host, ip2host):
    '''
    Function to find regex for two ip address
    such that if ip1 = "201.12.123.12" and ip2="201.12.122.12"
    return regex for two input ip addresses. 201.12(123.12|122.12)
    '''
    if not isIP(ip1host):
        ip1 = getIpAddress(ip1host)
    else:
        ip1 = ip1host
    if not isIP(ip2host):
        ip2 = getIpAddress(ip2host)
    else:
        ip2 = ip2host
    ip1list = ip1.split(".")
    ip2list = ip2.split(".")
    i = 0
    pattern = ""
    for ip1node in ip1list:
        ip2node = ip2list[i]
        if ip1node != ip2node:
            break
        else:
            pattern = pattern + ip1node + "."
        i = i + 1
    pattern = pattern + "("
    m1 = ".".join(ip1list[i:])
    m2 = ".".join(ip2list[i:])
    pattern = pattern + m1 + "|" + m2 + ")"
    return pattern


def get_min_or_max_time(list_of_time_strings, dateformat="%Y-%m-%d %H:%M:%S", option="min"):
    '''
    Function to get minimum or maximum time stamp. Pass option = "min" or "max" accordingly
    parameters:
       - list_of_time_strings = [
            '2014-08-08 01:47:03', '2014-08-08 01:47:09', '2014-08-08 01:47:13', '2014-08-08 01:48:04'
         ]
       - dateformat = "%Y-%m-%d %H:%M:%S"
       - option = min or max
    returns:
       - if option = min, returns minimum timestamp in epoch format
       - if option = max, returns maximum timestamp in epoch format
    '''
    try:
        if option != "min" and option != "max":
            return None
        list_of_time_in_epoch = []
        for t in list_of_time_strings:
            list_of_time_in_epoch.append(time.mktime(time.strptime(t, dateformat)))
        if option == "min":
            return min(list_of_time_in_epoch)
        else:
            return max(list_of_time_in_epoch)
    except Exception:
        return None


def get_test_name(test_name):
    """
    Get only test name without arguments
    """
    test_name = test_name.replace("pytest-", "")
    index = test_name.find("[")
    test_name = test_name[:index]
    return test_name


def get_file_md5(path):
    '''

    :param path:
        path to the file for which you want to calculate the md5
    :return:
        return the hashlib object for the file
    '''
    m = hashlib.md5(open(path, 'rb').read())
    return m


def xml2list(data):
    '''
    Takes xml ElementTree or string containing xml.
    if provided  xml is in xml then firest input is converting xml ElementTree using ElementTree.XML(${data})
    :param data: xml ElementTree structure or test string containing XML
    :return: List created out xml elements

    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
    '''
    xml_to_list = list()
    if isinstance(data, str):
        data = ET.XML(data)
    for element in data:
        if element:
            # treat like dict
            if len(element) == 1 or element[0].tag != element[1].tag:
                xml_to_list.append(xml2dict(element))
            # treat like list
            elif element[0].tag == element[1].tag:
                xml_to_list.append(xml2list(element))
        elif element.text:
            text = element.text.strip()
            if text:
                xml_to_list.append(text)
    return xml_to_list


def xml2dict(data, is_first_call=False):
    '''
    Takes xml ElementTree or string containing xml.
    if provided  xml is in xml then first input is converting xml ElementTree using ElementTree.XML(${data})
    :param data: xml ElementTree structure or test string containing XML
    :param is_first_call: Is it first time we calling this function, so that while returning
            this we can append root tag
                          return dict
    :return: Dict created out xml elements

    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
    '''
    xml_to_dict = dict()
    if isinstance(data, str):
        data = ET.XML(data)
    if data.items():
        xml_to_dict.update(dict(data.items()))
    for element in data:
        if element:
            # treat like dict - we assume that if the first two tags
            # in a series are different, then they are all different.
            if len(element) == 1 or element[0].tag != element[1].tag:
                t_xml_to_dict = xml2dict(element)
            else:
                # here, we put the list in dictionary; the key is the
                # tag name the list elements all share in common, and the value is the list itself
                t_xml_to_dict = {element[0].tag: xml2list(element)}
            # if the tag has attributes, add those to the dict
            if element.items():
                t_xml_to_dict.update(dict(element.items()))
            xml_to_dict.update({element.tag: t_xml_to_dict})
        # this assumes that if you've got an attribute in a tag, you won't be having any text.
        elif element.items():
            xml_to_dict.update({element.tag: dict(element.items())})
        # finally, if there are no child tags and no attributes, extract the text
        else:
            xml_to_dict.update({element.tag: element.text})
    if is_first_call is True:
        return {data.tag: xml_to_dict}
    return xml_to_dict


def dict2element(root, structure, doc):
    """
    Gets a dictionary like structure and converts its
    content into xml elements. After that appends
    resulted elements to root element. If root element
    is a string object creates a new elements with the
    given string and use that element as root.

    This function returns a xml element object.

    Wrote this method specifically keeping mind RM and ATS Web Service API response
    """
    assert isinstance(structure, dict), 'Structure must be a mapping object such as dict'
    # if root is a string make it a element
    if isinstance(root, str):
        root = doc.createElement(root)
    for key, value in structure.iteritems():
        if isinstance(value, list):
            for items in value:
                if isinstance(items, str):
                    el = doc.createElement(str(key))
                    el.appendChild(doc.createTextNode(str(items) if items is not None else ''))
                    root.appendChild(el)
                else:
                    el = doc.createElement(str(key))
                    dict2element(el, items, doc)
                    root.appendChild(el)
        else:
            el = doc.createElement(str(key))
            if isinstance(value, dict):
                dict2element(el, value, doc)
            else:
                el.appendChild(doc.createTextNode(str(value) if value is not None else ''))
            root.appendChild(el)
    return root


def dict2xml(structure):
    """
    Gets a dict like object as a input and returns a corresponding xml string using minidom document's toxml() method
    Restrictions:
    Structure must only have one root.
    Structure must consist of str or dict objects (other types will be converted into string)

    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
    """
    # This is main function call. which will return a document
    assert len(structure) == 1, 'Structure must have only one root element'
    assert isinstance(structure, dict), 'Structure must be a mapping object such as dict'

    root_element_name, value = next(structure.iteritems())
    impl = minidom.getDOMImplementation()
    doc = impl.createDocument(None, str(root_element_name), None)
    dict2element(doc.documentElement, value, doc)
    return doc.toxml()


def parsed_ws_response_result(http_response, use_xml=False, append_xml_root_tag_in_return=True, use_xm2list=False):
    '''
    Parses the http_response as json object or xml object
    if http_response html tag no parsing happens
    Similarly if http_response does not contain <xml at starting no xml to xml2dict conversion happens
    And original content is returned as it is
    For json parsing if returned object is None or not dict then original content is returned
    Returning content is done keep in in case of urllib2.HTTPLib error message which is neither json or xml
    :param http_response: Web Service response got from curl or urllib2 or may also contain urllib2.HTTPError
    :param use_xml: if true the xml2dict conversion of http_response happens instead of json.loads which
    :param append_xml_root_tag_in_return: whether to append xml_root tag in return dictionary in xml when xml object
                                          are being used
    :return: return dict for valid WS response or original is error/failure case
    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
    '''
    if http_response is not None and http_response.strip("\n").strip("\r").strip():
        is_html = re.findall('^<html>.*</html>.*$', http_response, re.S)
        if is_html is None:
            return http_response
        if len(is_html) > 0:  # pylint: disable=len-as-condition
            return http_response
        if use_xml:
            is_xml = re.findall(r"^\s*<\?xml .*", http_response, re.S)
            if is_xml and isinstance(is_xml, list):
                if use_xm2list is True:
                    return xml2list(http_response)
                return xml2dict(http_response, append_xml_root_tag_in_return)
        else:
            parsed_json = getJSON(http_response)
            if parsed_json is not None and isinstance(parsed_json, (dict, list)):
                return parsed_json
    return http_response


def parse_curl_output(html_out, use_xml, append_xml_root_tag_in_return=True, use_xm2list=False):
    '''
    Parse the output of curl, it is based on the assumption that curl output contains header also
    Also first line of output is HTTP code and errors, then header start, which the line Server: .*
    After which with one blank actual server response data is present
    Assumption is curl is ran with -i (include header in output) -s (silent) options without using -v (verbose) option
    :param html_out: curl command output
    :param use_xml: uses if flag to pass on parsed_result, which called for parsing actul WebService Response
    :param append_xml_root_tag_in_return: whether to append xml_root tag in return dictionary in xml when xml object
                                          are being used
    :return: HTTP response code, response headers data, response headers

    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
          This method for parsing curl is writen with an assumption that curl is used with
          -i (include headers in query response), -s (silent) and response starts with <? or {
    '''
    ret_code = -1
    headers = {}
    response = ""
    assume_end_of_header_found = False
    for line in html_out.split("\n"):
        if re.match(r"HTTP/\d+.?\d*\s*(\d+).*$", line) is not None:
            ret_code = re.findall(r"HTTP/\d+.?\d*\s*(\d+).*$", line)[0]
            ret_code = int(ret_code)
            assume_end_of_header_found = False
        else:
            t_line = line.strip("\n").strip("\r").strip()
            if len(t_line) <= 0:
                continue
            if t_line.startswith("{") or t_line.startswith("<?") or t_line.startswith('['):
                assume_end_of_header_found = True
                response = response + t_line
                continue
            if not re.findall(r"(.*):\s*(.*)", t_line):
                assume_end_of_header_found = True
                response = response + t_line
                continue
            if not assume_end_of_header_found:
                header_key, header_val = re.findall(r"(.*):\s*(.*)", t_line)[0]
                if 'Set-Cookie:' in t_line:
                    obj = re.search(r"^\s*(Set-Cookie)\s*:\s*(.*)", t_line)
                    if obj is not None:
                        header_key = obj.group(1)
                        header_val = obj.group(2)
                if header_key is not None:
                    header_key = header_key.strip()
                    if header_val is not None:
                        header_val = header_val.strip()
                    headers[header_key] = header_val
            else:
                response = response + t_line
    logger.debug("Parsed Curl Query response %s %s %s", str(ret_code), str(response), str(headers))
    return ret_code, parsed_ws_response_result(response, use_xml, append_xml_root_tag_in_return, use_xm2list), headers


def change_kerberos_ticket_owner(user, kerberos_ticket_path):
    if kerberos_ticket_path is None:
        return kerberos_ticket_path
    if kerberos_ticket_path == "" or not kerberos_ticket_path:
        return kerberos_ticket_path
    from beaver.machine import Machine
    new_ticket_path = os.path.join(os.path.dirname(kerberos_ticket_path), user + '.as_owner.headless.ticket')
    if os.path.exists(new_ticket_path):
        return new_ticket_path
    copy_cmd = "cp -f -p %s %s" % (kerberos_ticket_path, new_ticket_path)
    Machine.runas(user=Machine.getAdminUser(), cmd=copy_cmd, host='', passwd=Machine.getAdminPasswd())
    chmod_cmd = 'chown %s %s' % (user, new_ticket_path)
    Machine.runas(user=Machine.getAdminUser(), cmd=chmod_cmd, host='', passwd=Machine.getAdminPasswd())
    Machine.chmod(
        perm="755", filepath=new_ticket_path, user=Machine.getAdminUser(), host=None, passwd=Machine.getAdminPasswd()
    )
    return new_ticket_path


def httpsTlsRequest(url, headers=None, data='', method='GET', retry=1, kerberosAuthDisabled=True):
    if not headers:
        headers = {}
    retcode = -1
    retdata = None
    retheaders = {}
    if data == '':
        data = None
    for _ in range(retry):
        try:

            response = requests.request(
                url=url,
                method=method,
                headers=headers,
                params=data,
                data=data,
                auth=_kerberosAuth(kerberosAuthDisabled=kerberosAuthDisabled),
                verify=False
            )

            response.raise_for_status()
            retcode = response.status_code
            retdata = response.content
            retheaders = response.headers
            break
        except requests.exceptions.RequestException, e:
            if isinstance(e, requests.exceptions.HTTPError):
                retcode = e.response.status_code
                retdata = e.response.content
                retheaders = e.response.headers
                break
            else:
                if retry > 1:
                    logger.info("Sleep 10 secs due to URL error before retry")
                    time.sleep(10)
    return retcode, retdata, retheaders


def query_yarn_web_service(
        ws_url,
        user,
        query_headers=None,
        data=None,
        use_xml=False,
        http_method='GET',
        also_check_modified_config_for_spnego=False,
        do_not_use_curl_in_secure_mode=False,
        use_user_auth_in_un_secure_mode=True,
        user_delegation_token=None,
        renew_cancel_delegation_token_use_curl=False,
        cookie_string=None,
        use_xm2list=False,
        max_time=0
):
    '''
    Queries Yarn RM WebService . Uses curl is Secure Hadoop (when usr_delegation_token is None) or Wire Encryption on

    :param ws_url: path component query
    :param user: used with curl to switch user to run command, other to pass username in query params in un-secure mode
    :param query_headers: Extra headers we pass on to query request
    :param data: data we pass in query for method such as PUT/POST
    :param use_xml: if true the xml2dict conversion of http_response happens instead of json.loads which
    :param http_method: HTTP request method to use, GET (dfault), other are POST/PUT/DELETE
    :param do_not_use_curl_in_secure_mode: Flag use normal urllib2 instead curl  in secure mode when (ST is not used)
    :param use_user_auth_in_un_secure_mode: Flag tells append user.name=user in query request params in un-secure
    :param renew_cancel_delegation_token_use_curl: If true use curl --negotiate -u : for renew/cancel delegation token
    :return: HTTP response code, data , headers

    NOTE: Wrote this method specifically keeping mind RM and ATS Web Service API response
          Token handling is going to change.
          TODO: Change token handling logic once new token handling JIRAs gets fixed in Hadoop
    '''
    from beaver.config import Config
    from beaver.machine import Machine
    from beaver.component.hadoop import Hadoop, YARN
    logger.info("query_yarn_web_service start Accessing %s", ws_url)
    append_xml_root_tag_in_return = True
    yarn_new_api = [
        '/ws/v1/cluster/apps/new-application', '/ws/v1/cluster/delegation-token',
        r'http.*/ws/v1/cluster/apps/application_\w+_\w+/state.*'
    ]
    hadoop_http_filters = False
    if Hadoop.isSecure():
        if also_check_modified_config_for_spnego is True:
            hadoop_http_filters = Hadoop.getmodifiedConfigValue('core-site.xml', 'hadoop.http.filter.initializers')
    use_spnego = hadoop_http_filters and 'AuthenticationFilterInitializer' in str(hadoop_http_filters)
    is_cookie_present = cookie_string is not None and len(cookie_string) > 0
    if '/ws/v1/timeline' in ws_url or '/ws/v1/applicationhistory' in ws_url:
        use_spnego = True
    if is_cookie_present is True:
        use_spnego = False
    is_web_app_secure = Hadoop.isSecure() and use_spnego
    if Hadoop.isUIKerberozied():
        is_web_app_secure = True
    logger.info("is_web_app_secure = %s", is_web_app_secure)
    env = dict()
    cmd = None
    ws_header = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    if use_xml is True:
        ws_header = {'Accept': 'application/xml', 'Content-Type': 'application/xml'}
        if yarn_new_api[0] in ws_url or yarn_new_api[1] in ws_url or re.match(yarn_new_api[2], ws_url) is not None:
            append_xml_root_tag_in_return = False

    if is_cookie_present is True:
        ws_header['Cookie'] = cookie_string
    if data is not None:
        if use_xml:
            if re.match(yarn_new_api[2], ws_url) is not None:
                data = dict2xml({'appstate': data})
            elif yarn_new_api[1] in ws_url:
                data = dict2xml({'delegation-token': data})
            elif '/ws/v1/cluster/apps' in ws_url:
                data = dict2xml({'application-submission-context': data})
            else:
                data = dict2xml(data)
        else:
            data = json.dumps(data)

    if query_headers is not None:
        for k in query_headers.keys():
            ws_header[k] = query_headers[k]

    if user_delegation_token is not None:
        logger.info("user_delegation_token = %s", user_delegation_token)
        if '/ws/v1/cluster' in ws_url:
            if '/ws/v1/cluster/delegation-token' in ws_url:
                ws_header['Hadoop-YARN-RM-Delegation-Token'] = user_delegation_token
            else:
                ws_header['Hadoop-YARN-Auth-Delegation-Token'] = user_delegation_token
        else:
            token_string_prop = 'delegation'
            if renew_cancel_delegation_token_use_curl:
                token_string_prop = 'token'
            if '?' in ws_url:
                ws_url = ws_url + '&' + token_string_prop + '=' + user_delegation_token
            else:
                ws_url = ws_url + '?' + token_string_prop + '=' + user_delegation_token

    if is_web_app_secure and not do_not_use_curl_in_secure_mode:
        logger.info("is_web_app_secure and not do_not_use_curl_in_secure_mode")
        set_kerb_tckt = user_delegation_token is None
        if not set_kerb_tckt:
            set_kerb_tckt = user_delegation_token is not None and renew_cancel_delegation_token_use_curl is True
        if set_kerb_tckt:
            kerb_ticket = Machine.getKerberosTicket(user)
            if Config.getEnv('USER') != user:
                kerb_ticket = change_kerberos_ticket_owner(user, kerb_ticket)
            env['KRB5CCNAME'] = kerb_ticket
            cmd = " --negotiate -u : '" + ws_url + "'"

    if do_not_use_curl_in_secure_mode:
        logger.info("do_not_use_curl_in_secure_mode")
        cmd = None
    if not is_web_app_secure:
        if use_user_auth_in_un_secure_mode and not is_cookie_present:
            logger.info("use_user_auth_in_un_secure_mode and not is_cookie_present")
            if '?' in ws_url:
                ws_url = ws_url + '&user.name=' + user
            else:
                ws_url = ws_url + '?user.name=' + user
    logger.info("query_yarn_web_service new ws_url Accessing %s", ws_url)
    if cmd is not None:
        cmd_headers = ""
        data_cmd_part = ""
        ssl_tls_opt = ''
        if YARN.isHttpsEnabled():
            ssl_tls_opt = '-1'
        if data is not None:
            if Config.getEnv('USER') != user:
                data = data.replace('"', '\"')
            data_cmd_part = " --data '" + data + "'"
        if http_method is not None and http_method != 'GET':
            data_cmd_part = " -X " + http_method + data_cmd_part
        for k in ws_header:
            cmd_headers = cmd_headers + " -H '" + k + ": " + ws_header[k] + "' "
        curl_max_time = None
        if max_time > 0:
            curl_max_time = ' --max-time %d' % max_time
        curl_cmd = "curl -i -k -s %s %s %s %s" % (ssl_tls_opt, cmd_headers, data_cmd_part, cmd)
        if curl_max_time:
            curl_cmd += curl_max_time
        logger.info("curl_cmd = %s", curl_cmd)
        exit_code, html_source = Machine.runas(user, curl_cmd, env=env, logoutput=False, host=None)
        if exit_code != 0:
            logger.error("curl command returned with exit code %s and output is %s", str(exit_code), str(html_source))
            return exit_code, None, None
        return parse_curl_output(html_source, use_xml, append_xml_root_tag_in_return, use_xm2list)

    if data is not None:
        logger.info("Using Data: %s", str(data))
    if YARN.isHttpsEnabled():
        ret_code, ret_data, ret_headers = httpsTlsRequest(ws_url, ws_header, data=data, method=http_method, retry=1)
        logger.info("Url Query response %s %s %s %s", ws_url, str(ret_code), str(ret_data), str(ret_headers))
        return (
            ret_code, parsed_ws_response_result(ret_data, use_xml, append_xml_root_tag_in_return, use_xm2list),
            ret_headers
        )

    logger.info("ws_url: %s", ws_url)
    logger.info("header value is : ")
    logger.info(ws_header)
    ret_code, ret_data, ret_headers = httpRequest(ws_url, ws_header, data=data, method=http_method, retry=1)
    logger.info("Url Query response %s %s %s %s", ws_url, str(ret_code), str(ret_data), str(ret_headers))
    return (
        ret_code, parsed_ws_response_result(ret_data, use_xml, append_xml_root_tag_in_return, use_xm2list), ret_headers
    )


def is_service_running(service_address):
    '''
    Checks whether service is running by checking whether service is listening of its specified address and
    port=${service_address}
    :param service_address: hostname/ip and port combination colon separated string
    :return True if service is running, False otherwise
    '''
    if service_address is None or len(service_address.strip()) < 1 or ':' not in service_address:
        return False
    address = service_address.split(':')
    try:
        address[0] = socket.gethostbyname(address[0])
        address[1] = int(address[1])
        return isPortOpen(address[0], address[1])

    except Exception, e:
        logger.error("Could able not connect %s server returned error %s", str(address), str(e))
    return False


def is_service_got_started_or_stopped(service_address, started_or_stopped, wait=30, interval=1):
    '''
    When service is started or stopped, checks whether service is came up or stopped
    :param service_address: hostname/ip and port combination colon separated string
    :param started_or_stopped: If True, it will wait server to start listening to it specified port after starting it
                                else, it will wait RM server to stop listening to http webapp port after being stopped
    :param wait: Maximum wait time
    :param interval: sleep interval between subsequent polls
    :return True (if server got started/stopped), False otherwise
    '''
    if service_address is None or len(service_address.strip()) < 1 or ':' not in service_address:
        return False
    if started_or_stopped is None or not isinstance(started_or_stopped, bool):
        return False
    count = 0
    while count < wait:
        status = is_service_running(service_address)
        if started_or_stopped and status:
            time.sleep(5)
            return True
        if not started_or_stopped and not status:
            time.sleep(5)
            return True
        count += interval
        time.sleep(interval)
    return False


def copy_back_to_original_config(src, dest, file_list=None, node_list=None):
    '''
    Copy over configs from Temp location (tmp/hadoopConf) to  default conf location (/etc/hadoop/conf)
    Required by RU
    '''
    if not file_list:
        file_list = ['all']
    if not node_list:
        node_list = [None]
    from beaver.machine import Machine
    if file_list == ["all"]:
        ## copy over complete dir
        for node in node_list:
            if (Machine.pathExists(Machine.getAdminUser(), node, src, passwd=Machine.getAdminPasswd())
                    and Machine.pathExists(Machine.getAdminUser(), node, dest, passwd=Machine.getAdminPasswd())):
                command = "cp -r " + src + "  " + dest
                Machine.runas(Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd())
            else:
                logger.info("src = %s or dest = %s location missing from %s", src, dest, node)
    else:
        for node in node_list:
            logger.info("Handling %s", node)
            for file_ in file_list:
                src_file = os.path.join(src, file_)
                dest_file = os.path.join(dest, file_)
                if (Machine.pathExists(Machine.getAdminUser(), node, src, passwd=Machine.getAdminPasswd())
                        and Machine.pathExists(Machine.getAdminUser(), node, dest, passwd=Machine.getAdminPasswd())):
                    logger.info('copying %s to %s', src_file, dest_file)
                    command = "cp " + src_file + " " + dest_file
                    Machine.runas(Machine.getAdminUser(), command, node, None, None, "True", Machine.getAdminPasswd())
                else:
                    logger.info("src = %s or dest = %s location missing from %s", src, dest, node)


def curlas(user, url, method='GET', params=None, logoutput=False, header=None, jsonfile=None, skipuser=False):
    '''
    Use curl to facilitate HTTP requests, advantage is it works both in secure and unsecure environment.
    Primarily used for WebHCat.
    '''
    if not params:
        params = {}
    from beaver.config import Config
    from beaver.machine import Machine
    from beaver.component.ambari import Ambari
    headerfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d' % int(999999 * random.random()))
    if header:
        cmd = 'curl --silent -k -X %s -H "%s"' % (method, header)
    else:
        cmd = "curl --silent -k -X %s " % method
    cmd += " -D %s " % headerfile
    if user is None:
        user = Config.getEnv('USER')
    env = {}
    if Ambari.is_cluster_secure():
        kerbTicket = Machine.getKerberosTicket(user, kerbTicketFile="%s_curl.kerberos.ticket" % user)
        env['KRB5CCNAME'] = kerbTicket
        user = None
        cmd += " --negotiate -u :"
    else:
        if not skipuser:
            cmd += " -d user.name=%s" % user
    for key, value in params.items():
        cmd += " -d \"%s=%s\"" % (key, value)
    if jsonfile:
        cmd += " -d @%s" % jsonfile
    cmd += " " + url
    _exit_code, stdout = Machine.run(cmd, env=env, logoutput=False)
    headercontent = open(headerfile).read()
    os.remove(headerfile)
    statusCode = re.findall(r"HTTP.* (\d+)", headercontent)
    if statusCode:
        statusCode = int(statusCode[-1])
    else:
        statusCode = -1
    if logoutput:
        logger.info("util.curlas (statusCode, stdout) = (%s, %s)", statusCode, stdout)
    return statusCode, stdout


def curl(url, method='GET', params=None, logoutput=False):
    if not params:
        params = {}
    return curlas(None, url, method=method, params=params, logoutput=logoutput)


def curlDirect(url, args, logoutput=True):
    """
    Direct curl call.

    Use it at your own risk because it might introduce cyclic dependency on Machine API.
    :param url:
    :param args:
    :param logoutput:
    :return:
    """
    from beaver.machine import Machine
    cmd = 'curl %s "%s"' % (args, url)
    return Machine.run(cmd, logoutput=logoutput)


def getReposFile(useHDPBaseRepoFile=False, isRUcluster=False):
    '''
  Get the repos file for the current platform. Returns None if not found.
  '''
    from beaver.machine import Machine
    from beaver.component.hadoop import Hadoop
    repos_path = ""
    #    '''
    #  Create regex in a way that for all platoforms the 1st group is the URL
    #  e.g. for centos
    #  baseurl=http://s3.amazonaws.com/dev.hortonworks.com/HDP/centos6/2.x/BUILDS/2.2.0.0-1037
    #  e.g. for debian
    #  deb http://s3.amazonaws.com/dev.hortonworks.com/HDP/debian6/2.x/BUILDS/2.2.0.0-1230 HDP main
    #  '''
    repos_regex_pattern = ""
    if useHDPBaseRepoFile:
        if Machine.isSuse():
            repos_path = "/etc/zypp/repos.d/hdpbase*.repo"
            repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
        elif Machine.isUbuntu() or Machine.isDebian():
            repos_path = "/etc/apt/sources.list.d/hdpbase*.list"
            repos_regex_pattern = r"deb (.*\/BUILDS\/[^ ]*) .*"
        elif Machine.isCentOs5() or Machine.isCentOs6() or Machine.isRedHat() or Machine.isAmazonLinux():
            repos_path = "/etc/yum.repos.d/hdpbase*.repo"
            repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
        else:
            # ''' default it to /etc/yum.repos.d/hdp*.repo '''
            repos_path = "/etc/yum.repos.d/hdpbase*.repo"
            repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
    else:
        if Hadoop.isAmbari():
            if Machine.isSuse():
                repos_path = "/etc/zypp/repos.d/HDP.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
            elif Machine.isUbuntu() or Machine.isDebian():
                repos_path = "/etc/apt/sources.list.d/HDP.list"
                repos_regex_pattern = r"deb (.*\/BUILDS\/[^ ]*) .*"
            elif Machine.isCentOs5() or Machine.isCentOs6() or Machine.isRedHat() or Machine.isAmazonLinux():
                repos_path = "/etc/yum.repos.d/HDP.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
            else:
                # ''' default it to /etc/yum.repos.d/HDP.repo '''
                repos_path = "/etc/yum.repos.d/HDP*.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
        else:
            if Machine.isSuse():
                repos_path = "/etc/zypp/repos.d/hdp*.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
            elif Machine.isUbuntu() or Machine.isDebian():
                repos_path = "/etc/apt/sources.list.d/hdp*.list"
                repos_regex_pattern = r"deb (.*\/BUILDS\/[^ ]*) .*"
            elif Machine.isCentOs5() or Machine.isCentOs6() or Machine.isRedHat() or Machine.isAmazonLinux():
                repos_path = "/etc/yum.repos.d/hdp*.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
            else:
                # ''' default it to /etc/yum.repos.d/hdp*.repo '''
                repos_path = "/etc/yum.repos.d/hdp*.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"
            if Machine.isSuse() and Hadoop.isAmbari():
                repos_path = "/etc/zypp/repos.d/hdp*.repo"
                repos_regex_pattern = r"baseurl=(.*\/BUILDS\/.*)"

    if isRUcluster:
        repos_regex_pattern = repos_regex_pattern.replace("BUILDS", "HDP")
        repos_path = findCorrectVersionHdprepo(repos_path)
    repos_files = glob.glob(repos_path)
    if repos_files:
        # ''' return the first match for repo file '''
        logger.info("Found the following repos file (using the first) - %s", repos_files)
        return (repos_files[0], repos_regex_pattern.strip())
    else:
        logger.error("Oops repos file not found!!")
        return (None, None)


def findCorrectVersionHdprepo(repos_path):
    '''
  In side by side install there can be 2 hdp.repo files
  1. HDP.repo
  2. HDP-<version>.repo
  If current version = 2.3.0.0-1111 and HDP-2.3.0.0.repo file is present, it returns HDP-2.3.0.0.repo
  If HDP-2.3.0.0.repo is not present, it return HDP.repo
  '''
    from beaver.component.slider import Slider
    slider_version = Slider.getShortVersion().split("-")[0]
    logger.info("slider-version = %s", slider_version)
    dir_ = repos_path.rsplit("/", 1)[0]
    from beaver.machine import Machine
    path = Machine.find(
        Machine.getAdminUser(),
        None,
        dir_,
        "*" + slider_version.strip() + "*",
        Machine.getAdminPasswd(),
        logoutput=True
    )
    if not path:
        return repos_path
    else:
        return path[0]


def getHDPBuildsBaseUrlFromRepos(useHDPBaseRepoFile=False, isRUcluster=False):
    '''
  Returns the HDP BUILDS base url string retrieved from repo file,
  e.g. /etc/yum.repos.d/hdpqe.repo
  Returns None if not found
  '''
    repos_file, repos_regex_pattern = getReposFile(useHDPBaseRepoFile, isRUcluster)
    logger.info("getHDPBuildsBaseUrlFromRepos repos_file, repos_regex_pattern=%s,%s", repos_file, repos_regex_pattern)
    if (repos_file is None) or (not os.path.isfile(repos_file)):
        logger.error("Repos file does not exist - %s", repos_file)
        return None

    base_url = None
    #    '''
    #  Look for the line which matches the regex pattern.
    #  As per QE there will be 1 and only 1 line which will match this criteria.
    #  '''
    hdp_builds_url = re.compile(repos_regex_pattern)
    f = None
    try:
        f = open(repos_file, "r")
        for line in f:
            found = hdp_builds_url.match(line)
            if found:
                logger.info("Matched line from repos file - %s", line)
                base_url = found.group(1)
                break
    except Exception:
        # ''' catch, log and return None '''
        logger.error("could not open repos file - %s", repos_file)
    finally:
        if f:
            f.close()
    logger.info("getHDPBuildsBaseUrlFromRepos returns %s", base_url)
    return base_url


def getJMXData(url, name, metric, defaultValue=None, tries=5, wait_time=15):
    count = 0
    my_data = 0
    while (my_data is None or my_data == 0 or my_data == '') and count < tries:
        JMX_DATA = getJSONContent(url)
        for data in JMX_DATA['beans']:
            # check for None
            if data is not None and data['name'] is not None and data['name'].startswith(name):
                my_data = data[str(metric)]
                break
        count += 1
        time.sleep(wait_time)

    if my_data is None or my_data == 0 or my_data == '':
        return defaultValue
    else:
        return my_data


def parseUpgradeProgressFile(progress_file):
    f = open(progress_file)
    content = f.read()
    import collections
    results = collections.defaultdict(dict)
    passed_pattern = r"(.*\[PASSED\]\[([\w]+)\](.*))"
    failed_pattern = r"(.*\[FAILED\]\[([\w]+)\](.*))"
    passed_tests = re.findall(passed_pattern, content)
    failed_tests = re.findall(failed_pattern, content)
    for test in passed_tests:
        component = test[1]
        detail = test[2]
        results[component]["result"] = "PASSED"
        results[component]["failure"] = ""
    for test in failed_tests:
        component = test[1]
        detail = test[2]
        if component in results.keys():
            results[component]["result"] = "FAILED"
            results[component]["failure"] = results[component]["failure"] + " , " + detail
        else:
            results[component]["result"] = "FAILED"
            results[component]["failure"] = detail
    return results


def convertWindowsDollarPathToCygdrivePath(s, logoutput=True):
    """
    Convert //maint22-yarn24/D$/hadoop/logs/hadoop/* to /cygdrive/d/hadoop/logs/hadoop
    """
    if logoutput:
        logger.info("convertWindowsDollarPathToCygdrivePath gets %s", s)
    if s.startswith("//"):
        s = s[2:]
        items = s.split("/")
        print(items)  # pylint: disable=superfluous-parens
        items = items[1:]
        items[0] = items[0][0].lower()
        s = "/cygdrive/" + "/".join(items)
    if logoutput:
        logger.info("convertWindowsDollarPathToCygdrivePath returns %s", s)
    return s


def getAllInternalNodes(logoutput=False):
    """
    Read /tmp/all_internal_nodes
    :param logoutput:
    :return: [] or list of str
    """
    all_internal_nodes = "/tmp/all_internal_nodes"
    f = None
    r = []
    try:
        f = open(all_internal_nodes)
        for line in f:
            r.append(line.strip('\n'))
    finally:
        if f is not None:
            f.close()
    if logoutput:
        logger.info("getAllInternalNodes return %s", r)
    return r


def getAllNodes(logoutput=False):
    """
    Read /tmp/all_nodes
    :param logoutput:
    :return: [] or list of str
    """
    all_internal_nodes = "/tmp/all_nodes"
    f = None
    r = []
    try:
        f = open(all_internal_nodes)
        for line in f:
            r.append(line.strip('\n').strip())
    finally:
        if f is not None:
            f.close()
    if logoutput:
        logger.info("getAllNodes return %s", r)
    return r


def createTestngXmlWithClasses(referenceXMLFilePath, newXMLFilePath, newClassList):
    try:
        doc = minidom.parse(referenceXMLFilePath)
        testList = doc.getElementsByTagName("test")
        for test in testList:
            classes = test.getElementsByTagName("classes")[0]
            for class1 in classes.getElementsByTagName("class"):
                classes.removeChild(class1)
            for class1 in newClassList:
                newClassElement = minidom.Document().createElement("class")
                newClassElement.setAttribute("name", class1)
                classes.appendChild(newClassElement)
        newXMLFileHandler = open(newXMLFilePath, "w")
        newXMLFileHandler.write(doc.toxml())
        newXMLFileHandler.close()
        return True
    except Exception, e:
        logger.exception(e)
        return False


def createIncludeFilewithIp(inputfile, outputfile):
    f = open(inputfile)
    fo = open(outputfile, "wb")
    t = f.read()
    created = False
    for s in t.split("\n"):
        if s.strip().find("humb") >= 0 or s.strip().find("internal.cloudapp.net") >= 0:
            host = socket.gethostbyname(s.strip())
            fo.write(host)
            fo.write("\n")
            created = True
    fo.close()
    f.close()
    return created


def sleep(num_secs):
    logger.info("sleep for %s sec", num_secs)
    time.sleep(num_secs)


def getPomPathToAnnotation(startFolder, neededFilename):
    annotation_path_prefix = "src.test."
    for root, _dirs, files in os.walk(startFolder):
        for name in files:
            fullname = os.path.join(root, name)
            # Check for matching marker Java class name
            if name.lower().startswith(neededFilename.lower() + ".java"):
                logger.info("Found [%s]", neededFilename)
                pathToFoundClass = fullname.replace(startFolder, "").replace("/",
                                                                             ".").replace(annotation_path_prefix,
                                                                                          "").replace(".java", "")
                if pathToFoundClass.startswith("."):
                    pathToFoundClass = pathToFoundClass[1:]
                logger.info("Found path for pom.xml groups config [%s]", pathToFoundClass)
                return pathToFoundClass
    return None


def getPomPathesToAnnotations(startFolder, neededFilenames):
    pathesToFoundClass = []
    comma_separated_path = ''
    try:
        for neededFilename in neededFilenames:
            pomPathToAnnotation = getPomPathToAnnotation(startFolder, neededFilename)
            if pomPathToAnnotation:
                pathesToFoundClass.append(pomPathToAnnotation)
        if not pathesToFoundClass:
            return ''
        comma_separated_path = ",".join(pathesToFoundClass)
    except Exception:
        logger.error("exception occured during getPomPathesToAnnotations")
        logger.error(traceback.format_exc())
    return comma_separated_path


def sha1_file(file_name, verbose=False):
    """
    Compute sha1sum of given file.
    :rtype: str
    """
    hasher = hashlib.sha1()
    block_size = 8 * 1024
    with open(file_name, 'rb') as fileDesc:
        block = fileDesc.read(block_size)
        while block:
            hasher.update(block)
            block = fileDesc.read(block_size)
    sha1sum = hasher.hexdigest()
    if verbose:
        logger.info("SHA1 of %s=%s", file_name, sha1sum)
    return sha1sum


def is_ci_run():
    """ Return true if this run is a CI run. CI run is started by passing -m CI option."""
    from conftest import MARK_EXPR
    if not MARK_EXPR:
        return False
    return "CI" in MARK_EXPR


def is_sanity_run():
    from conftest import MARK_EXPR
    if not MARK_EXPR:
        return False
    return "sanity" in MARK_EXPR.lower()


def is_marker_enabled(marker_name):
    from conftest import MARK_EXPR
    if not MARK_EXPR or not marker_name:
        return True
    from collections import defaultdict
    env = defaultdict(lambda: False)
    env[marker_name] = True
    res = eval(MARK_EXPR, {}, env)  # pylint: disable=eval-used
    logger.info("%s enabled =  %s", marker_name, res)
    return res


def get_marker():
    from conftest import MARK_EXPR
    return MARK_EXPR


def get_TESTSUITE_COMPONENT():
    '''
    Get Testsuite component
    :return: Testsuite component name or None
    '''
    from beaver.config import Config
    report_conf = os.path.join(Config.getEnv('WORKSPACE'), 'conf', 'report.conf')
    component = None
    if os.path.isfile(report_conf):
        logging.info("Going to Parse file %s", str(report_conf))
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        config.read(report_conf)
        section = "HW-QE-PUBLISH-REPORT"
        if config.has_section(section):
            component = config.get(section, "COMPONENT")
            logging.info("File %s contains %s with value %s", report_conf, section, component)
        return component
    else:
        return None


def get_SPLIT_ID():
    '''
    Get split id
    :return:
    '''
    split_id = get_value_from_report_conf("split_id")
    return split_id


def get_value_from_report_conf(param):
    """
    Find values for a variable from report conf
    :return:
    """
    from beaver.config import Config
    report_conf = os.path.join(Config.getEnv('WORKSPACE'), 'conf', 'report.conf')
    if os.path.isfile(report_conf):
        logging.info("Going to Parse file %s", str(report_conf))
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        config.read(report_conf)
        section = "HW-QE-PUBLISH-REPORT"
        if config.has_section(section):
            value = config.get(section, param)
            logging.info("File %s contains %s with value %s", report_conf, section, value)
        return value
    else:
        return None


def get_Dashboard_Host():
    """
    Get dashboard host
    :return:
    """
    config = ConfigParser.ConfigParser()
    suiteconf = os.path.join('conf', 'suite.conf')
    config.read(suiteconf)
    try:
        dashboard_host = str(config.get(section='dashboard', option='host'))
    except NoOptionError:
        dashboard_host = "dashboard.qe.hortonworks.com"
    return dashboard_host


def get_Dashboard_Port():
    """
    Get dahsboard port
    :return:
    """
    config = ConfigParser.ConfigParser()
    suiteconf = os.path.join('conf', 'suite.conf')
    config.read(suiteconf)
    try:
        dashboard_port = str(config.get(section='dashboard', option='port'))
    except NoOptionError:
        dashboard_port = "5000"
    return dashboard_port


def get_Dashboard_basepath():
    """
    Get dashboard base path
    :return:
    """
    config = ConfigParser.ConfigParser()
    suiteconf = os.path.join('conf', 'suite.conf')
    config.read(suiteconf)
    try:
        dashboard_route = str(config.get(section='dashboard', option='route'))
        dashboard_version = str(config.get(section='dashboard', option='version'))
        return "/%s/%s" % (dashboard_route, dashboard_version)
    except NoOptionError:
        return "/hwqe-dashboard-api/v1"


def get_qe_group_email():
    """
    Get qe group email id
    :return:
    """
    return "qe-group@hortonworks.com"


def getRandomAlphaNumeric(length=8):
    return "".join(random.sample(string.lowercase + string.digits, length))


def processPIPEOutput(myprocess, queue, cmd, artifactsDir):
    '''
    Process the PIPE to put client logs from PIPE to file and to console.
    Used to obtain AppId for applications running in background
    :param myprocess: PIPE
    :param queue: Used to return AppID
    :param cmd: command for which client logs are generated
    :param artifactsDir: Artifacts dir
    :return:
    '''
    nextline = None
    buf = ''
    threadId = current_thread().name
    localLogFile = os.path.join(artifactsDir, threadId)
    f = open(localLogFile, 'a')
    f.write("RUNNING - %s\n" % cmd)
    logger.info('"%s" logs are collected in %s', cmd, localLogFile)
    while True:
        #--- extract line using read(1)
        out = myprocess.stderr.read(1)
        if out == '' and myprocess.poll() is not None:
            break
        if out != '':
            buf += out
            if out == '\n':
                nextline = buf
                buf = ''
        if not nextline:
            continue
        line = nextline
        nextline = None
        f.write(line)
        logger.info(line)
        #logger.info("%s : %s"%(str(current_thread()),str(line)))
        searchForAppId = re.match(".*Submitted application (.*)", line)
        if searchForAppId is not None:
            applicationId = searchForAppId.group(1)
            queue.put(applicationId)
    if queue.empty():
        queue.put("None")
    myprocess.stderr.close()
    f.close()


def get_current_date(delta_days=0):
    """
    Get current date in the format YYYYMMDD with an optional offset delta_days
    """
    now = datetime.datetime.now()
    now_plus_days = now + datetime.timedelta(days=delta_days)
    year = now_plus_days.strftime("%Y")
    month = now_plus_days.strftime("%m")
    day = now_plus_days.strftime("%d")
    return '%s%s%s' % (year, month, day)


def ats_v2_url_encode(**kwargs):
    """
    Encode specified key-value pairs in the format, key1=value1&key2=value2=...
    """
    encoded_url = ''
    if kwargs is None:
        return None
    for param, value in kwargs.iteritems():
        if value:
            encoded_value = quote(str(value), safe='!,()')
            if not encoded_url:
                encoded_url = '%s=%s' % (param, encoded_value)
            else:
                key_val_pair = '&%s=%s' % (param, encoded_value)
                encoded_url += key_val_pair
    return '?' + encoded_url


def getKnoxHostFromGatewayIP(gatewayHost):
    from beaver.component.ambari import Ambari

    hosts = None
    if gatewayHost:
        weburl = Ambari.getWebUrl(hostname=gatewayHost)
        clusterName = Ambari.getClusterName(weburl=weburl)

        # get knox host
        cluster_KnoxHosts = Ambari.getHostsForComponent("KNOX_GATEWAY", cluster=clusterName, weburl=weburl)
        if cluster_KnoxHosts:
            # get knox gateway port (this may throw Exception when there is no Knox)
            cluster_Config = Ambari.getConfig(service="KNOX", type='gateway-site', webURL=weburl)
            cluster_KnoxPort = cluster_Config['gateway.port']
            # Append knox port to knox host list
            cluster_KnoxHosts[:] = [word.strip() + ":" + cluster_KnoxPort for word in cluster_KnoxHosts]
            hosts = ','.join(cluster_KnoxHosts)

    return hosts
