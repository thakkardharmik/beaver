#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import re, os
from beaver.machine import Machine


class Sahara:
    @classmethod
    def parseSaharaTestResult(self, logfile):
        testresult = {}
        for line in open(logfile, 'r'):
            if "successful:" in line and re.findall("test_\w*", line) != []:
                tcid = re.findall("test_\w*", line)[0]
                testresult[str(tcid)] = {'result': 'pass'}

            if "failure:" in line and re.findall("test_\w*", line) != []:
                tcid = re.findall("test_\w*", line)[0]
                testresult[str(tcid)] = {'result': 'fail'}

            if "skip:" in line and re.findall("test_\w*", line) != []:
                tcid = re.findall("test_\w*", line)[0]
                testresult[str(tcid)] = {'result': 'skip'}
        return testresult

    @classmethod
    def findSaharaLogFileId(self, stdout):
        log_file_id = None
        id = re.findall("(id=\d*)", stdout)
        if id != []:
            log_file_id = re.findall("\d+", id[0])[0]
        return log_file_id

    @classmethod
    def getVersion(cls):
        exit_code, stdout = Machine.run(cmd="nova-manage --verbose --version")
        if exit_code == 0:
            return stdout
        return ""
