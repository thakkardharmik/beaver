import logging
import os
from ConfigParser import ConfigParser

logger = logging.getLogger(__name__)

# '''
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d: - %(message)s ')
# ch.setFormatter(formatter)
# logger.addHandler(ch)
# '''


class ReportHelper(object):
    '''
    A class to help reporting.
    '''
    fileConfigAliasModuleName = "confReport/moduleNameAlias.conf"
    cachedConfigAliasModuleName = None

    def __init__(self):
        pass

    @classmethod
    def getFailedTestsList(cls, failedTestsStr, logoutput=False):
        '''
        Gets list of failedTests. Splitting failed test text with , is insufficient.
        This method takes care of cleaning up list of failed tests after the split.
        Returns a list of String.
        '''
        tempFailedTestsList = failedTestsStr.split(",")

        if logoutput:
            logger.info("getFailedTestsList tempFailedTestsList=%s", tempFailedTestsList)
        #a parameter can contain ",". Needs to clean up list of failed tests further.
        failedTestsList = []
        foundOpenedSquareBracket = False
        for elem in tempFailedTestsList:
            if foundOpenedSquareBracket:
                failedTestsList[-1] = failedTestsList[-1] + "," + elem
                if elem.find("]") > -1:
                    foundOpenedSquareBracket = False
            else:
                failedTestsList.append(elem)
                if elem.find("[") > -1 and elem.find("]") == -1:
                    foundOpenedSquareBracket = True
        return failedTestsList

    @classmethod
    def getFailureSummary(cls, failedTestsStr, logoutput=False):
        '''
        Gets failure summary. It is a wrapper to _getFailureSummaryInternal.
        Exception is handled here.
        Returns a String.
        '''
        result = None
        try:
            result = cls._getFailureSummaryInternal(failedTestsStr, logoutput=logoutput)
        except Exception as e:
            #Error or Exception
            logger.info("Exception is thrown in getFailureSummary. %s", e)
        if result is None:
            return "Error"
        else:
            return result

    @classmethod
    def _getFailureSummaryInternal(cls, failedTestsStr, logoutput=False):
        '''
        Internal method to get failure summary.
        Returns a String.
        '''
        countMap = {}
        failedTestsList = cls.getFailedTestsList(failedTestsStr, logoutput=logoutput)
        if logoutput:
            logger.info("_getFailureSummaryInternal failedTestsList=%s", failedTestsList)
        for failedTest in failedTestsList:
            #e.g. failedTest = "tests/nonnightly/reporting/failedTests/test3.py:test3c[1]"
            #get module name
            moduleName = cls.getModuleName(failedTest, logoutput)
            #get alias name for grouping
            moduleName = cls.getAliasModuleName(moduleName, cls.fileConfigAliasModuleName, logoutput)
            if moduleName.strip() != "":
                if moduleName in countMap:
                    countMap[moduleName] += 1
                else:
                    countMap[moduleName] = 1
        moduleNames = countMap.keys()
        #moduleNames = sorted(moduleNames)
        tmpResult = []
        for moduleNames in moduleNames:
            tmpResult.append("%s - %s - " % (countMap[moduleNames], moduleNames))
        if not tmpResult:
            return ""
        else:
            return ','.join(tmpResult)

    @classmethod
    def getModuleName(cls, failedTest, logoutput=False):  # pylint: disable=unused-argument
        '''
        Gets module name for failed test string.
        Current implementation uses modified test filename as module name.
        Parameter e.g. failedTest =
                "tests/hdfs/nfs/tests_nfsDataDownloadUpload.py:test_ValidateDataUpload[Test_Upload_1GB_File-1GB_File]"
        Returns a String. e.g. "nfsDataDownloadUpload"
        '''
        if not failedTest:
            return ""
        #discard parameter
        failedTest = failedTest.split("[")[0]
        testFilename = failedTest.split(os.sep)[-1].split(":")[0]
        testFilename = testFilename.replace(".py", "")
        if testFilename.startswith("test"):
            testFilename = testFilename.replace("test_", "")
            testFilename = testFilename.replace("test", "")
        return testFilename

    @classmethod
    def getAliasModuleName(cls, moduleName, configFile, logoutput=False):  # pylint: disable=unused-argument
        '''
        Gets alias module name if a user specifies an entry in alias file.
        Returns a String.
        '''
        if cls.cachedConfigAliasModuleName is None:
            cls.cachedConfigAliasModuleName = {}
            try:
                config = ConfigParser()
                config.optionxform = str  #make it case sensitive
                config.read(configFile)
                for section in config.sections():
                    nameValueList = config.items(section)
                    for (name, value) in nameValueList:
                        cls.cachedConfigAliasModuleName[name] = value
            except Exception as ex:
                logger.info("Exception is thrown when reading file in getAliasModuleName. %s", ex)
            finally:
                #logger.info("config alias module name = ", cls.cachedConfigAliasModuleName)
                pass
        if moduleName in cls.cachedConfigAliasModuleName:
            moduleName = cls.cachedConfigAliasModuleName[moduleName]
        return moduleName

    @classmethod
    def getGroupedFailedTests(cls, failedTestsStr, logoutput=False):
        '''
        Gets grouped failed tests. It is a wrapper to _getGroupedFailedTestsInternal.
        Exception is handled here.
        Returns a String.
        '''
        result = None
        try:
            result = cls._getGroupedFailedTestsInternal(failedTestsStr, logoutput=logoutput)
        except Exception as e:
            #Error or Exception
            logger.info("Exception is thrown in getGroupedFailedTests. %s", e)
        finally:
            if result is None:
                result = "Error"
        return result

    @classmethod
    def _getGroupedFailedTestsInternal(cls, failedTestsStr, logoutput=False):
        '''
        Internal method to get grouped failed test.
        Returns a String.
        FAILED_TESTS=<test suite file 1>:<failure1>,
                     <test suite file 1>:<failure2>,
                     <test suite file 1>:<failure3>,
                     <test suite file 2>:<failure1>,
                     <test suite file 2>:<failure2>
        is grouped to:
        FAILED_TESTS:<test suite file 1>:<failure1>,<failure2>,<failure3>,<test suite file 2>:<failure1>,<failure2>
        '''
        failedTestsList = cls.getFailedTestsList(failedTestsStr, logoutput=logoutput)
        if logoutput:
            logger.info("_getGroupedFailedTestsInternal failedTestsList=%s", failedTestsList)
        failedTestsList = sorted(failedTestsList)
        testSuiteDict = {}
        for failedTest in failedTestsList:
            #failedTest =
            #   "tests/hdfs/nfs/tests_nfsDataDownloadUpload.py:test_ValidateDataUpload[Test_Upload_1GB_File-1GB_File]"
            (suiteName, failure) = cls.getSuiteFailureTuple(failedTest, logoutput=logoutput)
            if (suiteName, failure) != ("", ""):
                if suiteName in testSuiteDict:
                    testSuiteDict[suiteName] += "," + failure
                else:
                    testSuiteDict[suiteName] = "%s:%s" % (suiteName, failure)
        if logoutput:
            logger.info("_getGroupedFailedTestsInternal testSuiteDict=%s", testSuiteDict)
        keys = testSuiteDict.keys()
        if not keys:
            return ""
        else:
            tmpResult = []
            for key in sorted(keys):
                tmpResult.append(testSuiteDict[key])
            return '||'.join(tmpResult)

    @classmethod
    def getSuiteFailureTuple(cls, failedTest, logoutput=False):  # pylint: disable=unused-argument
        '''
        Gets (suite name,failure) for failed test string.
        Parameter e.g. failedTest =
            "tests/hdfs/nfs/tests_nfsDataDownloadUpload.py:test_ValidateDataUpload[Test_Upload_1GB_File-1GB_File]"
        Returns a tuple of (String, String)
        e.g. ("hdfs/nfs/tests_nfsDataDownloadUpload.py",
              "test_ValidateDataUpload[Test_Upload_1GB_File-1GB_File]")
        '''
        if not failedTest:
            return ("", "")
        suiteName = failedTest.split(":")[0]
        failure = failedTest.split(":")[1]
        if suiteName.startswith("tests/"):
            suiteName = suiteName.replace("tests/", "")
        return (suiteName, failure)

    @classmethod
    def getMergedFailedTests(  # pylint: disable=unused-argument
            cls, failedTestsStr, failureSummaryStr, logoutput=False
    ):
        '''
        Gets failed test entry that has both summary and failed tests list.
        Returns a String.
        '''
        result = ""
        try:
            if failedTestsStr is None or not failedTestsStr.rstrip():
                return result
            failureSummaryList = failureSummaryStr.split(",")
            for failureSummary in failureSummaryList:
                result += failureSummary[0:-3] + "<br>"
            result += "<br>" + failedTestsStr
        except Exception as e:
            logger.info("Exception is thrown in getMergedFailedTests. %s", e)
        #finally:
        return result
