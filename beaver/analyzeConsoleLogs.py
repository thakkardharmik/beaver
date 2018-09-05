import getopt
import os
import re
import shutil
import sys

try:
    opts, args = getopt.getopt(sys.argv[1:], "f:h", ["filter=", "help="])
except getopt.GetoptError:
    print 'analyzeConsoleLogs.py [-h | --help] [-f | --filter] [<regex pattern>]'
    print 'regular expression matching is done with ignorecase.'
    print 'filter is done on either full path of python file or test name.'
    sys.exit(0)

filterPattern = None
for opt, arg in opts:
    if opt in ("-h", "--help"):
        print 'analyzeConsoleLogs.py [-h | --help] [-f | --filter] [<regex pattern>]'
        print 'regular expression matching is done with ignorecase.'
        print 'filter is done on either full path of python file or test name.'
        sys.exit(0)
    elif opt in ("-f", "--filter"):
        filterPattern = arg

# pylint: disable=superfluous-parens
if filterPattern:
    print("using filter pattern = %s" % filterPattern)
print("analyzing console logs...")
# pylint: enable=superfluous-parens

NANO_PATH = '/grid/0/hadoopqe/artifacts/output.log'
lines = None
if os.path.isfile(NANO_PATH):
    #make a copy and read from that file
    shutil.copyfile(NANO_PATH, NANO_PATH + ".copy")
    lines = [line.strip() for line in open(NANO_PATH + ".copy")]

FLUBBER_PATH = '/grid/0/tmp/hwqe/artifacts/output.log'
if os.path.isfile(FLUBBER_PATH):
    #make a copy and read from that file
    shutil.copyfile(FLUBBER_PATH, FLUBBER_PATH + ".copy")
    lines = [line.strip() for line in open(FLUBBER_PATH + ".copy")]

if lines is None:
    sys.exit(0)

#get failed test case names
failedTests = []
for line in lines:
    regExResult = re.search('main[|]INFO[|]TEST "(.*)" FAILED', line)
    if regExResult is not None:
        #print(line)
        failedTests.append(regExResult.group(1))
if not failedTests:
    sys.exit(0)

failedTestsTuples = []
#get filename of failed tests
for line in lines:
    regExResult = re.search('main[|]INFO[|]RUNNING TEST "(.*)" at location "(.*)" at line number "(.*)"', line)
    if regExResult:
        if filterPattern is None or re.search(filterPattern, line, re.IGNORECASE) is not None:
            testName = regExResult.group(1)
            #testDir = full path of python file
            testDir = regExResult.group(2)
            isFailed = any(testName == failedTest for failedTest in failedTests)
            if isFailed:
                #2nd element is python file name
                failedTestsTuples.append((testName, testDir.split("/")[-1], testDir))

numFailedTests = len(failedTestsTuples)

for index in range(numFailedTests):
    #print("")
    testName = failedTestsTuples[index][0]
    #testDir = failedTestsTuples[index][2]
    #print("{0:>3}: {1}:{2}".format(index + 1, testDir, testName))
    testName = testName.replace("[", "\\[")
    testName = testName.replace("]", "\\]")
    testName = testName.replace("(", "\\(")
    testName = testName.replace(")", "\\)")
    testTextFound = False
    textList = []
    for line in lines:
        regExResult = re.search('main[|]INFO[|]RUNNING TEST "%s"' % testName, line)
        if regExResult is not None:
            testTextFound = True
            textList.append("")
        if testTextFound is True:
            textList.append(line)
        regExResult = re.search('main[|]INFO[|]TEST "%s" FAILED' % testName, line)
        if regExResult is not None:
            textList.append("")
            break
    failedTestsTuples[index] = failedTestsTuples[index] + ("\n".join(textList), )
    #print("-------")

#sort result by testdir:testname
sorted(failedTestsTuples, key=lambda t: t[2] + ":" + t[0])

lastGroupHeader = ""
groupHeader = ""
for index in range(numFailedTests):
    testName = failedTestsTuples[index][0]
    testDir = failedTestsTuples[index][2]
    groupHeader = testDir
    if groupHeader != lastGroupHeader:
        print("FAILED GROUP: %s" % groupHeader)  # pylint: disable=superfluous-parens
    lastGroupHeader = groupHeader
    # pylint: disable=superfluous-parens
    print("")
    print("{0:>3}: {1}:{2}".format(index + 1, testDir, testName))
    print(failedTestsTuples[index][3])
    print("------")
    # pylint: enable=superfluous-parens

count = {}
for index in range(numFailedTests):
    key = failedTestsTuples[index][1]
    count[key] = count.get(key, 0) + 1

print("Number of aborted/failed tests: %s" % numFailedTests)  # pylint: disable=superfluous-parens
for index in range(numFailedTests):
    testDir = failedTestsTuples[index][2]
    #testFileName = failedTestsTuples[index][1]
    #if testDir != prevTestDir:
    #    print("%s %s" % (testFileName, count[testFileName] ))
    print(  # pylint: disable=superfluous-parens
        "{0:>3}: {1}:{2}".format(index + 1, testDir, failedTestsTuples[index][0])
    )
    prevTestDir = testDir
# pylint: disable=superfluous-parens
print("")
print("------")
print("Summary (online sheet format):")
print("")
# pylint: enable=superfluous-parens
prevFileName = ""
sumCount = 0
for index in range(numFailedTests):
    testFileName = failedTestsTuples[index][1]
    printedFileName = testFileName.replace("test_", "")
    printedFileName = printedFileName.replace(".py", "")
    if testFileName != prevFileName:
        if printedFileName.find("/") > -1:
            printedFileName = printedFileName.split("/")
        else:
            printedFileName = printedFileName.split("\\")
        if len(printedFileName) > 2:
            printedFileName = printedFileName[-2]
        else:
            printedFileName = printedFileName[-1]
        print("%s %s" % (count[testFileName], printedFileName))  # pylint: disable=superfluous-parens
        sumCount += count[testFileName]
    prevFileName = testFileName

print("total: %s" % sumCount)  # pylint: disable=superfluous-parens
#cleanup
if os.path.isfile(NANO_PATH + ".copy"):
    os.remove(NANO_PATH + ".copy")

if os.path.isfile(FLUBBER_PATH + ".copy"):
    os.remove(FLUBBER_PATH + ".copy")
