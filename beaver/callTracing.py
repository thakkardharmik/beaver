import dis
import linecache
import logging
import os
import re
import sys
import threading


class TraceFilter(logging.Filter):
    def filter(self, record):
        record.levelname = "TRACE"
        return True


class CallTracing(object):
    def __init__(self, inputDataDict=None):
        if not inputDataDict:
            inputDataDict = {}
        self.traceFilterObj = TraceFilter()
        self.logger = logging.getLogger(__name__)
        self.logger.addFilter(self.traceFilterObj)

        if "traceEventTypes" in inputDataDict.keys():
            self.traceEventTypes = inputDataDict["traceEventTypes"]
        else:
            self.traceEventTypes = ["call", "line", "return", "exception", "c_call", "c_return", "c_exception"]

        if "enableSystemCallTrace" in inputDataDict.keys():
            self.enableSystemCallTrace = inputDataDict["enableSystemCallTrace"]
        else:
            self.enableSystemCallTrace = False

        if "lineNumberMaxDigits" in inputDataDict.keys():
            self.lineNumberMaxDigits = inputDataDict["lineNumberMaxDigits"]
        else:
            self.lineNumberMaxDigits = 5

        if "printSourceCode" in inputDataDict.keys():
            self.printSourceCode = inputDataDict["printSourceCode"]
        else:
            self.printSourceCode = True

        if "printFuntionCallCompleteCode" in inputDataDict.keys():
            self.printFuntionCallCompleteCode = inputDataDict["printFuntionCallCompleteCode"]
        else:
            self.printFuntionCallCompleteCode = True

        if "printLocalVariables" in inputDataDict.keys():
            self.printLocalVariables = inputDataDict["printLocalVariables"]
        else:
            self.printLocalVariables = False

        if "printNameVariables" in inputDataDict.keys():
            self.printNameVariables = inputDataDict["printNameVariables"]
        else:
            self.printNameVariables = False

        if "printGlobalVariables" in inputDataDict.keys():
            self.printGlobalVariables = inputDataDict["printGlobalVariables"]
        else:
            self.printGlobalVariables = False

        if "printLineVariables" in inputDataDict.keys():
            self.printFilePath = inputDataDict["printLineVariables"]
        else:
            self.printLineVariables = True

        if "printThreadName" in inputDataDict.keys():
            self.printThreadName = inputDataDict["printThreadName"]
        else:
            self.printThreadName = False

        if "printFilePath" in inputDataDict.keys():
            self.printFilePath = inputDataDict["printFilePath"]
        else:
            self.printFilePath = False

        #Skip the file pattern from for call tracing
        self.skipFilesFromTracingList = ["python", "Python", "pytest", "callTracing.py", "conftest.py"]
        if "skipFilesFromTracingList" in inputDataDict.keys():
            self.skipFilesFromTracingList = self.skipFilesFromTracingList + inputDataDict["skipFilesFromTracingList"]

        if self.printLineVariables:
            pattern = "([a-zA-Z0-9_.]*)"
            self.compiledObj = re.compile(pattern)

    def getNextLineNumber(self, f_code, f_lineno):  # pylint: disable=no-self-use
        stdout = sys.stdout
        fileHandler = open(".temp", "w")
        sys.stdout = fileHandler
        dis.dis(f_code)
        sys.stdout = stdout
        fileHandler.close()

        filedata = open(".temp", "r").read()
        statementList = filedata.split(os.linesep + os.linesep)
        lineNumberList = []
        for statement in statementList:
            lineData = statement.split(os.linesep)[0]
            lineDataList = lineData.split(" ")
            for lineNumber in lineDataList:
                if lineNumber != "":
                    lineNumberList.append(lineNumber)
                    break
        index = -1
        try:
            index = lineNumberList.index(str(f_lineno))
        except Exception:
            pass
        if index == -1:
            if f_lineno < int(lineNumberList[0]):
                return int(lineNumberList[0])
        else:
            return int(lineNumberList[lineNumberList.index(str(f_lineno)) + 1])
        return None

    def tracingFunction(self, currentStackFrame, eventType, eventArguments):
        if eventType in self.traceEventTypes:
            #get the complete file path
            filePath = currentStackFrame.f_code.co_filename
            try:
                folderPath, fileName = os.path.split(filePath)
            except Exception:
                self.logger.debug("Exception happened during path=%s split", filePath)
                folderPath = "None"
                fileName = filePath

            #skip files from tracing, no need to trace these files
            if self.enableSystemCallTrace is False:
                for pattern in self.skipFilesFromTracingList:
                    result = filePath.find(pattern)
                    if result != -1:
                        return None

            #get the function name and its line number
            functionName = currentStackFrame.f_code.co_name
            lineNumber = currentStackFrame.f_lineno
            if eventType == "call":
                eventTypeData = "[CALL->]"
            elif eventType == "line":
                eventTypeData = "[-LINE-]"
            elif eventType == "return":
                eventTypeData = "[RETURN]"
            else:
                eventTypeData = "[" + eventType.upper().ljust(6, " ") + "]"

            #retrieve the source code line
            sourceCodeLine = ""
            if self.printSourceCode:
                try:
                    sourceCodeLine = linecache.getline(filePath, lineNumber).splitlines()[0]
                except Exception:
                    return None

            #retrieve the local, global and variables in the name space
            codeObject = currentStackFrame.f_code
            co_names = codeObject.co_names
            f_locals = currentStackFrame.f_locals
            f_globals = currentStackFrame.f_globals

            #Print THREAD name
            threadName = ""
            if self.printThreadName:
                try:
                    threadName = "[" + threading.currentThread().getName() + "] "
                except Exception:
                    threadName = "[None] "

            #print complete file path
            filepath = fileName
            if self.printFilePath:
                filepath = os.path.join(folderPath, fileName)

            #create traceData log line
            traceData = threadName + eventTypeData + filepath + ":" + functionName + "()-" + str(lineNumber).ljust(
                self.lineNumberMaxDigits, " "
            )

            #A function is called (or some other code block entered)
            if eventType == "call":
                self.logger.debug(traceData + sourceCodeLine)
                if self.printFuntionCallCompleteCode:
                    nextLineNumber = lineNumber
                    try:
                        nextLineNumber = self.getNextLineNumber(codeObject, lineNumber)
                    except Exception:
                        #self.logger.error("ERROR exception happened during 'getNextLineNumber'")
                        pass
                    if nextLineNumber != lineNumber:
                        for i in range(lineNumber + 1, nextLineNumber):
                            #retrieve the next source code line
                            try:
                                nextSourceCodeLine = linecache.getline(filePath, i).splitlines()[0]
                                traceData = threadName + "[      ]" + filepath + ":" + functionName + "()-" + str(
                                    i
                                ).ljust(self.lineNumberMaxDigits, " ")
                                self.logger.debug(traceData + nextSourceCodeLine)
                            except Exception:
                                self.logger.error("ERROR exception happened during 'getline'")

            #The interpreter is about to execute a new line of code.
            elif eventType == "line":
                #Print LOCAL variables
                localVariables = ""
                if self.printLocalVariables:
                    if f_locals:
                        try:
                            if "__builtins__" in f_locals.keys():
                                f_locals["__builtins__"]["copyright"] = "copyright truncated by tracing"
                                f_locals["__builtins__"]["credits"] = "credits truncated by tracing"
                            localVariables = " #LOCALS => " + str(f_locals)
                        except Exception:
                            localVariables = " #LOCALS => None"
                #Print NAME variables
                nameVariables = ""
                f_names = {}
                if self.printNameVariables:
                    for var in co_names:
                        if var not in f_locals:
                            try:
                                f_names[var] = f_globals[var]
                            except Exception:
                                pass
                    if f_names:
                        nameVariables = " #NAMES=> " + str(f_names)
                #Print GLOBAL variables
                globalVariables = ""
                if self.printGlobalVariables:
                    if f_globals:
                        try:
                            if "__builtins__" in f_globals.keys():
                                f_globals["__builtins__"]["copyright"] = "copyright truncated by tracing"
                                f_globals["__builtins__"]["credits"] = "credits truncated by tracing"
                            globalVariables = " #GLOBALS => " + str(f_globals)
                        except Exception:
                            globalVariables = " #GLOBALS => None"
                #Print line variable VALUES
                lineVariables = ""
                if self.printLineVariables:
                    f_lineVariables = {}
                    resultList = set(self.compiledObj.findall(sourceCodeLine))
                    for var in resultList:
                        try:
                            pyobject = eval(var, f_globals, f_locals)  # pylint: disable=eval-used
                            if hasattr(pyobject, "__call__"):
                                continue
                        except Exception:
                            pass
                        if var in f_locals:
                            f_lineVariables[var] = f_locals[var]
                        elif var in f_globals:
                            f_lineVariables[var] = f_globals[var]
                        else:
                            if "." in var:
                                try:
                                    varValue = eval(var, f_globals, f_locals)  # pylint: disable=eval-used
                                    f_lineVariables[var] = varValue
                                except Exception:
                                    pass
                    if f_lineVariables:
                        lineVariables = " #VALUES => " + str(f_lineVariables)

                self.logger.debug(
                    traceData + sourceCodeLine + lineVariables + localVariables + nameVariables + globalVariables
                )

            #A function (or other code block) is about to return.
            elif eventType == "return":
                self.logger.debug("%s%s #RETURN => %s", traceData, sourceCodeLine, str(eventArguments))
            #When an exception occurs during execution
            elif eventType == "exception":
                self.logger.debug("%s%s #EXCEPTION => %s", traceData, sourceCodeLine, str(eventArguments))
            #A C function is about to be called. This may be an extension function or a built-in.
            #  arg is the C function object.
            elif eventType == "c_call":
                self.logger.debug(traceData + sourceCodeLine)
            #A C function has returned. arg is the C function object.
            elif eventType == "c_return":
                self.logger.debug(traceData + sourceCodeLine)
            #A C function has raised an exception. arg is the C function object.
            elif eventType == "c_exception":
                self.logger.debug(traceData + sourceCodeLine)
            else:
                print "ERROR !!!"
            return self.tracingFunction
        return None

    def start(self):
        sys.settrace(self.tracingFunction)

    def stop(self):  # pylint: disable=no-self-use
        sys.settrace(None)
