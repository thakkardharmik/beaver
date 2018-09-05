from beaver.config import Config
from beaver import util
import os, logging, re
import datetime, time
from beaver.machine import Machine

logger = logging.getLogger(__name__)


# This class implements a logger for Upgrade operations
class UpgradeLogger:
    UPGRADE_STATUS_LOG_FILE = os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'upgrade_progress.log')

    # This method returns the name of upgrade log file
    @classmethod
    def get_progress_local_file(cls):
        return cls.UPGRADE_STATUS_LOG_FILE

    # This method writes info/error messages to the log file
    @classmethod
    def reportProgress(cls, message, is_info_message):
        message = message + "\n"
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        if is_info_message:
            logger.info(message)
            util.writeToFile(
                timestamp + "|INFO|" + message.encode('utf-8'), cls.UPGRADE_STATUS_LOG_FILE, isAppend=True
            )
        else:
            logger.error(message)
            util.writeToFile(
                timestamp + "|ERROR|" + message.encode('utf-8'), cls.UPGRADE_STATUS_LOG_FILE, isAppend=True
            )

    # This method returns last 100 lines of the error text. If number of lines < 100, all lines are returned
    @classmethod
    def get_error_text(cls, error_text):
        error_messages = error_text.split("\n")
        error_lines = "======================================\n"
        max_lines_to_print = 100
        total_lines = error_messages.__len__()

        if total_lines > max_lines_to_print:
            for error_message_index in range(total_lines - max_lines_to_print, total_lines, 1):
                error_lines = error_lines + error_messages[error_message_index].decode('utf-8') + '\n'
        else:
            for error_message_index in range(total_lines):
                error_lines = error_lines + error_messages[error_message_index].decode('utf-8') + '\n'

        error_lines += "\n======================================"

        return error_lines

    # This method returns testname and associated failure message
    @classmethod
    def get_stack_trace(cls, test_name, current_dir):
        apiReportDirectory = os.path.join(current_dir, 'target', 'surefire-reports', 'junitreports')
        uifrmReportDirectory = os.path.join(current_dir, 'target', 'surefire-reports')

        pattern = '*' + UpgradeLogger.get_testclass_name(test_name) + '*.xml'

        testResultFiles = util.findMatchingFiles(
            apiReportDirectory, pattern
        )  # First search for test result xml in api framework dir
        if testResultFiles.__len__() == 0:
            testResultFiles = util.findMatchingFiles(
                uifrmReportDirectory, pattern
            )  # Now search for test result xml in uifrm dir
            Machine.runas(
                'root', 'chmod -R 755 ' + uifrmReportDirectory
            )  # Provide read permissions to everyone in uifrm junit report xml directory
        else:
            Machine.runas(
                'root', 'chmod -R 755 ' + apiReportDirectory
            )  # Provide read permissions to everyone in api framework report xml directory

        testresult = {}
        for resultFile in testResultFiles:
            testresult.update(util.parseJUnitXMLResult(resultFile))

        testOutput = {}

        for key, value in testresult.items():
            m = re.search("([^\.]*)$", key)  # Extract test name
            print "key : %s " % key
            print "value : %s " % value
            key = m.group(0)
            fail_message = value['failure']
            print "Final key: %s" % key
            print "Final fail_msg: %s" % fail_message

            if fail_message:
                testOutput[str(key)] = fail_message

        print testOutput

        return testOutput

    @classmethod
    def get_testclass_name(cls, test_name):
        if "#" in test_name:
            return test_name.split("#")[0]
        else:
            return test_name
