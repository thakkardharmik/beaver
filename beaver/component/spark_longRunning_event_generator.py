#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import os, time, random, logging, string
import threading
from beaver.machine import Machine
from beaver.component.hadoop import YARN, Hadoop, HDFS, MAPRED
from beaver.config import Config
from beaver import util
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class GenerateHDFSWordcountEvents(threading.Thread):
    """ Run HDFS Wordcount event generator
    The run() method will keep adding files to HDFS input dir
    """

    def __init__(self, hdfs_input_dir, interval=300, times=2):
        """ Constructor
        :type hdfs_input_dir: HDFS input dir for Spark Hdfs Wordcount streaming application
        :type interval: add file to hdfs_input_dir after each interval (in seconds)
        :type times: Number of times for adding files to hdfs_input_dir
        """
        self.hdfs_input_dir = hdfs_input_dir
        self.interval = interval
        self.times = times
        threading.Thread.__init__(self)

    def run(self):
        """
        Move files to HDFS Input Dir after each interval period for n times.
        """
        for count in range(0, self.times):
            text = "hello world \n Testing HDFS Word count Spark application"
            random_name = ''.join(random.choice(string.lowercase) for i in range(5))
            filename = os.path.join(Config.getEnv('ARTIFACTS_DIR'), random_name)
            util.writeToFile(text, filename, isAppend=False)
            max_retry = 3
            count = 0
            while count < max_retry:
                try:
                    if "hdfs://ns2" in self.hdfs_input_dir:
                        cp_status = HDFS.copyFromLocal(filename, "hdfs://ns2/tmp", enableDebug=True)
                    else:
                        cp_status = HDFS.copyFromLocal(filename, "/tmp", enableDebug=True)
                    assert cp_status[0] == 0, "Failed to copy file to HDFS 'tmp'"
                    logger.info("copyFromLocal command finished for %s" % filename)
                    if "hdfs://ns2" in self.hdfs_input_dir:
                        mv_status = HDFS.mv(None, "hdfs://ns2/tmp/" + random_name, self.hdfs_input_dir, config=None)
                    else:
                        mv_status = HDFS.mv(None, "/tmp/" + random_name, self.hdfs_input_dir, config=None)
                    assert mv_status[0] == 0, "Failed to move file from 'tmp' to test directory"
                except:
                    if count < max_retry:
                        count = count + 1
                        logger.info(
                            "File copy into HDFS test directory failed after %s attempts, retrying after 120s sleep interval"
                            % count
                        )
                        time.sleep(120)
                    else:
                        logger.error("Failed to copy file into HDFS test directory, expect failures in HDFSWordCOunt")
                else:
                    break

            logger.info("%s moved to %s" % (filename, self.hdfs_input_dir))
            logger.info("sleeping for %s seconds" % self.interval)
            time.sleep(self.interval)
