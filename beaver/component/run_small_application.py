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
import random
import threading
import time

from beaver.component import HadoopJobHelper
from beaver.component.hadoop import HDFS, YARN
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)


class RunRWApps(object):
    """ Run small RandomTextWriter application in background
    The run() method will keep running randomtextwriter application one by one
    until stop() is called
    """

    def __init__(self, randomwriter_bytes="10", local_dir_name="small_rw_jobs"):
        """ Constructor
        :type count: number of apps needs to run
        :type randomwriter_bytes: num of bytes
        :type local_dir_name : dir name where job client logs will be stored
        :param interval: Check interval, in seconds
        """
        self.randomwriter_bytes = randomwriter_bytes
        self.local_dir_name = local_dir_name
        self.signal = True

        thread = threading.Thread(target=self.run, args=(self.randomwriter_bytes, self.local_dir_name))

        thread.start()

    def run(self, randomwriter_bytes="10", local_dir_name="small_rw_jobs"):  # pylint: disable=unused-argument
        local_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), self.local_dir_name)

        if not Machine.pathExists(None, None, local_dir, passwd=None):
            Machine.makedirs(None, None, local_dir, None)
            Machine.chmod("777", local_dir)

        while self.signal:
            input_dir = "rw_%d" % int(999999 * random.random())
            HDFS.deleteDirectory(input_dir)

            HadoopJobHelper.runRandomTextWriterJob(
                input_dir,
                self.randomwriter_bytes,
                bytesPerMap=1,
                mapsPerHost=1,
                jobArg="",
                user=None,
                config=None,
                runInBackground=False,
                redirect_file=os.path.join(local_dir, input_dir)
            )

    def stop(self):
        self.signal = False

    def validate_apps(self, local_dir_name="small_rw_jobs"):  # pylint: disable=unused-argument
        '''
        Validate small apps passed
        :param local_dir_name:
        :return:
        '''
        local_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), self.local_dir_name)
        appIds = []
        for root, _dirs, filenames in os.walk(local_dir):
            for f in filenames:
                logfile = open(os.path.join(root, f), 'r')
                stdout = logfile.read()
                appId = YARN.getApplicationIDFromStdout(stdout, logoutput=False)
                appIds.append(appId)
        # Sleep for 30 seconds before checking App status
        time.sleep(30)
        status, d = YARN.checkAppsSucceeded(appIds, logPrefix=None, useWS=True, localDir=None)
        for app, status in d.items():
            if status != "SUCCEEDED":
                appInfo = YARN.getApplicationInfo(app)
                logger.info(appInfo)
                if appInfo:
                    assert appInfo['state'] == 'ACCEPTED', "app is neither in ACCEPTED or SUCCEEDED State"
