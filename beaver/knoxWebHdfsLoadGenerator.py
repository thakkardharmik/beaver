#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import hashlib
import logging
import os
import random
import ssl
import tempfile
import threading
import time
import urllib
import urllib2

from beaver.component.hadoop import Hadoop
from beaver.config import Config
logger = logging.getLogger(__name__)

KNOX_GUEST_USER = "guest"
KNOX_GUEST_PWD = "guest-password"
KNOX_HOST = Config.get('knox', 'KNOX_HOST').split(',')[0]  # Just use the first Knox instance in the list for now.
KNOX_PORT = "8443"
HDFS_USER = Config.get('hadoop', 'HDFS_USER')
WEBHDFS_CONTEXT_ROOT = "/gateway/sandbox/webhdfs/v1"
if Hadoop.isAmbari():
    WEBHDFS_CONTEXT_ROOT = "/gateway/default/webhdfs/v1"


class KnoxWebHdfsLoadGenerator(object):
    """
  A class that generates WebHDFS load though the Knox gateway.
  Note: This currently does not support AD authentication.

  The following operations are currently performed in a loop by the load generator in each of the load threads.
  1. Mkdir
  2. Rename dir
  3. Create file with non-zero data
  4. Renaming the file
  5. Verifying the file data
  6. Ls on a dir
  """

    def __init__(
            self, numThreads, hdfs_test_dir, ip=KNOX_HOST, port=KNOX_PORT, username=KNOX_GUEST_USER, pwd=KNOX_GUEST_PWD
    ):
        """
    Initializing member vars.
    :param numThreads: # of threads used to generate the load. This is synonymous with # outstanding
                      requests (with 1 thread processing 1 request at a time).
    :param ip: Knox gateway ip.
    :param port: Knox gateway port.
    :param username: Knox gateway username.
    :param pwd: Knox gateway pwd.
    :return:
    """
        logger.info(
            "Initializing Load Gen with %d threads, %s:%s, UserInfo: %s, %s", numThreads, ip, port, username, pwd
        )
        self.numThreads = numThreads
        self.hdfsTestDir = hdfs_test_dir
        self.gatewayIp = ip
        self.gatewayPort = port
        self.username = username
        self.pwd = pwd
        self.threads = {}  # Dict to hold the thread objects created.
        self.stats = {}  # Holds success, failed counters for each thread generating the load.
        self.keepAlive = {}  # Holds threading.Event() object for each thread generating the load.
        # Note: No locks are required to update the above dicts from multiple threads because dicts are thread safe.
        for i in range(self.numThreads):
            self.stats[i] = (0, 0)  # (# successful iterations, # failed iterations)
            self.keepAlive[i] = threading.Event()
            self.keepAlive[i].set()

    def __getAndCompareFileChecksum(  # pylint: disable=no-self-use
            self,
            knox_webhdfs_client,
            hdfs_file_path,
            expected_checksum
    ):
        """
    The methods retrieves the checksum of a file from hdfs and compares it with expected checksum.
    :param knox_webhdfs_client: Knox webhdfs client to be used to perform the operation.
    :param hdfs_file_path: Path of file on hdfs.
    :param expected_checksum: Checksum expected.
    :return: Bool success status.
    """
        comparison_succeeded = False
        file_data = knox_webhdfs_client.readFile(hdfs_file_path)

        if file_data is None:
            logger.info("ReadFile failed for file: %s", hdfs_file_path)
        else:
            logger.debug("ReadFile output: %s", file_data)
            md5 = hashlib.md5()
            md5.update(file_data)
            data_digest = md5.hexdigest()
            logger.debug("Checksum of read file: %s", data_digest)
            if data_digest == expected_checksum:
                comparison_succeeded = True
            else:
                logger.info(
                    "Md5 checksum mismatch for %s: Expected: %s Got: %s", hdfs_file_path, expected_checksum,
                    data_digest
                )

        return comparison_succeeded

    def __updateStats(self, thread_id, iteration_succeeded):
        """
    Updates the success, failure counters for the given thread.
    :param thread_id: Thread id whose stats need to be updated.
    :param iteration_succeeded: Was the current interation successful.
    :return:
    """
        num_successful_iters, num_failed_iters = self.stats[thread_id]
        if iteration_succeeded:
            num_successful_iters += 1
        else:
            num_failed_iters += 1
        self.stats[thread_id] = (num_successful_iters, num_failed_iters)

    def __generateWebHdfsLoad(self, curr_thread_id, perform_file_operations=True, perform_data_verification=True):
        """
    This method generates webhdfs load via knox gw.
    The load continues to run until the Thread is terminated or until an error is encountered.
    :param curr_thread_id: Id of the current thread.
    :param perform_file_operations: Controls file creation as part of load generation.
    :param perform_data_verification: Verify the md5sum of the file created via. Knox.
    """
        # Create a test file locally
        tmp_text = "Test File Data."
        md5 = hashlib.md5()
        md5.update(tmp_text)

        # Calculate the digest only if verification is enabled.
        if perform_data_verification:
            reference_digest = md5.hexdigest()
            logger.info("Reference digest: %s", reference_digest)

        # Create a local tmp file with the above text and use this file to load
        # via Knox into Hdfs.
        temp = tempfile.NamedTemporaryFile(mode="w+b", delete=False)
        local_file_path = temp.name
        temp.write(tmp_text)
        temp.close()

        knox_webhdfs_client = KnoxWebHdfsClient(self.gatewayIp, self.gatewayPort, self.username, self.pwd)

        # Keep creating directories and copy a file into the dir using Knox WebHdfs.
        # Download the file from HDFS and compare their md5sums.
        i = 1
        while self.keepAlive[curr_thread_id].isSet():
            # Generate a random floating number between 0 to 2 and sleep for that amt of time.
            # This introduces variation in requests that are outstanding at the knox gateway.
            # Without this, we were seeing that all threads, at any given time,
            #   were making equal progress and were submitting
            # the same types of requests to the gateway. The sleep offsets this uniformity of execution across threads
            # and we will see different types of requests at the gateway at a given time.
            rand_time_secs = random.uniform(0, 2)
            time.sleep(rand_time_secs)

            # Create dir
            random_dir_name = "test_dir_%d_%s" % (i, curr_thread_id)
            hdfs_dir_path = "%s/%s" % (self.hdfsTestDir, random_dir_name)
            mkdir_status = knox_webhdfs_client.mkdir(hdfs_dir_path)

            if not mkdir_status:
                logger.info("Knox WebHdfs mkdir failed.")
                self.__updateStats(curr_thread_id, False)
                continue

            # Rename the created dir
            renamed_hdfs_dir_path = "%s_renamed" % hdfs_dir_path
            renamed_dir_status = knox_webhdfs_client.rename(hdfs_dir_path, renamed_hdfs_dir_path)

            if not renamed_dir_status:
                logger.info("Rename dir failed.")
                self.__updateStats(curr_thread_id, False)
                continue

            if perform_file_operations:
                # Create a file inside the dir
                random_file_name = "test_file_%d_%s" % (i, curr_thread_id)
                hdfs_file_path = "%s/%s" % (renamed_hdfs_dir_path, random_file_name)
                create_file_status = knox_webhdfs_client.createFile(hdfs_file_path, local_file_path)

                if create_file_status != "":
                    logger.info("Knox WebHdfs Create file failed.")
                    self.__updateStats(curr_thread_id, False)
                    continue

                # Rename the created file from above
                current_hdfs_file_path = "%s/%s" % (renamed_hdfs_dir_path, random_file_name)
                renamed_hdfs_file_path = "%s_renamed" % hdfs_file_path
                renamed_file_name_status = knox_webhdfs_client.rename(current_hdfs_file_path, renamed_hdfs_file_path)

                if not renamed_file_name_status:
                    logger.info("Rename file failed.")
                    self.__updateStats(curr_thread_id, False)
                    continue

                # Verify the md5sum of the created file
                if perform_data_verification:
                    if not self.__getAndCompareFileChecksum(knox_webhdfs_client, renamed_hdfs_file_path,
                                                            reference_digest):
                        self.__updateStats(curr_thread_id, False)
                        continue
                    else:
                        logger.debug("Md5 comparison succeeded.")

            # Perform an ls on the test dir.
            ls_status = knox_webhdfs_client.ls(self.hdfsTestDir)

            if not ls_status:
                logger.info("Ls dir failed.")
                self.__updateStats(curr_thread_id, False)
                continue

            # If we are here then the iteration succeeded. Update Stats.
            self.__updateStats(curr_thread_id, True)
            i += 1

        # Delete the temp file
        os.remove(local_file_path)

    def start(self, perform_file_operations=True, perform_data_verification=True):
        """
    Starts the load generator threads.
    :param perform_file_operations: Controls if file creation needs to happen as part of load generation.
    :param perform_data_verification: Verify the md5sum of the file created via. Knox.
    :return:
    """
        logger.info("Starting LoadGen...")
        for i in range(self.numThreads):
            self.keepAlive[i].set()
            t = threading.Thread(
                target=self.__generateWebHdfsLoad,
                args=(i, perform_file_operations, perform_data_verification),
                name="LoadGen-%d" % i
            )
            self.threads[i] = t
            t.start()
        logger.info("Start Successful.")

    def stop(self):
        """
    Stops all the load generator threads.
    :return:
    """
        logger.info("Stopping LoadGen...")

        logger.info("Clearing threads...")
        for i in range(len(self.keepAlive)):
            logger.info("Clear thread %d", i)
            self.keepAlive[i].clear()

        logger.info("Joining threads...")
        for i in range(len(self.threads)):
            logger.info("Join thread %d", i)
            self.threads[i].join(60)

        logger.info("Stop LoadGen complete.")
        logger.info("Stats: %s", self.stats)

    def clearStats(self):
        """
    Clears the stats for all the threads.
    :return:
    """
        logger.info("Stats before clearing: %s", self.stats)
        for i in range(self.numThreads):
            self.stats[i] = (0, 0)
        logger.info("ClearStats Successful.")

    def verifyLoadGenerationSucceeded(self):
        """
    Verifies if the load generation on each of the threads succeeded (i.e., no failed iters).
    :return: Bool - success status.
    """
        success = True
        for i in range(self.numThreads):
            num_successful_iters, num_failed_iters = self.stats[i]
            if num_failed_iters > 0 or num_successful_iters <= 0:
                success = False

        logger.info("LoadGenerationStatus: %s Threads' Stats: %s", success, self.stats)
        return success


class KnoxWebHdfsClient(object):
    """
  Implements a WebHdfs client that operates over Knox.
  """

    def __init__(self, ip=KNOX_HOST, port=KNOX_PORT, username=KNOX_GUEST_USER, pwd=KNOX_GUEST_PWD):
        """
    Initializing member vars.
    :param ip: Knox gateway ip.
    :param port: Knox gateway port.
    :param username: Knox gateway username.
    :param pwd: Knox gateway pwd.
    :return:
    """
        logger.info("Initializing KnoxWebHdfsClient with %s:%s, UserInfo: %s, %s", ip, port, username, pwd)
        self.gatewayIp = ip
        self.gatewayPort = port
        self.username = username
        self.pwd = pwd
        self.root_url = 'https://%s:%s' % (self.gatewayIp, self.gatewayPort)

    def __makeWebHdfsRequest(self, url, type="PUT", data=None, headers=None):  # pylint: disable=redefined-builtin
        """
    Makes a HTTP request of type on url to the Knox gateway.
    :param url: Request url.
    :param type: Can be GET or PUT.
    :return: request_handle or None is returned.
    """
        # TODO: Change the method signature to accept type_
        type_ = type  # pylint: disable=redefined-builtin
        pwd_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        pwd_mgr.add_password(None, self.root_url, self.username, self.pwd)
        auth_handler = urllib2.HTTPBasicAuthHandler(pwd_mgr)
        # Init opener with auth and CustomRedirect Handlers.
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        opener = urllib2.build_opener(urllib2.HTTPSHandler(context=ctx), auth_handler, CustomRedirectHandler())
        urllib2.install_opener(opener)

        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            logger.debug("Making %s request using %s", type_, url)
            if not headers:
                headers = {}
            request = urllib2.Request(url, data, headers)
            if type_ == "PUT":
                request.get_method = lambda: 'PUT'
            request_handle = opener.open(request)
            return request_handle
        except Exception, e:
            logger.info(
                "An exception occurred while making %s request using %s issued at time %s : %s", type_, url, ts, e
            )
            return None

    def ls(self, hdfs_path):
        """
    Creates a dir using webhdfs.
    :param hdfs_path: Path on hdfs.
    :return: Ls output or None.
    """
        # This method executes a request equivalent to:
        # cmd = "curl -i -k -u %s:%s -X GET 'https://%s:%s/gateway/sandbox/webhdfs/v1/%s?op=LISTSTATUS'" % \
        #      (self.username, self.pwd, self.gatewayIp, self.gatewayPort, hdfs_path)

        ls_url = "%s%s%s?op=LISTSTATUS" % (self.root_url, WEBHDFS_CONTEXT_ROOT, hdfs_path)
        request_handle = self.__makeWebHdfsRequest(ls_url, "GET")

        if not request_handle:
            logger.info("Request failed. %s", ls_url)
            return None
        logger.debug("Request status: %d", request_handle.getcode())
        if request_handle.getcode() != 200:
            logger.info("Error code obtained while executing ls: %s", request_handle.getcode())
            return None
        else:
            data = request_handle.read()
            logger.debug("Ls output: %s", data)
            return data

    def mkdir(self, hdfs_path):
        """
    Creates a dir using webhdfs.
    :param hdfs_path: Dir path on hdfs.
    :return: Mkdir output or None.
    """
        # This method executes a request equivalent to:
        # cmd = "curl -i -k -u %s:%s -X PUT 'https://%s:%s/gateway/sandbox/webhdfs/v1/%s?op=MKDIRS'" % \
        #      (self.username, self.pwd, self.gatewayIp, self.gatewayPort, hdfs_path)

        mkdir_url = "%s%s%s?op=MKDIRS" % (self.root_url, WEBHDFS_CONTEXT_ROOT, hdfs_path)
        request_handle = self.__makeWebHdfsRequest(mkdir_url, "PUT")

        if not request_handle:
            logger.info("Request failed. %s", mkdir_url)
            return None
        logger.debug("Request status: %d", request_handle.getcode())
        if request_handle.getcode() != 200:
            logger.info("Error code obtained while executing mkdir: %s", request_handle.getcode())
            return None
        else:
            data = request_handle.read()
            logger.debug("Mkdir output: %s", data)
            return data

    def createFile(self, hdfs_path, local_file_path):
        """
    Places the file in path in dir using webhdfs by connecting to ip:port.
    :param hdfs_path: Filepath on hdfs.
    :param local_file_path: Path to file on local machine.
    :return: Create file output or None.
    """
        # This method executes a request equivalent to:
        # cmd = "curl -i -k -u %s:%s -X PUT -L
        #           'https://%s:%s/gateway/sandbox/webhdfs/v1/%s?op=CREATE&overwrite=true' -T %s" % \
        #      (self.username, self.pwd, self.gatewayIp, self.gatewayPort, hdfs_path, local_file_path)

        create_file_url = "%s%s%s?op=CREATE&overwrite=true" % (self.root_url, WEBHDFS_CONTEXT_ROOT, hdfs_path)
        request_handle = self.__makeWebHdfsRequest(create_file_url, "PUT")

        if not request_handle:
            logger.info("Request failed. %s", create_file_url)
            return None
        logger.debug("Request status: %d", request_handle.getcode())
        # We are expecting a redirect with 307 code here
        if request_handle.getcode() != 307:
            logger.info("Error code obtained while executing createFile: %s", request_handle.getcode())
            return None
        else:
            # Get the redirected url and create a request with it and post it
            new_url = request_handle.info().getheader('Location')
            headers = {'Content-Type': 'application/octet-stream'}
            new_request_handle = None
            with open(local_file_path, "rb") as fp:
                new_request_handle = self.__makeWebHdfsRequest(new_url, "PUT", fp.read(), headers)

            if not new_request_handle:
                logger.info("Request failed. %s", new_url)
                return None

            if new_request_handle.getcode() != 201:
                logger.info("Error code of createFile: %s", new_request_handle.getcode())
                return None
            else:
                data = new_request_handle.read()
                logger.debug("CreateFile output: %s", data)
                return data

    def rename(self, hdfs_path, destination_path):
        """
    Renames a path to target path on webhdfs.
    :param hdfs_path: Src path on hdfs.
    :param destination_path: Dest path on hdfs.
    :return: Rename output.
    """
        # This method executes a request equivalent to:
        # cmd = "curl -i -k -u %s:%s -X PUT
        #           'https://%s:%s/gateway/sandbox/webhdfs/v1/%s??op=RENAME&destination=%s'" % \
        #      (self.username, self.pwd, self.gatewayIp, self.gatewayPort, hdfs_path, destination_path)

        renamed_url = "%s%s%s?op=RENAME&destination=%s" % (
            self.root_url, WEBHDFS_CONTEXT_ROOT, hdfs_path, destination_path
        )
        request_handle = self.__makeWebHdfsRequest(renamed_url, "PUT")

        if not request_handle:
            logger.info("Request failed. %s", renamed_url)
            return None
        logger.debug("Request status: %d", request_handle.getcode())
        if request_handle.getcode() != 200:
            logger.info("Error code obtained while executing rename: %s", request_handle.getcode())
            return None
        else:
            data = request_handle.read()
            logger.debug("Rename output: %s", data)
            return data

    def readFile(self, hdfs_path):
        """
    Reads the file on hdfs and returns the data.
    :param hdfs_path: File path on hdfs.
    :return: Data in the file or None.
    """
        # This method executes a request equivalent to:
        # cmd = "curl -i -k -u %s:%s -X GET 'https://%s:%s/gateway/sandbox/webhdfs/v1/%s?op=OPEN'" % \
        #      (self.username, self.pwd, self.gatewayIp, self.gatewayPort, hdfs_path)

        read_file_url = "%s%s%s?op=OPEN" % (self.root_url, WEBHDFS_CONTEXT_ROOT, hdfs_path)
        request_handle = self.__makeWebHdfsRequest(read_file_url, "GET")

        if not request_handle:
            logger.info("Request failed. %s", read_file_url)
            return None
        logger.debug("Request status: %d", request_handle.getcode())
        # We are expecting a redirect with 307 code here
        if request_handle.getcode() != 307:
            logger.info("Error code obtained while executing ReadFile: %s", request_handle.getcode())
            return None
        else:
            # Get the redirected url and create a request with it and post it
            new_url = request_handle.info().getheader('Location')
            new_request_handle = self.__makeWebHdfsRequest(new_url, "GET")

            if not new_request_handle:
                logger.info("Request failed. %s", new_url)
                return None
            logger.debug("Request status: %d", request_handle.getcode())
            if new_request_handle.getcode() != 200:
                logger.info("Error code of ReadFile: %s", new_request_handle.getcode())
                return None
            else:
                data = new_request_handle.read()
                logger.debug("ReadFile output: %s", data)
                return data


class CustomRedirectHandler(urllib2.HTTPRedirectHandler):
    """
  Custom HTTP Redirection Handler to prevent redirection of 30x error codes.

  Note:
  If the server were to redirect the request using various 300 codes, we need
  to prevent the redirection. Because, according to webhdfs spec, the redirection
  URL needs to be handled separately through a GET/PUT request. Hence, we prevent the
  redirect by overriding the http_error_30x methods.
  """

    def __init__(self):
        pass

    def http_error_307(self, req, fp, code, msg, headers):  # pylint: disable=no-self-use,unused-argument
        """
    Method to prevent 307 code redirection.
    :param: All the params are passed into this method by the urllib2 module.
    :return: A new url with the extracted information from the redirect message.
    """
        infourl = urllib.addinfourl(fp, headers, req.get_full_url())
        infourl.status = code
        infourl.code = code
        return infourl

    # Ensure 300, 301, 302, 303 codes are handled similar to 307.
    http_error_300 = http_error_301 = http_error_302 = http_error_303 = http_error_307
