#
#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
import logging
import os
import time

from beaver import util
from beaver.component.hadoop import Hadoop
from beaver.config import Config
from beaver.machine import Machine

logger = logging.getLogger(__name__)
GATEWAY_NODE = Machine.getfqdn()
ADMIN_USER = Machine.getAdminUser()
ADMIN_PWD = Machine.getAdminPasswd()
ARTIFACTS_DIR = Config.getEnv("ARTIFACTS_DIR")


class AWS(object):
    _setup = False

    def __init__(self):
        pass

    @classmethod
    def setup(cls, S3_AWS_ACCESS_KEY=None, S3_AWS_SECRET=None):
        Machine.installPackageWithPip(packages="awscli", hosts=Hadoop.getAllNodes(), logoutput=True)
        aws_home = "/root/.aws"
        if not os.path.exists(aws_home):
            Machine.makedirs(ADMIN_USER, GATEWAY_NODE, "/root/.aws", ADMIN_PWD)
            util.writeToFile("[default]\nregion = us-west-2\noutput=json", os.path.join(ARTIFACTS_DIR, "config"))
            if S3_AWS_ACCESS_KEY:
                cls._aws_access_key_id = S3_AWS_ACCESS_KEY
            else:
                cls._aws_access_key_id = Config.get('machine', 'S3_AWS_ACCESS_KEY')
            if S3_AWS_SECRET:
                cls._aws_secret_access_key = S3_AWS_SECRET
            else:
                cls._aws_secret_access_key = Config.get('machine', 'S3_AWS_SECRET')
            util.writeToFile(
                "[default]\naws_access_key_id = %s\naws_secret_access_key = %s" %
                (cls._aws_access_key_id, cls._aws_secret_access_key), os.path.join(ARTIFACTS_DIR, "credentials")
            )
            Machine.runas(ADMIN_USER, "chown  %s '%s/config'" % (ADMIN_USER, ARTIFACTS_DIR), GATEWAY_NODE, ADMIN_PWD)
            Machine.runas(
                ADMIN_USER, "chown  %s '%s/credentials'" % (ADMIN_USER, ARTIFACTS_DIR), GATEWAY_NODE, ADMIN_PWD
            )
            Machine.copy(os.path.join(ARTIFACTS_DIR, "config"), aws_home, ADMIN_USER, ADMIN_PWD)
            Machine.copy(os.path.join(ARTIFACTS_DIR, "credentials"), aws_home, ADMIN_USER, ADMIN_PWD)

    @classmethod
    def run(cls, cmd):
        run_cmd = "aws s3 " + cmd
        return Machine.runas(user=Machine.getAdminUser(), cmd=run_cmd)

    @classmethod
    def createBucket(cls, bucket_name):
        logger.info("Creating S3 Bucket %s", bucket_name)
        return cls.run("mb s3://" + bucket_name)

    @classmethod
    def removeBucket(cls, bucket_name):
        logger.info("Deleting S3 Bucket %s", bucket_name)
        return cls.run("rb s3://%s --force " % bucket_name)

    @classmethod
    def copy(cls, srcpath, destpath, recursive=False):
        cmd = "cp %s %s" % (srcpath, destpath)
        if recursive:
            cmd = cmd + " --recursive"
        return cls.run(cmd)

    @classmethod
    def delete(cls, filepath, recursive=True):
        cmd = "rm " + filepath
        if recursive:
            cmd = cmd + " --recursive"
        return cls.run(cmd)

    @classmethod
    def generateBucketName(cls, test):
        return "%s-%s" % (test, util.getRandomAlphaNumeric())

    @classmethod
    def verifyBucketExists(cls, bucket):
        exit_code, _stdout = cls.run(("ls %s" % bucket))
        assert exit_code == 0, "Failed to list S3 bucket %s : %d" % (bucket, exit_code)

    @classmethod
    def waitForBucket(cls, bucket, sleeptime=30):
        exit_code, _stdout = cls.run(("ls %s" % bucket))
        if exit_code == 0:
            return
        else:
            logger.info("Wait %d seconds for S3 bucket creation: %s", sleeptime, bucket)
            time.sleep(sleeptime)
            cls.verifyBucketExists(bucket)
