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
from beaver.component.hadoop import HDFS, Hadoop
from beaver.config import Config
from beaver.machine import Machine

AWS_ACCESS_KEY = Config.get('machine', 'S3_AWS_ACCESS_KEY')
AWS_SECRET = Config.get('machine', 'S3_AWS_SECRET')

logger = logging.getLogger(__name__)
job_user = Config.getEnv('USER')
hdfs_user = HDFS.getHDFSUser()


class BeaconHDFS:
    def __init__(self):
        pass

    @classmethod
    def verifyHelper(
            cls, source_stdout, target_stdout, type="HDFS", wasbLocation=None, skipPath=False, skipPermission=False
    ):
        source_no_of_lines = len(source_stdout.splitlines())
        target_no_of_lines = len(target_stdout.splitlines())

        if type is "HDFS":
            logger.info("HDFS type. Comparing the Number of source and target file structures")
            logger.info("No of files in Source:" + str(source_no_of_lines))
            logger.info("No of files in Target:" + str(target_no_of_lines))
            assert source_no_of_lines == target_no_of_lines
            for each_line in range(source_no_of_lines):
                # Compare the file name. It also ensures file is present in the target
                source_file_name = source_stdout.splitlines()[each_line].split('//')[1].split('user')[1]
                source_file_size = source_stdout.splitlines()[each_line].rsplit(' ', 4)[1]
                source_file_permission = source_stdout.splitlines()[each_line].split(' ', 1)[0]
                source_file_owner = source_stdout.splitlines()[each_line].split(' ')[4]
                source_file_group = source_stdout.splitlines()[each_line].split(' ')[5]

                target_file_name = target_stdout.splitlines()[each_line].split('//')[1].split('user')[1]
                target_file_size = target_stdout.splitlines()[each_line].rsplit(' ', 4)[1]
                target_file_permission = target_stdout.splitlines()[each_line].split(' ', 1)[0]
                target_file_owner = target_stdout.splitlines()[each_line].split(' ')[4]
                target_file_group = target_stdout.splitlines()[each_line].split(' ')[5]

                # Compare the file name. It also ensures file is present in the target
                if skipPath is True:
                    logger.info("Source File Name:" + source_file_name.split('/')[-1])
                    logger.info("Target File Name:" + target_file_name.split('/')[-1])
                    assert source_file_name.split('/')[-1] == target_file_name.split('/')[-1]
                else:
                    logger.info("Source File Name:" + source_file_name)
                    logger.info("Target File Name:" + target_file_name)
                    assert source_file_name == target_file_name

                # Compare the file size
                logger.info("Source File Size:" + source_file_size)
                logger.info("Target File Size:" + target_file_size)
                assert source_file_size == target_file_size

                # Compare the file permission
                if skipPermission is False:
                    logger.info("Source File Permission:" + source_file_permission)
                    logger.info("Target File Permission:" + target_file_permission)
                    assert source_file_permission == target_file_permission

                # Compare the owner
                logger.info("Source File Owner:" + source_file_owner)
                logger.info("Target File Owner:" + target_file_owner)
                assert source_file_owner == target_file_owner

                # Compare the group
                logger.info("Source File Group:" + source_file_group)
                logger.info("Target File group:" + target_file_group)
                assert source_file_group == target_file_group

                each_line = each_line + 1
        elif type is "S3":
            logger.info("S3 type. Comparing the Number of source and target file structures")
            logger.info("No of files in Source:" + str(source_no_of_lines))
            logger.info("No of files in Target:" + str(target_no_of_lines))
            assert source_no_of_lines == target_no_of_lines
            for each_line in range(source_no_of_lines):
                # Compare the file name. It also ensures file is present in the target
                source_file_name = source_stdout.splitlines()[each_line].split('8020')[-1]
                source_file_size = source_stdout.splitlines()[each_line].rsplit(' ', 4)[1]

                target_file_name = target_stdout.splitlines()[each_line + 1].split(' ')[0]
                target_file_size = target_stdout.splitlines()[each_line + 1].split(' ')[1]

                # Compare the file name. It also ensures file is present in the target
                logger.info("Source File Name:" + source_file_name)
                logger.info("Target File Name:" + target_file_name)
                if target_file_name.endswith('/'):
                    target_file_name = target_file_name[:-1]
                assert source_file_name == target_file_name

                # Compare the file size
                logger.info("Source File Size:" + source_file_size)
                logger.info("Target File Size:" + target_file_size)
                assert source_file_size == target_file_size

                each_line = each_line + 1
        elif type is "WASB":
            logger.info("WASB type. Comparing the Number of source and target file structures")
            logger.info("No of files in Source:" + str(source_no_of_lines))
            logger.info("No of files in Target:" + str(target_no_of_lines))
            assert source_no_of_lines == target_no_of_lines - 6
            for each_line in range(source_no_of_lines):
                # Compare the file name. It also ensures file is present in the target
                source_file_name = source_stdout.splitlines()[each_line].split('8020')[-1]
                source_file_size = source_stdout.splitlines()[each_line].rsplit(' ', 4)[1]
                source_file_permission = source_stdout.splitlines()[each_line].split(' ', 1)[0]

                logger.info("line is %s" % target_stdout.splitlines()[each_line + 3])
                logger.info("wasbLocation is %s" % wasbLocation)
                target_file_name = target_stdout.splitlines()[each_line + 3].split(wasbLocation)[1]
                target_file_size = target_stdout.splitlines()[each_line + 3].rsplit(' ', 4)[1]
                target_file_permission = target_stdout.splitlines()[each_line + 3].split(' ', 1)[0]

                # Compare the file name. It also ensures file is present in the target
                logger.info("Source File Name:" + source_file_name)
                logger.info("Target File Name:" + target_file_name)
                assert source_file_name == target_file_name

                # Compare the file size
                logger.info("Source File Size:" + source_file_size)
                logger.info("Target File Size:" + target_file_size)
                assert source_file_size == target_file_size

                # Compare the file permission
                logger.info("Source File Permission:" + source_file_permission)
                logger.info("Target File Permission:" + target_file_permission)
                assert source_file_permission == target_file_permission

                each_line = each_line + 1

    @classmethod
    def verifyHDFSReplication(
            cls, source_endpoint, target_endpoint, source_dir, target_dir, skipPath=False, skipPermission=False
    ):
        """

        :param source_endpoint:
        :param target_endpoint:
        :param source_dir:
        :param target_dir:
        :return:
        """

        # List the hdfs files in the source directory
        source_location = source_endpoint + source_dir
        logger.info("source_location is %s" % source_location)

        target_location = target_endpoint + target_dir
        logger.info("target_location is %s" % target_location)

        exit_code, source_stdout = HDFS.runas(job_user, "dfs -%s %s" % (HDFS.lsrCmd(), source_location))
        logger.info("Source Folder Content")
        logger.info(source_stdout)
        assert exit_code == 0, source_stdout

        if target_endpoint.startswith('wasb'):
            logger.info("Target is wasb")
            exit_code, target_stdout = HDFS.runas(
                job_user, "dfs -%s %s" % (HDFS.lsrCmd(), target_endpoint + "/" + target_dir)
            )
            logger.info("Target Folder Content")
            logger.info(target_stdout)
            cls.verifyHelper(
                source_stdout,
                target_stdout,
                type="WASB",
                wasbLocation=target_endpoint,
                skipPath=skipPath,
                skipPermission=skipPermission
            )

        elif target_endpoint.startswith('hdfs'):
            logger.info("Target is hdfs")
            exit_code, target_stdout = HDFS.runas(
                job_user, "dfs -%s %s" % (HDFS.lsrCmd(), target_endpoint + "/" + target_dir)
            )
            logger.info("Target Folder Content")
            logger.info(target_stdout)
            assert exit_code == 0, target_stdout
            cls.verifyHelper(source_stdout, target_stdout, skipPath=skipPath, skipPermission=skipPermission)
        elif target_endpoint.startswith('s3'):
            logger.info("Target is s3")
        else:
            logger.info("target endpoint is not supported")

    @classmethod
    def verifyHDFSSnapshotReplication(cls, source_endpoint, target_endpoint, source_dir, target_dir):
        """

        :param source_endpoint:
        :param target_endpoint:
        :param source_dir:
        :param target_dir:
        :return:
        """

        cls.verifyHDFSReplication(source_endpoint, target_endpoint, source_dir, target_dir)

        # Compare the file name. It also ensures file is present in the target
        source_snapshot_names = cls.getSnapshotList(source_endpoint, source_dir)

        target_snapshot_names = cls.getSnapshotList(target_endpoint, target_dir)

        # Compare the file name. It also ensures file is present in the target
        logger.info("List of source snapshot names:%s" % source_snapshot_names)
        logger.info("List of target snapshot names:%s" % target_snapshot_names)
        assert cmp(source_snapshot_names, target_snapshot_names) == 0

    @classmethod
    def getSnapshotList(cls, endpoint, directory):
        # This is to verify the snapshots are replicated properly
        # List the snapshot files in the directory
        snapshot_names = []
        exit_code, stdout = HDFS.runas(job_user, "dfs -ls %s" % (endpoint + "/" + directory + "/" + '.snapshot'))
        logger.info("Snapshots Stdout:")
        logger.info(stdout)
        #assert exit_code == 0, stdout
        count = 0
        for snapshot_name in stdout.splitlines():
            if count == 0:
                count = count + 1
                continue
            else:
                logger.info("Inserting Snapshot Name:")
                logger.info(snapshot_name.rsplit(' ', 1)[1].rsplit('/', 1)[-1])
                snapshot_names.append(snapshot_name.rsplit(' ', 1)[1].rsplit('/', 1)[-1])

        logger.info(snapshot_names)
        return snapshot_names

    @classmethod
    def deleteHDFSSnapshots(cls, endpoint, directory, _beacon_host=None):
        if HDFS.isHAEnabled() and _beacon_host != None:
            snapshotendpoint = "hdfs://" + HDFS.getActiveNN(_beacon_host, "NAMENODE")

            snapshot_names = BeaconHDFS.getSnapshotList(snapshotendpoint, directory)
            for snapshot_name in snapshot_names:
                exit_code, stdout = HDFS.deleteSnapshot(snapshotendpoint + directory, snapshot_name, user=hdfs_user)
            #assert exit_code == 0
        else:
            snapshot_names = BeaconHDFS.getSnapshotList(endpoint, directory)
            for snapshot_name in snapshot_names:
                exit_code, stdout = HDFS.deleteSnapshot(endpoint + directory, snapshot_name, user=hdfs_user)

        if _beacon_host is None:
            exit_code, stdout = HDFS.disallowSnapshot(endpoint + directory, user=hdfs_user)
            assert exit_code == 0
        else:
            disallowsnapshot = " hdfs dfsadmin -disallowSnapshot " + endpoint + directory
            if Hadoop.isSecure():
                hdfsuser = Config.get("hadoop", 'HDFS_USER')
                kinitCmd = "kinit -k -t " + Machine.getHeadlessUserKeytab(
                    hdfsuser
                ) + " " + hdfsuser + "@" + Machine.get_user_realm()
                exit_code, stdout = Machine.runas(user=hdfsuser, cmd=kinitCmd, host=_beacon_host)
            exit_code, stdout = Machine.runas(user=hdfs_user, cmd=disallowsnapshot, host=_beacon_host)
            assert exit_code == 0
