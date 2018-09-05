import time, logging, re, os
from beaver.component.hadoop import Hadoop, MAPRED, YARN, HDFS, MAPRED2
from beaver.component.tez import Tez
from ..machine import Machine
from ..config import Config
from ..java import Java
from beaver.htmlparser import MRHTMLParser

from beaver import util

logger = logging.getLogger(__name__)

HDFS_USER = HDFS.getHDFSUser()


def balancerModifyConfig(duReservedValue):
    """
    Create /tmp/hadoopConf in all nodes
    :param duReservedValue:
    :return:
    """
    Hadoop.modifyConfig(
        {
            'hdfs-site.xml': {
                'dfs.datanode.du.reserved': duReservedValue,
                'dfs.replication': 1,
                'dfs.namenode.heartbeat.recheck-interval': 5000,
                'dfs.namenode.stale.datanode.interval': 1000,
                'dfs.namenode.replication.interval': 1
            },
        }, {'services': ['all']}
    )


def findOverUtilized(s, logoutput=True):
    """
    Find over-utilized disks from balancer output
    :param s: balancer output
    :param logoutput:
    :return: list of str e.g. ['172.22.72.50:1019:DISK']
    """
    p = 'over-utilized: [\[](.*?)[\]]'
    o = re.search(p, s, re.MULTILINE)
    if logoutput:
        logger.info(o)
    matchedStr = o.group(1)
    if logoutput:
        logger.info(matchedStr)
    tmpResult = matchedStr.split(",")
    result = []
    for e in tmpResult:
        result.append(e.strip())
    if logoutput:
        logger.info("findOverUtilized returns %s" % result)
    return result


def findUnderUtilized(s, logoutput=True):
    """
    Find under-utilized disks from balancer output
    :param s: balancer output
    :param logoutput:
    :return: list of str e.g. ['172.22.72.51:1019:DISK']
    """
    p = 'underutilized: [\[](.*?)[\]]'
    o = re.search(p, s, re.MULTILINE)
    if logoutput:
        logger.info(o)
    matchedStr = o.group(1)
    if logoutput:
        logger.info(matchedStr)
    tmpResult = matchedStr.split(",")
    result = []
    for e in tmpResult:
        result.append(e.strip())
    if logoutput:
        logger.info("findUnderUtilized returns %s" % result)
    return result


def getBalancerBlockMoves(s, logoutput=True):
    """
    Find moving block metadata from balancer output
    :param s: balancer stdout
    :param logoutput:
    :return: list of list of str
    """
    #Start moving blk_1073741907_1083 with size=134217728 from 172.22.72.50:1019:DISK to 172.22.72.51:1019:DISK through 172.22.72.50:1019
    p = 'Start moving (.*?) with size=(\d+?) from (.*?) to (.*?) through (.*?:\d+)'
    o = re.findall(p, s, re.MULTILINE)
    if logoutput:
        logger.info(o)
        logger.info("len(o) = %s" % len(o))
    result = []
    for groupNo in range(0, len(o)):
        e = o[groupNo]
        result.append(e)
    if logoutput:
        logger.info("getBalancerBlockMoves returns")
        for i in range(len(result)):
            logger.info("%s. %s" % (i + 1, result[i]))
    return result


def formatNN_SetupHDFS(duReservedValue, mod_conf_path):
    """
    Format NN. Setup HDFS dir for MR jobs.

    Note that this permission is too wide for default HDP use.
    """
    datanodes = HDFS.getDatanodes()
    logger.info("datanodes = %s" % datanodes)
    HDFS.stopDatanodes()
    HDFS.stopNamenode()
    HDFS.formatNN(force=True, logoutput=True)

    for dn in datanodes:
        Machine.rm(
            user=Machine.getAdminUser(),
            host=dn,
            filepath="%s/current" % HDFS.getConfigValue("dfs.datanode.data.dir"),
            isdir=True
        )

    balancerModifyConfig(duReservedValue)
    HDFS.startNamenode(mod_conf_path)
    HDFS.startDatanodes(mod_conf_path)
    sleepTime = 45
    logger.info("sleep for %s sec" % sleepTime)
    time.sleep(sleepTime)

    version = Hadoop.getShortVersion()
    paths = ["/hdp", "/hdp/apps", "/hdp/apps/%s" % version, "/hdp/apps/%s/mapreduce" % version]
    for path in paths:
        HDFS.mkdir(path=path, user=HDFS_USER)
    HDFS.chmod(runasUser=HDFS_USER, perm="777", directory="/hdp", recursive=True)
    HDFS.copyFromLocal(
        localpath="/usr/hdp/current/hadoop-client/mapreduce.tar.gz", hdfspath="/hdp/apps/%s/mapreduce/" % version
    )
    sleepTime = 45
    logger.info("sleep for %s sec for MR tarball replication" % sleepTime)
    time.sleep(sleepTime)
    paths = ["/app-logs", "/app-logs/hrt_qa", "/app-logs/hrt_qa/logs", "/mr-history"]
    for path in paths:
        HDFS.mkdir(path=path, user=HDFS_USER)
    HDFS.chmod(runasUser=HDFS_USER, perm="777", directory="/app-logs", recursive=True)
    HDFS.chmod(runasUser=HDFS_USER, perm="777", directory="/mr-history", recursive=True)
    HDFS.mkdir(path="/user", user=HDFS_USER)
    HDFS.mkdir(path="/user/hrt_qa", user=HDFS_USER)
    HDFS.chown(runasUser=HDFS_USER, new_owner="hrt_qa:hrt_qa", directory="/user/hrt_qa", recursive=False)
    HDFS.chmod(runasUser="hrt_qa", perm="770", directory="/user/hrt_qa", recursive=True)

    # smoke test
    #Hadoop.run(cmd='jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar randomtextwriter "-Ddfs.replication=1" "-Dmapreduce.randomtextwriter.totalbytes=41400" /user/hrt_qa/RTW')
    #HDFS.deleteDirectory(directory="/user/hrt_qa/RTW", skipTrash=True)
