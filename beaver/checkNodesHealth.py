import logging
import re

from beaver.machine import Machine
from beaver.component.hadoop import HDFS

logger = logging.getLogger(__name__)

nn = HDFS.getNamenode()
logger.info("NN = %s", nn)

#list of DNs
dns = HDFS.getDatanodes()
logger.info("DNs = %s", dns)

nodes = [nn]
for dn in dns:
    if dn not in nodes:
        nodes.append(dn)

paths = ["/grid/0", "/grid/1", "/grid/2", "/grid/3", "/grid/4", "/grid/5", ""]
logger.info("nodes=%s", nodes)
logger.info("paths=%s", paths)
suspiciousNodes = []
for node in nodes:
    logger.info("node=%s", node)
    for path in paths:
        fname = path + "/tmp/touchtest"
        #logger.info("testing %s at %s" % (fname, node))
        (exit_code, stdout) = Machine.touchRemote(node, fname)
        if re.search("Read-only file system", stdout) != None:
            #we can't detect the case when it exits but is very slow yet.
            #exit code is false positive.
            #if exit_code != 0:
            if node not in suspiciousNodes:
                suspiciousNodes.append(node)
        Machine.rm(None, node, fname)

if not suspiciousNodes:
    logger.info("All nodes are good.")
else:
    logger.info("Suspicious nodes are:")
    for index, item in enumerate(suspiciousNodes):
        logger.info("%s. %s", index + 1, item)
