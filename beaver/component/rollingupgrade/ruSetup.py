import json, logging, os
from beaver import util
from beaver.config import Config
from beaver.machine import Machine
from pprint import pprint, pformat
from beaver.component.hadoop import Hadoop, HDFS, YARN

logger = logging.getLogger(__name__)


class RuSetup:

    CONF_DRYRUN = "dryrun"
    CONF_COMPONENTS_TEST = "components_to_test"
    CONF_COMPONENTS_FLIP = "components_to_flip"
    CONF_STANDALONE = "standalone_test"
    CONF_SKIP_QUEUE = "skip_queue"
    CONF_DEFAULT_QUEUE = "default_queue"

    _skipQueue = None
    _defaultQueue = True

    @classmethod
    def readJson(cls, fileName):
        '''
        Read a Json file for RU
        '''
        jsonFile = open(fileName)
        jobData = json.load(jsonFile)
        jsonFile.close()
        return jobData

    @classmethod
    def __reverseEdges(cls, adjDic):
        '''
        Reverse all edges in a graph in its adjacency list form
        '''
        reversedDic = {}
        for key, adjList in adjDic.iteritems():
            for value in adjList:
                if value in reversedDic:
                    reversedDic[value].append(key)
                else:
                    reversedDic[value] = [key]
        return reversedDic

    @classmethod
    def __getComponents(cls, compFile, depFile):
        '''
        Get depended components for the components requested in compFile (comps) according to depFile
        The function returns the components that depend on comps
        '''
        # read in the config file
        conf = RuSetup.readJson(compFile)
        # read in the dependency file
        dependencies = RuSetup.readJson(depFile)
        dependencies = RuSetup.__reverseEdges(dependencies)
        # get the required components
        components = set(conf[RuSetup.CONF_COMPONENTS_TEST])
        # add all depended components
        finalComponents = set()
        while len(components) > 0:
            # get the current component
            comp = components.pop()
            # flip that componet to final comps
            finalComponents.add(comp)
            # get dependency
            if comp not in dependencies:
                continue
            deps = dependencies[comp]
            # add depended components to the current set for augmenting
            for dep in deps:
                if dep not in finalComponents:
                    components.add(dep)
        return list(finalComponents)

    @classmethod
    def getComponentsToFlip(cls, compFile, depFile):
        '''
        Get the components that should be upgraded during RU according to depFile
        '''
        # read in the config file
        conf = RuSetup.readJson(compFile)
        # get the components to flip
        returnSet = set(conf[RuSetup.CONF_COMPONENTS_FLIP])
        # skip tests according to cluster settings
        if not HDFS.isHAEnabled():
            logger.info("Skip flipping HDFS since HA is not enabled")
            returnSet.discard("hdfs")
        return list(returnSet)

    @classmethod
    def getComponnetsToTest(cls, compFile, depFile):
        '''
        Get the components that are being tested according to depFile
        '''
        # read in the config file
        conf = RuSetup.readJson(compFile)
        isStandalone = conf[RuSetup.CONF_STANDALONE]
        RuSetup._skipQueue = set(conf[RuSetup.CONF_SKIP_QUEUE])
        RuSetup._defaultQueue = conf[RuSetup.CONF_DEFAULT_QUEUE]
        returnSet = None
        if isStandalone:
            # get the components to test
            returnSet = set(conf[RuSetup.CONF_COMPONENTS_TEST])
        else:
            returnSet = set(RuSetup.getComponentsAffected(compFile, depFile))

        # skip tests according to cluster settings
        if not HDFS.isHAEnabled():
            logger.info("Skip HDFS since HA is not enabled")
            returnSet.discard("hdfs")

        # as discussed in Ru standup for 11/13, enabling storm-slider for non HA cluster and storm standalone for HA cluster
        if YARN.isHAEnabled():
            returnSet.discard("storm-slider")
        else:
            returnSet.discard("storm")

        if Hadoop.isEncrypted():
            returnSet.discard("knox")
            returnSet.discard("falcon")

        if Hadoop.isTez():
            logger.info("Add tez since Hadoop.isTez()")
            returnSet.add("tez")
        else:
            logger.info("Make sure tez is not in the list since Hadoop.isTez() is false")
            returnSet.discard("tez")
        # Note: component.xa is always available, even if xa is not installed
        # So this line should work even if the cluster does not have xa installed
        from beaver.component.xa import Xa
        if Xa.isArgusInstalled():
            logger.info("Add argus since argus is there")
            returnSet.add("argus")
        else:
            logger.info("Make sure argus is not in the list since it's not available")
            returnSet.discard("argus")

        return list(returnSet)

    @classmethod
    def getComponentsAffected(cls, compFile, depFile):
        '''
        Get the affected components according to depFile
        '''
        return RuSetup.__getComponents(compFile, depFile)

    @classmethod
    def isDryrun(cls, jobData):
        return jobData[RuSetup.CONF_DRYRUN]

    @classmethod
    def setup_capacity_scheduler(cls, components):
        """
        Setup yarn capacity scheduler based on components.
        This API is not called during setup_module.
        :param components: list of components
        :type components: list of str
        :return: None
        """
        if RuSetup._defaultQueue:
            components.append("default")

        logger.info("*** setup_capacity_scheduler ***")
        if RuSetup._skipQueue != None:
            logger.info("Components do not have a queue: " + str(RuSetup._skipQueue))
            components = list(set(components) - RuSetup._skipQueue)
        logger.info("components = %s" % components)
        numComponents = len(components)
        percentPerQueue = 100.0 / numComponents
        percentPerQueueStr = "{0:0.2f}".format(percentPerQueue)
        xmlDict = {}
        rootQueues = ",".join(components)
        xmlDict["yarn.scheduler.capacity.root.queues"] = rootQueues
        for component in components:
            xmlDict["yarn.scheduler.capacity.root.%s.capacity" % component] = percentPerQueueStr
            xmlDict["yarn.scheduler.capacity.root.%s.user-limit-factor" % component] = 1
            xmlDict["yarn.scheduler.capacity.root.%s.maximum-capacity" % component] = percentPerQueueStr
            xmlDict["yarn.scheduler.capacity.root.%s.state" % component] = "RUNNING"
            xmlDict["yarn.scheduler.capacity.root.%s.acl_submit_jobs" % component] = "*"
            xmlDict["yarn.scheduler.capacity.root.%s.acl_administer_jobs" % component] = "*"
        util.dumpTextString(xmlDict, "====== PLANNED QUEUES ======", "==================")
        master_capacity_file = os.path.join(
            Config.getEnv("WORKSPACE"), "tests", "rolling_upgrade", "yarn", "data", "capacity-scheduler.xml"
        )
        modified_capacity_file = os.path.join(Config.getEnv("ARTIFACTS_DIR"), "capacity-scheduler.xml")
        Machine.copy(master_capacity_file, modified_capacity_file)
        util.writePropertiesToConfigXMLFile(modified_capacity_file, modified_capacity_file, xmlDict)
        #util.dumpText(modified_capacity_file, "====== capacity-scheduler.xml ======", "==================")
        if RuSetup._defaultQueue:
            components.remove("default")
        return modified_capacity_file


__localTestDir = os.path.join(Config.getEnv("WORKSPACE"), "tests", "rolling_upgrade")
__confFile = __localTestDir + '/conf.json'
__depFile = __localTestDir + '/dep.json'
# Public: all components to run the test
COMPONENTS_TO_TEST = RuSetup.getComponnetsToTest(__confFile, __depFile)
# Public: all components to flip binary
COMPONENTS_TO_FLIP = RuSetup.getComponentsToFlip(__confFile, __depFile)
# Public: components that are affected by the tested components
COMPONENTS_AFFECTED = RuSetup.getComponentsAffected(__confFile, __depFile)
# Public: components that we should import
COMPONENTS_TO_IMPORT = list(set(COMPONENTS_TO_TEST) | set(COMPONENTS_TO_FLIP))
