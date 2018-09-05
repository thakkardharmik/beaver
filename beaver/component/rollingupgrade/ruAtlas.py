#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging, sys
from beaver.component.atlas_resources.atlas import Atlas
import random

logger = logging.getLogger(__name__)


class ruAtlas:
    type = "TypeForRUTest_" + str(random.randint(1, sys.maxint))

    type_request_payload = "{\"enumTypes\":[],\"structTypes\":[],\"traitTypes\":[],\"classTypes\":[{\"superTypes\":[],\"hierarchicalMetaTypeName\":\"org.apache.atlas.typesystem.types.ClassType\",\"typeName\":\"%s\",\"typeDescription\":null,\"attributeDefinitions\":[{\"name\":\"name\",\"dataTypeName\":\"string\",\"multiplicity\":\"required\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"optional\",\"dataTypeName\":\"string\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"collection\",\"dataTypeName\":\"array<string>\",\"multiplicity\":\"collection\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"set\",\"dataTypeName\":\"array<string>\",\"multiplicity\":\"set\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"bigdecimal\",\"dataTypeName\":\"bigdecimal\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"biginteger\",\"dataTypeName\":\"biginteger\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"boolean\",\"dataTypeName\":\"boolean\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"byte\",\"dataTypeName\":\"byte\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"date\",\"dataTypeName\":\"date\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"double\",\"dataTypeName\":\"double\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"float\",\"dataTypeName\":\"float\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"int\",\"dataTypeName\":\"int\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"long\",\"dataTypeName\":\"long\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null},{\"name\":\"short\",\"dataTypeName\":\"short\",\"multiplicity\":\"optional\",\"isComposite\":false,\"isUnique\":false,\"isIndexable\":true,\"reverseAttributeName\":null}]}]}" % type
    entity_create_request_payload = "{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Reference\",\"id\":{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\"id\":\"-9421917263126\",\"version\":0,\"typeName\":\"%s\",\"state\":\"ACTIVE\"},\"typeName\":\"%s\",\"values\":{\"name\":\"createEntityausjzpsvwe\",\"collection\":[\"collectionValue1\",\"collectionValue2\"],\"float\":\"5.5\",\"short\":\"9\",\"set\":[\"setValue1\",\"setValue2\"],\"double\":\"4.5\",\"long\":\"7\",\"biginteger\":\"2\",\"boolean\":\"false\",\"date\":\"2016-05-10T04:50:42.541Z\",\"bigdecimal\":\"1.4\",\"int\":\"6\",\"byte\":\"3\"},\"traitNames\":[],\"traits\":{}}" % (
        type, type
    )
    guid_list = []

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        UpgradePerNode.reportProgress("[INFO][Atlas][BackgroundJob] Long running test setup for Atlas started")

        if runSmokeTestSetup:
            logger.info("Running Atlas Smoke Test Setup")
            cls.smoke_test_setup()

        UpgradePerNode.reportProgress("[INFO][Atlas][BackgroundJob] Long running test setup for Atlas finished")

    @classmethod
    def smoke_test_setup(cls):
        '''
        Setup required to run Smoke test
        '''
        Atlas.create_type(input_json_string=cls.type_request_payload)

    @classmethod
    def run_background_job(cls):
        '''
        Runs background long running Yarn Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''
        logger.info("TODO")

    @classmethod
    def run_smoke_test(cls):
        '''
        Run smoke test for yarn
        :param smoketestnumber: Used for unique output log location
        '''

        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode

        host = Atlas.get_host()
        if not host:
            logger.error("No ACTIVE Atlas instance is available")
        else:
            logger.info("Active instance of Atlas is found: " + host)
            response, response_status = Atlas.create_entities(input_json_string=cls.entity_create_request_payload)
            if response_status != 201:
                UpgradePerNode.reportProgress("[FAILED][Atlas][Smoke] Entity creation failed = %d" % response_status)
            else:
                guid = response["definition"]["id"]["id"]
                logger.info("Entity creation was successful. GUID = %s" % guid)
                cls.guid_list.append(guid)

        UpgradePerNode.reportProgress("[INFO][Atlas][Smoke] Smoke test for Atlas component finished")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running Yarn job
        '''
        logger.info("TODO")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
        for guid in cls.guid_list:
            response, response_status = Atlas.get_entities(guid)
            if response_status != 200:
                UpgradePerNode.reportProgress("[FAILED][Atlas][Smoke] Get Entity failed for guid %d" % guid)
            else:
                guid_in_response = response["definition"]["id"]["id"]
                if guid == guid_in_response:
                    logger.info("Get Entity was successful for GUID %s" % guid)
                else:
                    logger.error("Get entity guid error for guid %s. Response - %s" % (guid, response))

    @classmethod
    def upgrade_master(cls, version, config=None):
        '''
        Upgrades Master services:
        :param version: Version to be upgraded to
        :param config: Config location
        '''
        logger.info("TODO")

    @classmethod
    def upgrade_slave(cls, version, node, config=None):
        '''
        Upgrades slave services :
        :param version: Version to be upgraded to
        :param node: Slave Node
        :param config: Config location
        :return:
        '''
        logger.info("TODO")

    @classmethod
    def downgrade_master(cls, version, config=None):
        '''
        Downgrade Master services
        :param version: Version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def downgrade_slave(cls, version, node, config=None):
        '''
        Downgrade slave services
        :param version: version to be downgraded to
        :param config: Configuration location
        '''
        logger.info("TODO")

    @classmethod
    def run_client_smoketest(cls, config=None, env=None):
        '''
        Run Smoke test after upgrading Client
        :param config: Configuration location
        :param env: Set Environment variables
        '''
        logger.info("TODO")

    @classmethod
    def testAfterAllSlavesRestarted(cls):
        '''
        Function to test upgrade is done properly after all master and slaves are upgraded for Hdfs, yarn and Hbase
        :return:
        '''
        logger.info("TODO : need to implement this function")
