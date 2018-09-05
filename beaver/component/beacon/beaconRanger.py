#
#
# Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#

import datetime
import logging
import time

from beaver.component.ambari import Ambari
from beaver.component.xa import Xa, XaPolicy
from beaver.config import Config

logger = logging.getLogger(__name__)
ARTIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')
_ambari_host_1 = Config.get("multicluster", "AMBARI_GATEWAY1")
_ambari_host_2 = Config.get("multicluster", "AMBARI_GATEWAY2")
source_weburl = Ambari.getWebUrl(hostname=_ambari_host_1)
target_weburl = Ambari.getWebUrl(hostname=_ambari_host_2)
source_datacenter = target_datacenter = "default"

primaryCluster = source_datacenter + "$" + Ambari.getClusterName(weburl=source_weburl)
backupCluster = target_datacenter + "$" + Ambari.getClusterName(weburl=target_weburl)
policy_prefix = 'pol_for_'


class BeaconRanger:
    def __init__(self):
        pass

    policyIntervalChanged = False
    policyActivationWaitTime = 30
    policiesAddedBeforeTest = []

    @classmethod
    def changePolicyInterval(cls):
        if not BeaconRanger.policyIntervalChanged:
            interval_for_source_cls = Ambari.getConfig(
                'ranger-hive-security', webURL=source_weburl
            )['ranger.plugin.hive.policy.pollIntervalMs']
            interval_for_target_cls = Ambari.getConfig(
                'ranger-hive-security', webURL=target_weburl
            )['ranger.plugin.hive.policy.pollIntervalMs']
            if not interval_for_source_cls == "5000":
                Xa.changePolicyInterval("HIVE", "5000", webURL=source_weburl)
            if not interval_for_target_cls == "5000":
                Xa.changePolicyInterval("HIVE", "5000", webURL=target_weburl)
            BeaconRanger.policyIntervalChanged = True
            BeaconRanger.policyActivationWaitTime = 6

    @classmethod
    def createRangerHivePolicy(cls, database, udf=None, table=None, column=None, userList=None, weburl=None):
        BeaconRanger.changePolicyInterval()
        logger.info('creating policy for  %s' % database)
        users = [Config.getEnv('USER')] if userList is None else userList
        if table is not None:
            polItem = XaPolicy.createPolicyItem(userList=users, PermissionList=XaPolicy.HIVE_ALL_PERMISSIONS)
            if column is not None:
                policy = XaPolicy.getPolicyJson(
                    policy_prefix + '_' + database + '_table_' + table + '_col_' + column,
                    'hive', [polItem],
                    ambariWeburl=weburl,
                    database=database,
                    table=table,
                    column=column
                )
            else:
                policy = XaPolicy.getPolicyJson(
                    policy_prefix + '_' + database + '_table_' + table,
                    'hive', [polItem],
                    ambariWeburl=weburl,
                    database=database,
                    table=table,
                    column='*'
                )
        elif udf is not None:
            polItem = XaPolicy.createPolicyItem(userList=users, PermissionList=XaPolicy.HIVE_ALL_PERMISSIONS)
            policy = XaPolicy.getPolicyJson(
                policy_prefix + '_' + database + '_udf_' + udf,
                'hive', [polItem],
                ambariWeburl=weburl,
                database=database,
                udf=udf
            )
        else:
            # repladmin check
            polItem = XaPolicy.createPolicyItem(
                userList=users,
                PermissionList=[
                    XaPolicy.PERMISSION_CREATE, XaPolicy.PERMISSION_SELECT, XaPolicy.PERMISSION_DROP,
                    XaPolicy.PERMISSION_REPL_ADMIN
                ]
            )
            policy = XaPolicy.getPolicyJson(
                policy_prefix + '_' + database,
                'hive', [polItem],
                ambariWeburl=weburl,
                database=database,
                table='*',
                column='*'
            )
        logger.info('going to create policy: ' + str(policy))
        status_code, response = Xa.createPolicy_api_v2(
            policy, policyActivationWaitTime=BeaconRanger.policyActivationWaitTime, weburl=weburl
        )

        if status_code == 200 and weburl != target_weburl:
            BeaconRanger.policiesAddedBeforeTest.append(policy)

    @classmethod
    def getRangerConfigs(cls, ambariWeburl=None):
        if Xa.isArgusInstalled() is True:
            logger.info("Ranger is ON")
            Address = Xa.getPolicyAdminAddress(ambariWeburl=ambariWeburl)
            hadoop_repo = \
            Xa.findRepositories(nameRegex="^.*_" + "hadoop" + "$", type="hdfs", status=True, ambariWeburl=ambariWeburl)[
                0]['name']
            hive_repo = \
            Xa.findRepositories(nameRegex="^.*_" + "hive" + "$", type="hive", status=True, ambariWeburl=ambariWeburl)[
                0]['name']
            ranger_config = {"ranger_url": Address, "hadoop_repo": hadoop_repo, "hive_repo": hive_repo}
            return ranger_config
        else:
            return None

    @classmethod
    def deleteRangerPolicyBasedOndDatabase(
            cls, serviceType, db, serviceName=None, weburl=None, deleteOnlyDenyPolicies=False
    ):
        if serviceName is None:
            repos = Xa.findRepositories(
                nameRegex="^.*_" + serviceType + "$", type=serviceType, status=True, ambariWeburl=weburl
            )
            serviceName = repos[0]['name']
        policies_to_delete = Xa.getPoliciesForResources(serviceType, serviceName, database=db, ambariWeburl=weburl)
        if policies_to_delete is not None:
            for policy in policies_to_delete["policies"]:
                if deleteOnlyDenyPolicies == True:
                    if primaryCluster + "_beacon deny policy for " + db == policy["name"]:
                        Xa.deletePolicy_by_id_api_v2(policy["id"], weburl=weburl)
                        break
                else:
                    Xa.deletePolicy_by_id_api_v2(policy["id"], weburl=weburl)
        #waiting for policy refresh after policies deletion
        time.sleep(30)

    @classmethod
    def createPoliciesFromJson(
            cls,
            file,
            serviceType,
            sourceHiveServiceName="mycluster0_hive",
            sourceHdfsServiceName="mycluster0_hadoop",
            targetServiceName=None,
            ambariWeburl=source_weburl,
            updateIfExists=False,
            polResource=None,
            isOverRideTrue=True
    ):
        if Xa.isArgusInstalled():
            servicesMapJson = Config.getEnv('ARTIFACTS_DIR') + '/' + datetime.datetime.now(
            ).strftime("%Y%m%d%H%M%S") + 'service_mapping.json'
            serviceName = "hadoop" if serviceType == "hdfs" else serviceType
            if targetServiceName is None:
                targetServiceName = \
                Xa.findRepositories(nameRegex="^.*_" + serviceName + "$", type=serviceType, status=True,
                                    ambariWeburl=ambariWeburl)[0]['name']
            f = open(servicesMapJson, 'w')
            if serviceType == "hive":
                f.write('{"' + sourceHiveServiceName + '":"' + targetServiceName + '"}')
            elif serviceType == "hdfs":
                f.write('{"' + sourceHdfsServiceName + '":"' + targetServiceName + '"}')
            f.close()
            Xa.importPoliciesInJsonFile(
                file,
                serviceType,
                servicesMapJson=servicesMapJson,
                ambariWeburl=ambariWeburl,
                updateIfExists=updateIfExists,
                polResource=polResource,
                isOverRideTrue=isOverRideTrue
            )

    @classmethod
    def verify_Policy_Exists_after_replication(
            cls,
            servicetype,
            verify_from_cluster=source_weburl,
            custer_to_verify=target_weburl,
            database=None,
            path=None,
            NoPolicyInTarget=False,
            expectedDesc="created by beacon while importing from " + primaryCluster,
            preDenyPolicyStr=primaryCluster + "_beacon deny policy for "
    ):
        if Xa.isArgusInstalled() is True:
            serviceName = "hadoop" if servicetype == "hdfs" else servicetype
            serviceNameOfverify_from_cluster = \
                Xa.findRepositories(nameRegex="^.*_" + serviceName + "$", type=servicetype, status=True,
                                    ambariWeburl=verify_from_cluster)[0]['name']
            serviceNameOfverify_to_cluster = \
                Xa.findRepositories(nameRegex="^.*_" + serviceName + "$", type=servicetype, status=True,
                                    ambariWeburl=custer_to_verify)[0]['name']
            logger.info("verifying if policy exist in target cluster")
            policies_in_source_Cluster = Xa.getPoliciesForResources(
                servicetype,
                serviceName=serviceNameOfverify_from_cluster,
                ambariWeburl=verify_from_cluster,
                database=database,
                path=path
            )
            policies_in_target_Cluster = Xa.getPoliciesForResources(
                servicetype,
                serviceName=serviceNameOfverify_to_cluster,
                ambariWeburl=custer_to_verify,
                database=database,
                path=path
            )
            if NoPolicyInTarget == False:
                assert len(policies_in_target_Cluster) != 0, "make sure policies were imported"
                BeaconRanger.setIdOfAllPolicyToZero(
                    policies_in_source_Cluster, policies_in_target_Cluster, expectedDesc
                )
                logger.info("set of policies in target cluster: " + str(policies_in_target_Cluster["policies"]))
                for policy in policies_in_source_Cluster["policies"]:
                    logger.info("policy is " + str(policy))
                    assert policy in policies_in_target_Cluster["policies"]
                logger.info("all policies are verified!! now will check for deny policy if it is true")
            isDenyPolicyTrue = Ambari.getConfig(
                'beacon-security-site', webURL=source_weburl
            )['beacon.ranger.plugin.create.denypolicy']
            all_policies_in_target_Cluster = Xa.getPolicy_api_v2(servicetype, weburl=target_weburl)
            if isDenyPolicyTrue == 'true':
                dataset = path if servicetype == "hdfs" else database
                BeaconRanger.denyPolicyValidation(
                    servicetype, dataset, all_policies_in_target_Cluster, preDenyPolicyStr
                )
            else:
                assert len(policies_in_target_Cluster) == len(policies_in_source_Cluster)

    @classmethod
    def setIdOfAllPolicyToZero(cls, policies_in_source_Cluster, policies_in_target_Cluster, expectedDesc):
        for pol in policies_in_source_Cluster["policies"]:
            pol["id"] = 0
            pol["version"] = 0
            pol["service"] = 'test'
            pol["description"] = 'description'
        for pol in policies_in_target_Cluster["policies"]:
            pol["id"] = 0
            pol["version"] = 0
            pol["service"] = 'test'
            assert expectedDesc in pol["description"]
            pol["description"] = 'description'

    @classmethod
    def denyPolicyValidation(cls, servicetype, dataset, policies_in_cluster, preDenyPolicyStr):
        readAndWritePermissions_hive = [
            {
                "type": "create",
                "isAllowed": True
            }, {
                "type": "update",
                "isAllowed": True
            }, {
                "type": "drop",
                "isAllowed": True
            }, {
                "type": "alter",
                "isAllowed": True
            }, {
                "type": "index",
                "isAllowed": True
            }, {
                "type": "lock",
                "isAllowed": True
            }, {
                "type": "write",
                "isAllowed": True
            }, {
                "type": "select",
                "isAllowed": True
            }, {
                "type": "read",
                "isAllowed": True
            }
        ]
        writePermissions_hive = [
            {
                "type": "create",
                "isAllowed": True
            }, {
                "type": "update",
                "isAllowed": True
            }, {
                "type": "drop",
                "isAllowed": True
            }, {
                "type": "alter",
                "isAllowed": True
            }, {
                "type": "index",
                "isAllowed": True
            }, {
                "type": "lock",
                "isAllowed": True
            }, {
                "type": "write",
                "isAllowed": True
            }
        ]
        writePermissions_hdfs = [{"type": "write", "isAllowed": True}]

        readAndWritePermissions_hdfs = [
            {
                "type": "read",
                "isAllowed": True
            }, {
                "type": "write",
                "isAllowed": True
            }, {
                "type": "execute",
                "isAllowed": True
            }
        ]
        writePermissions = writePermissions_hdfs if servicetype == "hdfs" else writePermissions_hive
        readAndWritePermissions = readAndWritePermissions_hdfs if servicetype == "hdfs" else readAndWritePermissions_hive
        denyPolicyFound = False
        for pol in policies_in_cluster:
            if pol["denyPolicyItems"]:
                if preDenyPolicyStr + dataset in pol["name"]:
                    assert pol["denyPolicyItems"][0]["groups"][0] == "public"
                    assert pol["denyPolicyItems"][0]["accesses"] == writePermissions
                    assert pol["denyExceptions"][0]["users"][0] == "beacon"
                    assert pol["denyExceptions"][0]["accesses"] == readAndWritePermissions
                    denyPolicyFound = True
                    break
        assert denyPolicyFound == True, "deny policy is not found!!"

    # enable policy should be false if we want to disable the policy , whereas it should be True if we want to enable the policy.
    @classmethod
    def disableEnableHiveInfraPolicies(cls, enablePolicy):
        policies_in_source_Cluster = Xa.getPolicy_api_v2("hive", weburl=source_weburl)
        policies_in_target_Cluster = Xa.getPolicy_api_v2("hive", weburl=target_weburl)
        for policy in policies_in_source_Cluster:
            if 'Qe-infra' in policy["name"]:
                policy["isEnabled"] = enablePolicy
                Xa.UpdatePolicy_api_v2(policy)
        for policy in policies_in_target_Cluster:
            if 'Qe-infra' in policy["name"]:
                policy["isEnabled"] = enablePolicy
                Xa.UpdatePolicy_api_v2(policy)
        # waiting for policy to be active
        time.sleep(30)

    @classmethod
    def disableOrEnableenableDenyAndExceptionsInPolicies(
            cls, serviceList, enableenableDenyAndExceptionsInPolicies=True
    ):
        Xa.disableOrEnableenableDenyAndExceptionsInPolicies(serviceList, enableenableDenyAndExceptionsInPolicies)
        Xa.disableOrEnableenableDenyAndExceptionsInPolicies(
            serviceList, enableenableDenyAndExceptionsInPolicies, weburl=target_weburl
        )

    @classmethod
    def disableEnableDenyPolicyCreation(cls, enableDenyPolicyCreation, webURL):
        isDenyPolicyTrue = Ambari.getConfig(
            'beacon-security-site', webURL=webURL
        )['beacon.ranger.plugin.create.denypolicy']
        if (isDenyPolicyTrue == 'true') == enableDenyPolicyCreation:
            logger.info("skiping changing config , as it is already present")
        else:
            propsToSet = {'beacon.ranger.plugin.create.denypolicy': enableDenyPolicyCreation}
            Ambari.setConfig("beacon-security-site", propsToSet, webURL=webURL)
            Ambari.start_stop_service("BEACON", 'INSTALLED', waitForCompletion=True, weburl=webURL)
            logger.info("---- Done stopping Beacon cluster")
            Ambari.start_stop_service('BEACON', 'STARTED', waitForCompletion=True, weburl=webURL)
            logger.info("---- Done starting Beacon cluster")

            # #json to json validation
            # @classmethod
            # def verify_Policy_Exists_In_Hive_bk(cls,ambariWeburl=target_weburl):
            #     if Xa.isArgusInstalled() is True:
            #         logger.info("verifying if policy exist in target cluster")
            #         expectedPolicies=list(BeaconRanger.policiesAddedBeforeTest)
            #         #emptying the expected list
            #         BeaconRanger.policiesAddedBeforeTest[:]=[]
            #         #check if there are atleast some policies created
            #         assert expectedPolicies, 'no policy was created'
            #         policies_in_target_Cluster=Xa.getPolicy_api_v2("hive",weburl=ambariWeburl)
            #         for expectedPolicy in expectedPolicies:
            #             policyFound=False
            #             for policy in policies_in_target_Cluster:
            #                 if expectedPolicy["name"] ==  policy["name"]:
            #                     policyFound=True
            #                     if expectedPolicy["resources"].get("database",None) is not None:
            #                         assert expectedPolicy["resources"]["database"]["values"] == policy["resources"]["database"]["values"]
            #                         if expectedPolicy["resources"].get("udf",None) is not None:
            #                             assert expectedPolicy["resources"]["udf"]["values"] == policy["resources"]["udf"]["values"]
            #                         else:
            #                             assert expectedPolicy["resources"]["table"]["values"] == policy["resources"]["table"]["values"]
            #                             assert expectedPolicy["resources"]["column"]["values"] == policy["resources"]["column"]["values"]
            #                     else:
            #                         assert expectedPolicy["resources"]["url"]["values"] == policy["resources"]["url"]["values"]
            #         assert policyFound == True, policy["name"] + "is not found"
            #     else:
            #         logger.info("Ranger is off! ranger policy validation is not needed")
