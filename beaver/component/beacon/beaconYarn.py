from beaver.component.ambari import Ambari
import logging, time
from beaver.machine import Machine
from beaver.config import Config
from beaver.component.beacon.beacon import Beacon

logger = logging.getLogger(__name__)

_ambari_host_1 = Config.get("multicluster", "AMBARI_GATEWAY1")
_ambari_host_2 = Config.get("multicluster", "AMBARI_GATEWAY2")

source_weburl = Ambari.getWebUrl(hostname=_ambari_host_1)
target_weburl = Ambari.getWebUrl(hostname=_ambari_host_2)


class BeaconYarn:
    @classmethod
    def addYarnQueue(cls, queueName="newQueue", capacity=26, webURL=source_weburl):
        defaultYarnQueueConfig = Ambari.getConfig(service='YARN', type='capacity-scheduler', webURL=webURL)
        updatedYarnQueueConfig = defaultYarnQueueConfig.copy()

        updatedYarnQueueConfig["yarn.scheduler.capacity.root.queues"
                               ] = defaultYarnQueueConfig["yarn.scheduler.capacity.root.queues"] + "," + queueName
        updatedYarnQueueConfig["yarn.scheduler.capacity.root.default.capacity"] = str(
            int(defaultYarnQueueConfig["yarn.scheduler.capacity.root.default.capacity"].split(".")[0]) - capacity
        )
        updatedYarnQueueConfig["yarn.scheduler.capacity.root.default.maximum-capacity"] = str(
            int(defaultYarnQueueConfig["yarn.scheduler.capacity.root.default.maximum-capacity"].split(".")[0]) -
            capacity
        )

        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".acl_administer_jobs"] = "*"
        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".acl_submit_applications"] = "*"
        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".capacity"] = str(capacity)
        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".maximum-capacity"] = str(capacity)
        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".state"] = "RUNNING"
        updatedYarnQueueConfig["yarn.scheduler.capacity.root." + queueName + ".user-limit-factor"] = "1"
        Ambari.setConfig(type='capacity-scheduler', config=updatedYarnQueueConfig, webURL=webURL)
        logger.info("Add new YARN Queue: Successful")
        return updatedYarnQueueConfig, defaultYarnQueueConfig

    @classmethod
    def removeYarnQueue(cls, queueName="newQueue", defaultConfig=None, webURL=source_weburl, cluster=None):
        if cluster == None:
            cluster = Ambari.getClusterName()
        if webURL == None:
            webURL = Ambari.getWebUrl()
        else:
            cluster = Ambari.getClusterName(weburl=webURL)
        # Delete new config
        deleteConfig = {}
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".acl_administer_jobs"] = "*"
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".acl_submit_applications"] = "*"
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".capacity"] = "0"
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".maximum-capacity"] = "0"
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".state"] = "RUNNING"
        deleteConfig["yarn.scheduler.capacity.root." + queueName + ".user-limit-factor"] = "1"
        Ambari.deleteConfig(type='capacity-scheduler', config=deleteConfig, webURL=webURL)
        # Reset to default config
        Ambari.setConfig(type='capacity-scheduler', config=defaultConfig, webURL=webURL)
        Ambari.start_stop_service('YARN', 'INSTALLED', waitForCompletion=True, weburl=webURL)
        logger.info("---- Done stopping YARN cluster")
        Ambari.start_stop_service('YARN', 'STARTED', waitForCompletion=True, weburl=webURL)
        time.sleep(30)
        logger.info("---- Done starting YARN cluster")

    @classmethod
    def refreshYarnQueues(cls, host_ip, weburl=source_weburl, cluster=None):
        hostname = Machine.runas('root', 'hostname', host_ip)
        if cluster == None:
            cluster = Ambari.getClusterName()
        if weburl == None:
            weburl = Ambari.getWebUrl()
        else:
            cluster = Ambari.getClusterName(weburl=weburl)
        uri = "/api/v1/clusters/%s/requests" % (cluster)
        data = '{"RequestInfo":{"context":"Refresh YARN Capacity Scheduler","command":"REFRESHQUEUES","parameters/forceRefreshConfigTags":"capacity-scheduler"},' \
               '"Requests/resource_filters":[{"service_name":"YARN","component_name":"RESOURCEMANAGER","hosts":"' + hostname[1] + '"}]}'
        response = Ambari.http_put_post_request(uri=uri, data=data, requestType="POST", weburl=target_weburl)
        assert response.status_code == 202, "Failed To refresh Yarn Queue: Response Code: " + str(
            response.status_code
        ) + "Response body: " + response._content

    @classmethod
    def getYarnQueue(cls, job_id=None, webURL=source_weburl, beaconURL=None):
        yarnSiteConfig = Ambari.getConfig(service='YARN', type='yarn-site', webURL=webURL)
        yarnHostPort = yarnSiteConfig["yarn.resourcemanager.webapp.address"]
        yarnHttpPolicy = yarnSiteConfig["yarn.http.policy"]
        protocol = None
        if yarnHttpPolicy == "HTTP_ONLY":
            protocol = "http"
        elif yarnHttpPolicy == "HTTPS_ONLY":
            protocol = "https"
        yarn_webURL = protocol + "://" + yarnHostPort
        uri = "/ws/v1/cluster/apps/application_" + job_id
        logger.info("Get Yarn application details: " + yarn_webURL + uri)
        response = Beacon.doGetWithBeaconSession(url=yarn_webURL + uri, beacon_url=beaconURL)
        logger.info("Get Yarn application details response: " + response._content)
        response_json = response.json()
        return response_json["app"]["queue"]
