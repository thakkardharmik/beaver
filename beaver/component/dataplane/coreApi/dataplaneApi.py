#
# Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#

import logging, requests, json, random, time
from beaver.config import Config
import beaver.component.dataplane.coreApi.DP_CONSTANTS as DP_CONSTANTS
from beaver.component.ambari import Ambari

logger = logging.getLogger(__name__)
DP_IP_ADDR = Config.get("dataplane", "DP_HOST_ADDR")
DP_HTTP_URL = ("https://%s/" % DP_IP_ADDR)
DP_KNOX_URL = ("https://%s:8443/gateway/knoxsso/api/v1/websso?originalUrl=https://%s/" % (DP_IP_ADDR, DP_IP_ADDR))
TEST_AMBARI_HOST1 = Config.get("multicluster", "AMBARI_GATEWAY1")
TEST_AMBARI_HOST2 = Config.get("multicluster", "AMBARI_GATEWAY2")
TEST_AMBARI_WEB_URL1 = Ambari.getWebUrl(hostname=TEST_AMBARI_HOST1)
TEST_AMBARI_WEB_URL2 = Ambari.getWebUrl(hostname=TEST_AMBARI_HOST2)


class DP_Api(object):
    def __init__(self):
        self.session = requests.session()
        self.session.headers['Content-Type'] = 'application/json'
        self.session.verify = False

    def getSessionCookies(self):
        """
        :return:
        """
        return self.session.cookies

    def formatJson(self, response, sort=True, indents=4):
        """
        :param response:
        :param sort:
        :param indents:
        :return:
        """
        logger.info("Session Cookies: %s" % self.getSessionCookies())
        logger.info("Response: %s" % response.status_code)
        if response.status_code == 200:
            try:
                logger.info(json.dumps(response.json(), indent=indents, sort_keys=sort))
            # Headers are logged in case of empty responses
            except Exception as e:
                logger.info("Empty Response: %s" % response.headers)
        else:
            logger.info(response._content)

    def localLogin(self, username=None, password=None):
        """
        :param username: (optional)
        :param password: (optional)
        :return status_code:
        """
        url = DP_HTTP_URL + 'auth/in'
        if username is None or password is None:
            payload = {"username": DP_CONSTANTS.USERNAME, "password": DP_CONSTANTS.PASSWORD}
        else:
            payload = {"username": username, "password": password}
        logger.info("LocalLogin Request: %s" % url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(url, json.dumps(payload))
        self.formatJson(resp)
        return resp

    def configureLdap(self, payload=None):
        """
        :param payload: (optional)
        :return status_code:
        """
        url = DP_HTTP_URL + 'api/knox/configure'
        if payload is None:
            payload = DP_CONSTANTS.LDAP_PAYLOAD
        logger.info("ConfigureLdap Request: %s" % url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(url, json=payload)
        self.formatJson(resp)
        return resp

    def registerUser(self, username=None):
        """
        :param username: (optional)
        :return status_code:
        """
        url = DP_HTTP_URL + 'api/users/registerAdmins'
        if username is None:
            username = DP_CONSTANTS.KNOX_USERNAME
        payload = {"users": [username]}
        logger.info("RegisterUser Request: %s" % url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(url, json=payload)
        self.formatJson(resp)
        # Looks like it takes some time to re-initialize the knox container
        # after registration of the user. So, sleeping for 15 seconds as
        # of now before Knox Login
        time.sleep(15)
        return resp

    def registerUserWithRole(self, username=None, roles=None):
        """
        :param username: (optional)
        :param role: (optional)
        :return status_code:
        """
        url = DP_HTTP_URL + 'api/users/addUsersWithRoles'
        if username is None:
            username = DP_CONSTANTS.KNOX_USERNAME
        if roles is None:
            roles = ['INFRAADMIN']
        payload = {"users": [username], "roles": roles}
        logger.info("RegisterUser Request: %s" % url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(url, json=payload)
        self.formatJson(resp)
        # Looks like it takes some time to re-initialize the knox container
        # after registration of the user. So, sleeping for 15 seconds as
        # of now before Knox Login
        time.sleep(15)
        return resp

    def knoxLogin(self, username=None, password=None):
        """
        :param username: (optional)
        :param password: (optional)
        :return status_code:
        """
        if username is None or password is None:
            username = DP_CONSTANTS.KNOX_USERNAME
            password = DP_CONSTANTS.KNOX_PASSWORD
        logger.info("Login Credentials: %s and %s" % (username, password))
        self.session.auth = (username, password)
        self.session.verify = False
        self.session.allow_redirects = False
        self.session.cookies.clear_session_cookies()
        logger.info("KnoxLogin Request: %s" % DP_KNOX_URL)
        resp = self.session.get(DP_KNOX_URL)
        self.formatJson(resp)
        return resp

    def getUserDetail(self):
        """
        :return status_code:
        """
        url = DP_HTTP_URL + 'auth/userDetail'
        logger.info("GetUserDetail Request: %s" % url)
        resp = self.session.get(url)
        self.formatJson(resp)
        return resp

    def enableService(self, service_name):
        """
        :param service_name:
        :return status_code:
        """
        url = DP_HTTP_URL + 'api/services/enable'
        payload = {'smartSenseId': DP_CONSTANTS.SMART_SENSE_ID, 'skuName': service_name}
        logger.info("EnableService Request: %s" % url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(url, json=payload)
        self.formatJson(resp)
        return resp

    def addAmbariCluster(self, ambari_web_url=None):
        """
        :param ambari_web_url (optional)
        :return resp
        """
        if ambari_web_url is None:
            ambari_web_url = TEST_AMBARI_WEB_URL1
        add_cluster_url = (DP_HTTP_URL + 'api/lakes/ambari/status?url=' + ambari_web_url)
        resp = self.session.get(add_cluster_url)
        logger.info("AddAmbariCluster Request: %s" % add_cluster_url)
        self.formatJson(resp)
        return resp

    def getClusterDetails(self, ambari_web_url=None):
        """
        :param ambari_web_url (optional)
        :return resp
        """
        if ambari_web_url is None:
            ambari_web_url = TEST_AMBARI_WEB_URL1
        # Future Reference : Hardcoding this boolean for now. Can be either
        # an input argument to this function or something that can be
        # configured later on
        knox_detected_flag = False
        payload = {"url": ambari_web_url, "knoxDetected": knox_detected_flag}
        get_cluster_details_url = DP_HTTP_URL + 'api/clusters/details'
        logger.info("GetClusterDetails Request: %s" % get_cluster_details_url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(get_cluster_details_url, json=payload)
        self.formatJson(resp)
        return resp

    def getLocations(self, location=None):
        """
        :param location (optional)
        :return resp
        """
        if location is not None:
            get_locations_url = DP_HTTP_URL + '/api/locations?query=' + location
        else:
            get_locations_url = DP_HTTP_URL + '/api/locations'
        logger.info("GetLocations Request: %s" % get_locations_url)
        resp = self.session.get(get_locations_url)
        # This Api returns huge response and we might not want to flood the
        # log file with this response. So, commenting out!
        # logger.info("Response is: %s" % resp.text)
        # self.formatJson(resp)
        return resp

    def pickRandomLocation(self, location_resp_content):
        """
        :param location_resp
        :return location_json/False
        """
        try:
            list_locations = json.loads(location_resp_content)
            logger.info("%s locations are available in the response" % len(list_locations))
            loc_ref = random.choice(list_locations)
            logger.info("Returning the random location: %s" % loc_ref)
            return loc_ref
        except Exception as e:
            logger.error("Caught Exception: %s" % e)
            return False

    def convertStringToJson(self, string):
        """
        Utility method to convert a string to json and return the json
        """
        try:
            return json.loads(string)
        except Exception as e:
            logger.error("Caught Exception: %s" % e)
            return False

    def addDataLake(self, location_id, ambari_url=None):
        """
        Add a data lake with the specified parameters
        """
        if ambari_url is None:
            ambari_url = TEST_AMBARI_WEB_URL1
        add_lake_url = DP_HTTP_URL + 'api/lakes'
        dc_name = 'test_dc_' + str(location_id)
        lake_name = 'test_lake_' + str(location_id)
        lake_desc = 'This is a test data lake'
        is_data_lake_flag = False
        # Get Cluster Details
        clust_resp = self.getClusterDetails()
        if clust_resp.status_code != 200:
            return False
        else:
            clust_details = self.convertStringToJson(clust_resp.content)
            clust_services = clust_details[0].get('services', None)
            logger.info("Services Available in the cluster: %s" % clust_services)
            if 'ATLAS' in clust_services and 'RANGER' in clust_services:
                logger.info("Enabling Data Lake Flag ...")
                is_data_lake_flag = True
            else:
                logger.info("Data Lake Flag is set to False")
        payload = {
            "dcName": dc_name,
            "ambariUrl": ambari_url,
            "location": location_id,
            "isDatalake": is_data_lake_flag,
            "name": lake_name,
            "description": lake_desc,
            "state": "TO_SYNC",
            "ambariIpAddress": ambari_url,
            "properties": {
                "tags": []
            }
        }
        logger.info("AddLake Request: %s" % add_lake_url)
        logger.info("Payload: %s" % payload)
        resp = self.session.post(add_lake_url, json=payload)
        self.formatJson(resp)
        return resp

    def logout(self):
        """
        :return status_code:
        """
        url = DP_HTTP_URL + 'auth/signOut'
        logger.info("Logout Request: %s" % url)
        resp = self.session.get(url)
        self.formatJson(resp)
        return resp


class DP_Onboard(DP_Api):
    def __init__(cls):
        super(cls.__class__, cls).__init__()

    def initiateDataplaneOnboarding(cls):
        """
        :return boolean:
        """
        logger.info("Initiating Dataplane Onboarding")
        # Local Login
        assert cls.localLogin().status_code == 200
        # Configure LDAP
        assert cls.configureLdap().status_code == 200
        # Register User
        user_roles = ['SUPERADMIN', 'INFRAADMIN']
        assert cls.registerUserWithRole(roles=user_roles).status_code == 200
        # Knox Login
        assert cls.knoxLogin().status_code == 200
        # Get User Detail
        assert cls.getUserDetail().status_code == 200
        # Enable Service, dlm by default for now
        assert cls.enableService('dlm').status_code == 200
        # Add clusters for DLM
        for clust in (TEST_AMBARI_WEB_URL1, TEST_AMBARI_WEB_URL2):
            logger.info("Adding the Ambari cluster: %s" % clust)
            # Add Ambari Cluster
            assert cls.addAmbariCluster(clust).status_code == 200
            # Pick a random location
            loc_resp = cls.getLocations()
            assert loc_resp != False
            loc_ref = cls.pickRandomLocation(loc_resp.content)
            assert loc_ref != False
            location_id = loc_ref.get('id')
            # Adding Data Lake
            resp = cls.addDataLake(location_id, clust)
            assert resp != False
            assert resp.status_code == 200
        # We reach here only if all the above steps are successful.
        # So, safe to return True
        return True
