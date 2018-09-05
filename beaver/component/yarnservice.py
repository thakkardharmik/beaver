import os
import logging
import random
import json
import time
import re

from taskreporter.taskreporter import TaskReporter

from beaver.config import Config
from beaver.component.hadoop import YARN, Hadoop, MAPRED, HDFS
from beaver.component.docker import Docker
from beaver import util
from beaver.machine import Machine
from beaver.component.ambari import Ambari
from beaver.component.spark import Spark


logger = logging.getLogger(__name__)
'''
Description:
Test API to work with YarnServices
'''


class Yarnservice(object):
    def __init__(self):
        pass

    _headers = "Content-Type: application/json"

    @classmethod
    def get_yarn_service_example_dir(cls):
        """
        Get Yarn service example dir
        :return:
        """
        return os.path.join(YARN.getYarnHome(), "yarn-service-examples")

    @classmethod
    def get_httpd_proxy_filename(cls):
        """
        Get httpd example proxy file name
        :return:
        """
        return "httpd-proxy.conf"

    @classmethod
    def get_httpd_proxy_file(cls):
        """
        Get httpd example proxy file
        :return:
        """
        return os.path.join(cls.get_yarn_service_example_dir(), "httpd", cls.get_httpd_proxy_filename())

    @classmethod
    def get_httpd_json(cls):
        """
        Get httpd example json file
        :return:
        """
        return os.path.join(cls.get_yarn_service_example_dir(), 'httpd', 'httpd.json')

    @classmethod
    def get_sleeper_json(cls, user=None, keytab=None, principal=None):
        """
        Get sleeper example json file
        :return:
        """
        if Hadoop.isSecure():
            src_yar = os.path.join(cls.get_yarn_service_example_dir(), 'sleeper', 'sleeper.json')
            if not user:
                user = Config.get('hbase', 'HBASE_USER')
            if not keytab:
                keytab = "file://%s" % Machine.getServiceKeytab(user)
            if not principal:
                principal = user + "/_HOST@" + Machine.get_user_realm()
            return Yarnservice.update_service_definition(src_yar, keytab=keytab, principal=principal)
        else:
            return os.path.join(cls.get_yarn_service_example_dir(), 'sleeper', 'sleeper.json')

    @classmethod
    def get_hbase_yar(cls):
        """
        Get hbase.yar file
        :return:
        """
        return os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'yarn-docker', 'yarnServices', 'data', 'hbase.yar')

    @classmethod
    @TaskReporter.report_test()
    def get_hbase_tarball_yar(cls):
        """
        Get hbase-tarball.yar file
        :return:
        """
        if Hadoop.isSecure():
            src_yar = os.path.join(
                Config.getEnv('WORKSPACE'), 'tests', 'yarn-docker', 'yarnServices', 'data', 'hbase-tarball-secure.yar'
            )
            dest_yar = cls.createTmpFile("hbase-tarball-secure.yar")
            Machine.copy(src_yar, dest_yar)
            util.replaceVarsInFile(dest_yar, {"USER_REALM": Machine.get_user_realm()})
            return dest_yar
        else:
            return os.path.join(
                Config.getEnv('WORKSPACE'), 'tests', 'yarn-docker', 'yarnServices', 'data', 'hbase-tarball.yar'
            )

    @classmethod
    def get_hbase_log4j(cls):
        """
        Get hbase-log4j.properties file
        :return:
        """
        return os.path.join(
            Config.getEnv('WORKSPACE'), 'tests', 'yarn-docker', 'yarnServices', 'data', 'hbase-log4j.properties'
        )

    @classmethod
    @TaskReporter.report_test()
    def get_hbase_tarball(cls):
        """
        Get hbase tarball
        :return:
        """
        hbase_tarball = Config.getEnv('ARTIFACTS_DIR')
        url = "http://qe-repo.s3.amazonaws.com/hbase-2.0.0.3.0.0.0.tar.gz"
        tarball = os.path.join(hbase_tarball, "hbase-2.0.0.3.0.0.0.tar.gz")
        cmd = "curl -o %s %s" % (tarball, url)
        Machine.runas(Machine.getAdminUser(), cmd, host=None, passwd=Machine.getAdminPasswd())
        return tarball

    @classmethod
    def get_sleeper_dependency_yar(cls):
        """
        Get sleeper dependency yar file
        :return:
        """
        return os.path.join(
            Config.getEnv('WORKSPACE'), 'tests', 'yarn-docker', 'yarnServices', 'data', 'sleeper-dependency.yar'
        )

    @classmethod
    @TaskReporter.report_test()
    def get_applicationId_from_yarnservice(cls, stdout):
        """
        Get application Id from yarn service stdout
        :param stdout:
        :return:
        """
        pattern = "client.ApiServiceClient: Application ID: (.*)"
        matchObj = re.search(pattern, stdout)
        if matchObj is None:
            return None
        else:
            return matchObj.group(1)

    @classmethod
    @TaskReporter.report_test()
    def launch_yarn_service(cls, service_name, service_description, user=None, get_full_output=False):
        """
        Launch yarn service
        :param service_name: yarn service name
        :param service_description: yarn service description file. It can be either .json or .yar
        Command line : yarn app -launch <service name> <service description file>
        :param user: user to launch the app
        :param get_full_output: this option will return ( exit_code, stdout )
                                otherwise it will return (appId)
        :return:
        """
        cmd = "app -launch %s %s" % (service_name, service_description)
        exit_code, stdout = YARN.runas(user, cmd)
        if get_full_output:
            return exit_code, stdout
        if exit_code != 0:
            return None
        else:
            return cls.get_applicationId_from_yarnservice(stdout)

    @classmethod
    @TaskReporter.report_test()
    def destroy_yarn_service(cls, service_name, user=None):
        """
        Destroy yarn service
        :param service_name: yarn service name
        :param user: user
        Command line: yarn app -destroy <Application Name>
        :return:
        """
        cmd = "app -destroy %s" % service_name
        return YARN.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def stop_yarn_service(cls, service_name, user=None):
        """
        Stop yarn service
        :param service_name: yarn service name
        :param user: user
        Command line: yarn app -stop <Application name>
        :return:
        """
        cmd = "app -stop %s" % service_name
        return YARN.runas(user, cmd)

    @classmethod
    @TaskReporter.report_test()
    def parse_application_json(cls, stdout):
        """
        Parse application json output
        :param stdout:
        :return:
        """
        output = ""
        json_started = False
        for line in stdout.split("\n"):
            if not line.startswith("{") and not json_started:
                continue
            elif line.startswith("{") and not json_started:
                json_started = True
                output = output + line + "\n"
            else:
                output = output + line + "\n"

        logger.info("********************** output ******************")
        logger.info(output.strip())
        logger.info("**********************************************")
        return output.strip()

    @classmethod
    @TaskReporter.report_test()
    def get_yarn_service_status(cls, service_name, user=None):
        """
        Get yarn service application status
        :param service_name:
        :param user:
        :return: json object
        """
        exit_code, stdout = YARN.getApplicationStatus(service_name, user=user)
        if exit_code == 0:
            status_json = cls.parse_application_json(stdout)
            return exit_code, status_json
        else:
            return exit_code, stdout

    @classmethod
    def get_yarn_services_url(cls):
        """
        Get yarn service url for Rest api
        :return: http://<rmhost>:<rmport>/app/v1/services
        """
        return YARN.getResourceManagerWebappAddress() + "/app/v1/services"

    @classmethod
    @TaskReporter.report_test()
    def create_yarn_services_with_api(cls, service_description, user=None, logoutput=True):
        """
        Create yarn services with rest api
        :param service_description: yarn service description file. It can be either .json or .yar
        :return: returns request object for POST call
        """
        try:
            #data = json.load(open(service_description))
            if not user:
                user = Config.get('hadoop', 'HADOOPQA_USER')
            url = cls.get_yarn_services_url() + "?user.name=%s" % user
            if logoutput:
                logger.info("url = %s ", url)
                logger.info("jsonfile = %s", service_description)
            status_code, stdout = util.curlas(
                user,
                url,
                method='POST',
                logoutput=False,
                header=Yarnservice._headers,
                jsonfile=service_description,
                skipuser=True
            )
            if logoutput:
                logger.info("status code = %d", status_code)
                logger.info("response json = %s", stdout)
            assert status_code in [200, 202], "create yarn service failed"
            return stdout
        except Exception as e:
            logger.info("create yarn service failed.")
            logger.info(e)
            return None

    @classmethod
    @TaskReporter.report_test()
    def wait_for_application_state(cls, service_name, timeout=300, interval=5, state="STABLE", user=None):
        """
        Wait for yarn service state to reach to a destination state
        If state=stable, all components of yarn service is in stable state
        If state=STARTED, few service component are in stable state and few components are not yet ready
        :param service_name:
        :param timeout:
        :param interval:
        :param state:
        :return:
        """
        starttime = time.time()
        while (time.time() - starttime) < timeout:
            response = cls.get_yarn_service_details_with_api(service_name, user=user)
            if response:
                if response["state"] != state:
                    time.sleep(interval)
                else:
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def get_yarn_service_details_with_api(cls, service_name, expected_status_code=None, user=None, logoutput=True):
        """
        Get yarn service status with rest api
        :param service_name: yarn service name
        :return: yarn service status json object
        """
        if not expected_status_code:
            expected_status_code = [200]
        try:
            if not user:
                user = Config.get('hadoop', 'HADOOPQA_USER')
            url = cls.get_yarn_services_url() + "/%s?user.name=%s" % (service_name, user)
            if logoutput:
                logger.info("url = %s ", url)
            statuscode, stdout = util.curlas(user, url, logoutput=True, skipuser=True)
            if logoutput:
                logger.info("status code = %s", statuscode)
                logger.info("response = %s", stdout)
            assert statuscode in expected_status_code, "get yarn service detail failed"
            return json.loads(stdout)
        except Exception as e:
            logger.info("get yarn service details failed.")
            logger.info(e)
            return None

    @classmethod
    @TaskReporter.report_test()
    def createTmpFile(cls, clientfile, tmp_dir=None, host=None):
        """
        Creates temporary client log file in Artifacts dir
        :param clientfile: The name of file
        :return: absolute path
        """
        ADMIN_USER = Machine.getAdminUser()
        ADMIN_PWD = Machine.getAdminPasswd()
        if not tmp_dir:
            tmp_dir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "YarnServiceLogs")
        Machine.makedirs(None, host, tmp_dir, passwd=None)
        Machine.chmod("777", tmp_dir, recursive=True)
        Local_clientlog = os.path.join(tmp_dir, clientfile)
        if Machine.pathExists(ADMIN_USER, host, Local_clientlog, ADMIN_PWD):
            Machine.rm(ADMIN_USER, host, Local_clientlog, isdir=False, passwd=ADMIN_PWD)
        return Local_clientlog

    @classmethod
    @TaskReporter.report_test()
    def validate_httpd_example(cls, service_name, user=None):
        """
        Validate httpd app by accessing quick link
        :param service_name: yarn service name
        :return:
        """
        response = cls.get_yarn_service_details_with_api(service_name, user=user)
        server_url = response["quicklinks"]["Apache HTTP Server"]
        tmp_local_file = cls.createTmpFile("Validate-%s.txt" % service_name)
        logger.info("Validating url : %s", server_url)
        util.getURLContents(server_url, outfile=tmp_local_file)
        assert util.findMatchingPatternInFile(tmp_local_file, "Hello from httpd-0!") == 0, "Httpd-0 pattern not found"

    @classmethod
    @TaskReporter.report_test()
    def destroy_yarn_service_with_api(cls, service_name, user=None, logoutput=True):
        """
        Destroy yarn service application with rest api
        :param service_name:
        :return:
        """
        try:
            if not user:
                user = Config.get('hadoop', 'HADOOPQA_USER')
            url = cls.get_yarn_services_url() + "/%s?user.name=%s" % (service_name, user)
            if logoutput:
                logger.info("Delete yarn service url = %s", url)
            status_code, _ = util.curlas(user, url, method='DELETE', logoutput=True, skipuser=True)
            if logoutput:
                logger.info("status code = %d", status_code)
            assert status_code in [200, 204], "Delete %s request failed" % service_name
            res = cls.get_yarn_service_details_with_api(service_name, expected_status_code=[404], user=user)
            assert res["diagnostics"].find("File does not exist:") >= 0
            return True
        except Exception as e:
            logger.info("delete yarn service details failed.")
            logger.info(e)
            return None

    @classmethod
    @TaskReporter.report_test()
    def stop_yarn_service_with_api(cls, service_name, user=None, logoutput=True):
        """
        Stop yarn service with api
        :param service_name: yarn service name
        :param logoutput:
        :return:
        """
        try:
            if not user:
                user = Config.get('hadoop', 'HADOOPQA_USER')
            data = {'state': 'STOPPED'}
            tmpfile = cls.createTmpFile("stopyarnservice-%s-%s" % (service_name, random.randint(1, 101)))
            with open(tmpfile, 'w') as outfile:
                json.dump(data, outfile)
            url = cls.get_yarn_services_url() + "/%s?user.name=%s" % (service_name, user)
            status_code, stdout = util.curlas(
                user, url, method='PUT', logoutput=True, header=Yarnservice._headers, jsonfile=tmpfile, skipuser=True
            )
            if logoutput:
                logger.info("url = %s", url)
                logger.info("status code = %d", status_code)
                logger.info("stdout = %s", stdout)
            assert status_code in [204, 200], "stop service failed"
            return True
        except Exception as e:
            logger.info("delete yarn service details failed.")
            logger.info(e)
            return None

    @classmethod
    @TaskReporter.report_test()
    def validate_with_docker_run(cls, node_manager, container_host, image, msg, port):
        """
        Validate Hello Httpd message from Httpd daemon
        Login to docker conatiner and access http://IP:Port to validate
        ( Workaround for BUG-94304)
        :param node_manager:
        :param container_host:
        :param http_daemon_name:
        :param image:
        :param msg:
        :param port:
        :return:
        """
        exit_code, stdout = Docker.run(
            "-v %s:%s:ro --net=hadoop" % (cls.get_resolv_conf(), cls.get_resolv_conf()),
            image,
            "curl http://%s:%s" % (container_host, port),
            host=node_manager
        )
        logger.info("stdout : %s", stdout)
        assert exit_code == 0, "docker run command failed on %s" % node_manager
        m = re.search(msg, stdout)
        return bool(m)

    @classmethod
    @TaskReporter.report_test()
    def validate_with_docker_exec(cls, node_manager, container_id, cmd, msgs, mode):
        """
        Validate Hbase App using Docker exec cmd
        Sample: docker exec -it container_id curl <url>
        <url> = http://hbasemaster-0.hbase-app-with-docker.hrt-qa.test.com:16010/master-status
        :param node_manager: nodemanager where docker container is running
        :param container_id: docker container Id
        :param cmd: cmd line to execute in docker
        :param msg: expected msg
        :return:
        """
        if mode == "docker":
            exit_code, stdout = Docker.execute(container_id, cmd, options="-i", host=node_manager)
        else:
            exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), cmd, host=node_manager, passwd=Machine.getAdminPasswd()
            )
        logger.info("stdout : %s", stdout)
        assert exit_code == 0, "docker exec command failed on %s" % node_manager
        for msg in msgs:
            m = re.search(msg, stdout)
            if not m:
                return False, "%s msg not found" % msg
        return True

    @classmethod
    @TaskReporter.report_test()
    def validate_dns(cls, hostname):
        """
        Validate dns entry exist in Yarn DNS registry
        :param hostname:
        :return:
        """
        cmd = "dig +noall +answer @%s %s" % (cls.get_Yarn_registryserver_host(), hostname)
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        assert exit_code == 0
        # validate there is no TXT record
        m = re.search(r"IN\s+TXT", stdout)
        if m:
            return False, "TXT record is present in dns check"
        # validate some ip address is associated with dig
        m1 = re.search(r"IN\s+A\s+\d+\.\d+\.\d+\.\d+", stdout)
        if not m1:
            return False, "A record is absent from dns check"
        # validate from log file
        local_yr_logfile = cls.copy_registryserver_log_to_local()
        cmd = "tail -n 2 %s" % local_yr_logfile
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        p = r"found answers { %s. \d+ IN A \[\d+\.\d+\.\d+\.\d+\] }" % hostname
        m = re.search(p, stdout)
        if not m:
            return False, "%s check failed for yarn registry log" % hostname
        return True

    @classmethod
    @TaskReporter.report_test()
    def validate_httpd(cls, service_name, port=8080, user=None, skip_dns_check=False):
        """
        Validate HTTPD example
        :param service_name:
        :param port:
        :param user:
        :return:
        """
        exit_code, stdout = cls.get_yarn_service_status(service_name, user=user)
        assert exit_code == 0, "yarn get application status failed for %s" % service_name
        datastore = json.loads(stdout)

        logger.info("Validate Httpd containers")

        for comp in datastore["components"]:
            http_image = comp["artifact"]["id"]
            for container in comp["containers"]:
                component_bare_host = container["bare_host"]
                http_container_ip = container["ip"]
                component_instance_name = container["component_instance_name"]
                hostname = container["hostname"]
                if component_instance_name.find("proxy") >= 0:
                    msg = "Hello from httpd-[0|1]!"
                else:
                    msg = "Hello from %s!" % component_instance_name
                state = container["state"]
                assert state == "READY"
                #host_validation = cls.validate_with_docker_run(component_bare_host, hostname, http_image, msg, port)
                #if not host_validation:
                #    logger.info("Host Validation for  %s failed" % component_instance_name)
                #    return False
                ip_validation = cls.validate_with_docker_run(
                    component_bare_host, http_container_ip, http_image, msg, port
                )
                if component_instance_name.find("proxy") >= 0 and not ip_validation:
                    ip_validation = cls.validate_with_docker_run(
                        component_bare_host, http_container_ip, http_image, msg, port
                    )
                if not ip_validation:
                    logger.info("IP Validation failed for %s", component_instance_name)
                    return False
                if not skip_dns_check:
                    assert cls.validate_dns(hostname), "dns check failed for %s" % hostname

        ## TODO add check for docker client log message BUG-101715
        return True

    @classmethod
    @TaskReporter.report_test()
    def get_Yarn_registryserver_host(cls):
        """
        Get host detail of yarn registry service
        :return:
        """
        host = Ambari.getHostsForComponent('YARN_REGISTRY_DNS')
        assert host, "Yarn registry dns is not present in cluster"
        return util.getIpAddress(hostname=host[0])

    @classmethod
    def start_yarn_registryserver(cls):
        """
        start yarn registry service
        :return:
        """
        Hadoop.startServices(services=["yarnregistry"])

    @classmethod
    def stop_yarn_registryserver(cls):
        """
        start yarn registry service
        :return:
        """
        Hadoop.stopServices(services=["yarnregistry"])

    @classmethod
    @TaskReporter.report_test()
    def restart_yarn_registryserver(cls, sleep=30):
        """
        restart yarn registry service
        :return:
        """
        cls.stop_yarn_registryserver()
        time.sleep(sleep)
        cls.start_yarn_registryserver()

    @classmethod
    @TaskReporter.report_test()
    def get_yarn_registryserver_logfile(cls, yr_host=None, user="root"):
        """
        find out yarn registry server logfile
        :param yr_host:
        :return:
        """
        if not yr_host:
            yr_host = cls.get_Yarn_registryserver_host()
        lines = Machine.find(
            user=Machine.getAdminUser(),
            host=yr_host,
            filepath=Config.get('hadoop', 'YARN_LOG_DIR'),
            searchstr="hadoop-yarn-" + user + "-registrydns-*.log",
            passwd=Machine.getAdminPasswd()
        )
        lines = util.prune_output(lines, Machine.STRINGS_TO_IGNORE)
        return lines[0]

    @classmethod
    @TaskReporter.report_test()
    def copy_registryserver_log_to_local(cls):
        """
        copy yarn registry server to localhost
        :return:
        """
        yr_host = cls.get_Yarn_registryserver_host()
        yr_LogFile = cls.get_yarn_registryserver_logfile(yr_host)
        localYRLogFile = os.path.join(
            Config.getEnv('ARTIFACTS_DIR'), 'local-registrydns-' + str(int(time.time())) + '.log'
        )
        # copy the file to local machine
        Machine.copyToLocal(
            user=Machine.getAdminUser(),
            host=yr_host,
            srcpath=yr_LogFile,
            destpath=localYRLogFile,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            Machine.getAdminUser(), "chmod 777 " + localYRLogFile, host=None, passwd=Machine.getAdminPasswd()
        )
        return localYRLogFile

    @classmethod
    def get_resolv_conf(cls):
        """
        Get resolv conf file location
        :return:
        """
        return "/etc/resolv.conf"

    @classmethod
    @TaskReporter.report_test()
    def update_resolv_conf(cls, hosts):
        """
        Update resolv conf to add "nameserver <yarn registry dns ip>"
        :param hosts:
        :return:
        """
        pattern = "nameserver %s" % cls.get_Yarn_registryserver_host()
        ispresent = util.findMatchingPatternInFile(cls.get_resolv_conf(), pattern)
        if ispresent == 0:
            return
        for host in hosts:
            tmpfile = cls.createTmpFile("resolv.conf", tmp_dir="/tmp/resolv_dir", host=host)
            cmd = "sed '3 i nameserver %s' %s > %s" % (
                cls.get_Yarn_registryserver_host(), cls.get_resolv_conf(), tmpfile
            )
            Machine.runas(Machine.getAdminUser(), cmd, host=host, passwd=Machine.getAdminPasswd())
            cmd = "cp %s %s" % (tmpfile, cls.get_resolv_conf())
            Machine.runas(Machine.getAdminUser(), cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    @TaskReporter.report_test()
    def wait_for_component_state(cls, service_name, timeout=90, interval=5, logoutput=True):
        """
        Wait for all component state to reach to destination state
        :param service_name:
        :param timeout:
        :param interval:
        :param destination_state:
        :return:
        """
        starttime = time.time()
        state_map = {}
        flag = False
        while (time.time() - starttime) < timeout and (not flag):
            time.sleep(interval)
            exit_code, stdout = cls.get_yarn_service_status(service_name)
            assert exit_code == 0, "yarn get application status failed for %s" % service_name
            datastore = json.loads(stdout)
            for comp in datastore["components"]:
                for container in comp["containers"]:
                    state = container["state"]
                    component_instance_name = container["component_instance_name"]
                    state_map[component_instance_name] = state
            if logoutput:
                logger.info("Component state map = %s", state_map)
            if all(value == "READY" for value in state_map.values()):
                return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def upload_docker_client_config_to_HDFS(cls, hdfs_dir, user=None):
        """
        upload docker config.json to HDFS
        :param hdfs_dir:
        :param user:
        :return: HDFS path to config.json
        """
        root_docker_client_config = Docker.get_docker_client_config()
        tmp_docker_client_config = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "config.json")
        Machine.copy(
            root_docker_client_config,
            tmp_docker_client_config,
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )
        Machine.chmod("777", tmp_docker_client_config, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
        HDFS.createDirectoryWithOutput(hdfs_dir, user=user, force=True)
        HDFS.copyFromLocal(tmp_docker_client_config, hdfs_dir, user=user)
        return hdfs_dir + "/config.json"

    @classmethod
    @TaskReporter.report_test()
    def update_service_definition(
            cls,
            service_description_file,
            keytab=None,
            principal=None,
            update_docker_network=True,
            docker_client_config=None,
            artifacts_id_for_httpd=None
    ):
        """
        Update json to add docker network
        :param service_description_file:
        :param update:

        "configuration": {
        "properties": {
          "docker.network": "hadoop"
        }
        },
        :return: tmpfile: location of new json file
        """
        filename = service_description_file.split("/")[-1]
        json_data = open(service_description_file).read()
        data = json.loads(json_data)
        if update_docker_network:
            data.update({"configuration": {"properties": {"docker.network": "hadoop"}}})
        if Hadoop.isSecure() and keytab and principal:
            data.update({"kerberos_principal": {"principal_name": principal, "keytab": keytab}})
        if docker_client_config:
            data.update({"docker_client_config": docker_client_config})
        if artifacts_id_for_httpd:
            data['components'][0]['artifact']['id'] = artifacts_id_for_httpd
            data['components'][1]['artifact']['id'] = artifacts_id_for_httpd

        tmpfile = cls.createTmpFile(filename)
        with open(tmpfile, 'w') as outfile:
            json.dump(data, outfile)
        return tmpfile

    @classmethod
    @TaskReporter.report_test()
    def update_sleeper_job_placement_policy(
            cls,
            service_description_file,
            number_of_containers,
            target_tags,
            constraint_type='ANTI_AFFINITY',
            scope='NODE',
            sleep_time=60
    ):
        """
        Update default sleeper service with placement constraints
        """
        filename = service_description_file.split("/")[-1]
        json_data = open(service_description_file).read()
        data = json.loads(json_data)
        components = data['components']
        sleeper_component = components[0]
        if number_of_containers:
            sleeper_component.update({'number_of_containers': number_of_containers})
        if sleep_time:
            sleeper_component.update({'launch_command': 'sleep %s' % str(sleep_time)})
        placement_policy = {}
        constraints = []
        sleeper_constraint = {}
        if constraint_type:
            sleeper_constraint.update({'type': constraint_type})
        if scope:
            sleeper_constraint.update({'scope': scope})
        tags = []
        if target_tags:
            for tag in target_tags:
                tags.append(tag)
            sleeper_constraint.update({'target_tags': tags})
            constraints.append(sleeper_constraint)
        if constraints:
            placement_policy.update({'constraints': constraints})
        sleeper_component.update({'placement_policy': placement_policy})
        components[0] = sleeper_component
        data.update({'components': components})
        updated_service_file = cls.createTmpFile(filename)
        with open(updated_service_file, 'w') as outfile:
            json.dump(data, outfile)
        return updated_service_file

    @classmethod
    @TaskReporter.report_test()
    def get_quick_links(cls, service_name, user=None):
        """
        Get quick links from yarn app status
        :param service_name:
        :return:
        """
        exit_code, stdout = cls.get_yarn_service_status(service_name, user=user)
        assert exit_code == 0, "yarn get application status failed for %s" % service_name
        datastore = json.loads(stdout)
        return datastore["quicklinks"]["HBase Master Status UI"]

    @classmethod
    @TaskReporter.report_test()
    def get_component_host_mapping(cls, service_name, user=None):
        """
        Get component-host mapping
        :param service_name: service name
        :return:{"component-name":["id", "ip", "hostname", "bare_host"]}
        """
        exit_code, stdout = cls.get_yarn_service_status(service_name, user=user)
        assert exit_code == 0, "yarn get application status failed for %s" % service_name
        datastore = json.loads(stdout)
        component_host_map = {}
        for comp in datastore["components"]:
            for container in comp["containers"]:
                component_instance_name = container["component_instance_name"]
                c_id = container["id"]
                c_ip = container["ip"] if "ip" in container else None
                hostname = container["hostname"] if "hostname" in container else None
                bare_host = container["bare_host"]
                component_host_map[component_instance_name] = [c_id, c_ip, hostname, bare_host]
        return component_host_map

    @classmethod
    @TaskReporter.report_test()
    def validate_hbase_master_url(cls, service_name, component_host_map, mode, user=None):
        """
        Validate Hbase master url accessibility for hbase yarn service
        :param service_name: yarn service name
        :param component_host_map: component-host mapping
        :param mode: docker or tarball
        :return:
        """
        quicklink = cls.get_quick_links(service_name, user=user)
        container_id = component_host_map["hbasemaster-0"][0]
        container_ip = component_host_map["hbasemaster-0"][1]
        master_bare_host = component_host_map["hbasemaster-0"][3]
        regionserver = component_host_map["regionserver-0"][2]
        msgs = [
            '<title>Master:.*%s</title>' % component_host_map["hbasemaster-0"][2],
            '<a href="//%s:16030/rs-status">%s,.*</a>' % (regionserver, regionserver)
        ]
        if mode == "docker":
            cmd = "curl %s" % quicklink
            assert cls.validate_with_docker_exec(master_bare_host, container_id, cmd, msgs, mode)
        else:
            logger.info("original quicklink = %s", quicklink)
            if not user:
                user = Config.get('hadoop', 'HADOOPQA_USER')
            quicklink = quicklink.replace(
                "hbasemaster-0.%s.%s.%s" % (
                    service_name, user.replace("_", "-"),
                    YARN.getConfigValue("hadoop.registry.dns.domain-name", "hwx.site")
                ), container_ip
            )
            logger.info("updated quicklink = %s", quicklink)
            cmd = "curl %s" % quicklink
            assert cls.validate_with_docker_exec(master_bare_host, container_id, cmd, msgs, mode)

    @classmethod
    @TaskReporter.report_test()
    def validate_dns_for_all_components(cls, component_host_map):
        """
        Validate dns check for all components
        :param component_host_map:
        :return:
        """
        for component in component_host_map.keys():
            hostname = component[2]
            assert cls.validate_dns(hostname), "dns check failed for %s" % hostname

    @classmethod
    @TaskReporter.report_test()
    def validate_hbase_app(cls, service_name, mode="docker", user=None):
        """
        Validate Hbase app ( docker and tarball)
        :param service_name:
        :param mode:
        :return:
        """
        component_host_map = cls.get_component_host_mapping(service_name, user=user)
        logger.info("********* component host map **************")
        logger.info(component_host_map)

        # validate hbase master url is accessible
        cls.validate_hbase_master_url(service_name, component_host_map, mode, user=user)

        # validate dns for hbase app
        cls.validate_dns_for_all_components(component_host_map)

    @classmethod
    @TaskReporter.report_test()
    def generate_keytab_for_yarnservice(
            cls, service_name, user, components=None, principal="EXAMPLE.COM"
    ):
        """
        /usr/sbin/kadmin.local -q 'addprinc -randkey hbase/hbasemaster-0.hbase-app-test.hbase.test.com@EXAMPLE.COM'
        /usr/sbin/kadmin.local -q 'addprinc -randkey hbase/regionserver-0.hbase-app-test.hbase.test.com@EXAMPLE.COM'
        /usr/sbin/kadmin.local -q 'addprinc -randkey HTTP/hbasemaster-0.hbase-app-test.hbase.test.com@EXAMPLE.COM'
        /usr/sbin/kadmin.local -q 'addprinc -randkey HTTP/regionserver-0.hbase-app-test.hbase.test.com@EXAMPLE.COM'
        /usr/sbin/kadmin.local -q 'xst -norandkey -k /tmp/hbase-app-test.service.keytab
          hbase/hbasemaster-0.hbase-app-test.hbase.test.com@EXAMPLE.COM
          hbase/regionserver-0.hbase-app-test.hbase.test.com@EXAMPLE.COM
          HTTP/hbasemaster-0.hbase-app-test.hbase.test.com@EXAMPLE.COM
          HTTP/regionserver-0.hbase-app-test.hbase.test.com@EXAMPLE.COM'
        :param service_name:
        :param components:
        :return: ( service keytab location, [user principal, HTTP principal]
        """
        if not components:
            components = ["hbasemaster-0", "regionserver-0"]
        domain = YARN.getConfigValue("hadoop.registry.dns.domain-name", "hwx.site")
        kadmin_local_loc = Machine.which("kadmin.local", Machine.getAdminUser())
        app_principals = []
        for comp in components:
            logger.info("create keytab for %s", comp)
            comp_principal = "%s/%s.%s.%s.%s@%s" % (user, comp, service_name, user, domain, principal)
            app_principals.append(comp_principal)
            cmd = "%s -q 'addprinc -randkey %s'" \
                  % (kadmin_local_loc, comp_principal)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())

            logger.info("create HTTP keytab for %s", comp)
            http_principal = "HTTP/%s.%s.%s.%s@%s" % (comp, service_name, user, domain, principal)
            app_principals.append(http_principal)
            cmd = "%s -q 'addprinc -randkey %s'" \
                  % (kadmin_local_loc, http_principal)
            Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())

        app_keytab = os.path.join(Machine.getTempDir(), "%s.service.keytab" % service_name)
        app_destination_keytab = os.path.join("/etc/security/keytabs", "%s.service.keytab" % service_name)

        cmd = "%s -q 'xst -norandkey -k %s %s'" % (kadmin_local_loc, app_keytab, ' '.join(map(str, app_principals)))
        Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())

        # copy application keytab to all hosts
        if Machine.isHumboldt():
            allHosts = util.getAllNodes()
        else:
            allHosts = Hadoop.getAllNodes()

        for host in allHosts:
            cmd = "chown %s:hadoop %s" % (user, app_destination_keytab)
            if not Machine.isSameHost(host):
                Machine.copyFromLocal(
                    Machine.getAdminUser(), host, app_keytab, app_destination_keytab, Machine.getAdminPasswd()
                )
                Machine.runas(Machine.getAdminUser(), cmd, host=host, passwd=Machine.getAdminPasswd())
            else:
                Machine.copy(app_keytab, app_destination_keytab, Machine.getAdminUser(), Machine.getAdminPasswd())
                Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())

        return app_destination_keytab, app_principals

    @classmethod
    def get_httpd_image(cls):
        """
        Get httpd image
        :return:
        """
        return "registry.eng.hortonworks.com/hwx-assemblies/httpd:0.1"

    @classmethod
    def get_hbase_image(cls):
        """
        Get hbase image
        :return:
        """
        return "registry.eng.hortonworks.com/hwx-assemblies/hbase:2.0.0.3.0.0.0"

    @classmethod
    @TaskReporter.report_test()
    def get_docker_dshell_env(cls, enable_delayed_removal=True):
        """
        Get docker dshell app env
        :return:
        """
        envs = ["YARN_CONTAINER_RUNTIME_TYPE=docker", "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=" + cls.get_httpd_image()]
        if enable_delayed_removal:
            envs.append("YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL=true")
        return envs

    @classmethod
    def get_docker_privileged_dshell_env(cls):
        """
        Get privileged docker dshell app env
        :return:
        """
        return [
            "YARN_CONTAINER_RUNTIME_TYPE=docker", "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=" + cls.get_hbase_image(),
            "YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER=true"
        ]

    @classmethod
    @TaskReporter.report_test()
    def inspect_docker_container(
            cls, containerId, containerHost, checkStatus=True, expectFailure=False, expectStatus=None
    ):
        """
        Run docker inspect for an container and validate if container is alive
        :param containerId: docker container Id
        :param containerHost: Host where docker container is running
        :param checkStatus: Validate status of the running docker container
        :param expectFailure: docker container is expected to be dead. Validate docker inspect fails
        :return:
        """
        exit_code, stdout = Docker.inspect(containerId, host=containerHost)
        if expectFailure:
            if expectStatus == "Exited":
                data = json.loads(stdout)
                assert data[0]["State"]["Status"] == "exited", "docker %s is not running" % containerId
                return exit_code, stdout
            else:
                assert exit_code != 0, "docker %s did not die" % containerId
                assert stdout.find("Error: No such object: " + containerId) >= 0, "docker %s did not die" % containerId
                return exit_code, stdout
        else:
            assert exit_code == 0, "docker inspect %s failed" % containerId
            data = json.loads(stdout)

            if checkStatus:
                assert data[0]["State"]["Status"] == "running", "docker %s is not running" % containerId
                assert data[0]["State"]["ExitCode"] == 0, "docker %s exitcode is non zero" % containerId
            return exit_code, data

    @classmethod
    @TaskReporter.report_test()
    def get_docker_container_list(
            cls,
            appId,
            expected_docker_container_count=0,
            timeout=120,
            interval=5,
            ignore_exepected_docker_container_count=False,
            user=None,
            multiple_am_attempts=False,
            check_for_finished_app=False
    ):
        """
        Get non AM docker container list
        :param appId: application id
        :param timeout: timeout to wait for docker containers
        :param interval: 5 sec interval between iterations
        :param ignore_exepected_docker_container_count: ignore expected docker count
        and get the container list found during timeout
        :return: Its a dict of attempt number and list of containers
        (0, ['container_1521814979616_0016_01_000002', 'container_1521814979616_0016_01_000001'])
        """
        starttime = time.time()
        docker_container_count = 0
        docker_container_list = []

        while (time.time() - starttime) < timeout:
            if not ignore_exepected_docker_container_count and \
                    docker_container_count >= expected_docker_container_count:
                break
            if not check_for_finished_app:
                if YARN.getAppStateFromID(appId) in ["FAILED", "KILLED", "SUCCEEDED", "FINISHED"]:
                    break
            docker_container_list = YARN.get_containers(appId, user=user)
            am_attempts = YARN.getAttemptIdList(appId, user)
            if multiple_am_attempts:
                assert len(am_attempts) == 2, "%s has more than 2 am attempts" % appId
            for am_attempt in am_attempts:
                am_container = YARN.getAMContainerIdFromattempt(appId, am_attempt, user)
                if am_container in docker_container_list:
                    docker_container_list.remove(am_container)
            docker_container_count = len(docker_container_list)
            time.sleep(interval)
        if not ignore_exepected_docker_container_count:
            assert docker_container_count == expected_docker_container_count, \
                "expected docker container count not be matched"
        return docker_container_list

    @classmethod
    def kill_docker_container(cls, containerId):
        """
        Kill docker container
        :param containerId:
        :return:
        """
        host = MAPRED.getContainerHost(containerId)
        return Docker.kill(containerId, host=host)

    @classmethod
    @TaskReporter.report_test()
    def is_docker_container_alive(cls, containerId, host=None, expectFailure=False, expectStatus=None):
        """
        run docker ps command and check if container is alive
        :param containerId:
        :return:
        """
        if not host:
            host = MAPRED.getContainerHost(containerId)
        options = " -a | grep %s" % containerId
        exit_code, stdout = Docker.ps(options, host=host)

        logger.info(exit_code)
        logger.info(stdout)
        exit_line = "Exited.*%s" % containerId
        if expectFailure:
            if expectStatus == "Exited":
                assert cls.validate_msg(exit_line, stdout), "container %s exit message not present" % containerId
                return True
            else:
                assert exit_code != 0, "container %s should not be listed in docker ps" % containerId
                return True
        else:
            assert exit_code == 0, "exitcode is non zero for docker ps -a cmd"
        lines = stdout.split("\n")
        for line in lines:
            line = line.decode('utf-8').strip()
            if line.find(containerId) >= 0:
                if cls.validate_msg(exit_line, stdout):
                    return False
                else:
                    return True
        return False

    @classmethod
    @TaskReporter.report_test()
    def validate_docker_containers_presence(
            cls,
            docker_container_list,
            alive_check=True,
            expectAliveFailure=False,
            inspect_check=False,
            checkInspectStatus=True,
            expectInspectFailure=False,
            check_container_status=False,
            user=None,
            expectStatus=None
    ):
        """
        Validate docker container presence with "docker ps" and "docker inspect"
        :param docker_container_list: docker container list
        :param alive_check: check if container is alive
        :param expectAliveFailure: expect container alive_check to fail
        :param inspect_check: check container inspect
        :param checkInspectStatus: validate status of the container
        :param expectInspectFailure: expect the status of the container to fail
        :param check_container_status: check container status
        :return:
        """
        # validate docker container is UP when application is in Running state
        for containerId in docker_container_list:
            host = MAPRED.getContainerHost(containerId)
            if alive_check:
                if check_container_status:
                    container_status = YARN.getContainerState(containerId, user=user)
                    assert container_status in ["RUNNING"], "%s is not in RUNNING STATE"
                assert cls.is_docker_container_alive(containerId, host, expectFailure=expectAliveFailure,
                                                     expectStatus=expectStatus), \
                    "docker container: %s not alive" % containerId
            if inspect_check:
                _, _ = cls.inspect_docker_container(
                    containerId,
                    host,
                    checkStatus=checkInspectStatus,
                    expectFailure=expectInspectFailure,
                    expectStatus=expectStatus
                )
        return True

    @classmethod
    @TaskReporter.report_test()
    def add_prop_container_executor(cls, line=None, container_exec_location=None):
        """
        Update container_executor [docker] section
        :param line:
        :param container_exec_location:
        :return:
        """
        if not line:
            line = "  docker.privileged-containers.registries=registry.eng.hortonworks.com,centos,hadoop-docker"
        if not container_exec_location:
            container_exec_location = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), 'container-executor.cfg')

        # create local file which can have modified container executor
        localDir = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "container_executor_dir")
        Machine.makedirs(user=None, host=None, filepath=localDir)
        localContainerCfgFile = os.path.join(localDir, 'container-executor.cfg')
        Machine.copy(
            container_exec_location,
            localContainerCfgFile,
            user=Machine.getAdminUser(),
            passwd=Machine.getAdminPasswd()
        )
        cmd = r"sed '/^\[docker\]/a\%s' %s > %s" % (line, container_exec_location, localContainerCfgFile)
        Machine.runas(user=Machine.getAdminUser(), cmd=cmd, host=None, passwd=Machine.getAdminPasswd())

        # update container executor on all NMs
        for nm in MAPRED.getTasktrackers():
            Machine.copyFromLocal(
                user=Machine.getAdminUser(),
                host=nm,
                srcpath=localContainerCfgFile,
                destpath=container_exec_location,
                passwd=Machine.getAdminPasswd()
            )

    @classmethod
    @TaskReporter.report_test()
    def validate_msg(cls, msg, output):
        """
        Validate if msg is present in output
        :param msg:
        :param output:
        :return:
        """
        m = re.search(msg, output)
        return bool(m)

    @classmethod
    @TaskReporter.report_test()
    def verify_stop_service(cls, service_name, user=None, expect_failure=False, msg="Successfully stopped service"):
        """
        Verify stop application cmd
        :param service_name: service name
        :param user: user which need to stop service
        :param expect_failure: if expect_failure = True, exit_code is expected to be non zero otherwise zero
        :param msg: expected msg to be present in stdout
        :return:
        """
        exit_code, stdout = cls.stop_yarn_service(service_name, user)
        if expect_failure:
            assert exit_code != 0
        else:
            assert exit_code == 0
        assert cls.validate_msg(msg, stdout), "msg: %s missing from output" % msg

    @classmethod
    @TaskReporter.report_test()
    def verify_destroy_service(
            cls, service_name, user=None, expect_failure=False, msg="Successfully destroyed service"
    ):
        """
        Verify destroy service cmd
        :param service_name: service name
        :param user: user which need to stop service
        :param expect_failure: if expect_failure = True, exit_code is expected to be non zero otherwise zero
        :param msg: expected msg to be present in stdout
        :return:
        """
        exit_code, stdout = cls.destroy_yarn_service(service_name, user)
        if expect_failure:
            assert exit_code != 0
        else:
            assert exit_code == 0
        assert cls.validate_msg(msg, stdout), "msg: %s missing from output" % msg

    @classmethod
    @TaskReporter.report_test()
    def get_component_instance(cls, app_id, user=None):
        """
        Get component instance data from ats v2
        :param app_id:
        :return:
        """
        status_code, entities = YARN.ats_v2_get_generic_entities_within_scope_of_app(
            app_id=app_id, entity_type=YARN.YARN_COMPONENT_INSTANCE, fields="ALL", user_id=user
        )
        assert status_code in [200, 202]
        return status_code, entities

    @classmethod
    @TaskReporter.report_test()
    def get_component_instance_launch_time_map(cls, app_id, user=None):
        """
        Get component instance and launch time map
        :param app_id: appId for yarn service
        :return:
        """
        comp_launch_time_map = {}
        _, entities = cls.get_component_instance(app_id, user=user)
        for entity in entities:
            component_name = entity["info"]["COMPONENT_INSTANCE_NAME"]
            launch_time = entity["info"]["LAUNCH_TIME"]
            comp_launch_time_map[component_name] = launch_time
        return comp_launch_time_map

    @classmethod
    @TaskReporter.report_test()
    def validate_launch_time_for_sleeper(cls, app_id, user=None):
        """
        Validate launch time for sleeper job.
        Sleeper-P0 component should be launched before sleeper-P1
        :param app_id:
        :return:
        """
        comp_launch_time_map = cls.get_component_instance_launch_time_map(app_id, user=user)
        assert comp_launch_time_map["sleeper-p0-0"] <= comp_launch_time_map["sleeper-p1-0"]
        assert comp_launch_time_map["sleeper-p0-1"] <= comp_launch_time_map["sleeper-p1-0"]

    @classmethod
    @TaskReporter.report_test()
    def validate_launch_time_for_hbase(cls, app_id, user=None):
        """
        Validate launch time for hbase
        :param app_id:
        :return:
        """
        comp_launch_time_map = cls.get_component_instance_launch_time_map(app_id, user=user)
        assert comp_launch_time_map["hbasemaster-0"] <= comp_launch_time_map["regionserver-0"]

    @classmethod
    @TaskReporter.report_test()
    def check_user_for_privileged_container(cls, user, useAmbari=True):
        """
        Validate is user is present in yarn.nodemanager.runtime.linux.docker.privileged-containers.acl
        :param user:
        :return:
        """
        priviledged_user = YARN.getConfigValue(
            "yarn.nodemanager.runtime.linux.docker.privileged-containers.acl", useAmbari=useAmbari
        )
        priviledged_user_list = priviledged_user.split(",")
        return bool(user in priviledged_user_list)

    @classmethod
    @TaskReporter.report_test()
    def check_dns_zk_secure_communication(cls):
        """
        Check if dns is talking to ZK securely
        :return:
        """
        if Hadoop.isSecure():
            local_yr_logfile = cls.copy_registryserver_log_to_local()
            pattern = "Using existing ZK sasl configuration: jaasClientEntry = Client, sasl client = true, jaas = " \
                      "/etc/hadoop/[0-9]+.[0-9]+.[0-9]+.[0-9]+-[0-9]+/0/yarn_registry_dns_jaas.conf"
            result = util.findMatchingPatternInFile(local_yr_logfile, pattern)
            return bool(result == 0)
        else:
            return False

    @classmethod
    @TaskReporter.report_test()
    def pre_setup_for_hbase_app(cls, service_name, hdfs_dir, user=None, mode="docker"):
        """
        Pre setup for Hbase Tarball app
        :param service_name: service name
        :param hdfs_dir: hdfs dir where hbase tarball , config file need to be updated
        :param user: user
        :return:
        """
        if mode == "tarball":
            # Generate app keytabs
            cls.generate_keytab_for_yarnservice(service_name, user)

        # upload hbase tarball and log4j to hdfs
        HDFS.copyFromLocal(Yarnservice.get_hbase_tarball(), hdfs_dir, user=user)
        HDFS.copyFromLocal(Yarnservice.get_hbase_log4j(), hdfs_dir, user=user)

        # updload hdfs-site and core-site to hdfs
        HDFS.copyFromLocal(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hdfs-site.xml"), hdfs_dir, user=user)
        HDFS.copyFromLocal(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "core-site.xml"), hdfs_dir, user=user)

    @classmethod
    @TaskReporter.report_test()
    def is_service_killed(cls, apps):
        """
        Check if all applications are killed
        :param apps: apps is a dict. apps["servicename"] = appId
        :return:
        """
        for service in apps:
            if YARN.getAppStateFromID(apps[service]) != "KILLED":
                return False
        return True

    @classmethod
    @TaskReporter.report_test()
    def validate_am_attempt_count(cls, appId, am_attempt_count=1, user=None):
        """
        Validate total am attempt count for appId
        :param appId:
        :param am_attempt_count:
        :param user:
        :return:
        """
        am_attempts = YARN.getAttemptIdList(appId, user)
        assert len(am_attempts) == am_attempt_count, \
            "Expected Am count=%s and actual AM count=%s do not match" % (am_attempt_count, len(am_attempts))

    @classmethod
    @TaskReporter.report_test()
    def validate_component_attempt_count(cls, appId, expected_container_count=range(1, 1), user=None):
        """
        Validate expected component count
        :param appId:
        :param expected_container_count:
        :param user:
        :return:
        """
        docker_container_list = YARN.get_containers(appId, user=user)
        am_attempts = YARN.getAttemptIdList(appId, user)
        for am_attempt in am_attempts:
            am_container = YARN.createContainerIdFromAttemptId(am_attempt, 1)
            if am_container in docker_container_list:
                docker_container_list.remove(am_container)
        assert len(docker_container_list) in expected_container_count, \
            "Expected component count=%s and Actual component count=%s" \
            % (expected_container_count, len(docker_container_list))

    @classmethod
    @TaskReporter.report_test()
    def workaround_for_registryui(cls, hosts):
        """
        Add workaround for BUG-104840
        :return:
        """
        for host in hosts:
            cmd = "echo -e '172.27.0.229\tregistry.eng.hortonworks.com' >> /etc/hosts"
            Machine.runas(Machine.getAdminUser(), cmd, host=host, passwd=Machine.getAdminPasswd())
            cmd = "echo -e '172.27.0.229\tregistryui.eng.hortonworks.com' >> /etc/hosts"
            Machine.runas(Machine.getAdminUser(), cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    @TaskReporter.report_test()
    def gather_app_logs(cls, app_id, user):
        """
        Gather app logs and save it in YarnServiceLog dirs
        :param appId:
        :return:
        """
        logfile = Yarnservice.createTmpFile("%s.log" % app_id)
        YARN.getLogsApplicationID(app_id, appOwner=user, pipeToFileOutput=logfile)
        Machine.chmod("777", logfile, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())

    @classmethod
    @TaskReporter.report_test()
    def get_spark_example_image(cls):
        """
        Get spark scala image
        :return:
        """
        return "registry.eng.hortonworks.com/hwx-assemblies/spark-examples-pyspark:0.1.0"

    @classmethod
    @TaskReporter.report_test()
    def get_spark_R_example_image(cls):
        """
        Get spark R image
        :return:
        """
        return "registry.eng.hortonworks.com/hwx-assemblies/spark-examples-r:0.1.0"

    @classmethod
    @TaskReporter.report_test()
    def get_spark_mount(cls, execution_mode="yarn-client"):
        """
        Get spark mount dir
        :return:
        """
        if Hadoop.isSecure():
            if Hadoop.isEncrypted():
                spark_all = Spark.getPropertyValueFromSparkDefaultConfFile(Spark.getSparkDefaultConfFile(),
                                                                           "spark.ssl.trustStore")
                spark_keystore = Spark.getPropertyValueFromSparkDefaultConfFile(Spark.getSparkDefaultConfFile(),
                                                                                "spark.ssl.keyStore")
                if execution_mode == "yarn-client":
                    return r"/etc/passwd:/etc/passwd:ro\,/etc/hadoop/conf:/etc/hadoop/conf:ro\," \
                           r"/etc/krb5.conf:/etc/krb5.conf:ro\,%s:%s:ro" \
                           % (spark_all, spark_all)
                else:
                    return r"/etc/passwd:/etc/passwd:ro\,/etc/krb5.conf:/etc/krb5.conf:ro\," \
                           r"/etc/hadoop/conf:/etc/hadoop/conf:ro\,%s:%s:ro\,%s:%s:ro" \
                           % (spark_all, spark_all, spark_keystore, spark_keystore)
            else:
                return r"/etc/passwd:/etc/passwd:ro\,/etc/krb5.conf:/etc/krb5.conf:ro\," \
                       r"/etc/hadoop/conf:/etc/hadoop/conf:ro"
        else:
            return "/etc/passwd:/etc/passwd:ro"

    @classmethod
    @TaskReporter.report_test()
    def get_spark_on_docker_conf(cls, docker_image=None, execution_mode="yarn-client", shuffle=False,
                                 return_as_str=True):
        """
        Get spark on docker conf set
        :return:
        """
        if not docker_image:
            docker_image = cls.get_spark_example_image()

        spark_conf = {
            "spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE": "docker",
            "spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE": docker_image,
            "spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK": "host",
            "spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS": cls.get_spark_mount(execution_mode)
        }

        if execution_mode == "yarn-cluster":
            spark_conf["spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE"] = "docker"
            spark_conf["spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] = docker_image
            spark_conf["spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK"] = "host"
            spark_conf["spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS"] = \
                cls.get_spark_mount(execution_mode)

        if shuffle:
            spark_conf["spark.shuffle.service.enabled"] = "true"

        if return_as_str:
            spark_conf_str = ""
            for key, value in spark_conf.items():
                spark_conf_str = key + "=" + value + "," + spark_conf_str
            spark_conf_str = spark_conf_str[:-1]
            return spark_conf_str

        return spark_conf
