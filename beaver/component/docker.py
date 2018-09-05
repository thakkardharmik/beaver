import logging
from beaver.machine import Machine

logger = logging.getLogger(__name__)
'''
Description:
Test API to work with Docker cmds
'''


class Docker(object):
    def __init__(self):
        pass

    @classmethod
    def get_docker_home(cls):
        """
        Get default docker home
        :return:
        """
        return "/usr/bin/docker"

    @classmethod
    def get_group(cls):
        """
        Get docker group
        :return:
        """
        return "docker"

    @classmethod
    def get_docker_client_config(cls):
        """
        Get docker client config file
        :return:
        """
        return "/root/.docker/config.json"

    @classmethod
    def run(cls, options, image, cmd, host=None):
        """
        launch docker run
        :param options:
        :param image:
        :param command:
        :return:
        """
        run_cmd = "docker run %s %s %s" % (options, image, cmd)
        return Machine.runas(Machine.getAdminUser(), run_cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    def execute(cls, container, cmd, options=None, host=None):
        """
        run docker exec
        :param container:
        :param cmd:
        :param options:
        :param host:
        :return:
        """
        exec_cmd = "docker exec "
        if options:
            exec_cmd += options
        exec_cmd += " %s %s" % (container, cmd)
        return Machine.runas(Machine.getAdminUser(), exec_cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    def inspect(cls, container_name, options=None, host=None):
        """
        run docker inspect
        :param container_name:
        :param options:
        :param host:
        :return:
        """
        inspect_cmd = "docker inspect "
        if options:
            inspect_cmd += options
        inspect_cmd += " " + container_name
        return Machine.runas(Machine.getAdminUser(), inspect_cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    def kill(cls, container_name, options=None, host=None):
        """
        kill docker container
        :param container_name:
        :param options:
        :param host:
        :return:
        """
        kill_cmd = "docker kill "
        if options:
            kill_cmd += options
        kill_cmd += " " + container_name
        return Machine.runas(Machine.getAdminUser(), kill_cmd, host=host, passwd=Machine.getAdminPasswd())

    @classmethod
    def ps(cls, options=None, host=None):
        """
        list docker container
        :param options:
        :param host:
        :return:
        """
        ps_cmd = "docker ps "
        if options:
            ps_cmd += options
        return Machine.runas(Machine.getAdminUser(), ps_cmd, host=host, passwd=Machine.getAdminPasswd())
