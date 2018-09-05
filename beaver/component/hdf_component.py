#
# Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os
import re
import socket
import tempfile

from beaver import util
from beaver.component.ambari import Ambari
from beaver.machine import Machine

LOGGER = logging.getLogger(__name__)


class HdfComponent:
    ambari_server_properties = '/etc/ambari-server/conf/ambari.properties'
    _ambari_server_ssl = None
    _ambari_server_host = None
    _ambari_server_port = None
    _gwHost = "localhost"
    _nifi_test_data_path = os.path.join(tempfile.gettempdir(), 'nifi-test-data-ycloud')
    _repo_file = None
    _repo_base_url = None

    @classmethod
    def get_ambari_cluster_name(cls):
        return Ambari.getClusterName(is_hdp=False, is_enc=cls.get_ambari_server_ssl())

    @classmethod
    def get_ambari_server_ssl(cls):
        if cls._ambari_server_ssl is None:
            cls._ambari_server_ssl = Ambari.is_ambari_encrypted()
        return cls._ambari_server_ssl

    @classmethod
    def get_ambari_server_host(cls):
        if not cls._ambari_server_host:
            cls._ambari_server_host = Ambari.getHost()

        return cls._ambari_server_host

    @classmethod
    def get_ambari_server_scheme(cls):
        if cls.get_ambari_server_ssl() is True:
            return 'https'
        return 'http'

    @classmethod
    def get_ambari_server_port(cls):
        if not cls._ambari_server_port:
            cls._ambari_server_port = util.getPropertyValueFromFile(
                cls.ambari_server_properties, "client.api.ssl.port"
            ) if cls.get_ambari_server_ssl() else '8080'

        return cls._ambari_server_port

    @classmethod
    def get_gateway(cls):
        if cls._gwHost == "localhost":
            if Machine.isWindows():
                # always use short hostname in Windows
                return Machine.getfqdn(name="")
            else:
                cls._gwHost = socket.gethostbyname(socket.gethostname())
        return cls._gwHost

    @classmethod
    def is_hdf_cluster(cls):
        """ Check if this is a hdf cluster. Note: a cluster can be both hdp & hdf cluster. """
        return os.path.exists("/usr/hdf")

    @classmethod
    def get_shared_keytabs_dir(cls):
        """
        Assuming shared keytabs are present at /tmp/nifi-test-data-ycloud.
        This method will change the machine.KEYTAB_FILE_DIR to this value.
        To change keytabs dir config set:
        Config.set('machine', 'KEYTAB_FILES_DIR', HdfComponent.get_shared_keytabs_dir(), overwrite=True)
        :return:
        """
        return cls._nifi_test_data_path

    @classmethod
    def get_repo_file(cls):
        """
        :return: Get the location of the repo file for the given host. If it runs on an OS that is not supported or on windows this will return None.
        """
        if Machine.isWindows():
            return None
        if not cls._repo_file:
            if Machine.isCentOs():
                cls._repo_file = '/etc/yum.repos.d/ambari-hdf-1.repo'
            elif Machine.isSuse():
                cls._repo_file = '/etc/zypp/repos.d/ambari-hdf-1.repo'
            elif Machine.isUbuntu() or Machine.isDebian():
                cls._repo_file = '/etc/apt/sources.list.d/ambari-hdf-1.list'

        return cls._repo_file

    @classmethod
    def get_repo_base_url(cls):
        """

        :return: Get the location of the base url for the repo. If it runs on an OS that is not supported or on windows this will return None.
        """
        if not cls._repo_base_url:
            repo_file = cls.get_repo_file()
            if repo_file:
                # QE-16037: Make sure file is readable.
                Machine.chmod("755", repo_file, False, Machine.getAdminUser(), None, Machine.getAdminPasswd())
                file = open(repo_file, 'r')
                content = file.read()
                file.close()

                if Machine.isCentOs() or Machine.isSuse():
                    m = re.search('^baseurl=(.*HDF.*)', content, re.MULTILINE)
                    if m and m.group(1):
                        cls._repo_base_url = m.group(1)
                elif Machine.isDebian() or Machine.isUbuntu():
                    m = re.search('^deb (.*) HDF main', content, re.MULTILINE)
                    if m and m.group(1):
                        cls._repo_base_url = m.group(1)

        return cls._repo_base_url
