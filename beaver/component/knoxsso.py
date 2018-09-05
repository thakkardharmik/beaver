###############################################################################
#
#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
###############################################################################
import sys
import os
import xml.dom.minidom
import xml.etree.ElementTree as ET
import pytest
import logging
import re
import time
import ssl
from beaver import util
from beaver.component.hadoop import Hadoop, HDFS
from beaver.component.knox import Knox
from beaver.machine import Machine
from beaver.component.xa import Xa
from beaver.config import Config
from beaver.component.ambari import Ambari
from beaver.component.xa_ambari_api_util import AmbariAPIUtil
from beaver.component.ambari_apilib import APICoreLib
from taskreporter.taskreporter import TaskReporter
from functools import wraps

###############################################################################
logger = logging.getLogger(__name__)

###############################################################################
SETUP_TIME = 60
SHORT_TEST_TIMEOUT = SETUP_TIME + 1 * 60
MEDIUM_TEST_TIMEOUT = SETUP_TIME + 3 * 60
LONG_TEST_TIMEOUT = SETUP_TIME + 9 * 60

CONF = {}
CONF['KNOX_GUEST_USERNAME'] = "guest"
CONF['KNOX_GUEST_PASSWORD'] = "guest-password"
CONF['KNOX_PROTO'] = "https"
CONF['KNOX_HOST'] = Config.get('knox', 'KNOX_HOST').split(',')[0]
CONF['AMBARI_HOST'] = Machine.getfqdn()
CONF['KNOX_PORT'] = 8443
CONF['AMBARI_PORT'] = 8080
CONF['KNOX_TOPO'] = "sandbox"
CONF['KNOX_BASE_URL'] = "%s://%s:%s/gateway" % (CONF['KNOX_PROTO'], CONF['KNOX_HOST'], CONF['KNOX_PORT'])
CONF['KNOX_TOPO_URL'] = "%s/%s" % (CONF['KNOX_BASE_URL'], CONF['KNOX_TOPO'])
CONF['KNOX_WEBHDFS_URL'] = "%s/%s/webhdfs/v1/" % (CONF['KNOX_BASE_URL'], CONF['KNOX_TOPO'])
CONF['DIRECT_WEBHDFS_URL'] = "http://%s/webhdfs/v1/" % (HDFS.getNamenodeHttpAddress())
CONF['RANGER_KNOX_POLICY'] = None

#CONF['SRC_DIR'] = os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'knox', 'knox_2')
knox_host = CONF['KNOX_HOST']
if Machine.isOpenStack():
    knox_host = knox_host + ".com"
KNOXSSO_PROVIDER_URL = "%s://%s:%s/gateway/knoxsso/api/v1/websso" % (CONF['KNOX_PROTO'], knox_host, CONF['KNOX_PORT'])
CLUSTER_NAME = Ambari.getClusterName(is_enc=Hadoop.isEncrypted())
KNOX_TRUSTSTORE_PASSWORD = "knoxsecret"
KNOX_KEYSTORE_PATH = "/usr/hdp/current/knox-server/data/security/keystores/"
KNOX_TOPOLOGY_DIR = "/etc/knox/conf/topologies/"

JAVA_HOME = Config.get("machine", "QA_CODE_JAVA_HOME")

_workspace = Config.getEnv('WORKSPACE')
_artifact = Config.getEnv('ARTIFACTS_DIR')

global apicorelib
DEPLOY_CODE_DIR = os.path.join(Config.getEnv('WORKSPACE'), '..', 'ambari_deploy')
uifrm_folder = "uifrm_old/uifrm"
amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder, 'ambari.properties')
if not os.path.isfile(amb_prop_file):
    uifrm_folder = "uifrm"
amb_prop_file = os.path.join(DEPLOY_CODE_DIR, uifrm_folder)
apicorelib = APICoreLib(amb_prop_file)

if Hadoop.isEncrypted():
    ambari_url = Ambari.getWebUrl(is_enc=True, hostname=CONF['AMBARI_HOST'])
else:
    ambari_url = Ambari.getWebUrl(is_enc=False, hostname=CONF['AMBARI_HOST'])


@TaskReporter.report_test()
def setup_for_ranger():
    global ranger_orig_url, admin_prop_loc
    if (isRangerInstalled()):
        CONF['XA_ADMIN_HOST'] = Ambari.getConfig(
            'admin-properties', webURL=ambari_url
        )['policymgr_external_url'].split('//', 1)[1].split(':', 1)[0]
        if Hadoop.isEncrypted():
            CONF['XA_ADMIN_PORT'] = Ambari.getConfig(
                'ranger-admin-site', webURL=ambari_url
            )['ranger.service.https.port']
        else:
            CONF['XA_ADMIN_PORT'] = Ambari.getConfig(
                'ranger-admin-site', webURL=ambari_url
            )['ranger.service.http.port']
        admin_prop_loc = os.path.join(_workspace, _artifact, "xaagents-knoxsso", "knox-sso-ui", "Test.properties")
        ranger_orig_url = "HOST=http://" + CONF['XA_ADMIN_HOST'] + ":" + str(CONF['XA_ADMIN_PORT']
                                                                             ) + "' " + admin_prop_loc


def isRangerInstalled():
    if Config.get('xasecure', 'XA_INSTALLED') == 'yes':
        return True
    else:
        return False


newAmbariUtil = AmbariAPIUtil()

if Config.get('xasecure', 'XA_INSTALLED') == 'yes':
    setup_for_ranger()


###############################################################################
def sslwrap(func):
    @wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLSv1
        return func(*args, **kw)

    return bar


ssl.wrap_socket = sslwrap(ssl.wrap_socket)


###############################################################################
@TaskReporter.report_test()
def setup_Add_TLD():
    if Machine.isOpenStack():
        knoxsso_awk_file = os.path.join(_workspace, "data", "knox", "awk_cmd_os.sh")
    elif Machine.isYCloud():
        knoxsso_awk_file = os.path.join(_workspace, "data", "knox", "awk_cmd_yc.sh")
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    #whitelist openstacklocal.com from proxy
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="export no_proxy=\$no_proxy,.openstacklocal.com,.site.com",
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    host_list = "/tmp/all_internal_nodes"
    with open(host_list, 'r') as mfile:
        hosts = mfile.readlines()
    lines = [line.rstrip('\n') for line in open(host_list)]
    for rhost in lines:
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="scp -o StrictHostKeyChecking=no -r -i /root/ec2-keypair %s root@%s:/tmp/" % (knoxsso_awk_file, rhost),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        if Machine.isOpenStack():
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="bash /tmp/awk_cmd_os.sh",
                host=rhost,
                cwd=None,
                env=None,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )
        elif Machine.isYCloud():
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="bash /tmp/awk_cmd_yc.sh",
                host=rhost,
                cwd=None,
                env=None,
                logoutput=True,
                passwd=Machine.getAdminPasswd()
            )


###############################################################################
@TaskReporter.report_test()
def setup_cert_local_truststore():
    logger.info("==============================Setting up trust store=============================")
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "openssl s_client -connect %s:%s < /dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > /tmp/knox.crt"
        % (CONF['KNOX_HOST'], CONF['KNOX_PORT']),
        host=CONF['KNOX_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    #Machine.runas(user=Machine.getAdminUser(), cmd="%s/bin/keytool -import -alias knoxsso -keystore %s/jre/lib/security/cacerts -storepass changeit -file /tmp/knox.crt -noprompt" % (JAVA_HOME, JAVA_HOME), host=CONF['KNOX_HOST'], cwd=None, env=None, logoutput=True, passwd=Machine.getAdminPasswd())


###############################################################################
'''
1.Get the topology file from Ambari
2.Parse the XML string and set TTL value
3.Set the modified XML.
4.Restart Knox
'''


@TaskReporter.report_test()
def modify_knox_sso_ttl(ttl):
    '''
    :param ttl: TTL value to be set in knoxsso-topology file.
    :return: None
    '''
    content = apicorelib.get_service_config(serviceName="KNOX", config_type="knoxsso-topology", config="content")
    import xml.etree.ElementTree as ET
    doc = ET.fromstring(content)
    service_node = doc.find('service')

    for node in service_node:
        if node.tag == "param":
            for params in node:
                if params.tag == "name":
                    param_text = params.text
                if params.tag == "value" and param_text == "knoxsso.token.ttl":
                    params.text = str(ttl)

    propsToSet = {
        'content': ET.tostring(doc),
    }

    apicorelib.modify_service_configs(serviceName="KNOX", config_type="knoxsso-topology", configs_dict=propsToSet)
    Ambari.start_stop_service(service="KNOX", state="INSTALLED", waitForCompletion=True)
    Ambari.start_stop_service(service="KNOX", state="STARTED", waitForCompletion=True)


@TaskReporter.report_test()
def restoreRangerHost():
    logger.info("============================== Restoring Ranger Host =============================")
    if Hadoop.isEncrypted():
        orig_url = "HOST=https:\/\/" + CONF['XA_ADMIN_HOST'] + ":6182/' " + admin_prop_loc
    else:
        orig_url = "HOST=http:\/\/" + CONF['XA_ADMIN_HOST'] + ":6080/' " + admin_prop_loc
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="sed -i '0,/^HOST.*/s//%s" % (orig_url),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )


###############################################################################
@TaskReporter.report_test()
def setup_KnoxSSO_basic_module():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    #Step 1: Import Knox Certificate into Java Keystore
    # $ openssl s_client -connect knoxsso-kpandey-erietp-1.novalocal:8443 < /dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > knoxssoRanger.crt
    # keytool -import -alias knoxsso-kpandey-erietp-1.novalocal -keystore /usr/jdk64/jdk1.8.0_40/jre/lib/security/cacerts -storepass changeit -file knoxssoRanger.crt -noprompt
    #Step 2: Set "Advanced knoxsso-topology" with contents from knoxssobasic.xml in resources directory using configs.sh
    #Step 3: Update "knoxsso-topology" property with KnoxSSO.xml using configs.sh

    knoxsso_topo_file = os.path.join(_workspace, "data", "knox", "knoxssobasic.xml")
    with open(knoxsso_topo_file, 'r') as myfile:
        #knoxsso_topo_form=myfile.read().replace('\n', '').replace('"','\'')
        knoxsso_topo_basic = myfile.read().replace('"', '\'')
    knoxsso_topo_default = os.path.join(_workspace, "data", "knox", "defaultknoxssobasic.xml")
    with open(knoxsso_topo_default, 'r') as myfile:
        knoxsso_topo_def = myfile.read().replace('"', '\'')

    if Hadoop.isEncrypted():
        port = "8443"
        protocol = "https"
    else:
        port = "8080"
        protocol = "http"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type knoxsso-topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_basic),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_def),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    newAmbariUtil.restart_service("KNOX")
    Knox.restartLdap()

    if isRangerInstalled():
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i '0,/^KNOX_AUTH.*/s//KNOX_AUTH=basic/' %s" % admin_prop_loc,
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

    logger.info("Knox Restarted after Basic Auth setup")
    Knox.logConfig(CONF)


################ GENERIC METHOD FOR KNOX SSO FORM LOGIN ######################################################
@TaskReporter.report_test()
def setup_KnoxSSO_form_module():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    #read knoxssoform.xml(has whitelist) and set using configs.sh
    #read defaultknoxssoform.xml, read and set
    #restart Knox & ldap
    knoxsso_topo_file = os.path.join(_workspace, "data", "knox", "knoxssoform.xml")
    with open(knoxsso_topo_file, 'r') as myfile:
        knoxsso_topo_form = myfile.read().replace('\n', '').replace('"', '\'')

    knoxsso_topo_default = os.path.join(_workspace, "data", "knox", "defaultknoxssoform.xml")
    with open(knoxsso_topo_default, 'r') as myfile:
        knoxsso_topo_def = myfile.read().replace('\n', '').replace('"', '\'')
    if Hadoop.isEncrypted():
        port = "8443"
        protocol = "https"
    else:
        port = "8080"
        protocol = "http"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type knoxsso-topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_form),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_def),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    if isRangerInstalled():
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i '0,/^HOST.*/s//%s" % ranger_orig_url,
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i '0,/^KNOX_AUTH.*/s//KNOX_AUTH=form/' %s" % admin_prop_loc,
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
    newAmbariUtil.restart_service("KNOX")
    Knox.restartLdap()
    logger.info("Knox Restarted after Form based Auth setup")


################################################################################
def setup_KnoxSSO_okta_module():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )


################################################################################
@TaskReporter.report_test()
def setup_KnoxSSO_basic_wlist_module():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    knoxsso_topo_file = os.path.join(_workspace, "data", "knox", "knoxssobasicwlist.xml")
    with open(knoxsso_topo_file, 'r') as myfile:
        #knoxsso_topo_form=myfile.read().replace('\n', '').replace('"','\'')
        knoxsso_topo_basic = myfile.read().replace('"', '\'')
    knoxsso_topo_default = os.path.join(_workspace, "data", "knox", "defaultknoxssobasic.xml")
    with open(knoxsso_topo_default, 'r') as myfile:
        knoxsso_topo_def = myfile.read().replace('"', '\'')

    if Hadoop.isEncrypted():
        port = "8443"
        protocol = "https"
    else:
        port = "8080"
        protocol = "http"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type knoxsso-topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_basic),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "/var/lib/ambari-server/resources/scripts/configs.py --port %s --protocol %s --action set --host %s --cluster %s --config-type topology --key content --value '%s' --unsafe"
        % (port, protocol, CONF['AMBARI_HOST'], CLUSTER_NAME, knoxsso_topo_def),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    newAmbariUtil.restart_service("KNOX")
    Knox.restartLdap()

    if isRangerInstalled():
        sso_prov_url = "PROVIDERURL=https:\/\/" + CONF[
            'KNOX_HOST'
        ] + ":8443\/gateway\/knoxsso\/api\/v1\/websso/' " + admin_prop_loc
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i '0,/^PROVIDERURL.*/s//%s" % (sso_prov_url),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="sed -i '0,/^KNOX_AUTH.*/s//KNOX_AUTH=basic/' %s" % admin_prop_loc,
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

    newAmbariUtil.restart_service("KNOX")
    logger.info("Knox Restarted after whitelist setup")
    #add a test case for some dashboard item not present


###############################################################################
#Does this need to be a different python file, invoking Ambari test cases -> possibly split_3


@TaskReporter.report_test()
def setupAmbariKnoxSSO(user_name="admin", new_password="admin-password", old_password="admin", ldap_port="33389"):
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )

    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "openssl s_client -connect %s:%s < /dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > /etc/ambari-server/conf/jwt-cert.pem"
        % (CONF['KNOX_HOST'], CONF['KNOX_PORT']),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="""echo "" >> /etc/ambari-server/conf/jwt-cert.pem""",
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    ambari_prop_file = "/etc/ambari-server/conf/ambari.properties"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="""echo "authentication.jwt.enabled=true" >> %s""" % (ambari_prop_file),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="""echo "authentication.jwt.providerUrl=https://%s.com:8443/gateway/knoxsso/api/v1/websso" >> %s""" %
        (CONF['KNOX_HOST'], ambari_prop_file),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="""echo "authentication.jwt.publicKey=/etc/ambari-server/conf/jwt-cert.pem" >> %s""" % (ambari_prop_file),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="ambari-server restart",
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    #add delay for Ambari to start
    time.sleep(60)
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "ambari-server setup-ldap --ldap-url=%s:%s --ldap-user-class=person --ldap-user-attr=uid --ldap-group-class=groupofnames --ldap-ssl=false --ldap-secondary-url= "
        "--ldap-referral="
        " --ldap-group-attr=cn --ldap-member-attr=member --ldap-dn=dn --ldap-base-dn=dc=hadoop,dc=apache,dc=org --ldap-bind-anonym=false --ldap-manager-dn=uid=%s,ou=people,dc=hadoop,dc=apache,dc=org --ldap-manager-password=%s --ldap-sync-username-collisions-behavior=convert --ldap-save-settings"
        % (CONF['KNOX_HOST'], ldap_port, user_name, old_password),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    time.sleep(10)
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "ambari-server setup-ldap --ldap-url=%s:%s --ldap-user-class=person --ldap-user-attr=uid --ldap-group-class=groupofnames --ldap-ssl=false --ldap-secondary-url= "
        "--ldap-referral="
        " --ldap-group-attr=cn --ldap-member-attr=member --ldap-dn=dn --ldap-base-dn=dc=hadoop,dc=apache,dc=org --ldap-bind-anonym=false --ldap-manager-dn=uid=%s,ou=people,dc=hadoop,dc=apache,dc=org --ldap-manager-password=%s --ldap-sync-username-collisions-behavior=convert --ldap-save-settings"
        % (CONF['KNOX_HOST'], ldap_port, user_name, new_password),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="echo 'authentication.ldap.pagination.enabled=false' >>  %s" % ambari_prop_file,
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="ambari-server restart",
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    time.sleep(60)
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="ambari-server sync-ldap --ldap-sync-admin-name=%s --ldap-sync-admin-password=%s --all" %
        (user_name, old_password),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="ambari-server sync-ldap --ldap-sync-admin-name=%s --ldap-sync-admin-password=%s --all" %
        (user_name, new_password),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    #First login using "guest" user, this will create an Ambari user internally and then grant privilege?
    #Maven command to invoke test to try login with "guest" user
    curl_cmd = "curl -v -uadmin:admin -H 'X-Requested-By: ambari' -X PUT -d '{\"Users/admin\":\"true\"}'"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="%s http://%s:%s/api/v1/users/guest" % (curl_cmd, CONF['AMBARI_HOST'], CONF['AMBARI_PORT']),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    #Then run ambari test cases using qe-ambariautomation code?
    #Again Maven Command


@TaskReporter.report_test()
def disableAmbariKnoxSSO():
    ambari_prop_file = "/etc/ambari-server/conf/ambari.properties"
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="""echo "authentication.jwt.enabled=false" >> %s""" % (ambari_prop_file),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="ambari-server restart",
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    # add delay for Ambari to start
    time.sleep(60)


###############################################################################
@TaskReporter.report_test()
def setupRangerKnoxSSO():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    #Set up Ranger for KnoxSSO from Ambari interface
    #Step 1: set "ranger.sso.enabled" : "true",
    #/var/lib/ambari-server/resources/scripts/configs.sh set knoxsso-kpandey-erietp-1.novalocal cl1 ranger-admin-site 'ranger.sso.enabled' 'true'
    #Step 2: set "ranger.sso.providerurl" : "https://<knox_gateway_ip>:8443/gateway/knoxsso/api/v1/websso",
    # e.g. "ranger.sso.providerurl" : "https://knoxsso-kpandey-erietp-1.novalocal:8443/gateway/knoxsso/api/v1/websso"
    #Step 3: set "ranger.sso.publicKey" : < Content of cert.pem between BEGIN & END CERTIFICATE >
    #Step 4: Restart Ranger
    logger.info("Knox Host: %s Knox Port: %s" % (CONF['KNOX_HOST'], CONF['KNOX_PORT']))

    #Machine.runas(user=Machine.getAdminUser(), cmd="keytool -export -alias gateway-identity -storepass %s -rfc -file %sknoxcert.pem -keystore %sgateway.jks" %(KNOX_TRUSTSTORE_PASSWORD,KNOX_KEYSTORE_PATH,KNOX_KEYSTORE_PATH), host=CONF['KNOX_HOST'], cwd=None, env=None, logoutput=True, passwd=Machine.getAdminPasswd())
    #Machine.runas(user=Machine.getAdminUser(), cmd="yum -y install dos2unix",host=CONF['AMBARI_HOST'], cwd=None, env=None, logoutput=True, passwd=Machine.getAdminPasswd())
    #Machine.runas(user=Machine.getAdminUser(), cmd="scp -o StrictHostKeyChecking=no -r %sknoxcert.pem root@%s:/tmp/" %(KNOX_KEYSTORE_PATH,CONF['AMBARI_HOST']),host=CONF['KNOX_HOST'], cwd=None, env=None, logoutput=True, passwd=Machine.getAdminPasswd())
    #Machine.runas(user=Machine.getAdminUser(), cmd="dos2unix /tmp/knoxcert.pem",host=CONF['AMBARI_HOST'], cwd=None, env=None, logoutput=True, passwd=Machine.getAdminPasswd())
    #cert_loc = "/tmp/knoxcert.pem"

    #with open(cert_loc, 'r') as mfile:
    #    cert=mfile.read()

    KNOX_CERT = get_knox_cert()
    base_cmd = "/var/lib/ambari-server/resources/scripts/configs.py --action set --host %s --cluster %s --config-type %s --unsafe" % (
        CONF['AMBARI_HOST'], CLUSTER_NAME, "ranger-admin-site"
    )
    if Hadoop.isEncrypted():
        base_cmd = "%s --port 8443" % (base_cmd)
        base_cmd = "%s --protocol https" % (base_cmd)

    if isRangerInstalled():
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="%s --key ranger.sso.enabled --value true" % (base_cmd),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        #get FQDN+".com" and then set.
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="%s --key ranger.sso.providerurl --value %s" % (base_cmd, KNOXSSO_PROVIDER_URL),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="%s --key ranger.sso.publicKey --value %s" % (base_cmd, KNOX_CERT),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd=
            "grep -q '^KNOX_SSO_ENABLED' %s && sed -i 's/^KNOX_SSO_ENABLED.*/KNOX_SSO_ENABLED=true/' %s || echo 'KNOX_SSO_ENABLED=true' >> %s"
            % (admin_prop_loc, admin_prop_loc, admin_prop_loc),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        newAmbariUtil.restart_service("RANGER")
        logger.info("Ranger restart after SSO setup.")


###############################################################################


@TaskReporter.report_test()
def get_knox_cert():
    cert = ssl.get_server_certificate(("%s" % CONF['KNOX_HOST'], CONF['KNOX_PORT']))
    # TODO make it as common module
    KNOX_CERT = re.sub(r"\-----B(.+?)\-----\\n", "", cert)
    KNOX_CERT = re.sub(r"\-----(.+?)\-----[\n]*", "", KNOX_CERT)
    KNOX_CERT = KNOX_CERT.replace("\n", "")
    logger.info("Knox Cert returned by Machine.runas command: %s" % KNOX_CERT)
    return KNOX_CERT


@TaskReporter.report_test()
def enable_knox_SSO(prop_file, knox_sso_enable_prop, provide_url_prop, public_key_prop, browser_prop):
    base_cmd = "/var/lib/ambari-server/resources/scripts/configs.py --action set --host %s --cluster %s --config-type %s --unsafe" % (
        CONF['AMBARI_HOST'], CLUSTER_NAME, prop_file
    )
    if Hadoop.isEncrypted():
        base_cmd = "%s --port 8443" % (base_cmd)
        base_cmd = "%s --protocol https" % (base_cmd)
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="%s --key  %s --value true" % (base_cmd, knox_sso_enable_prop),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    # get FQDN+".com" and then set.
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=" %s --key %s --value %s" % (base_cmd, provide_url_prop, KNOXSSO_PROVIDER_URL),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    logger.info("Knox Host: %s Knox Port: %s" % (CONF['KNOX_HOST'], CONF['KNOX_PORT']))
    KNOX_CERT = get_knox_cert()
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="%s --key %s --value '%s'" % (base_cmd, public_key_prop, KNOX_CERT),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    if browser_prop:
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="%s --key %s --value '%s'" % (base_cmd, browser_prop, "Mozilla,Chrome,Opera"),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )


@TaskReporter.report_test()
def setupAtlasKnoxSSO():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    enable_knox_SSO(
        "application-properties", "atlas.sso.knox.enabled", "atlas.sso.knox.providerurl", "atlas.sso.knox.publicKey",
        "atlas.sso.knox.browser.useragent"
    )
    logger.info("Atlas restart")
    newAmbariUtil.restart_service("ATLAS")
    logger.info("Atlas restart after enabling SSO.")


###############################################################################
@TaskReporter.report_test()
def setupLocalLoginRangerSSO():
    logger.info("============================== Setup Local Login Ranger SSO =============================")
    if Hadoop.isEncrypted():
        ranger_url = "HOST=https:\/\/" + CONF['XA_ADMIN_HOST'] + ":6182\/locallogin/' " + admin_prop_loc
    else:
        ranger_url = "HOST=http:\/\/" + CONF['XA_ADMIN_HOST'] + ":6080\/locallogin/' " + admin_prop_loc

    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="sed -i '0,/^HOST.*/s//%s" % (ranger_url),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )


###############################################################################
@TaskReporter.report_test()
def disableRangerSSO():
    logger.info("============================== Disabling Ranger SSO =============================")
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="/var/lib/ambari-server/resources/scripts/configs.sh set %s %s ranger-admin-site ranger.sso.enabled false"
        % (CONF['AMBARI_HOST'], CLUSTER_NAME),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "grep -q '^KNOX_SSO_ENABLED' %s && sed -i 's/^KNOX_SSO_ENABLED.*/KNOX_SSO_ENABLED=false/' %s || echo 'KNOX_SSO_ENABLED=false' >> %s"
        % (admin_prop_loc, admin_prop_loc, admin_prop_loc),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    newAmbariUtil.restart_service("RANGER")
    logger.info("Ranger restart after disabling SSO.")


################################################################################
@TaskReporter.report_test()
def disableEnableRangerSSO(isEnabled):
    propsToSet = {'ranger.sso.enabled': isEnabled}
    newAmbariUtil.modify_service_configs("RANGER", "ranger-admin-site", propsToSet, restart_service=True)


@TaskReporter.report_test()
def disableAtlasKnoxSSO():
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    base_cmd = "/var/lib/ambari-server/resources/scripts/configs.py --action set --host %s --cluster %s --config-type %s --unsafe " % (
        CONF['AMBARI_HOST'], CLUSTER_NAME, "application-properties"
    )
    if Hadoop.isEncrypted():
        base_cmd = "%s --port 8443" % (base_cmd)
        base_cmd = "%s --protocol https" % (base_cmd)
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="%s --key  %s --value false" % (base_cmd, "atlas.sso.knox.enabled"),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    logger.info("Atlas restart")
    newAmbariUtil.restart_service("ATLAS")
    logger.info("Atlas restart after disabling SSO.")


@TaskReporter.report_test()
def setupKnoxProxyAtlas(scheme, atlas_host, atlas_port):
    logger.info(
        "============================== %s.%s =============================" %
        (__name__, sys._getframe().f_code.co_name)
    )
    #Copy ui.xml to topologies after replacing hostnames
    knoxproxy_topo_file = os.path.join(_workspace, "data", "knox", "ui.xml")

    atlas_host_with_port = "%s://%s:%s" % (scheme, atlas_host, atlas_port)

    tree = ET.parse(knoxproxy_topo_file)

    doc = tree.getroot()

    service_nodes = doc.findall('service')

    for node in service_nodes:
        for params in node:
            if params.tag == "role":
                param_text = params.text
                print "param_text" + param_text
            if params.tag == "url" and "ATLAS" in param_text:
                params.text = atlas_host_with_port

    tree.write(knoxproxy_topo_file)

    # SCP ui.xml to Knox Host
    knox_proxy_topo = os.path.join(_workspace, "data", "knox", "ui.xml")
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="scp -o StrictHostKeyChecking=no -r -i /root/ec2-keypair %s root@%s:%s" %
        (knox_proxy_topo, CONF['KNOX_HOST'], KNOX_TOPOLOGY_DIR),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="chown knox:knox %sui.xml" % (KNOX_TOPOLOGY_DIR),
        host=CONF['KNOX_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="chmod 644 %sui.xml" % (KNOX_TOPOLOGY_DIR),
        host=CONF['KNOX_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )

    if Hadoop.isEncrypted():

        # get the atlas cert
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd=
            "openssl s_client -connect %s:%s < /dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > /tmp/knoxatlas.crt"
            % (atlas_host, atlas_port),
            host=CONF['KNOX_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

        # import it into java key store

        Machine.runas(
            user=Machine.getAdminUser(),
            cmd=
            "%s/jre/bin/keytool -import -alias knoxsso -keystore %s/jre/lib/security/cacerts -storepass changeit -file /tmp/knoxatlas.crt -noprompt"
            % (JAVA_HOME, JAVA_HOME),
            host=CONF['KNOX_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )

        # import it into knox key store
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd=
            "%s/jre/bin/keytool -import -alias knoxsso -storepass %s -keystore %sgateway.jks -file /tmp/knoxatlas.crt -noprompt"
            % (JAVA_HOME, KNOX_TRUSTSTORE_PASSWORD, KNOX_KEYSTORE_PATH),
            host=CONF['KNOX_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )


@TaskReporter.report_test()
def disableRangerProxy():
    logger.info("============================== Disabling Ranger Proxy =============================")
    restoreRangerHost()
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "grep -q '^KNOX_PROXY_ENABLED' %s && sed -i 's/^KNOX_PROXY_ENABLED.*/KNOX_PROXY_ENABLED=false/' %s || echo 'KNOX_PROXY_ENABLED=false' >> %s"
        % (admin_prop_loc, admin_prop_loc, admin_prop_loc),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )


###############################################################################
@TaskReporter.report_test()
def setupKnoxProxyRanger(setupOnlyFiles=False):
    if not setupOnlyFiles:
        logger.info(
            "============================== %s.%s =============================" %
            (__name__, sys._getframe().f_code.co_name)
        )
        #Copy ui.xml to topologies after replacing hostnames
        knoxproxy_topo_file = os.path.join(_workspace, "data", "knox", "ui.xml")
        with open(knoxproxy_topo_file, 'r') as mfile:
            proxy_topo = mfile.read()
        ambari_host = CONF['AMBARI_HOST']
        admin_host = CONF['XA_ADMIN_HOST']
        #change regex to match ambari_host / ranger_host to respective hostnames
        Updated_Proxy_Topo = re.sub(r"ambari_host", ambari_host, proxy_topo)
        Final_Proxy_Topo = re.sub(r"ranger_host", admin_host, Updated_Proxy_Topo)
        #write Final_Proxy_Topo to ui.xml
        with open(knoxproxy_topo_file, 'w') as file:
            file.write(Final_Proxy_Topo)
            # SCP ui.xml to Knox Host
        knox_proxy_topo = os.path.join(_workspace, "data", "knox", "ui.xml")
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="scp -o StrictHostKeyChecking=no -r -i /root/ec2-keypair %s root@%s:%s" %
            (knox_proxy_topo, CONF['KNOX_HOST'], KNOX_TOPOLOGY_DIR),
            host=CONF['AMBARI_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="chown knox:knox %sui.xml" % (KNOX_TOPOLOGY_DIR),
            host=CONF['KNOX_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
        Machine.runas(
            user=Machine.getAdminUser(),
            cmd="chmod 644 %sui.xml" % (KNOX_TOPOLOGY_DIR),
            host=CONF['KNOX_HOST'],
            cwd=None,
            env=None,
            logoutput=True,
            passwd=Machine.getAdminPasswd()
        )
    #In ranger test case, change HOST in admin.properties and KnoxSSO to false, run test case, revert changes
    if HDFS.isFederated():
        ui_xml_name = 'ui_' + HDFS.getNameServices()[1]
    else:
        ui_xml_name = 'ui'
    ranger_url = "HOST=https:\/\/" + CONF['KNOX_HOST'
                                          ] + ":8443\/gateway\/" + ui_xml_name + "\/ranger\//' " + admin_prop_loc
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="sed -i 's/^ui_xml_name.*/ui_xml_name=" + ui_xml_name + "/' %s" % (admin_prop_loc)
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd="sed -i '0,/^HOST.*/s//%s" % (ranger_url),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    Machine.runas(
        user=Machine.getAdminUser(),
        cmd=
        "grep -q '^KNOX_PROXY_ENABLED' %s && sed -i 's/^KNOX_PROXY_ENABLED.*/KNOX_PROXY_ENABLED=true/' %s || echo 'KNOX_PROXY_ENABLED=true' >> %s"
        % (admin_prop_loc, admin_prop_loc, admin_prop_loc),
        host=CONF['AMBARI_HOST'],
        cwd=None,
        env=None,
        logoutput=True,
        passwd=Machine.getAdminPasswd()
    )
    #wait for topology to be active
    time.sleep(5)


###############################################################################
###############################################################################
@pytest.mark.timeout(MEDIUM_TEST_TIMEOUT)
def SetupKnoxSSO():
    logger.info("========================= KnoxSSO & Knox Proxy Test Setup =========================")
