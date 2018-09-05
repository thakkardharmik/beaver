#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os
import platform
import re
import time

from beaver.machine import Machine

logger = logging.getLogger(__name__)


class LDAP(object):
    _osname = platform.platform()
    if _osname.find("debian") >= 0 or _osname.find("Ubuntu") >= 0:
        _openldap_home = "/etc/ldap"
    else:
        _openldap_home = "/etc/openldap"
    _conf_file = "/etc/openldap/slapd.conf"
    _conf_file_cent6 = "/etc/openldap/slapd.d/cn=config/olcDatabase={2}bdb.ldif"
    _conf_file_cent7 = "/etc/openldap/slapd.d/cn=config/olcDatabase={2}hdb.ldif"
    if Machine.isUbuntu():
        _conf_file_debian = "/etc/ldap/slapd.d/cn=config/olcDatabase={1}mdb.ldif"
    elif Machine.isDebian():
        _conf_file_debian = "/etc/ldap/slapd.d/cn=config/olcDatabase={1}mdb.ldif"
    _conf_file_ora6 = "/etc/openldap/slapd.d/cn=config/olcDatabase={2}bdb.ldif"
    _conf_file_ora6_conf = "/etc/openldap/slapd.d/cn=config/olcDatabase={0}config.ldif"
    _monitor_file = "/etc/openldap/slapd.d/cn=config/olcDatabase={1}monitor.ldif"
    _tmp_path = "/tmp/openldap"

    def __init__(self):
        pass

    @classmethod
    def isLDAPSet(cls):
        return os.path.isdir(cls._openldap_home) and os.path.isfile(cls._conf_file)

    @classmethod
    def setupLDAP(cls, domain_compon="dc=hortonworks,dc=com", common_name="Manager"):
        _exit_code = 0
        # if already installed, uninstall
        logger.info("Uninstalling openldap2 from platform %s", platform.platform())
        if cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            if Machine.run("pidof slapd")[0] != 1:
                command = "apt-get -y --purge remove slapd ldap-utils"
                _exit_code, _stdout = Machine.runas(
                    Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
                )
                #Do not check for now due to oozie throwing error
                #assert exit_code == 0, "Unable to uninstall OpenLDAP"
        elif cls._osname.find("SuSE") >= 0:
            if not Machine.isEC2():
                # pylint: disable=line-too-long
                if Machine.isOpenStack():
                    command = "rcldap stop; rm -rf /etc/openldap/; rm -rf /var/lib/ldap/; zypper --no-gpg-checks remove -y openldap2; zypper --no-gpg-checks clean -a; zypper --no-gpg-checks refresh"
                else:
                    command = "rcldap stop; rm -rf /etc/openldap/;rm -rf /var/lib/ldap/;zypper remove -y openldap2; zypper clean -a; zypper refresh"
                _exit_code, _stdout = Machine.runas(
                    Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
                )
                # pylint: enable=line-too-long
        else:
            # pylint: disable=line-too-long
            if os.path.isdir(cls._openldap_home):
                command = "service slapd stop; rm -rf /etc/openldap/;rm -rf /var/lib/ldap/;yum -y remove openldap-clients openldap-servers expect"
                _exit_code, _stdout = Machine.runas(
                    Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
                )
                # pylint: enable=line-too-long
            #assert exit_code == 0, "Unable to uninstall OpenLDAP"

        # install openLDAP
        logger.info("Installing openldap2 on %s", platform.platform())
        if cls._osname.find("SuSE") >= 0:
            # TODO: Clean the backslashes!!
            # pylint: disable=line-too-long,anomalous-backslash-in-string
            if Machine.isOpenStack():
                command = "expect -c 'spawn zypper --no-gpg-checks install openldap2; expect \\\"Choose from above solutions by number or cancel \[1/2/3/c\] (c): \\\"; send \\\"3\\r\\\"; expect \\\"Continue? \[y/n/?\] (y): \\\"; send \\\"y\\r\\\"; expect eof' "
                logger.info("Installing openldap2 on OpenStack")
            elif Machine.isEC2():
                logger.info("Installing openldap2 on EC2")
                command = "expect -c 'spawn zypper install openldap2; expect \\\"Choose from above solutions by number or cancel \[1/2/3/c\] (c): \\\"; send \\\"3\\r\\\"; expect \\\"Continue? \[y/n/?\] (y): \\\"; send \\\"y\\r\\\"; expect eof' "
            else:
                command = "zypper install -y openldap2 openldap2-client expect"
            # pylint: disable=line-too-long,anomalous-backslash-in-string
        elif cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            command = "DEBIAN_FRONTEND=noninteractive apt-get install -q -y slapd ldap-utils expect"
        else:
            command = "yum -y install openldap openldap-clients openldap-servers expect"
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd(), doEscapeQuote=False
        )

        # installation of openstack-client for suse
        if cls._osname.find("SuSE") >= 0:
            if Machine.isOpenStack():
                # pylint: disable=line-too-long
                command = "zypper --non-interactive install openldap2-client"
                #command = "expect -c 'spawn zypper --no-gpg-checks install openldap2-client; expect \\\"Choose from above solutions by number or cancel \[1/2/3/4/c\] (c): \\\"; send \\\"1\\r\\\"; expect \\\"Continue? \[y/n/?\] (y): \\\"; send \\\"y\\r\\\"; expect eof' "
                logger.info("Installing openldap2-client on OpenStack")
                _exit_code, _stdout = Machine.runas(
                    Machine.getAdminUser(),
                    command,
                    None,
                    None,
                    None,
                    "True",
                    Machine.getAdminPasswd(),
                    doEscapeQuote=False
                )
                # pylint: enable=line-too-long

        if Machine.isRedHat() or Machine.isCentOs():
            cn_config = os.path.join(cls._openldap_home, 'slapd.d', 'cn=config.ldif')
            Machine.runas(
                user=Machine.getAdminUser(),
                cmd="chmod -R 777 %s" % cls._openldap_home,
                passwd=Machine.getAdminPasswd()
            )
            file_handle = open(cn_config, 'r')
            old_cn_conf = file_handle.read().split("\n")
            logger.info("old_cn_conf = %s", old_cn_conf)
            file_handle.close()

            new_cn_conf = []
            for line in old_cn_conf:
                if 'olcTLSCACertificatePath' in line:
                    new_cn_conf.append("# " + line)
                elif 'olcTLSCertificateFile' in line:
                    new_cn_conf.append("# " + line)
                elif 'olcTLSCertificateKeyFile' in line:
                    new_cn_conf.append("# " + line)
                else:
                    new_cn_conf.append(line)

            new_cn_conf_str = "\n".join(new_cn_conf)
            logger.info("new_cn_conf_str = %s", new_cn_conf_str)
            file_handle = open(cn_config, 'w')
            file_handle.write(new_cn_conf_str)
            file_handle.close()

        #Do not check now due to oozie throwing error
        #assert exit_code == 0, "Unable to install OpenLDAP"
        time.sleep(30)
        if Machine.isCentOs7() or Machine.isRedHat7() or Machine.isAmazonLinux():
            cls._importBasicSchema()
        cls._setupConfigs(domain_compon, common_name)

    @classmethod
    def _importBasicSchema(cls):
        # pylint: disable=line-too-long
        command_0 = 'cp /usr/share/openldap-servers/DB_CONFIG.example /var/lib/ldap/DB_CONFIG; chown ldap /var/lib/ldap/DB_CONFIG; systemctl start slapd; systemctl enable slapd'
        # pylint: enable=line-too-long
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command_0, None, None, None, "True", Machine.getAdminPasswd()
        )

        logger.info("Set OpenLDAP admin password")
        passwordHash = cls._createPasswordHash()
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "chrootpw.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: olcDatabase={0}config,cn=config\n')
        file_.write('changetype: modify\n')
        file_.write('add: olcRootPW\n')
        file_.write('olcRootPW: %s\n' % passwordHash)
        file_.close()
        command_1 = "ldapadd  -Y EXTERNAL -H ldapi:/// -f " + tmp_path
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command_1, None, None, None, "True", Machine.getAdminPasswd()
        )
        command_2 = "ldapadd -Y EXTERNAL -H ldapi:/// -f /etc/openldap/schema/cosine.ldif"
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command_2, None, None, None, "True", Machine.getAdminPasswd()
        )
        command_3 = "ldapadd -Y EXTERNAL -H ldapi:/// -f /etc/openldap/schema/nis.ldif"
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command_3, None, None, None, "True", Machine.getAdminPasswd()
        )
        command_4 = "ldapadd -Y EXTERNAL -H ldapi:/// -f /etc/openldap/schema/inetorgperson.ldif"
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), command_4, None, None, None, "True", Machine.getAdminPasswd()
        )
        stop_cmd = "systemctl stop slapd"
        _exit_code, _stdout = Machine.runas(
            Machine.getAdminUser(), stop_cmd, None, None, None, "True", Machine.getAdminPasswd()
        )

    @classmethod
    def _setupConfigs(cls, domain_compon="dc=hortonworks,dc=com", common_name="Manager"):
        passwordHash = cls._createPasswordHash()
        logger.info("PasswordHash: %s", passwordHash)
        passwordHash2 = cls._createPasswordHash()
        logger.info("PasswordHash: %s", passwordHash2)
        command = "chown -R hrt_qa " + cls._openldap_home
        _exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
        )
        logger.info(stdout)
        #assert exit_code == 0, "Unable to change permission to %s" %cls._conf_file

        if cls._osname.find("SuSE") >= 0:
            cls._replaceAll(cls._conf_file, "dc=my-domain,dc=com", domain_compon)
            cls._replaceAll(
                cls._conf_file, r'dn.base="cn=manager,dc=\S*,dc=com"',
                'dn.base="cn=%s,%s"' % (common_name, domain_compon)
            )
            cls._replaceAll(cls._conf_file, "secret", passwordHash)
        elif cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            if Machine.isHumboldt():
                new_lines = []
                file_handle = open(cls._conf_file_debian, 'r')
                lines = file_handle.readlines()
                for line in lines:
                    logger.info(line)
                    if line.startswith('#'):
                        new_lines.append(line.rstrip())
                    else:
                        if ':' in line:
                            new_lines.append(line.rstrip())
                        else:
                            new_lines[-1] = new_lines[-1].rstrip() + line.strip()
                for line in new_lines:
                    logger.info(line)
                file_handle = open(cls._conf_file_debian, 'w')
                file_handle.write("\n".join([l for l in new_lines if not l.isspace()]))
                file_handle.close()
            hashPasswordLine = "olcRootPW: " + passwordHash
            cls._replaceAll(cls._conf_file_debian, r"olcRootPW::\s\S*", hashPasswordLine)
            cls._replaceAll(cls._conf_file_debian, "dc=[a-zA-Z0-9-_=,]*", domain_compon)
            cls._replaceAll(cls._conf_file_debian, "cn=admin", "cn=%s" % (common_name))
            #if Machine.isHumboldt():
            #    cls._replaceAll(cls._conf_file_debian, "dc=\S*,dc=in\n ternal", domain_compon)
            #    cls._replaceAll(cls._conf_file_debian, ",dc=cloudapp,dc=net", "")
            #    f = open(cls._conf_file_debian)
            #    removal = []
            #    for line in f:
            #        if line[0] != '#' and ':' not in line :
            #            removal.append(line)
            #    f.close()
            #    for r in removal:
            #      cls._replaceAll(cls._conf_file_debian,r,"")
            #    cls._replaceAll(cls._conf_file_debian,"\*\s+non","* none")

        elif not os.path.isdir(os.path.join(cls._openldap_home, "slapd.d")):
            cls._replaceAll(cls._conf_file, r"dc=\S*,dc=com", domain_compon)
            cls._replaceAll(
                cls._conf_file, 'cn=Manager,dc=my-domain,dc=com', 'cn=%s,%s' % (common_name, domain_compon)
            )
            appendLine = "rootpw " + passwordHash + '\n'
            file_ = open(cls._conf_file, "ab")
            file_.write(appendLine)
            file_.close()
        elif cls._osname.find("oracle") >= 0:
            conf_file = cls._conf_file_ora6_conf
            appendLine = "olcRootPW: " + passwordHash + '\n'
            file_ = open(conf_file, "ab")
            file_.write(appendLine)
            file_.close()
            cls._replaceAll(cls._conf_file_ora6, "dc=my-domain,dc=com", domain_compon)
            appendLine = "olcRootPW: " + passwordHash2 + '\n'
            file_ = open(cls._conf_file_ora6, "ab")
            file_.write(appendLine)
            file_.close()
            cls._replaceAll(
                cls._monitor_file, 'dn.base="cn=manager,dc=my-domain,dc=com"',
                'dn.base="cn=%s,%s"' % (common_name, domain_compon)
            )
        else:
            if Machine.isCentOs7() or Machine.isRedHat7() or Machine.isAmazonLinux():
                conf_file = cls._conf_file_cent7
            else:
                conf_file = cls._conf_file_cent6
            appendLine = "olcRootPW: " + passwordHash + '\n'
            file_ = open(conf_file, "ab")
            file_.write(appendLine)
            file_.close()
            cls._replaceAll(conf_file, "dc=my-domain,dc=com", domain_compon)
            cls._replaceAll(
                cls._monitor_file, 'dn.base="cn=manager,dc=my-domain,dc=com"',
                'dn.base="cn=%s,%s"' % (common_name, domain_compon)
            )

        if cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            command = "chown -R openldap " + cls._openldap_home
        else:
            command = "chown -R ldap " + cls._openldap_home
        _exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
        )
        logger.info(stdout)
        #assert exit_code == 0, "Unable to change back permission to %s" %cls._conf_file

    @classmethod
    def _createPasswordHash(cls):
        #generate a password hash
        # pylint: disable=line-too-long
        cmd = "expect -c 'spawn /usr/sbin/slappasswd; expect \"New password: \"; send \"password\\r\";  expect \"Re-enter new password: \"; send \"password\\r\"; expect eof' "
        # pylint: enable=line-too-long
        _exit_code, stdout = Machine.run(cmd)
        assert stdout.split("\n") > 4, "Unable to create password hash"
        return stdout.split("\n")[3]

    @classmethod
    def _replaceAll(cls, file_, pattern, subst):
        file_handle = open(file_, 'r')
        file_string = file_handle.read()
        file_handle.close()

        file_string = (re.sub(pattern, subst, file_string))
        file_handle = open(file_, 'w')
        file_handle.write(file_string)
        file_handle.close()

    @classmethod
    def stopLDAP(cls):
        if Machine.isSuse_12():
            command = "systemctl stop slapd"
            _exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
            )
            logger.info(stdout)
        elif cls._osname.find("SuSE") >= 0:
            command = "rcldap stop"
            _exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
            )
            logger.info(stdout)
            #assert exit_code == 0, "Unable to Stop RC LDAP server"
        elif cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            Machine.stopService("slapd")
            time.sleep(5)
        else:
            command = "service slapd stop"
            _exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
            )
            logger.info(stdout)
            #assert _exit_code == 0, "Unable to uninstall OpenLDAP"
        time.sleep(30)

    @classmethod
    def startLDAP(cls):
        if Machine.isSuse_12():
            command = "systemctl start slapd"
            _exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
            )
            logger.info(stdout)
        elif cls._osname.find("SuSE") >= 0:
            process = Machine.findProcess("rcldap")
            if process == "":
                command = "rcldap start"
                _exit_code, stdout = Machine.runas(
                    Machine.getAdminUser(), command, None, None, None, "True", Machine.getAdminPasswd()
                )
                #assert _exit_code == 0, "Unable to start LDAP server"
        elif cls._osname.find("debian") >= 0 or cls._osname.find("Ubuntu") >= 0:
            Machine.stopService("slapd")
            time.sleep(5)
            Machine.startService("slapd")
        elif os.path.isdir(os.path.join(cls._openldap_home, "slapd.d")):
            process = Machine.findProcess("slapd")
            if process == "":
                Machine.startService("slapd")

            # Some machines might require following step to be done to start slapd service
            _exit_code, stdout = Machine.runas(
                Machine.getAdminUser(), "slapd", None, None, None, "True", Machine.getAdminPasswd()
            )
        else:
            #To-DO: The below two lines return process even if ldap has not started
            #Start no matterwhat for now
            #process = Machine.findProcess("ldap")
            #if process == "":
            Machine.startService("ldap")
        time.sleep(30)

    @classmethod
    def createLDAPTree(cls):
        cls._dropOrganization()
        cls._addOrganization()
        cls._addOrganizationUnit()
        cls._addUser()
        cls._addGroup()

    @classmethod
    def _dropOrganization(cls):
        command = "ldapdelete -x -r -D cn=Manager,dc=hortonworks,dc=com -w password \"dc=hortonworks,dc=com\""
        Machine.runas("hrt_qa", command, None, None, None, "True", None)

    @classmethod
    def _addOrganization(cls):
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "hortonworks.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: dc=hortonworks,dc=com\n')
        file_.write('objectClass: dcObject\n')
        file_.write('objectClass: organization\n')
        file_.write('dc: hortonworks\n')
        file_.write('o: hortonworks\n')
        file_.close()

        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to add organization"

    @classmethod
    def _addOrganizationUnit(cls):
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "users.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: ou=Users,dc=hortonworks,dc=com\n')
        file_.write('objectClass: organizationalUnit\n')
        file_.write('ou: Users\n')
        file_.close()
        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to Users add organization unit"

        tmp_path = os.path.join(cls._tmp_path, "groups.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: ou=Groups,dc=hortonworks,dc=com\n')
        file_.write('objectClass: organizationalUnit\n')
        file_.write('ou: Groups\n')
        file_.close()
        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to add Groups organization unit"

    @classmethod
    def _addUser(cls):
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "hrt_qa.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: uid=hrt_qa,ou=Users,dc=hortonworks,dc=com\n')
        file_.write('objectClass: inetOrgPerson\n')
        file_.write('objectClass: top\n')
        file_.write('userPassword: pwd\n')
        file_.write('cn: Horton\n')
        file_.write('sn: Works\n')
        file_.write('uid: hrt_qa\n')
        file_.close()
        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to add User"

    @classmethod
    def _addGroup(cls):
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "groups1.ldif")
        file_ = open(tmp_path, 'w')
        file_.write('dn: uid=group1,ou=Groups,dc=hortonworks,dc=com\n')
        file_.write('objectClass: groupOfNames\n')
        file_.write('objectClass: top\n')
        file_.write('objectClass: ExtensibleObject\n')
        file_.write('cn: group1\n')
        file_.write('ou: Groups\n')
        file_.write('sn: group1\n')
        file_.write('uid: group1\n')
        file_.write('member: uid=hrt_qa,ou=Users,dc=hortonworks,dc=com\n')
        file_.close()
        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to add Group"

    @classmethod
    def editUserLdif(cls):
        if not os.path.isdir(cls._tmp_path):
            Machine.makedirs(None, None, cls._tmp_path)
        tmp_path = os.path.join(cls._tmp_path, "hrt_qa.ldif")
        with open(tmp_path, "r+") as f:
            text = f.read()
            text = (re.sub(r"dn: uid=\S*", "dn: cn=hrt_qa,ou=Users,dc=hortonworks,dc=com", text))
            f.seek(0)
            f.write(text)
            f.truncate()
            f.close()
        command = "ldapadd -x -f " + tmp_path + " -D cn=Manager,dc=hortonworks,dc=com -w password"
        exit_code, stdout = Machine.runas("hrt_qa", command, None, None, None, "True", None)
        # the below if statement takes care of a case when adding item already exists
        if stdout.find("Already exists") == -1:
            assert exit_code == 0, "Unable to add organization"

    @classmethod
    def addUserLdif(
            cls,
            ldif_file,
            domain_compon="dc=hadoop,dc=apache,dc=org",
            common_name="Manager",
            admin_pass="password",
            hostname="localhost",
            oldap_port="389"
    ):
        ldap_add_cmd = "ldapadd -x -h %s -p %s -f %s -D cn=%s,%s -w %s" % (
            hostname, oldap_port, ldif_file, common_name, domain_compon, admin_pass
        )
        logger.info(ldap_add_cmd)
        _exit_code, stdout = Machine.runas(
            Machine.getAdminUser(), ldap_add_cmd, None, None, None, "True", Machine.getAdminPasswd()
        )
        logger.info(stdout)
        #assert exit_code == 0, "Unable to add ldif file %s to open ldap" % ldif_file
