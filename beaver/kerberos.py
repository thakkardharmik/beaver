#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
'''
APIs for kerberos
'''
import logging
import re

from beaver.config import Config
from beaver.machine import Machine
logger = logging.getLogger(__name__)


class Kerberos(object):
    '''
    Kerberos helper class
    '''
    _kinitloc = None

    def __init__(self):
        pass

    @classmethod
    def _buildCommonVar(cls):
        '''
        Builds common variable used by the class and caches them.
        Returns None.
        '''
        if cls._kinitloc is None:
            cls._kinitloc = Machine.which("kinit", "root")

    @classmethod
    def kinit(cls, flag="", logoutput=True):
        '''
        Runs kinit as current user with default QE ticket/keytab.
        Returns (exit_code, stdout).
        '''
        return cls.kinitas(keytabUser=None, principal=None, keytabFile=None, flag=flag, logoutput=logoutput, host=None)

    @classmethod
    def kinitas(cls, keytabUser, principal=None, keytabFile=None, flag="-f", logoutput=True, host=None):
        '''
        Runs kinit as specified keytab user.
        Returns (exit_code, stdout).
        '''
        # Build common variables and cache them.
        cls._buildCommonVar()
        # If keytabUser is None, use current user
        if keytabUser is None:
            keytabUser = Config.getEnv('USER')
        # Get kerberos ticket location e.g. /grid/0/hadoopqe/artifacts/kerberosTickets/hrt_qa.kerberos.ticket
        kerbTicket = Machine.getKerberosTicket(user=keytabUser)
        # If keytab is unset, use default keytab path e.g. /home/hrt_qa/hadoopqa/keytabs/hrt_qa.headless.keytab
        if keytabFile is None:
            keytabFile = Machine.getHeadlessUserKeytab(keytabUser)
        # If principal is not set, use keytab user.
        if principal is None:
            principal = keytabUser
        # Build command
        cmd = "%s -c %s -k -t %s %s %s" % (
            cls._kinitloc, kerbTicket, keytabFile, flag, Machine.get_user_principal(principal)
        )
        # kinit always runs with credential of current user.
        return Machine.runas(user=None, cmd=cmd, env=None, logoutput=logoutput, host=host)

    @classmethod
    def klist(cls, cacheOrKeytab=None, flag="-f", logoutput=True, host=None):
        '''
        Runs klist with optional cache file or keytab file.
        Returns (exit_code, stdout).
        '''
        if cacheOrKeytab is None:
            cacheOrKeytab = Machine.getKerberosTicket(user=None)
        cmd = "klist %s %s" % (flag, cacheOrKeytab)
        return Machine.runas(user=None, cmd=cmd, logoutput=logoutput, host=host)

    @classmethod
    def requestRenewableTicket(  # pylint: disable=unused-argument
            cls, keytabUser, lifetime, renewableUpto, logoutput=True
    ):
        '''
        Requests renewable ticket.
        Returns (exit_code, stdout) of kinit.
        Renewable lifetime is bound by kerberos server max_renewable_life for the realm.
            Default to 0. What does it mean?
        '''
        return cls.kinit("-l %s -r %s" % (lifetime, renewableUpto), logoutput=logoutput)

    @classmethod
    def requestTicket(cls, keytabUser, lifetime, logoutput=True):  # pylint: disable=unused-argument
        '''
        Requests default-type ticket (non-renewable/forwardable in nano).
        Returns (exit_code, stdout) of kinit.
        Ticket lifetime is bound by kerberos server max_life for the realm. Default to 24 hours.
        '''
        return cls.kinit("-l %s" % lifetime, logoutput=logoutput)

    @classmethod
    def modifyTicketLifetime(cls, lifetime, isFirstUpdate=True):
        '''
        Restart KDC with new ticket_lifetime
        '''
        #Extending lifetime beyond 24 hours is currently supported for Redhat linux and Debian only
        logger.info("Modifying Keberos ticket lifetime")
        if Machine.isDebian() or Machine.isHumboldt():
            kdc_conf = '/etc/krb5kdc/kdc.conf'
            Machine.chmod(
                perm="777",
                filepath="/etc/krb5kdc",
                recursive=True,
                user=Machine.getAdminUser(),
                passwd=Machine.getAdminPasswd()
            )
            with open(kdc_conf, "r+") as f:
                text = f.read()
                text = (re.sub(r"max_life = \S*", "max_life = %s" % lifetime, text))
                f.seek(0)
                f.write(text)
                f.truncate()
                f.close()
        if Machine.isRedHat() or Machine.isCentOs():
            kdc_conf = '/var/kerberos/krb5kdc/kdc.conf'
            Machine.chmod(perm="777", filepath=kdc_conf, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
            with open(kdc_conf, "r+") as f:
                text = f.read()
                if isFirstUpdate:
                    text = (re.sub("EXAMPLE.COM = {", "EXAMPLE.COM = {\n  max_life = %s" % lifetime, text))
                else:
                    text = (
                        re.sub(
                            r"EXAMPLE.COM = {\n  max_life = \S*", "EXAMPLE.COM = {\n  max_life = %s" % lifetime, text
                        )
                    )
                f.seek(0)
                f.write(text)
                f.truncate()
                f.close()
        kerberos_conf = '/etc/krb5.conf'
        Machine.chmod(perm="777", filepath=kerberos_conf, user=Machine.getAdminUser(), passwd=Machine.getAdminPasswd())
        with open(kerberos_conf, "r+") as f:
            text = f.read()
            text = (re.sub(r"ticket_lifetime = \S*", "ticket_lifetime = %s" % lifetime, text))
            f.seek(0)
            f.write(text)
            f.truncate()
            f.close()
        kadmin_local_loc = Machine.which("kadmin.local", "root")
        logger.info("kadmin_local_loc: %s", kadmin_local_loc)
        cmd = "echo 'modify_principal -maxlife %s %s@%s' | %s" % (
            lifetime, Config.getEnv('USER'), Config.get('machine', 'USER_REALM', ''), kadmin_local_loc
        )
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        if exit_code != 0:
            logger.info("modify_principal failed with the following output:\n%s", stdout)
        cmd = "echo 'modify_principal -maxlife %s krbtgt/EXAMPLE.COM@EXAMPLE.COM' | %s" % (lifetime, kadmin_local_loc)
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        if exit_code != 0:
            logger.info("modify_principal failed with the following output:\n%s", stdout)
        cmd = "service krb5kdc restart"
        exit_code, stdout = Machine.runas(Machine.getAdminUser(), cmd, passwd=Machine.getAdminPasswd())
        if exit_code != 0:
            logger.info("KDC failed to restart with the following output:\n%s", stdout)
