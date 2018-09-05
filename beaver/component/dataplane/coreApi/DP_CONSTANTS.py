#
# All the constants used across the DP tests sit here
#
USERNAME = 'admin'
PASSWORD = 'admin'
KNOX_USERNAME = 'admin1'
KNOX_PASSWORD = 'Horton!#works'
LDAP_PAYLOAD = {
    "ldapUrl": "ldap://ad-nano.qe.hortonworks.com:389",
    "userSearchBase": "CN=Users,DC=HWQE,DC=HORTONWORKS,DC=COM",
    "userSearchAttributeName": "cn",
    "groupSearchBase": "OU=groups,DC=HWQE,DC=HORTONWORKS,DC=COM",
    "groupSearchAttributeName": "cn",
    "groupObjectClass": "person",
    "groupMemberAttributeName": "member",
    "bindDn": "cn=hwqe,DC=HWQE,DC=HORTONWORKS,DC=COM",
    "password": "Pass123!"
}
LDAP_USER = 'guest'
SMART_SENSE_ID = 'A-12345678-C-12345678'
