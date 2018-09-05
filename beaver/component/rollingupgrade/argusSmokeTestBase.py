import os, sys
from beaver.machine import Machine
import platform

dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(dir, '../../../tests/xasecure/xa-agents')))

from xa_testcase import *
from xa_admin import *
from xa_hdfs_test import *
from xa_hive_test import *
from xa_hbase_test import *
from xa_knox_test import *
from xa_logger import *
from xa_test import *


class RUTestCase(XATestCase):
    _mapred_queue = "argus"

    def __init__(self, *args, **kwargs):
        XATestCase.__init__(self, *args, **kwargs)

        self.HDFS_TEST_CASE = None
        self.HIVE_TEST_CASE = None
        self.HBASE_TEST_CASE = None
        self.KNOX_TEST_CASE = None

        self.hdfsPolicy = None
        self.hivePolicy = None
        self.hbasePolicy = None
        self.knoxPolicy = None

        self.HIVE_TEST_DB_NAME = 'xatest_ru'

    def setUpModule(self, components):
        print('************* setup of module ************************')
        XATestCase.setUpModule(self)
        XAAdmin.sInstance = None

        if 'hdfs' in components:
            print('************* setup of hdfs module ************************')
            self.HDFS_TEST_CASE = XAHDFSTestCase()
            self.HDFS_TEST_CASE.setUpModule()
            self.createHdfsPolicy()

        if 'hive' in components:
            print('************* setup of hive module ************************')
            self.HIVE_TEST_CASE = XAHiveTestCase()
            self.HIVE_TEST_CASE.setUpModule()
            self.createHivePolicy()
            self.HIVE_TEST_CASE.executeBeelineCmd(
                XAHiveTestCase.USER_USER2, XAHiveTestCase.PASS_USER2,
                'drop table if exists {dbname}.{tblname};'.format(
                    dbname=self.HIVE_TEST_DB_NAME, tblname='testtable_ru'
                )
            )
            self.HIVE_TEST_CASE.executeBeelineCmd(
                XAHiveTestCase.USER_USER2,
                XAHiveTestCase.PASS_USER2,
                'drop database if exists {dbname};'.format(dbname=self.HIVE_TEST_DB_NAME)
            )

        if 'hbase' in components:
            print('************* setup of hbase module ************************')
            self.HBASE_TEST_CASE = XAHBaseTestCase()
            self.HBASE_TEST_CASE.setUpModule()
            self.createHbasePolicy()

        if 'knox' in components:
            print('************* setup of knox module ************************')
            self.KNOX_TEST_CASE = XAKnoxTestCase()
            self.KNOX_TEST_CASE.setUpModule()
            self.createKnoxPolicy()
        print('************* setup of modules done ************************')

    def tearDownModule(self):
        XATestCase.tearDownModule(self)

        if self.hdfsPolicy is not None:
            self.getXAAdmin().deletePolicy(self.hdfsPolicy.id)

        if self.hivePolicy is not None:
            self.getXAAdmin().deletePolicy(self.hivePolicy.id)

        if self.hbasePolicy is not None:
            self.getXAAdmin().deletePolicy(self.hbasePolicy.id)

        if self.knoxPolicy is not None:
            self.getXAAdmin().deletePolicy(self.knoxPolicy.id)

        if self.HDFS_TEST_CASE is not None:
            self.HDFS_TEST_CASE.tearDownModule()

        if self.HIVE_TEST_CASE is not None:
            self.HIVE_TEST_CASE.tearDownModule()

        if self.HBASE_TEST_CASE is not None:
            self.HBASE_TEST_CASE.tearDownModule()

        if self.KNOX_TEST_CASE is not None:
            self.KNOX_TEST_CASE.tearDownModule()

        print('************* teardown of module done************************')

#************************* HDFS TEST CASES **************************************

    def createHdfsPolicy(self):
        """
		Create policy for group 'finance' with permission 'write' on resource '/demo/data'
		"""

        resourceList = ['/', '/demo', '/demo/data']
        hdfsPolicy = XAHdfsPolicy(
            policyName='/demo/data',
            repo=self.HDFS_TEST_CASE.hdfsrepo,
            resourceList=resourceList,
            description='/demo/data Write for Group:Finance',
            isRecursive=False,
            isAuditEnabled=True,
            groupPermList=[{
                'groupList': ['finance'],
                'permList': [XAPolicy.PERMISSION_WRITE]
            }],
            userPermList=[],
            isEnabled=True
        )

        self.hdfsPolicy = self.HDFS_TEST_CASE.getXAAdmin().createPolicy(hdfsPolicy)

        if self.hdfsPolicy is None:
            XALogger.logFatal('Failed to create policy: ' + str(hdfsPolicy))

        XALogger.logInfo('Created Policy (id = ' + str(self.hdfsPolicy.id) + '): ' + str(self.hdfsPolicy))

        self.getXAAdmin().waitForPolicyUpdate()

    def HDFSTestCase_01(self):
        """
		Verifies user1 has permission to create directory in '/demo/data' as policy is assigned to the group 'finance' which has user1
		"""
        username = 'user1'
        hdfsdir = '/demo/data/testUser1Dir'

        XAHdfsTest(
            'test_01_01_CreateHDFSDirectory_case1A',
            'HDFS Test Case: Running with group write permission for /demo/data for group finance',
            self.HDFS_TEST_CASE,
            'dfs -mkdir -p ' + hdfsdir,
            username,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Permission denied',
            isInRollingUpgrade=True
        ).execute()

    def HDFSTestCase_02(self):
        """
		Verifies user2 has no permission to create directory in '/demo/data' as user2 is not part of group 'finance'
		"""
        username = 'user2'
        hdfsdir = '/demo/data/testUser2Dir'

        XAHdfsTest(
            'test_01_02_CreateHDFSDirectory_case1B',
            'HDFS Test Case:  Running with group write permission for /demo/data for group finance',
            self.HDFS_TEST_CASE,
            'dfs -mkdir -p ' + hdfsdir,
            username,
            verifyCriteria=XATest.STDERR_CONTAINS,
            verifyStr='Permission denied',
            isInRollingUpgrade=True
        ).execute()

    def HDFSTestCase_03(self):
        """
		Verifies user1 has permission to delete the directory present in '/demo/data' as policy is assigned to the group 'finance' which has user1
		"""
        username = 'user1'
        hdfsdir = '/demo/data/testUser1Dir'

        XAHdfsTest(
            'test_01_03_DeleteHDFSDirectory_case1C',
            'HDFS Test Case: Running with group write permission for /demo/data for group finance',
            self.HDFS_TEST_CASE,
            'dfs -rm -r -skipTrash ' + hdfsdir,
            username,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Permission denied',
            isInRollingUpgrade=True
        ).execute()

#************************* HIVE TEST CASES **************************************

    def createHivePolicy(self):
        """
		Create policy for user 'user2' with all hive permission
		"""
        self.hivePolicy = self.HIVE_TEST_CASE.createPolicyAllAccessForUser(XAHiveTestCase.USER_USER2)

    def HIVETestCase_01(self):
        """
		Verifies user1 has no permission to create database as policy is not assigned to user1
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "create database {dbname};".format(dbname=self.HIVE_TEST_DB_NAME)
        XAHiveTest(
            'test_01_01_CreateDatabaseAsUser1_Case1A',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER1,
            XAHiveTestCase.PASS_USER1,
            verifyCriteria=XATest.STDERR_CONTAINS,
            verifyStr=XATestCase.HIVE_AUTHZ_DENIED_MSG,
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_02(self):
        """
		Verifies user2 has permission to create database as policy is assigned to user2 with all hive permission
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "create database {dbname};".format(dbname=self.HIVE_TEST_DB_NAME)
        XAHiveTest(
            'test_01_02_CreateDatabaseAsUser2_Case1B',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER2,
            XAHiveTestCase.PASS_USER2,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Error',
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_03(self):
        """
		Verifies user2 has permission to use database as policy is assigned to user2 with all hive permission
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "use {dbname};".format(dbname=self.HIVE_TEST_DB_NAME)
        XAHiveTest(
            'test_01_03_UseDatabaseAsUser2_Case1C',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER2,
            XAHiveTestCase.PASS_USER2,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Error',
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_04(self):
        """
		Verifies user1 has no permission to use database as policy is not assigned to user1
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "use {dbname};".format(dbname=self.HIVE_TEST_DB_NAME)
        XAHiveTest(
            'test_01_04_UseDatabaseAsUser1Deny_Case1D',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER1,
            XAHiveTestCase.PASS_USER1,
            verifyCriteria=XATest.STDERR_CONTAINS,
            verifyStr=XATestCase.HIVE_AUTHZ_DENIED_MSG,
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_05(self):
        """
		Verifies user2 has permission to create table as policy is assigned to user2 with all hive permission
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "create table {dbname}.{tblspec};".format(
            dbname=self.HIVE_TEST_DB_NAME, tblspec='testtable_ru(name String)'
        )
        XAHiveTest(
            'test_01_05_CreateTableAsUser2',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER2,
            XAHiveTestCase.PASS_USER2,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Error',
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_06(self):
        """
		Verifies user2 has permission to drop table as policy is assigned to user2 with all hive permission
		"""
        #query = "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "drop table {dbname}.{tblname};".format(dbname=self.HIVE_TEST_DB_NAME, tblname='testtable_ru')
        XAHiveTest(
            'test_01_06_DropTableAsUser2_Case1F',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER2,
            XAHiveTestCase.PASS_USER2,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Error',
            isInRollingUpgrade=True
        ).execute()

    def HIVETestCase_07(self):
        """
		Verifies user2 has permission to drop database as policy is assigned to user2 with all hive permission
		"""
        #query =  "set mapred.job.queue.name=%s;\n" % RUTestCase._mapred_queue
        query = "drop database {dbname};".format(dbname=self.HIVE_TEST_DB_NAME)
        XAHiveTest(
            'test_01_07_DropDatabaseAsUser2_Case1G',
            self.HIVE_TEST_CASE,
            query,
            XAHiveTestCase.USER_USER2,
            XAHiveTestCase.PASS_USER2,
            verifyCriteria=XATest.STDERR_NOT_CONTAINS,
            verifyStr='Error',
            isInRollingUpgrade=True
        ).execute()

#************************* HBASE TEST CASE ***************************************

    def createHbasePolicy(self):
        """
		Create policy for group 'finance' with permission all hbase permission
		"""
        self.hbasePolicy = self.HBASE_TEST_CASE.create_tesetcase_04_policy()

    def HBASETestCase_01(self):
        """
		Verifies user1 has permission to create table as policy is assigned to the group 'finance' which has user1
		"""
        XAHBaseTest(
            'test_01_01_createHbaseTable_Case1A',
            self.HBASE_TEST_CASE,
            hBaseCmds=["create 'testTable01','personal','medical'"],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_02(self):
        """
		Verifies user1 has permission to put cell value
		"""
        XAHBaseTest(
            'test_01_02_putHbaseTable_Case1B',
            self.HBASE_TEST_CASE,
            hBaseCmds=["put 'testTable01', '1',  'personal:fname', 'Mike'    "],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_03(self):
        """
		Verifies user1 has permission to scan table
		"""
        XAHBaseTest(
            'test_01_03_scanHbaseTable_Case1C',
            self.HBASE_TEST_CASE,
            hBaseCmds=["scan 'testTable01'"],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_04(self):
        """
		Verifies user1 has permission to get row
		"""
        XAHBaseTest(
            'test_01_04_getHbaseTable_Case1D',
            self.HBASE_TEST_CASE,
            hBaseCmds=["get 'testTable01', '1'    "],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_05(self):
        """
		Verifies user1 has permission to disable
		"""
        XAHBaseTest(
            'test_01_05_disableHbaseTable_Case1E',
            self.HBASE_TEST_CASE,
            hBaseCmds=["disable 'testTable01'"],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_06(self):
        """
		Verifies user1 has permission to drop table
		"""
        XAHBaseTest(
            'test_01_06_dropHbaseTable_Case1F',
            self.HBASE_TEST_CASE,
            hBaseCmds=["drop 'testTable01'"],
            reqUser='user1',
            verifyCriteria=XATest.STDOUT_NOT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()

    def HBASETestCase_07(self):
        """
		Verifies user2 has no permission to create table
		"""
        XAHBaseTest(
            'test_01_07_createHbaseTable_Case1G',
            self.HBASE_TEST_CASE,
            hBaseCmds=["create 'testTable01','personal','medical'"],
            reqUser='user2',
            verifyCriteria=XATest.STDOUT_CONTAINS,
            verifyStr='AccessDeniedException',
            checkAuditLog=False,
            newAuditLogMustExist=True,
            isInRollingUpgrade=True
        ).execute()


#************************* KNOX TEST CASE *****************************************

    def createKnoxPolicy(self):
        """
		Create policy to allow access to user 'sam' for service 'WEBHDFS' on  topology 'hdpargus'
		"""
        topologyList = ['hdpargus']
        serviceList = ['WEBHDFS']

        self.knoxPolicy = self.KNOX_TEST_CASE.createKnoxPolicy(
            policyName='KNOX_Policy_01_For_Sam',
            topologyList=topologyList,
            serviceList=serviceList,
            isAuditEnabled=True,
            groupPermList=[],
            userPermList=[{
                'userList': ['sam'],
                'permList': [XAPolicy.PERMISSION_ALLOW]
            }],
            isEnabled=True
        )

        XALogger.logInfo('Created Knox Policy For User Sam: ' + str(self.knoxPolicy))

    def KNOXTestCase_01(self):
        """
		Verifies guest Forbidden access to WEBHDFS on hdpargus
		"""
        username = 'guest'
        password = 'guest-password'
        component = 'WEBHDFS'
        gatewayPath = 'gateway'
        clusterName = 'hdpargus'
        httpMethod = 'GET'
        path = '?op=LISTSTATUS'

        XAKnoxTest(
            'test_01_01_AllowGuestToAccessWebhdfs_case1A',
            self.KNOX_TEST_CASE,
            username,
            password,
            component,
            gatewayPath,
            clusterName,
            httpMethod,
            path,
            verifyCriteria=XATest.STDOUT_CONTAINS,
            verifyStr='HTTP/1.1 403 Forbidden',
            checkAuditLog=True,
            isInRollingUpgrade=True
        ).execute()

    def KNOXTestCase_02(self):
        """
		Verifies sam is Allowed to access WEBHDFS on hdpargus
		"""
        username = 'sam'
        password = 'sam-password'
        component = 'WEBHDFS'
        gatewayPath = 'gateway'
        clusterName = 'hdpargus'
        httpMethod = 'GET'
        path = '?op=LISTSTATUS'

        XAKnoxTest(
            'test_01_02_AllowSamToAccessWebhdfs_case1B',
            self.KNOX_TEST_CASE,
            username,
            password,
            component,
            gatewayPath,
            clusterName,
            httpMethod,
            path,
            verifyCriteria=XATest.STDOUT_CONTAINS,
            verifyStr='HTTP/1.1 200 OK',
            checkAuditLog=False,
            isInRollingUpgrade=True
        ).execute()
