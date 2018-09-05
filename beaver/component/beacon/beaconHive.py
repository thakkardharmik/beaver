#
#
# Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#

import os
import logging
import re

from beaver.component.hive import Hive, Hive1
from beaver.component.hadoop import HDFS, Hadoop
from beaver.component.ambari import Ambari
from beaver.config import Config
from beaver import util
from beaver.machine import Machine

logger = logging.getLogger(__name__)
ARTIFACTS_DIR = Config.getEnv('ARTIFACTS_DIR')

_beacon_host_1 = Config.get("multicluster", "AMBARI_GATEWAY1")
_beacon_host_2 = Config.get("multicluster", "AMBARI_GATEWAY2")

source_weburl = Ambari.getWebUrl(hostname=_beacon_host_1)
target_weburl = Ambari.getWebUrl(hostname=_beacon_host_2)
primaryCluster = Ambari.getClusterName(weburl=source_weburl)
backupCluster = Ambari.getClusterName(weburl=target_weburl)

source_hdfsConfig = Ambari.getConfig(type='core-site', service='HDFS', webURL=source_weburl)
target_hdfsConfig = Ambari.getConfig(type='core-site', service='HDFS', webURL=target_weburl)
source_hdfsEndpoint = source_hdfsConfig['fs.defaultFS']
target_hdfsEndpoint = target_hdfsConfig['fs.defaultFS']
job_user = Config.getEnv('USER')
hive_dir = '/user/%s/beacon-regression' % job_user

datafile = os.path.join(os.path.join(ARTIFACTS_DIR, "all100k"))
# TODO we need to get the url from [dlm] section in infra scripts and retrieve using Config.get in code
datafile_url = "http://qe-repo.s3.amazonaws.com/dlm/hive/all100k"
if not os.path.isfile(datafile):
    # assert util.downloadUrl(Config.get('dlm', 'HIVEREPLICAION_TEST_DATA'), datafile)
    assert util.downloadUrl(datafile_url, datafile)
WAREHOUSE_DIR = Hive.getMetastoreWarehouseDir()

if Config.get('hive', 'HIVE2', 'False') == "False":
    datafile_local = datafile
    datafile = '/home/%s/all100k' % Config.get('hadoop', 'HADOOPQA_USER')
    for hiveserver_host in Hive1.getHiveserver2Hosts():
        Machine.copyFromLocal(Machine.getAdminUser(), hiveserver_host, datafile_local, datafile)

HDFS.copyFromLocal(os.path.join(Config.getEnv('WORKSPACE'), 'tests', 'beacon', 'data', 'udf', 'testudf.jar'), '/tmp/')

if HDFS.isHAEnabled():
    target_hdfsEndpoint = "hdfs://" + HDFS.getActiveNN(_beacon_host_2, "NAMENODE")


class TableBase(object):
    def __init__(self, db, table_suffix):
        self.db = db
        self.table_suffix = table_suffix
        self.table_name = db + '_' + table_suffix

    def create_query(self):
        pass

    def query_file_to_save(self):
        pass

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def prepare_create(self):
        pass

    def check_create(self):
        pass

    def verify_table(self):
        pass

    def expected_result(self):
        return None


class SuperSimpleTable(TableBase):
    def create_query(self):
        return '''use {db};CREATE TABLE {table_name} (a int);
        INSERT INTO {table_name} values(1);
        INSERT INTO {table_name} values(2);'''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'super_simple.sql'


class EmptyTable(TableBase):
    def create_query(self):
        return '''
        use {db};
        drop table if EXISTS {table_name};
        create table {table_name} (userid int, age int) row format delimited fields terminated by '|' stored as textfile;
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'empty.sql'


class BasicTable(TableBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create table {table_name} (t tinyint, si smallint, i int, b bigint, f decimal(38,2), d double, s string, dc decimal(38,6), bo boolean, v varchar(25), c char(25), ts timestamp, dt date) row format delimited fields terminated by '|' stored as textfile;
        load data local inpath '{data_file}' into table {table_name};
        '''.format(
            db=self.db, data_file=datafile, table_name=self.table_name
        )

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} order by b asc limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'basic.sql'


class BasicTableWithConstraints(TableBase):
    def __init__(self, constraint, *args, **kwargs):
        super(BasicTableWithConstraints, self).__init__(*args, **kwargs)
        self.constraint = constraint

    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create table {table_name} (t tinyint, si smallint, i int, b bigint, f decimal(38,2), d double, s string, dc decimal(38,6), bo boolean, v varchar(25), c char(25), ts timestamp, dt date, {constraint} (t) disable novalidate) row format delimited fields terminated by '|' stored as textfile;
        load data local inpath '{data_file}' into table {table_name};
        '''.format(
            db=self.db, data_file=datafile, table_name=self.table_name, constraint=self.constraint
        )

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} order by b asc limit 100;
        select count(*) from {table_name};
        describe extended {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "basic_table_with_constraints.sql"


class CompressedTable(TableBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;

        drop table if exists temptable purge;
        create TEMPORARY table temptable (t tinyint, si smallint, i int, b bigint, f decimal(38,2), d double, s string, dc decimal(38,6), bo boolean, v varchar(25), c char(25), ts timestamp, dt date)
             row format delimited
             fields terminated by '|'
             stored as textfile;
        load data local inpath '{datafile}' into table temptable;

        create table {table_name} (t tinyint, si smallint, i int, b bigint, f decimal(38,2), d double, s string, dc decimal(38,6), bo boolean, v varchar(25), c char(25), ts timestamp, dt date)
            stored as orc tblproperties ("orc.compress"="{compression}");
        insert into {table_name} select t, si, i, b, f, d, s, dc, bo, v, c, ts, dt from temptable limit 4;
        '''.format(
            db=self.db, compression=self.compression, datafile=datafile, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'compress_{compression}.sql'.format(compression=self.compression)

    def check_create(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name)
        assert ('orc.compress=%s' % self.compression) in stdout

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, weburl=target_weburl)
        assert ('orc.compress=%s' % self.compression) in stdout


class ZlibTable(CompressedTable):
    def __init__(self, db, table_suffix):
        self.compression = "ZLIB"
        super(ZlibTable, self).__init__(db, table_suffix)


class SnappyTable(CompressedTable):
    def __init__(self, db, table_suffix):
        self.compression = "SNAPPY"
        super(SnappyTable, self).__init__(db, table_suffix)


class UncompressedTable(CompressedTable):
    def __init__(self, db, table_suffix):
        self.compression = "NONE"
        super(UncompressedTable, self).__init__(db, table_suffix)


class BasicExternalTable(TableBase):
    def __init__(self, *args, **kwargs):
        super(BasicExternalTable, self).__init__(*args, **kwargs)
        self.data_dir = hive_dir + '/' + self.__class__.__name__

    def prepare_create(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        logger.info('current folder is %s' % current_dir)
        HDFS.deleteDirectory(source_hdfsEndpoint + '/%s' % self.data_dir, user='hive')
        HDFS.createDirectory(source_hdfsEndpoint + '/%s' % self.data_dir, user='hive')
        HDFS.copyFromLocal(datafile, source_hdfsEndpoint + '/%s' % self.data_dir, user='hive')
        HDFS.chmod(runasUser='hive', perm="777", directory=source_hdfsEndpoint + '/%s' % self.data_dir, recursive=True)

    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create external table {table_name}
                 (t tinyint, si smallint, i int, b bigint, f decimal(38,2), d double, s string, dc decimal(38,6),
                 bo boolean, v varchar(25), c char(25), ts timestamp, dt date)
                 row format delimited
                 fields terminated by '|'
                 stored as textfile
                 LOCATION '{location}';
        '''.format(
            db=self.db, table_name=self.table_name, location=self.data_dir
        )

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} order by b asc limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'basic_external.sql'

    def check_create(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name)
        assert 'tableType:EXTERNAL_TABLE' in stdout

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, weburl=target_weburl)
        assert 'tableType:MANAGED_TABLE' in stdout


class TableFromExternalBase(TableBase):
    def __init__(self, db, table_suffix, *args, **kwargs):
        self.number_of_colors = len(
            Data.COLORS
        ) if kwargs.get('number_of_colors', None) is None else kwargs.get('number_of_colors')
        self.number_of_rows = Data.ROWCOUNT if kwargs.get('number_of_rows', None
                                                          ) is None else kwargs.get('number_of_rows')
        self.number_of_add_columns = 0 if kwargs.get('number_of_add_columns', None
                                                     ) is None else kwargs.get('number_of_add_columns')
        super(TableFromExternalBase, self).__init__(db, table_suffix, *args, **kwargs)
        self.datafile = None

    def temptable_creation(self):
        additional_columns = ""
        if self.number_of_add_columns != 0:
            for i in range(4, self.number_of_add_columns + 4):
                additional_columns = additional_columns + ", column_%s string" % str(i)
        return '''
            drop table if exists temptable purge;
            create TEMPORARY table temptable (user_id string, color string, gender int {additional_columns})
                     row format delimited
                     fields terminated by '|'
                     stored as textfile;
            load data local inpath '{datafile}' into table temptable;
        '''.format(
            datafile=self.datafile, additional_columns=additional_columns
        )

    def prepare_create(self):
        test_name = self.__class__.__name__
        self.datafile = Data.create_test_data_file(
            test_name, self.number_of_colors, self.number_of_rows, self.number_of_add_columns
        )


class StaticPartitionTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        create table {table_name} (user_id string, gender int) partitioned by (color string);
        insert into {table_name} partition(color='red') select user_id,gender from temptable where color='red';
        insert into {table_name} partition(color='blue') select user_id,gender from temptable where color='blue';
        insert into {table_name} partition(color='white') select user_id,gender from temptable where color='white';
        '''.format(
            db=self.db, temptable_creation=self.temptable_creation(), table_name=self.table_name
        )

    def query_file_to_save(self):
        return "static.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)

        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout


class StaticPartitionLargeTable(TableFromExternalBase):
    PARTITION_COUNT = 5000
    PARTITION_ROWS = 50

    def create_query(self):
        query = '''
        use {db};
        drop table if exists {table} purge;
        {temptable_creation}
        create table {table} (user_id string, gender int, color string) partitioned by (party int);
        '''.format(
            db=self.db, temptable_creation=self.temptable_creation(), table=self.table_name
        )

        for partition in range(StaticPartitionLargeTable.PARTITION_COUNT):
            query += '''insert into {table} partition(party={p})
                        select user_id, gender, color from temptable
                        where user_id < {rows};'''.format(
                table=self.table_name, p=partition, rows=StaticPartitionLargeTable.PARTITION_ROWS
            )

        return query

    def query_file_to_save(self):
        return "large.sql"

    def verify_query(self):
        return '''
        use {db};
        select * from {table} order by user_id asc limit {rows};
        select count(*) from {table};
        '''.format(
            db=self.db, table=self.table_name, rows=StaticPartitionLargeTable.PARTITION_ROWS * 3
        )

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        for i in range(StaticPartitionLargeTable.PARTITION_COUNT):
            assert ("party=%d\n" % i) in stdout

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        for i in range(StaticPartitionLargeTable.PARTITION_COUNT):
            assert ("party=%d\n" % i) in stdout


class DynamicPartitionTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create table {table_name} (user_id string, gender int) partitioned by (color string);
        insert into {table_name} partition(color) select user_id,gender,color from temptable;
        '''.format(
            db=self.db, temptable_creation=self.temptable_creation(), table_name=self.table_name
        )

    def query_file_to_save(self):
        return "dynamic.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout


class MultiplePartitionTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create table {table_name} (user_id string) partitioned by (color string, gender int);
        insert into {table_name} partition(color,gender) select user_id,color,gender from temptable;
        '''.format(
            db=self.db, temptable_creation=self.temptable_creation(), table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'multi.sql'

    def check_create(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white/gender=0' in stdout
        assert 'color=white/gender=1' in stdout
        assert 'color=red/gender=0' in stdout
        assert 'color=red/gender=1' in stdout
        assert 'color=blue/gender=0' in stdout
        assert 'color=blue/gender=1' in stdout

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'color=white/gender=0' in stdout
        assert 'color=white/gender=1' in stdout
        assert 'color=red/gender=0' in stdout
        assert 'color=red/gender=1' in stdout
        assert 'color=blue/gender=0' in stdout
        assert 'color=blue/gender=1' in stdout


class StaticPartitionExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        create external table {table_name} (user_id string, gender int) partitioned by (color string) LOCATION '{hive_dir}/{table_name}';

        insert overwrite directory '{hive_dir}/tmp/color=red' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        select user_id, gender from temptable where color='red';
        insert overwrite directory '{hive_dir}/tmp/color=blue' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        select user_id, gender from temptable where color='blue';
        insert overwrite directory '{hive_dir}/tmp/color=white' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        select user_id, gender from temptable where color='white';

        LOAD DATA INPATH '{hive_dir}/tmp/color=red' INTO
        TABLE {table_name} PARTITION (color='red');
        LOAD DATA INPATH '{hive_dir}/tmp/color=blue' INTO
        TABLE {table_name} PARTITION (color='blue');
        LOAD DATA INPATH '{hive_dir}/tmp/color=white' INTO
        TABLE {table_name} PARTITION (color='white');
        '''.format(
            hive_dir=hive_dir, table_name=self.table_name, db=self.db, temptable_creation=self.temptable_creation()
        )

    def query_file_to_save(self):
        return 'external_static_partition.sql'

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout
        assert 'tableType:EXTERNAL_TABLE' in stdout

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout
        # table on target cluster is expected to be a managed/internal table and not external table as on source cluster
        assert 'tableType:MANAGED_TABLE' in stdout


class DynamicPartitionExternalTable(TableFromExternalBase):
    def create_query(self):
        additional_columns = ""
        if self.number_of_add_columns != 0:
            for i in range(4, self.number_of_add_columns + 4):
                additional_columns = additional_columns + ", column_%s string" % str(i)
        columns_list = ""
        if self.number_of_add_columns != 0:
            for i in range(4, self.number_of_add_columns + 4):
                columns_list = columns_list + ",column_%s" % str(i)
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create external table {table_name} (user_id string, gender int {additional_columns}) partitioned by (color string) row format delimited  fields terminated by ',' stored as textfile LOCATION '{hive_dir}/{table_name}';
        insert into {table_name} partition(color) select user_id,gender,color{columns_list} from temptable;
        '''.format(
            db=self.db,
            table_name=self.table_name,
            temptable_creation=self.temptable_creation(),
            hive_dir=hive_dir,
            additional_columns=additional_columns,
            columns_list=columns_list
        )

    def query_file_to_save(self):
        return 'dynamicExternalTable.sql'

    def check_create(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white' in stdout
        assert 'color=red' in stdout
        assert 'color=blue' in stdout
        assert 'tableType:EXTERNAL_TABLE' in stdout

    def __init__(
            self,
            db,
            table_suffix,
            number_of_colors=None,
            number_of_rows=None,
            number_of_add_columns=None,
            *args,
            **kwargs
    ):
        super(DynamicPartitionExternalTable, self).__init__(db, table_suffix, *args, **kwargs)
        self.number_of_colors = len(Data.COLORS) if number_of_colors is None else number_of_colors
        self.number_of_rows = Data.ROWCOUNT if number_of_rows is None else number_of_rows
        self.number_of_add_columns = 0 if number_of_add_columns is None else number_of_add_columns


class MultiplePartitionExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        {temptable_creation}
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create external table {table_name} (user_id string) partitioned by (color string, gender int) row format delimited  fields terminated by ',' stored as textfile LOCATION '{hive_dir}/{table_name}';
        insert into {table_name} partition(color, gender) select user_id,color,gender from temptable;
        '''.format(
            db=self.db, table_name=self.table_name, temptable_creation=self.temptable_creation(), hive_dir=hive_dir
        )

    def query_file_to_save(self):
        return 'dynamicExternalTable.sql'

    def check_create(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'color=white/gender=0' in stdout
        assert 'color=white/gender=1' in stdout
        assert 'color=red/gender=0' in stdout
        assert 'color=red/gender=1' in stdout
        assert 'color=blue/gender=0' in stdout
        assert 'color=blue/gender=1' in stdout
        assert 'tableType:EXTERNAL_TABLE' in stdout

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'color=white/gender=0' in stdout
        assert 'color=white/gender=1' in stdout
        assert 'color=red/gender=0' in stdout
        assert 'color=red/gender=1' in stdout
        assert 'color=blue/gender=0' in stdout
        assert 'color=blue/gender=1' in stdout
        # table on target cluster is expected to be a managed/internal table and not external table as on source cluster
        assert 'tableType:MANAGED_TABLE' in stdout


class StaticPartitionWithTimestampTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create table {table_name} (quantity int) partitioned by (time_purchased timestamp);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def insert_query(self):
        return '''
        use {db};
        insert into {table_name} partition(time_purchased='2001-11-09 00:00:00.0') values(20);
        insert into {table_name} partition(time_purchased='2001-11-10 04:07:02.0') values(12);
        insert into {table_name} partition(time_purchased='2001-11-12 00:08:03.0') values(18);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "static_timestamp_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def alter_query(self):
        return '''
        use {db};
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09 00:00:00.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' not in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout


class StaticPartitionWithDateTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create table {table_name} (quantity int) partitioned by (time_purchased date);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def insert_query(self):
        return '''
        use {db};
        insert into {table_name} partition(time_purchased='2001-11-09') values(20);
        insert into {table_name} partition(time_purchased='2001-11-10') values(21);
        insert into {table_name} partition(time_purchased='2001-11-11') values(22);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "static_date_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def alter_query(self):
        return '''
        use {db};
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' not in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout


class StaticPartitionWithTimestampExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create external table {table_name} (quantity int) partitioned by (time_purchased timestamp) LOCATION '{hive_dir}/{table_name}';
        '''.format(
            db=self.db, table_name=self.table_name, hive_dir=hive_dir
        )

    def insert_query(self):
        return '''
        use {db};
        insert into {table_name} partition(time_purchased='2001-11-09 00:00:00.0') values(20);
        insert into {table_name} partition(time_purchased='2001-11-10 04:07:02.0') values(12);
        insert into {table_name} partition(time_purchased='2001-11-12 00:08:03.0') values(18);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "static_timestamp_external_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def alter_query(self):
        return '''
        use {db};
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09 00:00:00.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' not in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout


class StaticPartitionWithDateExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        create external table {table_name} (quantity int) partitioned by (time_purchased date) LOCATION '{hive_dir}/{table_name}';
        '''.format(
            db=self.db, table_name=self.table_name, hive_dir=hive_dir
        )

    def insert_query(self):
        return '''
        use {db};
        insert into {table_name} partition(time_purchased='2001-11-09') values(20);
        insert into {table_name} partition(time_purchased='2001-11-10') values(21);
        insert into {table_name} partition(time_purchased='2001-11-11') values(22);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "static_date_external_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def alter_query(self):
        return '''
        use {db};
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' not in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout


class DynamicPartitionWithTimestampTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create table {table_name} (quantity int) partitioned by (time_purchased timestamp);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def insert_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        insert into {table_name} partition(time_purchased) values(20, '2001-11-09 00:00:00.0');
        insert into {table_name} partition(time_purchased) values(12, '2001-11-10 04:07:02.0');
        insert into {table_name} partition(time_purchased) values(18, '2001-11-12 00:08:03.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "dynamic_timestamp_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def alter_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09 00:00:00.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' not in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout


class DynamicPartitionWithDateTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create table {table_name} (quantity int) partitioned by (time_purchased date);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def insert_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        insert into {table_name} partition(time_purchased) values(20, '2001-11-09');
        insert into {table_name} partition(time_purchased) values(12, '2001-11-10');
        insert into {table_name} partition(time_purchased) values(18, '2001-11-11');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "dynamic_date_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def alter_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' not in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout


class DynamicPartitionWithTimestampExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create external table {table_name} (quantity int) partitioned by (time_purchased timestamp) LOCATION '{hive_dir}/{table_name}';
        '''.format(
            db=self.db, table_name=self.table_name, hive_dir=hive_dir
        )

    def insert_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        insert into {table_name} partition(time_purchased) values(20, '2001-11-09 00:00:00.0');
        insert into {table_name} partition(time_purchased) values(12, '2001-11-10 04:07:02.0');
        insert into {table_name} partition(time_purchased) values(18, '2001-11-12 00:08:03.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "dynamic_timestamp_external_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout

    def alter_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09 00:00:00.0');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09 00%3A00%3A00.0' not in stdout
        assert 'time_purchased=2001-11-10 04%3A07%3A02.0' in stdout
        assert 'time_purchased=2001-11-12 00%3A08%3A03.0' in stdout


class DynamicPartitionWithDateExternalTable(TableFromExternalBase):
    def create_query(self):
        return '''
        use {db};
        drop table if exists {table_name} purge;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        create external table {table_name} (quantity int) partitioned by (time_purchased date) LOCATION '{hive_dir}/{table_name}';
        '''.format(
            db=self.db, table_name=self.table_name, hive_dir=hive_dir
        )

    def insert_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        insert into {table_name} partition(time_purchased) values(20, '2001-11-09');
        insert into {table_name} partition(time_purchased) values(12, '2001-11-10');
        insert into {table_name} partition(time_purchased) values(18, '2001-11-11');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return "dynamic_date_external_table.sql"

    def check_create(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'Table(tableName:' + self.table_name in stdout

    def check_insert(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def verify_query(self):
        return '''
        use {db};
        select * from {table_name} limit 100;
        select count(*) from {table_name};
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table(self):
        # verify expected partitions
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout

    def alter_query(self):
        return '''
        use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=true;
        set hive.exec.max.dynamic.partitions=2000;
        alter table {table_name} DROP IF EXISTS PARTITION(time_purchased='2001-11-09');
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def verify_table_after_alter(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, getPartitions=True, weburl=target_weburl)
        assert 'time_purchased=2001-11-09' not in stdout
        assert 'time_purchased=2001-11-10' in stdout
        assert 'time_purchased=2001-11-11' in stdout


class View(TableBase):
    def __init__(self, db, table_suffix, base_table=None):
        self.basic_table = base_table
        if self.basic_table is None:
            self.basic_table = BasicTable(db, '_basic')
        super(View, self).__init__(db, self.basic_table.table_suffix + table_suffix)

    def create_query(self):
        return self.basic_table.create_query() + '''use {db};
        create view {view_name} as select * from {table_name} limit 10000;
        '''.format(
            db=self.db, table_name=self.basic_table.table_name, view_name=self.table_name
        )

    def query_file_to_save(self):
        return 'createview.sql'

    def check_create(self):
        self.basic_table.check_create()
        stdout = BeaconHive.getTableDetails(self.db, self.table_name)
        # verify data structure type on local cluster
        assert stdout.find('tableType:VIRTUAL_VIEW') != -1

    def verify_query(self):
        return super(View, self).verify_query() + '''
        show tables;'''

    def expected_result(self):
        return self.table_name

    def verify_table(self):
        self.basic_table.verify_table()
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, weburl=target_weburl)
        assert 'tableType:VIRTUAL_VIEW' in stdout


class StorageTypedTable(TableBase):
    def __init__(
            self, db, table_suffix, stored_as="textfile", row_format_clause="", desc_signature="", *args, **kwargs
    ):
        super(StorageTypedTable, self).__init__(db, table_suffix, *args, **kwargs)
        self.row_format_clause = row_format_clause
        self.stored_as = stored_as
        self.desc_signature = desc_signature
        self.datafile = datafile

    def temptable_creation(self):
        return '''
             drop table if exists temptable purge;
            create TEMPORARY table temptable (user_id string, color string, gender int)
                      row format delimited
                      fields terminated by '|'
                      stored as textfile;
             load data local inpath '{datafile}' into table temptable;
              '''.format(datafile=self.datafile)

    def create_query(self):
        return '''
        use {db};
        drop table if exists {table} purge;
        {temptable_creation}
        create table {table} (user_id string, color string, gender int)
            {row_format_clause}
            stored as {stored_as};
        insert into {table} select user_id,color,gender from temptable;
        '''.format(
            db=self.db,
            table=self.table_name,
            temptable_creation=self.temptable_creation(),
            row_format_clause=self.row_format_clause,
            stored_as=self.stored_as
        )

    def query_file_to_save(self):
        return "%s_%sTable.sql" % (self.stored_as, self.table_suffix)

    def verify_table(self):
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, weburl=target_weburl)
        assert self.desc_signature in stdout

    def expected_result(self):
        return str(100000)  # all100k data file used to generate test data has 100K rows of data


class OrcTable(StorageTypedTable):
    def __init__(self, db, table_suffix):
        super(OrcTable, self).__init__(
            db, table_suffix, "orc", desc_signature="serializationLib:org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        )


class ParquetTable(StorageTypedTable):
    def __init__(self, db, table_suffix):
        super(ParquetTable, self).__init__(
            db,
            table_suffix,
            "parquet",
            desc_signature="serializationLib:"
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )


class AvroTable(StorageTypedTable):
    def __init__(self, db, table_suffix):
        super(AvroTable, self).__init__(
            db, table_suffix, "avro", desc_signature="serializationLib:org.apache.hadoop.hive.serde2.avro.AvroSerDe"
        )


class TextTable(StorageTypedTable):
    def __init__(self, db, table_suffix):
        super(TextTable, self).__init__(
            db,
            table_suffix,
            "textfile",
            "row format delimited fields terminated by ','",
            desc_signature="serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        )


class JsonTable(StorageTypedTable):
    def __init__(self, db, table_suffix):
        super(JsonTable, self).__init__(
            db,
            table_suffix,
            "textfile",
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'",
            desc_signature="serializationLib:org.apache.hive.hcatalog.data.JsonSerDe"
        )


class Index(TableBase):
    def __init__(self, db, table_suffix):
        self.empty_table = EmptyTable(db, '_empty')
        super(Index, self).__init__(db, self.empty_table.table_suffix + table_suffix)

    def create_query(self):
        return self.empty_table.create_query() + ''' use {db};
        set hive.execution.engine=mr;
        CREATE INDEX {index_name}
        ON TABLE {target_table} (user_id)
        AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
        WITH DEFERRED REBUILD IN TABLE {index_name}_table;
        '''.format(
            index_name=self.table_name, target_table=self.empty_table.table_name, db=self.db
        )

    def query_file_to_save(self):
        return 'createIndex.sql'

    def expected_result(self):
        return self.table_name

    def verify_query(self):
        return '''
        use {db};
        show index on {target_table};
        '''.format(
            db=self.db, target_table=self.empty_table.table_name
        )


class BucketTable(TableBase):
    def __init__(self, db, table_suffix):
        self.basic_table = BasicTable(db, '_basic')
        super(BucketTable, self).__init__(db, self.basic_table.table_suffix + table_suffix)

    def create_query(self):
        return self.basic_table.create_query() + ''' use {db};
        set hive.exec.dynamic.partition.mode=nonstrict;
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        (t tinyint, si smallint, i int, b bigint, f decimal(38,2),
        d double, s string, dc decimal(38,6), bo boolean, v varchar(25), c char(25),
        ts timestamp, dt date)
        CLUSTERED BY (t) INTO 10 BUCKETS;
        INSERT OVERWRITE TABLE {table_name} SELECT * FROM {target_table} LIMIT 50;
        '''.format(
            db=self.db, table_name=self.table_name, target_table=self.basic_table.table_name
        )

    def query_file_to_save(self):
        return 'bucketTable.sql'

    def check_create(self):
        self.basic_table.check_create()
        # check number of buckets in hdfs on source cluster
        exit_code, stdout = Hadoop.run("dfs -ls %s/%s/%s/" % (WAREHOUSE_DIR, self.db + ".db", self.table_name))
        assert exit_code == 0, "Failed to run dfs -ls command on %s/%s/%s/" % (
            WAREHOUSE_DIR, self.db + ".db", self.table_name
        )
        assert "Found 10 items" in stdout, "Expected number of bucket files not found"
        # verify bucket columns and expected number of buckets
        stdout = BeaconHive.getTableDetails(self.db, self.table_name)
        assert "bucketCols:[t]" in stdout
        assert "numFiles=10" in stdout

    def verify_table(self):
        self.basic_table.verify_table()
        # check number of buckets in hdfs on target cluster
        exit_code, stdout = Hadoop.run(
            "dfs -ls %s/%s/%s/%s/" % (target_hdfsEndpoint, WAREHOUSE_DIR, self.db + ".db", self.table_name)
        )
        assert exit_code == 0, "Failed to run dfs -ls command on %s/%s/%s/%s/" % (
            target_hdfsEndpoint, WAREHOUSE_DIR, self.db + ".db", self.table_name
        )
        assert "Found 10 items" in stdout, "Expected number of bucket files not found"
        # verify bucket columns and expected number of buckets
        stdout = BeaconHive.getTableDetails(self.db, self.table_name, weburl=target_weburl)
        assert "bucketCols:[t]" in stdout
        assert "numFiles=10" in stdout


class UnsupportedStorageEngineTable(TableFromExternalBase):
    def create_query(self):
        return '''
            use {db};
            drop table if exists {table} purge;
            {temptable_creation}
            CREATE TABLE {table}(user_id string, color string, gender int)
                STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:color,cf1:gender")
                TBLPROPERTIES ("hbase.table.name" = "{table}_in_hbase",
                "hbase.mapred.output.outputtable" = "{table}_in_hbase");
            INSERT OVERWRITE TABLE {table} SELECT * FROM temptable;
            '''.format(
            db=self.db, table=self.table_name, temptable_creation=self.temptable_creation()
        )

    def query_file_to_save(self):
        return "HBaseTable.sql"

    def verify_query(self):
        # Negative test: _no_ query should be executed both on primary and remote to compare results
        return None

    def verify_table(self):
        # verify that the table does _not_ exist on remote
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline("show tables;", weburl=target_weburl)
        assert self.table_name not in stdout


class Function(TableBase):
    def create_query(self):
        return '''
        use {db};
        delete jar {hdfsEndpoint}/tmp/testudf.jar;
        add jar {hdfsEndpoint}/tmp/testudf.jar;
        CREATE  FUNCTION {db}_{table_name}_mysquare AS 'org.apache.hive.udf.UDFSquare' using jar '{hdfsEndpoint}/tmp/testudf.jar';
        '''.format(
            db=self.db, data_file=datafile, table_name=self.table_name, hdfsEndpoint=source_hdfsEndpoint
        )

    def verify_query(self):
        return '''
        use {db};
        reload function;
        select {db}_{table_name}_mysquare(5);
        '''.format(
            db=self.db, table_name=self.table_name
        )

    def query_file_to_save(self):
        return 'function.sql'


class Data:
    def __init__(self):
        pass

    COLORS = ["red", "blue", "white"]
    ROWCOUNT = 30000

    @staticmethod
    def create_test_data_file(test_name, number_of_colors, number_of_rows, number_of_add_columns):
        data_name = test_name + ".dat"
        data_file = os.path.join(ARTIFACTS_DIR, data_name)

        with open(data_file, 'w') as f:
            for user_id in xrange(number_of_rows):
                color = Data.COLORS[user_id % number_of_colors
                                    ] if user_id % number_of_colors < len(Data.COLORS) else str(
                                        user_id % number_of_colors
                                    )
                gender = user_id % 2
                if number_of_add_columns != 0:
                    columns_data = ""
                    for i in range(0, number_of_add_columns):
                        columns_data = columns_data + "|%s" % (color)
                    f.write("%s|%s|%d%s\n" % (user_id, color, gender, columns_data))
                else:
                    f.write("%s|%s|%d\n" % (user_id, color, gender))

        if Config.get('hive', 'HIVE2', 'False') == "False":
            # datafile_local = data_file
            datafile = '/home/%s/%s' % (Config.get('hadoop', 'HADOOPQA_USER'), data_file.split('/')[-1])
            for hiveserver_host in Hive1.getHiveserver2Hosts():
                Machine.copyFromLocal(Machine.getAdminUser(), hiveserver_host, data_file, datafile)
            data_file = datafile
        return data_file


def table_factory(table_type, db):
    type_dict = {
        'empty': EmptyTable,
        'basic': BasicTable,
        'external': BasicExternalTable,
        'static': StaticPartitionTable,
        'dynamic': DynamicPartitionTable,
        'multidynamic': MultiplePartitionTable,
        'static_external': StaticPartitionExternalTable,
        'dynamic_external': DynamicPartitionExternalTable,
        'multidynamic_external': MultiplePartitionExternalTable,
        'view': View,
        'index': Index,
        'bucket': BucketTable,
        'static_timestamp': StaticPartitionWithTimestampTable,
        'static_date': StaticPartitionWithDateTable,
        'static_ext_timestamp': StaticPartitionWithTimestampExternalTable,
        'static_ext_date': StaticPartitionWithDateExternalTable,
        'dynamic_timestamp': DynamicPartitionWithTimestampTable,
        'dynamic_date': DynamicPartitionWithDateTable,
        'dynamic_ext_timestamp': DynamicPartitionWithTimestampExternalTable,
        'dynamic_ext_date': DynamicPartitionWithDateExternalTable
    }

    # storage formats
    type_dict.update(
        {
            'orc': OrcTable,
            'parquet': ParquetTable,
            'avro': AvroTable,
            'text': TextTable,
            'json': JsonTable
        }
    )

    # hbase storage engine (replication is unsupported - negative test)
    type_dict.update({'storedbyhbase': UnsupportedStorageEngineTable})

    # Function
    type_dict.update({'function': Function})

    # compressions
    type_dict.update({'zlib': ZlibTable, 'snappy': SnappyTable, 'uncompressed': UncompressedTable})

    return type_dict[table_type](db, table_type)


class BeaconHive:
    def __init__(self):
        pass

    @staticmethod
    def create(table, weburl=None):
        table.prepare_create()
        logger.info('Creating table: ' + table.__class__.__name__)
        query_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), table.query_file_to_save())
        query = table.create_query()
        util.writeToFile(query, query_file)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=weburl)
        assert exit_code == 0, "Failed to create test table err: {stderr}, out:{stdout} ".format(
            stderr=stderr, stdout=stdout
        )

        table.check_create()

    @staticmethod
    def mass_create(tables, weburl=None):
        """
        Create a list of tables, all in a single beeline query file. Every member of the list should be of the same
        descendant type of TableBase.
        :param tables: list of table objects to be created in the database
        :param weburl: URL of the cluster to create tables in (e.g. primary or remote)
        :return: None
        """
        proto_table = tables[0]
        count = len(tables)
        proto_table.prepare_create()
        query_file = os.path.join(Config.getEnv('ARTIFACTS_DIR'), str(count) + proto_table.query_file_to_save())

        query = ''
        for i in range(count):
            assert proto_table.__class__ is tables[i].__class__
            query += tables[i].create_query()

        logger.info('Creating {} tables of type {}'.format(count, proto_table.__class__.__name__))
        util.writeToFile(query, query_file)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(
            query_file, readFromFile=True, weburl=weburl, logoutput=True, queryIsFile=True
        )
        assert exit_code == 0, "Failed to mass create test tables."

        # spare time and check only some of the tables
        tables[0].check_create()
        tables[-1].check_create()

    @staticmethod
    def verify(table):
        logger.info('Verifying table: ' + table.__class__.__name__)

        verify_query = table.verify_query()
        if verify_query is not None:
            BeaconHive.verifyOutput(verify_query, expected=table.expected_result())

        table.verify_table()

    @classmethod
    def getHiveserver2Hosts(cls, weburl):
        if Config.get('hive', 'HIVE2', 'False') == "True":
            hiveServer2Hosts = Ambari.getHostsForComponent('HIVE_SERVER_INTERACTIVE', weburl=weburl)
        else:
            hiveServer2Hosts = Ambari.getHostsForComponent('HIVE_SERVER', weburl=weburl)
        return hiveServer2Hosts

    @classmethod
    def createHiveServer2UrlForHost(cls, host):
        local_hive_zk_quorum = Ambari.getConfig(service="HIVE", type="hive-site")['hive.zookeeper.quorum']
        remote_hive_zk_quorum = Ambari.getConfig(
            service="HIVE", type="hive-site", webURL=Ambari.getWebUrl(hostname=host)
        )['hive.zookeeper.quorum']
        splits = Hive.getHiveServer2Url().split(local_hive_zk_quorum)
        if splits:
            return splits[0] + remote_hive_zk_quorum + splits[1]

    @classmethod
    def getTableSchema(cls, db, tablename, weburl=None):
        logger.info("-- Run query to getTableSchema")
        query = "use %s;\n" % db
        query += "SHOW CREATE TABLE %s;\n" % tablename
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=weburl)
        assert exit_code == 0, "Query failed with the following error:\n" + stderr
        return stdout

    @classmethod
    def getTableDescription(cls, db, tablename, weburl=None):
        logger.info("-- Run query to get table description")
        query = "use %s;\n" % db
        query += "DESC %s;\n" % tablename
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=weburl)
        assert exit_code == 0, "Query failed with the following error:\n" + stderr
        return stdout

    @classmethod
    def getTableDetails(cls, db, tablename, getPartitions=False, getDescribeExtended=True, weburl=None):
        logger.info("-- Run query to getTableDetails")
        query = "use %s;\n" % db
        if getPartitions:
            query += "show partitions %s;\n" % tablename
        if getDescribeExtended:
            query += "describe extended %s;\n" % tablename
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=weburl)
        assert exit_code == 0, "Query failed with the following error:\n" + stderr
        return stdout

    @classmethod
    def getHiveServer2UrlForDynamicServiceDiscovery(cls, weburl):
        hiveconfig = Ambari.getConfig(service="HIVE", type='hive-site', webURL=weburl)
        zkquorum = hiveconfig['hive.zookeeper.quorum']
        hsiconfig = Ambari.getConfig(service="HIVE", type='hive-site', webURL=weburl)
        zknamespace = hsiconfig['hive.server2.zookeeper.namespace']
        zkquorumwithport = Hive._addPortToZkquorum(zkquorum)
        return "jdbc:hive2://" + zkquorumwithport + "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + zknamespace

    @classmethod
    def runQueryOnBeeline(
            cls,
            query,
            readFromFile=False,
            hiveconf={},
            hivevar={},
            option={},
            user=None,
            passwd="pwd",
            showHeaders=False,
            cwd=None,
            env=None,
            logoutput=False,
            direct=False,
            background=False,
            queryIsFile=False,
            httpmode=False,
            weburl=None
    ):
        if weburl is not None:
            hiveconfig = Ambari.getConfig(service="HIVE", type='hive-site', webURL=weburl)
            zkquorum = hiveconfig['hive.zookeeper.quorum']
            if Config.get('hive', 'HIVE2', 'False') == "True":
                hsiconfig = Ambari.getConfig(service="HIVE", type='hive-interactive-site', webURL=weburl)
            else:
                hsiconfig = Ambari.getConfig(service="HIVE", type='hive-site', webURL=weburl)
            zknamespace = hsiconfig['hive.server2.zookeeper.namespace']
        else:
            zkquorum = Hive.getConfigValue("hive.zookeeper.quorum")
            zknamespace = Hive.getConfigValue("hive.server2.zookeeper.namespace")

        return Hive.runQueryOnBeelineViaDynamicServiceDiscovery(
            query,
            zkquorum,
            zknamespace,
            readFromFile=readFromFile,
            hiveconf=hiveconf,
            hivevar=hivevar,
            option=option,
            user=user,
            passwd=passwd,
            showHeaders=showHeaders,
            cwd=cwd,
            env=env,
            logoutput=logoutput,
            background=background,
            queryIsFile=queryIsFile,
            httpmode=httpmode
        )

    @classmethod
    def preparedata(cls, tables, db, weburl=None):
        BeaconHive.createDatabase(db, weburl=weburl)
        for table in tables:
            BeaconHive.create(table_factory(table, db), weburl=weburl)

    @classmethod
    def verifydata(cls, tables, db):
        for table in tables:
            BeaconHive.verify(table_factory(table, db))

    @classmethod
    def verify_db_exists(cls, db):
        logger.info("verifying if db exists")
        query = "use {db}".format(db=db)
        BeaconHive.verifyOutput(query)

    @classmethod
    def verifyOutput(cls, query, expected=None):
        # execute on source
        source_exitcode, source_stdout, source_stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True)
        assert source_exitcode == 0
        if expected is not None:
            assert re.search("%s" % expected, source_stdout), "Output did not contain expected: %s" % (source_stderr)
        # execute on remote
        target_exitcode, target_stdout, target_stderr = BeaconHive.runQueryOnBeeline(
            query, readFromFile=True, weburl=target_weburl
        )
        assert target_exitcode == 0

        assert source_stdout == target_stdout, "Source output did not match target output: %s" % (target_stderr)

    @classmethod
    def createDatabase(cls, db, weburl=None):
        if db is None:
            logger.info("db name cannot be empty")

        query = "drop database if exists %s CASCADE;" % db
        query += "create database %s;" % db
        hivesetupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "createdb_source.sql")
        util.writeToFile(query, hivesetupfile)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=weburl)
        assert exit_code == 0, "Failed to create database err: {stderr}, out:{stdout} ".format(
            stderr=stderr, stdout=stdout
        )

    @classmethod
    def drop_database(cls, db):
        if db is None:
            logger.info("db name cannot be empty")

        query = "drop database if exists %s CASCADE;" % db
        hivesetupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "createdb_source.sql")
        util.writeToFile(query, hivesetupfile)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True)

        query = "drop database if exists %s CASCADE;" % db
        hivesetupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "dropdb_target.sql")
        util.writeToFile(query, hivesetupfile)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True, weburl=target_weburl)

    @classmethod
    def createAcidTable(cls, db):
        tablename = db + "_acid"

        query = "use %s;\n" % db
        query += "drop table if exists %s purge;\n" % tablename
        query += "create table %s (a int, b string, c date) clustered by (a) into 2 buckets stored as orc;\n" % tablename
        query += "insert overwrite table joinnonacid_table select t,s,dt from %s where t>=50 and t<100;\n" % db + "_basic_100k"
        query += " create table joinacid_table (a int, b string, c date, d timestamp) clustered by (a) into 2 buckets stored as orc tblproperties('transactional'='true');\n"
        query += "insert into table joinacid_table select t,s,dt,ts from %s where t>0 and t<50;" % tablename

        hivesetupfile = os.path.join(Config.getEnv('ARTIFACTS_DIR'), "acid.sql")
        util.writeToFile(query, hivesetupfile)
        exit_code, stdout, stderr = BeaconHive.runQueryOnBeeline(query, readFromFile=True)
        assert exit_code == 0, "Failed to create test table"
