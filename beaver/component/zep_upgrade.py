import logging
from beaver.component.hadoop import BaseHDFS
from beaver.component.zeppelin import ZeppelinRestClientSession, ZeppelinAmbariAPIUtil
from beaver.config import Config
from tests.zeppelin.zeppelin_onprem.zeppelin_spark2.interpreters.tests.test_d_concurrent_composite_notebooks import \
    test_concurrent_sessions_livy2_interpreter_group, test_concurrent_sessions_spark2_interpreter_group, \
    test_concurrent_sessions_jdbc_hive_interpreter, test_concurrent_sessions_jdbc_sts2_interpreter, \
    test_concurrent_sessions_jdbc_phoenix_interpreter, test_concurrent_sessions_jdbc_hive_hsi_interpreter

logger = logging.getLogger(__name__)


class ZeppelinUpg(object):
    def __init__(self):
        pass

    @classmethod
    def pre_upgrade_validations(cls):
        from tests.zeppelin.zeppelin_onprem.preupgrade.zeppelin_pixie import ZeppelinRestClientSession as OldZeppelinRestClientSession
        session = OldZeppelinRestClientSession()
        notebook_name = "test_pre_upgrade"
        assert session.login('hrt_1', 'hrt_1'), "Failed to login as 'hrt_1' user"
        assert session.create_new_notebook(
            notebook_name=notebook_name,
            content={0: ["%spark2", "sc.version"]},
            owners='hrt_1',
            readers='hrt_3',
            writers='hrt_2'
        ), "Failed to create the notebook: test_notebook_remote_storage"

        prop_name = 'new.prop.name'
        prop_val = "new.prop.val"
        interpreter = 'md'
        interpreter_mod_conf = {'properties': {'add': {prop_name: prop_val}}}
        assert session.edit_interpreters(change_map={interpreter: interpreter_mod_conf,}), \
            "Failed to add new property for {} interpreter settings".format(interpreter)

        session.get_notebook_id(notebook_name=notebook_name)

        assert session.logout(), "Failed to logout as 'hrt_1' user"

    @classmethod
    def test_remote_storage(cls):
        notebook_hdfs_path = "/user/zeppelin/notebook/{}"
        interpreter_hdfs_path = "/user/zeppelin/conf/interpreter.json"
        auth_hdfs_path = "/user/zeppelin/conf/notebook-authorization.json"
        HDFS_USER = Config.get('hadoop', 'HDFS_USER')

        prop_val = "new.prop.val"
        session = ZeppelinRestClientSession()
        notebook_name = "test_pre_upgrade"
        assert session.login('admin', 'admin'), "Failed to login as 'admin' user"

        notebook_id = session.get_notebook_id(notebook_name=notebook_name)

        actual_notebook_path = BaseHDFS.lsr(
            user=HDFS_USER, recursive=False, path=notebook_hdfs_path.format(notebook_id)
        )
        actual_interpreter_path = BaseHDFS.cat(user=HDFS_USER, hdfspath=interpreter_hdfs_path)
        actual_auth_path = BaseHDFS.cat(user=HDFS_USER, hdfspath=auth_hdfs_path)
        assert str(actual_notebook_path).__contains__("note.json"), "Notebook is not stored in HDFS"
        assert str(actual_interpreter_path).__contains__(prop_val), "Interpreter settings is not saved in HDFS"
        assert str(actual_auth_path).__contains__(notebook_id), "Interpreter settings is not saved in HDFS"

        interpreter_info = session.get_list_of_supported_interpreters()
        duplicate_interpreters = set([x for x in interpreter_info if interpreter_info.count(x) > 1])
        assert bool(duplicate_interpreters
                    ), "Found duplicate interpreters after upgrade " + str(duplicate_interpreters)

        ambari_connector = ZeppelinAmbariAPIUtil()
        property_type = "zeppelin-site"
        zeppelin_config_fs_dir = ambari_connector.get_property_value(
            prop_type=property_type, service_property="zeppelin.config.fs.dir"
        )
        zeppelin_notebook_storage = ambari_connector.get_property_value(
            prop_type=property_type, service_property="zeppelin.notebook.storage"
        )

        logger.info("zeppelin_config_fs_dir = %s", str(zeppelin_config_fs_dir))
        logger.info("zeppelin_notebook_storage = %s", str(zeppelin_notebook_storage))
        assert zeppelin_config_fs_dir == "conf", "Found mismatch in property 'zeppelin.notebook.storage'"
        assert zeppelin_notebook_storage == "org.apache.zeppelin.notebook.repo.FileSystemNotebookRepo", \
            "Found mismatch in zeppelin_notebook_storage."

    @classmethod
    def test_livy2_interpreter_group(cls):
        test_concurrent_sessions_livy2_interpreter_group()

    @classmethod
    def test_spark2_interpreter_group(cls):
        test_concurrent_sessions_spark2_interpreter_group()

    @classmethod
    def test_jdbc_hive_interpreter(cls):
        test_concurrent_sessions_jdbc_hive_interpreter()

    @classmethod
    def test_concurrent_sessions_jdbc_sts2_interpreter(cls):
        test_concurrent_sessions_jdbc_sts2_interpreter()

    @classmethod
    def test_jdbc_phoenix_interpreter(cls):
        test_concurrent_sessions_jdbc_phoenix_interpreter()

    @classmethod
    def test_jdbc_hive_hsi_interpreter(cls):
        test_concurrent_sessions_jdbc_hive_hsi_interpreter()
