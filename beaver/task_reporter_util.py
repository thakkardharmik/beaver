import copy
import logging
import traceback
from taskreporter.client import swagger_client
from taskreporter.client.swagger_client.configuration import Configuration
from taskreporter.taskreporter import TaskReporter, TaskTemplate
from beaver import util
from beaver.config import Config

logger = logging.getLogger(__name__)


def get_taskname():
    return Config.get('taskreporter', 'TASK_NAME')


def get_taskid():
    return Config.get('taskreporter', 'TASK_ID')


def get_taskowner():
    return Config.get('taskreporter', 'TASK_OWNER')


def get_taskparent_id():
    return Config.get('taskreporter', 'TASK_PARENT_ID')


def get_task_input():
    return Config.get('taskreporter', 'TASK_INPUT')


def construct_task_metadata():
    task_metadata = copy.deepcopy(TaskTemplate.task_metadata_template)

    run_id = Config.get('common', 'RUN_ID')
    logger.info("Run_Id = %s", run_id)
    if run_id and run_id.isdigit():
        task_metadata["run_id"] = run_id

    testsuite_file = util.get_TESTSUITE_COMPONENT()
    logger.info("TestSuite_file = %s", testsuite_file)
    task_metadata["component_name"] = testsuite_file

    split_id = util.get_SPLIT_ID()
    logger.info("split_id = %s", split_id)
    task_metadata["split_id"] = split_id

    split_num = Config.get('common', 'SPLIT_NUM')
    logger.info("split_num = %s", split_num)
    task_metadata["split_num"] = split_num

    task_metadata["job_name"] = "test"
    task_metadata["cluster_name"] = Config.get('common', 'CLUSTER', default="None")
    task_metadata["logserver_base_url"] = "http://testqelog.s3.amazonaws.com/qelogs/nat"
    task_metadata["sa_base_url"] = "http://mool.eng.hortonworks.com:9000/#!/log_search"

    scheme = "http"
    dashboard_host = util.get_Dashboard_Host()
    dashboard_port = util.get_Dashboard_Port()
    api_instance = None

    try:
        dashboard_host = "solr-taskreporter.eng.hortonworks.com"
        dashboard_port = 80
        base_path = "/trace/api/v1"
        config_obj = Configuration()
        config_obj.host = scheme + "://" + str(dashboard_host) + ":"
        config_obj.host += str(dashboard_port) + base_path
        api_instance = swagger_client.TaskClientApi(configuration=config_obj)
    except TypeError:
        logger.info("task reporter is going to use v1 version api")
        dashboard_host = util.get_Dashboard_Host()
        dashboard_port = util.get_Dashboard_Port()
        base_path = util.get_Dashboard_basepath()
        config_obj = Configuration()
        config_obj.host = scheme + "://" + str(dashboard_host) + ":"
        config_obj.host += str(dashboard_port) + base_path
        api_instance = swagger_client.TaskClientApi()
    except Exception as e:
        logger.error("exception occurred during construct_task_metadata: " + str(e))
        logger.error(str(traceback.format_exc()))
        task_metadata["skip_reporting"] = True

    logger.info("dashboard host = %s", dashboard_host)
    task_metadata["dashboard_host"] = dashboard_host

    logger.info("dashboard port = %s", dashboard_port)
    task_metadata["dashboard_port"] = dashboard_port

    task_metadata["api_instance"] = api_instance

    task_metadata["git_repo_name"] = "certification"
    task_metadata["git_repo_url"] = "https://github.com/hortonworks/" + task_metadata["git_repo_name"]

    try:
        git_commit_id = Config.get("common", "CERTIFICATION_COMMIT_ID", default=None)
    except:
        git_commit_id = None

    if git_commit_id:
        task_metadata["git_commit_id"] = git_commit_id

    cluster_type = Config.get("machine", "CLUSTER_TYPE", "ycloud")
    if cluster_type == "ycloud":
        work_dir = "/hwqe/hadoopqe"
    elif cluster_type == "humboldt":
        work_dir = "/mnt/hadoopqe"
    else:
        work_dir = "/grid/0"
    task_metadata["work_dir"] = work_dir
    task_metadata["git_prefix_path"] = "HDPTests"

    try:
        enable_task_reporting = Config.get('common', 'ENABLE_TASK_REPORTING', default='yes')
    except:
        enable_task_reporting = "no"

    if enable_task_reporting == "no" or cluster_type == "humboldt":
        task_metadata["skip_reporting"] = True

    # Temporarily skipping task reporting due to load issues in Taskreporting API
    # task_metadata["skip_reporting"] = True

    logger.info("final task_metadata = " + str(task_metadata))
    TaskReporter.set_task_metadata(task_metadata)


def task_bootstrap():
    logging.debug("**** task bootstrap for Pytest started ********")
    if TaskReporter.task_metadata["skip_reporting"]:
        logging.debug("skip_reporting is enabled, going to skip the task_bootstrap")
        return

    task_info = copy.deepcopy(TaskTemplate.task_info_template)
    task_info["task_name"] = get_taskname()
    task_info["task_id"] = get_taskid()
    task_info["parent_id"] = get_taskparent_id()
    task_info["owner"] = util.get_qe_group_email()
    TaskReporter.push_stack_info(task_info)
    logging.debug("**** task bootstrap for Pytest ended ********")
