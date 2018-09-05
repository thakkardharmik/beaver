import collections
import logging

from beaver import util
from beaver.component.dataStructure.storm_rest.topology import topology_summary
from beaver.component.dataStructure.storm_rest.topology import topology_info
from beaver.component.dataStructure.storm_rest.topology import spout_info
from beaver.component.dataStructure.storm_rest.topology import bolt_info

WorkerLog = collections.namedtuple("WorkerLog", "host log_link")
logger = logging.getLogger(__name__)


class StormRestClient(object):
    def __init__(self, host, port="8744"):
        assert host, "UI host can't be null"
        self.host = host
        self.api_ep = "http://%s:%s/api/v1/" % (host, port)
        self.topology_ep = self.api_ep + "topology/"

    @staticmethod
    def __fetch_url(url, verbose=False):
        # type: (str) -> str
        if not isinstance(url, str):
            raise TypeError("log_link = %s must be of type str" % str(url))
        if verbose:
            logger.info("Fetching url = %s" % url)
        exit_code, stdout = util.curl(url)
        if exit_code != 200:
            raise RuntimeError(
                "fetch worker_log from %s failed: exit_code = %s stdout = %s" % (url, str(exit_code), str(stdout))
            )
        return stdout

    @staticmethod
    def __get_json(url):
        import json
        url_content = StormRestClient.__fetch_url(url)
        json_content = json.loads(url_content)
        return json_content

    def get_topologies(self):
        topology_summary_url = (self.topology_ep + "summary")
        all_topology_summary = topology_summary.TopologiesSummary(self.__get_json(topology_summary_url))
        return all_topology_summary

    def get_running_topologies(self):
        topologies_summary = self.get_topologies()
        return [topology for topology in topologies_summary.get_topologies() if topology.get_status() == "ACTIVE"]

    def get_running_topology(self):
        running_topologies = self.get_running_topologies()
        assert len(running_topologies) == 1, "Expected 1 running topology found: %s" % running_topologies
        return running_topologies[0]

    def get_topology_info(self, one_topology):
        if not isinstance(one_topology, topology_summary.OneOfTopologies):
            raise TypeError("one_topology = %s must be of type topology_summary.OneOfTopologies" % str(one_topology))
        topology_info_url = self.topology_ep + one_topology.get_id()
        one_topology_info = topology_info.TopologySummary(self.__get_json(topology_info_url))
        return one_topology_info

    def get_spouts_summary(self, one_topology):
        """
        :type one_topology: topology_summary.OneOfTopologies
        """
        if not isinstance(one_topology, topology_summary.OneOfTopologies):
            raise TypeError("one_topology = %s must be of type topology_summary.OneOfTopologies" % str(one_topology))
        topo_info = self.get_topology_info(one_topology)
        spouts_summary = topo_info.get_spouts()
        return spouts_summary

    def get_bolts_summary(self, one_topology):
        """
        :type one_topology: topology_summary.OneOfTopologies
        """
        if not isinstance(one_topology, topology_summary.OneOfTopologies):
            raise TypeError("one_topology = %s must be of type topology_summary.OneOfTopologies" % str(one_topology))
        topo_info = self.get_topology_info(one_topology)
        bolts_summary = topo_info.get_bolts()
        return bolts_summary

    def get_spout_info(self, one_topology, one_spout_summary):
        """
        :type one_topology: topology_summary.OneOfTopologies
        :type one_spout_summary: topology_info.OneOfSpouts
        """
        if not isinstance(one_spout_summary, topology_info.OneOfSpouts):
            raise TypeError(
                "one_spout_summary = %s must be of type topology_info.OneOfSpouts" % str(one_spout_summary)
            )
        if not isinstance(one_topology, topology_summary.OneOfTopologies):
            raise TypeError("one_topology = %s must be of type topology_summary.OneOfTopologies" % str(one_topology))
        topo_id = one_topology.get_id()
        url = self.topology_ep + "%s/component/%s" % (topo_id, one_spout_summary.get_spout_id())
        sp_info = spout_info.Spout(self.__get_json(url))
        return sp_info

    def get_bolt_info(self, one_topology, one_bolt_summary):
        if not isinstance(one_bolt_summary, topology_info.OneOfBolts):
            raise TypeError("one_bolt_summary = %s must be of type topology_info.OneOfBolts" % str(one_bolt_summary))
        if not isinstance(one_topology, topology_summary.OneOfTopologies):
            raise TypeError("one_topology = %s must be of type topology_summary.OneOfTopologies" % str(one_topology))
        topo_id = one_topology.get_id()
        url = self.topology_ep + "%s/component/%s" % (topo_id, one_bolt_summary.get_bolt_id())
        bo_info = bolt_info.Bolt(self.__get_json(url))
        return bo_info

    def get_worker_logs(self, running_topology):
        """
        :type running_topology: topology_summary.OneOfTopologies
        :rtype: set[WorkerLog]
        """
        if not isinstance(running_topology, topology_summary.OneOfTopologies):
            raise TypeError(
                "one_topology = %s must be of type topology_summary.OneOfTopologies" % str(running_topology)
            )
        spouts_summary = self.get_spouts_summary(running_topology)
        bolts_summary = self.get_bolts_summary(running_topology)
        spouts_info = [self.get_spout_info(running_topology, one_summary) for one_summary in spouts_summary]
        bolts_info = [self.get_bolt_info(running_topology, one_summary) for one_summary in bolts_summary]
        logs = [
            WorkerLog(one_stats.get_host(), one_stats.get_worker_log_link())
            for one_info in spouts_info
            for one_stats in one_info.get_executor_stats()
        ]
        logs.extend(
            [
                WorkerLog(one_stats.get_host(), one_stats.get_worker_log_link())
                for one_info in bolts_info
                for one_stats in one_info.get_executor_stats()
            ]
        )
        return set(logs)

    def _fetch_all_worker_log(self, running_topology):
        """
        :type running_topology: topology_summary.OneOfTopologies
        :rtype: set[WorkerLog]
        """
        worker_logs = self.get_worker_logs(running_topology)
        fetched_logs = [self.__fetch_url(one_worker_log.log_link) for one_worker_log in worker_logs]
        return fetched_logs

    def fetch_all_worker_logs(self, topology_name):
        """
        Fetch all worker logs and return it as a list of strings
        :param topology_name: str
        :return: List[str]
        """
        running_topologies = self.get_running_topologies()  # type: List[topology_summary.OneOfTopologies]
        filtered_topologies = [one_topo for one_topo in running_topologies if topology_name == one_topo.get_name()]
        assert len(filtered_topologies) == 1, "Expected one running topology, found: %s" % str(filtered_topologies)
        worker_logs = self._fetch_all_worker_log(filtered_topologies[0])
        return worker_logs

    def fetch_combined_worker_log(self, topology_name):
        """
        :type topology_name: str
        :rtype: str
        """
        worker_logs = self.fetch_all_worker_logs(topology_name)
        return "\n".join(worker_logs)
