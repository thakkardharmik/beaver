class TopologiesSummary(object):
    """TopologiesSummary"""
    data = None
    expected_data_keys = ['topologies']

    def check_data(self, data):
        """
        Check the initialized data.
        :param data: data to be checked
        """
        expected_keys = set(self.expected_data_keys)
        actual_keys = set(data.keys())
        if expected_keys == actual_keys:
            return
        import logging
        logger = logging.getLogger(__name__)
        if len(actual_keys - expected_keys) > 0:
            logger.warning("actual_keys - expected_keys = %s", (actual_keys - expected_keys))
        if len(expected_keys - actual_keys) > 0:
            logger.warning("expected_keys - actual_keys = %s", (expected_keys - actual_keys))

    def __init__(self, data):
        """
        Initialize this class with dictionary or json.
        :param data: data must be a json string or a dictionary
        :return:
        """
        if isinstance(data, str):
            import json
            self.data = json.loads(data)
        else:
            self.data = data
        self.check_data(self.data)

    def get_data_dictionary(self):
        return self.data

    def __str__(self):
        return "TopologiesSummary(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_topologies(self):
        """
        Get value for the key: topologies
        """
        return [OneOfTopologies(x) for x in self.data['topologies']]


class OneOfTopologies(object):
    """OneOfTopologies"""
    data = None
    expected_data_keys = [
        'executorsTotal', 'name', 'replicationCount', 'workersTotal', 'id', 'tasksTotal', 'schedulerInfo', 'status',
        'uptime', 'encodedId', 'owner'
    ]

    def check_data(self, data):
        """
        Check the initialized data.
        :param data: data to be checked
        """
        expected_keys = set(self.expected_data_keys)
        actual_keys = set(data.keys())
        if expected_keys == actual_keys:
            return
        import logging
        logger = logging.getLogger(__name__)
        if len(actual_keys - expected_keys) > 0:
            logger.warning("actual_keys - expected_keys = %s", (actual_keys - expected_keys))
        if len(expected_keys - actual_keys) > 0:
            logger.warning("expected_keys - actual_keys = %s", (expected_keys - actual_keys))

    def __init__(self, data):
        """
        Initialize this class with dictionary or json.
        :param data: data must be a json string or a dictionary
        :return:
        """
        if isinstance(data, str):
            import json
            self.data = json.loads(data)
        else:
            self.data = data
        self.check_data(self.data)

    def get_data_dictionary(self):
        return self.data

    def __str__(self):
        return "OneOfTopologies(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_executors_total(self):
        """
        Get value for the key: executorsTotal
        """
        return float(self.data['executorsTotal'])

    def get_name(self):
        """
        Get value for the key: name
        """
        return str(self.data['name'])

    def get_replication_count(self):
        """
        Get value for the key: replicationCount
        """
        return float(self.data['replicationCount'])

    def get_workers_total(self):
        """
        Get value for the key: workersTotal
        """
        return float(self.data['workersTotal'])

    def get_id(self):
        """
        Get value for the key: id
        """
        return str(self.data['id'])

    def get_tasks_total(self):
        """
        Get value for the key: tasksTotal
        """
        return float(self.data['tasksTotal'])

    def get_scheduler_info(self):
        """
        Get value for the key: schedulerInfo
        """
        return str(self.data['schedulerInfo'])

    def get_status(self):
        """
        Get value for the key: status
        """
        return str(self.data['status'])

    def get_uptime(self):
        """
        Get value for the key: uptime
        """
        return str(self.data['uptime'])

    def get_encoded_id(self):
        """
        Get value for the key: encodedId
        """
        return str(self.data['encodedId'])

    def get_owner(self):
        """
        Get value for the key: owner
        """
        return str(self.data['owner'])
