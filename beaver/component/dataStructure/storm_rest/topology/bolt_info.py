class Bolt(object):
    """Bolt"""
    data = None
    expected_data_keys = [
        'outputStats', 'window', 'name', 'topologyId', 'executors', 'encodedTopologyId', 'componentType', 'inputStats',
        'boltStats', 'id', 'tasks', 'componentErrors', 'encodedId', 'executorStats', 'user', 'windowHint'
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
        return "Bolt(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_output_stats(self):
        """
        Get value for the key: outputStats
        """
        return [OneOfOutputStats(x) for x in self.data['outputStats']]

    def get_window(self):
        """
        Get value for the key: window
        """
        return str(self.data['window'])

    def get_name(self):
        """
        Get value for the key: name
        """
        return str(self.data['name'])

    def get_topology_id(self):
        """
        Get value for the key: topologyId
        """
        return str(self.data['topologyId'])

    def get_executors(self):
        """
        Get value for the key: executors
        """
        return float(self.data['executors'])

    def get_encoded_topology_id(self):
        """
        Get value for the key: encodedTopologyId
        """
        return str(self.data['encodedTopologyId'])

    def get_component_type(self):
        """
        Get value for the key: componentType
        """
        return str(self.data['componentType'])

    def get_input_stats(self):
        """
        Get value for the key: inputStats
        """
        return [OneOfInputStats(x) for x in self.data['inputStats']]

    def get_bolt_stats(self):
        """
        Get value for the key: boltStats
        """
        return [OneOfBoltStats(x) for x in self.data['boltStats']]

    def get_id(self):
        """
        Get value for the key: id
        """
        return str(self.data['id'])

    def get_tasks(self):
        """
        Get value for the key: tasks
        """
        return float(self.data['tasks'])

    def get_component_errors(self):
        """
        Get value for the key: componentErrors
        """
        return [OneOfComponentErrors(x) for x in self.data['componentErrors']]

    def get_encoded_id(self):
        """
        Get value for the key: encodedId
        """
        return str(self.data['encodedId'])

    def get_executor_stats(self):
        """
        Get value for the key: executorStats
        """
        return [OneOfExecutorStats(x) for x in self.data['executorStats']]

    def get_user(self):
        """
        Get value for the key: user
        """
        return str(self.data['user'])

    def get_window_hint(self):
        """
        Get value for the key: windowHint
        """
        return str(self.data['windowHint'])


class OneOfExecutorStats(object):
    """OneOfExecutorStats"""
    data = None
    expected_data_keys = [
        'processLatency', 'host', 'executeLatency', 'failed', 'workerLogLink', 'emitted', 'id', 'port', 'transferred',
        'executed', 'uptime', 'encodedId', 'acked', 'capacity'
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
        return "OneOfExecutorStats(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_process_latency(self):
        """
        Get value for the key: processLatency
        """
        return str(self.data['processLatency'])

    def get_host(self):
        """
        Get value for the key: host
        """
        return str(self.data['host'])

    def get_execute_latency(self):
        """
        Get value for the key: executeLatency
        """
        return str(self.data['executeLatency'])

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_worker_log_link(self):
        """
        Get value for the key: workerLogLink
        """
        return str(self.data['workerLogLink'])

    def get_emitted(self):
        """
        Get value for the key: emitted
        """
        return float(self.data['emitted'])

    def get_id(self):
        """
        Get value for the key: id
        """
        return str(self.data['id'])

    def get_port(self):
        """
        Get value for the key: port
        """
        return float(self.data['port'])

    def get_transferred(self):
        """
        Get value for the key: transferred
        """
        return float(self.data['transferred'])

    def get_executed(self):
        """
        Get value for the key: executed
        """
        return float(self.data['executed'])

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

    def get_acked(self):
        """
        Get value for the key: acked
        """
        return float(self.data['acked'])

    def get_capacity(self):
        """
        Get value for the key: capacity
        """
        return str(self.data['capacity'])


class OneOfComponentErrors(object):
    """OneOfComponentErrors"""
    data = None
    expected_data_keys = []

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
        return "OneOfComponentErrors(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfBoltStats(object):
    """OneOfBoltStats"""
    data = None
    expected_data_keys = [
        'processLatency', 'window', 'windowPretty', 'executeLatency', 'failed', 'emitted', 'transferred', 'executed',
        'acked'
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
        return "OneOfBoltStats(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_process_latency(self):
        """
        Get value for the key: processLatency
        """
        return str(self.data['processLatency'])

    def get_window(self):
        """
        Get value for the key: window
        """
        return str(self.data['window'])

    def get_window_pretty(self):
        """
        Get value for the key: windowPretty
        """
        return str(self.data['windowPretty'])

    def get_execute_latency(self):
        """
        Get value for the key: executeLatency
        """
        return str(self.data['executeLatency'])

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_emitted(self):
        """
        Get value for the key: emitted
        """
        return float(self.data['emitted'])

    def get_transferred(self):
        """
        Get value for the key: transferred
        """
        return float(self.data['transferred'])

    def get_executed(self):
        """
        Get value for the key: executed
        """
        return float(self.data['executed'])

    def get_acked(self):
        """
        Get value for the key: acked
        """
        return float(self.data['acked'])


class OneOfInputStats(object):
    """OneOfInputStats"""
    data = None
    expected_data_keys = [
        'processLatency', 'encodedComponent', 'executeLatency', 'failed', 'executed', 'stream', 'component', 'acked'
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
        return "OneOfInputStats(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_process_latency(self):
        """
        Get value for the key: processLatency
        """
        return str(self.data['processLatency'])

    def get_encoded_component(self):
        """
        Get value for the key: encodedComponent
        """
        return str(self.data['encodedComponent'])

    def get_execute_latency(self):
        """
        Get value for the key: executeLatency
        """
        return str(self.data['executeLatency'])

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_executed(self):
        """
        Get value for the key: executed
        """
        return float(self.data['executed'])

    def get_stream(self):
        """
        Get value for the key: stream
        """
        return str(self.data['stream'])

    def get_component(self):
        """
        Get value for the key: component
        """
        return str(self.data['component'])

    def get_acked(self):
        """
        Get value for the key: acked
        """
        return float(self.data['acked'])


class OneOfOutputStats(object):
    """OneOfOutputStats"""
    data = None
    expected_data_keys = ['stream', 'emitted', 'transferred']

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
        return "OneOfOutputStats(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_stream(self):
        """
        Get value for the key: stream
        """
        return str(self.data['stream'])

    def get_emitted(self):
        """
        Get value for the key: emitted
        """
        return float(self.data['emitted'])

    def get_transferred(self):
        """
        Get value for the key: transferred
        """
        return float(self.data['transferred'])
