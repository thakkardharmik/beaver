class TopologySummary(object):
    """TopologySummary"""
    data = None
    expected_data_keys = [
        'executorsTotal', 'window', 'name', 'replicationCount', 'spouts', 'msgTimeout', 'workersTotal', 'id',
        'tasksTotal', 'schedulerInfo', 'status', 'visualizationTable', 'configuration', 'bolts', 'uptime', 'encodedId',
        'owner', 'topologyStats', 'user', 'windowHint'
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
        return "TopologySummary(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_executors_total(self):
        """
        Get value for the key: executorsTotal
        """
        return float(self.data['executorsTotal'])

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

    def get_replication_count(self):
        """
        Get value for the key: replicationCount
        """
        return float(self.data['replicationCount'])

    def get_spouts(self):
        """
        Get value for the key: spouts
        """
        return [OneOfSpouts(x) for x in self.data['spouts']]

    def get_msg_timeout(self):
        """
        Get value for the key: msgTimeout
        """
        return float(self.data['msgTimeout'])

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

    def get_visualization_table(self):
        """
        Get value for the key: visualizationTable
        """
        return [OneOfVisualizationTable(x) for x in self.data['visualizationTable']]

    def get_configuration(self):
        """
        Get value for the key: configuration
        """
        return Configuration(self.data['configuration'])

    def get_bolts(self):
        """
        Get value for the key: bolts
        """
        return [OneOfBolts(x) for x in self.data['bolts']]

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

    def get_topology_stats(self):
        """
        Get value for the key: topologyStats
        """
        return [OneOfTopologyStats(x) for x in self.data['topologyStats']]

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


class OneOfTopologyStats(object):
    """OneOfTopologyStats"""
    data = None
    expected_data_keys = ['window', 'windowPretty', 'failed', 'completeLatency', 'emitted', 'transferred', 'acked']

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
        return "OneOfTopologyStats(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

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

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_complete_latency(self):
        """
        Get value for the key: completeLatency
        """
        return str(self.data['completeLatency'])

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

    def get_acked(self):
        """
        Get value for the key: acked
        """
        return float(self.data['acked'])


class OneOfBolts(object):
    """OneOfBolts"""
    data = None
    expected_data_keys = [
        'processLatency', 'boltId', 'executeLatency', 'encodedBoltId', 'executors', 'failed', 'errorPort', 'emitted',
        'lastError', 'tasks', 'transferred', 'executed', 'errorHost', 'errorLapsedSecs', 'errorWorkerLogLink', 'acked',
        'capacity'
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
        return "OneOfBolts(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_process_latency(self):
        """
        Get value for the key: processLatency
        """
        return str(self.data['processLatency'])

    def get_bolt_id(self):
        """
        Get value for the key: boltId
        """
        return str(self.data['boltId'])

    def get_execute_latency(self):
        """
        Get value for the key: executeLatency
        """
        return str(self.data['executeLatency'])

    def get_encoded_bolt_id(self):
        """
        Get value for the key: encodedBoltId
        """
        return str(self.data['encodedBoltId'])

    def get_executors(self):
        """
        Get value for the key: executors
        """
        return float(self.data['executors'])

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_error_port(self):
        """
        Get value for the key: errorPort
        """
        return str(self.data['errorPort'])

    def get_emitted(self):
        """
        Get value for the key: emitted
        """
        return float(self.data['emitted'])

    def get_last_error(self):
        """
        Get value for the key: lastError
        """
        return str(self.data['lastError'])

    def get_tasks(self):
        """
        Get value for the key: tasks
        """
        return float(self.data['tasks'])

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

    def get_error_host(self):
        """
        Get value for the key: errorHost
        """
        return str(self.data['errorHost'])

    def get_error_lapsed_secs(self):
        """
        Get value for the key: errorLapsedSecs
        """
        return str(self.data['errorLapsedSecs'])

    def get_error_worker_log_link(self):
        """
        Get value for the key: errorWorkerLogLink
        """
        return str(self.data['errorWorkerLogLink'])

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


class Configuration(object):
    """Configuration"""
    data = None
    expected_data_keys = [
        'topology.classpath', 'storm.auth.simple-acl.users', 'topology.bolts.outgoing.overflow.buffer.enable',
        'topology.kryo.factory', 'storm.auth.simple-acl.users.commands', 'logviewer.cleanup.age.mins',
        'topology.workers', 'drpc.invocations.threads', 'topology.acker.executors',
        'storm.group.mapping.service.cache.duration.secs', 'topology.submitter.principal',
        'topology.builtin.metrics.bucket.size.secs', 'supervisor.enable', 'supervisor.supervisors',
        'storm.zookeeper.port', 'drpc.https.keystore.password', 'storm.zookeeper.retry.interval',
        'drpc.worker.threads', 'dev.zookeeper.path', 'topology.executor.send.buffer.size', 'storm.zookeeper.auth.user',
        'topology.min.replication.count', 'topology.tasks', 'supervisor.worker.shutdown.sleep.secs',
        'worker.childopts', 'logs.users', 'zmq.linger.millis', 'transactional.zookeeper.servers',
        'nimbus.credential.renewers.freq.secs', 'storm.auth.simple-acl.admins', 'task.refresh.poll.secs',
        'storm.nimbus.retry.times', 'java.library.path', 'ui.port', 'drpc.invocations.port',
        'topology.multilang.serializer', 'nimbus.topology.validator', 'topology.name', 'drpc.queue.size',
        'storm.zookeeper.root', 'topology.state.synchronization.timeout.secs',
        'storm.nimbus.retry.intervalceiling.millis', 'drpc.max_buffer_size', 'topology.worker.childopts',
        'drpc.childopts', 'supervisor.run.worker.as.user', 'storm.nimbus.retry.interval.millis',
        'storm.zookeeper.retry.times', 'storm.local.mode.zmq', 'topology.testing.always.try.serialize',
        'ui.actions.enabled', 'topology.executor.receive.buffer.size', 'topology.kryo.decorators',
        'worker.heartbeat.frequency.secs', 'nimbus.thrift.threads', 'ui.filter.params', 'topology.spout.wait.strategy',
        'topology.sleep.spout.wait.strategy.time.ms', 'topology.trident.batch.emit.interval.millis',
        'storm.messaging.transport', 'storm.messaging.netty.flush.check.interval.ms', 'nimbus.cleanup.inbox.freq.secs',
        'topology.metrics.consumer.register', 'storm.zookeeper.connection.timeout', 'drpc.authorizer.acl.filename',
        'topology.kryo.register', 'task.heartbeat.frequency.secs', 'topology.debug', 'nimbus.task.timeout.secs',
        'topology.environment', 'storm.zookeeper.retry.intervalceiling.millis', 'transactional.zookeeper.port',
        'topology.skip.missing.kryo.registrations', 'nimbus.reassign', 'topology.message.timeout.secs', 'zmq.hwm',
        'nimbus.thrift.port', 'storm.messaging.netty.buffer_size', 'drpc.port', 'storm.messaging.netty.socket.backlog',
        'supervisor.childopts', 'zmq.threads', 'storm.meta.serialization.delegate', 'nimbus.code.sync.freq.secs',
        'topology.max.replication.wait.time.sec', 'metrics.reporter.register', 'logviewer.port', 'nimbus.childopts',
        'topology.disruptor.wait.strategy', 'ui.http.creds.plugin', 'topology.fall.back.on.java.serialization',
        'drpc.http.creds.plugin', 'supervisor.monitor.frequency.secs', 'supervisor.slots.ports',
        'storm.auth.simple-white-list.users', 'storm.messaging.netty.authentication', 'storm.local.dir',
        'nimbus.thrift.max_buffer_size', 'topology.submitter.user', 'topology.max.task.parallelism',
        'storm.group.mapping.service', 'drpc.request.timeout.secs', 'worker.log.level.reset.poll.secs',
        'topology.users', 'topology.max.error.report.per.interval', 'storm.zookeeper.servers',
        'storm.messaging.netty.server_worker_threads', 'drpc.authorizer.acl.strict',
        'storm.messaging.netty.client_worker_threads', 'drpc.https.keystore.type',
        'supervisor.heartbeat.frequency.secs', 'nimbus.monitor.freq.secs', 'ui.host',
        'topology.enable.message.timeouts', 'ui.childopts', 'storm.cluster.mode', 'storm.zookeeper.superACL',
        'ui.header.buffer.bytes', 'supervisor.worker.start.timeout.secs', 'supervisor.worker.timeout.secs',
        'topology.tick.tuple.freq.secs', 'storm.log.dir', 'storm.messaging.netty.min_wait_ms',
        'storm.codedistributor.class', 'topology.error.throttle.interval.secs', 'logviewer.appender.name',
        'nimbus.inbox.jar.expiration.secs', 'storm.zookeeper.auth.password', 'storm.id',
        'nimbus.supervisor.timeout.secs', 'logviewer.childopts', 'storm.messaging.netty.max_wait_ms',
        'storm.thrift.transport', 'topology.transfer.buffer.size', 'transactional.zookeeper.root',
        'nimbus.file.copy.expiration.secs', 'worker.gc.childopts', 'storm.messaging.netty.transfer.batch.size',
        'nimbus.task.launch.secs', 'storm.principal.tolocal', 'ui.users', 'storm.zookeeper.session.timeout',
        'task.credentials.poll.secs', 'drpc.http.port', 'supervisor.supervisors.commands',
        'topology.worker.receiver.thread.count', 'topology.max.spout.pending', 'topology.stats.sample.rate',
        'nimbus.seeds', 'topology.receiver.buffer.size', 'topology.tuple.serializer',
        'topology.worker.shared.thread.pool.size', 'storm.messaging.netty.max_retries', 'drpc.https.port', 'ui.filter',
        'topology.optimize'
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
        return "Configuration(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_topology_classpath(self):
        """
        Get value for the key: topology.classpath
        """
        return str(self.data['topology.classpath'])

    def get_storm_auth_simple_acl_users(self):
        """
        Get value for the key: storm.auth.simple-acl.users
        """
        return [OneOfStormAuthSimpleAclUsers(x) for x in self.data['storm.auth.simple-acl.users']]

    def get_topology_bolts_outgoing_overflow_buffer_enable(self):
        """
        Get value for the key: topology.bolts.outgoing.overflow.buffer.enable
        """
        return bool(self.data['topology.bolts.outgoing.overflow.buffer.enable'])

    def get_topology_kryo_factory(self):
        """
        Get value for the key: topology.kryo.factory
        """
        return str(self.data['topology.kryo.factory'])

    def get_storm_auth_simple_acl_users_commands(self):
        """
        Get value for the key: storm.auth.simple-acl.users.commands
        """
        return [OneOfStormAuthSimpleAclUsersCommands(x) for x in self.data['storm.auth.simple-acl.users.commands']]

    def get_logviewer_cleanup_age_mins(self):
        """
        Get value for the key: logviewer.cleanup.age.mins
        """
        return float(self.data['logviewer.cleanup.age.mins'])

    def get_topology_workers(self):
        """
        Get value for the key: topology.workers
        """
        return float(self.data['topology.workers'])

    def get_drpc_invocations_threads(self):
        """
        Get value for the key: drpc.invocations.threads
        """
        return float(self.data['drpc.invocations.threads'])

    def get_topology_acker_executors(self):
        """
        Get value for the key: topology.acker.executors
        """
        return str(self.data['topology.acker.executors'])

    def get_storm_group_mapping_service_cache_duration_secs(self):
        """
        Get value for the key: storm.group.mapping.service.cache.duration.secs
        """
        return float(self.data['storm.group.mapping.service.cache.duration.secs'])

    def get_topology_submitter_principal(self):
        """
        Get value for the key: topology.submitter.principal
        """
        return str(self.data['topology.submitter.principal'])

    def get_topology_builtin_metrics_bucket_size_secs(self):
        """
        Get value for the key: topology.builtin.metrics.bucket.size.secs
        """
        return float(self.data['topology.builtin.metrics.bucket.size.secs'])

    def get_supervisor_enable(self):
        """
        Get value for the key: supervisor.enable
        """
        return bool(self.data['supervisor.enable'])

    def get_supervisor_supervisors(self):
        """
        Get value for the key: supervisor.supervisors
        """
        return [OneOfSupervisorSupervisors(x) for x in self.data['supervisor.supervisors']]

    def get_storm_zookeeper_port(self):
        """
        Get value for the key: storm.zookeeper.port
        """
        return float(self.data['storm.zookeeper.port'])

    def get_drpc_https_keystore_password(self):
        """
        Get value for the key: drpc.https.keystore.password
        """
        return str(self.data['drpc.https.keystore.password'])

    def get_storm_zookeeper_retry_interval(self):
        """
        Get value for the key: storm.zookeeper.retry.interval
        """
        return float(self.data['storm.zookeeper.retry.interval'])

    def get_drpc_worker_threads(self):
        """
        Get value for the key: drpc.worker.threads
        """
        return float(self.data['drpc.worker.threads'])

    def get_dev_zookeeper_path(self):
        """
        Get value for the key: dev.zookeeper.path
        """
        return str(self.data['dev.zookeeper.path'])

    def get_topology_executor_send_buffer_size(self):
        """
        Get value for the key: topology.executor.send.buffer.size
        """
        return float(self.data['topology.executor.send.buffer.size'])

    def get_storm_zookeeper_auth_user(self):
        """
        Get value for the key: storm.zookeeper.auth.user
        """
        return str(self.data['storm.zookeeper.auth.user'])

    def get_topology_min_replication_count(self):
        """
        Get value for the key: topology.min.replication.count
        """
        return float(self.data['topology.min.replication.count'])

    def get_topology_tasks(self):
        """
        Get value for the key: topology.tasks
        """
        return str(self.data['topology.tasks'])

    def get_supervisor_worker_shutdown_sleep_secs(self):
        """
        Get value for the key: supervisor.worker.shutdown.sleep.secs
        """
        return float(self.data['supervisor.worker.shutdown.sleep.secs'])

    def get_worker_childopts(self):
        """
        Get value for the key: worker.childopts
        """
        return str(self.data['worker.childopts'])

    def get_logs_users(self):
        """
        Get value for the key: logs.users
        """
        return str(self.data['logs.users'])

    def get_zmq_linger_millis(self):
        """
        Get value for the key: zmq.linger.millis
        """
        return float(self.data['zmq.linger.millis'])

    def get_transactional_zookeeper_servers(self):
        """
        Get value for the key: transactional.zookeeper.servers
        """
        return str(self.data['transactional.zookeeper.servers'])

    def get_nimbus_credential_renewers_freq_secs(self):
        """
        Get value for the key: nimbus.credential.renewers.freq.secs
        """
        return float(self.data['nimbus.credential.renewers.freq.secs'])

    def get_storm_auth_simple_acl_admins(self):
        """
        Get value for the key: storm.auth.simple-acl.admins
        """
        return [OneOfStormAuthSimpleAclAdmins(x) for x in self.data['storm.auth.simple-acl.admins']]

    def get_task_refresh_poll_secs(self):
        """
        Get value for the key: task.refresh.poll.secs
        """
        return float(self.data['task.refresh.poll.secs'])

    def get_storm_nimbus_retry_times(self):
        """
        Get value for the key: storm.nimbus.retry.times
        """
        return float(self.data['storm.nimbus.retry.times'])

    def get_java_library_path(self):
        """
        Get value for the key: java.library.path
        """
        return str(self.data['java.library.path'])

    def get_ui_port(self):
        """
        Get value for the key: ui.port
        """
        return float(self.data['ui.port'])

    def get_drpc_invocations_port(self):
        """
        Get value for the key: drpc.invocations.port
        """
        return float(self.data['drpc.invocations.port'])

    def get_topology_multilang_serializer(self):
        """
        Get value for the key: topology.multilang.serializer
        """
        return str(self.data['topology.multilang.serializer'])

    def get_nimbus_topology_validator(self):
        """
        Get value for the key: nimbus.topology.validator
        """
        return str(self.data['nimbus.topology.validator'])

    def get_topology_name(self):
        """
        Get value for the key: topology.name
        """
        return str(self.data['topology.name'])

    def get_drpc_queue_size(self):
        """
        Get value for the key: drpc.queue.size
        """
        return float(self.data['drpc.queue.size'])

    def get_storm_zookeeper_root(self):
        """
        Get value for the key: storm.zookeeper.root
        """
        return str(self.data['storm.zookeeper.root'])

    def get_topology_state_synchronization_timeout_secs(self):
        """
        Get value for the key: topology.state.synchronization.timeout.secs
        """
        return float(self.data['topology.state.synchronization.timeout.secs'])

    def get_storm_nimbus_retry_intervalceiling_millis(self):
        """
        Get value for the key: storm.nimbus.retry.intervalceiling.millis
        """
        return float(self.data['storm.nimbus.retry.intervalceiling.millis'])

    def get_drpc_max_buffer_size(self):
        """
        Get value for the key: drpc.max_buffer_size
        """
        return float(self.data['drpc.max_buffer_size'])

    def get_topology_worker_childopts(self):
        """
        Get value for the key: topology.worker.childopts
        """
        return str(self.data['topology.worker.childopts'])

    def get_drpc_childopts(self):
        """
        Get value for the key: drpc.childopts
        """
        return str(self.data['drpc.childopts'])

    def get_supervisor_run_worker_as_user(self):
        """
        Get value for the key: supervisor.run.worker.as.user
        """
        return bool(self.data['supervisor.run.worker.as.user'])

    def get_storm_nimbus_retry_interval_millis(self):
        """
        Get value for the key: storm.nimbus.retry.interval.millis
        """
        return float(self.data['storm.nimbus.retry.interval.millis'])

    def get_storm_zookeeper_retry_times(self):
        """
        Get value for the key: storm.zookeeper.retry.times
        """
        return float(self.data['storm.zookeeper.retry.times'])

    def get_storm_local_mode_zmq(self):
        """
        Get value for the key: storm.local.mode.zmq
        """
        return bool(self.data['storm.local.mode.zmq'])

    def get_topology_testing_always_try_serialize(self):
        """
        Get value for the key: topology.testing.always.try.serialize
        """
        return bool(self.data['topology.testing.always.try.serialize'])

    def get_ui_actions_enabled(self):
        """
        Get value for the key: ui.actions.enabled
        """
        return bool(self.data['ui.actions.enabled'])

    def get_topology_executor_receive_buffer_size(self):
        """
        Get value for the key: topology.executor.receive.buffer.size
        """
        return float(self.data['topology.executor.receive.buffer.size'])

    def get_topology_kryo_decorators(self):
        """
        Get value for the key: topology.kryo.decorators
        """
        return [OneOfTopologyKryoDecorators(x) for x in self.data['topology.kryo.decorators']]

    def get_worker_heartbeat_frequency_secs(self):
        """
        Get value for the key: worker.heartbeat.frequency.secs
        """
        return float(self.data['worker.heartbeat.frequency.secs'])

    def get_nimbus_thrift_threads(self):
        """
        Get value for the key: nimbus.thrift.threads
        """
        return float(self.data['nimbus.thrift.threads'])

    def get_ui_filter_params(self):
        """
        Get value for the key: ui.filter.params
        """
        return str(self.data['ui.filter.params'])

    def get_topology_spout_wait_strategy(self):
        """
        Get value for the key: topology.spout.wait.strategy
        """
        return str(self.data['topology.spout.wait.strategy'])

    def get_topology_sleep_spout_wait_strategy_time_ms(self):
        """
        Get value for the key: topology.sleep.spout.wait.strategy.time.ms
        """
        return float(self.data['topology.sleep.spout.wait.strategy.time.ms'])

    def get_topology_trident_batch_emit_interval_millis(self):
        """
        Get value for the key: topology.trident.batch.emit.interval.millis
        """
        return float(self.data['topology.trident.batch.emit.interval.millis'])

    def get_storm_messaging_transport(self):
        """
        Get value for the key: storm.messaging.transport
        """
        return str(self.data['storm.messaging.transport'])

    def get_storm_messaging_netty_flush_check_interval_ms(self):
        """
        Get value for the key: storm.messaging.netty.flush.check.interval.ms
        """
        return float(self.data['storm.messaging.netty.flush.check.interval.ms'])

    def get_nimbus_cleanup_inbox_freq_secs(self):
        """
        Get value for the key: nimbus.cleanup.inbox.freq.secs
        """
        return float(self.data['nimbus.cleanup.inbox.freq.secs'])

    def get_topology_metrics_consumer_register(self):
        """
        Get value for the key: topology.metrics.consumer.register
        """
        return [OneOfTopologyMetricsConsumerRegister(x) for x in self.data['topology.metrics.consumer.register']]

    def get_storm_zookeeper_connection_timeout(self):
        """
        Get value for the key: storm.zookeeper.connection.timeout
        """
        return float(self.data['storm.zookeeper.connection.timeout'])

    def get_drpc_authorizer_acl_filename(self):
        """
        Get value for the key: drpc.authorizer.acl.filename
        """
        return str(self.data['drpc.authorizer.acl.filename'])

    def get_topology_kryo_register(self):
        """
        Get value for the key: topology.kryo.register
        """
        return str(self.data['topology.kryo.register'])

    def get_task_heartbeat_frequency_secs(self):
        """
        Get value for the key: task.heartbeat.frequency.secs
        """
        return float(self.data['task.heartbeat.frequency.secs'])

    def get_topology_debug(self):
        """
        Get value for the key: topology.debug
        """
        return bool(self.data['topology.debug'])

    def get_nimbus_task_timeout_secs(self):
        """
        Get value for the key: nimbus.task.timeout.secs
        """
        return float(self.data['nimbus.task.timeout.secs'])

    def get_topology_environment(self):
        """
        Get value for the key: topology.environment
        """
        return str(self.data['topology.environment'])

    def get_storm_zookeeper_retry_intervalceiling_millis(self):
        """
        Get value for the key: storm.zookeeper.retry.intervalceiling.millis
        """
        return float(self.data['storm.zookeeper.retry.intervalceiling.millis'])

    def get_transactional_zookeeper_port(self):
        """
        Get value for the key: transactional.zookeeper.port
        """
        return str(self.data['transactional.zookeeper.port'])

    def get_topology_skip_missing_kryo_registrations(self):
        """
        Get value for the key: topology.skip.missing.kryo.registrations
        """
        return bool(self.data['topology.skip.missing.kryo.registrations'])

    def get_nimbus_reassign(self):
        """
        Get value for the key: nimbus.reassign
        """
        return bool(self.data['nimbus.reassign'])

    def get_topology_message_timeout_secs(self):
        """
        Get value for the key: topology.message.timeout.secs
        """
        return float(self.data['topology.message.timeout.secs'])

    def get_zmq_hwm(self):
        """
        Get value for the key: zmq.hwm
        """
        return float(self.data['zmq.hwm'])

    def get_nimbus_thrift_port(self):
        """
        Get value for the key: nimbus.thrift.port
        """
        return float(self.data['nimbus.thrift.port'])

    def get_storm_messaging_netty_buffer_size(self):
        """
        Get value for the key: storm.messaging.netty.buffer_size
        """
        return float(self.data['storm.messaging.netty.buffer_size'])

    def get_drpc_port(self):
        """
        Get value for the key: drpc.port
        """
        return float(self.data['drpc.port'])

    def get_storm_messaging_netty_socket_backlog(self):
        """
        Get value for the key: storm.messaging.netty.socket.backlog
        """
        return float(self.data['storm.messaging.netty.socket.backlog'])

    def get_supervisor_childopts(self):
        """
        Get value for the key: supervisor.childopts
        """
        return str(self.data['supervisor.childopts'])

    def get_zmq_threads(self):
        """
        Get value for the key: zmq.threads
        """
        return float(self.data['zmq.threads'])

    def get_storm_meta_serialization_delegate(self):
        """
        Get value for the key: storm.meta.serialization.delegate
        """
        return str(self.data['storm.meta.serialization.delegate'])

    def get_nimbus_code_sync_freq_secs(self):
        """
        Get value for the key: nimbus.code.sync.freq.secs
        """
        return float(self.data['nimbus.code.sync.freq.secs'])

    def get_topology_max_replication_wait_time_sec(self):
        """
        Get value for the key: topology.max.replication.wait.time.sec
        """
        return float(self.data['topology.max.replication.wait.time.sec'])

    def get_metrics_reporter_register(self):
        """
        Get value for the key: metrics.reporter.register
        """
        return str(self.data['metrics.reporter.register'])

    def get_logviewer_port(self):
        """
        Get value for the key: logviewer.port
        """
        return float(self.data['logviewer.port'])

    def get_nimbus_childopts(self):
        """
        Get value for the key: nimbus.childopts
        """
        return str(self.data['nimbus.childopts'])

    def get_topology_disruptor_wait_strategy(self):
        """
        Get value for the key: topology.disruptor.wait.strategy
        """
        return str(self.data['topology.disruptor.wait.strategy'])

    def get_ui_http_creds_plugin(self):
        """
        Get value for the key: ui.http.creds.plugin
        """
        return str(self.data['ui.http.creds.plugin'])

    def get_topology_fall_back_on_java_serialization(self):
        """
        Get value for the key: topology.fall.back.on.java.serialization
        """
        return bool(self.data['topology.fall.back.on.java.serialization'])

    def get_drpc_http_creds_plugin(self):
        """
        Get value for the key: drpc.http.creds.plugin
        """
        return str(self.data['drpc.http.creds.plugin'])

    def get_supervisor_monitor_frequency_secs(self):
        """
        Get value for the key: supervisor.monitor.frequency.secs
        """
        return float(self.data['supervisor.monitor.frequency.secs'])

    def get_supervisor_slots_ports(self):
        """
        Get value for the key: supervisor.slots.ports
        """
        return [OneOfSupervisorSlotsPorts(x) for x in self.data['supervisor.slots.ports']]

    def get_storm_auth_simple_white_list_users(self):
        """
        Get value for the key: storm.auth.simple-white-list.users
        """
        return [OneOfStormAuthSimpleWhiteListUsers(x) for x in self.data['storm.auth.simple-white-list.users']]

    def get_storm_messaging_netty_authentication(self):
        """
        Get value for the key: storm.messaging.netty.authentication
        """
        return bool(self.data['storm.messaging.netty.authentication'])

    def get_storm_local_dir(self):
        """
        Get value for the key: storm.local.dir
        """
        return str(self.data['storm.local.dir'])

    def get_nimbus_thrift_max_buffer_size(self):
        """
        Get value for the key: nimbus.thrift.max_buffer_size
        """
        return float(self.data['nimbus.thrift.max_buffer_size'])

    def get_topology_submitter_user(self):
        """
        Get value for the key: topology.submitter.user
        """
        return str(self.data['topology.submitter.user'])

    def get_topology_max_task_parallelism(self):
        """
        Get value for the key: topology.max.task.parallelism
        """
        return str(self.data['topology.max.task.parallelism'])

    def get_storm_group_mapping_service(self):
        """
        Get value for the key: storm.group.mapping.service
        """
        return str(self.data['storm.group.mapping.service'])

    def get_drpc_request_timeout_secs(self):
        """
        Get value for the key: drpc.request.timeout.secs
        """
        return float(self.data['drpc.request.timeout.secs'])

    def get_worker_log_level_reset_poll_secs(self):
        """
        Get value for the key: worker.log.level.reset.poll.secs
        """
        return float(self.data['worker.log.level.reset.poll.secs'])

    def get_topology_users(self):
        """
        Get value for the key: topology.users
        """
        return [OneOfTopologyUsers(x) for x in self.data['topology.users']]

    def get_topology_max_error_report_per_interval(self):
        """
        Get value for the key: topology.max.error.report.per.interval
        """
        return float(self.data['topology.max.error.report.per.interval'])

    def get_storm_zookeeper_servers(self):
        """
        Get value for the key: storm.zookeeper.servers
        """
        return [OneOfStormZookeeperServers(x) for x in self.data['storm.zookeeper.servers']]

    def get_storm_messaging_netty_server_worker_threads(self):
        """
        Get value for the key: storm.messaging.netty.server_worker_threads
        """
        return float(self.data['storm.messaging.netty.server_worker_threads'])

    def get_drpc_authorizer_acl_strict(self):
        """
        Get value for the key: drpc.authorizer.acl.strict
        """
        return bool(self.data['drpc.authorizer.acl.strict'])

    def get_storm_messaging_netty_client_worker_threads(self):
        """
        Get value for the key: storm.messaging.netty.client_worker_threads
        """
        return float(self.data['storm.messaging.netty.client_worker_threads'])

    def get_drpc_https_keystore_type(self):
        """
        Get value for the key: drpc.https.keystore.type
        """
        return str(self.data['drpc.https.keystore.type'])

    def get_supervisor_heartbeat_frequency_secs(self):
        """
        Get value for the key: supervisor.heartbeat.frequency.secs
        """
        return float(self.data['supervisor.heartbeat.frequency.secs'])

    def get_nimbus_monitor_freq_secs(self):
        """
        Get value for the key: nimbus.monitor.freq.secs
        """
        return float(self.data['nimbus.monitor.freq.secs'])

    def get_ui_host(self):
        """
        Get value for the key: ui.host
        """
        return str(self.data['ui.host'])

    def get_topology_enable_message_timeouts(self):
        """
        Get value for the key: topology.enable.message.timeouts
        """
        return bool(self.data['topology.enable.message.timeouts'])

    def get_ui_childopts(self):
        """
        Get value for the key: ui.childopts
        """
        return str(self.data['ui.childopts'])

    def get_storm_cluster_mode(self):
        """
        Get value for the key: storm.cluster.mode
        """
        return str(self.data['storm.cluster.mode'])

    def get_storm_zookeeper_super_acl(self):
        """
        Get value for the key: storm.zookeeper.superACL
        """
        return str(self.data['storm.zookeeper.superACL'])

    def get_ui_header_buffer_bytes(self):
        """
        Get value for the key: ui.header.buffer.bytes
        """
        return float(self.data['ui.header.buffer.bytes'])

    def get_supervisor_worker_start_timeout_secs(self):
        """
        Get value for the key: supervisor.worker.start.timeout.secs
        """
        return float(self.data['supervisor.worker.start.timeout.secs'])

    def get_supervisor_worker_timeout_secs(self):
        """
        Get value for the key: supervisor.worker.timeout.secs
        """
        return float(self.data['supervisor.worker.timeout.secs'])

    def get_topology_tick_tuple_freq_secs(self):
        """
        Get value for the key: topology.tick.tuple.freq.secs
        """
        return str(self.data['topology.tick.tuple.freq.secs'])

    def get_storm_log_dir(self):
        """
        Get value for the key: storm.log.dir
        """
        return str(self.data['storm.log.dir'])

    def get_storm_messaging_netty_min_wait_ms(self):
        """
        Get value for the key: storm.messaging.netty.min_wait_ms
        """
        return float(self.data['storm.messaging.netty.min_wait_ms'])

    def get_storm_codedistributor_class(self):
        """
        Get value for the key: storm.codedistributor.class
        """
        return str(self.data['storm.codedistributor.class'])

    def get_topology_error_throttle_interval_secs(self):
        """
        Get value for the key: topology.error.throttle.interval.secs
        """
        return float(self.data['topology.error.throttle.interval.secs'])

    def get_logviewer_appender_name(self):
        """
        Get value for the key: logviewer.appender.name
        """
        return str(self.data['logviewer.appender.name'])

    def get_nimbus_inbox_jar_expiration_secs(self):
        """
        Get value for the key: nimbus.inbox.jar.expiration.secs
        """
        return float(self.data['nimbus.inbox.jar.expiration.secs'])

    def get_storm_zookeeper_auth_password(self):
        """
        Get value for the key: storm.zookeeper.auth.password
        """
        return str(self.data['storm.zookeeper.auth.password'])

    def get_storm_id(self):
        """
        Get value for the key: storm.id
        """
        return str(self.data['storm.id'])

    def get_nimbus_supervisor_timeout_secs(self):
        """
        Get value for the key: nimbus.supervisor.timeout.secs
        """
        return float(self.data['nimbus.supervisor.timeout.secs'])

    def get_logviewer_childopts(self):
        """
        Get value for the key: logviewer.childopts
        """
        return str(self.data['logviewer.childopts'])

    def get_storm_messaging_netty_max_wait_ms(self):
        """
        Get value for the key: storm.messaging.netty.max_wait_ms
        """
        return float(self.data['storm.messaging.netty.max_wait_ms'])

    def get_storm_thrift_transport(self):
        """
        Get value for the key: storm.thrift.transport
        """
        return str(self.data['storm.thrift.transport'])

    def get_topology_transfer_buffer_size(self):
        """
        Get value for the key: topology.transfer.buffer.size
        """
        return float(self.data['topology.transfer.buffer.size'])

    def get_transactional_zookeeper_root(self):
        """
        Get value for the key: transactional.zookeeper.root
        """
        return str(self.data['transactional.zookeeper.root'])

    def get_nimbus_file_copy_expiration_secs(self):
        """
        Get value for the key: nimbus.file.copy.expiration.secs
        """
        return float(self.data['nimbus.file.copy.expiration.secs'])

    def get_worker_gc_childopts(self):
        """
        Get value for the key: worker.gc.childopts
        """
        return str(self.data['worker.gc.childopts'])

    def get_storm_messaging_netty_transfer_batch_size(self):
        """
        Get value for the key: storm.messaging.netty.transfer.batch.size
        """
        return float(self.data['storm.messaging.netty.transfer.batch.size'])

    def get_nimbus_task_launch_secs(self):
        """
        Get value for the key: nimbus.task.launch.secs
        """
        return float(self.data['nimbus.task.launch.secs'])

    def get_storm_principal_tolocal(self):
        """
        Get value for the key: storm.principal.tolocal
        """
        return str(self.data['storm.principal.tolocal'])

    def get_ui_users(self):
        """
        Get value for the key: ui.users
        """
        return str(self.data['ui.users'])

    def get_storm_zookeeper_session_timeout(self):
        """
        Get value for the key: storm.zookeeper.session.timeout
        """
        return float(self.data['storm.zookeeper.session.timeout'])

    def get_task_credentials_poll_secs(self):
        """
        Get value for the key: task.credentials.poll.secs
        """
        return float(self.data['task.credentials.poll.secs'])

    def get_drpc_http_port(self):
        """
        Get value for the key: drpc.http.port
        """
        return float(self.data['drpc.http.port'])

    def get_supervisor_supervisors_commands(self):
        """
        Get value for the key: supervisor.supervisors.commands
        """
        return [OneOfSupervisorSupervisorsCommands(x) for x in self.data['supervisor.supervisors.commands']]

    def get_topology_worker_receiver_thread_count(self):
        """
        Get value for the key: topology.worker.receiver.thread.count
        """
        return float(self.data['topology.worker.receiver.thread.count'])

    def get_topology_max_spout_pending(self):
        """
        Get value for the key: topology.max.spout.pending
        """
        return float(self.data['topology.max.spout.pending'])

    def get_topology_stats_sample_rate(self):
        """
        Get value for the key: topology.stats.sample.rate
        """
        return float(self.data['topology.stats.sample.rate'])

    def get_nimbus_seeds(self):
        """
        Get value for the key: nimbus.seeds
        """
        return [OneOfNimbusSeeds(x) for x in self.data['nimbus.seeds']]

    def get_topology_receiver_buffer_size(self):
        """
        Get value for the key: topology.receiver.buffer.size
        """
        return float(self.data['topology.receiver.buffer.size'])

    def get_topology_tuple_serializer(self):
        """
        Get value for the key: topology.tuple.serializer
        """
        return str(self.data['topology.tuple.serializer'])

    def get_topology_worker_shared_thread_pool_size(self):
        """
        Get value for the key: topology.worker.shared.thread.pool.size
        """
        return float(self.data['topology.worker.shared.thread.pool.size'])

    def get_storm_messaging_netty_max_retries(self):
        """
        Get value for the key: storm.messaging.netty.max_retries
        """
        return float(self.data['storm.messaging.netty.max_retries'])

    def get_drpc_https_port(self):
        """
        Get value for the key: drpc.https.port
        """
        return float(self.data['drpc.https.port'])

    def get_ui_filter(self):
        """
        Get value for the key: ui.filter
        """
        return str(self.data['ui.filter'])

    def get_topology_optimize(self):
        """
        Get value for the key: topology.optimize
        """
        return bool(self.data['topology.optimize'])


class OneOfNimbusSeeds(object):
    """OneOfNimbusSeeds"""
    data = None
    expected_data_keys = ['String']

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
        return "OneOfNimbusSeeds(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_string(self):
        """
        Get value for the key: String
        """
        return str(self.data['String'])


class OneOfSupervisorSupervisorsCommands(object):
    """OneOfSupervisorSupervisorsCommands"""
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
        return "OneOfSupervisorSupervisorsCommands(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfStormZookeeperServers(object):
    """OneOfStormZookeeperServers"""
    data = None
    expected_data_keys = ['String']

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
        return "OneOfStormZookeeperServers(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_string(self):
        """
        Get value for the key: String
        """
        return str(self.data['String'])


class OneOfTopologyUsers(object):
    """OneOfTopologyUsers"""
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
        return "OneOfTopologyUsers(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfStormAuthSimpleWhiteListUsers(object):
    """OneOfStormAuthSimpleWhiteListUsers"""
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
        return "OneOfStormAuthSimpleWhiteListUsers(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfSupervisorSlotsPorts(object):
    """OneOfSupervisorSlotsPorts"""
    data = None
    expected_data_keys = ['Double']

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
        return "OneOfSupervisorSlotsPorts(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_double(self):
        """
        Get value for the key: Double
        """
        return float(self.data['Double'])


class OneOfTopologyMetricsConsumerRegister(object):
    """OneOfTopologyMetricsConsumerRegister"""
    data = None
    expected_data_keys = ['parallelism.hint', 'class']

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
        return "OneOfTopologyMetricsConsumerRegister(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_parallelism_hint(self):
        """
        Get value for the key: parallelism.hint
        """
        return float(self.data['parallelism.hint'])

    def get_class(self):
        """
        Get value for the key: class
        """
        return str(self.data['class'])


class OneOfTopologyKryoDecorators(object):
    """OneOfTopologyKryoDecorators"""
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
        return "OneOfTopologyKryoDecorators(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfStormAuthSimpleAclAdmins(object):
    """OneOfStormAuthSimpleAclAdmins"""
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
        return "OneOfStormAuthSimpleAclAdmins(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfSupervisorSupervisors(object):
    """OneOfSupervisorSupervisors"""
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
        return "OneOfSupervisorSupervisors(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfStormAuthSimpleAclUsersCommands(object):
    """OneOfStormAuthSimpleAclUsersCommands"""
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
        return "OneOfStormAuthSimpleAclUsersCommands(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfStormAuthSimpleAclUsers(object):
    """OneOfStormAuthSimpleAclUsers"""
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
        return "OneOfStormAuthSimpleAclUsers(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()


class OneOfVisualizationTable(object):
    """OneOfVisualizationTable"""
    data = None
    expected_data_keys = [':row']

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
        return "OneOfVisualizationTable(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get__row(self):
        """
        Get value for the key: :row
        """
        return [OneOfRow(x) for x in self.data[':row']]


class OneOfRow(object):
    """OneOfRow"""
    data = None
    expected_data_keys = [':stream', ':sani-stream', ':checked']

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
        return "OneOfRow(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get__stream(self):
        """
        Get value for the key: :stream
        """
        return str(self.data[':stream'])

    def get__sani_stream(self):
        """
        Get value for the key: :sani-stream
        """
        return str(self.data[':sani-stream'])

    def get__checked(self):
        """
        Get value for the key: :checked
        """
        return bool(self.data[':checked'])


class OneOfSpouts(object):
    """OneOfSpouts"""
    data = None
    expected_data_keys = [
        'encodedSpoutId', 'spoutId', 'executors', 'failed', 'completeLatency', 'errorPort', 'emitted', 'lastError',
        'tasks', 'transferred', 'errorHost', 'errorLapsedSecs', 'errorWorkerLogLink', 'acked'
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
        return "OneOfSpouts(%s)" % str(self.data)

    def __repr__(self):
        return self.__str__()

    def get_encoded_spout_id(self):
        """
        Get value for the key: encodedSpoutId
        """
        return str(self.data['encodedSpoutId'])

    def get_spout_id(self):
        """
        Get value for the key: spoutId
        """
        return str(self.data['spoutId'])

    def get_executors(self):
        """
        Get value for the key: executors
        """
        return float(self.data['executors'])

    def get_failed(self):
        """
        Get value for the key: failed
        """
        return float(self.data['failed'])

    def get_complete_latency(self):
        """
        Get value for the key: completeLatency
        """
        return str(self.data['completeLatency'])

    def get_error_port(self):
        """
        Get value for the key: errorPort
        """
        return str(self.data['errorPort'])

    def get_emitted(self):
        """
        Get value for the key: emitted
        """
        return float(self.data['emitted'])

    def get_last_error(self):
        """
        Get value for the key: lastError
        """
        return str(self.data['lastError'])

    def get_tasks(self):
        """
        Get value for the key: tasks
        """
        return float(self.data['tasks'])

    def get_transferred(self):
        """
        Get value for the key: transferred
        """
        return float(self.data['transferred'])

    def get_error_host(self):
        """
        Get value for the key: errorHost
        """
        return str(self.data['errorHost'])

    def get_error_lapsed_secs(self):
        """
        Get value for the key: errorLapsedSecs
        """
        return str(self.data['errorLapsedSecs'])

    def get_error_worker_log_link(self):
        """
        Get value for the key: errorWorkerLogLink
        """
        return str(self.data['errorWorkerLogLink'])

    def get_acked(self):
        """
        Get value for the key: acked
        """
        return float(self.data['acked'])
