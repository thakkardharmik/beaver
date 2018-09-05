#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import logging
import os
from datetime import datetime
import uuid
import threading
import time
from beaver.component.kafka import Kafka
from beaver.component.rollingupgrade.RuAssert import ruAssert

logger = logging.getLogger(__name__)


class ruKafka:
    TOPIC_NAME = "kafka-smoke-test-%s" % os.getpid()
    RU_TOPIC_NAME = "kafka-ru-topic-%s" % os.getpid()
    msg = "This is Kafka RU TEST."
    _background_thread = None
    _ru_stop_thread_event = threading.Event()
    _ru_messages = []
    broker_id_mapping = {}

    @classmethod
    def background_job_setup(cls, runSmokeTestSetup=True, config=None):
        '''
        Setup for background long running job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        '''
        cls._data_loss_test_pre_ru()
        cls._broker_id_test_pre_ru()

    @classmethod
    def _data_loss_test_pre_ru(cls):
        cls.create_topic_successfully(topic_name=cls.TOPIC_NAME)
        cls.publish_message_successfully(topic_name=cls.TOPIC_NAME, message=cls.msg)

        cls.create_topic_successfully(topic_name=cls.RU_TOPIC_NAME, replication=3)
        cls.publish_one_message_to_ru_topic()

    @classmethod
    def publish_one_message_to_ru_topic(cls):
        ru_message = datetime.now().isoformat() + " message for " + cls.RU_TOPIC_NAME + " uuid " + str(uuid.uuid4())
        exit_code = cls.publish_message_successfully(topic_name=cls.RU_TOPIC_NAME, message=ru_message)
        if exit_code == 0:
            cls._ru_messages.append(ru_message)

    @classmethod
    def publish_message_successfully(cls, topic_name, message):
        exit_code, stdout = Kafka.runConsoleProducer(topic_name, brokerlist=Kafka.get_broker_list(), message=message)
        cls.kru_assert(exit_code == 0, "Kafka Producer for topic: %s message: %s" % (topic_name, message))
        return exit_code

    @classmethod
    def create_topic_successfully(cls, topic_name, replication=1):
        exit_code, stdout = Kafka.createTopic(
            topic_name, replication=replication, runallowuser=False, user=Kafka.getkafkaAdminUser()
        )
        cls.kru_assert(exit_code == 0, "Kafka topic - %s failed at creation" % topic_name)
        Kafka.grantAllPermissionOnTopic(topic_name)

    @classmethod
    def _broker_id_test_pre_ru(cls):
        _broker_id_mapping = Kafka.get_broker_id_from_all_brokers()
        cls.broker_id_mapping.update(_broker_id_mapping)

    @classmethod
    def run_background_job(cls, runSmokeTestSetup=True, config=None):
        '''
        Runs background long running Job
        :param runSmokeTestSetup: Runs smoke test setup if set to true
        :param config: expected configuration location
        :return: Total number of long running jobs started
        '''

        def publish_every_min():
            wait_min = 30 * 24 * 60
            for i in range(wait_min):
                if cls._ru_stop_thread_event.isSet():  # thread termination condition
                    logger.info("Will not publish any more messages to topic: %s" % cls.RU_TOPIC_NAME)
                    return
                cls.publish_one_message_to_ru_topic()
                time.sleep(60)
            if cls._background_thread:
                logger.warning("Test did not complete even after " + str(wait_min) + " mins.")

        cls._background_thread = threading.Thread(target=publish_every_min)
        cls._background_thread.setDaemon(True)
        cls._background_thread.start()
        logger.info("No run_background_job needed for kafka smoke test.")
        return 1

    @classmethod
    def run_smoke_test(cls, config=None):
        '''
        Run smoke test for kafka
        '''
        from beaver.component.kafka import Kafka
        TOPIC_NAME = "kafka_ru_smoke-%s" % os.getpid()
        msg = "This is Kafka Smoke TEST for RU."
        exit_code, stdout = Kafka.createTopic(TOPIC_NAME, runallowuser=False, user=Kafka.getkafkaAdminUser())
        cls.kru_assert(exit_code == 0, "Kafka topic - %s failed at creation" % TOPIC_NAME)
        Kafka.grantAllPermissionOnTopic(TOPIC_NAME)
        exit_code, stdout = Kafka.runConsoleProducer(TOPIC_NAME, brokerlist=Kafka.get_broker_list(), message=msg)
        cls.kru_assert(exit_code == 0, "Kafka Producer for topic: %s message: %s" % (TOPIC_NAME, msg))
        exit_code, stdout = Kafka.runConsoleConsumer(TOPIC_NAME, "--from-beginning --max-messages 1")
        cls.kru_assert(msg in stdout, "The messages produced: %s was not found in stdout: %s" % (msg, stdout))
        logger.info("kafka brokers have come up properly after upgrade.")

    @classmethod
    def background_job_teardown(cls):
        '''
        Cleanup for long running background job
        '''
        logger.info("kafka background_job_teardown completed.")

    @classmethod
    def verifyLongRunningJob(cls):
        '''
        Validate long running background job after end of all component upgrade
        '''
        cls._data_loss_test_post_ru()
        cls._broker_id_test_post_ru()
        cls._ru_stop_thread_event.set()
        cls._background_thread.join()
        cls.kru_assert(
            len(cls._ru_messages) > 5,
            "Expected at least 5 messages publish to topic %s found %s" % (cls.RU_TOPIC_NAME, str(cls._ru_messages))
        )
        exit_code, stdout = Kafka.runConsoleConsumer(
            cls.RU_TOPIC_NAME, "--from-beginning --max-messages " + str(len(cls._ru_messages))
        )
        print_stdout_flag = False  # the stdout can be huge - we cant't print it in a loop or make it part of message
        for msg in cls._ru_messages:
            cls.kru_assert(msg in stdout, "The messages produced: %s was not found in stdout." % msg)
            if msg not in stdout:
                print_stdout_flag = True
        if print_stdout_flag:
            logger.info("stdout is: " + stdout)
            logger.info("cls._ru_messages is: " + str(cls._ru_messages))
        logger.info("kafka brokers have come up properly after upgrade.")

    @classmethod
    def _data_loss_test_post_ru(cls):
        exit_code, stdout = Kafka.runConsoleConsumer(cls.TOPIC_NAME, "--from-beginning --max-messages 1")
        cls.kru_assert(cls.msg in stdout, "The messages produced: %s was not found in stdout: %s" % (cls.msg, stdout))

    @classmethod
    def _broker_id_test_post_ru(cls):
        _broker_id_mapping = Kafka.get_broker_id_from_all_brokers()
        cls.kru_assert(
            cls.broker_id_mapping == _broker_id_mapping,
            "broker.id mapping has changed expected %s found %s" % (cls.broker_id_mapping, _broker_id_mapping)
        )

    @classmethod
    def kru_assert(cls, cond, error_message):
        ruAssert("Kafka", cond, error_message)
