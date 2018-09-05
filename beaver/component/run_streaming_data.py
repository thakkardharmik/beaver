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
import threading
import time

from beaver.component.kafka import Kafka
from beaver.component.spark import Spark

logger = logging.getLogger(__name__)


class RunKafkaProducer(object):
    """ Run kafka producer in background
    The run() method will keep running kafka producer one by one
    until stop() is called
    """

    def __init__(self, messages=None, topic="defaultTopic"):
        if not messages:
            messages = ["hello world"]
        self.messages = messages
        self.topic = topic

        thread = threading.Thread(target=self.run, args=(self.messages, self.topic))

        thread.start()

    def run(self, messages=None, topic="defaultTopic"):
        if not messages:
            messages = ["hello world"]
        for message in self.messages:
            exit_code, _stdout = Kafka.runConsoleProducer(
                self.topic, brokerlist=Kafka.get_broker_list(), message=message
            )
            assert exit_code == 0, "Kafka producer for %s failed" % (topic)


class RunNetcatserver(object):
    """
    Run Netcat server in background
    The run() method will keep running netcat server one by one for each message
    """

    def __init__(self, messages=None, host="localhost", port="9999"):
        if not messages:
            messages = ["hello world"]
        self.messages = messages
        self.host = host
        self.port = port

        thread = threading.Thread(target=self.run, args=(self.messages, self.host, self.port))

        thread.start()

    def run(self, messages=None, host="localhost", port="9999"):  # pylint: disable=unused-argument
        if not messages:
            messages = ["hello world"]
        for message in self.messages:
            Spark.startNetcatServerinBackground(message=message)
            time.sleep(4)
            Spark.stopNetcatServer(message=message)
