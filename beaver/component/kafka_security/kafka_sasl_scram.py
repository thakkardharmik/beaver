import time
from beaver.component.kafka import Kafka


class kafka_sasl_scram:
    client_username = None
    client_password = None
    consumer_group = None
    jaas_entry = None
    topic_name = None
    sasl_mechanism = None
    to_be_replaced = "        KafkaServer {"
    if not Kafka._isSecure:
        to_be_replaced = "{% endif %}"

    def __init__(self, username, password, topic_name, consumer_group=None, sasl_mechanism="SCRAM-SHA-256"):
        if not consumer_group:
            consumer_group = "%s-group" % topic_name
        self.sasl_mechanism = sasl_mechanism
        self.topic_name = topic_name
        self.client_username = username
        self.client_password = password
        self.consumer_group = consumer_group
        self.jaas_entry = "\norg.apache.kafka.common.security.scram.ScramLoginModule required\n" \
                          "username=\"%s\"\n" \
                          "password=\"%s\";" % (username, password)

    def get_client_properties(self):
        from beaver.component.kafka import Kafka
        from beaver.component.ambari import Ambari

        Kafka.alterUser(
            userName=self.client_username,
            config="'SCRAM-SHA-256=[iterations=8192,password=%s],"
            "SCRAM-SHA-512=[password=%s]'" % (self.client_password, self.client_password)
        )

        is_ambari_enc = Ambari.is_ambari_encrypted()
        kafka_jaas_config = Ambari.getConfig(
            "kafka_jaas_conf", webURL=Ambari.getWebUrl(is_hdp=False, is_enc=is_ambari_enc)
        )

        replacement_jaas_entry = self.jaas_entry
        if not Kafka._isSecure:
            replacement_jaas_entry = "\nKafkaServer {%s\n};" % self.jaas_entry

        if self.to_be_replaced + replacement_jaas_entry not in kafka_jaas_config['content']:
            print "old : %s" % kafka_jaas_config['content']
            kafka_jaas_config['content'] = kafka_jaas_config['content'].replace(
                self.to_be_replaced, self.to_be_replaced + replacement_jaas_entry
            )
            print "new : %s" % kafka_jaas_config['content']
        Ambari.setConfig(
            "kafka_jaas_conf", kafka_jaas_config, webURL=Ambari.getWebUrl(is_hdp=False, is_enc=is_ambari_enc)
        )
        Ambari.restart_services_with_stale_configs()
        time.sleep(20)
        return {'sasl.jaas.config': self.jaas_entry.replace("\n", " "), 'sasl.mechanism': 'SCRAM-SHA-256'}

    def get_protocol_type(self):
        return "PLAINTEXTSASL"
