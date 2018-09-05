import os
from beaver.config import Config
from beaver import util


class kafka_ssl:
    ssl_trustore_password = None
    ssl_trustore_location = None

    def __init__(self, truststoreLocation=None, truststorePassword=None):
        if truststoreLocation == None and truststorePassword == None:
            from beaver import beaverConfigs
            beaverConfigs.setConfigs()
            if not beaverConfigs.HDP_STACK_INSTALLED:
                self.ssl_trustore_password = "clientTrustStorePassword"
                self.ssl_trustore_location = "/etc/security/clientKeys/all.jks"
            else:
                xml_location = os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "ssl-client.xml")
                self.ssl_trustore_location = util.getPropertyValueFromConfigXMLFile(
                    xml_location, 'ssl.client.truststore.location'
                )
                self.ssl_trustore_password = util.getPropertyValueFromConfigXMLFile(
                    xml_location, 'ssl.client.truststore.password'
                )
        else:
            self.ssl_trustore_location = truststoreLocation
            self.ssl_trustore_password = truststorePassword

    def get_client_properties(self):
        return {
            'ssl.truststore.location': self.ssl_trustore_location,
            'ssl.truststore.password': self.ssl_trustore_password
        }

    def get_protocol_type(self):
        return "SSL"
