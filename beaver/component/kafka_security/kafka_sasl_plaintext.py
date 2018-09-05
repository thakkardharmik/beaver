class kafka_sasl_plaintext:
    def __init__(self):
        pass

    def get_client_properties(self):
        return {}

    def get_protocol_type(self):
        return "SASL_PLAINTEXT"