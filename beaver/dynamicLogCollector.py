import json


class DynamicLogCollector(object):
    element_list = []

    def __init__(self):
        pass

    @classmethod
    def add_log_entry(cls, source, destination, ipaddress=None):
        element = {}
        element['source'] = source
        element['destination'] = destination
        if ipaddress is not None:
            element['ipaddress'] = ipaddress
        cls.element_list.append(element)

    @classmethod
    def create_log_collector_output(cls, output_file):
        log_element = {'log-elements': cls.element_list}
        with open(output_file, 'wb') as outfile:
            json.dump(log_element, outfile)
