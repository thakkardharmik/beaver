#
# Copyright  (c) 2011-2018, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#


class Term(object):
    def __init__(
            self,
            display_name,
            glossary_guid,
            qualified_name=None,
            short_description=None,
            long_description=None,
            examples=None,
            abbreviation=None,
            usage=None
    ):
        self.term = dict()
        self.term["name"] = display_name
        self.term["qualifiedName"] = qualified_name
        self.term["anchor"] = dict()
        self.term["anchor"]["glossaryGuid"] = glossary_guid
        self.term["longDescription"] = long_description
        self.term["shortDescription"] = short_description
        self.term["examples"] = examples if not None else list()
        self.term["abbreviation"] = abbreviation
        self.term["usage"] = usage

    def get_term(self):
        return self.term

    def set_display_name(self, display_name):
        self.term["name"] = display_name

    def set_anchor_glossary_guid(self, glossary_guid):
        self.term["anchor"]["glossaryGuid"] = glossary_guid

    def set_long_description(self, long_description):
        self.term["longDescription"] = long_description

    def set_short_description(self, short_description):
        self.term["shortDescription"] = short_description

    def set_examples(self, examples):
        self.term["examples"] = examples

    def set_abbreviation(self, abbreviation):
        self.term["abbreviation"] = abbreviation

    def set_usage(self, usage):
        self.term["usage"] = usage
