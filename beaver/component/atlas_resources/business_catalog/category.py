#
# Copyright  (c) 2011-2018, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#


class Category(object):
    def __init__(
            self,
            display_name,
            glossary_guid,
            glossary_display_name,
            qualified_name=None,
            parent_category_guid=None,
            short_description=None,
            long_description=None
    ):
        self.category = dict()
        self.category["name"] = display_name
        if qualified_name:
            self.category["qualifiedName"] = qualified_name
        self.category["anchor"] = dict()
        self.category["anchor"]["glossaryGuid"] = glossary_guid
        self.category["anchor"]["displayText"] = glossary_display_name
        self.category["longDescription"] = long_description
        self.category["shortDescription"] = short_description
        if parent_category_guid:
            self.category["parentCategory"] = dict()
            self.category["parentCategory"]["categoryGuid"] = parent_category_guid

    def get_category(self):
        return self.category

    def set_display_name(self, display_name):
        self.category["name"] = display_name

    def set_anchor_glossary_guid(self, glossary_guid):
        self.category["anchor"]["glossaryGuid"] = glossary_guid

    def set_parent_category_guid(self, parent_category_guid):
        if 'parentCategory' not in self.category:
            self.category["parentCategory"] = dict()
        self.category["parentCategory"]["categoryGuid"] = parent_category_guid

    def set_long_description(self, long_description):
        self.category["longDescription"] = long_description

    def set_short_description(self, short_description):
        self.category["shortDescription"] = short_description
