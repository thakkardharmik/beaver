#
# Copyright  (c) 2011-2018, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#


class Glossary(object):
    def __init__(
            self,
            display_name,
            qualified_name=None,
            short_description=None,
            long_description=None,
            language=None,
            usage=None
    ):
        self.glossary = dict()
        self.glossary["name"] = display_name
        self.glossary["longDescription"] = long_description
        self.glossary["shortDescription"] = short_description
        self.glossary["language"] = language
        self.glossary["usage"] = usage
        if qualified_name:
            self.glossary["qualifiedName"] = qualified_name

    def get_glossary(self):
        return self.glossary

    def set_display_name(self, display_name):
        self.glossary["name"] = display_name

    def set_qualified_name(self, qualified_name):
        self.glossary["qualifedName"] = qualified_name

    def set_long_description(self, long_description):
        self.glossary["longDescription"] = long_description

    def set_short_description(self, short_description):
        self.glossary["shortDescription"] = short_description

    def set_language(self, language):
        self.glossary["language"] = language

    def set_usage(self, usage):
        self.glossary["usage"] = usage
