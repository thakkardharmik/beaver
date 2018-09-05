#
# Copyright  (c) 2011-2018, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import os
from taskreporter.taskreporter import TaskReporter
from beaver.component.atlas_resources.atlas import Atlas
from beaver.component.atlas_resources.business_catalog.baseREST import BaseAPI


class GlossaryAPI(BaseAPI):
    def __init__(self):
        BaseAPI.__init__(self, 'api/atlas/v2/glossary')

    @TaskReporter.report_test()
    def get_all(self, code=200):
        response, status = Atlas.http_get_request(url=self.url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_detailed(self, guid, code=200):
        url = os.path.join(self.url, guid, "detailed")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_associated_categories(self, guid, code=200):
        url = os.path.join(self.url, guid, "categories")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_associated_terms(self, guid, code=200):
        url = os.path.join(self.url, guid, "terms")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_associated_terms_headers(self, guid, code=200):
        url = os.path.join(self.url, guid, "terms", "headers")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response
