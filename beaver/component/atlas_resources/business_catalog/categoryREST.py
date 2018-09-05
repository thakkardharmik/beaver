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
import json
from taskreporter.taskreporter import TaskReporter
from beaver.component.atlas_resources.business_catalog.baseREST import BaseAPI
from beaver.component.atlas_resources.atlas import Atlas


class CategoryAPI(BaseAPI):
    def __init__(self):
        BaseAPI.__init__(self, 'api/atlas/v2/glossary/category')

    @TaskReporter.report_test()
    def create_categories(self, body, code=200):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/categories')
        response, status = Atlas.http_put_post_request(url=url, data=json.dumps(body))
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_related_categories(self, guid, code=200):
        url = os.path.join(self.url, guid, "related")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_terms_associated(self, guid, code=200):
        url = os.path.join(self.url, guid, "terms")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response
