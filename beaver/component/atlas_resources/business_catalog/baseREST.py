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
from beaver.component.atlas_resources.atlas import Atlas


class BaseAPI(object):
    def __init__(self, url):
        self.base_url = Atlas.get_base_url()
        self.url = os.path.join(self.base_url, url)

    @TaskReporter.report_test()
    def create(self, body, code=200):
        response, status = Atlas.http_put_post_request(url=self.url, data=json.dumps(body))
        assert status == code
        return response

    @TaskReporter.report_test()
    def update(self, guid, body, code=200):
        response, status = Atlas.http_put_post_request(
            url=os.path.join(self.url, guid), data=json.dumps(body), method='PUT'
        )
        assert status == code
        return response

    @TaskReporter.report_test()
    def partial_update(self, guid, body, code=200):
        url = os.path.join(self.url, guid, "partial")
        response, status = Atlas.http_put_post_request(url=url, data=json.dumps(body), method='PUT')
        assert status == code
        return response

    @TaskReporter.report_test()
    def delete(self, guid, code=204):
        status = Atlas.http_delete_request(url=os.path.join(self.url, guid))
        assert status == code

    @TaskReporter.report_test()
    def get(self, guid, code=200):
        response, status = Atlas.http_get_request(url=os.path.join(self.url, guid))
        assert status == code
        return response
