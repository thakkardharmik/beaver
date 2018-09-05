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


class TermAPI(BaseAPI):
    def __init__(self):
        BaseAPI.__init__(self, 'api/atlas/v2/glossary/term')

    @TaskReporter.report_test()
    def create_terms(self, body, code=200):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/terms')
        response, status = Atlas.http_put_post_request(url=url, data=json.dumps(body))
        assert status == code
        return response

    @TaskReporter.report_test()
    def associate_term_to_entities(self, guid, entity_guids_list, code=204):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/terms', guid, "assignedEntities")
        body = list()
        for each in entity_guids_list:
            body.append({"guid": each})
        _response, status = Atlas.http_put_post_request(url=url, data=json.dumps(body))
        assert status == code

    @TaskReporter.report_test()
    def disassociate_term_to_entities(self, guid, entity_guids_dict, code=204):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/terms', guid, "assignedEntities")
        body = list()
        for entity_guid, relationship_guid in entity_guids_dict.iteritems():
            body.append({"guid": entity_guid, "relationshipGuid": relationship_guid})
        status = Atlas.http_delete_request(url=url, body=json.dumps(body))
        assert status == code

    @TaskReporter.report_test()
    def get_assigned_entities(self, guid, code=200):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/terms', guid, "assignedEntities")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response

    @TaskReporter.report_test()
    def get_related_terms(self, guid, code=200):
        url = os.path.join(self.base_url, 'api/atlas/v2/glossary/terms', guid, "related")
        response, status = Atlas.http_get_request(url=url)
        assert status == code
        return response
