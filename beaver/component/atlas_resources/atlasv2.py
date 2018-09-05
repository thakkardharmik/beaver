#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import json
import logging
import time
import random
import datetime
from beaver.component.atlas_resources.atlas import Atlas
import string
import enum
from taskreporter.taskreporter import TaskReporter

logger = logging.getLogger(__name__)


class AtlasV2:

    ## TYPES REST
    # POST ,GET, PUT, DELETE
    TYPE_REQUEST = '/types/typedefs'
    GET_ALL_TYPES = '/types/typedefs/headers'

    ## ENTITY REST
    # POST
    ENTITY_REQUEST = '/entity'

    # GET, POST, DELETE
    BULK_ENTITY_REQUEST = '/entity/bulk'
    # DELETE , GET , PUT
    REQUEST_ENTITY_WITH_GUID = '/entity/guid/%s'
    GET_TRAITS_OF_ENTITY = "/entity/guid/%s/classifications"
    #POST
    BULK_TAG_ENTITIES = '/entity/bulk/classification'
    GET_TRAIT_OF_ENTITY = "/entity/guid/%s/classification/%s"
    GET_UNIQUE_ATTRIBUTE = '/entity/uniqueAttribute/type/%s?attr:%s=%s'

    # LINEAGE
    GET_ENTITY_LINEAGE = '/lineage/%s'

    #SEARCH
    DSL_SEARCH = '/search/dsl?%s'
    FULL_TEXT_SEARCH = '/search/fulltext?%s'
    BASIC_SEARCH = '/search/basic?%s'
    BASIC_SEARCH_POST = '/search/basic'

    #SAVED_SEARCH
    SAVED_SEARCH = '/search/saved'

    # RELATIONSHIP
    GET_RELATIONSHIP = '/relationship/guid/%s'
    RELATIONSHIP = '/relationship'
    SEARCH_RELATIONSHIP = '/search/relationship'

    @classmethod
    def set_base_url(cls, reset=False, with_host=None):
        return Atlas.set_base_url(with_host=with_host, reset=reset) + '/api/atlas/v2'

    @classmethod
    def get_base_url(cls, reset=False):
        return cls.set_base_url()

    ## TYPES
    @classmethod
    @TaskReporter.report_test()
    def get_type_def(cls, type=None, type_name=None, guid=None):
        '''
        Get types name as the argument Eg: tag name created
        :param types: string value of defined types
        :return: return json
        '''
        url = cls.get_base_url() + '/types/' + type + 'def'
        if type_name:
            url = url + '/name/' + type_name
        elif guid:
            url = url + '/guid/' + guid
        logger.info(url)
        response, response_status = Atlas.http_get_request(url)
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def create_type(
            cls,
            input_json_file=None,
            input_json_string=None,
            type=None,
            type_name=None,
            super_types_list=None,
            status_code=200
    ):
        '''
        Takes in input.json file to create type
        If json is not known , takes  type and type_name and constructs the json ex : type=entity type_name= myentity
        This method can be used to create all categories : entity , classification , enum , struct
        But a more sophisticated method "create_tag" for classification is written which can be used to create a tag (classification)
        "create_tag" method provides capabilites similar to UI.
        :param input_json_file: input.json
        :param input_json_string: json string
        :param : type : type to be created example : entity,classification
        :param : type_name : name of the type instance
        :return: json response , status code
        '''

        type_definition = Utils.construct_type_definition(
            input_json_file, input_json_string, type, type_name, super_types_list
        )
        url = cls.get_base_url() + cls.TYPE_REQUEST
        logger.info(url)
        response, response_status = Atlas.http_put_post_request(url, type_definition, 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def update_type(cls, input_json_file=None, input_json_string=None):
        type_definition = Utils.construct_type_definition(input_json_file, input_json_string)
        url = cls.get_base_url() + cls.TYPE_REQUEST
        response, response_status = Atlas.http_put_post_request(url, type_definition, 'PUT')
        logger.info(response)
        return response, response_status

    @classmethod
    def get_all_types(cls):
        url = cls.get_base_url() + cls.TYPE_REQUEST
        return Atlas.http_get_request(url)

    @classmethod
    def get_all_types_of_category(cls, category):
        return cls.get_all_types()[0][category + "Defs"]

    @classmethod
    def get_all_entity_types(cls):
        return cls.get_all_types()[0]["entityDefs"]

    @classmethod
    def get_all_classification_types(cls):
        return cls.get_all_types()[0]["classificationDefs"]

    @classmethod
    def get_all_enum_types(cls):
        return cls.get_all_types()[0]["enumDefs"]

    @classmethod
    def get_all_struct_types(cls):
        return cls.get_all_types()[0]["structDefs"]

    # ENTITY REST
    @classmethod
    @TaskReporter.report_test()
    def create_entity(cls, input_json_file=None, input_json_string=None, entity_details=None, is_bulk=False):
        '''
        Create atlas entities
        :param input_json_file: input.json
        :param input_json_string: json string
        :param entity_details : dict of entity name and type ex : {entity1:type1,entity2:type2}
        :param is_bulk : to create bulk entities or single entity
        :return: json response
        '''
        json_data = Utils.construct_entity_definition(
            input_json_file=input_json_file,
            input_json_string=input_json_string,
            entity_details=entity_details,
            is_bulk=is_bulk
        )
        entity_request = cls.get_base_url() + cls.ENTITY_REQUEST
        bulk_entity_request = cls.get_base_url() + cls.BULK_ENTITY_REQUEST
        response, response_status = Atlas.http_put_post_request(
            bulk_entity_request if is_bulk else entity_request, json_data, 'POST'
        )
        return response, response_status, json_data

    @classmethod
    @TaskReporter.report_test()
    def update_entity(cls, input_json_file=None, input_json_string=None, guid=None, attribute_name=None, value=None):
        '''
        update atlas entities
        :param input_json_file: input.json
        :param input_json_string: json string
        :param guid : guid of entity
        :param attribute_name : name of attribute to update
        :param value : new attribute value
        :param is_bulk : if bulk update
        :return: status code and json response
        '''
        if guid and attribute_name:

            url = (cls.get_base_url() + cls.REQUEST_ENTITY_WITH_GUID % guid) + '?name=' + attribute_name
            json_data = '\"' + str(value) + '\"'
            response, response_status = Atlas.http_put_post_request(url, json_data, 'PUT')
        else:
            json_data = Utils.construct_entity_definition(
                input_json_file=input_json_file, input_json_string=input_json_string
            )
            url = cls.get_base_url() + cls.ENTITY_REQUEST
            response, response_status = Atlas.http_put_post_request(url, json_data, 'POST')
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def delete_type(cls, input_json_file=None, input_json_string=None):
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        elif input_json_string:
            json_data = json.dumps(input_json_string)
        url = cls.get_base_url() + cls.TYPE_REQUEST
        return Atlas.http_delete_request(url=url, body=json_data)

    @classmethod
    def create_tag(cls, tag_name, attrib_map=None, description=None, parent_tag=None, entity_types=None):
        return Atlas.create_tag(
            tag_name=tag_name,
            attrib_map=attrib_map,
            description=description,
            parent_tag=parent_tag,
            entity_types=entity_types
        )

    @classmethod
    @TaskReporter.report_test()
    def delete_entity(cls, guid):
        '''
        :param guid: guid list or single guid ex : [guid1,guid2,guid3] or guid1 or [guid1]
        :return:
        '''

        if type(guid) is list:
            url = cls.get_base_url() + cls.BULK_ENTITY_REQUEST + '?'
            count = len(guid)
            for x in range(0, count):
                url = url + 'guid=' + guid[x]
                if x + 1 != count:
                    url = url + '&'
        else:
            url = cls.get_base_url() + cls.REQUEST_ENTITY_WITH_GUID % guid

        logger.info(url)
        return Atlas.http_delete_request(url)

    @classmethod
    @TaskReporter.report_test()
    def get_GUID_of_entity_with_state(cls, entityType, attribute="qualifiedName", value="", state="ACTIVE"):
        '''
        get GUID of the entity with given qualified name
        :param entityType : type of the entity
        :param qualifiedName : Qualified Name of the entity
        :param state : state of the entity ACTIVE or DELETED
        :return: GUID of the entity
        '''
        response, response_status = cls.poll_till_entity_is_ingested_by_Atlas(
            type_name=entityType, attribute=attribute, value=value
        )
        assert response_status == 200
        entityDef = response
        logger.info(response)
        guid = None
        if "entities" in entityDef:
            entity_defs = entityDef["entities"]
            for defn in entity_defs:
                entity_state = defn["status"]
                guid = defn["guid"]
                if entity_state == state:
                    break
        return guid

    @classmethod
    @TaskReporter.report_test()
    def get_GUID_of_entity(cls, entityType, attribute="qualifiedName", value=""):
        guid = cls.get_GUID_of_entity_with_state(entityType, attribute, value, state="ACTIVE")
        if not guid:
            logger.info("guid is none")
            guid = cls.get_GUID_of_entity_with_state(entityType, attribute, value, state="DELETED")
        return guid

    @classmethod
    @TaskReporter.report_test()
    def poll_till_entity_is_ingested_by_Atlas(cls, type_name, attribute, value, minutes=2):
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=minutes)
        response = None
        response_code = None
        while datetime.datetime.now() < endTime:
            response, response_code = cls.get_entity_with_attribute_and_value(
                type_name=type_name, attribute=attribute, value=value
            )
            assert response_code == 200
            logger.info(response)
            if "entities" in response:
                break
            else:
                logger.info("Entity is not yet found in Atlas . Sleeping for 5 secs (max %s minutes)" % (minutes))
                time.sleep(5)
                continue
        return response, response_code

    @classmethod
    @TaskReporter.report_test()
    def poll_for_entity_to_be_deleted(cls, type_name, attribute, value, minutes=2):
        cls.poll_till_entity_is_ingested_by_Atlas(type_name=type_name, attribute=attribute, value=value)
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=minutes)
        response = None
        response_code = None
        while datetime.datetime.now() < endTime:
            response, response_code = cls.get_entity_with_attribute_and_value(
                type_name=type_name, attribute=attribute, value=value
            )
            assert response_code == 200
            logger.info(response)
            if "entities" in response and response["entities"][0]["status"] == "DELETED":
                break
            else:
                logger.info("Entity is not yet found in Atlas . Sleeping for 5 secs (max %s minutes)" % (minutes))
                time.sleep(5)
                continue
        return response, response_code

    @classmethod
    @TaskReporter.report_test()
    def get_entity_def(cls, guid):
        '''

        :param guid: guid list or single guid ex : [guid1,guid2,guid3] or guid1 or [guid1]
        :return:
        '''
        if type(guid) is list:
            url = cls.get_base_url() + cls.BULK_ENTITY_REQUEST + '?'
            count = len(guid)
            for x in range(0, count):
                url = url + 'guid=' + guid[x]
                if x + 1 != count:
                    url = url + '&'
        else:
            url = cls.get_base_url() + cls.REQUEST_ENTITY_WITH_GUID % (guid)
        logger.info(url)
        return Atlas.http_get_request(url)

    @classmethod
    def check_status(cls, guid):
        '''
        check status of the entity with given GUID
        :param guid : GUID of the entity
        :return: status of the entity
        '''
        entityDef = cls.get_entity_def(guid)[0]
        return entityDef["entity"]["status"]

    @classmethod
    def get_traits_associated_to_entity(cls, guid):
        url = cls.get_base_url() + cls.GET_TRAITS_OF_ENTITY % (guid)
        return Atlas.http_get_request(url)

    @classmethod
    def get_trait_associated_to_entity(cls, guid, trait_name):
        url = cls.get_base_url() + cls.GET_TRAIT_OF_ENTITY % (guid, trait_name)
        return Atlas.http_get_request(url)

    @classmethod
    def associate_traits_to_entity(cls, trait_name, entity_guid, attrib_map=None, validity_periods=[], propagate=True):
        return Atlas.associate_traits_to_entity(
            trait_name=trait_name,
            attrib_map=attrib_map,
            validity_periods=validity_periods,
            entity_guid=entity_guid,
            propagate=propagate
        )

    # uses bulk API
    @classmethod
    @TaskReporter.report_test()
    def associate_trait_to_bulk_entity(
            cls, trait_name=None, attrib_map=None, guids=None, validity_periods=[], propagate=True
    ):
        '''
        :param trait_name: name of classification (required param)
        :param attrib_map: attribute map Ex : {"attrib1":"datatype1","attrib2":"datatype2"}
        :param guids: list of GUIDS to bulk associate (required param)
        :param validity_periods: lists of dictionary. each dictionary is of format : {startTime:"" , endTime:"" , timeZone:""}
        :param propagate: propagate flag to set propagation to True or False
        :return:
        '''
        file = "tests/resources/atlas_model_templates/trait/bulk_associate_tag_template.json"
        json_data = json.loads(open(file).read())
        json_data["classification"]["typeName"] = trait_name
        if attrib_map:
            json_data["classification"]["attributes"].update(attrib_map)
        for guid in guids:
            json_data["entityGuids"].append(guid)
        json_data["classification"]["propagate"] = propagate
        if len(validity_periods) != 0:
            json_data["classification"]["validityPeriods"] = validity_periods
        url = cls.get_base_url() + cls.BULK_TAG_ENTITIES
        return Atlas.http_put_post_request(url, json.dumps(json_data), 'POST')

    @classmethod
    @TaskReporter.report_test()
    def edit_trait_attribute_value_associated_to_entity(
            cls, guid, trait_name, new_attrib_value_map={}, validity_periods=None, propagate=None
    ):
        '''
        Edits the tag associated to an entity. attribute value map , validity period , propafate flag can be modified/updated.
        :param guid: GUID of entity (required param)
        :param trait_name: name of classification (required param)
        :param new_attrib_value_map: attribute and value map Ex : {"attrib1":"datatype1","attrib2":"datatype2"}
        :param validity_periods: lists of dictionary. each dictionary is of format : {startTime:"" , endTime:"" , timeZone:""} .
-        if empty list is given , validity period is updated to empty. Hence if validity period need not be modified , provide None or donot add the validity periods param at all which  at that case is defaulted to None.        :param propagate: propagate flag to set propagation to True or False
        :return:
        '''
        url = cls.get_base_url() + cls.GET_TRAITS_OF_ENTITY % (guid)
        tag_def, response_status = cls.get_trait_associated_to_entity(guid=guid, trait_name=trait_name)
        assert response_status == 200
        for attribute, value in new_attrib_value_map.iteritems():
            tag_def["attributes"][attribute] = value
        if propagate is not None:
            tag_def["propagate"] = propagate
        if validity_periods is not None:
            tag_def["validityPeriods"] = validity_periods
        return Atlas.http_put_post_request(url=url, data=json.dumps([tag_def]), method='PUT')

    @classmethod
    @TaskReporter.report_test()
    def dis_associate_traits(cls, trait_name, entity_guid=None):
        '''
        remove associated trait
        :param trait_name : trait to disassociate
        :param entity_guid: entity guid to which trait is associated
        :return: status code
        '''
        url = cls.get_base_url() + AtlasV2.GET_TRAIT_OF_ENTITY % (entity_guid, trait_name)
        response = Atlas.http_delete_request(url)
        return response

    @classmethod
    def get_entity_with_attribute_and_value(cls, type_name, value, attribute="qualifiedName"):
        return AtlasV2.dsl_query(type_name=type_name, query="%s=\"%s\"" % (attribute, value))

    @classmethod
    @TaskReporter.report_test()
    def dsl_query(cls, type_name=None, query=None, classification=None, limit=None, offset=None):
        '''

        :param type_name: type name
        :param query: query
        :param classification: tags
        :param limit:
        :param offset:
        :return:
        '''
        param = ""
        # type name or query can alone be queried
        if type_name:
            param = "typeName=%s" % (type_name)
        if query:
            if type_name:
                param = param + "&"
            param = param + "query=%s" % (query)
        #classification , limit and offset can't be queried without query or typename
        if classification:
            param = param + "&classification=%s" % (classification)
        if limit:
            param = param + "&limit=%s" % (limit)
        if offset:
            param = param + "&offset=%s" % (offset)
        url = cls.get_base_url() + (cls.DSL_SEARCH % (param))
        logger.info(url)
        return Atlas.http_get_request(url)

    @classmethod
    def get_entity_using_unique_attribute(cls, type, unique_attribute, val):
        url = cls.get_base_url() + (cls.GET_UNIQUE_ATTRIBUTE % (type, unique_attribute, val))
        return Atlas.http_get_request(url)

    @classmethod
    def delete_entity_using_unique_attribute(cls, type, unique_attribute, val):
        url = cls.get_base_url() + (cls.GET_UNIQUE_ATTRIBUTE % (type, unique_attribute, val))
        return Atlas.http_delete_request(url)

    @classmethod
    @TaskReporter.report_test()
    def update_entity_using_unique_attribute(
            cls, type, unique_attribute, val, input_json_string=None, input_json_file=None
    ):
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        elif input_json_string:
            json_data = json.dumps(input_json_string)
        url = cls.get_base_url() + (cls.GET_UNIQUE_ATTRIBUTE % (type, unique_attribute, val))
        return Atlas.http_put_post_request(url=url, data=json_data, method='PUT')

    @classmethod
    @TaskReporter.report_test()
    def fulltext_query(cls, query, offset=None, limit=None, exclude_deleted_entities=True):
        '''

        :param query: complete query
        :param offset: offset from which to return results
        :param limit: count of results
        :return: response and response code
        '''
        full_query = "excludeDeletedEntities=%s" % (exclude_deleted_entities)
        if query:
            full_query = "%s&query=%s" % (full_query, query)
        if offset:
            full_query = "%s&offset=%s" % (full_query, offset)
        if limit:
            full_query = "%s&limit=%s" % (full_query, limit)
        url = cls.get_base_url() + (cls.FULL_TEXT_SEARCH % (full_query))
        return Atlas.http_get_request(url)

    @classmethod
    @TaskReporter.report_test()
    def basic_get_search(
            cls, type_name=None, query=None, classification=None, limit=None, offset=None,
            exclude_deleted_entities=True
    ):
        '''

        :param type_name: name of type
        :param query: query
        :param classification: trait to search
        :param limit: limit
        :param offset: offset from which to return results
        :return:
        '''
        param = "excludeDeletedEntities=%s" % (exclude_deleted_entities)
        if type_name:
            param = param + "&typeName=%s" % (type_name)
        if query:
            param = param + "&query=%s" % (query)
        if classification:
            param = param + "&classification=%s" % (classification)
        if limit:
            param = param + "&limit=%s" % (limit)
        if offset:
            param = param + "&offset=%s" % (offset)
        url = cls.get_base_url() + (cls.BASIC_SEARCH % (param))
        response, response_status = Atlas.http_get_request(url)
        logger.info(response)
        return response, response_status

    @classmethod
    def id_generator(cls, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @classmethod
    @TaskReporter.report_test()
    def basic_post_search(
            cls,
            type_name=None,
            include_sub_types=True,
            entity_filters_list=None,
            query=None,
            classification=None,
            include_classification_subtypes=True,
            tag_filters_list=None,
            attributes=[],
            limit=100,
            offset=0,
            exclude_deleted_entities=True,
            term=None,
            params_json=None,
            save_search=False,
            saved_search_by_guid=False
    ):
        '''

        :param type_name           :   name of type
        :param entity_filters_list :   list of filters tuple. tuple should be in the form of (attributeName,operator,attributeValue)

          Ex : for type name = hive_table ,
               entity_filters_list can be [   ("name","contains","hive"),
                                              ("description","=","simple table"),
                                              ("retention","<","10")
                                          ]
        :param query               :  query
        :param classification      :  trait to search
        :param tag_filters_list    :  list of filters tuple. tuple should be in the form of (attributeName,operator,attributeValue)

          Ex : for type name = hive_table ,
               tag_filters_list can be [    ("attrib1","contains","value1"),
                                            ("attrib2","=","value2"),
                                            ("attrib3","<","value3")
                                          ]
        :param limit                : limit
        :param offset               : offset from which to return results
        :param params_json          : POST json for the search

        :return: response , response code
        '''
        if params_json:
            param = params_json
            body = json.dumps(params_json)
        else:
            param = {}
            param["excludeDeletedEntities"] = exclude_deleted_entities
            param["typeName"] = type_name
            param["query"] = query
            param["classification"] = classification
            param["limit"] = limit
            param["offset"] = offset
            param["entityFilters"] = Utils.construct_attribute_filter(entity_filters_list)
            param["tagFilters"] = Utils.construct_attribute_filter(tag_filters_list)
            param["attributes"] = attributes
            param["includeSubTypes"] = include_sub_types
            param["includeSubClassifications"] = include_classification_subtypes
            param["termName"] = term
            body = json.dumps(param)

        if save_search:
            name = "search_" + cls.id_generator()
            logger.info("search name is : " + name)
            saved_search = dict()
            saved_search["searchParameters"] = param
            saved_search["name"] = name
            saved_search["searchType"] = "BASIC"
            body = json.dumps(saved_search)
            logger.info("Posting body :")
            logger.info(body)
            ss_utils = SavedSearchUtils()
            #saving the search query
            save_response, save_status = ss_utils.save_search(json_data=body)
            #executing the save search query
            if save_status == 200:
                if saved_search_by_guid:
                    response, status = ss_utils.execute_saved_search(guid=save_response['guid'])
                else:
                    response, status = ss_utils.execute_saved_search(name=name)
                logger.info(response)
                return response, status
        else:
            logger.info("Posting body :")
            logger.info(body)
            url = cls.get_base_url() + cls.BASIC_SEARCH_POST
            logger.info(url)
            response, response_status = Atlas.http_put_post_request(url, method='POST', data=body)
            logger.info(response)
            return response, response_status

    @classmethod
    def get_lineage_of_entity(cls, guid):
        url = cls.get_base_url() + (cls.GET_ENTITY_LINEAGE % (guid))
        return Atlas.http_get_request(url)

    '''
       Fetches the columns of the given hive_table
    '''

    @classmethod
    @TaskReporter.report_test()
    def get_columns_of_hive_table(cls, hive_table_qn):
        '''
        :param hive_table_qn: Qualified Name of hive_table
        :return: Map of name and guid of columns of hive_table Ex : {"col1":"guid1","col2":"guid2"}
        '''
        response, response_status = cls.dsl_query(
            type_name="hive_table", query="qualifiedName =\"%s\" select columns" % (hive_table_qn)
        )
        columns = {}
        for entities in response["entities"]:
            columns[entities["attributes"]["qualifiedName"]] = entities["guid"]
        return columns

    @classmethod
    def get_relationship_def(cls, relationship_guid):
        url = cls.get_base_url() + (cls.GET_RELATIONSHIP % (relationship_guid))
        return Atlas.http_get_request(url)

    @classmethod
    @TaskReporter.report_test()
    def update_relationship_for_propagateTags(cls, guid1, guid2, propagateTags, guid1_to_guid2_relationship=None):
        '''

        :param guid1: GUID of entity1 (mandatory)
        :param guid2: GUID of entity2 (mandatory)
        :param propagateTags: NONE , ONE_TO_TWO , TWO_TO_ONE , BOTH (mandatory)
        :param guid1_to_guid2_relationship: if entity1 is hive_table , entity2 is hive_column ,
                guid1_to_guid2_relationship is "columns". If the value is not set , this method finds the relationship by itself.
        :return:
        '''
        relationship_guid, status = cls.get_relationship_guid(guid1, guid2, guid1_to_guid2_relationship)
        response, response_status = cls.get_relationship_def(relationship_guid=relationship_guid)
        if response_status == 200:
            relationship_def = response
            relationship_def["relationship"]["propagateTags"] = propagateTags
            url = cls.get_base_url() + cls.RELATIONSHIP
            response, response_status = Atlas.http_put_post_request(
                url, method='PUT', data=json.dumps(relationship_def["relationship"])
            )
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def update_edge_to_disable_propagation(cls, guid1, guid2, tag_name, entity_guid):
        '''

        :param guid1: guid1
        :param guid2: guid2
        :param tag_name: name of tag to disable
        :param entity_guid: source of tag
        :return:
        '''
        relationship_guid, status = cls.get_relationship_guid(guid1, guid2, guid1_to_guid2_relationship=None)
        logger.info(relationship_guid)
        response, response_status = cls.get_relationship_def(relationship_guid=relationship_guid)
        if response_status == 200:
            relationship_def = response
            logger.info(relationship_def)
            propagatedClassifications = relationship_def["relationship"]["propagatedClassifications"]
            for propagatedClassification in propagatedClassifications:
                if propagatedClassification["typeName"] == tag_name and propagatedClassification["entityGuid"
                                                                                                 ] == entity_guid:
                    relationship_def["relationship"]["blockedPropagatedClassifications"
                                                     ].append(propagatedClassification)
                    url = cls.get_base_url() + cls.RELATIONSHIP
                    response, response_status = Atlas.http_put_post_request(
                        url, method='PUT', data=json.dumps(relationship_def["relationship"])
                    )
                    logger.info(cls.get_relationship_def(relationship_guid=relationship_guid)[0])

                    break

        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def update_edge_to_enable_propagation(cls, guid1, guid2, tag_name, entity_guid):
        '''

        :param guid1: guid1
        :param guid2: guid2
        :param tag_name:  name of tag to enable
        :param entity_guid: source of tag
        :return:
        '''
        relationship_guid, status = cls.get_relationship_guid(guid1, guid2, guid1_to_guid2_relationship=None)
        logger.info(relationship_guid)
        response, response_status = cls.get_relationship_def(relationship_guid=relationship_guid)
        if response_status == 200:
            relationship_def = response
            logger.info(relationship_def)
            blockedPropagatedClassifications = relationship_def["relationship"]["blockedPropagatedClassifications"]
            for blockedPropagatedClassification in blockedPropagatedClassifications:
                if blockedPropagatedClassification["typeName"] == tag_name and blockedPropagatedClassification[
                        "entityGuid"
                ] == entity_guid:
                    relationship_def["relationship"]["blockedPropagatedClassifications"
                                                     ].remove(blockedPropagatedClassification)
                    url = cls.get_base_url() + cls.RELATIONSHIP
                    response, response_status = Atlas.http_put_post_request(
                        url, method='PUT', data=json.dumps(relationship_def["relationship"])
                    )
                    break
        return response, response_status

    @classmethod
    @TaskReporter.report_test()
    def get_relationship_guid(cls, guid1, guid2, guid1_to_guid2_relationship=None):
        '''

        :param guid1: GUID of entity1
        :param guid2: GUID of entity2
        :param guid1_to_guid2_relationship: relationship between the entities ex : table or coluns
        :return:
        '''
        relationship_guid = None
        response, status = cls.get_entity_def(guid=guid1)
        if status == 200:
            entity_def = response
            if guid1_to_guid2_relationship is not None:
                entities = entity_def["entity"]["relationshipAttributes"][guid1_to_guid2_relationship]
                relationship_guid = cls._fetch_relationship_guid(entities, guid2)
            else:
                for relationship, entities in entity_def["entity"]["relationshipAttributes"].iteritems():
                    relationship_guid = cls._fetch_relationship_guid(entities, guid2)
                    if relationship_guid:
                        break
        return relationship_guid, status

    @classmethod
    @TaskReporter.report_test()
    def _fetch_relationship_guid(cls, entities, guid2):
        relationship_guid = None
        if isinstance(entities, list):
            for entity in entities:
                if entity.get("guid") == guid2:
                    relationship_guid = entity.get("relationshipGuid")
                    break
        elif entities != None:
            if entities.get("guid") == guid2:
                relationship_guid = entities.get("relationshipGuid")

        return relationship_guid

    @classmethod
    @TaskReporter.report_test()
    def get_entities_in_relationship(
            cls,
            guid,
            relation,
            sort_by=None,
            sort_order=None,
            exclude_deleted_entities=True,
            include_sub_classifications=True,
            include_sub_types=True,
            limit=25,
            offset=0
    ):

        # param = "guid='%s" %(guid)
        # param = param + "guid='%s" %(guid)
        # param = "excludeDeletedEntities=%s" % (excludeDeletedEntities)
        # param = param + "&includeSubClassifications=%s" % (includeSubClassifications)
        # param = param + "&includeSubTypes=%s" % (includeSubTypes)

        param = "?guid=%s&relation=%s&excludeDeletedEntities=%s&includeSubClassifications=%s&includeSubTypes=%s&limit=%s&offset=%s" % (
            guid, relation, exclude_deleted_entities, include_sub_classifications, include_sub_types, limit, offset
        )

        if sort_by:
            param = param + "&sortBy=%s" % (sort_by)
        if sort_order:
            param = param + "&sortOrder=%s" % (sort_order)

        url = cls.get_base_url() + (cls.SEARCH_RELATIONSHIP + (param))
        response, response_status = Atlas.http_get_request(url)
        logger.info(response)
        return response, response_status


class Utils:
    @classmethod
    def generate_random_guid(cls):
        rand = ''.join(str(random.randint(0, 9)) for _ in xrange(12))
        return "-" + str(rand)

    @classmethod
    @TaskReporter.report_test()
    def construct_type_definition(
            cls,
            input_json_file=None,
            input_json_string=None,
            type=None,
            type_name=None,
            super_types_list=None,
            status_code=200
    ):
        json_data = ''
        if input_json_file:
            json_data = open(input_json_file).read()
        elif input_json_string:
            json_data = json.dumps(input_json_string)
        elif type and type_name:
            base_def = cls.construct_json_from_type_and_typename(type, type_name, super_types_list)
            json_data = json.dumps(base_def)
        return json_data

    @classmethod
    @TaskReporter.report_test()
    def construct_json_from_type_and_typename(cls, type=None, type_name=None, super_types_list=None):
        type_def = cls.create_type_def(type, type_name, super_types_list)
        base_def = cls.add_to_type_template(type, type_def)
        return base_def

    @classmethod
    @TaskReporter.report_test()
    def create_type_def(cls, type, type_name, super_types_list=None):
        '''

        :param type: category
        :param type_name:  name of type
        :param super_types_list: super types if exist
        :return: json ready to post with the category type .The returned JSON can be modified according to user's needs.
        '''

        if type == "enum":
            type_file = "tests/resources/atlas_model_templates/type/enum_type.json"
        else:
            type_file = "tests/resources/atlas_model_templates/type/entity_classification_def_template.json"
        type_def = json.loads(open(type_file).read())
        type_def["name"] = type_name
        type_def["guid"] = cls.generate_random_guid()
        type_def["category"] = type.upper()
        if type == "entity" or type == "classification":
            type_def["superTypes"] = []
            if super_types_list:
                for super_type in super_types_list:
                    type_def["superTypes"].append(super_type)
        logger.info("typee def")
        logger.info(type_def)
        return type_def

    @classmethod
    @TaskReporter.report_test()
    def add_to_type_template(cls, type, type_def, template=None):
        if template == None:
            base_file = "tests/resources/atlas_model_templates/type/base_template_v2.json"
            base_def = json.loads(open(base_file).read())
        else:
            base_def = template
        base_def[type + "Defs"].append(type_def)
        return base_def

    @classmethod
    @TaskReporter.report_test()
    def construct_entity_definition(
            cls, input_json_file=None, input_json_string=None, entity_details=None, is_bulk=False
    ):
        json_data = ''
        if input_json_file:
            json_data = json.loads(open(input_json_file).read())
        elif input_json_string:
            json_data = json.dumps(input_json_string)
        elif entity_details:
            entity_def = cls.create_entity_def(entity_details)
            json_data = json.dumps(entity_def)
        return json_data

    @classmethod
    @TaskReporter.report_test()
    def create_entity_def(cls, entity_details):
        instance_file = 'tests/resources/atlas_model_templates/type/entity_type.json'
        inst_def = json.loads(open(instance_file).read())
        if len(entity_details) > 1:
            del inst_def["entity"]
            inst_def["entities"] = []
            count = -1
            for entity_name, type_name in entity_details.iteritems():
                dup_inst_def = json.loads(open(instance_file).read())
                count = count + 1
                inst_def["entities"].append(dup_inst_def["entity"])
                logger.info(count)
                inst_def["entities"][count]["typeName"] = type_name
                inst_def["entities"][count]["attributes"]["name"] = entity_name
                inst_def["entities"][count]["guid"] = cls.generate_random_guid()
                logger.info("going back")
                logger.info(json.dumps(inst_def))
        if len(entity_details) == 1:
            for entity_name, type_name in entity_details.iteritems():
                inst_def["entity"]["typeName"] = type_name
                inst_def["entity"]["guid"] = cls.generate_random_guid()
                inst_def["entity"]["attributes"]["name"] = entity_name
        return inst_def

    @classmethod
    @TaskReporter.report_test()
    def construct_attribute_filter(cls, filters_list, nested_criteria=None, condition="AND"):
        attribute_filters = {}
        if filters_list:
            attribute_filters["condition"] = condition
            attribute_filters["criterion"] = []
            for attribute_filter in filters_list:
                filter = {}
                filter["attributeName"] = attribute_filter[0]
                filter["operator"] = attribute_filter[1]
                filter["attributeValue"] = attribute_filter[2]
                attribute_filters["criterion"].append(filter)
            if nested_criteria:
                attribute_filters["criterion"].extend(nested_criteria)

        return attribute_filters


class SavedSearchUtils(AtlasV2):
    def __init__(self):
        self.base_url = AtlasV2.get_base_url() + AtlasV2.SAVED_SEARCH

    def get_saved_search(self, name=None):
        url = self.base_url if not name else self.base_url + "/" + name.strip()
        return Atlas.http_request(url=url, body=None, method='GET')

    def save_search(self, json_data):
        return Atlas.http_put_post_request(url=self.base_url, data=json_data)

    def update_saved_search(self, json_data):
        return Atlas.http_put_post_request(url=self.base_url, data=json_data, method='PUT')

    @TaskReporter.report_test()
    def execute_saved_search(self, guid=None, name=None):
        if not (name or guid):
            logger.error(
                "EmptyArgumentException: For executing a saved search, atleast one of "
                "these(name or guid) arguments have to be passed"
            )
            logger.error("name and GUID are set to null")
            return
        if name and guid:
            logger.info("Both name and guid are passed: guid will take higher precedence")
        url = self.base_url + "/execute"
        if guid:
            url += "/guid/" + guid
        if name:
            url += "/" + name
        return Atlas.http_request(url=url, body=None, method='GET')

    def delete_saved_search(self, guid):
        url = self.base_url + "/" + guid
        return Atlas.http_delete_request(url=url)


class RelationshipCategory(enum.Enum):
    ASSOCIATION = 'ASSOCIATION'
    AGGREGATION = 'AGGREGATION'
    COMPOSITION = 'COMPOSITION'


class Cardinality(enum.Enum):
    SINGLE = 'SINGLE'
    LIST = 'LIST'
    SET = 'SET'


class PropagateTags(enum.Enum):
    NONE = 'NONE'
    ONE_TO_TWO = 'ONE_TO_TWO'
    TWO_TO_ONE = 'TWO_TO_ONE'
    BOTH = 'BOTH'


class RelationshipEndDef:
    def __init__(
            self,
            type_,
            name,
            is_container=False,
            cardinality=Cardinality.SINGLE,
            is_legacy_attribute=False,
            description=None
    ):
        self.relationship_end_def = dict()
        self.relationship_end_def['type'] = type_
        self.relationship_end_def['name'] = name
        self.relationship_end_def['isContainer'] = is_container
        self.relationship_end_def['cardinality'] = cardinality.value
        self.relationship_end_def['isLegacyAttribute'] = is_legacy_attribute
        self.relationship_end_def['description'] = description if description \
            else "default relationshipEndDef description with name: " + name

    def get_def(self):
        return self.relationship_end_def

    def set_type(self, type_):
        self.relationship_end_def['type'] = type_

    def set_name(self, name):
        self.relationship_end_def['name'] = name

    def set_container(self, is_container):
        self.relationship_end_def['isContainer'] = is_container

    def set_cardinality(self, cardinality):
        self.relationship_end_def['cardinality'] = cardinality

    def set_legacy_attribute(self, is_legacy_attribute):
        self.relationship_end_def['isLegacyAttribute'] = is_legacy_attribute

    def set_description(self, description):
        self.relationship_end_def['description'] = description


class RelationshipDef:
    def __init__(
            self,
            name,
            end_def1,
            end_def2,
            guid=None,
            type_version="1.0",
            category=RelationshipCategory.ASSOCIATION,
            description=None,
            propagate_tags=PropagateTags.NONE,
            attribute_defs=list()
    ):
        self.relationship_def = dict()
        self.relationship_def['name'] = name
        self.relationship_def['guid'] = guid
        self.relationship_def['typeVersion'] = type_version
        self.relationship_def['relationshipCategory'] = category.value
        self.relationship_def['description'] = description if description \
            else "default relationshipDef description with name: " + name
        self.relationship_def['endDef1'] = end_def1
        self.relationship_def['endDef2'] = end_def2
        self.relationship_def['propagateTags'] = propagate_tags.value
        self.relationship_def['attributeDefs'] = attribute_defs

    def get_def(self):
        return self.relationship_def

    def set_name(self, name):
        self.relationship_def['name'] = name

    def set_guid(self, guid):
        self.relationship_def['guid'] = guid

    def set_category(self, category):
        if isinstance(category, RelationshipCategory):
            self.relationship_def['relationshipCategory'] = category.value
        else:
            raise ValueError("set_category: Input category is not an instance of RelationshipCategory.")

    def set_description(self, desc):
        self.relationship_def['description'] = desc

    def set_end_def1(self, end_def1):
        self.relationship_def['endDef1'] = end_def1

    def set_end_def2(self, end_def2):
        self.relationship_def['endDef2'] = end_def2

    def set_propagate_tags(self, propagate_tags):
        self.relationship_def['propagateTags'] = propagate_tags.value

    def set_attribute_def(self, attribute_defs):
        self.relationship_def['attributeDefs'] = attribute_defs
