import json
import logging
import os
import traceback

from beaver.config import Config

logger = logging.getLogger(__name__)

#global non pytest annotation list
nonPyTestAnnotationsList = []


def getMarkerCondition():
    try:
        markerCondition = Config.get('marker', 'RUN_MARKER_CONDITION', default="")
        return markerCondition
    except Exception:
        logger.error("Exception occured during getMarkerCondition")
        logger.error(traceback.format_exc())
        return ""


def get_annotation_details():
    try:
        #read the properties from marker section in the suite.conf (RUN_MARKER_VERSION & RUN_MARKER_LIST )
        run_marker_version = str(Config.get('marker', 'RUN_MARKER_VERSION', default=''))
        run_marker_list = str(Config.get('marker', 'RUN_MARKER_LIST', default='')).split(",")
        logger.info("run_marker_version = %s", str(run_marker_version))
        logger.info("run_marker_list = %s", str(run_marker_list))

        if not run_marker_version or (len(run_marker_list) == 1 and run_marker_list[0] == ''):
            return ([], {})

        #generate the annotation_list & marker_dict
        WORKSPACE = os.path.sep.join(os.path.dirname(os.path.realpath(__file__)).split(os.path.sep)[:-1])
        version_marker_json_file = os.path.sep.join([WORKSPACE, "conf", "version_marker.json"])
        jsonData = json.load(open(version_marker_json_file))

        marker_cond_dict = {}
        for marker in run_marker_list:
            marker = marker.strip()
            annotationCondition = jsonData[run_marker_version][marker]
            annotationCondition = annotationCondition.replace("(", " ( ")
            annotationCondition = annotationCondition.replace(")", " ) ")
            marker_cond_dict[marker] = annotationCondition

        tempData = " ".join(marker_cond_dict.values())
        annotation_list = set(tempData.split(" "))
        try:
            annotation_list.remove('')
        except Exception:
            pass
        try:
            annotation_list.remove('(')
        except Exception:
            pass
        try:
            annotation_list.remove(')')
        except Exception:
            pass
        try:
            annotation_list.remove('or')
        except Exception:
            pass
        try:
            annotation_list.remove('and')
        except Exception:
            pass
        try:
            annotation_list.remove('not')
        except Exception:
            pass

        logger.info("annotation_list = %s", str(annotation_list))
        logger.info("marker_cond_dict = %s", str(marker_cond_dict))
        return (annotation_list, marker_cond_dict)
    except Exception:
        logger.error("exception occured during get_annotation_details")
        logger.error(traceback.format_exc())
        return ([], {})


def get_testcase_markers(testCaseAnnotationList, marker_cond_dict):
    try:
        testCaseMarkerList = []
        for marker, annotationCondition in marker_cond_dict.iteritems():
            for annotation in testCaseAnnotationList:
                if annotationCondition.find(annotation) != -1:
                    annotationCondition = annotationCondition.replace(annotation, "True")

            annotationConditionParts = annotationCondition.split(" ")
            for part in annotationConditionParts:
                if part not in ["", "(", ")", "and", "or", "not", "True"]:
                    annotationCondition = annotationCondition.replace(part, "False")

            if eval(annotationCondition):  # pylint: disable=eval-used
                testCaseMarkerList.append(marker)
        return testCaseMarkerList
    except Exception:
        logger.error("exception occured during get_testcase_markers")
        logger.error(
            "InputData: testCaseAnnotationList=%s marker_cond_dict=%s", str(testCaseAnnotationList),
            str(marker_cond_dict)
        )
        logger.error(traceback.format_exc())
        return []


def get_annotation_dict():
    try:
        #read the properties from marker section in the suite.conf (RUN_MARKER_VERSION & RUN_MARKER_LIST )
        run_marker_version = str(Config.get('marker', 'RUN_MARKER_VERSION', default=''))
        run_marker_list = str(Config.get('marker', 'RUN_MARKER_LIST', default='')).split(",")
        logger.info("run_marker_version = %s", str(run_marker_version))
        logger.info("run_marker_list = %s", str(run_marker_list))

        if not run_marker_version or (len(run_marker_list) == 1 and run_marker_list[0] == ''):
            return {True: [], False: []}

        #generate the annotation_list & marker_dict
        WORKSPACE = os.path.sep.join(os.path.dirname(os.path.realpath(__file__)).split(os.path.sep)[:-1])
        version_marker_json_file = os.path.sep.join([WORKSPACE, "conf", "version_marker.json"])
        json_data = json.load(open(version_marker_json_file))

        annotation_condition_dict = {True: [], False: []}
        annotation_dict = {True: [], False: []}

        for marker in run_marker_list:
            marker = marker.strip()
            annotation_condition = json_data[run_marker_version][marker]
            annotation_condition = annotation_condition.replace("(", " ( ")
            annotation_condition = annotation_condition.replace(")", " ) ")

            if "and" in annotation_condition or "or" in annotation_condition:
                for each_condition in annotation_condition.split("or"):
                    if "and" in each_condition:
                        for each in each_condition.split("and"):
                            annotation_condition_dict[is_annotation_true(each)].append(each)
                    else:
                        annotation_condition_dict[is_annotation_true(each_condition)].append(each_condition)
            else:
                annotation_condition_dict[is_annotation_true(annotation_condition)].append(annotation_condition)

        logger.info("annotation_condition_dict = %s", str(annotation_condition_dict))

        for isExpected in annotation_condition_dict:
            for annotation in annotation_condition_dict[isExpected]:
                annotation_list = set(annotation.split(" "))
                try:
                    annotation_list.remove('')
                except Exception:
                    pass
                try:
                    annotation_list.remove('(')
                except Exception:
                    pass
                try:
                    annotation_list.remove(')')
                except Exception:
                    pass
                try:
                    annotation_list.remove('or')
                except Exception:
                    pass
                try:
                    annotation_list.remove('and')
                except Exception:
                    pass
                try:
                    annotation_list.remove('not')
                except Exception:
                    pass
                annotation_dict[isExpected].extend(annotation_list)

        logger.info("annotation_dict = %s", str(annotation_dict))
        return annotation_dict
    except Exception:
        logger.error("Exception occured during get_annotation_dict")
        logger.error(traceback.format_exc())
        return {True: [], False: []}


def is_annotation_true(each):
    if "not" in each:
        return False
    else:
        return True
