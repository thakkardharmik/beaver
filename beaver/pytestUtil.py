#
# Copyright  (c) 2018, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
import re


# Remove hashcode from a potential java object's toString.
# For example, from: "[org.apache.falcon.regression.core.helpers.entity.ProcessEntityHelper@52937d7e]
def cleanup_hashcode(param):
    as_string = str(param)
    return re.sub('@[0-9a-f]*', '@', as_string)


# Remove the ycloud cluster node name
# For example, from: "test_datanode_reboot[nodemanager_reboot-138-1518143905142-415443-01-000003.hwx.site:8090/cluster/nodes/lost]"
def remove_hwx_site(param):
    as_string = str(param)
    return re.sub('[0-9\-]*.hwx.site','(HWX.SITE)', as_string)


def replace_text(old_text, new_value):
    def inner(param):
        as_string = str(param)
        return param.replace(old_text,new_value)
    return inner

