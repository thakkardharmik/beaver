import traceback
from pprint import pformat


def ruAssert(component, expr, *args):
    from beaver.component.rollingupgrade.ruUpgrade import UpgradePerNode
    try:
        assert expr, args
    except AssertionError as e:
        if len(args) > 0:
            UpgradePerNode.reportProgress("[FAILED][%s] %s" % (component, str(args[0])))
        else:
            UpgradePerNode.reportProgress("[FAILED][%s] %s" % (component, str(e)))
        UpgradePerNode.reportProgress(pformat(traceback.format_stack(limit=3)))
    else:
        if len(args) > 0:
            UpgradePerNode.reportProgress("[PASSED][%s] Didn't fail on: %s" % (component, str(args[0])))
        else:
            UpgradePerNode.reportProgress("[PASSED][%s]" % component)
    return
