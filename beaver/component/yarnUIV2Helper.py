import logging
from beaver.component.yarnUIV2.queuePage import QueuePage
from beaver.component.yarnUIV2.applicationPage import ApplicationPage
from beaver.component.yarnUIV2.servicesPage import ServicesPage
from beaver.component.yarnUIV2.flowActivityPage import FlowActivityPage
from beaver.component.yarnUIV2.nodesPage import NodesPage
from beaver.component.hadoop import Hadoop, MAPRED, YARN
from beaver.component.yarnUIV2.commonPage import CommonPage
from taskreporter.taskreporter import TaskReporter
'''
Helper APIs for yarnUIV2
'''


@TaskReporter.report_test()
def verifyApplicationTab(basePage, commonPage, appsVisible):
    '''
    Verify if apps visible in application tab
    :param appsVisible: dict{appID:True/False based on if app is visible or not}
    :return:
    '''
    commonPage.clickClusterOverview()
    assert commonPage.isHomePage(retryCount=2)
    commonPage.clickApplications()
    appPage = ApplicationPage(basePage)
    assert appPage.isAppPage()

    for appID, visibility in appsVisible.iteritems():
        if visibility == True:
            assert appPage.isSpecificAppLink(appID)
        else:
            assert not appPage.isSpecificAppLink(appID)


@TaskReporter.report_test()
def verifyServiceTab(basePage, commonPage, appsVisible, component, service):
    '''
    Verify Service Tab
    :param appsVisible: dict{appID:True/False based on if app is visible or not}
    :return:
    '''
    commonPage.clickClusterOverview()
    assert commonPage.isHomePage(retryCount=2)

    for appID, visibility in appsVisible.iteritems():
        if visibility == True:
            commonPage.clickServices()
            servicePage = ServicesPage(basePage)
            assert servicePage.isServicePage()
            assert servicePage.isSpecificServiceLink(appID)
            servicePage.clickServiceComponent(appID)
            servicePage.isServiceComponentLink(appID, component, service)
        else:
            commonPage.clickServices()
            servicePage = ServicesPage(basePage)
            assert servicePage.isServicePage()
            assert not servicePage.isSpecificServiceLink(appID)


@TaskReporter.report_test()
def verifyFlowActivityTab(basePage, commonPage, appsVisible, flowName):
    '''
    Verify Flow Activity Tab
    :param appsVisible: dict{user:True/False based on if user'sapp is visible or not}
    :return:
    '''
    commonPage.clickClusterOverview()
    assert commonPage.isHomePage(retryCount=2)
    commonPage.clickFlowActivity()
    flowAppPage = FlowActivityPage(basePage)
    assert flowAppPage.isFlowPage()

    for user, visibility in appsVisible.iteritems():
        if visibility == True:
            assert flowAppPage.isSpecificFlowIDLink(user, flowName)
        else:
            assert not flowAppPage.isSpecificFlowIDLink(user, flowName)


@TaskReporter.report_test()
def verifyQueueTab(basePage, commonPage, appsVisible, queue):
    '''
    Verify Queue tab
    :param appsVisible: dict{appID:True/False based on if app is visible or not}
    :param queue: Queue name
    :return:
    '''
    commonPage.clickClusterOverview()
    assert commonPage.isHomePage(retryCount=2)
    appPage = ApplicationPage(basePage)

    #Queues Tab
    commonPage.clickQueues()
    queuePage = QueuePage(basePage)
    assert queuePage.isQueuePage()

    #Queue <queue name> Tab
    queuePage.clickQ(queue)
    assert queuePage.isSpecificQPage(queue)
    queuePage.clickQApp(queue)
    assert queuePage.isSpecificQAppPage(queue)

    for appID, visibility in appsVisible.iteritems():
        if visibility == True:
            # XPATH same as appPage AppID link;
            assert appPage.isSpecificAppLink(appID)
        else:
            assert not appPage.isSpecificAppLink(appID)


@TaskReporter.report_test()
def verifyNodesTab(basePage, commonPage, appsVisible):
    '''
    Verify Nodes Tab
    :param appsVisible: dict{appID:True/False based on if app is visible or not}
    :return:
    '''
    commonPage.clickClusterOverview()
    nodesPage = NodesPage(basePage)

    for node in YARN.getNodeManagerHosts(True):
        commonPage.clickNodes()
        assert nodesPage.isNodesPage()
        nodesPage.clickNodeAddress(node)
        assert nodesPage.isNodeInfoPage()

        #List of Applications tab
        nodesPage.clickNodeAppPage()
        assert nodesPage.isNodeAppPage()
        for appID, visibility in appsVisible.iteritems():
            if visibility == True:
                assert nodesPage.isNodeAppIDLink(appID)
            else:
                assert not nodesPage.isNodeAppIDLink(appID)

        #List of Containers tab
        nodesPage.clickNodeContainerPage()
        assert nodesPage.isNodeContainerPage()
        for appID, visibility in appsVisible.iteritems():
            if visibility == True:
                assert nodesPage.isNodeContainerIDLink(appID)
            else:
                assert not nodesPage.isNodeContainerIDLink(appID)
