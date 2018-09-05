#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#

import logging, re, time, uuid
from beaver.seleniumDP import SeleniumDP
from selenium.webdriver.common.by import By
from beaver.component.dataplane.core.basePage import BasePage
from selenium.webdriver.common.keys import Keys

from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class InfraLandingPage(BasePage):
    def __init__(self, basePageObj):
        BasePage.__init__(self, basePageObj.driver)

    clusterInfoTableXPath = "//table[@dp-config-table='']"
    locator_dictionary = {
        #TODO: Change below XPATH
        #"clusterInfoTable": (By.XPATH, "/html/body/data-plane/main/div/dp-infra-lakes/div/section/dp-lakes-list/div/table"),
        "clusterInfoTable": (By.XPATH, clusterInfoTableXPath),
        "clusterInfoTableBody": (By.XPATH, "%s/tbody" % (clusterInfoTableXPath)),
        "addButton": (By.ID, "add-btn"),
        "clusterTableRows": (By.XPATH, "//table/tbody/tr"),
        "statusLocatorClass": (By.XPATH, "//*[@class='fa fa-circle']"),
        "statusLocator": (By.XPATH, "//table/tbody/tr/td[1]"),
        # A dynamic xpath which needs to be constructed by the caller
        "clusterRowButton": None,
        "headerColumnValues": None,
        "refreshClusterButton": (By.XPATH, "//table/tbody/tr/td[8]/button"),
        "clusterRefreshHover": (By.XPATH, "//div[@class='mdl-menu__container is-upgraded is-visible']"),
        "clusterRefreshClickElement": (
            By.XPATH,
            "//div[@class='mdl-menu__container is-upgraded is-visible']//li/span/i[@class='fa__fa-close--icon fa fa-refresh']/parent::*/parent::*"
        ),
        "searchBar": (By.XPATH, "//input[@placeholder='Search']"),
        "searchFilterHover": (By.XPATH, "//div[@id='filter-option-list']"),
        "uncheckSearchFilter": (By.XPATH, "//span/i[@class='fa fa-times']"),
        "infoLabelElement": (By.XPATH, "//label[contains(text(),'INFORMATION')]"),
        "dpClusterDetailsElement": (By.XPATH, "//dp-cluster-details"),
        "editClusterLocationElement": (By.XPATH, "//input[@name='location']"),
        "editClusterDatacenterElement": (By.XPATH, "//input[@name='datacenter']"),
        "updateClusterDetailsButton": (By.XPATH, "//button[@id='add-btn']"),
        "clusterLocationDropdownElement": (By.XPATH, "//ngui-auto-complete/div/ul"),
        "editClusterLocationErrorTextElement": (By.XPATH, "//span[@class='location-error-text']"),
        "locationMarkerOnMap": (By.XPATH, "//div[@class='marker-wrapper']"),
        "sortableHeadersOfClusterTable": (By.XPATH, "//table//th/dp-config-sorter/span"),
    }

    def isInfraLandingPage(self, retryCount=0, timeout=None, quitWebdriver=False):
        return self.checkElementonPage(
            locatorName=self.getClusterInfoTable(returnLocatorName=True),
            locatorMessage='Infra Landing Page',
            retryCount=retryCount,
            timeout=timeout,
            quitWebdriver=quitWebdriver
        )

    def getClusterInfoTable(self, returnLocatorName=False):
        return self.getElement('clusterInfoTable', returnLocatorName)

    def getAddButton(self, returnLocatorName=False):
        return self.getElement('addButton', returnLocatorName)

    def clickAddButton(self):
        SeleniumDP.click(self.driver, self.getAddButton())

    def getStatusLocator(self, returnLocatorName=False):
        return self.getElement('statusLocatorClass', returnLocatorName)

    def getClusterRowButton(self, returnLocatorName=False):
        return self.getElement('clusterRowButton', returnLocatorName)

    def getClusterInfoTableBody(self, returnLocatorName=False):
        return self.getElement('clusterInfoTableBody', returnLocatorName)

    def getInfoLabelElement(self, returnLocatorName=False):
        return self.getElement('infoLabelElement', returnLocatorName)

    def getDPClusterDetailsElement(self, returnLocatorName=False):
        return self.getElement('dpClusterDetailsElement', returnLocatorName)

    def getEditClusterLocationElement(self, returnLocatorName=False):
        return self.getElement('editClusterLocationElement', returnLocatorName)

    def getEditClusterDatacenterElement(self, returnLocatorName=False):
        return self.getElement('editClusterDatacenterElement', returnLocatorName)

    def getUpdateClusterDetailsButton(self, returnLocatorName=False):
        return self.getElement('updateClusterDetailsButton', returnLocatorName)

    def getClusterLocationDropdownElement(self, returnLocatorName=False):
        return self.getElement('clusterLocationDropdownElement', returnLocatorName)

    def getEditClusterLocationErrorTextElement(self, returnLocatorName=False):
        return self.getElement('editClusterLocationErrorTextElement', returnLocatorName)

    def clickClusterRowButton(self, buttonRowNum):
        xpathForRowButton = "//table/tbody/tr[%s]/td[8]/button" % buttonRowNum
        InfraLandingPage.locator_dictionary["clusterRowButton"] = (By.XPATH, xpathForRowButton)
        if self.getClusterRowButton().is_enabled():
            logger.info("Button is available and clickable, Proceeding to click ...")
            SeleniumDP.click(self.driver, self.getClusterRowButton())
            return True
        else:
            logger.error("ClusterRowButton not clickable, might need a retry !!!")
            return False

    def getClusterRefreshHover(self, returnLocatorName=False):
        return self.getElement('clusterRefreshHover', returnLocatorName)

    def checkIfRefreshHoverIsVisible(self):
        return self.checkElementonPage(
            locatorName=self.getClusterRefreshHover(returnLocatorName=True),
            locatorMessage='Refresh Hover',
            retryCount=3
        )

    def getClusterRefreshClickableElement(self, returnLocatorName=False):
        return self.getElement('clusterRefreshClickElement', returnLocatorName)

    def clickRefreshCluster(self, rowNum, retryNum=3):
        try:
            SeleniumDP.click(self.driver, self.getClusterRefreshClickableElement())
            return True
        except Exception as e:
            # We might reach this point only if StaleElementReferenceException
            # is seen where-in we might need to invoke the whole process again
            logger.error("Caught an exception: %s, Let's retry to try our luck !!!" % e)
            if retryNum > 0:
                logger.info("Retry Attempt:  %s" % retryNum)
                logger.info(
                    "Invoking clickClusterButtonAndValidateHoverVisibility"
                    "for Row: %s since refresh caused cache to loose state" % rowNum
                )
                if self.clickClusterButtonAndValidateHoverVisibility(rowNum=rowNum):
                    logger.info("Hover visible in clickRefreshCluster, Proceeding to click now")
                    return self.clickRefreshCluster(rowNum, retryNum - 1)
                else:
                    logger.error("No point proceeding further ... Failing the test")
                    return False
            else:
                logger.info("Retry Attempts failed")
                return False

    def getSearchBar(self, returnLocatorName=False):
        return self.getElement('searchBar', returnLocatorName)

    def getSearchFilterHover(self, returnLocatorName=False):
        return self.getElement('searchFilterHover', returnLocatorName)

    def checkIfSearchFilterHoverIsVisible(self):
        return self.checkElementonPage(
            locatorName=self.getSearchFilterHover(returnLocatorName=True),
            locatorMessage='Search Filter Hover',
            retryCount=3
        )

    def getSearchFilterHoverContent(self):
        searchFilterHoverElement = self.getSearchFilterHover(returnLocatorName=False)
        rows = searchFilterHoverElement.find_elements_by_class_name('option-value')
        return [row.text for row in rows]

    def getSearchFilterElementsFromHover(self):
        searchFilterHoverElement = self.getSearchFilterHover(returnLocatorName=False)
        return searchFilterHoverElement.find_elements_by_class_name('option-value')

    def checkIfSearchFilterElementIsClickable(self, searchFilterElement):
        if searchFilterElement.is_enabled():
            logger.info("searchFilterElement is available and clickable, Proceeding to click ...")
            SeleniumDP.click(self.driver, searchFilterElement)
            self.take_screenshot("checkIfSearchFilterElementIsClickable")
            return True
        else:
            logger.error("SearchFilterElement: %s is not enabled yet" % searchFilterElement.text)
            return False

    # Getter Methods for all available elements on the web page
    # Symbolically 'All' in fucntion depicts getElements -> findElements
    # is used
    def getAllRefreshClusterButtons(self, returnLocatorName=False):
        # It's observed during testing that it takes some time to load the
        # content of the table and hence its better to wait for the entire
        # table to be loaded before fetching table content
        # Also, the latest fix for the issue: https://hortonworks.jira.com/browse/BUG-89799
        # enables any operations on the cluster table rows only after
        # all the table entries are loaded properly
        if self.waitForTheClusterTableToBeLoaded():
            # getElements returns an empty list in case no rows are available
            # So, safe to return directly
            return self.getElements('refreshClusterButton', returnLocatorName)
        else:
            return False

    def getAllClusterTableRows(self, returnLocatorName=False):
        if self.waitForTheClusterTableToBeLoaded():
            self.take_screenshot("getAllClusterTableRows")
            return self.getElements('clusterTableRows', returnLocatorName)
        else:
            return False

    def getAllStatusEntries(self, returnLocatorName=False):
        self.waitForElement(self.getStatusLocator(returnLocatorName=True))
        return self.getElements('statusLocator', returnLocatorName)

    def getAllSearchFiltersApplied(self, returnLocatorName=False):
        return self.getElements('uncheckSearchFilter', returnLocatorName)

    def getAllLocationMarkersOnMap(self, returnLocatorName=False):
        return self.getElements('locationMarkerOnMap', returnLocatorName)

    def getAllSortableHeaderElements(self, returnLocatorName=False):
        if self.waitForTheClusterTableToBeLoaded():
            return self.getElements('sortableHeadersOfClusterTable', returnLocatorName)
        else:
            return False

    def getAllColumnValuesForHeader(self, returnLocatorName=False):
        return self.getElements('headerColumnValues', returnLocatorName)

    # Utility Methods for the test cases
    def getHealthStatusOfAllClusters(self):
        """
        Returns a list of status for all the clusters available on the web
        page in the order of index
        """
        clusterStatusEntries = self.getAllStatusEntries()
        returnStatus = []
        logger.info("Identified %s clusters on the InfraLanding Page" % len(clusterStatusEntries))
        for iter in clusterStatusEntries:
            matches = re.search(r'status-(\w+)$', iter.get_attribute("class"))
            if matches:
                returnStatus.append(matches.group(1))

        logger.info("Health status retrieved from the clusters: %s" % returnStatus)
        return returnStatus

    def waitForTheClusterTableToBeLoaded(self):
        """
        Waits for the cluster table to be loaded completely so that we fetch
        the table content only after it is loaded. This fix is for the issue:
        https://hortonworks.jira.com/browse/QE-15914. Since, this is an
        intermittent issue we need to monitor the test execution under NAT's
        to validate that this fix helps.
        Fix : The idea here is to wait for a time of 500 seconds and poll for
        every 5 secs to verify if all the health status entries
        available in the cluster table return the status: 'up' else fail
        """
        timeoutInSecs = 500
        waitTimeInSecs = 5
        numOfIterations = timeoutInSecs / waitTimeInSecs
        logger.info(
            "Let's poll for [%s] iterations of [%s] seconds each to"
            " check if the content of the cluster table is loaded" % (numOfIterations, waitTimeInSecs)
        )
        for iter in range(numOfIterations):
            logger.info("Polling for the iteration: %s" % iter)
            clusterStatus = self.getHealthStatusOfAllClusters()
            if all(x != 'waiting' for x in clusterStatus):
                logger.info("All the rows of the cluster table are loaded: %s" % clusterStatus)
                return True
            else:
                logger.info(
                    "Sleeping for %s secs as all the rows: %s are not"
                    " loaded" % (waitTimeInSecs, clusterStatus)
                )
                time.sleep(waitTimeInSecs)
        # We reach here after a wait of timeoutInSecs seconds. If the cluster
        # table is not loaded even after wait let's consider it a failure
        logger.info(
            "Looks like the cluster table is not loaded even after a "
            "wait of [%s] secs, cannot help anymore !!!" % timeoutInSecs
        )
        return False

    def getRefreshClusterButtonsForAllCLusters(self):
        """
        Returns a list of clickable button elements for every cluster
        available on the web page
        """
        refreshButtons = self.getAllRefreshClusterButtons()
        respButtons = []
        for btn in refreshButtons:
            respButtons.append(btn)

        return respButtons

    def clickClusterButtonAndValidateHoverVisibility(self, rowNum, retryCount=3):
        """
        It's observed in testing that sometimes button is clicked but hover
        is not discoverable by the test.  Utility function just to be safe
        with a retry
        """
        if (self.clickClusterRowButton(rowNum) and self.checkIfRefreshHoverIsVisible()):
            logger.info("Check successful, Saving screenshot for reference")
            self.take_screenshot("clickClusterButtonAndValidateHoverVisibility")
            return True
        else:
            if retryCount > 0:
                logger.info("Retry Attempt: %s" % retryCount)
                return self.clickClusterButtonAndValidateHoverVisibility(rowNum, retryCount - 1)
            else:
                logger.error("Retry attempts failed")
                return False

    def getClusterTableSearchableContent(self):
        """
        Search input on DP InfraLanding page supports search for the following
        attributes: Name, Data Center, City and Country.
        So, this function returns all the content available in these columns
        of the cluster table as a dict of the same format
        """
        if self.waitForTheClusterTableToBeLoaded():
            logger.info("Cluster Table is loaded properly, Constructing" "Searchable Content")

            def getReturnResp(driver):
                returnResp = {}
                tableRows = self.getAllClusterTableRows()
                for row in tableRows:
                    tdElements = row.find_elements(By.TAG_NAME, 'td')
                    tdList = [entry.text for entry in tdElements]
                    # We are interested in 3 elements 1, 2 and 3 which are Name,
                    # Location and DC respectively
                    # Adding Name entries to the response
                    if returnResp.get('name', None) is None:
                        returnResp['name'] = [tdList[1]]
                    else:
                        returnResp['name'].append(tdList[1])

                    city, country = tdList[2].split(', ')
                    # Adding city entries to the response
                    if returnResp.get('city', None) is None:
                        returnResp['city'] = [city]
                    else:
                        returnResp['city'].append(city)

                    # Adding Country entries to the response
                    if returnResp.get('country', None) is None:
                        returnResp['country'] = [country]
                    else:
                        returnResp['country'].append(country)

                    # Adding data center entries to the response
                    if returnResp.get('datacenter', None) is None:
                        returnResp['datacenter'] = [tdList[3]]
                    else:
                        returnResp['datacenter'].append(tdList[3])

                logger.info("Searchable Content constructed is: %s" % returnResp)

                return returnResp

            # Dont load the page untill all the data from the table is fetched
            # Setting it equal to that of implicit time
            return WebDriverWait(self.driver, 30).until(getReturnResp)
        else:
            logger.error("Cluster Table not loaded properly")
            return False

    def submitTextIntoSearchBarAndValidateHoverContent(self, searchIndex):
        """
        Submits the text into the search bar and validates if the content of
        the hover is appropriate
        """
        searchField = self.getSearchBar()
        searchField.clear()
        SeleniumDP.sendKeys(searchField, searchIndex)
        if self.checkIfSearchFilterHoverIsVisible():
            logger.info("Search Filter Hover visible for the search index: %s" % searchIndex)
            filteredContent = self.getSearchFilterHoverContent()
            logger.info("Filtered content is: %s" % filteredContent)
            for line in filteredContent:
                if searchIndex.lower().rstrip() not in line.lower():
                    logger.info("Search index: %s not found in: %s" % (searchIndex, line))
                    return False
            return True
        else:
            logger.error("Search Hover is not visible even after retry attempts")
            return False

    def validateClusterTableEntriesWithSearchIndex(self, searchIndex):
        """
        Validate if the entries in the cluster table match the searchIndex
        """
        tableContent = self.getClusterTableSearchableContent()
        for attribute in tableContent:
            columnEntries = tableContent[attribute]
            for entry in columnEntries:
                if searchIndex.lower() in entry.lower():
                    logger.info("Searchable index: %s available in: %s" % (searchIndex, entry))
                    return True

        return False

    def removeAllSearchFiltersApplied(self):
        """
        Removes all the search filters applied on the cluster table
        """
        searchFiltersApplied = self.getAllSearchFiltersApplied()
        if searchFiltersApplied:
            for searchFilter in searchFiltersApplied:
                SeleniumDP.click(self.driver, searchFilter)
            logger.info("%s filter/s removed" % len(searchFiltersApplied))
            return True
        else:
            logger.info("No filters are applied")
            return False

    def getAllSearchableContentFromAnyRow(self, searchableContent, contentType='string', rowNum=1):
        """
        Some of the use cases need data from a single row. This function
        facilitates the same in returning all the searchable content from
        first row
        @params: contentType is string/numeric
        """
        returnResp = []
        for column in searchableContent:
            if rowNum < len(searchableContent[column]):
                if contentType == 'string':
                    returnResp.append(searchableContent[column][rowNum - 1])
                elif contentType == 'numeric':
                    string = searchableContent[column][rowNum - 1]
                    matches = re.findall(r'\d+', string)
                    if matches:
                        returnResp.extend(matches)
                else:
                    logger.error("Unknown content type: %s passed as input argument" % contentType)
            else:
                logger.info("rowNum: %s is not supported with the current " "cluster data" % rowNum)
                return False

        return returnResp

    def getRandomString(self):
        """
        Return a hex string for negative search
        """
        return uuid.uuid4().hex

    def clickUpdateClusterButton(self):
        SeleniumDP.click(self.driver, self.getUpdateClusterDetailsButton())

    def viewClusterDetails(self, clusterName):
        self.isInfraLandingPage(retryCount=2)
        clusterInfoTableExists = self.checkElementonPage(
            locatorName=self.getClusterInfoTable(returnLocatorName=True),
            locatorMessage='Clusters info table',
            retryCount=5
        )
        if clusterInfoTableExists:
            clusterInfoTableBody = self.findElement(
                *self.locator_dictionary[self.getClusterInfoTableBody(returnLocatorName=True)]
            )
            for row in clusterInfoTableBody.find_elements(By.TAG_NAME, 'tr'):
                columns = row.find_elements(By.TAG_NAME, 'td')
                if clusterName.lower() == columns[1].text.lower():
                    SeleniumDP.click(self.driver, columns[1])
                    break

    def verifyClusterDetailsPage(self, clusterName):
        infoLabelExists = self.checkElementonPage(
            locatorName=self.getInfoLabelElement(returnLocatorName=True),
            locatorMessage='Information Label',
            retryCount=2
        )
        dpClusterDetailsExists = self.checkElementonPage(
            locatorName=self.getDPClusterDetailsElement(returnLocatorName=True),
            locatorMessage='DP Cluster Details',
            retryCount=2
        )
        if infoLabelExists and dpClusterDetailsExists:
            dpClusterDetails = self.findElement(
                *self.locator_dictionary[self.getDPClusterDetailsElement(returnLocatorName=True)]
            )
            if dpClusterDetails.find_element(By.XPATH,
                                             "//div[@class='hwx-title'][contains(text(),'%s')]" % (clusterName)):
                return True
        return False

    def selectClusterToEdit(self, clusterName):
        self.isInfraLandingPage(retryCount=2)
        clusterInfoTableExists = self.checkElementonPage(
            locatorName=self.getClusterInfoTable(returnLocatorName=True),
            locatorMessage='Clusters info table',
            retryCount=5
        )
        if clusterInfoTableExists:
            tries = 5
            while tries > 0:
                try:
                    clusterInfoTableBody = self.findElement(
                        *self.locator_dictionary[self.getClusterInfoTableBody(returnLocatorName=True)]
                    )
                    for row in clusterInfoTableBody.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if clusterName.lower() == columns[1].text.lower():
                            actionsButton = columns[7].find_element(By.TAG_NAME, 'button')
                            SeleniumDP.click(self.driver, actionsButton)
                            dropDownMenuElement = columns[7].find_element(By.TAG_NAME, 'ul')
                            dropDownOptions = dropDownMenuElement.find_elements(By.TAG_NAME, 'li')
                            for option in dropDownOptions:
                                entries = option.find_elements(By.TAG_NAME, 'a')
                                for entry in entries:
                                    if entry.text.lower() == 'edit':
                                        SeleniumDP.click(self.driver, entry)
                                        return
                except:
                    tries = tries - 1

    def editClusterLocation(self, newClusterLocation):
        editClusterLocationExists = self.checkElementonPage(
            locatorName=self.getEditClusterLocationElement(returnLocatorName=True),
            locatorMessage='Edit Cluster Location',
            retryCount=2
        )
        if editClusterLocationExists:
            tries = 5
            while tries > 0:
                try:
                    SeleniumDP.sendKeys(self.getEditClusterLocationElement(), newClusterLocation)
                    clusterLocationDropdownVisible = self.waitForElement(
                        locatorName=self.getClusterLocationDropdownElement(returnLocatorName=True)
                    )
                    if clusterLocationDropdownVisible:
                        clusterLocationDropdown = self.findElement(
                            *self.locator_dictionary[self.getClusterLocationDropdownElement(returnLocatorName=True)]
                        )
                        options = clusterLocationDropdown.find_elements(By.TAG_NAME, 'li')
                        for option in options:
                            if option:
                                SeleniumDP.sendKeys(
                                    self.getEditClusterLocationElement(), newClusterLocation + Keys.ENTER
                                )
                                if self.checkElementonPage(
                                        locatorName=self.getEditClusterLocationErrorTextElement(returnLocatorName=True
                                                                                                ),
                                        locatorMessage='Cluster Location Error'):
                                    raise ValueError('Cluster Location Error text found')
                                return
                except:
                    self.take_screenshot("editClusterLocation")
                    tries = tries - 1

    def editClusterDatacenter(self, newClusterDatacenter):
        editClusterLocationExists = self.checkElementonPage(
            locatorName=self.getEditClusterLocationElement(returnLocatorName=True),
            locatorMessage='Edit Cluster Location',
            retryCount=2
        )
        editClusterDatacenterExists = self.checkElementonPage(
            locatorName=self.getEditClusterDatacenterElement(returnLocatorName=True),
            locatorMessage='Edit Cluster Datacenter',
            retryCount=2
        )
        if editClusterLocationExists and editClusterDatacenterExists:
            SeleniumDP.sendKeys(self.getEditClusterDatacenterElement(), newClusterDatacenter)
            return

    def verifyClusterLocationValue(self, clusterName, clusterLocationValue):
        self.isInfraLandingPage(retryCount=2)
        clusterInfoTableBodyExists = self.checkElementonPage(
            locatorName=self.getClusterInfoTableBody(returnLocatorName=True),
            locatorMessage='Clusters info table',
            retryCount=5
        )
        if clusterInfoTableBodyExists:
            tries = 5
            while tries > 0:
                try:
                    clusterInfoTableBody = self.findElement(
                        *self.locator_dictionary[self.getClusterInfoTableBody(returnLocatorName=True)]
                    )
                    for row in clusterInfoTableBody.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if clusterName.lower() == columns[1].text.lower():
                            if clusterLocationValue.lower() == columns[2].text.lower():
                                return True
                except:
                    tries = tries - 1
        return False

    def verifyClusterDatacenterValue(self, clusterName, clusterDatacenterValue):
        self.isInfraLandingPage(retryCount=2)
        clusterInfoTableBodyExists = self.checkElementonPage(
            locatorName=self.getClusterInfoTableBody(returnLocatorName=True),
            locatorMessage='Clusters info table',
            retryCount=5
        )
        if clusterInfoTableBodyExists:
            tries = 5
            while tries > 0:
                try:
                    clusterInfoTableBody = self.findElement(
                        *self.locator_dictionary[self.getClusterInfoTableBody(returnLocatorName=True)]
                    )
                    for row in clusterInfoTableBody.find_elements(By.TAG_NAME, 'tr'):
                        columns = row.find_elements(By.TAG_NAME, 'td')
                        if clusterName.lower() == columns[1].text.lower():
                            if clusterDatacenterValue.lower() == columns[3].text.lower():
                                return True
                except:
                    tries = tries - 1
        return False

    def clickClusterTableSortableHeader(self, clickableElement):
        """
        Clicks the Cluster Table Header element which comes in as the input
        argument
        """
        try:
            SeleniumDP.click(self.driver, clickableElement)
            logger.info("Clicked the element")
            return True
        except Exception as e:
            logger.info("Caught Exception: %s" % e)
            return False

    def convertToNumericals(self, inputList, convertType):
        """
        Some of the headers are sorted based on some qualifiers and hence this
        functions converts the input list to corresponding values
        """
        if convertType == 'NUMERIC':
            return [int(x) for x in inputList]
        elif convertType == 'HDFS_USED':
            convertDict = {
                'GB': 1000,
                'TB': 1000000,
            }
            vals = [x.split(' /')[0] for x in inputList]
            returnVals = []
            for val in vals:
                matches = re.search(r'(\d+)\s+(\w+)$', val)
                if matches:
                    qualifier = matches.group(2)
                    returnVals.append(int(matches.group(1)) * convertDict[qualifier])
                else:
                    # All the items in the input list should be of the same format
                    return False

            return returnVals
        else:
            logger.error("Inappropriate convertType passed")
            return False

    def getAllColumnValuesOfSortedHeader(self, headerNum):
        """
        In the headers we know that the first header: 'status' doesn't support
        sorting and hence it'll be not part of the sortable headers. So, let's
        add 1 to any headerNum requested in order to avoid the first header
        """
        headerNumAsPerTable = int(headerNum + 1)
        xpathForColumnValues = "//table/tbody/tr/td[%s]" % headerNumAsPerTable
        InfraLandingPage.locator_dictionary["headerColumnValues"] = (By.XPATH, xpathForColumnValues)
        returnCols = []
        cols = []
        # Let's get all the values first
        for ele in self.getAllColumnValuesForHeader():
            cols.append(ele.text)
        # Sorting logic is implemented based on header for some of the columns
        # like: HDFS Used, Nodes and Uptime and rest all are simple string
        # sort. Test support is added for HDFS Used and Nodes for now. Uptime
        # is one another column which needs to be calculated after converting
        # human readable time from the cluster table to milliseconds and then
        # sort based on that but needs a lot of effort.
        if headerNumAsPerTable == 5:
            logger.info("Header is Nodes, so converting to numericals")
            returnCols = self.convertToNumericals(cols, convertType='NUMERIC')
        elif headerNumAsPerTable == 6:
            # For the header Uptime, the UI has a humar readable format which
            # is difficult to change to a format which can be sorted. So, lets
            # skip the validation for now
            logger.info("Header is Uptime and hence returning dummy values for" " validation")
            returnCols = ['None', 'None']
        elif headerNumAsPerTable == 7:
            logger.info("Header is HDFS used, so converting it to numericals " "as per the current Implementation")
            returnCols = self.convertToNumericals(cols, convertType='HDFS_USED')
        else:
            returnCols = cols

        logger.info("The Values to be returned back are: %s" % returnCols)
        return returnCols
