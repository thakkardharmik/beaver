from HTMLParser import HTMLParser


class MyHTMLParser(HTMLParser):
    tableDetected = False
    currentTag = ''
    parsedData = {}
    isFound = False
    currentParsedData = {}
    currentDataToCheck = {}

    def __init__(self, _dataToCheck):
        self.dataToCheck = _dataToCheck
        self.parsedData = {}
        HTMLParser.__init__(self)  #need to call supper class  init

    def handle_starttag(self, tag, attrs):
        self.parseData(self.dataToCheck, self.parsedData, tag, attrs)

    def parseData(self, _dataToCheck, _parsedData, tag, attrs):
        for key, value in _dataToCheck.items():
            if key == tag:
                if not _parsedData.has_key(key):
                    _parsedData[key] = self.getNewElement(value)

                #tmp assigment for easy referece of current tag structure in other functions
                self.currentParsedData = _parsedData[key]
                self.currentDataToCheck = _dataToCheck[key]
                self.isFound = True  #process data handle only when we tag we want to check

            else:
                if isinstance(value, dict):
                    if _parsedData.has_key(key):
                        self.parseData(value, _parsedData[key], tag, attrs)

        #add all common attributes
        if isinstance(self.currentDataToCheck, dict):
            if self.currentDataToCheck.has_key('attr'):

                for key, value in self.currentDataToCheck['attr'].items():
                    if (key, value) in attrs:
                        self.currentParsedData.setdefault('attr', {})[key] = value

    def handle_data(self, data):

        if self.isFound is True:
            if isinstance(self.currentParsedData, dict):
                if self.currentDataToCheck.has_key('data'):
                    self.currentParsedData['data'] = data.strip()
            elif isinstance(self.currentParsedData, list):

                if data.strip() in self.currentDataToCheck:
                    self.currentParsedData.append(data.strip())

    def handle_endtag(self, tag):
        self.currentTag = ''
        self.isFound = False

    def getNewElement(self, value):  # pylint: disable=no-self-use
        result = []
        if isinstance(value, dict):
            result = {}
        if isinstance(value, list):
            result = []
        return result

    def is_valid(self, debug):
        if debug is True:
            print "Data to check:", self.dataToCheck
            print "Parsed data:", self.parsedData
        return self.dataToCheck == self.parsedData


class MRHTMLParser(HTMLParser):
    tableDetected = False
    rowData = []
    rowCount = 0
    cColCount = 0
    currentTag = ''

    def __init__(self):  # pylint: disable=super-init-not-called
        self.rowData = []
        self.rowCount = 0
        self.cColCount = 0
        self.currentTag = ''
        self.tableDetected = False
        HTMLParser.reset(self)

    def handle_starttag(self, tag, attrs):
        self.currentTag = tag
        if tag == 'table' and ('id', 'jobs') in attrs:
            print 'Table Detected'
            self.tableDetected = True
        if tag == 'tr' and self.tableDetected is True:
            self.rowCount = self.rowCount + 1
            self.rowData.append([])
            self.cColCount = 0

    def handle_data(self, data):
        if self.currentTag == 'td' and self.tableDetected is True:
            self.rowData[self.rowCount - 1].append(data.strip())
            self.cColCount = self.cColCount + 1

        if self.currentTag == 'a' and self.tableDetected is True and self.cColCount == 1:
            self.rowData[self.rowCount - 1][0] = data

    def handle_endtag(self, tag):
        self.currentTag = ''
        if tag == 'table' and self.tableDetected is True:
            self.tableDetected = False
