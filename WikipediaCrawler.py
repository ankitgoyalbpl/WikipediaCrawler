'''

'''

from bs4 import BeautifulSoup
import sys
import urllib
import os
import pickle
import Queue
import hashlib
import re

'''
Class to Query the Wikipedia Site
and fetch the data according from
the concerned webpage
'''
class WikipediaPageData:

    __WikipediaBasePage = u'https://en.wikipedia.org'

    def __init__(self, webpageAddress=None):
        self.webpageAddress = webpageAddress
        self.webpageData = None
        self.textContent = None
        self.internalLinks = None


    def __SetDefaultEncodingToUnicode__(self):
        reload(sys)
        sys.setdefaultencoding('utf8')

    '''
    Set Wikipedia Page address to fetch data from
    '''
    def SetWebpageAddress(self, webpageAddress):
        self.webpageAddress = webpageAddress

    '''
    Get the current Wikipedia Page address
    '''
    def GetWebpageAddress(self):
        return self.webpageAddress

    '''
    Main Function that fetches content from the
    Wikipedia Server and stores the HTML Content
    locally for futher processing.
    Return a Tuple of Bool and string to show success
    and reason respectively.
    '''
    def ProcessWikipediaPage(self):
        if self.webpageAddress is None:
            return (False, 'No Webpage Set to Prcess')

        try:
            self.webpageData = urllib.urlopen(url=self.webpageAddress).read()
            self.webpageData = BeautifulSoup(self.webpageData, 'html.parser')

        except:
            return (False, sys.exc_info()[1])

        return (True, 'Succesfully processed the Wikipedia page: ' + self.webpageAddress)

    '''
    Function to extract textual content from the current webpage
    '''
    def GetAllTextFromWebpage(self):
        try:
            if self.textContent is not None:
                return self.textContent

            if self.webpageData is None:
                self.ProcessWikipediaPage()

            if self.webpageData is None:
                return (False, 'No Webpage Content Found')

            self.textContent = u''
            resultTextSet = self.webpageData.find_all('p')
            for resultTextItem in resultTextSet:
                self.textContent += u' ' + (resultTextItem.get_text())

            self.textContent = self.textContent.replace(u'\xa0', u' ')
            self.textContent = re.sub(u'\[[\w\d\s]+\]', u'', self.textContent).strip()
            return (True, self.textContent

        except:
            return (False, sys.exc_info()[1])

    '''
    Function to extract all internal links from the page
    '''
    def GetAllWikiLinksFromWebpage(self):
        try:
            if self.internalLinks is not None:
                return self.internalLinks

            if self.webpageData is None:
                self.ProcessWikipediaPage()

            if self.webpageData is None:
                return (False, 'No Webpage Content Found')

            internalLinksList = []
            resultLinkList = self.webpageData.find_all('a')
            for resultLinkItem in resultLinkList:
                internalLinksList.append(resultLinkItem.get('href'))

            self.internalLinks = set()
            for urlLink in internalLinksList:
                if urlLink is not None and urlLink.startswith(u'/wiki/') and not urlLink.startswith(u'/wiki/File'):
                    if "#" in urlLink:
                        self.internalLinks.add(WikipediaPageData.__WikipediaBasePage + urlLink[:urlLink.find("#")])
                    elif "?" in urlLink:
                        self.internalLinks.add(WikipediaPageData.__WikipediaBasePage + urlLink[:urlLink.find("?")])
                    else:
                        self.internalLinks.add(WikipediaPageData.__WikipediaBasePage + urlLink)

            del internalLinksList
            self.internalLinks = list(self.internalLinks)
            return (True, self.internalLinks)

        except:
            return (False, sys.exc_info()[1])

    '''
    TODO
    '''
    def GetAllImagesLinksFromWebpage(self):
        pass


'''
Class that Crawls Wikipedia pages in count of a Batch Size
starting from the seed page.
'''
class WikipediaCrawler:

    def __Constants__(self):
        self.__CompletedWebpagesSetFile = './CompletedPages.pkl'
        self.__PendingWebpagesQueueFile = './PendingPages.pkl'

    def __InitializeCrawler__(self):
        try:
            if os.path.exists(self.__CompletedWebpagesSetFile):
                self.completedWebpagesSet = pickle.load(self.__CompletedWebpagesSetFile, 'rb')

        except:
            self.completedWebpagesSet = set()

        try:
            if os.path.exists(self.__PendingWebpagesQueueFile):
                self.pendingWebpagesQueue = pickle.load(self.__PendingWebpagesQueueFile, 'rb')

        except:
            self.pendingWebpagesQueue = Queue()


    def DumpCrawlerFiles(self):
        try:
            print 'Pickling Completed Webpage Set'
            pickle.dump(self.completedWebpagesSet, file(self.__CompletedWebpagesSetFile, 'rb'))
            print 'Pickling Pending Webpage Queue'
            pickle.dump(self.pendingWebpagesQueue, file(self.__PendingWebpagesQueueFile, 'rb'))
            print 'Done!'
        except:
            print sys.exc_info()


    def __init__(self, batchSize=10):
        self.batchSize = batchSize
        self.completedWebpagesSet = None
        self.pendingWebpagesQueue = None

        self.__Constants__()
        self.__InitializeCrawler__()


    def __del__(self):
        self.DumpCrawlerFiles()


    def __EncodeWikipediaPageAddress__(self, wikipediaPageAddress):
        if wikipediaPageAddress is None:
            return None

        try:
            return hashlib.sha1(wikipediaPageAddress.encode()).hexdigest()

        except:
            return None


    def GetPendingWikiPagesList(self):
        return self.pendingWebpagesQueue


    def SetWikipediaSeedPage(self, wikipediaPageAddress):
        if self.pendingWebpagesQueue == None:
                self.pendingWebpagesQueue = Queue.Queue()

        self.pendingWebpagesQueue.put(wikipediaPageAddress)


    def CrawlWikipedia(self):

        resultSetCount = self.batchSize
        self.completedWebpagesSet = set()
        resultData = []

        while resultSetCount > 0 and self.pendingWebpagesQueue.qsize() > 0:
            try:
                wikipediaPageAddress = self.pendingWebpagesQueue.get()
                if self.__EncodeWikipediaPageAddress__(wikipediaPageAddress) in self.completedWebpagesSet:
                    continue

                wikipediaPageText = self.CrawlWikipediaPage(wikipediaPageAddress)
                resultData.append((wikipediaPageAddress, wikipediaPageText))
                resultSetCount -= 1
                self.completedWebpagesSet.add(self.__EncodeWikipediaPageAddress__(wikipediaPageAddress))

            except:
                continue

        return resultData


    def CrawlWikipediaPage(self, wikipediaPageAddress):
        if wikipediaPageAddress is None:
            return None

        wikiPageData = WikipediaPageData(wikipediaPageAddress)
        wikiPageResult = wikiPageData.ProcessWikipediaPage()

        if wikiPageResult[0] is False:
            return None

        wikiPageLinksResult = wikiPageData.GetAllWikiLinksFromWebpage()
        wikiPageTextResult = wikiPageData.GetAllTextFromWebpage()

        if wikiPageLinksResult[0] is True:
            if self.pendingWebpagesQueue is None:
                self.pendingWebpagesQueue = Queue.Queue()

            for wikiPageLink in wikiPageLinksResult[1]:
                self.pendingWebpagesQueue.put(wikiPageLink)

        if wikiPageTextResult[0] is True:
            if self.completedWebpagesSet is None:
                self.completedWebpagesSet = set()

            print wikiPageResult[1]
            self.completedWebpagesSet.add(self.__EncodeWikipediaPageAddress__(self.completedWebpagesSet))
            return wikiPageTextResult[1]

        return None
