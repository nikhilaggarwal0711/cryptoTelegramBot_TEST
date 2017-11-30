from threading import Thread
from fetchCoinMarketCap import FetchCoinMarketCap
from fetchBittrex import FetchBittrex
from fetchBitfinex import FetchBitfinex
from runTelegram import RunTelegram


class MyThread(Thread):
    def __init__(self,threadName,sleepTime):
        #print "Inside thread constructor"
        Thread.__init__(self)
        self.threadName = threadName
        self.sleepTime = sleepTime
    
    def run(self):
        #print "Starting run method of MyThread"
        if ( self.threadName == "coinMarketCap" ):
            #print "coinmarketcap"
            fcmc = FetchCoinMarketCap()
            fcmc.start(self.sleepTime)
        elif  ( self.threadName == "bittrex" ):
            #print "bittrex"
            fBittrex = FetchBittrex()
            fBittrex.start(self.sleepTime)
        elif  ( self.threadName == "bitfinex" ):
            #print "bitfinex"
            fBitfinex = FetchBitfinex()
            fBitfinex.start(self.sleepTime)
        elif ( self.threadName == "telegram" ):
            #print "telegram"
            rt = RunTelegram()
            rt.setup_pythonAnyWhere()
            rt.start(self.sleepTime)
        else:
            print "Wrong input"
