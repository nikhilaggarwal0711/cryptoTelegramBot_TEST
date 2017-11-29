from threading import Thread
from fetchCoinMarketCap import fetchCoinMarketCap
from fetchBittrex import fetchBittrex
from runTelegram import runTelegram


class MyThread(Thread):
    def __init__(self,threadName,sleepTime):
        print "Inside thread constructor"
        Thread.__init__(self)
        self.threadName = threadName
        self.sleepTime = sleepTime
    
    def run(self):
        print "Starting run method of MyThread"
        if ( self.threadName == "coinMarketCap" ):
            print "coinmarketcap"
            fcmc = fetchCoinMarketCap()
            fcmc.start(self.sleepTime)
        elif  ( self.threadName == "bittrex" ):
            print "bittrex"
            fb = fetchBittrex()
            fb.start(self.sleepTime)
        elif ( self.threadName == "telegram" ):
            print "telegram"
            #rt = runTelegram()
            #rt.start(self.sleepTime)
        else:
            print "Wrong input"
