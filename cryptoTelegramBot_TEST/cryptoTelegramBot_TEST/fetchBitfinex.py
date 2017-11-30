import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep

class FetchBitfinex:
    def __init__(self):
        #print "inside Bitfinex constructor"
        self.allSymbols_link = "https://api.bitfinex.com/v1/symbols"
        self.symbolDetails_link = "https://api.bitfinex.com/v1/pubticker/"
        
        self.db = DBHelper()
        self.db.setup()
        
    def setFetchTime(self):
        #print "setFetchTime -- Bitfinex"
        self.fetchTime = int(time.time())

    def setDelTillFetchTime(self):
        #print "delTillFetchTime -- Bitfinex"
        #2days older data.
        deleteTime = 172800
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
        #print "fetchData -- Bitfinex"
        self.f1 = requests.get(url = self.allSymbols_link)
        self.data1 = self.f1.text.replace("null","0")
        self.jsonList1  = json.loads(self.data1)
        length1 = len(json.loads(self.data1))

        for x in range(0,length1):
            self.f2 = requests.get(url = (self.symbolDetails_link + self.jsonList[x]))
            self.data2 = self.f2.text.replace("null","0")
            self.jsonList2  = json.loads(self.data2)
            
            self.marketname = self.jsonList[x]
            self.mid = self.jsonList2["mid"]
            self.bid = self.jsonList2["bid"]
            self.ask = self.jsonLis2t[x]["ask"]
            self.last_price = self.jsonList2["last_price"]
            self.low = self.jsonList2["low"]
            self.high = self.jsonList2["high"]
            self.volume = self.jsonList2["volume"]
            self.timestamp = self.jsonList2["timestamp"]
            
            self.saveIntoDB()

    def saveIntoDB(self): 
        ##print "saveIntoDB -- Bitfinex"           
        self.db.addBitfinex(self.marketname,self.mid,self.bid,self.ask,self.last_price,self.low,self.high,self.volume,self.timestamp,self.fetchTime)

    def deleteFromDB_fetchTime(self):
        #print "deleteFromDB_fetchTime -- Bitfinex"
        self.db.deleteFromDB_fetchTime("bitfinex",self.delTillFetchTime)
    
    def start(self,sleepTime):
        #print "Start method -- Bitfinex"
        self.sleepTime = sleepTime
        try:
            while True:
                try:
                    self.setFetchTime()
                    self.fetchData()
                    self.setDelTillFetchTime()
                    self.deleteFromDB_fetchTime()
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print(e)
                    self.sleepTime = 2 * self.sleepTime
                    #print "exception caught in while loop -- Bitfinex"
                sleep(self.sleepTime)
        except Exception as e: 
            print(e)
            #print "Exception caught in start function -- Bitfinex"