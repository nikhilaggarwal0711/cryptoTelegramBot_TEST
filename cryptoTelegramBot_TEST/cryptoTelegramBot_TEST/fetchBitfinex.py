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
        #print "url set -- Bitfinex"
        self.data1 = self.f1.text.replace("null","0")
        #print "replace null -- Bitfinex"
        self.jsonList1  = json.loads(self.data1)
        #print "jsonlist1 -- bitfinex"
        length1 = len(json.loads(self.data1))
        #print "length1 -- bitfinex"
        
        for x in range(0,length1):
            try:
                #print "fetchData -- for loop -- Bitfinex"
                self.f2 = requests.get(url = self.symbolDetails_link + self.jsonList1[x])
                #print "url set 2 -- Bitfinex"
                self.data2 = self.f2.text.replace("null","0")
                #print "DATA -- bitfinex"
                #print self.data2 
                #print "replace null 2 -- Bitfinex"
                self.jsonList2  = json.loads(self.data2)
                #print "jsonlist2 -- bitfinex"
                
                #print "marketname --> " +  self.jsonList1[x]
                self.marketname = self.jsonList1[x]
                #print "mid --> " +  self.jsonList2["mid"]
                self.mid = self.jsonList2["mid"]
                self.bid = self.jsonList2["bid"]
                self.ask = self.jsonList2["ask"]
                self.last_price = self.jsonList2["last_price"]
                self.low = self.jsonList2["low"]
                self.high = self.jsonList2["high"]
                self.volume = self.jsonList2["volume"]
                self.timestamp = self.jsonList2["timestamp"]
                
                self.saveIntoDB()
                #Added delay of 1 second so as to avoid crossing ErrorLimit 
                sleep(2)
            except Exception as e: 
                    print(e)
                    #print "mid --> " + self.jsonList2["mid"]
                

    def saveIntoDB(self): 
        #print "saveIntoDB -- Bitfinex"           
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