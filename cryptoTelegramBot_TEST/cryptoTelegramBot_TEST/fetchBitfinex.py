import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep
from config import Bitfinex,COMMON

class FetchBitfinex:
    def __init__(self):
        print "inside Bitfinex constructor"
        self.allSymbols_link = "https://api.bitfinex.com/v1/symbols"
        self.symbolDetails_link = "https://api.bitfinex.com/v1/pubticker/"
        
        self.db = DBHelper()
        #self.db.setup()
        
    def setFetchTime(self):
        print "setFetchTime -- Bitfinex"
        self.fetchTime = int(time.time())

    def setDelTillFetchTime(self):
        print "delTillFetchTime -- Bitfinex"
        #2days older data.
        deleteTime = 172800
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
        print "fetchData -- Bitfinex"
        self.f1 = requests.get(url = self.allSymbols_link)
        print "url set -- Bitfinex"
        self.data1 = self.f1.text.replace("null","0")
        print "replace null -- Bitfinex"
        self.jsonList1  = json.loads(self.data1)
        print "jsonlist1 -- bitfinex"
        length1 = len(json.loads(self.data1))
        print "length1 -- bitfinex"
        
        for x in range(0,length1):
            try:
                print "fetchData -- for loop -- Bitfinex"
                self.f2 = requests.get(url = self.symbolDetails_link + self.jsonList1[x])
                print "url set 2 -- Bitfinex"
                self.data2 = self.f2.text.replace("null","0")
                print "DATA -- bitfinex"
                print self.data2 
                print "replace null 2 -- Bitfinex"
                self.jsonList2  = json.loads(self.data2)
                print "jsonlist2 -- bitfinex"
                
                print "marketname --> " +  self.jsonList1[x]
                self.marketname = self.jsonList1[x]
                print "mid --> " +  self.jsonList2["mid"]
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
                sleep(12)
            except Exception as e: 
                print "Exception Caught : fetchData -- Bitfinex"
                print(e)
                print e.message
                print "mid --> " + self.jsonList2["mid"]

    def saveIntoDB(self): 
        print "saveIntoDB -- Bitfinex"           
        self.db.addBitfinex(self.marketname,self.mid,self.bid,self.ask,self.last_price,self.low,self.high,self.volume,self.timestamp,self.fetchTime)


    def deleteFromDB_BKPonFetchTime(self):
        print "deleteFromDB_fetchTime -- Bitfinex"
        self.db.deleteFromDB_BKPonFetchTime("bitfinex",self.delTillFetchTime)
    
    def deleteFromDB_oldData(self):
        self.db.deleteFromDB_oldData("bitfinex")

    def start(self,sleepTime):
        print "Start method -- Bitfinex"
        self.sleepTime = sleepTime
        try:
            while True:
                try:
                    print "inserting again."
                    self.setFetchTime()
                    self.fetchData()
                    self.deleteFromDB_oldData()
                    self.setDelTillFetchTime()
                    self.deleteFromDB_BKPonFetchTime()
                    self.sleepTime = sleepTime
                except Exception as e: 
                    self.sleepTime = 2 * self.sleepTime
                    print "exception caught in while loop -- Bitfinex"
                    print(e.message)
                    print(e)
                    with open(COMMON.errorDir + Bitfinex.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)
                sleep(self.sleepTime)
        except Exception as e: 
            print "Exception caught in start function -- Bitfinex"
            print(e)
            print(e.message)