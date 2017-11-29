import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep

class fetchBittrex:
    def __init__(self):
        print "inside Bittrex constructor"
        self.link1 = "https://bittrex.com/api/v1.1/public/getmarketsummaries"

    def setFetchTime(self):
        print "setFetchTime -- Bittrex"
        self.fetchTime = int(time.time())

    def delTillFetchTime(self):
        print "delTillFetchTime -- Bittrex"
#        deleteTime = 172800
        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
        print "fetchData -- Bittrex"
        self.f1 = requests.get(url = self.link1)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)

        for x in range(0,len(self.jsonList)):
            print "Add data for loop  -- Bittrex"
            self.MarketName = self.jsonList["result"][x]["MarketName"].encode('utf-8')
            self.High = self.jsonList["result"][x]["High"]
            self.Low = self.jsonList["result"][x]["Low"]
            self.Volume = self.jsonList["result"][x]["Volume"]
            self.Last = self.jsonList["result"][x]["Last"]
            self.BaseVolume = self.jsonList["result"][x]["BaseVolume"]
            self.TimeStamp = self.jsonList["result"][x]["TimeStamp"].encode('utf-8')
            self.Bid = self.jsonList["result"][x]["Bid"]
            self.Ask = self.jsonList["result"][x]["Ask"]
            self.OpenBuyOrders = self.jsonList["result"][x]["OpenBuyOrders"]
            self.OpenSellOrders = self.jsonList["result"][x]["OpenSellOrders"]
            self.PrevDay = self.jsonList["result"][x]["PrevDay"]
            self.Created = self.jsonList["result"][x]["Created"].encode('utf-8')
                
            self.saveIntoDB()
            
    def saveIntoDB(self): 
        print "saveIntoDB -- Bittrex"           
        self.db.addBittrex(self.id,self.name,self.symbol,self.rank,self.price_usd,self.price_btc,self.h24_volume_usd,self.market_cap_usd,self.available_supply,self.total_supply,self.percent_change_1h,self.percent_change_24h,self.percent_change_7d,self.last_updated,self.fetchTime)

    def deleteFromDB_fetchTime(self):
        print "deleteFromDB_fetchTime -- Bittrex"
        self.db.deleteFromDB_fetchTime("bittrex",self.delTillFetchTime)
    
    def start(self,sleepTime,db):
        print "Start method -- Bittrex"
        self.sleepTime = sleepTime
        self.db = db
        try:
            while True:
                try:
                    self.setFetchTime()
                    self.fetchData()
                    self.setDelTillFetchTime()
                    self.deleteFromDB_fetchTime()
                except:
                    print "exception caught in while loop -- Bittrex"
                sleep(self.sleepTime)
        except:
            print "Exception caught in start function -- Bittrex"