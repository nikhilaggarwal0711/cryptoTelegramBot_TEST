from time import sleep
import time
import requests
import json
from dbhelper import DBHelper



class FetchCoinMarketCap:
    def __init__(self):
        #print "Inside fetchCoinmarketcap constructor"
        self.link1 = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
        #self.db = DBHelper()
        #self.db.setup()
    
    def setFetchTime(self):
        #print "setFetchTime"
        self.fetchTime = int(time.time())

    def setDelTillFetchTime(self):
        #print "delTillFetchTime"
        #2days older data.
        deleteTime = 172800
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
        #print "fetchData"
        self.f1 = requests.get(url = self.link1)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)

        for x in range(0,len(self.jsonList)):
            ##print "#print data for loop - coinmarketcap" 
            self.id = self.jsonList[x]["id"]
            self.name = self.jsonList[x]["name"]
            self.symbol = self.jsonList[x]["symbol"]
            self.rank = self.jsonList[x]["rank"]
            self.price_usd = self.jsonList[x]["price_usd"]
            self.price_btc = self.jsonList[x]["price_btc"]
            self.h24_volume_usd = self.jsonList[x]["24h_volume_usd"]
            self.market_cap_usd = self.jsonList[x]["market_cap_usd"]
            self.available_supply = self.jsonList[x]["available_supply"]
            self.total_supply = self.jsonList[x]["total_supply"]
            self.percent_change_1h = self.jsonList[x]["percent_change_1h"]
            self.percent_change_24h = self.jsonList[x]["percent_change_24h"]
            self.percent_change_7d = self.jsonList[x]["percent_change_7d"]
            self.last_updated = self.jsonList[x]["last_updated"]

            self.saveIntoDB()


    def saveIntoDB(self):   
        ##print "SaveIntoDB -- coinmarketcap"         
        self.db.addCoinMarketCap(self.id,self.name,self.symbol,self.rank,self.price_usd,self.price_btc,self.h24_volume_usd,self.market_cap_usd,self.available_supply,self.total_supply,self.percent_change_1h,self.percent_change_24h,self.percent_change_7d,self.last_updated,self.fetchTime)

    def deleteFromDB_BKPonFetchTime(self):
        #print "deleteFromDB_fetchTime"
        self.db.deleteFromDB_BKPonFetchTime("coinmarketcap",self.delTillFetchTime)

    def deleteFromDB_oldData(self):
        self.db.deleteFromDB_oldData("coinmarketcap")

    def start(self,sleepTime):
        #print "Start method"
        self.sleepTime = sleepTime
        try:
            while True:
                self.db = DBHelper()
                try:
                    self.setFetchTime()
                    self.fetchData()
                    self.deleteFromDB_oldData()
                    self.setDelTillFetchTime()
                    self.deleteFromDB_BKPonFetchTime()
                except Exception as e: 
                    print(e)
                    self.sleepTime = 2 * self.sleepTime
                    #print "exception caught in while loop"   
                self.db.closeConnection()         
                sleep(self.sleepTime)

        except Exception as e: 
            print(e)
            #print "Exception caught in start function"
    
