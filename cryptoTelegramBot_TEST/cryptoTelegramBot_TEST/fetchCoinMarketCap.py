from time import sleep
import time
import requests
import json

class fetchCoinMarketCap:
    def __init__(self):
        self.link1 = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
    
    def setFetchTime(self):
        self.fetchTime = int(time.time())

    def delTillFetchTime(self):
        self.delTillFetchTime = self.fetchTime - 172800

    def fetchData(self):
        self.f1 = requests.get(url = self.link1)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)

        for x in range(0,len(self.jsonList)):
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
            
    def saveIntoDB(self):            
        self.db.addCoinMarketCap(self.id,self.name,self.symbol,self.rank,self.price_usd,self.price_btc,self.h24_volume_usd,self.market_cap_usd,self.available_supply,self.total_supply,self.percent_change_1h,self.percent_change_24h,self.percent_change_7d,self.last_updated,self.fetchTime)

    def deleteFromDB_fetchTime(self):
        self.db.deleteFromDB_fetchTime("coinmarketcap",self.delTillFetchTime())

    def start(self,sleepTime,db):
        self.sleepTime = sleepTime
        self.db = db
        try:
            while True:
                try:
                    self.setFetchTime()
                    self.fetchData()
                    self.saveIntoDB()
                    self.delTillFetchTime()
                    self.deleteFromDB_fetchTime()
                except:
                    print "exception caught in while loop"
                sleep(self.sleepTime)
        except:
            print "Exception caught in start function"
    