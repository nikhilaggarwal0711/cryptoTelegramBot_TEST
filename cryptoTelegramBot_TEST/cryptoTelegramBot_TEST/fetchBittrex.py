import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep
from config import Bittrex,Coinmarketcap,Denorms,COMMON

class FetchBittrex:
    def __init__(self):
        #print "inside Bittrex constructor"
        self.link1 = "https://bittrex.com/api/v1.1/public/getmarketsummaries"
        self.link2 = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
        self.db = DBHelper()
        #self.db.setup()

    def setFetchTime(self):
        #print "setFetchTime -- Bittrex"
        self.fetchTime = int(time.time())

    def fetchData_Bittrex(self):
        #print "fetchData -- Bittrex"
        self.f1 = requests.get(url = self.link1)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        length = len(json.loads(self.data)["result"])

        for x in range(0,length):
            ##print "Add data for loop  -- Bittrex"
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
                
            self.db.addBittrex(self.MarketName,self.High,self.Low,self.Volume,self.Last,self.BaseVolume,self.TimeStamp,self.Bid,self.Ask,self.OpenBuyOrders,self.OpenSellOrders,self.PrevDay,self.Created,self.fetchTime)    

    def fetchData_CoinMarketCap(self):
        #print "fetchData"
        self.f1 = requests.get(url = self.link2)
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

            self.db.addCoinMarketCap(self.id,self.name,self.symbol,self.rank,self.price_usd,self.price_btc,self.h24_volume_usd,self.market_cap_usd,self.available_supply,self.total_supply,self.percent_change_1h,self.percent_change_24h,self.percent_change_7d,self.last_updated,self.fetchTime)

        
    def start(self,sleepTime):
        #print "Start method -- Bittrex"
        self.sleepTime = sleepTime
        try:
            while True:
                try:
                    self.setFetchTime()
                    self.fetchData_Bittrex()
                    self.db.deleteFromDB_oldData("bittrex")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("bittrex",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e:
                    print(e)
                    self.sleepTime = 2 *  self.sleepTime
                    #print "exception caught in while loop -- Bittrex"
                    with open(COMMON.errorDir + Bittrex.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                #Fetch CoinMarketcap data.
                try:
                    self.setFetchTime()
                    self.fetchData_CoinMarketCap()
                    self.db.deleteFromDB_oldData("coinmarketcap")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("coinmarketcap",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print(e)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                #Creaete DENORM AND ALERT Tables
                try:
                    self.db.create_denorm_and_alerts()
                except Exception as e: 
                    print(e)
                    with open(COMMON.errorDir + Denorms.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                sleep(self.sleepTime)
        except Exception as e: 
            print(e)
            #print "Exception caught in start function -- Bittrex"