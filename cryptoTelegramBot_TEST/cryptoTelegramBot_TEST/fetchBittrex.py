import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep

class FetchBittrex:
    def __init__(self):
        #print "inside Bittrex constructor"
        self.link1 = "https://bittrex.com/api/v1.1/public/getmarketsummaries"
        self.db = DBHelper()
        #self.db.setup()
        
    def setFetchTime(self):
        #print "setFetchTime -- Bittrex"
        self.fetchTime = int(time.time())

    def setDelTillFetchTime(self):
        #print "delTillFetchTime -- Bittrex"
        #2days older data.
        deleteTime = 172800
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
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
                
            self.saveIntoDB()

    def saveIntoDB(self): 
        ##print "saveIntoDB -- Bittrex"   
        self.db.addBittrex(self.MarketName,self.High,self.Low,self.Volume,self.Last,self.BaseVolume,self.TimeStamp,self.Bid,self.Ask,self.OpenBuyOrders,self.OpenSellOrders,self.PrevDay,self.Created,self.fetchTime)
    
    def deleteFromDB_BKPonFetchTime(self):
        #print "deleteFromDB_fetchTime -- Bittrex"
        self.db.deleteFromDB_BKPonFetchTime("bittrex",self.delTillFetchTime)
    
    def deleteFromDB_oldData(self):
        self.db.deleteFromDB_oldData("bittrex")
        
    def start(self,sleepTime):
        #print "Start method -- Bittrex"
        self.sleepTime = sleepTime
        try:
            while True:
                try:
                    self.setFetchTime()
                    self.fetchData()
                    self.deleteFromDB_oldData()
                    self.setDelTillFetchTime()
                    self.deleteFromDB_BKPonFetchTime()
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print(e)
                    self.sleepTime = 2 *  self.sleepTime
                    #print "exception caught in while loop -- Bittrex"

                #Creaete DENORM AND ALERT Tables
                try:
                    self.db.create_denorm_and_alerts()
                except Exception as e: 
                    print(e)
                    
                sleep(self.sleepTime)
        except Exception as e: 
            print(e)
            #print "Exception caught in start function -- Bittrex"