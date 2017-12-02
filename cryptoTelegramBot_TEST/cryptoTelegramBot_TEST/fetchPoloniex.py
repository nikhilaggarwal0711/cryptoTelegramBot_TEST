import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep

class FetchPoloniex:
    def __init__(self):
        print "inside Poloniex constructor"
        self.link1 = "https://poloniex.com/public?command=returnCurrencies"
        self.db = DBHelper()
        self.db.setup()

    def setFetchTime(self):
        print "setFetchTime -- Poloniex"
        self.fetchTime = int(time.time())

    def setDelTillFetchTime(self):
        print "delTillFetchTime -- Poloniex"
        #2days older data.
        deleteTime = 172800
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def fetchData(self):
        print "fetchData -- Poloniex"
        self.f1 = requests.get(url = self.link1)
        self.data = self.f1.text.replace("null","0")
        print "DATA --> " + self.data
        self.jsonList  = json.loads(self.data)
        length = len(json.loads(self.data))
        print "Length --> " + str(length)
        
        for x in range(0,length):
            print "Add data for loop  -- Poloniex"
            print "currencySymbol --> " + self.jsonList[x]
            self.currencySymbol = self.jsonList[x]
            print "id --> " + self.jsonList[x]["id"]
            self.id = self.jsonList[x]["id"]
            print "name --> " + self.jsonList[x]["name"]
            self.name = self.jsonList[x]["name"]
            print "disabled --> " + self.jsonList[x]["disabled"]
            self.disabled = self.jsonList[x]["disabled"]
            print "delisted --> " + self.jsonList[x]["delisted"]
            self.delisted = self.jsonList[x]["delisted"]
            print "frozen --> " + self.jsonList[x]["frozen"]
            self.frozen = self.jsonList[x]["frozen"]

            self.saveIntoDB()

    def saveIntoDB(self): 
        print "saveIntoDB -- Poloniex"           
        self.db.addPoloniex(self.currencySymbol,self.id,self.name,self.disabled,self.delisted,self.frozen,self.fetchTime)

    def deleteFromDB_fetchTime(self):
        print "deleteFromDB_fetchTime -- Poloniex"
        self.db.deleteFromDB_fetchTime("Poloniex",self.delTillFetchTime)

    def start(self,sleepTime):
        print "Start method -- Poloniex"
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
                    print "exception caught in while loop -- Poloniex"
                sleep(self.sleepTime)
        except Exception as e: 
            print(e)
            print "Exception caught in start function -- Poloniex"