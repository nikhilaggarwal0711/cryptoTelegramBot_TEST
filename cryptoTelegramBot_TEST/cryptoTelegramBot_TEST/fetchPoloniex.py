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
        self.data = requests.get(url = self.link1)
        #self.data = self.f1.text.replace("null","0")
        print "DATA --> " + str(self.data)
        #self.jsonList  = json.loads(self.data)
        #length = len(json.loads(self.data))
        #print "Length --> " + str(length)
        self.currencyList = map(str, self.data.keys())
        print "Currencies --> " + str(self.currencyList)
        length = len(self.currencyList)
        print "Length --> " + str(length)
        
        for x in range(0,length):
            print "Add data for loop  -- Poloniex"
            print "currencySymbol --> " + str(self.currencyList[x])
            self.currencySymbol = self.currencyList[x]
            print "id --> " + str(self.data[self.currencyList[x]]["id"])
            self.id = self.data[self.currencyList[x]]["id"]
            print "name --> " + str(self.data[self.currencyList[x]]["name"])
            self.name = self.data[self.currencyList[x]]["name"]
            print "disabled --> " + str(self.data[self.currencyList[x]]["disabled"])
            self.disabled = self.data[self.currencyList[x]]["disabled"]
            print "delisted --> " + str(self.data[self.currencyList[x]]["delisted"])
            self.delisted = self.data[self.currencyList[x]]["delisted"]
            print "frozen --> " + str(self.data[self.currencyList[x]]["frozen"])
            self.frozen = self.data[self.currencyList[x]]["frozen"]

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