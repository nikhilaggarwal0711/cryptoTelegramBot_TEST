import time
import requests
import json
from dbhelper import  DBHelper
from time import sleep
from config import Bittrex,Coinmarketcap,Denorms,COMMON,Binance,Twitter,Kucoin,Cryptopia

class FetchBittrex:
    def __init__(self):
        #print "inside Bittrex constructor"
        self.link1 = "https://bittrex.com/api/v1.1/public/getmarketsummaries"
        self.link2 = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
        self.link3 = "https://api.binance.com/api/v1/ticker/allPrices"
        self.link4 = "https://api.kucoin.com/v1/open/tick"
        self.link5 = "https://www.cryptopia.co.nz/api/GetMarkets"
        self.link6 = "https://api.idex.market/returnTicker"
        self.link7 = "https://api.hitbtc.com/api/2/public/ticker"
        self.link8  = "https://bitbns.com/order/getTickerWithVolume/"
        self.link9  = "https://www.zebapi.com/api/v1/market/"
        self.link10 = "https://koinex.in/api/ticker"
        self.link11 = "https://api.coindelta.com/api/v1/public/getticker/"
        self.link12 = "https://api.wazirx.com/api/v2/tickers"
        self.link13 = "https://api.unocoin.com/api/exchange/unodax-ticker"
        #self.db = DBHelper()
        #self.db.setup()

    def setFetchTime(self):
        #print "setFetchTime -- Bittrex"
        self.fetchTime = int(time.time())

    def fetchData_Bitbns(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link8)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        #length = len(self.jsonList)

        for elem in self.jsonList:
            ##print "For loop  -- BitBns"
            try:
                self.MarketName = elem.encode('utf-8')
                self.Price = self.jsonList[self.MarketName]["last_traded_price"]
                try:
                    self.volume = self.jsonList[self.MarketName]["volume"]["volume"]
                except Exception as e:
                    self.volume=0
                    #print "no volume"
                self.db.addBitbns(self.MarketName+"-INR",self.Price,self.volume,self.fetchTime)
            except Exception as e:
                print e.message

    def fetchData_Zebpay(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link9)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        #length = len(self.jsonList)

        for elem in self.jsonList:
            ##print "For loop  -- BitBns"
            try:
                self.MarketName = elem["pair"].encode('utf-8')
                self.Price = elem["buy"]
                try:
                    self.volume = elem["volume"]
                except Exception as e:
                    self.volume=0
                    #print "no volume"
                self.db.addZebpay(self.MarketName,self.Price,self.volume,self.fetchTime)
            except Exception as e:
                print e.message

    def fetchData_Koinex(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link10)
        self.data = self.f1.text.replace("null","0")
        jsonList  = json.loads(self.data)
        #length = len(self.jsonList)

        for elem in jsonList["stats"]:
            #print elem
            #print jsonList["stats"][elem]
            if elem == "bitcoin":
                market = "btc"
            elif elem == "ether":
                market = "eth"
            elif elem == "ripple":
                market = "xrp"
            elif elem == "inr":
                market = "inr"
            else:
                market = "NA"
            for coin in jsonList["stats"][elem]:
                #print coin
                try:
                    currency = jsonList["stats"][elem][coin]["currency_short_form"].encode('utf-8')
                    last_price = jsonList["stats"][elem][coin]["last_traded_price"]
                    volume = jsonList["stats"][elem][coin]["vol_24hrs"]
                    Marketname = currency + "-" + market
                    #print coin + "-" + market + " | " + last_price + " | " + volume
                    self.db.addKoinex(Marketname,last_price,volume,self.fetchTime)
                except Exception as e:
                    print e.message


    def fetchData_Coindelta(self):
        #print "fetchData -- Cryptopia"
        self.f1 = requests.get(url = self.link11)
        data = self.f1.text.replace("null","0")
        jsonList  = json.loads(data)
        length = len(jsonList)

        for x in range(0,length):
            ##print "Add data for loop  -- Cryptopia"
            try:
                MarketName = jsonList[x]["MarketName"].encode('utf-8')
                last_price = jsonList[x]["Last"]
                self.db.addCoindelta(MarketName,last_price,self.fetchTime)    
            except Exception as e:
                print e.message

    def fetchData_Wazirx(self):
        #print "fetchData -- Cryptopia"
        self.f1 = requests.get(url = self.link12)
        data = self.f1.text.replace("null","0")
        jsonList  = json.loads(data)
        #length = len(jsonList)

        for elem in jsonList:
            ##print "Add data for loop  -- Cryptopia"
            try:
                MarketName = jsonList[elem]["name"].encode('utf-8')
                last_price = jsonList[elem]["last"]
                volume = jsonList[elem]["volume"]
                #print MarketName + " | " + last_price + " - " + volume
                self.db.addWazirx(MarketName,last_price,volume,self.fetchTime)    
            except Exception as e:
                print e.message

    def fetchData_Unocoin(self):
        #print "fetchData -- Cryptopia"
        self.f1 = requests.get(url = self.link13)
        data = self.f1.text.replace("null","0")
        jsonList  = json.loads(data)
        #length = len(jsonList)

        for elem in jsonList["stats"]:
            ##print "Add data for loop  -- Cryptopia"
            market = elem
            for coin in jsonList["stats"][elem]:
                #print coin
                try:
                    currency = jsonList["stats"][elem][coin]["currency_short_form"]
                    last_price = jsonList["stats"][elem][coin]["last_traded_price"]
                    volume = jsonList["stats"][elem][coin]["vol_24hrs"]
                    Marketname = currency + "-" + market
                    #print coin + "-" + Marketname + " | " + last_price + " | " + volume
                    self.db.addUnocoin(Marketname,last_price,volume,self.fetchTime)
                except Exception as e : 
                    print e.message

    def fetchData_Cryptopia(self):
        #print "fetchData -- Cryptopia"
        self.f1 = requests.get(url = self.link5)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        length = len(json.loads(self.data)["Data"])

        for x in range(0,length):
            ##print "Add data for loop  -- Cryptopia"
            try:
                self.marketname = self.jsonList["Data"][x]["Label"].encode('utf-8')
                self.last_price = self.jsonList["Data"][x]["LastPrice"]
                #print "Inserting Data"
                self.db.addCryptopia(self.marketname,self.last_price,self.fetchTime)    
            except Exception as e:
                print e.message

    def fetchData_Kucoin(self):
        #print "fetchData -- Kucoin"
        self.f1 = requests.get(url = self.link4)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        length = len(json.loads(self.data)["data"])

        for x in range(0,length):
            ##print "Add data for loop  -- Kucoin"
            try:
                self.coinType = self.jsonList["data"][x]["coinType"].encode('utf-8')
                self.trading = self.jsonList["data"][x]["trading"]
                #print self.trading
                self.symbol = self.jsonList["data"][x]["symbol"].encode('utf-8')
                self.lastDealPrice = self.jsonList["data"][x]["lastDealPrice"]
                #self.buy = self.jsonList["data"][x]["buy"]
                #self.sell = self.jsonList["data"][x]["sell"]
                #self.change = self.jsonList["data"][x]["change"]
                #self.coinTypePair = self.jsonList["data"][x]["coinTypePair"].encode('utf-8')
                #self.sort = self.jsonList["data"][x]["sort"]
                #self.feeRate = self.jsonList["data"][x]["feeRate"]
                #self.volValue = self.jsonList["data"][x]["volValue"]
                #self.high = self.jsonList["data"][x]["high"]
                #self.datetime = self.jsonList["data"][x]["datetime"]
                #self.vol = self.jsonList["data"][x]["vol"]
                #self.low = self.jsonList["data"][x]["low"]
                #self.changeRate = self.jsonList["data"][x]["changeRate"]
    
                if self.trading == True:
                    #print "Inserting Data"
                    self.db.addKucoin(self.coinType,self.symbol,self.lastDealPrice,self.fetchTime)    
            except Exception as e:
                print e.message
 
    def fetchData_Binance(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link3)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        length = len(json.loads(self.data))

        for x in range(0,length):
            ##print "Add data for loop  -- Binance"
            #print self.jsonList[x]["symbol"].encode('utf-8')
            self.MarketName = self.jsonList[x]["symbol"].encode('utf-8')
            #print self.jsonList[x]["price"]
            self.Price = self.jsonList[x]["price"]

            self.db.addBinance(self.MarketName,self.Price,self.fetchTime)    


    def fetchData_Idex(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link6)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        #length = len(self.jsonList)

        for elem in self.jsonList:
            ##print "For loop  -- Idex"
            #print elem.encode('utf-8')
            self.MarketName = elem.encode('utf-8')
            #print self.jsonList[self.MarketName]["last"].encode('utf-8')
            self.Price = self.jsonList[self.MarketName]["last"].encode('utf-8')
            if self.Price == "N/A":
                self.Price = 0
            self.db.addIdex(self.MarketName,self.Price,self.fetchTime)

    def fetchData_Hitbtc(self):
        #print "fetchData -- Binance"
        self.f1 = requests.get(url = self.link7)
        self.data = self.f1.text.replace("null","0")
        self.jsonList  = json.loads(self.data)
        length = len(self.jsonList)

        for x in range(0,length):
            ##print "Add data for loop  -- Binance"
            #print self.jsonList[x]["symbol"].encode('utf-8')
            self.MarketName = self.jsonList[x]["symbol"].encode('utf-8')
            #print self.jsonList[x]["price"]
            self.last_price = self.jsonList[x]["last"]

            self.db.addHitbtc(self.MarketName,self.last_price,self.fetchTime) 


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
                self.db = DBHelper()
                try:
                    self.setFetchTime()
                    self.fetchData_Bittrex()
                    self.db.deleteFromDB_oldData("bittrex")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("bittrex",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e:
                    print "exception caught in while loop -- Bittrex"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 *  self.sleepTime
                    with open(COMMON.errorDir + Bittrex.errorFileName,'a+') as f:
                        f.write("\n\nBittrex Error : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                #Fetch Binance data.
                try:
                    self.setFetchTime()
                    self.fetchData_Binance()
                    self.db.deleteFromDB_oldData("binance")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("binance",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e:
                    print "exception caught in while loop -- Binance"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Binance.errorFileName,'a+') as f:
                        f.write("\n\nBinance Error : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                #Fetch Cryptopia data.
                try:
                    self.setFetchTime()
                    self.fetchData_Cryptopia()
                    self.db.deleteFromDB_oldData("cryptopia")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("cryptopia",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e:
                    print "exception caught in while loop -- Cryptopia"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Cryptopia.errorFileName,'a+') as f:
                        f.write("\n\nCryptopia Error : ")
                        f.write(e.__doc__)
                        f.write(e.message)

                #Fetch Kucoin data.
                try:
                    self.setFetchTime()
                    self.fetchData_Kucoin()
                    self.db.deleteFromDB_oldData("kucoin")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("kucoin",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e:
                    print "exception caught in while loop -- Kucoin"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Kucoin.errorFileName,'a+') as f:
                        f.write("\n\nKucoin Error : ")
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
                    print "exception caught in while loop -- coinmarketcap"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch IDEX data.
                try:
                    self.setFetchTime()
                    self.fetchData_Idex()
                    self.db.deleteFromDB_oldData("idex")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("idex",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- idex"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch HitBtc data.
                try:
                    self.setFetchTime()
                    self.fetchData_Hitbtc()
                    self.db.deleteFromDB_oldData("hitbtc")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("hitbtc",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- hitbtc"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch BitBns data.
                try:
                    self.setFetchTime()
                    self.fetchData_Bitbns()
                    self.db.deleteFromDB_oldData("bitbns")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("bitbns",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- bitbns"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch Zebpay data.
                try:
                    self.setFetchTime()
                    self.fetchData_Zebpay()
                    self.db.deleteFromDB_oldData("zebpay")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("zebpay",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- zebpay"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch Koinex data.
                try:
                    self.setFetchTime()
                    self.fetchData_Koinex()
                    self.db.deleteFromDB_oldData("koinex")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("koinex",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- koinex"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch CoinDelta data.
                try:
                    self.setFetchTime()
                    self.fetchData_Coindelta()
                    self.db.deleteFromDB_oldData("coindelta")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("coindelta",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- coindelta"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch WazirX data.
                try:
                    self.setFetchTime()
                    self.fetchData_Wazirx()
                    self.db.deleteFromDB_oldData("wazirx")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("wazirx",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- wazirx"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Fetch Unocoin data.
                try:
                    self.setFetchTime()
                    self.fetchData_Unocoin()
                    self.db.deleteFromDB_oldData("unocoin")
                    deleteTime = 172800
                    self.delTillFetchTime = self.fetchTime - deleteTime
                    self.db.deleteFromDB_BKPonFetchTime("unocoin",self.delTillFetchTime)
                    self.sleepTime = sleepTime
                except Exception as e: 
                    print "exception caught in while loop -- unocoin"
                    print(e)
                    print(e.message)
                    self.sleepTime = 2 * self.sleepTime
                    with open(COMMON.errorDir + Coinmarketcap.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)


                #Populate tweets to final denorm
                try:
                    #print "Insert denorm -- Tweets"
                    self.db.deleteFromDB_oldData("tweets")
                except Exception as e: 
                    print "exception caught in while loop -- tweet denorm"
                    print(e)
                    print(e.message)
                    with open(COMMON.errorDir + Twitter.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)
                        
                                        
                #Creaete DENORM AND ALERT Tables
                try:
                    #print "Creating denorm"
                    self.db.create_denorm_and_alerts()
                except Exception as e: 
                    print "exception caught in while loop -- denorms"
                    print(e)
                    print(e.message)
                    with open(COMMON.errorDir + Denorms.errorFileName,'a+') as f:
                        f.write("\n\nError : ")
                        f.write(e.__doc__)
                        f.write(e.message)
                
                self.db.closeConnection()
                sleep(self.sleepTime)
        except Exception as e: 
            print "Exception caught in start function -- fetchBittrex"
            print(e)
            print(e.message)
