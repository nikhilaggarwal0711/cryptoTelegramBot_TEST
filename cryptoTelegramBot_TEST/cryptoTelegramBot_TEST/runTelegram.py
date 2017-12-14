import telepot
import urllib3
from config import TELEGRAM
from dbhelper import  DBHelper
from time import sleep
import time

class RunTelegram:
    def __init__(self):
        #print "Inside telegram constructor -- Telegram"
        self.TOKEN = TELEGRAM.token
        self.TelegramBot = telepot.Bot(self.TOKEN)
        self.db = DBHelper()
        #self.db.setup()
        self.category = "g"

    def setup_pythonAnyWhere(self):
        #print "Inside setup method -- Telegram" 
        proxy_url = TELEGRAM.proxy_url
        telepot.api._pools = {
                'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=60),
        }
        telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=60))

    def setFetchTime(self):
        #print "setFetchTime -- Telegram"
        self.fetchTime = int(time.time())
        
    def getLastOffset(self):
        #print "Inside getLastOffset -- Telegram"
        self.lastOffset = self.db.getLastOffset()

    def getUpdates(self):
        #print "Inside getUpdates -- Telegram"
        self.updates = self.TelegramBot.getUpdates(offset=int(self.lastOffset)+1,timeout=60)

    def fetchData(self,update):
        #print "Inside fetchData -- Telegram"
        self.text = update["message"]["text"]
        self.chatId = update["message"]["from"]["id"]
        self.offsetId = update["update_id"]
        self.firstName = update["message"]["from"]["first_name"]

    #Following function will return either new or old
    def checkUser(self):
        #print "Inside checkUser -- Telegram"
        self.user = self.db.checkUser(self.chatId)

    def newUser(self):
        #print "Inside newUser -- Telegram"
        self.message = "I have added you in my notification list. \nFuture Upgrades : \n1. More Exchanges \n2. Provide Rank of newly added market \n3. Price Alerts \n4. Portfolio Tracker\n5. FREE Money/Coins alerts\n6. Real time price\n7.DoubleTrouble Game - To 100 BTC from 0.1 BTC"
        
    def addBotMessageInDB(self):
        #print "Inside addBotMessageInDB -- Telegram"
        self.db.addBotMessage(self.chatId , self.firstName, self.category , self.offsetId, self.fetchTime, self.text)
        
    def handleUpdate(self):
        #print "Inside handleUpdate -- Telegram"
        self.textArray = self.text.split()

        if self.textArray[0] == "/start" or self.textArray[0] == "start":
            self.message="I know its too long since any new market is added, but I am tracking and will keep you posted. Thanks for poking :) "
        elif self.textArray[0] == "/check_tweet":
            if len(self.textArray) != 2 :
                self.message="Please provide information in following format : \n/check_tweet ETH" 
            else:
                currencySymbol = self.textArray[1]
                tweets = self.db.fetchTweet(currencySymbol)
                #name,id,tweet_id
                if self.is_empty(tweets):
                    self.message = "No currency with symbol - " + currencySymbol + " found. Please provide correct currency symbol."
                else:
                    print tweets
                    #name,twitter_screen_name,tweet_id
                    for tweet in tweets:
                        if self.is_empty(tweet[1]):
                            self.message = "We don't have Official Twitter Handler for this currency."                        
                        elif self.is_empty(tweet[2]):
                            self.message = "We havn't captured any tweet for this coin. Please try again after some time."
                        else:
                            self.message = "<a href='https://twitter.com/"+tweet[1]+"/status/"+tweet[2]+"'>"+ str(currencySymbol).upper() +"("+tweet[0]+") tweeted : </a>"
        elif self.textArray[0] == "/check_price":
            if len(self.textArray) != 2 :
                self.message="Please provide information in following format : \n/check_price ETH"
            else:
                currencySymbol = self.textArray[1]
                prices = self.db.fetchPrice(currencySymbol)
                #name,exchange,exchange_price,exchange_price_in,cmc_price_usd
                if prices is None :
                    self.message = "No currency with symbol - " + currencySymbol + " found. Please provide correct currency symbol."
                else:
                    self.message = ""
                    for price in prices:
                        name=str(price[0])
                        exchange=str(price[1])
                        exchange_price=str(price[2])
                        exchange_price_in = str(price[3])
                        cmc_price_usd = str(price[4])
                        self.message = self.message + "Price of " + currencySymbol + "( " + name + " ) :\nExchange : " + exchange +"\n  Price : " + exchange_price + " " + exchange_price_in + " or $" + cmc_price_usd + "\n\n"
        elif self.textArray[0] == "/set_alert_tweet":
            if len(self.textArray) != 2 :
                self.message="Please provide information in following format : \n/check_price ETH"
            else:
                currencySymbol = self.textArray[1]
                self.db.add_alert(self.chatId,"tweet",self.fetchTime,currencySymbol,"yes",0,"btc")
                self.message = "We have taken your interest if you provided correct input, you can find your Alerts in 'MyAlerts'"
        elif self.textArray[0] == "/set_alert_price_incr":
            if len(self.textArray) == 1:            
                self.message="Please provide information in following format : \n/set_alert_price_incr ETH 0.001 BTC"
            else:
                currencySymbol = str(self.textArray[1]).lower()
                price_alert = self.textArray[2]
                price_in = self.textArray[3]

            if currencySymbol is None:
                self.message="Please provide information in following format : \n/set_alert_price_incr ETH 0.0001 BTC"
            elif price_alert is None:
                self.message="Please provide information in following format : \n/set_alert_price_incr ETH 0.001 BTC"
            elif price_in is None:
                self.message="Please provide information in following format : \n/set_alert_price_incr ETH 0.001 BTC"
            elif price_in not in ("btc","usd","eth","satoshi","stats","sats"):
                self.message="Please provide information in following format : \n/set_alert_price_incr ETH 0.001 BTC"                
            else: 
                if price_in in ("satoshi","stats","sats"):
                    price_alert = price_alert/1000000000
                    price_in = "btc"
                self.db.add_alert(self.chatId,"p_incr",self.fetchTime,currencySymbol,"yes",price_alert,price_in)
                self.message = "We have taken your interest if you provided correct input, you can find your Alerts in 'MyAlerts'"
        elif self.textArray[0] == "/set_alert_price_decr":
            currencySymbol = self.textArray[1]
            price_alert = self.textArray[2]
            price_in = self.textArray[3]
            
            if currencySymbol is None:
                self.message="Please provide information in following format : \n/check_price ETH 0.0001 BTC"
            elif price_alert is None:
                self.message="Please provide information in following format : \n/check_price 0.08 BTC"
            elif price_in is None:
                self.message="Please provide information in following format : \n/check_price 0.08 BTC"
            elif price_in not in ("btc","usd","eth","satoshi","stats","sats"):
                self.message="Please provide information in following format : \n/check_price 0.08 BTC"                
            else: 
                if price_in in ("satoshi","stats","sats"):
                    price_alert = price_alert/1000000000
                    price_in = "btc"
                self.db.add_alert(self.chatId,"p_decr",self.fetchTime,currencySymbol,"yes",price_alert,price_in)
                self.message = "We have taken your interest if you provided correct input, you can find your Alerts in 'MyAlerts'"
        elif self.textArray[0] == "/my_alerts":
            try:
                self.message=""
                alerts = self.db.my_alerts(str(self.chatId))
                #id,chatId,alert_type,coin_symbol,alert_price,price_in
                if not self.is_empty(alerts):
                    for alert in alerts:
                        if alert[2] == "tweet":
                            self.message = self.message + "Tweet alert : "+ alert[3] + "\n To delete --> /del_alert__" + alert[0] + "\n\n"
                        elif alert[2] == "p_incr":
                            self.message = self.message + "Price Increase alert : "+ alert[3] + " " + alert[4] + " " + alert[5] + "\n To delete --> /del_alert__" + alert[0] + "\n\n"
                        elif alert[2] == "p_decr":
                            self.message = self.message + "Price Decrease alert : "+ alert[3] + " " + alert[4] + " " + alert[5] + "\n To delete --> /del_alert__" + alert[0] + "\n\n"
                else:
                    self.message = "There are no alerts set for you."
            except Exception as e: 
                print(e)  
        elif  self.textArray[0].split("__")[0] == "/del_alert":  
            alert_id = self.textArray[0].split("__")[1]
            try:
                self.db.delete_alert(self.chatId,alert_id)     
            except Exception as e: 
                print(e)
        elif self.textArray[0] == "/feedback":
            self.message = "Every feedback is important for us. Thank you for taking your time and writing to us."
        elif self.textArray[0] == "/suggest_2x_coin":
            coin = self.textArray[1]
            explanation = self.textArray[2]
#            self.db.add_2xCoin_suggestion(self.chatId,self.fetchTime,coin,explanation)
            self.message = "We will look into your suggestion. If selected and gave 2x return within a month, you will get your share for sharing this coin."
        else : 
            self.message = "Please select proper command (starts from / )."

    def sendTelegramMessage(self):
        ##print "Inside sendTelegramMessage -- Telegram"
        try:
            self.TelegramBot.sendMessage(self.chatId,self.message)
        except Exception as e: 
            print(e)
            
    def insertIntoBittrex_DB(self, marketName , fetchTime):
        self.db.insertIntoBittrex_DuplicateRow(marketName , fetchTime)
            
    def insertIntoBitfinex_DB(self, marketName , fetchTime):
        self.db.insertIntoBitfinex_DuplicateRow(marketName , fetchTime)
            
    def insertIntoPoloniex_DB(self, currencySymbol , fetchTime):
        self.db.insertIntoPoloniex_DuplicateRow(currencySymbol , fetchTime)

    def is_empty(self,any_structure):
        if any_structure:
            print('Structure is not empty.')
            return False
        else:
            print('Structure is empty.')
            return True
    
    def start(self,sleepTime):
        #print "Start method -- Telegram"
        self.sleepTime = sleepTime
        while True:
            try:
#               Send New Market Notification
                newMarkets = self.db.get_newMarketListings()
                
                allUsers = self.db.getAllUsers()

                if not self.is_empty(newMarkets) :
                    self.message = "New Market Added"
                    for market in newMarkets:
                        rank = market[0]
                        symbol = market[1]
                        name = market[2]
                        exchange = market[3]
                        marketname = market[4]
                        exchange_last_price = market[5]
                        cmc_price_usd = market[6]
    
                        self.message = self.message + "\n\nExchange : " + exchange + "\nMarket Name : "+marketname +"\nSymbol : "+symbol+"\nName : "+name+"\nRank : "+ rank +"\nLast Price : "+exchange_last_price+"\nActual Price in USD : " + cmc_price_usd

                    for user in allUsers:
                        self.chatId = user[0]
                        self.sendTelegramMessage()

#Send alerts 
                for user in allUsers:
                    alerts = self.db.getAlerts()
                    if not self.is_empty(alerts):
                        for alert in alerts:
                            alert_id = alert[0]
                            self.chatId = alert[1]
                            alert_type = alert[2]
                            coin_symbol = alert[3]
                            alert_price = alert[4]
                            price_in = alert[5]
                            twitter_screen_name = alert[6]
                            tweet_id = alert[7]
                            coin_name = alert[8]
                            exchange = alert[9]
                            new_price = alert[10]

                            if alert_type == "tweet":
                                self.message = "<a href='https://twitter.com/"+twitter_screen_name+"/status/"+tweet_id+"'>"+coin_symbol +"("+coin_name+") tweeted : </a>"
                            #if alert_type == "p_incr":
    #                            self.message = ""
                            
                            self.db.delete_alert(self.chatId, alert_id)
                            self.sendTelegramMessage()

                newMarkets=""
                allUsers=""

                self.setFetchTime()
                self.getLastOffset()
                self.getUpdates()
                
                for update in self.updates:
                    try:
                        self.fetchData(update)
                        self.checkUser()
                        if self.user == 'new':
                            self.newUser()
                        else:
                            self.handleUpdate()
                        self.addBotMessageInDB()
                        self.sendTelegramMessage()
                    except Exception as e: 
                        print(e)
                        #print "Exception caught inside for loop"
            except Exception as e: 
                print(e)
                #print "Exception Caught"
            #sleep(self.sleepTime)
            sleep(0.5)

