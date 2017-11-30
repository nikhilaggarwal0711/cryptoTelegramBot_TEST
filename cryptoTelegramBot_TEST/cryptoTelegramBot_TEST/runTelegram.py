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
        self.db.setup()
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
        self.message = "I have added you in my notification list. \nFuture Upgrades : \n1. More Exchanges \n2. Provide Rank of newly added market \n3. Price Alerts \n4. Portfolio Tracker\n5. FREE Money/Coins alerts"
        
    def addBotMessageInDB(self):
        #print "Inside addBotMessageInDB -- Telegram"
        self.db.addBotMessage(self.chatId , self.firstName, self.category , self.offsetId, self.fetchTime, self.text)
        
    def handleUpdate(self):
        #print "Inside handleUpdate -- Telegram"
        if self.text == "/start" or self.text == "start":
            self.message="I know its too long since any new market is added, but I am tracking and will keep you posted. Thanks for poking :) "
        else:
            self.message="I only understand START signal as of now. I am learning new stuffs. Thanks for poking :) "


    def sendTelegramMessage(self):
        ##print "Inside sendTelegramMessage -- Telegram"
        try:
            self.TelegramBot.sendMessage(self.chatId,self.message)
        except Exception as e: 
            print(e)
            
    def insertIntoBittrex_DB(self, marketName , fetchTime):
        self.db.insertIntoBittrex_DuplicateRow(marketName , fetchTime)
    
    def start(self,sleepTime):
        #print "Start method -- Telegram"
        self.sleepTime = sleepTime
        while True:
            try:
                #print "At beginning of while loop -- Telegram"
#               Send New Market Notification
                newMarketsBittrex = self.db.getNewListingsBittrex()
                newMarketsBitfinex = self.db.getNewListingsBitfinex()
                
                allUsers = self.db.getAllUsers()
                for user in allUsers:
                    market = "Bittrex"
                    for newMarket in newMarketsBittrex:
                        self.message = str(market) + "\nNew Market Added\nMarket Name : "+str(newMarket[0])+"\nVolume : "+str(newMarket[1])+"\nBid : "+str(newMarket[2])+"\nAsk : "+str(newMarket[3])+"\nOpen Buy Orders : "+str(newMarket[4])+"\nOpen Sell Orders : "+str(newMarket[5])
                        self.chatId = user[0]
                        self.sendTelegramMessage()

                    market = "Bitfinex"
                    for newMarket in newMarketsBitfinex:
                        self.message = str(market) + "\nNew Market Added\nMarket Name : "+str(newMarket[0])+"\nBid : "+str(newMarket[1])+"\nAsk : "+str(newMarket[2])+"\nLow : "+str(newMarket[3])+"\nHigh : "+str(newMarket[4])+"\nVolume : "+str(newMarket[5])
                        self.chatId = user[0]
                        #self.sendTelegramMessage()
                
                for newMarket in newMarketsBittrex:
                    self.insertIntoBittrex_DB(str(newMarket[0]) , str(newMarket[6]) )
                for newMarket in newMarketsBitfinex:
                    self.insertIntoBitfinex_DB(str(newMarket[0]) , str(newMarket[6]) )
                
                newMarketsBittrex=""
                allUsers=""
#                Reply to users
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

