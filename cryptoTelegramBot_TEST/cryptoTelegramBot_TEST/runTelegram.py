import telepot
import urllib3
from config import TELEGRAM
from dbhelper import  DBHelper
from time import sleep

class RunTelegram:
    def __init__(self):
        print "Inside telegram constructor -- Telegram"
        self.TOKEN = TELEGRAM.token
        self.TelegramBot = telepot.Bot(self.TOKEN)
        self.db = DBHelper()
        self.db.setup()
        self.category = "g"

    def setup_pythonAnyWhere(self):
        print "Inside setup method -- Telegram" 
        proxy_url = TELEGRAM.proxy_url
        telepot.api._pools = {
                'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=60),
        }
        telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=60))
        
    def getLastOffset(self):
        print "Inside getLastOffset -- Telegram"
        self.lastOffset = self.db.getLastOffset()

    def getUpdates(self):
        print "Inside getUpdates -- Telegram"
        self.updates = self.TelegramBot.getUpdates(int(self.lastOffset)+1)

    def fetchData(self,update):
        print "Inside fetchData -- Telegram"
        self.text = update["message"]["text"]
        self.chatId = update["message"]["from"]["id"]
        self.offsetId = update["update_id"]
        self.firstName = update["message"]["from"]["first_name"]

    #Following function will return either new or old
    def checkUser(self):
        print "Inside checkUser -- Telegram"
        self.user = self.db.checkUser(self.chatId)

    def newUser(self):
        print "Inside newUser -- Telegram"
        self.message = "I have added you in my notification list. \nFuture Upgrades : \n1. More Exchanges \n2. Provide Rank of newly added market \n3. Price Alerts \n4. Portfolio Tracker\n5. FREE Money/Coins alerts"
        
    def addBotMessage(self):
        print "Inside addBotMessage -- Telegram"
        self.db.addBotMessage(self.lastOffset,self.chatId , self.firstName, "g" , self.offsetId, self.fetchTime)
        
    def handleUpdate(self):
        print "Inside handleUpdate -- Telegram"
        if self.text == "/start" or self.text == "start":
            self.message="I know its too long since any new market is added, but I am tracking and will keep you posted. Thanks for poking :) "
        else:
            self.message="I only understand START signal as of now. I am learning new stuffs. Thanks for poking :) "


    def sendTelegramMessage(self):
        print "Inside sendTelegramMessage -- Telegram"
        self.TelegramBot.sendMessage(self.chatId,self.message)

    def start(self,sleepTime):
        print "Start method -- Telegram"
        self.sleepTime = sleepTime
        while True:
            try:
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
                        self.addBotMessage(self.chatId ,self.firstName, self.category , self.offsetId, self.fetchTime, self.text)
                        self.sendTelegramMessage()
                    except Exception as e: 
                        print(e)
                        print "Exception caught inside for loop"
            except Exception as e: 
                print(e)
                print "Exception Caught"
            sleep(0.5)

