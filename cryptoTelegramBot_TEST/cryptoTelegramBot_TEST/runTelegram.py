import telepot
import urllib3
from config import TELEGRAM
from dbhelper import  DBHelper
from time import sleep

class runTelegram:
    def __init__(self):
        self.TOKEN = TELEGRAM.token
        self.TelegramBot = telepot.Bot(self.TOKEN)
    
    def setup_pythonAnyWhere(self):
        proxy_url = TELEGRAM.proxy_url
        telepot.api._pools = {
                'default': urllib3.ProxyManager(proxy_url=proxy_url, num_pools=3, maxsize=10, retries=False, timeout=60),
        }
        telepot.api._onetime_pool_spec = (urllib3.ProxyManager, dict(proxy_url=proxy_url, num_pools=1, maxsize=1, retries=False, timeout=60))
        
    def getlastOffset(self):
        self.lastOffset = DBHelper.getLastOffset()

    def getUpdates(self):
        self.updates = self.TelegramBot.getUpdates(int(self.lastOffset)+1)

    def fetchData(self,update):
        self.text = update["message"]["text"]
        self.chatId = update["message"]["from"]["id"]
        self.offSetId = update["update_id"]
        self.firstName = update["message"]["from"]["first_name"]

    #Following function will return either new or old
    def checkUser(self):
        self.user = DBHelper.checkUser(self.chatId)

    def newUser(self):
        self.message = "I have added you in my notification list. \nFuture Upgrades : \n1. More Exchanges \n2. Provide Rank of newly added market \n3. Price Alerts \n4. Portfolio Tracker\n5. FREE Money/Coins alerts"

    def addBotMessage(self):
        DBHelper.addBotMessage(self.lastOffset,self.chatId , self.firstName, "g" , self.offSetid, self.fetchTime)
        
    def handleUpdate(self):
        if self.text == "/start" or self.text == "start":
            self.message="I know its too long since any new market is added, but I am tracking and will keep you posted. Thanks for poking :) "
        else:
            self.message="I only understand START signal as of now. I am learning new stuffs. Thanks for poking :) "


    def sendTelegramMessage(self):
        self.TelegramBot.sendMessage(self.chatId,self.message)

    def start(self):
        while True:
            try:
                self.getlastOffset()
                self.getUpdates()
                for update in self.updates:
                    try:
                        self.fetchData(update)
                        self.checkUser()
                        if self.user == 'new':
                            self.newUser()
                        else:
                            self.handleUpdate()
                        self.addBotMessage(self.chatId ,self.firstName, self.category , self.offSetId, self.fetchTime, self.text)
                        self.sendTelegramMessage()
                    except:
                        print "Exception caught inside for loop"
            except:
                print "Exception Caught"
            sleep(0.5)

