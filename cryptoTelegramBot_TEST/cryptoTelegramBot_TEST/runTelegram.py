import telepot
import urllib3
from config import TELEGRAM,COMMON
from dbhelper import  DBHelper
from time import sleep
import time
from decimal import Decimal
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
from telepot.loop import MessageLoop

class RunTelegram:
    def __init__(self):
        #print "Inside telegram constructor -- Telegram"
        self.TOKEN = TELEGRAM.token
        self.TelegramBot = telepot.Bot(self.TOKEN)
        self.db = DBHelper()
        #self.db.setup()
        self.category = "g"
        self.LAST_COMMAND_MAP = {}
        self.main_keyboard()
        self.sendMessage = "Please choose any of the following."
        MessageLoop(self.TelegramBot, {'chat': self.on_chat_message, 'callback_query': self.on_callback_query}).run_as_thread()

    def main_keyboard(self):
        self.keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Updating..",callback_data='freeCoins'),
                                                          InlineKeyboardButton(text="My Alerts",callback_data='myAlerts'),
                                                          InlineKeyboardButton(text="New Alert",callback_data='newAlert'),
                                                          ],
                                                         [InlineKeyboardButton(text="Check Price",callback_data='checkPrice'),
                                                          InlineKeyboardButton(text="Last Tweet",callback_data='lastTweet'),
                                                          InlineKeyboardButton(text="Info",callback_data='info'),
                                                          ],
                                                         [#InlineKeyboardButton(text="DoubleTrouble - Game for 100 BTC",callback_data='doubleTrouble'),
                                                          InlineKeyboardButton(text="Feedback/Report Issues",callback_data='feedbackAndReportIssues'),
                                                          ],
                                                         ])
    def back_to_menu_keyboard(self):
        self.keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back to Menu",callback_data='backToMenu'),
                                                          ],
                                                         ])
    
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

    def fetchData(self,message):
        #print "Inside fetchData -- Telegram -- Printing message......."
        #print message
        self.text = message["text"]
        self.chatId = message["from"]["id"]
        self.offsetId = message["date"]
        self.firstName = message["from"]["first_name"]

    #Following function will return either new or old
    def checkUser(self):
        #print "Inside checkUser -- Telegram"
        self.user = self.db.checkUser(self.chatId)

    def newUser(self):
        #print "Inside newUser -- Telegram"
        self.message = "I have added you in my notification list. \n\n<b>Features</b> : \n\n1. New Market Addition on Exchange alert ( Bittrex, Bitfinex and Binance)\n\n2. Tweet alerts\n\n3. Price increase alerts\n\n4. Price decrease alerts\n\n5. Check last tweet\n\n6. Check last price\n\n7. Earn FREE BTC by suggesting 2x coin\n\n8. FREE coins/ Airdrop alerts\n\n9. DoubleTrouble -- Game to 100 BTC from 0.1 BTC\n\n\n<b>Future Upgrades</b> : \n\n1. More Exchanges \n\n2. Portfolio Tracker\n\n3. DoubleTrouble Game - More Updates\n\n4. Better UI\n\n5. Upcoming Coin updates"
        
    def addBotMessageInDB(self):
        #print "Inside addBotMessageInDB -- Telegram"
        self.db.addBotMessage(self.chatId , self.firstName, self.category , self.offsetId, self.fetchTime, self.text)
        

    def set_last_command_map(self,command):
        if self.chatId in self.LAST_COMMAND_MAP:
            del self.LAST_COMMAND_MAP[self.chatId]
        
        self.LAST_COMMAND_MAP[self.chatId] = command

    def del_last_command_map(self,chatId):
        if chatId in self.LAST_COMMAND_MAP:
            del self.LAST_COMMAND_MAP[chatId]


    def handleUpdate(self):
        #print "Inside handleUpdate -- Telegram"
        self.textArray = self.text.split()

        if self.textArray[0] == "/start" or self.textArray[0] == "start":
            self.del_last_command_map(self.chatId)
            self.checkUser()
            if self.user == 'new':
                self.newUser()
            else:
                self.message="I remember you and will keep you posted with any new update/alert. Thanks for poking :) "
        elif self.textArray[0] == "/check_tweet":
            if len(self.textArray) == 1 :
                #self.message="Please provide information in following format : \n/check_tweet CURRENCY_SYMBOL"
                self.set_last_command_map("/check_tweet")
                self.message="Please provide CURRENCY_SYMBOL like :\nETH"
                self.keyboard=''
            else:
                currencySymbol = self.textArray[1]
                tweets = self.db.fetchTweet(currencySymbol)
                #name,id,tweet_id
                if self.is_empty(tweets):
                    self.message = "No currency with symbol - " + str(currencySymbol).upper() + " found. Please provide correct currency symbol."
                    #print "message set"
                else:
                    #print tweets
                    #name,twitter_screen_name,tweet_id
                    for tweet in tweets:
                        if self.is_empty(tweet[1]):
                            self.message = "We don't have Official Twitter Handler for this currency."                        
                        elif self.is_empty(tweet[2]):
                            self.message = "We havn't captured any tweet for this coin. As soon as team will post anything we will capture it. Please try again after some time."
                        else:
                            self.message = "<a href='https://twitter.com/"+tweet[1]+"/status/"+tweet[2]+"'>"+ str(currencySymbol).upper() +"("+tweet[0]+") tweeted : </a>"
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)
        elif self.textArray[0] == "/check_price":
            if len(self.textArray) == 1 :
                #self.message="Please provide information in following format : \n/check_price CURRENCY_SYMBOL"
                self.set_last_command_map("/check_price")
                self.message="Please provide CURRENCY_SYMBOL like :\nETH"
                self.keyboard=''
            else:
                currencySymbol = str(self.textArray[1]).lower()
                print "curr = " + str(currencySymbol)
                prices = self.db.fetchPrice(currencySymbol)
                print "prices = " + str(prices)
                #name,exchange,exchange_price,exchange_price_in,cmc_price_usd
                if self.is_empty(prices) :
                    self.message = "No currency with symbol - " + str(currencySymbol).upper() + " found. Please provide correct currency symbol."
                    print "Set last message of no currency"
                else:
                    self.message = ""
                    for price in prices:
                        name=str(price[0])
                        exchange=str(price[1])
                        exchange_price=str("%.9f" % Decimal(price[2]))
                        exchange_price_in = str(price[3])
                        cmc_price_usd = str("%.9f" % Decimal(price[4]))
                        if currencySymbol == "btc":
                            self.message = self.message + "Price of " + currencySymbol.upper() + "( " + name + " ) :\nExchange : " + exchange +"\n  Price : $" + cmc_price_usd + "\n\n"
                        else:
                            self.message = self.message + "Price of " + currencySymbol.upper() + "( " + name + " ) :\nExchange : " + exchange +"\n  Price : " + exchange_price + " " + exchange_price_in + " or $" + cmc_price_usd + "\n\n"
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)
        elif self.textArray[0] == "/set_alert_tweet":
            if len(self.textArray) == 1 :
#                self.message="Please provide information in following format : \n/check_price CURRENCY_SYMBOL"
                self.set_last_command_map("/set_alert_tweet")
                self.message="Please provide CURRENCY_SYMBOL like :\nETH"
                self.keyboard=''
            else:
                currencySymbol = self.textArray[1]
                self.db.add_alert(self.chatId,"tweet",self.fetchTime,currencySymbol,"yes",0,"btc")
                self.message = "We will ping you for any new tweet."
                self.del_last_command_map(self.chatId)    
                self.back_to_menu_keyboard()
        elif self.textArray[0] == "/set_alert_price_incr":
            print "inside price inc function"
            if len(self.textArray) == 1:
                print "when len = 1"
                self.set_last_command_map("/set_alert_price_incr")
                self.message="Please provide following information : \nCURRENCY_SYMBOL PRICE BTC/satoshi \n For Example: \n DGB 0.00000195 btc"
                self.keyboard=''
            elif len(self.textArray) != 4:
                print "when len not equal to 4"
                self.message="Please provide information in correct format : \nCURRENCY_SYMBOL PRICE BTC/satoshi \n For Example: \n DGB 0.00000195 btc"
                self.back_to_menu_keyboard()
            else:
                print "inside else part"
                currencySymbol = str(self.textArray[1]).lower()
                price_alert = self.textArray[2]
                price_in = str(self.textArray[3]).lower()

                if self.is_empty(currencySymbol):
                    self.message="Please provide information in following format : \n/set_alert_price_incr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif self.is_empty(price_alert):
                    self.message="Please provide information in following format : \n/set_alert_price_incr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif self.is_empty(price_in):
                    self.message="Please provide information in following format : \n/set_alert_price_incr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif price_in not in ("btc","usd","eth","satoshi","stats","sats"):
                    self.message="Please provide information in following format : \n/set_alert_price_incr CURRENCY_SYMBOL PRICE BTC/satoshi"                
                else: 
                    if price_in.lower() in ("satoshi","stats","sats"):
                        price_alert = Decimal( Decimal(price_alert) / Decimal(1000000000.0) )
                        price_in = "btc"
                            
                    if price_in.lower() != "btc":
                        self.message = "Price supported only in BTC/satoshi"
                    else:
                        self.db.add_alert(self.chatId,"p_incr",self.fetchTime,currencySymbol,"yes",price_alert,price_in)
                        self.message = "We have captured your alert and will keep you posted."
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)            
        elif self.textArray[0] == "/set_alert_price_decr":
            if len(self.textArray) == 1:
                self.set_last_command_map("/set_alert_price_decr")
                self.message="Please provide following information : \nCURRENCY_SYMBOL PRICE BTC/satoshi \n For Example: \n DGB 0.00000195 btc"
                self.keyboard=''
            elif len(self.textArray) != 4:            
                self.message="Please provide information in following format : \n/set_alert_price_decr CURRENCY_SYMBOL PRICE BTC/satoshi"
                self.back_to_menu_keyboard()
            else:
                currencySymbol = str(self.textArray[1]).lower()
                price_alert = self.textArray[2]
                price_in = str(self.textArray[3]).lower()

                if self.is_empty(currencySymbol):
                    self.message="Please provide information in following format : \n/set_alert_price_decr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif self.is_empty(price_alert):
                    self.message="Please provide information in following format : \n/set_alert_price_decr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif self.is_empty(price_in):
                    self.message="Please provide information in following format : \n/set_alert_price_decr CURRENCY_SYMBOL PRICE BTC/satoshi"
                elif price_in not in ("btc","satoshi","stats","sats"):
                    self.message="Please provide information in following format : \n/set_alert_price_decr CURRENCY_SYMBOL PRICE BTC/satoshi"                
                else: 
                    if price_in.lower() in ("satoshi","stats","sats"):
                        price_alert = Decimal( Decimal(price_alert) / Decimal(1000000000.0) )
                        price_in = "btc"
                            
                    if price_in.lower() != "btc":
                        self.message = "Price supported only in BTC/satoshi"
                        self.back_to_menu_keyboard()
                    else:
                        self.db.add_alert(self.chatId,"p_decr",self.fetchTime,currencySymbol,"yes",price_alert,price_in)
                        self.message = "We have captured your alert and will keep you posted."
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)
        elif self.textArray[0] == "/my_alerts":
            self.del_last_command_map(self.chatId)
            try:
                self.message=""
                alerts = self.db.my_alerts(str(self.chatId))
                #id,chatId,alert_type,coin_symbol,alert_price,price_in
                #print "Going inside if check condition -- MY ALERTS"
                if not self.is_empty(alerts):
                    for alert in alerts:
                        alert_id=str(alert[0])
                        chatId=str(alert[1])
                        alert_type=str(alert[2])
                        coin_symbol=str(alert[3])
                        alert_price=str("%.9f" % Decimal(alert[4]))
                        price_in=str(alert[5]).upper()

                        if alert_type == "tweet":
                            print "Inside TWEET CONDITION CHECK "
                            self.message = self.message + "Tweet alert : "+ coin_symbol + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                        elif alert_type == "p_incr":
                            self.message = self.message + "Price Increase alert : "+ coin_symbol + " " + alert_price + " " + price_in.upper() + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                        elif alert_type == "p_decr":
                            self.message = self.message + "Price Decrease alert : "+ coin_symbol + " " + alert_price + " " + price_in.upper() + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                else:
                    self.message = "There are no alerts set for you."
            except Exception as e: 
                print(e)  
        elif  self.textArray[0].split("__")[0] == "/del_alert":  
            self.del_last_command_map(self.chatId)
            if len(self.textArray[0].split("__")) != 2:   
                self.message = "Something is wrong with this delete tag"         
            else:
                alert_id = self.textArray[0].split("__")[1]
                try:
                    self.db.delete_alert(self.chatId,alert_id) 
                    self.message = "Alert Deleted"    
                except Exception as e: 
                    print(e)
        elif self.textArray[0] == "/feedback":
            if len(self.textArray) == 1:            
                #self.message="Looks like you missed typing your feedback."
                self.set_last_command_map("/feedback")
                self.message="Please provide your feedback : "
                self.keyboard=''
            else :
                self.message = "Every feedback is important for us. Thank you for taking your time and writing."          
                self.del_last_command_map(self.chatId)
                self.back_to_menu_keyboard()
        elif self.textArray[0] == "/suggest_2x_coin":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/suggest_2x_coin")
                self.message="Please provide information in following format : \nCURRENCY_SYMBOL DESCRIPTION"
            else:
                self.message = "We will look into your suggestion. If selected and gave 2x return within a month, you will get your commission for sharing this coin. \nWe will ask for your BTC address after success."
                self.del_last_command_map(self.chatId)                
        elif self.textArray[0] == "/suggest_free_coin":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/suggest_2x_coin")
                self.message="Please provide information in following format : \nCURRENCY_SYMBOL YOUR_REFERRAL_LINK"
            else:
                self.message = "We will look into your suggestion. If selected, your referral link will be shared with 70% of our members and we will notify you."
                self.del_last_command_map(self.chatId)                
        elif self.textArray[0] == "/help":
            self.message = "I can help you managing your crypto currency world and earn a lot of BTC.\n\n<b>Commands Usage Example :</b>\n\n/check_tweet ETH\n/check_price ETH\n/set_alert_tweet ETH\n/set_alert_price_incr ETH 0.06 BTC\n/set_alert_price_decr ETH 0.03 BTC\n/suggest_2x_coin XEM Catapult Update, WeChat update, Easy 2x in a week Buy: Under 3000 satoshi StopLoss: 2500 satoshi \n/suggest_free_coin SPHERE YOUR_REFERRAL_LINK \n/feedback We LOVE your project. Keep Sharing.\n/report ISSUE_DESCRIPTION\n\n<b>Features</b> : \n1. New Market Addition on Exchange alert ( Bittrex, Bitfinex and Binance)\n2. Tweet alerts\n3. Price increase alerts\n4. Price decrease alerts\n5. Check last tweet\n6. Check last price\n7. Earn FREE BTC by suggesting 2x coin\n8. FREE coins/ Airdrop alerts\n9. DoubleTrouble -- Game to 100 BTC from 0.1 BTC9. New Market Addition alert ( Currently Bittrex, Bitfinex and Binance)\n\n<b>Future Upgrades</b> : \n1. More Exchanges \n2. Portfolio Tracker\n3. DoubleTrouble Game - More Updates\n4. Better UI\n5. Upcoming Coin updates\n\n<b>Earning Model : </b>\n1. Suggest 2x coin\nIf you were the first one with proper justification of 2x coin and it works like that in a month, then you will earn 5% of total profit from admin for sure and 30% from all donations we will receive.\nCommission : 5% on profit + 30% on donation\n\n2. Suggest Free coins / Airdrops : \nSuggest free coin and we will share your referral link with 70% of our members. You will get huge referrals by letting us know.\n\n3. Free coins / Airdrops : \nCollect all Airdrops signals and you will have plenty of amount after a year. Slow but steady.\n\n\n<b>What is DoubleTrouble Game ?</b>\nYou just need 10 signals which can go 2x and only these 10 signals will increase your portfolio by 1000 times. Yes, you read that correct 1000 TIMES (1024 to be precise).\n\nWe are starting with 0.1 BTC and will take it to 100 BTC within a year.\nInstead of providing you 100 coins, we will provide you only 2 coins for a month. \nOne coin from top 50 and other below 150 rank. Keep 80 percent in first coin and 20 percent in second coin but only 50% of your portfolio.\n\nConsider you have 1 BTC then distribute it as : \nTotal Holding : 1 BTC\nFirst coin : 0.4\nSecond coin : 0.1\nRest 0.5 BTC should not be touched.\n\nWithin a year, I am expecting many millionaires in this group.\n\n<b>Donate : </b>\nBTC - 16YhanuEHv4UguTfTrD71383xxtwfaf4Hk\nETH - 0x50ca788af6cb75f48fc20feb324a6f02865ef3ff"
            self.back_to_menu_keyboard()
            self.del_last_command_map(self.chatId)
        elif self.textArray[0] == "/answer":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/answer")
                self.message="Please provide your response : "
            else:
                self.message = "Thank you for your response !!"
                self.del_last_command_map(self.chatId)
        else :
            if self.chatId in self.LAST_COMMAND_MAP:
                self.text = self.LAST_COMMAND_MAP[self.chatId] + " " + self.text
                #print "New text" 
                #print self.text
                #self.del_last_command_map(self.chatId)
                self.handleUpdate()
            else:
                self.main_keyboard()
                self.message = "Please choose from below menu"

    def sendTelegramMessage(self):
        ##print "Inside sendTelegramMessage -- Telegram"
        try:
            self.TelegramBot.sendMessage(parse_mode='HTML',chat_id=self.chatId,text=self.message,reply_markup=self.keyboard)
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
            #print('Structure is not empty.')
            return False
        else:
            #print('Structure is empty.')
            return True


    def on_callback_query(self,msg):
        query_id, from_id, query_data = telepot.glance(msg, flavor='callback_query')
        print "MESSAGE"
        print msg
        self.text = ""
        self.chatId =  msg['message']['chat']['id']
        msg_id = msg['message']['message_id']
        tup = self.chatId,msg_id

        if query_data == "freeCoins":
            self.message = "Details Coming Sooon....\n"
            self.back_to_menu_keyboard()
            self.TelegramBot.editMessageText(tup,self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "myAlerts":
            self.message = ""
            self.set_last_command_map("/my_alerts")
            self.text = "/my_alerts"
            self.handleUpdate()
            self.back_to_menu_keyboard()
            self.TelegramBot.editMessageText(tup,self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "newAlert":
            #TelegramBot.answerCallbackQuery(query_id, text="Flash Message on top")
            self.message = "Select alert type : "
            self.keyboard = InlineKeyboardMarkup(inline_keyboard=[
                                                            [InlineKeyboardButton(text="Tweet Alert",callback_data='tweetAlert')],
                                                            [InlineKeyboardButton(text="Price Increase Alert",callback_data='priceIncreaseAlert')],
                                                            [InlineKeyboardButton(text="Price Decrease Alert",callback_data='priceDecreaseAlert')],
                                                            [InlineKeyboardButton(text="<< Back",callback_data='backToMenu')],
                                                             ])
            self.TelegramBot.editMessageText(tup,self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "checkPrice":
            self.message = ""
            self.set_last_command_map("/check_price")
            self.text = "/check_price"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text+"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "lastTweet":
            self.message = ""
            self.set_last_command_map("/check_tweet")
            self.text = "/check_tweet"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text+"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "doubleTrouble": 
            self.message = "Details Coming Sooon....\n"
            self.back_to_menu_keyboard()
            self.TelegramBot.editMessageText(tup,self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "feedbackAndReportIssues":
            self.message = ""
            self.set_last_command_map("/feedback")
            self.text = "/feedback"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text,parse_mode='HTML',reply_markup=self.keyboard)      
        elif query_data == "tweetAlert":
            self.message = ""
            self.set_last_command_map("/set_alert_tweet")
            self.text = "/set_alert_tweet"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text +"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)     
        elif query_data == "priceIncreaseAlert":
            self.message = ""
            self.set_last_command_map("/set_alert_price_incr")
            self.text = "/set_alert_price_incr"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text +"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)      
        elif query_data == "priceDecreaseAlert":
            self.message = ""
            self.set_last_command_map("/set_alert_price_decr")
            self.text = "/set_alert_price_decr"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text +"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "info":
            self.message = ""
            self.set_last_command_map("/help")
            self.text = "/help"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text+"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)     
        elif query_data == "backToMenu":
            self.main_keyboard()
            self.message = "Please choose from below option"
            self.TelegramBot.editMessageReplyMarkup(tup,reply_markup=self.keyboard)


    def start(self,sleepTime):
        #print "Start method -- Telegram"
        self.sleepTime = sleepTime
        while True:
            try:
#               Send New Market Notification
                newMarkets = self.db.get_newMarketListings()
                allUsers = self.db.getAllUsers()
                #print newMarkets
                try:
                    if (not self.is_empty(newMarkets)) and (newMarkets is not None) :
                        self.message = "New Market Added"
                        for market in newMarkets:
                            rank = str(market[0])
                            symbol = str(market[1]).upper()
                            name = str(market[2]).upper()
                            exchange = str(market[3]).upper()
                            marketname = str(market[4]).upper()
                            exchange_last_price = str("%.9f" % Decimal(market[5]))
                            if (not self.is_empty(market[6])) and (market[6] is not None) :
                                cmc_price_usd = str("%.9f" % Decimal(market[6]))
                            else:
                                cmc_price_usd = "-"

                            self.message = self.message + "\n\nExchange : " + exchange + "\nMarket Name : "+marketname +"\nSymbol : "+symbol+"\nName : "+name+"\nRank : "+ rank +"\nLast Price : "+exchange_last_price+"\nActual Price in USD : " + cmc_price_usd
    
                        for user in allUsers:
                            self.chatId = user[0]
                            self.main_keyboard()
                            self.sendTelegramMessage()
                        #Update is_new_market to NO , so that it wont be fetched again.
                        self.db.update_priceDenorm_marketTypes()
                except Exception as e:
                    print "New markets alerts Exception "
                    print e.message 

#Send alerts 
                try:
                    for user in allUsers:
                        alerts = self.db.getAlerts()
                        self.message = ""
                        #print "ALERTS --> " + str(alerts)
                        if (not self.is_empty(alerts)) and (alerts is not None):
                            for alert in alerts:
                                alert_id = str(alert[0])
                                self.chatId = str(alert[1])
                                alert_type = str(alert[2])
                                coin_symbol = str(alert[3])
                                alert_price = str("%.9f" % Decimal(alert[4]))
                                price_in = str(alert[5])
                                twitter_screen_name = str(alert[6])
                                tweet_id = str(alert[7])
                                coin_name = str(alert[8])
                                exchange = str(alert[9])
                                new_price = str("%.9f" % Decimal(alert[10]))

                                if alert_type == "tweet":
                                    self.message = self.message + "\n" + "<a href='https://twitter.com/"+twitter_screen_name+"/status/"+tweet_id+"'>"+coin_symbol +"("+coin_name+") tweeted : </a>"
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    #self.main_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "p_incr":
                                    self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) increases to " + new_price + " " + price_in.upper() + " on " + exchange + " exchange."
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    #self.main_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "p_decr":
                                    self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) decreases to " + new_price + " " + price_in.upper() + " on " + exchange + " exchange."
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    #self.main_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "special_tweet" and (self.chatId == "443841255" or self.chatId == "477750932" ):
                                    self.message = self.message + "\n" + "<a href='https://twitter.com/"+twitter_screen_name+"/status/"+tweet_id+"'>"+coin_symbol +"("+coin_name+") tweeted about FORK or REBRANDING : </a>"
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    #self.main_keyboard()
                                    #self.sendTelegramMessage()
                            self.main_keyboard()
                            self.sendTelegramMessage()
                except Exception as e:
                    print "Alerts Exception "
                    print e.message 

                newMarkets=""
                allUsers=""

                self.setFetchTime() 

            except Exception as e: 
                print "Error while processing alerts"                            
                print(e)
                print e.message
                with open(COMMON.errorDir + TELEGRAM.errorFileName,'a+') as f:
                    f.write("\n\nError : ")
                    f.write(e.__doc__)
                    f.write(e.message)
                #print "Exception Caught"
            #sleep(self.sleepTime)
            sleep(30)

    def on_chat_message(self,msg):
        try:
            self.setFetchTime()
            self.fetchData(msg)
            #print "Offset : " + str(self.lastOffset)
            self.handleUpdate()
            self.addBotMessageInDB()
            self.sendTelegramMessage()
        except Exception as e:
            print "Error while processing chat message"
            print(e)      
            with open(COMMON.errorDir + TELEGRAM.errorFileName,'a+') as f:
                f.write("\n\nError : ")
                f.write(e.__doc__)
                f.write(e.message)  
                f.write(self.message)