import telepot
import urllib3
from config import TELEGRAM,COMMON
from dbhelper import  DBHelper
from time import sleep
import time
from decimal import Decimal
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
from telepot.loop import MessageLoop
import locale

class RunTelegram:
    def __init__(self):
        #print "Inside telegram constructor -- Telegram"
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
        self.TOKEN = TELEGRAM.token
        self.TelegramBot = telepot.Bot(self.TOKEN)
        #self.db = DBHelper()
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
        self.text = message["text"].encode('utf8')
        self.text = self.text.replace("@mycoins_bot", "")
        self.type = message["chat"]["type"]
        self.username_group = ""
        self.username_user  = ""
        if self.type == "group" or self.type=="supergroup" :
            self.chatId = message["chat"]["id"]  #This represents group name
            self.userId = message["from"]["id"]
            try:
                self.username_group = message["chat"]["username"].encode('utf8')
            except Exception as e:
                self.username_group = ""
                print "Exception from REFRESH DENORM method" 
                print e.message
                print(e)    
        else:
            self.chatId = message["from"]["id"]
            self.userId = message["from"]["id"]

        self.offsetId = message["date"]
        self.firstName = message["from"]["first_name"].encode('utf8')
        try: 
            self.username_user = message["from"]["username"].encode('utf8')
        except Exception as e:
            self.username_user = ""
            print "Exception from REFRESH DENORM method" 
            print e.message
            print(e)


    #Following function will return either new or old
    def checkUser(self):
        #print "Inside checkUser -- Telegram"
        self.user = self.db.checkUser(self.chatId)

    def newUser(self):
        #print "Inside newUser -- Telegram"
        self.message = "I have added you in my notification list. \n\n<b>Features</b> : \n\n- New Market Addition on Exchange alert ( Bittrex, Bitfinex and Binance)\n\n- Tweet alerts\n\n- Price increase alerts\n\n- Price decrease alerts\n\n- Check last tweet\n\n- Check last price\n\n- Earn FREE BTC by suggesting 2x coin\n\n- FREE coins/ Airdrop alerts\n\n- DoubleTrouble -- Game to 100 BTC from 0.1 BTC\n\n\n<b>Future Upgrades</b> : \n\n- More Exchanges \n\n- Portfolio Tracker\n\n- DoubleTrouble Game - More Updates\n\n- Better UI\n\n- Upcoming Coin updates"
        
    def addBotMessageInDB(self):
        #print "Inside addBotMessageInDB -- Telegram"
        self.db.addBotMessage(self.chatId , self.firstName, self.category , self.offsetId, self.fetchTime, self.text , self.userId, self.username_user,self.username_group)

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

        if self.textArray[0].lower() == "/start" or self.textArray[0].lower() == "start":
            self.del_last_command_map(self.chatId)
            self.checkUser()
            if self.user == 'new':
                self.newUser()
            else:
                self.message="I remember you and will keep you posted with any new update/alert. Thanks for poking :) "
        elif self.textArray[0].lower() == "/t":
            if len(self.textArray) == 1 :
                #self.message="Please provide information in following format : \n/t CURRENCY_SYMBOL"
                self.set_last_command_map("/t")
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
        elif self.textArray[0].lower() == "/p":
            if len(self.textArray) == 1 :
                #self.message="Please provide information in following format : \n/p CURRENCY_SYMBOL"
                self.set_last_command_map("/p")
                self.message="Please provide CURRENCY_SYMBOL like :\nETH"
                self.keyboard=''
            else:
                currencySymbol = str(self.textArray[1]).lower()
                print "curr = " + str(currencySymbol)
                prices = self.db.fetchPrice(currencySymbol)
                print "prices = " + str(prices)
                #name,exchange,exchange_price,exchange_price_in,cmc_price_usd
                if self.is_empty(prices) :
                    prices_NOT_ON_CMC = self.db.fetchPrice_not_on_CMC(currencySymbol)
                    if self.is_empty(prices_NOT_ON_CMC):
                        self.message = "No currency with symbol - " + str(currencySymbol).upper() + " found. Please provide correct currency symbol."
                        print "Set last message of no currency"
                    else:
                        self.message = ""
                        finalMessage = currencySymbol.upper() + " - not yet available on Coinmarketcap" + "\n\n" + "May be link to these markets on Exchanges : "
                        for price in prices_NOT_ON_CMC:
                            exchange = str(price[0])
                            marketname = str(price[1]).upper()
                            exchange_last_price   = str("%.9f" % Decimal(price[2]))
                            exchange_last_price_in = str(price[3]).upper()
                            finalMessage = finalMessage + "\n" + exchange + " | " + marketname + " | " + exchange_last_price + " " + exchange_last_price_in
                        self.message = finalMessage
                else:
                    self.message = ""
                    finalMessage = ""
                    firstRecord=True
                    flag=1
                    for price in prices:
                        currencySymbol = str(price[0]).upper()
                        name=str(price[1])
                        cmc_price_usd = str("%.5f" % Decimal(price[2]))
                        cmc_price_btc = str("%.9f" % Decimal(price[3]))
                        cmc_percent_change_24h = str("%.2f" % Decimal(price[4]))
                        cmc_market_cap_usd = locale.currency(price[5], grouping=True)
                        cmc_24h_volume_usd = locale.currency(price[6], grouping=True)
                        exchange=str(price[7])
                        exchange_price=str("%.9f" % Decimal(price[8]))
                        exchange_price_in = str(price[9])
                        cmc_percent_change_1h = str("%.2f" % Decimal(price[10]))
                        cmc_percent_change_7d = str("%.2f" % Decimal(price[11]))
                        rank = str(price[12]).upper()
                        if firstRecord == True:
                            finalMessage = currencySymbol + " ( " + name + " ) : $" + cmc_price_usd + " | " + cmc_price_btc + " BTC\n1 Hour Change   : " + cmc_percent_change_1h  + "%\n24 Hours Change : " + cmc_percent_change_24h + "%\n1 Week Change   : " + cmc_percent_change_7d +"%\n\nRank : " + rank + "\nMarket Cap : " + cmc_market_cap_usd + "\nVolume : " + cmc_24h_volume_usd
                            if not currencySymbol.lower() == "btc" and not exchange.lower() == "coinmarketcap":
                                finalMessage = finalMessage + "\n\nExchange : \n" + exchange + " | " + exchange_price + " " + exchange_price_in
                                flag=0
                            firstRecord=False
                        else:
                            if flag == 1:
                                finalMessage = finalMessage + "\n\nExchange :"
                                flag=0
                            if not currencySymbol.lower() == "btc" and not exchange.lower() == "coinmarketcap":
                                finalMessage = finalMessage + "\n" + exchange + " | " + exchange_price + " " + exchange_price_in
                    self.message = finalMessage
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)
        elif self.textArray[0].lower() == "/set_alert_tweet":
            if len(self.textArray) == 1 :
#                self.message="Please provide information in following format : \n/p CURRENCY_SYMBOL"
                self.set_last_command_map("/set_alert_tweet")
                self.message="Please provide CURRENCY_SYMBOL like :\nETH"
                self.keyboard=''
            else:
                currencySymbol = self.textArray[1]
                self.db.add_alert(self.chatId,"tweet",self.fetchTime,currencySymbol,"yes",0,"btc")
                self.message = "We will ping you for any new tweet."
                self.del_last_command_map(self.chatId)
                self.back_to_menu_keyboard()
        elif self.textArray[0].lower() == "/set_alert_price_incr":
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
        elif self.textArray[0].lower() == "/set_alert_price_decr":
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
        elif self.textArray[0].lower() == "/set_alert_price_watch":    #ALWAYS IN USD
            if len(self.textArray) == 1:
                self.set_last_command_map("/set_alert_price_watch")
                self.message="Any Major change in prices (in USD) will be notified\nPlease provide CURRENCY_SYMBOL \n For Example: \n DGB"
                self.keyboard=''
            elif len(self.textArray) != 2:            
                self.message="Please provide information in following format : \n/set_alert_price_watch CURRENCY_SYMBOL"
                self.back_to_menu_keyboard()
            else:
                currencySymbol = str(self.textArray[1]).lower()
                price_in = "usd"
                
                if self.is_empty(currencySymbol):
                    self.message="Please provide information in following format : \n/set_alert_price_watch CURRENCY_SYMBOL"
                else:
                    self.db.add_alert(self.chatId,"p_watch",self.fetchTime,currencySymbol,"yes",0,price_in)
                    self.message = "We have captured your alert and will keep you posted."
                self.back_to_menu_keyboard()
                self.del_last_command_map(self.chatId)
        elif self.textArray[0].lower() == "/my_alerts":
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
                            #print "Inside TWEET CONDITION CHECK "
                            self.message = self.message + "Tweet alert : "+ coin_symbol.upper() + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                        elif alert_type == "p_watch":
                            self.message = self.message + "Price Watching : "+ coin_symbol.upper() + " (in " + price_in.upper() + ")\n To delete --> /del_alert__" + alert_id + "\n\n"
                        elif alert_type == "p_incr":
                            self.message = self.message + "Price Increase alert : "+ coin_symbol.upper() + " " + alert_price + " " + price_in.upper() + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                        elif alert_type == "p_decr":
                            self.message = self.message + "Price Decrease alert : "+ coin_symbol.upper() + " " + alert_price + " " + price_in.upper() + "\n To delete --> /del_alert__" + alert_id + "\n\n"
                else:
                    self.message = "There are no alerts set for you."
            except Exception as e: 
                print(e)  
        elif  self.textArray[0].split("__")[0].lower() == "/del_alert":  
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
        elif self.textArray[0].lower() == "/feedback":
            if len(self.textArray) == 1:            
                #self.message="Looks like you missed typing your feedback."
                self.set_last_command_map("/feedback")
                self.message="Please provide your feedback : "
                self.keyboard=''
            else :
                self.message = "Every feedback is important for us. Thank you for taking your time and writing."          
                self.del_last_command_map(self.chatId)
                self.back_to_menu_keyboard()
        elif self.textArray[0].lower() == "/suggest_2x_coin":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/suggest_2x_coin")
                self.message="Please provide information in following format : \nCURRENCY_SYMBOL DESCRIPTION"
            else:
                self.message = "We will look into your suggestion. If selected and gave 2x return within a month, you will get your commission for sharing this coin. \nWe will ask for your BTC address after success."
                self.del_last_command_map(self.chatId)                
        elif self.textArray[0].lower() == "/suggest_free_coin":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/suggest_2x_coin")
                self.message="Please provide information in following format : \nCURRENCY_SYMBOL YOUR_REFERRAL_LINK"
            else:
                self.message = "We will look into your suggestion. If selected, your referral link will be shared with 70% of our members and we will notify you."
                self.del_last_command_map(self.chatId)                
        elif self.textArray[0].lower() == "/help":
            self.message = "I can help you managing your crypto currency world and earn a lot of BTC.\n\n<b>Commands Usage Example :</b>\n\n/t ETH\n/p ETH\n/suggest_2x_coin XEM Catapult Update, WeChat update, Easy 2x in a week Buy: Under 3000 satoshi StopLoss: 2500 satoshi \n/suggest_free_coin Coin name + YOUR_REFERRAL_LINK \n/feedback We LOVE your project. Keep Sharing.\n/report ISSUE_DESCRIPTION\n\n<b>Features</b> : \n- New Market Addition on Exchange alert ( Bittrex, Bitfinex and Binance)\n- Tweet alerts\n- Price increase alerts\n- Price decrease alerts\n- Check last tweet\n- Check last price\n- Earn FREE BTC by suggesting 2x coin\n- FREE coins/ Airdrop alerts\n- DoubleTrouble -- Game to 100 BTC from 0.1 BTC9.\n\n<b>Future Upgrades</b> : \n- More Exchanges \n- Portfolio Tracker\n- DoubleTrouble Game - More Updates\n- Better UI\n- Upcoming Coin updates\n\n<b>Earning Model : </b>\n- Suggest 2x coin\nIf you were the first one with proper justification of 2x coin and it works like that in a month, then you will earn 5% of total profit from admin for sure and 30% from all donations we will receive.\nCommission : 5% on profit + 30% on donation\n\n- Suggest Free coins / Airdrops : \nSuggest free coin and we will share your referral link with 70% of our members. You will get huge referrals by letting us know.\n\n- Free coins / Airdrops : \nCollect all Airdrops signals and you will have plenty of amount after a year. Slow but steady.\n\n\n<b>What is DoubleTrouble Game ?</b>\nYou just need 10 signals which can go 2x and only these 10 signals will increase your portfolio by 1000 times. Yes, you read that correct 1000 TIMES (1024 to be precise).\n\nWe are starting with 0.1 BTC and will take it to 100 BTC within a year.\nInstead of providing you 100 coins, we will provide you only 2 coins for a month. \nOne coin from top 50 and other below 150 rank. Keep 80 percent in first coin and 20 percent in second coin but only 50% of your portfolio.\n\nConsider you have 1 BTC then distribute it as : \nTotal Holding : 1 BTC\nFirst coin : 0.4\nSecond coin : 0.1\nRest 0.5 BTC should not be touched.\n\nWithin a year, I am expecting many millionaires in this group.\n\n"
            self.back_to_menu_keyboard()
            self.del_last_command_map(self.chatId)
        elif self.textArray[0].lower() == "/answer":
            if len(self.textArray) == 1:            
                self.set_last_command_map("/answer")
                self.message="Please provide your response : "
                self.keyboard=''
            else:
                self.message = "Thank you for your response !!"
                self.del_last_command_map(self.chatId)
                self.back_to_menu_keyboard()
        elif self.textArray[0].lower() == "/IamActive":
            self.message = "Thank you for your response, we will update you very soon with your reward !!"
            self.del_last_command_map(self.chatId)
            self.back_to_menu_keyboard()
        elif self.textArray[0].lower() == "/top":
            self.del_last_command_map(self.chatId)
            self.back_to_menu_keyboard()
            try:
                self.message=""
                alerts = self.db.top_coins()
                #symbol,name,percentage_change
                if not self.is_empty(alerts):
                    for alert in alerts:
                        coin_symbol=str(alert[0]).upper()
                        coin_name=str(alert[1])
                        per_change=str("%.2f" % Decimal(alert[2]))
                        
                        if str(per_change).startswith("-"):
                            per_change="&#128315; " + str(per_change)
                        else:
                            per_change="&#9650; " + str(per_change)
                        
                        self.message = self.message + coin_name + "(" + coin_symbol + ") : " + per_change + "% \n"
                else:
                    self.message = "We are having some trouble in bot. Please wait for sometime."
            except Exception as e: 
                print(e)
        elif self.textArray[0].lower() == "/bottom":
            self.del_last_command_map(self.chatId)
            self.back_to_menu_keyboard()
            try:
                self.message=""
                alerts = self.db.bottom_coins()
                #symbol,name,percentage_change
                if not self.is_empty(alerts):
                    for alert in alerts:
                        coin_symbol=str(alert[0]).upper()
                        coin_name=str(alert[1])
                        per_change=str("%.2f" % Decimal(alert[2]))
                        
                        if str(per_change).startswith("-"):
                            per_change="&#128315; " + str(per_change)
                        else:
                            per_change="&#9650; " + str(per_change)

                        self.message = self.message + coin_name + "(" + coin_symbol + ") : " + per_change + "% \n"
                else:
                    self.message = "We are having some trouble in bot. Please wait for sometime."
            except Exception as e: 
                print(e)
        else :
            if self.chatId < 0:
                self.groupSilence = True;
            elif self.chatId in self.LAST_COMMAND_MAP:
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
            if self.chatId < 0 or str(self.chatId).startswith("-"):
                self.keyboard=''
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
        self.db = DBHelper()
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
                                                            [InlineKeyboardButton(text="Price Watch",callback_data='priceWatch')],
                                                            [InlineKeyboardButton(text="Price Increase Alert",callback_data='priceIncreaseAlert')],
                                                            [InlineKeyboardButton(text="Price Decrease Alert",callback_data='priceDecreaseAlert')],
                                                            [InlineKeyboardButton(text="<< Back",callback_data='backToMenu')],
                                                             ])
            self.TelegramBot.editMessageText(tup,self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "checkPrice":
            self.message = ""
            self.set_last_command_map("/p")
            self.text = "/p"
            self.handleUpdate()
            self.TelegramBot.editMessageText(tup,self.text+"\n"+self.message,parse_mode='HTML',reply_markup=self.keyboard)
        elif query_data == "lastTweet":
            self.message = ""
            self.set_last_command_map("/t")
            self.text = "/t"
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
        elif query_data == "priceWatch":
            self.message = ""
            self.set_last_command_map("/set_alert_price_watch")
            self.text = "/set_alert_price_watch"
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
        self.db.closeConnection()


    def start(self,sleepTime):
        #print "Start method -- Telegram"
        self.sleepTime = sleepTime
        while True:
            self.db = DBHelper()
            try:
#               Send New Market Notification
                newMarkets = self.db.get_newMarketListings()
                newMarkets_for_groups = self.db.get_newMarketListings_for_groups()                
                allUsers = self.db.getAllUsers()
                user_message  = "-"
                group_message = "-"
                #print newMarkets
                try:
                    if (not self.is_empty(newMarkets)) and (newMarkets is not None) :
                        user_message = "New Market Added"
                        for market in newMarkets:
                            rank = str(market[0])
                            symbol = str(market[1]).upper()
                            name = str(market[2]).upper()
                            exchange = str(market[3]).upper()
                            #exchange_last_price = str("%.9f" % Decimal(market[5]))
                            if (not self.is_empty(market[4])) and (market[4] is not None) :
                                cmc_price_usd = str("%.9f" % Decimal(market[4]))
                            else:
                                cmc_price_usd = "-"
                            marketname = str(market[5]).upper()
                            
                            if not rank == "99999":
                                user_message = user_message + "\n\nExchange : <b>" + exchange + "</b>\nMarket Name : <b>"+marketname +"</b>\nSymbol : "+symbol+"\nName : "+name+"\nRank : "+ rank +"\nActual Price in USD : " + cmc_price_usd
                            else:
                                user_message = user_message + "\n\nExchange : <b>" + exchange + "</b>\nMarket Name : <b>"+marketname+"</b>"
                        
                    if (not self.is_empty(newMarkets_for_groups)) and (newMarkets_for_groups is not None) :
                        group_message = "New Crypto Added\n"
                        for market in newMarkets_for_groups:
                            exchange = str(market[0]).upper()
                            crypto =  str(market[1]).upper()
                            group_message = group_message + "\nExchange : <b>" + exchange + "</b> | Crypto : <b>" + crypto + "</b>"

                    if not user_message == "-" or not group_message == "-":
                        for user in allUsers:
                            self.chatId = user[0]
                            if self.chatId.startswith("-"):
                                self.message = group_message
                            else:
                                self.message = user_message
                            if self.message.count(",") < 30 and not self.message == "-":    #comma separates markets and more than 30 means new exchange was added and trying to send message by mistake
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
                        self.message = " "
                        flag = 0
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
                                    self.message = self.message + "\n\n" + "<a href='https://twitter.com/"+twitter_screen_name+"/status/"+tweet_id+"'>"+coin_symbol +"("+coin_name+") tweeted : </a>"
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    self.back_to_menu_keyboard()
                                    self.sendTelegramMessage()
                                    self.message = " "
                                    flag = 0
                                elif alert_type == "p_watch":
                                    tweet_id = str("%.2f" % Decimal(alert[7]))    #HERE tweet_id contains the percentage change in price
                                    if tweet_id.startswith("-"):
                                        self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) decreases &#128315; by " + tweet_id + "%"
                                    else:
                                        self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) increases &#9650; by " + tweet_id + "%"
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    flag = 1
                                    #self.back_to_menu_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "p_incr":
                                    self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) increases &#9650; to " + new_price + " " + price_in.upper() + " on " + exchange + " exchange."
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    flag = 1
                                    #self.back_to_menu_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "p_decr":
                                    self.message = self.message + "\n" + "Price of " + coin_symbol.upper() + " ( " + coin_name + " ) decreases &#128315; to " + new_price + " " + price_in.upper() + " on " + exchange + " exchange."
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    flag = 1
                                    #self.back_to_menu_keyboard()
                                    #self.sendTelegramMessage()
                                elif alert_type == "special_tweet" and (not self.chatId.startswith("-")):
                                    self.message = self.message + "\n\n" + "<a href='https://twitter.com/"+twitter_screen_name+"/status/"+tweet_id+"'>"+coin_symbol +"("+coin_name+") tweeted something related to FORK or REBRANDING : </a>"
                                    self.db.delete_send_alert(self.chatId, alert_id)
                                    self.back_to_menu_keyboard()
                                    self.sendTelegramMessage()
                                    self.message = " "
                                    flag = 0

                            if flag == 1:
                                self.back_to_menu_keyboard()
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
            self.db.closeConnection()
            sleep(30)

    def on_chat_message(self,msg):
        try:
            self.groupSilence = False  #to stop replying on messages in groups which are not commands related to this bot
            self.fetchData(msg)
            if self.chatId > 0 and self.chatId not in self.LAST_COMMAND_MAP and not self.text.startswith("/") :
                self.back_to_menu_keyboard()
                self.message = "Incorrect command\nPlease choose from below menu"
                self.sendTelegramMessage()
            else:
                self.db = DBHelper()
                self.setFetchTime()
                #print "Offset : " + str(self.lastOffset)
                self.handleUpdate()
                self.addBotMessageInDB()
                if self.groupSilence == False:
                    self.sendTelegramMessage()
                self.db.closeConnection()
        except Exception as e:
            print "Error while processing chat message"
            print(e)      
            with open(COMMON.errorDir + TELEGRAM.errorFileName,'a+') as f:
                f.write("\n\nError : ")
                f.write(e.__doc__)
                f.write(e.message)  
                f.write(self.message)