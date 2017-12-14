#import sqlite3
import MySQLdb
from config import MYSQL
from config import SQL_Scripts

class DBHelper:

    def __init__(self):
        print "Inside DBHelper constructor"
        try:
            self.conn = MySQLdb.connect(host = MYSQL.HOST, user = MYSQL.USER, passwd = MYSQL.PASSWORD, db = MYSQL.DBNAME)
            print "After setting connectino with DB"
            self.conn.autocommit(True)
            print "Setting cursor now"
            self.DB = self.conn.cursor()
        except Exception as e: 
            print(e) 

    def setup(self):
        setup_script=SQL_Scripts.setup_script_path
        self.executeScriptsFromFile(setup_script)

    def create_denorm_and_alerts(self):
        setup_script=SQL_Scripts.denorm_and_alerts_script_path
        self.executeScriptsFromFile(setup_script)

    def checkUser(self, chatId):
        rowsCount = 0
        try:
            rowsCount = self.DB.execute("""SELECT chatId from botMessages where chatId=%s""",[chatId])
        except Exception as e: 
            print(e) 
        if ( rowsCount > 0 ):
            return 'old'
        else:
            return 'new'

    def getLastOffset(self):
        try:
            self.DB.execute("""SELECT MAX(offsetId) from botMessages""")
        except Exception as e: 
            print(e) 

        lastOffsets = self.DB.fetchall()
        for lastOffset in lastOffsets:
            if lastOffset[0] is None:
                lastOffset=-1
            else:
                lastOffset = lastOffset[0]
        return lastOffset

    def addBotMessage(self, chatId ,firstName, category , offsetId, fetchTime, text):
        ##print "Inside addBotMessage -- DBHELPER"
        try:
            self.DB.execute("""INSERT INTO botMessages (chatId, firstName, category, offsetId, fetchTime, message) VALUES (%s,%s,%s,%s,%s,%s)""", (chatId ,firstName, category , offsetId, fetchTime, text))
            self.conn.commit()
        except Exception as e: 
            print(e)

    def addBittrex(self,MarketName,High,Low,Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime):
        try:
            self.DB.execute("""INSERT INTO bittrex VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(MarketName,float(High),float(Low),Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addPoloniex(self,currencySymbol,idd,name,disabled,delisted,frozen,fetchTime):
        try:
            self.DB.execute("""INSERT INTO poloniex VALUES (%s,%s,%s,%s,%s,%s,%s)""",(currencySymbol,idd,name,disabled,delisted,frozen,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addBitfinex(self,marketName,mid,bid,ask,last_price,low,high,volume,timestamp,fetchTime):
        try:
            self.DB.execute("""INSERT INTO bitfinex VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(marketName,float(mid),float(bid),float(ask),float(last_price),float(low),float(high),float(volume),timestamp,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addCoinMarketCap(self,idd,name,symbol,rank,price_usd,price_btc,h24_volume_usd,market_cap_usd,available_supply,total_supply,percent_change_1h,percent_change_24h,percent_change_7d,last_updated,fetchTime):
        try:
            self.DB.execute("""INSERT INTO coinmarketcap VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(idd,name,symbol,int(rank),float(price_usd),float(price_btc),float(h24_volume_usd),float(market_cap_usd),float(available_supply),float(total_supply),float(percent_change_1h),float(percent_change_24h),float(percent_change_7d),last_updated,fetchTime))
            self.conn.commit()
        except Exception as e:
            print rank
            print(e) 

    def insertIntoTweets(self, tweet_id, screen_name, created_at, tweet, inReplyToScreenName, fetchTime):
        try:
            self.DB.execute("""INSERT INTO tweets (tweet_id, screen_name, created_at, tweet, inReplyToScreenName, fetchTime) VALUES (%s,%s,%s,%s,%s,%s)""",(tweet_id,screen_name, created_at, tweet, inReplyToScreenName, int(fetchTime) ))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def deleteFromDB_BKPonFetchTime(self,tablename , delTillFetchTime):
        try:
            self.DB.execute("""Delete from """+ tablename + """_BKP where fetchTime <= %s""", [delTillFetchTime] )
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def deleteFromDB_oldData(self,tablename):
        if tablename == "coinmarketcap":
            col1 = "id"
        else:
            col1 = "marketname"
        try:
            self.DB.execute("""INSERT INTO """+  tablename + """_BKP SELECT * FROM """ + tablename)
            self.DB.execute("""INSERT INTO """+  tablename + """_dn_ld select * from """ + tablename)
            self.DB.execute("""DELETE FROM """+  tablename + """_t1""")
            self.DB.execute("""INSERT INTO """+  tablename + """_t1 select """ + col1 + """ , max(fetchTime) as fetchTime from """ + tablename +"""_dn_ld group by """+ col1)
            self.DB.execute("""DELETE FROM """+  tablename + """_dn_ld where fetchTime NOT IN ( select fetchTime from """ + tablename +"""_t1 group by fetchTime)""")
            self.DB.execute("""DELETE FROM """+  tablename + """_dn_ld where (""" + col1 + """,fetchTime) NOT IN ( select """ + col1 + """ , fetchTime from """ + tablename +"""_t1 group by """+ col1 + """, fetchTime)""")
            self.DB.execute("""DELETE FROM """+  tablename )
            
            self.DB.execute("RENAME TABLE "+ tablename +"_dn TO " + tablename + "_dn_md")
            self.DB.execute("RENAME TABLE "+ tablename +"_dn_ld TO " + tablename + "_dn")
            self.DB.execute("RENAME TABLE "+ tablename +"_dn_md TO " + tablename + "_dn_ld")
            self.DB.execute("DELETE FROM "+  tablename +"_dn_ld" )
            self.DB.execute("INSERT INTO "+  tablename + "_dn_ld SELECT * FROM " + tablename +"_dn" )

            #self.DB.execute("""Delete from """+ tablename + """ where fetchTime <= %s""", [delTillFetchTime] )
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def getAllUsers(self):
        try:
            self.DB.execute("SELECT chatId from botMessages where category=\"g\" GROUP BY chatId")
            chatIds = self.DB.fetchall()
        except Exception as e: 
            print(e) 
        return chatIds
    def getAlerts(self):
        try:
            self.DB.execute("SELECT alert_number,chatId,alert_type,coin_symbol,alert_price,price_in,twitter_screen_name,tweet_id,coin_name,exchange,new_price FROM send_alerts")
            alerts = self.DB.fetchall()
            return alerts
        except Exception as e: 
            print(e) 
                
    def fetchCurrencyName(self,currencySymbol):
        try:
            self.DB.execute("select name from price_denorm where symbol=%s group by name",[currencySymbol])
            currencyNames = self.DB.fetchall()
        except Exception as e: 
            print(e) 
        return currencyNames
    
    def fetchTweet(self,currencySymbol):
        currencySymbol=str(currencySymbol).lower()
        try:
            self.DB.execute("SELECT name,twitter_screen_name,tweet_id FROM price_denorm WHERE lower(symbol) = %s group by name,twitter_screen_name,tweet_id limit 2",[currencySymbol])
            tweets = self.DB.fetchall()
        except Exception as e: 
            print(e) 
        return tweets    


    def fetchPrice(self,currencySymbol):
        currencySymbol = str(currencySymbol).lower()
        try:
            self.DB.execute("SELECT name,exchange,exchange_last_price,upper(exchange_last_price_in),cmc_price_usd FROM price_denorm WHERE lower(symbol) = lower(%s) and exchange_last_price_in =\"btc\" ",[currencySymbol])
            prices = self.DB.fetchall()
            return prices       
        except Exception as e: 
            print(e) 
    
    def add_alert(self,chatId,alert_type,fetchTime,currencySymbol,is_first,alert_price,price_in):
        try:
            self.DB.execute("""INSERT INTO alerts_subscription(chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in) VALUES (%s,%s,%s,%s,%s,%s,%s)""",[chatId,alert_type,fetchTime,currencySymbol,is_first,alert_price,price_in])
            self.conn.commit()
        except Exception as e: 
            print(e) 
    
    def my_alerts(self,chatId):
        try:
            print "fetching data"
            self.DB.execute("SELECT id,chatId,alert_type,coin_symbol,alert_price,price_in FROM alerts_subscription_dn WHERE chatId=%s",[chatId] )
            print "DATA FETCHED........"
            alerts = self.DB.fetchall()
            return alerts
        except Exception as e: 
            print(e) 

    def delete_alert(self,chatId,alert_id):
        try:
            self.DB.execute("""DELETE FROM alerts_subscription WHERE chatId=%s AND id=%s""",[chatId,alert_id])
            self.DB.execute("""DELETE FROM alerts_subscription_dn_ld WHERE chatId=%s AND id=%s""",[chatId,alert_id])
            self.DB.execute("""DELETE FROM alerts_subscription_dn WHERE chatId=%s AND id=%s""",[chatId,alert_id])
            self.conn.commit()
        except Exception as e: 
            print(e)         

    def delete_send_alert(self,chatId,alert_id):
        try:
            self.DB.execute("""DELETE FROM send_alerts WHERE chatId=%s AND alert_number=%s""",[chatId,alert_id])
            self.conn.commit()
        except Exception as e: 
            print(e)         


    def closeConnection(self):
        try:
            self.conn.close()
        except Exception as e: 
            print(e) 

    def get_newMarketListings(self):
        try:
            self.DB.execute("SELECT rank,symbol,name,exchange,marketname,exchange_last_price,cmc_price_usd FROM price_denorm WHERE is_new_market = \"new\" GROUP BY rank,symbol,name,exchange,marketname,exchange_last_price,cmc_price_usd")
            newMarkets = self.DB.fetchall()
            return newMarkets
        except Exception as e: 
            print(e) 

    def update_priceDenorm_marketTypes(self):
        try:
            self.DB.execute("UPDATE price_denorm SET is_new_market = \"old\" WHERE is_new_market = \"new\"")
            self.DB.execute("UPDATE price_denorm_ld SET is_new_market = \"old\" WHERE is_new_market = \"new\"")
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def executeScriptsFromFile(self,filename):
        # Open and read the file as a single buffer
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()

        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')


        # Execute every command from the input file
        for command in sqlCommands:
            print "Running commnad --> " + command
            if command is None or len(command) < 2: 
                continue
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                self.DB.execute(command)
                #chatIds = DB.fetchall()
                #print chatIds
                self.conn.commit()
            except Exception as msg:
                print "Command skipped: ", msg


    

"""    def getNewListingsBittrex(self):
        #print "getNewListings -- DBHelper from Telegram"
        self.DB.execute("SELECT marketname,volume,bid,ask,openbuyorders,opensellorders,fetchTime FROM bittrex group by marketname having count(marketname)=1")
        newMarkets = self.DB.fetchall()
        return newMarkets

    def getNewListingsBitfinex(self):
        print "getNewListings -- DBHelper from Telegram"
        self.DB.execute("SELECT marketname,bid,ask,low,high,volume,fetchTime FROM bitfinex group by marketname having count(marketname)=1")
        newMarkets = self.DB.fetchall()
        return newMarkets
 
    def getNewListingsPoloniex(self):
        print "getNewListings -- DBHelper from Telegram"
        self.DB.execute("SELECT currencySymbol,name,fetchTime FROM poloniex WHERE disabled=0 AND delisted=0 AND frozen=0 group by currencySymbol having count(currencySymbol)=1")
        newMarkets = self.DB.fetchall()
        return newMarkets
    
    def insertIntoBittrex_DuplicateRow(self, marketName , fetchTime):
        self.DB.execute("INSERT INTO bittrex (marketname, fetchTime) VALUES (%s,%s),(marketName , int(fetchTime)-1 ))
        self.conn.commit()

    def insertIntoBitfinex_DuplicateRow(self, marketName , fetchTime):
        self.DB.execute(INSERT INTO bitfinex (marketname, fetchTime) VALUES (%s,%s),(marketName , int(fetchTime)-1 ))
        self.conn.commit()

    def insertIntoPoloniex_DuplicateRow(self, currencySymbol , fetchTime):
        self.DB.execute(INSERT INTO poloniex (currencySymbol, fetchTime) VALUES (%s,%s),(currencySymbol , int(fetchTime)-1 ))
        self.conn.commit()
"""
