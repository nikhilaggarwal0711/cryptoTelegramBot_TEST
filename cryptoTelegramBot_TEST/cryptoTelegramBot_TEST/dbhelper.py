#import sqlite3
import MySQLdb
from config import MYSQL,COMMON
from config import SQL_Scripts

class DBHelper:

    def __init__(self):
        print "Inside DBHelper constructor"
        try:
            self.conn = MySQLdb.connect(host = MYSQL.HOST, user = MYSQL.USER, passwd = MYSQL.PASSWORD, db = MYSQL.DBNAME)
            print "After setting connection with DB"
            self.conn.autocommit(True)
            print "Setting cursor now"
            self.DB = self.conn.cursor()
        except Exception as e: 
            print "Error while starting curson from DBHelper constructor : DBHelper"
            print(e) 
            print(e.message)

    def setup(self):
        setup_script = COMMON.sqlDir + SQL_Scripts.setup_script
        self.executeScriptsFromFile(setup_script)

    def create_denorm_and_alerts(self):
        setup_script = COMMON.sqlDir + SQL_Scripts.denorm_and_alerts_script
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

    def addBotMessage(self, chatId ,firstName, category , offsetId, fetchTime, text, userId, username_user, username_group):
        ##print "Inside addBotMessage -- DBHELPER"
        try:
            self.DB.execute("""INSERT INTO botMessages (chatId, firstName, category, offsetId, fetchTime, message, userId, username_user, username_group) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (chatId ,firstName, category , offsetId, fetchTime, text, userId, username_user, username_group))
            self.conn.commit()
        except Exception as e: 
            print(e)

    def addCryptopia(self,marketname,last_price,BaseVolume,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO cryptopia VALUES (%s,%s,%s,%s,%s)""",(marketname,float(last_price),BaseVolume,volume,fetchTime))
            self.conn.commit()
        except Exception as e:
            print "Exception caught while Inserting data in cryptopia table"
            print(e)
            print e.message

    def addKucoin(self,coinType,symbol,lastDealPrice,volValue,vol,fetchTime):
        #coinType --> symbol in DB table
        #symbol --> marketname in DB table
        try:
            self.DB.execute("""INSERT INTO kucoin VALUES (%s,%s,%s,%s,%s,%s)""",(coinType,symbol,float(lastDealPrice),volValue,vol,fetchTime))
            self.conn.commit()
        except Exception as e:
            print "Exception caught while Inserting data in kucoin table"
            print(e)
            print e.message

    def addBittrex(self,MarketName,High,Low,Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime):
        try:
            self.DB.execute("""INSERT INTO bittrex VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(MarketName,float(High),float(Low),Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addBinance(self,MarketName,Price,quoteVolume,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO binance VALUES (%s,%s,%s,%s,%s)""",(MarketName,float(Price),quoteVolume,volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addBitbns(self,MarketName,Price,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO bitbns VALUES (%s,%s,%s,%s)""",(MarketName,float(Price),volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addZebpay(self,MarketName,Price,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO zebpay VALUES (%s,%s,%s,%s)""",(MarketName,float(Price),volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addKoinex(self,MarketName,Price,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO koinex VALUES (%s,%s,%s,%s)""",(MarketName,float(Price),volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addCoindelta(self,MarketName,Price,fetchTime):
        try:
            self.DB.execute("""INSERT INTO coindelta VALUES (%s,%s,%s)""",(MarketName,float(Price),fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addWazirx(self,MarketName,Price,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO wazirx VALUES (%s,%s,%s,%s)""",(MarketName,float(Price),volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 


    def addUnocoin(self,MarketName,Price,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO unocoin VALUES (%s,%s,%s,%s)""",(MarketName,float(Price),volume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addIdex(self,MarketName,Price,baseVolume,quoteVolume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO idex VALUES (%s,%s,%s,%s,%s)""",(MarketName,float(Price),baseVolume,quoteVolume,fetchTime))
            self.conn.commit()
        except Exception as e: 
            print(e) 

    def addHitbtc(self,MarketName,Price,volumeQuote,volume,fetchTime):
        try:
            self.DB.execute("""INSERT INTO hitbtc VALUES (%s,%s,%s,%s,%s)""",(MarketName,float(Price),volumeQuote,volume,fetchTime))
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
            print "Exception caught while inserting data -- Bitfinex"
            print(e) 
            print e.message
            print marketName,mid,bid,ask,last_price,low,high,volume,timestamp,fetchTime

    def addCoinMarketCap(self,idd,name,symbol,rank,price_usd,price_btc,h24_volume_usd,market_cap_usd,available_supply,total_supply,percent_change_1h,percent_change_24h,percent_change_7d,last_updated,fetchTime):
        try:
            self.DB.execute("""INSERT INTO coinmarketcap VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(idd,name,symbol,int(rank),float(price_usd),float(price_btc),float(h24_volume_usd),float(market_cap_usd),float(available_supply),float(total_supply),float(percent_change_1h),float(percent_change_24h),float(percent_change_7d),last_updated,fetchTime))
            self.conn.commit()
        except Exception as e:
            print "Exception caught while inserting data -- coinmarketcap"
            print "RANK --" + str(rank)
            print(e) 
            print e.message
            print idd,name,symbol,rank,price_usd,price_btc,h24_volume_usd,market_cap_usd,available_supply,total_supply,percent_change_1h,percent_change_24h,percent_change_7d,last_updated,fetchTime

    def insertIntoTweets(self, tweet_id, screen_name, created_at, inReplyToScreenName,tweet, fetchTime):
        try:
            self.DB.execute("""INSERT INTO tweets (tweet_id, screen_name, created_at, inReplyToScreenName, tweet, fetchTime) VALUES (%s,%s,%s,%s,%s,%s)""",(tweet_id,screen_name, created_at, inReplyToScreenName,tweet, int(fetchTime) ))
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
            col2 = "fetchTime"
            self.DB.execute("DELETE FROM "+  tablename + "_dn_ld") #Deleting data from ld otherwise old ids were getting stuck in table.. need to generalise this below.
        elif tablename == "tweets":
            col1 = "screen_name"
            col2 = "tweet_id"
        else:
            col1 = "marketname"
            col2 = "fetchTime"
        try:
            self.DB.execute("DELETE FROM "+  tablename + "_t1")
            self.DB.execute("INSERT INTO "+  tablename + "_t1 SELECT * FROM " + tablename)
            self.DB.execute("DELETE FROM "+  tablename )
            self.DB.execute("INSERT INTO "+  tablename + "_BKP SELECT * FROM " + tablename + "_t1")

            self.DB.execute("DELETE FROM "+  tablename + "_dn_ld WHERE " + col1 +" IN (select " + col1 +"  FROM " + tablename + "_t1 GROUP BY " + col1 +")")
            self.DB.execute("INSERT INTO "+  tablename + "_dn_ld SELECT * FROM " + tablename + "_t1 WHERE (" + col1 + "," + col2 + ") IN ( SELECT " + col1 + ",MAX(" + col2 + ") AS " + col2 + " FROM " + tablename + "_t1 GROUP BY " + col1 + ")")

            #self.DB.execute("INSERT INTO "+  tablename + "_dn_ld select * FROM " + col1 + "_t1 , max(" + col2 + ") as " + col2 + " from " + tablename +"_dn_ld group by "+ col1)

            #self.DB.execute("DELETE FROM "+  tablename + "_dn_ld where " + col2 + " NOT IN ( select " + col2 + " from " + tablename +"_t1 group by " + col2 + ")")
            #self.DB.execute("DELETE FROM "+  tablename + "_dn_ld where (" + col1 + "," + col2 + ") NOT IN ( select " + col1 + " , " + col2 + " from " + tablename +"_t1 group by "+ col1 + ", " + col2 + ")")
            #self.DB.execute("DELETE FROM "+  tablename )

            self.DB.execute("RENAME TABLE "+ tablename +"_dn TO " + tablename + "_dn_md")
            self.DB.execute("RENAME TABLE "+ tablename +"_dn_ld TO " + tablename + "_dn")
            self.DB.execute("RENAME TABLE "+ tablename +"_dn_md TO " + tablename + "_dn_ld")
            self.DB.execute("DELETE FROM "+  tablename +"_dn_ld" )
            self.DB.execute("INSERT INTO "+  tablename + "_dn_ld SELECT * FROM " + tablename +"_dn" )

            #self.DB.execute("""Delete from """+ tablename + """ where fetchTime <= %s""", [delTillFetchTime] )
            self.conn.commit()
        except Exception as e:
            print "Exception from REFRESH DENORM method" 
            print e.message
            print(e) 
            with open(COMMON.errorDir + COMMON.errorFileName,'a+') as f:
                f.write("\n\nError : ")
                f.write(e.__doc__)
                f.write(e.message)

    def getAllUsers(self):
        try:
            self.DB.execute("SELECT chatId from botMessages where category=\"g\" GROUP BY chatId")
            chatIds = self.DB.fetchall()
            return chatIds
        except Exception as e: 
            print(e) 

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
            return currencyNames
        except Exception as e: 
            print(e) 
    
    def fetchTweet(self,currencySymbol):
        currencySymbol=str(currencySymbol).lower()
        try:
            self.DB.execute("SELECT name,twitter_screen_name,tweet_id FROM price_denorm WHERE lower(symbol) = %s group by name,twitter_screen_name,tweet_id limit 2",[currencySymbol])
            tweets = self.DB.fetchall()
            return tweets    
        except Exception as e: 
            print(e) 


    def fetchPrice(self,currencySymbol):
        currencySymbol = str(currencySymbol).lower()
        print "Inside fetchPrice method" + str(currencySymbol)
        try:
            #self.DB.execute("SELECT name,exchange,exchange_last_price,upper(exchange_last_price_in),cmc_price_usd FROM price_denorm WHERE lower(symbol) = lower(%s) and exchange_last_price_in =\"btc\" ",[currencySymbol])
            self.DB.execute("SELECT symbol, name , cmc_price_usd , cmc_price_btc , cmc_percent_change_24h , cmc_market_cap_usd , cmc_24h_volume_usd , exchange , exchange_last_price, upper(exchange_last_price_in) , cmc_percent_change_1h , cmc_percent_change_7d, rank FROM price_denorm WHERE lower(symbol) = lower(%s) and (exchange_last_price_in =\"btc\" or ( exchange_last_price_in =\"eth\" and exchange =\"idex\")) order by rank,exchange_last_price_in",[currencySymbol])
            print "Inside try block : After executing query"
            prices = self.DB.fetchall()
            print "fetched result" + str(prices)
            return prices
        except Exception as e: 
            print(e) 
    
    def fetchPrice_not_on_CMC(self,currencySymbol):
        currencySymbol = str(currencySymbol).lower()
        #print "Inside fetchPrice_not_on_CMC method" + str(currencySymbol)
        try:
            #self.DB.execute("SELECT name,exchange,exchange_last_price,upper(exchange_last_price_in),cmc_price_usd FROM price_denorm WHERE lower(symbol) = lower(%s) and exchange_last_price_in =\"btc\" ",[currencySymbol])
            self.DB.execute("SELECT exchange, marketname, exchange_last_price, exchange_last_price_in FROM price_denorm WHERE id = \"-\" and marketname like %s ORDER BY exchange, exchange_last_price_in",("%" + currencySymbol + "%",))
            #print "Inside try block : After executing query"
            prices = self.DB.fetchall()
            #print "fetched result" + str(prices)
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
            #print "fetching data"
            self.DB.execute("SELECT id,chatId,alert_type,coin_symbol,alert_price,price_in FROM alerts_subscription_dn WHERE chatId=%s order by chatId,alert_type,id,coin_symbol,alert_price,price_in",[chatId] )
            #print "DATA FETCHED........"
            alerts = self.DB.fetchall()
            return alerts
        except Exception as e: 
            print(e) 

    def top_coins(self):
        try:
            #print "fetching data"
            self.DB.execute("SELECT symbol,name,cmc_percent_change_24h from price_denorm where rank<200  group by symbol, name,cmc_percent_change_24h order by cmc_percent_change_24h desc limit 10")
            #print "DATA FETCHED........"
            alerts = self.DB.fetchall()
            return alerts
        except Exception as e: 
            print(e) 

    def bottom_coins(self):
        try:
            #print "fetching data"
            self.DB.execute("SELECT symbol,name,cmc_percent_change_24h from price_denorm where rank<200  group by symbol, name,cmc_percent_change_24h order by cmc_percent_change_24h asc limit 10")
            #print "DATA FETCHED........"
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
            self.DB.close()
        except Exception as e: 
            print "ERROR FOUND DURING CLOSING CONNECTION"
            print(e) 

    def get_newMarketListings(self):
        try:
            self.DB.execute("SELECT rank,symbol,name,exchange,cmc_price_usd,GROUP_CONCAT(marketname SEPARATOR ', ') AS marketname FROM price_denorm WHERE is_new_market = \"yes\" GROUP BY rank,symbol,name,exchange,cmc_price_usd")
            newMarkets = self.DB.fetchall()
            return newMarkets
        except Exception as e: 
            print(e) 

    def get_newMarketListings_for_groups(self):
        try:
            self.DB.execute("SELECT F_DN.exchange , GROUP_CONCAT(F_DN.symbol SEPARATOR ', ') AS symbol FROM (SELECT N_DN.exchange, N_DN.symbol FROM (select N_MKT.exchange , N_MKT.symbol , P_DN.is_new_market FROM (select exchange , symbol from price_denorm where is_new_market = \"yes\" and exchange <> \"Coinmarketcap\" group by exchange , symbol) N_MKT JOIN (select exchange , symbol , is_new_market from price_denorm group by exchange ,symbol ,is_new_market) P_DN ON  N_MKT.exchange = P_DN.exchange AND N_MKT.symbol = P_DN.symbol ) N_DN group by N_DN.exchange , N_DN.symbol  having count(N_DN.is_new_market)=1) F_DN GROUP BY F_DN.exchange")
            newMarkets = self.DB.fetchall()
            return newMarkets
        except Exception as e: 
            print(e) 

    def update_priceDenorm_marketTypes(self):
        try:
            self.DB.execute("UPDATE price_denorm    SET is_new_market = \"no\" WHERE is_new_market = \"yes\"")
            self.DB.execute("UPDATE price_denorm_ld SET is_new_market = \"no\" WHERE is_new_market = \"yes\"")
            self.DB.execute("UPDATE price_denorm_t1 SET is_new_market = \"no\" WHERE is_new_market = \"yes\"")
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
            #print "Running commnad --> " + command
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
