#import sqlite3
import MySQLdb
import user
from config import MYSQL

class DBHelper:

    def __init__(self):
        print "Inside DBHelper constructor"
        self.conn = MySQLdb.connect(host = MYSQL.HOST, user = MYSQL.USER, passwd = MYSQL.PASSWORD, db = MYSQL.DBNAME)
        print "Setting cursor now"
        self.DB = self.conn.cursor()

    def setup(self):
        tblstmt1 = "CREATE TABLE IF NOT EXISTS botMessages ( chatId text,firstName text , category text,offsetId text,fetchTime int(11), message text)"
        self.DB.execute(tblstmt1)
        self.conn.commit()
 
        tblstmt2 = "CREATE TABLE IF NOT EXISTS bittrex ( marketname text,high decimal(18,8) ,low decimal(18,8) ,volume decimal(18,8) ,last decimal(18,8) ,basevolume decimal(18,8) ,timestampp text,bid decimal(18,8) ,ask decimal(18,8) ,openbuyorders int(11) ,opensellorders int(11) ,prevday decimal(18,8) ,created text,fetchTime int(11) )"
        self.DB.execute(tblstmt2)
        self.conn.commit()
        
        tblstmt3 = "CREATE TABLE IF NOT EXISTS coinmarketcap ( id text,name text,symbol text,rank int(11) ,price_usd decimal(18,2) ,price_btc decimal(38,6) ,24h_volume_usd decimal(38,8) ,market_cap_usd decimal(38,8) ,available_supply decimal(38,8) ,total_supply decimal(38,8) ,percent_change_1h decimal(38,8) ,percent_change_24h decimal(38,8) ,percent_change_7d decimal(38,8) ,last_updated text,fetchTime int(11) )"
        self.DB.execute(tblstmt3)
        self.conn.commit()

    def checkUser(self, chatId):
        rowsCount = self.DB.execute("""SELECT chatId from botMessages where chatId=%s""",[chatId])
        if ( rowsCount > 0 ):
            return 'old'
        else:
            return 'new'

    def getLastOffset(self):
        self.DB.execute("""SELECT MAX(offsetId) from botMessages""")
        lastOffsets = self.DB.fetchall()
        for lastOffset in lastOffsets:
            if lastOffset[0] is None:
                lastOffset=-1
            else:
                lastOffset = lastOffset[0]
        return lastOffset

    def addBotMessage(self, chatId ,firstName, category , offsetId, fetchTime, text):
        print "Inside addBotMessage -- DBHELPER"
        try:
            self.DB.execute("""INSERT INTO botMessages (chatId, firstName, category, offsetId, fetchTime, message) VALUES (%s,%s,%s,%s,%s,%s)""", (chatId ,firstName, category , offsetId, fetchTime, text))
            self.conn.commit()
        except Exception as e: 
            print(e)

    def addBittrex(self,MarketName,High,Low,Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime):
        self.DB.execute("""INSERT INTO bittrex VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(MarketName,float(High),float(Low),Volume,Last,BaseVolume,TimeStamp,Bid,Ask,OpenBuyOrders,OpenSellOrders,PrevDay,Created,fetchTime))
        self.conn.commit()
    
    def addCoinMarketCap(self,id,name,symbol,rank,price_usd,price_btc,h24_volume_usd,market_cap_usd,available_supply,total_supply,percent_change_1h,percent_change_24h,percent_change_7d,last_updated,fetchTime):
        self.DB.execute("""INSERT INTO coinmarketcap VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(id,name,symbol,int(rank),float(price_usd),float(price_btc),float(h24_volume_usd),float(market_cap_usd),float(available_supply),float(total_supply),float(percent_change_1h),float(percent_change_24h),float(percent_change_7d),last_updated,fetchTime))
        self.conn.commit()

    def deleteFromDB_fetchTime(self,tablename , delTillFetchTime):
        self.DB.execute("""Delete from """+ tablename + """ where fetchTime <= %s""", [delTillFetchTime] )
        self.conn.commit()

    def getNewListings(self):
        self.DB.execute("SELECT marketname,volume,bid,ask,openbuyorders,opensellorders FROM bittrex group by marketname having count(marketname)=1")
        newMarkets = self.DB.fetchall()
        return newMarkets
    
    def getAllUsers(self):
        self.DB.execute("SELECT chatId from botMessages where category=\"g\" GROUP BY chatId")
        chatIds = self.DB.fetchall()
        return chatIds
    