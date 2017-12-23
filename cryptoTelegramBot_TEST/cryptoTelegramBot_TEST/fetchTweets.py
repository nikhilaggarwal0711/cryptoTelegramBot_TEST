#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
from time import sleep
from dbhelper import  DBHelper
import json
from config import Twitter,COMMON
#Variables that contains the user credentials to access Twitter API 


#This is a basic listener that just prints received tweets to stdout.
class FetchTweets:
    def __init__(self):
        #print "inside Twitter constructor"
        self.db = DBHelper()
        #self.db.setup()
    
    def setFetchTime(self):
        self.fetchTime = int(time.time())


    def setDelTillFetchTime(self):
        #print "delTillFetchTime -- Twitter"
        #1day = 86400
        deleteTime = 2592000
#        deleteTime = 1
        self.delTillFetchTime = self.fetchTime - deleteTime

    def deleteFromDB_fetchTime(self):
        #print "deleteFromDB_fetchTime -- Twitter"
        self.db.deleteFromDB_fetchTime("tweets",self.delTillFetchTime)


    def start(self,sleepTime):
        #print "Start method -- Twitter"
        self.sleepTime = sleepTime
        while True:
            try:
                print "Waiting for " + str(self.sleepTime) + " seconds before trying .... "
                time.sleep(self.sleepTime)
                #This handles Twitter authentication and the connection to Twitter Streaming API
                listener = StdOutListener()
                auth = OAuthHandler(Twitter.consumer_key, Twitter.consumer_secret)
                auth.set_access_token(Twitter.access_token, Twitter.access_token_secret)
                stream = Stream(auth, listener)
                #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
                #stream.filter(follow=['@nikhilCrypto'])

                try:
                    stream.userstream(_with='nikhilCrypto')
                except Exception, e:
                    print "Error. Restarting Stream.... Error: Inside fetchTweets"
                    print e.__doc__
                    print e.message
                    self.sleepTime = self.sleepTime * 2

                #self.setDelTillFetchTime()
                #self.deleteFromDB_fetchTime()
                #self.sleepTime = sleepTime
            except Exception as e: 
                print(e)
                print e.message
                self.sleepTime = 2 * self.sleepTime
                print "exception caught in while loop -- Twitter"
            sleep(self.sleepTime)

class StdOutListener(StreamListener):
        #A listener handles tweets that are received from the stream. This is a basic listener that just prints received tweets to stdout.
        def on_data(self, data):
                #print data
                try:
                    #db = DBHelper()
                    #db.setup()
                    config = json.loads(data)
                    tweet_id=str(config["id"]).encode('utf-8')
                    screen_name = config["user"]["screen_name"].encode('utf-8')
                    created_at = config["created_at"].encode('utf-8')
                    #tweet = config["text"].encode('utf-8')
                    inReplyToScreenName = config["in_reply_to_screen_name"]
                    
                    #config["retweeted_status"]["full_text"]
                    #Keeping it as full_text can be mising in text variable above. and can be fetched using this one.
                    #But this is for retweeted tweet. Need to look into tweet which is big but was not retweeted.

                    fetchTime = int(time.time())
                    
                    #print "inReplyToScreenName --> " + str(inReplyToScreenName)
                    if inReplyToScreenName is not None:
                        inReplyToScreenName = inReplyToScreenName.encode('utf-8') 
                    else:
                        print "insert tweet data"
                        #print data
                        inReplyToScreenName = ""
                        db = DBHelper()
                        db.insertIntoTweets(tweet_id,screen_name,created_at,str(inReplyToScreenName),fetchTime)
                        db.closeConnection()
                        #print "inReplyToScreenName --> " + inReplyToScreenName

                    #db.deleteFromDB_oldData("tweets")
                    #db.closeConnection()
                except Exception, e:
                        print "Error. Inside StdOutListener class.... Error: "
                        print e.__doc__
                        print e.message
                        #db.closeConnection()
                        with open(COMMON.errorDir + Twitter.errorFileName,'a+') as f:
                            f.write("\n\nTwitter Write to DB Error : ")
                            f.write(data)
                            f.write(e.__doc__)
                            f.write(e.message)

                #Writing tweets in file as well, incase miss any in database because of any formating issue
                try:
                    with open(COMMON.tweetDir + Twitter.tweetFileName,'a+') as f:
                        f.write(data)
                except Exception, e:
                    print "Error. While writing Tweets in file.... File location: " + COMMON.tweetDir + Twitter.tweetFileName
                    print e.__doc__
                    print e.message 

                return True
        def on_error(self, status):
                print status
