import json
import requests
import time
import urllib
import myThread

import config
from dbhelper import DBHelper



def main():
    print "inside main "
    db = DBHelper()
    db.setup()
#    last_update_id = None
    print "Starting Thread"
    thread1 = myThread("coinMarketCap",60,db)
#    thread2 = myThread("bittrex",43200,db)
#    thread3 = myThread("twitter",60,db)
#    thread4 = myThread("telegram",60,db)

    print "Starting Threads"
    #start new Threads
    thread1.start()
#    thread2.start()
#    thread3.start()
#    thread4.start()

    #Waiting for Threads to finish
    thread1.join()
#    thread2.join()
#    thread3.join()
#    thread4.join()
    print "Program Ends"
    
def my_long_running_process():
    print "Inside long process"
#    if __name__ == '__main__':
    print "startin main"
    main()
    #else:
        #print "inside else"
my_long_running_process()