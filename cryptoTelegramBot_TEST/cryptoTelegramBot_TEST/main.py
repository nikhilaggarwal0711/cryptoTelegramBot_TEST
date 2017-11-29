import json
import requests
import time
import urllib
from myThread import MyThread

import config



def main():
    print "inside main "

#    last_update_id = None
    print "Starting Thread"
    thread1 = MyThread("coinMarketCap",60)
    thread2 = MyThread("bittrex",43200)
    thread3 = MyThread("telegram",60)
#    thread4 = myThread("telegram",60)

    print "Starting Threads"
    #start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
#    thread4.start()

    #Waiting for Threads to finish
    thread1.join()
    thread2.join()
    thread3.join()
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