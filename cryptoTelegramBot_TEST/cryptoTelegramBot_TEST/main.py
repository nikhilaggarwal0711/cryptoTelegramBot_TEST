from myThread import MyThread

def main():
    #print "inside main "

#    last_update_id = None
    print "Starting Thread"
    thread1 = MyThread("coinMarketCap",43200)
    thread2 = MyThread("bittrex",60)
    thread3 = MyThread("telegram",0.5)
    thread4 = MyThread("bitfinex",90)
    thread5 = MyThread("poloniex",60)

    #print "Starting Threads"
    #start new Threads
#    thread1.start()
#    thread2.start()
#    thread3.start()
#    thread4.start()
    thread5.start()

    #Waiting for Threads to finish
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    print "Program Ends"
    
def my_long_running_process():
    main()
