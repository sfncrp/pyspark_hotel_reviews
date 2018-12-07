import time
import datetime

def tolog(time_start, logtext):
    now = time.time()
    elapsed = now - time_start
    with open('log.txt', 'a') as f:
    f.write( str(datetime.datetime.now())+ ' elapsed: '+str(round(nbtime/60))+ ' minutes :' + logtext + '\n')

