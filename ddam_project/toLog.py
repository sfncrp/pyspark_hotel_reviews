import time
import datetime

class log:
    def __init__(self, logtext = ' '):
        """
        
        """
        self.times = [time.time()]
        self.close(logtext)

    def close(self, logtext = ' '):
        with open('log.txt', 'a') as f:
            f.write('-'*60+'\n')
            f.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")) + ' ' + logtext+ ' \n')
            f.write('-'*60+'\n')

    def toLog(self, logtext):
        now = time.time()
        from_start = now - self.times[0]
        from_prev = now - self.times[-1]
        self.times.append(now)
        with open('log.txt', 'a') as f:
            f.write( str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")) + \
                     ' -- from start: '+str(round(from_start/60))+ ' min' + \
                     ' -- from prev : '+str(round(from_prev/60))+ ' min -- ' + \
                     logtext + ' --\n')
            
