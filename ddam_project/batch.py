import os
import toLog

log = toLog.log('starting batch')


#os.system(' source ~/env-py3/bin/activate ')

os.system('jupyter nbconvert --to script PARTE_1_etl_process.ipynb ')
    
os.system('python PARTE_1_etl_process.py')

log.toLog('ended batch')
log.close()



