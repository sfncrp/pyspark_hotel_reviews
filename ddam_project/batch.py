import os
import toLog

log = toLog.log('starting batch')

# activating py3 virtual environment
#os.system(' source ~/env-py3/bin/activate ')

# os.system('jupyter nbconvert --to script PARTE_1_etl_process.ipynb ')
# os.system('python PARTE_1_etl_process.py')


os.system('jupyter nbconvert --to script PARTE_2_Feature_Extraction.ipynb ')
os.system('python PARTE_2_Feature_Extraction.py')

log.toLog('ended batch')
log.close()



