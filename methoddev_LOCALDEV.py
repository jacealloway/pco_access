# Isolate specific methods from the Exporter class in PCO_ETL.py
# Import packages 
try:
    from PCO_ETL_LIB import * 
    import numpy as np 
    import pandas as pd 
    import sys 
    from datetime import timedelta, datetime 
except ModuleNotFoundError:
    print("Ensure all packages are installed. Consult 'requirements.txt'.")


#Set environment: LOCAL / SERVER
runpoint = 'LOCAL'
if runpoint == 'LOCAL':
    log_dir = 'LOG/'
    api_dir = 'apikey/'
if runpoint == 'SERVER':
    log_dir = 'User/pco_access/LOG/'
    api_dir = 'User/pco_access/apikey/'
# else:
#     sys.exit()



# Try to load planning centre API URL
try:
    SECRET = np.loadtxt(api_dir + f'keys.txt', dtype = str)   
    API_APP_ID = f"{SECRET[0]}"     
    API_SECRET = f"{SECRET[1]}"   
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    sys.exit()



# Add importer construct for other files 
if __name__ == "__main__": 
    # GET ALL DATA
    _ENGINE_ = Exporter(api_app_id = API_APP_ID, api_secret = API_SECRET)

    PPL_DF = _ENGINE_.checkinsDFGenerator()
    PPL_DF.to_csv('TEMP_FILES/checkins_test.csv', index = False)


