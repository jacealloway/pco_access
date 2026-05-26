# Isolate specific methods from the Exporter class in PCO_ETL.py
# Import packages 
try:
    from PCO_ETL_LIB_LOCALDEV import * 
    import numpy as np 
    import pandas as pd 
    import sys 
    from datetime import timedelta, datetime 
except ModuleNotFoundError:
    print("Ensure all packages are installed. Consult 'requirements.txt'.")



# Try to load planning centre API URL
try:
    SECRET = np.loadtxt(f'apikey/keys.txt', dtype = str)   
    API_APP_ID = f"{SECRET[0]}"     
    API_SECRET = f"{SECRET[1]}"   
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    sys.exit()



# Add importer construct for other files 
if __name__ == "__main__": 
    # GET ALL DATA
    _ENGINE_ = Exporter(api_app_id = API_APP_ID, api_secret = API_SECRET)

    PPL_DF = _ENGINE_.rosterDFGenerator()
    PPL_DF.to_csv('TEMP_FILES/rosters_test.csv')


