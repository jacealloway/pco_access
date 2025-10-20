try:
    import numpy as np 
    import pandas as pd 
    import requests 
    from datetime import date, datetime, timedelta
    import sys 
    import time 
    from tqdm import tqdm 
    from googleapiclient import errors, discovery
    from google.oauth2 import service_account
    import json 
except ModuleNotFoundError:
    print("Ensure all packages are installed. Consult 'requirements.txt'.")

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


# Try to load planning centre API URL
try:
    SECRET = np.loadtxt(f'apikey/keys.txt', dtype = str)    #load passwords
    API_APP_ID = f"{SECRET[0]}"     #username
    API_SECRET = f"{SECRET[1]}"     #password key for API
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    sys.exit()


from ast import literal_eval


# test = pd.read_csv('TEMP_FILES/oct11_results.csv')
# test = test[test['workflow_name'] == 'DT NEW PEOPLE FOLLOW UP']
# test = test[test['current_step_name'] == 'Initial Followup']
# test.drop_duplicates(subset = ['card_id'], inplace = True)
# test.reset_index(drop = True, inplace = True)

# # print(test)
# print(len(test))




test = pd.read_csv('TEMP_FILES/DF_GROUPS_ALL_UNPROCESSED.csv')
# print(type(test['phone_numbers'].values[0]), type(test['email_addresses'].values[0]))
list_test = test['email_address'].values[0]
print(list_test)
print() 
extract = literal_eval(list_test)
print(extract[0]['address'])