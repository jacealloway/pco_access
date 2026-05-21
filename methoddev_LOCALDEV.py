# Isolate specific methods from the Exporter class in PCO_ETL.py
# Import packages 
try:
    from PCO_ETL_LIB_LOCALDEV import * 
    import numpy as np 
    import pandas as pd 
    import sys 
    from datetime import timedelta, datetime 
    import logging
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


# Set global API fetch points
SERVICES_BASE = 'https://api.planningcenteronline.com/services/v2'
# Load the exporter class from the primary ETL code 
_EXPORTER_ = Exporter(api_app_id=API_APP_ID, api_secret=API_SECRET)



# # Execute code here 
# if __name__ == "__main__":
#     # _EXPORTER_.checkinsDFGenerator()
#     wf_data_test = _EXPORTER_.workflowDFGenerator()
#     wf_data_test.to_csv('TEMP_FILES/workflows_with_log_entry.csv', index = False)




# Add importer construct for other files 
if __name__ == "__main__": 
    #####----- PUSHING DATA TO GOOGLE SHEET -----#####

    # Load scopes and service account information from api key
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_JSON = f'/Users/jacealloway/Desktop/python/pco_access/apikey/service_account.json'
    SECRET = np.loadtxt(f'/Users/jacealloway/Desktop/python/pco_access/apikey/keys.txt', dtype = str)
    SPREADSHEET_ID = f"{SECRET[2]}" 
    # Generate the API push engine 
    _UPLOAD_ = GoogleAPIPush(SCOPES = SCOPES, SERVICE_ACCOUNT_JSON = SERVICE_ACCOUNT_JSON, SPREADSHEET_ID = SPREADSHEET_ID)





    logging.info(f"----- Beginning PCO API fetch at {datetime.today()} -----")
    start_time = time.time()
    # GET ALL DATA
    _ENGINE_ = Exporter(api_app_id = API_APP_ID, api_secret = API_SECRET)


    # EXTRACT WORKFLOW DATA 
    try:
        DF_WFS = _ENGINE_.workflowDFGenerator()
        dataframeCycle(dataframe = DF_WFS, upload_engine = _UPLOAD_, sheet_name = 'workflows')
    except Exception as e:
        logging.error(f"fetchError extracting workflow data for DF_WFS: {e}")

    # EXTRACT TEAM ROSTERING DATA 
    try:
        DF_ROSTERS = _ENGINE_.rosterDFGenerator()
        dataframeCycle(dataframe = DF_ROSTERS, upload_engine = _UPLOAD_, sheet_name = 'planrosters')
    except Exception as e:
        logging.error(f"fetchError extracting roster data for DF_ROSTERS: {e}")
    
    # EXTRACT GROUP DATA 
    try:
        DF_GROUPS = _ENGINE_.groupDFGenerator()
        dataframeCycle(dataframe = DF_GROUPS, upload_engine = _UPLOAD_, sheet_name = 'groups')
    except Exception as e:
        logging.error(f"fetchError extracting group data for DF_GROUPS: {e}")

    # EXTRACT CHECK-IN DATA



    end_time = time.time()
    elapsed = end_time - start_time
    logging.info(f"API fetched and processed in {elapsed:.2f} seconds / {(elapsed/60):.2f} minutes.")
