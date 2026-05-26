### AUTHOR: Jace Alloway (Github jacealloway)
###         For questions and inqurires related to the ETL code, please contact jacealloway@gmail.com.     
###         API access is restricted to key holders. 
### CODE VERSION: 4 - LOCAL/SERVER
###         See GitHub commit history for the description of updates.

# Import packages 
try:
    import PCO_ETL_LIB_SERV as ETL_LIB
    import numpy as np 
    from datetime import datetime
    import sys 
    import time 
    import logging
    from gc import collect

    logging.basicConfig(filename = 'User/pco_access/LOG/PCO_ETL_log.txt', 
                    filemode='w',
                    level = logging.INFO, 
                    format = '%(asctime)s - %(levelname)s - %(message)s'
                    )
except ModuleNotFoundError:
    print('ModuleNotFoundError: Ensure all packages are installed. Terminating.')



# Add importer construct for other files 
if __name__ == "__main__": 
    # Try to load planning centre API URL
    try:
        SECRET = np.loadtxt(f'User/pco_access/apikey/keys.txt', dtype = str)   
        API_APP_ID = f"{SECRET[0]}"     
        API_SECRET = f"{SECRET[1]}"   
    except FileNotFoundError:
        print('API Secret KEY not found. Terminating.')
        sys.exit()



    #####----- PUSHING DATA TO GOOGLE SHEET -----#####
    # Load scopes and service account information from api key
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_JSON = f'/User/pco_access/apikey/service_account.json'
    SPREADSHEET_ID = f"{SECRET[2]}" 
    # Generate the API push engine 
    _UPLOAD_ = ETL_LIB.GoogleAPIPush(SCOPES = SCOPES, SERVICE_ACCOUNT_JSON = SERVICE_ACCOUNT_JSON, SPREADSHEET_ID = SPREADSHEET_ID)


    #####----- GENERATING POSTGRES CONNECTION -----#####
    server_conn_url = f'postgresql+psycopg2://{SECRET[4]}'


    logging.info(f"----- Beginning PCO API fetch at {datetime.today()} -----")
    start_time = time.time()
    # GET ALL DATA
    _ENGINE_ = ETL_LIB.Exporter(api_app_id = API_APP_ID, api_secret = API_SECRET)


    # EXTRACT WORKFLOW DATA -> PUSH TO POSTGRES AND GOOGLE
    try:
        DF_WFS = _ENGINE_.workflowDFGenerator()
        ETL_LIB.postgresUpsert(conn_url = server_conn_url, dataframe = DF_WFS, schema = 'public', table_name = 'workflows')
        ETL_LIB.dataframeCycle(dataframe = DF_WFS, upload_engine = _UPLOAD_, sheet_name = 'workflows')
        del DF_WFS 
        collect()
    except Exception:
        logging.error(f"fetchError extracting workflow data for DF_WFS.")

    # EXTRACT TEAM ROSTERING DATA -> PUSH TO POSTGRES AND GOOGLE
    try:
        DF_ROSTERS = _ENGINE_.rosterDFGenerator()
        ETL_LIB.postgresUpsert(conn_url = server_conn_url, dataframe = DF_ROSTERS, schema = 'public', table_name = 'rosters')
        ETL_LIB.dataframeCycle(dataframe = DF_ROSTERS, upload_engine = _UPLOAD_, sheet_name = 'planrosters')
        del DF_ROSTERS 
        collect()
    except Exception:
        logging.error(f"fetchError extracting roster data for DF_ROSTERS.")
    
    # EXTRACT GROUP DATA -> PUSH TO POSTGRES AND GOOGLE
    try:
        DF_GROUPS = _ENGINE_.groupDFGenerator()
        ETL_LIB.postgresUpsert(conn_url = server_conn_url, dataframe = DF_GROUPS, schema = 'public', table_name = 'groups')
        ETL_LIB.dataframeCycle(dataframe = DF_GROUPS, upload_engine = _UPLOAD_, sheet_name = 'groups')
        del DF_GROUPS 
        collect()
    except Exception:
        logging.error(f"fetchError extracting group data for DF_GROUPS.")

    # EXTRACT CHECK-INS DATA -> PUSH TO POSTGRES AND GOOGLE
    try:
        DF_CHECKINS = _ENGINE_.checkinDFGenerator()
        ETL_LIB.postgresUpsert(conn_url = server_conn_url, dataframe = DF_CHECKINS, schema = 'public', table_name = 'checkins')
        ETL_LIB.dataframeCycle(dataframe = DF_CHECKINS, upload_engine = _UPLOAD_, sheet_name = 'kids')
        del DF_CHECKINS
        collect()
    except Exception:
        logging.error(f"fetchError extracting check-in data for DF_CHECKINS.")

    
    # EXTRACT ALL PEOPLE ACTIVITY -> PUSH TO POSTGRES
    try:
        DF_PEOPLE_ACTIVITY = _ENGINE_.peopleactivityDFGenerator()
        ETL_LIB.postgresUpsert(conn_url = server_conn_url, dataframe = DF_PEOPLE_ACTIVITY, schema = 'public', table_name = 'people_activity')

        del DF_PEOPLE_ACTIVITY
        collect()
    except Exception:
        logging.error(f"fetchError extracting people activity data for DF_PEOPLE_ACTIVITY.")


    end_time = time.time()
    elapsed = end_time - start_time
    logging.info(f"API fetched and processed in {elapsed:.2f} seconds / {(elapsed/60):.2f} minutes.")
