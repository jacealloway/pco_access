### AUTHOR: Jace Alloway (Github jacealloway)
###         For questions and inqurires related to the ETL code, please contact jacealloway@gmail.com.     
###         API access is restricted to key holders. 
### CODE VERSION: 4 - LOCAL/SERVER
###         See GitHub commit history for the description of updates.

#Set environment: LOCAL / SERVER
runpoint = 'SERVER'
if runpoint == 'LOCAL':
    log_dir = 'LOG/'
    api_dir = 'apikey/'
if runpoint == 'SERVER':
    log_dir = 'User/pco_access/LOG/'
    api_dir = 'User/pco_access/apikey/'
else:
    sys.exit()

# Import packages 
try:
    import PCO_ETL_LIB as ETL_LIB
    import numpy as np 
    from datetime import datetime
    import sys 
    import time 
    import logging
    from gc import collect

    logging.basicConfig(filename = log_dir + 'PCO_ETL_log.txt', 
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
        SECRET = np.loadtxt(api_dir + f'keys.txt', dtype = str)   
        API_APP_ID = f"{SECRET[0]}"     
        API_SECRET = f"{SECRET[1]}"   
    except FileNotFoundError:
        print('API Secret KEY not found. Terminating.')
        sys.exit()



    #####----- PUSHING DATA TO GOOGLE SHEET -----#####
    # Load scopes and service account information from api key
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_JSON = api_dir + f'service_account.json'
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
        ETL_LIB.dataframeCycle(dataframe = DF_ROSTERS, upload_en