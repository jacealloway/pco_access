
# Import packages 
try:
    from PCO_ETL import GoogleAPIPush, Exporter
    import numpy as np 
    import pandas as pd 
    import sys 
    from gc import collect 
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

# Define a function to get the data and return a Pandas dataframe object 
def getHostingBirthdays() -> object:
    DF_TEAMS = _EXPORTER_.parseJSON(url = SERVICES_BASE + '/' + 'teams')
    DF_TEAMS_REFINED = DF_TEAMS[["id", "attributes.name", "relationships.service_type.data.id"]]

    # Sort out non-hosting, non-downtown service types 
        # Hosting team codes: 6126877, 6135648, 6145244, 6169161, 6185863, 6190100, 6274363, 6479087, 6479195, 6973475
        # Downtown hosting servicetype code: 1517612
    DF_HOSTING_TEAM = DF_TEAMS_REFINED.where((DF_TEAMS_REFINED['relationships.service_type.data.id'] == '1517612') & (DF_TEAMS_REFINED['attributes.name'] == 'Hosting')).dropna(how = 'all')


    # Get the people on the downtown hosting team 
    DF_HOSTING_MEMBERS = pd.DataFrame()
    for team_id in DF_HOSTING_TEAM['id'].values:
        req_url = SERVICES_BASE +  f"/teams/{team_id}/people"
        DF_HOSTING_MEMBERS = pd.concat([DF_HOSTING_MEMBERS, _EXPORTER_.parseJSON(url = req_url)])

    DF_HOSTING_MEMBERS.rename(columns = {'attributes.full_name' : 'Name', 
                                        'attributes.birthdate' : 'Birthdate'}, inplace = True)
    return DF_HOSTING_MEMBERS[['Name', 'Birthdate']]
    



if __name__ == "__main__":
    HOSTING_DF_BIRTHDAYS = getHostingBirthdays()

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_JSON = f'/Users/jacealloway/Desktop/python/pco_access/apikey/service_account.json'
    SECRET = np.loadtxt(f'/Users/jacealloway/Desktop/python/pco_access/apikey/keys.txt', dtype = str)
    SPREADSHEET_ID = f"{SECRET[2]}" 
    # Generate the API push engine 
    _UPLOAD_ = GoogleAPIPush(SCOPES = SCOPES, SERVICE_ACCOUNT_JSON = SERVICE_ACCOUNT_JSON, SPREADSHEET_ID = SPREADSHEET_ID)

    _UPLOAD_.pushData(DATAFRAME = HOSTING_DF_BIRTHDAYS, sheet_name = "dt_hosting_birthdays")

    del HOSTING_DF_BIRTHDAYS
    collect()
