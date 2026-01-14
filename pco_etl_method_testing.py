
# Import packages 
try:
    from PCO_ETL import Exporter
    import numpy as np 
    import pandas as pd 
    import sys 
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



# Execute code here 
if __name__ == "__main__":
    _EXPORTER_.checkinsDFGenerator()