### AUTHOR: Jace Alloway (Github jacealloway)
###         For questions and inqurires related to the ETL code, please contact jacealloway@gmail.com.     
###         API access is restricted to key holders. 
### CODE VERSION: 2 - SERVER
###         See GitHub commit history for the description of updates.


import os
import requests
import numpy as np
import sys


# Try to load planning centre API URL
try:
    SECRET = np.loadtxt(f'User/pco_access/apikey/keys.txt', dtype = str)   
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    sys.exit()


sandbox = "sandboxb80d4ce6153e4424be2ba0d8b3ec5cf8.mailgun.org"
base = "https://api.mailgun.net"
fpath = '/root/User/pco_access/LOG/PCO_ETL_log.txt'

with open(fpath, 'r') as f:
    log_content = f.read()
    

if 'API fetched and processed' in log_content:
    text = 'PCO_ETL_SERV.py ran successfully.'
elif 'fetchError' in log_content:
    text = 'PCO_ETL_SERV.py ran with errors. Please check the server log for details.'
else: 
    text = 'PCO_ETL_SERV.py encountered an execution error. Please check the server log for details.'


def send_simple_message():
        return requests.post(
                "https://api.mailgun.net/v3/sandboxb80d4ce6153e4424be2ba0d8b3ec5cf8.mailgun.org/messages",
                auth=("api", os.getenv('API_KEY', SECRET[3])),
                data={"from": "Mailgun Sandbox <postmaster@sandboxb80d4ce6153e4424be2ba0d8b3ec5cf8.mailgun.org>",
                        "to": "Jace Alloway <jacealloway@gmail.com>",
                        "subject": "PCO_ETL.py Run Status",
                        "text": text})


send_simple_message()