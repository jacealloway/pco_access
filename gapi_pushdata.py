import csv
import numpy as np
from googleapiclient import errors, discovery
from google.oauth2 import service_account

#load scopes and service account information from api key
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
           'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = f'/Users/jacealloway/Desktop/python/pco_access/apikey/' + r'service_account.json'
SPREADSHEET_ID_PATH = f'/Users/jacealloway/Desktop/python/pco_access/apikey/'
#load spreadsheet ID from API key
SECRET = np.loadtxt(f'{SPREADSHEET_ID_PATH}keys.txt', dtype = str)
SPREADSHEET_ID = f"{SECRET[2]}" 

#retrieve data from csv
workflows_filepath = f'/Users/jacealloway/Desktop/python/pco_access/analyzed/workflows.csv'
newpeople_filepath = f'/Users/jacealloway/Desktop/python/pco_access/analyzed/newpeople.csv'


def authenticate() -> str:
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes = SCOPES)
    return creds

def upload_data(file_path: str, sheet_name: str) -> None:
    creds = authenticate()
    service = discovery.build('sheets', 'v4', credentials = creds)

    f = open(file_path, "r")
    values = [r for r in csv.reader(f)]

    request = service.spreadsheets().values().update(
        spreadsheetId = SPREADSHEET_ID, 
        range = f"{sheet_name}!A1", 
        valueInputOption = "USER_ENTERED",
        body = {"values" : values}).execute()


def sheet_clear(sheet_name: str) -> None:
    creds = authenticate()
    service = discovery.build('sheets', 'v4', credentials = creds)

    rangeAll = '{0}!A1:Z'.format(sheet_name)
    body = {}
    clearing = service.spreadsheets().values().clear(
        spreadsheetId = SPREADSHEET_ID,
        range = rangeAll,
        body = body).execute()


#push data. number of files and number of unique sheet names must be the same.
files_to_push = (workflows_filepath, newpeople_filepath)
sheetnames = ("teamworkflows", "newpeople")

try:
    for n in range(len(files_to_push)):
        file = files_to_push[n]
        subsheet_name = sheetnames[n]
        #clear the sheet
        sheet_clear(subsheet_name)
        #re-populate the sheet with new data
        upload_data(file, subsheet_name)
except errors.HttpError:
    print("Google sheet name range inconsistent with requested.")








