from numpy import loadtxt, ceil
from datetime import datetime, timedelta
import csv
from tqdm import tqdm 
from sys import exit
import requests
import time 
from pydantic import BaseModel
from typing import List
from pandas import read_csv




# --- Fetch and Write Paths ---
KEY_path = r'/Users/jacealloway/Desktop/python/pco_access/apikey/'
PEOPLE_path = r'/Users/jacealloway/Desktop/python/pco_access/campuspeopleexports/'
SERVICES_writepath = r'/Users/jacealloway/Desktop/python/pco_access/analyzed/'

# --- API Credentials & Base URL ---
BASE_URL = f"https://api.planningcenteronline.com/services/v2/"


# Try to load planning centre API passwords
try:
    SECRET = loadtxt(f'{KEY_path}keys.txt', dtype = str)    #load passwords
    API_APP_ID = f"{SECRET[0]}"     #username
    API_SECRET = f"{SECRET[1]}"     #password key for API
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    exit()

# --- load people csv data ---
people_data_df = read_csv(PEOPLE_path + f'peopledata.csv', delimiter = ',')






# --- Reformat datetime objects ---
def reformatTimestring(time: str) -> str:
    """
    Reformat an input iso datetime string object of format '%Y-%m-%dT%H:%M:%SZ' into datetime string object of format '%m/%d/%Y'.
    """
    old_format = f'%Y-%m-%dT%H:%M:%SZ'
    new_format = f'%m/%d/%Y'

    try:            #check to make sure the input timestring is formatted properly
        old_string = datetime.strptime(time, old_format)
        new_string = datetime.strftime(old_string, new_format)
        return new_string
    
    except ValueError as e:
        print(e)

    


# --- function to compute week-end-sunday of datetime object
def week_end_sunday(input_date: str) -> str:
    """
    Compute the week-end leading Sunday of a given datetime string object.

    Args:
        input_date: datetime object in format '%m/%d/%Y'

    Returns:
        week_end_sunday_date: datetime object in format '%m/%d/%Y'
    """
    format = f'%m/%d/%Y'
    weekday = datetime.strptime(input_date, format).isoweekday()

    output_date = datetime.strptime(input_date, format) + timedelta(days = int(7 - (ceil(weekday))  ))

    return datetime.strftime(output_date.date(), format)




# --- Helper: safe_get with retry on 429 errors ---
def safe_get(
        url: str, 
        auth: tuple, 
        max_retries: int = 5, 
        backoff_factor: float = 1.0
        ) -> requests.Response:
    """
    Performs a GET request with retries. On a 429 error, waits for the time
    specified in the 'Retry-After' header (or uses exponential backoff) before retrying.

    Args:

        url: request source url. 

        auth: (api_app_id, api_secret).

        max_retries: number of retries associated with each 429 error.

        backoff_factor: exponential amount of time to wait between requests.

    Returns:

        requests.Response: "requests" object.

    """
    for attempt in range(max_retries):
       
        try:    
            #try to request the url
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            return response
        
        except requests.exceptions.HTTPError as e:
            #if error populates, diagnose
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    wait_time = float(retry_after)
                else:
                    wait_time = backoff_factor * (2 ** attempt)
                print(f"429 received for URL {url}. Waiting for {wait_time} seconds before retrying.")
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    raise
                else:
                    time.sleep(backoff_factor * (2 ** attempt))

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            else:
                time.sleep(backoff_factor * (2 ** attempt))

    #raise Exception if the fetch failed.
    raise Exception(f"Failed to get URL {url} after {max_retries} attempts.")



# --- Pydantic Models ---
class ServiceTypes(BaseModel):
    """
    Class of services types. Access via list index/slice; '.id' and '.name' .
    """
    id : str
    name: str


class Teams(BaseModel):
    """
    Class of team data. Access via list index/slice; '.id' and '.name' .
    """
    id : str
    name : str
    servicetype_id : str

class Person(BaseModel):
    """
    Person information. 
    """
    id : str
    name : str
    


class sundayData():
    def __init__(self, api_app_id: str, api_secret: str):
        self.auth = (api_app_id, api_secret)

    #make dictionary of sunday service types 
    def get_sunday_service_types(self, url: str = BASE_URL+"service_types") -> List:
        """
        Retrieve list of sunday service types 
        """
        data = safe_get(url, self.auth).json().get("data", [])
        service_type_list: List[ServiceTypes] = []

        for s_type in data:
            key = s_type['id']
            name = s_type['attributes']['name']
            if not name.startswith("SUNDAY SERVICES"):
                continue
                
            service_type_list.append(ServiceTypes(id=key, name=name))
            
        return service_type_list
    
 
    def get_teams(self, url: str = BASE_URL+"teams") -> List:
        """
        Retrieve list of teams and associated service types.
        """
        data = safe_get(url, self.auth).json().get("data", [])
        teams_list: List[Teams] = []

        for team in data:
            key = team['id']
            name = team['attributes']['name']
            servicetype_id = team['relationships']['service_type']['data']['id']

            teams_list.append(Teams(id=key, name=name, servicetype_id=servicetype_id))

        return teams_list


    # def get_people(self, url: str = BASE_URL+"people"):
    #     basic = safe_get(url, self.auth).json().get("data", [])
    #     team_person_list: List[Person] = []

    #     for person in basic:
    #         key = person['id']
    #         name = person['attributes']['full_name']










# print(safe_get(BASE_URL+"teams", auth=(API_APP_ID, API_SECRET)).json())
# export = sundayData(api_app_id=API_APP_ID, api_secret=API_SECRET)
# L = export.get_teams()
# print(L)