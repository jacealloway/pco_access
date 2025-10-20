from numpy import loadtxt, ceil
from datetime import datetime, timedelta
import csv
from tqdm import tqdm 
from sys import exit
import requests
import time 
from pydantic import BaseModel
from typing import List




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







# --- Reformat datetime objects ---
def reformatTimestring(time: str) -> str:
    """
    Reformat an input iso datetime string object of format '%Y-%m-%dT%H:%M:%SZ' into datetime string object of format '%m/%d/%Y'.
    """
    old_format1 = f'%Y-%m-%dT%H:%M:%SZ'
    old_format2 = f'%B %d, %Y'
    new_format = f'%m/%d/%Y'

    try:            #check to make sure the input timestring is formatted properly
        old_string = datetime.strptime(time, old_format1)
        new_string = datetime.strftime(old_string, new_format)
        return new_string
    
    except ValueError:
        pass
    
    try:
        old_string = datetime.strptime(time, old_format2)
        new_string = datetime.strftime(old_string, new_format)
        return new_string
    
    except ValueError:
        pass

    


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

class PlanSunday(BaseModel):
    """
    Sunday service plan information.
    """
    id : str
    date : str
    servicetype : str

class TeamMember(BaseModel):
    """
    Team member serving on a service type plan.
    """
    id : str
    person_name : str
    team_id : str
    position_name : str
    status : str


    


class sundayData():
    def __init__(self, api_app_id: str, api_secret: str, servicetype_id = None, plan_id = None):
        self.auth = (api_app_id, api_secret)
        self.servicetype_id = servicetype_id
        self.plan_id = plan_id

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

    #this is probably not useful 
    def get_people(self, url: str = BASE_URL+"people"):
        person_list: List[Person] = []

        while True: 
            fetch = safe_get(url, self.auth).json()

            for person in fetch['data']:
                
                key = person['id']
                name = person['attributes']['full_name']

                person_list.append(Person(id=key, name=name))

            try:
                url = fetch['links']['next']
                continue

            except KeyError:
                return person_list



    def get_plans(self):
        sunday_roster_list: List[PlanSunday] = []
        url = BASE_URL + f"service_types/" + self.servicetype_id + f"/plans"

        while True:
            fetch = safe_get(url, self.auth).json()
            

            for plan in fetch['data']:
                key = plan['id']
                date = reformatTimestring(str(plan['attributes']['dates']))
                
                sunday_roster_list.append(PlanSunday(id=key, date=str(date), servicetype=self.servicetype_id))
                
            try:
                url = fetch['links']['next']
                continue
            
            except KeyError:
                return sunday_roster_list


    def get_plan_people(self):
        url = BASE_URL +  f"service_types/" + self.servicetype_id + f"/plans/" + self.plan_id + '/team_members'
        person_list: List[TeamMember] = []

        while True:
            fetch = safe_get(url, self.auth).json()

            for person in fetch['data']:

                person_id = person['relationships']['person']['data']['id']
                full_name = person['attributes']['name']
                position = person['attributes']['team_position_name']
                pos_status = person['attributes']['status']
                team_id = person['relationships']['team']['data']['id']

                person_list.append(TeamMember(id=person_id, person_name=full_name, position_name=position, status=pos_status, team_id=team_id))



            try:
                url = fetch['links']['next']
                continue

            except KeyError:
                return person_list

    



class csvExporter():
    def __init__(self, api_app_id: str, api_secret: str):
        self.auth = (api_app_id, api_secret)


    def csvWriter(self):
        
        csv_columns = [
            'sunday plan date', 'team name', 'service type name', '# confirmed', '# pending / declined'
        ]

        try:
            #initialize writing parameters
            with open(SERVICES_writepath + f'teamsservices.csv', 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader() 
                primary_fetch = sundayData(api_app_id=self.auth[0], api_secret=self.auth[1])
                
                
                #extract data
                serviceTypes_list = primary_fetch.get_sunday_service_types()
                teams_list = primary_fetch.get_teams()

                for servicetype in serviceTypes_list:
                    servicetype_id = servicetype.id
                    servicetype_name = servicetype.name

                    plans_list = sundayData(api_app_id=self.auth[0], api_secret=self.auth[1], servicetype_id=servicetype_id).get_plans()

                    for plan in tqdm(plans_list, f"servicetype plans export progress"):

                        plan_id = plan.id
                        sunday_date = plan.date

                        plan_people = sundayData(api_app_id=API_APP_ID, api_secret=API_SECRET, servicetype_id=servicetype_id, plan_id=plan_id).get_plan_people()

                        for team in teams_list:
                            team_id = team.id
                            team_name = team.name
                            service_id = team.servicetype_id

                            #initialize variables
                            num_confirmed = 0
                            num_declined = 0

                            for person in plan_people:
                                if (team_id == person.team_id) and (service_id == servicetype_id):
                                    if person.status == 'C':
                                        num_confirmed += 1
                                    else:
                                        num_declined += 1
                                    
                        
                            


                            row = {
                                'sunday plan date': sunday_date,
                                'team name': team_name,
                                'service type name': servicetype_name,
                                '# confirmed': num_confirmed,
                                '# pending / declined': num_declined
                            }
                            writer.writerow(row)

            print(f"CSV file 'teamsservices.csv' created successfully on {datetime.today()}.")


        except IOError as e:
            print(f'Error writing CSV: {e}')



csvExporter(api_app_id=API_APP_ID, api_secret=API_SECRET).csvWriter()

# print(safe_get(BASE_URL+"teams", auth=(API_APP_ID, API_SECRET)).json())
# export = sundayData(api_app_id=API_APP_ID, api_secret=API_SECRET)
# L = export.get_sunday_service_types()
# dt_id = L[0].id
# print(dt_id)
# march2 = '78628248'
# export = sundayData(api_app_id=API_APP_ID, api_secret=API_SECRET, servicetype_id=dt_id)
# X = export.get_plans()
# print(X)






