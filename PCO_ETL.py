### AUTHOR: Jace Alloway (Github jacealloway)
###         For questions and inqurires related to the ETL code, please contact jacealloway@gmail.com.     
###         API access is restricted to key holders. 
### CODE VERSION: 2 
###         See GitHub commit history for the description of updates.

# Import packages 
try:
    import numpy as np 
    import pandas as pd 
    import requests 
    from datetime import date, datetime, timedelta
    import sys 
    import time 
    from gc import collect 
    from tqdm import tqdm 
    from googleapiclient import discovery
    from google.oauth2 import service_account
except ModuleNotFoundError:
    print("Ensure all packages are installed. Consult 'requirements.txt'.")

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


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
PEOPLE_BASE = 'https://api.planningcenteronline.com/people/v2/people'
EMAIL_BASE = 'https://api.planningcenteronline.com/people/v2/emails'
CAMPUS_BASE = 'https://api.planningcenteronline.com/people/v2/campuses'
WORKFLOW_BASE = 'https://api.planningcenteronline.com/people/v2/workflows'
GROUPS_BASE = 'https://api.planningcenteronline.com/groups/v2'


def timeFunction(func):
    """
    Decorator to time a function.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time() 
        elapsed = end_time - start_time 
        print(f"'{func.__name__}' executed in {elapsed:.2f} seconds / {(elapsed/60):.2f} minutes.")
        return result 
    return wrapper 


def safeGET(url: str, auth: tuple, max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Response:
    """
    Performs a GET request with retries. On a 429 error, waits for the time
    specified in the 'Retry-After' header (or uses exponential backoff) before retrying.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    wait_time = float(retry_after)
                else:
                    wait_time = backoff_factor * (2 ** attempt)
                print(f"429 received for URL {url}. Waiting for {wait_time} seconds before retrying.")
                time.sleep(wait_time)
            elif response.status_code == 404:
                raise Exception(f"404 received for URL {url}.")
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
    raise Exception(f"Failed to get URL {url} after {max_retries} attempts.")


def weekEndSunday(input_date: str) -> str:
    """
    Compute the week-end leading Sunday based off a given time string.

    Input must be of format '%Y-%m-%d'
    """
    format = f'%Y-%m-%d'

    weekday = datetime.strptime(input_date, format).isoweekday()
    output_date = datetime.strptime(input_date, format) + timedelta(days = int(7 - (np.ceil(weekday))  ))
    new_string = datetime.strftime(output_date.date(), format)

    return new_string


def reformatTimestring(time: str) -> str:
    old_format = f'%Y-%m-%dT%H:%M:%SZ'
    new_format = f'%Y-%m-%d'

    old_string = datetime.strptime(time, old_format)
    new_string = datetime.strftime(old_string, new_format)

    return new_string


def getTimeGap(time1: str, time2: str) -> str:
    """
    Compute the elapsed time between two inputs. 

    Args:
        time1: Input of format '%Y-%m-%d' (str)
        time2: Input of format '%Y-%m-%d' (str)

    Returns: 
        Number of days and hours between inputs (str).
    """
    format = f'%Y-%m-%d'

    dt = datetime.strptime(time1, format) - datetime.strptime(time2, format)
    gap = str(dt.days).replace('-', '')  
        
    return str(gap)


@timeFunction
def dataframeCycle(dataframe: object, upload_engine: classmethod, sheet_name: str) -> None:
    """
    Creating a function to push the dataframe to the Google API then to dump the dataframe and garbage collect.
    """
    print(f"Pushing dataframe to Google sheet '{sheet_name}'.")
    # 1. Push the dataframe 
    upload_engine.pushData(dataframe, sheet_name = sheet_name)
    # 2. Delete the dataframe 
    del dataframe 
    collect()
    print(f'Dataframe removed successfully.')


class GoogleAPIPush():
    def __init__(self, SCOPES: list[str], SERVICE_ACCOUNT_JSON: str, SPREADSHEET_ID: str):
        self.SCOPES = SCOPES 
        self.SERVICE_ACCOUNT_JSON = SERVICE_ACCOUNT_JSON
        self.SPREADSHEET_ID = SPREADSHEET_ID

    def authenticate(self) -> str:
        creds = service_account.Credentials.from_service_account_file(self.SERVICE_ACCOUNT_JSON, scopes = self.SCOPES)
        return creds
    
    def sheetPush(self, DATAFRAME: object, sheet_name: str) -> None:
        creds = self.authenticate()
        service = discovery.build('sheets', 'v4', credentials = creds)

        
        values = DATAFRAME.values.tolist()
        values.insert(0, DATAFRAME.columns.values.tolist()) 
        

        request = service.spreadsheets().values().update(
            spreadsheetId = self.SPREADSHEET_ID, 
            range = f"{sheet_name}!A1", 
            valueInputOption = "USER_ENTERED",
            body = {"values" : values}).execute()


    def sheetClear(self, sheet_name: str) -> None:
        creds = self.authenticate()
        service = discovery.build('sheets', 'v4', credentials = creds)

        rangeAll = '{0}!A1:Z'.format(sheet_name)
        body = {}
        clearing = service.spreadsheets().values().clear(
            spreadsheetId = self.SPREADSHEET_ID,
            range = rangeAll,
            body = body).execute()



    def pushData(self, DATAFRAME: object, sheet_name: str) -> None:
        # try:
        self.sheetClear(sheet_name)
        self.sheetPush(DATAFRAME.fillna(''), sheet_name)
        print(f'Dataframe pushed successfully to {sheet_name}.')
        # except errors.HttpError:
        #     print("Google sheet name range inconsistent with requested.")
        







                    #####----- GET DATA FROM API -----#####


class Exporter():
    def __init__(self, api_app_id: str, api_secret: str):
        self.auth = requests.auth.HTTPBasicAuth(api_app_id, api_secret)


    def parseJSON(self, url: str) -> object:
        df = pd.DataFrame()
        while True:
            try:
                response = safeGET(url = url, auth = self.auth)
                response_flattened = pd.json_normalize(response.json())
            except:
                print('Error fetching .json for {url}. Returning empty dataframe.')
                response_flattened = pd.Dataframe()

            for i in response_flattened["data"]:
                df = pd.concat([df, pd.json_normalize(i)], ignore_index = True)

                
            # Try to see if a next page exists; if it doesn't, break the loop.
            try:
                url = response_flattened["links.next"].values[0]

            except KeyError:
                break

        return df

    @timeFunction
    def workflowDFGenerator(self) -> object:
        # DATA LOADING AND REFINING ---------------------------------------------------------------------------------------------------------------------

        #Fetch primary API data
        pbar = tqdm(total = 4, desc = 'Fetching workflows API JSON')
        DF_WORKFLOWS = self.parseJSON(WORKFLOW_BASE)
        pbar.update(1)
        DF_CAMPUSES = self.parseJSON(CAMPUS_BASE)
        pbar.update(1)
        DF_PEOPLE = self.parseJSON(PEOPLE_BASE+'?per_page=100')
        pbar.update(1)
        DF_EMAILS = self.parseJSON(EMAIL_BASE+'?per_page=100')
        pbar.update(1)
        pbar.close()
                

        # Filtering
        DF_WORKFLOWS_REFINED = DF_WORKFLOWS[["id", "attributes.name", "relationships.campus.data.id", "attributes.my_ready_card_count", "attributes.completed_card_count"]]
        DF_CAMPUSES_REFINED = DF_CAMPUSES[["id", "attributes.name"]]
        DF_EMAILS_REFINED = DF_EMAILS[["relationships.person.data.id", "attributes.address"]]
        DF_PEOPLE_REFINED = DF_PEOPLE[["id", "attributes.name", "relationships.primary_campus.data.id", "attributes.child", "attributes.gender"]]

        # GET PCO secondary data, workflow cards and steps 
        DF_WORKFLOW_STEPS = pd.DataFrame()
        DF_WORKFLOW_CARDS = pd.DataFrame()
        for workflow_id in tqdm(DF_WORKFLOWS_REFINED["id"].values, "Fetching workflow cards and steps"):
            req1_url = f'{WORKFLOW_BASE}/{workflow_id}/steps'
            req2_url = f'{WORKFLOW_BASE}/{workflow_id}/cards'
             
            DF_WORKFLOW_STEPS = pd.concat([DF_WORKFLOW_STEPS, self.parseJSON(req1_url)], ignore_index = True)
            DF_WORKFLOW_CARDS = pd.concat([DF_WORKFLOW_CARDS, self.parseJSON(req2_url)], ignore_index = True)



        # Filtering
        DF_WORKFLOW_STEPS_REFINED = DF_WORKFLOW_STEPS[["relationships.workflow.data.id", "id", "attributes.name", "attributes.sequence"]]
        DF_WORKFLOW_CARDS_REFINED = DF_WORKFLOW_CARDS[["id", "relationships.person.data.id", "relationships.assignee.data.id", "relationships.workflow.data.id", "attributes.stage", "attributes.created_at", "attributes.moved_to_step_at", "relationships.current_step.data.id"]].where(DF_WORKFLOW_CARDS["attributes.stage"] != 'removed')
        DF_WORKFLOW_CARDS_REFINED.dropna(subset = ["attributes.stage"], inplace = True) # Remove all rows where 'attributes.stage' is not 'ready' or 'completed' or 'snoozed' (only removed cards are removed)

        # GET PCO tertiary data, workflow history of activities
        DF_ALL_WORKFLOW_HISTORY = pd.DataFrame()
        for i in tqdm(range(len(DF_WORKFLOW_CARDS_REFINED["relationships.person.data.id"].values)), "Fetching workflow card history"):
            person_id = int(DF_WORKFLOW_CARDS_REFINED["relationships.person.data.id"].values[i])
            card_id = int(DF_WORKFLOW_CARDS_REFINED["id"].values[i])       

            req_url = f'{PEOPLE_BASE}/{person_id}/workflow_cards/{card_id}/activities'
            DF_ALL_WORKFLOW_HISTORY = pd.concat([DF_ALL_WORKFLOW_HISTORY, self.parseJSON(url = req_url)])


        # Filtering
        DF_ALL_WORKFLOW_HISTORY_REFINED = DF_ALL_WORKFLOW_HISTORY[["relationships.workflow_card.data.id", "attributes.created_at", "relationships.workflow_step.data.id", "attributes.type"]]

        




        # JOINS -----------------------------------------------------------------------------------------------------------------------------
        # Join all emails to people
        DF_PEOPLE_EMAILS = pd.merge(DF_PEOPLE_REFINED, DF_EMAILS_REFINED, how='left', left_on = 'id', right_on='relationships.person.data.id') 
        # Join campus to people
        DF_PEOPLE_ALL = pd.merge(DF_PEOPLE_EMAILS, DF_CAMPUSES_REFINED, how = 'left', left_on = 'relationships.primary_campus.data.id', right_on='id')
        # Rename columns for cleaner table
        DF_PEOPLE_ALL.rename(columns={'id_x' : 'id',
                        'attributes.name_x' : 'full_name',
                        'relationships.primary_campus.data.id' : 'campus_id', 
                        'attributes.child' : 'child', 
                        'attributes.address' : 'email_address', 
                        'attributes.name_y' : 'campus_name', 
                        'attributes.gender' : 'gender'}, inplace = True)
        # Drop duplicate columns
        DF_PEOPLE_ALL.drop(['relationships.person.data.id', 'id_y'], axis = 1, inplace = True)



        # Join campuses to workflows
        DF_WORKFLOWS_ALL = pd.merge(DF_WORKFLOWS_REFINED, DF_CAMPUSES_REFINED, how = 'left', left_on = 'relationships.campus.data.id', right_on = 'id')
        # Rename columns for cleaner table
        DF_WORKFLOWS_ALL.rename(columns={'id_x' : 'id',
                        'attributes.name_x' : 'workflow_name',
                        'relationships.campus.data.id' : 'campus_id', 
                        'attributes.name_y' : 'campus_name'}, inplace = True)
        # Drop duplicate columns
        DF_WORKFLOWS_ALL.drop(['id_y'], axis = 1, inplace = True)




        # Join WORKFLOW_ALL table to WORKFLOW_STEPS table 
        DF_WORKFLOW_STEPS_ALL = pd.merge(DF_WORKFLOW_STEPS_REFINED, DF_WORKFLOWS_ALL, how='left', left_on='relationships.workflow.data.id', right_on = 'id')
        # Rename the columns for a cleaner table 
        DF_WORKFLOW_STEPS_ALL.rename(columns={'relationships.workflow.data.id' : 'workflow_id',
                        'id_x' : 'workflow_step_id',
                        'attributes.name' : 'step_name', 
                        'attributes.sequence' : 'sequence'}, inplace = True)
        # Drop duplicate columns
        DF_WORKFLOW_STEPS_ALL.drop(['id_y'], axis = 1, inplace = True)




        # Join PEOPLE_ALL to a table called ALL_CARDS for person and assignee IDs
        DF_WORKFLOW_CARDS_PEOPLE = pd.merge(DF_WORKFLOW_CARDS_REFINED, DF_PEOPLE_ALL, how = 'left', left_on='relationships.person.data.id', right_on='id')  #Card person
        DF_WORKFLOW_CARDS_ALL_PEOPLE= pd.merge(DF_WORKFLOW_CARDS_PEOPLE, DF_PEOPLE_ALL, how = 'left', left_on='relationships.assignee.data.id', right_on='id')     #Card assignee
        #Rename the columns for a cleaner table
        DF_WORKFLOW_CARDS_ALL_PEOPLE.rename(columns={'id_x' : 'card_id',
                        'relationships.person.data.id' : 'person_id',
                        'relationships.assignee.data.id' : 'assignee_id', 
                        'relationships.workflow.data.id' : 'workflow_id',
                        'relationships.current_step.data.id' : 'current_step_id',
                        'attributes.created_at' : 'card_created_at', 
                        'attributes.moved_to_step_at' : 'moved_to_step_at',
                        'attributes.stage' : 'stage', 
                        'full_name_x' : 'person_name', 
                        'campus_id_x' : 'person_campus_id', 
                        'email_address_x' : 'person_email',
                        'campus_name_x' : 'person_campus', 
                        'full_name_y' : 'assignee_name', 
                        'email_address_y' : 'assignee_email', 
                        'gender_x' : 'gender'}, inplace = True)
        # Drop duplicate or unneeded columns
        DF_WORKFLOW_CARDS_ALL_PEOPLE.drop(['id_y', 'child_x', 'id', 'campus_id_y', 'child_y', 'campus_name_y', 'gender_y'], axis = 1, inplace = True)

        # Join STEPS ALL to CURRENT CARD STEP on CARDS ALL 
        DF_WORKFLOW_CARDS_ALL = pd.merge(DF_WORKFLOW_CARDS_ALL_PEOPLE, DF_WORKFLOW_STEPS_ALL, how= 'left', left_on = 'current_step_id', right_on = 'workflow_step_id')
        # Rename columns 
        DF_WORKFLOW_CARDS_ALL.rename(columns = {'gender_x' : 'gender', 
                        'workflow_id_x' : 'workflow_id',    
                        'step_name' : 'current_step_name',
                        'sequence' : 'current_step_sequence', 
                        'workflow_name' : 'current_workflow_name', 
                        'campus_name' : 'current_workflow_campus_name'
                        }, inplace = True)
        # Drop duplicate or unneeded columns 
        DF_WORKFLOW_CARDS_ALL.drop(['attributes.my_ready_card_count', 'attributes.completed_card_count', 'workflow_id_y', 'workflow_step_id', 'campus_id'], axis = 1, inplace = True)


        # Join CARDS_ALL and STEPS ALL onto WORKFLOW_HISTORY 
        DF_ALL_WORKFLOW_HISTORY_CARDS = pd.merge(DF_ALL_WORKFLOW_HISTORY_REFINED, DF_WORKFLOW_CARDS_ALL, how='left', left_on='relationships.workflow_card.data.id', right_on='card_id')
        DF_ALL_DATA = pd.merge(DF_ALL_WORKFLOW_HISTORY_CARDS, DF_WORKFLOW_STEPS_ALL, how='left', right_on='workflow_step_id', left_on='relationships.workflow_step.data.id')
        # Rename columns 
        DF_ALL_DATA.rename(columns={'campus_name' : 'history_workflow_primary_campus',
                        'attributes.created_at' : 'log_created_at',
                        'person_campus' : 'person_primary_campus', 
                        'gender_y' : 'person_gender', 
                        'attributes.type' : 'log_entry',
                        'relationships.workflow_step.data.id': 'history_step_id', 
                        'workflow_id_x' : 'workflow_id', 
                        'step_name' : 'history_step_name', 
                        'sequence' : 'history_sequence', 
                        'workflow_name' : 'history_workflow_name'}, inplace = True)
        # Drop duplicate or unneeded columns
        DF_ALL_DATA.drop(['relationships.workflow_card.data.id', 'assignee_id', 'person_campus_id', 'workflow_id_y', 'workflow_step_id', 'campus_id', 'attributes.my_ready_card_count', 'attributes.completed_card_count'], axis = 1, inplace = True)
        




        # FINAL DATAFRAME PROCESSING -------------------------------------------------------------------------------------------------------------------
        
        # Organize the columns 
        DF_ALL_DATA = DF_ALL_DATA[['stage', 'current_step_name', 'current_workflow_name', 'current_workflow_campus_name', 'current_step_sequence', 'card_created_at', 'moved_to_step_at', 
                                   'history_step_name', 'history_workflow_name', 'history_workflow_primary_campus', 'history_sequence', 'log_entry', 'log_created_at', 
                                   'person_name', 'person_email', 'gender', 'person_primary_campus', 'assignee_name', 'assignee_email', 
                                   'card_id', 'person_id', 'current_step_id', 'history_step_id', 'workflow_id']]
        
        # Sort 
        DF_ALL_DATA.sort_values(by = ['card_id', 'log_created_at'], inplace = True)
        # Remove any duplicate columns post-sort. 
        DF_ALL_DATA.drop_duplicates(keep = 'first', inplace = True)

        # Generate columns for days_at_step and, time initiated/completed, week-end Sundays, days in workflow, completion indicator, and a notes column
        DF_ALL_DATA['current_day_initiated'] = ''
        DF_ALL_DATA['current_initiated_week_end'] = ''
        DF_ALL_DATA['current_days_at_step'] = '0'

        DF_ALL_DATA['history_day_initiated'] = ''
        DF_ALL_DATA['history_initiated_week_end'] = ''
        DF_ALL_DATA['history_day_completed'] = ''  
        DF_ALL_DATA['history_completed_week_end'] = ''
        DF_ALL_DATA['history_days_at_step'] = ''

        DF_ALL_DATA['days_in_workflow'] = '0'
        DF_ALL_DATA['card_complete_ind'] = 'False'
     

        # We need to process step 0's prior to processing history for the workflow and the timedeltas. First filter by step 0s with no workflow history. Skips / reversals don't matter here, the person is still at step 0.
        DF_ALL_DATA_STEP_ZERO = DF_ALL_DATA.where(DF_ALL_DATA['current_step_sequence'] == 0).dropna(how = 'all').copy()
        # Drop the duplicates and keep the first one - the other rows lose time data
        DF_ALL_DATA_STEP_ZERO.drop_duplicates(subset = ['current_step_sequence', 'card_id'], keep = 'first', inplace = True)    
        # Filter all non-zero sequences 
        DF_ALL_DATA_STEP_NONZERO = DF_ALL_DATA.where(DF_ALL_DATA['current_step_sequence'] != 0).dropna(how = 'all').copy()
        # The 'stage' column contains all the information about the status of the card. Thus we can filter out everything where history entries are null / if steps have been previously removed. 
        DF_ALL_DATA_STEP_NONZERO.dropna(subset = ['history_sequence'], inplace = True)
        # Bring the two together, redefining the dataframe 
        DF_ALL_DATA = pd.concat([DF_ALL_DATA_STEP_ZERO, DF_ALL_DATA_STEP_NONZERO])
        # Sort again
        DF_ALL_DATA.sort_values(by = ['card_id', 'log_created_at'], inplace = True)
        # Remove any duplicate columns post-sort. 
        DF_ALL_DATA.drop_duplicates(keep = 'first', inplace = True)
        # Rest the index again after the dropna()
        DF_ALL_DATA.reset_index(drop = True, inplace = True)


        # Current date information
        today = datetime.strftime(date.today(), f'%Y-%m-%d')

        # Initialize counter
        i = 0 
        pbar = tqdm(total = len(DF_ALL_DATA), desc = 'Computing workflow timedeltas')
        while True: 
            try:
                card_stage = DF_ALL_DATA['stage'].values[i]
                card_id = DF_ALL_DATA['card_id'].values[i]
                current_sequence = DF_ALL_DATA['current_step_sequence'].values[i]

                card_created_at = reformatTimestring(DF_ALL_DATA['card_created_at'].values[i])
                moved_to_step_at = reformatTimestring(DF_ALL_DATA['moved_to_step_at'].values[i])
                log_created_at = reformatTimestring(DF_ALL_DATA['log_created_at'].values[i])

                DF_ALL_DATA['card_created_at'].values[i] = card_created_at # Overwrite the card creation date value with the right format

            except IndexError:
                break 

            if current_sequence == 0:
                pass


            # Process initiated dates
            DF_ALL_DATA['current_day_initiated'].values[i] = moved_to_step_at
            DF_ALL_DATA['current_initiated_week_end'].values[i] = weekEndSunday(moved_to_step_at)


            if card_stage == 'completed':
                # Take the start and end difference
                DF_ALL_DATA['days_in_workflow'].values[i] = getTimeGap(moved_to_step_at, card_created_at)
                # Log the completion
                DF_ALL_DATA['card_complete_ind'].values[i] = 'True'
                # Populate all the missing workflow information for the completed step
                DF_ALL_DATA['current_step_name'].values[i] = 'Workflow Completed'
                DF_ALL_DATA['current_workflow_name'].values[i] = DF_ALL_DATA['history_workflow_name'].values[i]
                DF_ALL_DATA['current_workflow_campus_name'].values[i] = DF_ALL_DATA['history_workflow_primary_campus'].values[i]


            elif card_stage in ('snoozed', 'ready'):
                # Process sequences to integers 
                DF_ALL_DATA['current_step_sequence'].values[i] = int(DF_ALL_DATA['current_step_sequence'].values[i])
                # Take the start and today difference
                DF_ALL_DATA['days_in_workflow'].values[i] = getTimeGap(today, card_created_at)
                # Compute days at step
                DF_ALL_DATA['current_days_at_step'].values[i] = getTimeGap(today, moved_to_step_at)


            # Process start of dataframe 
            if i == 0:
                DF_ALL_DATA['history_day_initiated'].values[i] = card_created_at 
                DF_ALL_DATA['history_initiated_week_end'].values[i] = weekEndSunday(card_created_at) 
                DF_ALL_DATA['history_day_completed'].values[i] = log_created_at
                DF_ALL_DATA['history_completed_week_end'].values[i] = weekEndSunday(log_created_at)
                DF_ALL_DATA['history_days_at_step'].values[i] = getTimeGap(log_created_at, card_created_at)


            # Process end of dataframe 
            try:
                next_card_id = DF_ALL_DATA['card_id'].values[i+1]
            except IndexError:
                # Process end dates for the last row in the dataframe 
                DF_ALL_DATA['history_day_completed'].values[i] = log_created_at
                DF_ALL_DATA['history_completed_week_end'].values[i] = weekEndSunday(log_created_at)
                DF_ALL_DATA['history_days_at_step'].values[i] = getTimeGap(log_created_at, DF_ALL_DATA['history_day_initiated'].values[i])
                break 



            # Process the workflow histories
            if card_id == next_card_id:
                DF_ALL_DATA['history_day_initiated'].values[i+1] = log_created_at
                DF_ALL_DATA['history_initiated_week_end'].values[i+1] = weekEndSunday(log_created_at)


            if card_id != next_card_id: 
                DF_ALL_DATA['history_day_initiated'].values[i+1] = card_created_at
                DF_ALL_DATA['history_initiated_week_end'].values[i+1] = weekEndSunday(DF_ALL_DATA['history_day_initiated'].values[i+1])

            # Process the histories for the middle of the dataframe 
            if i > 0:
                DF_ALL_DATA['history_day_completed'].values[i] = log_created_at
                DF_ALL_DATA['history_completed_week_end'].values[i] = weekEndSunday(log_created_at)
                DF_ALL_DATA['history_days_at_step'].values[i] = getTimeGap(log_created_at, DF_ALL_DATA['history_day_initiated'].values[i])

            # Increase the counter 
            i+=1
            pbar.update(1)
        
        pbar.close()

        # Rename the old workflow columns because they are redundant 
        DF_ALL_DATA.rename(columns = {'current_workflow_name' : 'workflow_name', 
                                      'current_workflow_campus_name' : 'workflow_campus_name'}, inplace = True)

        return DF_ALL_DATA[[
            'stage', 
            'workflow_name', 
            'workflow_campus_name', 
            'current_step_name', 
            # 'current_step_sequence',      # I don't think you need this one
            'current_day_initiated', 
            'current_initiated_week_end', 
            'current_days_at_step',
            'history_step_name', 
            'history_day_initiated', 
            'history_initiated_week_end', 
            'history_day_completed',
            'history_completed_week_end',
            'history_days_at_step', 
            'card_created_at',   
            # 'moved_to_step_at', 
            'days_in_workflow',
            'card_complete_ind', 
            'person_name',
            'person_email', 
            'gender', 
            'person_primary_campus', 
            'assignee_name', 
            'assignee_email', 
            'card_id',
            'person_id', 
            'workflow_id',
            'current_step_id', 
            'history_step_id', 
        ]]

    @timeFunction
    def groupDFGenerator(self) -> object:
        # DATA LOADING AND REFINING ---------------------------------------------------------------------------------------------------------------------

        pbar = tqdm(total = 5, desc = 'Fetching groups API JSON')
        DF_GROUPTYPE = self.parseJSON(GROUPS_BASE+'/'+'group_types')
        pbar.update(1)
        DF_EVENTS = self.parseJSON(GROUPS_BASE + '/' + 'events')
        pbar.update(1)
        DF_GROUPS_PEOPLE = self.parseJSON(GROUPS_BASE + '/' + 'people')
        pbar.update(1)
        DF_CAMPUSES = self.parseJSON(GROUPS_BASE + '/' + 'campuses')
        pbar.update(1)
        DF_GROUPS = self.parseJSON(GROUPS_BASE + '/' + 'groups')
        pbar.update(1)
        pbar.close()


        DF_CAMPUSES_REFINED = DF_CAMPUSES[['id', 'attributes.name']]
        DF_GROUPS_REFINED = DF_GROUPS[['id', 'attributes.name', 'attributes.memberships_count', 'relationships.group_type.data.id', 'attributes.archived_at', 'attributes.created_at']]
        DF_GROUPTYPE_REFINED = DF_GROUPTYPE[['id', 'attributes.name']]
        DF_EVENTS_REFINED = DF_EVENTS[['id', 'attributes.name', 'attributes.visitors_count', 'relationships.group.data.id', 'attributes.starts_at']]
        DF_GROUPS_PEOPLE_REFINED = DF_GROUPS_PEOPLE[['id', 'attributes.first_name', 'attributes.last_name', 'attributes.phone_numbers', 'attributes.email_addresses']]

        DF_GROUPS_BY_CAMPUS = pd.DataFrame() 
        for campus_id in tqdm(DF_CAMPUSES['id'].values, "Fetching groups by campus"):
            req_url = GROUPS_BASE + f'/campuses/{campus_id}/groups'

            DATA = self.parseJSON(req_url)
            DATA['campus_id'] = campus_id
            DF_GROUPS_BY_CAMPUS = pd.concat([DF_GROUPS_BY_CAMPUS, DATA])

            
        # Filter out unused columns
        DF_GROUPS_BY_CAMPUS_REFINED = DF_GROUPS_BY_CAMPUS[['id', 'campus_id']]

        DF_MEMBERSHIPS = pd.DataFrame() 
        DF_GROUP_TAGS = pd.DataFrame()
        for group_id in tqdm(DF_GROUPS['id'].values, "Fetching group memberships and tags"):
            req_url1 = GROUPS_BASE + f'/groups/{group_id}/memberships'
            req_url2 = GROUPS_BASE + f'/groups/{group_id}/tags'
            DF_MEMBERSHIPS = pd.concat([DF_MEMBERSHIPS, self.parseJSON(req_url1)])

            tag_data = self.parseJSON(req_url2)
            tag_data['group_id'] = group_id
            DF_GROUP_TAGS = pd.concat([DF_GROUP_TAGS, tag_data])
  
        # Filter out unused columns
        DF_MEMBERSHIPS_REFINED = DF_MEMBERSHIPS[['attributes.joined_at', 'relationships.group.data.id', 'relationships.person.data.id']]
        DF_GROUP_TAGS_REFINED = DF_GROUP_TAGS[['attributes.name', 'group_id']]

            
        # Fetch all event attendances - note that any null event ID is dropped (there is no event, no attendance, and it doesn't exist so we cannot pull it)
        DF_ATTENDANCES = pd.DataFrame()
        for event_id in tqdm(DF_EVENTS_REFINED['id'].values, "Fetching events data"):
            req_url = GROUPS_BASE + f'/events/{event_id}/attendances'
            DF_ATTENDANCES = pd.concat([DF_ATTENDANCES, self.parseJSON(req_url)])

        # Filter out unused columns
        DF_ATTENDANCES_REFINED = DF_ATTENDANCES[["attributes.attended", "relationships.person.data.id", "relationships.event.data.id", "attributes.role"]]



        # Filter the group types so that we only end up with connect groups and coach groups
        DF_GROUPTYPE_REFINED = DF_GROUPTYPE_REFINED.where((DF_GROUPTYPE_REFINED['id'] == '448283') | (DF_GROUPTYPE_REFINED['id'] == '448862')).dropna(how = 'all')


        # JOINS ---------------------------------------------------------------------------------------------------------------------
        DF_GROUP_TYPES = pd.merge(DF_GROUPTYPE_REFINED, DF_GROUPS_REFINED, how = 'left', left_on = 'id', right_on = 'relationships.group_type.data.id')
        DF_EVENTS_GROUPS = pd.merge(DF_GROUP_TYPES, DF_EVENTS_REFINED, how = 'left', left_on = 'id_y', right_on = 'relationships.group.data.id')
        # Renaming the dropping columns for a cleaner table 
        DF_EVENTS_GROUPS.rename(columns={'id_x' : 'group_type_id',
                                         'attributes.name_x' : 'group_type', 
                                         'id_y' : 'group_id',
                                         'attributes.name_y' : 'group_name',
                                         'attributes.memberships_count' : 'member_count',
                                         'id' : 'event_id',
                                         'attributes.name' : 'event_name',
                                         'attributes.visitors_count' : 'visitor_count',
                                         'attributes.starts_at' : 'event_starts_at', 
                                         'attributes.archived_at' : 'group_archived_at', 
                                         'attributes.created_at' : 'group_created_at'
                                        }, inplace = True)
        # Drop keep only used columns
        DF_EVENTS_GROUPS = DF_EVENTS_GROUPS[['group_type_id', 'group_type', 'group_id', 'group_name', 'member_count', 'event_id', 'event_name', 'visitor_count', 'event_starts_at', 'group_archived_at', 'group_created_at']]
       
       
        DF_GROUPS_AND_CAMPUSES = pd.merge(DF_GROUPS_BY_CAMPUS_REFINED, DF_CAMPUSES_REFINED, how = 'left', left_on = 'campus_id', right_on = 'id')
        # Renaming 
        DF_GROUPS_AND_CAMPUSES.rename(columns = {'id_x' : 'group_id', 
                                                 'attributes.name' : 'campus_name'
                                                 }, inplace = True)
        # Drop unused columns 
        DF_GROUPS_AND_CAMPUSES = DF_GROUPS_AND_CAMPUSES[['group_id', 'campus_id', 'campus_name']]



        DF_ALL_GROUP_EVENTS = pd.merge(DF_EVENTS_GROUPS, DF_GROUPS_AND_CAMPUSES, how = 'left', left_on = 'group_id', right_on = 'group_id')
        # No need to drop unused columns or rename, as this was already done 



        DF_GROUP_EVENT_ATTENDANCES_ALL = pd.merge(DF_ALL_GROUP_EVENTS, DF_ATTENDANCES_REFINED, how = 'inner', left_on = 'event_id', right_on = 'relationships.event.data.id')
        # Renaming 
        DF_GROUP_EVENT_ATTENDANCES_ALL.rename(columns = {'attributes.attended' : 'attended_ind', 
                                                         'relationships.person.data.id' : 'person_id', 
                                                         'attributes.role' : 'role', 
                                                         }, inplace = True)
        # Drop unused columns 
        DF_GROUP_EVENT_ATTENDANCES_ALL = DF_GROUP_EVENT_ATTENDANCES_ALL[['attended_ind', 'person_id', 'role', 'group_type_id', 'group_type', 'group_id', 'group_name', 'member_count', 'event_id', 'event_name', 'visitor_count', 'event_starts_at', 'campus_id', 'campus_name', 'group_archived_at', 'group_created_at']]
 
        


        DF_MEMBERSHIPS_PEOPLE = pd.merge(DF_MEMBERSHIPS_REFINED, DF_GROUPS_PEOPLE_REFINED, how = 'left', left_on = 'relationships.person.data.id', right_on = 'id')
        # Rename 
        DF_MEMBERSHIPS_PEOPLE.rename(columns = {'attributes.joined_at' : 'joined_at', 
                                                'relationships.group.data.id' : 'group_id', 
                                                'relationships.person.data.id' : 'person_id', 
                                                'attributes.first_name' : 'first_name', 
                                                'attributes.last_name' : 'last_name', 
                                                'attributes.phone_numbers' : 'phone_number', 
                                                'attributes.email_addresses' : 'email_address'
                                                }, inplace = True)
        # Drop unused columns 
        DF_MEMBERSHIPS_PEOPLE = DF_MEMBERSHIPS_PEOPLE[['joined_at', 'group_id', 'person_id', 'first_name', 'last_name', 'phone_number', 'email_address']]



        DF_ALL_EVENT_ATTENDANCE_MEMBERS = pd.merge(DF_GROUP_EVENT_ATTENDANCES_ALL, DF_MEMBERSHIPS_PEOPLE, how = 'inner', left_on = ['group_id', 'person_id'], right_on = ['group_id', 'person_id'])
        # No need to rename or drop anything 
        


        # Add the tags to the groups; Need to left join because not every coach group has a tag
        DF_GROUPS_ALL = pd.merge(DF_ALL_EVENT_ATTENDANCE_MEMBERS, DF_GROUP_TAGS_REFINED, how = 'left', left_on = 'group_id', right_on = 'group_id')
        # Rename
        DF_GROUPS_ALL.rename(columns = {'attributes.name' : 'tag'}, inplace = True)
        




        # FINAL DATAFRAME PROCESSING -------------------------------------------------------------------------------------------------------------------
        # Drop all columns where group_id is null, since we just did a full outer join 
        DF_GROUPS_ALL.dropna(subset = 'group_id', inplace = True)
        DF_GROUPS_ALL['visitor_count'] = DF_GROUPS_ALL['visitor_count'].fillna(0)
        DF_GROUPS_ALL['group_archived_at'] = DF_GROUPS_ALL['group_archived_at'].astype(str)


        DF_GROUPS_ALL['full_name'] = ''
        DF_GROUPS_ALL['group_archived_at_week_end'] = ''
        DF_GROUPS_ALL['group_created_at_week_end'] = ''
        DF_GROUPS_ALL['event_starts_at_week_end'] = ''
        DF_GROUPS_ALL['joined_at_week_end'] = ''
        for i in tqdm(range(len(DF_GROUPS_ALL.index)), desc = "Processing group dates"):
            
            phone_numbers_list = DF_GROUPS_ALL['phone_number'].values[i]
            emails_list = DF_GROUPS_ALL['email_address'].values[i]


            # Use try-except statements in the case where an email/phone number is not on file 
            try:
                # The first element is always the primary number 
                DF_GROUPS_ALL['phone_number'].values[i] = phone_numbers_list[0]['number']
            except IndexError:
                DF_GROUPS_ALL['phone_number'].values[i] = ''

            try:
                DF_GROUPS_ALL['email_address'].values[i] = emails_list[0]['address']
            except IndexError:
                DF_GROUPS_ALL['email_address'].values[i] = ''



            # Process the dates, and account for null values (e.g. if a group has not been archived)
            try:
                DF_GROUPS_ALL['group_archived_at'].values[i] = reformatTimestring(DF_GROUPS_ALL['group_archived_at'].values[i])
                DF_GROUPS_ALL['group_archived_at_week_end'].values[i] = weekEndSunday(DF_GROUPS_ALL['group_archived_at'].values[i])
            except:
                DF_GROUPS_ALL['group_archived_at'].values[i] = ''
                DF_GROUPS_ALL['group_archived_at_week_end'].values[i] = ''

            try:
                DF_GROUPS_ALL['group_created_at'].values[i] = reformatTimestring(DF_GROUPS_ALL['group_created_at'].values[i])
                DF_GROUPS_ALL['group_created_at_week_end'].values[i] = weekEndSunday(DF_GROUPS_ALL['group_created_at'].values[i])
            except: 
                DF_GROUPS_ALL['group_created_at'].values[i] = ''
                DF_GROUPS_ALL['group_created_at_week_end'].values[i] = ''

            try: 
                DF_GROUPS_ALL['event_starts_at'].values[i] = reformatTimestring(DF_GROUPS_ALL['event_starts_at'].values[i])
                DF_GROUPS_ALL['event_starts_at_week_end'].values[i] = weekEndSunday(DF_GROUPS_ALL['event_starts_at'].values[i])
            except: 
                DF_GROUPS_ALL['event_starts_at'].values[i] = ''
                DF_GROUPS_ALL['event_starts_at_week_end'].values[i] = ''
            
            try:
                DF_GROUPS_ALL['joined_at'].values[i] = reformatTimestring(DF_GROUPS_ALL['joined_at'].values[i])
                DF_GROUPS_ALL['joined_at_week_end'].values[i] = weekEndSunday(DF_GROUPS_ALL['joined_at'].values[i])
            except:
                DF_GROUPS_ALL['joined_at'].values[i] = ''
                DF_GROUPS_ALL['joined_at_week_end'].values[i] = ''



            # Concatenate names
            first_name = DF_GROUPS_ALL['first_name'].values[i]
            last_name = DF_GROUPS_ALL['last_name'].values[i]
            DF_GROUPS_ALL['full_name'].values[i] = f"{first_name} {last_name}"
    

        return DF_GROUPS_ALL[[
                    'group_type', 
                    'group_name', 
                    'campus_name', 
                    'member_count', 
                    'tag',
                    'group_archived_at', 
                    'group_archived_at_week_end', 
                    'group_created_at',
                    'group_created_at_week_end',
                    'event_starts_at', 
                    'event_starts_at_week_end',
                    'visitor_count', 
                    'first_name', 
                    'last_name', 
                    'full_name',
                    'phone_number', 
                    'email_address', 
                    'role', 
                    'joined_at',
                    'joined_at_week_end',  
                    'attended_ind', 
                    'campus_id', 
                    'group_id', 
                    'group_type_id', 
                    'event_id', 
                    'person_id']]

    @timeFunction                                                                                                                                                                                               
    def rosterDFGenerator(self) -> object:
        # DATA LOADING AND REFINING ---------------------------------------------------------------------------------------------------------------------

        pbar = tqdm(total = 3, desc = 'Fetching roster API JSON')
        DF_SERVICE_TYPES = self.parseJSON(SERVICES_BASE + '/' + 'service_types')
        pbar.update(1)
        DF_TEAMS = self.parseJSON(SERVICES_BASE + '/' + 'teams')
        pbar.update(1)
        DF_ROSTER_PEOPLE = self.parseJSON(SERVICES_BASE + '/' + 'people')
        pbar.update(1)
        pbar.close()


        # Get the Sunday service rosters 
        DF_SERVICE_TYPES_REFINED = DF_SERVICE_TYPES[["id", "attributes.name"]].where(DF_SERVICE_TYPES["attributes.name"].str.contains("SUNDAY SERVICES")).dropna(how='all')
        DF_TEAMS_REFINED = DF_TEAMS[["id", "attributes.name", "relationships.service_type.data.id"]]
        # Sort out inactive people
        DF_ROSTER_PEOPLE = DF_ROSTER_PEOPLE[DF_ROSTER_PEOPLE['attributes.status'] == 'active']
        DF_ROSTER_PEOPLE_REFINED = DF_ROSTER_PEOPLE[["id", "attributes.full_name", "attributes.passed_background_check"]]
        

        DF_PLANS = pd.DataFrame()
        for servicetype_id in tqdm(DF_SERVICE_TYPES_REFINED["id"].values, "Fetching service type plans"):
            req_url = SERVICES_BASE + f'/service_types/{servicetype_id}/plans'
            DF_PLANS = pd.concat([DF_PLANS, self.parseJSON(req_url)])

        DF_PLANS_REFINED = DF_PLANS[["id", "attributes.dates", "attributes.plan_people_count", "relationships.service_type.data.id"]]


        DF_TEAM_MEMBERS = pd.DataFrame() 
        for i in tqdm(range(len(DF_PLANS_REFINED.index)), "Fetching plan rosters"):
            plan_id = DF_PLANS_REFINED["id"].values[i]
            servicetype_id = DF_PLANS_REFINED["relationships.service_type.data.id"].values[i]
            req_url = SERVICES_BASE +  f"/service_types/{servicetype_id}/plans/{plan_id}/team_members"

            DF_TEAM_MEMBERS = pd.concat([DF_TEAM_MEMBERS, self.parseJSON(req_url)])


        DF_TEAM_MEMBERS_REFINED = DF_TEAM_MEMBERS[["attributes.status", "relationships.plan.data.id", "relationships.person.data.id", "attributes.name", "relationships.scheduled_by.data.id", "relationships.service_type.data.id", "relationships.team.data.id"]]


        # EXECUTE JOINS ---------------------------------------------------------------------------------------------------------------------
 
        DF_ALL_ROSTERS = pd.merge(DF_TEAM_MEMBERS_REFINED, DF_SERVICE_TYPES_REFINED, how = 'left', left_on = 'relationships.service_type.data.id', right_on = 'id')
        DF_ALL_ROSTERS = pd.merge(DF_ALL_ROSTERS, DF_TEAMS_REFINED, how = 'left', left_on = 'relationships.team.data.id', right_on = 'id')
        DF_ALL_ROSTERS = pd.merge(DF_ALL_ROSTERS, DF_PLANS_REFINED, how = 'left', left_on = 'relationships.plan.data.id', right_on = 'id')



        DF_ALL_ROSTERS.rename(columns = {
                                'attributes.status' : 'status',
                                'relationships.person.data.id' : 'person_id',
                                'attributes.name' : 'team_name',
                                'attributes.name_x' : 'person_name', 
                                'attributes.name_y' : 'servicetype_name', 
                                'attributes.dates' : 'plan_date', 
                                'relationships.scheduled_by.data.id' : 'scheduler_id'
                                }, inplace = True)
        DF_ALL_ROSTERS = DF_ALL_ROSTERS[['status', 'person_id', 'person_name', 'scheduler_id', 'servicetype_name', 'team_name', 'plan_date']]


        # Left join person name to scheduler data
        DF_ALL_ROSTERS = pd.merge(DF_ALL_ROSTERS, DF_ROSTER_PEOPLE_REFINED, how = 'inner', left_on = 'scheduler_id', right_on = 'id')
        DF_ALL_ROSTERS.rename(columns = {
                    'attributes.full_name' : 'scheduler_name'
                    }, inplace = True)
        # Remove some columns 
        DF_ALL_ROSTERS = DF_ALL_ROSTERS[['status', 'person_id', 'scheduler_id', 'scheduler_name', 'servicetype_name', 'team_name', 'plan_date']]



        # Left join person name to person data, just to have the pass_background_check boolean 
        DF_ALL_ROSTERS = pd.merge(DF_ALL_ROSTERS, DF_ROSTER_PEOPLE_REFINED, how = 'inner', left_on = 'person_id', right_on = 'id')
        DF_ALL_ROSTERS.rename(columns = {
                    'attributes.full_name' : 'person_name', 
                    'attributes.passed_background_check' : 'passed_background_check'
                    }, inplace = True)
        # Remove some columns 
        DF_ALL_ROSTERS = DF_ALL_ROSTERS[['status', 'person_id', 'person_name', 'passed_background_check', 'scheduler_id', 'scheduler_name', 'servicetype_name', 'team_name', 'plan_date']]


        # Drop the duplicates
        DF_ALL_ROSTERS.drop_duplicates(inplace = True)
        # Remove some null values 
        DF_ALL_ROSTERS.dropna(subset = 'plan_date', inplace = True)
        DF_ALL_ROSTERS.dropna(subset = 'person_name', inplace = True)
        # Add a field to determine whether or not the plan has passed based on the current fetch date 
        DF_ALL_ROSTERS['future_plan'] = 'False'


        # Get the current rundate and format it
        today = datetime.strftime(date.today(), f'%Y-%m-%d')
        # Process dates 
        for i in tqdm(range(len(DF_ALL_ROSTERS.index)), desc = 'Processing Service Plan Dates'):
            old_format = f'%B %d, %Y'
            new_format = f'%Y-%m-%d'

            date_value = DF_ALL_ROSTERS['plan_date'].astype(str).values[i]

            if '&' in date_value:
                broken = date_value.split(' ')
                broken_date_value = broken[0] + ' ' + broken[1][0:2] + ', ' + broken[len(broken)-1][0:4]
                old_string = datetime.strptime(broken_date_value, old_format)

            else:
                old_string = datetime.strptime(date_value, old_format)

            plan_date = datetime.strftime(old_string, new_format)
            DF_ALL_ROSTERS['plan_date'].values[i] = plan_date

            # Check if the plan is in the past or in the future
            if datetime.strptime(plan_date, new_format) >= datetime.strptime(today, new_format):
                DF_ALL_ROSTERS['future_plan'].values[i] = 'True'


        # Sort the columns, then return them 
        DF_ALL_ROSTERS.sort_values(by = ['plan_date', 'servicetype_name', 'team_name', 'person_id', 'status'])

        return DF_ALL_ROSTERS











#####----- PUSHING DATA TO GOOGLE SHEET -----#####

# Load scopes and service account information from api key
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
           'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_JSON = f'/Users/jacealloway/Desktop/python/pco_access/apikey/service_account.json'
SECRET = np.loadtxt(f'/Users/jacealloway/Desktop/python/pco_access/apikey/keys.txt', dtype = str)
SPREADSHEET_ID = f"{SECRET[2]}" 
# Generate the API push engine 
_UPLOAD_ = GoogleAPIPush(SCOPES = SCOPES, SERVICE_ACCOUNT_JSON = SERVICE_ACCOUNT_JSON, SPREADSHEET_ID = SPREADSHEET_ID)



print(f"----- Beginning PCO API fetch at {datetime.today()} -----")
start_time = time.time()
# GET ALL DATA
_ENGINE_ = Exporter(api_app_id = API_APP_ID, api_secret = API_SECRET)
# Extract workflow data
DF_WFS = _ENGINE_.workflowDFGenerator()
# Extract New People and everything else
DF_NEW_PEOPLE = DF_WFS[DF_WFS['workflow_name'].str.contains('NEW', na = False)] # Filter in new people
DF_WORKFLOWS = DF_WFS[~DF_WFS['workflow_name'].str.contains('NEW', na = False)] # Filter out new people 
dataframeCycle(dataframe = DF_NEW_PEOPLE, upload_engine = _UPLOAD_, sheet_name = 'newpeople')
dataframeCycle(dataframe = DF_WORKFLOWS, upload_engine = _UPLOAD_, sheet_name = 'workflows')
# Extract team roster data
DF_ROSTERS = _ENGINE_.rosterDFGenerator()
dataframeCycle(dataframe = DF_ROSTERS, upload_engine = _UPLOAD_, sheet_name = 'planrosters')
# Extract group data 
DF_GROUPS = _ENGINE_.groupDFGenerator()
dataframeCycle(dataframe = DF_GROUPS, upload_engine = _UPLOAD_, sheet_name = 'groups')
end_time = time.time()
elapsed = end_time - start_time

print(f"API fetched and processed in {elapsed:.2f} seconds / {(elapsed/60):.2f} minutes.")



