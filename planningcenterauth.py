        #Import packages 
import numpy as np 
import requests
from requests.auth import HTTPBasicAuth
import csv
from datetime import date
import sys

#set file variables 
BASE_URL_SERVICES = 'https://api.planningcenteronline.com/services/v2/'
PEOPLE_BASE_URL = 'https://api.planningcenteronline.com/people/v2/people/'
EMAIL_BASE_URL = 'https://api.planningcenteronline.com/people/v2/emails/'
CAMPUS_BASE_URL = 'https://api.planningcenteronline.com/people/v2/campuses'
BASE_URL_WORKFLOW = 'https://api.planningcenteronline.com/people/v2/workflows/'
# Try to load planning centre API passworkds
try:
    SECRET = np.loadtxt('keys.txt', dtype = str)    #load passwords
    API_APP_ID = f"{SECRET[0]}"     #username
    API_SECRET = f"{SECRET[1]}"     #password key for API
except FileNotFoundError:
    print('API Secret KEY not found. Terminating.')
    sys.exit()

#request retries
retries = 5

#set toggle to write CSV files or not
WRITE_CSV = True
#set path to writing files - this will be different for each person running the code
writepath = r'/Users/jacealloway/Desktop/python/C3/workflowexports/'
#add toggle to fetch workflow data or team data 
PEOPLE = False
WORKFLOWS = False 
TEAMS = True

desired_workflows={'548715': 'CREATIVE TEAM',
# '544795': 'DT BAPTISMS',
# '544778': 'DT CHILD DEDICATIONS',
'564585': 'DT MAINTENANCE TEAM',
# '544755': 'DT NEW PEOPLE FOLLOW UP',
'544593': 'DT TEAM ONBOARDING - C3 KIDS',
'550431': 'DT TEAM ONBOARDING - CAFE',
'550408': 'DT TEAM ONBOARDING - HOSPITALITY',
'544671': 'DT TEAM ONBOARDING - HOSTING',
'544864': 'DT TEAM ONBOARDING - PRODUCTION',
'544777': 'DT TEAM ONBOARDING - SERVICE PRODUCTION',
'544842': 'DT TEAM ONBOARDING - WORSHIP',
'550389': 'DT TEAM ONBOARDING - YOUTH',
# '541922': 'EXAMPLE FLOW TEMPLATE',
'550371': 'HAM TEAM ONBOARDING - PRODUCTION',
'544885': 'HAM TEAM ONBOARDING - SERVICE PRODUCTION',
# '544809': 'HAMILTON BAPTISMS',
'564649': 'HAMILTON MAINTENANCE TEAM',
# '548180': 'HAMILTON NEW PEOPLE FOLLOW UP',
'544664': 'HAMILTON TEAM ONBOARDING - C3 KIDS',
'550421': 'HAMILTON TEAM ONBOARDING - HOSPITALITY',
'544725': 'HAMILTON TEAM ONBOARDING - HOSTING',
'550386': 'HAMILTON TEAM ONBOARDING - MUSCLE',
'544846': 'HAMILTON TEAM ONBOARDING - WORSHIP',
# '555089': 'Love This City',
'564646': 'MT MAINTENANCE TEAM',
# '548148': 'MT NEW PEOPLE FOLLOW UP',
'544649': 'MT TEAM ONBOARDING - C3 KIDS',
'550418': 'MT TEAM ONBOARDING - HOSPITALITY',
'544718': 'MT TEAM ONBOARDING - HOSTING',
'550368': 'MT TEAM ONBOARDING - PRODUCTION',
'544881': 'MT TEAM ONBOARDING - SERVICE PRODUCTION',
'544844': 'MT TEAM ONBOARDING - WORSHIP',
'550397': 'MT TEAM ONBOARDING - YOUTH',
# '561167': 'TEST')
}

def params() -> None:
    """
    Initializing function to confirm user settings.
    """
    print('Date: {}'.format(date.today()))
    print()
    print(f'------ Execute API request for the following data: ------')
    print()
    print('Retrieve_people:     {}'.format(PEOPLE))
    print('Retrieve_workflows:  {}'.format(WORKFLOWS))
    print('Retrieve_teams:      {}'.format(TEAMS))
    print('....Retries:         {}'.format(retries))
    print()
    print('Write_CSV:           {}'.format(WRITE_CSV))
    print('....Write Filepath:  {}'.format(writepath))
    print()
    # if WORKFLOWS == True:
    print('Exporting workflows:')
    for wf_id, wf_name in desired_workflows.items():
        print('..................   {}'.format(wf_name))

    print()
    initialize = input(f'Press ENTER to continue export. To edit parameters, press "%any key%+ENTER". To exit export at any time, press "CTRL+C".')
    if len(initialize) > 0:
        print('Exiting...')
        sys.exit()
    else:
        print(f'---------------------------------------------------------')
        print('Terminal:')





#write class to store API campus/workflow requests - can import elsewhere
class pcfetch():
    """
    **Functions:**

        - getWorkflowIDS -> [dict]: retrieves dictionary of all workflow ID's, names, associated campuses.

        - getWorkflowSteps -> [dict]: retrieves dictionary of all workflow steps and nammes associated with each workflow ID.

        - getCampusIDS -> [dict] : retrieves dictionary of all campus ID's and names.
    """
    def getWorkflowIDS() -> dict:
        """
        Retrieve dictionary of all workflow ID's, names, and associated campuses. 
        """
        workflowDict = {}
        url = BASE_URL_WORKFLOW 

        while url:          
            #log into PCO using API id's in URL
            response = requests.get(url, auth=HTTPBasicAuth(API_APP_ID, API_SECRET))   

            if response.status_code == 200:
                #call workflow data 
                workflows_data = response.json()


                #iterate through each workflow key and append dictionary
                for workflow in workflows_data['data']:
                    fill = {}
                    fill['name'] = workflow['attributes']['name']
                    fill['campus ID'] = workflow['attributes']['campus_id']
                    workflowDict[workflow['id']] = fill

                #check if there is a next page
                url = workflows_data['links'].get('next')

            else:
                print(f"Error fetching workflows: {response.status_code}, {response.json()}")
                break

        #return dict 
        return workflowDict


    def getWorkflowSteps(workflow_dict: dict) -> dict:
        """
        Retrieve dictionary of all workflow steps and names associated with each workflow ID.
        """
        #get ID list from workflow keys 
        ID_list = workflow_dict.keys()

        #initialize empty dictionary 
        stepsDict = {}
        
        #iterate through each workflow ID in list 
        for workflow_id in list(ID_list):
            #retrieve URL
            url = f'{BASE_URL_WORKFLOW}{workflow_id}/steps'
            response = requests.get(url, auth=HTTPBasicAuth(API_APP_ID, API_SECRET))

            #initialize empty dictionary to store workflow steps 
            idDict = {}

            if response.status_code == 200:
                #get number of workflow steps 
                N = len(response.json()['data'])

                #iterate through workflow steps and extract ID and name 
                for k in range(N):
                    attributes_dict = {}
                    attributes_dict['id'] = response.json()['data'][k]['id']
                    attributes_dict['name'] = response.json()['data'][k]['attributes']['name']
                    idDict[k] = attributes_dict

                #once all steps are given, assign 'completed' 
                idDict[N] = {'id' : None, 'name' : 'COMPLETED'}

                #add to steps dictionary for each workflow ID
                stepsDict[workflow_id] = idDict

            else:
                print(f"Error fetching steps for workflow {workflow_id}: {response.status_code}, {response.json()}")
                return None
            
        #return dict 
        return stepsDict


    def getCampusIDS() -> dict:
        """
        Retrieve dictionary of all campus ID's and names. 
        """
        campusDict = {}
        url = CAMPUS_BASE_URL 
        
        #log into PCO using API id's in URL
        response = requests.get(url, auth=HTTPBasicAuth(API_APP_ID, API_SECRET)) 

        if response.status_code == 200:
            #call campus data 
            campus_data = (response.json())['data']
            N = len(campus_data)
            for k in range(N):
                campusDict[campus_data[k]['id']] = campus_data[k]['attributes']['name']
        
        #check to make sure each worflow is tied to a campus ID
        response_wf = requests.get(BASE_URL_WORKFLOW, auth=HTTPBasicAuth(API_APP_ID, API_SECRET))
        if response_wf.status_code == 200:
            #get length of data (# of workflows)
            M = response_wf.json()['data']
            for k in range(len(M)):
                #iterate through and check to see if any workflow is not tied to a campus
                if M[k]['attributes']['campus_id'] == None:
                    #if it is not tied to a campus, log it as a 'none' campus under workflow name.
                    campusDict['None'] = M[k]['attributes']['name']

        else:
            print(f"Error fetching workflows: {response.status_code}, {response.json()}")
            

        #return dict 
        return campusDict




if __name__ == '__main__':
    #communicate parameters
    params()

    #PEOPLE TOGGLE
    if PEOPLE == True:
        def fetch_all_people(per_page = 100) -> dict:
            """
            Retrieve all information on people.
            """
            url = f'{PEOPLE_BASE_URL}'
            all_results = []
            offset = 0

            while True:
                # Fetch a batch of data
                response = requests.get(f'{url}?per_page={per_page}&offset={offset}', auth=HTTPBasicAuth(API_APP_ID, API_SECRET))

                if response.status_code == 200:
                    data = response.json()
                    if not data['data']:  # No more data to fetch
                        break

                    all_results.extend(data['data'])  # Append results to the list

                    # Update offset to fetch the next batch
                    offset += per_page
                else:
                    print(f"Error fetching data: {response.status_code}, {response.json()}")
                    break

            return all_results


        def fetch_emails(per_page = 100) -> dict:
            """
            Retrieve all people emails.

            Returns:
                Dict {'person_id' : 'address'}
            """
            url = f'{EMAIL_BASE_URL}'
            all_results = {}
            offset = 0


            # Fetch a batch of data
            response = requests.get(f'{url}?per_page={per_page}&offset={offset}', auth=HTTPBasicAuth(API_APP_ID, API_SECRET))

            if response.status_code == 200:
                data = response.json()
                for idx in range(len(data['data'])):

                    person_id = data['data'][idx]['relationships']['person']['data']['id']
                    person_email = data['data'][idx]['attributes']['address']

                    all_results[person_id] = person_email

            elif response.status_code != 200:
                print(f"Error fetching data: {response.status_code}, {response.json()}")



            return all_results


        def save_people_data_to_csv(people_data: dict, emails_dict: dict) -> None:
            """
            Saves a list of people data to a CSV file.

            Args:
                people_data (list): A list of dictionaries containing people data.
                csv_filename (str): The name of the CSV file to save the data to.
            """
            created_at = date.today()

            # Define the columns for the CSV
            csv_columns = [
                'id', 'first_name', 'last_name', 'name', 'birthdate', 'gender', 'email', 'membership',
                'status', 'created_at', 'updated_at', 'avatar', 'accounting_administrator',
                'can_create_forms', 'can_email_lists', 'child', 'passed_background_check',
                'people_permissions', 'site_administrator', 'primary_campus_id'
            ]

            # Open CSV file for writing
            try:
                with open(writepath + f'peopledata{created_at}.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()

                    # Write each person entry to the CSV file
                    for person in people_data:
                        attributes = person['attributes']
                        person_id = person['id']
                        try:
                            email_addy = emails_dict[f'{person_id}']
                        except KeyError:
                            email_addy = 'None'

                        relationships = person['relationships']

                        try:
                            primary_campus_id = relationships['primary_campus']['data']['id']
                        except TypeError:
                            primary_campus_id = 'None'

                        row = {
                            'id': person_id,
                            'first_name': attributes.get('first_name'),
                            'last_name': attributes.get('last_name'),
                            'name': attributes.get('name'),
                            'birthdate': attributes.get('birthdate'),
                            'gender': attributes.get('gender'),
                            'email' : email_addy,
                            'membership': attributes.get('membership'),
                            'status': attributes.get('status'),
                            'created_at': attributes.get('created_at'),
                            'updated_at': attributes.get('updated_at'),
                            'avatar': attributes.get('avatar'),
                            'accounting_administrator': attributes.get('accounting_administrator'),
                            'can_create_forms': attributes.get('can_create_forms'),
                            'can_email_lists': attributes.get('can_email_lists'),
                            'child': attributes.get('child'),
                            'passed_background_check': attributes.get('passed_background_check'),
                            'people_permissions': attributes.get('people_permissions'),
                            'site_administrator': attributes.get('site_administrator'),
                            'primary_campus_id' : primary_campus_id
                        }

                        writer.writerow(row)

                print(f"CSV file 'peopledata{created_at}.csv' created successfully.")

            except IOError:
                print("I/O error while writing the CSV file.")


        #fetch
        people = fetch_all_people()
        emails = fetch_emails()
        if WRITE_CSV:
            save_people_data_to_csv(people, emails)


    #WORKFLOW TOGGLE
    if WORKFLOWS == True:

        #get campus dictionary
        Campus_Dict = pcfetch.getCampusIDS()
        #get workflow dictionary
        Workflow_Dict = pcfetch.getWorkflowIDS()
        #get workflow steps dictionary
        WorkflowSteps_Dict = pcfetch.getWorkflowSteps(Workflow_Dict)



        def writeWorkflowCampusDictCSV(workflow_dict: dict, campus_dict: dict) -> None:
            """
            Write CSV of current workflows, ID's, and associated campuses.
            """
            #write current time 
            time_created = date.today()
            
            #initialize columns
            cols_init=[
                'campus ID',
                'campus name',
                'workflow ID',
                'workflow name'
            ]

            try:
                #begin writing with open
                with open(writepath + f'campusexport{time_created}.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=cols_init)
                    writer.writeheader()

                    #iterate through workflow dictionary to retrieve all ID's and names
                    for workflow_id in workflow_dict.keys():
                        workflow_name = workflow_dict[f'{workflow_id}']['name']
                        campus_ID = workflow_dict[f'{workflow_id}']['campus ID']
                        if campus_ID == None:
                            campus_name = 'None'
                            campus_ID = 'None'
                        elif campus_ID != None:
                            campus_name = campus_dict[f'{campus_ID}']

                        #fill the row with the given variables
                        row = {
                            'campus ID' : campus_ID,
                            'campus name' : campus_name,
                            'workflow ID' : workflow_id,
                            'workflow name' : workflow_name
                        }

                        #write the row 
                        writer.writerow(row)

                print(f"CSV file 'campusexport{time_created}.csv' created successfully.")

            except IOError:
                print('I/O Error while writing CSV.')


        # #write all workflow's and campus's to CSV
        if WRITE_CSV:
            writeWorkflowCampusDictCSV(Workflow_Dict, Campus_Dict)


        def retrieveWorkflowPeople(workflow_ID: int, batchsize = 100) -> dict:
            """
            Retrieve dictionary of workflow cards at each workflow step.

            Args:
                workflow_ID: ID of workflow (int)

                retries: number of attempts to fetch data (int) (default = 5)

                batchsize: number of people listed per page in fetch (int) (default = 5)

            Returns:
                dict = {
                "id" : person_id [int],
                "workflow id" : workflow_id [int],
                "workflow step" : workflow_step [int],
                "step name" : step_name [str],
                "workflow step sequence" : sequence [int],
                "assignee id" : assignee_id [int],
                "removed" : removed [bool]
                }
            """
            offset = 0
            url = f'{BASE_URL_WORKFLOW}/{workflow_ID}'


            returndict = {}

            #sometimes number of attempts exceeds retrieval limit; retry up to {retries} times 
            for attempt in range(retries):
                cardresponse = requests.get(f'{url}/cards?offset={offset}&per_page={batchsize}', auth=HTTPBasicAuth(API_APP_ID, API_SECRET))
                stepresponse = requests.get(f'{url}/steps', auth=HTTPBasicAuth(API_APP_ID, API_SECRET))

                if cardresponse.status_code == 200:
                    people_data = cardresponse.json()
                    N = len(people_data['data'])
                    
                    for k in range(N):
                        #init new dictionary for each person 
                        person_dict = {}

                        #try to retrieve person ID and respective workflow step for each card in workflow 
                        try:
                            if people_data['data'][k]['attributes']['removed_at'] != None:
                                removed = True 
                            else:
                                removed = False


                            person_id = people_data['data'][k]['relationships']['person']['data']['id']             #retrieve person ID for card 
                            card_id = people_data['data'][k]['id']
                            workflow_step = people_data['data'][k]['relationships']['current_step']['data']['id']   #retrieve current workflow step for person
                    
                        #if no workflow step is associated, lodge None 
                        except:
                            workflow_step = None
                            sequence = None

                        assignee_id = people_data['data'][k]['relationships']['assignee']['data']['id']         #retrieve assignee ID for person 
                                    

                        #locate the step sequence for each card in workflow
                        if stepresponse.status_code == 200:
                            stepdata = stepresponse.json()
                            M = len(stepdata['data'])
                            #iterate through each step id 
                            for l in range(M):
                                step_id = stepdata['data'][l]['id']
                                #if the step id for each workflow step is the same as the workflow step ID for the person, match the sequence number to the person dictionary 
                                if step_id == workflow_step:
                                    #lodge 
                                    sequence = stepdata['data'][l]['attributes']['sequence'] 


                            #add to dictionary 
                            person_dict['id'] = person_id 
                            person_dict['card id'] = card_id     
                            person_dict['workflow id'] = workflow_ID
                            person_dict['workflow step'] = workflow_step
                            person_dict['workflow step sequence'] = sequence
                            person_dict['assignee id'] = assignee_id
                            person_dict['removed'] = removed

                        #populate the dictionary 
                        returndict[k] = person_dict

                    #too many requests
                elif cardresponse.status_code or stepresponse.status_code == 429:
                    retry_after = np.max(cardresponse.headers.get('Retry-After'), stepresponse.headers.get('Retry-After'))
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = 2 ** attempt  # Exponential backoff if Retry-After is not provided
                    print(f"[Workflow People]: Rate limit exceeded. Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)

                else:
                    print(f"Error fetching people for workflow {workflow_ID}: {cardresponse.status_code}, {cardresponse.json()}")
                    return None
            
            #return the dictionary
            return returndict


        def retrieveWorkflowHistory(person_id: int, workflow_card: int, workflow_id: int) -> dict:
            """
            Retrieve list of timestamps for workflow card history moved through workflow steps. 

            Args:
                person_id: Person ID (int)

                workflow_card: Workflow card ID of person in workflow (int)

                workflow_id: ID of workflow (int)

            Returns:
                dict = {
                "sequence" : sequence associated with workflow step (dict) 
                    = {
                    "step name" : step_name (str),
                    "initialized at" : created_at (str)
                    }
                }
            """
            #get workflow cards history URL
            url = f'{PEOPLE_BASE_URL}{person_id}/workflow_cards/{workflow_card}/activities'
            
            #initialize empty history dictionary for return 
            hist_dict = {}

            #get workflow steps list, names, sequences for the given workflow ID
            workflow_stepslist = WorkflowSteps_Dict[f'{workflow_id}']


            #request data 
            for attempt in range(retries):
                response = requests.get(url, auth=HTTPBasicAuth(API_APP_ID, API_SECRET))
                if response.status_code == 200:
                    #extract json
                    person = response.json()

                    #determine how many updates a card has 
                    L = len(person['data'])

                    

                    #set a counter to iterate through workflow steps 
                    indexer = 0

                    for o in range(L)[::-1]:
                        #initialize blank dictionary to populate step timestamps 
                        step_tracker = {}   

                        #determine when card was created (person signed up)
                        if person['data'][o]['attributes']['comment'].startswith("created this card"):
                            #get time and sequence name 
                            time_init = person['data'][o]['attributes']['created_at']   
                            seq_name = workflow_stepslist[indexer]['name']

                            #add to step tracker dict 
                            step_tracker['step name'] = seq_name
                            step_tracker['initialized at'] = time_init
                            #set the step index with the timestamp in return dict
                            hist_dict[indexer] = step_tracker

                            #up the counter and return to the beginning of the loop
                            indexer += 1
                            continue

                        #determine if a person was moved back to a step
                        if person['data'][o]['attributes']['comment'].startswith("sent this back"):
                            #if a person was moved back a step, only consider the time once they have completed this step
                            indexer += -1
                            continue

                        #determine when step was completed (person moved forward )
                        if person['data'][o]['attributes']['comment'].startswith("completed the step"):
                            #get tiem and sequence name 
                            time_init = person['data'][o]['attributes']['created_at']
                            seq_name = workflow_stepslist[indexer]['name']

                            #add step name and time to history dict 
                            step_tracker['step name'] = seq_name
                            step_tracker['initialized at'] = time_init
                            hist_dict[indexer] = step_tracker

                            #up the counter and return to the beginning of the loop 
                            indexer += 1
                            continue

                                    #check to see if all steps have been completed. if so, break the loop and append 'completed' once all steps have been completed.
                        if person['data'][o]['attributes']['comment'].startswith("completed this card"):
                            #break the loop once the card is completed
                            break


                elif response.status_code == 429:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = 2 ** attempt  # Exponential backoff if Retry-After is not provided
                    print(f"[Workflow History]: Rate limit exceeded. Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)

            #return the history dictionary for the given individual
            return hist_dict


        def writeWorkflowHistCSV(workflow_id) -> None:
            """
            Write a workflow history to a CSV file for people in each workflow. 
            """
            #get time of creation
            time_created = date.today()

            #retrieve total number of workflow steps from the workflow steps dict
            num_steps = len(list(WorkflowSteps_Dict[f'{workflow_id}']))

            #create a loop to write columns for however long the workflow process is 
            #init information
            cols_init = [
                'index',
                'person id', 
                'assignee id',
                'removed'
            ]
            #add each workflow step, including the 'completed' step
            for sequence in range(num_steps):
                cols_init.append(sequence)
                cols_init.append(str((sequence))+' created at')

            #retrieve the people in a workflow
            workflow_people = retrieveWorkflowPeople(workflow_id)
            #get the total number of cards
            num_people = len(list(workflow_people))

            
            try:
                with open(writepath + f'{workflow_id}export{time_created}.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=cols_init)
                    writer.writeheader()

                    #iterate through cards
                    for cardnum in range(num_people):
                        #get person's ID and card ID
                        personID = workflow_people[cardnum]['id']
                        cardID = workflow_people[cardnum]['card id']
                        
                        #use card and person ID to retireve workflow history / card history for that person.
                        card_history = retrieveWorkflowHistory(personID, cardID, workflow_id)
                        Z = len(card_history)
                        
                        #initialize row parameters
                        row={
                            'index' : cardnum,
                            'person id' : workflow_people[cardnum]['id'],
                            'assignee id' : workflow_people[cardnum]['assignee id'],
                            'removed' : workflow_people[cardnum]['removed']
                        }
                        #for every additional sequence workflow step, add a new row element in accordance with number of columns.
                        #if person hasn't completed all workflow steps yet, append blank
                        for sequence in range(num_steps):
                            if sequence < Z:
                                row[sequence] = WorkflowSteps_Dict[f'{workflow_id}'][sequence]['name']
                                row[str((sequence))+' created at'] = card_history[sequence]['initialized at']
                            else:
                                row[sequence] = ''
                                row[str((sequence))+' created at'] = ''

                        #write the row 
                        writer.writerow(row)

                print(f"CSV file '{workflow_id}export{time_created}.csv' created successfully.")

            except IOError:
                print('I/O Error while writing CSV.')

        #script to export all workflow histories.
        if WRITE_CSV:
            for flow_id in list(desired_workflows.keys()):
                writeWorkflowHistCSV(flow_id)
