import numpy as np 
import pandas as pd 
from datetime import datetime, date, timedelta
from workflowspeople_fetch import pcfetch
import csv

#set global date variable 
date_string = date.today()

#set path for fetching and writing files - this will be different for each person running the code
workflow_fetchpath = r'/Users/jacealloway/Desktop/python/pco_access/workflowexports/'
campuspeople_fetchpath = r'/Users/jacealloway/Desktop/python/pco_access/campuspeopleexports/'
team_fetchpath = r'Users/jacealloway/Desktop/python/pco_access/teamexports/' 

#path to writing files
writepath = r'/Users/jacealloway/Desktop/python/pco_access/analyzed/'
WF_filename = r'workflows.csv'
NEWPPL_filename = r'newpeople.csv'

#load in the people data
people_data_df = pd.read_csv(campuspeople_fetchpath + f'peopledata.csv', delimiter = ',')


#get campus dictionary
Campus_Dict = pcfetch.getCampusIDS()
#get workflow dictionary
Workflow_Dict = pcfetch.getWorkflowIDS()
#get workflow steps dictionary
WorkflowSteps_Dict = pcfetch.getWorkflowSteps(Workflow_Dict)


#load dict of desired workflows. - try to write a way to do this automatically instead of manually
team_workflows={
'548715': 'CREATIVE TEAM',
# '544795': 'DT BAPTISMS',
# '544778': 'DT CHILD DEDICATIONS',
'564585': 'DT MAINTENANCE TEAM',
# '544755': 'DT NEW PEOPLE FOLLOW UP',        #FOR WINNIE
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
# '548180': 'HAMILTON NEW PEOPLE FOLLOW UP',      #FOR WINNIE
'544664': 'HAMILTON TEAM ONBOARDING - C3 KIDS',
'550421': 'HAMILTON TEAM ONBOARDING - HOSPITALITY',
'544725': 'HAMILTON TEAM ONBOARDING - HOSTING',
'550386': 'HAMILTON TEAM ONBOARDING - MUSCLE',
'544846': 'HAMILTON TEAM ONBOARDING - WORSHIP',
# '555089': 'Love This City',
'564646': 'MT MAINTENANCE TEAM',
# '548148': 'MT NEW PEOPLE FOLLOW UP',        #FOR WINNIE
'544649': 'MT TEAM ONBOARDING - C3 KIDS',
'550418': 'MT TEAM ONBOARDING - HOSPITALITY',
'544718': 'MT TEAM ONBOARDING - HOSTING',
'550368': 'MT TEAM ONBOARDING - PRODUCTION',
'544881': 'MT TEAM ONBOARDING - SERVICE PRODUCTION',
'544844': 'MT TEAM ONBOARDING - WORSHIP',
'550397': 'MT TEAM ONBOARDING - YOUTH',
# '561167': 'TEST'
}

newpeople_workflows={
'544755': 'DT NEW PEOPLE FOLLOW UP',        #FOR WINNIE
'548180': 'HAMILTON NEW PEOPLE FOLLOW UP',      #FOR WINNIE
'548148': 'MT NEW PEOPLE FOLLOW UP'        #FOR WINNIE
}



#function to compute amount of days and hours between two timestrings
def getTimeGap(time1: str, time2: str) -> str:
    """
    Compute the elapsed time between two inputs. 

    Args:
        time1: Input of format 'yyyy-mm-ddThh:mm:ssZ' (str)
        time2: Input of format 'yyyy-mm-ddThh:mm:ssZ' (str)

    Returns: 
        Number of days and hours between inputs (str).
    """
    format = '%Y-%m-%dT%H:%M:%SZ'


    dt = datetime.strptime(time1, format) - datetime.strptime(time2, format)

    return str(dt.days).replace('-', '')  

def reformatTimestring(time: str) -> str:
    old_format = f'%Y-%m-%dT%H:%M:%SZ'
    new_format = f'%m/%d/%Y'

    old_string = datetime.strptime(time, old_format)
    new_string = datetime.strftime(old_string, new_format)

    return new_string


def week_end_sunday(input_date):
    """
    Compute the week-end leading Sunday based off a given time string.
    """
    format = f'%m/%d/%Y'
    weekday = datetime.strptime(input_date, format).isoweekday()

    output_date = datetime.strptime(input_date, format) + timedelta(days = int(7 - (np.ceil(weekday))  ))

    return datetime.strftime(output_date.date(), format)


def reformatNAN(value: str | float) -> bool:
    """    
    Replace any NaN value with empty string.
    """
    if value != value:
        value = ''
        return value
    
    else:
        return value



def getPerson(person_id: int, people_dataframe: list[int]) -> dict:
    """
    Match peson ID with all person info. 

    Returns: dict = {
        'name',
        'gender',
        'address',
        'child',
        'campus name'    
    }

    """
    idx = np.where(people_dataframe['id'].values == person_id)[0][0]
    person_info = people_dataframe.T[idx]

    name = person_info.get('name')
    gender = person_info.get('gender')
    email = person_info.get('email')
    child_bool = person_info.get('child')
    
    if np.isnan(person_info.get('primary_campus_id')):
        campus = ''
    else:
        try:
            campus_id = int(person_info.get('primary_campus_id'))
            campus = Campus_Dict[f'{campus_id}']
        except KeyError:
            campus = ''

    return  {'name' : name, 'gender' : gender, 'address' : email, 'child' : child_bool, 'campus name' : campus}





def workflowExportCSV(
        workflow_list: dict,
        filename: str
        ) -> None:
    """
    insert documentation here 
    """

    cols = [
        'full name',
        'workflow name',
        'workflow step',
        'time initiated',
        'time initiated week-end',
        'time completed',
        'time completed week-end',
        'days at step',
        'gender',
        'address',
        'child',
        'campus name',
        'assignee',
        'assignee address',
        'week end sunday'
    ]

    try:
        with open(writepath + f'{filename}', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=cols)
            writer.writeheader()

            for id in list(workflow_list.keys()):
                #load export data and steps for current ID
                workflow_data = pd.read_csv(workflow_fetchpath + f'{id}export.csv', delimiter=',')
                stepslist = WorkflowSteps_Dict[f'{id}']
                workflow_name = Workflow_Dict[f'{id}']['name']

                #get number of people in current workflow
                num_people = len(workflow_data.index)
                #get total number of steps per workflow
                N = len(list(stepslist))
                

                for idx in range(num_people):
                    person = getPerson(workflow_data['person id'].values[idx], people_data_df)
                    name = person['name']
                    gender = person['gender']
                    child_bool = person['child']
                    campus = person['campus name']
                    email = person['address']

                    #check if an assignee exists. If it doesn't, append as blank values.
                    try:
                        assignee = getPerson(workflow_data['assignee id'].values[idx], people_data_df)
                        assignee_name = assignee['name']
                        assignee_email = assignee['address']
                    except IndexError:
                        assignee_name = ''
                        assignee_email = ''

                    gender = reformatNAN(gender)
                    email = reformatNAN(email)
                    assignee_email = reformatNAN(assignee_email)

                    #if a person has been removed from the WF, we move to the next person.
                    removed = str(workflow_data['removed'].values[idx])
                    if removed == 'True':
                        continue

                    #evaluate time stamps
                    for sequence in range(N-1):
                        step_name = stepslist[sequence]['name']
                        try:
                            time_initiated = str(workflow_data[f'{sequence} created at'].values[idx])
                            time_completed = str(workflow_data[f'{sequence+1} created at'].values[idx])
                        except KeyError:
                            time_initiated = str(workflow_data[f'{sequence} created at'].values[idx])
                            time_completed = ''


                        current_time = str(datetime.now().strftime(f"%Y-%m-%dT%H:%M:%SZ"))


                        #remove nan values, evaluated time stamps
                        try:
                            if time_initiated.startswith('nan'):
                                #break the loop and do not write the row if the step has not been initiated
                                init_sunday = ''
                                comp_sunday = ''
                                continue

                            if time_completed.startswith('nan'):
                                time_completed = ''
                                days_at_step = getTimeGap(time_initiated, current_time)
                            
                                #compute week-end sunday
                                init_sunday = week_end_sunday(datetime.strftime(datetime.strptime(time_initiated, f"%Y-%m-%dT%H:%M:%SZ"),  f'%m/%d/%Y'))
                                comp_sunday = ''

                                #reformat the string if not empty
                                time_initiated = reformatTimestring(time_initiated)

                            else:
                                days_at_step = getTimeGap(time_initiated, time_completed)

                                #compute week-end sunday
                                init_sunday = week_end_sunday(datetime.strftime(datetime.strptime(time_initiated, f"%Y-%m-%dT%H:%M:%SZ"),  f'%m/%d/%Y'))
                                comp_sunday = week_end_sunday(datetime.strftime(datetime.strptime(time_initiated, f"%Y-%m-%dT%H:%M:%SZ"),  f'%m/%d/%Y'))

                                #reformat the strings if not empty
                                time_initiated = reformatTimestring(time_initiated)
                                time_completed = reformatTimestring(time_completed)

                        except ValueError:
                            days_at_step = ''
                            init_sunday = week_end_sunday(datetime.strftime(datetime.strptime(time_initiated, f"%Y-%m-%dT%H:%M:%SZ"),  f'%m/%d/%Y'))
                            comp_sunday = ''


                        
                        row = {
                            'full name' : name,
                            'workflow name' : workflow_name,
                            'workflow step' : step_name,
                            'time initiated' : time_initiated,
                            'time initiated week-end' : init_sunday,
                            'time completed' : time_completed,
                            'time completed week-end' : comp_sunday,
                            'days at step' : days_at_step,
                            'gender' : gender,
                            'address' : email,
                            'child' : child_bool,
                            'campus name' : campus,
                            'assignee' : assignee_name,
                            'assignee address' : assignee_email
                        }
                    
                        writer.writerow(row)



            print(f"CSV file '{filename}' created successfully on {date_string}.")

    except IOError:
        print("I/O error while writing CSV.")
        



workflowExportCSV(team_workflows, WF_filename)
workflowExportCSV(newpeople_workflows, NEWPPL_filename)


