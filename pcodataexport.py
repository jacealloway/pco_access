import numpy as np 
import pandas as pd 
from datetime import datetime, date
from planningcenterauth import pcfetch
import csv

#set global date variable 
date_string = date.today()

#set path for fetching and writing files - this will be different for each person running the code
workflow_fetchpath = r'/Users/jacealloway/Desktop/python/C3/workflowexports/'
campuspeople_fetchpath = r'/Users/jacealloway/Desktop/python/C3/campuspeopleexports/'
# team_fetchpath = r'Users/jacealloway/Desktop/python/C3/teamexports/' #don't need this yet

#path to writing files
writepath = r'/Users/jacealloway/Desktop/python/C3/analyzed/'
WF_filename = f'workflows{date_string}.csv'


people_data_df = pd.read_csv(campuspeople_fetchpath + f'peopledata{date_string}.csv', delimiter = ',')



#get campus dictionary
Campus_Dict = pcfetch.getCampusIDS()
#get workflow dictionary
Workflow_Dict = pcfetch.getWorkflowIDS()
#get workflow steps dictionary
WorkflowSteps_Dict = pcfetch.getWorkflowSteps(Workflow_Dict)


#load dict of desired workflows. - try to write a way to do this automatically instead of manually
desired_workflows={
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
# '561167': 'TEST')
}






#not too concerned about this function
def getTimestring(time: str) -> str:
    """
    Convert a numerical time string to readable string.

    Input format: "2024-08-30T22:26:53Z"
    """
    year = time[0:4]
    month = time[5:7]
    day = time[8:10]


    hour = int(time[11:13])
    if hour > 12:
        hour = str(hour%12)+'pm'

    months = {'01' : 'Jan',
              '02' : 'Feb',
              '03' : 'Mar',
              '04' : 'Apr',
              '05' : 'May',
              '06' : 'Jun',
              '07' : 'Jul',
              '08' : 'Aug',
              '09' : 'Sep',
              '10' : 'Oct',
              '11' : 'Nov',
              '12' : 'Dec'}
    
    for month_entry in list(months.keys()):
        if month_entry == month:
            month = months[month_entry]

    return month+day+year+','+hour

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
    format = '%Y-%m-%dT%H:%M:%SZ'


    dt = datetime.strptime(time1, format) - datetime.strptime(time2, format)

    return str(dt.days).replace('-', '')  # +  ' days'   #uncomment if you want 'days' units tied to return string





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





def workflowExportCSV(workflow_list: dict) -> None:
    """
    insert documentation here 
    """

    cols = [
        'full name',
        'workflow name',
        'workflow step',
        'time initiated',
        'days at step',
        'time completed',
        'gender',
        'address',
        'child',
        'campus name',
        'assignee',
        'assignee address'
    ]

    try:
        with open(writepath + f'{WF_filename}', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=cols)
            writer.writeheader()

            for id in list(workflow_list.keys()):
                #load export data and steps for current ID
                workflow_data = pd.read_csv(workflow_fetchpath + f'{id}export{date_string}.csv', delimiter=',')
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

                    assignee = getPerson(workflow_data['assignee id'].values[idx], people_data_df)
                    assignee_name = assignee['name']
                    assignee_email = assignee['address']

                    #if a person has been removed from the WF, we move to the next person.
                    removed = str(workflow_data['removed'].values[idx])
                    if removed == 'True':
                        continue

                    for sequence in range(N-1):
                        step_name = stepslist[sequence]['name']
                        try:
                            time_initiated = str(workflow_data[f'{sequence} created at'].values[idx])
                            time_completed = str(workflow_data[f'{sequence+1} created at'].values[idx])
                        except KeyError:
                            time_initiated = str(workflow_data[f'{sequence} created at'].values[idx])
                            time_completed = ''


                        current_time = str(datetime.now().strftime(f"%Y-%m-%dT%H:%M:%SZ"))

                        try:
                            days_at_step = getTimeGap(time_initiated, time_completed)
                        except ValueError:
                            days_at_step = ''

                        try:
                            if time_completed.startswith('nan'):
                                days_at_step = getTimeGap(time_initiated, current_time)
                        except ValueError:
                            days_at_step = ''



                        row = {
                            'full name' : name,
                            'workflow name' : workflow_name,
                            'workflow step' : step_name,
                            'time initiated' : time_initiated,
                            'days at step' : days_at_step,
                            'time completed' : time_completed,
                            'gender' : gender,
                            'address' : email,
                            'child' : child_bool,
                            'campus name' : campus,
                            'assignee' : assignee_name,
                            'assignee address' : assignee_email
                        }
                    
                        writer.writerow(row)



            print(f"CSV file '{WF_filename}' created successfully.")

    except IOError:
        print("I/O error while writing CSV.")
        



workflowExportCSV(desired_workflows)

