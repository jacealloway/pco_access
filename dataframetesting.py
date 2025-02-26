import numpy as np 
import pandas as pd 
from datetime import date, datetime, timedelta

# datetime.now().strftime(f"%Y-%m-%dT%H:%M:%SZ")
# datetime.now().strftime(f"%d/%m/%Y")



format = f"%d/%m/%Y"
date_string = datetime.strptime('2024-11-19T19:35:27Z', "%Y-%m-%dT%H:%M:%SZ")
new_string = datetime.strftime(date_string, format)



def week_end_sunday(input_date):
    f'%m/%d/%Y'
    weekday = datetime.strptime(input_date, format).isoweekday()

    output_date = datetime.strptime(input_date, format) + timedelta(days= int(7 - (np.ceil(weekday))  ))

    return datetime.strftime(output_date.date(), format)



print(new_string, week_end_sunday(new_string))





# path = r'/Users/jacealloway/Desktop/python/pco_access/analyzed/'


# df1  = pd.read_csv(path + f'544593export{date_string}.csv', delimiter=',')
# df2  = pd.read_csv(path + f'544777export{date_string}.csv', delimiter=',')

# # print(df1)
# print(df2)

# #this works for merging data files with identical columns, use ignore_index = True to reinstate indexing for rows along side of df
# df_merged = pd.concat([df1, df2], ignore_index=True)
# # print(df_merged)

# #for df2, num steps is 5
# # print(df2['4 created at'].values[1])

# # nan_val = df2['4 created at'].values[1]


# print(df1)


#some function for JSON navigation
def getdictdir(input_dict: dict):
    """
    Retrieve directory of dictionary keys for nested input.
    """
    for key, value in input_dict.items():
        if type(value) is dict:
            yield (key, value)
            yield from getdictdir(value)
        else:
            yield (key, value)


def printdictkeys(input_dict: dict) -> tuple:
    """
    Print dictionary keys for nested input from func: getdictdir call.
    """
    for key, value in getdictdir(input_dict):
        print(key, value)


# data = np.loadtxt('joe mamam.txt', unpack=True)


# df = pd.read_csv(path + 'workflows.csv', delimiter=',')
# N = len(df.gender.values)

# print(df.gender.values)

# def isnan(value):
#     if value != value:
#         value = ''
#         return value
    
#     else:
#         return value

# for k in range(N):
#     df.gender.values[k] = isnan(df.gender.values[k])





