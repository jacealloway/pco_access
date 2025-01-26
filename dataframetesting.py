import numpy as np 
import pandas as pd 
from datetime import date, datetime

print(datetime.now().strftime(f"%Y-%m-%dT%H:%M:%SZ"))


date_string = '2024-12-11'



# path = r'/Users/jacealloway/Desktop/python/C3/workflowexports/'
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


