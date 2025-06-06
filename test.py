
import pandas as pd
import csv
import json
'''
question = "Percent of adults who engage in no leisure-time physical activity"
data = pd.read_csv('nutrition_activity_obesity_usa_subset.csv', usecols=['YearStart', 'YearEnd', 'LocationDesc','Question','Data_Value', 'StratificationCategory1', 'Stratification1'])
data = data.dropna()
# mean per intrebare
data2 = data.groupby('Question')['Data_Value'].mean()
#mean per stat-> toate
data3 = data.loc[data['Question'] == question].groupby(['Question', 'LocationDesc'])['Data_Value'].mean()
data5 = data.loc[data['Question'] == question].groupby(['Question'])['Data_Value'].mean()
data3 = data3.sort_values(ascending=False)
# Merge cand ai intrebare specifica
data6 = data.groupby(['Question'])
data6 = data6.get_group((question,))['Data_Value'].mean()
# Merge cand ai stat + intrebare specifica
data7 = data.groupby(['Question', 'LocationDesc'])
data7 = data7.get_group((question, 'Nebraska'))['Data_Value'].mean()
global_m = 50
data3 = data3.apply(lambda x: global_m - x)

data8 = data.loc[data['Question'] == question].groupby(['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
data9 = data.loc[data['Question'] == question].groupby(['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
data10 = data[(data['Question'] == question) & (data['LocationDesc'] == 'Ohio')]
data10 = data10.groupby(['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
#print(data)
#print(data2)
#print(data3)
#print(data3.iloc[0])
#print(data3)
#print(data3.to_dict())
#print(data3.keys()[0][1])
#keys = data3.keys()
#print(data6)
#print(data7)
#print(data5.iloc[0])
#print(data8)
#print(data9)
data8.to_dict()
dictionary = dict()
counter = 0
for key in data8.keys():
    dictionary[(key[1], key[2], key[3])] = data8[key]
    counter += 1
    if counter == 2:
        break
#print(dictionary)
#print(data9)
#print(data10)

rez = data.groupby(['Question'])
rez = rez.get_group((question,))['Data_Value'].mean()
result = rez
print(result)
'''
question = "Percent of adults who engage in no leisure-time physical activity"
state = "Ohio"
data = pd.read_csv('unittests/nutrition_small.csv')
'''rez = data.groupby(['Question'])
rez = rez.get_group((question,))['Data_Value']
global_mean = rez.mean()
cols = ["Question", "LocationDesc"]
col = 'Question'
rez = data.loc[data[col] == question].groupby(cols)['Data_Value']._get_numeric_data().mean()
global_extra_mean = rez.mean()
rez = rez.to_dict()
values_dict = dict()
for key in rez.keys():
    values_dict[key[1]] = global_mean - rez[key]
# add synch here
# end synch
'''
cols = ["Question", "LocationDesc"]
col = 'Question'
rez = data.loc[data[col]
                == question].groupby(cols)['Data_Value'].mean()
rez = rez.to_dict()
values_dict = {}
for key in rez.keys():
    values_dict[key[1]] = rez[key]

print(values_dict)
'''

cols = ["Question", "LocationDesc"]
rez = data.groupby(cols)
rez = rez.get_group((question, state))['Data_Value'].mean()
result = {state : rez}
with open("test_output", 'w') as outfile:
   json.dump(values_dict, outfile)
'''
print(result)