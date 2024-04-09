
import pandas as pd
import csv
question = "Percent of adults aged 18 years and older who have an overweight classification"
data = pd.read_csv('nutrition_small.csv', usecols=['YearStart', 'YearEnd', 'LocationDesc','Question','Data_Value', 'StratificationCategory1', 'Stratification1'])
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
data7 = data7.get_group((question, 'National'))['Data_Value'].mean()
global_m = 50
data3 = data3.apply(lambda x: global_m - x)
#print(data)
#print(data2)
#print(data3)
#print(data3.iloc[0])
print(data3)
#print(data3.to_dict())
#print(data3.keys()[0][1])
#keys = data3.keys()
#for value in data3.keys():
#    print(value[1], data3[value])
#print(data6)
#print(data7)
#print(data5.iloc[0])