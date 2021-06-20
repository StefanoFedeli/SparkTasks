import pandas as pd       #data processing
import numpy as np        #linear algebra
#import seaborn as sns     #visualization

data=pd.read_csv("data/yellow_tripdata_2018-09.csv")
print(data.head())
print(data.isnull().sum())
print(data.describe())

sns.countplot(x='VendorID',data=data)
sns.countplot(x='passenger_count',data=data)

print(data['trip_distance'].value_counts())
