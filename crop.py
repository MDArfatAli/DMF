import pandas as pd

# Read csv data as pandas dataframe
data = pd.read_csv("bristol-air-quality-data.csv", sep=';')
print("Before cropping data rows:", data.shape[0])

# See the info of the data - (data type, null value)
print(data.info())

#converting the 'Date Time' column to datetime all non null values have only actual valid timestamps.
data['Date Time'] = pd.to_datetime(data['Date Time'], format='%Y-%m-%d %H:%M:%S')

#delete any rows where 'Date Time' column have dates before 00:00 1 Jan 2010
mask = data["Date Time"].dt.year < 2010
data = data[~mask]
print("After cropping data rows:", data.shape[0])


# Exporting the data to cropped.csv file without generating index as column.
data.to_csv('cropped.csv', sep=';', index=False)