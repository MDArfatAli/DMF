import pandas as pd

'''
  A function to insert sql data from pandas dataframe
  dataframe is any dataframe to insert into SQL database
  table takes a string value which is the destination table name.
'''
def insert_sql_from_dataframe(dataframe, table):
    scripts = []
    half_insert = 'INSERT INTO '+table+' VALUES '
    for index in range(100):    
        scripts.append(half_insert + str(tuple(dataframe.iloc[index].values))+';')        
    return '\n\n'.join(scripts)


#read cleaned.csv into a dataframe
data = pd.read_csv("cleaned.csv", sep=';')

# Preparing the data for tables.

'''
|-------------------------------------|
|          Table: location            |
|-------------------------------------|
'''

# Given 18 stations list.
stations = {
    188 : 'AURN Bristol Centre', 203 : 'Brislington Depot',
    206 : 'Rupert Street', 209 : 'IKEA M32',
    213 : 'Old Market', 215 : 'Parson Street School',
    228 : 'Temple Meads Station', 270 : 'Wells Road',
    271 : 'Trailer Portway P&R', 375 : 'Newfoundland Road Police Station',
    395 : "Shiner's Garage", 452 : 'AURN St Pauls',
    447 : 'Bath Road', 459 : 'Cheltenham Road \ Station Road',
    463 : 'Fishponds Road', 481 : 'CREATE Centre Roof',
    500 : 'Temple Way', 501 : 'Colston Avenue'
}

# Converting the stations dict to dataframe
df_stations = pd.DataFrame(stations.items(), columns = ['Id','Station Name'])


# Keep the first data of each SiteID and drop others.
df_locations = data.drop_duplicates('SiteID', keep='first')

# Select the required column for the location table.
df_locations = df_locations[['SiteID', 'Location', 'geo_point_2d']]

# Left joining both of the location data to makes sure all stations are available in the list.
df_merged_stations = pd.merge(left=df_stations, right=df_locations, how='left', left_on=['Id','Station Name'], right_on = ['SiteID','Location'])
# Select necessary columns 
df_merged_stations = df_merged_stations[['Id', 'Location', 'geo_point_2d']]
# Replace nan to empty string.
df_merged_stations.fillna('', inplace=True)


'''
|-------------------------------------|
|        Table: airquality            |
|-------------------------------------|
'''

# Remove unnecessary columns
df_airquality = data.drop(['Location', 'geo_point_2d'], axis=1)
# Auto increamental Id column.
df_airquality.insert(0, 'Id', 'AUTO')
# Replace nan to empty string.
df_airquality.fillna('', inplace=True)



# Writing sql in a file
f = open('indert-100.sql', 'w')
f.write('\n--Table: location')
f.write('\n-------------------------------------------\n')
f.write(insert_sql_from_dataframe(df_merged_stations, 'location'))
f.write('\n\n\n-- Table: airquality')
f.write('\n-------------------------------------------\n')
f.write(insert_sql_from_dataframe(df_airquality, 'airquality'))
f.close()
