import pandas as pd
from sqlalchemy import create_engine

# Read cleaned.csv into a dataframe
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

'''
|-------------------------------------|
|  Populate the database 'pollution'  |
|-------------------------------------|
'''

# Create sqlalchemy engine for connecting to database
user = 'root'
pw = ''
db = 'pollution-db2'
sql_engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

# Insert merged_df_stations DataFrame into MySQL table 'location' 
df_merged_stations.to_sql('location', con = sql_engine, if_exists = 'append', chunksize = 1000, index=False)

# Insert data DataFrame into MySQL table 'airquality' 
data.to_sql('airquality', con = sql_engine, if_exists = 'append', chunksize = 1000, index=False)