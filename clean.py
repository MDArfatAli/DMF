import pandas as pd

# read cropped.csv(task 1a result) into a pandas dataframe
cropped_data = pd.read_csv("cropped.csv", sep=';')

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

# check for missing value in 'SiteID'
print("Missing value for SiteID:",data['SiteID'].isna().sum())

# Converting the stations dict to dataframe
df_stations = pd.DataFrame(stations.items(), columns = ['Station ID','Station Name'])

# Merged the dataframes based on SiteID, Location comparing with Station Id and Station Name.
# keep indication = True to get the result with a column to the output DataFrame called “_merge” with information on the source of each row
merged_df = pd.merge(left=cropped_data, right=df_stations, how='left', left_on=['SiteID','Location'], right_on = ['Station ID','Station Name'], indicator=True)

# checking the Dud records(mismatch of 'SiteID' and'Location' in between both dataframes)
dud_mask = merged_df['_merge'] != "both"
dud_records_df = merged_df[dud_mask]
print(f"{dud_records_df.shape[0]} dud records were found.")

# Print mismatch field values
print('\n\nDud records:\n')
print(dud_records_df[['SiteID', 'Location']])

# Correct records
result = merged_df[~dud_mask]
# Remove the columns that created through merge which are not required anymore.
result = result.drop(['Station ID','Station Name', '_merge'], axis=1)

# Exporting the cleaned data to cleaned.csv file without generating index as column.
result.to_csv('cleaned.csv', sep=';', index=False)
