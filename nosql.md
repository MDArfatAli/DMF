## Problem Statement:  

1. Model the data for a specific monitor (station) to a NoSQL data model
2. Pipe or import the data.

## Implementation steps:

### 1. Download and installing mongodb

For this work, I chose MongoDB and utilized MongoDB Compass to create and populate the database. 
So, I install mongodb compass and pymongo using "python -m pip install pymongo" command.
    
### 2. Creating database and Collection
I have created database named AirQuality and Collection named Readings using mongodb compass.
	
	Database: AirQuality
    
    Collection: Readings


### 3. Modeling the data for specific monitor (station) to a mongodb data mode


```
[
  {
	"SiteID": 203,
    "Location": "Brislington Depot",
    "geo_point_2d": "51.4417471802,-2.55995583224",
    "AirQualityReadings": [
		  Date Time:"2013-08-23 19:00:00+00:00"
		  NOx:95.75
		  NO2:65.5
		  NO:19.75
		  PM10:null
		  NVPM10:null
		  VPM10:null
		  NVPM2.5:null
		  PM2.5:null
		  VPM2.5:null
		  CO:null
		  O3:null
		  SO2:null
		  Temperature:null
		  RH:null
		  Air Pressure:null
		  DateStart:"2002-02-01T00:00:00+00:00"
		  DateEnd:null
		  Current:true
		  Instrument Type,:"Continuous (Reference)"
]

```

### 3. Importing Json Data to Mongodb using python

#### 3.1. Convert dataframe to json data accroding to data model.

    j_convert = (df.groupby(['SiteID', 'Location', 'geo_point_2d'], as_index=True)
       .apply(lambda x:x[['Date Time', 'NOx', 'NO2', 'NO', 'PM10', 'NVPM10', 'VPM10',
       'NVPM2.5', 'PM2.5', 'VPM2.5', 'CO', 'O3', 'SO2', 'Temperature', 'RH',
       'Air Pressure','DateStart', 'DateEnd',
       'Current', 'Instrument Type,']].to_dict('records'))
       .reset_index()
       .rename(columns={0:'AirQualityReadings'})
       .to_json(orient='records'))


#### 3.2. Connecting with MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = myclient["AirQuality"]
    Collection = db["Readings"]

#### 3.3. Inserting the loaded data in the Collection
    Collection.insert_many(json.loads(j_convert))
    print(json.dumps(json.loads(j_convert), indent=2, sort_keys=True))


### 4. Query implementation.

#### 4.1 Pipeline Design: 

```
[{$project: {
  SiteID: 1,
  Location: 1,
  'AirQualityReadings.DateTime': 1,
  'AirQualityReadings.NO': 1,
  'AirQualityReadings.NO2': 1
}}, {$addFields: {
 AirQualityReadings: {
        $map: {
          input: "$AirQualityReadings",
          in: {
            $mergeObjects: [
              "$$this",
              {
                year :  { $substr : ["$$this.DateTime", 0, 4 ] },
                hour :  { $substr : ["$$this.DateTime", 11, 2 ] }
              }
            ]
          }
        }
      }
}},{$unwind: {
  path: "$AirQualityReadings"
}}, {$group: {
  _id: '$SiteID',
  Mean_Of_NO: {
    $avg: '$AirQualityReadings.NO'
  },
  Mean_Of_NO2: {
    $avg: '$AirQualityReadings.NO2'
  }
}}]

```

### 5. Result of the Query:

![My result](https://raw.githubusercontent.com/MDArfatAli/DMF/main/c-query.jpg)