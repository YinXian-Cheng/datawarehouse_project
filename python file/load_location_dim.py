import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load yelp_business_philadelphia.json
yelp_file = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_business_philadelphia.json'
yelp_data = pd.read_json(yelp_file, lines=True)

# Load crime.csv
crime_file = '/Users/yinxc/Desktop/CS689/termprojectdata/crime_simplified.csv'
crime_data = pd.read_csv(crime_file)

# Transform Yelp data (only including relevant columns)
yelp_transformed = yelp_data[['business_id', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude']]
yelp_transformed.rename(columns={
    'business_id': 'Location_ID',
    'address': 'Address',
    'city': 'City',
    'state': 'State',
    'postal_code': 'Postal_Code',
    'latitude': 'Latitude',
    'longitude': 'Longitude'
}, inplace=True)

# Add default values for missing columns in Yelp data
yelp_transformed['Location_Block'] = None
yelp_transformed['Police_Districts'] = None
yelp_transformed['Start_Date'] = "2000-01-01"
yelp_transformed['End_Date'] = '9999-12-31'
yelp_transformed['Current_Flag'] = True

# Generate Location_ID based on Address, City, and State
yelp_transformed["Location_ID"] = yelp_transformed.apply(
    lambda x: hash((x.Address, x.City, x.State)), axis=1
)

# Transform Crime data (only including relevant columns)
crime_transformed = crime_data[['Location_Block', 'Police_Districts', 'Lat', 'Lon']]
crime_transformed.rename(columns={
    'Lat': 'Latitude',
    'Lon': 'Longitude',
}, inplace=True)

# Add default values for missing columns in Crime data
crime_transformed['Postal_Code'] = None
crime_transformed['Address'] = None
crime_transformed['City'] = "Philadelphia"  # As crimes are specific to Philadelphia
crime_transformed['State'] = "PA"
crime_transformed['Start_Date'] = "2000-01-01"
crime_transformed['End_Date'] = '9999-12-31'
crime_transformed['Current_Flag'] = True

# Generate Location_ID based on Location_Block and Police_Districts
crime_transformed["Location_ID"] = crime_transformed.apply(
    lambda x: hash((x.Location_Block, x.Police_Districts)), axis=1
)

# Combine both datasets (Yelp and Crime)
combined_data = pd.concat([yelp_transformed, crime_transformed], ignore_index=True)

# Define the Location_Dim table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Location_Dim (
    Location_ID BIGINT PRIMARY KEY,
    Postal_Code VARCHAR(20),
    Address TEXT,
    City VARCHAR(100),
    State VARCHAR(50),
    Location_Block TEXT,
    Police_Districts TEXT,
    Latitude FLOAT,
    Longitude FLOAT,
    Start_Date DATE,
    End_Date DATE,
    Current_Flag BOOLEAN
);
""")

# Execute table creation query
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load data into Location_Dim
with engine.connect() as connection:
    combined_data.to_sql("Location_Dim", con=connection, if_exists="replace", index=False)

print("Location_Dim table loaded successfully.")
