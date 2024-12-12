import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load crime.csv
crime_file = '/Users/yinxc/Desktop/CS689/termprojectdata/crime_simplified.csv'
crime_data = pd.read_csv(crime_file)

# Define Crime table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Crime (
    "Dc_Dist" INTEGER,
    "Psa" INTEGER,
    "Dispatch_Date_Time" TIMESTAMP,
    "Dispatch_Date" DATE,
    "Dispatch_Time" TIME,
    "Hour" INTEGER,
    "Dc_Key" BIGINT,
    "Location_Block" TEXT,
    "UCR_General" INTEGER,
    "Text_General_Code" TEXT,
    "Police_Districts" TEXT,
    "Month" TEXT,
    "Lon" FLOAT,
    "Lat" FLOAT
);
""")

# Execute table creation query
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load data into Crime table
crime_data.to_sql('Crime_Lightweight', engine, if_exists='replace', index=False)

print("Crime_Lightweight table loaded successfully.")
