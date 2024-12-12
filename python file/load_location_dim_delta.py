import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load the delta data from the JSON file
delta_file = '/Users/yinxc/Desktop/CS689/termprojectdata/location_dim_delta.json'
delta_data = pd.read_json(delta_file, lines=True)

# Define the Location_Dim_Delta table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS "Location_Dim_Delta" (
    "Location_ID" BIGINT PRIMARY KEY,
    "Postal_Code" VARCHAR(20),
    "Address" TEXT,
    "City" VARCHAR(100),
    "State" VARCHAR(50),
    "Location_Block" TEXT,
    "Police_Districts" TEXT,
    "Latitude" FLOAT,
    "Longitude" FLOAT,
    "Start_Date" DATE,
    "End_Date" DATE,
    "Current_Flag" BOOLEAN
);
""")

# Create table and load data into Location_Dim_Delta
with engine.connect() as connection:
    # Create the table if it does not exist
    connection.execute(create_table_query)
    
    # Load data into the table
    delta_data.to_sql("Location_Dim_Delta", con=connection, if_exists="replace", index=False)

print("Location_Dim_Delta table loaded successfully.")
