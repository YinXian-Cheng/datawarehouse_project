import pandas as pd
import random
from datetime import datetime

# Load original Location_Dim data
original_data = pd.DataFrame({
    "Location_ID": [hash(("123 Main St", "Philadelphia", "PA")), hash(("456 Elm St", "Philadelphia", "PA"))],
    "Postal_Code": ["19103", "19104"],
    "Address": ["123 Main St", "456 Elm St"],
    "City": ["Philadelphia", "Philadelphia"],
    "State": ["PA", "PA"],
    "Location_Block": ["Main St Block", "Elm St Block"],
    "Police_Districts": ["District 1", "District 2"],
    "Latitude": [39.9526, 39.9527],
    "Longitude": [-75.1652, -75.1653],
    "Start_Date": ["2000-01-01", "2000-01-01"],
    "End_Date": ["9999-12-31", "9999-12-31"],
    "Current_Flag": [True, True]
})

# Create delta data (modifying some records and adding new ones)
delta_data = pd.DataFrame({
    "Location_ID": [
        original_data.loc[0, "Location_ID"],  # Simulate an update for existing record
        hash(("789 Oak St", "Philadelphia", "PA"))  # Simulate a new record
    ],
    "Postal_Code": ["19103", "19106"],  # Update postal code for first record, new for second
    "Address": ["123 Main St", "789 Oak St"],  # Same address for update, new for insert
    "City": ["Philadelphia", "Philadelphia"],
    "State": ["PA", "PA"],
    "Location_Block": ["Updated Main St Block", "Oak St Block"],  # Update block for first record
    "Police_Districts": ["Updated District 1", "District 3"],  # Update district for first record
    "Latitude": [39.9530, 39.9540],  # Slightly modified latitude
    "Longitude": [-75.1655, -75.1660],  # Slightly modified longitude
    "Start_Date": ["2023-12-01", "2023-12-01"],  # Set start date for delta data
    "End_Date": ["9999-12-31", "9999-12-31"],
    "Current_Flag": [True, True]
})

# Save the delta data to a CSV or JSON file
delta_file = '/Users/yinxc/Desktop/CS689/termprojectdata/location_dim_delta.json'
delta_data.to_json(delta_file, orient='records', lines=True)

print("Delta data created and saved to:", delta_file)

# Display delta data for verification
print(delta_data)
