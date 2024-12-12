import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load reference tables
with engine.connect() as connection:
    date_dim_df = pd.read_sql('SELECT "Date_ID", "Full_Date" FROM "Date_Dim"', connection)
    print("Date_Dim loaded successfully.")
    time_dim_df = pd.read_sql('SELECT "Time_ID", "Hour", "Minute", "Second" FROM "Time_Dim"', connection)
    print("Time_Dim loaded successfully.")
    location_dim_df = pd.read_sql('SELECT "Location_ID" FROM "Location_Dim"', connection)
    print("Location_Dim loaded successfully.")

# Ensure Full_Date is in datetime format
date_dim_df['Full_Date'] = pd.to_datetime(date_dim_df['Full_Date'])

# Function to get Date_ID and Time_ID
def get_date_time_ids(timestamp):
    try:
        parsed_datetime = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        parsed_date = parsed_datetime.date()
        parsed_time = parsed_datetime.time()

        # Find Date_ID
        date_id_row = date_dim_df.loc[date_dim_df['Full_Date'] == parsed_date, 'Date_ID']
        date_id = date_id_row.values[0] if not date_id_row.empty else None

        # Find Time_ID
        time_id_row = time_dim_df.loc[
            (time_dim_df['Hour'] == parsed_time.hour) &
            (time_dim_df['Minute'] == parsed_time.minute) &
            (time_dim_df['Second'] == parsed_time.second),
            'Time_ID'
        ]
        time_id = time_id_row.values[0] if not time_id_row.empty else None

        return date_id, time_id
    except Exception as e:
        print(f"Error parsing timestamp {timestamp}: {e}")
        return None, None

# Flatten and aggregate check-in data
def process_batch(batch):
    checkin_fact_list = []
    for record in batch:
        business_id = record.get("business_id", "")
        timestamps = record.get("date", "").split(", ") if record.get("date") else []

        # The Location_ID is the same as Business_ID for each location
        location_id = business_id

        # Process each timestamp
        for timestamp in timestamps:
            date_id, time_id = get_date_time_ids(timestamp)
            checkin_fact_list.append({
                "Business_ID": business_id,
                "Location_ID": location_id,
                "Date_ID": date_id,
                "Time_ID": time_id,
                "Check_In_Count": 1,  # Each timestamp represents one check-in
                "Check_In_Date_Time_List": timestamp  # Keep individual timestamp as a list
            })
    return pd.DataFrame(checkin_fact_list)

# Define Check_In_Fact table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Check_In_Fact (
    Business_ID VARCHAR(50),
    Location_ID VARCHAR(50),
    Date_ID INTEGER,
    Time_ID INTEGER,
    Check_In_Count INTEGER,
    Check_In_Date_Time_List TEXT,
    PRIMARY KEY (Business_ID, Date_ID, Time_ID)
);
""")

with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Check_In_Fact table schema created successfully.")

# Load JSON file in batches
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_checkin.json'
batch_size = 10000
batch = []
count = 0

def load_data_to_database(df):
    with engine.connect() as connection:
        df.to_sql('Check_In_Fact', engine, if_exists='append', index=False)

try:
    with open(file_path, 'r') as f:
        print("Started loading data from yelp_checkin.json.")
        for line in f:
            record = json.loads(line.strip())
            batch.append(record)
            if len(batch) == batch_size:
                checkin_fact_df = process_batch(batch)
                load_data_to_database(checkin_fact_df)
                count += 1
                print(f"Batch {count} loaded successfully.")
                batch = []  # Clear batch after processing

        # Process the remaining records
        if batch:
            checkin_fact_df = process_batch(batch)
            load_data_to_database(checkin_fact_df)
            count += 1
            print(f"Batch {count} loaded successfully.")

except Exception as e:
    print(f"Error occurred: {e}")

print("All data from yelp_checkin.json loaded successfully.")
