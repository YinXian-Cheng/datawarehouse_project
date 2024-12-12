import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load JSON file
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_checkin.json'
checkin_data = []
with open(file_path, 'r') as f:
    for line in f:
        checkin_data.append(json.loads(line))

# Load Date_Dim and Time_Dim data for reference
with engine.connect() as connection:
    date_dim_df = pd.read_sql('SELECT "Date_ID", "Full_Date" FROM "Date_Dim"', connection)
    time_dim_df = pd.read_sql('SELECT "Time_ID", "Hour", "Minute", "Second" FROM "Time_Dim"', connection)

# Ensure Full_Date is in datetime format
date_dim_df['Full_Date'] = pd.to_datetime(date_dim_df['Full_Date'])

# Function to find Date_ID and Time_ID
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

# Flatten the check-in data with Date_ID and Time_ID lookup
def flatten_checkin_data(checkin_data):
    checkin_fact_list = []
    for record in checkin_data:
        business_id = record.get("business_id", "")
        timestamps = record.get("date", "").split(", ") if record.get("date") else []
        
        for timestamp in timestamps:
            date_id, time_id = get_date_time_ids(timestamp)
            if date_id and time_id:
                checkin_fact_list.append({
                    "Business_ID": business_id,
                    "Date_ID": date_id,
                    "Time_ID": time_id
                })
    return pd.DataFrame(checkin_fact_list)

# Transform check-in data into a DataFrame
checkin_fact_df = flatten_checkin_data(checkin_data)

# Define Check_In_Fact table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Check_In_Fact (
    Business_ID VARCHAR(50),
    Date_ID INTEGER,
    Time_ID INTEGER,
    PRIMARY KEY (Business_ID, Date_ID, Time_ID)
);
""")

# Execute table creation
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load data into Check_In_Fact table
checkin_fact_df.to_sql('Check_In_Fact', engine, if_exists='replace', index=False)

print("Check_In_Fact table loaded successfully.")
