import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# JSON file path
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_user.json'

# Load Date_Dim and Time_Dim data for reference
with engine.connect() as connection:
    date_dim_df = pd.read_sql('SELECT "Date_ID", "Full_Date" FROM "Date_Dim"', connection)
    time_dim_df = pd.read_sql('SELECT "Time_ID", "Hour", "Minute", "Second" FROM "Time_Dim"', connection)

# Helper function to find Date_ID and Time_ID
def get_date_time_ids(yelping_since):
    try:
        yelp_datetime = datetime.strptime(yelping_since, "%Y-%m-%d %H:%M:%S")
        # Find Date_ID
        date_id = date_dim_df.loc[date_dim_df['Full_Date'].dt.date == yelp_datetime.date(), 'Date_ID']
        date_id = date_id.values[0] if not date_id.empty else None
        # Find Time_ID
        time_id = time_dim_df.loc[
            (time_dim_df['Hour'] == yelp_datetime.hour) &
            (time_dim_df['Minute'] == yelp_datetime.minute) &
            (time_dim_df['Second'] == yelp_datetime.second), 'Time_ID']
        time_id = time_id.values[0] if not time_id.empty else None
        return date_id, time_id
    except Exception:
        return None, None

# Define User_Fact table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS "User_Fact" (
    "User_ID" VARCHAR(50),
    "Date_ID" INTEGER,
    "Time_ID" INTEGER,
    "Useful" INTEGER,
    "Funny" INTEGER,
    "Cool" INTEGER,
    "Elite" VARCHAR(255),
    "Review_Count" INTEGER,
    PRIMARY KEY ("User_ID")
);
""")

with engine.connect() as connection:
    connection.execute(create_table_query)

# Process the JSON file in chunks
batch_size = 10000
batch = []

def process_batch(batch):
    # Transform data for User_Fact
    user_fact_list = []
    for record in batch:
        yelping_since = record.get("yelping_since", None)
        date_id, time_id = get_date_time_ids(yelping_since) if yelping_since else (None, None)
        user_fact_entry = {
            "User_ID": record.get("user_id", ""),
            "Date_ID": date_id,
            "Time_ID": time_id,
            "Useful": record.get("useful", 0),
            "Funny": record.get("funny", 0),
            "Cool": record.get("cool", 0),
            "Elite": record.get("elite", ""),
            "Review_Count": record.get("review_count", 0)
        }
        user_fact_list.append(user_fact_entry)

    user_fact_df = pd.DataFrame(user_fact_list)
    # Append to database
    with engine.connect() as connection:
        user_fact_df.to_sql('User_Fact', connection, if_exists='append', index=False)

try:
    with open(file_path, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                batch.append(record)
                if len(batch) == batch_size:
                    process_batch(batch)
                    batch = []  # Clear the batch after processing
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON line: {line.strip()} - {e}")
        # Process remaining records in the last batch
        if batch:
            process_batch(batch)
except Exception as e:
    print(f"Error reading file: {e}")

print("User_Fact table loaded successfully.")
