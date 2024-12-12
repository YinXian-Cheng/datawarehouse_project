import pandas as pd
from datetime import datetime
import json
from sqlalchemy import create_engine, text

# File path for the yelp_review.json file
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_review.json'

# Sample defaults for fields without references
default_user_id = "UNKNOWN_USER"
default_time_id = "0000"

# Helper function to convert a datetime string to Date_ID and Time_ID
def extract_date_time_ids(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    date_id = int(date_obj.strftime("%Y%m%d"))  # Format YYYYMMDD for Date_ID
    time_id = int(date_obj.strftime("%H%M"))    # Format HHMM for Time_ID
    return date_id, time_id

# Parse JSON file and load review data into Review_Fact format
review_data = []
with open(file_path, 'r') as f:
    for line in f:
        record = json.loads(line)
        
        # Extract date and time IDs
        date_id, time_id = extract_date_time_ids(record["date"])
        
        # Prepare the review fact record
        review_fact = {
            "Review_ID": record["review_id"],
            "Business_ID": record["business_id"],
            "User_ID": record.get("user_id", default_user_id),  # Use actual ID or default
            "Date_ID": date_id,
            "Time_ID": time_id if time_id else default_time_id,  # Use actual time ID or default
            "Review_Count": 1,  # Each review counts as 1 instance
            "Stars": record.get("stars", 0),  # Default to 0 if stars missing
            "Useful": record.get("useful", 0),
            "Funny": record.get("funny", 0),
            "Cool": record.get("cool", 0),
            "Text": record.get("text", "")
        }
        
        review_data.append(review_fact)

# Convert list of dicts to DataFrame
review_fact_df = pd.DataFrame(review_data)

# Display the first 20 rows for verification
print("First 20 rows of Review_Fact DataFrame:")
print(review_fact_df.head(20))

# Create connection to PostgreSQL using SQLAlchemy
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Step 1: Query Business_Dim and Date_Dim to get valid IDs
with engine.connect() as connection:
    business_query = 'SELECT "Business_ID" FROM "Business_Dim"'
    business_ids_db = pd.read_sql(business_query, connection)
    
    date_query = 'SELECT "Date_ID" FROM "Date_Dim"'
    date_ids_db = pd.read_sql(date_query, connection)

# Step 2: Filter Review_Fact DataFrame with valid Business_IDs and Date_IDs
review_fact_df = review_fact_df[
    (review_fact_df['Business_ID'].isin(business_ids_db['Business_ID'])) &
    (review_fact_df['Date_ID'].isin(date_ids_db['Date_ID']))
]

# Log mismatched IDs for debugging
mismatched_business_ids = set(review_fact_df['Business_ID']) - set(business_ids_db['Business_ID'])
if mismatched_business_ids:
    print("Mismatched Business_IDs:", mismatched_business_ids)

mismatched_date_ids = set(review_fact_df['Date_ID']) - set(date_ids_db['Date_ID'])
if mismatched_date_ids:
    print("Mismatched Date_IDs:", mismatched_date_ids)

# Step 3: Define the Review_Fact table schema in SQL
create_table_query = text("""
CREATE TABLE IF NOT EXISTS "Review_Fact" (
    "Review_ID" VARCHAR(50) PRIMARY KEY,
    "Business_ID" VARCHAR(50),
    "User_ID" VARCHAR(50),
    "Date_ID" INTEGER,
    "Time_ID" INTEGER,
    "Review_Count" INTEGER,
    "Stars" REAL,
    "Useful" INTEGER,
    "Funny" INTEGER,
    "Cool" INTEGER,
    "Text" TEXT
);
""")

# Step 4: Execute the create table command
with engine.connect() as connection:
    connection.execute(create_table_query)

# Step 5: Load the filtered DataFrame into PostgreSQL as Review_Fact table
review_fact_df.to_sql('Review_Fact', engine, if_exists='replace', index=False)

# Verification: Print message indicating successful load
print("Review_Fact data loaded successfully into PostgreSQL.")
