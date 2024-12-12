import pandas as pd
import json
from sqlalchemy import create_engine, text

# Create database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load JSON file into a list
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_user.json'
json_data = []
with open(file_path, 'r') as f:
    for line in f:
        json_data.append(json.loads(line))

# Transform JSON data into DataFrame
def transform_user_data(json_data):
    user_dim_list = []
    for record in json_data:
        user_dim_list.append({
            "User_ID": record.get("user_id", ""),
            "Name": record.get("name", ""),
            "Yelping_Since": record.get("yelping_since", None),  # Keep as string for now
        })
    return pd.DataFrame(user_dim_list)

user_dim_df = transform_user_data(json_data)

# Define User_Dim table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS User_Dim (
    User_ID VARCHAR(50) PRIMARY KEY,
    Name VARCHAR(255),
    Yelping_Since TIMESTAMP
);
""")

# Execute table creation query
with engine.connect() as connection:
    connection.execute(create_table_query)

# Write the data into the database table
user_dim_df.to_sql('User_Dim', engine, if_exists='fail', index=False)

print("Initial load completed for User_Dim.")
