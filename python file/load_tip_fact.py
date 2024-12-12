import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Load yelp_tip.json
tip_file = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_tip.json'
tip_data = pd.read_json(tip_file, lines=True)

# Define Tip_Fact table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Tip_Fact (
    "User_ID" TEXT,
    "Business_ID" TEXT,
    "Text" TEXT,
    "Date" TIMESTAMP,
    "Compliment_Count" INTEGER
);
""")

# Execute table creation query
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load data into Yelp_Tip table
tip_data.to_sql('Tip_Fact', engine, if_exists='replace', index=False)

print("Tip_Fact table loaded successfully.")
