import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# Function to generate Date_Dim data
def create_date_dim(start_date, end_date):
    date_data = []
    current_date = start_date

    while current_date <= end_date:
        date_dim_entry = {
            "Date_ID": int(current_date.strftime("%Y%m%d")),  # Use YYYYMMDD as unique Date_ID
            "Full_Date": current_date,
            "Year": current_date.year,
            "Quarter": (current_date.month - 1) // 3 + 1,  # Calculate quarter
            "Month": current_date.month,
            "Day": current_date.day,
            "Weekday": current_date.strftime("%A"),  # Day name (e.g., Monday)
            "Is_Weekend": current_date.weekday() >= 5  # True if Saturday (5) or Sunday (6)
        }
        date_data.append(date_dim_entry)
        current_date += timedelta(days=1)  # Increment by one day

    return pd.DataFrame(date_data)

# Define the date range
start_date = datetime(2000, 1, 1)
end_date = datetime(2025, 1, 1)

# Generate the Date_Dim DataFrame
date_dim_df = create_date_dim(start_date, end_date)

# Define Date_Dim table schema
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Date_Dim (
    Date_ID INTEGER PRIMARY KEY,
    Full_Date DATE,
    Year INTEGER,
    Quarter INTEGER,
    Month INTEGER,
    Day INTEGER,
    Weekday VARCHAR(20),
    Is_Weekend BOOLEAN
);
""")

# Execute table creation
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load data into Date_Dim table
date_dim_df.to_sql('Date_Dim', engine, if_exists='replace', index=False)

print("Date_Dim table created and data loaded successfully.")
