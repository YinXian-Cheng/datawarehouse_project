import pandas as pd
import json
from sqlalchemy import create_engine, text

# 创建数据库连接
engine = create_engine('postgresql+psycopg2://postgres:313622@localhost:5432/termproject')

# 加载 JSON 文件到 DataFrame
file_path = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_business.json'
json_data = []
with open(file_path, 'r') as f:
    for line in f:
        json_data.append(json.loads(line))

# 转换数据为 DataFrame
def transform_business_data(json_data):
    business_dim_list = []
    for record in json_data:
        attributes = record.get("attributes") or {}
        category_list = record.get("categories", "").split(", ") if record.get("categories") else []
        primary_category = category_list[0] if len(category_list) > 0 else "Unknown"
        secondary_category = category_list[1] if len(category_list) > 1 else None
        tertiary_category = category_list[2] if len(category_list) > 2 else None

        business_dim_list.append({
            "Business_ID": record.get("business_id", ""),
            "Name": record.get("name", ""),
            "Location": record.get("address", ""),
            "Primary_Category": primary_category,
            "Secondary_Category": secondary_category,
            "Tertiary_Category": tertiary_category,
            "Is_Current": 1,
            "Effective_Start_Date": pd.Timestamp.now().date(),
            "Effective_End_Date": "9999-12-31",
            "Outdoor_Seating": attributes.get("OutdoorSeating", "Unknown"),
            "Parking_Availability": attributes.get("ParkingAvailability", "Unknown")
        })
    return pd.DataFrame(business_dim_list)

business_dim_df = transform_business_data(json_data)

# 定义 Business_Dim 表结构
create_table_query = text("""
CREATE TABLE IF NOT EXISTS Business_Dim (
    Business_ID VARCHAR(50) PRIMARY KEY,
    Name VARCHAR(255),
    Location VARCHAR(255),
    Primary_Category VARCHAR(100),
    Secondary_Category VARCHAR(100),
    Tertiary_Category VARCHAR(100),
    Is_Current INT,
    Effective_Start_Date DATE,
    Effective_End_Date DATE,
    Outdoor_Seating VARCHAR(50),
    Parking_Availability VARCHAR(50)
);
""")

# 执行表创建语句
with engine.connect() as connection:
    connection.execute(create_table_query)

# 将数据写入数据库
business_dim_df.to_sql('Business_Dim', engine, if_exists='fail', index=False)

print("Initial load completed for Business_Dim.")
