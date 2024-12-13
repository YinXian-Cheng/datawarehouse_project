# Analyzing Business Success Impact Factors With Yelp & Crime Data
Project Overview
This project explores factors influencing business success and customer satisfaction in Philadelphia using the Yelp dataset and Philadelphia Crime Data. It integrates structured and semi-structured data to uncover trends and insights into customer behaviors, public safety, and business performance. The project applies advanced data warehousing techniques and SQL queries for analytics and visualization.

Key Business Questions
How do the time of day, day of the week, and cuisine type impact customer check-ins?
Which cuisine types experience the highest check-ins during different times of the day?
What is the impact of crime on businesses in Philadelphia?
What is the relationship between the number of reviews, reactions to reviews, and tips?
How do public safety concerns influence business types, customer behaviors, check-in times, and reviews?
Datasets
Yelp Dataset: JSON-based dataset containing business, user, and review data.
Philadelphia Crime Data: CSV dataset containing details of crime incidents, locations, and timestamps.
Challenges in the Dataset
Size: Crime data initially contained over 2 million rows, which was reduced to ~40,000 rows for efficiency.
Integration: Merging structured (crime data) and semi-structured (Yelp JSON) data required careful normalization and transformation.
Geospatial Analysis: Lat/Long coordinates were calculated for crime data to align with business locations.
Schema Design
Fact Tables
Crime_Fact

Grain: Each row represents a single crime event.
Attributes: Crime_ID, Dispatch_Date_ID, Dispatch_Time_ID, Location_ID, Lat, Lon, etc.
Check_In_Fact

Grain: Each row represents a single customer check-in.
Attributes: Business_ID, single_timestamp.
User_Fact

Grain: Each row represents user activity within a time period.
Attributes: User_ID, Date_ID, Useful, Funny, Cool, etc.
Review_Fact

Grain: Each row represents a user review for a business.
Attributes: Review_ID, Business_ID, User_ID, Stars, Text, Useful, Cool, etc.
Dimensions
Business_Dim: Details about businesses, including name, category, and location.
Location_Dim: Geographical information (City, State, Latitude, Longitude).
Date_Dim: Calendar details, including Weekday and Month.
Time_Dim: Specific times of the day (hour, minute, second).
Tip_Dim: User-generated tips with timestamps and compliment counts.
Slowly Changing Dimensions (SCDs)
SCD Type 2: Tracks historical changes for Business_Dim (e.g., location or category changes).
SCD Type 3: Tracks limited history for Location_Dim (e.g., current and previous city/state).
ETL Process
Data Extraction:

Yelp JSON data and Philadelphia Crime CSV are ingested.
Semi-structured JSON data is normalized for relational database usage.
Data Transformation:

Data cleaning, including handling missing values and standardizing formats.
Delta loads implemented for SCD maintenance:
Type 2: Maintains full history in Business_Dim.
Type 3: Tracks current and previous states in Location_Dim.
Data Loading:

Data is loaded into a PostgreSQL database with schema design applied.
Aggregated tables are created for efficient querying and visualization.
Key Transformations
Aggregated check-ins by time and business for analysis.
Merged business data with crime data based on geospatial proximity.
Created additional columns (e.g., Weekday in Date_Dim) for better analysis granularity.
Analytics and Visualizations
Visualizations
Time, Day, and Cuisine Impact on Check-Ins

Stacked bar chart showing trends by cuisine, time of day, and day of week.
Cuisine Check-In Trends by Time

Bar chart highlighting peak hours for different cuisines.
Crime Impact on Businesses

Scatter plot showing spatial relationships between crime and business performance.
Review and Reaction Analysis

Line chart showing trends in reviews, tips, and user reactions (e.g., Funny, Cool).
Public Safety and Customer Behavior

Combined analysis showing the correlation between safety concerns and business success.
Analytical Queries
SQL queries used advanced techniques such as window functions, Common Table Expressions (CTEs), and pivot tables to uncover trends.
Implementation Notes
Tools: PostgreSQL, Python, Tableau, Power BI.
Challenges:
Processing large datasets (2M+ crime records).
Normalizing semi-structured data (Yelp JSON).
Calculating geospatial proximity for crime and business locations.
How to Run the Project
Set Up:
Install PostgreSQL, Python, and Tableau/Power BI.
Load Data:
Download the datasets and load them into the PostgreSQL database.
ETL:
Run Python and SQL scripts to clean, transform, and load data into the schema.
Analytics:
Use the provided SQL queries to generate insights and export visualizations to Tableau/Power BI.
Additional Deliverables
ERD: Fully labeled constellation schema in Lucidchart or draw.io.
ETL Diagrams: Physical and logical data flows for transformations and loading.
Source Code: Python and SQL scripts with documentation.
Delta Report: Includes feedback updates and additional insights after the presentation.

Conclusion
This project demonstrates how customer behaviors, public safety, and business success interconnect in Philadelphia. By combining Yelp and crime data, the analysis offers actionable insights to optimize business strategies and address challenges in high-crime areas.
