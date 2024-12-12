# Analyzing Business Success Impact Factors With Yelp & Crime Data

roject Overview
This project explores factors influencing business success and customer satisfaction in Philadelphia using the Yelp dataset and Philadelphia Crime Data. Key areas of focus include the relationship between check-ins, time, cuisine type, and crime, as well as the interplay between public safety and business performance.

Key Business Questions
How do the time of day, day of the week, and cuisine type impact customer check-ins?
Which cuisine types experience the highest check-ins during different times of the day?
What is the impact of crime on businesses in Philadelphia?
What is the relationship between the number of reviews, reactions to reviews, and tips?
How do public safety concerns influence business types, customer behaviors, check-in times, and reviews?
Datasets
Yelp Dataset
Philadelphia Crime Data
Schema Design
Fact Tables
Crime_Fact

Grain: Each row represents a single crime event.
Attributes: Crime_ID, Dispatch_Date_ID, Dispatch_Time_ID, Location_ID, etc.
Check_In_Fact

Grain: Each row represents a single customer check-in.
Attributes: Business_ID, single_timestamp.
User_Fact

Grain: Each row represents user activity within a time period.
Attributes: User_ID, Date_ID, Useful, Funny, Cool, etc.
Review_Fact

Grain: Each row represents a user review for a business.
Attributes: Review_ID, Business_ID, User_ID, Stars, Text, etc.
Dimensions
Business_Dim: Details about businesses (e.g., location, category).
Location_Dim: Details about locations (e.g., City, State).
Date_Dim: Calendar dates.
Time_Dim: Specific times (e.g., hour, minute, second).
Tip_Dim: User tips for businesses.
Slowly Changing Dimensions
SCD Type 2: Used in Business_Dim for full historical tracking.
SCD Type 3: Used in Location_Dim for limited historical tracking.
ETL Process
Data extracted from Yelp JSON and Philadelphia Crime CSV datasets.
Crime data was reduced from 2M+ rows to ~40K rows for efficiency.
Delta data handled for SCD maintenance:
Type 2: Full history tracked for Business_Dim.
Type 3: Current and previous states tracked for Location_Dim.
Key Transformations
Aggregated check-ins by time and business for analysis.
Joined business data with crime data to analyze location-based impacts.
Created a clean and normalized schema in PostgreSQL.
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
Implementation Notes
Tools: PostgreSQL, Python, Tableau, Power BI.
Challenges:
Processing large datasets (2M+ crime records).
Implementing multiple SCD types.
Integrating semi-structured (JSON) and structured data.
How to Run the Project
Download Data Source.
Set up a PostgreSQL database.
Load the provided datasets into the database.
Run the ETL scripts (Python and SQL) to transform and load the data into the schema.
Use Tableau to visualize the results using the queries provided.
Additional Deliverables
ERD: Fully labeled constellation schema.
ETL Diagrams: Physical and logical data flows.
Source Code: Python and SQL scripts for ETL and analytics.
Delta Report: Includes updates from feedback received during the presentation.
Conclusion
This project highlights how customer behaviors, business performance, and public safety interconnect in Philadelphia. By analyzing Yelp and crime data, businesses can gain actionable insights to improve strategies and adapt to challenges in high-crime areas.
