CREATE TABLE Checkin_Analysis AS
WITH Expanded_Checkins AS (
    SELECT
        yc.business_id,
        unnest(string_to_array(yc.date, ','))::timestamp AS checkin_timestamp
    FROM 
        yelp_checkin yc
),
Checkin_Summary AS (
    SELECT
        ec.business_id,
        b."Primary_Category" AS Primary_Category,
        b."Secondary_Category" AS Secondary_Category,
        COALESCE(b."Tertiary_Category", 'Miscellaneous') AS Tertiary_Category,
        EXTRACT(DOW FROM ec.checkin_timestamp) AS Day_Of_Week,
        CASE 
            WHEN EXTRACT(HOUR FROM ec.checkin_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM ec.checkin_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM ec.checkin_timestamp) BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS Time_Of_Day,
        COUNT(*) AS Total_Checkins
    FROM 
        Expanded_Checkins ec
    JOIN 
        "Business_Dim" b ON ec.business_id = b."Business_ID"
    GROUP BY 
        ec.business_id, b."Primary_Category", b."Secondary_Category", b."Tertiary_Category", Day_Of_Week, Time_Of_Day
)
SELECT 
    business_id,
    Primary_Category,
    Secondary_Category,
    Tertiary_Category,
    CASE 
        WHEN Day_Of_Week = 0 THEN 'Sunday'
        WHEN Day_Of_Week = 1 THEN 'Monday'
        WHEN Day_Of_Week = 2 THEN 'Tuesday'
        WHEN Day_Of_Week = 3 THEN 'Wednesday'
        WHEN Day_Of_Week = 4 THEN 'Thursday'
        WHEN Day_Of_Week = 5 THEN 'Friday'
        WHEN Day_Of_Week = 6 THEN 'Saturday'
    END AS Day_Of_Week,
    Time_Of_Day,
    Total_Checkins,
    6 AS Number_Of_Hours,
    ROUND(Total_Checkins::numeric / 6, 2) AS Avg_Checkins_Per_Hour
FROM 
    Checkin_Summary
ORDER BY 
    Primary_Category, Secondary_Category, Tertiary_Category, Day_Of_Week, Time_Of_Day;
