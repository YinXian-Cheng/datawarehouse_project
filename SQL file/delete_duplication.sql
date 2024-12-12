WITH cte AS (
    SELECT 
        "Location_ID",
        "Postal_Code",
        "Address",
        "City",
        "State",
        "Location_Block",
        "Police_Districts",
        "Latitude",
        "Longitude",
        "Start_Date",
        "End_Date",
        "Current_Flag",
        ROW_NUMBER() OVER (
            PARTITION BY "Location_ID" 
            ORDER BY "Start_Date" DESC
        ) AS rn
    FROM "Location_Dim"
)
DELETE FROM "Location_Dim"
WHERE "Location_ID" IN (
    SELECT "Location_ID" 
    FROM cte 
    WHERE rn > 1
);
