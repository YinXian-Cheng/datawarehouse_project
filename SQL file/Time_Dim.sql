INSERT INTO "Crime_Fact" (
    "Crime_ID", 
    "Dispatch_Date_ID", 
    "Dispatch_Time_ID", 
    "Location_ID", 
    "Dc_Dist", 
    "Psa", 
    "Dc_Key", 
    "Location_Block", 
    "UCR_General", 
    "Text_General_Code"
)
SELECT DISTINCT 
    nextval('crime_id_seq') AS "Crime_ID", -- Generate unique Crime_ID
    CAST(TO_CHAR(TO_DATE(c."Dispatch_Date", 'YYYY-MM-DD'), 'YYYYMMDD') AS INTEGER) AS "Dispatch_Date_ID",
    (EXTRACT(HOUR FROM TO_TIMESTAMP(c."Dispatch_Time", 'HH24:MI:SS')) * 3600 + 
     EXTRACT(MINUTE FROM TO_TIMESTAMP(c."Dispatch_Time", 'HH24:MI:SS')) * 60 + 
     EXTRACT(SECOND FROM TO_TIMESTAMP(c."Dispatch_Time", 'HH24:MI:SS')) + 1) AS "Dispatch_Time_ID",
    l."Location_ID",
    c."Dc_Dist",
    c."Psa",
    c."Dc_Key",
    c."Location_Block",
    c."UCR_General",
    c."Text_General_Code"
FROM "Crime_Lightweight" c
LEFT JOIN "Location_Dim" l
    ON (l."Location_Block" = c."Location_Block" OR l."Police_Districts" = c."Police_Districts")
GROUP BY 
    c."Dispatch_Date",
    c."Dispatch_Time",
    c."Dc_Key",
    l."Location_ID",
    c."Dc_Dist",
    c."Psa",
    c."Location_Block",
    c."UCR_General",
    c."Text_General_Code";
