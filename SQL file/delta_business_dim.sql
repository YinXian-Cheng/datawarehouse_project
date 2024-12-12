-- Step 1: mark the old data as expired with is_current = 0
WITH Expired AS (
    UPDATE "Business_Dim"
    SET 
        "Is_Current" = 0,
        "Effective_End_Date" = d."Effective_Start_Date"::DATE - INTERVAL '1 day'
    FROM "Business_Dim_Delta" d
    WHERE "Business_Dim"."Business_ID" = d."Business_ID"
    AND (
        "Business_Dim"."Name" != d."Name" OR
        "Business_Dim"."Location" != d."Location" OR
        "Business_Dim"."Primary_Category" != d."Primary_Category" OR
        "Business_Dim"."Secondary_Category" != d."Secondary_Category" OR
        "Business_Dim"."Tertiary_Category" != d."Tertiary_Category" OR
        "Business_Dim"."Outdoor_Seating" != d."Outdoor_Seating" OR
        "Business_Dim"."Parking_Availability" != d."Parking_Availability"
    )
    RETURNING "Business_Dim"."Business_ID"
)

-- Step 2: insert newly added or updated record
INSERT INTO "Business_Dim" (
    "Business_ID",
    "Name",
    "Location",
    "Primary_Category",
    "Secondary_Category",
    "Tertiary_Category",
    "Is_Current",
    "Effective_Start_Date",
    "Effective_End_Date",
    "Outdoor_Seating",
    "Parking_Availability"
)
SELECT 
    d."Business_ID",
    d."Name",
    d."Location",
    d."Primary_Category",
    d."Secondary_Category",
    d."Tertiary_Category",
    1,
    d."Effective_Start_Date",
    '9999-12-31',
    d."Outdoor_Seating",
    d."Parking_Availability"
FROM "Business_Dim_Delta" d
LEFT JOIN "Business_Dim" b
ON d."Business_ID" = b."Business_ID"
WHERE b."Business_ID" IS NULL
   OR (
        b."Name" != d."Name" OR
        b."Location" != d."Location" OR
        b."Primary_Category" != d."Primary_Category" OR
        b."Secondary_Category" != d."Secondary_Category" OR
        b."Tertiary_Category" != d."Tertiary_Category" OR
        b."Outdoor_Seating" != d."Outdoor_Seating" OR
        b."Parking_Availability" != d."Parking_Availability"
    );
