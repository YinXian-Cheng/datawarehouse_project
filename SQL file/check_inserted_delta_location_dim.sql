SELECT *
FROM "Location_Dim" dim
LEFT JOIN "Location_Dim_Delta" delta
    ON dim."Location_ID" = delta."Location_ID"
WHERE delta."Location_ID" = dim."Location_ID"
