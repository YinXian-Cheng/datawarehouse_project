CREATE TABLE yelp_checkin_cleaned AS
WITH Expanded_Timestamps AS (
    SELECT
        business_id,
        unnest(string_to_array(date, ','))::timestamp AS single_timestamp
    FROM
        yelp_checkin
)
SELECT
    business_id,
    single_timestamp
FROM
    Expanded_Timestamps;
