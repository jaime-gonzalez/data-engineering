SELECT
"Store_ID" AS store_id,
"Store_Name" AS store_name,
"Store_City" AS store_city,
"Store_Location" AS store_location,
"Store_Open_Date" AS store_open_date
FROM {{ source('raw', 'stores') }}