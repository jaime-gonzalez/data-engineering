SELECT
"Date" AS sale_date,
"Store_ID" AS store_ID,
"Product_ID" AS product_id,
SUM("Units") AS product_quantity,
COUNT(DISTINCT "Sale_ID") AS sales_quantity
FROM {{ source('raw', 'sales') }}
GROUP BY 1,2,3