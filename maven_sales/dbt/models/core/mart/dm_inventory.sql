SELECT
"Store_ID" AS store_id,
"Product_ID" AS product_id,
"Stock_On_Hand" AS stock_on_hand
FROM {{ source('raw', 'inventory') }}