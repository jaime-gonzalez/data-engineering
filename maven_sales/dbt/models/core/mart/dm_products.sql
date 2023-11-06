SELECT
    "Product_ID" AS product_id,
    LOWER("Product_Name") AS product_name,
    LOWER("Product_Category") AS product_category,
    regexp_replace("Product_Cost", '[$,]', '', 'g')::float AS product_cost,
    regexp_replace("Product_Price", '[$,]', '', 'g')::float AS product_price
FROM {{ source('raw', 'products') }} 