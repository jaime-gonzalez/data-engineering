WITH

dm_stores AS(SELECT * FROM {{ ref('dm_stores') }}),

dm_inventory AS(SELECT * FROM {{ ref('dm_inventory') }}),

dm_products AS(SELECT * FROM {{ ref('dm_products') }}),

ft_sales AS(SELECT * FROM {{ ref('ft_sales') }}),

final AS(
SELECT
fs.sale_date,
ds.store_name,
ds.store_city,
ds.store_location,
ds.store_open_date,
dp.product_name,
dp.product_category,
dp.product_price,
dp.product_cost,
di.stock_on_hand,
SUM(fs.sales_quantity) AS sales_quantity,
SUM(fs.product_quantity) AS product_quantity,
SUM(COALESCE(dp.product_cost,0) * COALESCE(fs.product_quantity,0) * COALESCE(fs.sales_quantity,0)) AS gross_cost,
SUM(COALESCE(dp.product_price,0) * COALESCE(fs.product_quantity,0) * COALESCE(fs.sales_quantity,0)) AS gross_revenue,
SUM((COALESCE(dp.product_price,0) * COALESCE(fs.product_quantity,0) * COALESCE(fs.sales_quantity,0)) - (COALESCE(dp.product_cost,0) * COALESCE(fs.product_quantity,0) * COALESCE(fs.sales_quantity,0))) AS gross_profit
FROM ft_sales fs

LEFT JOIN dm_products dp 
ON fs.product_id = dp.product_id

LEFT JOIN dm_stores ds
ON fs.store_id=ds.store_id

LEFT JOIN dm_inventory di
ON fs.store_id=di.store_id AND fs.product_id = di.product_id

GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM final