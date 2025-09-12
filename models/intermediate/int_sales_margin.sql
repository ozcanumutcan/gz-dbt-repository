WITH sales_with_product AS (
    SELECT
        s.products_id,
        s.date_date,
        s.orders_id,
        s.revenue,
        s.quantity,
        p.purchase_price
    FROM {{ ref('stg_gz_raw_data__sales') }} AS s
    LEFT JOIN {{ ref('stg_gz_raw_data__product') }} AS p
        ON s.products_id = p.products_id
)

SELECT
    products_id,
    date_date,
    orders_id,
    revenue,
    quantity,
    purchase_price,
    ROUND(quantity * COALESCE(purchase_price, 0), 2) AS purchase_cost,
    ROUND(revenue - quantity * COALESCE(purchase_price, 0), 2) AS margin
FROM sales_with_product