WITH orders_per_day AS (
    SELECT
        date_date
        ,COUNT(orders_id) AS nb_transactions --distinct yapmadım, çünkü orders_id'ler zaten tekil. chatgpt ekstradan distinct yaparsam sorgu performansı düşer dedi. belki if'li bi şey ekleyebilirim. bunu bi sorcam
        ,SUM(revenue) AS revenue
        ,SUM(margin) AS margin
        ,SUM(operational_margin) AS operational_margin
        ,SUM(purchase_cost) AS purchase_cost
        ,SUM(shipping_fee) AS shipping_fee
        ,SUM(log_cost) AS log_cost
        ,SUM(ship_cost) AS ship_cost
        ,SUM(quantity) AS quantity
    FROM {{ ref("int_orders_operational") }}
    GROUP BY date_date
)
SELECT
    date_date
    ,ROUND(revenue,0) AS revenue
    ,ROUND(margin,0) AS margin
    ,ROUND(operational_margin,0) AS operational_margin
    ,ROUND(purchase_cost,0) AS purchase_cost
    ,ROUND(shipping_fee,0) AS shipping_fee
    ,ROUND(log_cost,0) AS log_cost
    ,ROUND(ship_cost,0) AS ship_cost
    ,quantity
    ,ROUND(revenue/NULLIF(nb_transactions, 0), 2) AS average_basket
FROM orders_per_day
ORDER BY date_date DESC