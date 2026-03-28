with

orders_operational as (

    select * from {{ ref('int_orders_operational') }}

),

sales_margin as (

    select * from {{ ref('int_sales_margin') }}

),

quantity_per_order as (

    select
        orders_id,
        SUM(quantity) as quantity

    from sales_margin

    group by orders_id

),

joined as (

    select
        orders_operational.orders_id,
        orders_operational.date_date,
        orders_operational.revenue,
        orders_operational.purchase_cost,
        orders_operational.margin,
        orders_operational.shipping_fee,
        orders_operational.ship_cost,
        orders_operational.operational_margin,
        quantity_per_order.quantity

    from orders_operational

    left join quantity_per_order
        on orders_operational.orders_id = quantity_per_order.orders_id

),

daily as (

    select
        date_date,
        COUNT(DISTINCT orders_id)            as nb_transactions,
        SUM(revenue)                         as revenue,
        SUM(revenue) / COUNT(DISTINCT orders_id) as average_basket,
        SUM(operational_margin)              as operational_margin,
        SUM(purchase_cost)                   as purchase_cost,
        SUM(shipping_fee)                    as shipping_fee,
        SUM(ship_cost)                       as ship_cost,
        SUM(quantity)                        as quantity

    from joined

    group by date_date

)

select * from daily
