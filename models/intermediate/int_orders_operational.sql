with

orders_margin as (

    select * from {{ ref('int_orders_margin') }}

),

ship as (

    select * from {{ ref('stg_raw__ship') }}

),

joined as (

    select
        orders_margin.orders_id,
        orders_margin.date_date,
        orders_margin.revenue,
        orders_margin.purchase_cost,
        orders_margin.margin,
        ship.shipping_fee,
        ship.ship_cost,
        orders_margin.margin + ship.shipping_fee - ship.ship_cost as operational_margin

    from orders_margin

    left join ship
        on orders_margin.orders_id = ship.orders_id

)

select * from joined