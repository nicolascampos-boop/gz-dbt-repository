with

int_sales_margin as (

    select * from {{ ref('int_sales_margin') }}

),

aggregated as (

    select
        orders_id,
        MAX(date_date) as date_date,
        SUM(revenue) as revenue,
        SUM(purchase_cost) as purchase_cost,
        SUM(margin) as margin

    from int_sales_margin

    group by orders_id

)

select * from aggregated