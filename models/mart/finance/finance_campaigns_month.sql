with

finance_campaigns_day as (

    select * from {{ ref('finance_campaigns_day') }}

),

monthly as (

    select
        DATE_TRUNC(date, MONTH)          as datemonth,
        SUM(revenue)                     as revenue,
        SUM(purchase_cost)               as purchase_cost,
        SUM(margin)                      as margin,
        SUM(shipping_fee)                as shipping_fee,
        SUM(ship_cost)                   as ship_cost,
        SUM(operational_margin)          as operational_margin,
        SUM(revenue) / COUNT(DISTINCT date) as average_basket,
        SUM(quantity)                    as quantity,
        SUM(ads_cost)                    as ads_cost,
        SUM(ads_impression)              as ads_impression,
        SUM(ads_clicks)                  as ads_clicks,
        SUM(ads_margin)                  as ads_margin

    from finance_campaigns_day

    group by datemonth

    order by datemonth desc

)

select * from monthly