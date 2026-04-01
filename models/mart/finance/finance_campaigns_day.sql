with

finance_days as (

    select * from {{ ref('finance_days') }}

),

campaigns_day as (

    select * from {{ ref('int_campaigns_day') }}

),

joined as (

    select
        finance_days.date_date                                    as date,
        finance_days.revenue,
        finance_days.purchase_cost,
        finance_days.margin,
        finance_days.shipping_fee,
        finance_days.ship_cost,
        finance_days.operational_margin,
        finance_days.average_basket,
        finance_days.quantity,
        COALESCE(campaigns_day.ads_cost, 0) as ads_cost,
        campaigns_day.impression                                  as ads_impression,
        campaigns_day.click                                       as ads_clicks,
        finance_days.operational_margin - COALESCE(campaigns_day.ads_cost, 0) as ads_margin

    from finance_days

    left join campaigns_day
        on finance_days.date_date = campaigns_day.date_date

)

select * from joined
