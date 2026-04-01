with

int_campaigns as (

    select * from {{ ref('int_campaigns') }}

),

aggregated as (

    select
        date_date,
        SUM(ads_cost)    as ads_cost,
        SUM(impression)  as impression,
        SUM(click)       as click

    from int_campaigns

    group by date_date

    order by date_date desc

)

select * from aggregated
