with 

source as (

    select * from {{ source('raw', 'product') }}

),

renamed as (

    select
        products_id,
          - name: product
        identifier: raw_gz_product
        description: "Products sold by Greenweez"
        columns:
          - name: products_id
            description: "Unique product identifier"
            tests:
              - unique
              - not_null
        CAST(purchase_price AS FLOAT64) AS 

    from source

)

select * from renamed