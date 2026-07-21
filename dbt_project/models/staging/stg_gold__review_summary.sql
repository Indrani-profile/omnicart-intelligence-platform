-- review_month is a VARCHAR ('YYYY-MM' string), not a native DATE — see
-- sources.yml note. Parsed to a real DATE here for downstream convenience,
-- since staging is the right layer for this kind of light type-shaping.
with source as (

    select * from {{ source('gold', 'ext_review_summary') }}

),

renamed as (

    select
        category,
        review_month as review_month_raw,
        to_date(review_month || '-01') as review_month,
        review_count,
        avg_rating,
        verified_purchase_pct

    from source

)

select * from renamed
