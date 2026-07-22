-- Trends review volume, rating, and verified-purchase mix by category
-- across the full historical span (1998-2023). year_month is parsed from
-- the staging layer's review_month DATE. category_lifetime stats give
-- each category's overall averages as a comparison baseline against its
-- own year-over-year trend, rather than comparing categories to each
-- other (categories differ too much in scale/nature for a fair head-to-head).

with reviews as (

    select * from {{ ref('stg_gold__review_summary') }}

),

with_year as (

    select
        *,
        extract(year from review_month) as review_year

    from reviews

),

category_lifetime as (

    select
        category,
        sum(review_count) as lifetime_review_count,
        avg(avg_rating) as lifetime_avg_rating,
        avg(verified_purchase_pct) as lifetime_avg_verified_pct

    from with_year
    group by category

),

yearly_by_category as (

    select
        category,
        review_year,
        sum(review_count) as review_count,
        avg(avg_rating) as avg_rating,
        avg(verified_purchase_pct) as avg_verified_purchase_pct

    from with_year
    group by category, review_year

),

joined as (

    select
        yearly_by_category.category,
        yearly_by_category.review_year,
        yearly_by_category.review_count,
        yearly_by_category.avg_rating,
        yearly_by_category.avg_verified_purchase_pct,
        category_lifetime.lifetime_review_count,
        category_lifetime.lifetime_avg_rating,
        category_lifetime.lifetime_avg_verified_pct,
        yearly_by_category.avg_rating - category_lifetime.lifetime_avg_rating as rating_delta_vs_lifetime_avg

    from yearly_by_category
    left join category_lifetime on yearly_by_category.category = category_lifetime.category

)

select * from joined
order by category, review_year
