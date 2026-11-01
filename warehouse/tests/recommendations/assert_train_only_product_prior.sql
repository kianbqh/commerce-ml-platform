with actual as (
    select product_id,count(*) n from {{ ref('rec_lines') }} where split='train' group by product_id
)
select p.product_id from {{ ref('rec_product_prior') }} p left join actual a using(product_id)
where p.purchase_count != coalesce(a.n,0)
