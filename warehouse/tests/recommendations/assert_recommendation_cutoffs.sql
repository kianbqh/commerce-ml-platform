with user_bounds as (
    select user_id,max(order_number) filter(where split='train') train_last,
           min(order_number) filter(where split='validation') validation_order,
           min(order_number) filter(where split='test') test_order,
           count(*) filter(where split='validation') validation_count,
           count(*) filter(where split='test') test_count
    from {{ ref('rec_order_split') }} group by user_id
)
select cast(user_id as varchar) as invalid_key from user_bounds
where train_last >= validation_order or validation_order >= test_order
   or validation_count != 1 or test_count != 1
union all
select cast(h.user_id as varchar) from {{ ref('rec_history_product') }} h
join {{ ref('rec_history_user') }} u using(stage,user_id)
where h.last_order_number > u.cutoff_order_number or h.purchase_count <= 0
