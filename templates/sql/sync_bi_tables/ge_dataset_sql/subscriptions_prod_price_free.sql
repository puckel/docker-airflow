select case when count(*) = 0
            then true
            else false
       end as test_result
from {{ params.schema }}.subscriptions_stage
where product_price > 0
and plan_is_free = true;
