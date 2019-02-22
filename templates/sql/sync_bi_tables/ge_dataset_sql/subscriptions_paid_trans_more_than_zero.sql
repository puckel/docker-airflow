select case when count(*) = 0
            then true
            else false
       end as test_result
from {{ params.schema }}.subscriptions_stage
where n_transactions_paid = 0
and total_paid > 0;
