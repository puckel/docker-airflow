with n_capitals as (
    select case when REGEXP_SUBSTR(cc, '[A-Z]') = ''
                then 0
                else 1
           end as n_capitals
    from {{ params.schema }}.device_acnts_stage
    where cc is not null
)
select case when sum(n_capitals) = 0
            then true
            else false
       end as test_result
from n_capitals;
