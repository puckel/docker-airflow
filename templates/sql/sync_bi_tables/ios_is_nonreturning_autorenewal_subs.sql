 /*
If ios people haven't returned after free trial ended
    but had autorenewal on and aren't in billing retry
    then they did subscribe and we don't know about it yet.
    We will create extra row in table for these subscriptions.
    When we haved fix it on the backend, can remove nonreturned_subs and ios
*/
DROP TABLE IF EXISTS {{ params.schema }}.ios_is_nonreturning_autorenewal_subs_stage;
CREATE TABLE {{ params.schema }}.ios_is_nonreturning_autorenewal_subs_stage
distkey (acnt_id)
interleaved sortkey (purchased,
                product_id)
as
select *
from {{ params.schema }}.ios_subscriptions_w_acnts_stage
where acnt_id in (select acnt_id
                  from {{ params.schema }}.ios_subscriptions_w_acnts_stage
                  group by acnt_id
                  having count(*) = 1)
and is_trial_period = true
and expires < current_timestamp
and auto_renewing = true
and (is_in_billing_retry_period is null or is_in_billing_retry_period = false)
and product_id <> 'com.calm.trial.month';
