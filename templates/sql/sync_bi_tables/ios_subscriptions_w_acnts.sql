/*
Join ios_subscriptions with subscription_details to get acnt_id (and user_id, device_id)

Duplicates Problem
    iOS, same details id applied to multiple acnt_ids (user_ids or device_id)
        problem detail
            -so when you join ios_subscriptions on subscription_details table
                you can double or more the number of subscriptions!!
            -this happens because apple is connecting multiple devices to a receipt
                -i.e., this is a problem because of apple and cant be fixed by us
            -there aren't multiple subs though
            -so need to decide which user_id/device_id to give the subscription to
        solution
            1) look at original_transaction_id of that transaction (first in the series if renewals)
            2) see which acnt_id is associated with the most transactions for that original_transaction_id
            3) then keep
             - acnt_id associated with most ios transactions
             - if a tie, possible to break it in the following order:
                -acnt that has user_id (vs device_id only)
                -pick one randomly
                (could do but havent implmement
                    -give to account with most recent transaction
                )
            -now only 1 one acnt id per transaction

Missing Subs
    about 5k ios_subs do not have a corresponding row in subscription details table
    so the count of this table should be about 5k less than the count of ios_subscriptions
    {add explanation - I forget why right now}
*/
DROP TABLE IF EXISTS {{ params.schema }}.ios_subscriptions_w_acnts_stage;
CREATE TABLE {{ params.schema }}.ios_subscriptions_w_acnts_stage
distkey (acnt_id)
interleaved sortkey (purchased,
                product_id)
as
with get_ios as (--setup for next query; ios subs w/ acnt_id and orig trans id
    select  coalesce(user_id, device_id) as acnt_id,
        user_id,
        case when user_id is not null then 1 else 0 end as has_user_id,
        i.id as details_id,
        JSON_EXTRACT_PATH_TEXT(receipt, 'original_transaction_id') as original_transaction_id,
        i.product_id,
        i.purchased, i.expires,
        case when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'true' then true
            when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'false' then false
            ELSE null
            END AS is_trial_period,
        i.auto_renewing,
        i.price_in_usd,
        i.is_in_billing_retry_period,
        i.created_at, i.updated_at,
        i.receipt,
        i.receipt_s3_path
    from appdb.ios_subscriptions i
    join (select --'NULL' strings should really be NULL
            CASE
                WHEN user_id = 'NULL' then NULL
                ELSE user_id
            END as user_id,
            CASE
                WHEN device_id = 'NULL' then NULL
                ELSE device_id
            END as device_id,
           details_id
        from appdb.subscription_details
     ) sd
    on sd.details_id = i.id
),
get_has_user_id  as (--does the account have a user id
    select acnt_id, original_transaction_id, max(has_user_id) as has_user_id
    from get_ios
    group by 1, 2
),
get_n_ios_transactions_per_acnt as (--how many transactions is each acnt associated with
    select acnt_id, count(acnt_id) as n_ios_transactions_per_acnt
    from get_ios
    group by acnt_id
),
get_rank_acnt as (--get first values for removing duplicates
    --for each original transaction_id, rank acnts that belong to it by number of transactions;
        --most transactions ranked number 1
    select h.acnt_id, h.original_transaction_id,
           n_i.n_ios_transactions_per_acnt,
           --ranking function
           row_number() over (partition by original_transaction_id
                                order by n_i.n_ios_transactions_per_acnt desc,
                                h.has_user_id desc) as rank_acnt
    from get_has_user_id h
    --and add number of transactions for that account to it
    left join get_n_ios_transactions_per_acnt n_i
    on n_i.acnt_id = h.acnt_id
)
select i.*
from get_ios i
left join get_rank_acnt r
on (i.acnt_id = r.acnt_id and i.original_transaction_id = r.original_transaction_id)
where r.rank_acnt = 1
