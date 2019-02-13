with ios as (--setup for next query; ios subs w/ acnt_id and orig trans id
        select  acnt_id, user_id, details_id, 
        JSON_EXTRACT_PATH_TEXT(receipt, 'original_transaction_id') as original_transaction_id,                
        case when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'true' then true
            when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'false' then false
            ELSE null
            END AS is_trial_period,
        i.*     
        from appdb.ios_subscriptions i
        join (select coalesce(user_id, device_id) as acnt_id, 
                    user_id, 
                    device_id, 
                    details_id
            from appdb.subscription_details
         ) sd 
        on sd.details_id = i.id
        where acnt_id != 'NULL' --test should count this val to make sure it doesn't grow (curently 20 of em)
),
n_ios_transactions_per_acnt as (--how many transactions is each acnt associated with
        select acnt_id, count(acnt_id) as n_ios_transactions_per_acnt
        from ios
        group by acnt_id
),
rank_acnt_by_number_of_transactions as (--get first values for removing duplicates
    --for each original transaction_id, rank acnts that belong to it by number of transactions;
        --most transactions ranked number 1
    select a.acnt_id, a.original_transaction_id,
           n_i.n_ios_transactions_per_acnt,
           --ranking function
           row_number() over (partition by original_transaction_id 
                            order by n_i.n_ios_transactions_per_acnt desc) as rank_acnt_w_most_transactions
    from (-- get distinct acnt_id, orig_trans combos
        select distinct acnt_id, original_transaction_id
        from ios
    ) a
    --and add number of transactions for that account to it
    left join n_ios_transactions_per_acnt n_i
    on n_i.acnt_id = a.acnt_id
)
    
select r.*, *
from ios i
left join rank_acnt_by_number_of_transactions r
on (i.acnt_id = r.acnt_id and i.original_transaction_id = r.original_transaction_id)
where i.original_transaction_id = '90000487165927'
--where user_id in ('EyZJVY1o4', 'V1GQbf3AV') --, 'EJC8GDwuE', 'Nyv6MLHpG'
--or details_id in ('140000406017556', '140000408431954')
order by i.acnt_id, purchased  
limit 200;          

--and t.rank_acnt_w_most_transactions = 1

