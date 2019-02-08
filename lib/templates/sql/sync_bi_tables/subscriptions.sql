/*
Two current problems in DB (besides schema being very differnet across subs tables haha)


1) for iOS, same details id applied to multiple acnt_ids (user_ids or device_id)
    problem detail
        -so when you join ios_subscriptions on subscription_details table
            you can double or more the number of subscriptions!!
        -this happens because apple is connecting multiple devices to a receipt
        -there aren't multiple subs though
        -so need to decide which user_id/device_id to give the subscription to
    solution
        1) look at original_transaction_id of that transaction (first in the series if renewals)
        2) see which acnt_id is associated with the most transactions for that original_transaction_id
        3( then keep
         - acnt_id associated with most ios transactions
         - if a tie, possible to break it in the following order
            -pick one randomly
            (could do but havent implmement
                -keep acnt_id associated with most for each original_transaction_id)??
                -then pick acnt_id that uses the user_id instead of device_id
            )

        )
        -now only 1 one acnt id per transaction

2) is_nonreturning_auto_renewal_on users
     ios and nonreturned subs is a temporary fix to address ios-receipts issue.
    if someone purchases yearly after free trial, should have a new row in the DB.
    But some only have one row even though:
      1. their free trial has expired
      2. they had autorenewal on
      3. they are not in a billing retry period.
    We think these people have purchased, but haven't returned to the app
      (which would trigger a new line in our DB)

3) need cc (country code) in order to get prices
4) try to incorporate itunes and google play revenue information
*/
DROP TABLE IF EXISTS {{ params.schema }}.subscriptions_stage;
CREATE TABLE {{ params.schema }}.subscriptions_stage
distkey (acnt_id)
interleaved sortkey (purchased,
                 platform,
                 is_trial_period,
                 plan_is_free)
AS
with subs_all AS (--meat of the query; union all the subscriptions and combine schemas
  --Get data from ios_subscrptions, stripe_subscrptions, android_subscrptions,
      --gift_subscrptions, and nonreturned_subs (see temp table above)
  SELECT coalesce(s.user_id, s.device_id) as acnt_id,
          s.user_id,
          s.device_id,
          subscription_id,
          details_id,
          transaction_base_id,
          CASE
              WHEN user_id = 'NULL' or user_id is null THEN false
              WHEN user_id <> 'NULL' THEN true
              ELSE NULL
          END AS acnt_has_user_id,
          CASE
              WHEN s_table = 'gift' and gift_type = 'stripe' then 'gift_stripe'
              WHEN s_table = 'gift' then gift_type
              ELSE plan
          END AS plan,
          CASE
              WHEN s_table = 'gift' and gift_type = 'stripe' then 'gift_stripe'
              WHEN s_table = 'gift' then gift_type
              WHEN plan ILIKE '%trial%' AND is_trial_period = true THEN 'free_trial'
              WHEN plan = 'com.calm.trial.month' AND is_trial_period = false THEN 'monthly'
              WHEN plan ILIKE 'com.calm.yearly.trial.one_%' AND is_trial_period = false THEN 'yearly'
              WHEN plan ILIKE '%content%' THEN 'additional_content'
              WHEN plan ILIKE '%3_month%' THEN '3_months'
              WHEN plan ILIKE '%6_month%' THEN '6_months'
              WHEN plan ILIKE '%year%' THEN 'yearly'
              WHEN plan ILIKE '%12_month%' THEN 'yearly'
              WHEN plan ILIKE '%lifetime%' THEN 'lifetime'
              WHEN plan ILIKE '%month%' THEN 'monthly'
              ELSE NULL
          END AS plan_type,
          CASE
              WHEN s_table = 'gift' and gift_type = 'stripe'  then 'year'
              WHEN s_table = 'gift' and gift_type = 'support'  then 'year'
              WHEN s_table = 'gift' and gift_type = 'calm-box'  then 'year'
              WHEN s_table = 'gift' and gift_type = 'influencer'  then 'year'
              WHEN s_table = 'gift' and gift_type = 'college'  then 'year'
              WHEN s_table = 'gift' and gift_type = 'teacher'  then 'lifetime'
              WHEN s_table = 'gift' and gift_type = 'partner'  then 'custom'
              WHEN s_table = 'gift'  then 'year'
              WHEN plan ILIKE 'com.calm.trial.month%' THEN 'month'
              WHEN plan ILIKE 'com.calm.yearly.trial.one_month%' AND is_trial_period = true THEN 'month'
              WHEN plan ILIKE 'com.calm.yearly.trial.one_week%' AND is_trial_period = true THEN 'week'
              WHEN plan ILIKE '%trial.one_week' AND is_trial_period = true THEN 'week'
              WHEN plan = 'com.calm.trial.month' AND is_trial_period = false THEN 'month'
              WHEN plan ILIKE 'com.calm.yearly.trial.one_%' AND is_trial_period = false THEN 'year'
              WHEN plan ILIKE '%content%' THEN 'lifetime'
              WHEN plan ILIKE '%3_month%' THEN '3_months'
              WHEN plan ILIKE '%6_month%' THEN '6_months'
              WHEN plan ILIKE '%year%' THEN 'year'
              WHEN plan ILIKE '%12_month%' THEN 'year'
              WHEN plan ILIKE '%lifetime%' THEN 'lifetime'
              WHEN plan ILIKE '%month%' THEN 'month'
              ELSE NULL
          END AS plan_duration,
          CASE
              WHEN product_price = 0 then true
              WHEN product_price > 0 then false
              WHEN s_table = 'gift' and gift_type = 'teacher' then true
              WHEN s_table = 'gift' and product_price is null then NULL
              WHEN is_trial_period = true THEN true
              ELSE false  --assume plan costs money if missing information
          END AS plan_is_free,
          CASE
              WHEN s_table = 'gift' and gift_type = 'partner' and campaign_name = 'groupon' THEN false
              WHEN s_table = 'gift' THEN true
              WHEN is_trial_period = true THEN true
              ELSE false
          END AS plan_is_free_for_user,
          s_table AS platform,
          purchased,
          expires,
          expires_date,
          auto_renewing,
          auto_renewal_canceled_at,
          coupon,
          is_trial_period,
          start_w_trial_period,
          CASE
            WHEN is_in_billing_retry_period is NULL then false
            ELSE is_in_billing_retry_period
            END as is_in_billing_retry_period,
          is_nonreturning_auto_renewal,
          --single transaction
          product_price,
          product_price_after_taxes,
          first_fee,
          product_proceeds_first_transaction,
          --total transactions
          n_transactions_raw,
          --refund info
          refund_requested,
          n_transactions_refunded,
          total_refunded,
          refund_requested_at,
          datediff(days, purchased, refund_requested_at) as time_til_refund_request,
          refund_reason,
          --total final $$$
          n_transactions_paid,
          total_paid,
          total_proceeds,
          --extra info
          gift_type,
          campaign_name,
          created_at,
          updated_at
   FROM (--user id is sometimes a string 'NULL' instead of a null value
            --that will mess up coalesce(user_id, device_id) as acnt_id
            --so fix here
        SELECT
            CASE
              WHEN user_id = 'NULL' then NULL
              WHEN user_id is NULL then NULL
              ELSE user_id
            END as user_id,
            CASE
              WHEN device_id = 'NULL' then NULL
              WHEN device_id is NULL then NULL
              ELSE device_id
            END as device_id,
            details_id
        FROM appdb.subscription_details
        ) s
   JOIN (
        (--iOS users
            SELECT id::text AS subscription_id,
                id::text AS purchase_id,
                --iOS has multiple transactions/renewal all attached to first transaction id;
                    --use first transaction id as the base id (the parent id)
                JSON_EXTRACT_PATH_TEXT(receipt, 'original_transaction_id') as transaction_base_id,
                product_id AS plan,
                NULL AS gift_type,
                purchased,
                expires,
                CASE
                   WHEN expires > '2100-01-01' THEN '2100-01-01'
                   ELSE date_trunc('day', expires)
                END AS expires_date,
                auto_renewing,
                NULL AS auto_renewal_canceled_at,
                NULL AS coupon,
                'ios' AS s_table,
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'true' then true
                  when JSON_EXTRACT_PATH_TEXT(receipt, 'is_trial_period') = 'false' then false
                  ELSE null
                  END as is_trial_period,
                CASE WHEN plan ILIKE 'com.calm.yearly.trial%' then true
                    when plan ILIKE  'com.calm.trial.month%' then true
                    else false
                    end as start_w_trial_period,
                is_in_billing_retry_period,
                --refund info
                --single transaction
                NULL as product_price,
                NULL as product_price_after_taxes,
                NULL as first_fee,
                NULL as product_proceeds_first_transaction,
                --total transactions
                1 as n_transactions_raw,
                --refund info
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date') <> '' then true
                    ELSE FALSE
                    END as refund_requested,
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date') <> '' then 1
                    else 0
                    end as n_transactions_refunded,
                NULL as total_refunded,
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date') <> ''
                        then JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date')::timestamp without time zone
                    else null
                    end as refund_requested_at,
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_reason') = '0' then 'accidental_purchase'
                    when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_reason') = '1' then 'problem_with_app'
                    when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_reason') = ''
                        and JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date') <> '' then NULL
                    else null
                    end as refund_reason,
                --total final $$$
                case when JSON_EXTRACT_PATH_TEXT(receipt, 'cancellation_date') <> '' then 0
                    else 1
                    END as n_transactions_paid,
                NULL as total_paid, --dont know it
                NULL as total_proceeds,
                --extra information
                NULL as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM appdb.ios_subscriptions
            where plan != 'com.wb.LEGOStarWars.ARCADE' -- a weird sub that occurs once
        )
        UNION ALL
        (--IOS FREE TRIAL:  iOS users who converted from free-trial but who haven't returned to use the app yet
            SELECT SUBSTRING(details_id::text, 2, 20) AS subscription_id,
                details_id::text AS purchase_id,
                original_transaction_id as transaction_base_id,
                product_id AS plan,
                NULL AS gift_type,
                expires as purchased,
                dateadd(year, 1, purchased) + '7 days'::interval as expires,
                CASE
                    WHEN expires > '2100-01-01' THEN '2100-01-01'
                    ELSE date_trunc('day', expires)
                    END AS expires_date,
                auto_renewing,
                NULL AS auto_renewal_canceled_at,
                NULL AS coupon,
                'ios' AS s_table,
                false as is_trial_period,
                true as start_w_trial_period,
                is_in_billing_retry_period,
                --single transaction
                NULL as product_price,
                NULL as product_price_after_taxes,
                NULL as first_fee,
                NULL as product_proceeds_first_transaction,
                --total transactions
                1 as n_transactions_raw,
                --refund info
                false as refund_requested,
                0 as n_transactions_refunded,
                NULL as total_refunded,
                NULL as refund_requested_at,
                NULL as refund_reason,
                --total final $$$
                1 as n_transactions_paid,
                NULL as total_paid, --$$ spent are not counted if prod was refunded
                NULL as total_proceeds,

                NULL as campaign_name,
                created_at,
                updated_at,
                true as is_nonreturning_auto_renewal
            FROM {{ params.schema }}.ios_is_nonreturning_autorenewal_subs_stage
        )
        UNION ALL
        (--android subs - NOT free trial products
            SELECT a.id::text AS subscription_id,
                a.id::text AS purchase_id,
                --transactions have '..0', '..1' suffix
                    --but you don't see multiple transactions in android_subs.
                    --use id without suffix at the end as the base id (the parent id)
                a.id as transaction_base_id,
                product_id AS plan,
                NULL AS gift_type,
                purchased,
                expires,
                CASE
                    WHEN expires > '2100-01-01' THEN '2100-01-01'
                    ELSE expires
                    END AS expires_date,
                auto_renewing,
                canceled_at AS auto_renewal_canceled_at,
                NULL AS coupon,
                'android' AS s_table,
                false as is_trial_period,
                case when product_id like '%trial%' then true
                    else false
                    end as start_w_trial_period,
                NULL as is_in_billing_retry_period,
                --we only get refund requests once a month so may be wrong for recent subs
                finance.product_price,
                finance.product_price_after_taxes,
                finance.first_fee,
                finance.product_proceeds_first_transaction,
                --total transactions
                finance.n_transactions,--if first subs was recent may be null
                --refund info
                case when finance.n_transactions_refunded > 0 then true
                  else false
                  end as refund_requested,
                finance.n_transactions_refunded,
                finance.total_refunded,
                finance.first_refund_at::timestamp as refund_requested_at,
                case when finance.details_id is not null then 'unknown'
                  else NULL
                  end as refund_reason,
                --total final $$$
                finance.n_transactions_paid,
                finance.total_paid, --$$ spent are not counted if prod was refunded
                finance.total_proceeds,
                NULL as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM appdb.android_subscriptions a
            -- get refund information from google_play_earning
            left join (
                  select details_id,
                        --single transaction
                        product_price,
                        product_price_after_taxes,
                        google_fee_first_transaction as first_fee,
                        product_proceeds_first_transaction,
                        --total transactions
                        n_transactions,
                        --refund info
                        n_transactions_refunded,
                        total_refunded,
                        first_refund_at,
                        --total final $$$
                        n_transactions_paid,
                        total_paid, --$$ spent are not counted if prod was refunded
                        total_proceeds
                  from financial.google_play_details_id_summary
                )  finance
            on finance.details_id = a.id
            --if over 8 days have purchased but cuz of maybe billing retry using longer period to be certain
            where (product_id not like '%trial%')
        )
        UNION ALL
        (--android subs - free-trial product - paid transactions
            SELECT a.id::text AS subscription_id,
                a.id::text AS purchase_id,
                a.id as transaction_base_id,
                product_id AS plan,
                NULL AS gift_type,
                purchased + '7 days'::interval,  --must be changed to show first day
                expires,
                CASE
                    WHEN expires > '2100-01-01' THEN '2100-01-01'
                    ELSE expires
                    END AS expires_date,
                auto_renewing,
                canceled_at AS auto_renewal_canceled_at,
                NULL AS coupon,
                'android' AS s_table,
                false as is_trial_period,
                case when product_id like '%trial%' then true
                    else false
                    end as start_w_trial_period,
                NULL as is_in_billing_retry_period,
                --we only get refund requests once a month so may be wrong for recent subs
                finance.product_price,
                finance.product_price_after_taxes,
                finance.first_fee,
                finance.product_proceeds_first_transaction,
                --total transactions
                finance.n_transactions,--if first subs was recent may be null
                --refund info
                case when finance.n_transactions_refunded > 0 then true
                  else false
                  end as refund_requested,
                finance.n_transactions_refunded,
                finance.total_refunded,
                finance.first_refund_at::timestamp as refund_requested_at,
                case when finance.details_id is not null then 'unknown'
                  else NULL
                  end as refund_reason,
                --total final $$$
                finance.n_transactions_paid,
                finance.total_paid, --$$ spent are not counted if prod was refunded
                finance.total_proceeds,
                NULL as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM appdb.android_subscriptions a
            -- get refund information from google_play_earning
            left join (
                  select details_id,
                        --single transaction
                        product_price,
                        product_price_after_taxes,
                        google_fee_first_transaction as first_fee,
                        product_proceeds_first_transaction,
                        --total transactions
                        n_transactions,
                        --refund info
                        n_transactions_refunded,
                        total_refunded,
                        first_refund_at,
                        --total final $$$
                        n_transactions_paid,
                        total_paid, --$$ spent are not counted if prod was refunded
                        total_proceeds
                  from financial.google_play_details_id_summary
                )  finance
            on finance.details_id = a.id
            --if over 8 days have purchased but cuz of maybe billing retry using longer period to be certain
            where (product_id like '%trial%' and expires >= purchased + '16 days'::interval)
        )
        UNION ALL
        (--android subs - trial period for free-trial products
          --there isn't an additional row in android for the trial period
            ---so going to create it if expires > 14 days after purchase
            SELECT SUBSTRING(a.id::text, 2, 20) AS subscription_id,
                a.id::text AS purchase_id,
                a.id as transaction_base_id,
                product_id AS plan,
                NULL AS gift_type,
                purchased,
                purchased + '7 days'::interval as expires,
                CASE
                    WHEN expires > '2100-01-01' THEN '2100-01-01'
                    ELSE expires
                    END AS expires_date,
                auto_renewing,
                canceled_at AS auto_renewal_canceled_at,
                NULL AS coupon,
                'android' AS s_table,
                true as is_trial_period,
                true as start_w_trial_period,
                NULL as is_in_billing_retry_period,

                --single transaction
                0 as product_price,
                0 as product_price_after_taxes,
                0 as first_fee,
                0 as product_proceeds_first_transaction,
                --total transactions
                1 as n_transactions_raw,
                --refund info
                false as refund_requested,
                0 as n_transactions_refunded,
                0 as total_refunded,
                NULL as refund_requested_at,
                NULL as refund_reason,
                --total final $$$
                0 as n_transactions_paid,
                0 as total_paid, --$$ spent are not counted if prod was refunded
                0 as total_proceeds,

                NULL as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM appdb.android_subscriptions a
            -- free trials do not have refund info because they are free
            where product_id like '%trial%'
        )
        UNION ALL
        (--stripe subscriptions excluding free trial; free trial is put into different row - see next union
            SELECT id::text AS subscription_id,
                  id::text AS purchase_id,
                  id as transaction_base_id,
                  plan,
                  NULL AS gift_type,
                  case when trial_start is not null then trial_end
                      else created_at
                      end AS purchased,
                  coalesce(current_period_end, expires) as expires,
                  CASE
                      WHEN coalesce(current_period_end, expires) > '2100-01-01' THEN '2100-01-01'
                      ELSE coalesce(current_period_end, expires)
                      END AS expires_date,
                  CASE
                      WHEN status = 'active' THEN true
                      WHEN status = 'canceled' THEN false
                      ELSE NULL
                      END AS auto_renewing,
                  canceled_at AS auto_renewal_canceled_at,
                  coupon,
                  'stripe' AS s_table,
                  false as is_trial_period,
                  case when trial_start is not null then true
                      else false
                      end as start_w_trial_period,
                  CASE WHEN status = 'past_due' THEN true
                      else NULL
                      END as is_in_billing_retry_period,

                  --single transaction
                  amount::real/100 as product_price,
                  amount::real/100 as product_price_after_taxes,
                  amount::real/100 * .0275 + 0.30 as first_fee, --as of 2017, fee is 2.75% and flat 30 cents
                  case when amount::real/100 = 0 then 0
                       when amount::real/100 - (1-.0275) - 0.30 < 0 then 0
                       else amount::real/100 - (1-.0275) - 0.30
                       end as product_proceeds_first_transaction,
                  --total transactions
                  NULL as n_transactions_raw,
                  --refund info
                  case when refunded_at is not null then true
                      when refunded_at is null then false
                      END as refund_requested,
                  case when refunded_at is not null then 1
                      when refunded_at is null then 0
                      END as n_transactions_refunded,
                  case when refunded_at is not null then amount::real/100
                      when refunded_at is null then 0
                      END as total_refunded,
                  refunded_at as refund_requested_at,
                  case when refunded_at is not null then 'unknown'
                      else null
                      end as refund_reason,
                  --total final $$$
                  NULL as n_transactions_paid, --will calculate further down
                  NULL as total_paid, --"amount" is price of product but, at this point, dont know # of transactions/renewals
                  NULL as total_proceeds,
                  NULL as campaign_name,
                  created_at,
                  updated_at,
                  false as is_nonreturning_auto_renewal
            FROM appdb.stripe_subscriptions
            where (current_period_end > trial_end or trial_start is null)
        )
        UNION ALL
        (--STRIPE FREE TRIAL;
          --this code adds new row to db to reflect this free-trial
            SELECT --give it a unique id
                SUBSTRING(id::text, 2, 20) AS subscription_id, --give a unique details_id
                id::text AS purchase_id,  --same id used when person converts to paid
                id as transaction_base_id,
                plan,
                NULL AS gift_type,
                trial_start AS purchased,
                trial_end as expires,
                date_trunc('day', trial_end) as expires_date,
                CASE
                    WHEN status = 'trialing' THEN true
                    WHEN status = 'canceled' THEN false
                    ELSE NULL
                    END AS auto_renewing,
                canceled_at AS auto_renewal_canceled_at,
                coupon,
                'stripe' AS s_table,
                true as is_trial_period,
                true as start_w_trial_period,
                false as is_in_billing_retry_period,
                --single transaction
                0 as product_price,
                0 as product_price_after_taxes,
                0 as first_fee,
                0 as product_proceeds_first_transaction,
                --total transactions
                1 as n_transactions_raw,
                --refund info
                false as refund_requested,
                0 as n_transactions_refunded,
                0 as total_refunded,
                NULL as refund_requested_at,
                NULL as refund_reason,
                --total final $$$
                0 as n_transactions_paid,
                0 as total_paid, --$$ spent are not counted if prod was refunded
                0 as total_proceeds,
                NULL as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM appdb.stripe_subscriptions
            where trial_end <= current_period_end
        )
        UNION ALL
        (--GIFTS
            SELECT id::text AS subscription_id,
                id::text AS purchase_id,
                id as transaction_base_id,
                NULL as plan,
                type AS gift_type,
                purchased,
                expires,
                CASE
                    WHEN expires > '2100-01-01' THEN '2100-01-01'
                    ELSE expires
                    END AS expires_date,
                CASE
                    WHEN expires > '2100-01-01' THEN true
                    WHEN expires < '2100-01-01' THEN false
                    ELSE NULL
                    END AS auto_renewing,
                NULL AS auto_renewal_canceled_at,
                NULL AS coupon,
                'gift' AS s_table,
                false as is_trial_period,
                false as start_w_trial_period,
                NULL as is_in_billing_retry_period,
                --single transaction
                product_price,
                product_price as product_price_after_taxes,
                product_price * .0275 + 0.30 as first_fee,
                case when amount::real/100 = 0 then 0
                       when amount::real/100 - (1-.0275) - 0.30 < 0 then 0
                       else amount::real/100 - (1-.0275) - 0.30
                       end as product_proceeds_first_transaction,
                --total transactions
                1 as n_transactions_raw,
                --refund info
                false as refund_requested,
                0 as n_transactions_refunded,
                0 as total_refunded,
                NULL as refund_requested_at,
                NULL as refund_reason,
                --total final $$$
                0 as n_transactions_paid,
                0 as total_paid, --$$ spent are not counted if prod was refunded
                0 as total_proceeds,
                purchased_by as campaign_name,
                created_at,
                updated_at,
                false as is_nonreturning_auto_renewal
            FROM (--update missing price information in gifts for select purchased_by groups where amount is null
                select *,
                      case when amount is not null then amount::real/100
                            when purchased_by = 'groupon' then 40.00
                            when purchased_by like '%PwC%' then 5.00
                            else null
                            END as product_price
                from appdb.gifts
                --generate rows in gifts table for partners to use but usually wont get paid until redeemed_by someone
                where redeemed_by is not null
              )
        )
    ) details
    ON details.purchase_id = s.details_id
),
subs_nonduplicates as (
    --some duplicated transaction ids (<5k at time of writing);
      --problem with itunes re-assignomg subscriptions using itunes id
    --choose 1 acnt to give it to; give it to an acnt associated w/ a user id over one that is details_id only
    select * from (
         SELECT *,
         ROW_NUMBER() OVER (PARTITION BY subscription_id
                            ORDER BY acnt_has_user_id DESC) AS rk
        FROM subs_all)
    WHERE rk = 1
    ),
devices_recent as (
    /* Get estimated country code (CC) from most recent device to estimate country of user
      Price of some products (eg %us_60_not_usa_48) are determined by country
      Use store_country_code if it exists (was added ~2018/04 for iOS and is very accurate.).
      It is a much more reliable predictor of where a person bought product
        (asks apple for cc of the person apple store)
        vs. cc which just uses most recent ip of person (changes based on location, vpn, etd)
      */
    select acnt_id,
            cc,
            store_country_code
    FROM {{ params.schema }}.device_acnts_stage
    WHERE order_appeared_reversed = 1
),
subs_plus_country_info as (
  /*Add country information
  plus info about total days of plan and refund request info  */
    SELECT s.*,
            date_trunc('month', purchased) as purchased_month,
            case when d_rec.cc = 'us' then 'us'
                when d_rec.cc <> 'us' then 'not_usa'
                else 'missing'
                end as in_usa,
            case when s.plan_duration = 'year' then 366
                when s.plan_duration = 'month' then 31
                when s.plan_duration = 'lifetime' then 10000
                when s.plan_duration = '3_months' then 96
                when s.plan_duration = '6_months' then 187
                when s.plan_duration = 'week' then 8
                else null
                end as estimated_plan_duration_days,

            case when refund_requested = false then 0
                when refund_requested = true and s.platform <> 'ios' then 1
                when is_nonreturning_auto_renewal = true then .025
                when refund_requested = true and time_til_refund_request <= 14 then 1
                when refund_requested = true and time_til_refund_request <= 42 and plan_type = 'yearly' then .5
                when refund_requested = true and time_til_refund_request >42 and plan_type = 'yearly' then .05
                when refund_requested = true and time_til_refund_request <= 21 and plan_type = 'monthly' then .5
                when refund_requested = true and time_til_refund_request >21 and plan_type = 'yearly' then .05
                when refund_requested = true  then 1
                else null
                end as probability_refunded,
          d_rec.cc,
          d_rec.store_country_code
    FROM subs_nonduplicates s
    left join devices_recent d_rec
    on d_rec.acnt_id = s.acnt_id
),
subs_prod_price as (--get prod price and and proceeds by country from lookup tables
   select s_cc.*,
            cc_by_month.transaction_month,
            case when s_cc.product_price is not null then s_cc.product_price
                when plan_is_free = true then 0.0
                when ios_cc.product_price is not null then ios_cc.product_price
                when android_cc.product_price is not null then android_cc.product_price
                when default_price.price is not null then default_price.price
                else null
                END as product_price_estimated,
            case when s_cc.product_price is not null then 'transaction_found' --don't need to know 'cc' to estimate price, should be for android or stripe only
                 when cc_by_month.product_proceeds is not null then 'by_prod_cc_and_month'
                 when ios_cc.product_proceeds is not null then 'by_prod_and_cc'
                 when android_cc.product_proceeds is not null then 'by_prod_and_cc'
                 when default_price.price is not null then 'by_prod_usd'
                 else null
                 end as how_product_price_estimated,
            cc_by_month.product_proceeds as cc_month_proceeds,
            coalesce(ios_cc.product_proceeds, android_cc.product_proceeds) as cc_proceeds
    from subs_plus_country_info s_cc
        --join by month and country
        left join (
            select
                'ios' as platform,
                lower(buyer_country) as cc,
                *
            from financial.itunes_prod_price_by_cc_by_month
            ) cc_by_month
        on (cc_by_month.platform = s_cc.platform
        and cc_by_month.plan = s_cc.plan
        and cc_by_month.cc = s_cc.cc
        and cc_by_month.transaction_month = s_cc.purchased_month)
        --for ios, join by country if month/country/combo isn't found
        left join (
            select
                'ios' as platform,
                lower(buyer_country) as cc,
                *
            from financial.itunes_prod_recent_price_by_cc
            ) ios_cc
        on ios_cc.platform = s_cc.platform
        and ios_cc.plan = s_cc.plan
        and ios_cc.cc = s_cc.cc
        --for android, if missing transactional level data, use most recent prod price per country
        left join (
            select
                'android' as platform,
                lower(buyer_country) as cc,
                *
            from financial.google_play_recent_price_by_cc
            ) android_cc
        on android_cc.platform = s_cc.platform
        and android_cc.plan = s_cc.plan
        and android_cc.cc = s_cc.cc

        --join by USD estimate if neither is found
        left join (
            select *
            from financial.plan_price_estimates
            where in_usa = 'us' -- default the price to usa values
            )  default_price
        on default_price.platform = s_cc.platform
        and default_price.plan = s_cc.plan
),
subs_estimated_tax_n_fees as (--estimate price and number transactions per line
    select s_t.*,
          coalesce(tax_n_fee_info.tax_rate, 0.0) as tax_rate_estimated,
          --can use this information to calculate proceeds after tax and fee
              --assumes 30% fee, so not right for stripe or forr renewals > 1 year of subs time & > 2018-01
          (1 - tax_n_fee_info.year1_portion_of_price) as first_fee_percent_estimated,
          --reduced for subs > 1 year long after 2018/01
          (1 - tax_n_fee_info.year2_portion_of_price) as fee_percent_if_reduced_estimated
    from subs_prod_price s_t
    --tax rate and proceeds for android and ios
    left join financial.country_proceed_and_tax_rates tax_n_fee_info
    on tax_n_fee_info.cc = s_t.cc
    and tax_n_fee_info.platform = s_t.platform
    --can add google play info here when updated
),
subs_estimated_n_transactions as (
    select *,
        --includes/counts transactions that were refunded
        --do not coalesce w/ "n_transaction" values
        case when estimated_plan_duration_days is not null
                then ceiling(datediff(days, purchased, case when expires > getdate()
                                                            then getdate()
                                                            else expires
                                                            end)::real / estimated_plan_duration_days)
          else 1
          END AS n_transactions_estimated
    from subs_estimated_tax_n_fees
),
subs_price_breakdown as (
    select *,
            case when product_price_estimated = 0 then 0
                when product_price_after_taxes is not null then product_price_after_taxes
                else product_price_estimated * (1 - tax_rate_estimated)
                end as product_price_after_taxes_estimated,
            case when product_price_estimated = 0 then 0
                when first_fee is not null then first_fee
                when (product_price_estimated is not null and tax_rate_estimated is not null and first_fee_percent_estimated is not null)
                    then product_price_estimated * (1 - tax_rate_estimated) * first_fee_percent_estimated
                when product_price_estimated is not null
                    then product_price_estimated * .3
                else null
                end as first_fee_estimated
    from subs_estimated_n_transactions
),
subs_proceeds as (
    select *,
            case when product_proceeds_first_transaction is not null then product_proceeds_first_transaction
                when plan_is_free = true then 0.0
                when product_price_estimated = 0 then 0.0
                when cc_month_proceeds is not null then cc_month_proceeds
                when cc_proceeds is not null then cc_proceeds
                when (product_price_estimated is not null and product_price_after_taxes_estimated is not null and first_fee_estimated is not null)
                    then product_price_after_taxes_estimated - first_fee_estimated
                when product_price_estimated is not null
                    then product_price_estimated * .7
                else null
                END as product_proceeds_first_transaction_estimated,
            product_proceeds_first_transaction as product_proceeds_first_transaction_orig
    from subs_price_breakdown

),
subs_n_transactions_corrected as (
    select *,
            --fix n_transactions
                    --part of problem is from google play cuz data is only updated monthly
                    --if new transaction happened recently then it would have the wrong count
                    --so if estimated n_transactions is higher than in google_play, it is probably correct
            case when n_transactions_estimated = n_transactions_raw
                    then n_transactions_raw
                when n_transactions_estimated > n_transactions_raw --could happen for android if recent transactions;
                    then n_transactions_estimated
                when n_transactions_raw > n_transactions_estimated --n_transactions_raw from DB is probably correct in this situation
                    then n_transactions_raw
                when n_transactions_raw is not null and n_transactions_estimated is null
                    then n_transactions_raw
                when n_transactions_estimated <= 0 then 1 --all rows should have at least one transaction
                else n_transactions_estimated
                end as n_transactions_corrected
    from subs_proceeds
),
subs_with_n_transactions_paid AS (
    select *,
          --if missing transaction number assume at least one happened
          --remove a transaction if a person requested a refund
          case when product_price_estimated = 0 then 0
                when n_transactions_corrected = 0 then 0
                --android can have more than one refunded
                when n_transactions_refunded > 0 then n_transactions_corrected - n_transactions_refunded
                --or just assume only one was refunded
                when probability_refunded > .1 then n_transactions_corrected - 1
                else n_transactions_corrected
                end as n_transactions_paid_estimated
    from subs_n_transactions_corrected
),
subs_with_total_revenue AS (--calculate total proceeds and total paid
    --some adjustments need to be made for android
        --want to use transaction information where available
            --but if don't have info on recent transaction still wnat to adjust total_proceeds
    select *,
        --total paid
            --n_transactions should always be 1 per row for ios
        case when n_transactions_paid_estimated = 1
                then product_price_estimated
            when n_transactions_paid_estimated = 0
                then 0
            when platform = 'android' and n_transactions_paid_estimated = n_transactions_corrected and total_paid is not null
                then total_paid
            --fix total proceeds if missing transactions in google play data
            when platform = 'android' and n_transactions_paid_estimated > n_transactions_raw
                then total_paid + (n_transactions_paid_estimated - n_transactions_raw) * product_price_estimated
            --covers stripe/gifts and android when n_transaction is null
            else n_transactions_corrected * product_price_estimated
            end as total_paid_estimated,
        ---total proceeds
        case when n_transactions_paid_estimated = 1
                then product_proceeds_first_transaction_estimated
            when n_transactions_paid_estimated = 0
                then 0
            when platform = 'android' and n_transactions_paid_estimated = n_transactions_corrected and total_proceeds is not null
                then total_proceeds
           when platform = 'android' and n_transactions_paid_estimated > n_transactions_raw
                --it is unclear if recent transactions get 15% or 30% renewal fee
                    --most are renewals so some, if been around for > year, get 15% fee
                    --guess halfway - 22.5%; ie, google fee at 30% * .75
                then total_proceeds + (n_transactions_paid_estimated - n_transactions_raw) * (product_price_after_taxes - (first_fee * .75) )
            --best guess if 1) stripe or gifts cuz flat fee across renewals
                --and 2) if n_transactions_raw is null for android cuz prob just 1 transaction in that case)
            else n_transactions_corrected * product_proceeds_first_transaction_estimated
            end as total_proceeds_estimated
    from subs_with_n_transactions_paid
),
first_purchase as (
    select acnt_id,
        min(purchased) as first_purchase
    from subs_all
    group by acnt_id
)
--final step: adjust, if refund, total number of transaction and estimated total price paid
select subscription_id,--basically device_id but made unique for subs table (e.g. if extra rows added for free-trial, etc)
    s.acnt_id,
    user_id,
    device_id,
    acnt_has_user_id,
    purchased,
    expires,
    expires_date,
    platform,
    --in_usa,
    plan,
    plan_type,
    plan_duration,
    is_trial_period,
    plan_is_free,
    lower(cc) as cc,
    --revenue information
      --single transaction
    round(product_price_estimated, 2) as product_price,
    --round(product_price_after_taxes_estimated, 2) as product_price_after_taxes,
    --round(first_fee_estimated, 2) as first_fee,
    round(product_proceeds_first_transaction_estimated, 2) as product_proceeds_first_transaction,
      --total transactions
    n_transactions_corrected as n_transactions,
    --refund info
    refund_requested,
    probability_refunded,
    case when n_transactions_refunded is null and probability_refunded > .1 then 1
        when n_transactions_refunded is null and probability_refunded < .1 then 0
        else n_transactions_refunded
        end as n_transactions_refunded,
    case when total_refunded is not null then ABS(total_refunded)
        when probability_refunded > .1 then round(product_price_estimated, 2)
        else 0 end as total_refunded,
    refund_requested_at,
    datediff(days, purchased, refund_requested_at) as time_til_refund_request,
    case when n_transactions_refunded = 0 then null
        else refund_reason
        end as refund_reason,
      --total final $$$
    n_transactions_paid_estimated as n_transactions_paid,
    round(total_paid_estimated, 2) as total_paid,
    round(total_proceeds_estimated, 2) as total_proceeds,
    how_product_price_estimated,
    --details
    plan_is_free_for_user,
    start_w_trial_period,
    is_in_billing_retry_period,
    is_nonreturning_auto_renewal,
    auto_renewing,
    auto_renewal_canceled_at as auto_renewal_canceled_at_stripe_only,
    --secondary extra info
    details_id,
    transaction_base_id,
    fs.first_purchase,
    coupon,
    gift_type,
    campaign_name,
    created_at,
    updated_at,
    store_country_code,
    tax_rate_estimated,
    estimated_plan_duration_days,
    current_timestamp as table_created_at
from subs_with_total_revenue s
left join first_purchase fs
on fs.acnt_id = s.acnt_id

;
