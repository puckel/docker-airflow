/*
combines explicit and implicit ratings of sessions into a new 1-4 star scale
4.  Fave, 5-stars, or complete 2 or more times
3.  4 stars
2.  3 stars or
        implicit:  transfer (cancel/incomplete (>15secs) & progress < .90 & and complete different story within an hour)

1.  2 or 1 star (not enough of either in data so reduce them to one star)

if ratings are given in more than one way (fave, star rating, mult_compl, transfer), then use the below order of priority
1.  fave
2.  star rating
3.  mult_compl
4.  transfer
e.g., if called something a fave, it gets 4 stars (the max) regardless of any other behavior
e.g., if completed a session 2 or more times, it is 4 stars even if later on, person cancels that sessions and starts something new
  (e.g., person may be sick of that session but still like that type of contnet)
*/
drop table if exists {{ params.schema }}.integrated_ratings_stage;

create table {{ params.schema }}.integrated_ratings_stage
distkey (acnt_id)
interleaved sortkey (combined_rating_at, subscribed_combined_rating_at)
as

with get_sessions_all as (
    select *
    from {{ params.schema }}.sessions_all_stage
    where acnt_id is not null
    and guide_variant_id is not null
),
get_faves as (
    select
        distinct 'fave'::varchar as rating_method,
        1 as rating_method_priority,
        acnt_id,
        guide_variant_id,
        4 as rating,
        faves_created_at as logged_at
    from get_sessions_all
    where faves_created_at is not null
    and faves_deleted_at is null
),
get_stars as (
    select distinct 'star'::varchar as rating_method,
        2 as rating_method_priority,
        acnt_id,
        guide_variant_id,
        case when rating in (2, 3, 4, 5)
             then rating - 1
             when rating = 1
             then rating
        end as rating,
        rating_created_at as logged_at
    from get_sessions_all
    where rating in (1, 2, 3, 4, 5)
),
get_multicompletes as (
     select 'multicomplete'::varchar as rating_method,
        3 as rating_method_priority,
        acnt_id,
        guide_variant_id,
        4 as rating,
        logged_at
     from (
        select acnt_id,
            guide_variant_id,
            logged_at,
            row_number() over (partition by acnt_id, guide_variant_id order by logged_at) as nth_compl
        from get_sessions_all
        where progress > .95
     )
     where
      nth_compl > 1
    -- every completion after the first counts as a separate multicompletion rating because each is expressing an implicit opinion at a particular time)
),
get_transfers as (
    select 'transfer'::varchar as rating_method,
        4 as rating_method_priority,
        canc.acnt_id,
        canc.guide_variant_id,
        2 as rating,
        compl.logged_at
    from get_sessions_all canc
    join get_sessions_all compl on canc.acnt_id = compl.acnt_id
    where canc.progress < .90
    and canc.duration > 15
    and compl.progress > .95
    and canc.guide_id <> compl.guide_id
    and canc.session_type = compl.session_type
    and canc.logged_at < compl.logged_at
    and canc.logged_at_hour_later > compl.logged_at
),
get_unioned_ratings as (
    select *,
        (rating_method = 'fave') as is_fave_rating,
        (rating_method = 'star') as is_star_rating,
        (rating_method = 'multicomplete') as is_multicomplete_rating,
        (rating_method = 'transfer') as is_transfer_rating
    from (
    select * from get_faves
    union all
    select * from get_stars
    union all
    select * from get_multicompletes
    union all
    select * from get_transfers
    )
),
get_rating_windows as (
     select r.*,
      (r.logged_at > a.first_subs_purchased) as is_after_subscribed,
      -- to get alltime combined rating, prioritize by rating method quality then break ties with most recent within that rating method
      (row_number() over (partition by r.acnt_id, guide_variant_id order by rating_method_priority, logged_at desc) = 1)
        as is_alltime_combined_rating,

      -- get alltime ratings for particular methods
      (is_fave_rating
        and row_number() over (partition by r.acnt_id, guide_variant_id, is_fave_rating order by logged_at desc) = 1)
         as is_alltime_fave_rating,

      (is_star_rating
        and row_number() over (partition by r.acnt_id, guide_variant_id, is_star_rating order by logged_at desc) = 1)
         as is_alltime_star_rating,

      (is_multicomplete_rating
        and row_number() over (partition by r.acnt_id, guide_variant_id, is_multicomplete_rating order by logged_at desc) = 1)
         as is_alltime_multicomplete_rating,

      (is_transfer_rating
        and row_number() over (partition by r.acnt_id, guide_variant_id, is_transfer_rating order by logged_at desc) = 1)
         as is_alltime_transfer_rating,

      -- for anyone who rated after subscribing, get combined rating after subscribing
      (is_after_subscribed
        and row_number() over (partition by r.acnt_id, guide_variant_id, is_after_subscribed order by rating_method_priority, logged_at desc) = 1)
         as is_subscribed_combined_rating,

      -- get subscriber rating for particular methods
      (is_after_subscribed
        and is_fave_rating
         and row_number() over (partition by r.acnt_id, guide_variant_id, is_after_subscribed, is_fave_rating order by logged_at desc) = 1)
           as is_subscribed_fave_rating,

      (is_after_subscribed
        and is_star_rating
         and row_number() over (partition by r.acnt_id, guide_variant_id, is_after_subscribed, is_star_rating order by logged_at desc) = 1)
           as is_subscribed_star_rating,

      (is_after_subscribed
        and is_multicomplete_rating
         and row_number() over (partition by r.acnt_id, guide_variant_id, is_after_subscribed, is_multicomplete_rating order by logged_at desc) = 1)
           as is_subscribed_multicomplete_rating,

      (is_after_subscribed
        and is_transfer_rating
         and row_number() over (partition by r.acnt_id, guide_variant_id, is_after_subscribed, is_transfer_rating order by logged_at desc) = 1)
           as is_subscribed_transfer_rating

     from get_unioned_ratings r
      join {{ params.schema }}.accounts_stage a on a.acnt_id = r.acnt_id
),
get_wide as
(
    select
    acnt_id,
    guide_variant_id,

    max(case when is_alltime_combined_rating then rating end) as combined_rating,
    max(case when is_alltime_combined_rating then rating_method end) as combined_rating_method,
    max(case when is_alltime_combined_rating then logged_at end) as combined_rating_at,

    max(case when is_subscribed_combined_rating then rating end) as subscribed_combined_rating,
    max(case when is_subscribed_combined_rating then rating_method end) as subscribed_combined_rating_method,
    max(case when is_subscribed_combined_rating then logged_at end) as subscribed_combined_rating_at,

    max(case when is_alltime_fave_rating then rating end) as fave_rating,
    max(case when is_alltime_fave_rating then logged_at end) as fave_rating_at,

    max(case when is_alltime_star_rating then rating end) as star_rating,
    max(case when is_alltime_star_rating then logged_at end) as star_rating_at,

    max(case when is_alltime_multicomplete_rating then rating end) as multicomplete_rating,
    max(case when is_alltime_multicomplete_rating then logged_at end) as multicomplete_rating_at,

    max(case when is_alltime_transfer_rating then rating end) as transfer_rating,
    max(case when is_alltime_transfer_rating then logged_at end) as transfer_rating_at,

    max(case when is_subscribed_fave_rating then rating end) as subscribed_fave_rating,
    max(case when is_subscribed_fave_rating then logged_at end) as subscribed_fave_rating_at,

    max(case when is_subscribed_star_rating then rating end) as subscribed_star_rating,
    max(case when is_subscribed_star_rating then logged_at end) as subscribed_star_rating_at,

    max(case when is_subscribed_multicomplete_rating then rating end) as subscribed_multicomplete_rating,
    max(case when is_subscribed_multicomplete_rating then logged_at end) as subscribed_multicomplete_rating_at,

    max(case when is_subscribed_transfer_rating then rating end) as subscribed_transfer_rating,
    max(case when is_subscribed_transfer_rating then logged_at end) as subscribed_transfer_rating_at

    from get_rating_windows
    group by 1,2
)

select
     *,
     current_timestamp as table_created_at
from get_wide
