/*
combines sessions, uncompleted sessions, program_variants, guide_variants, faves, and session polls
NOTES -1.  we did not start recording uncompleted sessions in our DB until April, 2017
       2. duration col in sessions/uncompleted is wrong and fixed here
       3. breathe_pace_id, guide_variant_id and program_variant_id are all missing from 12% of rows. this is a bug
              -this code will assume that the guides with missing variant ids are all content_group {0-default}
              -enables session info for rows with missing data
maybe add - acnt's session_number - row_number() over (partition BY acnt_id ORDER BY logged_at ASC) session_number
*/
DROP TABLE IF EXISTS {{ params.schema }}.sessions_all_stage;

CREATE TABLE {{ params.schema }}.sessions_all_stage
distkey (acnt_id)
interleaved sortkey (logged_at,
                     session_type,
                     meditation_type,
                     rating,
                     progress)
AS
with default_pg_ids AS (
--some sessions, expecially in uncompleted have a guide_id but not guide_variant_id.  so will add guide_variant_id
  SELECT guide_id,
         guide_variant_id,
         program_variant_id
  FROM {{ params.schema }}.guide_variants_info_stage
  WHERE content_group_order = 1
)
SELECT sess.id AS session_id,
       coalesce(sess.user_id,
                sess.device_id) AS acnt_id,
       sess.user_id,
       sess.device_id,
       sess.guide_id,
       sess.guide_variant_id,
       sess.program_id,
       sess.program_variant_id,
       gv.guide_title,
       gv.program_title,
       gv.program_subtitle,
       CASE WHEN gv.session_type is null
                AND sess.breathe_style_id is not null then 'breathe'
            ELSE gv.session_type END AS session_type,
       coalesce(meditation_type, sleep_type, music_type) as session_subtype,
       meditation_type,
       sleep_type,
       music_type,
       sess.logged_at,
       sess.logged_at_hour_later,
       sess.logged_at_local,
       logged_hour,
       CASE WHEN logged_hour >=6 AND logged_hour <12 then 'morning'
            when logged_hour >=12 and logged_hour < 18 then 'afternoon'
            when logged_hour >=18 and logged_hour <24 then 'evening'
            when logged_hour <6 or logged_hour >=24 then 'middle_night'
            else NULL END as logged_part_of_day,
       logged_dayofweek,
       CASE WHEN logged_dayofweek = 0 or logged_dayofweek = 6 then true
            when logged_dayofweek > 0 and logged_dayofweek < 6 then false
            else NULL END as logged_is_weekend,
       sess.completed,
       sess.progress,
       datediff(day, logged_at, getdate()) AS days_since_sess,
       gv.guide_duration,
       case when gv.meditation_type = 'timer' then sess.duration
            else sess.progress * gv.guide_duration end as duration, --note sessions.duration has a bug for android and isn't correct for this platform
       CASE
           WHEN sp.rating is null then false
           WHEN sp.rating is not null then true
       END AS rated_session,
       sp.rating_created_at,
       sp.rating,
       sp.comment AS rating_comment,
       sp.question AS rating_question,
       gv.guide_created_at,
       CASE
           WHEN f.guide_id is null then false
           WHEN f.created_at is not null then true
       END AS favorited_guide,
       CASE
           WHEN f.created_at is null then false
           WHEN f.created_at is not null
                AND f.deleted_at is not null then false
           WHEN f.created_at is not null
                AND f.deleted_at is null then true
       END AS current_fav_guide,
       CASE
           WHEN f.deleted_at is null then false
           WHEN f.deleted_at is not null then true
       END AS deleted_fav_guide,
       f.created_at AS faves_created_at,
       f.deleted_at AS faves_deleted_at,
       f.updated_at AS faves_updated_at,
       gv.guide_position,
       gv.program_created_at,
       gv.program_updated_at,
       gv.program_free,
       gv.program_content_group,
       gv.program_author,
       gv.program_description,
       gv.program_long_description,
       gv.guide_variant_narrator_id,
       gv.narrator_name,
       gv.narrator_bio,
       gv.narrator_created_at,
       gv.is_composer,
       sp.poll_id,
       sess.breathe_style_id,
       sess.breathe_pace_id,
       gv.asset_id,
       b.breathe_style_title,
       b.breathe_pace,
       sess.created_at AS sess_created_at,
       sess.updated_at AS sess_updated_at,
       meditation_type AS orig_meditation_type_in_pg_db,
       current_timestamp as table_created_at
FROM (--UNION COMPLETED AND UNCOMPLETED SESSIONS TOGTHER
        (SELECT id,
                user_id,
                device_id,
                sess.guide_id,
                coalesce(sess.guide_variant_id, vars.guide_variant_id) as guide_variant_id,
                program_id,
                coalesce(sess.program_variant_id, vars.program_variant_id) as program_variant_id,
                breathe_style_id,
                breathe_pace_id,
                logged_at_local,
                logged_at,
                logged_at + '1 hour'::interval AS logged_at_hour_later,
                extract(hour from logged_at_local) as logged_hour,
                extract(dayofweek from logged_at_local) as logged_dayofweek,
                created_at,
                updated_at,
                1 AS progress,
                true AS completed,
                duration
          FROM appdb.sessions sess
          left join default_pg_ids vars
          on vars.guide_id = sess.guide_id)
      UNION all
        (SELECT id,
                user_id,
                device_id,
                sess.guide_id,
                coalesce(sess.guide_variant_id, vars.guide_variant_id) as guide_variant_id,
                program_id,
                coalesce(sess.program_variant_id, vars.program_variant_id) as program_variant_id,
                NULL AS breathe_style_id,
                NULL AS breathe_pace_id,
                NULL AS logged_at_local,
                logged_at,
                logged_at + '1 hour'::interval AS logged_at_hour_later,
                NULL AS logged_hour,
                NULL AS logged_dayofweek,
                created_at,
                updated_at,
                progress,
                false AS completed,
                duration
         FROM appdb.uncompleted_sessions sess
        left join default_pg_ids vars
         on vars.guide_id = sess.guide_id
         ) ) sess
--now add extra information
left join {{ params.schema }}.guide_variants_info_stage gv
on gv.guide_variant_id = sess.guide_variant_id
left join
  (SELECT id AS poll_id,
          session_id,
          created_at AS rating_created_at,
          question,
          rating,
          comment
   FROM appdb.session_polls
   ) sp
  ON sp.session_id = sess.id
left join appdb.faves f
  ON f.guide_id = sess.guide_id
  AND f.user_id = sess.user_id
left join
  (SELECT id,
          title AS breathe_style_title,
          pace AS breathe_pace
   FROM appdb.breathe_paces
  ) b
  ON b.id = sess.breathe_pace_id
