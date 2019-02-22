/*
Gets information about a given guide variant

as of 2018-08-01, programs > program_variant_id > guides > guide_variant_ids > asset_ids

asset_ids belong to, say, a given audio file]
guide_variants:  different ones for different voice actors, different langauges;
        has a position (eg position 1 if day 1 of 7 days of calm)
guides are the same across narrators (one guide for first day of seven days of calm even if multiple versions of that first day)
program_variant_id
    -e.g., if have a version of 7 days Calm with all having shorter days
    -diff pv ids feed down to unique guides, gvs, and asset ids
program id
  -e.g,. one for 7 days of calm, id across all the variations
*/

DROP TABLE IF EXISTS {{ params.schema }}.guide_variants_info_stage;

CREATE TABLE {{ params.schema }}.guide_variants_info_stage
distkey (guide_variant_id)
AS
with gv as (
    SELECT gv.id AS guide_variant_id,
          gv.guide_id,
          gv.timer_duration,
          g.program_variant_id as program_variant_id,
          gv.created_at AS guide_created_at,
          gv.position AS guide_position,
          gv.title AS guide_title,
          gv.subtitle AS guide_subtitle,
          gv.asset_id AS asset_id,
          gv.narrator_id as guide_variant_narrator_id
    FROM appdb.guide_variants gv
    JOIN appdb.guides g on g.id = gv.guide_id
    ),
guides as (
    select id,
          program_variant_id
    from appdb.guides
),
assets as (
    SELECT id AS asset_id,
            case when JSON_EXTRACT_PATH_TEXT(metadata, 'duration') = '' then NULL
              else round(JSON_EXTRACT_PATH_TEXT(metadata, 'duration'), 3) end as duration
   FROM appdb.assets
),
narrators as (
    SELECT id,
          name as narrator_name,
          bio as narrator_bio,
          created_at as narrator_created_at,
          is_composer
   FROM appdb.narrators
),
program_variants as (
    SELECT id AS program_variant_id,
            program_id,
            created_at AS program_created_at,
            updated_at AS program_updated_at,
            free AS program_free,
            position AS program_position,
            subtitle AS program_subtitle,
            title AS program_title,
            meditation_type,
            content_group AS program_content_group,
            author AS program_author,
            description AS program_description,
            long_description AS program_long_description
   FROM appdb.program_variants
)
select gv.guide_variant_id,
    g.id as guide_id,
    a.asset_id,
    pv.program_variant_id,
    pv.program_id,
    guide_title,
    guide_subtitle,
    pv.program_title,
    pv.program_subtitle,
    CASE
      WHEN pv.meditation_type in ('freeform',
                               'hidden',
                               'series',
                               'sequential',
                               'timer',
                               'manual') then 'meditation'
      WHEN pv.meditation_type = 'sleep' then 'sleep'
      WHEN pv.meditation_type = 'music' then 'music'
      WHEN pv.meditation_type = 'masterclass' then 'masterclass'
      WHEN pv.meditation_type = 'body' then 'body'
      ELSE pv.meditation_type
    END AS session_type,
    CASE
           WHEN pv.program_variant_id = 'Wd3y3az6Q' then 'daily_calm'
           WHEN pv.program_variant_id = 'Jj06Zy1P5' then 'freeform'
           WHEN pv.meditation_type = 'freeform' then 'freeform'
           WHEN pv.meditation_type = 'series' then 'series'
           WHEN pv.meditation_type = 'sequential' then 'sequential'
           WHEN pv.meditation_type = 'timer' then 'timer'
           WHEN pv.meditation_type = 'manual' then 'manual'
           ELSE NULL
     END AS meditation_type,
     CASE
         WHEN pv.meditation_type = 'sleep' then lower(pv.program_subtitle)
         ELSE NULL
     END AS sleep_type,
     CASE
         WHEN pv.meditation_type = 'sleep' then true
         WHEN pv.meditation_type = 'music' and lower(pv.program_subtitle) like '%sleep%' then true
         WHEN pv.meditation_type = 'masterclass' and lower(pv.program_title) like '%sleep%' then true
         WHEN lower(pv.program_title) like '%sleep%' then true
         else false end as sleep_content,
     CASE
         WHEN pv.meditation_type = 'music' then lower(pv.program_subtitle)
         ELSE NULL
     END AS music_type,
    guide_position,
    coalesce(a.duration, gv.timer_duration) as guide_duration,
    timer_duration,
    n.narrator_name,
    n.narrator_bio,
    n.is_composer,
    pv.program_author,
    program_description,
    program_long_description,
    program_free,
    gv.guide_created_at,
    program_created_at,
    program_updated_at,
    n.narrator_created_at,
    gv.guide_variant_narrator_id,
    pv.program_content_group,
    row_number() OVER (PARTITION BY gv.guide_id ORDER BY pv.program_content_group) content_group_order,
    pv.meditation_type as original_meditation_type,
    current_timestamp as table_created_at

from gv
left join guides g
on g.id = gv.guide_id
left join assets a
ON a.asset_id = gv.asset_id
left join narrators n
ON n.id = gv.guide_variant_narrator_id
left JOIN program_variants pv
ON pv.program_variant_id = g.program_variant_id
