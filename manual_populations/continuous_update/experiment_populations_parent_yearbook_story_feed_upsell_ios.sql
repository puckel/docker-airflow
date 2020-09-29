drop table if exists ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios_temp;
create table ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios_temp
  distkey(entity_id)
  sortkey(entity_id)
  as (
    SELECT 'f0c98535-6092-425d-9cb1-6979a20863f2' AS experiment_id
         , eventvalue                             as variant
         , entityid                               AS entity_id
         , 'Parents'                              AS entity_type
         , min(createdat)                         AS entered_at
    from logs.product_event
    where eventname = 'ios.experiment.iOS_parentYearbookStoryFeed'
      and eventvalue != 'off'
      and createdat >= '2020-09-28'
    GROUP BY 1, 2, 3, 4
);
begin;
grant all on ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios_temp to group team;
grant all on ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios_temp to astronomer;
drop table if exists ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios cascade;
alter table ab_platform.experiment_populations_parent_yearbook_story_feed_upsell_ios_temp rename to experiment_populations_parent_yearbook_story_feed_upsell_ios;
commit;
