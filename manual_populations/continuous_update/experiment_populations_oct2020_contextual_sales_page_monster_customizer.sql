drop table if exists ab_platform.experiment_populations_oct2020_contextual_monster_customizer_temp cascade;

create table ab_platform.experiment_populations_oct2020_contextual_monster_customizer_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '01b84631-f0a3-4b9f-8d4b-dfdb1c08c415'                                       as experiment_id,
    json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageMonsterCustomizer', TRUE) as variant, 
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and createdat >= '2020-10-07'
    and createdat <= getdate()
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageMonsterCustomizer', TRUE) != ''
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageMonsterCustomizer', TRUE) is not null
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_oct2020_contextual_monster_customizer_temp to group team;
grant all on ab_platform.experiment_populations_oct2020_contextual_monster_customizer_temp to astronomer;
drop table if exists ab_platform.experiment_populations_oct2020_contextual_monster_customizer cascade;
alter table ab_platform.experiment_populations_oct2020_contextual_monster_customizer_temp
  rename to experiment_populations_oct2020_contextual_monster_customizer;
commit;
