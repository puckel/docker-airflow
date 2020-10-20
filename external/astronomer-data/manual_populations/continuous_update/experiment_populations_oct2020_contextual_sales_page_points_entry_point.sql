drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point_temp cascade;

create table ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '1bd1c764-aad7-4d06-994e-bc2a4db3d8a1'                                       as experiment_id,
    json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPagePoints', TRUE) as variant, 
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and createdat >= '2020-10-07'
    and createdat <= getdate()
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPagePoints', TRUE) != ''
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPagePoints', TRUE) != 'off'
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPagePoints', TRUE) is not null
    and json_extract_path_text(metadata, 'entryPoint', TRUE) in ('outOfPointsUpgradeNow', 'outOfPoints')
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point_temp to group team;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point_temp to astronomer;
drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point cascade;
alter table ab_platform.experiment_populations_oct2020_contextual_sales_page_points_entry_point_temp
  rename to experiment_populations_oct2020_contextual_sales_page_points_entry_point;
commit;
