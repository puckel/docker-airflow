drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point_temp cascade;

create table ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '08970b02-b5c1-446b-ab83-cf2c3300e70a'                                       as experiment_id,
    json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageReport', TRUE) as variant,
    entityid                                                                     as entity_id,
    'Parents'                                                                    as entity_type,
    min(createdat)                                                               as entered_at
  from logs.product_event_no_pii
  where
    eventname = 'ios.parent.paid_product.sales_page.exposure'
    and createdat >= '2020-10-07'
    and createdat <= getdate()
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageReport', TRUE) != ''
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageReport', TRUE) != 'off'
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageReport', TRUE) is not null
    and json_extract_path_text(metadata, 'entryPoint', TRUE) in ('homeReports', 'schoolReports')
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point_temp to group team;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point_temp to astronomer;
drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point cascade;
alter table ab_platform.experiment_populations_oct2020_contextual_sales_page_report_entry_point_temp
  rename to experiment_populations_oct2020_contextual_sales_page_report_entry_point;
commit;
