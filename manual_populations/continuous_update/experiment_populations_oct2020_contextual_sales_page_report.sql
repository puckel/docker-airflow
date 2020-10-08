drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_report_temp cascade;

create table ab_platform.experiment_populations_oct2020_contextual_sales_page_report_temp
  distkey (entity_id)
  sortkey (entity_id) as (
  select
    '041eb60e-5906-4ada-8697-6ec3800cdd3d'                                       as experiment_id,
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
    and json_extract_path_text(metadata, 'iOS_october2020ContextualSalesPageReport', TRUE) is not null
  group by
    entity_id,
    variant,
    experiment_id,
    entity_type
);

begin;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_report_temp to group team;
grant all on ab_platform.experiment_populations_oct2020_contextual_sales_page_report_temp to astronomer;
drop table if exists ab_platform.experiment_populations_oct2020_contextual_sales_page_report cascade;
alter table ab_platform.experiment_populations_oct2020_contextual_sales_page_report_temp
  rename to experiment_populations_oct2020_contextual_sales_page_report;
commit;
