begin;

drop table if exists {{ params.schema }}.{{ params.table_name }};

alter table {{ params.schema }}.{{ params.stage_name }} rename to {{ params.table_name }};

commit;
