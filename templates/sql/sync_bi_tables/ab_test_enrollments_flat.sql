DROP TABLE IF EXISTS {{ params.schema }}.ab_test_enrollments_flat_stage;

CREATE TABLE {{ params.schema }}.ab_test_enrollments_flat_stage
    distkey (acnt_id)
    interleaved sortkey (ab_test_name,
                         variant,
                         first_assigned)
AS
with ab_w_acnts as (
    select coalesce(du.user_id, ab.device_id) as acnt_id,
           ab.device_id,
           ab.ab_test_name,
           ab.variant,
           ab.created_at,
           ab.updated_at,
           row_number() over (partition by coalesce(du.user_id, ab.device_id), ab.ab_test_name order by ab.created_at) row_num
    from appdb.ab_test_enrollments ab
    left join appdb.device_users du
        on du.device_id = ab.device_id
)

select acnt_id,
       device_id as fist_device_id,
       ab_test_name,
       variant,
       created_at as first_assigned,
       updated_at,
       current_timestamp as table_created_at
from ab_w_acnts
where row_num = 1
