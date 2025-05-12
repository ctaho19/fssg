-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- sqlfluff:disable:disable_progress_bar:deprecated
select distinct
    upper(resource_name) as resource_name,
    compliance_status,
    control_id,
    role_type,
    create_date,
    current_timestamp() as load_timestamp
from EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2
where control_id = %(control_id)s
    and date(create_date) = current_date
    and role_type = 'MACHINE' 