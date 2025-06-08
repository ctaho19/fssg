-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    resource_id,
    resource_type,
    compliance_status,
    control_id
from 
    eiam_db.phdp_cybr_iam.identity_reports_controls_violations_stream_v2
where 
    control_id in (
        'AC-3.AWS.146.v02',
        'CM-2.AWS.20.v02',
        'AC-3.AWS.123.v01',
        'CM-2.AWS.4914.v01',
        'SC-7.AWS.6501.v01',
        'AC-3.AWS.5771.v01',
        'AC-3.AWS.128.v01',
        'AC-3.AWS.117.v01',
        'AC-1.AWS.5099.v02',
        'IA-5.AWS.20.v02'
    )
    and date(sf_load_timestamp) = current_date