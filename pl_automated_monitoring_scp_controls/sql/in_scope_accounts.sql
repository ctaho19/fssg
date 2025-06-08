-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select distinct
    account
from 
    eiam_db.phdp_cybr_iam.identity_reports_divisions_v2
where 
    date(sf_load_timestamp) = current_date
    and status = 'Active'