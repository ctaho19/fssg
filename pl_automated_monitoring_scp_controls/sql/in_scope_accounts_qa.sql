-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Note: QA may use different table or mock data
select distinct
    account
from 
    eiam_db.qhdp_cybr.identity_reports_divisions_v3_v2
where 
    date(sf_load_timestamp) = current_date
    and status = 'Active'