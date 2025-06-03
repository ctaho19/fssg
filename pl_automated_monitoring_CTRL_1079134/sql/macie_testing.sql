-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the most recent Macie control test results (within the last 3 days)
with last_scan_date as (
    select max(reportdate) as recent_date
    from cybr_db.phdp_cybr.macie_controls_testing
    where reportdate between dateadd(day, -3, current_date()) and current_date()
)
select
    t.reportdate,
    t.testissuccessful,
    t.testid,
    t.testname
from
    cybr_db.phdp_cybr.macie_controls_testing t
join
    last_scan_date lsd
on
    t.reportdate = lsd.recent_date