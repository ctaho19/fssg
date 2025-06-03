-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get historical test statistics over past 90 days for anomaly detection
with daily_tests as (
    select
        reportdate,
        count(*) as total_tests
    from
        cybr_db.phdp_cybr.macie_controls_testing
    where
        reportdate between dateadd(day, -90, current_date()) and dateadd(day, -1, current_date())
    group by
        reportdate
)
select
    avg(total_tests) as avg_historical_tests,
    min(total_tests) as min_historical_tests,
    max(total_tests) as max_historical_tests
from
    daily_tests