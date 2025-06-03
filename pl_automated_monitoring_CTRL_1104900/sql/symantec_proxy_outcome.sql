-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the symantec proxy test outcomes from the previous day
select
    test_id,
    test_name,
    platform,
    expected_outcome,
    actual_outcome,
    test_description,
    sf_load_timestamp
from
    cybr_db.phdp_cybr.outcome_monitoring_storage
where
    datediff(day, to_date(sf_load_timestamp), current_date) = 1
    and platform = 'symantec_proxy'