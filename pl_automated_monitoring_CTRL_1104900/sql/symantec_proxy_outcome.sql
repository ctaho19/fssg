-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the symantec proxy test outcomes from the previous day
SELECT
    TEST_ID,
    TEST_NAME,
    PLATFORM,
    EXPECTED_OUTCOME,
    ACTUAL_OUTCOME,
    TEST_DESCRIPTION,
    SF_LOAD_TIMESTAMP
FROM 
    CYBR_DB.PHDP_CYBR.outcome_monitoring_storage
WHERE 
    DATEDIFF(day, TO_DATE(SF_LOAD_TIMESTAMP), CURRENT_DATE) = 1
    AND PLATFORM = 'symantec_proxy'