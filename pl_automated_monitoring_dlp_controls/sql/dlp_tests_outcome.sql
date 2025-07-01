-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the DLP test outcomes from the previous day for all platforms
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
    AND PLATFORM IN ('proofpoint', 'symantec_proxy', 'slack_cloudsoc')