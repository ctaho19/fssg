-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get evaluated IAM roles from Snowflake QA environment
SELECT DISTINCT
    RESOURCE_NAME,
    COMPLIANCE_STATUS,
    CONTROL_ID,
    EVALUATION_DATE
FROM
    etip_db.qhdp_cybr.identity_reports_controls_violations_stream_1_v3
WHERE
    CONTROL_ID IN ('AC-6.AWS.13.v01', 'AC-6.AWS.35.v02')
    AND EVALUATION_DATE >= DATEADD(day, -30, CURRENT_DATE())
    AND RESOURCE_NAME IS NOT NULL