-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get evaluated IAM roles from Snowflake QA environment
SELECT DISTINCT
    RESOURCE_NAME,
    COMPLIANCE_STATUS,
    CONTROL_ID,
    EVALUATION_DATE
FROM
    etip_db.qhdp_techcos.dim_evaluated_iam_roles
WHERE
    CONTROL_ID IN ('AC-6.AWS.13.v01', 'AC-6.AWS.35.v02')
    AND EVALUATION_DATE >= DATEADD(day, -30, CURRENT_DATE())
    AND RESOURCE_NAME IS NOT NULL