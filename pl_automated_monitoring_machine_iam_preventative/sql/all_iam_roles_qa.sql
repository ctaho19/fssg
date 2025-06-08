-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get all IAM roles from Snowflake QA environment
SELECT DISTINCT
    RESOURCE_ID,
    UPPER(AMAZON_RESOURCE_NAME) as AMAZON_RESOURCE_NAME,
    BA,
    ACCOUNT,
    CREATE_DATE,
    TYPE,
    FULL_RECORD,
    ROLE_TYPE,
    SF_LOAD_TIMESTAMP,
    LOAD_TIMESTAMP
FROM
    etip_db.qhdp_cybr.identity_reports_iam_resource_v4
WHERE
    ROLE_TYPE = 'MACHINE'
    AND ACCOUNT IS NOT NULL
    AND AMAZON_RESOURCE_NAME IS NOT NULL