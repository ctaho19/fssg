-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get evaluated roles for machine IAM preventative controls
SELECT DISTINCT
    UPPER(RESOURCE_NAME) as RESOURCE_NAME,
    COMPLIANCE_STATUS,
    CONTROL_ID
FROM 
    EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2
WHERE 
    CONTROL_ID in ('AC-6.AWS.13.v01', 'AC-6.AWS.35.v02')
    AND DATE(CREATE_DATE) = CURRENT_DATE
    AND ROLE_TYPE = 'MACHINE'