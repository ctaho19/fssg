-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get roles that have been evaluated for MFA compliance
SELECT 
    DISTINCT cv.RESOURCE_NAME
FROM 
    EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2 cv
WHERE 
    cv.CONTROL_ID = 'IA-5.AWS.01.v01' 
    AND DATE(cv.SF_LOAD_TIMESTAMP) = CURRENT_DATE