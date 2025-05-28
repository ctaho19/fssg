-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get in-scope roles that require MFA (human roles in specific account with MFA disabled)
SELECT 
    AMAZON_RESOURCE_NAME
FROM 
    EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3 ir
WHERE 
    ir.ROLE_TYPE = 'HUMAN' 
    AND ir.ACCOUNT = '601575451863' 
    AND ir.FULL_RECORD LIKE '%"mfa_active": false%' 
    AND DATE(ir.SF_LOAD_TIMESTAMP) = CURRENT_DATE