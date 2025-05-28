-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get SLA data for non-compliant resources (Tier 3)
SELECT 
    RESOURCE_ID,
    CONTROL_RISK,
    OPEN_DATE_UTC_TIMESTAMP
FROM 
    CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
WHERE 
    CONTROL_ID = 'AC-3.AWS.39.v02'
    AND ID NOT IN (
        SELECT ID 
        FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04
    )