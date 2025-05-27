-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- sqlfluff:disable:disable_progress_bar:deprecated
SELECT 
    RESOURCE_ID, 
    CONTROL_ID,
    CONTROL_RISK, 
    OPEN_DATE_UTC_TIMESTAMP,
    -- Add validation flag to check if resource exists in TCRD
    CASE 
        WHEN RESOURCE_ID IS NOT NULL THEN 'TCRD_VALIDATED'
        ELSE 'TCRD_MISSING'
    END as TCRD_STATUS,
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
WHERE CONTROL_ID = %(control_id)s
  AND RESOURCE_ID IN (%(resource_id_list)s)
  AND ID NOT IN (SELECT ID FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04) 