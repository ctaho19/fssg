-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the most recent Macie metrics
SELECT 
    METRIC_DATE,
    SF_LOAD_TIMESTAMP,
    TOTAL_BUCKETS_SCANNED_BY_MACIE,
    TOTAL_CLOUDFRONTED_BUCKETS
FROM 
    CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
WHERE 
    SF_LOAD_TIMESTAMP = (
        SELECT MAX(SF_LOAD_TIMESTAMP) 
        FROM CYBR_DB.PHDP_CYBR.MACIE_METRICS_V3
    )