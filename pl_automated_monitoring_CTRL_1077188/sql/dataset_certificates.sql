-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get raw certificate dataset for coverage and compliance calculations
SELECT DISTINCT
    certificate_arn,
    certificate_id,
    nominal_issuer,
    not_valid_before_utc_timestamp,
    not_valid_after_utc_timestamp,
    last_usage_observation_utc_timestamp
FROM 
    CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE 
WHERE 
    certificate_arn LIKE '%arn:aws:acm%' 
    AND nominal_issuer = 'Amazon' 
    AND not_valid_after_utc_timestamp >= DATEADD('DAY', -365, CURRENT_TIMESTAMP)