-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get historical test statistics over past 90 days for anomaly detection
WITH DAILY_TESTS AS (
    SELECT 
        REPORTDATE, 
        COUNT(*) AS TOTAL_TESTS
    FROM 
        CYBR_DB.PHDP_CYBR.MACIE_CONTROLS_TESTING
    WHERE 
        REPORTDATE BETWEEN DATEADD(DAY, -90, CURRENT_DATE()) AND DATEADD(DAY, -1, CURRENT_DATE())
    GROUP BY 
        REPORTDATE
)
SELECT
    AVG(TOTAL_TESTS) AS AVG_HISTORICAL_TESTS,
    MIN(TOTAL_TESTS) AS MIN_HISTORICAL_TESTS,
    MAX(TOTAL_TESTS) AS MAX_HISTORICAL_TESTS
FROM 
    DAILY_TESTS