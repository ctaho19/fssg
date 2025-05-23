WITH ONE_YEAR_AGO AS (
    SELECT DATEADD(YEAR, -1, CURRENT_DATE()) AS START_DATE
),
JOINED_TABLE_BTWN_SF_SN AS 
(
    SELECT 
        B.HPSM_NAME, A.APPLICATION_ASV 
    FROM EIAM_DB.PHDP_EIAM_IIQCAP1.AA_MICROCERTIFICATION A
    RIGHT JOIN CMDB_DB.PHDP_CMDB.CMDB_CI_SUMMARY_BC B
    ON A.APPLICATION_ASV = B.HPSM_NAME
    WHERE A.APPLICATION_ASV IS NOT NULL
    AND INSTALL_STATUS_DESC = 'In Production'
),
COMPLIANCE_RESULT AS 
(
    SELECT HPSM_NAME,
           APPLICATION_ASV,
           CASE 
               WHEN HPSM_NAME IS NOT NULL THEN 'GREEN'
               ELSE 'RED' 
           END AS IN_COMPLIANCE
    FROM JOINED_TABLE_BTWN_SF_SN
),
REVIEW_STATS AS (
    SELECT
        COUNT(*) AS TOTAL_ROLES,
        SUM(CASE
            WHEN cert_past_due_30_days = 0 AND cert_past_due_60_days = 0 AND cert_past_due_90_days = 0 THEN 1 ELSE 0 END) AS COMPLIANT_ROLES
    FROM (
        SELECT
            CERT.*,
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 30 THEN 1 ELSE 0 END AS cert_past_due_30_days,
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 60 THEN 1 ELSE 0 END AS cert_past_due_60_days,
            CASE WHEN CERT.STATUS IN (0, 1, 2, 3) AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.CREATED_DATE), CURRENT_TIMESTAMP()) >= 90 THEN 1 ELSE 0 END AS cert_past_due_90_days,
            CASE WHEN CERT.STATUS = 6 THEN 0 ELSE 1 END AS not_in_remediation
        FROM EIAM_DB.PHDP_EIAM_IIQCAP1.AA_MICROCERTIFICATION CERT
        WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR'
            AND TO_TIMESTAMP(CERT.CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())
    ) CERT
    WHERE not_in_remediation = 1
),
REMEDIATION_STATS AS (
    SELECT
        COUNT(*) AS TOTAL_ROLES_IN_REMEDIATION,
        SUM(CASE
            WHEN remediation_past_due_30_days = 0 AND remediation_past_due_60_days = 0 AND remediation_past_due_90_days = 0 THEN 1 ELSE 0 END) AS COMPLIANT_ROLES
    FROM (
        SELECT
            CERT.*,
            CASE WHEN CERT.STATUS = 6 AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 30 THEN 1 ELSE 0 END AS remediation_past_due_30_days,
            CASE WHEN CERT.STATUS = 6 AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 60 THEN 1 ELSE 0 END AS remediation_past_due_60_days,
            CASE WHEN CERT.STATUS = 6 AND TIMESTAMPDIFF('DAY', TO_TIMESTAMP(CERT.TARGET_REMEDIATION_DATE), CURRENT_TIMESTAMP()) >= 90 THEN 1 ELSE 0 END AS remediation_past_due_90_days
        FROM EIAM_DB.PHDP_EIAM_IIqCAP1.AA_MICROCERTIFICATION CERT
        WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR'
            AND TO_TIMESTAMP(CERT.CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())
            AND CERT.STATUS = 6
    ) CERT
)
SELECT
    CURRENT_TIMESTAMP() AS DATE,
    'CTRL-1104900' AS CTRL_ID,
    'MNTR-1104900-T0' AS MONITORING_METRIC_NUMBER,
    TO_DOUBLE(ROUND(100.0 * COUNT(CASE WHEN TO_TIMESTAMP(CERT.CREATED_DATE) >= START_DATE THEN 1 END) / COUNT(*), 2)) AS MONITORING_METRIC,
    CASE
        WHEN COUNT(*) > 0 THEN 'GREEN'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    TO_DOUBLE(ROUND(COUNT(CASE WHEN TO_TIMESTAMP(CERT.CREATED_DATE) >= START_DATE THEN 1 END),2)) AS NUMERATOR,
    1 AS DENOMINATOR
FROM EIAM_DB.PHDP_EIAM_IIQCAP1.AA_MICROCERTIFICATION CERT
CROSS JOIN ONE_YEAR_AGO
WHERE CERT.CERTIFICATION_TYPE = 'Compute Groups APR'
AND TO_TIMESTAMP(CERT.CREATED_DATE) >= START_DATE

UNION ALL

SELECT CURRENT_TIMESTAMP() AS DATE,
    'CTRL-1104900' AS CTRL_ID,
    'MNTR-1104900-T1' AS MONITORING_METRIC_NUMBER,
    ROUND(COUNT(DISTINCT CASE WHEN IN_COMPLIANCE='GREEN' THEN APPLICATION_ASV END)/COUNT(DISTINCT APPLICATION_ASV)* 100,2) AS MONITORING_METRIC,
    CASE WHEN ROUND(COUNT(DISTINCT CASE WHEN IN_COMPLIANCE='GREEN' THEN APPLICATION_ASV END)/COUNT(DISTINCT APPLICATION_ASV)* 100,2) = 100 THEN 'GREEN'
         ELSE 'RED' 
    END AS COMPLIANCE_STATUS,
    TO_DOUBLE(ROUND(COUNT(DISTINCT CASE WHEN IN_COMPLIANCE = 'GREEN' THEN APPLICATION_ASV END),2)) AS NUMERATOR,
    TO_DOUBLE(ROUND(COUNT(DISTINCT APPLICATION_ASV),2)) AS DENOMINATOR
FROM COMPLIANCE_RESULT

UNION ALL

SELECT
    CURRENT_TIMESTAMP() AS DATE,
    'CTRL-1104900' AS CTRL_ID,
    'MNTR-1104900-T2' AS MONITORING_METRIC_NUMBER,
    ROUND(100.0 * COMPLIANT_ROLES / NULLIF(TOTAL_ROLES, 0), 2) AS MONITORING_METRIC,
    CASE
        WHEN EXISTS (SELECT 1 FROM EIAM_DB.PHDP_EIAM_IIQCAP1.AA_MICROCERTIFICATION WHERE STATUS IS NULL AND CERTIFICATION_TYPE = 'Compute Groups APR' AND TO_TIMESTAMP(CREATED_DATE) >= DATEADD(YEAR, -1, CURRENT_DATE())) THEN 'RED'
        WHEN (COMPLIANT_ROLES = TOTAL_ROLES) THEN 'GREEN'
        WHEN MONITORING_METRIC >= 99 THEN 'GREEN'
        WHEN MONITORING_METRIC >= 98 AND MONITORING_METRIC <= 99 THEN 'YELLOW'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    TO_DOUBLE(COMPLIANT_ROLES) AS NUMERATOR,
    TOTAL_ROLES AS DENOMINATOR
FROM REVIEW_STATS

UNION ALL

SELECT
    CURRENT_TIMESTAMP() AS DATE,
    'CTRL-1104900' AS CTRL_ID,
    'MNTR-1104900-T3' AS MONITORING_METRIC_NUMBER,
    CASE
        WHEN TOTAL_ROLES_IN_REMEDIATION = 0 THEN 100
        ELSE ROUND(100.0 * COMPLIANT_ROLES / NULLIF(TOTAL_ROLES_IN_REMEDIATION, 0), 2)
    END AS MONITORING_METRIC,
    CASE
        WHEN TOTAL_ROLES_IN_REMEDIATION = 0 THEN 'GREEN'
        WHEN (COMPLIANT_ROLES = TOTAL_ROLES_IN_REMEDIATION) THEN 'GREEN'
        ELSE 'RED'
    END AS COMPLIANCE_STATUS,
    TO_DOUBLE(CASE
    WHEN COMPLIANT_ROLES IS NULL THEN 0
    ELSE COMPLIANT_ROLES
END) AS NUMERATOR,
    TOTAL_ROLES_IN_REMEDIATION AS DENOMINATOR
FROM REMEDIATION_STATS;