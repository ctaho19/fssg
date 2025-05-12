-- Tier 1 Validation Query with Monitoring Metric Calculation
WITH InScopeRoles AS (
    SELECT 
        ir.AMAZON_RESOURCE_NAME
    FROM 
        EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3 ir
    WHERE 
        ir.ROLE_TYPE = 'HUMAN' 
        AND ir.ACCOUNT = '601575451863' 
        AND ir.FULL_RECORD LIKE '%"mfa_active": false%' 
        AND DATE(ir.SF_LOAD_TIMESTAMP) = CURRENT_DATE
),
EvaluatedRoles AS (
    SELECT 
        DISTINCT cv.RESOURCE_NAME
    FROM 
        EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2 cv
    WHERE 
        cv.CONTROL_ID = 'IA-5.AWS.01.v01' 
        AND DATE(cv.SF_LOAD_TIMESTAMP) = CURRENT_DATE
),
Metrics AS (
    SELECT 
        CURRENT_DATE AS "DATE",
        'CTRL-1106680' AS "CTRL_ID",
        'MNTR-1106680-T1' AS "MONITORING_METRIC_NUMBER",
        ROUND(
            CASE 
                WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = 0 THEN 100.00
                ELSE (COUNT(DISTINCT cv.RESOURCE_NAME) * 100.0 / COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME))
            END, 2
        ) AS "MONITORING_METRIC",
        CASE 
            WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = 0 THEN 'GREEN'
            WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = COUNT(DISTINCT cv.RESOURCE_NAME) THEN 'GREEN'
            ELSE 'RED'
        END AS "COMPLIANCE_STATUS",
        COUNT(DISTINCT cv.RESOURCE_NAME) AS "NUMERATOR",
        COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) AS "DENOMINATOR"
    FROM 
        InScopeRoles ir
    LEFT JOIN 
        EvaluatedRoles cv
    ON 
        ir.AMAZON_RESOURCE_NAME = cv.RESOURCE_NAME
)
SELECT * FROM Metrics;

-- Tier 2: Accuracy
WITH NonCompliantRoles AS (
    -- Identify roles with non-compliance status over time
    SELECT 
        cv.RESOURCE_NAME AS AMAZON_RESOURCE_NAME,
        DATE(cv.SF_LOAD_TIMESTAMP) AS NON_COMPLIANT_DATE
    FROM 
        EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2 cv
    WHERE 
        cv.CONTROL_ID = 'IA-5.AWS.01.v01'
),
ConsecutiveNonCompliance AS (
    -- Calculate consecutive non-compliance periods
    SELECT 
        AMAZON_RESOURCE_NAME,
        NON_COMPLIANT_DATE,
        NON_COMPLIANT_DATE - ROW_NUMBER() OVER (PARTITION BY AMAZON_RESOURCE_NAME ORDER BY NON_COMPLIANT_DATE) AS GROUP_ID
    FROM 
        NonCompliantRoles
),
NonCompliantPeriods AS (
    -- Group by consecutive periods and count days
    SELECT 
        AMAZON_RESOURCE_NAME,
        MIN(NON_COMPLIANT_DATE) AS START_DATE,
        MAX(NON_COMPLIANT_DATE) AS END_DATE,
        COUNT(*) AS CONSECUTIVE_DAYS
    FROM 
        ConsecutiveNonCompliance
    GROUP BY 
        AMAZON_RESOURCE_NAME, GROUP_ID
    HAVING 
        COUNT(*) > 10 -- More than 10 consecutive days
),
InScopeRoles AS (
    -- Identify all roles in scope for evaluation
    SELECT 
        ir.AMAZON_RESOURCE_NAME
    FROM 
        EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3 ir
    WHERE 
        ir.ROLE_TYPE = 'HUMAN' 
        AND ir.ACCOUNT = '601575451863' 
        AND ir.FULL_RECORD LIKE '%"mfa_active": false%' 
        AND DATE(ir.SF_LOAD_TIMESTAMP) = CURRENT_DATE
),
Tier2Metrics AS (
    -- Calculate Tier 2 metrics
    SELECT 
        CURRENT_DATE AS "DATE",
        'CTRL-1106680' AS "CTRL_ID",
        'MNTR-1106680-T2' AS "MONITORING_METRIC_NUMBER",
        ROUND(
            CASE 
                WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = 0 THEN 100.00
                ELSE (COUNT(DISTINCT ncp.AMAZON_RESOURCE_NAME) * 100.0 / COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME))
            END, 2
        ) AS "MONITORING_METRIC",
        CASE 
            WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = 0 THEN 'GREEN'
            WHEN COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) = COUNT(DISTINCT ncp.AMAZON_RESOURCE_NAME) THEN 'GREEN'
            ELSE 'RED'
        END AS "COMPLIANCE_STATUS",
        COUNT(DISTINCT ncp.AMAZON_RESOURCE_NAME) AS "NUMERATOR",
        COUNT(DISTINCT ir.AMAZON_RESOURCE_NAME) AS "DENOMINATOR"
    FROM 
        InScopeRoles ir
    LEFT JOIN 
        NonCompliantPeriods ncp
    ON 
        ir.AMAZON_RESOURCE_NAME = ncp.AMAZON_RESOURCE_NAME
)
SELECT * FROM Tier2Metrics;s