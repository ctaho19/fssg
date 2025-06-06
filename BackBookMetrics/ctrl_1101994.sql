-- Tier 2: Accuracy Metric (Is the control executing precisely).
-- Description: This metric determines the % of test cases that returned the expected outcome for blocking transmission traffic.
-- Precondition: Test expected outcome.
-- Postcondition: Test actual outcome.

WITH SUMMARY AS 
-- Calculate total tests and successful tests for most recent scan
(SELECT
        COUNT(*) AS TOTAL_TESTS, --DO WE WANT TO CONSIDER EXPIRED OR CANCELLED?
        SUM(CASE WHEN EXPECTED_OUTCOME = ACTUAL_OUTCOME THEN 1 ELSE 0 END) AS TOTAL_SUCCESSFUL_TESTS
    FROM CYBR_DB.PHDP_CYBR.outcome_monitoring_storage 
    WHERE DATEDIFF(day, TO_DATE(SF_LOAD_TIMESTAMP), CURRENT_DATE) = 1 AND PLATFORM = 'symantec_proxy' --USING ADITYA'S EXPRESSION AS A FIX
)
    
SELECT CURRENT_TIMESTAMP AS DATE,
'CTRL-1101994' AS CTRL_ID,
'MNTR-1101994-T2' AS MONITORING_METRIC_NUMBER,
CASE 
        WHEN TOTAL_TESTS = 0 THEN 0
        ELSE ROUND(100.00 * TOTAL_SUCCESSFUL_TESTS / TOTAL_TESTS, 2)
    END AS MONITORING_METRIC,
CASE 
        WHEN TOTAL_TESTS = 0 THEN 'RED'                                            -- No tests run
        WHEN MONITORING_METRIC >=90 THEN 'GREEN'                    -- All tests passed
        WHEN MONITORING_METRIC >=80 AND MONITORING_METRIC <=90 THEN 'YELLOW'
        WHEN TOTAL_SUCCESSFUL_TESTS = 0 THEN 'RED'                               -- All tests failed
        ELSE 'RED'                                                  
END AS COMPLIANCE_STATUS,
ROUND(TOTAL_SUCCESSFUL_TESTS,2) AS NUMERATOR,
ROUND(TOTAL_TESTS, 2) AS DENOMINATOR
FROM SUMMARY;