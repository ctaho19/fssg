-- Query to fetch monitoring thresholds for CTRL-1101994
SELECT 
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    warning_threshold,
    alerting_threshold,
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
FROM monitoring_thresholds 
WHERE control_id = %(control_id)s
  AND (metric_threshold_end_date IS NULL OR metric_threshold_end_date > CURRENT_TIMESTAMP)
  AND metric_threshold_start_date <= CURRENT_TIMESTAMP;