-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
select
    monitoring_metric_id,
    control_id,
    monitoring_metric_tier,
    metric_name,
    metric_description,
    warning_threshold,
    alerting_threshold,
    current_timestamp() as load_timestamp,
    metric_threshold_start_date
from ETIP_DB.PHDP_ETIP_CONTROLS_MONITORING.ETIP_CONTROLS_MONITORING_METRICS_DETAILS
where METRIC_ACTIVE_STATUS = 'Y'
  and control_id in (%(control_ids)s)