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
    control_executor,
    metric_threshold_start_date,
    metric_threshold_end_date
from
    etip_db.qhdp_techcos.dim_controls_monitoring_metrics
where 
    metric_threshold_end_date is null
    and control_id in (
        'CTRL-1105806',  -- AC-6.AWS.13.v01
        'CTRL-1077124',  -- AC-6.AWS.35.v02
        'CTRL-1104930',  -- AC-6.AWS.72.v02
        'CTRL-1102813',  -- AC-3.AWS.41.v02
        'CTRL-1088213',  -- AC-3.AWS.40.v02
        'CTRL-1101993',  -- AC-6.AWS.69.v02
        'CTRL-1101992',  -- AC-2.AWS.20.v01
        'CTRL-1101989',  -- AC-6.AWS.68.v02
        'CTRL-1094973',  -- AC-6.AWS.62.v02
        'CTRL-1088212',  -- AC-6.AWS.52.v02
        'CTRL-1078031',  -- AC-3.AWS.36.v02
        'CTRL-1100340'   -- AC-3.AWS.91.v01
    )