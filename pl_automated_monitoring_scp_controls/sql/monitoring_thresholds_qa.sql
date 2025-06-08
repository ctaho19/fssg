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
        'CTRL-1077770',
        'CTRL-1079538',
        'CTRL-1081889',
        'CTRL-1102567',
        'CTRL-1105846',
        'CTRL-1105996',
        'CTRL-1105997',
        'CTRL-1106155',
        'CTRL-1103299',
        'CTRL-1106425'
    )