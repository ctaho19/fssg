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
    current_timestamp() as load_timestamp
from CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
where control_id = %(control_id)s 