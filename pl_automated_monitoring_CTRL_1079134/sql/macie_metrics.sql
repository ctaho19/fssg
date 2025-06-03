-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Get the most recent Macie metrics
select
    metric_date,
    sf_load_timestamp,
    total_buckets_scanned_by_macie,
    total_cloudfronted_buckets
from
    cybr_db.phdp_cybr.macie_metrics_v3
where
    sf_load_timestamp = (
        select max(sf_load_timestamp)
        from cybr_db.phdp_cybr.macie_metrics_v3
    )