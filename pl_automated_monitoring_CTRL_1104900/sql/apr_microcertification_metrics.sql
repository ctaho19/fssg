-- sqlfluff:dialect:snowflake
-- sqlfluff:templater:placeholder:param_style:pyformat
-- Extract raw APR microcertification data for processing in pipeline.py
-- All compliance logic and calculations moved to pipeline

-- In-scope ASVs that need microcertification
select 
    'in_scope_asvs' as data_type,
    asv,
    null::string as hpsm_name,
    null::int as status,
    null::timestamp as created_date,
    null::timestamp as target_remediation_date,
    null::string as certification_type
from eiam_db.phdp_eiam_ciw_apr_compute_group_certification_as

union all

-- Microcertification records
select 
    'microcertification_records' as data_type,
    null::string as asv,
    hpsm_name,
    status,
    to_timestamp(created_date) as created_date,
    to_timestamp(target_remediation_date) as target_remediation_date,
    certification_type
from eiam_db.phdp_eiam_iiqcap1.aa_microcertification
where certification_type = 'Compute Groups APR'
    and to_timestamp(created_date) >= dateadd(year, -1, current_date())