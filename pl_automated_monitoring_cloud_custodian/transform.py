import json
from datetime import datetimeimport pandas as pdimport pipelines.pl_automated_monitoring_cloud_custodian.pipeline as pipeline
from transform_library import transformerdictionary_tier_1 = {
    "SC-8.AWS.43": "CTRL-1077109",
    "SC-8.AWS.42": "CTRL-1077108",
    "AC-3.AWS.80": "CTRL-1078029",
}aws_services_tier_1 = {
    "GuardDuty": "CTRL-1106206",
    "Directory Service": "CTRL-1106204",
    "DynamoDB Accelerator": "CTRL-1106205",
    "Key Management Service": "CTRL-1106207",
}@transformer
def tier_1_transform_cloud_custodian(
    threshold_df: pd.DataFrame,
    total_inventory_df: pd.DataFrame,
    not_evaluated_df: pd.DataFrame,
) -> pd.DataFrame:
    current_date = datetime.now()

tier_1_metric_df = pd.DataFrame(columns=pipeline.columns)

# Iterate through controls in dictionary and build out metrics
for service, fuse_id in aws_services_tier_1.items():
    if total_inventory_df.empty:
        filtered_ozone_df = total_inventory_df
    else:
        filtered_ozone_df = total_inventory_df.loc[
            (total_inventory_df["service_name"] == service)
        ]
    tier_1_metric_df = build_rows(
        tier_1_metric_df=tier_1_metric_df,
        filtered_ozone_df=filtered_ozone_df,
        threshold_df=threshold_df,
        fuse_id=fuse_id,
        not_evaluated_df=not_evaluated_df,
        current_date=current_date,
        flow="one-to-many",
        filter_condition=service,
    )

for key, fuse_id in dictionary_tier_1.items():
    if total_inventory_df.empty:
        filtered_ozone_df = total_inventory_df
    else:
        filtered_ozone_df = total_inventory_df.loc[
            (total_inventory_df["control_id"].str.contains(key))
            & (total_inventory_df["fuse_id"].str.contains(fuse_id))
        ]
    tier_1_metric_df = build_rows(
        tier_1_metric_df=tier_1_metric_df,
        filtered_ozone_df=filtered_ozone_df,
        threshold_df=threshold_df,
        fuse_id=fuse_id,
        not_evaluated_df=not_evaluated_df,
        current_date=current_date,
        flow="one-to-one",
        filter_condition=key,
    )
return tier_1_metric_df

def build_rows(
    tier_1_metric_df: pd.DataFrame,
    filtered_ozone_df: pd.DataFrame,
    threshold_df: pd.DataFrame,
    fuse_id,
    not_evaluated_df: pd.DataFrame,
    current_date: datetime,
    flow: str,
    filter_condition,
) -> pd.DataFrame:
    total_compliant_df = filtered_ozone_df.loc[
        (filtered_ozone_df["compliance_state"] == "True")
        | (filtered_ozone_df["compliance_state"] == "False")
    ]
    all_evals = total_compliant_df["total_resources"].sum()
    all_inventory = filtered_ozone_df["total_resources"].sum()
    if all_inventory > 0:
        tier_1_calculation = round((all_evals / all_inventory) * 100, 2)
    else:
        tier_1_calculation = 0

if tier_1_calculation < 100.0 and tier_1_calculation > 0:
    # Filter values
    if flow == "one-to-one":
        filtered_not_evaluated_df = not_evaluated_df.loc[
            not_evaluated_df["control_id"].str.contains(filter_condition)
        ].sort_values(by="control_id", ascending=False)
    elif flow == "one-to-many":
        filtered_not_evaluated_df = not_evaluated_df.loc[
            not_evaluated_df["service_name"] == filter_condition
        ].sort_values(by="control_id", ascending=False)
    filtered_not_evaluated_json = filtered_not_evaluated_df.to_json(
        orient="records"
    )
    resource_info = json.loads(filtered_not_evaluated_json)
    empty_array = []
    for x in resource_info:
        x = json.dumps(x)
        empty_array.append(x)
    resource_info = empty_array
else:
    resource_info = None
filtered_threshold_df = threshold_df.loc[
    (threshold_df["control_id"] == fuse_id)
    & (threshold_df["monitoring_metric_tier"] == "Tier 1")
]
# Filter to most recent version of threshold
if not filtered_threshold_df.empty:
    filtered_threshold_df = filtered_threshold_df.loc[
        filtered_threshold_df["metric_threshold_start_date"]
        == filtered_threshold_df["metric_threshold_start_date"].max()
    ]

    if tier_1_calculation < filtered_threshold_df["alerting_threshold"].values[0]:
        monitoring_metric_status = "Red"
    elif (
        tier_1_calculation >= filtered_threshold_df["alerting_threshold"].values[0]
    ) & (tier_1_calculation < filtered_threshold_df["warning_threshold"].values[0]):
        monitoring_metric_status = "Yellow"
    else:
        monitoring_metric_status = "Green"

    d = [
        [
            filtered_threshold_df["monitoring_metric_id"].values[0],
            fuse_id,
            tier_1_calculation,
            monitoring_metric_status,
            all_evals,
            all_inventory,
            resource_info,
            current_date,
        ]
    ]
    tier_1_metric_df = pd.concat(
        [pd.DataFrame(d, columns=pipeline.columns), tier_1_metric_df],
        ignore_index=True,
    )
    tier_1_metric_df = tier_1_metric_df.astype(
        {
            "monitoring_metric_value": float,
            "monitoring_metric_id": int,
            "metric_value_numerator": int,
            "metric_value_denominator": int,
        }
    )
return tier_1_metric_df

