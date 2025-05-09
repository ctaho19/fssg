import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
from config_pipeline import ConfigPipeline
from etip_env import Env
from connectors.exchange.oauth_token import refresh
from transform_library import transformer

logger = logging.getLogger(__name__)

# --- CONTROL CONFIGS ---
# Defines the list of controls and their associated metric IDs that this pipeline will process.
CONTROL_CONFIGS = [
    {
        "cloud_control_id": "AC-3.AWS.39.v02", # Cloud Control ID used in source systems (e.g., for violations)
        "ctrl_id": "CTRL-1074653",             # FUSE Control ID used for reporting and threshold mapping
        "metric_ids": {
            "tier1": 1,                        # Integer ID for Tier 1 metric
            "tier2": 2,                        # Integer ID for Tier 2 metric
            "tier3": 3,                        # Integer ID for Tier 3 metric
        },
        "requires_tier3": True                 # Flag indicating if Tier 3 (SLA) calculation is needed
    },
    {
        "cloud_control_id": "AC-6.AWS.13.v01",
        "ctrl_id": "CTRL-1105806",
        "metric_ids": {
            "tier1": 4,
            "tier2": 5,
        },
        "requires_tier3": False,
    },
    {
        "cloud_control_id": "AC-6.AWS.35.v02",
        "ctrl_id": "CTRL-1077124",
        "metric_ids": {
            "tier1": 6,
            "tier2": 7,
        },
        "requires_tier3": False,
    },
]

# --- SQL HELPERS ---
def read_sql_file(filename: str) -> str:
    """Reads a SQL query from a file in the 'sql' subdirectory."""
    sql_path = Path(__file__).parent / "sql" / filename
    with open(sql_path, "r") as f:
        return f.read()

def parameterize_sql(sql: str, params: Dict[str, Any]) -> str:
    """Substitutes parameters into a SQL query using pyformat style (%(key)s)."""
    # Only supports %(param)s style
    for k, v in params.items():
        sql = sql.replace(f"%({k})s", f"'{v}'") # Basic replacement, assumes string values need quotes
    return sql

# --- PIPELINE CLASS ---
class PLAutomatedMonitoringMachineIAM(ConfigPipeline):
    """
    Pipeline class for calculating Machine IAM control monitoring metrics.
    Inherits from ConfigPipeline to leverage standard ETL stages (extract, transform, load).
    """
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        self.snowflake = getattr(env, "snowflake", None) # Get Snowflake connector from env
        try:
            self.client_id = self.env.exchange.client_id
            self.client_secret = self.env.exchange.client_secret
            self.exchange_url = self.env.exchange.exchange_url
        except AttributeError as e:
            logger.error(f"Missing OAuth configuration in environment: {e}")
            raise ValueError(f"Environment object missing expected OAuth attributes: {e}") from e

    def _get_api_token(self) -> str:
        """Refreshes the OAuth token for API calls using Exchange credentials."""
        logger.info(f"Refreshing API token from {self.exchange_url}...")
        try:
            token = refresh(
                client_id=self.client_id,
                client_secret=self.client_secret,
                exchange_url=self.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            raise RuntimeError("API token refresh failed") from e

    def _execute_sql(self, sql: str) -> pd.DataFrame:
        """Executes a SQL query using the configured Snowflake connector."""
        # This should use the configured Snowflake connector from the environment
        # In real code, replace with actual DB call mechanism provided by ConfigPipeline or env
        if hasattr(self.snowflake, "query"):
            logger.debug(f"Executing SQL query: {sql[:200]}...")
            return self.snowflake.query(sql)
        else:
            logger.error("Snowflake query method not found in env.snowflake object.")
            raise NotImplementedError("Snowflake query method not implemented in env.snowflake")

    def transform(self) -> None:
        """
        Main transformation stage.
        Iterates through CONTROL_CONFIGS, extracts data for each control, runs the
        transformer function, and aggregates results into the 'monitoring_metrics' context.
        """
        logger.info("Preparing transform stage for Machine IAM pipeline...")
        all_results = []
        for control in CONTROL_CONFIGS:
            ctrl_id = control["ctrl_id"]
            cloud_control_id = control["cloud_control_id"]
            metric_ids = control["metric_ids"]
            requires_tier3 = control.get("requires_tier3", False)
            logger.info(f"Processing control: {ctrl_id} ({cloud_control_id})")
            # --- Extract ---
            # Fetch data required for this specific control
            try:
                # Thresholds
                logger.debug(f"Fetching thresholds for {ctrl_id}...")
                sql_thresholds = read_sql_file("monitoring_thresholds.sql")
                sql_thresholds = parameterize_sql(sql_thresholds, {"control_id": ctrl_id})
                thresholds_raw = self._execute_sql(sql_thresholds)
                # IAM Roles (assuming this doesn't need per-control filtering)
                logger.debug("Fetching IAM roles...")
                sql_iam_roles = read_sql_file("iam_roles.sql")
                iam_roles = self._execute_sql(sql_iam_roles)
                # Evaluated Roles (specific to this control)
                logger.debug(f"Fetching evaluated roles for {cloud_control_id}...")
                sql_evaluated_roles = read_sql_file("evaluated_roles.sql")
                sql_evaluated_roles = parameterize_sql(sql_evaluated_roles, {"control_id": cloud_control_id})
                evaluated_roles = self._execute_sql(sql_evaluated_roles)
                # --- Tier 3 SLA Data (if needed) ---
                sla_data = None
                if requires_tier3 and "tier3" in metric_ids:
                    logger.debug(f"Fetching SLA data for {cloud_control_id}...")
                    non_compliant_resource_ids = []
                    # Find non-compliant resources after Tier 2 join
                    combined = pd.merge(
                        iam_roles,
                        evaluated_roles,
                        left_on="AMAZON_RESOURCE_NAME",
                        right_on="resource_name",
                        how="left",
                    )
                    non_compliant = combined[combined["compliance_status"] == "NonCompliant"]
                    non_compliant_resource_ids = non_compliant["RESOURCE_ID"].tolist()
                    if non_compliant_resource_ids:
                        # Read and parameterize SQL for SLA data
                        sla_sql_template = read_sql_file("sla_data.sql")
                        # Ensure resource IDs are properly quoted for IN clause
                        resource_id_list = ",".join([f"'{rid}'" for rid in non_compliant_resource_ids])
                        sla_sql = parameterize_sql(sla_sql_template, {"control_id": cloud_control_id, "resource_id_list": resource_id_list})
                        sla_data = self._execute_sql(sla_sql)
                    else:
                        logger.debug("No non-compliant resources found, skipping SLA data fetch.")
                        sla_data = pd.DataFrame(columns=["RESOURCE_ID", "CONTROL_RISK", "OPEN_DATE_UTC_TIMESTAMP"])
            except Exception as e:
                logger.error(f"Failed to extract data for control {ctrl_id}: {e}", exc_info=True)
                continue # Skip this control if data extraction fails
            # --- Transform ---
            # Pass extracted data to the transformer function
            try:
                logger.debug(f"Calculating metrics for {ctrl_id}...")
                metrics_df = calculate_machine_iam_metrics(
                    thresholds_raw=thresholds_raw,
                    iam_roles=iam_roles,
                    evaluated_roles=evaluated_roles,
                    ctrl_id=ctrl_id,
                    metric_ids=metric_ids,
                    requires_tier3=requires_tier3,
                    sla_data=sla_data,
                )
                all_results.append(metrics_df)
            except Exception as e:
                logger.error(f"Failed to calculate metrics for control {ctrl_id}: {e}", exc_info=True)
                continue # Skip this control if calculation fails
        # Concatenate all results from different controls
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            self.context["monitoring_metrics"] = final_df
            logger.info(f"Machine IAM pipeline transform complete. Generated {len(final_df)} metric records.")
        else:
            logger.warning("No metric records generated for any control.")
            # Ensure the context has the expected DataFrame structure even if empty
            self.context["monitoring_metrics"] = pd.DataFrame(columns=[
                "date", "control_id", "monitoring_metric_id", "monitoring_metric_value",
                "compliance_status", "numerator", "denominator", "non_compliant_resources"
            ])
        super().transform()

# --- TRANSFORMER ---
@transformer
def calculate_machine_iam_metrics(
    thresholds_raw: pd.DataFrame,
    iam_roles: pd.DataFrame,
    evaluated_roles: pd.DataFrame,
    ctrl_id: str,
    metric_ids: Dict[str, int],
    requires_tier3: bool = False,
    sla_data: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Calculates Tier 1, 2, and optionally 3 metrics for a single control.
    Input DataFrames are assumed to be filtered for the specific control where necessary.
    Output DataFrame matches the avro schema.
    """
    now = int(datetime.utcnow().timestamp() * 1000)
    results = []
    # --- Tier 1: % of machine roles evaluated ---
    logger.debug(f"Calculating Tier 1 for {ctrl_id}...")
    total_roles = len(iam_roles)
    evaluated = pd.merge(
        iam_roles,
        evaluated_roles,
        left_on="AMAZON_RESOURCE_NAME",
        right_on="resource_name", # Corrected column name
        how="inner",
    )
    evaluated_count = len(evaluated)
    # Find the specific threshold row for Tier 1
    t1_threshold = thresholds_raw[thresholds_raw["monitoring_metric_id"] == metric_ids["tier1"]]
    if t1_threshold.empty:
        raise ValueError(f"Missing Tier 1 threshold for metric ID {metric_ids['tier1']} in control {ctrl_id}")
    t1_threshold = t1_threshold.iloc[0]
    alert = t1_threshold["alerting_threshold"]
    warning = t1_threshold["warning_threshold"]
    # Calculate metric value
    metric = evaluated_count / total_roles * 100 if total_roles > 0 else 100.0
    metric = round(metric, 2)
    # Determine compliance status
    if alert is not None:
        if metric < alert:
            status = "Red"
        elif warning is not None and metric < warning:
            status = "Yellow"
        else:
            status = "Green"
    else:
        status = "Unknown"
        logger.warning(f"Tier 1 ({metric_ids['tier1']}) for {ctrl_id} has no alert threshold, status is Unknown.")
    # Generate evidence for non-evaluated roles
    t1_non_compliant_df = iam_roles[~iam_roles["AMAZON_RESOURCE_NAME"].isin(evaluated_roles["resource_name"] if not evaluated_roles.empty else [])]
    t1_non_compliant = [json.dumps(row.to_dict()) for _, row in t1_non_compliant_df.iterrows()] if status != "Green" and not t1_non_compliant_df.empty else None
    results.append({
        "date": now,
        "control_id": ctrl_id,
        "monitoring_metric_id": metric_ids["tier1"],
        "monitoring_metric_value": metric,
        "compliance_status": status,
        "numerator": int(evaluated_count),
        "denominator": int(total_roles),
        "non_compliant_resources": t1_non_compliant,
    })
    logger.debug(f"Tier 1 for {ctrl_id}: {metric}% ({status}) [{evaluated_count}/{total_roles}]")
    # --- Tier 2: % of machine roles compliant ---
    logger.debug(f"Calculating Tier 2 for {ctrl_id}...")
    combined = pd.merge(
        iam_roles,
        evaluated_roles,
        left_on="AMAZON_RESOURCE_NAME",
        right_on="resource_name", # Corrected column name
        how="left", # Keep all IAM roles
    )
    # Count compliant roles (handle potential NAs from left join if a role wasn't evaluated)
    compliant_roles = combined[combined["compliance_status"].isin(["Compliant", "CompliantControlAllowance"])]
    compliant_count = len(compliant_roles)
    # Find the specific threshold row for Tier 2
    t2_threshold = thresholds_raw[thresholds_raw["monitoring_metric_id"] == metric_ids["tier2"]]
    if t2_threshold.empty:
        raise ValueError(f"Missing Tier 2 threshold for metric ID {metric_ids['tier2']} in control {ctrl_id}")
    t2_threshold = t2_threshold.iloc[0]
    alert = t2_threshold["alerting_threshold"]
    warning = t2_threshold["warning_threshold"]
    # Calculate metric value
    metric = compliant_count / total_roles * 100 if total_roles > 0 else 100.0
    metric = round(metric, 2)
    # Determine compliance status
    if alert is not None:
        if metric < alert:
            status = "Red"
        elif warning is not None and metric < warning:
            status = "Yellow"
        else:
            status = "Green"
    else:
        status = "Unknown"
        logger.warning(f"Tier 2 ({metric_ids['tier2']}) for {ctrl_id} has no alert threshold, status is Unknown.")
    # Generate evidence for non-compliant roles
    t2_non_compliant_df = combined[~combined["compliance_status"].isin(["Compliant", "CompliantControlAllowance"])]
    t2_non_compliant = [json.dumps(row.to_dict()) for _, row in t2_non_compliant_df.iterrows()] if status != "Green" and not t2_non_compliant_df.empty else None
    results.append({
        "date": now,
        "control_id": ctrl_id,
        "monitoring_metric_id": metric_ids["tier2"],
        "monitoring_metric_value": metric,
        "compliance_status": status,
        "numerator": int(compliant_count),
        "denominator": int(total_roles),
        "non_compliant_resources": t2_non_compliant,
    })
    logger.debug(f"Tier 2 for {ctrl_id}: {metric}% ({status}) [{compliant_count}/{total_roles}]")
    # --- Tier 3: SLA compliance (optional) ---
    if requires_tier3 and "tier3" in metric_ids:
        logger.debug(f"Calculating Tier 3 for {ctrl_id}...")
        non_compliant = combined[combined["compliance_status"] == "NonCompliant"]
        total_non_compliant = len(non_compliant)
        if total_non_compliant == 0:
            # If no non-compliant roles, SLA is 100% Green
            metric = 100.0
            status = "Green"
            numerator = 0
            denominator = 0
            t3_non_compliant = None
            logger.debug(f"Tier 3 for {ctrl_id}: No non-compliant resources.")
        else:
            # Calculate SLA compliance for the non-compliant roles
            if sla_data is not None and not sla_data.empty:
                merged = pd.merge(
                    non_compliant,
                    sla_data,
                    left_on="RESOURCE_ID",
                    right_on="RESOURCE_ID",
                    how="left", # Keep all non-compliant roles
                )
                sla_thresholds = {"Critical": 0, "High": 30, "Medium": 60, "Low": 90}
                now_dt = pd.Timestamp.utcnow()
                within_sla = 0
                evidence_rows = []
                # Calculate SLA status for each non-compliant role
                for _, row in merged.iterrows():
                    control_risk = row.get("CONTROL_RISK", "Low") # Default risk if not found
                    open_date_str = row.get("OPEN_DATE_UTC_TIMESTAMP")
                    open_date = pd.to_datetime(open_date_str) if pd.notnull(open_date_str) else None
                    sla_limit = sla_thresholds.get(control_risk, 90)
                    if open_date is not None:
                        days_open = (now_dt - open_date).days
                        sla_status = "Within SLA" if days_open <= sla_limit else "Past SLA"
                        if days_open <= sla_limit:
                            within_sla += 1
                    else:
                        days_open = None
                        sla_status = "Unknown" # SLA status unknown if open date is missing
                    evidence_rows.append({
                        "RESOURCE_ID": row["RESOURCE_ID"],
                        "CONTROL_RISK": control_risk,
                        "OPEN_DATE": str(open_date) if open_date else None,
                        "DAYS_OPEN": days_open,
                        "SLA_LIMIT": sla_limit,
                        "SLA_STATUS": sla_status,
                    })
                # Calculate Tier 3 metric (% within SLA)
                metric = within_sla / total_non_compliant * 100 if total_non_compliant > 0 else 100.0
                metric = round(metric, 2)
                numerator = within_sla
                denominator = total_non_compliant
                # Generate evidence only for roles Past SLA or Unknown SLA status
                t3_non_compliant = [json.dumps(ev) for ev in evidence_rows if ev["SLA_STATUS"] != "Within SLA"] if status != "Green" and any(ev["SLA_STATUS"] != "Within SLA" for ev in evidence_rows) else None
            else:
                # If SLA data is missing, assume 0% compliance for Tier 3
                logger.warning(f"Missing or empty SLA data for {ctrl_id}, setting Tier 3 metric to 0% Red.")
                metric = 0.0
                status = "Red"
                numerator = 0
                denominator = total_non_compliant
                # Evidence includes all non-compliant roles without SLA info
                t3_non_compliant = [json.dumps(row.to_dict()) for _, row in non_compliant.iterrows()] if not non_compliant.empty else None
            # Find the specific threshold row for Tier 3
            t3_threshold = thresholds_raw[thresholds_raw["monitoring_metric_id"] == metric_ids["tier3"]]
            if t3_threshold.empty:
                 raise ValueError(f"Missing Tier 3 threshold for metric ID {metric_ids['tier3']} in control {ctrl_id}")
            t3_threshold = t3_threshold.iloc[0]
            alert = t3_threshold["alerting_threshold"]
            warning = t3_threshold["warning_threshold"]
            # Determine compliance status based on calculated metric and thresholds
            if alert is not None:
                if metric < alert:
                    status = "Red"
                elif warning is not None and metric < warning:
                    status = "Yellow"
                else:
                    status = "Green"
            else:
                status = "Unknown"
                logger.warning(f"Tier 3 ({metric_ids['tier3']}) for {ctrl_id} has no alert threshold, status is Unknown.")
            results.append({
                "date": now,
                "control_id": ctrl_id,
                "monitoring_metric_id": metric_ids["tier3"],
                "monitoring_metric_value": metric,
                "compliance_status": status,
                "numerator": int(numerator),
                "denominator": int(denominator),
                "non_compliant_resources": t3_non_compliant,
            })
            logger.debug(f"Tier 3 for {ctrl_id}: {metric}% ({status}) [{numerator}/{denominator}]")
    # Return results for all tiers for this control
    return pd.DataFrame(results)

# --- Entrypoint for the pipeline ---
# This function is typically called by the ETIP framework to run the pipeline.
def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    """Instantiates and runs the Machine IAM pipeline."""
    pipeline = PLAutomatedMonitoringMachineIAM(env)
    # Load configuration from the associated config.yml file
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    # Execute the pipeline run (or test data export if specified)
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )
