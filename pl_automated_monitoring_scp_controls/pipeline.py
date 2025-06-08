import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import pandas as pd
from config_pipeline import ConfigPipeline
from etip_env import Env

# Control configuration mapping for SCP controls
CONTROL_CONFIGS = [
    {
        "cloud_control_id": "AC-3.AWS.146.v02",
        "ctrl_id": "CTRL-1077770",
        "metric_ids": {
            "tier1": "MNTR-1077770-T1",
            "tier2": "MNTR-1077770-T2"
        }
    },
    {
        "cloud_control_id": "CM-2.AWS.20.v02",
        "ctrl_id": "CTRL-1079538",
        "metric_ids": {
            "tier1": "MNTR-1079538-T1",
            "tier2": "MNTR-1079538-T2"
        }
    },
    {
        "cloud_control_id": "AC-3.AWS.123.v01",
        "ctrl_id": "CTRL-1081889",
        "metric_ids": {
            "tier1": "MNTR-1081889-T1",
            "tier2": "MNTR-1081889-T2"
        }
    },
    {
        "cloud_control_id": "CM-2.AWS.4914.v01",
        "ctrl_id": "CTRL-1102567",
        "metric_ids": {
            "tier1": "MNTR-1102567-T1",
            "tier2": "MNTR-1102567-T2"
        }
    },
    {
        "cloud_control_id": "SC-7.AWS.6501.v01",
        "ctrl_id": "CTRL-1105846",
        "metric_ids": {
            "tier1": "MNTR-1105846-T1",
            "tier2": "MNTR-1105846-T2"
        }
    },
    {
        "cloud_control_id": "AC-3.AWS.5771.v01",
        "ctrl_id": "CTRL-1105996",
        "metric_ids": {
            "tier1": "MNTR-1105996-T1",
            "tier2": "MNTR-1105996-T2"
        }
    },
    {
        "cloud_control_id": "AC-3.AWS.128.v01",
        "ctrl_id": "CTRL-1105997",
        "metric_ids": {
            "tier1": "MNTR-1105997-T1",
            "tier2": "MNTR-1105997-T2"
        }
    },
    {
        "cloud_control_id": "AC-3.AWS.117.v01",
        "ctrl_id": "CTRL-1106155",
        "metric_ids": {
            "tier1": "MNTR-1106155-T1",
            "tier2": "MNTR-1106155-T2"
        }
    },
    {
        "cloud_control_id": "AC-1.AWS.5099.v02",
        "ctrl_id": "CTRL-1103299",
        "metric_ids": {
            "tier1": "MNTR-1103299-T1",
            "tier2": "MNTR-1103299-T2"
        }
    },
    {
        "cloud_control_id": "IA-5.AWS.20.v02",
        "ctrl_id": "CTRL-1106425",
        "metric_ids": {
            "tier1": "MNTR-1106425-T1",
            "tier2": "MNTR-1106425-T2"
        }
    }
]


def run(env: Env, is_load: bool = True, dq_actions: bool = True):
    """Run the consolidated SCP controls pipeline."""
    pipeline = PLAutomatedMonitoringScpControls(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return pipeline.run(load=is_load, dq_actions=dq_actions)


class PLAutomatedMonitoringScpControls(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env

    def extract(self) -> pd.DataFrame:
        """Override extract to integrate SQL data"""
        # First get data from configured SQL sources
        df = super().extract()
        
        # Then calculate metrics using class methods
        # Wrap the DataFrame in a list to store it as a single value in the cell
        df["monitoring_metrics"] = [self._calculate_metrics(
            df["thresholds_raw"].iloc[0],
            df["in_scope_accounts"].iloc[0],
            df["evaluated_accounts"].iloc[0]
        )]
        return df

    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
        in_scope_accounts: pd.DataFrame,
        evaluated_accounts: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Core business logic for SCP controls
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            in_scope_accounts: DataFrame containing all active accounts
            evaluated_accounts: DataFrame containing evaluation results
            
        Returns:
            DataFrame with standardized output schema
        """
        
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError(
                "No threshold data found. Cannot proceed with metrics calculation."
            )
        
        if in_scope_accounts.empty:
            raise RuntimeError(
                "No in-scope accounts found. Cannot proceed with metrics calculation."
            )
        
        # Step 2: Get list of active accounts
        active_accounts = set(in_scope_accounts['ACCOUNT'].astype(str).str.strip())
        total_accounts = len(active_accounts)
        
        # Step 3: Process each control configuration
        all_results = []
        now = datetime.now()
        
        # Group thresholds by control_id
        control_groups = thresholds_raw.groupby('control_id')
        
        for control_id, control_thresholds in control_groups:
            # Find the matching control configuration
            control_config = next((config for config in CONTROL_CONFIGS if config["ctrl_id"] == control_id), None)
            if not control_config:
                continue
                
            cloud_control_id = control_config["cloud_control_id"]
            
            # Filter evaluated accounts for this specific control
            control_evaluations = evaluated_accounts[
                evaluated_accounts['CONTROL_ID'] == cloud_control_id
            ] if not evaluated_accounts.empty else pd.DataFrame()
            
            # Process each threshold for this control
            for _, threshold in control_thresholds.iterrows():
                metric_id = threshold["monitoring_metric_id"]
                tier = threshold.get("monitoring_metric_tier", "")
                
                if "Tier 1" in tier or "tier1" in metric_id.lower():
                    # Tier 1: Coverage - percentage of accounts evaluated
                    metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier1_metrics(
                        active_accounts, control_evaluations, cloud_control_id
                    )
                elif "Tier 2" in tier or "tier2" in metric_id.lower():
                    # Tier 2: Compliance - percentage of evaluated accounts compliant
                    metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier2_metrics(
                        active_accounts, control_evaluations, cloud_control_id
                    )
                else:
                    # Default case
                    metric_value = 0.0
                    compliant_count = 0
                    total_count = 0
                    non_compliant_resources = None
                
                # Determine compliance status
                alert_threshold = threshold.get("alerting_threshold", 95.0)
                warning_threshold = threshold.get("warning_threshold", 97.0)
                
                if warning_threshold is not None and metric_value >= warning_threshold:
                    compliance_status = "Green"
                elif metric_value >= alert_threshold:
                    compliance_status = "Yellow"
                else:
                    compliance_status = "Red"
                
                # Format output with standard fields
                result = {
                    "control_monitoring_utc_timestamp": now,
                    "control_id": control_id,
                    "monitoring_metric_id": metric_id,
                    "monitoring_metric_value": float(metric_value),
                    "monitoring_metric_status": compliance_status,
                    "metric_value_numerator": int(compliant_count),
                    "metric_value_denominator": int(total_count),
                    "resources_info": non_compliant_resources
                }
                all_results.append(result)
        
        result_df = pd.DataFrame(all_results)
        
        # Ensure correct data types to match test expectations
        if not result_df.empty:
            result_df = result_df.astype(
                {
                    "metric_value_numerator": "int64",
                    "metric_value_denominator": "int64",
                    "monitoring_metric_value": "float64",
                }
            )
        
        return result_df

    def _calculate_tier1_metrics(self, active_accounts: set, control_evaluations: pd.DataFrame, cloud_control_id: str):
        """Calculate Tier 1 (Coverage) metrics - percentage of accounts evaluated"""
        metric_value = 0.0
        compliant_count = 0
        total_count = len(active_accounts)
        non_compliant_resources = None
        
        if total_count > 0:
            # Get accounts that were evaluated for this control
            if not control_evaluations.empty:
                evaluated_accounts = set(control_evaluations['RESOURCE_ID'].astype(str).str.strip())
                evaluated_in_scope = active_accounts.intersection(evaluated_accounts)
                compliant_count = len(evaluated_in_scope)
            else:
                evaluated_in_scope = set()
                compliant_count = 0
            
            metric_value = round((compliant_count / total_count * 100), 2)
            
            # Report unevaluated accounts if any
            if compliant_count < total_count:
                unevaluated_accounts = active_accounts - evaluated_in_scope
                unevaluated_list = sorted(list(unevaluated_accounts))[:50]  # Limit to 50
                non_compliant_resources = [json.dumps({
                    "account": account,
                    "issue": f"Account not evaluated for {cloud_control_id}"
                }) for account in unevaluated_list]
                
                if len(unevaluated_accounts) > 50:
                    non_compliant_resources.append(json.dumps({
                        "message": f"... and {len(unevaluated_accounts) - 50} more unevaluated accounts"
                    }))
        else:
            non_compliant_resources = [json.dumps({"issue": "No active accounts found"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _calculate_tier2_metrics(self, active_accounts: set, control_evaluations: pd.DataFrame, cloud_control_id: str):
        """Calculate Tier 2 (Compliance) metrics - percentage of evaluated accounts compliant"""
        metric_value = 0.0
        compliant_count = 0
        total_count = 0
        non_compliant_resources = None
        
        if not control_evaluations.empty:
            # Filter evaluations to only include active accounts
            active_evaluations = control_evaluations[
                control_evaluations['RESOURCE_ID'].astype(str).str.strip().isin(active_accounts)
            ]
            
            total_count = len(active_evaluations)
            
            if total_count > 0:
                # Count compliant accounts
                compliant_evaluations = active_evaluations[
                    active_evaluations['COMPLIANCE_STATUS'].isin(['Compliant', 'CompliantControlAllowance'])
                ]
                compliant_count = len(compliant_evaluations)
                
                metric_value = round((compliant_count / total_count * 100), 2)
                
                # Report non-compliant accounts
                non_compliant_evaluations = active_evaluations[
                    ~active_evaluations['COMPLIANCE_STATUS'].isin(['Compliant', 'CompliantControlAllowance'])
                ]
                
                if len(non_compliant_evaluations) > 0:
                    non_compliant_list = []
                    for _, row in non_compliant_evaluations.iterrows():
                        non_compliant_list.append({
                            "account": str(row['RESOURCE_ID']),
                            "resource_type": row.get('RESOURCE_TYPE', 'Account'),
                            "compliance_status": row['COMPLIANCE_STATUS'],
                            "control_id": cloud_control_id
                        })
                    
                    limited_list = non_compliant_list[:50]  # Limit to 50
                    non_compliant_resources = [json.dumps(item) for item in limited_list]
                    
                    if len(non_compliant_list) > 50:
                        non_compliant_resources.append(json.dumps({
                            "message": f"... and {len(non_compliant_list) - 50} more non-compliant accounts"
                        }))
            else:
                non_compliant_resources = [json.dumps({"issue": f"No active accounts evaluated for {cloud_control_id}"})]
        else:
            non_compliant_resources = [json.dumps({"issue": f"No evaluation data available for {cloud_control_id}"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources


if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception:
        import sys
        sys.exit(1)