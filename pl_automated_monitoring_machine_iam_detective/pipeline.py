import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import pandas as pd
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env

# Control configurations for Machine IAM Detective controls
CONTROL_CONFIGS = [
    {
        "cloud_control_id": "AC-3.AWS.39.v02",
        "ctrl_id": "CTRL-1074653",
        "metric_ids": {
            "tier1": "MNTR-1074653-T1",
            "tier2": "MNTR-1074653-T2",
            "tier3": "MNTR-1074653-T3"
        },
        "requires_tier3": True,
        "has_grace_period": False
    },
    {
        "cloud_control_id": "AC-3.AWS.91.v01",
        "ctrl_id": "CTRL-1104500",
        "metric_ids": {
            "tier1": "MNTR-1104500-T1",
            "tier2": "MNTR-1104500-T2"
        },
        "requires_tier3": False,
        "has_grace_period": True,
        "grace_period_days": 10
    }
]


def run(env: Env, is_load: bool = True, dq_actions: bool = True):
    """Run the Machine IAM Detective control pipeline."""
    pipeline = PLAutomatedMonitoringMachineIamDetective(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return pipeline.run(load=is_load, dq_actions=dq_actions)


class PLAutomatedMonitoringMachineIamDetective(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        # API settings for fetching approved accounts
        self.api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/accounts"

    def _get_api_connector(self) -> OauthApi:
        """Standard OAuth API connector setup following Zach's pattern"""
        api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        
        return OauthApi(
            url=self.api_url,
            api_token=f"Bearer {api_token}"
        )

    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
        all_iam_roles: pd.DataFrame,
        evaluated_roles: pd.DataFrame,
        sla_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Core business logic for Machine IAM Detective controls (multi-control support)
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
            all_iam_roles: DataFrame containing all IAM roles
            evaluated_roles: DataFrame containing evaluated roles for compliance
            sla_data: DataFrame containing SLA tracking data for non-compliant resources
            
        Returns:
            DataFrame with standardized output schema
        """
        
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError(
                "No threshold data found. Cannot proceed with metrics calculation."
            )
        
        # Step 2: Get approved accounts from API
        api_connector = self._get_api_connector()
        approved_accounts = self._get_approved_accounts(api_connector)
        
        # Step 3: Filter roles to approved accounts and machine type
        filtered_roles = all_iam_roles[
            (all_iam_roles['ACCOUNT'].isin(approved_accounts)) &
            (all_iam_roles['ROLE_TYPE'] == 'MACHINE')
        ]
        
        # Step 4: Group thresholds by control_id for multi-control processing
        control_groups = thresholds_raw.groupby('control_id')
        all_results = []
        now = datetime.now()
        
        for ctrl_id, control_thresholds in control_groups:
            # Find the control configuration
            control_config = next((config for config in CONTROL_CONFIGS if config["ctrl_id"] == ctrl_id), None)
            if not control_config:
                continue
            
            # Filter evaluated roles for this specific control
            control_evaluated_roles = evaluated_roles[
                evaluated_roles['CONTROL_ID'] == control_config["cloud_control_id"]
            ]
            
            # Process each threshold for this control
            for _, threshold in control_thresholds.iterrows():
                metric_id = threshold["monitoring_metric_id"]
                tier = threshold.get("monitoring_metric_tier", "")
                
                if "Tier 1" in tier:
                    # Tier 1: Coverage - percentage of roles evaluated
                    metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier1_metrics(
                        filtered_roles, control_evaluated_roles
                    )
                elif "Tier 2" in tier:
                    # Tier 2: Compliance - percentage of roles compliant
                    # Special handling for CTRL-1104500 with 10-day grace period
                    if control_config.get("has_grace_period", False):
                        metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier2_metrics_with_grace_period(
                            filtered_roles, control_evaluated_roles, evaluated_roles, control_config["grace_period_days"]
                        )
                    else:
                        metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier2_metrics(
                            filtered_roles, control_evaluated_roles
                        )
                elif "Tier 3" in tier and control_config.get("requires_tier3", False):
                    # Tier 3: SLA compliance - percentage of non-compliant roles within SLA
                    metric_value, compliant_count, total_count, non_compliant_resources = self._calculate_tier3_metrics(
                        filtered_roles, control_evaluated_roles, sla_data
                    )
                else:
                    # Default case or unsupported tier
                    metric_value = 0.0
                    compliant_count = 0
                    total_count = 0
                    non_compliant_resources = None
                
                # Determine compliance status
                alert_threshold = threshold.get("alerting_threshold", 95.0)
                warning_threshold = threshold.get("warning_threshold")
                
                if warning_threshold is not None and metric_value >= warning_threshold:
                    compliance_status = "Green"
                elif metric_value >= alert_threshold:
                    compliance_status = "Yellow"
                else:
                    compliance_status = "Red"
                
                # Format output with standard fields
                result = {
                    "control_monitoring_utc_timestamp": now,
                    "control_id": ctrl_id,
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

    def _get_approved_accounts(self, api_connector: OauthApi) -> List[str]:
        """Fetch approved AWS accounts from API"""
        try:
            headers = {
                'X-Cloud-Accounts-Business-Application': 'BACyberProcessAutomation',
                'Authorization': api_connector.api_token,
                'Accept': 'application/json;v=2.0',
                'Content-Type': 'application/json'
            }
            
            params = {
                'accountStatus': 'Active',
                'region': ['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1', 'eu-west-2', 'ca-central-1']
            }
            
            response = api_connector.send_request(
                url=api_connector.url,
                request_type="get",
                request_kwargs={
                    "headers": headers,
                    "params": params,
                    "verify": C1_CERT_FILE,
                    "timeout": 120,
                },
                retry_delay=5,
                retry_count=3,
            )
            
            if response.status_code != 200:
                raise RuntimeError(
                    f"API request failed: {response.status_code} - {response.text}"
                )
            
            data = response.json()
            account_numbers = [acc['accountNumber'] for acc in data['accounts'] 
                              if acc.get('accountNumber') and acc['accountNumber'].strip()]
            
            if not account_numbers:
                raise ValueError("No valid account numbers received from API")
            
            return account_numbers
            
        except Exception as e:
            raise RuntimeError(f"Failed to fetch approved accounts: {str(e)}")

    def _calculate_tier1_metrics(self, filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame):
        """Calculate Tier 1 (Coverage) metrics - percentage of roles evaluated"""
        metric_value = 0.0
        compliant_count = 0
        total_count = len(filtered_roles)
        non_compliant_resources = None
        
        if total_count > 0:
            # Get role ARNs that were evaluated
            evaluated_arns = set(evaluated_roles['RESOURCE_NAME'].str.upper()) if not evaluated_roles.empty else set()
            filtered_arns = set(filtered_roles['AMAZON_RESOURCE_NAME'].str.upper())
            
            # Count how many filtered roles were evaluated
            evaluated_count = len(filtered_arns.intersection(evaluated_arns))
            compliant_count = evaluated_count
            
            metric_value = round((evaluated_count / total_count * 100), 2)
            
            # Report unevaluated roles if any
            if evaluated_count < total_count:
                unevaluated_arns = filtered_arns - evaluated_arns
                unevaluated_list = sorted(list(unevaluated_arns))[:50]  # Limit to 50
                non_compliant_resources = [json.dumps({
                    "arn": arn,
                    "issue": "Role not evaluated"
                }) for arn in unevaluated_list]
                
                if len(unevaluated_arns) > 50:
                    non_compliant_resources.append(json.dumps({
                        "message": f"... and {len(unevaluated_arns) - 50} more unevaluated roles"
                    }))
        else:
            non_compliant_resources = [json.dumps({"issue": "No machine roles found in approved accounts"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _calculate_tier2_metrics(self, filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame):
        """Calculate Tier 2 (Compliance) metrics - percentage of roles compliant"""
        metric_value = 0.0
        compliant_count = 0
        total_count = len(filtered_roles)
        non_compliant_resources = None
        
        if total_count > 0 and not evaluated_roles.empty:
            # Get role ARNs and their compliance status
            role_compliance = {}
            for _, role in evaluated_roles.iterrows():
                arn = role['RESOURCE_NAME'].upper()
                status = role['COMPLIANCE_STATUS']
                role_compliance[arn] = status
            
            # Check compliance for all filtered roles
            non_compliant_roles = []
            for _, role in filtered_roles.iterrows():
                arn = role['AMAZON_RESOURCE_NAME'].upper()
                compliance_status = role_compliance.get(arn, 'NotEvaluated')
                
                if compliance_status in ['Compliant', 'CompliantControlAllowance']:
                    compliant_count += 1
                else:
                    non_compliant_roles.append({
                        "arn": arn,
                        "account": role['ACCOUNT'],
                        "compliance_status": compliance_status
                    })
            
            metric_value = round((compliant_count / total_count * 100), 2)
            
            # Report non-compliant roles
            if non_compliant_roles:
                limited_list = non_compliant_roles[:50]  # Limit to 50
                non_compliant_resources = [json.dumps(role) for role in limited_list]
                
                if len(non_compliant_roles) > 50:
                    non_compliant_resources.append(json.dumps({
                        "message": f"... and {len(non_compliant_roles) - 50} more non-compliant roles"
                    }))
        else:
            non_compliant_resources = [json.dumps({"issue": "No machine roles or evaluation data available"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _calculate_tier2_metrics_with_grace_period(self, filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame, all_evaluations: pd.DataFrame, grace_period_days: int):
        """
        Calculate Tier 2 metrics with grace period for CTRL-1104500
        Resources are considered compliant until they've been non-compliant for grace_period_days consecutive days
        """
        metric_value = 0.0
        compliant_count = 0
        total_count = len(filtered_roles)
        non_compliant_resources = None
        
        if total_count > 0 and not evaluated_roles.empty:
            # Get role ARNs and their compliance status
            role_compliance = {}
            for _, role in evaluated_roles.iterrows():
                arn = role['RESOURCE_NAME'].upper()
                status = role['COMPLIANCE_STATUS']
                role_compliance[arn] = status
            
            # Check compliance for all filtered roles with grace period logic
            non_compliant_roles = []
            grace_cutoff_date = datetime.now() - timedelta(days=grace_period_days)
            
            for _, role in filtered_roles.iterrows():
                arn = role['AMAZON_RESOURCE_NAME'].upper()
                current_compliance = role_compliance.get(arn, 'NotEvaluated')
                
                # If currently compliant, count as compliant
                if current_compliance in ['Compliant', 'CompliantControlAllowance']:
                    compliant_count += 1
                else:
                    # Check if this resource has been non-compliant for more than grace period
                    is_within_grace_period = self._check_grace_period(arn, all_evaluations, grace_cutoff_date)
                    
                    if is_within_grace_period:
                        # Still within grace period, count as compliant
                        compliant_count += 1
                    else:
                        # Beyond grace period, count as non-compliant
                        non_compliant_roles.append({
                            "arn": arn,
                            "account": role['ACCOUNT'],
                            "compliance_status": current_compliance,
                            "days_non_compliant": self._calculate_days_non_compliant(arn, all_evaluations)
                        })
            
            metric_value = round((compliant_count / total_count * 100), 2)
            
            # Report non-compliant roles (beyond grace period)
            if non_compliant_roles:
                limited_list = non_compliant_roles[:50]  # Limit to 50
                non_compliant_resources = [json.dumps(role) for role in limited_list]
                
                if len(non_compliant_roles) > 50:
                    non_compliant_resources.append(json.dumps({
                        "message": f"... and {len(non_compliant_roles) - 50} more roles beyond {grace_period_days}-day grace period"
                    }))
        else:
            non_compliant_resources = [json.dumps({"issue": "No machine roles or evaluation data available"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    def _check_grace_period(self, arn: str, all_evaluations: pd.DataFrame, grace_cutoff_date: datetime) -> bool:
        """
        Check if a resource is still within its grace period
        Returns True if the resource became non-compliant after the grace cutoff date
        """
        try:
            # Filter evaluations for this specific resource and control
            resource_evals = all_evaluations[
                (all_evaluations['RESOURCE_NAME'].str.upper() == arn) &
                (all_evaluations['CONTROL_ID'] == 'AC-3.AWS.91.v01')
            ].sort_values('EVALUATION_DATE', ascending=False)
            
            if resource_evals.empty:
                return True  # No evaluation history, assume within grace period
            
            # Find the first non-compliant evaluation
            for _, eval_row in resource_evals.iterrows():
                if eval_row['COMPLIANCE_STATUS'] not in ['Compliant', 'CompliantControlAllowance']:
                    eval_date = pd.to_datetime(eval_row['EVALUATION_DATE'])
                    # If first non-compliant date is after grace cutoff, still within grace period
                    return eval_date > grace_cutoff_date
            
            return True  # All evaluations are compliant
        except Exception:
            # If any error in parsing dates or data, assume within grace period
            return True

    def _calculate_days_non_compliant(self, arn: str, all_evaluations: pd.DataFrame) -> int:
        """Calculate how many consecutive days a resource has been non-compliant"""
        try:
            # Filter evaluations for this specific resource and control
            resource_evals = all_evaluations[
                (all_evaluations['RESOURCE_NAME'].str.upper() == arn) &
                (all_evaluations['CONTROL_ID'] == 'AC-3.AWS.91.v01')
            ].sort_values('EVALUATION_DATE', ascending=False)
            
            if resource_evals.empty:
                return 0
            
            # Find the first non-compliant evaluation
            first_non_compliant_date = None
            for _, eval_row in resource_evals.iterrows():
                if eval_row['COMPLIANCE_STATUS'] not in ['Compliant', 'CompliantControlAllowance']:
                    first_non_compliant_date = pd.to_datetime(eval_row['EVALUATION_DATE'])
                else:
                    break  # Found a compliant evaluation, stop looking
            
            if first_non_compliant_date:
                days_diff = (datetime.now() - first_non_compliant_date).days
                return max(0, days_diff)
            
            return 0
        except Exception:
            return 0

    def _calculate_tier3_metrics(self, filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame, sla_data: pd.DataFrame):
        """Calculate Tier 3 (SLA) metrics - percentage of non-compliant roles within SLA"""
        metric_value = 0.0
        compliant_count = 0
        total_count = 0
        non_compliant_resources = None
        
        if not evaluated_roles.empty:
            # Get non-compliant roles
            non_compliant_roles = evaluated_roles[
                evaluated_roles['COMPLIANCE_STATUS'] == 'NonCompliant'
            ]
            
            total_non_compliant = len(non_compliant_roles)
            total_count = total_non_compliant
            
            if total_non_compliant <= 0:
                # If no non-compliant roles, assume perfect compliance
                metric_value = 100.0
                compliant_count = 0
                non_compliant_resources = [json.dumps({"message": "All evaluated roles are compliant"})]
            else:
                # Define SLA time limits by risk level (days before overdue)
                sla_thresholds = {"Critical": 0, "High": 30, "Medium": 60, "Low": 90}
                current_date = datetime.now()
                
                # Process SLA data for non-compliant roles
                sla_data_map = {}
                if not sla_data.empty:
                    for _, sla_row in sla_data.iterrows():
                        resource_id = sla_row['RESOURCE_ID']
                        control_risk = sla_row['CONTROL_RISK']
                        open_date = sla_row['OPEN_DATE_UTC_TIMESTAMP']
                        
                        if open_date and control_risk in sla_thresholds:
                            days_open = (current_date - open_date).days
                            sla_limit = sla_thresholds.get(control_risk, 90)
                            sla_status = "Past SLA" if days_open > sla_limit else "Within SLA"
                        else:
                            days_open = None
                            sla_limit = None
                            sla_status = "Unknown"
                        
                        sla_data_map[resource_id] = {
                            "control_risk": control_risk,
                            "open_date": open_date,
                            "days_open": days_open,
                            "sla_limit": sla_limit,
                            "sla_status": sla_status
                        }
                
                # Calculate SLA compliance
                past_sla_count = 0
                evidence_rows = []
                
                for _, role in non_compliant_roles.iterrows():
                    resource_name = role['RESOURCE_NAME']
                    # Try to find matching SLA data by resource name or ID
                    sla_info = sla_data_map.get(resource_name, {
                        "control_risk": "Unknown",
                        "open_date": None,
                        "days_open": None,
                        "sla_limit": None,
                        "sla_status": "Unknown"
                    })
                    
                    if sla_info["sla_status"] == "Past SLA":
                        past_sla_count += 1
                    
                    evidence_rows.append({
                        "resource_name": resource_name,
                        "compliance_status": "NonCompliant",
                        "control_risk": sla_info["control_risk"],
                        "days_open": str(sla_info["days_open"]) if sla_info["days_open"] else "Unknown",
                        "sla_status": sla_info["sla_status"]
                    })
                
                # Calculate metric: percentage within SLA (higher is better)
                within_sla_count = total_non_compliant - past_sla_count
                compliant_count = within_sla_count
                metric_value = round((within_sla_count / total_non_compliant * 100), 2) if total_non_compliant > 0 else 0.0
                
                # Build evidence with SLA details
                if evidence_rows:
                    limited_evidence = evidence_rows[:50]  # Limit to 50
                    non_compliant_resources = [json.dumps(row) for row in limited_evidence]
                    
                    if len(evidence_rows) > 50:
                        non_compliant_resources.append(json.dumps({
                            "message": f"... and {len(evidence_rows) - 50} more non-compliant roles"
                        }))
        else:
            non_compliant_resources = [json.dumps({"issue": "No evaluation data available for Tier 3 calculation"})]
        
        return metric_value, compliant_count, total_count, non_compliant_resources

    # This is the extract portion for the API
    def extract(self) -> pd.DataFrame:
        df = super().extract()
        # Wrap the DataFrame in a list to store it as a single value in the cell
        df["monitoring_metrics"] = [self._calculate_metrics(
            df["thresholds_raw"].iloc[0],
            df["all_iam_roles"].iloc[0],
            df["evaluated_roles"].iloc[0],
            df["sla_data"].iloc[0]
        )]
        return df


if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception:
        import sys
        sys.exit(1)