from typing import Dict, Any
import pandas as pd
from datetime import datetime
import json
import ssl
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env
from transform_library import transformer

# Control configuration mapping for Machine IAM Preventative controls
CONTROL_CONFIGS = [
    {
        "cloud_control_id": "AC-6.AWS.13.v01",
        "ctrl_id": "CTRL-1105806",
        "metric_ids": {
            "tier1": "MNTR-1105806-T1",
            "tier2": "MNTR-1105806-T2"
        },
        "requires_tier3": False
    },
    {
        "cloud_control_id": "AC-6.AWS.35.v02",
        "ctrl_id": "CTRL-1077124",
        "metric_ids": {
            "tier1": "MNTR-1077124-T1",
            "tier2": "MNTR-1077124-T2"
        },
        "requires_tier3": False
    }
]

def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringMachineIamPreventative(env)
    pipeline.configure_from_filename("config.yml")
    
    if is_export_test_data:
        return pipeline.run_test_data_export(dq_actions=dq_actions)
    else:
        return pipeline.run(load=is_load, dq_actions=dq_actions)

class PLAutomatedMonitoringMachineIamPreventative(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        # API settings for fetching approved accounts
        self.api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/accounts"
        self.api_headers = {
            'X-Cloud-Accounts-Business-Application': 'BACyberProcessAutomation',
            'Accept': 'application/json;v=2.0',
            'Content-Type': 'application/json'
        }
        self.api_params = {
            'accountStatus': 'Active',
            'region': ['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1', 'eu-west-2', 'ca-central-1']
        }
    
    def _get_api_connector(self) -> OauthApi:
        """Standard OAuth API connector setup following Zach's pattern"""
        api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        
        # Create SSL context if certificate file exists
        ssl_context = None
        if C1_CERT_FILE:
            ssl_context = ssl.create_default_context(cafile=C1_CERT_FILE)
        
        return OauthApi(
            url=self.api_url,
            api_token=f"Bearer {api_token}",
            ssl_context=ssl_context
        )
        
    def transform(self) -> None:
        """Override transform to set up API context"""
        self.context["api_connector"] = self._get_api_connector()
        super().transform()

@transformer
def calculate_metrics(thresholds_raw: pd.DataFrame, all_iam_roles: pd.DataFrame, evaluated_roles: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    """
    Core business logic transformer for Machine IAM Preventative controls
    
    Args:
        thresholds_raw: DataFrame containing metric thresholds from SQL query
        all_iam_roles: DataFrame containing all IAM roles
        evaluated_roles: DataFrame containing evaluated roles for compliance
        context: Pipeline context including API connector
        
    Returns:
        DataFrame with standardized output schema
    """
    
    # Step 1: Input Validation (REQUIRED)
    if thresholds_raw.empty:
        raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
    
    # Step 2: Get approved accounts from API
    api_connector = context["api_connector"]
    approved_accounts = _get_approved_accounts(api_connector)
    
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
        
        # Filter roles to approved accounts and machine type
        filtered_roles = all_iam_roles[
            (all_iam_roles['ACCOUNT'].isin(approved_accounts)) &
            (all_iam_roles['ROLE_TYPE'] == 'MACHINE')
        ]
        
        # Filter evaluated roles for this specific control
        control_evaluated_roles = evaluated_roles[
            evaluated_roles['CONTROL_ID'] == cloud_control_id
        ]
        
        # Process each threshold for this control
        for _, threshold in control_thresholds.iterrows():
            metric_id = threshold["monitoring_metric_id"]
            tier = threshold.get("monitoring_metric_tier", "")
            
            if "Tier 1" in tier or "tier1" in metric_id.lower():
                # Tier 1: Coverage - percentage of roles evaluated
                metric_value, compliant_count, total_count, non_compliant_resources = _calculate_tier1_metrics(
                    filtered_roles, control_evaluated_roles
                )
            elif "Tier 2" in tier or "tier2" in metric_id.lower():
                # Tier 2: Compliance - percentage of roles compliant
                metric_value, compliant_count, total_count, non_compliant_resources = _calculate_tier2_metrics(
                    filtered_roles, control_evaluated_roles
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
            
            if metric_value >= alert_threshold:
                compliance_status = "Green"
            elif warning_threshold is not None and metric_value >= warning_threshold:
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
    
    return pd.DataFrame(all_results)

def _get_approved_accounts(api_connector: OauthApi) -> list:
    """Fetch approved AWS accounts from API"""
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    
    try:
        # Use the API token from the connector
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
        
        response = session.get(
            "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/accounts",
            headers=headers,
            params=params,
            verify=False
        )
        response.raise_for_status()
        
        data = response.json()
        account_numbers = [acc['accountNumber'] for acc in data['accounts'] 
                          if acc.get('accountNumber') and acc['accountNumber'].strip()]
        
        if not account_numbers:
            raise ValueError("No valid account numbers received from API")
        
        return account_numbers
        
    except Exception as e:
        raise RuntimeError(f"Failed to fetch approved accounts: {str(e)}")

def _calculate_tier1_metrics(filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame):
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

def _calculate_tier2_metrics(filtered_roles: pd.DataFrame, evaluated_roles: pd.DataFrame):
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

if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception as e:
        import sys
        sys.exit(1)