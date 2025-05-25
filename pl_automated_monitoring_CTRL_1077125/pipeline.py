from typing import Dict, Any
import pandas as pd
from datetime import datetime
import json
import ssl
import os
import logging
import time
from pipeline_framework import ConfigPipeline, transformer
from pipeline_framework.env import Env
from pipeline_framework.connectors.oauth_api import OauthApi
from pipeline_framework.connectors.auth import refresh

logger = logging.getLogger(__name__)

class PLAutomatedMonitoringCTRL1077125(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        
        # Initialize control-specific variables
        self.api_url = f"https://{self.env.exchange.exchange_url}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        
        # KMS Key Origin configuration
        self.control_id = "CTRL-1077125"
        self.origin_config_key = "configuration.origin"
        self.origin_expected_value = "AWS_KMS"
        
    def _get_api_connector(self) -> OauthApi:
        """Standard OAuth API connector setup following Zach's pattern"""
        api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        
        # Create SSL context if certificate file exists
        ssl_context = None
        if os.environ.get("C1_CERT_FILE"):
            ssl_context = ssl.create_default_context(cafile=os.environ["C1_CERT_FILE"])
        
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
def calculate_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    """
    Core business logic transformer following Zach's established patterns
    
    Args:
        thresholds_raw: DataFrame containing metric thresholds from SQL query
        context: Pipeline context including API connector
        
    Returns:
        DataFrame with standardized output schema
    """
    
    # Step 1: Input Validation (REQUIRED)
    if thresholds_raw.empty:
        raise RuntimeError("No threshold data found. Cannot proceed with metrics calculation.")
    
    # Step 2: Extract Threshold Configuration
    thresholds = thresholds_raw.to_dict("records")
    api_connector = context["api_connector"]
    control_id = "CTRL-1077125"
    
    # Get metric IDs from thresholds DataFrame based on tier
    tier1_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                                 (thresholds_raw["monitoring_metric_tier"] == "Tier 1")]
    tier2_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                                 (thresholds_raw["monitoring_metric_tier"] == "Tier 2")]

    # Ensure metric IDs are found
    if tier1_metrics.empty:
        raise RuntimeError(f"Tier 1 metric data not found in thresholds for control {control_id}")
    if tier2_metrics.empty:
        raise RuntimeError(f"Tier 2 metric data not found in thresholds for control {control_id}")

    tier1_metric_id = tier1_metrics.iloc[0]["monitoring_metric_id"]
    tier2_metric_id = tier2_metrics.iloc[0]["monitoring_metric_id"]
    
    # Step 3: API Data Collection with Pagination
    all_resources = []
    next_record_key = None
    
    # Set up search payload for KMS keys
    payload = {
        "searchParameters": [{"resourceType": "AWS::KMS::Key"}],
        "responseFields": [
            "resourceId", "accountResourceId", "resourceType", "awsRegion", "accountName", "awsAccountId",
            "configurationList", "configuration.origin", "configuration.keyState", "configuration.keyManager", "source"
        ]
    }
    
    while True:
        try:
            # Standard API request headers
            headers = {
                "Accept": "application/json;v=1",  # Note: v=1 not v=1.0
                "Authorization": api_connector.api_token,
                "Content-Type": "application/json"
            }
            
            # Add pagination parameters
            params = {"limit": 10000}
            if next_record_key:
                params["nextRecordKey"] = next_record_key
            
            # Create SSL context if certificate file exists
            ssl_context = None
            if os.environ.get("C1_CERT_FILE"):
                ssl_context = ssl.create_default_context(cafile=os.environ["C1_CERT_FILE"])
            
            response = api_connector.send_request(
                url=api_connector.url,
                request_type="post",
                request_kwargs={
                    "params": params,
                    "headers": headers,
                    "json": payload,
                    "verify": ssl_context.check_hostname if ssl_context else True,
                },
                retry_delay=20,
                max_retries=3
            )
            
            if response.status_code != 200:
                raise RuntimeError(f"API request failed: {response.status_code} - {response.text}")
            
            data = response.json()
            resources = data.get("resourceConfigurations", [])
            all_resources.extend(resources)
            
            # Handle pagination
            next_record_key = data.get("nextRecordKey")
            if not next_record_key:
                break
                
            time.sleep(0.15)  # Small delay to avoid rate limits
                
        except Exception as e:
            raise RuntimeError(f"Failed to fetch resources from API: {str(e)}")
    
    # Step 4: Compliance Calculation
    if not all_resources:
        logger.warning("No KMS Keys found via API. Returning zero metrics.")
        now = datetime.now()
        results = [
            {
                "control_monitoring_utc_timestamp": now,
                "control_id": control_id,
                "monitoring_metric_id": tier1_metric_id,
                "monitoring_metric_value": 0.0,
                "monitoring_metric_status": "Red",
                "metric_value_numerator": 0,
                "metric_value_denominator": 0,
                "resources_info": None
            },
            {
                "control_monitoring_utc_timestamp": now,
                "control_id": control_id,
                "monitoring_metric_id": tier2_metric_id,
                "monitoring_metric_value": 0.0,
                "monitoring_metric_status": "Red",
                "metric_value_numerator": 0,
                "metric_value_denominator": 0,
                "resources_info": None
            }
        ]
        return pd.DataFrame(results)
    
    # Filter and categorize resources
    tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant = _filter_resources(
        all_resources, "configuration.origin", "AWS_KMS"
    )
    
    total_resources = len([r for r in all_resources if r.get("source") != "CT-AccessDenied"])
    tier1_metric = tier1_numerator / total_resources if total_resources > 0 else 0
    tier2_metric = tier2_numerator / tier1_numerator if tier1_numerator > 0 else 0

    # Get thresholds
    t1_threshold = tier1_metrics.iloc[0]
    t2_threshold = tier2_metrics.iloc[0]

    # Calculate compliance status
    t1_status = _get_compliance_status(tier1_metric, t1_threshold["alerting_threshold"], t1_threshold["warning_threshold"])
    t2_status = _get_compliance_status(tier2_metric, t2_threshold["alerting_threshold"], t2_threshold["warning_threshold"])

    # Step 5: Format Output with Standard Fields
    now = datetime.now()
    results = [
        {
            "control_monitoring_utc_timestamp": now,
            "control_id": control_id,
            "monitoring_metric_id": tier1_metric_id,
            "monitoring_metric_value": float(tier1_metric * 100),
            "monitoring_metric_status": t1_status,
            "metric_value_numerator": int(tier1_numerator),
            "metric_value_denominator": int(total_resources),
            "resources_info": [json.dumps(x) for x in tier1_non_compliant[:50]] if tier1_non_compliant else None
        },
        {
            "control_monitoring_utc_timestamp": now,
            "control_id": control_id,
            "monitoring_metric_id": tier2_metric_id,
            "monitoring_metric_value": float(tier2_metric * 100),
            "monitoring_metric_status": t2_status,
            "metric_value_numerator": int(tier2_numerator),
            "metric_value_denominator": int(tier1_numerator),
            "resources_info": [json.dumps(x) for x in tier2_non_compliant[:50]] if tier2_non_compliant else None
        }
    ]
    
    logger.info(f"Metrics: Tier 1 = {tier1_metric:.2%}, Tier 2 = {tier2_metric:.2%}")
    logger.info(f"Status: Tier 1 = {t1_status}, Tier 2 = {t2_status}")
    
    return pd.DataFrame(results)

def _filter_resources(resources: list, config_key: str, config_value: str) -> tuple:
    """Filter and categorize resources based on their configuration values."""
    tier1_numerator = 0
    tier2_numerator = 0
    tier1_non_compliant = []
    tier2_non_compliant = []
    
    fields_to_keep = ["resourceId", "accountResourceId", "resourceType", "awsRegion", "accountName", "awsAccountId", 
                     config_key, "configuration.keyState", "configuration.keyManager", "source"]
    
    for resource in resources:
        exclude = False
        
        # Check for orphaned keys
        source_field = resource.get("source")
        if source_field == "CT-AccessDenied":
            exclude = True
            continue

        # Check configurations
        config_list = resource.get("configurationList", [])
        key_state = None
        key_manager = None

        for config in config_list:
            config_name = config.get("configurationName")
            config_value_current = config.get("configurationValue")

            if config_name == "configuration.keyState":
                key_state = config_value_current
            elif config_name == "configuration.keyManager":
                key_manager = config_value_current

        # Check exclusion conditions
        if key_state in ["PendingDeletion", "PendingReplicaDeletion"]:
            exclude = True
        elif key_manager == "AWS":
            exclude = True
        
        if not exclude:
            # Get the target configuration value
            config_item = next((c for c in config_list if c.get("configurationName") == config_key), None)
            actual_value = config_item.get("configurationValue") if config_item else None
            
            if actual_value and str(actual_value).strip():
                tier1_numerator += 1
                if str(actual_value) == config_value:
                    tier2_numerator += 1
                else:
                    non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                    non_compliant_info[config_key] = actual_value
                    non_compliant_info["configuration.keyState"] = key_state if key_state else "N/A"
                    non_compliant_info["configuration.keyManager"] = key_manager if key_manager else "N/A"
                    tier2_non_compliant.append(non_compliant_info)
            else:
                non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                non_compliant_info[config_key] = actual_value if actual_value is not None else "MISSING"
                non_compliant_info["configuration.keyState"] = key_state if key_state else "N/A"
                non_compliant_info["configuration.keyManager"] = key_manager if key_manager else "N/A"
                tier1_non_compliant.append(non_compliant_info)
                
    return tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant

def _get_compliance_status(metric: float, alert_threshold: float, warning_threshold: float = None) -> str:
    """Calculate compliance status based on metric value and thresholds."""
    metric_percentage = metric * 100
    try:
        alert_threshold_f = float(alert_threshold)
    except (TypeError, ValueError):
        return "Red"
    warning_threshold_f = None
    if warning_threshold is not None:
        try:
            warning_threshold_f = float(warning_threshold)
        except (TypeError, ValueError):
            warning_threshold_f = None
    if metric_percentage >= alert_threshold_f:
        return "Green"
    elif warning_threshold_f is not None and metric_percentage >= warning_threshold_f:
        return "Yellow"
    else:
        return "Red"