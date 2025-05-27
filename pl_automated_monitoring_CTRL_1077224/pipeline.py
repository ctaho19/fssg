from typing import Dict, Any
import pandas as pd
from datetime import datetime
import json
import ssl
import os
from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env
from transform_library import transformer

def run(env: Env, is_load: bool = True, dq_actions: bool = True):
    """Run the pipeline with the provided configuration."""
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    pipeline.configure_from_filename("config.yml")
    return pipeline.run(load=is_load, dq_actions=dq_actions)

class PLAutomatedMonitoringCTRL1077224(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        self.api_url = f"https://{self.env.exchange.exchange_url}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        
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
    
    # Step 3: API Data Collection with Pagination
    all_resources = []
    next_record_key = None
    
    while True:
        try:
            # Standard API request headers
            headers = {
                "Accept": "application/json;v=1",  # Note: v=1 not v=1.0
                "Authorization": api_connector.api_token,
                "Content-Type": "application/json"
            }
            
            # Construct request payload for KMS keys
            payload = {
                "searchParameters": [{"resourceType": "AWS::KMS::Key"}],
                "responseFields": [
                    "accountName", "accountResourceId", "amazonResourceName", "resourceId", 
                    "resourceType", "configurationList", "configuration.keyManager", 
                    "configuration.keyState", "supplementaryConfiguration", 
                    "supplementaryConfiguration.KeyRotationStatus", "source"
                ]
            }
            
            # Add pagination parameters
            params = {"limit": 10000}
            if next_record_key:
                params["nextRecordKey"] = next_record_key
            
            response = api_connector.send_request(
                url="",
                request_type="post",
                request_kwargs={
                    "headers": headers,
                    "json": payload,
                    "params": params,
                    "timeout": 120
                },
                retry_delay=5,
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
                
        except Exception as e:
            raise RuntimeError(f"Failed to fetch resources from API: {str(e)}")
    
    # Step 4: Filter and Compliance Calculation
    results = []
    now = datetime.now()
    
    # Filter out excluded resources
    in_scope_resources = []
    for resource in all_resources:
        exclude = False
        
        # Check for orphaned keys
        if resource.get("source") == "CT-AccessDenied":
            exclude = True
            continue
            
        # Check configurations
        config_list = resource.get("configurationList", [])
        key_state = None
        key_manager = None
        
        for config in config_list:
            config_name = config.get("configurationName")
            config_value = config.get("configurationValue")
            
            if config_name == "configuration.keyState":
                key_state = config_value
            elif config_name == "configuration.keyManager":
                key_manager = config_value
        
        # Check exclusion conditions
        if key_state in ["PendingDeletion", "PendingReplicaDeletion"]:
            exclude = True
        elif key_manager == "AWS":
            exclude = True
        
        if not exclude:
            in_scope_resources.append(resource)
    
    # Process metrics for each threshold
    for threshold in thresholds:
        ctrl_id = threshold["control_id"]
        metric_id = threshold["monitoring_metric_id"]
        tier = threshold.get("monitoring_metric_tier", "")
        
        # Tier-specific compliance logic
        compliant_count = 0
        total_count = len(in_scope_resources)
        non_compliant_resources = []
        
        for resource in in_scope_resources:
            # Check for rotation status in supplementaryConfiguration
            supp_config = resource.get("supplementaryConfiguration", [])
            rotation_item = next((c for c in supp_config 
                                if c.get("supplementaryConfigurationName") == "supplementaryConfiguration.KeyRotationStatus"), None)
            rotation_value = rotation_item.get("supplementaryConfigurationValue") if rotation_item else None
            
            is_compliant = False
            
            if "Tier 1" in tier:
                # Tier 1: Check if rotation status exists
                is_compliant = rotation_value is not None and str(rotation_value).strip()
            elif "Tier 2" in tier:
                # Tier 2: Check if rotation is enabled (only for resources with rotation status)
                if rotation_value is not None and str(rotation_value).strip():
                    is_compliant = str(rotation_value).upper() == "TRUE"
                else:
                    # Skip resources without rotation status for Tier 2
                    total_count -= 1
                    continue
            
            if is_compliant:
                compliant_count += 1
            else:
                # Collect non-compliant resource info
                non_compliant_info = {
                    "resourceId": resource.get("resourceId", "N/A"),
                    "accountResourceId": resource.get("accountResourceId", "N/A"),
                    "keyState": next((c.get("configurationValue") for c in config_list 
                                    if c.get("configurationName") == "configuration.keyState"), "N/A"),
                    "rotationStatus": rotation_value if rotation_value else "N/A (Not Found)"
                }
                non_compliant_resources.append(non_compliant_info)
        
        # Calculate compliance metrics
        compliance_percentage = (compliant_count / total_count * 100) if total_count > 0 else 0
        
        # Determine compliance status
        alert_threshold = threshold.get("alerting_threshold", 95.0)
        warning_threshold = threshold.get("warning_threshold", 97.0)
        
        if compliance_percentage >= alert_threshold:
            compliance_status = "Green"
        elif compliance_percentage >= warning_threshold:
            compliance_status = "Yellow"
        else:
            compliance_status = "Red"
        
        # Step 5: Format Output with Standard Fields
        result = {
            "control_monitoring_utc_timestamp": now,
            "control_id": ctrl_id,
            "monitoring_metric_id": metric_id,
            "monitoring_metric_value": float(compliance_percentage),
            "monitoring_metric_status": compliance_status,
            "metric_value_numerator": int(compliant_count),
            "metric_value_denominator": int(total_count),
            "resources_info": [json.dumps(resource) for resource in non_compliant_resources[:50]] if non_compliant_resources else None
        }
        results.append(result)
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    from etip_env import set_env_vars
    
    env = set_env_vars()
    try:
        run(env=env, is_load=False, dq_actions=False)
    except Exception as e:
        import sys
        sys.exit(1)