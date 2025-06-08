import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from etip_env import Env

# Control configuration mapping for CloudRadar-based controls
CONTROL_CONFIGS = {
    "CTRL-1077224": {
        "resource_type": "AWS::KMS::Key",
        "config_key": "supplementaryConfiguration.KeyRotationStatus",
        "config_location": "supplementaryConfiguration",
        "expected_value": "True",
        "apply_kms_exclusions": True,
        "description": "KMS Key Rotation Status",
    },
    "CTRL-1077231": {
        "resource_type": "AWS::EC2::Instance",
        "config_key": "configuration.metadataOptions.httpTokens",
        "config_location": "configurationList",
        "expected_value": "required",
        "apply_kms_exclusions": False,
        "description": "EC2 Instance Metadata Service v2",
    },
    "CTRL-1077125": {
        "resource_type": "AWS::KMS::Key",
        "config_key": "configuration.origin",
        "config_location": "configurationList",
        "expected_value": "AWS_KMS",
        "apply_kms_exclusions": True,
        "description": "KMS Key Origin",
    },
}


def run(env: Env, is_load: bool = True, dq_actions: bool = True):
    """Run the consolidated CloudRadar controls pipeline."""
    pipeline = PLAutomatedMonitoringCloudradarControls(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return pipeline.run(load=is_load, dq_actions=dq_actions)


class PLAutomatedMonitoringCloudradarControls(ConfigPipeline):
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        self.api_url = f"https://{self.env.exchange.exchange_url}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
    
    def _get_api_connector(self) -> OauthApi:
        """Standard OAuth API connector setup"""

        return OauthApi(
            url=self.api_url,
            api_token=refresh(
                client_id=self.env.exchange.client_id,
                client_secret=self.env.exchange.client_secret,
                exchange_url=self.env.exchange.exchange_url,
            ),
        )
    
    def _calculate_metrics(
        self,
        thresholds_raw: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Consolidated transformer for CloudRadar-based controls
        
        Args:
            thresholds_raw: DataFrame containing metric thresholds from SQL query
        
        Returns:
            DataFrame with standardized output schema for all controls
        """
        
        # Step 1: Input Validation (REQUIRED)
        if thresholds_raw.empty:
            raise RuntimeError(
                "No threshold data found. Cannot proceed with metrics calculation."
            )
            
        # Step 2: Extract threshold configuration and group by control
        # api_connector = context["api_connector"]
        control_groups = thresholds_raw.groupby("control_id")
        
        # Step 3: Efficiently fetch all required resource types
        required_controls = list(control_groups.groups.keys())
        all_resources = self._fetch_resources_by_type(required_controls)
        
        # Step 4: Process each control with its specific configuration
        all_results = []
        for control_id, control_thresholds in control_groups:
            if control_id not in CONTROL_CONFIGS:
                # Skip unknown controls
                continue
            
            control_config = CONTROL_CONFIGS[control_id]
            resource_type = control_config["resource_type"]
            
            # Get resources for this control's resource type
            resources = all_resources.get(resource_type, [])
            
            # Calculate compliance for this control
            control_results = self._calculate_control_compliance(
                control_id, control_thresholds, resources, control_config
            )
            all_results.extend(control_results)
        
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

    def _fetch_resources_by_type(
        self, required_controls: List[str]
    ) -> Dict[str, List[Dict]]:
        """Efficiently fetch all required resource types with minimal API calls"""
        # Determine unique resource types needed
        resource_types = list(
            set(
                CONTROL_CONFIGS[control_id]["resource_type"]
                for control_id in required_controls
                if control_id in CONTROL_CONFIGS
            )
        )
                
        resources_by_type = {}
        for resource_type in resource_types:
            resources_by_type[resource_type] = self._fetch_cloudradar_resources(
                resource_type
            )
        
        return resources_by_type

    def _fetch_cloudradar_resources(self, resource_type: str) -> List[Dict]:
        """Fetch resources from CloudRadar API for a specific resource type"""
        api_connector = self._get_api_connector()
        
        headers = {
            "Accept": "application/json;v=1",
            "Authorization": api_connector.api_token,
            "Content-Type": "application/json",
        }

        all_resources = []
        next_record_key = None

        while True:
            try:
                payload = {
                    "searchParameters": [{"resourceType": resource_type}],
                    "responseFields": [
                        "accountName",
                        "accountResourceId",
                        "amazonResourceName",
                        "resourceId",
                        "resourceType",
                        "configurationList",
                        "configuration.keyManager",
                        "configuration.keyState",
                        "configuration.origin",
                        "configuration.metadataOptions",
                        "supplementaryConfiguration",
                        "supplementaryConfiguration.KeyRotationStatus",
                        "source",
                    ],
                }

                params = {"limit": 10000}
                if next_record_key:
                    params["nextRecordKey"] = next_record_key

                response = api_connector.send_request(
                    url=api_connector.url,
                    request_type="post",
                    request_kwargs={
                        "headers": headers,
                        "json": payload,
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
                resources = data.get("resourceConfigurations", [])
                all_resources.extend(resources)

                next_record_key = data.get("nextRecordKey")
                if not next_record_key:
                    break

            except Exception as e:
                raise RuntimeError(
                    f"Failed to fetch {resource_type} resources from API: {str(e)}"
                )
                
        return all_resources
    
    def _apply_resource_exclusions(
        self, resources: List[Dict], control_config: Dict
    ) -> List[Dict]:
        """Apply control-specific resource exclusions"""
        filtered_resources = []
        
        for resource in resources:
            exclude = False
            
            # Check for orphaned resources
            if resource.get("source") == "CT-AccessDenied":
                exclude = True
                continue
            
            # Apply KMS-specific exclusions if needed
            if control_config["apply_kms_exclusions"]:
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
                
                # Exclude pending deletion and AWS-managed keys
                if key_state in [
                    "PendingDeletion", 
                    "PendingReplicaDeletion",
                    "Disabled",
                    ]:
                    exclude = True
                elif key_manager == "AWS":
                    exclude = True
            
            if not exclude:
                filtered_resources.append(resource)
        
        return filtered_resources

    def _get_config_value(self, resource: Dict, control_config: Dict) -> str:
        """Extract configuration value based on control configuration"""
        config_key = control_config["config_key"]
        config_location = control_config["config_location"]
        
        if config_location == "supplementaryConfiguration":
            # Handle supplementaryConfiguration array
            supp_config = resource.get("supplementaryConfiguration", [])
            config_item = next(
                (
                    c
                    for c in supp_config
                    if c.get("supplementaryConfigurationName") == config_key
                ),
                None,
            )
            return (
                config_item.get("supplementaryConfigurationValue")
                if config_item
                else None
            )
        
        elif config_location == "configurationList":            
            # Handle configurationList array
            config_list = resource.get("configurationList", [])
            config_item = next(
                (c for c in config_list if c.get("configurationName") == config_key),
                None,
            )
            return config_item.get("configurationValue") if config_item else None
        
        return None
    
    def _calculate_control_compliance(
        self,
        control_id: str,
        control_thresholds: pd.DataFrame,
        resources: List[Dict],
        control_config: Dict,
    ) -> List[Dict]:
        """Calculate compliance metrics for a specific control"""
        now = datetime.now()
        results = []

# Apply resource exclusions
        filtered_resources = self._apply_resource_exclusions(resources, control_config)
        
        # Process each threshold (Tier 1 and Tier 2)
        for _, threshold in control_thresholds.iterrows():
            metric_id = threshold["monitoring_metric_id"]
            tier = threshold.get("monitoring_metric_tier", "")
            
            compliant_count = 0
            non_compliant_resources = []
            
            #Determine the relevant resources for the current tier
            if "Tier 2" in tier:
                #For Tier 2, only consider resources where the configuration is present
                tier_resources = [
                    res
                    for res in filtered_resources
                    if self._get_config_value(res, control_config) is not None
                    and str(self._get_config_value(res, control_config)).strip()
                ]
            else:
                #For Tier 1, consider all filtered resources
                tier_resources = filtered_resources

            total_count = len(tier_resources) #<---- Moved and re-evaluated

            for resource in tier_resources: #<--- Iterate over tier_resources
                config_value = self._get_config_value(resource, control_config)
                is_compliant = False

                if "Tier 1" in tier:
                    #Tier 1: Check if configuration exists and has value
                    is_compliant = (
                        config_value is not None and str(config_value).strip()
                    )
                elif "Tier 2" in tier:
                    #Tier 2: Check if configuration equals expected value (only for resources with config)
                    #We already ensured config_value is present in tier_resources
                    is_compliant = str(config_value) == control_config["expected_value"]

                if is_compliant:
                    compliant_count += 1
                else:
                    #Collect non-compliant resource info
                    non_compliant_info = {
                        "resourceId": resource.get("resourceId", "N/A"),
                        "accountResourceId": resource.get("accountResourceId", "N/A"),
                        "configValue": (
                            config_value if config_value else "N/A (Not Found)"
                        ),
                    }

                    #Add resource-specific info
                    if control_config["apply_kms_exclusions"]:
                        config_list = resource.get("configurationList", [])
                        key_state = next(
                            (
                                c.get("configurationValue")
                                for c in config_list
                                if c.get("configurationName")
                                == "configuration.keyState"
                            ),
                            "N/A",
                        )
                        non_compliant_info["keyState"] = key_state
                    
                    non_compliant_resources.append(non_compliant_info)

            # Calculate compliance metrics
            compliance_percentage = (
                (compliant_count / total_count * 100) if total_count > 0 else 0
            )
            
            # Determine compliance status
            alert_threshold = threshold.get("alerting_threshold", 95.0)
            warning_threshold = threshold.get(
                "warning_threshold"
            ) #Get without a default, so it can be None

            if compliance_percentage < alert_threshold:
                compliance_status = "Red"
            elif (
                warning_threshold 
                is not None # Check if warning_threshold actually exists
                and compliance_percentage < warning_threshold
            ):
                compliance_status = "Yellow"
            else:
                compliance_status = "Green"

            # Format output with standard fields
            result = {
                "control_monitoring_utc_timestamp": now,
                "control_id": control_id,
                "monitoring_metric_id": metric_id,
                "monitoring_metric_value": float(compliance_percentage),
                "monitoring_metric_status": compliance_status,
                "metric_value_numerator": int(compliant_count),
                "metric_value_denominator": int(total_count),
                "resources_info": (
                    [json.dumps(resource) for resource in non_compliant_resources[:50]]
                if non_compliant_resources
                else None
            ),
            }
            results.append(result)
        
        return results
    
    # This is the extract portion for the API
    def extract(self) -> pd.DataFrame:
        df = super().extract()
        df["monitoring_metrics"] = self._calculate_metrics(df["thresholds_raw"])
        return df
