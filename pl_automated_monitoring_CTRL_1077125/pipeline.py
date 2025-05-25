import json
import pandas as pd
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from pandas.core.api import DataFrame as DataFrame

from config_pipeline import ConfigPipeline
from etip_env import Env
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from transform_library import transformer

logger = logging.getLogger(__name__)

# API constants
CLOUD_RADAR_BASE_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
CONFIG_URL = f"{CLOUD_RADAR_BASE_URL}/search-resource-configurations"

# KMS Key Origin configuration
KMS_KEY_ORIGIN_CONFIG = {
    "CTRL-1077125": {
        "origin_config_key": "configuration.origin",
        "origin_expected_value": "AWS_KMS",
    }
}

# Output columns for metrics dataframe
METRIC_COLUMNS = [
    "date",
    "control_id",
    "monitoring_metric_id",
    "monitoring_metric_value",
    "compliance_status",
    "numerator",
    "denominator",
    "non_compliant_resources"
]

def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    """Run the pipeline with the provided configuration.
    
    Args:
        env: The environment configuration
        is_export_test_data: Whether to export test data
        is_load: Whether to load the data to the destination
        dq_actions: Whether to run data quality actions
    """
    pipeline = PLAutomatedMonitoringCtrl1077125(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

class PLAutomatedMonitoringCtrl1077125(ConfigPipeline):
    """Pipeline for monitoring KMS Key Origin (CTRL-1077125)."""
    
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        self.control_id = "CTRL-1077125"
        self.cloudradar_api_url = CONFIG_URL
        
        # Store OAuth configuration for test compatibility
        self.client_id = env.exchange.client_id
        self.client_secret = env.exchange.client_secret
        self.exchange_url = env.exchange.exchange_url
        
    def _get_api_token(self) -> str:
        """Get API token for AWS Tooling API calls.
        
        This method is maintained for test compatibility.
        
        Returns:
            Token string for API authorization
        """
        try:
            token = refresh(
                client_id=self.env.exchange.client_id,
                client_secret=self.env.exchange.client_secret,
                exchange_url=self.env.exchange.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            raise RuntimeError("API token refresh failed") from e
            
    def _get_api_connector(self) -> OauthApi:
        """Get an OauthApi instance for making API requests.
        
        Returns:
            OauthApi: Configured API connector
        """
        try:
            api_token = refresh(
                client_id=self.env.exchange.client_id,
                client_secret=self.env.exchange.client_secret,
                exchange_url=self.env.exchange.exchange_url,
            )
            return OauthApi(
                url=self.cloudradar_api_url,
                api_token=f"Bearer {api_token}",
            )
        except Exception as e:
            logger.error(f"Failed to initialize API connector: {e}")
            raise RuntimeError("API connector initialization failed") from e
            
    def transform(self) -> None:
        """Prepare transformation stage by injecting API connector into context."""
        logger.info("Preparing transform stage: Initializing API connector...")
        api_connector = self._get_api_connector()
        
        self.context["api_connector"] = api_connector
        self.context["api_verify_ssl"] = C1_CERT_FILE
        
        logger.info("API context injected. Proceeding with config-defined transforms.")
        super().transform()

def fetch_all_resources(api_connector: OauthApi, verify_ssl: Any, search_payload: Dict, limit: Optional[int] = None) -> List[Dict]:
    """Fetch all resources using the OauthApi connector with pagination support."""
    all_resources = []
    next_record_key = ""
    # The response_fields are passed in the search_payload for this pipeline
    fetch_payload = {"searchParameters": search_payload.get("searchParameters", [{}]), "responseFields": search_payload.get("responseFields", [])}

    headers = {
        "Accept": "application/json;v=1.0",
        "Authorization": api_connector.api_token,
        "Content-Type": "application/json"
    }

    while True:
        params = {"limit": min(limit, 10000) if limit else 10000}
        if next_record_key:
            params["nextRecordKey"] = next_record_key

        request_kwargs = {
            "params": params,
            "headers": headers,
            "json": fetch_payload,
            "verify": verify_ssl,
        }

        try:
            response = api_connector.send_request(
                url=api_connector.url,
                request_type="post",
                request_kwargs=request_kwargs,
                retry_delay=20,
                max_retries=3
            )
        except Exception as e:
            logger.error(f"API request failed during pagination: {str(e)}")
            raise RuntimeError(f"API request failed during pagination: {str(e)}") from e

        if response is None:
            raise RuntimeError("API response is None")

        if not hasattr(response, 'status_code'):
            raise RuntimeError("API response does not have status_code attribute")

        if response.status_code > 299:
            err_msg = f"Error occurred while retrieving resources with status code {response.status_code}."
            raise RuntimeError(err_msg)

        data = response.json()
        resources = data.get("resourceConfigurations", [])
        new_next_record_key = data.get("nextRecordKey", "")
        all_resources.extend(resources)
        next_record_key = new_next_record_key

        if not next_record_key or (limit and len(all_resources) >= limit):
            break

        time.sleep(0.15)  # Small delay to avoid rate limits

    return all_resources

def _filter_resources(resources: List[Dict], config_key: str, config_value: str) -> Tuple[int, int, List[Dict], List[Dict]]:
    """Filter and categorize resources based on their configuration values."""
    tier1_numerator = 0
    tier2_numerator = 0
    tier1_non_compliant = []
    tier2_non_compliant = []
    config_key_full = config_key  # Already includes 'configuration.' prefix
    fields_to_keep = ["resourceId", "accountResourceId", "resourceType", "awsRegion", "accountName", "awsAccountId", 
                     config_key_full, "configuration.keyState", "configuration.keyManager", "source"]
    
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
            config_item = next((c for c in config_list if c.get("configurationName") == config_key_full), None)
            actual_value = config_item.get("configurationValue") if config_item else None
            
            if actual_value and str(actual_value).strip():
                tier1_numerator += 1
                if str(actual_value) == config_value:
                    tier2_numerator += 1
                else:
                    non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                    non_compliant_info[config_key_full] = actual_value
                    non_compliant_info["configuration.keyState"] = key_state if key_state else "N/A"
                    non_compliant_info["configuration.keyManager"] = key_manager if key_manager else "N/A"
                    tier2_non_compliant.append(non_compliant_info)
            else:
                non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                non_compliant_info[config_key_full] = actual_value if actual_value is not None else "MISSING"
                non_compliant_info["configuration.keyState"] = key_state if key_state else "N/A"
                non_compliant_info["configuration.keyManager"] = key_manager if key_manager else "N/A"
                tier1_non_compliant.append(non_compliant_info)
            
    return tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant

@transformer
def calculate_ctrl1077125_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    """Calculate metrics for CTRL-1077125 (KMS Key Origin) based on resource configurations and thresholds."""
    control_id = "CTRL-1077125"
    
    # Get control config
    control_config = KMS_KEY_ORIGIN_CONFIG.get(control_id, {})
    config_key = control_config.get("origin_config_key", "configuration.origin")
    expected_value = control_config.get("origin_expected_value", "AWS_KMS")

    # Get metric IDs from thresholds DataFrame based on tier
    tier1_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                                 (thresholds_raw["monitoring_metric_tier"] == "Tier 1")]
    tier2_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                                 (thresholds_raw["monitoring_metric_tier"] == "Tier 2")]

    # Ensure metric IDs are found
    if tier1_metrics.empty:
        raise ValueError(f"Tier 1 metric data not found in thresholds for control {control_id}")
    if tier2_metrics.empty:
        raise ValueError(f"Tier 2 metric data not found in thresholds for control {control_id}")

    tier1_metric_id = tier1_metrics.iloc[0]["monitoring_metric_id"]
    tier2_metric_id = tier2_metrics.iloc[0]["monitoring_metric_id"]

    try:
        api_connector = context["api_connector"]
        verify_ssl = context["api_verify_ssl"]

        # Set up search payload for KMS keys
        search_payload = {
            "searchParameters": [{"resourceType": "AWS::KMS::Key"}],
            "responseFields": [
                "resourceId", "accountResourceId", "resourceType", "awsRegion", "accountName", "awsAccountId",
                "configurationList", config_key, "configuration.keyState", "configuration.keyManager", "source"
            ]
        }

        # Fetch resources
        try:
            resources = fetch_all_resources(api_connector, verify_ssl, search_payload)
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise RuntimeError(f"Critical API fetch failure: {str(e)}") from e

        if not resources:
            logger.warning("No KMS Keys found via API. Returning zero metrics.")
            now = int(datetime.utcnow().timestamp() * 1000)
            results = [
                {"date": now, "control_id": control_id, "monitoring_metric_id": tier1_metric_id,
                 "monitoring_metric_value": 0.0, "compliance_status": "Red", "numerator": 0,
                 "denominator": 0, "non_compliant_resources": None},
                {"date": now, "control_id": control_id, "monitoring_metric_id": tier2_metric_id,
                 "monitoring_metric_value": 0.0, "compliance_status": "Red", "numerator": 0,
                 "denominator": 0, "non_compliant_resources": None}
            ]
            return pd.DataFrame(results)

        # Filter and calculate metrics
        tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant = _filter_resources(
            resources, config_key, expected_value
        )
        
        total_resources = len([r for r in resources if r.get("source") != "CT-AccessDenied"])
        tier1_metric = tier1_numerator / total_resources if total_resources > 0 else 0
        tier2_metric = tier2_numerator / tier1_numerator if tier1_numerator > 0 else 0

        # Get thresholds
        t1_threshold = tier1_metrics.iloc[0]
        t2_threshold = tier2_metrics.iloc[0]

        # Calculate compliance status
        t1_status = get_compliance_status(tier1_metric, t1_threshold["alerting_threshold"], t1_threshold["warning_threshold"])
        t2_status = get_compliance_status(tier2_metric, t2_threshold["alerting_threshold"], t2_threshold["warning_threshold"])

        # Create results
        now = int(datetime.utcnow().timestamp() * 1000)
        results = [
            {"date": now, "control_id": control_id, "monitoring_metric_id": tier1_metric_id,
             "monitoring_metric_value": float(tier1_metric * 100), "compliance_status": t1_status,
             "numerator": int(tier1_numerator), "denominator": int(total_resources),
             "non_compliant_resources": [json.dumps(x) for x in tier1_non_compliant[:50]] if tier1_non_compliant else None},
            {"date": now, "control_id": control_id, "monitoring_metric_id": tier2_metric_id,
             "monitoring_metric_value": float(tier2_metric * 100), "compliance_status": t2_status,
             "numerator": int(tier2_numerator), "denominator": int(tier1_numerator),
             "non_compliant_resources": [json.dumps(x) for x in tier2_non_compliant[:50]] if tier2_non_compliant else None}
        ]

        # Create DataFrame and ensure correct types
        df = pd.DataFrame(results)
        df["date"] = df["date"].astype("int64")
        df["numerator"] = df["numerator"].astype("int64")
        df["denominator"] = df["denominator"].astype("int64")
        df["monitoring_metric_value"] = df["monitoring_metric_value"].astype(float)

        logger.info(f"Metrics: Tier 1 = {tier1_metric:.2%}, Tier 2 = {tier2_metric:.2%}")
        logger.info(f"Status: Tier 1 = {t1_status}, Tier 2 = {t2_status}")

        return df

    except Exception as e:
        if not isinstance(e, RuntimeError):
            logger.error(f"Unexpected error in calculate_ctrl1077125_metrics: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to calculate metrics: {str(e)}") from e
        raise

def get_compliance_status(metric: float, alert_threshold: float, warning_threshold: Optional[float] = None) -> str:
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