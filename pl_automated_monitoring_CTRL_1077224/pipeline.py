import json
import pandas as pd
import logging
import random
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

# KMS Keys API constants
AWS_TOOLING_BASE_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
CONFIG_URL = f"{AWS_TOOLING_BASE_URL}/search-resource-configurations"
SUMMARY_URL = f"{AWS_TOOLING_BASE_URL}/summary-view"

# Control IDs and info for KMS Key Rotation
KMS_KEY_ROTATION_TIERS = {
    "CTRL-1077224": {
        "tier1_metric_id": "MNTR-1077224-T1",
        "tier2_metric_id": "MNTR-1077224-T2",
        "rotation_config_key": "supplementaryConfiguration.KeyRotationStatus",
        "rotation_expected_value": "TRUE"
    }
}

# Output columns for metrics dataframe
METRIC_COLUMNS = [
    "monitoring_metric_id",
    "control_id",
    "monitoring_metric_value",
    "monitoring_metric_status",
    "metric_value_numerator",
    "metric_value_denominator",
    "resources_info",
    "control_monitoring_utc_timestamp",
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
    pipeline = PLAutomatedMonitoringCTRL1077224(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

class PLAutomatedMonitoringCTRL1077224(ConfigPipeline):
    """Pipeline for monitoring KMS Key Rotation (CTRL-1077224)."""
    
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        self.env = env
        self.control_id = "CTRL-1077224"
        self.cloudradar_api_url = f"{AWS_TOOLING_BASE_URL}/search-resource-configurations"
        
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
    
    def get_summary_count(self, payload: Dict, timeout: int = 30, max_retries: int = 2) -> Optional[int]:
        """Fetch the total count of resources from the summary-view API.
        
        Args:
            payload: API request payload containing search parameters
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            
        Returns:
            Total resource count or None if request fails
        """
        logger.info(f"Calling Summary View API with payload: {json.dumps(payload, indent=2)}")
        
        for retry in range(max_retries + 1):
            try:
                response = requests.post(
                    SUMMARY_URL, 
                    headers=self.headers, 
                    json=payload,
                    verify=True,
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    level1_list = data.get("level1List", [])
                    if not level1_list:
                        logger.warning("Empty level1List in response")
                        return 0
                        
                    count = level1_list[0].get("level1ResourceCount", 0)
                    logger.info(f"Summary count: {count}")
                    return count
                    
                elif response.status_code == 429:
                    wait_time = min(2 ** retry, 30)
                    logger.warning(f"Rate limited (429). Waiting {wait_time}s before retry {retry+1}/{max_retries}.")
                    time.sleep(wait_time)
                    if retry == max_retries:
                        logger.error("Max retries reached for rate limiting.")
                        return None
                else:
                    logger.error(f"Summary API failed: {response.status_code} - {response.text}")
                    if retry < max_retries:
                        wait_time = min(2 ** retry, 15)
                        logger.info(f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        return None
                        
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout after {timeout}s")
                if retry < max_retries:
                    wait_time = min(2 ** retry, 15)
                    logger.info(f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached after timeouts.")
                    return None
                    
            except Exception as e:
                logger.error(f"Exception in Summary API: {str(e)}")
                if retry < max_retries:
                    wait_time = min(2 ** retry, 15)
                    logger.info(f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    return None
        
        return None

    def fetch_all_resources(self, payload: Dict, limit: Optional[int] = None, 
                          timeout: int = 60, max_retries: int = 3) -> Tuple[List[Dict], int]:
        """Fetch resource configurations with pagination via URL params, timeout, and retry logic."""
        all_resources = []
        fetched_count = 0
        next_record_key = ""
        
        fetch_payload = {
            "searchParameters": payload.get("searchParameters", []),
            "responseFields": [
                "accountName", "accountResourceId", "amazonResourceName", "asvName", 
                "awsAccountId", "awsRegion", "businessApplicationName", 
                "environment", "resourceCreationTimestamp", "resourceId", 
                "resourceType", "supplementaryConfiguration", "configuration.keyManager", "configuration.keyState",
                "supplementaryConfiguration.KeyRotationStatus"
            ],
        }
        
        logger.info(f"Fetching resources. Request payload: {json.dumps(fetch_payload, indent=2)}")
        
        page_count = 0
        start_time = datetime.now()

        while True:
            page_start_time = time.time()
            
            # --- Prepare URL Query Parameters ---
            current_page_params = {}
            # Set the desired page size for the API call
            current_page_params['limit'] = 10000 
            
            # Add pagination key if available for subsequent pages
            if next_record_key:
                current_page_params['nextRecordKey'] = next_record_key
            
            # The request URL is always the base config URL
            request_url = CONFIG_URL 

            page_fetched_successfully = False
            for retry in range(max_retries + 1):
                try:
                    # Log the URL and params being used for this attempt
                    logger.info(f"Requesting page {page_count + 1} with URL params: {current_page_params}" + 
                               (f" (retry {retry})" if retry > 0 else ""))
                    
                    # --- Make the API call using the 'params' argument ---
                    response = requests.post(
                        request_url,
                        headers=self.headers,
                        json=fetch_payload, # The main search criteria go in the body
                        params=current_page_params, # Limit and pagination key go in URL params
                        verify=True,
                        timeout=timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        resources_on_page = data.get("resourceConfigurations", [])
                        num_on_page = len(resources_on_page)
                        
                        if page_count == 0 and num_on_page > 0:
                            logger.debug(f"First page response structure keys: {list(data.keys())}")
                            # Log structure of first resource's config lists if they exist
                            sample_resource = resources_on_page[0]
                            if "configurationList" in sample_resource:
                                 logger.debug("Sample resource configuration list found")
                            if "supplementaryConfiguration" in sample_resource:
                                 logger.debug("Sample resource supplementary configuration found")

                        all_resources.extend(resources_on_page)
                        fetched_count += num_on_page
                        next_record_key = data.get("nextRecordKey", "") # Get key for the *next* page
                        page_elapsed = time.time() - page_start_time
                        logger.info(f"Page {page_count + 1}: Fetched {num_on_page} resources (Total: {fetched_count}) in {page_elapsed:.2f}s. NextKey: {'Yes' if next_record_key else 'No'}")
                        
                        page_count += 1
                        page_fetched_successfully = True
                        break # Success, exit retry loop for this page

                    elif response.status_code == 429:
                        # Add jitter to backoff
                        wait_time = min(2 ** retry, 60) * (1 + random.uniform(0.1, 0.5)) 
                        logger.warning(f"Rate limited (429) on page {page_count + 1}. Waiting {wait_time:.1f}s before retry {retry+1}/{max_retries}.")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Config API error page {page_count + 1}: {response.status_code} - {response.text}")
                        if retry < max_retries:
                            wait_time = min(2 ** retry, 30) * (1 + random.uniform(0.1, 0.5))
                            logger.info(f"Retrying page {page_count + 1} in {wait_time:.1f}s... (Attempt {retry+1}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to API error {response.status_code}")
                            raise Exception(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to API error {response.status_code}")

                except requests.exceptions.Timeout:
                     logger.warning(f"Request timeout fetching page {page_count + 1} after {timeout}s (Attempt {retry+1}/{max_retries})")
                     if retry < max_retries:
                         wait_time = min(2 ** retry, 30) * (1 + random.uniform(0.1, 0.5))
                         logger.info(f"Retrying page {page_count + 1} due to timeout in {wait_time:.1f}s...")
                         time.sleep(wait_time)
                     else:
                          logger.error(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to timeout.")
                          raise Exception(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to timeout")
                          
                except Exception as e:
                    logger.error(f"Exception fetching page {page_count + 1} (Attempt {retry+1}/{max_retries}): {str(e)}")
                    if retry < max_retries:
                        wait_time = min(2 ** retry, 30) * (1 + random.uniform(0.1, 0.5))
                        logger.info(f"Retrying page {page_count + 1} due to exception in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                         logger.error(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to exception.")
                         raise Exception(f"Failed to fetch page {page_count + 1} after {max_retries + 1} attempts due to exception: {e}")
            
            # Exit conditions for the main 'while' loop:
            if not next_record_key:
                logger.info("No nextRecordKey found, fetch complete.")
                break
                
            # Check against overall fetch limit if provided by the caller
            if limit is not None and fetched_count >= limit:
                logger.info(f"Reached fetch limit of {limit} resources specified in function call.")
                # Trim excess resources if the last page fetched more than needed
                all_resources = all_resources[:limit]
                fetched_count = len(all_resources)
                break
                
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Fetch complete: Retrieved {fetched_count} resources in {page_count} pages, total time {total_time:.1f} seconds")
        return all_resources, fetched_count

    def filter_out_of_scope_keys(self, resources: List[Dict], desired_fields_for_report: List[str]) -> Tuple[List[Dict], pd.DataFrame]:
        """Filters out resources based on predefined exclusion criteria for KMS keys."""
        in_scope_resources = []
        excluded_resources_data = []
        
        required_filter_fields = {"resourceId", "accountResourceId", "configuration.keyState", "configuration.keyManager", "source"}
        report_fields = set(desired_fields_for_report) | required_filter_fields 
        
        logger.info(f"Starting filtering of {len(resources)} fetched resources based on exclusion rules...")

        for resource in resources:
            exclude = False
            reason = "N/A"
            
            # --- Exclusion Check 1: Orphaned Keys
            source_field = resource.get("source") # Use lowercase 'source'
            if source_field == "CT-AccessDenied":
                exclude = True
                reason = "Source = CT-AccessDenied (Orphaned)"

            # --- Exclusion Check 2 & 3: Configuration List Checks ---
            key_state = "N/A" 
            key_manager = "N/A" 
                
            if not exclude: 
                config_list = resource.get("configurationList", [])
                
                for config in config_list:
                    config_name = config.get("configurationName")
                    config_value = config.get("configurationValue")

                    # --- Store the found values for later use in reporting ---
                    if config_name == "configuration.keyState":
                        key_state = config_value if config_value is not None else "N/A" 
                    elif config_name == "configuration.keyManager":
                        key_manager = config_value if config_value is not None else "N/A"
                
                if key_state in ["PendingDeletion", "PendingReplicaDeletion"]:
                    exclude = True
                    reason = f"KeyState = {key_state} (Pending Deletion/Replica Deletion)"
                elif key_manager == "AWS": # Assuming API returns 'AWS' - verify case if needed
                    exclude = True
                    reason = "KeyManager = AWS (AWS Managed)"

            # --- Store results ---
            if exclude:
                excluded_info = {}
                # --- Improved report population for excluded items ---
                for field in report_fields:
                     value = "N/A" # Default
                     if field == "source": # Handle top-level source correctly
                         value = resource.get("source", "N/A")
                     elif field.startswith("supplementaryConfiguration."):
                         s_list = resource.get("supplementaryConfiguration", [])
                         item = next((i for i in s_list if i.get("supplementaryConfigurationName") == field), None)
                         if item: value = item.get("supplementaryConfigurationValue", "N/A (Key Found, No Value)")
                         else: value = "N/A (Key Not Found)"
                     elif field.startswith("configuration."):
                         # Use values extracted earlier if available, otherwise search list
                         if field == "configuration.keyState": value = key_state
                         # Check for KeyManager - use the extracted value
                         elif field == "configuration.keyManager": value = key_manager 
                         else: # Search configList for other configuration.* fields
                             c_list = resource.get("configurationList", [])
                             item = next((i for i in c_list if i.get("configurationName") == field), None)
                             if item: value = item.get("configurationValue", "N/A (Key Found, No Value)")
                             else: value = "N/A (Key Not Found)"
                     elif '.' in field: 
                         parts = field.split('.', 1)
                         parent, child = parts[0], parts[1]
                         value = resource.get(parent, {}).get(child, "N/A")
                     else: # Other top-level fields like resourceId
                         value = resource.get(field, "N/A")
                     excluded_info[field] = value
                
                excluded_info["exclusionReason"] = reason
                excluded_resources_data.append(excluded_info)
            else:
                in_scope_resources.append(resource)

        # Create DataFrame with columns in a predictable order if possible
        ordered_columns = sorted(list(report_fields)) + ["exclusionReason"]
        excluded_df = pd.DataFrame(excluded_resources_data)
        # Reindex to ensure all columns are present and in order, handling cases where some fields might be totally absent
        excluded_df = excluded_df.reindex(columns=ordered_columns, fill_value="N/A (Column Missing)")

        logger.info(f"Exclusion filtering complete. In-scope: {len(in_scope_resources)}, Excluded: {len(excluded_df)}")
        
        if not excluded_df.empty:
            logger.debug("Exclusion reasons summary:")
            logger.debug(excluded_df['exclusionReason'].value_counts().to_string())
            
        return in_scope_resources, excluded_df

    def filter_tier1_resources(self, resources: List[Dict], config_key: str, fields_for_report: List[str]) -> Tuple[int, pd.DataFrame]:
        """Filter resources based on Tier 1 compliance (non-empty specified config_key),
           checking supplementaryConfiguration or configurationList based on key prefix."""
        matching_count = 0
        non_matching_resources = []
        
        target_config_name = config_key 
        output_config_col_name = config_key 

        for resource in resources:
            config_value = None 
            key_state_val = "N/A" # Store for reporting
            key_manager_val = "N/A" # Store for reporting

            # --- Extract KeyState and KeyManager for reporting ---
            config_list_main = resource.get("configurationList", [])
            for item in config_list_main:
                name = item.get("configurationName")
                if name == "configuration.keyState":
                     key_state_val = item.get("configurationValue", "N/A")
                elif name == "configuration.keyManager":
                     key_manager_val = item.get("configurationValue", "N/A")

            # --- Parsing logic for the target config_key ---
            if target_config_name.startswith("supplementaryConfiguration."):
                search_list = resource.get("supplementaryConfiguration", [])
                name_key = "supplementaryConfigurationName"
                value_key = "supplementaryConfigurationValue"
            else: # Assumes configuration.* or other (defaults to configurationList)
                search_list = config_list_main
                name_key = "configurationName"
                value_key = "configurationValue"
                if not target_config_name.startswith("configuration."):
                     logger.warning(f"Config key '{target_config_name}' does not have expected prefix.")

            target_config = next((item for item in search_list if item.get(name_key) == target_config_name), None)
            if target_config:
                config_value = target_config.get(value_key)
                
            # --- Tier 1 Check ---
            if config_value is not None and str(config_value).strip(): 
                matching_count += 1
            else:
                # Tier 1 Fail: Populate non-compliant report
                filtered_resource = {}
                # --- Corrected report population ---
                for field in fields_for_report:
                     value = "N/A" 
                     if field == "source": # Handle top-level source
                         value = resource.get("source", "N/A")
                     elif field == "configuration.keyState": # Use extracted value
                         value = key_state_val
                     elif field == "configuration.keyManager": # Use extracted value
                         value = key_manager_val
                     elif field.startswith("supplementaryConfiguration."):
                         s_list = resource.get("supplementaryConfiguration", [])
                         item = next((i for i in s_list if i.get("supplementaryConfigurationName") == field), None)
                         # For the target key, use the value already found (config_value)
                         if field == target_config_name: value = config_value if config_value is not None else "N/A (Not Found)"
                         elif item: value = item.get("supplementaryConfigurationValue", "N/A (Key Found, No Value)")
                         else: value = "N/A (Key Not Found)"
                     elif field.startswith("configuration."):
                         c_list = config_list_main
                         item = next((i for i in c_list if i.get("configurationName") == field), None)
                         # For the target key, use the value already found (config_value)
                         if field == target_config_name: value = config_value if config_value is not None else "N/A (Not Found)"
                         elif item: value = item.get("configurationValue", "N/A (Key Found, No Value)")
                         else: value = "N/A (Key Not Found)"
                     elif '.' in field: 
                         parts = field.split('.', 1); parent, child = parts[0], parts[1]
                         value = resource.get(parent, {}).get(child, "N/A")
                     else: # Top-level field
                         value = resource.get(field, "N/A")
                     filtered_resource[field] = value

                # Ensure target key's value is explicitly set (handles case where it's not in fields_for_report)
                filtered_resource[output_config_col_name] = config_value if config_value is not None else "N/A (Not Found)"
                non_matching_resources.append(filtered_resource)
                
        logger.info(f"Tier 1 Check ({config_key} non-empty): Found {matching_count} compliant resources.")
        
        # Ensure DataFrame columns match the requested fields
        report_columns = list(fields_for_report)
        if output_config_col_name not in report_columns:
             report_columns.append(output_config_col_name)
             
        final_df = pd.DataFrame(non_matching_resources)
        # Reindex to ensure all expected columns are present
        final_df = final_df.reindex(columns=report_columns, fill_value="N/A (Column Missing)")
        
        return matching_count, final_df

    def filter_tier2_resources(self, resources: List[Dict], config_key: str, expected_config_value: str, 
                               fields_for_report: List[str]) -> Tuple[int, pd.DataFrame]:
        """Filter resources based on Tier 2 compliance (config_key == expected_config_value).
           Only considers resources where the key exists (implicitly passed Tier 1).
           Checks supplementaryConfiguration or configurationList based on key prefix."""
        matching_count = 0
        non_matching_resources = []
        
        target_config_name = config_key
        output_config_col_name = config_key

        is_bool_expected = str(expected_config_value).upper() in ['TRUE', 'FALSE']
        if is_bool_expected:
            expected_bool_str = str(expected_config_value).upper()
            
        # First, filter to only include resources that would pass Tier 1
        tier1_passing_resources = []
        for resource in resources:
            # Determine where to look for the config key
            if target_config_name.startswith("supplementaryConfiguration."):
                search_list = resource.get("supplementaryConfiguration", [])
                name_key = "supplementaryConfigurationName"
                value_key = "supplementaryConfigurationValue"
            else:
                search_list = resource.get("configurationList", [])
                name_key = "configurationName"
                value_key = "configurationValue"
            
            # Check if config exists and has a non-empty value (Tier 1 check)
            config_item = next((item for item in search_list if item.get(name_key) == target_config_name), None)
            if config_item and config_item.get(value_key) is not None and str(config_item.get(value_key)).strip():
                tier1_passing_resources.append(resource)
                
        logger.info(f"Filtered out {len(resources) - len(tier1_passing_resources)} resources that failed Tier 1 checks")
        
        # Process only resources that passed Tier 1
        for resource in tier1_passing_resources:
            config_value_actual = None 
            key_state_val = "N/A" # Store for reporting
            key_manager_val = "N/A" # Store for reporting
            
            # --- Extract KeyState and KeyManager for reporting ---
            config_list_main = resource.get("configurationList", [])
            for item in config_list_main:
                name = item.get("configurationName")
                if name == "configuration.keyState":
                     key_state_val = item.get("configurationValue", "N/A")
                elif name == "configuration.keyManager":
                     key_manager_val = item.get("configurationValue", "N/A")

            # --- Parsing logic for the target config_key ---
            if target_config_name.startswith("supplementaryConfiguration."):
                search_list = resource.get("supplementaryConfiguration", [])
                name_key = "supplementaryConfigurationName"
                value_key = "supplementaryConfigurationValue"
            else: # Assumes configuration.* or other
                search_list = config_list_main
                name_key = "configurationName"
                value_key = "configurationValue"
                if not target_config_name.startswith("configuration."):
                     logger.warning(f"Config key '{target_config_name}' does not have expected prefix.")

            target_config = next((item for item in search_list if item.get(name_key) == target_config_name), None)
            if target_config:
                config_value_actual = target_config.get(value_key)

            # --- Tier 2 Check: Key must EXIST and have a value ---
            if config_value_actual is not None: 
                # Key exists, now compare the value
                is_match = False
                if is_bool_expected:
                    is_match = str(config_value_actual).upper() == expected_bool_str
                else:
                    is_match = str(config_value_actual) == str(expected_config_value)

                if is_match:
                    matching_count += 1
                else:
                     # --- Tier 2 Fail: Key exists, but value MISMATCH ---
                     # Populate non-compliant report
                    filtered_resource = {}
                    # --- Corrected report population (similar to Tier 1) ---
                    for field in fields_for_report:
                         value = "N/A" 
                         if field == "source": value = resource.get("source", "N/A")
                         elif field == "configuration.keyState": value = key_state_val
                         elif field == "configuration.keyManager": value = key_manager_val
                         elif field.startswith("supplementaryConfiguration."):
                             s_list = resource.get("supplementaryConfiguration", [])
                             item = next((i for i in s_list if i.get("supplementaryConfigurationName") == field), None)
                             if field == target_config_name: value = config_value_actual # Use the value we already found
                             elif item: value = item.get("supplementaryConfigurationValue", "N/A (Key Found, No Value)")
                             else: value = "N/A (Key Not Found)"
                         elif field.startswith("configuration."):
                             c_list = config_list_main
                             item = next((i for i in c_list if i.get("configurationName") == field), None)
                             if field == target_config_name: value = config_value_actual # Use the value we already found
                             elif item: value = item.get("configurationValue", "N/A (Key Found, No Value)")
                             else: value = "N/A (Key Not Found)"
                         elif '.' in field: 
                             parts = field.split('.', 1); parent, child = parts[0], parts[1]
                             value = resource.get(parent, {}).get(child, "N/A")
                         else: # Top-level field
                             value = resource.get(field, "N/A")
                         filtered_resource[field] = value

                    # Ensure target key's value is explicitly set
                    filtered_resource[output_config_col_name] = config_value_actual # Report the actual non-matching value
                    non_matching_resources.append(filtered_resource)
            
        logger.info(f"Tier 2 Check ({config_key} == {expected_config_value}): Found {matching_count} compliant resources")

        report_columns = list(fields_for_report)
        if output_config_col_name not in report_columns:
             report_columns.append(output_config_col_name)
             
        final_df = pd.DataFrame(non_matching_resources)
        # Reindex to ensure all expected columns are present
        final_df = final_df.reindex(columns=report_columns, fill_value="N/A (Column Missing)")

        return matching_count, final_df

    def extract_kms_key_rotation(self, threshold_df: pd.DataFrame) -> pd.DataFrame:
        """Extract KMS Key Rotation compliance data."""
        control_id = "CTRL-1077224"
        current_date = datetime.now()
        
        # Get config for this control
        control_config = KMS_KEY_ROTATION_TIERS.get(control_id, {})
        config_key = control_config.get("rotation_config_key", "supplementaryConfiguration.KeyRotationStatus")
        expected_value = control_config.get("rotation_expected_value", "TRUE")
        tier1_metric_id = control_config.get("tier1_metric_id", "MNTR-1077224-T1")
        tier2_metric_id = control_config.get("tier2_metric_id", "MNTR-1077224-T2")
        
        # Desired fields for reporting
        desired_fields = [
            "accountResourceId", "resourceId", "resourceType", 
            config_key, "configuration.keyState", "configuration.keyManager", "source"
        ]
        
        # Initialize output dataframe
        result_df = pd.DataFrame(columns=METRIC_COLUMNS)
        
        # Set API headers with token
        self.headers['Authorization'] = self._get_api_token()
        
        try:
            # API Payload for KMS Keys
            config_payload = {
                "searchParameters": [{"resourceType": "AWS::KMS::Key"}],
                "responseFields": desired_fields
            }
            
            # Step 1: Fetch all KMS Keys
            logger.info("Fetching all KMS Key configurations...")
            all_resources, fetched_total_count = self.fetch_all_resources(
                config_payload, 
                limit=None,
                timeout=120,
                max_retries=3
            )
            
            if fetched_total_count == 0:
                logger.warning("No KMS Keys found via API. Returning zero metrics.")
                # Create default metrics entries with 0 values
                for tier_metric_id in [tier1_metric_id, tier2_metric_id]:
                    result_df = pd.concat([result_df, pd.DataFrame([{
                        "monitoring_metric_id": int(tier_metric_id.split('-')[-1]),
                        "control_id": control_id,
                        "monitoring_metric_value": 0.0,
                        "monitoring_metric_status": "Red",
                        "metric_value_numerator": 0,
                        "metric_value_denominator": 0,
                        "resources_info": None,
                        "control_monitoring_utc_timestamp": current_date
                    }])], ignore_index=True)
                return result_df
                
            logger.info(f"Successfully fetched {fetched_total_count} total KMS Key configurations.")

            # Step 2: Filter out excluded resources
            in_scope_resources, excluded_df = self.filter_out_of_scope_keys(
                all_resources, 
                desired_fields
            )
            
            final_denominator = len(in_scope_resources) 
            logger.info(f"In-Scope resources: {final_denominator}, Excluded: {len(excluded_df)}")

            # Handle case where no resources are left after filtering
            if final_denominator == 0:
                logger.warning("No resources remaining in scope after filtering. Setting metrics to N/A.")
                tier1_metric = 0.0
                tier2_metric = 0.0
                tier1_numerator = 0
                tier2_numerator = 0
                tier2_denominator = 0
                tier1_resources_info = None
                tier2_resources_info = None
            else:
                # Step 3: Apply Tier 1 compliance filtering
                logger.info(f"Applying Tier 1 compliance filter ({config_key} non-empty)...")
                tier1_numerator, tier1_non_compliant_df = self.filter_tier1_resources(
                    in_scope_resources, 
                    config_key, 
                    desired_fields
                )
                
                # Step 4: Apply Tier 2 compliance filtering
                logger.info(f"Applying Tier 2 compliance filter ({config_key} == '{expected_value}')...")
                tier2_numerator, tier2_non_compliant_df = self.filter_tier2_resources(
                    in_scope_resources, 
                    config_key, 
                    expected_value, 
                    desired_fields
                )
                
                # Calculate metrics
                tier1_metric = tier1_numerator / final_denominator if final_denominator > 0 else 0
                tier2_denominator = tier1_numerator # Tier 2 Denom = Tier 1 Num
                tier2_metric = tier2_numerator / tier2_denominator if tier2_denominator > 0 else 0
                
                # Prepare resources_info for non-compliant resources
                tier1_resources_info = None
                if not tier1_non_compliant_df.empty and len(tier1_non_compliant_df) > 0:
                    tier1_resources_info = []
                    for _, row in tier1_non_compliant_df.head(50).iterrows():  # Limit to 50 for efficiency
                        info = {
                            "resourceId": row.get("resourceId", "N/A"),
                            "accountResourceId": row.get("accountResourceId", "N/A"),
                            "keyState": row.get("configuration.keyState", "N/A"),
                            "rotationStatus": row.get(config_key, "N/A")
                        }
                        tier1_resources_info.append(json.dumps(info))
                
                tier2_resources_info = None
                if not tier2_non_compliant_df.empty and len(tier2_non_compliant_df) > 0:
                    tier2_resources_info = []
                    for _, row in tier2_non_compliant_df.head(50).iterrows():  # Limit to 50 for efficiency
                        info = {
                            "resourceId": row.get("resourceId", "N/A"),
                            "accountResourceId": row.get("accountResourceId", "N/A"),
                            "keyState": row.get("configuration.keyState", "N/A"),
                            "rotationStatus": row.get(config_key, "N/A")
                        }
                        tier2_resources_info.append(json.dumps(info))
            
            # Get thresholds from threshold_df
            filtered_t1_thresholds = threshold_df.loc[
                (threshold_df["control_id"] == control_id) & 
                (threshold_df["monitoring_metric_id"] == int(tier1_metric_id.split('-')[-1]))
            ]
            
            filtered_t2_thresholds = threshold_df.loc[
                (threshold_df["control_id"] == control_id) & 
                (threshold_df["monitoring_metric_id"] == int(tier2_metric_id.split('-')[-1]))
            ]
            
            # Get threshold values or use defaults
            t1_alert = filtered_t1_thresholds["alerting_threshold"].values[0] if not filtered_t1_thresholds.empty else 95.0
            t1_warning = filtered_t1_thresholds["warning_threshold"].values[0] if not filtered_t1_thresholds.empty else 97.0
            
            t2_alert = filtered_t2_thresholds["alerting_threshold"].values[0] if not filtered_t2_thresholds.empty else 95.0
            t2_warning = filtered_t2_thresholds["warning_threshold"].values[0] if not filtered_t2_thresholds.empty else 97.0
            
            # Determine status
            tier1_status = self.get_compliance_status(tier1_metric, t1_alert, t1_warning)
            tier2_status = self.get_compliance_status(tier2_metric, t2_alert, t2_warning)
            
            # Create metrics DataFrame
            tier1_data = {
                "monitoring_metric_id": int(tier1_metric_id.split('-')[-1]),
                "control_id": control_id,
                "monitoring_metric_value": round(tier1_metric * 100, 2),
                "monitoring_metric_status": tier1_status,
                "metric_value_numerator": tier1_numerator,
                "metric_value_denominator": final_denominator,
                "resources_info": tier1_resources_info,
                "control_monitoring_utc_timestamp": current_date
            }
            
            tier2_data = {
                "monitoring_metric_id": int(tier2_metric_id.split('-')[-1]),
                "control_id": control_id,
                "monitoring_metric_value": round(tier2_metric * 100, 2),
                "monitoring_metric_status": tier2_status,
                "metric_value_numerator": tier2_numerator,
                "metric_value_denominator": tier2_denominator,
                "resources_info": tier2_resources_info,
                "control_monitoring_utc_timestamp": current_date
            }
            
            # Combine results
            result_df = pd.concat([result_df, pd.DataFrame([tier1_data, tier2_data])], ignore_index=True)
            
            logger.info(f"Metrics: Tier 1 = {tier1_metric:.2%}, Tier 2 = {tier2_metric:.2%}")
            logger.info(f"Status: Tier 1 = {tier1_status}, Tier 2 = {tier2_status}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in extract_kms_key_rotation: {e}", exc_info=True)
            # Return empty DataFrame on error
            return pd.DataFrame(columns=METRIC_COLUMNS)
    
    def get_compliance_status(self, metric: float, alert_threshold: float, warning_threshold: float = None) -> str:
        """Determine compliance status based on metric and thresholds."""
        metric_percentage = metric * 100
        
        if metric_percentage >= alert_threshold:
            return "Green"
        elif warning_threshold is not None and metric_percentage >= warning_threshold:
            return "Yellow"
        else:
            return "Red"
    
    def extract(self) -> DataFrame:
        """Extract monitoring data."""
        logger.info("Starting extraction for CTRL-1077224 (KMS Key Rotation)")
        df = super().extract()
        return df

@transformer
def calculate_ctrl1077224_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
    """Calculate metrics for CTRL-1077224 (KMS Key Rotation) based on resource configurations and thresholds."""
    control_id = "CTRL-1077224"
    current_date = datetime.now()
    
    # Get control config and metric IDs
    control_config = KMS_KEY_ROTATION_TIERS.get(control_id, {})
    config_key = control_config.get("rotation_config_key", "supplementaryConfiguration.KeyRotationStatus")
    expected_value = control_config.get("rotation_expected_value", "TRUE")
    
    # Get metric IDs from thresholds DataFrame based on tier
    tier1_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                               (thresholds_raw["monitoring_metric_tier"] == "Tier 1")]
    tier2_metrics = thresholds_raw[(thresholds_raw["control_id"] == control_id) & 
                               (thresholds_raw["monitoring_metric_tier"] == "Tier 2")]
    
    if tier1_metrics.empty or tier2_metrics.empty:
        # Fallback to config if metrics not found in thresholds
        tier1_metric_id = control_config.get("tier1_metric_id", "MNTR-1077224-T1")
        tier2_metric_id = control_config.get("tier2_metric_id", "MNTR-1077224-T2")
        logger.warning(f"Missing threshold data in DataFrame, using fallback metric IDs: {tier1_metric_id}, {tier2_metric_id}")
    else:
        tier1_metric_id = tier1_metrics.iloc[0]["monitoring_metric_id"]
        tier2_metric_id = tier2_metrics.iloc[0]["monitoring_metric_id"]
        
    # Convert string metric IDs to int if needed
    tier1_metric_id_int = int(tier1_metric_id.split('-')[-1]) if isinstance(tier1_metric_id, str) else tier1_metric_id
    tier2_metric_id_int = int(tier2_metric_id.split('-')[-1]) if isinstance(tier2_metric_id, str) else tier2_metric_id
    
    # Initialize output dataframe and resource fields
    desired_fields = [
        "accountResourceId", "resourceId", "resourceType", 
        config_key, "configuration.keyState", "configuration.keyManager", "source"
    ]
    
    try:
        # Get API connector from context
        api_connector = context["api_connector"]
        verify_ssl = context["api_verify_ssl"]
        
        # Set up request for KMS keys
        headers = {
            "Accept": "application/json;v=1.0",
            "Authorization": api_connector.api_token,
            "Content-Type": "application/json"
        }
        
        config_payload = {
            "searchParameters": [{"resourceType": "AWS::KMS::Key"}],
            "responseFields": [
                "accountName", "accountResourceId", "amazonResourceName", "resourceId", 
                "resourceType", "configurationList", "configuration.keyManager", "configuration.keyState",
                "supplementaryConfiguration", "supplementaryConfiguration.KeyRotationStatus", 
                "source"
            ]
        }
        
        # Step 1: Fetch all KMS Keys
        logger.info("Fetching all KMS Key configurations...")
        request_kwargs = {
            "headers": headers,
            "json": config_payload,
            "verify": verify_ssl,
            "timeout": 120
        }
        
        try:
            # Use OauthApi to fetch resources
            all_resources = []
            fetched_count = 0
            next_record_key = ""
            
            while True:
                params = {"limit": 10000}
                if next_record_key:
                    params["nextRecordKey"] = next_record_key
                
                curr_request_kwargs = request_kwargs.copy()
                curr_request_kwargs["params"] = params
                
                response = api_connector.send_request(
                    url=api_connector.url,
                    request_type="post",
                    request_kwargs=curr_request_kwargs,
                    retry_delay=5,
                    max_retries=3
                )
                
                if response is None or response.status_code > 299:
                    raise RuntimeError(f"API error: {getattr(response, 'status_code', 'None')}")
                
                data = response.json()
                resources = data.get("resourceConfigurations", [])
                all_resources.extend(resources)
                fetched_count += len(resources)
                
                next_record_key = data.get("nextRecordKey", "")
                if not next_record_key:
                    break
                
                # Small delay to avoid rate limits
                time.sleep(0.15)
                
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise RuntimeError(f"Critical API fetch failure: {str(e)}") from e
            
        fetched_total_count = len(all_resources)
            
        if fetched_total_count == 0:
            logger.warning("No KMS Keys found via API. Returning zero metrics.")
            # Create default metrics entries with 0 values
            now = int(datetime.utcnow().timestamp() * 1000)
            results = [
                {"date": now, "control_id": control_id, "monitoring_metric_id": tier1_metric_id_int, 
                 "monitoring_metric_value": 0.0, "compliance_status": "Red", "numerator": 0, 
                 "denominator": 0, "non_compliant_resources": None},
                {"date": now, "control_id": control_id, "monitoring_metric_id": tier2_metric_id_int, 
                 "monitoring_metric_value": 0.0, "compliance_status": "Red", "numerator": 0, 
                 "denominator": 0, "non_compliant_resources": None}
            ]
            df = pd.DataFrame(results)
            return df
            
        logger.info(f"Successfully fetched {fetched_total_count} total KMS Key configurations.")
        
        # Step 2: Filter resources
        in_scope_resources = []
        excluded_resources = []
        
        # Apply exclusion filters
        for resource in all_resources:
            exclude = False
            
            # Check for orphaned keys
            source_field = resource.get("source")
            if source_field == "CT-AccessDenied":
                exclude = True
                excluded_resources.append(resource)
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
                excluded_resources.append(resource)
            elif key_manager == "AWS":
                exclude = True
                excluded_resources.append(resource)
            else:
                in_scope_resources.append(resource)
                
        # Prepare for Tier 1 and Tier 2 checks
        final_denominator = len(in_scope_resources)
        logger.info(f"In-Scope resources: {final_denominator}, Excluded: {len(excluded_resources)}")
        
        if final_denominator == 0:
            logger.warning("No resources remaining in scope after filtering.")
            tier1_numerator = 0
            tier2_numerator = 0
            tier1_non_compliant = []
            tier2_non_compliant = []
            tier2_denominator = 0
        else:
            # Tier 1 and Tier 2 filtering
            tier1_numerator = 0
            tier2_numerator = 0
            tier1_non_compliant = []
            tier2_non_compliant = []
            
            for resource in in_scope_resources:
                # Check for rotation status
                supp_config_list = resource.get("supplementaryConfiguration", [])
                rotation_item = next((c for c in supp_config_list if c.get("supplementaryConfigurationName") == config_key), None)
                rotation_value = rotation_item.get("supplementaryConfigurationValue") if rotation_item else None
                
                # Tier 1 check: Does the key have a rotation status?
                if rotation_value and str(rotation_value).strip():
                    tier1_numerator += 1
                    # Tier 2 check: Is rotation enabled?
                    if str(rotation_value).upper() == expected_value:
                        tier2_numerator += 1
                    else:
                        non_compliant_info = {
                            "resourceId": resource.get("resourceId", "N/A"),
                            "accountResourceId": resource.get("accountResourceId", "N/A"),
                            "keyState": next((c.get("configurationValue") for c in resource.get("configurationList", []) 
                                          if c.get("configurationName") == "configuration.keyState"), "N/A"),
                            "rotationStatus": rotation_value
                        }
                        tier2_non_compliant.append(non_compliant_info)
                else:
                    non_compliant_info = {
                        "resourceId": resource.get("resourceId", "N/A"),
                        "accountResourceId": resource.get("accountResourceId", "N/A"),
                        "keyState": next((c.get("configurationValue") for c in resource.get("configurationList", []) 
                                      if c.get("configurationName") == "configuration.keyState"), "N/A"),
                        "rotationStatus": "MISSING"
                    }
                    tier1_non_compliant.append(non_compliant_info)
            
            tier2_denominator = tier1_numerator
        
        # Calculate metrics
        tier1_metric = tier1_numerator / final_denominator if final_denominator > 0 else 0
        tier2_metric = tier2_numerator / tier2_denominator if tier2_denominator > 0 else 0
        
        # Get thresholds from DataFrame
        t1_threshold = tier1_metrics.iloc[0] if not tier1_metrics.empty else None
        t2_threshold = tier2_metrics.iloc[0] if not tier2_metrics.empty else None
        
        # Set default thresholds if not found
        t1_alert = t1_threshold["alerting_threshold"] if t1_threshold is not None else 95.0
        t1_warning = t1_threshold["warning_threshold"] if t1_threshold is not None else 97.0
        
        t2_alert = t2_threshold["alerting_threshold"] if t2_threshold is not None else 95.0
        t2_warning = t2_threshold["warning_threshold"] if t2_threshold is not None else 97.0
        
        # Determine compliance status
        tier1_status = get_compliance_status(tier1_metric, t1_alert, t1_warning)
        tier2_status = get_compliance_status(tier2_metric, t2_alert, t2_warning)
        
        # Create final result
        now = int(datetime.utcnow().timestamp() * 1000)
        results = [
            {"date": now, "control_id": control_id, "monitoring_metric_id": tier1_metric_id_int, 
             "monitoring_metric_value": float(tier1_metric * 100), "compliance_status": tier1_status, 
             "numerator": int(tier1_numerator), "denominator": int(final_denominator), 
             "non_compliant_resources": [json.dumps(x) for x in tier1_non_compliant[:50]] if tier1_non_compliant else None},
            {"date": now, "control_id": control_id, "monitoring_metric_id": tier2_metric_id_int, 
             "monitoring_metric_value": float(tier2_metric * 100), "compliance_status": tier2_status, 
             "numerator": int(tier2_numerator), "denominator": int(tier2_denominator), 
             "non_compliant_resources": [json.dumps(x) for x in tier2_non_compliant[:50]] if tier2_non_compliant else None}
        ]
        
        # Create DataFrame with standardized columns
        df = pd.DataFrame(results)
        df["date"] = df["date"].astype("int64")
        df["numerator"] = df["numerator"].astype("int64")
        df["denominator"] = df["denominator"].astype("int64")
        
        logger.info(f"Metrics: Tier 1 = {tier1_metric:.2%}, Tier 2 = {tier2_metric:.2%}")
        logger.info(f"Status: Tier 1 = {tier1_status}, Tier 2 = {tier2_status}")
        
        return df
        
    except Exception as e:
        if not isinstance(e, RuntimeError):
            logger.error(f"Unexpected error in calculate_ctrl1077224_metrics: {str(e)}", exc_info=True)
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