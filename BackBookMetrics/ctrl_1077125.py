#Connecting to snowflake, necessary for querying metric thresholds
#username = dbutils.secrets.get(scope="uno201", key="eid") #NOTE: Using Chris' scope
#password = dbutils.secrets.get(scope="uno201", key="password")

username = dbutils.secrets.get(scope="sdd802_scope", key="eid") #NOTE: Using Aditya's scope
password = dbutils.secrets.get(scope="sdd802_scope", key="password")

# Define Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = dict(
sfUrl= "cptlone-sfprod.snowflakecomputing.com", #REGION-SPECIFIC CONNECTION: prod.us-east-1.capitalone.snowflakecomputing.com",
sfUser=username,
sfPassword=password,
sfDatabase="SB",
#sfSchema="USER_UNO201", #NOTE: Using Chris's credentials
sfSchema ="USER_SDD802", #NOTE: Using Aditya's credentials
sfWarehouse="CYBR_Q_DI_WH"
)

#Import necessary packages
import requests
import random
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
from IPython.display import display, HTML
import sys
from pyspark.sql import SparkSession

# Configure logging for both file and console output in Databricks environment.
def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# API Endpoints and Auth
AUTH_TOKEN = ""
BASE_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
SUMMARY_URL = f"{BASE_URL}/summary-view"
CONFIG_URL = f"{BASE_URL}/search-resource-configurations"

HEADERS = {
    'Accept': 'application/json;v=1.0',
    'Authorization': f'Bearer {AUTH_TOKEN}',
    'Content-Type': 'application/json'
}

def get_summary_count(payload: Dict, timeout: int = 30, max_retries: int = 2) -> Optional[int]:
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
                headers=HEADERS, 
                json=payload,  # Use json= for consistency
                verify=False,
                timeout=timeout
            )
            
            if response.status_code == 200: #200 is a succesful response code
                data = response.json()
                level1_list = data.get("level1List", [])
                if not level1_list:
                    logger.warning("Empty level1List in response")
                    return 0
                    
                count = level1_list[0].get("level1ResourceCount", 0)
                logger.info(f"Summary count: {count}")
                return count
                
            elif response.status_code == 429: #HTTP Response Code for Too Many Requests
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

def fetch_all_resources(payload: Dict, limit: Optional[int] = None, 
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
            "resourceType", "configurationList", "configuration.keyManager", "configuration.keyState",
            "configuration.origin", "source"
        ],
    }
    
    logger.info(f"Fetching resources. Request Body payload: {json.dumps(fetch_payload, indent=2)}")
    
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
            # if 'limit' in current_page_params: del current_page_params['limit'] 
        
        # The request URL is always the base config URL now
        request_url = CONFIG_URL 

        page_fetched_successfully = False
        for retry in range(max_retries + 1):
            try:
                # Log the URL and params being used for this attempt
                logger.info(f"Requesting page {page_count + 1} with URL params: {current_page_params}" + (f" (retry {retry})" if retry > 0 else ""))
                
                # --- Make the API call using the 'params' argument ---
                response = requests.post(
                    request_url,
                    headers=HEADERS,
                    json=fetch_payload, # The main search criteria go in the body
                    params=current_page_params, # Limit and pagination key go in URL params
                    verify=True, # Recommended: Set to True and manage certs
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
                             logger.debug(f"Sample resource configList structure (first item): {sample_resource['configurationList'][0] if sample_resource['configurationList'] else 'Empty'}")
                        if "supplementaryConfiguration" in sample_resource:
                             logger.debug(f"Sample resource supplementaryConfiguration structure (first item): {sample_resource['supplementaryConfiguration'][0] if sample_resource['supplementaryConfiguration'] else 'Empty'}")


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
                logger.error(f"Exception fetching page {page_count + 1} (Attempt {retry+1}/{max_retries}): {str(e)}", exc_info=True)
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

def filter_out_of_scope_keys(resources: List[Dict], desired_fields_for_report: List[str]) -> Tuple[List[Dict], pd.DataFrame]:
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

    logger.info(f"Exclusion filtering complete. In-scope: {len(in_scope_resources)}, Excluded: {len(excluded_resources_data)}")
    
    if not excluded_df.empty:
        logger.debug("Exclusion reasons summary:")
        logger.debug(excluded_df['exclusionReason'].value_counts().to_string())
        
    return in_scope_resources, excluded_df

def filter_tier1_resources(resources: List[Dict], config_key: str, fields_for_report: List[str]) -> Tuple[int, pd.DataFrame]:
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
                 logger.warning(f"Config key '{target_config_name}' does not have expected prefix. Assuming 'configurationList'. Resource ID: {resource.get('resourceId', 'N/A')}")

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

.def filter_tier2_resources(resources: List[Dict], config_key: str, expected_config_value: str, fields_for_report: List[str]) -> Tuple[int, pd.DataFrame]:
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

    for resource in resources:
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
                 logger.warning(f"Config key '{target_config_name}' does not have expected prefix. Assuming 'configurationList'. Resource ID: {resource.get('resourceId', 'N/A')}")

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
        # else:
            # If config_value_actual is None, the key didn't exist. 
            # This resource already failed Tier 1. Do NOT add to Tier 2 non-compliant list.
            # matching_count remains unchanged.
            
    logger.info(f"Tier 2 Check ({config_key} == {expected_config_value}): Found {matching_count} compliant resources (out of those with the key present).")

    report_columns = list(fields_for_report)
    if output_config_col_name not in report_columns:
         report_columns.append(output_config_col_name)
         
    final_df = pd.DataFrame(non_matching_resources)
    # Reindex to ensure all expected columns are present
    final_df = final_df.reindex(columns=report_columns, fill_value="N/A (Column Missing)")

    return matching_count, final_df

def load_thresholds(spark: SparkSession, metric_ids: List[str]) -> pd.DataFrame:
    """Load compliance thresholds from Snowflake for specific metric IDs."""
    
    if not metric_ids:
        logger.warning("No metric IDs provided to load_thresholds. Returning empty DataFrame.")
        return pd.DataFrame()
        
    # Format metric IDs for SQL IN clause 
    # Create list of properly quoted strings: ['id1', 'id2'] -> ["'id1'", "'id2'"]
    safe_metric_ids = []
    for mid in metric_ids:
        if mid: # Ensure mid is not None or empty before processing
            # 1. Clean the ID: remove internal single quotes
            cleaned_mid = str(mid).replace("'", "") 
            # 2. Wrap the cleaned ID in literal single quotes for SQL
            quoted_id = f"'{cleaned_mid}'" 
            safe_metric_ids.append(quoted_id)
    
    if not safe_metric_ids:
         logger.warning("No valid metric IDs remaining after validation. Returning empty DataFrame.")
         return pd.DataFrame()
         
    # --- !!! CORRECTLY FORMAT THE STRING FOR THE IN CLAUSE !!! ---
    # Join the quoted strings with a comma: "'id1', 'id2'"
    metric_ids_sql_string = ", ".join(safe_metric_ids) 
    
    # Construct the dynamic query using the correctly formatted string
    query = f"""
    SELECT MONITORING_METRIC_ID, ALERT_THRESHOLD, WARNING_THRESHOLD 
    FROM CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
    WHERE MONITORING_METRIC_ID IN ({metric_ids_sql_string}) 
    """

    # Log the *exact* query being sent
    logger.info(f"Loading thresholds from Snowflake with query:\n{query}") 
    
    try:
        thresholds_spark_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("query", query) \
            .load()
        
        thresholds_pd_df = thresholds_spark_df.toPandas() 
        logger.info(f"Threshold data collected from Snowflake. Found {len(thresholds_pd_df)} rows.")
        
        if not thresholds_pd_df.empty:
             thresholds_pd_df.columns = [col.upper() for col in thresholds_pd_df.columns]
             required_cols = ["MONITORING_METRIC_ID", "ALERT_THRESHOLD", "WARNING_THRESHOLD"]
             if not all(col in thresholds_pd_df.columns for col in required_cols):
                  logger.error(f"Snowflake response missing required columns. Expected: {required_cols}, Got: {list(thresholds_pd_df.columns)}")
                  return pd.DataFrame() 
                  
             logger.debug(f"Thresholds loaded:\n{thresholds_pd_df.to_string()}")
             return thresholds_pd_df
        else:
             logger.warning("No threshold data found in Snowflake for the specified metric IDs.")
             return pd.DataFrame() 
             
    except Exception as e:
        # Log the specific query that failed along with the error
        logger.error(f"Failed to load thresholds from Snowflake using query:\n{query}\nError: {e}", exc_info=True) 
        return pd.DataFrame()

def get_compliance_status(metric: float, alert_threshold: float, warning_threshold: Optional[float] = None) -> str:
    """Determine compliance status based on metric and thresholds.
    
    Args:
        metric: The compliance metric as a decimal (0-1)
        alert_threshold: The alert threshold as a whole number percentage (0-100)
        warning_threshold: Optional warning threshold as a whole number percentage (0-100)
    """

    if alert_threshold is None:
        logger.warning("Alert threshold is missing (None). Compliant status cannot be determined, returning UNKNOWN.")
        return "UNKNOWN"
    metric_percentage = metric * 100
    
    if metric_percentage >= alert_threshold:
        return "GREEN"
    elif warning_threshold is not None and metric_percentage >= warning_threshold:
        return "YELLOW"
    else:
        return "RED" 

def main():
    """Main pipeline for KMS Origin Compliance Monitoring (CTRL-1077125)."""
    control_id = "CTRL-1077125"
    logger.info(f"*** Starting Monitoring Run for {control_id} ***")

    # Initialize Spark session 
    try:
        spark = SparkSession.builder.appName(f"KMS_Origin_Monitor_{control_id}").getOrCreate()
        logger.info("Spark session initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}", exc_info=True)
        return # Cannot proceed without Spark for thresholds

    # --- Configuration specific to Origin Check ---
    resource_config = {
        "control_name": f"KMS Origin Check ({control_id})",
        "resource_type": "AWS::KMS::Key",
        # Note: Use the precise key name from the API response structure
        "config_key": "configuration.origin", 
        "config_value": "AWS_KMS", # Expected value for Tier 2 compliance (as string for robust comparison)
        # Fields needed for reporting AND for exclusion filtering
        "desired_fields_for_report": [
            "accountResourceId", "resourceId", "resourceType", 
            "configuration.origin", # The key being checked
            "configuration.keyState", # Needed for exclusion
            "configuration.keyManager", # Needed for exclusion
            "source" # Needed for exclusion
        ],
        "tier1_metric_id": "MNTR-1077125-T1", # <<< REPLACE with ACTUAL Metric ID for T1
        "tier2_metric_id": "MNTR-1077125-T2", # <<< REPLACE with ACTUAL Metric ID for T2
        "full_limit": None,        # None = fetch all, set number for testing (e.g., 50000)
        "timeout": 120,            # API request timeout
        "max_retries": 3           # Max retries for failed API requests
    }
    logger.info(f"Configuration loaded for {resource_config['control_name']}")
    logger.debug(f"Resource Config: {json.dumps(resource_config, indent=2)}")

    # --- API Payload Definitions ---
    # Payload to fetch detailed configurations for ALL keys
    config_payload = {
        "searchParameters": [{"resourceType": resource_config["resource_type"]}],
        "responseFields": list(set(resource_config["desired_fields_for_report"])) # Unique fields
    }
    
    # Step 1: Fetch ALL relevant resources 
    logger.info("Step 1: Fetching all KMS Key configurations...")
    all_resources, fetched_total_count = fetch_all_resources(
        config_payload, 
        limit=resource_config['full_limit'],
        timeout=resource_config['timeout'],
        max_retries=resource_config['max_retries']
    )
    
    if fetched_total_count == 0:
        logger.warning("No KMS Keys found via API. Check API endpoint or search parameters. Exiting.")
        return
        
    logger.info(f"Successfully fetched {fetched_total_count} total KMS Key configurations.")

    # Step 2: Filter out excluded resources
    logger.info("Step 2: Filtering fetched resources for exclusions...")
    in_scope_resources, excluded_resources_df = filter_out_of_scope_keys(
        all_resources, 
        resource_config["desired_fields_for_report"]
    )
    
    final_denominator = len(in_scope_resources) 
    logger.info(f"Filtering complete. In-Scope resources: {final_denominator}, Excluded: {len(excluded_resources_df)}")

    # Handle case where no resources are left after filtering
    if final_denominator == 0:
        logger.warning("No resources remaining in scope after filtering. Setting metrics to N/A.")
        tier1_numerator = 0
        tier2_numerator = 0
        tier1_non_compliant_df = pd.DataFrame(columns=resource_config["desired_fields_for_report"])
        tier2_non_compliant_df = pd.DataFrame(columns=resource_config["desired_fields_for_report"])
        tier2_denominator = 0
    else:
        # Step 3: Apply Tier 1 & Tier 2 compliance filtering ONLY to IN-SCOPE resources
        logger.info(f"Step 3: Applying Tier 1 compliance filter ({resource_config['config_key']} non-empty) to {final_denominator} in-scope resources...")
        tier1_numerator, tier1_non_compliant_df = filter_tier1_resources(
            in_scope_resources, 
            resource_config["config_key"], 
            resource_config["desired_fields_for_report"]
        )
        
        logger.info(f"Step 3b: Applying Tier 2 compliance filter ({resource_config['config_key']} == '{resource_config['config_value']}') to in-scope resources...")
        tier2_numerator, tier2_non_compliant_df = filter_tier2_resources(
            in_scope_resources, 
            resource_config["config_key"], 
            resource_config["config_value"], 
            resource_config["desired_fields_for_report"]
        )
        tier2_denominator = tier1_numerator # Tier 2 Denom = Tier 1 Num

    logger.info(f"Compliance counts: Tier 1 Compliant (Numerator) = {tier1_numerator}, Tier 2 Compliant (Numerator) = {tier2_numerator}")
    logger.info(f"Denominators: Tier 1 Denom = {final_denominator}, Tier 2 Denom = {tier2_denominator}")

    # Step 4: Load thresholds and calculate compliance status
    logger.info("Step 4: Loading thresholds and calculating compliance...")
    metric_ids_to_load = [resource_config["tier1_metric_id"], resource_config["tier2_metric_id"]]
    thresholds_pd_df = load_thresholds(spark, metric_ids_to_load)

    logger.info(f"Is thresholds_pd_df empty? {thresholds_pd_df.empty}")
    if not thresholds_pd_df.empty:
        logger.info(f"Columns in thresholds_pd_df: {thresholds_pd_df.columns.tolist()}")
        #Log the first few rows to see the actual data and column names
        logger.info(f"Thresholds DataFrame Head:\n{thresholds_pd_df.head().to_string()}")
    else:
        logger.warning("Thresholds DataFrame is empty after calling load_thresholds.")
    
    # Function to safely get threshold value (same as in other script)
    def get_threshold_value(df, metric_id, threshold_type):
        if df is None or df.empty:
            logger.warning(f"Cannot get threshold for {metric_id}/{threshold_type} because the input DataFrame is empty or None.")
            return None
        
        #Check if the essential column exists *before* trying to filter
        required_col = "MONITORING_METRIC_ID"
        if required_col not in df.columns:
            logger.error(f"Required column '{required_col}' not found in threshold DataFrame! Available columns: {df.columns.tolist()}")
            return None #Cannot proceed without this column
        
        row = df[df[required_col] == metric_id] # Use variable required_col
        if not row.empty:
            # Check if the target threshold_type column exists
            if threshold_type not in row.columns:
                 logger.warning(f"Column '{threshold_type}' not found in threshold DataFrame for metric {metric_id}. Columns: {row.columns.tolist()}")
                 return None
                 
            value = row.iloc[0][threshold_type]
            # Convert to float if possible, handle various null representations
            if value is None or str(value).upper() in ["NULL", "NONE", ""]:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert threshold '{threshold_type}' value '{value}' for {metric_id} to float. Treating as None.")
                return None
                
        # If no row matched the metric_id after filtering
        logger.warning(f"Threshold definition not found for metric_id '{metric_id}' within the loaded DataFrame.")
        return None # Not found

    # Get specific thresholds
    t1_alert = get_threshold_value(thresholds_pd_df, resource_config["tier1_metric_id"], "ALERT_THRESHOLD")
    t1_warn = get_threshold_value(thresholds_pd_df, resource_config["tier1_metric_id"], "WARNING_THRESHOLD")
    t2_alert = get_threshold_value(thresholds_pd_df, resource_config["tier2_metric_id"], "ALERT_THRESHOLD")
    t2_warn = get_threshold_value(thresholds_pd_df, resource_config["tier2_metric_id"], "WARNING_THRESHOLD")
    logger.info(f"Thresholds Used: T1 Alert={t1_alert}, T1 Warn={t1_warn}, T2 Alert={t2_alert}, T2 Warn={t2_warn}")
        
    # Calculate metrics
    tier1_metric = tier1_numerator / final_denominator if final_denominator > 0 else 0
    tier2_metric = tier2_numerator / tier2_denominator if tier2_denominator > 0 else 0 

    # Determine status
    tier1_status = get_compliance_status(tier1_metric, t1_alert, t1_warn)
    tier2_status = get_compliance_status(tier2_metric, t2_alert, t2_warn)
    logger.info(f"Metrics: Tier 1 = {tier1_metric:.2%}, Tier 2 = {tier2_metric:.2%}")
    logger.info(f"Status: Tier 1 = {tier1_status}, Tier 2 = {tier2_status}")

    # Step 5: Format results into monitoring DataFrame
    logger.info("Step 5: Creating final monitoring DataFrame...")
    monitoring_data = [
        {
            "DATE": datetime.utcnow(),
            "CTRL_ID": "CTRL-1077125",
            "MONITORING_METRIC_NUMBER": resource_config["tier1_metric_id"],
            "MONITORING_METRIC": round(tier1_metric * 100, 4),
            "COMPLIANCE_STATUS": tier1_status,
            "NUMERATOR": tier1_numerator,
            "DENOMINATOR": final_denominator 
        },
        {
            "DATE": datetime.utcnow(),
            "CTRL_ID": "CTRL-1077125",
            "MONITORING_METRIC_NUMBER": resource_config["tier2_metric_id"],
            "MONITORING_METRIC": round(tier2_metric * 100, 4),
            "COMPLIANCE_STATUS": tier2_status,
            "NUMERATOR": tier2_numerator,
            "DENOMINATOR": tier2_denominator 
        }
    ]
    monitoring_df = pd.DataFrame(monitoring_data)

    # Step 6: Output detailed compliance reports and metrics
    logger.info("Step 6: Reporting results...")
    print(f"\n--- Final Compliance Report for: {resource_config['control_name']} ---")
    print(f"\nTotal Fetched Resources: {fetched_total_count}")
    print(f"Total Excluded Resources: {len(excluded_resources_df)}")
    print(f"Total In-Scope Resources (Tier 1 Denominator): {final_denominator}")
    print(f"Total Tier 1 Compliant Resources (Tier 2 Denominator): {tier1_numerator}")
    
    tier1_non_compliant_header = f"\nTier 1 Non-compliant (In-Scope) Resources ({resource_config['config_key']} missing or empty):"
    print(tier1_non_compliant_header)
    if not tier1_non_compliant_df.empty:
         print(f"(Showing first 50 of {len(tier1_non_compliant_df)} non-compliant resources)")
         # In Databricks use: display(tier1_non_compliant_df)
         print(tier1_non_compliant_df.head(50).to_string())
         # Consider saving: tier1_non_compliant_df.to_csv("tier1_noncompliant_origin.csv", index=False)
    else:
        print("None found (All in-scope resources passed Tier 1)")
    
    tier2_non_compliant_header = f"\nTier 2 Non-compliant (In-Scope) Resources ({resource_config['config_key']} != {resource_config['config_value']}):"
    print(tier2_non_compliant_header)
    if not tier2_non_compliant_df.empty:
         print(f"(Showing first 50 of {len(tier2_non_compliant_df)} non-compliant resources)")
         # In Databricks use: display(tier2_non_compliant_df)
         print(tier2_non_compliant_df.head(50).to_string())
         # Consider saving: tier2_non_compliant_df.to_csv("tier2_noncompliant_origin.csv", index=False)
    else:
        print("None found (All in-scope resources meeting Tier 1 criteria also met Tier 2)")
    
    print("\n--- Monitoring Metrics Summary ---")

    print(monitoring_df.to_string())

    logger.info(f"*** Monitoring Run for {control_id} Completed ***")

    monitoring_spark_df = spark.createDataFrame(monitoring_df)
    display(monitoring_spark_df)

    monitoring_spark_df.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", "CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING") \
    .mode("APPEND") \
    .save()

if __name__ == "__main__":
    main()