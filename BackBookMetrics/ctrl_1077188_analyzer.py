import os
import time
import json
import requests
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional, Any
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

# API details
API_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
API_HEADERS = {
    'Accept': 'application/json;v=1.0',
    'Authorization': 'Bearer YOUR_API_TOKEN',  # Replace with actual token
    'Content-Type': 'application/json'
}
API_CALL_DELAY = 0.15  # Delay between API calls to avoid rate limits
API_TIMEOUT = 60  # Timeout for API calls

# Snowflake connection parameters using dbutils.secrets
username = dbutils.secrets.get(scope="uno201_scope", key="eid")
password = dbutils.secrets.get(scope="uno201_scope", key="password")

# Define Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = dict(
    sfUrl="prod.us-east-1.capitalone.snowflakecomputing.com",
    sfUser=username,
    sfPassword=password,
    sfDatabase="SB",
    sfSchema="USER_UNO201",
    sfWarehouse="CYBR_Q_DI_WH",
)

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_certificates_from_snowflake():
    """Fetch certificates from Snowflake using Spark."""
    try:
        query = """
        SELECT DISTINCT 
            CERTIFICATE_ARN, 
            CERTIFICATE_ID, 
            NOMINAL_ISSUER, 
            NOT_VALID_BEFORE_UTC_TIMESTAMP, 
            NOT_VALID_AFTER_UTC_TIMESTAMP, 
            LAST_USAGE_OBSERVATION_UTC_TIMESTAMP
        FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE 
        WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%' 
          AND NOMINAL_ISSUER = 'Amazon' 
          AND NOT_VALID_AFTER_UTC_TIMESTAMP >= DATEADD('DAY', -365, CURRENT_TIMESTAMP)
        """
        logger.info("Fetching certificates from Snowflake using Spark...")
        
        # Use Spark to query Snowflake
        snowflake_df = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptions)
            .option("query", query)
            .load()
        )
        
        # Convert all column names to lowercase for consistency
        snowflake_df = snowflake_df.toDF(*[c.lower() for c in snowflake_df.columns])
        
        logger.info(f"Retrieved {snowflake_df.count()} certificates from Snowflake")
        return snowflake_df
    except Exception as e:
        logger.error(f"Error fetching certificates from Snowflake: {e}")
        raise

def fetch_certificates_from_api(payload: Dict) -> List[Dict]:
    """Fetch certificates from the AWS Tooling API with pagination."""
    all_certificates = []
    next_record_key = None
    page_count = 0
    
    logger.info(f"Fetching certificates from API with payload: {json.dumps(payload, indent=2)}")
    
    while True:
        page_count += 1
        current_url = API_URL
        params = {'limit': 10000}
        
        if next_record_key:
            params['nextRecordKey'] = next_record_key
        
        try:
            logger.info(f"Fetching page {page_count} from API...")
            response = requests.post(
                current_url, 
                headers=API_HEADERS, 
                json=payload,
                params=params,
                timeout=API_TIMEOUT, 
                verify=False
            )
            response.raise_for_status()
            data = response.json()
            
            certificates = data.get('resourceConfigurations', [])
            logger.info(f"Retrieved {len(certificates)} certificates from page {page_count}")
            all_certificates.extend(certificates)
            
            next_record_key = data.get('nextRecordKey')
            if not next_record_key:
                logger.info("No more pages to fetch")
                break
                
            # Sleep to avoid rate limits
            time.sleep(API_CALL_DELAY)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed on page {page_count}: {e}")
            # Continue with certificates fetched so far
            break
    
    logger.info(f"Total certificates fetched from API: {len(all_certificates)}")
    return all_certificates

def create_flattened_cert_df(api_certificates: List[Dict]):
    """
    Convert API certificates list to a flattened structure,
    then create a Spark DataFrame. This approach avoids complex SQL expressions.
    """
    if not api_certificates:
        logger.warning("No certificates provided to flatten")
        return spark.createDataFrame([], StructType([
            StructField("amazonresourcename", StringType(), True)
        ]))
    
    # Extract and flatten the certificate data
    flattened_certs = []
    
    try:
        for cert in api_certificates:
            # Start with top-level attributes
            flat_cert = {
                "amazonresourcename": cert.get("amazonResourceName", ""),
                "resourceid": cert.get("resourceId", ""),
                "awsaccountid": cert.get("awsAccountId", ""),
                "awsregion": cert.get("awsRegion", ""),
                "businessapplicationname": cert.get("businessApplicationName", ""),
                "environment": cert.get("environment", ""),
                "source": cert.get("source", "")
            }
            
            # Extract configuration values
            if "configurationList" in cert and isinstance(cert["configurationList"], list):
                for config in cert["configurationList"]:
                    if isinstance(config, dict) and "configurationName" in config and "configurationValue" in config:
                        # Use the configuration name as the column name
                        # Convert to lowercase and replace dots with underscores for valid column names
                        col_name = config["configurationName"].lower().replace(".", "_")
                        flat_cert[col_name] = config["configurationValue"]
            
            # Extract supplementary configuration values
            if "supplementaryConfiguration" in cert and isinstance(cert["supplementaryConfiguration"], list):
                for config in cert["supplementaryConfiguration"]:
                    if isinstance(config, dict) and "supplementaryConfigurationName" in config and "supplementaryConfigurationValue" in config:
                        # Use the configuration name as the column name
                        # Convert to lowercase and replace dots with underscores for valid column names
                        col_name = ("supp_" + config["supplementaryConfigurationName"]).lower().replace(".", "_")
                        flat_cert[col_name] = config["supplementaryConfigurationValue"]
            
            flattened_certs.append(flat_cert)
        
        # Create a Spark DataFrame directly from the flattened dictionaries
        if not flattened_certs:
            logger.warning("No certificates were flattened successfully")
            return spark.createDataFrame([], StructType([
                StructField("amazonresourcename", StringType(), True)
            ]))
            
        # This will infer the schema automatically from the data
        df = spark.createDataFrame(flattened_certs)
        logger.info(f"Successfully created DataFrame with {df.count()} rows and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error flattening certificates: {e}")
        # Return empty DataFrame on error
        return spark.createDataFrame([], StructType([
            StructField("amazonresourcename", StringType(), True)
        ]))

def identify_common_attributes_spark(df_snowflake, df_api):
    """Identify common attributes between dataset and API certificates using Spark."""
    # Convert ARN columns to lowercase for consistency
    df_snowflake = df_snowflake.withColumn("certificate_arn", F.lower(F.col("certificate_arn")))
    df_api = df_api.withColumn("amazonresourcename", F.lower(F.col("amazonresourcename")))
    
    # Get counts for analysis
    snowflake_count = df_snowflake.count()
    api_count = df_api.count()
    
    if snowflake_count == 0 or api_count == 0:
        logger.warning("One or both DataFrames are empty, cannot compare attributes")
        return {
            "matching_count": 0,
            "api_only_count": 0,
            "snowflake_only_count": 0,
            "differences": {},
            "common_configurations": {}
        }
    
    # Find matching certificates using a join
    df_matching = df_api.join(
        df_snowflake,
        df_api.amazonresourcename == df_snowflake.certificate_arn,
        "inner"
    )
    matching_count = df_matching.count()
    
    # Find API-only and Snowflake-only certificates
    df_api_only = df_api.join(
        df_snowflake,
        df_api.amazonresourcename == df_snowflake.certificate_arn,
        "left_anti"  # Equivalent to NOT IN subquery
    )
    api_only_count = df_api_only.count()
    
    df_snowflake_only = df_snowflake.join(
        df_api,
        df_snowflake.certificate_arn == df_api.amazonresourcename,
        "left_anti"  # Equivalent to NOT IN subquery
    )
    snowflake_only_count = df_snowflake_only.count()
    
    logger.info(f"Matching certificates: {matching_count}")
    logger.info(f"API-only certificates: {api_only_count}")
    logger.info(f"Snowflake-only certificates: {snowflake_only_count}")
    
    # Analyze configurations that might differentiate between matching and non-matching
    differences = {}
    common_configurations = {}
    
    # Only analyze columns that have reasonable, non-null values
    # Skip identifier columns
    skip_columns = ["amazonresourcename", "resourceid", "certificate_arn", "certificate_id"]
    
    # Get a list of all columns in the API DataFrame, excluding identifiers
    api_columns = [col for col in df_api.columns if col not in skip_columns]
    
    for col in api_columns:
        # Check if the column has a significant number of non-null values
        non_null_count = df_api.filter(F.col(col).isNotNull()).count()
        if non_null_count / api_count < 0.05:  # Skip columns with less than 5% non-null values
            continue
        
        # Get unique values for matching certificates
        if matching_count > 0:
            matching_values = [row[col] for row in df_matching.select(col).distinct().filter(F.col(col).isNotNull()).collect()]
        else:
            matching_values = []
            
        # Get unique values for non-matching certificates
        if api_only_count > 0:
            non_matching_values = [row[col] for row in df_api_only.select(col).distinct().filter(F.col(col).isNotNull()).collect()]
        else:
            non_matching_values = []
        
        # Check if there are differences in values
        if matching_values and set(matching_values) != set(non_matching_values):
            # Find values that are common in the matching set but rare in the non-matching set
            distinctive_values = []
            
            for val in matching_values:
                if val is None:
                    continue
                    
                if val not in non_matching_values:
                    distinctive_values.append(val)
                elif api_only_count > 0:
                    # Compare frequency of the value in matching vs non-matching
                    matching_freq = df_matching.filter(F.col(col) == val).count() / matching_count
                    non_matching_freq = df_api_only.filter(F.col(col) == val).count() / api_only_count
                    
                    # If the value is 3x more common in matching certs, consider it distinctive
                    if matching_freq > non_matching_freq * 3:
                        distinctive_values.append(val)
            
            if distinctive_values:
                differences[col] = {
                    "matching_values": matching_values,
                    "non_matching_values": non_matching_values,
                    "distinctive_values": distinctive_values
                }
                
                # Look for common values in the matching set
                common_value = None
                
                # If there's only one unique value in matching certificates
                if len(matching_values) == 1:
                    common_value = matching_values[0]
                else:
                    # Get value counts to find the most common value
                    value_counts = df_matching.groupBy(col).count().orderBy(F.desc("count"))
                    
                    if value_counts.count() > 0:
                        top_row = value_counts.first()
                        top_value = top_row[col]
                        top_count = top_row["count"]
                        
                        # If the most common value appears in >80% of matching records
                        if top_count / matching_count > 0.8:
                            common_value = top_value
                
                if common_value:
                    common_configurations[col] = common_value
    
    # Map column names back to original API field names for the API payload
    mapped_common_configs = {}
    for col_name, value in common_configurations.items():
        # Map the flattened column names back to the original API field names
        if col_name.startswith("configuration_"):
            api_field = "configuration." + col_name[13:].replace("_", ".")
            mapped_common_configs[api_field] = value
        elif col_name.startswith("supp_"):
            api_field = "supplementaryConfiguration." + col_name[5:].replace("_", ".")
            mapped_common_configs[api_field] = value
        else:
            # For top-level fields, keep as is
            mapped_common_configs[col_name] = value
    
    return {
        "matching_count": matching_count,
        "api_only_count": api_only_count,
        "snowflake_only_count": snowflake_only_count,
        "differences": differences,
        "common_configurations": mapped_common_configs
    }

def suggest_api_payload(analysis_results):
    """Suggest an improved API payload based on analysis results."""
    # Start with the basic payload
    suggested_payload = {
        "searchParameters": [
            {
                "resourceType": "AWS::ACM::Certificate",
                "configurationItems": [
                    {
                        "configurationName": "issuer",
                        "configurationValue": "Amazon"
                    }
                ]
            }
        ]
    }
    
    # Add additional configurations that seem to be common across dataset certificates
    common_configs = analysis_results.get("common_configurations", {})
    for config_name, config_value in common_configs.items():
        # Skip if already present
        if config_name in ['issuer']:
            continue
            
        # Check if it's a configuration item or a top-level attribute
        if config_name.startswith("configuration."):
            # Add as a configuration item
            suggested_payload["searchParameters"][0]["configurationItems"].append({
                "configurationName": config_name,
                "configurationValue": config_value
            })
        elif config_name.startswith("supplementaryConfiguration."):
            # Add as a supplementary configuration item
            if "supplementaryConfigurationItems" not in suggested_payload["searchParameters"][0]:
                suggested_payload["searchParameters"][0]["supplementaryConfigurationItems"] = []
            
            suggested_payload["searchParameters"][0]["supplementaryConfigurationItems"].append({
                "supplementaryConfigurationName": config_name,
                "supplementaryConfigurationValue": config_value
            })
        else:
            # Add as a top-level attribute (like businessApplicationName)
            suggested_payload["searchParameters"][0][config_name] = config_value
    
    return suggested_payload

def main():
    # 1. Fetch certificates from Snowflake
    try:
        df_snowflake = fetch_certificates_from_snowflake()
        # Cache the DataFrame for faster performance
        df_snowflake.cache()
        print("Snowflake certificates:")
        display(df_snowflake.limit(10))
    except Exception as e:
        logger.error(f"Failed to fetch Snowflake data: {e}")
        # Create an empty DataFrame with the expected schema
        df_snowflake = spark.createDataFrame([], StructType([
            StructField("certificate_arn", StringType(), True),
            StructField("certificate_id", StringType(), True),
            StructField("nominal_issuer", StringType(), True),
            StructField("not_valid_before_utc_timestamp", StringType(), True),
            StructField("not_valid_after_utc_timestamp", StringType(), True),
            StructField("last_usage_observation_utc_timestamp", StringType(), True)
        ]))
    
    # 2. Fetch certificates from API with initial payload
    initial_payload = {
        "searchParameters": [
            {
                "resourceType": "AWS::ACM::Certificate",
                "configurationItems": [
                    {
                        "configurationName": "issuer",
                        "configurationValue": "Amazon"
                    }
                ]
            }
        ]
    }
    
    try:
        # Fetch certificates from API
        api_certificates = fetch_certificates_from_api(initial_payload)
        
        df_api = create_flattened_cert_df(api_certificates)
        # Cache the DataFrame for faster performance
        df_api.cache()
        print("API certificates:")
        display(df_api.limit(10))
    except Exception as e:
        logger.error(f"Failed to fetch API data: {e}")
        df_api = spark.createDataFrame([], StructType([
            StructField("amazonresourcename", StringType(), True)
        ]))
    
    # 3. Compare and analyze certificates
    try:
        snowflake_count = df_snowflake.count()
        api_count = df_api.count()
        
        logger.info(f"Certificate counts - Snowflake: {snowflake_count}, API: {api_count}")
        
        if snowflake_count > 0 and api_count > 0:
            analysis_results = identify_common_attributes_spark(df_snowflake, df_api)
            
            # 4. Suggest improved API payload
            suggested_payload = suggest_api_payload(analysis_results)
            
            # 5. Print results
            print("\n=== Analysis Results ===")
            print(f"Matching certificates: {analysis_results['matching_count']}")
            print(f"API-only certificates: {analysis_results['api_only_count']}")
            print(f"Snowflake-only certificates: {analysis_results['snowflake_only_count']}")
            
            print("\n=== Common Configurations ===")
            for config, value in analysis_results['common_configurations'].items():
                print(f"{config}: {value}")
            
            print("\n=== Suggested API Payload ===")
            print(json.dumps(suggested_payload, indent=2))
            
            # 6. Save results to tables for better viewing in Databricks
            try:
                # Convert the analysis results to a Spark DataFrame
                diff_rows = []
                for col_name, diff in analysis_results['differences'].items():
                    diff_rows.append({
                        "column_name": col_name,
                        "matching_values": str(diff["matching_values"]),
                        "non_matching_values": str(diff["non_matching_values"]),
                        "distinctive_values": str(diff["distinctive_values"])
                    })
                
                if diff_rows:
                    diff_df = spark.createDataFrame(diff_rows)
                    print("\n=== Differences Analysis ===")
                    display(diff_df)
            except Exception as e:
                logger.error(f"Error creating differences DataFrame: {e}")
            
            # Save the suggested payload for reference
            try:
                with open("/tmp/suggested_payload.json", "w") as f:
                    json.dump(suggested_payload, indent=2, fp=f)
                print(f"\nResults saved to /tmp/suggested_payload.json")
            except Exception as e:
                print(f"Could not save payload to file: {str(e)}")
                print("Suggested payload (copy from here):")
                print(json.dumps(suggested_payload, indent=2))
            
            # Cache dataframes for faster access without creating temporary views
            df_snowflake.cache()
            df_api.cache()
            
            return {
                "snowflake_df": df_snowflake,
                "api_df": df_api,
                "analysis_results": analysis_results,
                "suggested_payload": suggested_payload
            }
        else:
            logger.error(f"Cannot perform analysis because one or both datasets are empty. Snowflake count: {snowflake_count}, API count: {api_count}")
            return {
                "snowflake_df": df_snowflake,
                "api_df": df_api,
                "analysis_results": {
                    "matching_count": 0,
                    "api_only_count": 0,
                    "snowflake_only_count": 0,
                    "differences": {},
                    "common_configurations": {}
                },
                "suggested_payload": {
                    "searchParameters": [
                        {
                            "resourceType": "AWS::ACM::Certificate",
                            "configurationItems": [
                                {
                                    "configurationName": "issuer",
                                    "configurationValue": "Amazon"
                                }
                            ]
                        }
                    ]
                }
            }
    except Exception as e:
        logger.error(f"Error during analysis stage: {e}")
        return None

# Run the main function if this is executed directly
if __name__ == "__main__":
    results = main()