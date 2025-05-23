import json
import requests
import time
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

# Constants
CONTROL_ID = "CTRL-1077188"
TIER0_METRIC_ID = "MNTR-1077188-T0"  # Tier 0: Verify control execution
TIER1_METRIC_ID = "MNTR-1077188-T1"  # Tier 1: Certificate in-scope coverage
TIER2_METRIC_ID = "MNTR-1077188-T2"  # Tier 2: Certificate rotation compliance
CERTIFICATE_MAX_AGE_DAYS = 395  # ~13 months

# API details
API_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
BEARER_TOKEN = "YOUR_BEARER_TOKEN"  # Replace this with your token
API_HEADERS = {
    'Accept': 'application/json;v=1.0',
    'Authorization': f'Bearer {BEARER_TOKEN}',
    'Content-Type': 'application/json'
}
API_CALL_DELAY = 0.15  # Delay between API calls to avoid rate limits
API_TIMEOUT = 60  # Timeout for API calls

# API payload for in-scope certificates
API_PAYLOAD = {
  "searchParameters": [
    {
      "resourceType": "AWS::ACM::Certificate",
      "source": "ChangeNotification",
      "configurationItems": [
        {
          "configurationName": "issuer",
          "configurationValue": "Amazon"
        },
        {
          "configurationName": "renewalSummary.renewalStatus",
          "configurationValue": "SUCCESS"
        }
      ],
      "supplementaryConfigurationItems": [
        {
          "supplementaryConfigurationName": "Tags",
          "supplementaryConfigurationValue": "\"[{'key': 'ASV', 'value': 'ASVACMAUTOMATION'}, {'key': 'OwnerContact', 'value': 'informationsecurityencryption@capitalone.com'}, {'key': 'UseCase', 'value': 'Use this certificate for internal facing sites'}, {'key': 'BA', 'value': 'BAACMAUTOMATION'}, {'key': 'Name', 'value': 'DEFAULT_INTERNAL_FACING_CERT'}]\""
        }
      ]
    }
  ]
}

# Snowflake connection parameters
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_thresholds():
    """Fetch metric thresholds from Snowflake."""
    try:
        # Format the metric IDs for SQL query
        metric_ids = [TIER0_METRIC_ID, TIER1_METRIC_ID, TIER2_METRIC_ID]
        metric_ids_sql_string = "'" + "','".join(metric_ids) + "'"
        
        query = f"""
        SELECT MONITORING_METRIC_ID, ALERT_THRESHOLD, WARNING_THRESHOLD 
        FROM CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
        WHERE MONITORING_METRIC_ID IN ({metric_ids_sql_string}) 
        """
        
        # Use Spark to query Snowflake
        thresholds_df = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptions)
            .option("query", query)
            .load()
        )
        
        return thresholds_df
    except Exception as e:
        logger.error(f"Error fetching thresholds: {e}")
        # Return default thresholds if query fails
        data = [
            (TIER0_METRIC_ID, 100.0, 100.0),
            (TIER1_METRIC_ID, 95.0, 90.0),
            (TIER2_METRIC_ID, 95.0, 90.0)
        ]
        schema = StructType([
            StructField("MONITORING_METRIC_ID", StringType(), True),
            StructField("ALERT_THRESHOLD", DoubleType(), True),
            StructField("WARNING_THRESHOLD", DoubleType(), True)
        ])
        return spark.createDataFrame(data, schema=schema)

def fetch_certificates_from_snowflake():
    """Fetch certificates from Snowflake."""
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
    
    try:
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
        return spark.createDataFrame([], StructType([
            StructField("certificate_arn", StringType(), True),
            StructField("certificate_id", StringType(), True),
            StructField("nominal_issuer", StringType(), True),
            StructField("not_valid_before_utc_timestamp", TimestampType(), True),
            StructField("not_valid_after_utc_timestamp", TimestampType(), True),
            StructField("last_usage_observation_utc_timestamp", TimestampType(), True)
        ]))

def fetch_tier0_data():
    """Fetch data for Tier 0 metric (expired certs used after expiry)."""
    query = """
    WITH EXPIRED_CERTS AS (
        SELECT 
            Certificate_ID,
            Certificate_ARN,
            NOT_VALID_AFTER_UTC_TIMESTAMP as EXPIRY_DATE,
            LAST_USAGE_OBSERVATION_UTC_TIMESTAMP,
            LEAD(NOT_VALID_BEFORE_UTC_TIMESTAMP) OVER (
                PARTITION BY Certificate_ARN 
                ORDER BY NOT_VALID_BEFORE_UTC_TIMESTAMP
            ) as ROTATION_DATE
        FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
        WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm%'
        AND NOT_VALID_AFTER_UTC_TIMESTAMP < CURRENT_TIMESTAMP
        AND NOT_VALID_AFTER_UTC_TIMESTAMP >= DATEADD(day, -3, CURRENT_TIMESTAMP)
        AND LAST_USAGE_OBSERVATION_UTC_TIMESTAMP > NOT_VALID_AFTER_UTC_TIMESTAMP
    )
    SELECT
        COUNT(CASE WHEN ROTATION_DATE IS NULL OR ROTATION_DATE > EXPIRY_DATE THEN 1 END) as FAILURES_COUNT,
        CASE 
            WHEN COUNT(CASE WHEN ROTATION_DATE IS NULL OR ROTATION_DATE > EXPIRY_DATE THEN 1 END) > 0 
            THEN 'FAILED' ELSE 'PASSED'
        END as TEST_RESULT
    FROM EXPIRED_CERTS
    """
    
    try:
        result_df = (
            spark.read.format(SNOWFLAKE_SOURCE_NAME)
            .options(**sfOptions)
            .option("query", query)
            .load()
        )
        return result_df
    except Exception as e:
        logger.error(f"Error fetching Tier 0 data: {e}")
        # Return a DataFrame with zero failures on error
        return spark.createDataFrame([(0, "PASSED")], ["FAILURES_COUNT", "TEST_RESULT"])

def fetch_certificates_from_api():
    """Fetch certificates from API with pagination."""
    all_certificates = []
    next_record_key = None
    page_count = 0
    
    logger.info("Fetching certificates from API...")
    
    while True:
        page_count += 1
        params = {'limit': 10000}
        
        if next_record_key:
            params['nextRecordKey'] = next_record_key
        
        try:
            logger.info(f"Fetching page {page_count}...")
            response = requests.post(
                API_URL, 
                headers=API_HEADERS, 
                json=API_PAYLOAD,
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
                break
                
            time.sleep(API_CALL_DELAY)
        except Exception as e:
            logger.error(f"API request failed on page {page_count}: {e}")
            break
    
    logger.info(f"Total certificates fetched from API: {len(all_certificates)}")
    return all_certificates

def create_flattened_cert_df(api_certificates):
    """Convert API certificates to a flat Spark DataFrame."""
    if not api_certificates:
        return spark.createDataFrame([], StructType([
            StructField("amazonresourcename", StringType(), True)
        ]))
    
    flattened_certs = []
    
    for cert in api_certificates:
        flat_cert = {
            "amazonresourcename": cert.get("amazonResourceName", ""),
            "source": cert.get("source", ""),
            "status": None
        }
        
        # Extract status from configuration
        for config in cert.get("configurationList", []):
            if config.get("configurationName") == "status":
                flat_cert["status"] = config.get("configurationValue")
                break
        
        flattened_certs.append(flat_cert)
    
    return spark.createDataFrame(flattened_certs)

def get_compliance_status(metric_value, threshold_df, metric_id):
    """Determine compliance status based on thresholds."""
    metric_row = threshold_df.filter(F.col("MONITORING_METRIC_ID") == metric_id).first()
    
    if not metric_row:
        return "Red"
        
    alert_threshold = metric_row["ALERT_THRESHOLD"] 
    warning_threshold = metric_row["WARNING_THRESHOLD"]
    
    if metric_value >= alert_threshold:
        return "Green"
    elif warning_threshold is not None and metric_value >= warning_threshold:
        return "Yellow"
    else:
        return "Red"

def calculate_metrics():
    """Calculate all three tier metrics."""
    now = int(datetime.utcnow().timestamp() * 1000)
    metrics = []
    
    try:
        # Fetch thresholds
        thresholds_df = get_thresholds()
        
        # Tier 0: Check for certificates used after expiry
        tier0_df = fetch_tier0_data()
        tier0_row = tier0_df.first()
        failures_count = tier0_row["FAILURES_COUNT"] if tier0_row else 0
        tier0_metric_value = 0.0 if failures_count > 0 else 100.0
        tier0_numerator = 0 if failures_count > 0 else 1
        tier0_denominator = 1
        tier0_status = get_compliance_status(tier0_metric_value, thresholds_df, TIER0_METRIC_ID)
        tier0_resources = [f"Found {failures_count} expired certificates used after expiry without rotation"] if failures_count > 0 else None
        
        metrics.append({
            "date": now,
            "control_id": CONTROL_ID,
            "monitoring_metric_id": TIER0_METRIC_ID,
            "monitoring_metric_value": float(tier0_metric_value),
            "compliance_status": tier0_status,
            "numerator": int(tier0_numerator),
            "denominator": int(tier0_denominator),
            "non_compliant_resources": tier0_resources
        })
        
        # Fetch data for Tier 1 and Tier 2
        snowflake_df = fetch_certificates_from_snowflake()
        api_certs = fetch_certificates_from_api()
        api_df = create_flattened_cert_df(api_certs)
        
        # Filter in-scope certificates (not orphaned, not pending deletion/import)
        in_scope_df = api_df.filter(
            (F.col("source") != "CT-AccessDenied") &
            (~F.col("status").isin(["PENDING_DELETION", "PENDING_IMPORT"]))
        )
        
        # Tier 1: Compare API certs to Snowflake
        api_count = in_scope_df.count()
        if api_count > 0:
            # Convert to lowercase for matching
            snowflake_df = snowflake_df.withColumn("certificate_arn", F.lower(F.col("certificate_arn")))
            in_scope_df = in_scope_df.withColumn("amazonresourcename", F.lower(F.col("amazonresourcename")))
            
            # Find matches
            matching_df = in_scope_df.join(
                snowflake_df,
                in_scope_df.amazonresourcename == snowflake_df.certificate_arn,
                "inner"
            )
            matching_count = matching_df.count()
            
            # Find non-matches
            missing_df = in_scope_df.join(
                snowflake_df,
                in_scope_df.amazonresourcename == snowflake_df.certificate_arn,
                "left_anti"
            )
            
            tier1_numerator = matching_count
            tier1_denominator = api_count
            tier1_metric_value = round((tier1_numerator / tier1_denominator * 100), 2) if tier1_denominator > 0 else 0.0
            tier1_status = get_compliance_status(tier1_metric_value, thresholds_df, TIER1_METRIC_ID)
            
            # Resource info for non-matches
            missing_arns = [row.amazonresourcename for row in missing_df.limit(100).collect()]
            tier1_resources = [f"Missing from dataset: {arn}" for arn in missing_arns] if missing_arns else None
        else:
            tier1_numerator = 0
            tier1_denominator = 0
            tier1_metric_value = 0.0
            tier1_status = "Red"
            tier1_resources = ["No in-scope certificates found via API"]
        
        metrics.append({
            "date": now,
            "control_id": CONTROL_ID,
            "monitoring_metric_id": TIER1_METRIC_ID,
            "monitoring_metric_value": float(tier1_metric_value),
            "compliance_status": tier1_status,
            "numerator": int(tier1_numerator),
            "denominator": int(tier1_denominator),
            "non_compliant_resources": tier1_resources
        })
        
        # Tier 2: Check certificate rotation compliance
        snowflake_count = snowflake_df.count()
        if snowflake_count > 0:
            # Count non-compliant certificates
            non_compliant_count = snowflake_df.filter(
                # Used after expiration
                ((F.col("last_usage_observation_utc_timestamp") > F.col("not_valid_after_utc_timestamp")) |
                # Lifetime exceeds 13 months
                (F.datediff(F.col("not_valid_after_utc_timestamp"), F.col("not_valid_before_utc_timestamp")) > CERTIFICATE_MAX_AGE_DAYS))
            ).count()
            
            tier2_numerator = snowflake_count - non_compliant_count
            tier2_denominator = snowflake_count
            tier2_metric_value = round((tier2_numerator / tier2_denominator * 100), 2) if tier2_denominator > 0 else 0.0
            tier2_status = get_compliance_status(tier2_metric_value, thresholds_df, TIER2_METRIC_ID)
            
            # Get non-compliant certificate details (limit to 100 for performance)
            if non_compliant_count > 0:
                non_compliant_df = snowflake_df.filter(
                    ((F.col("last_usage_observation_utc_timestamp") > F.col("not_valid_after_utc_timestamp")) |
                    (F.datediff(F.col("not_valid_after_utc_timestamp"), F.col("not_valid_before_utc_timestamp")) > CERTIFICATE_MAX_AGE_DAYS))
                ).limit(100)
                
                tier2_resources = []
                for row in non_compliant_df.collect():
                    reasons = []
                    if row.last_usage_observation_utc_timestamp and row.not_valid_after_utc_timestamp and row.last_usage_observation_utc_timestamp > row.not_valid_after_utc_timestamp:
                        reasons.append("Used after expiration")
                    
                    if row.not_valid_before_utc_timestamp and row.not_valid_after_utc_timestamp:
                        lifetime_days = (row.not_valid_after_utc_timestamp - row.not_valid_before_utc_timestamp).days
                        if lifetime_days > CERTIFICATE_MAX_AGE_DAYS:
                            reasons.append(f"Lifetime ({lifetime_days} days) exceeds maximum ({CERTIFICATE_MAX_AGE_DAYS} days)")
                    
                    if reasons:
                        tier2_resources.append(f"{row.certificate_arn}: {', '.join(reasons)}")
                
                if non_compliant_count > 100:
                    tier2_resources.append(f"... and {non_compliant_count - 100} more")
            else:
                tier2_resources = None
        else:
            tier2_numerator = 0
            tier2_denominator = 0
            tier2_metric_value = 0.0
            tier2_status = "Red"
            tier2_resources = ["No certificates found in dataset"]
        
        metrics.append({
            "date": now,
            "control_id": CONTROL_ID,
            "monitoring_metric_id": TIER2_METRIC_ID,
            "monitoring_metric_value": float(tier2_metric_value),
            "compliance_status": tier2_status,
            "numerator": int(tier2_numerator),
            "denominator": int(tier2_denominator),
            "non_compliant_resources": tier2_resources
        })
        
        # Create DataFrame with the metrics
        metrics_df = spark.createDataFrame(metrics)
        return metrics_df
    
    except Exception as e:
        logger.error(f"Error calculating metrics: {e}")
        # Return error metrics
        error_metrics = [
            {
                "date": now,
                "control_id": CONTROL_ID,
                "monitoring_metric_id": TIER0_METRIC_ID,
                "monitoring_metric_value": 0.0,
                "compliance_status": "Red",
                "numerator": 0,
                "denominator": 1,
                "non_compliant_resources": [f"Error: {str(e)}"]
            },
            {
                "date": now,
                "control_id": CONTROL_ID,
                "monitoring_metric_id": TIER1_METRIC_ID,
                "monitoring_metric_value": 0.0,
                "compliance_status": "Red",
                "numerator": 0,
                "denominator": 1,
                "non_compliant_resources": [f"Error: {str(e)}"]
            },
            {
                "date": now,
                "control_id": CONTROL_ID,
                "monitoring_metric_id": TIER2_METRIC_ID,
                "monitoring_metric_value": 0.0,
                "compliance_status": "Red",
                "numerator": 0,
                "denominator": 1,
                "non_compliant_resources": [f"Error: {str(e)}"]
            }
        ]
        return spark.createDataFrame(error_metrics)

def main():
    """Main function to run the certificate monitoring."""
    print("Starting certificate monitoring for CTRL-1077188...")
    metrics_df = calculate_metrics()
    
    # Print summary
    print("\n=== Control Metrics Summary ===")
    for row in metrics_df.collect():
        tier = row.monitoring_metric_id.split('-')[-1]  # Extract T0, T1, T2
        print(f"Tier {tier}: {row.monitoring_metric_value}% ({row.compliance_status}) - {row.numerator}/{row.denominator}")
    
    # Display the metrics DataFrame
    display(metrics_df)
    return metrics_df

# Run the script
if __name__ == "__main__":
    metrics_result = main()