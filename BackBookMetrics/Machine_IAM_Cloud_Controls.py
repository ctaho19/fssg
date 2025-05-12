# Import necessary Python libraries and modules
import pyspark
import json  # For converting evidence data to JSON format
import logging  # For logging messages to track progress and errors
import sys  # For system-level operations like exiting on error
import re  # For regular expression pattern matching (e.g., validating metric IDs)
from datetime import datetime, date  # For handling dates and timestamps
import pandas as pd  # For temporary data manipulation before converting to Spark
import requests  # For making API calls to fetch approved accounts
from requests.adapters import HTTPAdapter  # For configuring HTTP retries
from requests.packages.urllib3.util.retry import Retry  # For retry logic on API calls
import traceback  # For detailed error stack traces in logs

# Import PySpark modules for data processing
from pyspark.sql import SparkSession  # Core class for Spark operations
from pyspark.sql.functions import lit, col  # Functions for DataFrame operations (e.g., adding columns, filtering)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  # For defining DataFrame schemas
from pyspark.sql.utils import AnalysisException  # For handling SQL-related errors

# Setup logging to track execution and debug issues
def setup_logging():
    # Configure logging to show INFO level messages and above (INFO, WARNING, ERROR)
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG for detailed logs; change to INFO for less verbosity
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True  # Ensure this overrides any existing logging config
    )
    logger = logging.getLogger(__name__)  # Get a logger specific to this module
    console_handler = logging.StreamHandler(sys.stdout)  # Output logs to console
    console_handler.setLevel(logging.DEBUG)  # Show all debug messages
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Format with timestamp
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)  # Attach the console output to the logger
    return logger

logger = setup_logging()  # Initialize the logger for use throughout the script
logger.info("Logging setup complete")

# Define the controls we're monitoring, including their IDs and metrics
CONTROL_CONFIGS = [
    {
        "cloud_control_id": "AC-3.AWS.39.v02",  # Lazer Control ID control ID (used in queries)
        "ctrl_id": "CTRL-1074653",              # FUSE Control ID (e.g., maps to monitoring metrics)
        "metric_ids": {                         # Metrics to calculate for this control
            "tier1": "MNTR-1074653-T1",         # Tier 1: Coverage metric
            "tier2": "MNTR-1074653-T2",         # Tier 2: Compliance metric
            "tier3": "MNTR-1074653-T3"          # Tier 3: SLA compliance metric
        },
        "requires_tier3": True                  # This control needs Tier 3
    },
    {
        "cloud_control_id": "AC-6.AWS.13.v01",
        "ctrl_id": "CTRL-1105806",
        "metric_ids": {
            "tier1": "MNTR-1105806-T1",
            "tier2": "MNTR-1105806-T2"
        },
        "requires_tier3": False                 # No Tier 3 for this control
    },
    {
        "cloud_control_id": "AC-6.AWS.35.v02",
        "ctrl_id": "CTRL-1077124",             # Machine IAM Cloud Control
        "metric_ids": {
            "tier1": "MNTR-1077124-T1",
            "tier2": "MNTR-1077124-T2"
        },
        "requires_tier3": False
    }
]

# Snowflake connection settings
username = dbutils.secrets.get(scope="uno201", key="eid") #NOTE: Using Chris' scope
password = dbutils.secrets.get(scope="uno201", key="password")
#username = dbutils.secrets.get(scope="sdd802_scope", key="eid") #NOTE: Using Aditya's scope
#password = dbutils.secrets.get(scope="sdd802_scope", key="password")

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = dict(
    sfUrl="cptlone-sfprod.snowflakecomputing.com", #"prod.us-east-1.capitalone.snowflakecomputing.com",
    sfUser=username,
    sfPassword=password,
    sfDatabase="SB",
    sfSchema="USER_UNO201", #NOTE: Using Chris's credentials
    #sfSchema="USER_SDD802", #NOTE: Using Aditya's credentials
    sfWarehouse="CYBR_Q_DI_WH"
)
# Define API settings for fetching approved accounts
API_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/accounts"
API_HEADERS = {
    'X-Cloud-Accounts-Business-Application': 'BACyberProcessAutomation',  # Identifies our app
    'Authorization': 'Bearer eyJwY2siOjEsImFsZyI6ImRpciIsImVuYyI6IkExMjhDQkMtSFMyNTYiLCJraWQiOiJyNHEiLCJ0diI6Mn0..GlTvA5yFSD0i7cjWt3ZlIA.ejG2I8LPSefBMsPVcR0jTad7c1CCBpZdVF-7-UGuF1vMjLmdaHFJXFQwH4tbeTgznB2ffk606dW7FqqVtr5G_FH_ceQCC-oVUX4qqlJCaBOAGtxkIWu1U_nbnaqp21XVuaPzW8FbjZbT6PHUGC1LJu9rEMejcWypxsaRxEDAAMrNICIub3vmCJE-V1ADo3EDj2oSSIAY89l1Xgj-1xkDRprVH-2lZEMAaLOsvg67oyplLeMdQliwYv07yI-uDAhEdVqteIPTJFLGIZAb2SvbTeA3e2H_LPSUbBumIv3TA00xdwLjwKfHqDKxEOtyY1j0.gnhTfrRDNr3AIQ4hH-9Oww',  # Token (replace with secure method)
    'Accept': 'application/json;v=2.0',  # Expect JSON response
    'Content-Type': 'application/json'  # Send JSON data
}
API_PARAMS = {
    'accountStatus': 'Active',  # Only fetch active accounts
    'region': ['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1', 'eu-west-2', 'ca-central-1']  # Regions to include
}
logger.info("Configuration defined for controls, Snowflake, and API")
# SQL query to fetch all IAM roles from Snowflake
ALL_IAM_ROLES_QUERY = """
SELECT DISTINCT  -- Remove duplicates
    RESOURCE_ID,  -- Unique ID for each role
    UPPER(AMAZON_RESOURCE_NAME) as AMAZON_RESOURCE_NAME,  -- Role ARN (uppercase for consistency)
    BA,  -- Business area
    ACCOUNT,  -- AWS account number
    CREATE_DATE,  -- When the role was created
    TYPE,  -- Type of resource (e.g., role)
    FULL_RECORD,  -- Raw JSON data
    ROLE_TYPE,  -- Machine or human role
    SF_LOAD_TIMESTAMP,  -- When data was loaded into Snowflake
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP  -- Current time of query
FROM EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_IAM_RESOURCE_V3
WHERE TYPE = 'role'  -- Only want roles, not users or groups
    AND AMAZON_RESOURCE_NAME LIKE 'arn:aws:iam::%role/%'  -- Filter for valid role ARNs
    AND NOT REGEXP_LIKE(UPPER(FULL_RECORD), '.(DENY[-]?ALL|QUARANTINEPOLICY).')  -- Exclude denied/quarantined roles
    AND DATE(SF_LOAD_TIMESTAMP) = CURRENT_DATE -- Only including roles loaded today
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RESOURCE_ID
    ORDER BY CREATE_DATE DESC) = 1
"""

# SQL query to fetch evaluated roles (compliance status) for a specific control
EVALUATED_ROLES_QUERY = """
SELECT DISTINCT
    UPPER(RESOURCE_NAME) as RESOURCE_NAME,  -- Role ARN (uppercase)
    COMPLIANCE_STATUS  -- Compliant or NonCompliant
FROM EIAM_DB.PHDP_CYBR_IAM.IDENTITY_REPORTS_CONTROLS_VIOLATIONS_STREAM_V2
WHERE CONTROL_ID = '{control_id}'
    AND DATE(CREATE_DATE) = CURRENT_DATE
    AND ROLE_TYPE = 'MACHINE'
"""

# SQL query to fetch thresholds for a specific metric
THRESHOLD_QUERY_TEMPLATE = """
SELECT
    MONITORING_METRIC_ID,  -- Metric ID (e.g., MNTR-CM2AWS12v02-T1)
    ALERT_THRESHOLD,  -- Value below which status is RED
    WARNING_THRESHOLD  -- Value below which status is YELLOW (but above alert)
FROM CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD
WHERE MONITORING_METRIC_ID = '{metric_id}'
"""
logger.info("SQL query templates defined")
def get_approved_accounts(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Fetch approved AWS accounts from an API and turn them into a Spark DataFrame."""
    # Setup retry logic for the API call (tries 3 times if it fails due to server errors)
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    http = requests.Session()  # Create a session to manage the API call
    http.mount("https://", HTTPAdapter(max_retries=retry_strategy))  # Apply retry logic
    try:
        logger.info("Fetching approved accounts from API")
        response = http.get(API_URL, headers=API_HEADERS, params=API_PARAMS, verify=False)  # Make the API call
        response.raise_for_status()  # Throw an error if the response isn't successful (e.g., 200 OK)

        data = response.json()  # Convert response to JSON
        # Extract account numbers, ensuring they're valid (not empty)
        account_numbers = [acc['accountNumber'] for acc in data['accounts'] if acc.get('accountNumber') and acc['accountNumber'].strip()]
        if not account_numbers:
            raise ValueError("No valid account numbers received from API")

        # Create a Spark DataFrame with just one column: ACCOUNT
        df_approved_accounts = spark.createDataFrame(
            [(acc,) for acc in account_numbers], ['ACCOUNT']
        ).cache()  # Cache it so it's fast to reuse
        df_approved_accounts.createOrReplaceTempView("approved_accounts")  # Make it available for SQL queries
        logger.info(f"Retrieved {df_approved_accounts.count()} approved accounts")
        logger.debug(f"Sample approved accounts: {df_approved_accounts.limit(5).collect()}")  # Log sample for debugging
        return df_approved_accounts

    except requests.RequestException as e:
        logger.error(f"API call failed: {str(e)}", exc_info=True)  # Log full error details
        raise
    except Exception as e:
        logger.error(f"Error processing approved accounts: {str(e)}", exc_info=True)
        raise
def validate_configs():
    """Check that our control configurations are correct before we start."""
    for config in CONTROL_CONFIGS:
        control_id = config["cloud_control_id"]  # Cloud control ID (e.g., CM-2.AWS.12.v02)
        ctrl_id = config.get("ctrl_id")  # Control ID for Snowflake (e.g., CTRL-1234567)
        
        # Ensure ctrl_id exists and looks valid (e.g., CTRL- followed by numbers)
        if not ctrl_id or not re.match(r'^CTRL-\d+', ctrl_id):
            logger.error(f"Invalid ctrl_id for {control_id}: {ctrl_id}")
            raise ValueError(f"Invalid ctrl_id: {ctrl_id}")
            
        # Check metric IDs for each tier
        for tier, metric_id in config.get("metric_ids", {}).items():
            if not metric_id or not re.match(r'^MNTR-\d+-T[123]$', metric_id):
                logger.error(f"Invalid metric ID for {control_id} in {tier}: {metric_id}")
                raise ValueError(f"Invalid metric ID: {metric_id}")
    
    logger.info("Control configurations validated successfully")
def calculate_metrics(alert_val, warning_val, numerator, denominator):
    """Calculate a percentage and status (RED, YELLOW, GREEN, UNKNOWN) based on thresholds."""
    alert = float(alert_val) if alert_val is not None else None  # Convert alert to float if present
    warning = float(warning_val) if warning_val is not None else None  # Convert warning to float if present
    numerator = float(numerator)  # Ensure float for Snowflake
    denominator = float(denominator)  # Ensure float for Snowflake
    
    # Calculate percentage (100% if denominator is 0)
    metric = numerator / denominator * 100 if denominator > 0 else 100.0
    metric = round(metric, 2)

    # Determine status based on available thresholds
    if alert is not None:  # If alert threshold exists, use it
        if metric < alert:
            status = "RED"  # Below alert is always RED
        elif warning is not None and metric < warning:  # Check warning only if it exists
            status = "YELLOW"  # Between alert and warning is YELLOW
        else:
            status = "GREEN"  # Above warning (or alert if no warning) is GREEN
    else:
        status = "UNKNOWN"  # No alert threshold means we can't judge compliance
        logger.warning(f"No alert threshold provided; status set to UNKNOWN for metric calculation")

    logger.debug(f"Metric calculated: {metric}%, status: {status}, alert={alert}, warning={warning}, num={numerator}, denom={denominator}")
    return {
        "metric": metric,
        "status": status,
        "numerator": numerator,
        "denominator": denominator
    }
Use code with caution.
def process_tier1(spark, control_config):
"""Calculate Tier 1 metrics (coverage: % of roles evaluated) and evidence."""
metric_id = control_config["metric_ids"]["tier1"]  # Get the Tier 1 metric ID (e.g., MNTR-1234567-T1)
logger.info(f"Processing Tier 1 for control {control_config['cloud_control_id']}, metric {metric_id}")
# Load thresholds (alert and warning levels) for this metric from Snowflake
tier1_threshold_query = THRESHOLD_QUERY_TEMPLATE.format(metric_id=metric_id)
threshold_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", tier1_threshold_query).load()
threshold_data = threshold_df.first()  # Expect one row with thresholds
alert = threshold_data["ALERT_THRESHOLD"] if threshold_data else None  # RED if below this
warning = threshold_data["WARNING_THRESHOLD"] if threshold_data else None  # YELLOW if below this
logger.debug(f"Tier 1 thresholds: alert={alert}, warning={warning}")

# Filter roles to only those in approved accounts and of type MACHINE
df_filtered = spark.sql("""
    SELECT RESOURCE_ID, AMAZON_RESOURCE_NAME, ACCOUNT, BA, ROLE_TYPE
    FROM all_iam_roles
    WHERE ACCOUNT IN (SELECT ACCOUNT FROM approved_accounts)  -- Only approved accounts
      AND ROLE_TYPE = 'MACHINE'  -- Only machine roles
""")
total_roles = df_filtered.count()  # Count total machine roles
logger.debug(f"Total machine roles: {total_roles}")

# Get roles that have been evaluated for compliance
evaluated_roles = spark.table("evaluated_roles")
# Count how many of our filtered roles were evaluated
evaluated_count = df_filtered.join(
    evaluated_roles, 
    df_filtered.AMAZON_RESOURCE_NAME == evaluated_roles.RESOURCE_NAME, 
    "inner"  # Only keep matches
).count()
logger.debug(f"Evaluated roles: {evaluated_count}")

# Calculate the coverage metric (% of roles evaluated)
metrics = calculate_metrics(alert, warning, evaluated_count, total_roles)
# Create a DataFrame matching Snowflake's schema
metrics_df = spark.createDataFrame([(
    date.today(),              # DATE: Today's date
    control_config["ctrl_id"], # CTRL_ID: Control ID (e.g., CTRL-1234567)
    metric_id,                 # MONITORING_METRIC_NUMBER: Metric ID
    metrics["metric"],         # MONITORING_METRIC: Percentage (float)
    metrics["status"],         # COMPLIANCE_STATUS: GREEN/YELLOW/RED
    metrics["numerator"],      # NUMERATOR: Number evaluated (float)
    metrics["denominator"]     # DENOMINATOR: Total roles (float)
)], ["DATE", "CTRL_ID", "MONITORING_METRIC_NUMBER", "MONITORING_METRIC", "COMPLIANCE_STATUS", "NUMERATOR", "DENOMINATOR"])
logger.info(f"Tier 1 metrics: {metrics}")

# Create evidence showing which roles were evaluated
evidence_df = df_filtered.join(
    evaluated_roles, 
    df_filtered.AMAZON_RESOURCE_NAME == evaluated_roles.RESOURCE_NAME, 
    "left_outer"  # Include all filtered roles, even if not evaluated
).select("RESOURCE_ID", "AMAZON_RESOURCE_NAME", col("COMPLIANCE_STATUS").alias("EVALUATION_STATUS"))
logger.debug(f"Tier 1 evidence rows: {evidence_df.count()}")

return metrics_df, evidence_df  # Return both for writing and displaying
Use code with caution.
def process_tier2(spark, control_config):
"""Calculate Tier 2 metrics (compliance: % of roles compliant) and evidence."""
metric_id = control_config["metric_ids"]["tier2"]
logger.info(f"Processing Tier 2 for control {control_config['cloud_control_id']}, metric {metric_id}")
# Load thresholds for Tier 2
tier2_threshold_query = THRESHOLD_QUERY_TEMPLATE.format(metric_id=metric_id)
threshold_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", tier2_threshold_query).load()
threshold_data = threshold_df.first()
alert = threshold_data["ALERT_THRESHOLD"] if threshold_data else None
warning = threshold_data["WARNING_THRESHOLD"] if threshold_data else None
logger.debug(f"Tier 2 thresholds: alert={alert}, warning={warning}")

# Combine all roles with their compliance status
df_combined = spark.sql("""
    SELECT a.RESOURCE_ID, a.AMAZON_RESOURCE_NAME, a.ACCOUNT, a.BA, a.ROLE_TYPE, e.COMPLIANCE_STATUS
    FROM all_iam_roles a
    LEFT JOIN evaluated_roles e  -- Include all roles, even if not evaluated
    ON a.AMAZON_RESOURCE_NAME = e.RESOURCE_NAME
    WHERE a.ACCOUNT IN (SELECT ACCOUNT FROM approved_accounts)
      AND a.ROLE_TYPE = 'MACHINE'
""")
total_roles = df_combined.count()  # Total machine roles
logger.debug(f"Total roles in Tier 2: {total_roles}")

# Count roles that are compliant
compliant_roles = df_combined.filter(col("COMPLIANCE_STATUS").isin(["Compliant", "CompliantControlAllowance"])).count()
logger.debug(f"Compliant roles: {compliant_roles}")

# Calculate compliance metric (% compliant)
metrics = calculate_metrics(alert, warning, compliant_roles, total_roles)
metrics_df = spark.createDataFrame([(
    date.today(),
    control_config["ctrl_id"],  # Use Control ID
    metric_id,
    metrics["metric"],
    metrics["status"],
    metrics["numerator"],
    metrics["denominator"]
)], ["DATE", "CTRL_ID", "MONITORING_METRIC_NUMBER", "MONITORING_METRIC", "COMPLIANCE_STATUS", "NUMERATOR", "DENOMINATOR"])
logger.info(f"Tier 2 metrics: {metrics}")

# Save combined data for Tier 3 to use
df_combined.createOrReplaceTempView("evaluated_roles_with_compliance")
# Evidence includes all roles and their compliance status
evidence_df = df_combined.select("RESOURCE_ID", "AMAZON_RESOURCE_NAME", "COMPLIANCE_STATUS")
logger.debug(f"Tier 2 evidence rows: {evidence_df.count()}")

return metrics_df, evidence_df
Use code with caution.
def process_tier3(spark, control_config):
"""Calculate Tier 3 metrics (SLA compliance: % of non-compliant roles within SLA) and evidence."""
metric_id = control_config["metric_ids"]["tier3"]
logger.info(f"Processing Tier 3 for control {control_config['cloud_control_id']}, metric {metric_id}")
# Load thresholds for Tier 3
tier3_threshold_query = THRESHOLD_QUERY_TEMPLATE.format(metric_id=metric_id)
threshold_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", tier3_threshold_query).load()
threshold_data = threshold_df.first()
alert = threshold_data["ALERT_THRESHOLD"] if threshold_data else None
warning = threshold_data["WARNING_THRESHOLD"] if threshold_data else None
logger.debug(f"Tier 3 thresholds: alert={alert}, warning={warning}")

# Check if Tier 2 data is available (needed for non-compliant roles)
#if not 'evaluated_roles_with_compliance' in spark.catalog.listTables():
    #logger.error("Missing evaluated_roles_with_compliance from Tier 2")
    #raise ValueError("evaluated_roles_with_compliance temp view not found. Ensure Tier 2 ran successfully.")
df_evaluated_roles = spark.table("evaluated_roles_with_compliance")

# Count non-compliant roles
total_non_compliant = df_evaluated_roles.filter(col("COMPLIANCE_STATUS") == "NonCompliant").count()
logger.debug(f"Total non-compliant roles: {total_non_compliant}")

if total_non_compliant <= 0:
    # If no non-compliant roles, assume perfect compliance
    logger.warning("No non-compliant roles found. Setting metric to 100% GREEN.")
    metrics = {"metric": 100.0, "status": "GREEN", "numerator": 0.0, "denominator": 0.0}
    evidence_data = {
        "RESOURCE_ID": [None], "AMAZON_RESOURCE_NAME": [None], "ACCOUNT": [None], "BA": [None], "ROLE_TYPE": [None],
        "CONTROL_RISK": [None], "OPEN_DATE": [None], "DAYS_OPEN": [None], "SLA_LIMIT": [None], "SLA_STATUS": [None],
        "NOTES": ["All Evaluated Roles are Compliant"]
    }
else:
    # Get details of non-compliant roles
    non_compliant_resources = [(row["RESOURCE_ID"], row["AMAZON_RESOURCE_NAME"], row["ACCOUNT"], row["BA"], row["ROLE_TYPE"])
                               for row in df_evaluated_roles.filter(col("COMPLIANCE_STATUS") == "NonCompliant").collect()]
    resource_ids = [resource_id for resource_id, _, _, _, _ in non_compliant_resources]
    resource_id_list = ",".join([f"'{rid}'" for rid in resource_ids])  # Format for SQL
    logger.debug(f"Non-compliant resource IDs: {len(resource_ids)}")

    # Query SLA data (how long non-compliance issues have been open)
    sla_query = f"""
    SELECT 
        RESOURCE_ID,
        CONTROL_RISK,  -- Risk level (e.g., High, Medium)
        OPEN_DATE_UTC_TIMESTAMP  -- When the issue was reported
    FROM CLCN_DB.PHDP_CLOUD.OZONE_NON_COMPLIANT_RESOURCES_TCRD_VIEW_V01
    WHERE CONTROL_ID = '{control_config["cloud_control_id"]}'  -- Use cloud_control_id here
      AND RESOURCE_ID IN ({resource_id_list})
      AND ID NOT IN (SELECT ID FROM CLCN_DB.PHDP_CLOUD.OZONE_CLOSED_NON_COMPLIANT_RESOURCES_V04)"""
    df_sla_data = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", sla_query).load()
    logger.debug(f"SLA data rows: {df_sla_data.count()}")

    # Define SLA time limits by risk level (days before overdue)
    sla_thresholds = {"Critical": 0, "High": 30, "Medium": 60, "Low": 90}
    current_date = datetime.now()  # Today's date for comparison
    sla_data = [(row["RESOURCE_ID"], row["CONTROL_RISK"], row["OPEN_DATE_UTC_TIMESTAMP"]) for row in df_sla_data.collect()]
    
    # Calculate how many issues are past their SLA
    past_sla_count = 0
    sla_data_map = {}  # Store SLA details for evidence
    for resource_id, control_risk, open_date in sla_data:
        if open_date is not None and control_risk in sla_thresholds:
            days_open = (current_date - open_date).days  # Days since issue opened
            sla_limit = sla_thresholds.get(control_risk, 90)  # Default to 90 days if unknown
            sla_status = "Past SLA" if days_open > sla_limit else "Within SLA"
            if days_open > sla_limit:
                past_sla_count += 1
        else:
            days_open = None
            sla_limit = None
            sla_status = "Unknown"
            logger.warning(f"Invalid SLA data for resource_id={resource_id}")
        sla_data_map[resource_id] = {
            "CONTROL_RISK": control_risk, "OPEN_DATE": open_date, "DAYS_OPEN": days_open,
            "SLA_LIMIT": sla_limit, "SLA_STATUS": sla_status
        }

    # Calculate how many are within SLA
    within_sla_count = total_non_compliant - past_sla_count if total_non_compliant >= past_sla_count else 0
    metrics = calculate_metrics(alert, warning, within_sla_count, total_non_compliant)
    logger.info(f"Tier 3 metrics: {metrics}")

    # Build evidence with SLA details for non-compliant roles
    evidence_rows = []
    for resource_id, arn, account, ba, role_type in non_compliant_resources:
        sla_data = sla_data_map.get(resource_id, {
            "CONTROL_RISK": "Unknown", "OPEN_DATE": None, "DAYS_OPEN": None, "SLA_LIMIT": None, "SLA_STATUS": "Unknown"
        })
        evidence_rows.append((
            resource_id, arn, account, ba, role_type,
            sla_data["CONTROL_RISK"], sla_data["OPEN_DATE"], str(sla_data["DAYS_OPEN"]), str(sla_data["SLA_LIMIT"]),
            sla_data["SLA_STATUS"], f"NonCompliant - {sla_data['SLA_STATUS']}"
        ))

# Define the structure (schema) for the evidence DataFrame
evidence_schema = StructType([
    StructField("RESOURCE_ID", StringType(), True),
    StructField("AMAZON_RESOURCE_NAME", StringType(), True),
    StructField("ACCOUNT", StringType(), True),
    StructField("BA", StringType(), True),
    StructField("ROLE_TYPE", StringType(), True),
    StructField("CONTROL_RISK", StringType(), True),
    StructField("OPEN_DATE", TimestampType(), True),
    StructField("DAYS_OPEN", StringType(), True),
    StructField("SLA_LIMIT", StringType(), True),
    StructField("SLA_STATUS", StringType(), True),
    StructField("NOTES", StringType(), True)
])

# Create metrics DataFrame for Snowflake
metrics_df = spark.createDataFrame([(
    date.today(),
    control_config["ctrl_id"],  # Use Control ID
    metric_id,
    metrics["metric"],
    metrics["status"],
    metrics["numerator"],
    metrics["denominator"]
)], ["DATE", "CTRL_ID", "MONITORING_METRIC_NUMBER", "MONITORING_METRIC", "COMPLIANCE_STATUS", "NUMERATOR", "DENOMINATOR"])

# Create evidence DataFrame (stored but not written to Snowflake)
evidence_df = spark.createDataFrame(
    evidence_rows if total_non_compliant > 0 else [(
        None, None, None, None, None, None, None, None, None, None, "All Evaluated Roles are Compliant"
    )],
    schema=evidence_schema
)
logger.debug(f"Tier 3 evidence rows: {evidence_df.count()}")

return metrics_df, evidence_df
Use code with caution.
def write_to_snowflake(spark, df, metric_id, evidence_df):
"""Save metrics to Snowflake (evidence is stored separately, not written)."""
logger.info(f"Writing metrics for {metric_id} to Snowflake")
# Write only the metrics DataFrame to Snowflake (no evidence column exists yet)
df.write.format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("dbtable", "CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING")\
    .mode("append")\
    .save()
logger.info(f"Wrote metrics for {metric_id} to Snowflake (evidence stored separately with {evidence_df.count()} rows)")
# Evidence is kept in memory for display, not written to Snowflake
Use code with caution.
def main(spark):
"""Run the entire pipeline and display results."""
logger.info("Starting consolidated control pipeline")
# Fetch approved accounts from the API
try:
    df_approved_accounts = get_approved_accounts(spark)
except Exception as e:
    logger.error(f"Failed to fetch approved accounts: {str(e)}", exc_info=True)
    raise

# Load all IAM roles from Snowflake
try:
    df_all_iam_roles = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", ALL_IAM_ROLES_QUERY).load()
    df_all_iam_roles.cache().createOrReplaceTempView("all_iam_roles")
    if df_all_iam_roles.isEmpty():
        raise ValueError("Empty IAM roles dataset")
    logger.info(f"Loaded {df_all_iam_roles.count()} IAM roles")
    logger.debug(f"IAM roles schema: {df_all_iam_roles.schema}")
except Exception as e:
    logger.error(f"Failed to load IAM roles: {str(e)}", exc_info=True)
    raise

# Dictionary to store results for all controls
results = {}

# Process each control in the configuration
for control in CONTROL_CONFIGS:
    logger.info(f"Processing control: {control['cloud_control_id']}")
    try:
        # Load evaluated roles for this control using cloud_control_id
        evaluated_query = EVALUATED_ROLES_QUERY.format(control_id=control["cloud_control_id"])
        df_evaluated_roles = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", evaluated_query).load()
        df_evaluated_roles.cache().createOrReplaceTempView("evaluated_roles")
        logger.debug(f"Evaluated roles count: {df_evaluated_roles.count()}")

        # Store results for this control
        results[control["cloud_control_id"]] = {"metrics": {}, "evidence": {}}

        # Process Tier 1
        tier1_df, tier1_evidence = process_tier1(spark, control)
        write_to_snowflake(spark, tier1_df, control["metric_ids"]["tier1"], tier1_evidence)
        results[control["cloud_control_id"]]["metrics"]["tier1"] = tier1_df
        results[control["cloud_control_id"]]["evidence"]["tier1"] = tier1_evidence

        # Process Tier 2
        tier2_df, tier2_evidence = process_tier2(spark, control)
        write_to_snowflake(spark, tier2_df, control["metric_ids"]["tier2"], tier2_evidence)
        results[control["cloud_control_id"]]["metrics"]["tier2"] = tier2_df
        results[control["cloud_control_id"]]["evidence"]["tier2"] = tier2_evidence

        # Process Tier 3 if required
        if control.get("requires_tier3", False):
            tier3_df, tier3_evidence = process_tier3(spark, control)
            write_to_snowflake(spark, tier3_df, control["metric_ids"]["tier3"], tier3_evidence)
            results[control["cloud_control_id"]]["metrics"]["tier3"] = tier3_df
            results[control["cloud_control_id"]]["evidence"]["tier3"] = tier3_evidence

    except Exception as e:
        logger.error(f"Error processing control {control['cloud_control_id']}: {str(e)}", exc_info=True)
        raise

# Display all results in a readable format
logger.info("Displaying final results for all controls")
for control_id, data in results.items():
    print(f"\n=== Control: {control_id} ===")
    for tier, metrics_df in data["metrics"].items():
        print(f"\nTier {tier} Metrics:")
        metrics_df.show(truncate=False)  # Show metrics without cutting off text
    for tier, evidence_df in data["evidence"].items():
        print(f"\nTier {tier} Evidence:")
        evidence_df.show(truncate=False)  # Show evidence without cutting off text

    logger.info("All controls processed and results displayed successfully")
    return results  # Return results in case you want to use them later
if __name__ == "__main__":
    # Check if we're running in a Spark environment (like Databricks)
    if 'spark' not in globals():
        logger.error("No Spark session found. Must run in Databricks or similar.")
        raise Exception("No Spark session found. This script must be run in a Spark environment.")
    
    # Start a Spark session with specific settings
    spark = spark

    try:
        validate_configs()  # Check configurations before running
        main(spark)  # Execute the pipeline
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)