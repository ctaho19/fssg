import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

from pandas.core.api import DataFrame as DataFrame

from config_pipeline import ConfigPipeline
from etip_env import Env
from connectors.exchange.oauth_token import refresh

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Control IDs and metric IDs for Macie monitoring
MACIE_MONITORING_TIERS = {
    "CTRL-1079134": {
        "tier0_metric_id": "MNTR-1079134-T0",
        "tier1_metric_id": "MNTR-1079134-T1", 
        "tier2_metric_id": "MNTR-1079134-T2"
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
    pipeline = PLAmCTRL1079134Pipeline(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

class PLAmCTRL1079134Pipeline(ConfigPipeline):
    """Pipeline for monitoring Macie Compliance (CTRL-1079134)."""
    
    def __init__(self, env: Env) -> None:
        super().__init__(env)
        # Set up Exchange credentials
        try:
            self.client_id = self.env.exchange.client_id
            self.client_secret = self.env.exchange.client_secret
            self.exchange_url = self.env.exchange.exchange_url
        except AttributeError as e:
            logger.error(f"Missing OAuth configuration in environment: {e}")
            raise ValueError(f"Environment object missing expected OAuth attributes: {e}") from e
            
    def _get_api_token(self) -> str:
        """Refreshes the OAuth token for API calls."""
        logger.info(f"Refreshing API token from {self.exchange_url}...")
        try:
            token = refresh(
                client_id=self.client_id,
                client_secret=self.client_secret,
                exchange_url=self.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            raise RuntimeError("API token refresh failed") from e
    
    def extract_macie_metrics(self, threshold_df: pd.DataFrame, 
                             metrics_df: pd.DataFrame, 
                             testing_df: pd.DataFrame,
                             historical_df: pd.DataFrame) -> pd.DataFrame:
        """Extract and process Macie metrics from Snowflake data.
        
        Args:
            threshold_df: DataFrame containing metric thresholds
            metrics_df: DataFrame containing Macie metrics
            testing_df: DataFrame containing Macie control test results
            historical_df: DataFrame containing historical test statistics
            
        Returns:
            DataFrame containing calculated metrics for all tiers
        """
        control_id = "CTRL-1079134"
        current_date = datetime.now()
        result_df = pd.DataFrame(columns=METRIC_COLUMNS)
        
        try:
            # Process Tier 0 (Heartbeat) Metrics
            tier0_df = self._calculate_tier0_metrics(metrics_df, control_id, current_date)
            
            # Process Tier 1 (Completeness) Metrics
            tier1_df = self._calculate_tier1_metrics(metrics_df, threshold_df, control_id, current_date)
            
            # Process Tier 2 (Accuracy) Metrics
            tier2_df = self._calculate_tier2_metrics(testing_df, historical_df, threshold_df, control_id, current_date)
            
            # Combine all metrics
            result_df = pd.concat([result_df, tier0_df, tier1_df, tier2_df], ignore_index=True)
            
            logger.info(f"Successfully processed all metrics for {control_id}")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in extract_macie_metrics: {e}", exc_info=True)
            # Return empty DataFrame on error
            return pd.DataFrame(columns=METRIC_COLUMNS)
    
    def _calculate_tier0_metrics(self, metrics_df: pd.DataFrame, control_id: str, 
                                current_date: datetime) -> pd.DataFrame:
        """Calculate Tier 0 (Heartbeat) metrics for Macie monitoring.
        
        Args:
            metrics_df: DataFrame containing Macie metrics
            control_id: The control ID
            current_date: Current timestamp
            
        Returns:
            DataFrame with Tier 0 metrics
        """
        logger.info("Calculating Tier 0 (Heartbeat) metrics")
        
        # Initialize with default values
        tier0_metric_id = MACIE_MONITORING_TIERS[control_id]["tier0_metric_id"]
        monitoring_metric = 0.0
        monitoring_status = "Red"
        numerator = 0
        denominator = 1
        
        if not metrics_df.empty:
            # Check if data is current (within last day)
            is_current = pd.to_datetime(metrics_df['SF_LOAD_TIMESTAMP'].iloc[0]).date() >= \
                         (pd.Timestamp.now().date() - pd.Timedelta(days=1))
            
            buckets_scanned = metrics_df['TOTAL_BUCKETS_SCANNED_BY_MACIE'].iloc[0] \
                             if 'TOTAL_BUCKETS_SCANNED_BY_MACIE' in metrics_df.columns else 0
            
            # Calculate Tier 0 metric
            if is_current and buckets_scanned > 0:
                monitoring_metric = 100.0
                monitoring_status = "Green"
                numerator = buckets_scanned
            
            logger.info(f"Tier 0 metric: {monitoring_metric}, Status: {monitoring_status}")
        else:
            logger.warning("No metrics data available for Tier 0 calculation")
        
        # Create result DataFrame
        tier0_data = {
            "monitoring_metric_id": int(tier0_metric_id.split('-')[-1]),
            "control_id": control_id,
            "monitoring_metric_value": monitoring_metric,
            "monitoring_metric_status": monitoring_status,
            "metric_value_numerator": numerator,
            "metric_value_denominator": denominator,
            "resources_info": None,
            "control_monitoring_utc_timestamp": current_date
        }
        
        return pd.DataFrame([tier0_data])
    
    def _calculate_tier1_metrics(self, metrics_df: pd.DataFrame, threshold_df: pd.DataFrame, 
                               control_id: str, current_date: datetime) -> pd.DataFrame:
        """Calculate Tier 1 (Completeness) metrics for Macie monitoring.
        
        Args:
            metrics_df: DataFrame containing Macie metrics
            threshold_df: DataFrame containing metric thresholds
            control_id: The control ID
            current_date: Current timestamp
            
        Returns:
            DataFrame with Tier 1 metrics
        """
        logger.info("Calculating Tier 1 (Completeness) metrics")
        
        # Initialize with default values
        tier1_metric_id = MACIE_MONITORING_TIERS[control_id]["tier1_metric_id"]
        monitoring_metric = 0.0
        monitoring_status = "Red"
        numerator = 0
        denominator = 0
        
        if not metrics_df.empty:
            # Check if data is current (within last day)
            is_current = pd.to_datetime(metrics_df['SF_LOAD_TIMESTAMP'].iloc[0]).date() >= \
                         (pd.Timestamp.now().date() - pd.Timedelta(days=1))
            
            if is_current:
                buckets_scanned = metrics_df['TOTAL_BUCKETS_SCANNED_BY_MACIE'].iloc[0] \
                                if 'TOTAL_BUCKETS_SCANNED_BY_MACIE' in metrics_df.columns else 0
                total_buckets = metrics_df['TOTAL_CLOUDFRONTED_BUCKETS'].iloc[0] \
                              if 'TOTAL_CLOUDFRONTED_BUCKETS' in metrics_df.columns else 0
                
                # Calculate Tier 1 metric
                if total_buckets > 0:
                    monitoring_metric = round(100.0 * buckets_scanned / total_buckets, 2)
                    numerator = buckets_scanned
                    denominator = total_buckets
                
                # Get thresholds for Tier 1
                t1_thresholds = threshold_df[(threshold_df['control_id'] == control_id) & 
                                            (threshold_df['monitoring_metric_id'] == int(tier1_metric_id.split('-')[-1]))]
                
                alert_threshold = t1_thresholds['alerting_threshold'].iloc[0] if not t1_thresholds.empty else 95.0
                warning_threshold = t1_thresholds['warning_threshold'].iloc[0] if not t1_thresholds.empty else 97.0
                
                # Determine status
                if monitoring_metric >= alert_threshold:
                    monitoring_status = "Green"
                elif monitoring_metric >= warning_threshold:
                    monitoring_status = "Yellow"
                else:
                    monitoring_status = "Red"
            
            logger.info(f"Tier 1 metric: {monitoring_metric}, Status: {monitoring_status}")
        else:
            logger.warning("No metrics data available for Tier 1 calculation")
        
        # Create result DataFrame
        tier1_data = {
            "monitoring_metric_id": int(tier1_metric_id.split('-')[-1]),
            "control_id": control_id,
            "monitoring_metric_value": monitoring_metric,
            "monitoring_metric_status": monitoring_status,
            "metric_value_numerator": numerator,
            "metric_value_denominator": denominator,
            "resources_info": None,
            "control_monitoring_utc_timestamp": current_date
        }
        
        return pd.DataFrame([tier1_data])
    
    def _calculate_tier2_metrics(self, testing_df: pd.DataFrame, historical_df: pd.DataFrame,
                               threshold_df: pd.DataFrame, control_id: str, 
                               current_date: datetime) -> pd.DataFrame:
        """Calculate Tier 2 (Accuracy) metrics for Macie monitoring.
        
        Args:
            testing_df: DataFrame containing Macie control test results
            historical_df: DataFrame containing historical test statistics
            threshold_df: DataFrame containing metric thresholds
            control_id: The control ID
            current_date: Current timestamp
            
        Returns:
            DataFrame with Tier 2 metrics
        """
        logger.info("Calculating Tier 2 (Accuracy) metrics")
        
        # Initialize with default values
        tier2_metric_id = MACIE_MONITORING_TIERS[control_id]["tier2_metric_id"]
        monitoring_metric = 0.0
        monitoring_status = "Red"
        numerator = 0
        denominator = 0
        
        if not testing_df.empty:
            # Calculate total tests and successful tests
            total_tests = len(testing_df)
            successful_tests = testing_df['TESTISSUCCESSFUL'].sum() if 'TESTISSUCCESSFUL' in testing_df.columns else 0
            
            # Calculate Tier 2 metric
            if total_tests > 0:
                monitoring_metric = round(100.0 * successful_tests / total_tests, 2)
                numerator = successful_tests
                denominator = total_tests
                
                # Get thresholds for Tier 2
                t2_thresholds = threshold_df[(threshold_df['control_id'] == control_id) & 
                                           (threshold_df['monitoring_metric_id'] == int(tier2_metric_id.split('-')[-1]))]
                
                alert_threshold = t2_thresholds['alerting_threshold'].iloc[0] if not t2_thresholds.empty else 95.0
                warning_threshold = t2_thresholds['warning_threshold'].iloc[0] if not t2_thresholds.empty else 97.0
                
                # Check for anomalies in test count if historical data is available
                has_anomaly = False
                if not historical_df.empty:
                    avg_historical_tests = historical_df['AVG_HISTORICAL_TESTS'].iloc[0]
                    min_historical_tests = historical_df['MIN_HISTORICAL_TESTS'].iloc[0]
                    max_historical_tests = historical_df['MAX_HISTORICAL_TESTS'].iloc[0]
                    
                    # Test count outside historical range or deviates >20% from average
                    if (total_tests < min_historical_tests or 
                        total_tests > max_historical_tests or 
                        abs(total_tests - avg_historical_tests) > avg_historical_tests * 0.2):
                        has_anomaly = True
                
                # Determine status
                if monitoring_metric < alert_threshold:
                    monitoring_status = "Red"
                elif monitoring_metric < warning_threshold or has_anomaly:
                    monitoring_status = "Yellow"
                else:
                    monitoring_status = "Green"
            
            logger.info(f"Tier 2 metric: {monitoring_metric}, Status: {monitoring_status}")
        else:
            logger.warning("No test data available for Tier 2 calculation")
        
        # Create result DataFrame
        tier2_data = {
            "monitoring_metric_id": int(tier2_metric_id.split('-')[-1]),
            "control_id": control_id,
            "monitoring_metric_value": monitoring_metric,
            "monitoring_metric_status": monitoring_status,
            "metric_value_numerator": numerator,
            "metric_value_denominator": denominator,
            "resources_info": None,
            "control_monitoring_utc_timestamp": current_date
        }
        
        return pd.DataFrame([tier2_data])
    
    def extract(self) -> DataFrame:
        """Extract monitoring data from Snowflake."""
        logger.info("Starting extraction for CTRL-1079134 (Macie Monitoring)")
        df = super().extract()
        
        # Get data from Snowflake
        threshold_df = df.get("threshold_df", pd.DataFrame())
        metrics_df = df.get("macie_metrics_df", pd.DataFrame())
        testing_df = df.get("macie_testing_df", pd.DataFrame())
        historical_df = df.get("historical_stats_df", pd.DataFrame())
        
        # Extract and process Macie metrics
        df["macie_monitoring_df"] = self.extract_macie_metrics(
            threshold_df, metrics_df, testing_df, historical_df
        )
        
        return df