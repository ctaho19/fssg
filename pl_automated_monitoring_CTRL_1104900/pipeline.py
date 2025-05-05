import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

from pandas.core.api import DataFrame as DataFrame

from config_pipeline import ConfigPipeline
from etip_env import Env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Control IDs and metric IDs for Symantec Proxy monitoring
SYMANTEC_PROXY_MONITORING = {
    "CTRL-1104900": {
        "tier2_metric_id": "MNTR-1104900-T2",
        "platform": "symantec_proxy"
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
    pipeline = PLAmCTRL1104900Pipeline(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

class PLAmCTRL1104900Pipeline(ConfigPipeline):
    """Pipeline for monitoring Symantec Proxy control (CTRL-1104900)."""
    
    def __init__(self, env: Env) -> None:
        super().__init__(env)
    
    def extract_symantec_proxy_metrics(self, threshold_df: pd.DataFrame, outcome_df: pd.DataFrame) -> pd.DataFrame:
        """Extract and process Symantec Proxy test outcomes from Snowflake.
        
        Args:
            threshold_df: DataFrame containing metric thresholds
            outcome_df: DataFrame containing test outcomes from Symantec Proxy
            
        Returns:
            DataFrame containing calculated metrics for Tier 2
        """
        control_id = "CTRL-1104900"
        current_date = datetime.now()
        platform = SYMANTEC_PROXY_MONITORING[control_id]["platform"]
        tier2_metric_id = SYMANTEC_PROXY_MONITORING[control_id]["tier2_metric_id"]
        
        # Initialize metrics with default values
        monitoring_metric = 0.0
        monitoring_status = "Red"
        numerator = 0
        denominator = 0
        
        try:
            logger.info(f"Processing {control_id} metrics for platform {platform}")
            
            # Filter outcome data for the specific platform
            filtered_df = outcome_df
            if not filtered_df.empty:
                # Count total tests and successful tests
                total_tests = len(filtered_df)
                successful_tests = sum(filtered_df['EXPECTED_OUTCOME'] == filtered_df['ACTUAL_OUTCOME'])
                
                # Calculate metrics
                if total_tests > 0:
                    monitoring_metric = round(100.0 * successful_tests / total_tests, 2)
                    numerator = successful_tests
                    denominator = total_tests
                    
                    # Determine status based on thresholds
                    if monitoring_metric >= 90:
                        monitoring_status = "Green"
                    elif monitoring_metric >= 80:
                        monitoring_status = "Yellow"
                    else:
                        monitoring_status = "Red"
                else:
                    logger.warning(f"No test data found for {platform}. Setting metrics to 0.")
            else:
                logger.warning(f"Filtered DataFrame is empty for {platform}. Setting metrics to 0.")
            
            logger.info(f"Calculated metrics: {monitoring_metric}%, status: {monitoring_status}")
            
            # Create metrics DataFrame
            metrics_df = pd.DataFrame([{
                "monitoring_metric_id": int(tier2_metric_id.split('-')[-1]),
                "control_id": control_id,
                "monitoring_metric_value": monitoring_metric,
                "monitoring_metric_status": monitoring_status,
                "metric_value_numerator": numerator,
                "metric_value_denominator": denominator,
                "resources_info": None,
                "control_monitoring_utc_timestamp": current_date
            }])
            
            return metrics_df
            
        except Exception as e:
            logger.error(f"Error in extract_symantec_proxy_metrics: {e}", exc_info=True)
            # Return empty DataFrame with default values on error
            return pd.DataFrame([{
                "monitoring_metric_id": int(tier2_metric_id.split('-')[-1]),
                "control_id": control_id,
                "monitoring_metric_value": 0.0,
                "monitoring_metric_status": "Red",
                "metric_value_numerator": 0,
                "metric_value_denominator": 0,
                "resources_info": None,
                "control_monitoring_utc_timestamp": current_date
            }])
    
    def extract(self) -> DataFrame:
        """Extract monitoring data from Snowflake."""
        logger.info("Starting extraction for CTRL-1104900 (Symantec Proxy Monitoring)")
        df = super().extract()
        
        # Get data from Snowflake
        threshold_df = df.get("threshold_df", pd.DataFrame())
        outcome_df = df.get("symantec_proxy_outcome_df", pd.DataFrame())
        
        # Extract and process Symantec Proxy metrics
        df["symantec_proxy_monitoring_df"] = self.extract_symantec_proxy_metrics(
            threshold_df, outcome_df
        )
        
        return df